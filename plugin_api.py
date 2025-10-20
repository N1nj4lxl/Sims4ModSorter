from __future__ import annotations

import json
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional


@dataclass
class _TimelineEvent:
    elapsed: float
    message: str
    level: str = "info"


@dataclass
class _PluginMetric:
    key: str
    display_name: str
    total_time: float = 0.0
    files: Optional[int] = None
    warnings: Optional[int] = None
    status: str = "Idle"
    active_count: int = 0
    last_start: Optional[float] = None

    def start(self, now: float) -> None:
        self.active_count += 1
        if self.active_count == 1:
            self.last_start = now
            self.status = "Running"

    def stop(
        self,
        now: float,
        *,
        status: Optional[str] = None,
        files_processed: Optional[int] = None,
        warnings: Optional[int] = None,
    ) -> None:
        if self.active_count > 0:
            self.active_count -= 1
            if self.active_count == 0 and self.last_start is not None:
                self.total_time += max(0.0, now - self.last_start)
                self.last_start = None
        if files_processed is not None:
            self.files = files_processed
        if warnings is not None:
            self.warnings = warnings
        if status:
            self.status = status
        elif warnings is not None:
            self.status = "Warning" if warnings else "Done"
        elif self.active_count == 0 and self.status == "Running":
            self.status = "Done"

    def current_elapsed(self, now: float) -> float:
        if self.active_count > 0 and self.last_start is not None:
            return now - self.last_start
        return 0.0


@dataclass
class _SessionData:
    session_id: int
    started_at: float
    started_wall: float
    label: str
    plugin_order: List[str] = field(default_factory=list)
    plugin_metrics: Dict[str, _PluginMetric] = field(default_factory=dict)
    events: List[_TimelineEvent] = field(default_factory=list)
    total_files: int = 0
    warnings: int = 0
    extras: Dict[str, int] = field(default_factory=dict)
    completed: bool = False
    total_time: float = 0.0

    def ensure_metric(self, key: str, display: Optional[str] = None) -> _PluginMetric:
        metric = self.plugin_metrics.get(key)
        if metric is None:
            display_name = display or key
            metric = _PluginMetric(key=key, display_name=display_name)
            self.plugin_metrics[key] = metric
            self.plugin_order.append(key)
        elif display and metric.display_name != display:
            metric.display_name = display
        return metric

    def snapshot(self) -> Dict[str, Any]:
        now = time.perf_counter()
        if self.completed:
            duration = self.total_time
        else:
            duration = max(0.0, now - self.started_at)
        plugins: List[Dict[str, Any]] = []
        for key in self.plugin_order:
            metric = self.plugin_metrics[key]
            elapsed = metric.total_time
            if metric.active_count > 0:
                elapsed += metric.current_elapsed(now)
            entry = {
                "name": metric.display_name,
                "time": round(elapsed, 3),
                "files": metric.files if metric.files is not None else 0,
                "warnings": metric.warnings if metric.warnings is not None else 0,
                "status": "Running" if metric.active_count > 0 else metric.status,
            }
            plugins.append(entry)
        events = [
            {
                "offset": _format_elapsed(event.elapsed),
                "message": event.message,
                "level": event.level,
            }
            for event in self.events
        ]
        return {
            "session_id": self.session_id,
            "label": self.label,
            "started_at": self.started_wall,
            "duration": duration,
            "plugins": plugins,
            "events": events,
            "total_files": self.total_files,
            "warnings": self.warnings,
            "extras": dict(self.extras),
            "completed": self.completed,
        }

    def to_history_entry(self) -> Dict[str, Any]:
        snapshot = self.snapshot()
        return {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.started_wall)),
            "plugins": [
                {
                    "name": entry["name"],
                    "time": round(entry["time"], 3),
                    "files": entry["files"],
                    "warnings": entry["warnings"],
                }
                for entry in snapshot["plugins"]
            ],
            "total_time": round(snapshot["duration"], 3),
            "total_files": snapshot["total_files"],
            "events": snapshot["events"],
        }


def _format_elapsed(elapsed: float) -> str:
    seconds = int(max(0.0, elapsed))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"[{hours:02d}:{minutes:02d}:{secs:02d}]"
    return f"[{minutes:02d}:{secs:02d}]"


class ScanMetricsAggregator:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._session_counter = 0
        self._current: Optional[_SessionData] = None
        self._listeners: Dict[int, Callable[[Dict[str, Any]], None]] = {}
        self._listener_index = 0
        self._history: Deque[Dict[str, Any]] = deque(maxlen=10)
        self._last_summary: Optional[str] = None
        self._logs_dir = Path(__file__).resolve().parent / "Logs" / "scan_history"
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        self._load_history()

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------
    def begin_session(self, *, plugins: Optional[Iterable[str]] = None) -> None:
        with self._lock:
            if self._current is not None and not self._current.completed:
                self._finalise_session(self._current, cancelled=True)
            self._session_counter += 1
            session_id = self._session_counter
            session = _SessionData(
                session_id=session_id,
                started_at=time.perf_counter(),
                started_wall=time.time(),
                label=f"Session {session_id}",
            )
            if plugins:
                for name in plugins:
                    text = str(name).strip()
                    if text:
                        session.ensure_metric(text, text)
            self._current = session
            self._last_summary = None
            session.events.append(_TimelineEvent(0.0, "Starting scan..."))
            self._notify_listeners_locked()

    def complete_session(
        self,
        *,
        total_files: int,
        warnings: Optional[int] = None,
        extras: Optional[Dict[str, int]] = None,
        cancelled: bool = False,
        error: bool = False,
    ) -> None:
        with self._lock:
            session = self._current
            if session is None:
                return
            now = time.perf_counter()
            session.total_time = max(0.0, now - session.started_at)
            session.total_files = max(0, int(total_files))
            if warnings is not None:
                session.warnings = max(0, int(warnings))
            if extras:
                session.extras.update({key: int(value) for key, value in extras.items()})
            active_plugins = self._count_active_plugins(session)
            if cancelled:
                summary = (
                    f"Scan cancelled — {session.total_files} files "
                    f"({active_plugins} plugin{'s' if active_plugins != 1 else ''} active)"
                )
                session.events.append(_TimelineEvent(session.total_time, "Scan cancelled."))
            elif error:
                summary = (
                    f"Scan failed — {session.total_files} files "
                    f"({active_plugins} plugin{'s' if active_plugins != 1 else ''} active)"
                )
                session.events.append(_TimelineEvent(session.total_time, "Scan failed."))
            else:
                summary = (
                    f"Scan complete — {session.total_files} files in {session.total_time:.2f} s "
                    f"({active_plugins} plugin{'s' if active_plugins != 1 else ''} active)"
                )
                session.events.append(_TimelineEvent(session.total_time, summary))
            session.completed = True
            self._last_summary = summary
            self._finalise_session(session)
            self._current = None
            self._notify_listeners_locked()

    # ------------------------------------------------------------------
    def start(self, key: str, *, display: Optional[str] = None) -> None:
        identifier = str(key).strip()
        if not identifier:
            return
        now = time.perf_counter()
        with self._lock:
            session = self._current
            if session is None:
                return
            metric = session.ensure_metric(identifier, display)
            metric.start(now)
            session.events.append(
                _TimelineEvent(max(0.0, now - session.started_at), f"{metric.display_name} started.")
            )
            self._notify_listeners_locked()

    def stop(
        self,
        key: str,
        *,
        display: Optional[str] = None,
        files_processed: Optional[int] = None,
        warnings: Optional[int] = None,
        status: Optional[str] = None,
    ) -> None:
        identifier = str(key).strip()
        if not identifier:
            return
        now = time.perf_counter()
        with self._lock:
            session = self._current
            if session is None:
                return
            metric = session.ensure_metric(identifier, display)
            metric.stop(
                now,
                status=status,
                files_processed=files_processed,
                warnings=warnings,
            )
            event_parts = [f"{metric.display_name} finished"]
            if metric.files is not None:
                event_parts.append(f"files={metric.files}")
            if metric.warnings:
                event_parts.append(f"warnings={metric.warnings}")
            message = "; ".join(event_parts)
            session.events.append(
                _TimelineEvent(max(0.0, now - session.started_at), message)
            )
            self._notify_listeners_locked()

    def log(self, message: str, level: str = "info") -> None:
        text = str(message).strip()
        if not text:
            return
        level_norm = level.lower() if isinstance(level, str) else "info"
        with self._lock:
            session = self._current
            if session is None:
                return
            now = time.perf_counter()
            session.events.append(
                _TimelineEvent(max(0.0, now - session.started_at), text, level_norm)
            )
            self._notify_listeners_locked()

    # ------------------------------------------------------------------
    def latest_summary(self) -> Optional[str]:
        with self._lock:
            return self._last_summary

    def register_listener(self, callback: Callable[[Dict[str, Any]], None]) -> Callable[[], None]:
        if not callable(callback):
            return lambda: None
        with self._lock:
            listener_id = self._listener_index
            self._listener_index += 1
            self._listeners[listener_id] = callback
            snapshot = self._current.snapshot() if self._current else None
            history = list(self._history)
        if snapshot is not None:
            try:
                callback({"session": snapshot, "history": history})
            except Exception:
                pass
        else:
            try:
                callback({"session": None, "history": history})
            except Exception:
                pass

        def _remove() -> None:
            with self._lock:
                self._listeners.pop(listener_id, None)

        return _remove

    def get_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._history)

    # ------------------------------------------------------------------
    def _finalise_session(self, session: _SessionData, cancelled: bool = False) -> None:
        entry = session.to_history_entry()
        self._history.appendleft(entry)
        self._write_history_entry(session)
        self._prune_history_files()

    def _count_active_plugins(self, session: _SessionData) -> int:
        count = 0
        now = time.perf_counter()
        for key in session.plugin_order:
            metric = session.plugin_metrics.get(key)
            if not metric:
                continue
            elapsed = metric.total_time
            if metric.active_count > 0:
                elapsed += metric.current_elapsed(now)
            if elapsed > 0.0 or (metric.files or metric.warnings):
                count += 1
        return count

    def _notify_listeners_locked(self) -> None:
        snapshot = self._current.snapshot() if self._current else None
        history = list(self._history)
        listeners = list(self._listeners.values())
        payload = {"session": snapshot, "history": history}
        for listener in listeners:
            try:
                listener(payload)
            except Exception:
                continue

    def _load_history(self) -> None:
        files = sorted(self._logs_dir.glob("*.json"), reverse=True)
        for path in files[:10]:
            try:
                with path.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
                if isinstance(data, dict):
                    self._history.append(data)
            except Exception:
                continue

    def _write_history_entry(self, session: _SessionData) -> None:
        filename = time.strftime("scan_%Y%m%dT%H%M%S", time.gmtime(session.started_wall))
        path = self._logs_dir / f"{filename}_{session.session_id:03d}.json"
        try:
            with path.open("w", encoding="utf-8") as handle:
                json.dump(session.to_history_entry(), handle, indent=2)
        except Exception:
            pass

    def _prune_history_files(self) -> None:
        files = sorted(self._logs_dir.glob("*.json"), reverse=True)
        for path in files[10:]:
            try:
                path.unlink()
            except Exception:
                continue


scan_metrics = ScanMetricsAggregator()

