"""Shared plugin API helpers for Sims4 Mod Sorter plugins."""
from __future__ import annotations

import copy
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

BASE_DIR = Path(__file__).resolve().parent
HISTORY_DIR = BASE_DIR / "Logs" / "scan_history"
_HISTORY_LIMIT = 10


def _ensure_history_dir() -> None:
    try:
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _format_offset(seconds: float) -> str:
    seconds = max(0.0, seconds)
    minutes = int(seconds // 60)
    remainder = int(seconds % 60)
    return f"{minutes:02d}:{remainder:02d}"


@dataclass
class _PluginStats:
    name: str
    total_time: float = 0.0
    files: int = 0
    warnings: int = 0
    status: str = "Idle"
    runs: int = 0


@dataclass
class _LogEntry:
    timestamp: float
    message: str
    level: str
    plugin: Optional[str]
    offset: float


@dataclass
class _Session:
    session_id: int
    started_wall: float
    started_perf: float
    plugins: Dict[str, _PluginStats] = field(default_factory=dict)
    active: Dict[str, float] = field(default_factory=dict)
    logs: List[_LogEntry] = field(default_factory=list)
    completed_wall: Optional[float] = None
    total_files: int = 0
    total_time: float = 0.0
    plugin_count: int = 0
    failed: bool = False

    @property
    def label(self) -> str:
        return f"Session {self.session_id}"

    def snapshot(self) -> Dict[str, object]:
        plugins = [
            {
                "name": stats.name,
                "time": round(stats.total_time, 3),
                "files": stats.files,
                "warnings": stats.warnings,
                "status": stats.status,
                "runs": stats.runs,
            }
            for stats in self.plugins.values()
        ]
        plugins.sort(key=lambda item: (-item["time"], item["name"]))
        logs = [
            {
                "offset": _format_offset(entry.offset),
                "message": entry.message,
                "level": entry.level,
                "plugin": entry.plugin,
                "timestamp": _iso(entry.timestamp),
            }
            for entry in self.logs[-500:]
        ]
        return {
            "session_id": self.session_id,
            "label": self.label,
            "started": _iso(self.started_wall),
            "completed": _iso(self.completed_wall) if self.completed_wall else None,
            "plugins": plugins,
            "logs": logs,
            "total_time": round(self.total_time, 3),
            "total_files": self.total_files,
            "plugin_count": self.plugin_count or len(plugins),
            "running": self.completed_wall is None,
            "failed": self.failed,
        }

    def to_json(self) -> Dict[str, object]:
        return {
            "session_id": self.session_id,
            "started": _iso(self.started_wall),
            "completed": _iso(self.completed_wall) if self.completed_wall else None,
            "total_time": self.total_time,
            "total_files": self.total_files,
            "plugin_count": self.plugin_count,
            "failed": self.failed,
            "plugins": [
                {
                    "name": stats.name,
                    "total_time": stats.total_time,
                    "files": stats.files,
                    "warnings": stats.warnings,
                    "status": stats.status,
                    "runs": stats.runs,
                }
                for stats in self.plugins.values()
            ],
            "logs": [
                {
                    "timestamp": _iso(entry.timestamp),
                    "offset": entry.offset,
                    "message": entry.message,
                    "level": entry.level,
                    "plugin": entry.plugin,
                }
                for entry in self.logs
            ],
        }


class ScanMetrics:
    """Collects plugin scan timing information and exposes session data."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._current: Optional[_Session] = None
        self._session_counter = 0
        self._listeners: Dict[int, Callable[[Dict[str, object]], None]] = {}
        self._listener_index = 0
        self._history_cache: List[Dict[str, object]] = []
        self._load_history_index()

    # ------------------------------------------------------------------
    # Public API for plugins and the core app
    # ------------------------------------------------------------------
    def begin_session(self, *, plugins: Optional[Iterable[str]] = None) -> int:
        normalized_plugins: List[str] = []
        if plugins is not None:
            source: Iterable[str]
            if isinstance(plugins, str):
                source = [plugins]
            else:
                source = plugins
            seen: set[str] = set()
            for name in source:
                try:
                    text = str(name)
                except Exception:
                    continue
                text = text.strip()
                if not text or text in seen:
                    continue
                seen.add(text)
                normalized_plugins.append(text)
        with self._lock:
            self._finalize_active(failed=True)
            self._session_counter += 1
            now = time.time()
            session = _Session(
                session_id=self._session_counter,
                started_wall=now,
                started_perf=time.perf_counter(),
            )
            for plugin_name in normalized_plugins:
                session.plugins[plugin_name] = _PluginStats(name=plugin_name)
            if normalized_plugins:
                session.plugin_count = len(session.plugins)
            self._current = session
            self._notify({"event": "session_started", "session": session.snapshot()})
            return session.session_id

    def start(self, plugin_name: str, *, status: Optional[str] = None) -> None:
        if not plugin_name:
            return
        with self._lock:
            session = self._current
            if session is None:
                return
            stats = session.plugins.setdefault(plugin_name, _PluginStats(name=plugin_name))
            if plugin_name in session.active:
                return
            stats.runs += 1
            if status:
                stats.status = status
            session.active[plugin_name] = time.perf_counter()
            self._notify({"event": "session_update", "session": session.snapshot()})

    def stop(
        self,
        plugin_name: str,
        *,
        files_processed: int = 0,
        warnings: int = 0,
        status: Optional[str] = None,
    ) -> None:
        if not plugin_name:
            return
        with self._lock:
            session = self._current
            if session is None:
                return
            start = session.active.pop(plugin_name, None)
            stats = session.plugins.setdefault(plugin_name, _PluginStats(name=plugin_name))
            if start is not None:
                stats.total_time += max(0.0, time.perf_counter() - start)
            stats.files += max(0, files_processed)
            stats.warnings += max(0, warnings)
            if status:
                stats.status = status
            elif warnings:
                stats.status = "Warning"
            elif stats.total_time:
                stats.status = "Done"
            self._notify({"event": "session_update", "session": session.snapshot()})

    def log(self, message: str, *, level: str = "info", plugin: Optional[str] = None) -> None:
        if not message:
            return
        level_norm = level.lower()
        if level_norm not in {"info", "warn", "warning", "error"}:
            level_norm = "info"
        if level_norm == "warning":
            level_norm = "warn"
        with self._lock:
            session = self._current
            if session is None:
                return
            entry = _LogEntry(
                timestamp=time.time(),
                message=message,
                level=level_norm,
                plugin=plugin,
                offset=time.perf_counter() - session.started_perf,
            )
            session.logs.append(entry)
            self._notify({"event": "session_update", "session": session.snapshot()})

    def complete(
        self,
        *,
        total_files: int,
        total_time: float,
        plugin_count: Optional[int] = None,
        failed: bool = False,
    ) -> None:
        with self._lock:
            session = self._current
            if session is None:
                return
            session.completed_wall = time.time()
            session.total_files = max(0, int(total_files))
            session.total_time = max(0.0, float(total_time))
            session.plugin_count = max(0, int(plugin_count or len(session.plugins)))
            session.failed = bool(failed)
            session.active.clear()
            snapshot = session.snapshot()
            self._archive_session(session)
            self._notify({"event": "session_complete", "session": snapshot})
            self._notify({"event": "history", "history": self.get_history()})
            self._current = None

    def cancel(self) -> None:
        with self._lock:
            self._finalize_active(failed=True)
            self._current = None

    def active_plugin_count(self) -> int:
        with self._lock:
            session = self._current
            if session is None:
                return 0
            return len([name for name, stats in session.plugins.items() if stats.total_time or stats.runs])

    def current_snapshot(self) -> Optional[Dict[str, object]]:
        with self._lock:
            if self._current is None:
                return None
            return self._current.snapshot()

    def get_history(self) -> List[Dict[str, object]]:
        with self._lock:
            if not self._history_cache:
                self._history_cache = self._load_history()
            return copy.deepcopy(self._history_cache)

    def register_listener(self, callback: Callable[[Dict[str, object]], None]) -> int:
        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._lock:
            self._listener_index += 1
            token = self._listener_index
            self._listeners[token] = callback
            history = self.get_history()
            if history:
                try:
                    callback({"event": "history", "history": copy.deepcopy(history)})
                except Exception:
                    pass
            snapshot = self.current_snapshot()
            if snapshot:
                try:
                    callback({"event": "session_update", "session": snapshot})
                except Exception:
                    pass
            return token

    def unregister_listener(self, token: int) -> None:
        with self._lock:
            self._listeners.pop(token, None)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _notify(self, payload: Dict[str, object]) -> None:
        listeners = list(self._listeners.items())
        for _token, callback in listeners:
            try:
                callback(copy.deepcopy(payload))
            except Exception:
                continue

    def _archive_session(self, session: _Session) -> None:
        _ensure_history_dir()
        payload = session.to_json()
        filename = HISTORY_DIR / f"scan-{session.session_id:04d}.json"
        try:
            filename.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            return
        self._history_cache = self._load_history()
        self._prune_history_files()

    def _load_history_index(self) -> None:
        if not HISTORY_DIR.exists():
            return
        for path in HISTORY_DIR.glob("scan-*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            session_id = int(data.get("session_id", 0))
            if session_id > self._session_counter:
                self._session_counter = session_id

    def _load_history(self) -> List[Dict[str, object]]:
        entries: List[Dict[str, object]] = []
        if not HISTORY_DIR.exists():
            return entries
        files = sorted(HISTORY_DIR.glob("scan-*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        for path in files[:_HISTORY_LIMIT]:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            entries.append(self._deserialize_history_entry(data))
        return entries

    def _deserialize_history_entry(self, data: Dict[str, object]) -> Dict[str, object]:
        plugins_raw = data.get("plugins", [])
        plugins: List[Dict[str, object]] = []
        for entry in plugins_raw or []:
            if not isinstance(entry, dict):
                continue
            plugins.append(
                {
                    "name": str(entry.get("name", "Plugin")),
                    "time": round(float(entry.get("total_time", 0.0)), 3),
                    "files": int(entry.get("files", 0) or 0),
                    "warnings": int(entry.get("warnings", 0) or 0),
                    "status": str(entry.get("status", "Done")),
                    "runs": int(entry.get("runs", 0) or 0),
                }
            )
        logs_raw = data.get("logs", [])
        logs: List[Dict[str, object]] = []
        for entry in logs_raw or []:
            if not isinstance(entry, dict):
                continue
            offset_val = entry.get("offset", 0.0)
            try:
                offset = float(offset_val)
            except Exception:
                offset = 0.0
            logs.append(
                {
                    "offset": _format_offset(offset),
                    "message": str(entry.get("message", "")),
                    "level": str(entry.get("level", "info")),
                    "plugin": entry.get("plugin"),
                    "timestamp": entry.get("timestamp") or _iso(time.time()),
                }
            )
        return {
            "session_id": int(data.get("session_id", 0) or 0),
            "label": f"Session {int(data.get('session_id', 0) or 0)}",
            "started": data.get("started"),
            "completed": data.get("completed"),
            "plugins": plugins,
            "logs": logs,
            "total_time": round(float(data.get("total_time", 0.0)), 3),
            "total_files": int(data.get("total_files", 0) or 0),
            "plugin_count": int(data.get("plugin_count", len(plugins)) or len(plugins)),
            "running": False,
            "failed": bool(data.get("failed", False)),
        }

    def _finalize_active(self, *, failed: bool) -> None:
        if self._current is None:
            return
        self._current.failed = self._current.failed or failed
        self._current.active.clear()

    def _prune_history_files(self) -> None:
        files = sorted(HISTORY_DIR.glob("scan-*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        for path in files[_HISTORY_LIMIT:]:
            try:
                path.unlink()
            except Exception:
                continue


scan_metrics = ScanMetrics()

__all__ = ["scan_metrics"]
