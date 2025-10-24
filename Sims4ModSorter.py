"""Sims4 Mod Sorter

Refined single-file application with a cleaned scan pipeline, stable plugin hooks,
thread-safe Tk interactions, and offline heuristics. Python 3.10+ only.
"""
from __future__ import annotations

import importlib.util
import inspect
import json
import multiprocessing
import os
import queue
import re
import shutil
import tempfile
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urljoin
import webbrowser
import zipfile
from datetime import datetime
import uuid
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import MethodType
from typing import Callable, Dict, Iterable, Iterator, List, Literal, Optional, Sequence, Set, Tuple, Union

import tkinter as tk
import tkinter.font as tkfont
from tkinter import filedialog, messagebox, simpledialog, ttk

from launch_utils import UpdateResult, check_for_update
from plugin_api import scan_metrics
from scanner import (
    ARCHIVE_EXTS,
    CATEGORY_INDEX,
    CATEGORY_ORDER,
    DEFAULT_FOLDER_MAP,
    DUPLICATE_EXTRA_KEY,
    FileItem,
    ScanResult,
    FINGERPRINT_EXTRA_KEY,
    PACKAGE_EXTS,
    SCRIPT_EXTS,
    SUPPORTED_EXTS,
    TEXT_FILE_EXTS,
    bundle_scripts_and_packages,
    get_default_mods_path,
    guess_type_for_name,
    human_mb,
    normalize_extension,
    normalize_key,
    pretty_display_name,
    scan_folder,
)


def center_window(window: tk.Tk) -> None:
    """Center a Tk window on the active screen."""

    try:
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        if width <= 1 or height <= 1:
            geometry = window.geometry()
            size = geometry.split("+", 1)[0]
            if "x" in size:
                width_str, height_str = size.split("x", 1)
                width = int(width_str)
                height = int(height_str)
            else:
                width, height = 800, 600
        screen_w = window.winfo_screenwidth()
        screen_h = window.winfo_screenheight()
        x = max(int((screen_w - width) / 2), 0)
        y = max(int((screen_h - height) / 2), 0)
        window.geometry(f"{width}x{height}+{x}+{y}")
    except Exception:
        pass


THEMES: Dict[str, Dict[str, str]] = {
    "Dark Mode": {"bg": "#111316", "fg": "#E6E6E6", "alt": "#161A1E", "accent": "#4C8BF5", "sel": "#2A2F3A"},
    "Slightly Dark Mode": {"bg": "#14161a", "fg": "#EAEAEA", "alt": "#1b1e24", "accent": "#6AA2FF", "sel": "#2f3642"},
    "Light Mode": {"bg": "#FAFAFA", "fg": "#1f2328", "alt": "#FFFFFF", "accent": "#316DCA", "sel": "#E8F0FE"},
    "High Contrast Mode": {"bg": "#000000", "fg": "#FFFFFF", "alt": "#000000", "accent": "#FFD400", "sel": "#333333"},
    "Pink Holiday": {"bg": "#1a1216", "fg": "#FFE7F3", "alt": "#23171e", "accent": "#FF5BA6", "sel": "#3a1f2c"},
}


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------


def _scrim_color(bg_hex: str, *, strength: float = 0.45) -> str:
    """Blend the provided colour with black to simulate a translucent scrim."""

    value = bg_hex.strip().lstrip("#")
    if len(value) != 6:
        return "#000000"
    try:
        r = int(value[0:2], 16)
        g = int(value[2:4], 16)
        b = int(value[4:6], 16)
    except ValueError:
        return "#000000"
    ratio = max(0.0, min(1.0, strength))
    blend = lambda component: int(component * (1.0 - ratio))
    return f"#{blend(r):02x}{blend(g):02x}{blend(b):02x}"


# ---------------------------------------------------------------------------
# Plugin system
# ---------------------------------------------------------------------------

USER_PLUGINS_DIR: Path = Path(__file__).resolve().with_name("user_plugins")
VERSION_FILE: Path = Path(__file__).resolve().with_name("VERSION")


def _read_sorter_version() -> str:
    try:
        value = VERSION_FILE.read_text(encoding="utf-8").strip()
        return value or "0.0.0"
    except Exception:
        return "0.0.0"


APP_VERSION = _read_sorter_version()


def _parse_version(value: str) -> Tuple[int, ...]:
    parts: List[int] = []
    for token in re.split(r"[^0-9]+", value):
        if not token:
            continue
        try:
            parts.append(int(token))
        except ValueError:
            parts.append(0)
    return tuple(parts or [0])


def _compare_versions(current: str, required: str) -> int:
    left = _parse_version(current)
    right = _parse_version(required)
    for index in range(max(len(left), len(right))):
        a = left[index] if index < len(left) else 0
        b = right[index] if index < len(right) else 0
        if a != b:
            return 1 if a > b else -1
    return 0


class PluginMessageBus:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._messages: Dict[str, List[Tuple[str, str]]] = {"boot": [], "runtime": []}

    def post(self, channel: str, level: str, message: str) -> None:
        if channel not in self._messages:
            return
        with self._lock:
            self._messages[channel].append((level, message))

    def drain(self, channel: str) -> List[Tuple[str, str]]:
        if channel not in self._messages:
            return []
        with self._lock:
            messages = list(self._messages[channel])
            self._messages[channel].clear()
        return messages


@dataclass(slots=True)
class PluginColumn:
    column_id: str
    heading: str
    width: int
    anchor: str


@dataclass(slots=True)
class PluginStatus:
    name: str
    folder: str
    version: str
    status: str
    message: str = ""


@dataclass(slots=True)
class PluginToolbarButton:
    button_id: str
    text: str
    command: Callable[["Sims4ModSorterApp", "PluginAPI"], None]
    side: str = "left"
    insert_before: Optional[str] = None
    padx: int = 4
    tooltip: Optional[str] = None


def _extract_plugin_version(manifest: Dict[str, object], module_path: Path) -> str:
    version = manifest.get("version")
    if isinstance(version, str) and version.strip():
        return version.strip()
    version_file = module_path.parent / "VERSION"
    if version_file.exists():
        try:
            value = version_file.read_text(encoding="utf-8").strip()
            if value:
                return value
        except Exception:
            pass
    return "Unknown"


def _manifest_compatibility(manifest: Dict[str, object]) -> Optional[str]:
    minimum = manifest.get("min_sorter_version") or manifest.get("min_app_version")
    maximum = manifest.get("max_sorter_version") or manifest.get("max_app_version")
    if isinstance(minimum, str) and minimum.strip():
        required = minimum.strip()
        if _compare_versions(APP_VERSION, required) < 0:
            return f"Requires sorter {required}+"
    if isinstance(maximum, str) and maximum.strip():
        limit = maximum.strip()
        if _compare_versions(APP_VERSION, limit) > 0:
            return f"Incompatible beyond sorter {limit}"
    return None


def _partition_plugin_columns(
    columns: Sequence[PluginColumn], tree_columns: Iterable[str]
) -> Tuple[List[PluginColumn], List[PluginColumn]]:
    """Split plugin columns into accepted and rejected sets."""

    accepted: List[PluginColumn] = []
    rejected: List[PluginColumn] = []
    tree_column_set = set(tree_columns)
    for column in columns:
        if column.column_id in tree_column_set:
            accepted.append(column)
        else:
            rejected.append(column)
    return accepted, rejected


class PluginAPI:
    """API exposed to user plugins."""

    def __init__(self, manager: "PluginManager") -> None:
        self._manager = manager

    def reserved_extra_keys(self) -> Tuple[str, ...]:
        return tuple(sorted(self._manager.reserved_extras))

    def is_reserved_extra(self, key: object) -> bool:
        if key is None:
            return False
        try:
            text = str(key)
        except Exception:
            return False
        return text in self._manager.reserved_extras

    def register_pre_scan_hook(self, func: Callable[[Dict[str, object], "PluginAPI"], None]) -> None:
        if callable(func):
            self._manager.pre_scan_hooks.append(func)

    def register_post_scan_hook(self, func: Callable[[List[FileItem], Dict[str, object], "PluginAPI"], None]) -> None:
        if callable(func):
            self._manager.post_scan_hooks.append(func)

    def register_theme(self, name: str, palette: Dict[str, str]) -> None:
        required = {"bg", "fg", "alt", "accent", "sel"}
        if not name or not isinstance(palette, dict) or not required.issubset(palette):
            self._manager.message_bus.post("boot", "warn", f"Theme registration skipped for '{name or '?'}'")
            return
        THEMES[name] = {key: palette[key] for key in required}
        self._manager.message_bus.post("boot", "info", f"Theme registered: {name}")

    def log(self, message: str, level: str = "info") -> None:
        self._manager.message_bus.post("runtime", level, message)

    @property
    def app(self) -> Optional["Sims4ModSorterApp"]:
        return getattr(self._manager, "app", None)

    def register_column(self, column_id: str, heading: str, *, width: int = 80, anchor: str = "center") -> None:
        if not column_id or not heading:
            return
        self._manager.register_column(column_id, heading, width, anchor)

    def register_settings_section(
        self, title: str, builder: Callable[["Sims4ModSorterApp", ttk.Frame, "PluginAPI"], None]
    ) -> None:
        if callable(builder) and title:
            self._manager.settings_sections.append((title, builder))

    def request_refresh(self) -> None:
        app = self.app
        if app is not None:
            app.schedule_refresh()

    def register_toolbar_button(
        self,
        button_id: str,
        *,
        text: str,
        command: Callable[["Sims4ModSorterApp", "PluginAPI"], None],
        side: str = "left",
        insert_before: Optional[str] = None,
        padx: int = 4,
        tooltip: Optional[str] = None,
    ) -> None:
        if not button_id or not callable(command):
            self._manager.message_bus.post(
                "boot",
                "warn",
                f"Toolbar button registration skipped for '{button_id or '?'}'",
            )
            return
        self._manager.register_toolbar_button(
            button_id,
            text,
            command,
            side=side,
            insert_before=insert_before,
            padx=padx,
            tooltip=tooltip,
        )


class PluginManager:
    def __init__(self, plugins_dir: Path, message_bus: Optional[PluginMessageBus] = None) -> None:
        self.plugins_dir = plugins_dir
        self.pre_scan_hooks: List[Callable[[Dict[str, object], PluginAPI], None]] = []
        self.post_scan_hooks: List[Callable[[List[FileItem], Dict[str, object], PluginAPI], None]] = []
        self.message_bus = message_bus or PluginMessageBus()
        self.api = PluginAPI(self)
        self.columns: Dict[str, PluginColumn] = {}
        self.column_order: List[str] = []
        self.settings_sections: List[Tuple[str, Callable[["Sims4ModSorterApp", ttk.Frame, PluginAPI], None]]] = []
        self.app: Optional["Sims4ModSorterApp"] = None
        self.statuses: List[PluginStatus] = []
        self.toolbar_buttons: Dict[str, PluginToolbarButton] = {}
        self.toolbar_order: List[str] = []
        self.reserved_extras: Set[str] = {FINGERPRINT_EXTRA_KEY, DUPLICATE_EXTRA_KEY}
        self.reserved_columns: Set[str] = {DUPLICATE_EXTRA_KEY}

    def attach_app(self, app: "Sims4ModSorterApp") -> None:
        self.app = app

    def load(self) -> None:
        self.plugins_dir.mkdir(parents=True, exist_ok=True)
        self.statuses.clear()
        for entry in sorted(self.plugins_dir.iterdir(), key=lambda p: p.name.lower()):
            manifest: Dict[str, object]
            module_path: Path
            if entry.is_dir():
                manifest_path = entry / "plugin.json"
                if manifest_path.exists():
                    try:
                        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    except Exception as exc:
                        self.message_bus.post("boot", "error", f"Failed to parse {manifest_path.name}: {exc}")
                        continue
                else:
                    manifest = {"name": entry.name, "entry": "plugin.py", "enabled": True, "callable": "register"}
                module_path = entry / str(manifest.get("entry", "plugin.py"))
            else:
                manifest = {"name": entry.stem, "entry": entry.name, "enabled": True, "callable": "register"}
                module_path = entry
            self._load_manifest(manifest, module_path)

    def _load_manifest(self, manifest: Dict[str, object], module_path: Path) -> None:
        name = str(manifest.get("name") or module_path.stem)
        folder = module_path.parent.name if module_path.parent != module_path else module_path.stem
        version = _extract_plugin_version(manifest, module_path)
        compatibility = _manifest_compatibility(manifest)
        if compatibility:
            self.message_bus.post("boot", "warn", f"Plugin '{name}' skipped: {compatibility}")
            self.statuses.append(PluginStatus(name, folder, version, "incompatible", compatibility))
            return
        if not manifest.get("enabled", True):
            self.message_bus.post("boot", "info", f"Skipping disabled plugin: {name}")
            self.statuses.append(PluginStatus(name, folder, version, "disabled", "Disabled"))
            return
        if not module_path.exists():
            message = f"Missing entry: {module_path.name}"
            self.message_bus.post("boot", "error", f"Missing entry for {name}: {module_path.name}")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            return
        module_name = re.sub(r"[^0-9A-Za-z_]+", "_", manifest.get("import_name", name))
        try:
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            if spec is None or spec.loader is None:
                raise ImportError("spec creation failed")
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
        except Exception as exc:
            message = str(exc)
            self.message_bus.post("boot", "error", f"Failed to import {name}: {exc}")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            return
        callable_name = str(manifest.get("callable", "register"))
        register = getattr(module, callable_name, None)
        if not callable(register):
            message = f"Missing callable '{callable_name}'"
            self.message_bus.post("boot", "warn", f"Plugin '{name}' missing callable '{callable_name}'")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            return
        try:
            register(self.api)
        except Exception as exc:
            message = str(exc)
            self.message_bus.post("boot", "error", f"Plugin '{name}' failed during register: {exc}")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            return
        self.message_bus.post("boot", "info", f"Loaded plugin: {name}")
        self.statuses.append(PluginStatus(name, folder, version, "loaded", ""))

    def run_pre_scan(self, context: Dict[str, object]) -> None:
        for hook in list(self.pre_scan_hooks):
            try:
                hook(context, self.api)
            except Exception as exc:
                self.message_bus.post("runtime", "error", f"Pre-scan hook error: {exc}")

    def run_post_scan(self, items: List[FileItem], context: Dict[str, object]) -> None:
        for hook in list(self.post_scan_hooks):
            try:
                hook(items, context, self.api)
            except Exception as exc:
                self.message_bus.post("runtime", "error", f"Post-scan hook error: {exc}")

    def register_column(self, column_id: str, heading: str, width: int, anchor: str) -> None:
        normalized = column_id.strip()
        if not normalized:
            return
        if normalized in self.reserved_columns:
            self.message_bus.post(
                "boot",
                "warn",
                f"Plugin column '{normalized}' conflicts with a core column and was ignored.",
            )
            return
        if normalized in self.columns:
            return
        self.columns[normalized] = PluginColumn(normalized, heading, width, anchor)
        self.column_order.append(normalized)

    def get_columns(self) -> List[PluginColumn]:
        return [self.columns[column_id] for column_id in self.column_order if column_id in self.columns]

    def get_statuses(self) -> List[PluginStatus]:
        return list(self.statuses)

    def register_toolbar_button(
        self,
        button_id: str,
        text: str,
        command: Callable[["Sims4ModSorterApp", PluginAPI], None],
        *,
        side: str = "left",
        insert_before: Optional[str] = None,
        padx: int = 4,
        tooltip: Optional[str] = None,
    ) -> None:
        normalized = button_id.strip() if button_id else ""
        if not normalized:
            self.message_bus.post("boot", "warn", "Toolbar button registration skipped: missing identifier")
            return
        if normalized in self.toolbar_buttons:
            self.message_bus.post(
                "boot",
                "warn",
                f"Toolbar button '{normalized}' already registered; ignoring duplicate",
            )
            return
        if not callable(command):
            self.message_bus.post(
                "boot",
                "warn",
                f"Toolbar button '{normalized}' skipped: command is not callable",
            )
            return
        text_value = str(text) if text else normalized
        side_value = str(side).lower()
        if side_value not in {"left", "right", "sidebar"}:
            side_value = "left"
        try:
            padx_value = int(padx)
        except Exception:
            padx_value = 4
        if padx_value < 0:
            padx_value = 0
        insert_value = str(insert_before).strip() if insert_before else None
        tooltip_value = str(tooltip).strip() if tooltip else None
        entry = PluginToolbarButton(
            button_id=normalized,
            text=text_value,
            command=command,
            side=side_value,
            insert_before=insert_value or None,
            padx=padx_value,
            tooltip=tooltip_value,
        )
        self.toolbar_buttons[normalized] = entry
        self.toolbar_order.append(normalized)

    def get_toolbar_buttons(self) -> List[PluginToolbarButton]:
        return [
            self.toolbar_buttons[button_id]
            for button_id in self.toolbar_order
            if button_id in self.toolbar_buttons
        ]


def load_user_plugins() -> PluginManager:
    manager = PluginManager(USER_PLUGINS_DIR)
    try:
        manager.load()
    except Exception as exc:
        manager.message_bus.post("boot", "error", f"Plugin loading aborted: {exc}")
    return manager


def flush_plugin_messages(app, channel: str) -> None:
    manager = getattr(app, "plugin_manager", None)
    if not isinstance(manager, PluginManager):
        return
    prefix = {"error": "Plugin error", "warn": "Plugin warning", "info": "Plugin"}
    for level, message in manager.message_bus.drain(channel):
        if not message:
            continue
        tag = prefix.get(level, "Plugin")
        mapped_level = {
            "error": "plugin_error",
            "warn": "plugin_warning",
            "warning": "plugin_warning",
            "info": "plugin",
        }.get(level, "plugin")
        app.log(f"{tag}: {message}", level=mapped_level)


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
_NAT_SORT_RE = re.compile(r"(\d+)")


def _natural_key(value: str) -> Tuple[object, ...]:
    parts = _NAT_SORT_RE.split(value.lower())
    out: List[object] = []
    for part in parts:
        out.append(int(part) if part.isdigit() else part)
    return tuple(out)
# ---------------------------------------------------------------------------
# Bundling and move helpers
# ---------------------------------------------------------------------------

LOG_NAME = ".sims4_modsorter_moves.json"
LOADOUTS_FILENAME = ".sims4_modsorter_loadouts.json"
DEFAULT_LOADOUT_NAME = "Default Loadout"
LOADOUTS_VERSION = 1


def ensure_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def perform_moves(items: Sequence[FileItem], mods_root: Path) -> Tuple[int, int, List[Tuple[Path, Path, str]], List[Dict[str, str]]]:
    moved = 0
    skipped = 0
    collisions: List[Tuple[Path, Path, str]] = []
    moves_log: List[Dict[str, str]] = []
    for item in items:
        if not item.include:
            skipped += 1
            continue
        destination_dir = mods_root / item.target_folder
        ensure_folder(destination_dir)
        destination = destination_dir / item.name
        try:
            if destination.resolve() == item.path.resolve():
                skipped += 1
                continue
        except Exception:
            pass
        if destination.exists():
            collisions.append((item.path, destination, "name collision"))
            skipped += 1
            continue
        try:
            shutil.move(str(item.path), str(destination))
            moved += 1
            moves_log.append({"from": str(item.path), "to": str(destination)})
        except Exception as exc:
            collisions.append((item.path, destination, f"move error: {exc}"))
            skipped += 1
    return moved, skipped, collisions, moves_log


def save_moves_log(mods_root: Path, moves: List[Dict[str, str]]) -> None:
    if not moves:
        return
    log_path = mods_root / LOG_NAME
    try:
        if log_path.exists():
            history = json.loads(log_path.read_text(encoding="utf-8"))
            if not isinstance(history, list):
                history = []
        else:
            history = []
    except Exception:
        history = []
    timestamp = time.time()
    moved_count = len(moves)
    sample_moves: List[Dict[str, str]] = []
    for move in moves[:5]:
        if not isinstance(move, dict):
            continue
        src = move.get("from", "")
        dst = move.get("to", "")
        sample_moves.append({"from": src, "to": dst})
    source_folders: Set[str] = set()
    target_folders: Set[str] = set()
    for move in moves:
        if not isinstance(move, dict):
            continue
        src_value = move.get("from")
        dst_value = move.get("to")
        if src_value:
            try:
                source_folders.add(str(Path(src_value).parent))
            except Exception:
                source_folders.add(str(src_value))
        if dst_value:
            try:
                target_folders.add(str(Path(dst_value).parent))
            except Exception:
                target_folders.add(str(dst_value))
    entry_id = str(uuid.uuid4())
    display_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    history.append(
        {
            "id": entry_id,
            "ts": timestamp,
            "timestamp": timestamp,
            "label": display_time,
            "summary": f"Moved {moved_count} file(s)",
            "counts": {
                "moved": moved_count,
                "sources": len(source_folders),
                "targets": len(target_folders),
            },
            "sample_moves": sample_moves,
            "moves": moves,
        }
    )
    try:
        log_path.write_text(json.dumps(history, indent=2), encoding="utf-8")
    except Exception:
        pass


def load_move_history(mods_root: Path) -> List[Dict[str, object]]:
    log_path = mods_root / LOG_NAME
    if not log_path.exists():
        return []
    try:
        history = json.loads(log_path.read_text(encoding="utf-8"))
        if not isinstance(history, list):
            return []
    except Exception:
        return []
    entries: List[Dict[str, object]] = []
    for index, raw in enumerate(history):
        if not isinstance(raw, dict):
            continue
        moves = raw.get("moves", [])
        if not isinstance(moves, list):
            moves = []
        timestamp_value = raw.get("timestamp") or raw.get("ts") or 0.0
        try:
            timestamp = float(timestamp_value)
        except (TypeError, ValueError):
            timestamp = 0.0
        try:
            display_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S") if timestamp else "Unknown"
        except Exception:
            display_time = "Unknown"
        counts = raw.get("counts") if isinstance(raw.get("counts"), dict) else {}
        moved_count = counts.get("moved") if isinstance(counts.get("moved"), int) else len(moves)
        sample_moves = raw.get("sample_moves") if isinstance(raw.get("sample_moves"), list) else []
        if not sample_moves:
            for move in moves[:5]:
                if not isinstance(move, dict):
                    continue
                sample_moves.append({"from": move.get("from", ""), "to": move.get("to", "")})
        entry_id = raw.get("id") or raw.get("uid") or f"entry-{index}-{int(timestamp)}"
        if not isinstance(entry_id, str):
            entry_id = str(entry_id)
        sample_display: List[str] = []
        for move in sample_moves:
            if not isinstance(move, dict):
                continue
            dest = move.get("to") or ""
            src = move.get("from") or ""
            display = dest or src
            if display:
                try:
                    display = Path(display).name or display
                except Exception:
                    pass
                sample_display.append(display)
        entry = {
            "id": entry_id,
            "history_index": index,
            "timestamp": timestamp,
            "display_time": raw.get("label") if isinstance(raw.get("label"), str) else display_time,
            "summary": raw.get("summary") if isinstance(raw.get("summary"), str) else f"Moved {moved_count} file(s)",
            "moved_count": moved_count,
            "counts": counts if counts else {"moved": moved_count},
            "sample_moves": sample_moves,
            "sample_display": sample_display,
            "moves": moves,
        }
        entries.append(entry)
    entries.sort(key=lambda entry: entry.get("timestamp", 0.0), reverse=True)
    return entries


def undo_moves(mods_root: Path, entry_identifier: Optional[Union[str, int]] = None) -> Tuple[int, int, List[str], Optional[str]]:
    log_path = mods_root / LOG_NAME
    if not log_path.exists():
        return 0, 0, ["No log found"], None
    try:
        history = json.loads(log_path.read_text(encoding="utf-8"))
        if not isinstance(history, list) or not history:
            return 0, 0, ["No moves recorded"], None
    except Exception:
        return 0, 0, ["Log unreadable"], None
    target_index: Optional[int] = None
    if isinstance(entry_identifier, int):
        if -len(history) <= entry_identifier < len(history):
            target_index = entry_identifier % len(history)
    elif isinstance(entry_identifier, str):
        for idx, item in enumerate(history):
            if isinstance(item, dict) and str(item.get("id")) == entry_identifier:
                target_index = idx
                break
    if target_index is None:
        target_index = len(history) - 1
    if not (0 <= target_index < len(history)):
        return 0, 0, ["Requested history entry not found"], None
    selected = history.pop(target_index)
    moves = selected.get("moves", []) if isinstance(selected, dict) else []
    label = None
    if isinstance(selected, dict):
        label = selected.get("label")
        if not isinstance(label, str):
            summary = selected.get("summary")
            if isinstance(summary, str):
                label = summary
    undone = 0
    failed = 0
    errors: List[str] = []
    for move in reversed(moves):
        if not isinstance(move, dict):
            continue
        src_path = move.get("to", "")
        dst_path = move.get("from", "")
        src = Path(src_path) if src_path else None
        dst = Path(dst_path) if dst_path else None
        if not src or not dst:
            continue
        try:
            if not src.exists():
                errors.append(f"Missing {src.name} to undo")
                failed += 1
                continue
            ensure_folder(dst.parent)
            if dst.exists():
                errors.append(f"Collision on undo for {dst.name}")
                failed += 1
                continue
            shutil.move(str(src), str(dst))
            undone += 1
        except Exception as exc:
            errors.append(f"Undo error for {src.name}: {exc}")
            failed += 1
    if moves and undone == 0:
        history.insert(target_index, selected)
    try:
        log_path.write_text(json.dumps(history, indent=2), encoding="utf-8")
    except Exception:
        pass
    return undone, failed, errors, label


def undo_last_moves(mods_root: Path) -> Tuple[int, int, List[str], Optional[str]]:
    return undo_moves(mods_root)


# ---------------------------------------------------------------------------
# Tk application
# ---------------------------------------------------------------------------


class Sims4ModSorterApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Sims4 Mod Sorter")
        self.geometry("1280x860")
        self.minsize(1100, 740)
        self.resizable(True, True)

        self.folder_map: Dict[str, str] = DEFAULT_FOLDER_MAP.copy()
        self.recurse_var = tk.BooleanVar(value=True)
        self.ignore_exts_var = tk.StringVar(value=".log,.cfg,.txt,.html")
        self.ignore_names_var = tk.StringVar(value="thumbcache,desktop.ini,resource.cfg")
        self.include_adult_var = tk.BooleanVar(value=True)
        self.scan_package_var = tk.BooleanVar(value=True)
        self.scan_script_var = tk.BooleanVar(value=True)
        self.scan_archive_var = tk.BooleanVar(value=True)
        self.scan_misc_var = tk.BooleanVar(value=True)
        self.theme_name = tk.StringVar(value="Dark Mode")
        self.mods_root = tk.StringVar(value=get_default_mods_path())
        self._version_display_var = tk.StringVar(value=f"App Version: {APP_VERSION}")

        self.items: List[FileItem] = []
        self.items_by_path: Dict[str, FileItem] = {}
        self.scan_errors: List[str] = []
        self.disabled_items: List[FileItem] = []
        self.plugin_manager = load_user_plugins()
        self._plugin_columns: List[PluginColumn] = []
        self._plugin_toolbar_buttons: List[PluginToolbarButton] = []
        if self.plugin_manager:
            self.plugin_manager.attach_app(self)
            self._plugin_columns = self.plugin_manager.get_columns()
            self._plugin_toolbar_buttons = self.plugin_manager.get_toolbar_buttons()

        self.status_var = tk.StringVar(value="Ready")
        self.summary_var = tk.StringVar(value="No plan yet")
        self._duplicate_filter_var = tk.BooleanVar(value=False)
        self._duplicate_filter_check: Optional[ttk.Checkbutton] = None

        self._ui_queue: "queue.Queue[Callable[[], None]]" = queue.Queue()
        self._theme_cache: Dict[str, str] = {}
        self._column_order: List[str] = []
        self._tooltip_payload: Dict[str, Dict[str, str]] = {}
        self._tooltip_window: Optional[tk.Toplevel] = None
        self._tooltip_label: Optional[tk.Label] = None
        self._tooltip_after: Optional[str] = None
        self._tooltip_target: Tuple[str, str] = ("", "")
        self._toolbar_widgets: Dict[str, tk.Widget] = {}
        self._mod_status_window: Optional[tk.Toplevel] = None
        self._status_trees: Dict[str, ttk.Treeview] = {}
        self._status_summary_var = tk.StringVar(value="")
        self._update_check_in_progress = False
        self.check_updates_button: Optional[ttk.Button] = None
        self._latest_version: Optional[str] = None
        self._update_download_url: Optional[str] = None
        self._update_release_page_url: Optional[str] = None
        self._update_download_filename: Optional[str] = None
        self._update_available: bool = False
        self._update_overlay: Optional[tk.Toplevel] = None
        self._update_overlay_message = tk.StringVar(value="")
        self._update_overlay_headline = tk.StringVar(value="Sims4 Mod Sorter Update")
        self._update_overlay_status_icon = tk.StringVar(value="⬆️")
        self._update_overlay_progress_title = tk.StringVar(value="")
        self._update_overlay_progress_detail = tk.StringVar(value="")
        self._update_overlay_changelog = tk.StringVar(value="")
        self._update_overlay_progress: Optional[ttk.Progressbar] = None
        self._update_overlay_download_btn: Optional[ttk.Button] = None
        self._update_overlay_skip_btn: Optional[ttk.Button] = None
        self._update_overlay_button_frame: Optional[ttk.Frame] = None
        self._update_overlay_details_btn: Optional[ttk.Button] = None
        self._update_overlay_manual_btn: Optional[ttk.Button] = None
        self._update_overlay_progress_frame: Optional[ttk.Frame] = None
        self._update_overlay_progress_title_label: Optional[ttk.Label] = None
        self._update_overlay_progress_detail_label: Optional[ttk.Label] = None
        self._update_overlay_changelog_label: Optional[ttk.Label] = None
        self._update_overlay_headline_label: Optional[ttk.Label] = None
        self._update_overlay_body_label: Optional[ttk.Label] = None
        self._update_overlay_headline_font: Optional[tkfont.Font] = None
        self._update_overlay_icon_font: Optional[tkfont.Font] = None
        self._update_overlay_visible: bool = False
        self._update_overlay_origin: str = "general"
        self._update_download_mode = tk.StringVar(value="simple")
        self._update_mode_description = tk.StringVar(
            value="Downloads and installs every updated file automatically."
        )
        self._update_mode_frame: Optional[ttk.LabelFrame] = None
        self._update_mode_simple_radio: Optional[ttk.Radiobutton] = None
        self._update_mode_advanced_radio: Optional[ttk.Radiobutton] = None
        self._update_mode_description_label: Optional[ttk.Label] = None
        self._auto_size_pending = False

        self._history_window: Optional[tk.Toplevel] = None
        self._history_tree: Optional[ttk.Treeview] = None
        self._history_entries: Dict[str, Dict[str, object]] = {}
        self._history_preview_btn: Optional[ttk.Button] = None
        self._history_undo_btn: Optional[ttk.Button] = None

        self.scan_folder_display = tk.StringVar(value="All folders")
        self.scan_folders: Optional[set[str]] = None
        self._available_folders: List[str] = []
        self._folder_menu_vars: Dict[str, tk.BooleanVar] = {}
        self._folder_menu_refresh_after: Optional[str] = None
        self.view_menu_button: Optional[ttk.Menubutton] = None
        self.view_menu: Optional[tk.Menu] = None
        self._view_menu_actions: List[ResolvedViewAction] = []

        self.loadouts: Dict[str, Dict[str, bool]] = {}
        self._active_loadout_name: str = DEFAULT_LOADOUT_NAME
        self.loadout_var = tk.StringVar(value=self._active_loadout_name)
        self._loadout_selector: Optional[ttk.Combobox] = None
        self._loadout_apply_btn: Optional[ttk.Button] = None

        self._load_loadouts_from_disk()

        self._build_style()
        self._build_ui()
        self._build_settings_overlay()
        self.mods_root.trace_add("write", lambda *_: self._on_mods_root_change())
        self.after(16, self._pump_ui_queue)
        self._report_mod_boot_messages()
        self.after(1000, self._check_updates_on_launch)
        self.after(0, lambda: center_window(self))

    # ------------------------------------------------------------------
    # Compatibility shims
    # ------------------------------------------------------------------
    def _report_mod_boot_messages(self) -> None:
        flush_plugin_messages(self, "boot")

    def _report_mod_runtime_messages(self) -> None:
        flush_plugin_messages(self, "runtime")

    # ------------------------------------------------------------------
    # Loadout management
    # ------------------------------------------------------------------
    def _on_mods_root_change(self) -> None:
        try:
            self._schedule_folder_menu_refresh()  # type: ignore[attr-defined]
        except AttributeError:
            pass
        self._load_loadouts_from_disk()

    def _resolve_loadouts_path(self) -> Optional[Path]:
        root_value = self.mods_root.get().strip()
        if not root_value:
            return None
        try:
            root_path = Path(root_value).expanduser()
        except Exception:
            return None
        return root_path / LOADOUTS_FILENAME

    def _load_loadouts_from_disk(self) -> None:
        path = self._resolve_loadouts_path()
        loadouts: Dict[str, Dict[str, bool]] = {}
        active_name: Optional[str] = None
        if path and path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                raw_loadouts = payload.get("loadouts")
                if isinstance(raw_loadouts, dict):
                    for name, mapping in raw_loadouts.items():
                        if not isinstance(name, str) or not isinstance(mapping, dict):
                            continue
                        clean_map: Dict[str, bool] = {}
                        for item_path, include_flag in mapping.items():
                            if isinstance(item_path, str):
                                clean_map[item_path] = bool(include_flag)
                        loadouts[name] = clean_map
                active_token = payload.get("active") or payload.get("active_loadout")
                if isinstance(active_token, str):
                    active_name = active_token
        self.loadouts = loadouts
        self._ensure_loadout_defaults(active_name=active_name)
        self._refresh_loadout_controls()

    def _ensure_loadout_defaults(self, active_name: Optional[str] = None) -> None:
        if not self.loadouts:
            self.loadouts = {DEFAULT_LOADOUT_NAME: {}}
        if active_name and active_name in self.loadouts:
            self._active_loadout_name = active_name
        elif self._active_loadout_name not in self.loadouts:
            self._active_loadout_name = sorted(self.loadouts.keys())[0]
        if not self.loadout_var.get():
            self.loadout_var.set(self._active_loadout_name)

    def _refresh_loadout_controls(self) -> None:
        names = sorted(self.loadouts.keys())
        if names:
            current = self.loadout_var.get()
            if current not in names:
                target = self._active_loadout_name if self._active_loadout_name in names else names[0]
                self.loadout_var.set(target)
        selector = getattr(self, "_loadout_selector", None)
        if selector is not None and selector.winfo_exists():
            selector.configure(values=names)
            selected = self.loadout_var.get()
            if selected:
                try:
                    selector.set(selected)
                except tk.TclError:
                    pass
        apply_btn = getattr(self, "_loadout_apply_btn", None)
        if apply_btn is not None and apply_btn.winfo_exists():
            apply_btn.configure(state="normal" if names else "disabled")

    def _save_loadouts_to_disk(self) -> None:
        path = self._resolve_loadouts_path()
        if not path:
            return
        payload = {
            "version": LOADOUTS_VERSION,
            "active": self._active_loadout_name,
            "loadouts": self.loadouts,
        }
        try:
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _record_loadout(self, name: Optional[str] = None, *, save: bool = True) -> None:
        if not name:
            name = self._active_loadout_name
        if not name:
            return
        self.loadouts[name] = {str(item.path): bool(item.include) for item in self.items}
        if save:
            self._save_loadouts_to_disk()

    def _apply_loadout_to_items(self, name: Optional[str] = None) -> bool:
        if not name:
            name = self._active_loadout_name
        if not name:
            return False
        mapping = self.loadouts.get(name, {})
        changed = False
        for item in self.items:
            include_flag = mapping.get(str(item.path))
            if include_flag is None:
                include_flag = True
            include_value = bool(include_flag)
            if item.include != include_value:
                item.include = include_value
                changed = True
        self._record_loadout(name, save=False)
        self._save_loadouts_to_disk()
        return changed

    def _build_loadout_export(self) -> Optional[Dict[str, object]]:
        if not self.loadouts:
            return None
        payload: Dict[str, Dict[str, bool]] = {}
        for name, mapping in self.loadouts.items():
            if not isinstance(name, str):
                continue
            clean_map: Dict[str, bool] = {}
            if isinstance(mapping, dict):
                for item_path, include_flag in mapping.items():
                    if isinstance(item_path, str):
                        clean_map[item_path] = bool(include_flag)
            payload[name] = clean_map
        if not payload:
            return None
        return {
            "version": LOADOUTS_VERSION,
            "active": self._active_loadout_name,
            "loadouts": payload,
        }

    def _merge_imported_loadouts(
        self, loadouts: Dict[str, object], active_name: Optional[str]
    ) -> int:
        sanitized: Dict[str, Dict[str, bool]] = {}
        for name, mapping in loadouts.items():
            if not isinstance(name, str) or not isinstance(mapping, dict):
                continue
            clean_map: Dict[str, bool] = {}
            for item_path, include_flag in mapping.items():
                if isinstance(item_path, str):
                    clean_map[item_path] = bool(include_flag)
            sanitized[name] = clean_map
        if not sanitized:
            return 0
        self.loadouts.update(sanitized)
        if active_name and active_name in self.loadouts:
            self._active_loadout_name = active_name
        self.loadout_var.set(self._active_loadout_name)
        self._refresh_loadout_controls()
        self._save_loadouts_to_disk()
        return len(sanitized)

    def _on_loadout_selected(self, _event: Optional[tk.Event] = None) -> None:
        value = (self.loadout_var.get() or "").strip()
        if not value:
            if self.loadouts:
                self.loadout_var.set(self._active_loadout_name)
            return
        if value not in self.loadouts:
            if self.loadouts:
                self.loadout_var.set(self._active_loadout_name)
            return
        self.loadout_var.set(value)

    # ------------------------------------------------------------------
    # Loadout actions
    # ------------------------------------------------------------------
    def on_create_loadout(self) -> None:
        self._record_loadout(save=True)
        name = simpledialog.askstring("New Loadout", "Enter a name for the new loadout:", parent=self)
        if name is None:
            return
        name = name.strip()
        if not name:
            return
        if name in self.loadouts:
            messagebox.showerror("New Loadout", f"A loadout named '{name}' already exists.", parent=self)
            return
        self.loadouts[name] = {str(item.path): bool(item.include) for item in self.items}
        self._active_loadout_name = name
        self.loadout_var.set(name)
        self._refresh_loadout_controls()
        self._save_loadouts_to_disk()
        self.log(f"Created loadout '{name}'.")

    def on_rename_loadout(self) -> None:
        if not self.loadouts:
            return
        current = self.loadout_var.get() or self._active_loadout_name
        if not current or current not in self.loadouts:
            return
        new_name = simpledialog.askstring(
            "Rename Loadout",
            "Enter a new name for the loadout:",
            initialvalue=current,
            parent=self,
        )
        if new_name is None:
            return
        new_name = new_name.strip()
        if not new_name or new_name == current:
            return
        if new_name in self.loadouts:
            messagebox.showerror("Rename Loadout", f"A loadout named '{new_name}' already exists.", parent=self)
            return
        self.loadouts[new_name] = self.loadouts.pop(current)
        if self._active_loadout_name == current:
            self._active_loadout_name = new_name
        if self.loadout_var.get() == current:
            self.loadout_var.set(new_name)
        self._refresh_loadout_controls()
        self._save_loadouts_to_disk()
        self.log(f"Renamed loadout '{current}' to '{new_name}'.")

    def on_delete_loadout(self) -> None:
        if not self.loadouts:
            return
        target = self.loadout_var.get() or self._active_loadout_name
        if not target or target not in self.loadouts:
            return
        if not messagebox.askyesno("Delete Loadout", f"Delete loadout '{target}'?", parent=self):
            return
        was_active = target == self._active_loadout_name
        self.loadouts.pop(target, None)
        if not self.loadouts:
            self.loadouts = {DEFAULT_LOADOUT_NAME: {str(item.path): bool(item.include) for item in self.items}}
            self._active_loadout_name = DEFAULT_LOADOUT_NAME
        elif was_active:
            self._active_loadout_name = sorted(self.loadouts.keys())[0]
        if self.loadout_var.get() == target or self.loadout_var.get() not in self.loadouts:
            self.loadout_var.set(self._active_loadout_name)
        self._record_loadout(save=False)
        self._refresh_loadout_controls()
        self._save_loadouts_to_disk()
        self.log(f"Deleted loadout '{target}'.")

    def on_apply_loadout(self) -> None:
        target = (self.loadout_var.get() or "").strip()
        if not target:
            messagebox.showinfo("Apply Loadout", "Select a loadout to apply first.", parent=self)
            return
        if target not in self.loadouts:
            messagebox.showerror("Apply Loadout", f"Loadout '{target}' is no longer available.", parent=self)
            self._refresh_loadout_controls()
            return
        self._record_loadout(save=True)
        self._active_loadout_name = target
        changed = self._apply_loadout_to_items(target)
        if self.items and changed:
            self._refresh_tree(preserve_selection=True)
        self.log(f"Applied loadout '{target}' to the current plan.")
    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------
    def _build_style(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        palette = THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        self._theme_cache = palette.copy()
        style.configure("TFrame", background=palette["bg"])
        style.configure("TLabel", background=palette["bg"], foreground=palette["fg"])
        style.configure("TCheckbutton", background=palette["bg"], foreground=palette["fg"])
        style.configure("TButton", background=palette["alt"], foreground=palette["fg"], padding=6)
        style.map("TButton", background=[("active", palette["sel"])])
        style.configure("Accent.TButton", background=palette["accent"], foreground=palette["fg"], padding=(12, 8))
        style.map(
            "Accent.TButton",
            background=[("active", palette["sel"]), ("pressed", palette["sel"]), ("disabled", palette["alt"])],
            foreground=[("disabled", palette["fg"])],
        )
        style.configure(
            "Treeview",
            background=palette["alt"],
            fieldbackground=palette["alt"],
            foreground=palette["fg"],
            rowheight=28,
            bordercolor=palette["bg"],
            borderwidth=0,
        )
        style.map("Treeview", background=[("selected", palette["sel"])])
        style.configure("Treeview.Heading", background=palette["bg"], foreground=palette["fg"])
        style.configure("Horizontal.TProgressbar", background=palette["accent"], troughcolor=palette["alt"])
        style.configure("UpdateOverlay.TFrame", background=palette["alt"])
        style.configure("UpdateOverlayHero.TFrame", background=palette["alt"])
        style.configure("UpdateOverlayHeadline.TLabel", background=palette["alt"], foreground=palette["fg"])
        style.configure("UpdateOverlayBody.TLabel", background=palette["alt"], foreground=palette["fg"])
        style.configure("UpdateOverlayIcon.TLabel", background=palette["alt"], foreground=palette["accent"])
        style.configure("UpdateOverlayProgress.TFrame", background=palette["alt"])
        style.configure("UpdateOverlayProgressTitle.TLabel", background=palette["alt"], foreground=palette["fg"], font=("", 10, "bold"))
        style.configure("UpdateOverlayProgressDetail.TLabel", background=palette["alt"], foreground=palette["fg"])
        style.configure("UpdateOverlayChangelog.TLabel", background=palette["alt"], foreground=palette["fg"])
        style.configure(
            "UpdateOverlay.Horizontal.TProgressbar",
            background=palette["accent"],
            troughcolor=palette["bg"],
        )
        sidebar_bg = palette["alt"]
        style.configure("Sidebar.TFrame", background=sidebar_bg)
        style.configure(
            "Sidebar.TLabelframe",
            background=sidebar_bg,
            foreground=palette["fg"],
            borderwidth=0,
            relief="flat",
        )
        style.configure(
            "Sidebar.TLabelframe.Label",
            background=sidebar_bg,
            foreground=palette["fg"],
            font=("", 10, "bold"),
        )
        style.configure("Sidebar.TLabel", background=sidebar_bg, foreground=palette["fg"])
        style.configure(
            "SidebarHeading.TLabel",
            background=sidebar_bg,
            foreground=palette["fg"],
            font=("", 9, "bold"),
        )
        style.configure("Sidebar.TButton", background=palette["alt"], foreground=palette["fg"], padding=(10, 6))
        style.map("Sidebar.TButton", background=[("active", palette["sel"]), ("pressed", palette["sel"])])
        style.configure(
            "Sidebar.TEntry",
            fieldbackground=sidebar_bg,
            foreground=palette["fg"],
            background=sidebar_bg,
            bordercolor=palette["bg"],
        )
        style.configure(
            "Sidebar.TCombobox",
            fieldbackground=sidebar_bg,
            background=sidebar_bg,
            foreground=palette["fg"],
            arrowcolor=palette["fg"],
            bordercolor=palette["bg"],
        )
        style.map(
            "Sidebar.TCombobox",
            fieldbackground=[("readonly", sidebar_bg)],
            background=[("readonly", sidebar_bg)],
            foreground=[("readonly", palette["fg"])],
        )
        style.configure(
            "Sidebar.TSeparator",
            background=palette["sel"],
            foreground=palette["sel"],
        )
        self.configure(bg=palette["bg"])

    def _build_ui(self) -> None:
        if self.plugin_manager:
            self._plugin_columns = self.plugin_manager.get_columns()
            self._plugin_toolbar_buttons = self.plugin_manager.get_toolbar_buttons()
        else:
            self._plugin_toolbar_buttons = []
        self._toolbar_widgets.clear()
        try:
            self._tree_font = tkfont.nametofont("TkDefaultFont")
        except tk.TclError:
            self._tree_font = tkfont.Font()
        try:
            self._tree_heading_font = tkfont.nametofont("TkHeadingFont")
        except tk.TclError:
            self._tree_heading_font = self._tree_font
        root_container = ttk.Frame(self)
        root_container.pack(fill="both", expand=True)

        top = ttk.Frame(root_container)
        top.pack(fill="x", padx=12, pady=10)
        ttk.Label(top, text="Mods folder:").pack(side="left")
        self.entry_path = ttk.Entry(top, textvariable=self.mods_root, width=80)
        self.entry_path.pack(side="left", padx=8)
        browse_btn = ttk.Button(top, text="Browse", command=self.on_browse)
        browse_btn.pack(side="left", padx=4)
        self._toolbar_widgets["browse"] = browse_btn
        self.btn_scan = ttk.Button(top, text="Scan", command=self.on_scan)
        self.btn_scan.pack(side="left", padx=4)
        self._toolbar_widgets["scan"] = self.btn_scan
        import_btn = ttk.Button(top, text="Import Plan", command=self.on_import)
        import_btn.pack(side="left", padx=4)
        self._toolbar_widgets["import_plan"] = import_btn
        export_btn = ttk.Button(top, text="Export Plan", command=self.on_export)
        export_btn.pack(side="left", padx=4)
        self._toolbar_widgets["export_plan"] = export_btn

        sidebar_buttons: List[PluginToolbarButton] = list(self._plugin_toolbar_buttons)

        settings_btn = ttk.Button(top, text="⚙", width=3, command=self.show_settings)
        settings_btn.pack(side="right", padx=4)
        self._toolbar_widgets["settings"] = settings_btn

        mid = ttk.Frame(root_container)
        mid.pack(fill="both", expand=True, padx=12, pady=(6, 8))
        header = ttk.Frame(mid)
        header.pack(fill="x", pady=(0, 6))
        ttk.Label(header, textvariable=self.summary_var).pack(side="left")
        self._duplicate_filter_check = ttk.Checkbutton(
            header,
            text="Duplicates only",
            variable=self._duplicate_filter_var,
            command=self._on_duplicate_filter_toggle,
        )
        self._duplicate_filter_check.pack(side="right")

        left = ttk.Frame(mid)
        left.pack(side="left", fill="both", expand=True)
        tree_frame = ttk.Frame(left)
        tree_frame.pack(fill="both", expand=True)
        base_columns = [
            "inc",
            "rel",
            "name",
            "size",
            "type",
            "target",
            "conf",
            "linked",
            DUPLICATE_EXTRA_KEY,
            "meta",
            "notes",
        ]
        base_headings = {
            "inc": "✔",
            "rel": "Folder",
            "name": "File",
            "size": "MB",
            "type": "Type",
            "target": "Target Folder",
            "conf": "Conf",
            "linked": "Linked",
            DUPLICATE_EXTRA_KEY: "Dup",
            "meta": "Tags",
            "notes": "Notes",
        }
        base_anchors = {
            "inc": "center",
            "rel": "w",
            "name": "w",
            "size": "e",
            "type": "w",
            "target": "w",
            "conf": "e",
            "linked": "center",
            DUPLICATE_EXTRA_KEY: "center",
            "meta": "w",
            "notes": "w",
        }
        columns = list(base_columns)
        if self._plugin_columns:
            insert_at = columns.index("linked")
            for plugin_column in self._plugin_columns:
                columns.insert(insert_at, plugin_column.column_id)
                insert_at += 1
        self.tree = ttk.Treeview(tree_frame, columns=tuple(columns), show="headings", selectmode="extended")
        tree_columns = tuple(self.tree["columns"])
        accepted, rejected = _partition_plugin_columns(self._plugin_columns, tree_columns)
        for plugin_column in rejected:
            self.log(f"Plugin column '{plugin_column.column_id}' was rejected by Tk and will be ignored.")
        self._plugin_columns = accepted
        self._column_order = list(tree_columns)
        headings = dict(base_headings)
        anchors = dict(base_anchors)
        for plugin_column in self._plugin_columns:
            headings[plugin_column.column_id] = plugin_column.heading
            anchors[plugin_column.column_id] = plugin_column.anchor
        self._column_anchors = anchors
        for column in self._column_order:
            self.tree.heading(column, text=headings.get(column, column))
            anchor = self._column_anchors.get(column, "w") or "w"
            self.tree.column(column, anchor=anchor, stretch=False, width=80)
        ysb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        ysb.grid(row=0, column=1, sticky="ns")
        xsb.grid(row=1, column=0, sticky="ew")
        tree_frame.grid_columnconfigure(0, weight=1)
        tree_frame.grid_rowconfigure(0, weight=1)

        right_container = ttk.Frame(mid)
        right_container.pack(side="right", fill="y", padx=(10, 0))
        right_container.pack_propagate(False)
        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        sidebar_canvas = tk.Canvas(
            right_container,
            borderwidth=0,
            highlightthickness=0,
            width=260,
            bg=palette.get("alt", "#1f2328"),
        )
        self.sidebar_canvas = sidebar_canvas
        sidebar_canvas.pack(side="left", fill="both", expand=True)
        sidebar_scrollbar = ttk.Scrollbar(right_container, orient="vertical", command=sidebar_canvas.yview)
        sidebar_scrollbar.pack(side="right", fill="y")
        right_container.configure(width=260 + sidebar_scrollbar.winfo_reqwidth())
        sidebar_canvas.configure(yscrollcommand=sidebar_scrollbar.set)
        sidebar_frame = ttk.Frame(sidebar_canvas, style="Sidebar.TFrame")
        sidebar_window = sidebar_canvas.create_window((0, 0), window=sidebar_frame, anchor="nw")
        self.sidebar = sidebar_frame
        self.sidebar_frame = sidebar_frame

        def _configure_sidebar(_event=None) -> None:
            sidebar_canvas.configure(scrollregion=sidebar_canvas.bbox("all"))
            sidebar_canvas.itemconfigure(sidebar_window, width=sidebar_canvas.winfo_width())

        sidebar_frame.bind("<Configure>", _configure_sidebar)
        sidebar_canvas.bind(
            "<Configure>", lambda event: sidebar_canvas.itemconfigure(sidebar_window, width=event.width)
        )
        sidebar_frame.update_idletasks()
        _configure_sidebar()

        def _on_sidebar_mousewheel(event) -> None:
            if event.delta:
                sidebar_canvas.yview_scroll(int(-event.delta / 120), "units")
            elif event.num == 4:
                sidebar_canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                sidebar_canvas.yview_scroll(1, "units")

        sidebar_canvas.bind("<MouseWheel>", _on_sidebar_mousewheel)
        sidebar_canvas.bind("<Button-4>", lambda _e: sidebar_canvas.yview_scroll(-1, "units"))
        sidebar_canvas.bind("<Button-5>", lambda _e: sidebar_canvas.yview_scroll(1, "units"))
        sidebar_frame.bind("<MouseWheel>", _on_sidebar_mousewheel)
        sidebar_frame.bind("<Button-4>", lambda _e: sidebar_canvas.yview_scroll(-1, "units"))
        sidebar_frame.bind("<Button-5>", lambda _e: sidebar_canvas.yview_scroll(1, "units"))

        selection_section = ttk.LabelFrame(
            sidebar_frame,
            text="Selection",
            padding=(12, 10),
            style="Sidebar.TLabelframe",
        )
        selection_section.pack(fill="x", pady=(0, 10))
        self.sel_label = ttk.Label(selection_section, text="None selected", style="Sidebar.TLabel")
        self.sel_label.pack(anchor="w", pady=(0, 6))
        ttk.Label(selection_section, text="Type", style="SidebarHeading.TLabel").pack(anchor="w")
        self.type_cb = ttk.Combobox(
            selection_section,
            values=CATEGORY_ORDER,
            state="readonly",
            style="Sidebar.TCombobox",
        )
        self.type_cb.pack(fill="x", pady=(0, 6))
        ttk.Label(selection_section, text="Target Folder", style="SidebarHeading.TLabel").pack(anchor="w")
        self.target_entry = ttk.Entry(selection_section, style="Sidebar.TEntry")
        self.target_entry.pack(fill="x", pady=(0, 6))
        ttk.Button(
            selection_section,
            text="Apply to Selected",
            command=self.on_apply_selected,
            style="Sidebar.TButton",
        ).pack(fill="x", pady=(0, 4))
        ttk.Button(
            selection_section,
            text="Toggle Include",
            command=self.on_toggle_include,
            style="Sidebar.TButton",
        ).pack(fill="x")

        batch_section = ttk.LabelFrame(
            sidebar_frame,
            text="Batch Tools",
            padding=(12, 10),
            style="Sidebar.TLabelframe",
        )
        batch_section.pack(fill="x", pady=(0, 10))
        ttk.Label(batch_section, text="Keyword", style="SidebarHeading.TLabel").pack(anchor="w")
        self.batch_keyword = ttk.Entry(batch_section, style="Sidebar.TEntry")
        self.batch_keyword.pack(fill="x", pady=(0, 6))
        ttk.Button(
            batch_section,
            text="Assign Type to Matches",
            command=self.on_batch_assign,
            style="Sidebar.TButton",
        ).pack(fill="x")

        utilities_section = ttk.LabelFrame(
            sidebar_frame,
            text="Utilities",
            padding=(12, 10),
            style="Sidebar.TLabelframe",
        )
        utilities_section.pack(fill="x", pady=(0, 10))
        ttk.Button(
            utilities_section,
            text="Recalculate Targets",
            command=self.on_recalc_targets,
            style="Sidebar.TButton",
        ).pack(fill="x")
        ttk.Button(
            utilities_section,
            text="Select All",
            command=lambda: self.tree.selection_set(self.tree.get_children()),
            style="Sidebar.TButton",
        ).pack(fill="x", pady=(6, 0))
        ttk.Button(
            utilities_section,
            text="Select None",
            command=lambda: self.tree.selection_remove(self.tree.get_children()),
            style="Sidebar.TButton",
        ).pack(fill="x", pady=(6, 0))
        ttk.Separator(utilities_section, style="Sidebar.TSeparator").pack(fill="x", pady=10)
        ttk.Button(
            utilities_section,
            text="Plugin Status",
            command=self.show_mod_status_popup,
            style="Sidebar.TButton",
        ).pack(fill="x")
        ttk.Button(
            utilities_section,
            text="Undo Last",
            command=self.on_undo,
            style="Sidebar.TButton",
        ).pack(fill="x", pady=(6, 0))
        ttk.Button(
            utilities_section,
            text="History",
            command=self.show_move_history,
            style="Sidebar.TButton",
        ).pack(fill="x", pady=(6, 0))

        loadout_section = ttk.LabelFrame(
            sidebar_frame,
            text="Loadouts",
            padding=(12, 10),
            style="Sidebar.TLabelframe",
        )
        loadout_section.pack(fill="x", pady=(0, 10))
        ttk.Label(loadout_section, text="Active Loadout", style="SidebarHeading.TLabel").pack(anchor="w")
        self._loadout_selector = ttk.Combobox(
            loadout_section,
            textvariable=self.loadout_var,
            state="readonly",
            width=24,
            values=tuple(sorted(self.loadouts.keys())),
            style="Sidebar.TCombobox",
        )
        self._loadout_selector.pack(fill="x", pady=(0, 6))
        self._loadout_selector.bind("<<ComboboxSelected>>", self._on_loadout_selected)
        ttk.Button(
            loadout_section,
            text="New Loadout",
            command=self.on_create_loadout,
            style="Sidebar.TButton",
        ).pack(fill="x")
        ttk.Button(
            loadout_section,
            text="Rename Loadout",
            command=self.on_rename_loadout,
            style="Sidebar.TButton",
        ).pack(fill="x", pady=(6, 0))
        ttk.Button(
            loadout_section,
            text="Delete Loadout",
            command=self.on_delete_loadout,
            style="Sidebar.TButton",
        ).pack(fill="x", pady=(6, 0))
        self._loadout_apply_btn = ttk.Button(
            loadout_section,
            text="Apply Selected Loadout",
            command=self.on_apply_loadout,
            style="Sidebar.TButton",
        )
        self._loadout_apply_btn.pack(fill="x", pady=(10, 0))

        if sidebar_buttons:
            plugin_section = ttk.LabelFrame(
                sidebar_frame,
                text="Plugin Buttons",
                padding=(12, 10),
                style="Sidebar.TLabelframe",
            )
            plugin_section.pack(fill="x", pady=(0, 10))
            plugin_container = ttk.Frame(plugin_section, style="Sidebar.TFrame")
            plugin_container.pack(fill="x")
            self._build_plugin_toolbar_buttons(plugin_container, sidebar_buttons, placement="sidebar")

        bottom = ttk.Frame(root_container)
        bottom.pack(fill="x", padx=12, pady=8)
        progress_container = ttk.Frame(bottom)
        progress_container.pack(side="left", fill="x", expand=True)
        self.progress = ttk.Progressbar(progress_container, orient="horizontal", mode="determinate")
        self.progress.pack(fill="x")
        self.status_label = ttk.Label(progress_container, textvariable=self.status_var, anchor="w")
        self.status_label.pack(fill="x", pady=(6, 0))
        ttk.Button(bottom, text="Complete Sorting", command=self.on_complete).pack(side="right", padx=6, anchor="n")

        log_frame = ttk.Frame(root_container)
        log_frame.pack(fill="both", padx=12, pady=(0, 10))
        palette = self._theme_cache
        self.log_text = tk.Text(
            log_frame,
            height=6,
            wrap="word",
            state="disabled",
            relief="flat",
            bg=palette.get("alt", "#1f2328"),
            fg=palette.get("fg", "#E6E6E6"),
        )
        self.log_text.pack(fill="both", expand=False)
        self._configure_log_tags()

        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self.tree.bind("<Double-1>", self.on_double_click)
        self.tree.bind("<Motion>", self._on_tree_motion)
        self.tree.bind("<Leave>", lambda _e: self._hide_tooltip())
        self._refresh_loadout_controls()

    def _pack_toolbar_button(
        self,
        widget: tk.Widget,
        *,
        button_id: Optional[str] = None,
        side: str = "left",
        padx: int = 4,
        insert_before: Optional[str] = None,
        fill: Optional[str] = None,
        pady: int = 0,
    ) -> None:
        target = None
        parent = widget.master
        if insert_before:
            target = self._toolbar_widgets.get(insert_before)
            if target is not None:
                if not target.winfo_exists():
                    target = None
                else:
                    try:
                        target_parent = target.nametowidget(target.winfo_parent())
                    except Exception:
                        target_parent = getattr(target, "master", None)
                    if target_parent is not parent:
                        target = None
        pack_kwargs = {"side": side, "padx": padx}
        if fill:
            pack_kwargs["fill"] = fill
        if pady:
            pack_kwargs["pady"] = pady
        if target is not None:
            pack_kwargs["before"] = target
        try:
            widget.pack(**pack_kwargs)
        except tk.TclError:
            # Retry without relative placement if Tk rejects the request.
            pack_kwargs.pop("before", None)
            widget.pack(**pack_kwargs)
        if button_id:
            self._toolbar_widgets[button_id] = widget

    def _build_plugin_toolbar_buttons(
        self,
        parent: tk.Widget,
        entries: Sequence[PluginToolbarButton],
        *,
        placement: str,
    ) -> None:
        if not entries:
            return
        manager = self.plugin_manager
        if manager is None:
            return
        placement = placement.lower()
        sidebar_mode = placement not in {"left", "right"}
        side = "top" if sidebar_mode else placement
        fill = "x" if sidebar_mode else None
        pady = 4 if sidebar_mode else 0
        style = "Sidebar.TButton" if sidebar_mode else None
        for entry in entries:
            try:
                text = entry.text or entry.button_id
            except Exception:
                text = entry.button_id
            button = ttk.Button(
                parent,
                text=text,
                command=lambda e=entry: self._invoke_plugin_toolbar_button(e),
                style=style,
            )
            padx = entry.padx if not sidebar_mode else 0
            self._pack_toolbar_button(
                button,
                button_id=entry.button_id,
                side=side,
                padx=padx,
                pady=pady,
                fill=fill,
                insert_before=entry.insert_before,
            )

    def _invoke_plugin_toolbar_button(self, entry: PluginToolbarButton) -> None:
        manager = self.plugin_manager
        if manager is None:
            return
        try:
            entry.command(self, manager.api)
        except Exception as exc:
            self.log(f"Plugin toolbar '{entry.button_id}' failed: {exc}", level="error")

    def _build_settings_overlay(self) -> None:
        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        self._settings_sidebar_width = 360
        self.settings_scrim = None

        self.settings_sidebar = tk.Frame(
            self,
            bg=palette.get("sel", "#2A2F3A"),
            width=self._settings_sidebar_width,
            highlightthickness=0,
            bd=0,
        )
        self.settings_sidebar.place_forget()
        self.settings_sidebar.grid_propagate(False)
        self.settings_sidebar.columnconfigure(0, weight=1)
        self.settings_sidebar.rowconfigure(0, weight=1)

        container = ttk.Frame(self.settings_sidebar, padding=(16, 20, 16, 20))
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)

        row = 0
        header = ttk.Frame(container)
        header.grid(row=row, column=0, sticky="ew")
        ttk.Label(header, text="Settings", font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Button(header, text="Close", command=self.hide_settings, width=7).pack(side="right")
        row += 1

        ttk.Separator(container).grid(row=row, column=0, sticky="ew", pady=(12, 10))
        row += 1

        updates_section = ttk.Frame(container)
        updates_section.grid(row=row, column=0, sticky="ew")
        updates_section.columnconfigure(0, weight=1)
        ttk.Label(
            updates_section,
            textvariable=self._version_display_var,
            font=("TkDefaultFont", 10, "bold"),
        ).grid(row=0, column=0, sticky="w")
        self.check_updates_button = ttk.Button(
            updates_section,
            text="Check for Updates",
            command=self._on_manual_update_check,
        )
        self.check_updates_button.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        row += 1

        ttk.Separator(container).grid(row=row, column=0, sticky="ew", pady=(12, 10))
        row += 1

        theme_section = ttk.Frame(container)
        theme_section.grid(row=row, column=0, sticky="ew")
        theme_section.columnconfigure(0, weight=1)
        ttk.Label(theme_section, text="Themes", font=("TkDefaultFont", 10, "bold")).grid(row=0, column=0, sticky="w")
        theme_controls = ttk.Frame(theme_section)
        theme_controls.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        theme_controls.columnconfigure(1, weight=1)
        ttk.Label(theme_controls, text="Theme").grid(row=0, column=0, sticky="w")
        self.theme_cb = ttk.Combobox(
            theme_controls,
            values=list(THEMES.keys()),
            textvariable=self.theme_name,
            state="readonly",
        )
        self.theme_cb.grid(row=0, column=1, sticky="ew", padx=(6, 0))
        ttk.Button(theme_controls, text="Apply", command=self.on_apply_theme).grid(row=0, column=2, padx=(6, 0))

        self.theme_preview_container = ttk.Frame(theme_section)
        self.theme_preview_container.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        for column in range(max(1, min(2, len(THEMES)))):
            self.theme_preview_container.columnconfigure(column, weight=1)
        self._build_theme_preview_widgets()
        row += 1

        scan_section = ttk.Frame(container)
        scan_section.grid(row=row, column=0, sticky="ew", pady=(18, 0))
        scan_section.columnconfigure(0, weight=1)
        ttk.Label(scan_section, text="Scanning", font=("TkDefaultFont", 10, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(scan_section, text="Scan subfolders", variable=self.recurse_var).grid(
            row=1, column=0, sticky="w", pady=(6, 0)
        )
        ttk.Label(scan_section, text="Ignore extensions (comma separated)").grid(
            row=2, column=0, sticky="w", pady=(10, 2)
        )
        ttk.Entry(scan_section, textvariable=self.ignore_exts_var).grid(row=3, column=0, sticky="ew")
        ttk.Label(scan_section, text="Ignore names containing (comma separated)").grid(
            row=4, column=0, sticky="w", pady=(10, 2)
        )
        ttk.Entry(scan_section, textvariable=self.ignore_names_var).grid(row=5, column=0, sticky="ew")
        ttk.Label(scan_section, text="File types to include").grid(row=6, column=0, sticky="w", pady=(12, 2))
        type_frame = ttk.Frame(scan_section)
        type_frame.grid(row=7, column=0, sticky="w")
        ttk.Checkbutton(
            type_frame,
            text="Packages (.package)",
            variable=self.scan_package_var,
        ).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(
            type_frame,
            text="Scripts (.ts4script)",
            variable=self.scan_script_var,
        ).grid(row=0, column=1, sticky="w", padx=(12, 0))
        ttk.Checkbutton(
            type_frame,
            text="Archives (.zip/.rar/.7z)",
            variable=self.scan_archive_var,
        ).grid(row=1, column=0, sticky="w", pady=(4, 0))
        ttk.Checkbutton(
            type_frame,
            text="Other supported files",
            variable=self.scan_misc_var,
        ).grid(row=1, column=1, sticky="w", padx=(12, 0), pady=(4, 0))
        ttk.Checkbutton(
            scan_section,
            text="Include adult content",
            variable=self.include_adult_var,
        ).grid(row=8, column=0, sticky="w", pady=(12, 0))
        row += 1

        actions = ttk.Frame(container)
        actions.grid(row=row + 1, column=0, sticky="e", pady=(20, 0))
        ttk.Button(actions, text="Done", command=self.hide_settings).grid(row=0, column=0)

        self.settings_sidebar.bind("<Escape>", lambda _e: self.hide_settings())
        self._update_theme_preview_highlight()
    # ------------------------------------------------------------------
    # Theme preview helpers
    # ------------------------------------------------------------------
    def _build_theme_preview_widgets(self) -> None:
        for child in self.theme_preview_container.winfo_children():
            child.destroy()
        self.theme_preview_canvases: Dict[str, tk.Canvas] = {}
        columns = max(1, min(2, len(THEMES)))
        for index, (name, palette) in enumerate(THEMES.items()):
            row = index // columns
            column = index % columns
            cell = ttk.Frame(self.theme_preview_container)
            cell.grid(row=row, column=column, padx=4, pady=4, sticky="nsew")
            cell.columnconfigure(0, weight=1)
            canvas = tk.Canvas(cell, width=120, height=60, highlightthickness=2)
            canvas.grid(row=0, column=0, sticky="ew")
            canvas.create_rectangle(0, 0, 120, 60, fill=palette["bg"], outline="")
            canvas.create_rectangle(0, 0, 120, 24, fill=palette["alt"], outline="")
            canvas.create_rectangle(0, 24, 120, 40, fill=palette["accent"], outline="")
            canvas.create_rectangle(0, 40, 120, 60, fill=palette["sel"], outline="")
            label = ttk.Label(cell, text=name, anchor="center")
            label.grid(row=1, column=0, pady=(4, 0))

            def bind_target(widget: tk.Widget, theme_name: str = name) -> None:
                widget.bind("<Button-1>", lambda _e, tn=theme_name: self._on_theme_preview_click(tn))

            bind_target(canvas)
            bind_target(label)
            self.theme_preview_canvases[name] = canvas
        self._update_theme_preview_highlight()

    def _on_theme_preview_click(self, theme_name: str) -> None:
        self.theme_name.set(theme_name)
        if hasattr(self, "theme_cb"):
            self.theme_cb.set(theme_name)
        self._update_theme_preview_highlight()

    def _update_theme_preview_highlight(self) -> None:
        canvases = getattr(self, "theme_preview_canvases", {})
        selected = self.theme_name.get()
        accent = THEMES.get(selected, THEMES["Dark Mode"])["accent"]
        neutral = self._theme_cache.get("alt", "#444444")
        for name, canvas in canvases.items():
            border = accent if name == selected else neutral
            canvas.configure(highlightbackground=border, highlightcolor=border)

    def _resolve_allowed_extensions(self) -> Optional[set[str]]:
        allowed: set[str] = set()
        if self.scan_package_var.get():
            allowed.update(PACKAGE_EXTS)
        if self.scan_script_var.get():
            allowed.update(SCRIPT_EXTS)
        if self.scan_archive_var.get():
            allowed.update(ARCHIVE_EXTS)
        if self.scan_misc_var.get():
            other = SUPPORTED_EXTS - PACKAGE_EXTS - SCRIPT_EXTS - ARCHIVE_EXTS
            allowed.update(other)
        if not allowed:
            return set()
        if allowed >= SUPPORTED_EXTS:
            return None
        return {ext.lower() for ext in allowed}
    # ------------------------------------------------------------------
    # Queue helpers and logging
    # ------------------------------------------------------------------
    def _enqueue_ui(self, func: Callable[[], None]) -> None:
        self._ui_queue.put(func)

    def _pump_ui_queue(self) -> None:
        try:
            while True:
                func = self._ui_queue.get_nowait()
                try:
                    func()
                except Exception as exc:
                    print(f"UI callback error: {exc}", file=sys.stderr)
        except queue.Empty:
            pass
        self.after(16, self._pump_ui_queue)

    def schedule_refresh(self) -> None:
        self._enqueue_ui(lambda: self._refresh_tree(preserve_selection=True))

    def _configure_log_tags(self) -> None:
        if not hasattr(self, "log_text"):
            return
        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        base_fg = palette.get("fg", "#E6E6E6")
        accent_fg = palette.get("accent", "#4C8BF5")
        error_fg = "#FF6B6B"
        warning_fg = "#FFB347"
        self.log_text.tag_configure("info", foreground=base_fg)
        self.log_text.tag_configure("plugin", foreground=accent_fg)
        self.log_text.tag_configure("error", foreground=error_fg)
        self.log_text.tag_configure("warning", foreground=warning_fg)

    @staticmethod
    def _resolve_log_tag(level: str) -> str:
        value = (level or "info").lower()
        if value in {"error", "critical"}:
            return "error"
        if value in {"warn", "warning"}:
            return "warning"
        if value.startswith("plugin"):
            return "plugin"
        return "info"

    def log(self, message: str, level: str = "info") -> None:
        timestamp = time.strftime("%H:%M:%S")
        prefix = ""
        if level and level.lower() != "info":
            display_level = level.replace("_", " ").upper()
            prefix = f"[{display_level}] "
        self.log_text.configure(state="normal")
        entry = f"[{timestamp}] {prefix}{message}\n"
        tag = self._resolve_log_tag(level)
        self.log_text.insert("end", entry, (tag,))
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    # ------------------------------------------------------------------
    # Update checks
    # ------------------------------------------------------------------
    def _refresh_version_display(self) -> None:
        if hasattr(self, "_version_display_var"):
            if self._update_available and self._latest_version:
                self._version_display_var.set(
                    f"App Version: {APP_VERSION} (Update available: {self._latest_version})"
                )
            else:
                self._version_display_var.set(f"App Version: {APP_VERSION}")

    def _ensure_update_overlay(self) -> tk.Toplevel:
        overlay = self._update_overlay
        if overlay and overlay.winfo_exists():
            self._refresh_update_overlay_theme()
            return overlay

        overlay = tk.Toplevel(self)
        overlay.withdraw()
        overlay.title("Checking for Updates")
        overlay.transient(self)
        overlay.resizable(False, False)
        overlay.protocol("WM_DELETE_WINDOW", lambda: None)
        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        overlay.configure(bg=palette.get("bg", "#111316"))

        container = ttk.Frame(overlay, padding=(16, 20, 16, 20), style="UpdateOverlay.TFrame")
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)

        hero = ttk.Frame(container, style="UpdateOverlayHero.TFrame")
        hero.grid(row=0, column=0, sticky="we", pady=(0, 16))
        hero.columnconfigure(1, weight=1)

        icon_label = ttk.Label(
            hero,
            textvariable=self._update_overlay_status_icon,
            style="UpdateOverlayIcon.TLabel",
            anchor="center",
            width=3,
        )
        if not self._update_overlay_icon_font:
            try:
                base_font = tkfont.nametofont("TkHeadingFont")
                size = max(int(base_font.cget("size")) + 12, int(base_font.cget("size")))
                self._update_overlay_icon_font = tkfont.Font(
                    family=base_font.cget("family"), size=size, weight="bold"
                )
            except tk.TclError:
                self._update_overlay_icon_font = tkfont.Font(size=28, weight="bold")
        icon_label.configure(font=self._update_overlay_icon_font)
        icon_label.grid(row=0, column=0, rowspan=2, sticky="n", padx=(0, 20))

        headline_label = ttk.Label(
            hero,
            textvariable=self._update_overlay_headline,
            style="UpdateOverlayHeadline.TLabel",
        )
        if not self._update_overlay_headline_font:
            try:
                heading_font = tkfont.nametofont("TkHeadingFont")
                headline_size = max(int(heading_font.cget("size")) + 6, int(heading_font.cget("size")))
                self._update_overlay_headline_font = tkfont.Font(
                    family=heading_font.cget("family"), size=headline_size, weight="bold"
                )
            except tk.TclError:
                self._update_overlay_headline_font = tkfont.Font(size=20, weight="bold")
        headline_label.configure(font=self._update_overlay_headline_font)
        headline_label.grid(row=0, column=1, sticky="w")
        self._update_overlay_headline_label = headline_label

        body_label = ttk.Label(
            hero,
            textvariable=self._update_overlay_message,
            style="UpdateOverlayBody.TLabel",
            wraplength=460,
            justify="left",
        )
        body_label.grid(row=1, column=1, sticky="we", pady=(6, 0))
        self._update_overlay_body_label = body_label

        progress_frame = ttk.Frame(container, style="UpdateOverlayProgress.TFrame")
        progress_frame.grid(row=1, column=0, sticky="we")
        progress_frame.columnconfigure(0, weight=1)
        self._update_overlay_progress_frame = progress_frame

        progress_title_label = ttk.Label(
            progress_frame,
            textvariable=self._update_overlay_progress_title,
            style="UpdateOverlayProgressTitle.TLabel",
        )
        progress_title_label.pack(fill="x")
        self._update_overlay_progress_title_label = progress_title_label

        progress_detail_label = ttk.Label(
            progress_frame,
            textvariable=self._update_overlay_progress_detail,
            style="UpdateOverlayProgressDetail.TLabel",
            wraplength=460,
            justify="left",
        )
        progress_detail_label.pack(fill="x", pady=(4, 0))
        self._update_overlay_progress_detail_label = progress_detail_label

        progress = ttk.Progressbar(
            progress_frame,
            mode="indeterminate",
            style="UpdateOverlay.Horizontal.TProgressbar",
        )
        progress.pack(fill="x", pady=(12, 0))

        changelog_label = ttk.Label(
            progress_frame,
            textvariable=self._update_overlay_changelog,
            style="UpdateOverlayChangelog.TLabel",
            wraplength=460,
            justify="left",
        )
        changelog_label.pack(fill="x", pady=(12, 0))
        self._update_overlay_changelog_label = changelog_label

        mode_frame = ttk.LabelFrame(container, text="Download Mode")
        mode_frame.grid(row=2, column=0, sticky="we", pady=(24, 0))
        self._update_mode_frame = mode_frame

        simple_radio = ttk.Radiobutton(
            mode_frame,
            text="Simple — automatically replace all updated files",
            value="simple",
            variable=self._update_download_mode,
            command=self._on_update_mode_changed,
        )
        simple_radio.pack(anchor="w", padx=12, pady=(8, 0))
        self._update_mode_simple_radio = simple_radio

        advanced_radio = ttk.Radiobutton(
            mode_frame,
            text="Advanced — choose which files to install",
            value="advanced",
            variable=self._update_download_mode,
            command=self._on_update_mode_changed,
        )
        advanced_radio.pack(anchor="w", padx=12, pady=(4, 4))
        self._update_mode_advanced_radio = advanced_radio

        description_label = ttk.Label(
            mode_frame,
            textvariable=self._update_mode_description,
            wraplength=400,
            justify="left",
        )
        description_label.pack(fill="x", padx=12, pady=(0, 8))
        self._update_mode_description_label = description_label

        buttons = ttk.Frame(container, style="UpdateOverlay.TFrame")
        buttons.grid(row=3, column=0, sticky="e", pady=(24, 0))

        download_btn = ttk.Button(
            buttons,
            text="Auto Update",
            command=self._on_update_overlay_auto_update,
            state="disabled",
            style="Accent.TButton",
        )
        download_btn.pack(side="left", padx=4)

        manual_btn = ttk.Button(
            buttons,
            text="Manual Download",
            command=self._on_update_overlay_manual_download,
            state="disabled",
        )
        manual_btn.pack(side="left", padx=4)

        details_btn = ttk.Button(
            buttons,
            text="View Release",
            command=self._on_update_overlay_details,
            state="disabled",
        )
        details_btn.pack(side="left", padx=4)

        skip_btn = ttk.Button(
            buttons,
            text="Skip for Now",
            command=self._on_update_overlay_skip,
            state="disabled",
        )
        skip_btn.pack(side="left", padx=4)

        if self._update_overlay_progress_title_label:
            self._update_overlay_progress_title_label.pack_forget()
        if self._update_overlay_progress_detail_label:
            self._update_overlay_progress_detail_label.pack_forget()
        if progress.winfo_manager():
            progress.pack_forget()
        if self._update_overlay_changelog_label:
            self._update_overlay_changelog_label.pack_forget()
        if self._update_overlay_progress_frame:
            self._update_overlay_progress_frame.grid_remove()

        self._update_overlay = overlay
        self._update_overlay_progress = progress
        self._update_overlay_download_btn = download_btn
        self._update_overlay_manual_btn = manual_btn
        self._update_overlay_details_btn = details_btn
        self._update_overlay_skip_btn = skip_btn
        self._update_overlay_button_frame = buttons
        self._on_update_mode_changed()
        return overlay

    def _refresh_update_overlay_theme(self) -> None:
        overlay = getattr(self, "_update_overlay", None)
        if overlay and overlay.winfo_exists():
            palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
            overlay.configure(bg=palette.get("bg", "#111316"))
            style = ttk.Style()
            style.configure("UpdateOverlay.TFrame", background=palette["alt"])
            style.configure("UpdateOverlayHero.TFrame", background=palette["alt"])
            style.configure("UpdateOverlayHeadline.TLabel", background=palette["alt"], foreground=palette["fg"])
            style.configure("UpdateOverlayBody.TLabel", background=palette["alt"], foreground=palette["fg"])
            style.configure("UpdateOverlayIcon.TLabel", background=palette["alt"], foreground=palette["accent"])
            style.configure("UpdateOverlayProgress.TFrame", background=palette["alt"])
            style.configure("UpdateOverlayProgressTitle.TLabel", background=palette["alt"], foreground=palette["fg"])
            style.configure("UpdateOverlayProgressDetail.TLabel", background=palette["alt"], foreground=palette["fg"])
            style.configure("UpdateOverlayChangelog.TLabel", background=palette["alt"], foreground=palette["fg"])
            style.configure("Accent.TButton", background=palette["accent"], foreground=palette["fg"], padding=(12, 8))
            style.map(
                "Accent.TButton",
                background=[("active", palette["sel"]), ("pressed", palette["sel"]), ("disabled", palette["alt"])],
                foreground=[("disabled", palette["fg"])]
            )
            style.configure(
                "UpdateOverlay.Horizontal.TProgressbar",
                background=palette["accent"],
                troughcolor=palette["bg"],
            )
            if self._update_overlay_button_frame:
                self._update_overlay_button_frame.configure(style="UpdateOverlay.TFrame")
            if self._update_overlay_progress_frame:
                self._update_overlay_progress_frame.configure(style="UpdateOverlayProgress.TFrame")
            if self._update_overlay_headline_label:
                self._update_overlay_headline_label.configure(style="UpdateOverlayHeadline.TLabel")
            if self._update_overlay_body_label:
                self._update_overlay_body_label.configure(style="UpdateOverlayBody.TLabel")
            if self._update_overlay_progress_title_label:
                self._update_overlay_progress_title_label.configure(style="UpdateOverlayProgressTitle.TLabel")
            if self._update_overlay_progress_detail_label:
                self._update_overlay_progress_detail_label.configure(style="UpdateOverlayProgressDetail.TLabel")
            if self._update_overlay_changelog_label:
                self._update_overlay_changelog_label.configure(style="UpdateOverlayChangelog.TLabel")
            if self._update_mode_frame:
                try:
                    self._update_mode_frame.configure(style="TLabelframe")
                except Exception:
                    pass

    def _center_update_overlay(self) -> None:
        overlay = getattr(self, "_update_overlay", None)
        if not overlay or not overlay.winfo_exists():
            return
        self.update_idletasks()
        try:
            ow = overlay.winfo_width()
            oh = overlay.winfo_height()
            if ow <= 1 and oh <= 1:
                overlay.update_idletasks()
                ow = overlay.winfo_width()
                oh = overlay.winfo_height()
            x = self.winfo_rootx() + max((self.winfo_width() - ow) // 2, 0)
            y = self.winfo_rooty() + max((self.winfo_height() - oh) // 2, 0)
            overlay.geometry(f"+{x}+{y}")
        except Exception:
            pass

    def _show_update_overlay(
        self,
        message: str,
        *,
        headline: Optional[str] = None,
        progress: bool,
        enable_download: bool,
        enable_skip: bool,
        enable_details: bool,
        enable_manual: bool = False,
        progress_title: Optional[str] = None,
        progress_subtext: Optional[str] = None,
        changelog: Optional[str] = None,
        status_icon: Optional[str] = None,
        origin: str = "general",
    ) -> None:
        overlay = self._ensure_update_overlay()
        self._update_overlay_origin = origin
        resolved_headline: str
        if headline:
            resolved_headline = headline
        else:
            if progress:
                resolved_headline = "Working on update"
            elif enable_download:
                resolved_headline = "Update available"
            elif enable_details:
                resolved_headline = "Release information"
            else:
                resolved_headline = "Update status"
        self._update_overlay_headline.set(resolved_headline)
        icon_value = status_icon if status_icon is not None else ("🔄" if progress else "⬆️")
        self._update_overlay_status_icon.set(icon_value)
        self._update_overlay_message.set(message)

        if self._update_overlay_progress_frame:
            if progress or (progress_subtext and progress_subtext.strip()) or (changelog and changelog.strip()):
                self._update_overlay_progress_frame.grid()
            else:
                self._update_overlay_progress_frame.grid_remove()

        resolved_progress_title = progress_title
        if resolved_progress_title is None:
            resolved_progress_title = "Progress" if progress else ""
        self._update_overlay_progress_title.set(resolved_progress_title)
        if self._update_overlay_progress_title_label:
            if resolved_progress_title:
                if not self._update_overlay_progress_title_label.winfo_manager():
                    self._update_overlay_progress_title_label.pack(fill="x")
            elif self._update_overlay_progress_title_label.winfo_manager():
                self._update_overlay_progress_title_label.pack_forget()

        detail_text = progress_subtext.strip() if progress_subtext else ""
        self._update_overlay_progress_detail.set(detail_text)
        if self._update_overlay_progress_detail_label:
            if detail_text:
                if not self._update_overlay_progress_detail_label.winfo_manager():
                    self._update_overlay_progress_detail_label.pack(fill="x", pady=(4, 0))
            elif self._update_overlay_progress_detail_label.winfo_manager():
                self._update_overlay_progress_detail_label.pack_forget()

        changelog_text = changelog.strip() if changelog else ""
        self._update_overlay_changelog.set(changelog_text)
        if self._update_overlay_changelog_label:
            if changelog_text:
                if not self._update_overlay_changelog_label.winfo_manager():
                    self._update_overlay_changelog_label.pack(fill="x", pady=(12, 0))
            elif self._update_overlay_changelog_label.winfo_manager():
                self._update_overlay_changelog_label.pack_forget()

        if self._update_overlay_progress:
            self._update_overlay_progress.stop()
            if progress:
                if not self._update_overlay_progress.winfo_manager():
                    self._update_overlay_progress.pack(fill="x", pady=(12, 0))
                self._update_overlay_progress.configure(mode="indeterminate", value=0)
                self._update_overlay_progress.start(12)
            elif self._update_overlay_progress.winfo_manager():
                self._update_overlay_progress.pack_forget()

        if self._update_overlay_download_btn:
            state = "normal" if enable_download else "disabled"
            self._update_overlay_download_btn.configure(state=state)
        if self._update_overlay_manual_btn:
            state = "normal" if enable_manual else "disabled"
            self._update_overlay_manual_btn.configure(state=state)
        if self._update_overlay_details_btn:
            state = "normal" if enable_details else "disabled"
            self._update_overlay_details_btn.configure(state=state)
        if self._update_overlay_skip_btn:
            state = "normal" if enable_skip else "disabled"
            self._update_overlay_skip_btn.configure(state=state)
        mode_state = "normal" if enable_download else "disabled"
        if self._update_mode_simple_radio:
            self._update_mode_simple_radio.configure(state=mode_state)
        if self._update_mode_advanced_radio:
            advanced_state = mode_state
            if advanced_state == "normal" and not self._update_download_url:
                advanced_state = "disabled"
            self._update_mode_advanced_radio.configure(state=advanced_state)

        overlay.deiconify()
        overlay.lift()
        try:
            overlay.grab_set()
        except tk.TclError:
            pass

        focus_target: Optional[tk.Widget] = None
        if enable_download and self._update_overlay_download_btn:
            focus_target = self._update_overlay_download_btn
        elif enable_manual and self._update_overlay_manual_btn:
            focus_target = self._update_overlay_manual_btn
        elif enable_skip and self._update_overlay_skip_btn:
            focus_target = self._update_overlay_skip_btn
        elif enable_details and self._update_overlay_details_btn:
            focus_target = self._update_overlay_details_btn
        if focus_target and focus_target.winfo_exists():
            focus_target.focus_set()
        else:
            overlay.focus_set()

        self._center_update_overlay()
        self._update_overlay_visible = True

    def _hide_update_overlay(self) -> None:
        overlay = getattr(self, "_update_overlay", None)
        if overlay and overlay.winfo_exists():
            try:
                overlay.grab_release()
            except tk.TclError:
                pass
            overlay.withdraw()
        if self._update_overlay_progress:
            self._update_overlay_progress.stop()
            self._update_overlay_progress.configure(mode="indeterminate", value=0)
            if self._update_overlay_progress.winfo_manager():
                self._update_overlay_progress.pack_forget()
        self._update_overlay_headline.set("Sims4 Mod Sorter Update")
        self._update_overlay_status_icon.set("⬆️")
        self._update_overlay_progress_title.set("")
        self._update_overlay_progress_detail.set("")
        self._update_overlay_changelog.set("")
        if self._update_overlay_progress_title_label and self._update_overlay_progress_title_label.winfo_manager():
            self._update_overlay_progress_title_label.pack_forget()
        if self._update_overlay_progress_detail_label and self._update_overlay_progress_detail_label.winfo_manager():
            self._update_overlay_progress_detail_label.pack_forget()
        if self._update_overlay_changelog_label and self._update_overlay_changelog_label.winfo_manager():
            self._update_overlay_changelog_label.pack_forget()
        if self._update_overlay_progress_frame:
            self._update_overlay_progress_frame.grid_remove()
        if self._update_overlay_download_btn:
            self._update_overlay_download_btn.configure(state="disabled")
        if self._update_overlay_details_btn:
            self._update_overlay_details_btn.configure(state="disabled")
        if self._update_overlay_manual_btn:
            self._update_overlay_manual_btn.configure(state="disabled")
        if self._update_overlay_skip_btn:
            self._update_overlay_skip_btn.configure(state="disabled")
        if self._update_mode_simple_radio:
            self._update_mode_simple_radio.configure(state="disabled")
        if self._update_mode_advanced_radio:
            self._update_mode_advanced_radio.configure(state="disabled")
        previous_origin = self._update_overlay_origin
        self._update_overlay_origin = "general"
        self._update_overlay_visible = False
        self._after_update_overlay_hidden(previous_origin)

    def _after_update_overlay_hidden(self, origin: str) -> None:
        if origin != "settings":
            return
        button = getattr(self, "check_updates_button", None)
        if button and button.winfo_exists():
            button.configure(state="normal")
        sidebar = getattr(self, "settings_sidebar", None)
        if sidebar and sidebar.winfo_exists():
            try:
                sidebar.focus_set()
            except tk.TclError:
                pass

    def _on_update_mode_changed(self) -> None:
        mode = self._update_download_mode.get()
        if mode == "advanced":
            description = (
                "You will be prompted to choose which files to install after the download completes."
            )
        else:
            description = "Downloads and installs every updated file automatically."
        self._update_mode_description.set(description)

    def _on_update_overlay_auto_update(self) -> None:
        self._start_update_download(mode="auto-install")

    def _on_update_overlay_manual_download(self) -> None:
        if self._update_download_url:
            self._start_update_download(mode="manual-download")
        elif self._update_release_page_url:
            self._open_release_page()
        else:
            self.bell()

    def _on_update_overlay_details(self) -> None:
        self._open_release_page()

    def _on_update_overlay_skip(self) -> None:
        self._hide_update_overlay()

    def _start_update_download(
        self, *, mode: Literal["auto-install", "manual-download"]
    ) -> None:
        url = self._update_download_url
        if not url:
            if self._update_release_page_url:
                self._open_release_page()
            else:
                messagebox.showinfo(
                    "Update Available",
                    "Download information is not configured.",
                    parent=self,
                )
            if mode == "auto-install":
                self._hide_update_overlay()
            return

        destination = self._prompt_update_destination()
        if not destination:
            if self._update_overlay_visible:
                if self._update_overlay_download_btn and self._update_download_url:
                    self._update_overlay_download_btn.configure(state="normal")
                if self._update_overlay_manual_btn and (
                    self._update_download_url or self._update_release_page_url
                ):
                    self._update_overlay_manual_btn.configure(state="normal")
                if self._update_overlay_skip_btn:
                    self._update_overlay_skip_btn.configure(state="normal")
                if self._update_overlay_details_btn and self._update_release_page_url:
                    self._update_overlay_details_btn.configure(state="normal")
            return

        target_path = Path(destination)
        origin = "settings" if self._update_overlay_origin == "settings" else "general"
        headline = "Downloading update" if mode == "auto-install" else "Downloading update package"
        message = "Downloading update…" if mode == "auto-install" else "Downloading update package…"
        self._show_update_overlay(
            message,
            headline=headline,
            progress=True,
            enable_download=False,
            enable_skip=False,
            enable_details=False,
            enable_manual=False,
            progress_title="Download progress",
            progress_subtext="Preparing download…",
            status_icon="⬇️",
            origin=origin,
        )
        if self._update_overlay_progress:
            self._update_overlay_progress.configure(mode="determinate", maximum=100, value=0)

        threading.Thread(
            target=self._download_update_worker,
            args=(url, target_path, mode),
            daemon=True,
        ).start()

    def _prompt_update_destination(self) -> Optional[str]:
        default_name = self._update_download_filename or f"Sims4ModSorter-{self._latest_version or 'update'}.zip"
        try:
            initial_dir = Path.home()
        except Exception:
            initial_dir = Path.cwd()
        downloads = initial_dir / "Downloads"
        if downloads.exists():
            initial_dir = downloads
        return filedialog.asksaveasfilename(
            parent=self,
            title="Save Update Package As",
            initialfile=default_name,
            defaultextension=Path(default_name).suffix or ".zip",
            filetypes=[("All files", "*.*")],
            initialdir=str(initial_dir),
        ) or None

    def _download_update_worker(
        self, url: str, target_path: Path, mode: Literal["auto-install", "manual-download"]
    ) -> None:
        try:
            opener = urllib.request.build_opener(
                urllib.request.HTTPHandler(), urllib.request.HTTPSHandler()
            )
            headers = {
                "User-Agent": "Sims4ModSorter Update Check",
                "Accept": "application/octet-stream",
            }
            current_url = url
            response = None
            for _ in range(10):
                request = urllib.request.Request(current_url, headers=headers)
                try:
                    response = opener.open(request, timeout=30)
                except urllib.error.HTTPError as exc:
                    if exc.code in {301, 302, 303, 307, 308}:
                        location = exc.headers.get("Location")
                        if not location:
                            raise
                        current_url = urljoin(current_url, location)
                        continue
                    raise
                else:
                    break
            if response is None:
                raise RuntimeError("Too many redirects while downloading update")

            chunk_size = 1024 * 64
            written = 0
            total_header = response.headers.get("Content-Length")
            try:
                total = int(total_header) if total_header is not None else 0
            except ValueError:
                total = 0
            with response:
                with target_path.open("wb") as handle:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        handle.write(chunk)
                        written += len(chunk)
                        self._enqueue_ui(
                            lambda w=written, t=total: self._update_download_progress(w, t)
                        )

            if total > 0 and written != total:
                raise ValueError(
                    f"Downloaded size mismatch: expected {total} bytes but received {written} bytes"
                )
            if written <= 0:
                raise ValueError("Downloaded file was empty")
            if not zipfile.is_zipfile(target_path):
                raise zipfile.BadZipFile("Downloaded file is not a valid ZIP archive")
        except Exception as exc:
            if target_path.exists():
                try:
                    target_path.unlink()
                except Exception:
                    pass
            self._enqueue_ui(
                lambda e=exc: self._handle_update_download_failure(target_path, e, mode)
            )
        else:
            self._enqueue_ui(lambda: self._handle_update_download_success(target_path, mode))

    def _update_download_progress(self, written: int, total: int) -> None:
        if self._update_overlay_progress:
            progress = self._update_overlay_progress
            progress.stop()
            if total > 0:
                progress.configure(mode="determinate", maximum=total, value=written)
            else:
                progress.configure(mode="indeterminate", value=0)
                progress.start(12)
        if self._update_overlay_progress_frame:
            self._update_overlay_progress_frame.grid()
        if self._update_overlay_progress_title_label and not self._update_overlay_progress_title_label.winfo_manager():
            self._update_overlay_progress_title_label.pack(fill="x")
        if self._update_overlay_progress_detail_label and not self._update_overlay_progress_detail_label.winfo_manager():
            self._update_overlay_progress_detail_label.pack(fill="x", pady=(4, 0))
        if self._update_overlay_progress and not self._update_overlay_progress.winfo_manager():
            self._update_overlay_progress.pack(fill="x", pady=(12, 0))
        self._update_overlay_progress_title.set("Download progress")
        self._update_overlay_message.set("Downloading update…")
        self._update_overlay_status_icon.set("⬇️")
        if total > 0:
            percent = min(100, max(0, int((written / total) * 100)))
            human_total = total / (1024 * 1024)
            human_written = written / (1024 * 1024)
            detail = f"{percent}% complete ({human_written:.2f} / {human_total:.2f} MB)"
        else:
            human_written = written / (1024 * 1024)
            detail = f"{human_written:.2f} MB downloaded"
        self._update_overlay_progress_detail.set(detail)

    def _handle_update_download_success(
        self, target_path: Path, mode: Literal["auto-install", "manual-download"]
    ) -> None:
        self._hide_update_overlay()
        self.log(f"Update downloaded to {target_path}")
        if mode == "manual-download":
            messagebox.showinfo(
                "Update Downloaded",
                (
                    "The update package was downloaded successfully.\n"
                    f"Saved to: {target_path}"
                ),
                parent=self,
            )
            return
        selected_entries: Optional[Set[PurePosixPath]] = None
        if self._update_download_mode.get() == "advanced":
            selection = self._prompt_advanced_file_selection(target_path)
            if selection is None:
                messagebox.showinfo(
                    "Update Cancelled",
                    "The update installation was cancelled before copying any files.",
                    parent=self,
                )
                return
            if not selection:
                messagebox.showinfo(
                    "Update Cancelled",
                    "No files were selected for installation. The update was not applied.",
                    parent=self,
                )
                return
            selected_entries = selection
        try:
            new_install_path, copied = self._install_update_package(target_path, selected_entries)
        except zipfile.BadZipFile as exc:
            self.log(f"Downloaded update is not a valid ZIP archive: {exc}", level="error")
            messagebox.showerror(
                "Update Installation Failed",
                "The downloaded update could not be installed because it is not a valid ZIP archive.",
                parent=self,
            )
            return
        except Exception as exc:
            self.log(f"Failed to install downloaded update: {exc}", level="error")
            messagebox.showerror(
                "Update Installation Failed",
                f"Unable to install the downloaded update: {exc}",
                parent=self,
            )
            return

        summary = (
            "The update package was downloaded and installed successfully.\n"
            f"Copied {copied} file{'s' if copied != 1 else ''} into the new installation.\n"
            "The new version will launch automatically and this window will close."
        )
        messagebox.showinfo("Update Installed", summary, parent=self)

        if self._launch_new_installation(new_install_path):
            self._schedule_update_cleanup(Path(__file__).resolve().parent, new_install_path)
            self.after(500, self._shutdown_after_update)
        else:
            messagebox.showwarning(
                "Update Installed",
                (
                    "The update was installed but the new version could not be launched automatically.\n"
                    "Please start the new installation manually."
                ),
                parent=self,
            )

    def _install_update_package(
        self, package_path: Path, selected_entries: Optional[Set[PurePosixPath]] = None
    ) -> Tuple[Path, int]:
        self.log(f"Installing update from {package_path}")
        app_root = Path(__file__).resolve().parent

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            with zipfile.ZipFile(package_path, "r") as archive:
                archive.extractall(temp_path)

            extracted_root = self._resolve_update_root(temp_path)
            new_install_path, copied = self._prepare_new_installation(
                extracted_root, app_root, selected_entries
            )

        return new_install_path, copied

    def _resolve_update_root(self, extracted_root: Path) -> Path:
        candidates = [p for p in extracted_root.iterdir() if p.name != "__MACOSX"]
        if len(candidates) == 1 and candidates[0].is_dir():
            return candidates[0]
        return extracted_root

    def _prepare_new_installation(
        self,
        source: Path,
        current_root: Path,
        selected_entries: Optional[Set[PurePosixPath]] = None,
    ) -> Tuple[Path, int]:
        parent = current_root.parent
        base_name = source.name or current_root.name
        destination = self._next_installation_path(parent, base_name, current_root)
        destination.mkdir(parents=True, exist_ok=False)
        copied = self._copy_update_contents(source, destination, selected_entries)
        preserve = self._identify_preserve_entries(current_root)
        self._copy_preserved_entries(current_root, destination, preserve)
        return destination, copied

    def _next_installation_path(self, parent: Path, base_name: str, current_root: Path) -> Path:
        candidate = parent / base_name
        if candidate.resolve() == current_root:
            base_name = f"{base_name}_new"
            candidate = parent / base_name
        index = 1
        while candidate.exists():
            candidate = parent / f"{base_name}_{index}"
            index += 1
        return candidate

    def _copy_update_contents(
        self, source: Path, destination: Path, selected_entries: Optional[Set[PurePosixPath]] = None
    ) -> int:
        replaced = 0
        if not source.exists():
            raise FileNotFoundError("Extracted update does not contain any files")

        allowed: Optional[Set[PurePosixPath]] = None
        if selected_entries is not None:
            allowed = {PurePosixPath(str(entry)) for entry in selected_entries if str(entry)}

        for path in source.rglob("*"):
            if any(part == "__MACOSX" for part in path.parts):
                continue
            relative = path.relative_to(source)
            relative_posix = PurePosixPath(relative.as_posix())
            if allowed is not None and path.is_file() and relative_posix not in allowed:
                continue
            target = destination / relative
            if path.is_dir():
                if allowed is not None:
                    has_descendant = any(
                        relative_posix == entry or relative_posix in entry.parents
                        for entry in allowed
                    )
                    if not has_descendant:
                        continue
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, target)
                replaced += 1

        return replaced

    def _prompt_advanced_file_selection(
        self, package_path: Path
    ) -> Optional[Set[PurePosixPath]]:
        try:
            with zipfile.ZipFile(package_path, "r") as archive:
                entries = [
                    PurePosixPath(*[part for part in PurePosixPath(info.filename).parts if part])
                    for info in archive.infolist()
                    if not info.is_dir()
                ]
        except Exception as exc:
            messagebox.showerror(
                "Advanced Download",
                f"Unable to inspect the update package: {exc}",
                parent=self,
            )
            return None

        filtered_entries: List[PurePosixPath] = []
        for entry in entries:
            if not entry.parts:
                continue
            if entry.parts[0] == "__MACOSX":
                continue
            filtered_entries.append(entry)

        if not filtered_entries:
            messagebox.showwarning(
                "Advanced Download",
                "The update package did not contain any installable files.",
                parent=self,
            )
            return set()

        top_levels = {entry.parts[0] for entry in filtered_entries if entry.parts}
        if len(top_levels) == 1 and all(len(entry.parts) > 1 for entry in filtered_entries):
            trimmed_entries = [PurePosixPath(*entry.parts[1:]) for entry in filtered_entries]
        else:
            trimmed_entries = filtered_entries

        display_entries = [entry for entry in trimmed_entries if entry.parts]
        if not display_entries:
            display_entries = trimmed_entries

        sorted_entries = sorted(display_entries, key=lambda p: p.as_posix().lower())

        dialog = tk.Toplevel(self)
        dialog.title("Select Update Files")
        dialog.transient(self)
        dialog.resizable(True, True)
        dialog.grab_set()
        dialog.configure(padx=16, pady=16)

        info_label = ttk.Label(
            dialog,
            text=(
                "Choose the files to install from this update. Only the selected files will be copied "
                "into the new installation."
            ),
            wraplength=420,
            justify="left",
        )
        info_label.grid(row=0, column=0, columnspan=3, sticky="we", pady=(0, 12))

        listbox = tk.Listbox(dialog, selectmode=tk.MULTIPLE, exportselection=False)
        scrollbar = ttk.Scrollbar(dialog, orient="vertical", command=listbox.yview)
        listbox.configure(yscrollcommand=scrollbar.set)
        for entry in sorted_entries:
            listbox.insert(tk.END, entry.as_posix())
        listbox.grid(row=1, column=0, sticky="nsew")
        scrollbar.grid(row=1, column=1, sticky="ns")

        button_frame = ttk.Frame(dialog)
        button_frame.grid(row=1, column=2, sticky="ns", padx=(12, 0))

        selected_state = {"cancelled": True, "values": set()}

        def confirm_selection() -> None:
            indices = tuple(map(int, listbox.curselection()))
            if not indices:
                if not messagebox.askyesno(
                    "No Files Selected",
                    "No files are selected. This will skip installing the update. Continue?",
                    parent=dialog,
                ):
                    return
            selected_state["cancelled"] = False
            selected_state["values"] = {sorted_entries[i] for i in indices}
            dialog.destroy()

        def cancel_selection() -> None:
            selected_state["cancelled"] = True
            dialog.destroy()

        def select_all() -> None:
            listbox.select_set(0, tk.END)

        def select_none() -> None:
            listbox.select_clear(0, tk.END)

        all_button = ttk.Button(button_frame, text="Select All", command=select_all)
        all_button.pack(fill="x", pady=(0, 6))
        none_button = ttk.Button(button_frame, text="Select None", command=select_none)
        none_button.pack(fill="x", pady=(0, 6))
        confirm_button = ttk.Button(button_frame, text="Install Selected", command=confirm_selection)
        confirm_button.pack(fill="x", pady=(0, 6))
        cancel_button = ttk.Button(button_frame, text="Cancel", command=cancel_selection)
        cancel_button.pack(fill="x")

        dialog.columnconfigure(0, weight=1)
        dialog.rowconfigure(1, weight=1)

        dialog.protocol("WM_DELETE_WINDOW", cancel_selection)

        center_window(dialog)
        self.wait_window(dialog)

        if selected_state["cancelled"]:
            return None
        return set(selected_state["values"])

    def _launch_new_installation(self, new_install_path: Path) -> bool:
        current_root = Path(__file__).resolve().parent
        launcher_path = Path(sys.argv[0]).resolve()
        candidates: List[Path] = []
        try:
            rel_launcher = launcher_path.relative_to(current_root)
        except Exception:
            rel_launcher = None
        if rel_launcher:
            candidate = new_install_path / rel_launcher
            if candidate.exists():
                candidates.append(candidate)

        fallback_names = [launcher_path.name, "Sims4ModSorter.exe", "Sims4ModSorter.py"]
        seen: set[Path] = set()
        for candidate in candidates:
            seen.add(candidate.resolve())
        for name in fallback_names:
            if not name:
                continue
            candidate = new_install_path / name
            if candidate.exists():
                resolved = candidate.resolve()
                if resolved not in seen:
                    candidates.append(candidate)
                    seen.add(resolved)

        for candidate in candidates:
            try:
                if candidate.is_dir():
                    continue
                suffix = candidate.suffix.lower()
                if suffix in {".py", ".pyw"}:
                    python = sys.executable
                    if python and Path(python).exists():
                        subprocess.Popen([python, str(candidate)], cwd=str(candidate.parent))
                        return True
                if os.access(candidate, os.X_OK):
                    subprocess.Popen([str(candidate)], cwd=str(candidate.parent))
                    return True
                self._open_path(candidate)
                return True
            except Exception:
                continue

        try:
            self._open_path(new_install_path)
            return True
        except Exception:
            return False

    @staticmethod
    def _identify_preserve_entries(root: Path) -> List[Path]:
        preserve: List[Path] = []
        try:
            for child in root.iterdir():
                name_lower = child.name.lower()
                if child.name == "user_plugins" or "log" in name_lower or "setting" in name_lower:
                    preserve.append(child)
        except Exception:
            return preserve
        return preserve

    @staticmethod
    def _copy_preserved_entries(
        old_root: Path, new_root: Path, entries: Iterable[Path], *, overwrite: bool = True
    ) -> None:
        def _merge(source: Path, target: Path) -> None:
            try:
                if source.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    for child in source.iterdir():
                        _merge(child, target / child.name)
                elif source.exists():
                    if not overwrite and target.exists():
                        return
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, target)
            except Exception:
                return

        for entry in entries:
            try:
                relative = entry.relative_to(old_root)
            except ValueError:
                continue
            destination = new_root / relative
            if not overwrite and entry.is_file() and destination.exists():
                continue
            _merge(entry, destination)

    def _schedule_update_cleanup(self, old_root: Path, new_root: Path) -> None:
        if not self._start_cleanup_process(old_root, new_root):
            cleanup_thread = threading.Thread(
                target=self._cleanup_old_installation,
                args=(old_root, new_root),
                name="UpdateCleanup",
            )
            cleanup_thread.start()

    def _start_cleanup_process(self, old_root: Path, new_root: Path) -> bool:
        python = sys.executable
        if not python or not Path(python).exists():
            return False
        cleanup_code = """
import pathlib, shutil, sys, time
old = pathlib.Path(sys.argv[1])
new = pathlib.Path(sys.argv[2])
time.sleep(2)

def preserve(root: pathlib.Path) -> list[pathlib.Path]:
    out: list[pathlib.Path] = []
    try:
        for child in root.iterdir():
            name = child.name.lower()
            if child.name == 'user_plugins' or 'log' in name or 'setting' in name:
                out.append(child)
    except Exception:
        return out
    return out

def copy_entries(entries: list[pathlib.Path]) -> None:
    def merge(source: pathlib.Path, target: pathlib.Path) -> None:
        try:
            if source.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                for child in source.iterdir():
                    merge(child, target / child.name)
            elif source.exists():
                if target.exists():
                    return
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
        except Exception:
            pass

    for entry in entries:
        try:
            rel = entry.relative_to(old)
        except Exception:
            continue
        dest = new / rel
        merge(entry, dest)

for _ in range(10):
    try:
        if old.exists():
            copy_entries(preserve(old))
            shutil.rmtree(old)
        break
    except Exception:
        time.sleep(1)
""".strip()
        try:
            subprocess.Popen(
                [python, "-c", cleanup_code, str(old_root), str(new_root)],
                cwd=str(new_root),
            )
            return True
        except Exception:
            return False

    @staticmethod
    def _cleanup_old_installation(old_root: Path, new_root: Path) -> None:
        try:
            time.sleep(1.0)
            preserve = Sims4ModSorterApp._identify_preserve_entries(old_root)
            Sims4ModSorterApp._copy_preserved_entries(
                old_root, new_root, preserve, overwrite=False
            )
            shutil.rmtree(old_root, ignore_errors=True)
        except Exception:
            pass

    def _shutdown_after_update(self) -> None:
        try:
            self.quit()
        except Exception:
            pass
        try:
            self.destroy()
        except Exception:
            pass

    def _handle_update_download_failure(
        self,
        target_path: Path,
        error: BaseException,
        mode: Literal["auto-install", "manual-download"],
    ) -> None:
        if target_path.exists():
            try:
                target_path.unlink()
            except Exception:
                pass
        self._hide_update_overlay()
        reason = error
        if isinstance(error, urllib.error.URLError) and hasattr(error, "reason"):
            reason = error.reason  # type: ignore[assignment]
        messagebox.showerror("Update Download Failed", f"Unable to download update: {reason}", parent=self)
        self.log(f"Update download failed: {error}", level="error")
        if mode == "manual-download" and self._update_release_page_url and messagebox.askyesno(
            "Update Download Failed",
            "Would you like to open the release page instead?",
            parent=self,
        ):
            self._open_release_page()

    def _open_release_page(self) -> None:
        url = self._update_release_page_url or self._update_download_url
        if not url:
            messagebox.showinfo("Update Available", "Release information is not configured.", parent=self)
            return
        try:
            webbrowser.open(url)
        except Exception as exc:
            messagebox.showerror("Update Available", f"Unable to open release page: {exc}", parent=self)

    def _open_path(self, path: Path) -> None:
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.run(["open", str(path)], check=False)
            else:
                subprocess.run(["xdg-open", str(path)], check=False)
        except Exception as exc:
            self.log(f"Open failed: {exc}")

    def _check_updates_on_launch(self) -> None:
        self._start_update_check(manual=False, from_settings=False)

    def _on_manual_update_check(self) -> None:
        self._start_update_check(manual=True, from_settings=True)

    def _start_update_check(self, *, manual: bool, from_settings: bool = False) -> None:
        if self._update_check_in_progress:
            if manual:
                messagebox.showinfo("Update Check", "An update check is already running.", parent=self)
            return
        self._update_check_in_progress = True
        button = getattr(self, "check_updates_button", None)
        if manual and button and button.winfo_exists():
            button.configure(state="disabled")
        if not manual:
            self._show_update_overlay(
                "Checking for updates…",
                headline="Checking for updates",
                progress=True,
                enable_download=False,
                enable_skip=False,
                enable_details=False,
                progress_title="Status",
                progress_subtext="Contacting update service…",
                status_icon="🔄",
            )

        def worker() -> None:
            error_message: Optional[str] = None
            result: Optional[UpdateResult]
            try:
                result = check_for_update("app", APP_VERSION)
            except Exception as exc:  # pragma: no cover - defensive
                error_message = f"Update check failed: {exc}"
                result = None
            self._enqueue_ui(
                lambda r=result, m=manual, err=error_message, origin=from_settings: self._complete_update_check(
                    r, m, err, origin
                )
            )

        threading.Thread(target=worker, daemon=True).start()

    def _complete_update_check(
        self,
        result: Optional[UpdateResult],
        manual: bool,
        error_message: Optional[str],
        from_settings: bool,
    ) -> None:
        self._update_check_in_progress = False
        button = getattr(self, "check_updates_button", None)
        if button and button.winfo_exists():
            button.configure(state="normal")

        if error_message:
            self._update_download_url = None
            self._update_release_page_url = None
            self._update_download_filename = None
            if manual:
                messagebox.showerror("Update Check", error_message, parent=self)
            else:
                self.log(error_message, level="error")
                self._show_update_overlay(
                    error_message,
                    headline="Update check failed",
                    progress=False,
                    enable_download=False,
                    enable_skip=True,
                    enable_details=False,
                    status_icon="⚠️",
                )
            return

        if not result:
            self._update_download_url = None
            self._update_release_page_url = None
            self._update_download_filename = None
            if not manual:
                self._hide_update_overlay()
            return

        if result.message:
            self._update_download_url = None
            self._update_release_page_url = None
            self._update_download_filename = None
            if manual:
                messagebox.showerror("Update Check", result.message, parent=self)
            else:
                self.log(result.message, level="warn")
                self._show_update_overlay(
                    result.message,
                    headline="Update service message",
                    progress=False,
                    enable_download=False,
                    enable_skip=True,
                    enable_details=False,
                    status_icon="⚠️",
                )
            return

        if result.download_url:
            self._update_download_url = result.download_url
        else:
            self._update_download_url = None
        self._update_release_page_url = result.release_page_url
        self._update_download_filename = result.asset_name

        update_available = bool(result.is_newer and result.latest_version)
        self._update_available = update_available
        if result.latest_version:
            self._latest_version = result.latest_version
        elif not update_available:
            self._latest_version = None
        self._refresh_version_display()

        if update_available and result.latest_version:
            self.log(f"Update available: {result.latest_version}")
            base_message = (
                f"Version {result.latest_version} is available (current version is {APP_VERSION})."
            )
            download_available = bool(self._update_download_url)
            release_available = bool(self._update_release_page_url)
            if download_available:
                message = (
                    base_message
                    + "\nUse 'Auto Update' to download and install automatically or 'Manual Download' to save the update package yourself."
                )
            elif release_available:
                message = (
                    base_message
                    + "\nUse 'Manual Download' or 'View Release' to open the release page, or 'Skip for Now' to continue."
                )
            else:
                message = (
                    base_message
                    + "\nDownload information is not configured for this release."
                )
            status_icon = "✨" if download_available else "ℹ️"
            if manual and from_settings:
                self._show_update_overlay(
                    message,
                    headline=f"Version {result.latest_version} available",
                    progress=False,
                    enable_download=download_available,
                    enable_skip=True,
                    enable_details=release_available,
                    enable_manual=download_available or release_available,
                    status_icon=status_icon,
                    origin="settings",
                )
            elif manual:
                if download_available:
                    prompt = base_message + "\nWould you like to download the update now?"
                    if messagebox.askyesno("Update Available", prompt, parent=self):
                        self._start_update_download(mode="auto-install")
                    elif self._update_release_page_url and messagebox.askyesno(
                        "Update Available",
                        "Would you like to open the release page instead?",
                        parent=self,
                    ):
                        self._open_release_page()
                elif release_available:
                    info = (
                        base_message
                        + "\nDownload information is not configured. Would you like to view the release page?"
                    )
                    if messagebox.askyesno("Update Available", info, parent=self):
                        self._open_release_page()
                    else:
                        messagebox.showinfo(
                            "Update Available",
                            "Download information is not configured for this release.",
                            parent=self,
                        )
                else:
                    messagebox.showinfo(
                        "Update Available",
                        "Download information is not configured for this release.",
                        parent=self,
                    )
            else:
                self._show_update_overlay(
                    message,
                    headline=f"Version {result.latest_version} available",
                    progress=False,
                    enable_download=download_available,
                    enable_skip=True,
                    enable_details=release_available,
                    enable_manual=download_available or release_available,
                    status_icon=status_icon,
                    origin="general",
                )
        else:
            self._update_download_url = None
            self._update_release_page_url = None
            self._update_download_filename = None
            if manual:
                messagebox.showinfo(
                    "Update Check",
                    f"You are using the latest version ({APP_VERSION}).",
                    parent=self,
                )
            else:
                self._hide_update_overlay()

    # ------------------------------------------------------------------
    # Settings overlay
    # ------------------------------------------------------------------
    def show_settings(self) -> None:
        if not hasattr(self, "settings_sidebar"):
            return
        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        scrim = getattr(self, "settings_scrim", None)
        if scrim:
            scrim.configure(bg=_scrim_color(palette.get("bg", "#111316")))
            scrim.place(relx=0, rely=0, relwidth=1, relheight=1)
            scrim.tkraise()
        self.settings_sidebar.configure(bg=palette.get("sel", "#2A2F3A"))
        self.settings_sidebar.place(
            relx=1.0,
            rely=0,
            relheight=1.0,
            anchor="ne",
            width=getattr(self, "_settings_sidebar_width", 360),
        )
        self.settings_sidebar.tkraise()
        self.settings_sidebar.focus_set()
        self._refresh_version_display()
        self._update_theme_preview_highlight()

    def hide_settings(self) -> None:
        if hasattr(self, "settings_sidebar"):
            self.settings_sidebar.place_forget()
        scrim = getattr(self, "settings_scrim", None)
        if scrim:
            scrim.place_forget()

    # ------------------------------------------------------------------
    # Plugin status popup
    # ------------------------------------------------------------------
    def show_mod_status_popup(self) -> None:
        if not self.plugin_manager:
            messagebox.showinfo("Plugin Status", "No plugins loaded.", parent=self)
            return
        if self._mod_status_window and self._mod_status_window.winfo_exists():
            self._populate_mod_status_popup()
            self._mod_status_window.deiconify()
            self._mod_status_window.lift()
            return
        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        window = tk.Toplevel(self)
        window.title("Plugin Status")
        window.transient(self)
        window.resizable(False, True)
        window.configure(bg=palette.get("bg", "#111316"))
        window.protocol("WM_DELETE_WINDOW", self._close_mod_status_popup)
        self._mod_status_window = window

        container = ttk.Frame(window, padding=16)
        container.pack(fill="both", expand=True)

        notebook = ttk.Notebook(container)
        notebook.pack(fill="both", expand=True)

        loaded_frame = ttk.Frame(notebook)
        blocked_frame = ttk.Frame(notebook)
        notebook.add(loaded_frame, text="Loaded")
        notebook.add(blocked_frame, text="Blocked")

        def create_tree(parent: ttk.Frame) -> ttk.Treeview:
            frame = ttk.Frame(parent)
            frame.pack(fill="both", expand=True)
            tree = ttk.Treeview(frame, columns=("name", "folder", "version", "status", "message"), show="headings", height=8)
            tree.heading("name", text="Name")
            tree.heading("folder", text="Folder")
            tree.heading("version", text="Version")
            tree.heading("status", text="Status")
            tree.heading("message", text="Details")
            tree.column("name", width=220, anchor="w")
            tree.column("folder", width=120, anchor="w")
            tree.column("version", width=90, anchor="center")
            tree.column("status", width=100, anchor="center")
            tree.column("message", width=260, anchor="w")
            tree.pack(side="left", fill="both", expand=True)
            scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
            tree.configure(yscrollcommand=scroll.set)
            scroll.pack(side="right", fill="y")
            return tree

        loaded_tree = create_tree(loaded_frame)
        blocked_tree = create_tree(blocked_frame)
        self._status_trees = {"loaded": loaded_tree, "blocked": blocked_tree}

        summary = ttk.Label(container, textvariable=self._status_summary_var)
        summary.pack(anchor="w", pady=(12, 0))

        self._populate_mod_status_popup()

    def _close_mod_status_popup(self) -> None:
        if self._mod_status_window and self._mod_status_window.winfo_exists():
            self._mod_status_window.destroy()
        self._mod_status_window = None
        self._status_trees = {}

    def _populate_mod_status_popup(self) -> None:
        if not self.plugin_manager:
            return
        statuses = self.plugin_manager.get_statuses()
        loaded = [status for status in statuses if status.status == "loaded"]
        blocked = [status for status in statuses if status.status != "loaded"]
        for key, entries in (("loaded", loaded), ("blocked", blocked)):
            tree = self._status_trees.get(key)
            if not tree or not tree.winfo_exists():
                continue
            tree.delete(*tree.get_children())
            if not entries:
                tree.insert("", "end", values=("No entries", "", "", "", ""))
                continue
            for status in entries:
                tree.insert(
                    "",
                    "end",
                    values=(
                        status.name,
                        status.folder,
                        status.version,
                        status.status.capitalize(),
                        status.message,
                    ),
                )
        self._status_summary_var.set(f"Loaded: {len(loaded)} | Blocked: {len(blocked)}")

    def on_apply_theme(self) -> None:
        self._hide_tooltip()
        self._build_style()
        palette = self._theme_cache
        self.log_text.configure(bg=palette.get("alt", "#1f2328"), fg=palette.get("fg", "#E6E6E6"))
        self._configure_log_tags()
        sidebar_canvas = getattr(self, "sidebar_canvas", None)
        if sidebar_canvas and sidebar_canvas.winfo_exists():
            sidebar_canvas.configure(bg=palette.get("alt", "#1f2328"))
        if hasattr(self, "settings_sidebar"):
            self.settings_sidebar.configure(bg=palette.get("sel", "#2A2F3A"))
        scrim = getattr(self, "settings_scrim", None)
        if scrim:
            scrim.configure(bg=_scrim_color(palette.get("bg", "#111316")))
        self._refresh_update_overlay_theme()
        self._update_theme_preview_highlight()
        self.log(f"Theme applied: {self.theme_name.get()}")
    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------
    def on_browse(self) -> None:
        path = filedialog.askdirectory(initialdir=self.mods_root.get(), title="Select Mods folder")
        if path:
            self.mods_root.set(path)

    def on_scan(self) -> None:
        mods_path = Path(self.mods_root.get())
        if not mods_path.exists():
            self.status_var.set("Folder not found")
            self.log("Error: folder not found")
            return
        allowed_exts_preview = self._resolve_allowed_extensions()
        if allowed_exts_preview is not None and not allowed_exts_preview:
            messagebox.showwarning("Scan", "Enable at least one file type before scanning.", parent=self)
            return
        include_adult_flag = self.include_adult_var.get()
        selected_folders_preview: Optional[List[str]] = None
        self.btn_scan.configure(state="disabled")
        self.progress.configure(mode="determinate", value=0, maximum=100)
        self.status_var.set("Scanning…")
        self.items.clear()
        self.items_by_path.clear()
        self.scan_errors.clear()

        try:
            ignore_exts = [part.strip() for part in self.ignore_exts_var.get().split(",") if part.strip()]
            ignore_names = [part.strip() for part in self.ignore_names_var.get().split(",") if part.strip()]
        except Exception:
            ignore_exts, ignore_names = [], []

        context: Dict[str, object] = {
            "mods_root": str(mods_path),
            "recurse": self.recurse_var.get(),
            "ignore_exts": list(ignore_exts),
            "ignore_names": list(ignore_names),
            "folder_map": self.folder_map,
            "allowed_exts": allowed_exts_preview,
            "selected_folders": selected_folders_preview,
            "include_adult": include_adult_flag,
        }
        if self.plugin_manager:
            self.plugin_manager.run_pre_scan(context)
            ignore_exts = [str(ext).strip() for ext in context.get("ignore_exts", ignore_exts) if str(ext).strip()]
            ignore_names = [str(name).strip() for name in context.get("ignore_names", ignore_names) if str(name).strip()]
            mods_path = Path(context.get("mods_root", mods_path))
        recurse = bool(context.get("recurse", self.recurse_var.get()))

        def _normalise_allowed_exts(value: object) -> Optional[set[str]]:
            if value is None:
                return None
            if isinstance(value, (list, set, tuple)):
                normalized = {
                    (str(ext).lower() if str(ext).startswith(".") else f".{str(ext).lower()}")
                    for ext in value
                    if str(ext).strip()
                }
                return normalized
            text = str(value).strip()
            if not text:
                return set()
            return {
                text.lower() if text.startswith(".") else f".{text.lower()}"
            }

        def _normalise_selected_folders(value: object) -> Optional[List[str]]:
            if value is None:
                return None
            if isinstance(value, (list, tuple, set)):
                result = [str(folder).strip() for folder in value if str(folder).strip()]
                return result or []
            text = str(value).strip()
            return [text] if text else []

        allowed_exts_value = _normalise_allowed_exts(context.get("allowed_exts", allowed_exts_preview))
        selected_folders_value = _normalise_selected_folders(context.get("selected_folders", selected_folders_preview))
        include_adult_flag = bool(context.get("include_adult", include_adult_flag))
        if allowed_exts_value is not None and not allowed_exts_value:
            self.log("Scan cancelled: no file types allowed after plugin adjustments.", level="warning")
            self.status_var.set("Scan cancelled")
            self.btn_scan.configure(state="normal")
            return
        if selected_folders_value is not None and not selected_folders_value:
            self.log("Scan cancelled: no folders selected after plugin adjustments.", level="warning")
            self.status_var.set("Scan cancelled")
            self.btn_scan.configure(state="normal")
            return

        folder_desc = "All folders" if not selected_folders_value else ", ".join(
            "Root files" if folder in (".", "") else folder for folder in selected_folders_value
        )
        type_desc = (
            "all file types"
            if allowed_exts_value is None
            else ", ".join(sorted(allowed_exts_value)) or "(none)"
        )
        self.log(
            f"Starting scan in {folder_desc} ({'including' if include_adult_flag else 'excluding'} adult content, {type_desc})."
        )

        def progress_cb(done: int, total: int, current: Path, state: str) -> None:
            percent = int(done * 100 / total) if total else 0
            display = current.name
            self._enqueue_ui(lambda: self._update_progress(percent, done, total, display, state))

        def worker() -> None:
            result = scan_folder(
                mods_path,
                folder_map=self.folder_map,
                recurse=recurse,
                ignore_exts=ignore_exts,
                ignore_names=ignore_names,
                progress_cb=progress_cb,
                selected_folders=selected_folders_value,
                include_adult=include_adult_flag,
                allowed_exts=allowed_exts_value,
            )
            stats = bundle_scripts_and_packages(result.items, self.folder_map)
            if self.plugin_manager:
                post_context = {
                    "mods_root": str(mods_path),
                    "recurse": recurse,
                    "ignore_exts": ignore_exts,
                    "ignore_names": ignore_names,
                    "items": result.items,
                    "bundle_stats": stats,
                    "allowed_exts": allowed_exts_value,
                    "selected_folders": selected_folders_value,
                    "include_adult": include_adult_flag,
                }
                self.plugin_manager.run_post_scan(result.items, post_context)
            result.items.sort(
                key=lambda item: (
                    CATEGORY_INDEX.get(item.guess_type, len(CATEGORY_ORDER)),
                    _natural_key(os.path.dirname(item.relpath) or "."),
                    _natural_key(item.name),
                )
            )
            self._enqueue_ui(lambda: self._apply_scan_result(result, stats))

        threading.Thread(target=worker, daemon=True).start()

    def _update_progress(self, percent: int, done: int, total: int, name: str, state: str) -> None:
        self.progress.configure(value=percent, maximum=100)
        self.status_var.set(f"Scanning {done}/{total}: {name}" if total else "Scanning…")
        if state == "error":
            self.log(f"Scan error: {name}")

    def _apply_scan_result(self, result: ScanResult, stats: Dict[str, int]) -> None:
        self.btn_scan.configure(state="normal")
        self.disabled_items = list(result.disabled_items)
        combined_items = list(result.items)
        if self.disabled_items:
            combined_items.extend(self.disabled_items)
        self.items = combined_items
        self.items_by_path = {str(item.path): item for item in self.items}
        self.scan_errors = result.errors
        self._apply_loadout_to_items()
        self._refresh_tree()
        self.status_var.set(f"Plan: {len(self.items)} files")
        self.log(
            f"Scan complete. Planned {len(self.items)} files. Linked packages: {stats['linked']} across {stats['scripts']} script(s)."
        )
        if self.disabled_items:
            self.log(
                f"Found {len(self.disabled_items)} disabled mod(s); they remain excluded from moves unless re-enabled.",
                level="warning",
            )
        for error in result.errors:
            self.log(f"Scan warning: {error}")
        self._report_mod_runtime_messages()
    def on_select(self, _event: Optional[tk.Event] = None) -> None:
        selection = self.tree.selection()
        if not selection:
            self.sel_label.configure(text="None selected")
            return
        iid = selection[0]
        item = self.items_by_path.get(iid)
        if not item:
            self.sel_label.configure(text="None selected")
            return
        self.sel_label.configure(text=pretty_display_name(item.name))
        if item.guess_type in CATEGORY_ORDER:
            self.type_cb.set(item.guess_type)
        else:
            self.type_cb.set("Unknown")
        self.target_entry.delete(0, tk.END)
        self.target_entry.insert(0, item.target_folder)

    def on_double_click(self, event: tk.Event) -> None:
        iid = self.tree.identify_row(event.y)
        if not iid:
            return
        item = self.items_by_path.get(iid)
        if not item:
            return
        path = Path(iid)
        try:
            if sys.platform.startswith("win"):
                os.startfile(path)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.run(["open", str(path)], check=False)
            else:
                subprocess.run(["xdg-open", str(path)], check=False)
        except Exception as exc:
            self.log(f"Open failed: {exc}")

    def on_apply_selected(self) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        new_type = self.type_cb.get() or "Unknown"
        new_target = self.target_entry.get().strip() or self.folder_map.get(new_type, "Unsorted")
        for iid in selection:
            item = self.items_by_path.get(iid)
            if not item:
                continue
            item.guess_type = new_type
            item.target_folder = new_target
        self._refresh_tree(preserve_selection=True)

    def on_toggle_include(self) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        changed = False
        for iid in selection:
            item = self.items_by_path.get(iid)
            if item:
                item.include = not item.include
                changed = True
        if not changed:
            return
        self._record_loadout(save=True)
        self._refresh_tree(preserve_selection=True)

    def on_batch_assign(self) -> None:
        keyword = self.batch_keyword.get().strip().lower()
        if not keyword:
            return
        new_type = self.type_cb.get() or "Unknown"
        count = 0
        for item in self.items:
            if keyword in item.name.lower():
                item.guess_type = new_type
                item.target_folder = self.folder_map.get(new_type, "Unsorted")
                count += 1
        self._refresh_tree()
        self.log(f"Assigned {new_type} to {count} file(s) containing '{keyword}'.")

    def on_recalc_targets(self) -> None:
        for item in self.items:
            item.target_folder = self.folder_map.get(item.guess_type, "Unsorted")
        bundle_scripts_and_packages(self.items, self.folder_map)
        self._refresh_tree(preserve_selection=True)
    def on_complete(self) -> None:
        plan = [item for item in self.items if item.include]
        if not plan:
            self.log("No files selected to move.")
            return
        mods_root = Path(self.mods_root.get())
        self.progress.configure(mode="determinate", value=0, maximum=len(plan))
        self.status_var.set("Moving files…")

        def worker() -> None:
            moved_total = 0
            skipped_total = 0
            collisions: List[Tuple[Path, Path, str]] = []
            moves_log: List[Dict[str, str]] = []
            for index, item in enumerate(plan, start=1):
                moved, skipped, collides, moves = perform_moves([item], mods_root)
                moved_total += moved
                skipped_total += skipped
                collisions.extend(collides)
                moves_log.extend(moves)
                self._enqueue_ui(lambda idx=index: self._update_move_progress(idx, len(plan)))
            save_moves_log(mods_root, moves_log)
            self._enqueue_ui(lambda: self._complete_moves(moved_total, skipped_total, collisions))

        threading.Thread(target=worker, daemon=True).start()

    def _update_move_progress(self, index: int, total: int) -> None:
        self.progress.configure(value=index, maximum=max(total, 1))
        self.status_var.set(f"Moving {index}/{total}")

    def _complete_moves(self, moved: int, skipped: int, collisions: List[Tuple[Path, Path, str]]) -> None:
        self.status_var.set("Move complete")
        self.log(f"Move complete. Moved {moved}, Skipped {skipped}, Issues {len(collisions)}")
        for src, dst, reason in collisions[:50]:
            self.log(f"Collision: {src.name} -> {dst.parent} ({reason})")
        self.on_scan()

    def on_undo(self) -> None:
        mods_root = Path(self.mods_root.get())
        self.status_var.set("Undoing moves…")

        def worker() -> None:
            undone, failed, errors, label = undo_last_moves(mods_root)
            self._enqueue_ui(lambda: self._handle_undo_result(undone, failed, errors, label))

        threading.Thread(target=worker, daemon=True).start()

    def _handle_undo_result(
        self, undone: int, failed: int, errors: List[str], label: Optional[str]
    ) -> None:
        context = label or "latest batch"
        status_message = f"Undo ({context}): {undone} restored, {failed} failed"
        self.status_var.set(status_message)
        self.summary_var.set(status_message)
        self.log(status_message)
        for error in errors[:50]:
            self.log(error)
        if failed:
            error_body = "\n".join(errors[:10]) if errors else "Unknown error"
            messagebox.showerror(
                "Undo Issues",
                f"Unable to restore {failed} file(s).\n{error_body}",
                parent=self,
            )
        self.on_scan()

    def show_move_history(self) -> None:
        mods_root = Path(self.mods_root.get())
        entries = load_move_history(mods_root)
        if not entries and (
            not self._history_window or not self._history_window.winfo_exists()
        ):
            messagebox.showinfo(
                "Move History",
                "No move batches have been recorded yet.",
                parent=self,
            )
            return
        if not self._history_window or not self._history_window.winfo_exists():
            window = tk.Toplevel(self)
            window.title("Move History")
            window.transient(self)
            window.grab_set()
            container = ttk.Frame(window, padding=12)
            container.pack(fill="both", expand=True)
            tree = ttk.Treeview(
                container,
                columns=("timestamp", "moved", "samples"),
                show="headings",
                selectmode="browse",
            )
            tree.heading("timestamp", text="Timestamp")
            tree.heading("moved", text="Files")
            tree.heading("samples", text="Sample Destinations")
            tree.column("timestamp", width=190, anchor="w")
            tree.column("moved", width=60, anchor="center")
            tree.column("samples", width=420, anchor="w")
            tree.pack(fill="both", expand=True)
            tree.bind("<<TreeviewSelect>>", lambda *_: self._update_history_button_states())
            self._history_tree = tree
            button_row = ttk.Frame(container)
            button_row.pack(fill="x", pady=(10, 0))
            preview_btn = ttk.Button(
                button_row, text="Preview", command=self._preview_history_selection
            )
            undo_btn = ttk.Button(
                button_row, text="Undo Selected", command=self._undo_history_selection
            )
            close_btn = ttk.Button(button_row, text="Close", command=self._close_history_window)
            preview_btn.pack(side="left")
            undo_btn.pack(side="left", padx=(8, 0))
            close_btn.pack(side="right")
            self._history_preview_btn = preview_btn
            self._history_undo_btn = undo_btn
            self._history_window = window
            window.protocol("WM_DELETE_WINDOW", self._close_history_window)
            window.bind("<Destroy>", self._on_history_destroy)
            self._update_history_button_states()
            center_window(window)
        else:
            try:
                self._history_window.deiconify()
                self._history_window.lift()
                self._history_window.focus_set()
            except Exception:
                pass
        self._refresh_move_history(entries)

    def _refresh_move_history(self, entries: Optional[List[Dict[str, object]]] = None) -> None:
        if not self._history_tree or not self._history_tree.winfo_exists():
            return
        if entries is None:
            entries = load_move_history(Path(self.mods_root.get()))
        tree = self._history_tree
        tree.delete(*tree.get_children())
        self._history_entries = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            row_id = entry.get("id")
            if not isinstance(row_id, str):
                row_id = str(row_id)
            entry["row_id"] = row_id
            self._history_entries[row_id] = entry
            samples = entry.get("sample_display") or []
            sample_text = ", ".join(samples[:5])
            tree.insert(
                "",
                "end",
                iid=row_id,
                values=(
                    entry.get("display_time", "Unknown"),
                    entry.get("moved_count", 0),
                    sample_text,
                ),
            )
        if entries:
            first_row = entries[0]
            row_id = first_row.get("row_id")
            if isinstance(row_id, str):
                tree.selection_set(row_id)
                tree.focus(row_id)
        self._update_history_button_states()

    def _update_history_button_states(self) -> None:
        has_selection = bool(self._history_tree and self._history_tree.selection())
        if self._history_preview_btn:
            if has_selection:
                self._history_preview_btn.state(["!disabled"])
            else:
                self._history_preview_btn.state(["disabled"])
        if self._history_undo_btn:
            if has_selection:
                self._history_undo_btn.state(["!disabled"])
            else:
                self._history_undo_btn.state(["disabled"])

    def _preview_history_selection(self) -> None:
        if not self._history_tree:
            return
        selection = self._history_tree.selection()
        if not selection:
            messagebox.showinfo(
                "Preview Move Batch",
                "Select a move batch to preview.",
                parent=self._history_window or self,
            )
            return
        entry = self._history_entries.get(selection[0])
        if not entry:
            return
        preview_parent = self._history_window or self
        preview = tk.Toplevel(preview_parent)
        preview.title(f"Batch Preview ({entry.get('display_time', 'Unknown')})")
        preview.transient(preview_parent)
        summary = entry.get("summary", "Move batch")
        counts = entry.get("counts") if isinstance(entry.get("counts"), dict) else {}
        extra_bits: List[str] = []
        sources = counts.get("sources")
        targets = counts.get("targets")
        if isinstance(sources, int):
            extra_bits.append(f"Sources: {sources}")
        if isinstance(targets, int):
            extra_bits.append(f"Targets: {targets}")
        header_text = summary
        if extra_bits:
            header_text = f"{summary} ({', '.join(extra_bits)})"
        ttk.Label(preview, text=header_text, padding=(12, 12, 12, 4)).pack(anchor="w")
        text_frame = ttk.Frame(preview, padding=(12, 0, 12, 12))
        text_frame.pack(fill="both", expand=True)
        text_widget = tk.Text(text_frame, height=18, wrap="none")
        text_widget.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(text_frame, orient="vertical", command=text_widget.yview)
        scroll.pack(side="right", fill="y")
        text_widget.configure(yscrollcommand=scroll.set)
        moves = entry.get("moves") if isinstance(entry.get("moves"), list) else []
        max_preview = 200
        for idx, move in enumerate(moves[:max_preview], start=1):
            if not isinstance(move, dict):
                continue
            src = move.get("from", "")
            dst = move.get("to", "")
            text_widget.insert("end", f"{idx}. {src} -> {dst}\n")
        if isinstance(moves, list) and len(moves) > max_preview:
            text_widget.insert(
                "end",
                f"… {len(moves) - max_preview} additional move(s) not shown.\n",
            )
        text_widget.configure(state="disabled")
        ttk.Button(preview, text="Close", command=preview.destroy).pack(pady=(0, 12))
        center_window(preview)

    def _undo_history_selection(self) -> None:
        if not self._history_tree:
            return
        selection = self._history_tree.selection()
        if not selection:
            messagebox.showinfo(
                "Undo Move Batch",
                "Select a move batch to undo.",
                parent=self._history_window or self,
            )
            return
        entry = self._history_entries.get(selection[0])
        if not entry:
            messagebox.showerror(
                "Undo Move Batch",
                "The selected history entry could not be located.",
                parent=self._history_window or self,
            )
            return
        moved_count = entry.get("moved_count", 0)
        label = entry.get("display_time", "this batch")
        if not messagebox.askyesno(
            "Undo Move Batch",
            f"Restore {moved_count} file(s) from {label}?",
            parent=self._history_window or self,
        ):
            return
        mods_root = Path(self.mods_root.get())
        identifier: Optional[Union[str, int]]
        history_index = entry.get("history_index")
        if isinstance(history_index, int):
            identifier = history_index
        else:
            entry_id = entry.get("id")
            identifier = entry_id if isinstance(entry_id, str) and entry_id else None
        self.status_var.set("Undoing selected batch…")

        def worker() -> None:
            undone, failed, errors, undo_label = undo_moves(mods_root, identifier)
            self._enqueue_ui(
                lambda: self._handle_history_undo_result(undone, failed, errors, undo_label)
            )

        threading.Thread(target=worker, daemon=True).start()

    def _handle_history_undo_result(
        self, undone: int, failed: int, errors: List[str], label: Optional[str]
    ) -> None:
        self._handle_undo_result(undone, failed, errors, label)
        self._refresh_move_history()

    def _close_history_window(self) -> None:
        if self._history_window and self._history_window.winfo_exists():
            self._history_window.destroy()

    def _on_history_destroy(self, event: tk.Event) -> None:
        if event.widget is self._history_window:
            self._history_window = None
            self._history_tree = None
            self._history_entries = {}
            self._history_preview_btn = None
            self._history_undo_btn = None

    def on_import(self) -> None:
        filename = filedialog.askopenfilename(
            parent=self,
            filetypes=[("JSON", "*.json"), ("All files", "*.*")],
            title="Import plan from JSON",
        )
        if not filename:
            return
        path = Path(filename)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            messagebox.showerror("Import Plan", f"Unable to import plan: {exc}", parent=self)
            self.log(f"Import failed: {exc}", level="error")
            return

        entries: List[Dict[str, object]] = []
        loadout_payload: Optional[Dict[str, object]] = None
        active_name: Optional[str] = None
        if isinstance(payload, dict):
            raw_plan = payload.get("plan")
            if isinstance(raw_plan, list):
                entries = [entry for entry in raw_plan if isinstance(entry, dict)]
            raw_loadouts = payload.get("loadouts")
            if isinstance(raw_loadouts, dict):
                loadout_payload = raw_loadouts
            active_token = payload.get("active_loadout") or payload.get("active")
            if isinstance(active_token, str):
                active_name = active_token
        elif isinstance(payload, list):
            entries = [entry for entry in payload if isinstance(entry, dict)]
        else:
            messagebox.showerror("Import Plan", "The selected file is not a valid plan export.", parent=self)
            return

        imported_loadouts = 0
        loadout_applied = False
        if loadout_payload:
            imported_loadouts = self._merge_imported_loadouts(loadout_payload, active_name)
            if imported_loadouts and self.items:
                loadout_applied = self._apply_loadout_to_items(self._active_loadout_name)

        applied_entries = 0
        include_updates = 0
        if entries and self.items:
            for entry in entries:
                path_value = entry.get("path")
                if not isinstance(path_value, str):
                    continue
                item = self.items_by_path.get(path_value)
                if not item:
                    continue
                type_value = entry.get("type")
                if isinstance(type_value, str) and type_value:
                    item.guess_type = type_value
                folder_value = entry.get("folder")
                if isinstance(folder_value, str) and folder_value:
                    item.target_folder = folder_value
                tags_value = entry.get("tags")
                if isinstance(tags_value, str):
                    item.meta_tags = tags_value
                dep_status = entry.get("dependency_status")
                if isinstance(dep_status, str):
                    item.dependency_status = dep_status
                dep_detail = entry.get("dependency_detail")
                if isinstance(dep_detail, str):
                    item.dependency_detail = dep_detail
                include_value = entry.get("include")
                if include_value is not None:
                    include_flag = bool(include_value)
                    if item.include != include_flag:
                        item.include = include_flag
                        include_updates += 1
                applied_entries += 1
            self._record_loadout(save=True)
        elif entries and not self.items:
            self.log(
                f"Plan '{path.name}' includes {len(entries)} entry(s) but no scan is loaded; only loadouts were imported."
            )

        needs_refresh = bool(self.items and (applied_entries or include_updates or loadout_applied))
        if needs_refresh:
            self._refresh_tree(preserve_selection=True)

        fragments: List[str] = []
        if applied_entries:
            fragments.append(f"updated {applied_entries} item(s)")
        if include_updates:
            fragments.append(f"applied {include_updates} include flag(s)")
        if imported_loadouts:
            fragments.append(f"imported {imported_loadouts} loadout(s)")
        summary = ", ".join(fragments) if fragments else "no changes detected"
        self.log(f"Imported plan from {filename}: {summary}.")

    def on_export(self) -> None:
        if not self.items:
            self.log("No plan to export.")
            return
        self._record_loadout(save=True)
        filename = filedialog.asksaveasfilename(
            parent=self,
            defaultextension=".json",
            filetypes=[("JSON", "*.json")],
            title="Export plan to JSON",
        )
        if not filename:
            return
        data: List[Dict[str, object]] = []
        for item in self.items:
            extras_dict = item.extras if isinstance(item.extras, dict) else {}
            extras_payload: Dict[str, str] = {}
            if self._plugin_columns:
                extras_payload.update(
                    {
                        column.column_id: extras_dict.get(column.column_id, "") if extras_dict else ""
                        for column in self._plugin_columns
                    }
                )
            duplicate_marker = extras_dict.get(DUPLICATE_EXTRA_KEY, "") if extras_dict else ""
            if duplicate_marker:
                extras_payload[DUPLICATE_EXTRA_KEY] = duplicate_marker
            fingerprint = extras_dict.get(FINGERPRINT_EXTRA_KEY, "") if extras_dict else ""
            if fingerprint:
                extras_payload[FINGERPRINT_EXTRA_KEY] = fingerprint
            entry: Dict[str, object] = {
                "path": str(item.path),
                "name": item.name,
                "type": item.guess_type,
                "size": item.size_mb,
                "tags": item.meta_tags,
                "folder": item.target_folder,
                "dependency_status": item.dependency_status,
                "dependency_detail": item.dependency_detail,
            }
            entry["include"] = bool(item.include)
            if extras_payload:
                entry["extras"] = extras_payload
            data.append(entry)
        loadout_export = self._build_loadout_export()
        if loadout_export:
            payload: Union[List[Dict[str, object]], Dict[str, object]] = {
                "plan": data,
                "loadouts": loadout_export.get("loadouts", {}),
                "active_loadout": loadout_export.get("active", self._active_loadout_name),
                "loadout_version": loadout_export.get("version", LOADOUTS_VERSION),
            }
        else:
            payload = data
        try:
            Path(filename).write_text(json.dumps(payload, indent=2), encoding="utf-8")
            self.log(f"Exported plan to {filename}")
        except Exception as exc:
            self.log(f"Export failed: {exc}")

    # ------------------------------------------------------------------
    # Table refresh
    # ------------------------------------------------------------------
    def _on_duplicate_filter_toggle(self) -> None:
        self._refresh_tree()

    def _refresh_tree(self, preserve_selection: bool = False) -> None:
        selected = set(self.tree.selection()) if preserve_selection else set()
        self._clear_tree_items()
        self._tooltip_payload.clear()
        self.items_by_path = {str(item.path): item for item in self.items}
        duplicate_count = sum(
            1
            for item in self.items
            if isinstance(item.extras, dict) and item.extras.get(DUPLICATE_EXTRA_KEY)
        )
        duplicates_only = bool(self._duplicate_filter_var.get())
        check = getattr(self, "_duplicate_filter_check", None)
        if check is not None and check.winfo_exists():
            label = "Duplicates only" if duplicate_count == 0 else f"Duplicates only ({duplicate_count})"
            try:
                check.configure(text=label)
            except Exception:
                pass
            try:
                if duplicate_count:
                    check.state(["!disabled"])
                else:
                    check.state(["disabled"])
                    if duplicates_only:
                        self._duplicate_filter_var.set(False)
                        duplicates_only = False
            except Exception:
                pass

        overall_counts: Dict[str, int] = {}
        for item in self.items:
            overall_counts[item.guess_type] = overall_counts.get(item.guess_type, 0) + 1

        if duplicates_only:
            display_items = [
                item
                for item in self.items
                if isinstance(item.extras, dict) and item.extras.get(DUPLICATE_EXTRA_KEY)
            ]
        else:
            display_items = list(self.items)

        counts: Dict[str, int] = {}
        for item in display_items:
            counts[item.guess_type] = counts.get(item.guess_type, 0) + 1
            include_icon = ""
            if item.disabled:
                include_icon = "🚫"
            elif item.include:
                include_icon = "✓"
            row_map = {
                "inc": include_icon,
                "rel": os.path.dirname(item.relpath) or ".",
                "name": pretty_display_name(item.name),
                "size": f"{item.size_mb:.2f}",
                "type": item.guess_type,
                "target": item.target_folder,
                "conf": f"{item.confidence:.2f}",
                "linked": "🔗" if item.bundle else "",
                DUPLICATE_EXTRA_KEY: "",
                "meta": item.meta_tags,
                "notes": item.notes,
            }
            extras = getattr(item, "extras", {})
            if isinstance(extras, dict):
                row_map[DUPLICATE_EXTRA_KEY] = extras.get(DUPLICATE_EXTRA_KEY, "")
                if self._plugin_columns:
                    for plugin_column in self._plugin_columns:
                        row_map[plugin_column.column_id] = extras.get(plugin_column.column_id, "")
            values = tuple(row_map.get(column_id, "") for column_id in self._column_order)
            iid = str(item.path)
            self.tree.insert("", "end", iid=iid, values=values)
            if iid in selected:
                self.tree.selection_add(iid)
            tooltips = item.tooltips if isinstance(item.tooltips, dict) else {}
            self._tooltip_payload[iid] = {
                key: value for key, value in tooltips.items() if key in self._column_order and value
            }

        total_files = len(self.items)
        if duplicates_only:
            if display_items:
                topcats = sorted(counts.items(), key=lambda pair: -pair[1])[:4]
                fragment = ", ".join(f"{name}: {count}" for name, count in topcats if count)
                summary = f"Showing {len(display_items)} duplicate file(s)"
                if total_files:
                    summary += f" of {total_files} total"
                if fragment:
                    summary += f" | {fragment}"
                self.summary_var.set(summary)
            else:
                suffix = f" across {total_files} scanned files" if total_files else ""
                self.summary_var.set(f"No duplicates found{suffix}")
        else:
            if total_files:
                topcats = sorted(overall_counts.items(), key=lambda pair: -pair[1])[:4]
                fragment = ", ".join(f"{name}: {count}" for name, count in topcats)
                self.summary_var.set(f"Planned {total_files} files | {fragment}")
            else:
                self.summary_var.set("No plan yet")
        self._schedule_auto_size_columns()

    def _clear_tree_items(self) -> None:
        tree = getattr(self, "tree", None)
        if not tree or not tree.winfo_exists():
            return
        try:
            children = list(tree.get_children())
        except tk.TclError:
            return
        if not children:
            return
        try:
            tree.delete(*children)
            return
        except tk.TclError:
            pass
        batch_size = 500
        for index in range(0, len(children), batch_size):
            batch = children[index : index + batch_size]
            try:
                tree.delete(*batch)
            except tk.TclError:
                for iid in batch:
                    try:
                        tree.delete(iid)
                    except tk.TclError:
                        continue

    def _schedule_auto_size_columns(self) -> None:
        if self._auto_size_pending:
            return
        self._auto_size_pending = True
        try:
            self.after_idle(self._run_auto_size_columns)
        except Exception:
            self._auto_size_pending = False
            self._auto_size_columns()

    def _run_auto_size_columns(self) -> None:
        self._auto_size_pending = False
        self._auto_size_columns()

    def _auto_size_columns(self) -> None:
        tree = getattr(self, "tree", None)
        if not tree or not tree.winfo_exists():
            return
        font = getattr(self, "_tree_font", None)
        if font is None:
            try:
                font = tkfont.nametofont("TkDefaultFont")
            except tk.TclError:
                font = tkfont.Font()
            self._tree_font = font
        heading_font = getattr(self, "_tree_heading_font", None)
        if heading_font is None:
            try:
                heading_font = tkfont.nametofont("TkHeadingFont")
            except tk.TclError:
                heading_font = font
            self._tree_heading_font = heading_font
        try:
            display_columns = list(tree["displaycolumns"])
        except Exception:
            display_columns = []
        if not display_columns or display_columns == ["#all"]:
            target_columns = list(self._column_order)
        else:
            target_columns = [column for column in self._column_order if column in display_columns]
        exempt_columns = {"meta", "notes"}
        target_columns = [column for column in target_columns if column not in exempt_columns]
        if not target_columns:
            return
        padding = 24
        widths: Dict[str, int] = {}
        column_indices = {column: index for index, column in enumerate(self._column_order)}
        for column in target_columns:
            try:
                heading_info = tree.heading(column)
            except tk.TclError:
                continue
            heading = heading_info.get("text", column)
            widths[column] = heading_font.measure(str(heading)) + padding
        for iid in tree.get_children(""):
            try:
                values = tree.item(iid, "values")
            except tk.TclError:
                continue
            for column in target_columns:
                index = column_indices.get(column)
                if index is None or index >= len(values):
                    continue
                width = font.measure(str(values[index])) + padding
                if width > widths[column]:
                    widths[column] = width
        for column in target_columns:
            width = widths.get(column, 0)
            minimum = 36 if column in {"inc", "linked", DUPLICATE_EXTRA_KEY} else 60
            try:
                tree.column(column, width=max(minimum, int(width)), stretch=False)
            except tk.TclError:
                continue

    # ------------------------------------------------------------------
    # Tooltip helpers
    # ------------------------------------------------------------------
    def _schedule_tooltip(self, text: str, x: int, y: int) -> None:
        if self._tooltip_after:
            self.after_cancel(self._tooltip_after)
        self._tooltip_after = self.after(400, lambda: self._show_tooltip(text, x, y))

    def _show_tooltip(self, text: str, x: int, y: int) -> None:
        if self._tooltip_after:
            self.after_cancel(self._tooltip_after)
            self._tooltip_after = None
        if not text:
            self._hide_tooltip()
            return
        if self._tooltip_window is None or not self._tooltip_window.winfo_exists():
            self._tooltip_window = tk.Toplevel(self)
            self._tooltip_window.wm_overrideredirect(True)
            try:
                self._tooltip_window.attributes("-topmost", True)
            except Exception:
                pass
            palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
            self._tooltip_label = ttk.Label(
                self._tooltip_window,
                text=text,
                background=palette.get("alt", "#333333"),
                foreground=palette.get("fg", "#ffffff"),
                relief="solid",
                borderwidth=1,
                padding=(6, 4),
                wraplength=360,
                justify="left",
            )
            self._tooltip_label.pack()
        else:
            if self._tooltip_label is not None:
                self._tooltip_label.configure(text=text)
        if self._tooltip_window is not None and self._tooltip_window.winfo_exists():
            self._tooltip_window.update_idletasks()
            self._tooltip_window.geometry(f"+{x}+{y}")

    def _position_tooltip(self, x: int, y: int) -> None:
        if self._tooltip_window is not None and self._tooltip_window.winfo_exists():
            self._tooltip_window.geometry(f"+{x}+{y}")

    def _hide_tooltip(self) -> None:
        if self._tooltip_after:
            self.after_cancel(self._tooltip_after)
            self._tooltip_after = None
        if self._tooltip_window is not None and self._tooltip_window.winfo_exists():
            self._tooltip_window.destroy()
        self._tooltip_window = None
        self._tooltip_label = None
        self._tooltip_target = ("", "")

    def _on_tree_motion(self, event: tk.Event) -> None:
        row = self.tree.identify_row(event.y)
        column_token = self.tree.identify_column(event.x)
        if not row or not column_token:
            self._hide_tooltip()
            return
        try:
            index = int(column_token.replace("#", "")) - 1
        except ValueError:
            self._hide_tooltip()
            return
        if index < 0 or index >= len(self._column_order):
            self._hide_tooltip()
            return
        column_id = self._column_order[index]
        tooltip_map = self._tooltip_payload.get(row, {})
        text = tooltip_map.get(column_id, "")
        if not text:
            self._hide_tooltip()
            return
        target = (row, column_id)
        pointer_x = event.x_root + 12
        pointer_y = event.y_root + 18
        if target == self._tooltip_target and self._tooltip_window is not None:
            self._position_tooltip(pointer_x, pointer_y)
            return
        self._tooltip_target = target
        self._schedule_tooltip(text, pointer_x, pointer_y)
# ---------------------------------------------------------------------------
# Entry points and self-test
# ---------------------------------------------------------------------------


def _selftest() -> None:
    samples = {
        "wickedwhims_nude_top.package": {"Adult CAS"},
        "durex_bundle.zip": {"Adult Other", "Adult BuildBuy", "Adult Gameplay"},
        "uicheats.ts4script": {"Script Mod"},
        "fantasyhair.package": {"CAS Hair", "Other"},
        "object_sofa.package": {"BuildBuy Object"},
    }
    for name, expected in samples.items():
        cat, *_ = guess_type_for_name(name, Path(name).suffix)
        assert cat in expected, f"{name} -> {cat}, expected one of {sorted(expected)}"
    dummy_columns = [
        PluginColumn("valid", "Valid", 80, "center"),
        PluginColumn("invalid column", "Invalid", 80, "center"),
    ]
    accepted, rejected = _partition_plugin_columns(dummy_columns, {"inc", "valid", "name"})
    assert [column.column_id for column in accepted] == ["valid"]
    assert [column.column_id for column in rejected] == ["invalid column"]
    print("selftest ok")


def main() -> None:
    if "--selftest" in sys.argv:
        _selftest()
        return
    app = Sims4ModSorterApp()
    app.mainloop()


if __name__ == "__main__":
    main()
