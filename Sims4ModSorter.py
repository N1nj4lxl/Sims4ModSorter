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
from tkinter import filedialog, simpledialog, ttk

from launch_utils import UpdateResult, check_for_update
from command_center import CommandCenter
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


@dataclass(slots=True)
class CommandCenterPanel:
    panel_id: str
    title: str
    builder: Callable[["Sims4ModSorterApp", ttk.Frame, "PluginAPI"], None]
    priority: int = 100


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

    def register_command_center_panel(
        self,
        panel_id: str,
        title: str,
        builder: Callable[["Sims4ModSorterApp", ttk.Frame, "PluginAPI"], None],
        *,
        priority: int = 100,
    ) -> None:
        self._manager.register_command_center_panel(
            panel_id,
            title,
            builder,
            priority=priority,
        )

    def refresh_command_center(self) -> None:
        app = self.app
        if app is None:
            return
        refresher = getattr(app, "command_center", None)
        if refresher is None:
            return
        refresh = getattr(refresher, "refresh", None)
        if callable(refresh):
            refresh()


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
        self.command_center_panels: Dict[str, CommandCenterPanel] = {}
        self.command_center_order: List[str] = []
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

    def register_command_center_panel(
        self,
        panel_id: str,
        title: str,
        builder: Callable[["Sims4ModSorterApp", ttk.Frame, PluginAPI], None],
        *,
        priority: int = 100,
    ) -> None:
        normalized = panel_id.strip() if panel_id else ""
        if not normalized or not callable(builder):
            self.message_bus.post(
                "boot",
                "warn",
                f"Command center panel registration skipped for '{panel_id or '?'}'",
            )
            return
        if normalized in self.command_center_panels:
            self.message_bus.post(
                "boot",
                "warn",
                f"Command center panel '{normalized}' already registered; ignoring duplicate",
            )
            return
        try:
            priority_value = int(priority)
        except Exception:
            priority_value = 100
        entry = CommandCenterPanel(
            panel_id=normalized,
            title=title or normalized.title(),
            builder=builder,
            priority=priority_value,
        )
        self.command_center_panels[normalized] = entry
        self.command_center_order.append(normalized)

    def get_command_center_panels(self) -> List[CommandCenterPanel]:
        ordered: List[CommandCenterPanel] = []
        order_index = {panel_id: idx for idx, panel_id in enumerate(self.command_center_order)}
        for panel_id in self.command_center_order:
            entry = self.command_center_panels.get(panel_id)
            if entry is not None:
                ordered.append(entry)
        ordered.sort(key=lambda item: (item.priority, order_index.get(item.panel_id, 0)))
        return ordered


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
LOADOUT_GALLERY_FILENAME = ".sims4_modsorter_gallery.json"
AUTOMATION_FILENAME = ".sims4_modsorter_automations.json"
AUTOMATION_VERSION = 1
PLUGIN_MARKETPLACE_FILENAME = "plugin_marketplace.json"
PLUGIN_RATINGS_FILENAME = ".sims4_modsorter_plugin_ratings.json"
MOVE_FAVORITES_FILENAME = ".sims4_modsorter_favorite_moves.json"
DEFAULT_LOADOUT_NAME = "Default Loadout"
LOADOUTS_VERSION = 1
SETTINGS_VERSION = 1


def _default_settings_path() -> Path:
    return Path.home() / ".sims4_modsorter" / "settings.json"


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
        self.protocol("WM_DELETE_WINDOW", self.destroy)

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
        self.show_command_center_var = tk.BooleanVar(value=True)
        self.mods_root = tk.StringVar(value=get_default_mods_path())
        self._version_display_var = tk.StringVar(value=f"App Version: {APP_VERSION}")

        self.items: List[FileItem] = []
        self.items_by_path: Dict[str, FileItem] = {}
        self.scan_errors: List[str] = []
        self.disabled_items: List[FileItem] = []
        self._plugin_columns: List[PluginColumn] = []
        self._plugin_toolbar_buttons: List[PluginToolbarButton] = []

        self.status_var = tk.StringVar(value="Ready")
        self.summary_var = tk.StringVar(value="No plan yet")
        self._duplicate_filter_var = tk.BooleanVar(value=False)
        self._duplicate_filter_check: Optional[ttk.Checkbutton] = None

        self._ui_queue: "queue.Queue[Callable[[], None]]" = queue.Queue()
        self._theme_cache: Dict[str, str] = {}
        self._recent_mods_dirs: List[str] = []
        self._column_order: List[str] = []
        self._settings_path: Path = _default_settings_path()
        self._settings_version: int = SETTINGS_VERSION
        self._desired_plugin_states: Dict[str, bool] = {}
        self._load_app_settings()
        self.plugin_manager = load_user_plugins()
        if self.plugin_manager:
            self.plugin_manager.attach_app(self)
            self._plugin_columns = self.plugin_manager.get_columns()
            self._plugin_toolbar_buttons = self.plugin_manager.get_toolbar_buttons()
        self._tooltip_payload: Dict[str, Dict[str, str]] = {}
        self._tooltip_window: Optional[tk.Toplevel] = None
        self._tooltip_label: Optional[tk.Label] = None
        self._tooltip_after: Optional[str] = None
        self._tooltip_target: Tuple[str, str] = ("", "")
        self._toolbar_widgets: Dict[str, tk.Widget] = {}
        self._mod_status_overlay: Optional[tk.Frame] = None
        self._mod_status_container: Optional[ttk.Frame] = None
        self._status_trees: Dict[str, ttk.Treeview] = {}
        self._status_summary_var = tk.StringVar(value="")
        self._update_check_in_progress = False
        self.check_updates_button: Optional[ttk.Button] = None
        self._latest_version: Optional[str] = None
        self._update_download_url: Optional[str] = None
        self._update_release_page_url: Optional[str] = None
        self._update_download_filename: Optional[str] = None
        self._update_available: bool = False
        self._update_release_notes: Optional[str] = None
        self._update_overlay: Optional[tk.Frame] = None
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
        self._update_overlay_container: Optional[ttk.Frame] = None
        self._update_download_mode = tk.StringVar(value="simple")
        self._update_mode_description = tk.StringVar(
            value="Downloads and installs every updated file automatically."
        )
        self._update_mode_frame: Optional[ttk.LabelFrame] = None
        self._update_mode_simple_radio: Optional[ttk.Radiobutton] = None
        self._update_mode_advanced_radio: Optional[ttk.Radiobutton] = None
        self._update_mode_description_label: Optional[ttk.Label] = None
        self._auto_size_pending = False

        self._dialog_overlay: Optional[tk.Toplevel] = None
        self._dialog_overlay_scrim: Optional[tk.Frame] = None
        self._dialog_overlay_container: Optional[ttk.Frame] = None
        self._dialog_overlay_button_frame: Optional[ttk.Frame] = None
        self._dialog_overlay_icon_label: Optional[ttk.Label] = None
        self._dialog_overlay_headline_label: Optional[ttk.Label] = None
        self._dialog_overlay_message_label: Optional[ttk.Label] = None
        self._dialog_overlay_icon = tk.StringVar(value="")
        self._dialog_overlay_headline = tk.StringVar(value="")
        self._dialog_overlay_message = tk.StringVar(value="")
        self._dialog_overlay_icon_font: Optional[tkfont.Font] = None
        self._dialog_overlay_headline_font: Optional[tkfont.Font] = None
        self._dialog_overlay_visible: bool = False
        self._dialog_overlay_default: str = ""
        self._dialog_overlay_cancel: str = ""
        self._dialog_overlay_wait_var: Optional[tk.StringVar] = None

        self._overlay_registry: Dict[str, Dict[str, object]] = {}

        self._history_entries: Dict[str, Dict[str, object]] = {}
        self._history_selection: Optional[str] = None

        self.loadout_gallery: List[Dict[str, object]] = []
        self._gallery_selection: Optional[str] = None
        self.automation_macros: List[Dict[str, object]] = []
        self.plugin_catalog: List[Dict[str, object]] = []
        self.plugin_ratings: Dict[str, Dict[str, object]] = {}
        self._plugin_marketplace_selection: Optional[str] = None
        self._plugin_gallery_selection: Optional[str] = None

        self._duplicate_groups: Dict[str, List[FileItem]] = {}
        self.move_favorites: Set[str] = set()

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
        self._load_loadout_gallery()
        self._load_automation_macros()
        self._load_plugin_catalog()
        self._load_plugin_ratings()
        self._load_history_favorites()

        self._build_style()
        self._build_ui()
        self._build_settings_overlay()
        self.command_center = CommandCenter(self)
        self.mods_root.trace_add("write", lambda *_: self._on_mods_root_change())
        self._remember_mods_directory(self.mods_root.get())
        self.after(16, self._pump_ui_queue)
        self._report_mod_boot_messages()
        self.after(1000, self._check_updates_on_launch)
        self.after(0, lambda: center_window(self))
        self.after(120, self._maybe_show_command_center)
        self.bind("<Configure>", self._on_root_configure, add="+")

    # ------------------------------------------------------------------
    # Settings persistence
    # ------------------------------------------------------------------
    def _settings_file_path(self) -> Path:
        path = getattr(self, "_settings_path", None)
        if isinstance(path, Path):
            return path
        return _default_settings_path()

    def _load_app_settings(self) -> None:
        self._desired_plugin_states = {}
        path = self._settings_file_path()
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return

        theme_value = payload.get("theme")
        if isinstance(theme_value, str):
            candidate = theme_value.strip()
            if candidate in THEMES:
                try:
                    self.theme_name.set(candidate)
                except Exception:
                    pass

        string_settings = {
            "mods_root": self.mods_root,
            "ignore_exts": self.ignore_exts_var,
            "ignore_names": self.ignore_names_var,
        }
        for key, variable in string_settings.items():
            value = payload.get(key)
            if isinstance(value, str):
                try:
                    variable.set(value)
                except Exception:
                    pass

        bool_settings = {
            "recurse": self.recurse_var,
            "scan_package": self.scan_package_var,
            "scan_script": self.scan_script_var,
            "scan_archive": self.scan_archive_var,
            "scan_misc": self.scan_misc_var,
            "include_adult": self.include_adult_var,
            "show_command_center": self.show_command_center_var,
        }
        for key, variable in bool_settings.items():
            if key in payload:
                try:
                    variable.set(bool(payload.get(key)))
                except Exception:
                    pass

        recents = payload.get("recent_mods_dirs")
        clean_recents: List[str] = []
        if isinstance(recents, list):
            for entry in recents:
                if not isinstance(entry, str):
                    continue
                normalized = entry.strip()
                if not normalized or normalized in clean_recents:
                    continue
                clean_recents.append(normalized)
                if len(clean_recents) >= 8:
                    break
        self._recent_mods_dirs = clean_recents[:5]

        plugin_states = payload.get("plugin_states")
        desired: Dict[str, bool] = {}
        if isinstance(plugin_states, dict):
            for folder, enabled in plugin_states.items():
                if isinstance(folder, str):
                    token = folder.strip()
                    if token:
                        desired[token] = bool(enabled)
        self._desired_plugin_states = desired
        if self._desired_plugin_states:
            self._apply_plugin_state_preferences()

    def _apply_plugin_state_preferences(self) -> None:
        desired = getattr(self, "_desired_plugin_states", {})
        if not desired:
            return
        for folder, enabled in desired.items():
            try:
                folder_name = str(folder).strip()
            except Exception:
                continue
            if not folder_name:
                continue
            plugin_dir = USER_PLUGINS_DIR / folder_name
            if not plugin_dir.exists():
                continue
            manifest_path = plugin_dir / "plugin.json"
            manifest: Dict[str, object]
            if manifest_path.exists():
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                except Exception:
                    manifest = {}
                if not isinstance(manifest, dict):
                    manifest = {}
            else:
                manifest = {
                    "name": folder_name,
                    "entry": "plugin.py",
                    "enabled": True,
                    "callable": "register",
                }
            current = bool(manifest.get("enabled", True))
            desired_state = bool(enabled)
            if current == desired_state:
                continue
            manifest["enabled"] = desired_state
            try:
                manifest_path.parent.mkdir(parents=True, exist_ok=True)
                manifest_path.write_text(
                    json.dumps(manifest, indent=2, sort_keys=True),
                    encoding="utf-8",
                )
            except Exception:
                continue

    def _collect_settings_payload(self) -> Dict[str, object]:
        payload: Dict[str, object] = {"version": getattr(self, "_settings_version", SETTINGS_VERSION)}

        def _safe_get(var, default=None):
            if var is None:
                return default
            try:
                value = var.get()
            except Exception:
                return default
            return value

        theme_var = getattr(self, "theme_name", None)
        payload["theme"] = str(_safe_get(theme_var, "Dark Mode"))

        mods_var = getattr(self, "mods_root", None)
        payload["mods_root"] = str(_safe_get(mods_var, "")).strip()

        ignore_exts_var = getattr(self, "ignore_exts_var", None)
        payload["ignore_exts"] = str(_safe_get(ignore_exts_var, ""))
        ignore_names_var = getattr(self, "ignore_names_var", None)
        payload["ignore_names"] = str(_safe_get(ignore_names_var, ""))

        bool_sources = {
            "recurse": getattr(self, "recurse_var", None),
            "scan_package": getattr(self, "scan_package_var", None),
            "scan_script": getattr(self, "scan_script_var", None),
            "scan_archive": getattr(self, "scan_archive_var", None),
            "scan_misc": getattr(self, "scan_misc_var", None),
            "include_adult": getattr(self, "include_adult_var", None),
            "show_command_center": getattr(self, "show_command_center_var", None),
        }
        for key, variable in bool_sources.items():
            if variable is None:
                payload[key] = False
                continue
            try:
                payload[key] = bool(variable.get())
            except Exception:
                payload[key] = bool(variable)

        payload["recent_mods_dirs"] = list(getattr(self, "_recent_mods_dirs", []))

        plugin_states: Dict[str, bool] = {}
        manager = getattr(self, "plugin_manager", None)
        if manager is not None:
            try:
                statuses = manager.get_statuses()
            except Exception:
                statuses = []
            for status in statuses:
                state = getattr(status, "status", "")
                if state not in {"loaded", "disabled"}:
                    continue
                folder = getattr(status, "folder", "")
                if not folder:
                    continue
                plugin_states[str(folder)] = state == "loaded"
        payload["plugin_states"] = plugin_states
        return payload

    def _save_settings_to_disk(self) -> None:
        path = self._settings_file_path()
        try:
            payload = self._collect_settings_payload()
        except Exception:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except Exception:
            return

    def destroy(self) -> None:  # type: ignore[override]
        try:
            self._save_settings_to_disk()
        except Exception:
            pass
        super().destroy()

    # ------------------------------------------------------------------
    # Launch helpers
    # ------------------------------------------------------------------
    def _maybe_show_command_center(self) -> None:
        show = False
        try:
            show = bool(self.show_command_center_var.get())
        except Exception:
            show = False
        dashboard = getattr(self, "command_center", None)
        if dashboard is None:
            return
        if show:
            dashboard.show()
        else:
            dashboard.hide()

    def _remember_mods_directory(self, value: Optional[str]) -> None:
        text = (value or "").strip()
        if not text:
            return
        try:
            normalized = str(Path(text).expanduser())
        except Exception:
            normalized = text
        if normalized in self._recent_mods_dirs:
            self._recent_mods_dirs.remove(normalized)
        self._recent_mods_dirs.insert(0, normalized)
        del self._recent_mods_dirs[5:]
        dashboard = getattr(self, "command_center", None)
        if dashboard is not None:
            dashboard.refresh()

    def get_recent_mods_dirs(self) -> List[str]:
        return list(self._recent_mods_dirs)

    def open_command_center(self) -> None:
        dashboard = getattr(self, "command_center", None)
        if dashboard is not None:
            dashboard.show(modal=False)

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
        self._remember_mods_directory(self.mods_root.get())
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

    def _resolve_gallery_path(self) -> Optional[Path]:
        root_value = self.mods_root.get().strip()
        if not root_value:
            return None
        try:
            root_path = Path(root_value).expanduser()
        except Exception:
            return None
        return root_path / LOADOUT_GALLERY_FILENAME

    def _automation_path(self) -> Path:
        base = self._settings_file_path().parent
        return base / AUTOMATION_FILENAME

    def _plugin_ratings_path(self) -> Path:
        base = self._settings_file_path().parent
        return base / PLUGIN_RATINGS_FILENAME

    def _plugin_catalog_path(self) -> Path:
        return Path(__file__).resolve().with_name(PLUGIN_MARKETPLACE_FILENAME)

    def _history_favorites_path(self) -> Path:
        return self._settings_file_path().parent / MOVE_FAVORITES_FILENAME

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

    def _load_loadout_gallery(self) -> None:
        path = self._resolve_gallery_path()
        entries: List[Dict[str, object]] = []
        if path and path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = []
            if isinstance(payload, list):
                for entry in payload:
                    if not isinstance(entry, dict):
                        continue
                    name = str(entry.get("name") or "").strip()
                    loadout = entry.get("loadout")
                    if not name or not isinstance(loadout, dict):
                        continue
                    normalized: Dict[str, object] = {
                        "id": str(entry.get("id") or uuid.uuid4().hex),
                        "name": name,
                        "description": str(entry.get("description") or "").strip(),
                        "tags": [str(tag).strip() for tag in entry.get("tags", []) if str(tag).strip()],
                        "created": str(entry.get("created") or datetime.utcnow().isoformat()),
                        "updated": str(entry.get("updated") or datetime.utcnow().isoformat()),
                        "rating": float(entry.get("rating")) if isinstance(entry.get("rating"), (int, float)) else 0.0,
                        "downloads": int(entry.get("downloads") or 0),
                        "loadout": {str(path): bool(flag) for path, flag in loadout.items() if isinstance(path, str)},
                    }
                    entries.append(normalized)
        entries.sort(key=lambda item: (str(item.get("name"))).lower())
        self.loadout_gallery = entries

    def _save_loadout_gallery(self) -> None:
        path = self._resolve_gallery_path()
        if not path:
            return
        payload = []
        for entry in self.loadout_gallery:
            if not isinstance(entry, dict):
                continue
            loadout = entry.get("loadout")
            if not isinstance(loadout, dict):
                continue
            payload.append(
                {
                    "id": entry.get("id"),
                    "name": entry.get("name"),
                    "description": entry.get("description", ""),
                    "tags": list(entry.get("tags", [])),
                    "created": entry.get("created"),
                    "updated": entry.get("updated"),
                    "rating": entry.get("rating", 0.0),
                    "downloads": entry.get("downloads", 0),
                    "loadout": loadout,
                }
            )
        try:
            if path.parent:
                path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            self.log("Unable to save loadout gallery to disk", level="warn")

    def _load_automation_macros(self) -> None:
        path = self._automation_path()
        macros: List[Dict[str, object]] = []
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                raw_macros = payload.get("macros")
                if isinstance(raw_macros, list):
                    for entry in raw_macros:
                        if not isinstance(entry, dict):
                            continue
                        name = str(entry.get("name") or "").strip()
                        steps = entry.get("steps")
                        if not name or not isinstance(steps, list):
                            continue
                        macros.append(
                            {
                                "id": str(entry.get("id") or uuid.uuid4().hex),
                                "name": name,
                                "description": str(entry.get("description") or "").strip(),
                                "steps": [dict(step) for step in steps if isinstance(step, dict)],
                            }
                        )
        if not macros:
            macros = self._default_automation_macros()
        self.automation_macros = macros

    def _save_automation_macros(self) -> None:
        path = self._automation_path()
        payload = {
            "version": AUTOMATION_VERSION,
            "macros": [
                {
                    "id": entry.get("id"),
                    "name": entry.get("name"),
                    "description": entry.get("description", ""),
                    "steps": entry.get("steps", []),
                }
                for entry in self.automation_macros
                if isinstance(entry, dict)
            ],
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            self.log("Unable to save automation macros", level="warn")

    def _default_automation_macros(self) -> List[Dict[str, object]]:
        return [
            {
                "id": uuid.uuid4().hex,
                "name": "Scan & Apply Active Loadout",
                "description": "Runs a scan and reapplies whichever loadout is currently selected.",
                "steps": [
                    {"action": "scan"},
                    {"action": "apply_loadout", "name": "__ACTIVE__"},
                ],
            },
            {
                "id": uuid.uuid4().hex,
                "name": "Open Mods Folder",
                "description": "Opens the Mods directory in your file browser.",
                "steps": [
                    {"action": "open_mods"},
                ],
            },
            {
                "id": uuid.uuid4().hex,
                "name": "Dependency Refresh",
                "description": "Triggers the dependency tracker to recheck required frameworks after a scan.",
                "steps": [
                    {"action": "scan"},
                    {"action": "run_plugin", "plugin": "dependency_tracker"},
                ],
            },
        ]

    def _load_plugin_catalog(self) -> None:
        path = self._plugin_catalog_path()
        entries: List[Dict[str, object]] = []
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                raw_entries = payload.get("plugins")
                if isinstance(raw_entries, list):
                    for entry in raw_entries:
                        if not isinstance(entry, dict):
                            continue
                        plugin_id = str(entry.get("id") or entry.get("slug") or "").strip()
                        name = str(entry.get("name") or "").strip()
                        if not plugin_id or not name:
                            continue
                        entries.append(
                            {
                                "id": plugin_id,
                                "name": name,
                                "description": str(entry.get("description") or "").strip(),
                                "version": str(entry.get("version") or "").strip(),
                                "folder": str(entry.get("folder") or plugin_id),
                                "author": str(entry.get("author") or "Unknown"),
                                "tags": [str(tag).strip() for tag in entry.get("tags", []) if str(tag).strip()],
                                "rating": float(entry.get("rating")) if isinstance(entry.get("rating"), (int, float)) else 0.0,
                                "download_url": str(entry.get("download_url") or entry.get("url") or "").strip(),
                                "homepage": str(entry.get("homepage") or "").strip(),
                                "requires": [str(req).strip() for req in entry.get("requires", []) if str(req).strip()],
                            }
                        )
        entries.sort(key=lambda item: (str(item.get("name"))).lower())
        self.plugin_catalog = entries

    def _load_plugin_ratings(self) -> None:
        path = self._plugin_ratings_path()
        ratings: Dict[str, Dict[str, object]] = {}
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                for plugin_id, rating in payload.items():
                    try:
                        plugin_key = str(plugin_id).strip()
                    except Exception:
                        continue
                    if not plugin_key:
                        continue
                    if isinstance(rating, dict):
                        entry = {
                            "rating": float(rating.get("rating", 0.0)),
                            "notes": str(rating.get("notes") or ""),
                            "updated": str(rating.get("updated") or datetime.utcnow().isoformat()),
                        }
                    else:
                        entry = {
                            "rating": float(rating),
                            "notes": "",
                            "updated": datetime.utcnow().isoformat(),
                        }
                    ratings[plugin_key] = entry
        self.plugin_ratings = ratings

    def _save_plugin_ratings(self) -> None:
        path = self._plugin_ratings_path()
        payload = {
            plugin_id: {
                "rating": float(entry.get("rating", 0.0)),
                "notes": entry.get("notes", ""),
                "updated": entry.get("updated", datetime.utcnow().isoformat()),
            }
            for plugin_id, entry in self.plugin_ratings.items()
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            self.log("Unable to persist plugin ratings", level="warn")

    def _user_rating_for(self, plugin_id: str) -> float:
        entry = self.plugin_ratings.get(plugin_id)
        if not entry:
            return 0.0
        try:
            return float(entry.get("rating", 0.0))
        except Exception:
            return 0.0

    def _load_history_favorites(self) -> None:
        path = self._history_favorites_path()
        favorites: Set[str] = set()
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = []
            if isinstance(payload, (list, set, tuple)):
                favorites = {str(entry).strip() for entry in payload if str(entry).strip()}
        self.move_favorites = favorites

    def _save_history_favorites(self) -> None:
        path = self._history_favorites_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(sorted(self.move_favorites)), encoding="utf-8")
        except Exception:
            self.log("Unable to save history favorites", level="warn")

    def _find_gallery_entry(self, entry_id: str) -> Optional[Dict[str, object]]:
        for entry in self.loadout_gallery:
            if str(entry.get("id")) == entry_id:
                return entry
        return None

    def _rebuild_duplicate_groups(self) -> None:
        groups: Dict[str, List[FileItem]] = {}
        for entry in self.items:
            extras = getattr(entry, "extras", {})
            if not isinstance(extras, dict):
                continue
            fingerprint = str(extras.get(FINGERPRINT_EXTRA_KEY, ""))
            if fingerprint:
                groups.setdefault(fingerprint, []).append(entry)
        self._duplicate_groups = {key: value for key, value in groups.items() if len(value) > 1}

    def show_duplicate_comparison(self) -> None:
        self._rebuild_duplicate_groups()
        if not self._duplicate_groups:
            self._show_info_overlay("Duplicates", "No duplicates were flagged in the current plan.")
            return
        selection = list(self.tree.selection()) if hasattr(self, "tree") else []
        target_group: Optional[str] = None
        for iid in selection:
            item = self.items_by_path.get(iid)
            extras = getattr(item, "extras", {}) if item else {}
            fingerprint = str(extras.get(FINGERPRINT_EXTRA_KEY, "")) if isinstance(extras, dict) else ""
            if fingerprint and fingerprint in self._duplicate_groups:
                target_group = fingerprint
                break
        if not target_group:
            target_group = next(iter(self._duplicate_groups.keys()))
        group_items = list(self._duplicate_groups.get(target_group, []))
        if not group_items:
            self._show_info_overlay("Duplicates", "No duplicates were found for the selection.")
            return
        group_items.sort(key=lambda entry: _natural_key(entry.relpath))

        def builder(body: ttk.Frame, footer: ttk.Frame) -> None:
            if body is None or footer is None:
                return
            body.grid_columnconfigure(0, weight=1)
            body.grid_columnconfigure(1, weight=1)
            body.grid_rowconfigure(0, weight=1)

            tree_frame = ttk.Frame(body, style="Overlay.Body.TFrame")
            tree_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
            tree_frame.grid_rowconfigure(0, weight=1)
            tree_frame.grid_columnconfigure(0, weight=1)

            tree = ttk.Treeview(
                tree_frame,
                columns=("include", "path", "size", "target"),
                show="headings",
                selectmode="browse",
            )
            tree.heading("include", text="✔")
            tree.heading("path", text="Path")
            tree.heading("size", text="MB")
            tree.heading("target", text="Target")
            tree.column("include", width=40, anchor="center")
            tree.column("path", width=220, anchor="w")
            tree.column("size", width=70, anchor="e")
            tree.column("target", width=120, anchor="w")
            tree.grid(row=0, column=0, sticky="nsew")

            scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
            tree.configure(yscrollcommand=scroll.set)
            scroll.grid(row=0, column=1, sticky="ns")

            comparison = ttk.Frame(body, style="Overlay.Body.TFrame")
            comparison.grid(row=0, column=1, sticky="nsew")
            for idx in range(2):
                comparison.grid_columnconfigure(idx, weight=1)

            primary = group_items[0]

            ttk.Label(comparison, text="Primary", style="Overlay.Title.TLabel").grid(
                row=0, column=0, sticky="w"
            )
            ttk.Label(comparison, text="Selected", style="Overlay.Title.TLabel").grid(
                row=0, column=1, sticky="w"
            )

            primary_path = tk.StringVar(value=primary.relpath)
            primary_size = tk.StringVar(value=f"{primary.size_mb:.2f} MB")
            primary_target = tk.StringVar(value=primary.target_folder)
            primary_include = tk.StringVar(value="Yes" if primary.include else "No")

            ttk.Label(comparison, textvariable=primary_path, style="Overlay.Subtitle.TLabel").grid(
                row=1, column=0, sticky="w"
            )
            ttk.Label(comparison, textvariable=primary_size, style="Overlay.Subtitle.TLabel").grid(
                row=2, column=0, sticky="w"
            )
            ttk.Label(comparison, textvariable=primary_target, style="Overlay.Subtitle.TLabel").grid(
                row=3, column=0, sticky="w"
            )
            ttk.Label(comparison, textvariable=primary_include, style="Overlay.Subtitle.TLabel").grid(
                row=4, column=0, sticky="w"
            )

            selected_path = tk.StringVar(value="Select a duplicate to compare")
            selected_size = tk.StringVar(value="")
            selected_target = tk.StringVar(value="")
            selected_include = tk.StringVar(value="")
            diff_var = tk.StringVar(value="")

            ttk.Label(comparison, textvariable=selected_path, style="Overlay.Subtitle.TLabel").grid(
                row=1, column=1, sticky="w"
            )
            ttk.Label(comparison, textvariable=selected_size, style="Overlay.Subtitle.TLabel").grid(
                row=2, column=1, sticky="w"
            )
            ttk.Label(comparison, textvariable=selected_target, style="Overlay.Subtitle.TLabel").grid(
                row=3, column=1, sticky="w"
            )
            ttk.Label(comparison, textvariable=selected_include, style="Overlay.Subtitle.TLabel").grid(
                row=4, column=1, sticky="w"
            )
            ttk.Label(comparison, textvariable=diff_var, style="Overlay.Subtitle.TLabel").grid(
                row=5, column=0, columnspan=2, sticky="w", pady=(10, 0)
            )

            def refresh_tree(select_id: Optional[str] = None) -> None:
                tree.delete(*tree.get_children())
                for item in group_items:
                    include_icon = "✓" if item.include else ""
                    tree.insert(
                        "",
                        "end",
                        iid=str(item.path),
                        values=(
                            include_icon,
                            item.relpath,
                            f"{item.size_mb:.2f}",
                            item.target_folder,
                        ),
                    )
                if select_id:
                    try:
                        tree.selection_set(select_id)
                        tree.focus(select_id)
                    except tk.TclError:
                        pass

            def describe_difference(base: FileItem, other: FileItem) -> str:
                differences: List[str] = []
                if abs(base.size_mb - other.size_mb) > 0.01:
                    differences.append(
                        f"Size diff: {base.size_mb:.2f} vs {other.size_mb:.2f} MB"
                    )
                if base.target_folder != other.target_folder:
                    differences.append(
                        f"Folder diff: {base.target_folder or 'Default'} vs {other.target_folder or 'Default'}"
                    )
                if base.include != other.include:
                    differences.append(
                        "Include state differs"
                    )
                if base.bundle != other.bundle:
                    differences.append("Bundle status differs")
                return "; ".join(differences) if differences else "Files share identical metadata."

            def update_detail(iid: Optional[str]) -> None:
                if not iid:
                    selected_path.set("Select a duplicate to compare")
                    selected_size.set("")
                    selected_target.set("")
                    selected_include.set("")
                    diff_var.set("")
                    return
                current = next((item for item in group_items if str(item.path) == iid), None)
                if current is None:
                    selected_path.set("Select a duplicate to compare")
                    selected_size.set("")
                    selected_target.set("")
                    selected_include.set("")
                    diff_var.set("")
                    return
                selected_path.set(current.relpath)
                selected_size.set(f"{current.size_mb:.2f} MB")
                selected_target.set(current.target_folder)
                selected_include.set("Included" if current.include else "Excluded")
                diff_var.set(describe_difference(primary, current))

            def on_select(_event=None) -> None:
                selection = tree.selection()
                update_detail(selection[0] if selection else None)

            tree.bind("<<TreeviewSelect>>", on_select)

            def keep_selected() -> None:
                selection = tree.selection()
                if not selection:
                    self._show_info_overlay("Duplicates", "Select a duplicate to keep first.")
                    return
                selected_item = next((item for item in group_items if str(item.path) == selection[0]), None)
                if selected_item is None:
                    return
                for item in group_items:
                    item.include = item is selected_item
                self._refresh_tree()
                self._rebuild_duplicate_groups()
                refresh_tree(selection[0])
                update_detail(selection[0])
                self._show_info_overlay("Duplicates", f"Keeping only '{selected_item.relpath}'.")

            def open_location() -> None:
                selection = tree.selection()
                if not selection:
                    self._show_info_overlay("Duplicates", "Select a duplicate to open first.")
                    return
                target = Path(selection[0])
                folder = target.parent
                try:
                    if sys.platform.startswith("win"):
                        os.startfile(str(folder))  # type: ignore[attr-defined]
                    elif sys.platform == "darwin":
                        subprocess.Popen(["open", str(folder)])
                    else:
                        subprocess.Popen(["xdg-open", str(folder)])
                except Exception as exc:
                    self._show_error_overlay("Duplicates", f"Unable to open location: {exc}")

            ttk.Button(footer, text="Keep only selected", command=keep_selected).pack(side="left", padx=(0, 8))
            ttk.Button(footer, text="Open location", command=open_location).pack(side="left")
            ttk.Button(footer, text="Close", command=lambda: self._hide_overlay_panel("duplicate_diff")).pack(
                side="right"
            )

            refresh_tree(str(group_items[0].path))
            tree.selection_set(str(group_items[0].path))
            update_detail(str(group_items[0].path))

        title = f"Duplicate Comparison ({len(group_items)} files)"
        self._show_overlay_panel("duplicate_diff", title, builder, width=900, height=520)

    def show_loadout_gallery(self) -> None:
        def builder(body: ttk.Frame, footer: ttk.Frame) -> None:
            if body is None or footer is None:
                return
            body.grid_columnconfigure(0, weight=1)
            body.grid_columnconfigure(1, weight=2)
            body.grid_rowconfigure(0, weight=1)

            tree_frame = ttk.Frame(body, style="Overlay.Body.TFrame")
            tree_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
            tree_frame.grid_columnconfigure(0, weight=1)
            tree_frame.grid_rowconfigure(0, weight=1)

            tree = ttk.Treeview(
                tree_frame,
                columns=("name", "tags", "rating"),
                show="headings",
                selectmode="browse",
                height=12,
            )
            tree.heading("name", text="Name")
            tree.heading("tags", text="Tags")
            tree.heading("rating", text="Rating")
            tree.column("name", width=200, anchor="w")
            tree.column("tags", width=120, anchor="w")
            tree.column("rating", width=80, anchor="center")
            tree.grid(row=0, column=0, sticky="nsew")

            scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
            tree.configure(yscrollcommand=scroll.set)
            scroll.grid(row=0, column=1, sticky="ns")

            detail = ttk.Frame(body, style="Overlay.Body.TFrame")
            detail.grid(row=0, column=1, sticky="nsew")
            detail.grid_columnconfigure(0, weight=1)

            title_var = tk.StringVar(value="Select a gallery entry")
            desc_var = tk.StringVar(value="Pick a curated loadout to review its details.")
            tags_var = tk.StringVar(value="")
            stats_var = tk.StringVar(value="")

            ttk.Label(detail, textvariable=title_var, style="Overlay.Title.TLabel").grid(
                row=0, column=0, sticky="w"
            )
            ttk.Label(detail, textvariable=desc_var, style="Overlay.Subtitle.TLabel").grid(
                row=1, column=0, sticky="w", pady=(6, 0)
            )
            ttk.Label(detail, textvariable=tags_var, style="Overlay.Subtitle.TLabel").grid(
                row=2, column=0, sticky="w", pady=(4, 0)
            )
            ttk.Label(detail, textvariable=stats_var, style="Overlay.Subtitle.TLabel").grid(
                row=3, column=0, sticky="w", pady=(4, 0)
            )

            publish_frame = ttk.LabelFrame(
                detail,
                text="Publish current loadout",
                padding=(12, 10),
                style="CommandCenter.Section.TLabelframe",
            )
            publish_frame.grid(row=4, column=0, sticky="ew", pady=(16, 0))
            publish_frame.grid_columnconfigure(0, weight=1)

            publish_name = tk.StringVar(value=self._active_loadout_name)
            publish_tags = tk.StringVar(value="")
            ttk.Label(publish_frame, text="Name").grid(row=0, column=0, sticky="w")
            ttk.Entry(publish_frame, textvariable=publish_name).grid(row=1, column=0, sticky="ew")
            ttk.Label(publish_frame, text="Tags (comma separated)").grid(row=2, column=0, sticky="w", pady=(8, 0))
            ttk.Entry(publish_frame, textvariable=publish_tags).grid(row=3, column=0, sticky="ew")
            ttk.Label(publish_frame, text="Description").grid(row=4, column=0, sticky="w", pady=(8, 0))
            desc_text = tk.Text(publish_frame, height=4, wrap="word")
            desc_text.grid(row=5, column=0, sticky="ew")
            desc_text.insert("1.0", f"Captured from loadout '{self._active_loadout_name}'.")

            def refresh_tree(select_id: Optional[str] = None) -> None:
                tree.delete(*tree.get_children())
                for entry in self.loadout_gallery:
                    tags_display = ", ".join(entry.get("tags", [])[:3])
                    tree.insert(
                        "",
                        "end",
                        iid=str(entry.get("id")),
                        values=(entry.get("name"), tags_display, f"{float(entry.get('rating', 0.0)):.1f}"),
                    )
                target = select_id or self._gallery_selection
                if target:
                    try:
                        tree.selection_set(target)
                        tree.focus(target)
                    except tk.TclError:
                        pass

            def update_detail(entry_id: Optional[str]) -> None:
                entry = self._find_gallery_entry(entry_id or "")
                if not entry:
                    title_var.set("Select a gallery entry")
                    desc_var.set("Pick a curated loadout to review its details.")
                    tags_var.set("")
                    stats_var.set("")
                    return
                title_var.set(entry.get("name", "Untitled"))
                desc = entry.get("description", "") or "No description provided."
                desc_var.set(desc)
                tags = entry.get("tags", [])
                tags_var.set("Tags: " + (", ".join(tags) if tags else "None"))
                loadout = entry.get("loadout", {})
                stats_var.set(
                    f"Entries: {len(loadout)} | Rating: {float(entry.get('rating', 0.0)):.1f} | Downloads: {int(entry.get('downloads', 0))}"
                )

            def on_select(_event=None) -> None:
                selection = tree.selection()
                if selection:
                    self._gallery_selection = selection[0]
                else:
                    self._gallery_selection = None
                update_detail(self._gallery_selection)

            tree.bind("<<TreeviewSelect>>", on_select)

            def publish_entry() -> None:
                name = publish_name.get().strip()
                if not name:
                    self._show_error_overlay("Loadout Gallery", "Provide a name before publishing.")
                    return
                mapping = self.loadouts.get(self._active_loadout_name)
                if not mapping:
                    mapping = {str(item.path): bool(item.include) for item in self.items}
                if not mapping:
                    self._show_warning_overlay(
                        "Loadout Gallery",
                        "No files are associated with the active loadout yet.",
                    )
                    return
                tags = [token.strip() for token in publish_tags.get().split(",") if token.strip()]
                description = desc_text.get("1.0", "end").strip()
                entry_id = uuid.uuid4().hex
                now = datetime.utcnow().isoformat()
                self.loadout_gallery.append(
                    {
                        "id": entry_id,
                        "name": name,
                        "description": description,
                        "tags": tags,
                        "created": now,
                        "updated": now,
                        "rating": 0.0,
                        "downloads": 0,
                        "loadout": dict(mapping),
                    }
                )
                self.loadout_gallery.sort(key=lambda item: str(item.get("name")).lower())
                self._save_loadout_gallery()
                refresh_tree(entry_id)
                update_detail(entry_id)
                self._show_info_overlay("Loadout Gallery", f"Published '{name}' to the gallery.")

            ttk.Button(publish_frame, text="Publish", command=publish_entry).grid(
                row=6, column=0, sticky="e", pady=(10, 0)
            )

            def import_selected(apply: bool) -> None:
                selection = tree.selection()
                if not selection:
                    self._show_info_overlay("Loadout Gallery", "Select a loadout to import first.")
                    return
                entry = self._find_gallery_entry(selection[0])
                if not entry:
                    self._show_error_overlay("Loadout Gallery", "Selected entry is no longer available.")
                    refresh_tree()
                    return
                mapping = entry.get("loadout", {})
                if not isinstance(mapping, dict):
                    self._show_error_overlay("Loadout Gallery", "This entry has no loadout data.")
                    return
                base_name = str(entry.get("name") or "Imported Loadout").strip() or "Imported Loadout"
                target_name = base_name
                counter = 1
                while target_name in self.loadouts:
                    counter += 1
                    target_name = f"{base_name} ({counter})"
                self.loadouts[target_name] = {str(path): bool(flag) for path, flag in mapping.items()}
                self._active_loadout_name = target_name
                self.loadout_var.set(target_name)
                entry["downloads"] = int(entry.get("downloads", 0)) + 1
                entry["updated"] = datetime.utcnow().isoformat()
                self._refresh_loadout_controls()
                self._save_loadouts_to_disk()
                self._save_loadout_gallery()
                if apply:
                    self._apply_loadout_to_items(target_name)
                self._show_info_overlay(
                    "Loadout Gallery",
                    f"Imported loadout '{target_name}'.",
                )
                refresh_tree(selection[0])
                update_detail(selection[0])

            def delete_selected() -> None:
                selection = tree.selection()
                if not selection:
                    self._show_info_overlay("Loadout Gallery", "Select a loadout to remove first.")
                    return
                entry = self._find_gallery_entry(selection[0])
                if not entry:
                    refresh_tree()
                    return
                if not self._ask_yes_no_overlay(
                    "Remove Entry",
                    f"Remove '{entry.get('name', 'Untitled')}' from the gallery?",
                    default="no",
                ):
                    return
                self.loadout_gallery = [item for item in self.loadout_gallery if item is not entry]
                self._save_loadout_gallery()
                self._gallery_selection = None
                refresh_tree()
                update_detail(None)

            ttk.Button(footer, text="Import", command=lambda: import_selected(False)).pack(
                side="left", padx=(0, 8)
            )
            ttk.Button(footer, text="Import & Apply", command=lambda: import_selected(True)).pack(
                side="left", padx=(0, 8)
            )
            ttk.Button(footer, text="Remove", command=delete_selected).pack(side="left")
            ttk.Button(footer, text="Close", command=lambda: self._hide_overlay_panel("loadout_gallery")).pack(
                side="right"
            )

            refresh_tree()
            update_detail(self._gallery_selection)

        self._show_overlay_panel("loadout_gallery", "Community Loadout Gallery", builder, width=880, height=560)

    def show_plugin_gallery(self) -> None:
        def builder(body: ttk.Frame, footer: ttk.Frame) -> None:
            if body is None or footer is None:
                return
            body.grid_columnconfigure(0, weight=1)
            body.grid_columnconfigure(1, weight=2)
            body.grid_rowconfigure(0, weight=1)

            tree_frame = ttk.Frame(body, style="Overlay.Body.TFrame")
            tree_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
            tree_frame.grid_columnconfigure(0, weight=1)
            tree_frame.grid_rowconfigure(0, weight=1)

            tree = ttk.Treeview(
                tree_frame,
                columns=("name", "tags", "rating"),
                show="headings",
                selectmode="browse",
                height=12,
            )
            tree.heading("name", text="Plugin")
            tree.heading("tags", text="Tags")
            tree.heading("rating", text="Rating")
            tree.column("name", width=220, anchor="w")
            tree.column("tags", width=150, anchor="w")
            tree.column("rating", width=80, anchor="center")
            tree.grid(row=0, column=0, sticky="nsew")

            scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
            tree.configure(yscrollcommand=scroll.set)
            scroll.grid(row=0, column=1, sticky="ns")

            detail = ttk.Frame(body, style="Overlay.Body.TFrame")
            detail.grid(row=0, column=1, sticky="nsew")
            detail.grid_columnconfigure(0, weight=1)

            title_var = tk.StringVar(value="Select a plugin")
            desc_var = tk.StringVar(value="Browse curated extensions and discover new tools.")
            meta_var = tk.StringVar(value="")
            tags_var = tk.StringVar(value="")
            version_var = tk.StringVar(value="")
            status_var = tk.StringVar(value="")
            rating_var = tk.StringVar(value="")
            user_rating_var = tk.StringVar(value="Your rating: –")
            requires_var = tk.StringVar(value="")

            ttk.Label(detail, textvariable=title_var, style="Overlay.Title.TLabel").grid(
                row=0, column=0, sticky="w"
            )
            ttk.Label(detail, textvariable=desc_var, style="Overlay.Subtitle.TLabel", wraplength=380).grid(
                row=1, column=0, sticky="w", pady=(6, 0)
            )
            ttk.Label(detail, textvariable=meta_var, style="Overlay.Subtitle.TLabel").grid(
                row=2, column=0, sticky="w", pady=(4, 0)
            )
            ttk.Label(detail, textvariable=tags_var, style="Overlay.Subtitle.TLabel").grid(
                row=3, column=0, sticky="w", pady=(4, 0)
            )
            ttk.Label(detail, textvariable=version_var, style="Overlay.Subtitle.TLabel").grid(
                row=4, column=0, sticky="w", pady=(4, 0)
            )
            ttk.Label(detail, textvariable=status_var, style="Overlay.Subtitle.TLabel").grid(
                row=5, column=0, sticky="w", pady=(4, 0)
            )
            ttk.Label(detail, textvariable=rating_var, style="Overlay.Subtitle.TLabel").grid(
                row=6, column=0, sticky="w", pady=(4, 0)
            )
            ttk.Label(detail, textvariable=user_rating_var, style="Overlay.Subtitle.TLabel").grid(
                row=7, column=0, sticky="w", pady=(4, 0)
            )
            ttk.Label(detail, textvariable=requires_var, style="Overlay.Subtitle.TLabel").grid(
                row=8, column=0, sticky="w", pady=(4, 0)
            )

            buttons_frame = ttk.Frame(detail, style="Overlay.Body.TFrame")
            buttons_frame.grid(row=9, column=0, sticky="w", pady=(16, 0))
            download_btn = ttk.Button(buttons_frame, text="Open download")
            download_btn.grid(row=0, column=0, padx=(0, 8))
            homepage_btn = ttk.Button(buttons_frame, text="Homepage")
            homepage_btn.grid(row=0, column=1, padx=(0, 8))
            folder_btn = ttk.Button(buttons_frame, text="Open folder")
            folder_btn.grid(row=0, column=2, padx=(0, 8))

            installed_map: Dict[str, PluginStatus] = {}
            if self.plugin_manager:
                for status in self.plugin_manager.get_statuses():
                    installed_map[status.folder] = status

            def refresh_tree(select_id: Optional[str] = None) -> None:
                tree.delete(*tree.get_children())
                for entry in self.plugin_catalog:
                    entry_id = str(entry.get("id"))
                    if not entry_id:
                        continue
                    tags_display = ", ".join(entry.get("tags", [])[:3])
                    rating_value = max(
                        float(entry.get("rating", 0.0)),
                        self._user_rating_for(entry_id),
                    )
                    tree.insert(
                        "",
                        "end",
                        iid=entry_id,
                        values=(entry.get("name"), tags_display, f"{rating_value:.1f}"),
                    )
                target = select_id or self._plugin_gallery_selection
                if target:
                    try:
                        tree.selection_set(target)
                        tree.focus(target)
                    except tk.TclError:
                        pass

            def update_detail(entry_id: Optional[str]) -> None:
                entry = next((item for item in self.plugin_catalog if str(item.get("id")) == entry_id), None)
                if not entry:
                    title_var.set("Select a plugin")
                    desc_var.set("Browse curated extensions and discover new tools.")
                    meta_var.set("")
                    tags_var.set("")
                    version_var.set("")
                    status_var.set("")
                    rating_var.set("")
                    user_rating_var.set("Your rating: –")
                    requires_var.set("")
                    for widget in (download_btn, homepage_btn, folder_btn):
                        widget.configure(state="disabled", command=lambda: None)
                    return

                entry_id_str = str(entry.get("id"))
                title_var.set(entry.get("name", "Unknown plugin"))
                desc = entry.get("description", "") or "No description provided."
                desc_var.set(desc)
                author = entry.get("author", "Unknown")
                folder_name = str(entry.get("folder") or entry_id_str)
                meta_var.set(f"Author: {author} | Folder: {folder_name}")
                tags = entry.get("tags", [])
                tags_var.set("Tags: " + (", ".join(tags) if tags else "None"))
                catalog_version = str(entry.get("version") or "Unknown")
                version_var.set(f"Catalog version: {catalog_version}")

                installed = installed_map.get(folder_name)
                if installed:
                    installed_version = installed.version or "Unknown"
                    status_text = installed.status.title() if installed.status else "Installed"
                    status_var.set(f"Installed: {installed_version} ({status_text})")
                else:
                    status_var.set("Installed: Not installed")

                community_rating = float(entry.get("rating", 0.0))
                user_rating = self._user_rating_for(entry_id_str)
                rating_var.set(f"Community rating: {max(community_rating, user_rating):.1f}/5")
                if user_rating:
                    user_rating_var.set(f"Your rating: {user_rating:.1f}/5")
                else:
                    user_rating_var.set("Your rating: –")

                requires = [str(item).strip() for item in entry.get("requires", []) if str(item).strip()]
                requires_var.set("Requires: " + (", ".join(requires) if requires else "None"))

                download_url = str(entry.get("download_url") or "")
                homepage_url = str(entry.get("homepage") or "")
                folder_exists = bool(folder_name) and (USER_PLUGINS_DIR / folder_name).exists()

                download_btn.configure(
                    state="normal" if download_url else "disabled",
                    command=(lambda url=download_url: webbrowser.open(url) if url else None),
                )
                homepage_btn.configure(
                    state="normal" if homepage_url else "disabled",
                    command=(lambda url=homepage_url: webbrowser.open(url) if url else None),
                )
                folder_btn.configure(
                    state="normal" if folder_exists else "disabled",
                    command=(
                        lambda name=folder_name: self._open_plugin_folder(name)
                        if name and folder_exists
                        else None
                    ),
                )

            def on_select(_event=None) -> None:
                selection = tree.selection()
                if selection:
                    self._plugin_gallery_selection = selection[0]
                else:
                    self._plugin_gallery_selection = None
                update_detail(self._plugin_gallery_selection)

            tree.bind("<<TreeviewSelect>>", on_select)

            ttk.Button(
                footer,
                text="Close",
                command=lambda: self._hide_overlay_panel("plugin_gallery"),
            ).pack(side="right")

            refresh_tree()
            update_detail(self._plugin_gallery_selection)

        self._show_overlay_panel("plugin_gallery", "Plugin Gallery", builder, width=880, height=560)

    def show_plugin_marketplace(self) -> None:
        def builder(body: ttk.Frame, footer: ttk.Frame) -> None:
            if body is None or footer is None:
                return
            body.grid_columnconfigure(0, weight=1)
            body.grid_columnconfigure(1, weight=2)
            body.grid_rowconfigure(0, weight=1)

            tree_frame = ttk.Frame(body, style="Overlay.Body.TFrame")
            tree_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
            tree_frame.grid_columnconfigure(0, weight=1)
            tree_frame.grid_rowconfigure(0, weight=1)

            tree = ttk.Treeview(
                tree_frame,
                columns=("name", "status", "rating"),
                show="headings",
                selectmode="browse",
                height=12,
            )
            tree.heading("name", text="Plugin")
            tree.heading("status", text="Status")
            tree.heading("rating", text="Rating")
            tree.column("name", width=220, anchor="w")
            tree.column("status", width=120, anchor="center")
            tree.column("rating", width=80, anchor="center")
            tree.grid(row=0, column=0, sticky="nsew")

            scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
            tree.configure(yscrollcommand=scroll.set)
            scroll.grid(row=0, column=1, sticky="ns")

            detail = ttk.Frame(body, style="Overlay.Body.TFrame")
            detail.grid(row=0, column=1, sticky="nsew")
            detail.grid_columnconfigure(0, weight=1)

            title_var = tk.StringVar(value="Select a plugin")
            desc_var = tk.StringVar(value="Browse curated extensions and manage ratings.")
            meta_var = tk.StringVar(value="")
            version_var = tk.StringVar(value="")
            user_rating_var = tk.StringVar(value="Your rating: –")

            ttk.Label(detail, textvariable=title_var, style="Overlay.Title.TLabel").grid(
                row=0, column=0, sticky="w"
            )
            ttk.Label(detail, textvariable=desc_var, style="Overlay.Subtitle.TLabel").grid(
                row=1, column=0, sticky="w", pady=(6, 0)
            )
            ttk.Label(detail, textvariable=meta_var, style="Overlay.Subtitle.TLabel").grid(
                row=2, column=0, sticky="w", pady=(4, 0)
            )
            ttk.Label(detail, textvariable=version_var, style="Overlay.Subtitle.TLabel").grid(
                row=3, column=0, sticky="w", pady=(4, 0)
            )
            ttk.Label(detail, textvariable=user_rating_var, style="Overlay.Subtitle.TLabel").grid(
                row=4, column=0, sticky="w", pady=(4, 0)
            )

            rating_var = tk.DoubleVar(value=0.0)
            rating_frame = ttk.Frame(detail, style="Overlay.Body.TFrame")
            rating_frame.grid(row=5, column=0, sticky="ew", pady=(12, 0))
            ttk.Label(rating_frame, text="Set rating (0-5)").grid(row=0, column=0, sticky="w")
            rating_scale = ttk.Scale(rating_frame, from_=0.0, to=5.0, orient="horizontal", variable=rating_var)
            rating_scale.grid(row=1, column=0, sticky="ew")

            buttons_frame = ttk.Frame(detail, style="Overlay.Body.TFrame")
            buttons_frame.grid(row=6, column=0, sticky="w", pady=(16, 0))
            install_btn = ttk.Button(buttons_frame, text="Open download")
            install_btn.grid(row=0, column=0, padx=(0, 8))
            homepage_btn = ttk.Button(buttons_frame, text="Homepage")
            homepage_btn.grid(row=0, column=1, padx=(0, 8))
            folder_btn = ttk.Button(buttons_frame, text="Open folder")
            folder_btn.grid(row=0, column=2, padx=(0, 8))
            rate_btn = ttk.Button(buttons_frame, text="Save rating")
            rate_btn.grid(row=0, column=3)

            catalog_by_id = {str(entry.get("id")): entry for entry in self.plugin_catalog}
            catalog_by_folder = {str(entry.get("folder")): entry for entry in self.plugin_catalog}
            installed_map: Dict[str, PluginStatus] = {}
            if self.plugin_manager:
                for status in self.plugin_manager.get_statuses():
                    installed_map[status.folder] = status

            rows: List[Dict[str, object]] = []
            for entry_id, entry in catalog_by_id.items():
                folder = str(entry.get("folder"))
                installed = installed_map.get(folder)
                installed_version = installed.version if installed else ""
                catalog_version = str(entry.get("version") or "")
                if installed and catalog_version:
                    comparison = _compare_versions(installed_version or "0", catalog_version)
                    if comparison < 0:
                        status_text = "Update available"
                    elif comparison > 0:
                        status_text = "Ahead of catalog"
                    else:
                        status_text = "Up to date"
                elif installed:
                    status_text = installed.status.title()
                else:
                    status_text = "Not installed"
                rating = max(float(entry.get("rating", 0.0)), self._user_rating_for(entry_id))
                rows.append(
                    {
                        "id": entry_id,
                        "name": entry.get("name"),
                        "status": status_text,
                        "rating": rating,
                        "catalog": entry,
                        "installed": installed,
                    }
                )

            for folder, status in installed_map.items():
                if folder not in catalog_by_folder:
                    entry_id = f"installed::{folder}"
                    rows.append(
                        {
                            "id": entry_id,
                            "name": status.name,
                            "status": status.status.title(),
                            "rating": 0.0,
                            "catalog": None,
                            "installed": status,
                        }
                    )

            def refresh_tree(select_id: Optional[str] = None) -> None:
                tree.delete(*tree.get_children())
                for record in rows:
                    tree.insert(
                        "",
                        "end",
                        iid=str(record["id"]),
                        values=(record.get("name"), record.get("status"), f"{record.get('rating', 0.0):.1f}"),
                    )
                target = select_id or self._plugin_marketplace_selection
                if target:
                    try:
                        tree.selection_set(target)
                        tree.focus(target)
                    except tk.TclError:
                        pass

            def update_detail(entry_id: Optional[str]) -> None:
                record = next((item for item in rows if str(item.get("id")) == entry_id), None)
                if not record:
                    title_var.set("Select a plugin")
                    desc_var.set("Browse curated extensions and manage ratings.")
                    meta_var.set("")
                    version_var.set("")
                    user_rating_var.set("Your rating: –")
                    rating_var.set(0.0)
                    for widget in (install_btn, homepage_btn, folder_btn, rate_btn):
                        widget.configure(state="disabled")
                    return
                catalog = record.get("catalog")
                installed = record.get("installed")
                title_var.set(str(record.get("name") or "Unknown plugin"))
                if isinstance(catalog, dict):
                    desc = catalog.get("description", "") or "No description provided."
                    desc_var.set(desc)
                    tags = catalog.get("tags", [])
                    meta_var.set(
                        "Tags: " + (", ".join(tags) if tags else "None") + f" | Author: {catalog.get('author', 'Unknown')}"
                    )
                    catalog_version = str(catalog.get("version") or "Unknown")
                else:
                    desc_var.set("Installed plugin not in catalog.")
                    meta_var.set("Tags: – | Author: –")
                    catalog_version = "Unknown"
                installed_version = getattr(installed, "version", "") or "Not installed"
                version_var.set(f"Catalog version: {catalog_version} | Installed: {installed_version}")
                entry_id = str(record.get("id"))
                user_rating = self._user_rating_for(entry_id)
                if user_rating:
                    user_rating_var.set(f"Your rating: {user_rating:.1f}/5")
                else:
                    user_rating_var.set("Your rating: –")
                rating_var.set(user_rating or float(record.get("rating", 0.0)))

                download_url = str(catalog.get("download_url") if isinstance(catalog, dict) else "")
                homepage_url = str(catalog.get("homepage") if isinstance(catalog, dict) else "")
                folder_name = str(getattr(installed, "folder", ""))

                install_btn.configure(
                    state="normal" if download_url else "disabled",
                    command=(lambda url=download_url: webbrowser.open(url) if url else None),
                )
                homepage_btn.configure(
                    state="normal" if homepage_url else "disabled",
                    command=(lambda url=homepage_url: webbrowser.open(url) if url else None),
                )
                folder_btn.configure(
                    state="normal" if folder_name else "disabled",
                    command=(lambda name=folder_name: self._open_plugin_folder(name) if name else None),
                )

                def save_rating() -> None:
                    self.plugin_ratings[entry_id] = {
                        "rating": float(rating_var.get()),
                        "notes": "",
                        "updated": datetime.utcnow().isoformat(),
                    }
                    self._save_plugin_ratings()
                    user_rating_var.set(f"Your rating: {float(rating_var.get()):.1f}/5")
                    record["rating"] = max(float(record.get("rating", 0.0)), float(rating_var.get()))
                    refresh_tree(entry_id)

                rate_btn.configure(state="normal", command=save_rating)

            def on_select(_event=None) -> None:
                selection = tree.selection()
                if selection:
                    self._plugin_marketplace_selection = selection[0]
                else:
                    self._plugin_marketplace_selection = None
                update_detail(self._plugin_marketplace_selection)

            tree.bind("<<TreeviewSelect>>", on_select)

            ttk.Button(footer, text="Close", command=lambda: self._hide_overlay_panel("plugin_marketplace")).pack(
                side="right"
            )

            refresh_tree()
            update_detail(self._plugin_marketplace_selection)

        self._show_overlay_panel("plugin_marketplace", "Plugin Marketplace", builder, width=920, height=580)

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
        dashboard = getattr(self, "command_center", None)
        if dashboard is not None:
            dashboard.refresh()

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
            self._show_error_overlay("New Loadout", f"A loadout named '{name}' already exists.")
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
            self._show_error_overlay("Rename Loadout", f"A loadout named '{new_name}' already exists.")
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
        if not self._ask_yes_no_overlay("Delete Loadout", f"Delete loadout '{target}'?"):
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
            self._show_info_overlay("Apply Loadout", "Select a loadout to apply first.")
            return
        if target not in self.loadouts:
            self._show_error_overlay("Apply Loadout", f"Loadout '{target}' is no longer available.")
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
        style.configure("DialogOverlay.TFrame", background=palette["alt"])
        style.configure("DialogOverlayIcon.TLabel", background=palette["alt"], foreground=palette["accent"])
        style.configure("DialogOverlayHeadline.TLabel", background=palette["alt"], foreground=palette["fg"])
        style.configure("DialogOverlayMessage.TLabel", background=palette["alt"], foreground=palette["fg"])
        style.configure("DialogOverlayButtons.TFrame", background=palette["alt"])
        style.configure(
            "PluginStatus.Overlay.TFrame",
            background=palette["alt"],
            borderwidth=1,
            relief="solid",
        )
        style.configure("PluginStatus.Header.TFrame", background=palette["alt"])
        style.configure(
            "PluginStatus.Title.TLabel",
            background=palette["alt"],
            foreground=palette["fg"],
            font=("TkDefaultFont", 14, "bold"),
        )
        style.configure(
            "PluginStatus.Summary.TLabel",
            background=palette["alt"],
            foreground=palette["fg"],
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
            "Sidebar.Vertical.TScrollbar",
            gripcount=0,
            background=sidebar_bg,
            troughcolor=palette["sel"],
            bordercolor=palette["sel"],
            arrowcolor=palette["fg"],
        )
        style.map(
            "Sidebar.Vertical.TScrollbar",
            background=[("active", palette["accent"]), ("pressed", palette["accent"])],
            arrowcolor=[("active", palette["fg"]), ("pressed", palette["fg"])],
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
        command_center_btn = ttk.Button(top, text="Command Center", command=self.open_command_center)
        command_center_btn.pack(side="left", padx=4)
        self._toolbar_widgets["command_center"] = command_center_btn

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

        def _sidebar_contains(widget: Optional[tk.Widget]) -> bool:
            while widget is not None:
                if widget in (sidebar_canvas, sidebar_frame):
                    return True
                widget = getattr(widget, "master", None)
            return False

        def _scroll_sidebar(units: int) -> None:
            if units:
                sidebar_canvas.yview_scroll(units, "units")

        def _on_sidebar_mousewheel(event) -> Optional[str]:
            widget = sidebar_canvas.winfo_containing(event.x_root, event.y_root)
            if not _sidebar_contains(widget):
                return None
            if event.delta:
                delta = int(-event.delta / 120)
                if delta == 0:
                    delta = -1 if event.delta > 0 else 1
                _scroll_sidebar(delta)
            elif event.num == 4:
                _scroll_sidebar(-1)
            elif event.num == 5:
                _scroll_sidebar(1)
            return "break"

        sidebar_canvas.bind_all("<MouseWheel>", _on_sidebar_mousewheel, add="+")
        sidebar_canvas.bind_all("<Button-4>", lambda e: _on_sidebar_mousewheel(e) or "break", add="+")
        sidebar_canvas.bind_all("<Button-5>", lambda e: _on_sidebar_mousewheel(e) or "break", add="+")

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
            text="Compare Duplicates",
            command=self.show_duplicate_comparison,
            style="Sidebar.TButton",
        ).pack(fill="x", pady=(6, 0))
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
        ttk.Button(
            loadout_section,
            text="Loadout Gallery…",
            command=self.show_loadout_gallery,
            style="Sidebar.TButton",
        ).pack(fill="x", pady=(6, 0))

        plugin_gallery_section = ttk.LabelFrame(
            sidebar_frame,
            text="Plugins",
            padding=(12, 10),
            style="Sidebar.TLabelframe",
        )
        plugin_gallery_section.pack(fill="x", pady=(0, 10))
        ttk.Button(
            plugin_gallery_section,
            text="Plugin Gallery…",
            command=self.show_plugin_gallery,
            style="Sidebar.TButton",
        ).pack(fill="x")
        ttk.Button(
            plugin_gallery_section,
            text="Plugin Marketplace…",
            command=self.show_plugin_marketplace,
            style="Sidebar.TButton",
        ).pack(fill="x", pady=(6, 0))

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

        scroll_container = ttk.Frame(self.settings_sidebar)
        scroll_container.grid(row=0, column=0, sticky="nsew")
        scroll_container.columnconfigure(0, weight=1)
        scroll_container.rowconfigure(0, weight=1)

        settings_canvas = tk.Canvas(
            scroll_container,
            highlightthickness=0,
            borderwidth=0,
            bg=palette.get("sel", "#2A2F3A"),
        )
        settings_canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(
            scroll_container,
            orient="vertical",
            command=settings_canvas.yview,
            style="Sidebar.Vertical.TScrollbar",
        )
        scrollbar.grid(row=0, column=1, sticky="ns")
        settings_canvas.configure(yscrollcommand=scrollbar.set)

        container = ttk.Frame(settings_canvas, padding=(16, 20, 16, 20))
        window_id = settings_canvas.create_window((0, 0), window=container, anchor="nw")
        container.columnconfigure(0, weight=1)

        def _configure_settings_scroll_region(_event=None) -> None:
            settings_canvas.configure(scrollregion=settings_canvas.bbox("all"))
            settings_canvas.itemconfigure(window_id, width=settings_canvas.winfo_width())

        container.bind("<Configure>", _configure_settings_scroll_region)
        settings_canvas.bind("<Configure>", lambda event: settings_canvas.itemconfigure(window_id, width=event.width))
        container.update_idletasks()
        _configure_settings_scroll_region()

        def _settings_contains(widget: Optional[tk.Widget]) -> bool:
            while widget is not None:
                if widget in (self.settings_sidebar, settings_canvas, container):
                    return True
                widget = getattr(widget, "master", None)
            return False

        def _scroll_settings(units: int) -> None:
            if units:
                settings_canvas.yview_scroll(units, "units")

        def _on_settings_mousewheel(event) -> Optional[str]:
            if not self.settings_sidebar.winfo_ismapped():
                return None
            widget = settings_canvas.winfo_containing(event.x_root, event.y_root)
            if not _settings_contains(widget):
                return None
            if event.delta:
                delta = int(-event.delta / 120)
                if delta == 0:
                    delta = -1 if event.delta > 0 else 1
                _scroll_settings(delta)
            elif getattr(event, "num", None) == 4:
                _scroll_settings(-1)
            elif getattr(event, "num", None) == 5:
                _scroll_settings(1)
            return "break"

        settings_canvas.bind_all("<MouseWheel>", _on_settings_mousewheel, add="+")
        settings_canvas.bind_all("<Button-4>", lambda e: _on_settings_mousewheel(e) or "break", add="+")
        settings_canvas.bind_all("<Button-5>", lambda e: _on_settings_mousewheel(e) or "break", add="+")

        self.settings_canvas = settings_canvas
        self.settings_scrollbar = scrollbar

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

        launch_section = ttk.Frame(container)
        launch_section.grid(row=row, column=0, sticky="ew", pady=(18, 0))
        launch_section.columnconfigure(0, weight=1)
        ttk.Label(launch_section, text="Launch", font=("TkDefaultFont", 10, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(
            launch_section,
            text="Show Command Center on launch",
            variable=self.show_command_center_var,
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))
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

    def _ensure_update_overlay(self) -> tk.Frame:
        overlay = self._update_overlay
        if overlay and overlay.winfo_exists():
            self._refresh_update_overlay_theme()
            return overlay

        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        overlay = tk.Frame(
            self,
            bg=_scrim_color(palette.get("bg", "#111316")),
            highlightthickness=0,
            bd=0,
        )
        overlay.place_forget()
        overlay.grid_rowconfigure(0, weight=1)
        overlay.grid_columnconfigure(0, weight=1)
        overlay.bind("<Escape>", lambda _e: self._hide_update_overlay())

        container = ttk.Frame(overlay, padding=(16, 20, 16, 20), style="UpdateOverlay.TFrame")
        container.place(relx=0.5, rely=0.5, anchor="center")
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
        self._update_overlay_container = container
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
            overlay.configure(bg=_scrim_color(palette.get("bg", "#111316")))
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
            container = getattr(self, "_update_overlay_container", None)
            if container and container.winfo_exists():
                container.place_configure(relx=0.5, rely=0.5, anchor="center")

    def _center_update_overlay(self) -> None:
        overlay = getattr(self, "_update_overlay", None)
        container = getattr(self, "_update_overlay_container", None)
        if (
            not overlay
            or not overlay.winfo_exists()
            or not container
            or not container.winfo_exists()
        ):
            return
        self.update_idletasks()
        try:
            overlay.update_idletasks()
            container.update_idletasks()
            container.place_configure(relx=0.5, rely=0.5, anchor="center")
        except Exception:
            pass

    def _format_update_changelog(self) -> str:
        notes = getattr(self, "_update_release_notes", None)
        if not notes:
            return ""
        cleaned = notes.replace("\r\n", "\n").replace("\r", "\n").strip()
        if not cleaned:
            return ""
        version = getattr(self, "_latest_version", None) or APP_VERSION
        if getattr(self, "_update_available", False):
            header = "New features detected"
        else:
            header = f"What's new in version {version}"
        return f"{header}:\n{cleaned}"

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
        skip_label: Optional[str] = None,
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
            text = skip_label if skip_label else "Skip for Now"
            self._update_overlay_skip_btn.configure(state=state, text=text)
        mode_state = "normal" if enable_download else "disabled"
        if self._update_mode_simple_radio:
            self._update_mode_simple_radio.configure(state=mode_state)
        if self._update_mode_advanced_radio:
            advanced_state = mode_state
            if advanced_state == "normal" and not self._update_download_url:
                advanced_state = "disabled"
            self._update_mode_advanced_radio.configure(state=advanced_state)

        overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
        overlay.tkraise()
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
            container = getattr(self, "_update_overlay_container", None)
            if container and container.winfo_exists():
                container.focus_set()
            else:
                self.focus_set()

        self._center_update_overlay()
        self._update_overlay_visible = True

    def _hide_update_overlay(self) -> None:
        overlay = getattr(self, "_update_overlay", None)
        if overlay and overlay.winfo_exists():
            try:
                overlay.grab_release()
            except tk.TclError:
                pass
            overlay.place_forget()
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
            self._update_overlay_skip_btn.configure(state="disabled", text="Skip for Now")
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

    # Dialog overlay --------------------------------------------------

    def _ensure_dialog_overlay(self) -> tk.Toplevel:
        overlay = self._dialog_overlay
        if overlay and overlay.winfo_exists():
            self._refresh_dialog_overlay_theme()
            self._layout_dialog_overlay()
            return overlay

        overlay = tk.Toplevel(self)
        overlay.withdraw()
        overlay.overrideredirect(True)
        overlay.transient(self)
        overlay.lift()
        overlay.attributes("-topmost", True)
        scrim_color = _scrim_color(self._theme_cache.get("bg", "#111316"))
        overlay.configure(bg=scrim_color)

        scrim = tk.Frame(overlay, bg=scrim_color, borderwidth=0, highlightthickness=0)
        scrim.pack(fill="both", expand=True)

        container = ttk.Frame(scrim, padding=(24, 28, 24, 24), style="DialogOverlay.TFrame")
        container.place(relx=0.5, rely=0.5, anchor="center")
        container.columnconfigure(1, weight=1)

        icon_label = ttk.Label(container, textvariable=self._dialog_overlay_icon, style="DialogOverlayIcon.TLabel")
        icon_label.grid(row=0, column=0, rowspan=2, sticky="nw", padx=(0, 16))

        headline_label = ttk.Label(
            container,
            textvariable=self._dialog_overlay_headline,
            style="DialogOverlayHeadline.TLabel",
            anchor="w",
            justify="left",
        )
        headline_label.grid(row=0, column=1, sticky="nw")

        message_label = ttk.Label(
            container,
            textvariable=self._dialog_overlay_message,
            style="DialogOverlayMessage.TLabel",
            justify="left",
            wraplength=520,
            anchor="w",
        )
        message_label.grid(row=1, column=1, sticky="nw", pady=(8, 0))

        button_frame = ttk.Frame(container, style="DialogOverlayButtons.TFrame")
        button_frame.grid(row=2, column=0, columnspan=2, sticky="e", pady=(24, 0))

        if not self._dialog_overlay_icon_font:
            try:
                self._dialog_overlay_icon_font = tkfont.Font(size=34, weight="bold")
            except tk.TclError:
                self._dialog_overlay_icon_font = tkfont.Font()
        icon_label.configure(font=self._dialog_overlay_icon_font)

        if not self._dialog_overlay_headline_font:
            try:
                self._dialog_overlay_headline_font = tkfont.Font(size=15, weight="bold")
            except tk.TclError:
                self._dialog_overlay_headline_font = tkfont.Font(weight="bold")
        headline_label.configure(font=self._dialog_overlay_headline_font)

        overlay.bind("<Map>", lambda _e: self._layout_dialog_overlay(), add="+")

        self._dialog_overlay = overlay
        self._dialog_overlay_scrim = scrim
        self._dialog_overlay_container = container
        self._dialog_overlay_button_frame = button_frame
        self._dialog_overlay_icon_label = icon_label
        self._dialog_overlay_headline_label = headline_label
        self._dialog_overlay_message_label = message_label

        self._refresh_dialog_overlay_theme()
        self._layout_dialog_overlay()
        return overlay

    def _refresh_dialog_overlay_theme(self) -> None:
        overlay = getattr(self, "_dialog_overlay", None)
        if not overlay or not overlay.winfo_exists():
            return
        palette = self._theme_cache
        scrim_color = _scrim_color(palette.get("bg", "#111316"))
        try:
            overlay.configure(bg=scrim_color)
        except tk.TclError:
            return
        scrim = getattr(self, "_dialog_overlay_scrim", None)
        if scrim and scrim.winfo_exists():
            scrim.configure(bg=scrim_color)

        style = ttk.Style()
        style.configure("DialogOverlay.TFrame", background=palette.get("alt", palette.get("bg", "#111316")))
        style.configure("DialogOverlayIcon.TLabel", background=palette.get("alt", "#161A1E"), foreground=palette.get("accent", "#4C8BF5"))
        style.configure("DialogOverlayHeadline.TLabel", background=palette.get("alt", "#161A1E"), foreground=palette.get("fg", "#E6E6E6"))
        style.configure("DialogOverlayMessage.TLabel", background=palette.get("alt", "#161A1E"), foreground=palette.get("fg", "#E6E6E6"))
        style.configure("DialogOverlayButtons.TFrame", background=palette.get("alt", "#161A1E"))

        button_frame = getattr(self, "_dialog_overlay_button_frame", None)
        if button_frame and button_frame.winfo_exists():
            button_frame.configure(style="DialogOverlayButtons.TFrame")

    def _layout_dialog_overlay(self) -> None:
        overlay = getattr(self, "_dialog_overlay", None)
        if not overlay or not overlay.winfo_exists():
            return
        try:
            self.update_idletasks()
            overlay.update_idletasks()
        except tk.TclError:
            return
        width = max(self.winfo_width(), 1)
        height = max(self.winfo_height(), 1)
        x = self.winfo_rootx()
        y = self.winfo_rooty()
        try:
            overlay.geometry(f"{width}x{height}+{x}+{y}")
        except tk.TclError:
            return
        container = getattr(self, "_dialog_overlay_container", None)
        if container and container.winfo_exists():
            container.place_configure(relx=0.5, rely=0.5, anchor="center")

    def _on_root_configure(self, _event=None) -> None:
        if self._dialog_overlay_visible:
            self._layout_dialog_overlay()
        for key, record in list(self._overlay_registry.items()):
            if record.get("visible"):
                self._layout_overlay_panel(key)

    def _show_dialog_overlay(
        self,
        *,
        icon: str,
        title: str,
        message: str,
        buttons: Sequence[Tuple[str, str, bool]],
        default: Optional[str] = None,
        cancel: Optional[str] = None,
    ) -> str:
        overlay = self._ensure_dialog_overlay()
        self._dialog_overlay_icon.set(icon or "")
        self._dialog_overlay_headline.set(title or "")
        self._dialog_overlay_message.set(message or "")
        button_specs = list(buttons)
        if not button_specs:
            button_specs = [("OK", "ok", True)]
        if default is None and button_specs:
            default = button_specs[0][1]
        self._dialog_overlay_default = default or ""
        self._dialog_overlay_cancel = cancel or ""

        button_frame = getattr(self, "_dialog_overlay_button_frame", None)
        if button_frame and button_frame.winfo_exists():
            for child in button_frame.winfo_children():
                child.destroy()

        result_var = tk.StringVar(value="")
        self._dialog_overlay_wait_var = result_var

        def _finish(value: str) -> None:
            if result_var.get():
                return
            result_var.set(value)

        button_widgets: Dict[str, ttk.Button] = {}
        if button_frame and button_frame.winfo_exists():
            total = len(button_specs)
            for index, (text, value, accent) in enumerate(button_specs):
                style_name = "Accent.TButton" if accent else "TButton"
                btn = ttk.Button(
                    button_frame,
                    text=text,
                    style=style_name,
                    command=lambda v=value: _finish(v),
                )
                pad = (0, 8) if index < total - 1 else 0
                btn.pack(side="left", padx=pad)
                button_widgets[value] = btn

        def _handle_escape(_event=None) -> None:
            target = self._dialog_overlay_cancel or self._dialog_overlay_default or ""
            _finish(target)

        def _handle_return(_event=None) -> None:
            _finish(self._dialog_overlay_default or "")

        overlay.bind("<Escape>", _handle_escape, add="+")
        overlay.bind("<Return>", _handle_return, add="+")

        previous_grab_widget: Optional[tk.Widget] = None
        try:
            current_grab = self.grab_current()
        except tk.TclError:
            current_grab = None
        if current_grab and current_grab is not overlay:
            previous_grab_widget = current_grab
            try:
                current_grab.grab_release()
            except tk.TclError:
                previous_grab_widget = None

        self._refresh_dialog_overlay_theme()
        self._layout_dialog_overlay()
        overlay.deiconify()
        overlay.lift()
        try:
            overlay.grab_set()
        except tk.TclError:
            pass
        self._dialog_overlay_visible = True

        focus_target: Optional[ttk.Button] = None
        if self._dialog_overlay_default and self._dialog_overlay_default in button_widgets:
            focus_target = button_widgets[self._dialog_overlay_default]
        elif button_specs:
            first_value = button_specs[0][1]
            focus_target = button_widgets.get(first_value)
        try:
            if focus_target and focus_target.winfo_exists():
                focus_target.focus_set()
            else:
                overlay.focus_set()
        except tk.TclError:
            pass

        try:
            self.wait_variable(result_var)
        except tk.TclError:
            if not result_var.get():
                fallback = self._dialog_overlay_cancel or self._dialog_overlay_default or ""
                result_var.set(fallback)
        finally:
            self._dialog_overlay_visible = False
            self._dialog_overlay_wait_var = None
            try:
                overlay.grab_release()
            except tk.TclError:
                pass
            overlay.unbind("<Escape>")
            overlay.unbind("<Return>")
            try:
                overlay.withdraw()
            except tk.TclError:
                pass
            if previous_grab_widget is not None and previous_grab_widget.winfo_exists():
                try:
                    previous_grab_widget.grab_set()
                except tk.TclError:
                    pass
                try:
                    previous_grab_widget.focus_set()
                except tk.TclError:
                    pass

        return result_var.get()

    def _show_info_overlay(self, title: str, message: str) -> None:
        self._show_dialog_overlay(
            icon="ℹ️",
            title=title,
            message=message,
            buttons=[("OK", "ok", True)],
            default="ok",
            cancel="ok",
        )

    def _show_warning_overlay(self, title: str, message: str) -> None:
        self._show_dialog_overlay(
            icon="⚠️",
            title=title,
            message=message,
            buttons=[("OK", "ok", True)],
            default="ok",
            cancel="ok",
        )

    def _show_error_overlay(self, title: str, message: str) -> None:
        self._show_dialog_overlay(
            icon="❌",
            title=title,
            message=message,
            buttons=[("OK", "ok", True)],
            default="ok",
            cancel="ok",
        )

    def _ask_yes_no_overlay(self, title: str, message: str, *, default: str = "yes") -> bool:
        default = default.lower()
        if default not in {"yes", "no"}:
            default = "yes"
        cancel = "no"
        result = self._show_dialog_overlay(
            icon="❓",
            title=title,
            message=message,
            buttons=[("Yes", "yes", True), ("No", "no", False)],
            default="yes" if default == "yes" else "no",
            cancel=cancel,
        )
        return result.lower() == "yes"

    # ------------------------------------------------------------------
    # Generic overlay panels
    # ------------------------------------------------------------------

    def _ensure_overlay_panel(
        self,
        key: str,
        title: str,
        *,
        width: int = 760,
        height: int = 520,
    ) -> Dict[str, object]:
        entry = self._overlay_registry.get(key)
        scrim = entry.get("window") if isinstance(entry, dict) else None
        if scrim is not None and scrim.winfo_exists():
            title_var = entry.get("title_var")
            if isinstance(title_var, tk.StringVar):
                title_var.set(title)
            entry["size"] = (width, height)
            return entry

        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        overlay = tk.Frame(self, bg=_scrim_color(palette.get("bg", "#111316")))
        overlay.configure(takefocus=True)
        overlay.place_forget()
        overlay.grid_rowconfigure(0, weight=1)
        overlay.grid_columnconfigure(0, weight=1)
        def _on_scrim_click(event: tk.Event) -> str:
            if event.widget is overlay:
                self._hide_overlay_panel(key)
            return "break"

        overlay.bind("<Button-1>", _on_scrim_click)

        shell = ttk.Frame(overlay, style="Overlay.Shell.TFrame", padding=0)
        shell.place(relx=0.5, rely=0.5, anchor="center")
        shell.grid_rowconfigure(0, weight=1)
        shell.grid_columnconfigure(0, weight=1)
        shell.configure(width=width, height=height)
        shell.grid_propagate(False)

        container = ttk.Frame(shell, style="Overlay.Container.TFrame", padding=(24, 22, 24, 24))
        container.grid(row=0, column=0, sticky="nsew")
        container.grid_rowconfigure(2, weight=1)
        container.grid_columnconfigure(0, weight=1)

        title_var = tk.StringVar(value=title)
        header = ttk.Frame(container, style="Overlay.Header.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        title_label = ttk.Label(header, textvariable=title_var, style="Overlay.Title.TLabel")
        title_label.pack(side="left")
        close_btn = ttk.Button(
            header,
            text="✖",
            width=3,
            style="Overlay.Close.TButton",
            command=lambda: self._hide_overlay_panel(key),
        )
        close_btn.pack(side="right", padx=(12, 0))

        body = ttk.Frame(container, style="Overlay.Body.TFrame")
        body.grid(row=1, column=0, sticky="nsew", pady=(18, 0))
        body.grid_rowconfigure(0, weight=1)
        body.grid_columnconfigure(0, weight=1)

        footer = ttk.Frame(container, style="Overlay.Footer.TFrame")
        footer.grid(row=2, column=0, sticky="ew", pady=(18, 0))

        overlay.bind("<KeyPress-Escape>", lambda _e: self._hide_overlay_panel(key))

        record = {
            "window": overlay,
            "shell": shell,
            "container": container,
            "title_var": title_var,
            "body": body,
            "footer": footer,
            "visible": False,
            "size": (width, height),
        }
        self._overlay_registry[key] = record
        self._apply_overlay_theme(record)
        self._layout_overlay_panel(key)
        return record

    def _layout_overlay_panel(self, key: str) -> None:
        record = self._overlay_registry.get(key)
        if not record:
            return
        scrim = record.get("window")
        shell = record.get("shell")
        if not scrim or not shell or not scrim.winfo_exists():
            return
        width, height = record.get("size", (shell.winfo_width(), shell.winfo_height()))
        try:
            shell.configure(width=width, height=height)
        except tk.TclError:
            pass
        scrim.update_idletasks()
        if record.get("visible"):
            try:
                scrim.place_configure(relx=0, rely=0, relwidth=1, relheight=1)
            except tk.TclError:
                pass
        try:
            shell.place_configure(relx=0.5, rely=0.5, anchor="center")
        except tk.TclError:
            pass

    def _show_overlay_panel(
        self,
        key: str,
        title: str,
        builder: Callable[[ttk.Frame, ttk.Frame], None],
        *,
        width: int = 760,
        height: int = 520,
    ) -> None:
        record = self._ensure_overlay_panel(key, title, width=width, height=height)
        body = record.get("body")
        footer = record.get("footer")
        if isinstance(body, ttk.Frame):
            for child in list(body.winfo_children()):
                child.destroy()
        if isinstance(footer, ttk.Frame):
            for child in list(footer.winfo_children()):
                child.destroy()
        if callable(builder):
            try:
                builder(body, footer)
            except Exception as exc:
                if isinstance(body, ttk.Frame):
                    ttk.Label(body, text=f"Unable to render overlay: {exc}").grid(
                        row=0, column=0, sticky="nsew"
                    )
        scrim = record.get("window")
        if scrim and scrim.winfo_exists():
            try:
                scrim.place(relx=0, rely=0, relwidth=1, relheight=1)
                scrim.lift()
                scrim.focus_set()
            except tk.TclError:
                pass
        record["visible"] = True
        self._layout_overlay_panel(key)

    def _hide_overlay_panel(self, key: str) -> None:
        record = self._overlay_registry.get(key)
        if not record:
            return
        scrim = record.get("window")
        if scrim and scrim.winfo_exists():
            try:
                scrim.place_forget()
            except tk.TclError:
                pass
        try:
            self.focus_set()
        except tk.TclError:
            pass
        record["visible"] = False

    def _apply_overlay_theme(self, record: Dict[str, object]) -> None:
        scrim = record.get("window")
        if not scrim or not scrim.winfo_exists():
            return
        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        scrim.configure(bg=_scrim_color(palette.get("bg", "#111316")))
        self._refresh_overlay_styles()

    def _refresh_overlay_styles(self) -> None:
        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        bg = palette.get("bg", "#111316")
        fg = palette.get("fg", "#E6E6E6")
        alt = palette.get("alt", "#1f2328")
        muted = _scrim_color(palette.get("fg", "#E6E6E6"), strength=0.65)
        accent = palette.get("accent", "#4C8BF5")
        style = ttk.Style(self)
        style.configure("Overlay.Shell.TFrame", background=bg)
        style.configure("Overlay.Container.TFrame", background=alt)
        style.configure("Overlay.Header.TFrame", background=alt)
        style.configure("Overlay.Body.TFrame", background=alt)
        style.configure("Overlay.Footer.TFrame", background=alt)
        style.configure(
            "Overlay.Title.TLabel",
            background=alt,
            foreground=fg,
            font=("TkDefaultFont", 16, "bold"),
        )
        style.configure("Overlay.Close.TButton", padding=(4, 2))
        style.map(
            "Overlay.Close.TButton",
            background=[("active", accent)],
            foreground=[("active", fg)],
        )
        style.configure("Overlay.Subtitle.TLabel", background=alt, foreground=muted, wraplength=560)

    def _refresh_all_overlays(self) -> None:
        self._refresh_overlay_styles()
        for key, record in list(self._overlay_registry.items()):
            self._apply_overlay_theme(record)
            if record.get("visible"):
                self._layout_overlay_panel(key)

    def _start_update_download(
        self, *, mode: Literal["auto-install", "manual-download"]
    ) -> None:
        url = self._update_download_url
        if not url:
            if self._update_release_page_url:
                self._open_release_page()
            else:
                self._show_info_overlay(
                    "Update Available",
                    "Download information is not configured.",
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
            try:
                with zipfile.ZipFile(target_path, "r") as archive:
                    has_members = any(not info.is_dir() for info in archive.infolist())
            except zipfile.BadZipFile:
                raise
            except Exception as exc:  # pragma: no cover - defensive guard for unexpected IO issues
                raise ValueError(f"Unable to inspect downloaded archive: {exc}") from exc
            if not has_members:
                raise ValueError("Downloaded update archive did not contain any files")
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
        self.log(f"Update downloaded to {target_path}")
        if mode == "manual-download":
            self._hide_update_overlay()
            self._show_info_overlay(
                "Update Downloaded",
                "The update package was downloaded successfully.\n",
                f"Saved to: {target_path}",
            )
            return

        self._show_update_overlay(
            "Installing update…",
            headline="Installing update",
            progress=True,
            enable_download=False,
            enable_skip=False,
            enable_details=False,
            enable_manual=False,
            progress_title="Installation status",
            progress_subtext="Copying files into the new installation…",
            status_icon="🛠️",
        )

        selected_entries: Optional[Set[PurePosixPath]] = None
        if self._update_download_mode.get() == "advanced":
            selection = self._prompt_advanced_file_selection(target_path)
            if selection is None:
                self._hide_update_overlay()
                self._show_info_overlay(
                    "Update Cancelled",
                    "The update installation was cancelled before copying any files.",
                )
                return
            if not selection:
                self._hide_update_overlay()
                self._show_info_overlay(
                    "Update Cancelled",
                    "No files were selected for installation. The update was not applied.",
                )
                return
            selected_entries = selection

        try:
            expected_version = self._latest_version if self._update_available else None
            new_install_path, copied = self._install_update_package(
                target_path, selected_entries, expected_version
            )
        except zipfile.BadZipFile as exc:
            self.log(f"Downloaded update is not a valid ZIP archive: {exc}", level="error")
            self._hide_update_overlay()
            self._show_error_overlay(
                "Update Installation Failed",
                "The downloaded update could not be installed because it is not a valid ZIP archive.",
            )
            return
        except Exception as exc:
            self.log(f"Failed to install downloaded update: {exc}", level="error")
            self._hide_update_overlay()
            self._show_error_overlay(
                "Update Installation Failed",
                f"Unable to install the downloaded update: {exc}",
            )
            return

        summary = (
            "The update package was downloaded and installed successfully.\n"
            f"Copied {copied} file{'s' if copied != 1 else ''} into the new installation.\n"
            "The new version will launch automatically and this window will close."
        )
        self._show_update_overlay(
            summary,
            headline="Update installed",
            progress=False,
            enable_download=False,
            enable_skip=False,
            enable_details=bool(self._update_release_page_url),
            enable_manual=False,
            status_icon="✅",
            changelog=self._format_update_changelog(),
        )

        if self._launch_new_installation(new_install_path):
            self._schedule_update_cleanup(Path(__file__).resolve().parent, new_install_path)
            self.after(500, self._shutdown_after_update)
        else:
            self._show_warning_overlay(
                "Update Installed",
                "The update was installed but the new version could not be launched automatically.\n",
                "Please start the new installation manually.",
            )

    def _install_update_package(
        self,
        package_path: Path,
        selected_entries: Optional[Set[PurePosixPath]] = None,
        expected_version: Optional[str] = None,
    ) -> Tuple[Path, int]:
        self.log(f"Installing update from {package_path}")
        app_root = Path(__file__).resolve().parent

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            with zipfile.ZipFile(package_path, "r") as archive:
                archive.extractall(temp_path)

            extracted_root = self._resolve_update_root(temp_path)
            new_install_path, copied = self._prepare_new_installation(
                extracted_root, app_root, selected_entries, expected_version
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
        expected_version: Optional[str] = None,
    ) -> Tuple[Path, int]:
        parent = current_root.parent
        base_name = source.name or current_root.name
        destination = self._next_installation_path(parent, base_name, current_root)
        destination.mkdir(parents=True, exist_ok=False)
        if selected_entries is not None:
            self._copy_existing_installation(current_root, destination)
        copied = self._copy_update_contents(
            source, destination, selected_entries
        )
        if selected_entries is None:
            preserve = self._identify_preserve_entries(current_root)
            self._copy_preserved_entries(current_root, destination, preserve)
        self._update_version_file(destination, expected_version)
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
        self,
        source: Path,
        destination: Path,
        selected_entries: Optional[Set[PurePosixPath]] = None,
    ) -> int:
        replaced = 0
        if not source.exists():
            raise FileNotFoundError("Extracted update does not contain any files")

        allowed: Optional[Set[PurePosixPath]] = None
        if selected_entries is not None:
            allowed = {PurePosixPath(str(entry)) for entry in selected_entries if str(entry)}
            allowed.update(self._mandatory_update_entries(source))

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

    @staticmethod
    def _mandatory_update_entries(source: Path) -> Set[PurePosixPath]:
        mandatory: Set[PurePosixPath] = set()
        try:
            for path in source.rglob("*"):
                if not path.is_file():
                    continue
                name_lower = path.name.lower()
                stem_lower = Path(path.name).stem.lower()
                if name_lower == "version" or stem_lower == "sims4modsorter":
                    relative = path.relative_to(source)
                    mandatory.add(PurePosixPath(relative.as_posix()))
        except Exception:
            return mandatory
        return mandatory

    @staticmethod
    def _copy_existing_installation(current_root: Path, destination: Path) -> None:
        for path in current_root.rglob("*"):
            relative = path.relative_to(current_root)
            target = destination / relative
            try:
                if path.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, target)
            except Exception:
                continue

    def _update_version_file(self, install_root: Path, expected_version: Optional[str]) -> None:
        if not expected_version:
            return
        version_path = install_root / "VERSION"
        try:
            current = version_path.read_text(encoding="utf-8").strip() if version_path.exists() else None
        except Exception:
            current = None
        if current == expected_version:
            return
        try:
            version_path.parent.mkdir(parents=True, exist_ok=True)
            version_path.write_text(f"{expected_version}\n", encoding="utf-8")
        except Exception as exc:
            self.log(f"Failed to synchronize version file: {exc}", level="warn")

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
            self._show_error_overlay(
                "Advanced Download",
                f"Unable to inspect the update package: {exc}",
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
            self._show_warning_overlay(
                "Advanced Download",
                "The update package did not contain any installable files.",
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
                if not self._ask_yes_no_overlay(
                    "No Files Selected",
                    "No files are selected. This will skip installing the update. Continue?",
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
        self._show_error_overlay("Update Download Failed", f"Unable to download update: {reason}")
        self.log(f"Update download failed: {error}", level="error")
        if mode == "manual-download" and self._update_release_page_url and self._ask_yes_no_overlay(
            "Update Download Failed",
            "Would you like to open the release page instead?",
        ):
            self._open_release_page()

    def _open_release_page(self) -> None:
        url = self._update_release_page_url or self._update_download_url
        if not url:
            self._show_info_overlay("Update Available", "Release information is not configured.")
            return
        try:
            webbrowser.open(url)
        except Exception as exc:
            self._show_error_overlay("Update Available", f"Unable to open release page: {exc}")

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
                self._show_info_overlay("Update Check", "An update check is already running.")
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

        self._update_release_notes = None
        self._update_download_url = None
        self._update_release_page_url = None
        self._update_download_filename = None

        if error_message:
            self._update_download_url = None
            self._update_release_page_url = None
            self._update_download_filename = None
            if manual:
                self._show_error_overlay("Update Check", error_message)
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
            if not manual:
                self._hide_update_overlay()
            return

        if result.message:
            if manual:
                self._show_error_overlay("Update Check", result.message)
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

        notes = result.release_notes or ""
        cleaned_notes = notes.strip()
        if cleaned_notes:
            self._update_release_notes = notes
        if result.download_url:
            self._update_download_url = result.download_url
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
                    changelog=self._format_update_changelog(),
                )
            elif manual:
                if download_available:
                    prompt = base_message + "\nWould you like to download the update now?"
                    if self._ask_yes_no_overlay("Update Available", prompt):
                        self._start_update_download(mode="auto-install")
                    elif self._update_release_page_url and self._ask_yes_no_overlay(
                        "Update Available",
                        "Would you like to open the release page instead?",
                    ):
                        self._open_release_page()
                elif release_available:
                    info = (
                        base_message
                        + "\nDownload information is not configured. Would you like to view the release page?"
                    )
                    if self._ask_yes_no_overlay("Update Available", info):
                        self._open_release_page()
                    else:
                        self._show_info_overlay(
                            "Update Available",
                            "Download information is not configured for this release.",
                        )
                else:
                    self._show_info_overlay(
                        "Update Available",
                        "Download information is not configured for this release.",
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
                    changelog=self._format_update_changelog(),
                )
        else:
            self._update_download_url = None
            self._update_download_filename = None
            if manual and from_settings and self._update_release_notes:
                current_version = self._latest_version or APP_VERSION
                message = f"You are using the latest version ({APP_VERSION})."
                release_available = bool(self._update_release_page_url)
                self._show_update_overlay(
                    message,
                    headline=f"Version {current_version} is current",
                    progress=False,
                    enable_download=False,
                    enable_skip=True,
                    enable_details=release_available,
                    enable_manual=False,
                    status_icon="✅",
                    origin="settings",
                    changelog=self._format_update_changelog(),
                    skip_label="Close",
                )
            elif manual:
                self._show_info_overlay(
                    "Update Check",
                    f"You are using the latest version ({APP_VERSION}).",
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
        canvas = getattr(self, "settings_canvas", None)
        if canvas:
            canvas.configure(bg=palette.get("sel", "#2A2F3A"))
            canvas.yview_moveto(0.0)
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
            self._show_info_overlay("Plugin Status", "No plugins loaded.")
            return
        overlay = self._ensure_mod_status_overlay()
        if overlay is None:
            return
        self._populate_mod_status_popup()
        overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
        overlay.tkraise()
        try:
            overlay.grab_set()
        except Exception:
            pass
        container = getattr(self, "_mod_status_container", None)
        if container and container.winfo_exists():
            container.focus_set()

    def _close_mod_status_popup(self) -> None:
        overlay = getattr(self, "_mod_status_overlay", None)
        if overlay and overlay.winfo_exists():
            try:
                overlay.grab_release()
            except Exception:
                pass
            overlay.place_forget()
            overlay.destroy()
        self._mod_status_overlay = None
        self._mod_status_container = None
        self._status_trees = {}
        self._status_summary_var.set("")

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

    def _ensure_mod_status_overlay(self) -> tk.Frame | None:
        overlay = getattr(self, "_mod_status_overlay", None)
        if overlay and overlay.winfo_exists():
            self._refresh_mod_status_overlay_theme()
            return overlay

        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        overlay = tk.Frame(
            self,
            bg=_scrim_color(palette.get("bg", "#111316")),
            highlightthickness=0,
            bd=0,
        )
        overlay.place_forget()
        overlay.grid_rowconfigure(0, weight=1)
        overlay.grid_columnconfigure(0, weight=1)
        overlay.bind("<Escape>", lambda _e: self._close_mod_status_popup())
        overlay.bind(
            "<Button-1>",
            lambda event: self._close_mod_status_popup() if event.widget is overlay else None,
        )

        container = ttk.Frame(
            overlay,
            padding=16,
            style="PluginStatus.Overlay.TFrame",
        )
        container.grid(row=0, column=0, padx=40, pady=40, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)
        container.bind("<Escape>", lambda _e: self._close_mod_status_popup())

        header = ttk.Frame(container, style="PluginStatus.Header.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="Plugin Status", style="PluginStatus.Title.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Button(header, text="Close", command=self._close_mod_status_popup).grid(
            row=0, column=1, sticky="e"
        )

        notebook = ttk.Notebook(container)
        notebook.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        notebook.bind("<Escape>", lambda _e: self._close_mod_status_popup())

        loaded_frame = ttk.Frame(notebook)
        blocked_frame = ttk.Frame(notebook)
        notebook.add(loaded_frame, text="Loaded")
        notebook.add(blocked_frame, text="Blocked")

        def create_tree(parent: ttk.Frame) -> ttk.Treeview:
            frame = ttk.Frame(parent)
            frame.pack(fill="both", expand=True)
            tree = ttk.Treeview(
                frame,
                columns=("name", "folder", "version", "status", "message"),
                show="headings",
                height=8,
            )
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
            tree.bind("<Escape>", lambda _e: self._close_mod_status_popup())
            return tree

        loaded_tree = create_tree(loaded_frame)
        blocked_tree = create_tree(blocked_frame)
        self._status_trees = {"loaded": loaded_tree, "blocked": blocked_tree}

        ttk.Label(
            container,
            textvariable=self._status_summary_var,
            style="PluginStatus.Summary.TLabel",
        ).grid(row=2, column=0, sticky="w", pady=(12, 0))

        self._mod_status_overlay = overlay
        self._mod_status_container = container
        self._refresh_mod_status_overlay_theme()
        return overlay

    def _refresh_mod_status_overlay_theme(self) -> None:
        overlay = getattr(self, "_mod_status_overlay", None)
        if not overlay or not overlay.winfo_exists():
            return
        palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        overlay.configure(bg=_scrim_color(palette.get("bg", "#111316")))
        container = getattr(self, "_mod_status_container", None)
        if container and container.winfo_exists():
            try:
                container.configure(style="PluginStatus.Overlay.TFrame")
            except tk.TclError:
                pass

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
        self._refresh_dialog_overlay_theme()
        self._refresh_mod_status_overlay_theme()
        self._refresh_all_overlays()
        self._update_theme_preview_highlight()
        dashboard = getattr(self, "command_center", None)
        if dashboard is not None:
            dashboard.refresh_theme()
        self.log(f"Theme applied: {self.theme_name.get()}")
    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------
    def on_browse(self) -> None:
        path = filedialog.askdirectory(initialdir=self.mods_root.get(), title="Select Mods folder")
        if path:
            self.mods_root.set(path)

    def run_automation_macro(self, macro_id: str) -> None:
        macro = None
        for entry in self.automation_macros:
            if str(entry.get("id")) == macro_id:
                macro = entry
                break
        if not macro:
            self._show_error_overlay("Automation", "Selected macro is no longer available.")
            return
        steps = [step for step in macro.get("steps", []) if isinstance(step, dict)]
        if not steps:
            self._show_warning_overlay("Automation", "This macro has no actions defined.")
            return
        self.log(f"Running automation macro '{macro.get('name', macro_id)}'.")
        self._execute_macro_steps(steps, 0, macro)

    def _execute_macro_steps(self, steps: Sequence[Dict[str, object]], index: int, macro: Dict[str, object]) -> None:
        if index >= len(steps):
            self.log(f"Automation '{macro.get('name', 'Macro')}' completed.")
            return
        step = steps[index]
        action = str(step.get("action") or "").strip().lower()

        def advance(delay: int = 250) -> None:
            self.after(delay, lambda: self._execute_macro_steps(steps, index + 1, macro))

        if action == "scan":
            self.after(0, self.on_scan)
            advance(600)
        elif action == "apply_loadout":
            target = str(step.get("name") or "").strip()
            if target == "__ACTIVE__" or not target:
                target = self._active_loadout_name
            if target in self.loadouts:
                self.loadout_var.set(target)
                self.after(0, self.on_apply_loadout)
            else:
                self.log(f"Macro skipped unknown loadout '{target}'.", level="warn")
            advance(400)
        elif action == "open_mods":
            self._open_mods_directory()
            advance(200)
        elif action == "run_plugin":
            plugin_id = str(step.get("plugin") or "").strip()
            self._run_plugin_macro_action(plugin_id)
            advance(400)
        else:
            self.log(f"Unknown automation action: {action}", level="warn")
            advance(200)

    def _open_mods_directory(self) -> None:
        mods_path = Path(self.mods_root.get()).expanduser()
        if not mods_path.exists():
            self._show_warning_overlay("Open Mods Folder", "Mods directory does not exist yet.")
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(mods_path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(mods_path)])
            else:
                subprocess.Popen(["xdg-open", str(mods_path)])
        except Exception as exc:
            self._show_error_overlay("Open Mods Folder", f"Unable to open folder: {exc}")

    def _run_plugin_macro_action(self, plugin_id: str) -> None:
        if not plugin_id or not self.plugin_manager:
            return
        buttons = getattr(self, "_plugin_toolbar_buttons", [])
        for button in buttons:
            if button.button_id == plugin_id:
                try:
                    button.command(self, self.plugin_manager.api)
                except Exception as exc:
                    self.log(f"Automation failed invoking plugin '{plugin_id}': {exc}", level="error")
                    self._show_error_overlay(
                        "Automation",
                        f"Plugin '{plugin_id}' reported an error while running.",
                    )
                break

    def _open_plugin_folder(self, folder: str) -> None:
        if not folder:
            return
        target = USER_PLUGINS_DIR / folder
        if not target.exists():
            self._show_warning_overlay(
                "Plugin Folder",
                f"Plugin directory '{folder}' was not found.",
            )
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(target))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(target)])
            else:
                subprocess.Popen(["xdg-open", str(target)])
        except Exception as exc:
            self._show_error_overlay("Plugin Folder", f"Unable to open plugin directory: {exc}")
    def open_plugin_manager_ui(self) -> None:
        self.show_plugin_marketplace()

    def on_scan(self) -> None:
        mods_path = Path(self.mods_root.get())
        if not mods_path.exists():
            self.status_var.set("Folder not found")
            self.log("Error: folder not found")
            return
        allowed_exts_preview = self._resolve_allowed_extensions()
        if allowed_exts_preview is not None and not allowed_exts_preview:
            self._show_warning_overlay("Scan", "Enable at least one file type before scanning.")
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
        self._rebuild_duplicate_groups()
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
        unknown_default = DEFAULT_FOLDER_MAP.get("Unknown", "Mods/NeedsReview")
        new_target = self.target_entry.get().strip() or self.folder_map.get(new_type, unknown_default)
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
        unknown_default = DEFAULT_FOLDER_MAP.get("Unknown", "Mods/NeedsReview")
        count = 0
        for item in self.items:
            if keyword in item.name.lower():
                item.guess_type = new_type
                item.target_folder = self.folder_map.get(new_type, unknown_default)
                count += 1
        self._refresh_tree()
        self.log(f"Assigned {new_type} to {count} file(s) containing '{keyword}'.")

    def on_recalc_targets(self) -> None:
        unknown_default = DEFAULT_FOLDER_MAP.get("Unknown", "Mods/NeedsReview")
        for item in self.items:
            item.target_folder = self.folder_map.get(item.guess_type, unknown_default)
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
            self._show_error_overlay(
                "Undo Issues",
                f"Unable to restore {failed} file(s).\n{error_body}",
            )
        self.on_scan()

    def show_move_history(self) -> None:
        mods_root = Path(self.mods_root.get())
        entries = load_move_history(mods_root)
        if not entries:
            self._show_info_overlay(
                "Sorting History",
                "No move batches have been recorded yet.",
            )
            return
        self._history_entries = {str(entry.get("id")): entry for entry in entries if isinstance(entry.get("id"), str)}
        history_rows = list(entries)

        def builder(body: ttk.Frame, footer: ttk.Frame) -> None:
            if body is None or footer is None:
                return
            body.grid_columnconfigure(0, weight=1)
            body.grid_columnconfigure(1, weight=2)
            body.grid_rowconfigure(0, weight=1)

            tree_frame = ttk.Frame(body, style="Overlay.Body.TFrame")
            tree_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
            tree_frame.grid_rowconfigure(0, weight=1)
            tree_frame.grid_columnconfigure(0, weight=1)

            tree = ttk.Treeview(
                tree_frame,
                columns=("time", "files", "favorite", "summary"),
                show="headings",
                selectmode="browse",
            )
            tree.heading("time", text="Timestamp")
            tree.heading("files", text="Files")
            tree.heading("favorite", text="★")
            tree.heading("summary", text="Summary")
            tree.column("time", width=180, anchor="w")
            tree.column("files", width=60, anchor="center")
            tree.column("favorite", width=40, anchor="center")
            tree.column("summary", width=260, anchor="w")
            tree.grid(row=0, column=0, sticky="nsew")

            scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
            tree.configure(yscrollcommand=scroll.set)
            scroll.grid(row=0, column=1, sticky="ns")

            detail = ttk.Frame(body, style="Overlay.Body.TFrame")
            detail.grid(row=0, column=1, sticky="nsew")
            detail.grid_columnconfigure(0, weight=1)

            title_var = tk.StringVar(value="Select a move batch")
            stats_var = tk.StringVar(value="")
            favorite_var = tk.BooleanVar(value=False)

            ttk.Label(detail, textvariable=title_var, style="Overlay.Title.TLabel").grid(
                row=0, column=0, sticky="w"
            )
            ttk.Label(detail, textvariable=stats_var, style="Overlay.Subtitle.TLabel").grid(
                row=1, column=0, sticky="w", pady=(6, 0)
            )
            favorite_check = ttk.Checkbutton(
                detail,
                text="Mark as favorite",
                variable=favorite_var,
                style="CommandCenter.Checkbutton.TCheckbutton",
            )
            favorite_check.grid(row=2, column=0, sticky="w", pady=(6, 0))

            preview = tk.Text(detail, height=18, wrap="none")
            preview.grid(row=3, column=0, sticky="nsew", pady=(12, 0))
            preview.configure(state="disabled")
            preview_scroll = ttk.Scrollbar(detail, orient="vertical", command=preview.yview)
            preview.configure(yscrollcommand=preview_scroll.set)
            preview_scroll.grid(row=3, column=1, sticky="ns")
            detail.grid_rowconfigure(3, weight=1)

            def refresh_tree(select_id: Optional[str] = None) -> None:
                tree.delete(*tree.get_children())
                for entry in history_rows:
                    entry_id = str(entry.get("id"))
                    favorite_icon = "★" if entry_id in self.move_favorites else ""
                    tree.insert(
                        "",
                        "end",
                        iid=entry_id,
                        values=(
                            entry.get("display_time", "Unknown"),
                            entry.get("moved_count", 0),
                            favorite_icon,
                            entry.get("summary", ""),
                        ),
                    )
                target = select_id or self._history_selection
                if target:
                    try:
                        tree.selection_set(target)
                        tree.focus(target)
                    except tk.TclError:
                        pass

            def update_preview(entry: Optional[Dict[str, object]]) -> None:
                preview.configure(state="normal")
                preview.delete("1.0", "end")
                if not entry:
                    preview.configure(state="disabled")
                    return
                moves = entry.get("moves") if isinstance(entry.get("moves"), list) else []
                for idx, move in enumerate(moves[:200], start=1):
                    if not isinstance(move, dict):
                        continue
                    src = move.get("from", "") or "(unknown)"
                    dst = move.get("to", "") or "(unknown)"
                    preview.insert("end", f"{idx}. {src} -> {dst}\n")
                if len(moves) > 200:
                    preview.insert("end", f"… {len(moves) - 200} additional move(s) not shown.\n")
                preview.configure(state="disabled")

            def update_detail(entry_id: Optional[str]) -> None:
                if not entry_id:
                    title_var.set("Select a move batch")
                    stats_var.set("")
                    favorite_var.set(False)
                    update_preview(None)
                    return
                entry = self._history_entries.get(entry_id)
                if not entry:
                    title_var.set("Select a move batch")
                    stats_var.set("")
                    favorite_var.set(False)
                    update_preview(None)
                    return
                counts = entry.get("counts") if isinstance(entry.get("counts"), dict) else {}
                sources = counts.get("sources") if isinstance(counts.get("sources"), int) else None
                targets = counts.get("targets") if isinstance(counts.get("targets"), int) else None
                fragments = [f"Moved {entry.get('moved_count', 0)} file(s)"]
                if sources is not None:
                    fragments.append(f"Sources: {sources}")
                if targets is not None:
                    fragments.append(f"Targets: {targets}")
                title_var.set(entry.get("summary", "Move batch"))
                stats_var.set(" | ".join(fragments))
                favorite_var.set(entry_id in self.move_favorites)
                update_preview(entry)

                def toggle_favorite() -> None:
                    if favorite_var.get():
                        self.move_favorites.add(entry_id)
                    else:
                        self.move_favorites.discard(entry_id)
                    self._save_history_favorites()
                    refresh_tree(entry_id)

                favorite_check.configure(command=toggle_favorite)

            def on_select(_event=None) -> None:
                selection = tree.selection()
                entry_id = selection[0] if selection else None
                self._history_selection = entry_id
                update_detail(entry_id)

            tree.bind("<<TreeviewSelect>>", on_select)

            def reload_entries(select_id: Optional[str] = None) -> None:
                new_entries = load_move_history(mods_root)
                self._history_entries = {str(entry.get("id")): entry for entry in new_entries if isinstance(entry.get("id"), str)}
                history_rows.clear()
                history_rows.extend(new_entries)
                refresh_tree(select_id)
                update_detail(select_id)

            def undo_selected() -> None:
                selection = tree.selection()
                if not selection:
                    self._show_info_overlay("Undo Move Batch", "Select a move batch to undo.")
                    return
                entry_id = selection[0]
                entry = self._history_entries.get(entry_id)
                if not entry:
                    self._show_error_overlay("Undo Move Batch", "The selected entry could not be located.")
                    return
                moved_count = entry.get("moved_count", 0)
                label = entry.get("display_time", "this batch")
                if not self._ask_yes_no_overlay(
                    "Undo Move Batch",
                    f"Restore {moved_count} file(s) from {label}?",
                ):
                    return
                history_index = entry.get("history_index")
                if isinstance(history_index, int):
                    identifier: Optional[Union[str, int]] = history_index
                else:
                    identifier = entry_id
                self.status_var.set("Undoing selected batch…")

                def worker() -> None:
                    undone, failed, errors, undo_label = undo_moves(mods_root, identifier)
                    self._enqueue_ui(
                        lambda: self._handle_history_undo_result(
                            undone,
                            failed,
                            errors,
                            undo_label,
                            lambda: reload_entries(None),
                        )
                    )

                threading.Thread(target=worker, daemon=True).start()

            ttk.Button(footer, text="Undo Selected", command=undo_selected).pack(side="left")
            ttk.Button(footer, text="Close", command=lambda: self._hide_overlay_panel("move_history")).pack(
                side="right"
            )

            initial_id = next(
                (str(entry.get("id")) for entry in history_rows if isinstance(entry.get("id"), str)),
                None,
            )
            refresh_tree(initial_id)
            if initial_id:
                tree.selection_set(initial_id)
                update_detail(initial_id)

        self._show_overlay_panel("move_history", "Sorting History Timeline", builder, width=980, height=600)

    def _handle_history_undo_result(
        self,
        undone: int,
        failed: int,
        errors: List[str],
        label: Optional[str],
        refresh_callback: Optional[Callable[[], None]] = None,
    ) -> None:
        self._handle_undo_result(undone, failed, errors, label)
        if refresh_callback:
            refresh_callback()
        else:
            self.show_move_history()

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
            self._show_error_overlay("Import Plan", f"Unable to import plan: {exc}")
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
            self._show_error_overlay("Import Plan", "The selected file is not a valid plan export.")
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
