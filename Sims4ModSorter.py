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
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from types import MethodType
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

import tkinter as tk
import tkinter.font as tkfont
from tkinter import filedialog, messagebox, ttk

from launch_utils import UpdateResult, check_for_update


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

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class FileItem:
    path: Path
    name: str
    ext: str
    size_mb: float
    relpath: str
    guess_type: str
    confidence: float
    notes: str
    include: bool = True
    target_folder: str = "Unsorted"
    bundle: str = ""
    meta_tags: str = ""
    dependency_status: str = ""
    dependency_detail: str = ""
    extras: Dict[str, str] = field(default_factory=dict)
    tooltips: Dict[str, str] = field(default_factory=dict)
    disabled: bool = False
    original_ext: str = ""


@dataclass(slots=True)
class ScanResult:
    items: List[FileItem]
    total_files: int
    errors: List[str] = field(default_factory=list)
    disabled_items: List[FileItem] = field(default_factory=list)


@dataclass(slots=True)
class AdultEvidence:
    score: float = 0.0
    hits: set[str] = field(default_factory=set)
    reasons: List[str] = field(default_factory=list)

    def add(self, hits: Iterable[str], score: float, reason: str) -> None:
        hits_set = {token for token in hits if isinstance(token, str) and token}
        if hits_set:
            self.hits.update(hits_set)
        if score:
            if score < 0:
                self.score = max(0.0, min(1.0, self.score + score))
            else:
                self.score = min(1.0, self.score + score)
        if reason:
            self.reasons.append(reason)

    @property
    def is_confident(self) -> bool:
        if self.score >= 0.75:
            return True
        return self.score >= 0.5 and bool(self.hits)

    def format_note(self) -> str:
        parts: List[str] = []
        if self.hits:
            parts.append("keywords: " + ", ".join(sorted(self.hits)))
        if self.reasons:
            parts.append("; ".join(self.reasons))
        if not parts:
            return ""
        return "Adult evidence - " + " | ".join(parts)

# ---------------------------------------------------------------------------
# Classification constants
# ---------------------------------------------------------------------------

CATEGORY_ORDER: List[str] = [
    "Script Mod",
    "Adult Script",
    "Adult Gameplay",
    "Adult Animation",
    "Adult Pose",
    "Adult CAS",
    "Adult BuildBuy",
    "Adult Override",
    "Adult Other",
    "Gameplay Tuning",
    "CAS Hair",
    "CAS Clothing",
    "CAS Makeup",
    "CAS Skin",
    "CAS Eyes",
    "CAS Accessories",
    "BuildBuy Object",
    "BuildBuy Recolour",
    "Animation",
    "Preset",
    "Pose",
    "Slider",
    "World",
    "Override",
    "Utility Tool",
    "Archive",
    "Other",
    "Unknown",
]

CATEGORY_INDEX: Dict[str, int] = {name: idx for idx, name in enumerate(CATEGORY_ORDER)}

DEFAULT_FOLDER_MAP: Dict[str, str] = {
    "Adult Script": "Adult - Scripts",
    "Adult Gameplay": "Adult - Gameplay",
    "Adult Animation": "Adult - Animations",
    "Adult Pose": "Adult - Poses",
    "Adult CAS": "Adult - CAS",
    "Adult BuildBuy": "Adult - Objects",
    "Adult Override": "Adult - Overrides",
    "Adult Other": "Adult - Other",
    "Script Mod": "Script Mods",
    "Gameplay Tuning": "Gameplay Mods",
    "CAS Hair": "CAS Hair",
    "CAS Clothing": "CAS Clothing",
    "CAS Makeup": "CAS Makeup",
    "CAS Skin": "CAS Skin",
    "CAS Eyes": "CAS Eyes",
    "CAS Accessories": "CAS Accessories",
    "BuildBuy Object": "BuildBuy Objects",
    "BuildBuy Recolour": "BuildBuy Recolours",
    "Animation": "Animations",
    "Preset": "Presets",
    "Pose": "Poses",
    "Slider": "Sliders",
    "World": "World",
    "Override": "Overrides",
    "Utility Tool": "Utilities",
    "Archive": "Archives",
    "Other": "Other",
    "Unknown": "Unsorted",
}

PACKAGE_EXTS = {".package"}
SCRIPT_EXTS = {".ts4script", ".t4script"}
ARCHIVE_EXTS = {".zip", ".rar", ".7z"}
SUPPORTED_EXTS = PACKAGE_EXTS | SCRIPT_EXTS | ARCHIVE_EXTS | {
    ".txt",
    ".cfg",
    ".ini",
    ".log",
    ".jpg",
    ".png",
    ".jpeg",
    ".bat",
    ".cmd",
    ".rtf",
}

TEXT_FILE_EXTS = {".txt", ".cfg", ".ini", ".log", ".rtf"}


def normalize_extension(ext: str) -> Tuple[str, bool]:
    lowered = ext.lower()
    if lowered.endswith("off") and len(lowered) > 4:
        return lowered[:-3], True
    return lowered, False

ADULT_CATEGORY_PROMOTIONS: Dict[str, str] = {
    "Script Mod": "Adult Script",
    "Gameplay Tuning": "Adult Gameplay",
    "Animation": "Adult Animation",
    "Pose": "Adult Pose",
    "CAS Hair": "Adult CAS",
    "CAS Clothing": "Adult CAS",
    "CAS Makeup": "Adult CAS",
    "CAS Skin": "Adult CAS",
    "CAS Eyes": "Adult CAS",
    "CAS Accessories": "Adult CAS",
    "BuildBuy Object": "Adult BuildBuy",
    "BuildBuy Recolour": "Adult BuildBuy",
    "Preset": "Adult Other",
    "Slider": "Adult Other",
    "World": "Adult Other",
    "Override": "Adult Override",
    "Archive": "Adult Other",
    "Other": "Adult Other",
    "Unknown": "Adult Other",
    "Utility Tool": "Adult Other",
}

ADULT_CATEGORY_DEMOTIONS: Dict[str, str] = {
    "Adult Script": "Script Mod",
    "Adult Gameplay": "Gameplay Tuning",
    "Adult Animation": "Animation",
    "Adult Pose": "Pose",
    "Adult CAS": "CAS Clothing",
    "Adult BuildBuy": "BuildBuy Object",
    "Adult Override": "Override",
    "Adult Other": "Other",
}

ADULT_SCAN_MAX_BYTES = 2 * 1024 * 1024
ADULT_SCAN_CHUNK_SIZE = 65_536
ADULT_SCAN_MAX_ARCHIVE_ENTRIES = 40
ADULT_SCAN_MAX_SOURCES = 5

TYPE_IDS: Dict[int, str] = {
    0x034AEECB: "CASP",
    0x319E4F1D: "COBJ/OBJD",
    0x02D5DF13: "JAZZ",
    0x220557DA: "STBL",
    0x015A1849: "GEOM",
    0x01661233: "MODL",
    0x01D10F34: "MLOD",
    0x0354796A: "TONE",
    0x067CAA11: "BGEO",
    0x00B2D882: "IMG",
}


# ---------------------------------------------------------------------------
# Adult vocabulary (offline)
# ---------------------------------------------------------------------------

ADULT_WORDS_BASE: Tuple[str, ...] = (
    "wickedwhims",
    "turbodriver",
    "basemental",
    "nisa",
    "wild_guy",
    "wildguy",
    "nsfw",
    "porn",
    "sex",
    "sexual",
    "kinky",
    "nude",
    "naked",
    "strip",
    "lapdance",
    "prostitution",
    "genital",
    "penis",
    "vagina",
    "condom",
    "condoms",
    "sheath",
    "dildo",
    "vibrator",
    "plug",
    "buttplug",
    "cum",
    "orgasm",
    "bdsm",
    "fetish",
    "bondage",
    "dominatrix",
    "orgy",
    "hentai",
    "lewd",
    "xxx",
    "xrated",
    "x-rated",
    "taboo",
    "sensual",
    "seduce",
    "seduction",
    "sultry",
    "provocative",
    "lingerie",
    "nipple",
    "areola",
    "sperm",
    "spermicide",
    "lubricant",
    "aphrodisiac",
    "escort",
    "brothel",
    "stripclub",
    "swinger",
    "swingers",
    "kamasutra",
    "playboy",
    "onlyfans",
    "camboy",
    "camgirl",
    "cammodel",
    "camshow",
    "latex",
    "polyurethane",
    "polyisoprene",
    "birthcontrol",
    "durex",
    "trojan",
    "std",
    "sti",
    "petplay",
    "pet play",
    "nudity",
    "desires",
    "deviantcore",
    "deviant core",
    "deviant",
    "devious",
    "deviousdesires",
    "devious desires",
    "dd",
    "kink",
    "flirtyfetishes",
    "flirty fetishes",
    "fetishes",
    "ww",
    "gay",
    "pubichair",
    "pubic hair",
    "pubic",
    "watersports",
    "water sports",
)

ADULT_WORDS: set[str] = set(ADULT_WORDS_BASE)
_ADULT_WORD_CACHE: Tuple[str, ...] = ()
_ADULT_MAX_WORD_LEN: int = 0

ADULT_AUTHORS: Tuple[str, ...] = (
    "onizu",
    "amozidan22",
    "oll",
    "nisak",
    "cherrypie",
    "!chingyu",
    "chingyu",
    "turbodriver",
    "khlas",
    "lychee",
    "alchemist",
    "falsehope",
)

_AUTHOR_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")


def _normalise_author_token(value: str) -> str:
    return _AUTHOR_NORMALIZE_RE.sub("", value.lower())


ADULT_AUTHOR_ALIASES: Dict[str, str] = {
    token: name
    for name in ADULT_AUTHORS
    for token in {_normalise_author_token(name)}
    if token
}


def _refresh_adult_word_cache() -> None:
    global _ADULT_WORD_CACHE, _ADULT_MAX_WORD_LEN
    _ADULT_WORD_CACHE = tuple(
        sorted((word for word in ADULT_WORDS if word), key=len, reverse=True)
    )
    _ADULT_MAX_WORD_LEN = max((len(word) for word in _ADULT_WORD_CACHE), default=0)


_refresh_adult_word_cache()


def _load_adult_words_override() -> None:
    override_path = Path(__file__).with_name("adult_words.json")
    if not override_path.exists():
        return
    try:
        data = json.loads(override_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if isinstance(data, dict):
        words = data.get("words", [])
    else:
        words = data
    if isinstance(words, Sequence):
        for word in words:
            if isinstance(word, str) and word.strip():
                ADULT_WORDS.add(word.strip().lower())
    _refresh_adult_word_cache()


_load_adult_words_override()


# ---------------------------------------------------------------------------
# Theme registry
# ---------------------------------------------------------------------------


def _ensure_adult_category(category: str) -> str:
    if category.startswith("Adult"):
        return category
    return ADULT_CATEGORY_PROMOTIONS.get(category, "Adult Other")


def _strip_adult_category(category: str) -> str:
    if not category.startswith("Adult"):
        return category
    base = ADULT_CATEGORY_DEMOTIONS.get(category)
    if base:
        return base
    return category.replace("Adult ", "", 1) or "Other"


def _scan_binary_stream(stream, limit: int) -> set[str]:
    hits: set[str] = set()
    if not _ADULT_WORD_CACHE or limit <= 0:
        return hits
    remaining = limit
    remainder = ""
    while remaining > 0:
        chunk = stream.read(min(ADULT_SCAN_CHUNK_SIZE, remaining))
        if not chunk:
            break
        remaining -= len(chunk)
        try:
            decoded = chunk.decode("utf-8", "ignore")
        except Exception:
            decoded = chunk.decode("latin-1", "ignore")
        lowered = (remainder + decoded).lower()
        for keyword in _ADULT_WORD_CACHE:
            if keyword and keyword in lowered:
                hits.add(keyword)
        if _ADULT_MAX_WORD_LEN > 1:
            remainder = lowered[-(_ADULT_MAX_WORD_LEN - 1) :]
        else:
            remainder = ""
    return hits


def _scan_file_for_adult_keywords(path: Path, limit: int = ADULT_SCAN_MAX_BYTES) -> set[str]:
    try:
        with path.open("rb") as handle:
            return _scan_binary_stream(handle, limit)
    except Exception:
        return set()


def _scan_text_file_for_adult_keywords(path: Path, limit: int = ADULT_SCAN_MAX_BYTES) -> set[str]:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            text = path.read_text(encoding="latin-1", errors="ignore")
        except Exception:
            return set()
    snippet = text[:limit].lower()
    return {word for word in _ADULT_WORD_CACHE if word and word in snippet}


def _scan_zip_for_adult_keywords(path: Path) -> Tuple[set[str], List[str]]:
    hits: set[str] = set()
    sources: List[str] = []
    try:
        with zipfile.ZipFile(path) as archive:
            for index, info in enumerate(archive.infolist()):
                if index >= ADULT_SCAN_MAX_ARCHIVE_ENTRIES:
                    break
                name = info.filename
                lowered = name.lower()
                name_hits = {word for word in _ADULT_WORD_CACHE if word and word in lowered}
                if name_hits:
                    hits.update(name_hits)
                    if len(sources) < ADULT_SCAN_MAX_SOURCES:
                        sources.append(f"{name} (name)")
                if info.is_dir():
                    continue
                limit = min(info.file_size, ADULT_SCAN_MAX_BYTES)
                if limit <= 0:
                    continue
                try:
                    with archive.open(info) as entry:
                        entry_hits = _scan_binary_stream(entry, int(limit))
                except Exception:
                    continue
                if entry_hits:
                    hits.update(entry_hits)
                    if len(sources) < ADULT_SCAN_MAX_SOURCES:
                        sources.append(f"{name} (content)")
    except (zipfile.BadZipFile, OSError, RuntimeError, ValueError):
        return set(), []
    return hits, sources


def _summarize_archive_sources(label: str, sources: Sequence[str]) -> str:
    if not sources:
        return f"{label} content"
    display = ", ".join(sources[:ADULT_SCAN_MAX_SOURCES])
    if len(sources) > ADULT_SCAN_MAX_SOURCES:
        display += ", ..."
    return f"{label} content ({display})"


def _scan_content_for_adult_keywords(path: Path, ext: str) -> List[Tuple[set[str], str, float]]:
    ext = ext.lower()
    results: List[Tuple[set[str], str, float]] = []
    if ext == ".package":
        hits = _scan_file_for_adult_keywords(path)
        if hits:
            results.append((hits, "package content", 0.6))
    elif ext in SCRIPT_EXTS or ext == ".zip":
        hits, sources = _scan_zip_for_adult_keywords(path)
        if hits:
            label = "script archive" if ext in SCRIPT_EXTS else "zip archive"
            results.append((hits, _summarize_archive_sources(label, sources), 0.65 if ext in SCRIPT_EXTS else 0.55))
    elif ext in ARCHIVE_EXTS:
        hits = _scan_file_for_adult_keywords(path)
        if hits:
            results.append((hits, f"{ext[1:]} archive binary content", 0.45))
    elif ext in TEXT_FILE_EXTS:
        hits = _scan_text_file_for_adult_keywords(path)
        if hits:
            results.append((hits, "text content", 0.4))
    return results


def inspect_adult_content(
    path: Path,
    ext: str,
    tokens: Tuple[str, ...],
    initial_category: str,
    *,
    relpath: Optional[str] = None,
    folder_tokens: Tuple[str, ...] = (),
) -> AdultEvidence:
    evidence = AdultEvidence()
    token_hits = {token for token in tokens if token in ADULT_WORDS}
    if token_hits:
        evidence.add(token_hits, 0.25, "filename tokens")
    if folder_tokens:
        folder_hits = {token for token in folder_tokens if token in ADULT_WORDS}
        if folder_hits:
            evidence.add(folder_hits, 0.2, "folder name keywords")
    author_sources = set()
    if relpath:
        author_sources.add(_normalise_author_token(relpath))
    author_sources.add(_normalise_author_token(path.name))
    if folder_tokens:
        author_sources.add(_normalise_author_token("".join(folder_tokens)))
    author_hits: set[str] = set()
    for source in author_sources:
        if not source:
            continue
        for token, name in ADULT_AUTHOR_ALIASES.items():
            if token and token in source:
                author_hits.add(name)
    if author_hits:
        evidence.add(author_hits, 0.3, "known adult author")
    if initial_category.startswith("Adult"):
        evidence.add((), 0.2, f"initial category '{initial_category}'")
    for hits, reason, score in _scan_content_for_adult_keywords(path, ext):
        evidence.add(hits, score, reason)
    return evidence


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
        if normalized in self.columns:
            return
        self.columns[normalized] = PluginColumn(normalized, heading, width, anchor)
        self.column_order.append(normalized)

    def get_columns(self) -> List[PluginColumn]:
        return [self.columns[column_id] for column_id in self.column_order if column_id in self.columns]

    def get_statuses(self) -> List[PluginStatus]:
        return list(self.statuses)


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
        app.log(f"{tag}: {message}")


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def human_mb(nbytes: int) -> float:
    return round(nbytes / (1024 * 1024), 2)


def get_default_mods_path() -> str:
    home = Path.home()
    candidates = [
        home / "Documents" / "Electronic Arts" / "The Sims 4" / "Mods",
        home / "OneDrive" / "Documents" / "Electronic Arts" / "The Sims 4" / "Mods",
    ]
    for candidate in candidates:
        if candidate.is_dir():
            return str(candidate)
    return str(candidates[0])


_NAT_SORT_RE = re.compile(r"(\d+)")


def _natural_key(value: str) -> Tuple[object, ...]:
    parts = _NAT_SORT_RE.split(value.lower())
    out: List[object] = []
    for part in parts:
        out.append(int(part) if part.isdigit() else part)
    return tuple(out)


def pretty_display_name(filename: str) -> str:
    base = re.sub(r"\.[^.]+$", "", filename)
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    tokens: List[str] = []
    for token in base.split(" "):
        if len(token) <= 4 and token.isupper():
            tokens.append(token)
        else:
            tokens.append(token[:1].upper() + token[1:])
    return " ".join(tokens)


def normalize_key(filename: str) -> str:
    base = re.sub(r"\.[^.]+$", "", filename)
    base = re.sub(r"\[[^\]]+\]", "", base)
    base = re.sub(r"[_\-\s]+", "", base)
    base = re.sub(r"[^a-zA-Z0-9]+", "", base)
    return base.lower()

# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------

_KEYWORD_MAP: Tuple[Tuple[str, str], ...] = (
    ("ui cheats", "Script Mod"),
    ("uicheats", "Script Mod"),
    ("mccc", "Script Mod"),
    ("mc command", "Script Mod"),
    ("command center", "Script Mod"),
    ("top", "CAS Clothing"),
    ("bottom", "CAS Clothing"),
    ("shirt", "CAS Clothing"),
    ("dress", "CAS Clothing"),
    ("skirt", "CAS Clothing"),
    ("pants", "CAS Clothing"),
    ("trousers", "CAS Clothing"),
    ("shorts", "CAS Clothing"),
    ("jacket", "CAS Clothing"),
    ("coat", "CAS Clothing"),
    ("jeans", "CAS Clothing"),
    ("legging", "CAS Clothing"),
    ("heels", "CAS Clothing"),
    ("boots", "CAS Clothing"),
    ("sneaker", "CAS Clothing"),
    ("shoe", "CAS Clothing"),
    ("hair", "CAS Hair"),
    ("ponytail", "CAS Hair"),
    ("bun", "CAS Hair"),
    ("brow", "CAS Accessories"),
    ("lash", "CAS Accessories"),
    ("eyelash", "CAS Accessories"),
    ("makeup", "CAS Makeup"),
    ("lipstick", "CAS Makeup"),
    ("blush", "CAS Makeup"),
    ("eyeliner", "CAS Makeup"),
    ("skinoverlay", "CAS Skin"),
    ("overlay", "CAS Skin"),
    ("tattoo", "CAS Skin"),
    ("freckle", "CAS Skin"),
    ("scar", "CAS Skin"),
    ("eyes", "CAS Eyes"),
    ("iris", "CAS Eyes"),
    ("eyeglass", "CAS Accessories"),
    ("spectacle", "CAS Accessories"),
    ("sunglass", "CAS Accessories"),
    ("eyewear", "CAS Accessories"),
    ("goggle", "CAS Accessories"),
    ("ring", "CAS Accessories"),
    ("necklace", "CAS Accessories"),
    ("earring", "CAS Accessories"),
    ("piercing", "CAS Accessories"),
    ("nails", "CAS Accessories"),
    ("glove", "CAS Accessories"),
    ("tail", "CAS Accessories"),
    ("recolor", "BuildBuy Recolour"),
    ("recolour", "BuildBuy Recolour"),
    ("swatch", "BuildBuy Recolour"),
    ("object", "BuildBuy Object"),
    ("clutter", "BuildBuy Object"),
    ("deco", "BuildBuy Object"),
    ("furniture", "BuildBuy Object"),
    ("sofa", "BuildBuy Object"),
    ("chair", "BuildBuy Object"),
    ("table", "BuildBuy Object"),
    ("bed", "BuildBuy Object"),
    ("animation", "Animation"),
    ("anim_", "Animation"),
    ("pose", "Pose"),
    ("preset", "Preset"),
    ("slider", "Slider"),
    ("world", "World"),
    ("override", "Override"),
    ("utility", "Utility Tool"),
    ("tool", "Utility Tool"),
)

_SCRIPT_HINTS = ("script", "ts4script", "py", "python")

_CAMEL_CASE_BOUNDARY_RE = re.compile(
    r"(?<=[A-Za-z])(?=[A-Z][a-z])|(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Za-z])(?=\d)|(?<=\d)(?=[A-Za-z])"
)


def _tokenise(name: str) -> Tuple[str, ...]:
    raw = re.sub(r"\.[^.]+$", "", name)
    base = _CAMEL_CASE_BOUNDARY_RE.sub(" ", raw)
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"[^A-Za-z0-9]+", " ", base)
    tokens: List[str] = []
    for chunk in base.split():
        lowered = chunk.lower()
        if lowered:
            tokens.append(lowered)
    compact = re.sub(r"[^A-Za-z0-9]+", "", raw).lower()
    if compact and compact not in tokens:
        tokens.append(compact)
    return tuple(tokens)


def _tokenise_path_parts(relpath: Union[str, Path]) -> Tuple[str, ...]:
    if isinstance(relpath, Path):
        parts = relpath.parts
    else:
        parts = Path(relpath).parts
    tokens: List[str] = []
    for part in parts:
        if part in {".", ""}:
            continue
        tokens.extend(_tokenise(part))
    return tuple(tokens)


def _keyword_matches(keyword: str, tokens: Tuple[str, ...], joined: str) -> bool:
    if keyword.endswith("_"):
        prefix = keyword.rstrip("_")
        return any(token.startswith(prefix) for token in tokens)
    if " " in keyword:
        return f" {keyword} " in joined
    for token in tokens:
        if token == keyword:
            return True
        if len(keyword) >= 4 and keyword in token:
            return True
        if len(keyword) <= 3 and token.endswith(keyword):
            return True
    return False


def guess_type_for_name(name: str) -> Tuple[str, Tuple[str, ...]]:
    tokens = _tokenise(name)
    joined = " " + " ".join(tokens) + " "
    adult = any(token in ADULT_WORDS for token in tokens)
    for keyword, category in _KEYWORD_MAP:
        if _keyword_matches(keyword, tokens, joined):
            result = category
            if adult and not result.startswith("Adult"):
                if result.startswith("CAS"):
                    result = "Adult CAS"
                elif result.startswith("BuildBuy"):
                    result = "Adult BuildBuy"
                elif result in {"Animation", "Pose"}:
                    result = "Adult Animation" if result == "Animation" else "Adult Pose"
                elif result == "Override":
                    result = "Adult Override"
                else:
                    result = "Adult Other"
            return result, tuple(sorted(set(tokens)))
    return ("Adult Other" if adult else "Unknown"), tuple(sorted(set(tokens)))


def _guess_from_name(name: str, ext: str) -> Tuple[str, float, str, Tuple[str, ...]]:
    tokens = _tokenise(name)
    joined = " " + " ".join(tokens) + " "
    words = set(tokens)
    adult = any(word in ADULT_WORDS for word in words)
    tags = tuple(sorted(words))
    lowered = name.lower()
    if ext in SCRIPT_EXTS or any(hint in lowered for hint in _SCRIPT_HINTS):
        cat = "Adult Script" if adult else "Script Mod"
        return cat, 0.9, "Script-like extension", tags
    if ext in ARCHIVE_EXTS:
        cat = "Adult Other" if adult else "Archive"
        return cat, 0.6, "Archive container", tags
    if ext == ".package":
        for keyword, category in _KEYWORD_MAP:
            if _keyword_matches(keyword, tokens, joined) or keyword in lowered:
                if adult and not category.startswith("Adult"):
                    if category.startswith("CAS"):
                        category = "Adult CAS"
                    elif category.startswith("BuildBuy"):
                        category = "Adult BuildBuy"
                    elif category.startswith("Animation"):
                        category = "Adult Animation"
                    elif category == "Pose":
                        category = "Adult Pose"
                    elif category == "Override":
                        category = "Adult Override"
                    else:
                        category = "Adult Other"
                return category, 0.7, f"Keyword '{keyword}'", tags
        return ("Adult Other" if adult else "Other"), 0.4, "Package (no keyword match)", tags
    if ext in {".txt", ".cfg", ".ini", ".log"}:
        return ("Adult Other" if adult else "Utility Tool"), 0.4, "Utility/config file", tags
    if adult:
        return "Adult Other", 0.5, "Adult keyword", tags
    return "Unknown", 0.3, "Unrecognised extension", tags


def _u32(blob: bytes, offset: int) -> int:
    return int.from_bytes(blob[offset : offset + 4], "little", signed=False)


def dbpf_scan_types(path: Path) -> Dict[int, int]:
    result: Dict[int, int] = {}
    try:
        with path.open("rb") as fh:
            head = fh.read(96)
            if len(head) < 96 or head[:4] != b"DBPF":
                return result
            try:
                count = _u32(head, 0x20)
                index_pos = _u32(head, 0x40)
            except Exception:
                return result
            if not count or not index_pos:
                return result
            fh.seek(index_pos)
            flags = int.from_bytes(fh.read(4), "little")
            flagged_slots = [idx for idx in range(8) if (flags >> idx) & 1]
            header_vals = [int.from_bytes(fh.read(4), "little") for _ in flagged_slots]
            per_entry = 8 - len(flagged_slots)
            for _ in range(count):
                entry_vals = [int.from_bytes(fh.read(4), "little") for _ in range(per_entry)]
                vals: Dict[int, int] = {}
                hi = 0
                mi = 0
                for slot in range(8):
                    if slot in flagged_slots:
                        vals[slot] = header_vals[hi]
                        hi += 1
                    else:
                        vals[slot] = entry_vals[mi]
                        mi += 1
                rtype = vals.get(0)
                if rtype is None:
                    continue
                result[rtype] = result.get(rtype, 0) + 1
    except Exception:
        return {}
    return result


def classify_from_types(types: Dict[int, int], filename: str, adult_hint: bool) -> Tuple[str, float, str, Tuple[str, ...]]:
    if not types:
        return ("Adult Other" if adult_hint else "Unknown"), 0.5, "No DBPF index", tuple()
    tags = tuple(TYPE_IDS.get(t, hex(t)) for t in sorted(types))
    notes = ", ".join(f"{TYPE_IDS.get(t, hex(t))}:{c}" for t, c in types.items())
    has = types.__contains__
    lower = filename.lower()
    if has(0x034AEECB):
        if adult_hint:
            return "Adult CAS", 0.9, notes, tags
        if any(key in lower for key in ("hair", "ponytail", "bun", "brow", "lash")):
            return "CAS Hair", 0.85, notes, tags
        if any(key in lower for key in ("lip", "liner", "blush", "makeup")):
            return "CAS Makeup", 0.85, notes, tags
        if any(key in lower for key in ("skin", "overlay", "tattoo", "freckle")):
            return "CAS Skin", 0.85, notes, tags
        if any(key in lower for key in ("eye", "iris")):
            return "CAS Eyes", 0.85, notes, tags
        if any(key in lower for key in ("ring", "necklace", "ear", "nail", "piercing", "tail")):
            return "CAS Accessories", 0.85, notes, tags
        return "CAS Clothing", 0.85, notes, tags
    if has(0x319E4F1D) or has(0x015A1849) or has(0x01661233) or has(0x01D10F34):
        return ("Adult BuildBuy" if adult_hint else "BuildBuy Object"), 0.85, notes, tags
    if has(0x0354796A):
        return ("Adult CAS" if adult_hint else "CAS Skin"), 0.85, notes, tags
    if has(0x02D5DF13):
        return ("Adult Animation" if adult_hint else "Animation"), 0.85, notes, tags
    if has(0x220557DA):
        return ("Adult Gameplay" if adult_hint else "Gameplay Tuning"), 0.75, notes, tags
    return ("Adult Other" if adult_hint else "Other"), 0.6, notes, tags


def refine_with_metadata(path: Path, current: Tuple[str, float, str, Tuple[str, ...]]) -> Tuple[str, float, str, Tuple[str, ...]]:
    category, confidence, notes, tags = current
    suffix_raw = path.suffix
    ext, disabled_flag = normalize_extension(suffix_raw)
    if ext not in PACKAGE_EXTS:
        return category, confidence, notes, tags
    display_name = path.name
    if disabled_flag and display_name.lower().endswith("off"):
        display_name = display_name[:-3]
    adult_hint = any(token in ADULT_WORDS for token in _tokenise(display_name))
    types = dbpf_scan_types(path)
    if not types:
        return category, confidence, notes, tags
    new_category, new_confidence, new_notes, new_tags = classify_from_types(types, path.name, adult_hint)
    if new_confidence >= confidence:
        return new_category, new_confidence, new_notes, new_tags
    merged_notes = f"{notes}; {new_notes}" if notes else new_notes
    merged_tags = tags or new_tags
    return category, confidence, merged_notes, merged_tags


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------

ProgressCallback = Callable[[int, int, Path, str], None]


def _iter_files(root: Path, recurse: bool) -> Iterator[Path]:
    if recurse:
        for base, _, files in os.walk(root):
            base_path = Path(base)
            for name in files:
                yield base_path / name
    else:
        with os.scandir(root) as entries:
            for entry in entries:
                if entry.is_file():
                    yield Path(entry.path)


def scan_folder(
    root: Path,
    *,
    folder_map: Optional[Dict[str, str]] = None,
    recurse: bool = True,
    ignore_exts: Optional[Iterable[str]] = None,
    ignore_names: Optional[Iterable[str]] = None,
    progress_cb: Optional[ProgressCallback] = None,
    selected_folders: Optional[Sequence[str]] = None,
    include_adult: bool = True,
    allowed_exts: Optional[Iterable[str]] = None,
) -> ScanResult:
    root = Path(root)
    if not root.is_dir():
        return ScanResult([], 0, ["Folder not found"])
    folder_map = folder_map or DEFAULT_FOLDER_MAP
    ignore_exts_set = {
        (ext.lower() if ext.startswith(".") else f".{ext.lower()}")
        for ext in (ignore_exts or [])
        if isinstance(ext, str) and ext.strip()
    }
    ignore_names_tokens = [token.lower() for token in (ignore_names or []) if isinstance(token, str) and token.strip()]
    if allowed_exts is None:
        allowed_exts_set: Optional[set[str]] = None
    else:
        allowed_exts_set = {
            (ext.lower() if ext.startswith(".") else f".{ext.lower()}")
            for ext in allowed_exts
            if isinstance(ext, str) and ext.strip()
        }
    selected_paths: Optional[List[Path]] = None
    if selected_folders is not None:
        seen_paths: set[Path] = set()
        selected_paths = []
        for folder in selected_folders:
            text = str(folder).strip()
            token = Path(".") if not text or text in {".", os.curdir} else Path(text)
            if token not in seen_paths:
                seen_paths.add(token)
                selected_paths.append(token)
    items: List[FileItem] = []
    disabled_items: List[FileItem] = []
    errors: List[str] = []
    candidates = list(_iter_files(root, recurse))
    total = len(candidates)
    for index, path in enumerate(candidates, start=1):
        name = path.name
        ext_raw = path.suffix
        ext, is_disabled_ext = normalize_extension(ext_raw)
        lowered_actual = name.lower()
        effective_name = name[:-3] if is_disabled_ext and lowered_actual.endswith("off") else name
        if allowed_exts_set is not None and ext not in allowed_exts_set:
            if progress_cb:
                progress_cb(index, total, path, "filtered")
            continue
        if ignore_exts_set and (ext in ignore_exts_set or ext_raw.lower() in ignore_exts_set):
            if progress_cb:
                progress_cb(index, total, path, "ignored")
            continue
        if ignore_names_tokens and any(token in lowered_actual for token in ignore_names_tokens):
            if progress_cb:
                progress_cb(index, total, path, "ignored")
            continue
        try:
            size = human_mb(path.stat().st_size)
        except OSError as exc:
            errors.append(f"stat failed for {name}: {exc}")
            if progress_cb:
                progress_cb(index, total, path, "error")
            continue
        relpath_obj = path.relative_to(root)
        if selected_paths is not None:
            include_file = False
            for target in selected_paths:
                if target == Path("."):
                    if relpath_obj.parent == Path("."):
                        include_file = True
                        break
                else:
                    try:
                        relpath_obj.relative_to(target)
                        include_file = True
                        break
                    except ValueError:
                        continue
            if not include_file:
                if progress_cb:
                    progress_cb(index, total, path, "filtered")
                continue
        relpath = str(relpath_obj) if path != root else name
        folder_tokens = _tokenise_path_parts(relpath_obj.parent)
        tokens = _tokenise(effective_name)
        initial_category, confidence, notes, tags = _guess_from_name(effective_name, ext)
        category, confidence, notes, tags = refine_with_metadata(path, (initial_category, confidence, notes, tags))
        adult_evidence = inspect_adult_content(
            path,
            ext,
            tokens,
            category,
            relpath=relpath,
            folder_tokens=folder_tokens,
        )
        adult_note = adult_evidence.format_note()
        note_applied = False
        if adult_evidence.is_confident:
            promoted = _ensure_adult_category(category)
            if promoted != category:
                category = promoted
            confidence = max(confidence, min(1.0, 0.5 + adult_evidence.score / 2))
            if adult_note:
                notes = f"{notes}; {adult_note}" if notes else adult_note
                note_applied = True
        elif category.startswith("Adult") and not adult_evidence.hits and adult_evidence.score < 0.35:
            category = _strip_adult_category(category)
            demote_note = "Adult deep scan found no explicit keywords"
            notes = f"{notes}; {demote_note}" if notes else demote_note
            note_applied = True
        if not note_applied and adult_note and adult_evidence.hits:
            notes = f"{notes}; {adult_note}" if notes else adult_note
        if not include_adult and not is_disabled_ext and category.startswith("Adult"):
            if progress_cb:
                progress_cb(index, total, path, "filtered")
            continue
        target_folder = folder_map.get(category, folder_map.get("Unknown", "Unsorted"))
        raw_tags: List[str]
        if isinstance(tags, (list, tuple)):
            raw_tags = [str(tag) for tag in tags if str(tag).strip()]
        else:
            raw_tags = [tag.strip() for tag in str(tags).split(",") if tag.strip()] if tags else []
        if adult_evidence.hits:
            for hit in sorted(adult_evidence.hits):
                if hit not in raw_tags:
                    raw_tags.append(hit)
        meta_tags = ", ".join(raw_tags)
        extras: Dict[str, str] = {}
        if adult_evidence.hits:
            extras["adult_keywords"] = ", ".join(sorted(adult_evidence.hits))
        if adult_note and (adult_evidence.hits or adult_evidence.is_confident):
            extras["adult_note"] = adult_note
        if is_disabled_ext:
            extras["disabled"] = "extension renamed"
            if notes:
                notes = f"{notes}; Disabled extension"
            else:
                notes = "Disabled extension"
        item = FileItem(
            path=path,
            name=name,
            ext=ext,
            size_mb=size,
            relpath=relpath,
            guess_type=category,
            confidence=confidence,
            notes=notes,
            include=not is_disabled_ext,
            target_folder=target_folder,
            meta_tags=meta_tags,
            extras=extras,
            disabled=is_disabled_ext,
            original_ext=ext,
        )
        if is_disabled_ext:
            disabled_items.append(item)
        else:
            items.append(item)
        if progress_cb:
            progress_cb(index, total, path, "scanned")
    items.sort(
        key=lambda item: (
            CATEGORY_INDEX.get(item.guess_type, len(CATEGORY_ORDER)),
            _natural_key(os.path.dirname(item.relpath) or "."),
            _natural_key(item.name),
        )
    )
    disabled_items.sort(
        key=lambda item: (
            _natural_key(os.path.dirname(item.relpath) or "."),
            _natural_key(item.name),
        )
    )
    return ScanResult(items, total, errors, disabled_items)

# ---------------------------------------------------------------------------
# Bundling and move helpers
# ---------------------------------------------------------------------------

LOG_NAME = ".sims4_modsorter_moves.json"


def bundle_scripts_and_packages(items: Sequence[FileItem], folder_map: Dict[str, str]) -> Dict[str, int]:
    script_lookup: Dict[str, FileItem] = {}
    for item in items:
        if item.disabled:
            continue
        if item.ext in SCRIPT_EXTS and item.guess_type in {"Script Mod", "Adult Script"}:
            script_lookup[normalize_key(item.name)] = item
    linked = 0
    for item in items:
        if item.disabled:
            continue
        if item.ext == ".package":
            key = normalize_key(item.name)
            if key in script_lookup:
                item.bundle = key
                item.target_folder = script_lookup[key].target_folder
                if "paired with script" not in item.notes:
                    suffix = "; paired with script" if item.notes else "paired with script"
                    item.notes = f"{item.notes}{suffix}" if item.notes else "paired with script"
                linked += 1
    return {"scripts": len(script_lookup), "linked": linked}


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
    history.append({"ts": time.time(), "moves": moves})
    try:
        log_path.write_text(json.dumps(history, indent=2), encoding="utf-8")
    except Exception:
        pass


def undo_last_moves(mods_root: Path) -> Tuple[int, int, List[str]]:
    log_path = mods_root / LOG_NAME
    if not log_path.exists():
        return 0, 0, ["No log found"]
    try:
        history = json.loads(log_path.read_text(encoding="utf-8"))
        if not isinstance(history, list) or not history:
            return 0, 0, ["No moves recorded"]
    except Exception:
        return 0, 0, ["Log unreadable"]
    last = history.pop()
    moves = last.get("moves", []) if isinstance(last, dict) else []
    undone = 0
    failed = 0
    errors: List[str] = []
    for move in reversed(moves):
        src = Path(move.get("to", ""))
        dst = Path(move.get("from", ""))
        if not src:
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
    try:
        log_path.write_text(json.dumps(history, indent=2), encoding="utf-8")
    except Exception:
        pass
    return undone, failed, errors


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
        self.plugin_manager = load_user_plugins()
        self._plugin_columns: List[PluginColumn] = []
        if self.plugin_manager:
            self.plugin_manager.attach_app(self)
            self._plugin_columns = self.plugin_manager.get_columns()

        self.status_var = tk.StringVar(value="Ready")
        self.summary_var = tk.StringVar(value="No plan yet")

        self._ui_queue: "queue.Queue[Callable[[], None]]" = queue.Queue()
        self._theme_cache: Dict[str, str] = {}
        self._column_order: List[str] = []
        self._tooltip_payload: Dict[str, Dict[str, str]] = {}
        self._tooltip_window: Optional[tk.Toplevel] = None
        self._tooltip_label: Optional[tk.Label] = None
        self._tooltip_after: Optional[str] = None
        self._tooltip_target: Tuple[str, str] = ("", "")
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
        self._update_overlay_progress: Optional[ttk.Progressbar] = None
        self._update_overlay_download_btn: Optional[ttk.Button] = None
        self._update_overlay_skip_btn: Optional[ttk.Button] = None
        self._update_overlay_button_frame: Optional[ttk.Frame] = None
        self._update_overlay_details_btn: Optional[ttk.Button] = None
        self._update_overlay_visible: bool = False

        self.scan_folder_display = tk.StringVar(value="All folders")
        self.scan_folders: Optional[set[str]] = None
        self._available_folders: List[str] = []
        self._folder_menu_vars: Dict[str, tk.BooleanVar] = {}
        self._folder_menu_refresh_after: Optional[str] = None

        self._build_style()
        self._build_ui()
        self._build_settings_overlay()
        self.mods_root.trace_add("write", lambda *_: self._schedule_folder_menu_refresh())
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
        self.configure(bg=palette["bg"])

    def _build_ui(self) -> None:
        if self.plugin_manager:
            self._plugin_columns = self.plugin_manager.get_columns()
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
        ttk.Button(top, text="Browse", command=self.on_browse).pack(side="left", padx=4)
        self.btn_scan = ttk.Button(top, text="Scan", command=self.on_scan)
        self.btn_scan.pack(side="left", padx=4)
        ttk.Button(top, text="Export Plan", command=self.on_export).pack(side="left", padx=4)
        ttk.Label(top, textvariable=self.status_var).pack(side="left", padx=12)
        ttk.Button(top, text="⚙", width=3, command=self.show_settings).pack(side="right")
        ttk.Button(top, text="Plugin Status", command=self.show_mod_status_popup).pack(side="right", padx=6)
        ttk.Button(top, text="Undo Last", command=self.on_undo).pack(side="right", padx=6)

        mid = ttk.Frame(root_container)
        mid.pack(fill="both", expand=True, padx=12, pady=(6, 8))
        header = ttk.Frame(mid)
        header.pack(fill="x", pady=(0, 6))
        ttk.Label(header, textvariable=self.summary_var).pack(side="left")

        left = ttk.Frame(mid)
        left.pack(side="left", fill="both", expand=True)
        tree_frame = ttk.Frame(left)
        tree_frame.pack(fill="both", expand=True)
        base_columns = ["inc", "rel", "name", "size", "type", "target", "conf", "linked", "meta", "notes"]
        base_headings = {
            "inc": "✔",
            "rel": "Folder",
            "name": "File",
            "size": "MB",
            "type": "Type",
            "target": "Target Folder",
            "conf": "Conf",
            "linked": "Linked",
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
        self.tree.configure(yscroll=ysb.set, xscroll=xsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        ysb.grid(row=0, column=1, sticky="ns")
        xsb.grid(row=1, column=0, sticky="ew")
        tree_frame.grid_columnconfigure(0, weight=1)
        tree_frame.grid_rowconfigure(0, weight=1)

        right = ttk.Frame(mid)
        right.pack(side="left", fill="y", padx=(10, 0))
        ttk.Label(right, text="Selection").pack(anchor="w")
        self.sel_label = ttk.Label(right, text="None selected")
        self.sel_label.pack(anchor="w", pady=(0, 10))
        ttk.Label(right, text="Type").pack(anchor="w")
        self.type_cb = ttk.Combobox(right, values=CATEGORY_ORDER, state="readonly")
        self.type_cb.pack(fill="x", pady=(0, 8))
        ttk.Label(right, text="Target Folder").pack(anchor="w")
        self.target_entry = ttk.Entry(right)
        self.target_entry.pack(fill="x", pady=(0, 8))
        ttk.Button(right, text="Apply to Selected", command=self.on_apply_selected).pack(fill="x", pady=4)
        ttk.Button(right, text="Toggle Include", command=self.on_toggle_include).pack(fill="x", pady=4)
        ttk.Separator(right).pack(fill="x", pady=10)
        ttk.Label(right, text="Batch assign by keyword").pack(anchor="w")
        self.batch_keyword = ttk.Entry(right)
        self.batch_keyword.pack(fill="x", pady=(0, 6))
        ttk.Button(right, text="Assign Type to Matches", command=self.on_batch_assign).pack(fill="x")
        ttk.Separator(right).pack(fill="x", pady=10)
        ttk.Button(right, text="Recalculate Targets", command=self.on_recalc_targets).pack(fill="x", pady=4)
        ttk.Button(right, text="Select All", command=lambda: self.tree.selection_set(self.tree.get_children())).pack(fill="x", pady=2)
        ttk.Button(right, text="Select None", command=lambda: self.tree.selection_remove(self.tree.get_children())).pack(fill="x", pady=2)

        bottom = ttk.Frame(root_container)
        bottom.pack(fill="x", padx=12, pady=8)
        self.progress = ttk.Progressbar(bottom, orient="horizontal", mode="determinate")
        self.progress.pack(fill="x", side="left", expand=True)
        ttk.Button(bottom, text="Complete Sorting", command=self.on_complete).pack(side="right", padx=6)

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

        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self.tree.bind("<Double-1>", self.on_double_click)
        self.tree.bind("<Motion>", self._on_tree_motion)
        self.tree.bind("<Leave>", lambda _e: self._hide_tooltip())

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

    def log(self, message: str, level: str = "info") -> None:
        timestamp = time.strftime("%H:%M:%S")
        prefix = ""
        if level and level.lower() != "info":
            prefix = f"[{level.upper()}] "
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{timestamp}] {prefix}{message}\n")
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

        container = ttk.Frame(overlay, padding=20)
        container.pack(fill="both", expand=True)

        message = ttk.Label(
            container,
            textvariable=self._update_overlay_message,
            wraplength=360,
            justify="center",
        )
        message.pack(fill="x")

        progress = ttk.Progressbar(container, mode="indeterminate")
        progress.pack(fill="x", pady=(16, 12))

        buttons = ttk.Frame(container)
        buttons.pack()

        download_btn = ttk.Button(
            buttons,
            text="Download Update",
            command=self._on_update_overlay_download,
            state="disabled",
        )
        download_btn.pack(side="left", padx=4)

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

        self._update_overlay = overlay
        self._update_overlay_progress = progress
        self._update_overlay_download_btn = download_btn
        self._update_overlay_details_btn = details_btn
        self._update_overlay_skip_btn = skip_btn
        self._update_overlay_button_frame = buttons
        return overlay

    def _refresh_update_overlay_theme(self) -> None:
        overlay = getattr(self, "_update_overlay", None)
        if overlay and overlay.winfo_exists():
            palette = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
            overlay.configure(bg=palette.get("bg", "#111316"))

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
        progress: bool,
        enable_download: bool,
        enable_skip: bool,
        enable_details: bool,
    ) -> None:
        overlay = self._ensure_update_overlay()
        self._update_overlay_message.set(message)
        if self._update_overlay_progress:
            if progress:
                if not self._update_overlay_progress.winfo_manager():
                    self._update_overlay_progress.pack(fill="x", pady=(16, 12))
                self._update_overlay_progress.configure(mode="indeterminate", value=0)
                self._update_overlay_progress.start(12)
            else:
                self._update_overlay_progress.stop()
                if self._update_overlay_progress.winfo_manager():
                    self._update_overlay_progress.pack_forget()
        if self._update_overlay_download_btn:
            state = "normal" if enable_download else "disabled"
            self._update_overlay_download_btn.configure(state=state)
        if self._update_overlay_details_btn:
            state = "normal" if enable_details else "disabled"
            self._update_overlay_details_btn.configure(state=state)
        if self._update_overlay_skip_btn:
            state = "normal" if enable_skip else "disabled"
            self._update_overlay_skip_btn.configure(state=state)
        overlay.deiconify()
        overlay.lift()
        try:
            overlay.grab_set()
        except tk.TclError:
            pass
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
        if self._update_overlay_download_btn:
            self._update_overlay_download_btn.configure(state="disabled")
        if self._update_overlay_details_btn:
            self._update_overlay_details_btn.configure(state="disabled")
        if self._update_overlay_skip_btn:
            self._update_overlay_skip_btn.configure(state="disabled")
        self._update_overlay_visible = False

    def _on_update_overlay_download(self) -> None:
        self._start_update_download(manual=False)

    def _on_update_overlay_details(self) -> None:
        self._open_release_page()

    def _on_update_overlay_skip(self) -> None:
        self._hide_update_overlay()

    def _start_update_download(self, *, manual: bool) -> None:
        url = self._update_download_url
        if not url:
            if self._update_release_page_url:
                self._open_release_page()
            else:
                messagebox.showinfo("Update Available", "Download information is not configured.", parent=self)
            if not manual:
                self._hide_update_overlay()
            return

        destination = self._prompt_update_destination()
        if not destination:
            if not manual and self._update_overlay_visible:
                if self._update_overlay_download_btn:
                    self._update_overlay_download_btn.configure(state="normal")
                if self._update_overlay_skip_btn:
                    self._update_overlay_skip_btn.configure(state="normal")
                if self._update_overlay_details_btn and self._update_release_page_url:
                    self._update_overlay_details_btn.configure(state="normal")
            return

        target_path = Path(destination)
        self._show_update_overlay(
            "Downloading update…",
            progress=True,
            enable_download=False,
            enable_skip=False,
            enable_details=False,
        )
        if self._update_overlay_progress:
            self._update_overlay_progress.configure(mode="determinate", maximum=100, value=0)

        threading.Thread(
            target=self._download_update_worker,
            args=(url, target_path, manual),
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

    def _download_update_worker(self, url: str, target_path: Path, manual: bool) -> None:
        try:
            with urllib.request.urlopen(url) as response:
                total = int(response.headers.get("Content-Length") or 0)
                chunk_size = 1024 * 64
                written = 0
                with target_path.open("wb") as handle:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        handle.write(chunk)
                        written += len(chunk)
                        self._enqueue_ui(lambda w=written, t=total: self._update_download_progress(w, t))
        except Exception as exc:
            self._enqueue_ui(lambda e=exc: self._handle_update_download_failure(target_path, e, manual))
        else:
            self._enqueue_ui(lambda: self._handle_update_download_success(target_path, manual))

    def _update_download_progress(self, written: int, total: int) -> None:
        if self._update_overlay_progress:
            progress = self._update_overlay_progress
            progress.stop()
            if total > 0:
                progress.configure(mode="determinate", maximum=total, value=written)
            else:
                progress.configure(mode="indeterminate", value=0)
                progress.start(12)
        if total > 0:
            percent = min(100, max(0, int((written / total) * 100)))
            human_total = total / (1024 * 1024)
            human_written = written / (1024 * 1024)
            self._update_overlay_message.set(
                f"Downloading update… {percent}% ({human_written:.2f} / {human_total:.2f} MB)"
            )
        else:
            human_written = written / (1024 * 1024)
            self._update_overlay_message.set(f"Downloading update… {human_written:.2f} MB")

    def _handle_update_download_success(self, target_path: Path, manual: bool) -> None:
        self._hide_update_overlay()
        self.log(f"Update downloaded to {target_path}")
        message = f"Saved update to {target_path}\nInstall the update by running the downloaded package."
        if messagebox.askyesno("Update Downloaded", message + "\nOpen the containing folder?", parent=self):
            self._open_path(target_path.parent)

    def _handle_update_download_failure(self, target_path: Path, error: BaseException, manual: bool) -> None:
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
        if manual and self._update_release_page_url and messagebox.askyesno(
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
        self._start_update_check(manual=False)

    def _on_manual_update_check(self) -> None:
        self._start_update_check(manual=True)

    def _start_update_check(self, *, manual: bool) -> None:
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
                progress=True,
                enable_download=False,
                enable_skip=False,
                enable_details=False,
            )

        def worker() -> None:
            error_message: Optional[str] = None
            result: Optional[UpdateResult]
            try:
                result = check_for_update("app", APP_VERSION)
            except Exception as exc:  # pragma: no cover - defensive
                error_message = f"Update check failed: {exc}"
                result = None
            self._enqueue_ui(lambda: self._complete_update_check(result, manual, error_message))

        threading.Thread(target=worker, daemon=True).start()

    def _complete_update_check(
        self, result: Optional[UpdateResult], manual: bool, error_message: Optional[str]
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
                    progress=False,
                    enable_download=False,
                    enable_skip=True,
                    enable_details=False,
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
                    progress=False,
                    enable_download=False,
                    enable_skip=True,
                    enable_details=False,
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
            if manual:
                download_available = bool(self._update_download_url)
                if download_available:
                    prompt = base_message + "\nWould you like to download the update now?"
                    if messagebox.askyesno("Update Available", prompt, parent=self):
                        self._start_update_download(manual=True)
                    elif self._update_release_page_url and messagebox.askyesno(
                        "Update Available",
                        "Would you like to open the release page instead?",
                        parent=self,
                    ):
                        self._open_release_page()
                else:
                    info = (
                        base_message
                        + "\nDownload information is not configured. Would you like to view the release page?"
                    )
                    if self._update_release_page_url and messagebox.askyesno(
                        "Update Available", info, parent=self
                    ):
                        self._open_release_page()
                    else:
                        messagebox.showinfo(
                            "Update Available",
                            "Download information is not configured for this release.",
                            parent=self,
                        )
            else:
                if self._update_download_url:
                    message = (
                        base_message
                        + "\nChoose 'Download Update' to save the update automatically or 'Skip for Now' to continue."
                    )
                elif self._update_release_page_url:
                    message = (
                        base_message
                        + "\nUse 'View Release' to open the release page or 'Skip for Now' to continue."
                    )
                else:
                    message = (
                        base_message
                        + "\nDownload information is not configured for this release."
                    )
                self._show_update_overlay(
                    message,
                    progress=False,
                    enable_download=bool(self._update_download_url),
                    enable_skip=True,
                    enable_details=bool(self._update_release_page_url),
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
        self.items = list(result.items)
        self.items_by_path = {str(item.path): item for item in self.items}
        self.scan_errors = result.errors
        self._refresh_tree()
        self.status_var.set(f"Plan: {len(self.items)} files")
        self.log(
            f"Scan complete. Planned {len(self.items)} files. Linked packages: {stats['linked']} across {stats['scripts']} script(s)."
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
        for iid in selection:
            item = self.items_by_path.get(iid)
            if item:
                item.include = not item.include
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

        def worker() -> None:
            undone, failed, errors = undo_last_moves(mods_root)
            self._enqueue_ui(lambda: self._handle_undo_result(undone, failed, errors))

        threading.Thread(target=worker, daemon=True).start()

    def _handle_undo_result(self, undone: int, failed: int, errors: List[str]) -> None:
        self.log(f"Undo: {undone} restored, {failed} failed")
        for error in errors[:50]:
            self.log(error)
        self.on_scan()

    def on_export(self) -> None:
        if not self.items:
            self.log("No plan to export.")
            return
        filename = filedialog.asksaveasfilename(
            parent=self,
            defaultextension=".json",
            filetypes=[("JSON", "*.json")],
            title="Export plan to JSON",
        )
        if not filename:
            return
        data = [
            {
                "path": str(item.path),
                "name": item.name,
                "type": item.guess_type,
                "size": item.size_mb,
                "tags": item.meta_tags,
                "folder": item.target_folder,
                "dependency_status": item.dependency_status,
                "dependency_detail": item.dependency_detail,
                "extras": {
                    column.column_id: item.extras.get(column.column_id, "")
                    for column in self._plugin_columns
                    if isinstance(item.extras, dict)
                },
            }
            for item in self.items
        ]
        for entry in data:
            extras = entry.get("extras")
            if not extras:
                entry.pop("extras", None)
        try:
            Path(filename).write_text(json.dumps(data, indent=2), encoding="utf-8")
            self.log(f"Exported plan to {filename}")
        except Exception as exc:
            self.log(f"Export failed: {exc}")

    # ------------------------------------------------------------------
    # Table refresh
    # ------------------------------------------------------------------
    def _refresh_tree(self, preserve_selection: bool = False) -> None:
        selected = set(self.tree.selection()) if preserve_selection else set()
        self.tree.delete(*self.tree.get_children())
        self._tooltip_payload.clear()
        self.items_by_path = {str(item.path): item for item in self.items}
        counts: Dict[str, int] = {}
        for item in self.items:
            counts[item.guess_type] = counts.get(item.guess_type, 0) + 1
            row_map = {
                "inc": "✓" if item.include else "",
                "rel": os.path.dirname(item.relpath) or ".",
                "name": pretty_display_name(item.name),
                "size": f"{item.size_mb:.2f}",
                "type": item.guess_type,
                "target": item.target_folder,
                "conf": f"{item.confidence:.2f}",
                "linked": "🔗" if item.bundle else "",
                "meta": item.meta_tags,
                "notes": item.notes,
            }
            extras = getattr(item, "extras", {})
            if isinstance(extras, dict) and self._plugin_columns:
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
        if self.items:
            topcats = sorted(counts.items(), key=lambda pair: -pair[1])[:4]
            fragment = ", ".join(f"{name}: {count}" for name, count in topcats)
            self.summary_var.set(f"Planned {len(self.items)} files | {fragment}")
        else:
            self.summary_var.set("No plan yet")
        self._auto_size_columns()

    def _auto_size_columns(self) -> None:
        if not getattr(self, "tree", None):
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
        padding = 24
        widths: Dict[str, int] = {}
        for column in self._column_order:
            heading = self.tree.heading(column).get("text", column)
            widths[column] = heading_font.measure(str(heading)) + padding
        for iid in self.tree.get_children(""):
            values = self.tree.item(iid, "values")
            for index, column in enumerate(self._column_order):
                value = values[index] if index < len(values) else ""
                width = font.measure(str(value)) + padding
                if width > widths[column]:
                    widths[column] = width
        for column, width in widths.items():
            minimum = 36 if column in {"inc", "linked"} else 60
            self.tree.column(column, width=max(minimum, int(width)), stretch=False)

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
        "durex_bundle.zip": {"Adult Other", "Adult BuildBuy"},
        "uicheats.ts4script": {"Script Mod"},
        "fantasyhair.package": {"CAS Hair"},
        "object_sofa.package": {"BuildBuy Object"},
    }
    guess_sig = inspect.signature(guess_type_for_name)
    for name, expected in samples.items():
        if len(guess_sig.parameters) == 1:
            cat, _ = guess_type_for_name(name)
        else:
            cat, *_ = guess_type_for_name(name, Path(name).suffix)
        assert cat in expected, f"{name} -> {cat}, expected one of {sorted(expected)}"
    script_guess = _guess_from_name("awesome_mod.ts4script", ".ts4script")
    assert script_guess[0] == "Script Mod"
    archive_guess = _guess_from_name("poses.zip", ".zip")
    assert archive_guess[0] in {"Archive", "Adult Other", "Script Mod", "Adult BuildBuy"}
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
# Sims4 Mod Sorter — single file
# Python 3.10+
import importlib.util
import os, re, io, json, time, shutil, struct, zipfile, threading, tkinter as tk
import sys
from dataclasses import dataclass
from pathlib import Path
from tkinter import ttk, filedialog, messagebox
from typing import List, Dict, Tuple, Optional
import urllib.request
import urllib.parse


# ---------------------------
# Data model
# ---------------------------
@dataclass
class FileItem:
    path: str
    name: str
    ext: str
    size_mb: float
    relpath: str = ""
    guess_type: str = "Unknown"
    confidence: float = 0.0
    notes: str = ""
    include: bool = True
    target_folder: str = "Unknown"
    bundle: str = ""      # script-package pairing key
    meta_tags: str = ""   # e.g., "CASP, OBJD, STBL"

# ---------------------------
# Categories and folders
# ---------------------------

# --- relatedwords.io integration (condom) ---
RELATEDWORDS_CACHE = {}     # in-memory; you can persist later if you want
ADULT_RW_TERMS = set()      # words fetched from relatedwords.io for "condom"

def _fetch_relatedwords_io(term: str, max_terms: int = 150, timeout: int = 10) -> List[str]:
    """
    Fetches words from https://relatedwords.io/<term> and returns a cleaned list (lowercased).
    Keeps it simple and stdlib-only.
    """
    url = f"https://relatedwords.io/{urllib.parse.quote(term)}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    html = urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", "ignore")

    # pull anchor text; page has lots of <a> items for each word
    # filter out obvious UI anchors and junk; keep A–Z, spaces, hyphens
    raw = re.findall(r">([^<]{1,40})</a>", html)
    words = []
    seen = set()
    STOP = {"starting with a","starting with b","starting with c","starting with d","starting with e",
            "starting with f","starting with g","starting with h","starting with i","starting with j",
            "starting with k","starting with l","starting with m","starting with n","starting with o",
            "starting with p","starting with q","starting with r","starting with s","starting with t",
            "starting with u","starting with v","starting with w","starting with x","starting with y",
            "starting with z","close","sort by:","also related to:","highlight:"}
    for w in raw:
        w2 = w.strip().lower()
        if not w2 or w2 in seen: 
            continue
        if w2 in STOP: 
            continue
        if not all(ch.isalpha() or ch in " -'" for ch in w2):  # drop mixed junk
            continue
        seen.add(w2); words.append(w2)
        if len(words) >= max_terms:
            break
    return words

def _load_relatedwords_condom(force: bool = False) -> set:
    """
    Fill ADULT_RW_TERMS with condom-related words (top-N).
    Caches in-memory so we don’t hammer the site.
    """
    global ADULT_RW_TERMS
    if ADULT_RW_TERMS and not force:
        return ADULT_RW_TERMS
    try:
        if "condom" not in RELATEDWORDS_CACHE or force:
            RELATEDWORDS_CACHE["condom"] = _fetch_relatedwords_io("condom", max_terms=180)
        terms = set(RELATEDWORDS_CACHE["condom"])
        # trim generic noise that page sometimes includes
        NOISE = {"water","mask","butter","texas","cigarette","banana","sunscreen","marijuana",
                 "gluten","advertisement","advertisements","catholic","catholicism","diaper"}
        terms = {t for t in terms if t not in NOISE and len(t) >= 3}
        # keep the obvious contraception/sexual cluster only
        KEEP_PAT = re.compile(r"(condom|rubber|sheath|prophyl|latex|poly(?:urethane|isoprene)|"
                              r"contracept|birth control|sti|std|sexual|sex|intercourse|hiv|aids|"
                              r"penis|vagina|genital|ejac|orgasm|sperm|spermicid|lube|lubricant|"
                              r"durex|dam|diaphragm|female condom|chlamydia|syphilis|gonorrhea|hpv|iud)")
        terms = {t for t in terms if KEEP_PAT.search(t)}
        # ensure base tokens present
        seeds = {"condom","condoms","rubber","sheath","prophylactic","latex","spermicide",
                 "personal lubricant","lube","durex","female condom"}
        ADULT_RW_TERMS = terms | seeds
    except Exception:
        # network failed; fall back to seeds only
        ADULT_RW_TERMS = {"condom","condoms","rubber","sheath","prophylactic","latex","spermicide",
                          "personal lubricant","lube","durex","female condom"}
    return ADULT_RW_TERMS

KEYWORD_MAP: List[Tuple[str, str]] = [
    # ---- Known mods / script-ish names (non-adult) ----
    ("ui cheats", "Script Mod"),
    ("uicheats", "Script Mod"),
    ("mccc", "Script Mod"),

    # ---- CAS: clothing ----
    ("top", "CAS Clothing"), ("bottom", "CAS Clothing"),
    ("shirt", "CAS Clothing"), ("blouse", "CAS Clothing"),
    ("jacket", "CAS Clothing"), ("coat", "CAS Clothing"),
    ("dress", "CAS Clothing"), ("skirt", "CAS Clothing"),
    ("jeans", "CAS Clothing"), ("pants", "CAS Clothing"), ("trousers", "CAS Clothing"),
    ("shorts", "CAS Clothing"), ("legging", "CAS Clothing"),

    # ---- CAS: shoes ----
    ("heels", "CAS Clothing"), ("boots", "CAS Clothing"), ("sneaker", "CAS Clothing"), ("shoe", "CAS Clothing"),

    # ---- CAS: hair / face ----
    ("hair", "CAS Hair"),
    ("brow", "CAS Accessories"), ("eyebrow", "CAS Accessories"),
    ("lash", "CAS Accessories"), ("eyelash", "CAS Accessories"),

    # ---- CAS: makeup ----
    ("makeup", "CAS Makeup"), ("lipstick", "CAS Makeup"),
    ("blush", "CAS Makeup"), ("eyeliner", "CAS Makeup"),

    # ---- CAS: skin / overlays / tattoos ----
    ("skinoverlay", "CAS Skin"), ("overlay", "CAS Skin"),
    ("tattoo", "CAS Skin"), ("freckle", "CAS Skin"), ("scar", "CAS Skin"),

    # ---- CAS: eyes / eyewear ----
    ("eyes", "CAS Eyes"), ("iris", "CAS Eyes"),
    ("eyeglass", "CAS Accessories"), ("eyeglasses", "CAS Accessories"),
    ("spectacle", "CAS Accessories"), ("spectacles", "CAS Accessories"),
    ("sunglass", "CAS Accessories"), ("sunglasses", "CAS Accessories"),
    ("eyewear", "CAS Accessories"), ("goggle", "CAS Accessories"), ("goggles", "CAS Accessories"),
    ("glasses", "CAS Accessories"),

    # ---- CAS: jewellery / misc accessories ----
    ("ring", "CAS Accessories"), ("necklace", "CAS Accessories"),
    ("earring", "CAS Accessories"), ("piercing", "CAS Accessories"),
    ("nails", "CAS Accessories"), ("glove", "CAS Accessories"),

    # ---- Occults / tails etc. treated as accessories ----
    ("werewolf", "CAS Accessories"), ("tail", "CAS Accessories"),

    # ---- Build/Buy recolours / objects ----
    ("recolor", "BuildBuy Recolour"), ("recolour", "BuildBuy Recolour"), ("swatch", "BuildBuy Recolour"),
    ("object", "BuildBuy Object"), ("clutter", "BuildBuy Object"), ("deco", "BuildBuy Object"),
    ("furniture", "BuildBuy Object"), ("sofa", "BuildBuy Object"), ("chair", "BuildBuy Object"),
    ("table", "BuildBuy Object"), ("bed", "BuildBuy Object"),

    # ---- Misc content types ----
    ("animation", "Animation"), ("anim_", "Animation"),
    ("pose", "Pose"), ("preset", "Preset"), ("slider", "Slider"),
    ("world", "World"), ("override", "Override"),

    # ---- Adult overlays you mentioned (kept here so filename-only packs get caught) ----
    ("cum", "Adult CAS"), ("layer set", "Adult CAS"),
]

CATEGORY_ORDER = [
    "Script Mod",
    # Adult
    "Adult Script", "Adult Gameplay", "Adult Animation", "Adult Pose",
    "Adult CAS", "Adult BuildBuy", "Adult Override", "Adult Other",
    # Standard
    "Gameplay Tuning",
    "CAS Hair", "CAS Clothing", "CAS Makeup", "CAS Skin", "CAS Eyes", "CAS Accessories",
    "BuildBuy Object", "BuildBuy Recolour",
    "Animation", "Preset", "Pose", "Slider", "World", "Override",
    "Utility Tool", "Archive", "Other", "Unknown",
]

CATEGORY_INDEX = {name: idx for idx, name in enumerate(CATEGORY_ORDER)}

CATEGORY_INDEX = {name: idx for idx, name in enumerate(CATEGORY_ORDER)}

CATEGORY_INDEX = {name: idx for idx, name in enumerate(CATEGORY_ORDER)}

DEFAULT_FOLDER_MAP = {
    "Adult Script": "Adult - Scripts",
    "Adult Gameplay": "Adult - Gameplay",
    "Adult Animation": "Adult - Animations",
    "Adult Pose": "Adult - Poses",
    "Adult CAS": "Adult - CAS",
    "Adult BuildBuy": "Adult - Objects",
    "Adult Override": "Adult - Overrides",
    "Adult Other": "Adult - Other",
    "Script Mod": "Script Mods",
    "Gameplay Tuning": "Gameplay Mods",
    "CAS Hair": "CAS Hair", "CAS Clothing": "CAS Clothing", "CAS Makeup": "CAS Makeup",
    "CAS Skin": "CAS Skin", "CAS Eyes": "CAS Eyes", "CAS Accessories": "CAS Accessories",
    "BuildBuy Object": "BuildBuy Objects", "BuildBuy Recolour": "BuildBuy Recolours",
    "Animation": "Animations", "Preset": "Presets", "Pose": "Poses", "Slider": "Sliders",
    "World": "World", "Override": "Overrides", "Utility Tool": "Utilities",
    "Archive": "Archives", "Other": "Other", "Unknown": "Unsorted",
}

# Resource type IDs used inside .package (DBPF)
TYPE_IDS = {
    0x034AEECB: "CASP",        # CAS part
    0x319E4F1D: "COBJ/OBJD",   # object/definition
    0x02D5DF13: "JAZZ",        # animation state machine
    0x220557DA: "STBL",        # string table
    0x015A1849: "GEOM",        # geometry
    0x01661233: "MODL",
    0x01D10F34: "MLOD",
    0x0354796A: "TONE",        # skin tone
    0x067CAA11: "BGEO",
    0x00B2D882: "IMG",
}

ARCHIVE_EXTS = {".zip", ".rar", ".7z"}
PACKAGE_EXTS = {".package"}
SCRIPT_EXTS = {".ts4script", ".t4script", ".zip"}  # zip counted only if it truly contains Python

LOG_NAME = ".sims4_modsorter_moves.json"

# ---------------------------
# Themes
# ---------------------------
THEMES = {
    "Dark Mode": {"bg": "#111316", "fg": "#E6E6E6", "alt": "#161A1E", "accent": "#4C8BF5", "sel": "#2A2F3A"},
    "Slightly Dark Mode": {"bg": "#14161a", "fg": "#EAEAEA", "alt": "#1b1e24", "accent": "#6AA2FF", "sel": "#2f3642"},
    "Light Mode": {"bg": "#FAFAFA", "fg": "#1f2328", "alt": "#FFFFFF", "accent": "#316DCA", "sel": "#E8F0FE"},
    "High Contrast Mode": {"bg": "#000000", "fg": "#FFFFFF", "alt": "#000000", "accent": "#FFD400", "sel": "#333333"},
    "Pink Holiday": {"bg": "#1a1216", "fg": "#FFE7F3", "alt": "#23171e", "accent": "#FF5BA6", "sel": "#3a1f2c"},
}

# ---------------------------
# Plugin support
# ---------------------------

USER_PLUGINS_DIR = Path(__file__).with_name("user_plugins")
VERSION_FILE = Path(__file__).with_name("VERSION")


def _read_sorter_version():
    try:
        text = VERSION_FILE.read_text(encoding="utf-8").strip()
        return text or "0.0.0"
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
    for idx in range(max(len(left), len(right))):
        a = left[idx] if idx < len(left) else 0
        b = right[idx] if idx < len(right) else 0
        if a != b:
            return 1 if a > b else -1
    return 0


def _extract_plugin_version(manifest: Dict[str, object], entry_path: Path) -> str:
    version = manifest.get("version")
    if isinstance(version, str) and version.strip():
        return version.strip()
    version_file = entry_path.parent / "VERSION"
    if version_file.exists():
        try:
            text = version_file.read_text(encoding="utf-8").strip()
            if text:
                return text
        except Exception:
            pass
    return "Unknown"


def _manifest_compatibility(manifest: Dict[str, object]) -> Optional[str]:
    minimum = manifest.get("min_sorter_version") or manifest.get("min_app_version")
    maximum = manifest.get("max_sorter_version") or manifest.get("max_app_version")
    if isinstance(minimum, str) and minimum.strip():
        needed = minimum.strip()
        if _compare_versions(APP_VERSION, needed) < 0:
            return f"Requires sorter {needed}+"
    if isinstance(maximum, str) and maximum.strip():
        limit = maximum.strip()
        if _compare_versions(APP_VERSION, limit) > 0:
            return f"Incompatible beyond sorter {limit}"
    return None


class PluginAPI:
    """Lightweight API exposed to external plugins."""

    def __init__(self, manager: "PluginManager"):
        self._manager = manager

    def register_pre_scan_hook(self, func):
        if callable(func):
            self._manager.pre_scan_hooks.append(func)

    def register_post_scan_hook(self, func):
        if callable(func):
            self._manager.post_scan_hooks.append(func)

    def register_theme(self, name: str, palette: Dict[str, str]):
        required = {"bg", "fg", "alt", "accent", "sel"}
        if not name or not isinstance(palette, dict) or not required.issubset(palette.keys()):
            self._manager.log_boot(f"Theme registration skipped for '{name or '?'}' (invalid palette)", level="warn")
            return
        THEMES[name] = {key: palette[key] for key in required}
        self._manager.log_boot(f"Theme registered: {name}", level="info")

    def log(self, message: str, level: str = "info"):
        self._manager.log(message, level=level)


class PluginManager:
    def __init__(self, plugins_dir: Path):
        self.plugins_dir = plugins_dir
        self.pre_scan_hooks = []
        self.post_scan_hooks = []
        self.boot_messages: List[Tuple[str, str]] = []
        self.runtime_messages: List[Tuple[str, str]] = []
        self.api = PluginAPI(self)
        self.statuses: List[PluginStatus] = []

    def log_boot(self, message: str, level: str = "info"):
        self.boot_messages.append((level, message))

    def log(self, message: str, level: str = "info"):
        self.runtime_messages.append((level, message))

    def drain_boot_messages(self):
        msgs = list(self.boot_messages)
        self.boot_messages.clear()
        return msgs

    def drain_runtime_messages(self):
        msgs = list(self.runtime_messages)
        self.runtime_messages.clear()
        return msgs

    def load(self):
        self.plugins_dir.mkdir(parents=True, exist_ok=True)
        self.statuses.clear()
        for entry in sorted(self.plugins_dir.iterdir()):
            if entry.is_file() and entry.suffix == ".py":
                manifest = {
                    "name": entry.stem,
                    "entry": entry.name,
                    "enabled": True,
                    "callable": "register",
                }
                self._load_mod(manifest, entry)
            elif entry.is_dir():
                manifest_path = entry / "plugin.json"
                if manifest_path.exists():
                    try:
                        with open(manifest_path, "r", encoding="utf-8") as fh:
                            manifest = json.load(fh)
                    except Exception as exc:
                        self.log_boot(f"Failed to read manifest for {entry.name}: {exc}", level="error")
                        continue
                else:
                    manifest = {
                        "name": entry.name,
                        "entry": "plugin.py",
                        "enabled": True,
                        "callable": "register",
                    }
                self._load_mod(manifest, entry / manifest.get("entry", "plugin.py"))

    def _load_mod(self, manifest: Dict[str, object], entry_path: Path):
        name = str(manifest.get("name") or entry_path.stem)
        folder = entry_path.parent.name if entry_path.parent != entry_path else entry_path.stem
        version = _extract_plugin_version(manifest, entry_path)
        compatibility = _manifest_compatibility(manifest)
        if compatibility:
            self.log_boot(f"Plugin '{name}' skipped: {compatibility}", level="warn")
            self.statuses.append(PluginStatus(name, folder, version, "incompatible", compatibility))
            return
        if not manifest.get("enabled", True):
            self.log_boot(f"Skipping disabled plugin: {name}")
            self.statuses.append(PluginStatus(name, folder, version, "disabled", "Disabled"))
            return
        if not entry_path.exists():
            message = f"Missing entry: {entry_path.name}"
            self.log_boot(f"Missing entry file for mod '{name}': {entry_path.name}", level="error")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            return

        module_name = manifest.get("import_name") or f"user_mod_{name}"
        module_name = re.sub(r"[^0-9A-Za-z_]+", "_", module_name)
        try:
            spec = importlib.util.spec_from_file_location(module_name, entry_path)
            if spec is None or spec.loader is None:
                raise ImportError("Could not create spec")
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
        except Exception as exc:
            message = str(exc)
            self.log_boot(f"Failed to import mod '{name}': {exc}", level="error")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            return

        callable_name = manifest.get("callable", "register")
        register = getattr(module, callable_name, None)
        if not callable(register):
            message = f"Missing callable '{callable_name}'"
            self.log_boot(f"Plugin '{name}' missing callable '{callable_name}'", level="warn")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            return
        try:
            register(self.api)
        except Exception as exc:
            message = str(exc)
            self.log_boot(f"Plugin '{name}' failed during register: {exc}", level="error")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            return
        self.log_boot(f"Loaded plugin: {name}")
        self.statuses.append(PluginStatus(name, folder, version, "loaded", ""))

    def run_pre_scan(self, context: Dict):
        for hook in list(self.pre_scan_hooks):
            try:
                hook(context, self.api)
            except Exception as exc:
                self.log(f"Pre-scan hook error ({hook.__module__}): {exc}", level="error")

    def run_post_scan(self, items: List[FileItem], context: Dict):
        for hook in list(self.post_scan_hooks):
            try:
                hook(items, context, self.api)
            except Exception as exc:
                self.log(f"Post-scan hook error ({hook.__module__}): {exc}", level="error")

    def get_statuses(self) -> List[PluginStatus]:
        return list(self.statuses)


def load_user_plugins() -> PluginManager:
    manager = PluginManager(USER_PLUGINS_DIR)
    try:
        manager.load()
    except Exception as exc:
        manager.log_boot(f"Plugin loading aborted: {exc}", level="error")
    return manager


def flush_plugin_messages(app, phase: str) -> None:
    """Send buffered plugin messages to the UI log without assuming Tk internals."""
    manager = getattr(app, "plugin_manager", None)
    if not manager:
        return
    if phase == "runtime":
        drain = manager.drain_runtime_messages
    else:
        drain = manager.drain_boot_messages
    prefix_map = {
        "error": "Plugin error",
        "warn": "Plugin warning",
    }
    for level, message in drain():
        if not message:
            continue
        prefix = prefix_map.get(level, "Mod")
        app.log(f"{prefix}: {message}", level=level)

# ---------------------------
# Plugin support
# ---------------------------

USER_PLUGINS_DIR = Path(__file__).with_name("user_plugins")


@dataclass
class ToolbarButtonSpec:
    button_id: str
    text: str
    command: Callable[[], None]
    side: str = "left"
    padx: int = 4
    width: Optional[int] = None
    owner: str = "core"


@dataclass
class PluginToolbarAction:
    plugin_id: str
    button_id: str
    text: Optional[str] = None
    command: Optional[Callable[["Sims4ModSorterApp", "PluginAPI"], None]] = None
    side: Optional[str] = None
    position: Optional[int] = None
    insert_before: Optional[str] = None
    insert_after: Optional[str] = None
    width: Optional[int] = None
    padx: Optional[int] = None
    replace: bool = False


class PluginAPI:
    """Lightweight API exposed to external plugins."""

    def __init__(self, manager: "PluginManager", plugin_id: str):
        self._manager = manager
        self._plugin_id = plugin_id

    def register_pre_scan_hook(self, func):
        if callable(func):
            self._manager.register_pre_scan_hook(self._plugin_id, func)

    def register_post_scan_hook(self, func):
        if callable(func):
            self._manager.register_post_scan_hook(self._plugin_id, func)

    def register_theme(self, name: str, palette: Dict[str, str]):
        required = {"bg", "fg", "alt", "accent", "sel"}
        if not name or not isinstance(palette, dict) or not required.issubset(palette.keys()):
            self._manager.log_boot(
                f"Theme registration skipped for '{name or '?'}' (invalid palette)",
                level="warn",
            )
            return
        THEMES[name] = {key: palette[key] for key in required}
        self._manager.log_boot(f"Theme registered: {name}", level="info")

    def log(self, message: str, level: str = "info"):
        self._manager.log_from_plugin(self._plugin_id, message, level=level)

    @property
    def app(self) -> Optional["Sims4ModSorterApp"]:
        return self._manager.app

    def register_column(self, column_id: str, heading: str, *, width: int = 80, anchor: str = "center") -> None:
        if column_id and heading:
            self._manager.register_column(self._plugin_id, column_id, heading, width, anchor)

    def register_settings_section(
        self,
        title: str,
        builder: Callable[["Sims4ModSorterApp", ttk.Frame, "PluginAPI"], None],
    ) -> None:
        if callable(builder) and title:
            self._manager.register_settings_section(self._plugin_id, title, builder)

    def register_toolbar_button(
        self,
        button_id: str,
        *,
        text: Optional[str] = None,
        command: Optional[Callable[["Sims4ModSorterApp", "PluginAPI"], None]] = None,
        side: Optional[str] = None,
        position: Optional[int] = None,
        insert_before: Optional[str] = None,
        insert_after: Optional[str] = None,
        width: Optional[int] = None,
        padx: Optional[int] = None,
        replace: bool = False,
    ) -> None:
        self._manager.register_toolbar_button(
            self._plugin_id,
            button_id,
            text=text,
            command=command,
            side=side,
            position=position,
            insert_before=insert_before,
            insert_after=insert_after,
            width=width,
            padx=padx,
            replace=replace,
        )

    def is_feature_enabled(self, feature_id: str, default: bool = False) -> bool:
        return self._manager.is_feature_enabled(self._plugin_id, feature_id, default)

    def request_refresh(self) -> None:
        app = self.app
        if app is None:
            return
        refresh = getattr(app, "_refresh_tree", None)
        if callable(refresh):
            app.after(0, refresh)


class PluginManager:
    def __init__(self, plugins_dir: Path):
        self.plugins_dir = plugins_dir
        self.pre_scan_hooks: List[Tuple[str, Callable]] = []
        self.post_scan_hooks: List[Tuple[str, Callable]] = []
        self.boot_messages: List[Tuple[str, str]] = []
        self.runtime_messages: List[Tuple[str, str]] = []
        self.plugin_features: Dict[str, Dict[str, bool]] = {}
        self.plugin_names: Dict[str, str] = {}
        self._plugin_apis: Dict[str, PluginAPI] = {}
        self.toolbar_actions: List[PluginToolbarAction] = []
        self.columns: Dict[str, PluginColumn] = {}
        self.column_order: List[str] = []
        self.column_owners: Dict[str, str] = {}
        self.settings_sections: List[Tuple[str, Callable[["Sims4ModSorterApp", ttk.Frame, PluginAPI], None]]] = []
        self._settings_by_plugin: Dict[str, List[Tuple[str, Callable]]] = {}
        self.app: Optional["Sims4ModSorterApp"] = None
        self.statuses: List[PluginStatus] = []
        self._toolbar_shimmed: set[str] = set()

    def log_boot(self, message: str, level: str = "info"):
        self.boot_messages.append((level, message))

    def log(self, message: str, level: str = "info"):
        self.runtime_messages.append((level, message))

    def log_from_plugin(self, plugin_id: str, message: str, level: str = "info") -> None:
        name = self.plugin_names.get(plugin_id, plugin_id)
        formatted = f"[{name}] {message}" if name else message
        self.log(formatted, level=level)

    def drain_boot_messages(self):
        msgs = list(self.boot_messages)
        self.boot_messages.clear()
        return msgs

    def drain_runtime_messages(self):
        msgs = list(self.runtime_messages)
        self.runtime_messages.clear()
        return msgs

    def attach_app(self, app: "Sims4ModSorterApp") -> None:
        self.app = app

    def load(self):
        self.plugins_dir.mkdir(parents=True, exist_ok=True)
        self.pre_scan_hooks.clear()
        self.post_scan_hooks.clear()
        self.toolbar_actions.clear()
        self.plugin_features.clear()
        self.plugin_names.clear()
        self._plugin_apis.clear()
        self.statuses.clear()
        self.columns.clear()
        self.column_order.clear()
        self.column_owners.clear()
        self.settings_sections.clear()
        self._settings_by_plugin.clear()
        for entry in sorted(self.plugins_dir.iterdir()):
            if entry.is_file() and entry.suffix == ".py":
                manifest = {
                    "name": entry.stem,
                    "entry": entry.name,
                    "enabled": True,
                    "callable": "register",
                }
                self._load_mod(manifest, entry)
            elif entry.is_dir():
                manifest_path = entry / "plugin.json"
                if manifest_path.exists():
                    try:
                        with open(manifest_path, "r", encoding="utf-8") as fh:
                            manifest = json.load(fh)
                    except Exception as exc:
                        self.log_boot(f"Failed to read manifest for {entry.name}: {exc}", level="error")
                        continue
                else:
                    manifest = {
                        "name": entry.name,
                        "entry": "plugin.py",
                        "enabled": True,
                        "callable": "register",
                    }
                self._load_mod(manifest, entry / manifest.get("entry", "plugin.py"))

    def _load_mod(self, manifest: Dict[str, object], entry_path: Path):
        name = str(manifest.get("name") or entry_path.stem)
        folder = entry_path.parent.name if entry_path.parent != entry_path else entry_path.stem
        version = _extract_plugin_version(manifest, entry_path)
        compatibility = _manifest_compatibility(manifest)
        if compatibility:
            self.log_boot(f"Plugin '{name}' skipped: {compatibility}", level="warn")
            self.statuses.append(PluginStatus(name, folder, version, "incompatible", compatibility))
            return
        if not manifest.get("enabled", True):
            self.log_boot(f"Skipping disabled plugin: {name}")
            self.statuses.append(PluginStatus(name, folder, version, "disabled", "Disabled"))
            return
        if not entry_path.exists():
            message = f"Missing entry: {entry_path.name}"
            self.log_boot(f"Missing entry file for mod '{name}': {entry_path.name}", level="error")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            return

        plugin_id = self._derive_plugin_id(manifest, entry_path)
        self.plugin_names[plugin_id] = name
        self.plugin_features[plugin_id] = self._extract_feature_flags(manifest)

        module_name = manifest.get("import_name") or f"user_mod_{name}"
        module_name = re.sub(r"[^0-9A-Za-z_]+", "_", module_name)
        try:
            spec = importlib.util.spec_from_file_location(module_name, entry_path)
            if spec is None or spec.loader is None:
                raise ImportError("Could not create spec")
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
        except Exception as exc:
            message = str(exc)
            self.log_boot(f"Failed to import mod '{name}': {exc}", level="error")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            self._cleanup_plugin(plugin_id)
            return

        callable_name = manifest.get("callable", "register")
        register = getattr(module, callable_name, None)
        if not callable(register):
            message = f"Missing callable '{callable_name}'"
            self.log_boot(f"Plugin '{name}' missing callable '{callable_name}'", level="warn")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            self._cleanup_plugin(plugin_id)
            return
        try:
            api = PluginAPI(self, plugin_id)
            self._plugin_apis[plugin_id] = api
            setattr(api, "_plugin_id", plugin_id)
            self._ensure_toolbar_api(api, plugin_id, name)
            register(api)
        except Exception as exc:
            message = str(exc)
            self.log_boot(f"Plugin '{name}' failed during register: {exc}", level="error")
            self.statuses.append(PluginStatus(name, folder, version, "error", message))
            self._cleanup_plugin(plugin_id)
            return
        self.log_boot(f"Loaded plugin: {name}")
        self.statuses.append(PluginStatus(name, folder, version, "loaded", ""))

    def register_pre_scan_hook(self, plugin_id: str, func: Callable) -> None:
        self.pre_scan_hooks.append((plugin_id, func))

    def register_post_scan_hook(self, plugin_id: str, func: Callable) -> None:
        self.post_scan_hooks.append((plugin_id, func))

    def run_pre_scan(self, context: Dict):
        for plugin_id, hook in list(self.pre_scan_hooks):
            api = self._plugin_apis.get(plugin_id)
            if not api:
                continue
            try:
                hook(context, api)
            except Exception as exc:
                self.log(f"Pre-scan hook error ({hook.__module__}): {exc}", level="error")

    def run_post_scan(self, items: List[FileItem], context: Dict):
        for plugin_id, hook in list(self.post_scan_hooks):
            api = self._plugin_apis.get(plugin_id)
            if not api:
                continue
            try:
                hook(items, context, api)
            except Exception as exc:
                self.log(f"Post-scan hook error ({hook.__module__}): {exc}", level="error")

    def register_column(self, plugin_id: str, column_id: str, heading: str, width: int, anchor: str) -> None:
        normalized = column_id.strip()
        if not normalized:
            return
        owner = self.column_owners.get(normalized)
        if owner and owner != plugin_id:
            self.log_boot(
                f"Plugin column '{normalized}' already defined; skipping duplicate from {self.plugin_names.get(plugin_id, plugin_id)}",
                level="warn",
            )
            return
        self.columns[normalized] = PluginColumn(normalized, heading, width, anchor)
        self.column_owners[normalized] = plugin_id
        if normalized not in self.column_order:
            self.column_order.append(normalized)

    def get_columns(self) -> List[PluginColumn]:
        return [self.columns[column_id] for column_id in self.column_order if column_id in self.columns]

    def register_settings_section(self, plugin_id: str, title: str, builder: Callable) -> None:
        cleaned = title.strip()
        if not cleaned or not callable(builder):
            return
        entry = (cleaned, builder)
        self.settings_sections.append(entry)
        self._settings_by_plugin.setdefault(plugin_id, []).append(entry)

    def get_statuses(self) -> List[PluginStatus]:
        return list(self.statuses)

    def register_toolbar_button(
        self,
        plugin_id: str,
        button_id: str,
        *,
        text: Optional[str] = None,
        command: Optional[Callable[["Sims4ModSorterApp", "PluginAPI"], None]] = None,
        side: Optional[str] = None,
        position: Optional[int] = None,
        insert_before: Optional[str] = None,
        insert_after: Optional[str] = None,
        width: Optional[int] = None,
        padx: Optional[int] = None,
        replace: bool = False,
    ) -> None:
        if not button_id:
            return
        action = PluginToolbarAction(
            plugin_id=plugin_id,
            button_id=button_id,
            text=text,
            command=command,
            side=side,
            position=position,
            insert_before=insert_before,
            insert_after=insert_after,
            width=width,
            padx=padx,
            replace=replace,
        )
        self.toolbar_actions.append(action)

    def resolve_toolbar_buttons(
        self, app: "Sims4ModSorterApp", base: List[ToolbarButtonSpec]
    ) -> List[ToolbarButtonSpec]:
        resolved: List[ToolbarButtonSpec] = [
            ToolbarButtonSpec(
                button_id=spec.button_id,
                text=spec.text,
                command=spec.command,
                side=spec.side,
                padx=spec.padx,
                width=spec.width,
                owner=spec.owner,
            )
            for spec in base
        ]
        spec_map = {spec.button_id: spec for spec in resolved}

        def insert_spec(spec: ToolbarButtonSpec, action: PluginToolbarAction) -> None:
            def find_index(target_id: str) -> Optional[int]:
                for idx, existing in enumerate(resolved):
                    if existing.button_id == target_id:
                        return idx
                return None

            if action.insert_before:
                idx = find_index(action.insert_before)
                if idx is not None:
                    resolved.insert(idx, spec)
                    return
            if action.insert_after:
                idx = find_index(action.insert_after)
                if idx is not None:
                    resolved.insert(idx + 1, spec)
                    return
            if action.position is not None:
                position = max(0, action.position)
                count = 0
                for idx, existing in enumerate(resolved):
                    if existing.side == spec.side:
                        if count >= position:
                            resolved.insert(idx, spec)
                            return
                        count += 1
                resolved.append(spec)
                return
            last_idx = None
            for idx, existing in enumerate(resolved):
                if existing.side == spec.side:
                    last_idx = idx
            if last_idx is None:
                resolved.append(spec)
            else:
                resolved.insert(last_idx + 1, spec)

        for action in self.toolbar_actions:
            spec = spec_map.get(action.button_id)
            if spec is not None and (action.replace or action.button_id in spec_map):
                if action.text is not None:
                    spec.text = action.text
                if action.side in {"left", "right"}:
                    spec.side = action.side
                if action.width is not None:
                    spec.width = action.width
                if action.padx is not None:
                    spec.padx = action.padx
                if action.command is not None:
                    spec.command = self._wrap_button_command(action, app)
                    spec.owner = action.plugin_id
                if any((action.insert_before, action.insert_after, action.position is not None)):
                    resolved.remove(spec)
                    insert_spec(spec, action)
                continue
            if action.button_id in spec_map and not action.replace:
                self.log_boot(
                    f"Toolbar button '{action.button_id}' already defined; skipping duplicate from {action.plugin_id}",
                    level="warn",
                )
                continue
            if action.command is None:
                self.log_boot(
                    f"Toolbar button '{action.button_id}' from {action.plugin_id} has no command and was ignored",
                    level="warn",
                )
                continue
            new_spec = ToolbarButtonSpec(
                button_id=action.button_id,
                text=action.text or action.button_id,
                command=self._wrap_button_command(action, app),
                side=action.side if action.side in {"left", "right"} else "left",
                padx=action.padx if action.padx is not None else 4,
                width=action.width,
                owner=action.plugin_id,
            )
            spec_map[action.button_id] = new_spec
            insert_spec(new_spec, action)
        return resolved

    def is_feature_enabled(self, plugin_id: str, feature_id: str, default: bool = False) -> bool:
        flags = self.plugin_features.get(plugin_id)
        if not flags:
            return default
        return bool(flags.get(feature_id, default))

    def _wrap_button_command(self, action: PluginToolbarAction, app: "Sims4ModSorterApp") -> Callable[[], None]:
        api = self._plugin_apis.get(action.plugin_id)

        def _callback() -> None:
            if not action.command or api is None:
                return
            try:
                action.command(app, api)
            except Exception as exc:
                self.log(
                    f"Toolbar button '{action.button_id}' from {action.plugin_id} failed: {exc}",
                    level="error",
                )

        return _callback

    def _derive_plugin_id(self, manifest: Dict[str, object], entry_path: Path) -> str:
        candidate = manifest.get("id") or manifest.get("plugin_id") or manifest.get("name")
        if isinstance(candidate, str) and candidate.strip():
            base = candidate.strip()
        else:
            base = entry_path.parent.name if entry_path.parent != entry_path else entry_path.stem
        sanitized = re.sub(r"[^0-9A-Za-z_-]+", "-", base).strip("-_")
        return sanitized or entry_path.stem

    def _extract_feature_flags(self, manifest: Dict[str, object]) -> Dict[str, bool]:
        flags: Dict[str, bool] = {}
        raw = manifest.get("features")
        if not isinstance(raw, list):
            return flags
        for item in raw:
            if not isinstance(item, dict):
                continue
            feature_id = str(item.get("id") or item.get("feature_id") or "").strip()
            if not feature_id:
                continue
            enabled = item.get("enabled")
            if enabled is None:
                default_value = item.get("default")
                enabled_flag = bool(default_value if default_value is not None else True)
            else:
                enabled_flag = bool(enabled)
            flags[feature_id] = enabled_flag
        return flags

    def _cleanup_plugin(self, plugin_id: str) -> None:
        self.plugin_features.pop(plugin_id, None)
        self.plugin_names.pop(plugin_id, None)
        self._plugin_apis.pop(plugin_id, None)
        self.pre_scan_hooks = [(pid, hook) for pid, hook in self.pre_scan_hooks if pid != plugin_id]
        self.post_scan_hooks = [(pid, hook) for pid, hook in self.post_scan_hooks if pid != plugin_id]
        self.toolbar_actions = [action for action in self.toolbar_actions if action.plugin_id != plugin_id]
        for column_id, owner in list(self.column_owners.items()):
            if owner == plugin_id:
                self.column_owners.pop(column_id, None)
                self.columns.pop(column_id, None)
                if column_id in self.column_order:
                    self.column_order.remove(column_id)
        sections = self._settings_by_plugin.pop(plugin_id, [])
        if sections:
            self.settings_sections = [entry for entry in self.settings_sections if entry not in sections]

    def _ensure_toolbar_api(self, api: PluginAPI, plugin_id: str, plugin_name: str) -> None:
        if hasattr(api, "register_toolbar_button"):
            return
        manager = self

        def _register_toolbar_button(
            self_api,
            button_id: str,
            *,
            text: Optional[str] = None,
            command: Optional[Callable[["Sims4ModSorterApp", "PluginAPI"], None]] = None,
            side: Optional[str] = None,
            position: Optional[int] = None,
            insert_before: Optional[str] = None,
            insert_after: Optional[str] = None,
            width: Optional[int] = None,
            padx: Optional[int] = None,
            replace: bool = False,
        ) -> None:
            handler = getattr(manager, "register_toolbar_button", None)
            if not callable(handler):
                key = plugin_name or plugin_id
                if key not in manager._toolbar_shimmed:
                    manager._toolbar_shimmed.add(key)
                    manager.log_boot(
                        f"Toolbar support unavailable; '{key}' button '{button_id}' was ignored.",
                        level="warn",
                    )
                return
            handler(
                plugin_id,
                button_id,
                text=text,
                command=command,
                side=side,
                position=position,
                insert_before=insert_before,
                insert_after=insert_after,
                width=width,
                padx=padx,
                replace=replace,
            )

        api.register_toolbar_button = MethodType(_register_toolbar_button, api)
        if plugin_name not in self._toolbar_shimmed:
            self._toolbar_shimmed.add(plugin_name)
            self.log_boot(
                f"Applied toolbar compatibility shim for plugin '{plugin_name}'.",
                level="info",
            )


def load_user_plugins() -> PluginManager:
    manager = PluginManager(USER_PLUGINS_DIR)
    try:
        manager.load()
    except Exception as exc:
        manager.log_boot(f"Plugin loading aborted: {exc}", level="error")
    return manager


def flush_plugin_messages(app, phase: str) -> None:
    """Send buffered plugin messages to the UI log without assuming Tk internals."""
    manager = getattr(app, "plugin_manager", None)
    if not manager:
        return
    if phase == "runtime":
        drain = manager.drain_runtime_messages
    else:
        drain = manager.drain_boot_messages
    prefix_map = {
        "error": "Plugin error",
        "warn": "Plugin warning",
    }
    for level, message in drain():
        if not message:
            continue
        prefix = prefix_map.get(level, "Mod")
        app.log(f"{prefix}: {message}", level=level)

# ---------------------------
# Utilities
# ---------------------------

def _u32(b, off):
    return int.from_bytes(b[off:off+4], 'little', signed=False)

def dbpf_scan_types(path: str) -> Dict[int, int]:
    """
    Parse the DBPF index and return a {type_id: count} map.
    Works with Sims 4 packages that use the bit-packed index header.
    """
    out: Dict[int, int] = {}
    with open(path, 'rb') as f:
        head = f.read(96)
        if len(head) < 96 or head[:4] != b'DBPF':
            return out

        # These offsets work with TS4 packages. If your pack differs, we still fail safe.
        try:
            count      = _u32(head, 0x20)
            index_pos  = _u32(head, 0x40)
            if not count or not index_pos:
                return out
        except Exception:
            return out

        f.seek(index_pos)
        # Bit flags tell which of 8 dwords are stored once in the header.
        # We only need ResourceType (slot 0), so the generic reconstruction is fine.
        try:
            flags = int.from_bytes(f.read(4), 'little')
            header_vals = []
            flagged_slots = [i for i in range(8) if (flags >> i) & 1]
            for _ in flagged_slots:
                header_vals.append(int.from_bytes(f.read(4), 'little'))
            per_entry_dwords = 8 - len(flagged_slots)

            for _ in range(count):
                entry_vals = [int.from_bytes(f.read(4), 'little') for __ in range(per_entry_dwords)]
                vals = {}
                hi = 0
                mi = 0
                for b in range(8):
                    if b in flagged_slots:
                        vals[b] = header_vals[hi]; hi += 1
                    else:
                        vals[b] = entry_vals[mi]; mi += 1
                rtype = vals.get(0, 0)  # slot 0 = ResourceType
                out[rtype] = out.get(rtype, 0) + 1
        except Exception:
            # Fail silent; metadata not available
            return {}
    return out

def classify_from_types(types: Dict[int, int], filename: str, adult_hint: bool) -> Tuple[str, float, str]:
    if not types:
        return ("Unknown", 0.5, "No DBPF index",)
    name = filename.lower()
    has = lambda tid: tid in types
    notes = "Types: " + ", ".join(f"{TYPE_IDS.get(t, hex(t))}:{n}" for t, n in types.items())

    # CAS
    if has(0x034AEECB):
        if adult_hint: return ("Adult CAS", 0.9, notes)
        if any(k in name for k in ("glasses","eyeglass","spectacle","sunglass","eyewear","goggle")):
            return ("CAS Accessories", 0.9, notes)
        if any(k in name for k in ("hair","brow","lash")):   return ("CAS Hair", 0.85, notes)
        if any(k in name for k in ("lip","liner","blush","makeup")): return ("CAS Makeup", 0.85, notes)
        if any(k in name for k in ("skin","overlay","tattoo","freckle")): return ("CAS Skin", 0.85, notes)
        if any(k in name for k in ("eye","eyes","iris")):    return ("CAS Eyes", 0.85, notes)
        if any(k in name for k in ("ring","necklace","ear","nail","piercing","tail")): return ("CAS Accessories", 0.85, notes)
        return ("CAS Clothing", 0.8, notes)

    # Objects / geometry
    if has(0x319E4F1D) or has(0x015A1849) or has(0x01661233) or has(0x01D10F34):
        return (("Adult BuildBuy" if adult_hint else "BuildBuy Object"), 0.85, notes)

    # Skintones
    if has(0x0354796A):  # TONE
        return (("Adult CAS" if adult_hint else "CAS Skin"), 0.85, notes)

    # Animations
    if has(0x02D5DF13):  # JAZZ
        return (("Adult Animation" if adult_hint else "Animation"), 0.85, notes)

    # Gameplay-ish (STBL alone isn’t definitive, but it’s a hint)
    if has(0x220557DA):
        return (("Adult Gameplay" if adult_hint else "Gameplay Tuning"), 0.75, notes)

    return (("Adult Other" if adult_hint else "Other"), 0.6, notes)

def get_default_mods_path() -> str:
    home = os.path.expanduser("~")
    candidates = [
        os.path.join(home, "Documents", "Electronic Arts", "The Sims 4", "Mods"),
        os.path.join(home, "OneDrive", "Documents", "Electronic Arts", "The Sims 4", "Mods"),
    ]
    for p in candidates:
        if os.path.isdir(p):
            return p
    return candidates[0]

def human_mb(nbytes: int) -> float:
    return round(nbytes / (1024 * 1024), 2)

def pretty_display_name(filename: str) -> str:
    base = re.sub(r'\.[^.]+$', '', filename)       # strip extension
    base = re.sub(r'[_\-]+', ' ', base)            # underscores/dashes -> spaces
    base = re.sub(r'\s+', ' ', base).strip()
    parts = []
    for w in base.split(' '):
        if len(w) <= 4 and w.isupper():
            parts.append(w)                     # keep ALLCAPS short tokens like WCC
        else:
            parts.append(w[:1].upper() + w[1:])
    return ' '.join(parts)

def normalize_key(filename: str) -> str:
    base = re.sub(r'\.[^.]+$', '', filename).lower()
    base = re.sub(r'\[[^\]]+\]', '', base)    # drop bracket tags like [Author]
    base = re.sub(r'[_\-\s]+', '', base)
    base = re.sub(r'[^a-z0-9]+', '', base)
    return base

_NAT_SORT_RE = re.compile(r'(\d+)')

def _natural_key(value: str):
    parts = _NAT_SORT_RE.split(value.lower())
    out = []
    for part in parts:
        out.append(int(part) if part.isdigit() else part)
    return out

ADULT_TOKENS = {
    "wickedwhims", "turbodriver", "basemental", "nisa", "wild_guy",
    "nsfw", "porn", "sex", "nude", "naked", "strip", "lapdance", "prostitution",
    "genital", "penis", "vagina", "condom", "dildo", "vibrator", "plug", "cum",
    "petplay", "nudity", "desires", "deviant", "deviantcore", "devious", "deviousdesires",
    "dd", "kink", "flirtyfetishes", "fetish", "fetishes", "ww", "gay", "pubic", "pubichair",
    "watersports", "brothel"
}

def is_ts4script_or_zip_script(path: str) -> bool:
    ext_raw = os.path.splitext(path)[1]
    ext, _ = normalize_extension(ext_raw)
    if ext not in SCRIPT_EXTS:
        return False
    try:
        with zipfile.ZipFile(path, 'r') as z:
            for n in z.namelist():
                nl = n.lower()
                if nl.endswith('.py') or nl.endswith('.pyc'):
                    return True
    except Exception:
        return False
    return False

def classify_from_types(types: Dict[int, int], filename: str, adult_hint: bool) -> Tuple[str, float, str, str]:
    if not types:
        return ("Unknown", 0.5, "No DBPF index or empty", "")
    tags = [TYPE_IDS.get(t, hex(t)) for t in sorted(types.keys())]
    notes = "Types: " + ', '.join(f"{TYPE_IDS.get(t, hex(t))}:{c}" for t, c in types.items())
    has = lambda tid: tid in types
    name = filename.lower()
    # CAS
    if has(0x034AEECB):
        if adult_hint:
            return ("Adult CAS", 0.9, notes, ', '.join(tags))
        if any(k in name for k in ["hair", "brow", "lash"]): return ("CAS Hair", 0.85, notes, ', '.join(tags))
        if any(k in name for k in ["lip", "liner", "blush", "makeup"]): return ("CAS Makeup", 0.8, notes, ', '.join(tags))
        if any(k in name for k in ["skin", "overlay"]): return ("CAS Skin", 0.8, notes, ', '.join(tags))
        if any(k in name for k in ["eye", "iris"]): return ("CAS Eyes", 0.8, notes, ', '.join(tags))
        if any(k in name for k in ["ring", "necklace", "ear", "nail", "piercing"]): return ("CAS Accessories", 0.8, notes, ', '.join(tags))
        return ("CAS Clothing", 0.8, notes, ', '.join(tags))
    # Objects
    if has(0x319E4F1D) or has(0x015A1849) or has(0x01661233) or has(0x01D10F34):
        return (("Adult BuildBuy" if adult_hint else "BuildBuy Object"), 0.85, notes, ', '.join(tags))
    # Skintones
    if has(0x0354796A):
        return (("Adult CAS" if adult_hint else "CAS Skin"), 0.85, notes, ', '.join(tags))
    # Animation/JAZZ
    if has(0x02D5DF13):
        return (("Adult Animation" if adult_hint else "Animation"), 0.85, notes, ', '.join(tags))
    # Gameplay-ish (string tables often accompany tuning)
    if has(0x220557DA):
        return (("Adult Gameplay" if adult_hint else "Gameplay Tuning"), 0.75, notes, ', '.join(tags))
    return (("Adult Other" if adult_hint else "Other"), 0.6, notes, ', '.join(tags))

def guess_type_for_name(name: str, ext: str) -> Tuple[str, float, str]:
    n = name.lower()

    # union of your existing adult hints + live terms from relatedwords.io/condom
    base_adult = {
        "wickedwhims","turbodriver","basemental","nisa","wild_guy","nsfw","porn","sex","nude",
        "naked","strip","lapdance","prostitution","erotic","aphrodisiac","genital","penis","vagina",
        "petplay","nudity","desires","deviant","deviantcore","devious","deviousdesires","dd","kink",
        "flirtyfetishes","fetish","fetishes","ww","gay","pubic","pubichair","watersports"
    }
    author_terms = {name.lower() for name in ADULT_AUTHORS}
    author_terms.update(ADULT_AUTHOR_ALIASES.keys())
    condom_terms = _load_relatedwords_condom()     # fetched once per run
    adult_terms = base_adult | condom_terms | author_terms

    is_adult = any(t in n for t in adult_terms)

    # If it contains a condom-related word specifically, bias to Adult BuildBuy
    # (A condom is an object; if the filename also screams CAS we’ll override below)
    has_condomish = any(t in n for t in condom_terms)

    # ... keep your existing ext checks ...
    # (no change needed above this comment block)

    if is_adult:
        if ("anim" in n) or ("animation" in n):
            return ("Adult Animation", 0.9, "Adult keyword + animation")
        if "pose" in n:
            return ("Adult Pose", 0.85, "Adult keyword + pose")

        cas_terms = ("hair","top","dress","skirt","makeup","lipstick","blush","eyeliner",
                     "skin","overlay","lingerie","bra","panties","nipple","areola",
                     "pubic","genital","penis","vagina","heels","stockings","tattoo","freckle")
        if any(t in n for t in cas_terms):
            # condom-ish beats nothing; but CAS beats object if clearly CAS
            if has_condomish and not any(t in n for t in ("hair","dress","top","skirt","tattoo","freckle")):
                return ("Adult BuildBuy", 0.85, "relatedwords.io condom term")
            return ("Adult CAS", 0.85, "Adult keyword + CAS")

        if has_condomish:
            return ("Adult BuildBuy", 0.85, "relatedwords.io condom term")

        if "override" in n:
            return ("Adult Override", 0.8, "Adult keyword + override")

        return ("Adult Gameplay", 0.75, "Adult keyword")

    # Non-adult heuristics -------------------------------------------------
    if ext in SCRIPT_EXTS:
        return ("Script Mod", 0.85, "Script-like extension")

    if ext in ARCHIVE_EXTS:
        return ("Archive", 0.5, "Archive container")

    if ext == ".package":
        for kw, cat in KEYWORD_MAP:
            if kw in n:
                return (cat, 0.65, f"Keyword '{kw}'")
        return ("Other", 0.4, "Package (no keyword match)")

    # Utilities / configs / leftovers
    if ext in {".cfg", ".ini", ".log", ".txt", ".md"}:
        return ("Utility Tool", 0.45, "Utility/config file")

    return ("Unknown", 0.3, "Unrecognised extension")

    # Non-adult heuristics -------------------------------------------------
    if ext in SCRIPT_EXTS:
        return ("Script Mod", 0.85, "Script-like extension")

    if ext in ARCHIVE_EXTS:
        return ("Archive", 0.5, "Archive container")

    if ext == ".package":
        for kw, cat in KEYWORD_MAP:
            if kw in n:
                return (cat, 0.65, f"Keyword '{kw}'")
        return ("Other", 0.4, "Package (no keyword match)")

    # Utilities / configs / leftovers
    if ext in {".cfg", ".ini", ".log", ".txt", ".md"}:
        return ("Utility Tool", 0.45, "Utility/config file")

    return ("Unknown", 0.3, "Unrecognised extension")

    # Non-adult heuristics -------------------------------------------------
    if ext in SCRIPT_EXTS:
        return ("Script Mod", 0.85, "Script-like extension")

    if ext in ARCHIVE_EXTS:
        return ("Archive", 0.5, "Archive container")

    if ext == ".package":
        for kw, cat in KEYWORD_MAP:
            if kw in n:
                return (cat, 0.65, f"Keyword '{kw}'")
        return ("Other", 0.4, "Package (no keyword match)")

    # Utilities / configs / leftovers
    if ext in {".cfg", ".ini", ".log", ".txt", ".md"}:
        return ("Utility Tool", 0.45, "Utility/config file")

    return ("Unknown", 0.3, "Unrecognised extension")

def refine_with_metadata(path: str, current: Tuple[str, float, str]) -> Tuple[str, float, str, str]:
    name, conf, notes = current
    ext_raw = os.path.splitext(path)[1]
    ext, disabled_flag = normalize_extension(ext_raw)
    filename = os.path.basename(path)
    if disabled_flag and filename.lower().endswith("off"):
        filename = filename[:-3]
    adult_hint = any(k in filename.lower() for k in ADULT_TOKENS)

    # Script check for zip/ts4script
    if ext in SCRIPT_EXTS:
        if is_ts4script_or_zip_script(path):
            return (("Adult Script" if adult_hint else "Script Mod"), 1.0, "Zip/ts4script with .py/.pyc", "")
        return (("Adult Other" if adult_hint else "Archive"), 0.6, "Zip without Python", "")

    if ext != ".package":
        return (name, conf, notes, "")

    try:
        types = dbpf_scan_types(path)
        if types:
            cat, newc, newnotes, tags = classify_from_types(types, filename, adult_hint)
            if newc >= conf:
                return (cat, newc, newnotes, tags)
            else:
                return (name, conf, f"{notes}; {newnotes}", tags)
        return (name, conf, notes, "")
    except Exception as e:
        return (name, conf, f"{notes}; dbpf-scan err: {e}", "")

def map_type_to_folder(cat: str, folder_map: Dict[str, str]) -> str:
    return folder_map.get(cat, folder_map.get("Unknown", "Unsorted"))

# ---------------------------
# Scan, bundle, move, undo
# ---------------------------
_scan_folder_impl = scan_folder


def scan_folder(
    path: str,
    folder_map: Dict[str, str],
    recurse: bool = True,
    ignore_exts: Optional[Iterable[str]] = None,
    ignore_name_contains: Optional[Iterable[str]] = None,
    progress_cb=None,
    selected_folders: Optional[Sequence[str]] = None,
    include_adult: bool = True,
    allowed_exts: Optional[Iterable[str]] = None,
) -> ScanResult:
    return _scan_folder_impl(
        Path(path),
        folder_map=folder_map,
        recurse=recurse,
        ignore_exts=ignore_exts,
        ignore_names=ignore_name_contains,
        progress_cb=progress_cb,
        selected_folders=selected_folders,
        include_adult=include_adult,
        allowed_exts=allowed_exts,
    )

def ensure_folder(path: str):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def perform_moves(items: List[FileItem], mods_root: str):
    moved = 0; skipped = 0; collisions = []; moves_log = []
    for it in items:
        if not it.include:
            skipped += 1; continue
        dst_dir = os.path.join(mods_root, it.target_folder)
        ensure_folder(dst_dir)
        dst_path = os.path.join(dst_dir, it.name)
        if os.path.abspath(dst_path) == os.path.abspath(it.path):
            skipped += 1; continue
        if os.path.exists(dst_path):
            collisions.append((it.path, dst_path, "name collision")); skipped += 1; continue
        try:
            shutil.move(it.path, dst_path)
            moved += 1
            moves_log.append({"from": it.path, "to": dst_path})
        except Exception as e:
            collisions.append((it.path, dst_path, f"move error: {e}")); skipped += 1
    return moved, skipped, collisions, moves_log

def save_moves_log(mods_root: str, moves):
    if not moves: return
    log_path = os.path.join(mods_root, LOG_NAME)
    try:
        existing = json.load(open(log_path, "r", encoding="utf-8")) if os.path.exists(log_path) else []
    except Exception:
        existing = []
    existing.append({"ts": time.time(), "moves": moves})
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
    except Exception:
        pass

def undo_last_moves(mods_root: str):
    log_path = os.path.join(mods_root, LOG_NAME)
    if not os.path.exists(log_path): return 0, 0, ["No log found"]
    try:
        history = json.load(open(log_path, "r", encoding="utf-8"))
    except Exception:
        return 0, 0, ["Log unreadable"]
    if not history: return 0, 0, ["No moves recorded"]
    last = history.pop()
    moves = last.get("moves", [])
    undone = failed = 0; errs = []
    for m in reversed(moves):
        src = m.get("to"); dst = m.get("from")
        try:
            if os.path.exists(src):
                ensure_folder(os.path.dirname(dst))
                if os.path.exists(dst):
                    errs.append(f"Collision on undo for {os.path.basename(dst)}"); failed += 1
                else:
                    shutil.move(src, dst); undone += 1
            else:
                errs.append(f"Missing {os.path.basename(src)} to undo"); failed += 1
        except Exception as e:
            errs.append(f"Undo error for {os.path.basename(src)}: {e}"); failed += 1
    try:
        with open(log_path, "w", encoding="utf-8") as f: json.dump(history, f, indent=2)
    except Exception:
        pass
    return undone, failed, errs

def bundle_scripts_and_packages(items: List[FileItem], folder_map: Dict[str, str]):
    scripts: Dict[str, FileItem] = {}
    for it in items:
        if it.disabled:
            continue
        if it.ext in SCRIPT_EXTS and it.guess_type in {"Script Mod", "Adult Script"}:
            scripts[normalize_key(it.name)] = it
    linked = 0
    for it in items:
        if it.disabled:
            continue
        if it.ext == ".package":
            key = normalize_key(it.name)
            if key in scripts:
                it.bundle = key
                it.target_folder = scripts[key].target_folder
                it.notes = (it.notes + "; paired with script").strip("; ")
                linked += 1
    return {"linked": linked, "scripts": len(scripts)}

# ---------------------------
# UI
# ---------------------------
class Sims4ModSorterApp(tk.Tk):
    def __init__(self):
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
        self.disabled_items: List[FileItem] = []
        self._disabled_items_by_path: Dict[str, FileItem] = {}
        self._disabled_window: Optional[tk.Toplevel] = None
        self._disabled_tree: Optional[ttk.Treeview] = None
        self._disabled_status_var = tk.StringVar(value="No disabled mods")
        self.plugin_manager = load_user_plugins()
        if self.plugin_manager:
            self.plugin_manager.attach_app(self)

        self.status_var = tk.StringVar(value="Ready")
        self.summary_var = tk.StringVar(value="No plan yet")
        self._mod_status_window = None
        self._status_trees: Dict[str, ttk.Treeview] = {}
        self._status_summary_var = tk.StringVar(value="")
        self._update_check_in_progress = False
        self.check_updates_button = None
        self._log_entries: List[Tuple[str, str, str]] = []

        self.scan_folder_display = tk.StringVar(value="All folders")
        self.scan_folders: Optional[set[str]] = None
        self._available_folders: List[str] = []
        self._folder_menu_vars: Dict[str, tk.BooleanVar] = {}
        self._folder_menu_refresh_after: Optional[str] = None

        self._build_style()
        self._build_ui()
        self._build_settings_overlay()
        self.mods_root.trace_add("write", lambda *_: self._schedule_folder_menu_refresh())
        flush_plugin_messages(self, "boot")
        self._report_mod_boot_messages()
        self.after(1000, self._check_updates_on_launch)
        self.after(0, lambda: center_window(self))

    def _build_style(self):
        style = ttk.Style()
        try: style.theme_use("clam")
        except Exception: pass
        theme = THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        bg, fg, alt, accent, sel = theme["bg"], theme["fg"], theme["alt"], theme["accent"], theme["sel"]
        style.configure("TFrame", background=bg)
        style.configure("TLabel", background=bg, foreground=fg)
        style.configure("TCheckbutton", background=bg, foreground=fg)
        style.configure("TButton", background=alt, foreground=fg, padding=6)
        style.map("TButton", background=[("active", sel)])
        style.configure("Treeview", background=alt, fieldbackground=alt, foreground=fg, rowheight=28, bordercolor=bg, borderwidth=0)
        style.map("Treeview", background=[("selected", sel)])
        style.configure("Treeview.Heading", background=bg, foreground=fg)
        style.configure("Horizontal.TProgressbar", background=accent, troughcolor=alt)
        self.configure(bg=bg)
        self._theme_cache = dict(bg=bg, fg=fg, alt=alt, accent=accent, sel=sel)

    def _build_ui(self):
        self._plugin_columns = []
        try:
            self._tree_font = tkfont.nametofont("TkDefaultFont")
        except tk.TclError:
            self._tree_font = tkfont.Font()
        try:
            self._tree_heading_font = tkfont.nametofont("TkHeadingFont")
        except tk.TclError:
            self._tree_heading_font = self._tree_font
        root_container = ttk.Frame(self); root_container.pack(fill="both", expand=True)

        top = ttk.Frame(root_container); top.pack(fill="x", padx=12, pady=10)
        ttk.Label(top, text="Mods folder:").pack(side="left")
        self.entry_path = ttk.Entry(top, textvariable=self.mods_root, width=80)
        self.entry_path.pack(side="left", padx=8)
        folders_section = ttk.Frame(top)
        folders_section.pack(side="left", padx=(8, 0))
        ttk.Label(folders_section, text="Scan folders:").pack(side="left")
        self.folder_menu_button = ttk.Menubutton(folders_section, textvariable=self.scan_folder_display, width=18)
        self.folder_menu = tk.Menu(self.folder_menu_button, tearoff=False)
        self.folder_menu_button["menu"] = self.folder_menu
        self.folder_menu_button.pack(side="left", padx=(4, 0))
        ttk.Button(folders_section, text="Refresh", command=self._refresh_folder_menu).pack(side="left", padx=(4, 0))
        base_buttons = [
            ToolbarButtonSpec("browse", "Browse", self.on_browse, side="left", padx=4),
            ToolbarButtonSpec("scan", "Scan", self.on_scan, side="left", padx=4),
            ToolbarButtonSpec("undo", "Undo Last", self.on_undo, side="right", padx=6),
            ToolbarButtonSpec("plugin_status", "Plugin Status", self.show_mod_status_popup, side="right", padx=6),
            ToolbarButtonSpec("settings", "⚙", self.show_settings, side="right", padx=0, width=3),
        ]
        if self.plugin_manager:
            toolbar_specs = self.plugin_manager.resolve_toolbar_buttons(self, base_buttons)
        else:
            toolbar_specs = list(base_buttons)
        self.toolbar_buttons: Dict[str, ttk.Button] = {}
        left_specs = [spec for spec in toolbar_specs if spec.side == "left"]
        right_specs = [spec for spec in toolbar_specs if spec.side == "right"]
        for spec in left_specs:
            button = ttk.Button(top, text=spec.text, command=spec.command)
            if spec.width is not None:
                button.configure(width=spec.width)
            button.pack(side="left", padx=spec.padx)
            self.toolbar_buttons[spec.button_id] = button
            if spec.button_id == "scan":
                self.btn_scan = button
        ttk.Label(top, textvariable=self.status_var).pack(side="left", padx=12)
        right_frame = ttk.Frame(top)
        right_frame.pack(side="right")
        for spec in right_specs:
            button = ttk.Button(right_frame, text=spec.text, command=spec.command)
            if spec.width is not None:
                button.configure(width=spec.width)
            button.pack(side="left", padx=spec.padx)
            self.toolbar_buttons[spec.button_id] = button

        mid = ttk.Frame(root_container); mid.pack(fill="both", expand=True, padx=12, pady=(6, 8))
        header = ttk.Frame(mid); header.pack(fill="x", pady=(0,6))
        ttk.Label(header, textvariable=self.summary_var).pack(side="left")

        left = ttk.Frame(mid); left.pack(side="left", fill="both", expand=True)
        tree_frame = ttk.Frame(left); tree_frame.pack(fill="both", expand=True)
        columns = ("inc", "rel", "name", "size", "type", "target", "conf", "linked", "meta", "notes")
        headings = {
            "inc": "✔",
            "rel": "Folder",
            "name": "File",
            "size": "MB",
            "type": "Type",
            "target": "Target Folder",
            "conf": "Conf",
            "linked": "Linked",
            "meta": "Tags",
            "notes": "Notes",
        }
        anchors = {
            "inc": "center",
            "rel": "w",
            "name": "w",
            "size": "e",
            "type": "w",
            "target": "w",
            "conf": "e",
            "linked": "center",
            "meta": "w",
            "notes": "w",
        }
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="extended")
        self._column_order = list(columns)
        self._column_anchors = anchors
        for column in self._column_order:
            self.tree.heading(column, text=headings.get(column, column))
            self.tree.column(column, anchor=self._column_anchors.get(column, "w"), stretch=False, width=80)
        ysb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscroll=ysb.set, xscroll=xsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        ysb.grid(row=0, column=1, sticky="ns")
        xsb.grid(row=1, column=0, sticky="ew")
        tree_frame.grid_columnconfigure(0, weight=1)
        tree_frame.grid_rowconfigure(0, weight=1)
        self.tree.tag_configure("adult", foreground="#d12a2a")

        right = ttk.Frame(mid); right.pack(side="left", fill="y", padx=(10, 0))
        ttk.Label(right, text="Selection").pack(anchor="w")
        self.sel_label = ttk.Label(right, text="None selected"); self.sel_label.pack(anchor="w", pady=(0, 10))
        ttk.Label(right, text="Type").pack(anchor="w")
        self.type_cb = ttk.Combobox(right, values=CATEGORY_ORDER, state="readonly"); self.type_cb.pack(fill="x", pady=(0, 8))
        ttk.Label(right, text="Target Folder").pack(anchor="w")
        self.target_entry = ttk.Entry(right); self.target_entry.pack(fill="x", pady=(0, 8))
        ttk.Button(right, text="Apply to Selected", command=self.on_apply_selected).pack(fill="x", pady=4)
        ttk.Button(right, text="Toggle Include", command=self.on_toggle_include).pack(fill="x", pady=4)
        ttk.Button(right, text="Disable Selected", command=self.on_disable_selected).pack(fill="x", pady=4)
        ttk.Button(right, text="Show Disabled Mods", command=self.show_disabled_mods).pack(fill="x", pady=4)
        ttk.Label(right, textvariable=self._disabled_status_var).pack(anchor="w", pady=(0, 8))
        ttk.Separator(right).pack(fill="x", pady=10)
        ttk.Label(right, text="Batch assign by keyword").pack(anchor="w")
        self.batch_keyword = ttk.Entry(right); self.batch_keyword.pack(fill="x", pady=(0, 6))
        ttk.Button(right, text="Assign Type to Matches", command=self.on_batch_assign).pack(fill="x")
        ttk.Separator(right).pack(fill="x", pady=10)
        ttk.Button(right, text="Recalculate Targets", command=self.on_recalc_targets).pack(fill="x", pady=4)
        ttk.Button(right, text="Select All", command=lambda: self.tree.selection_set(self.tree.get_children())).pack(fill="x", pady=2)
        ttk.Button(right, text="Select None", command=lambda: self.tree.selection_remove(self.tree.get_children())).pack(fill="x", pady=2)

        bottom = ttk.Frame(root_container); bottom.pack(fill="x", padx=12, pady=8)
        self.progress = ttk.Progressbar(bottom, orient="horizontal", mode="determinate")
        self.progress.pack(fill="x", side="left", expand=True)
        ttk.Button(bottom, text="Complete Sorting", command=self.on_complete).pack(side="right", padx=6)

        logf = ttk.Frame(root_container); logf.pack(fill="both", padx=12, pady=(0,10))
        log_header = ttk.Frame(logf)
        log_header.pack(fill="x", pady=(0, 6))
        ttk.Label(log_header, text="Log Output").pack(side="left")
        ttk.Button(
            log_header,
            text="Export All Logs",
            command=lambda: self.on_export_logs(errors_only=False),
        ).pack(side="right")
        ttk.Button(
            log_header,
            text="Export Errors",
            command=lambda: self.on_export_logs(errors_only=True),
        ).pack(side="right", padx=(0, 8))
        self.log_text = tk.Text(
            logf,
            height=6,
            wrap="word",
            state="disabled",
            relief="flat",
            bg=self._theme_cache["alt"],
            fg=self._theme_cache["fg"],
        )
        self.log_text.pack(fill="both", expand=False)

        self._refresh_folder_menu()

        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self.tree.bind("<Double-1>", self.on_double_click)

    def _schedule_folder_menu_refresh(self) -> None:
        job = getattr(self, "_folder_menu_refresh_after", None)
        if job:
            try:
                self.after_cancel(job)
            except Exception:
                pass
        self._folder_menu_refresh_after = self.after(250, self._refresh_folder_menu)

    def _refresh_folder_menu(self) -> None:
        menu = getattr(self, "folder_menu", None)
        button = getattr(self, "folder_menu_button", None)
        if not menu or not button:
            return
        root = Path(self.mods_root.get()).expanduser()
        available: List[str] = ["."]
        if root.is_dir():
            try:
                children = sorted(child for child in root.iterdir() if child.is_dir())
            except PermissionError:
                children = []
            for child in children:
                available.append(child.name)
        self._available_folders = available
        menu.delete(0, "end")
        menu.add_command(label="All folders", command=self._select_all_folders)
        menu.add_separator()
        self._folder_menu_vars = {}
        for rel in self._available_folders:
            label = "Root files" if rel in (".", "") else rel
            var = tk.BooleanVar(value=True)
            self._folder_menu_vars[rel] = var
            menu.add_checkbutton(
                label=label,
                variable=var,
                command=lambda folder=rel: self._on_folder_toggle(folder),
            )
        self._prune_scan_folders()
        self._sync_folder_menu_vars()
        self._update_folder_display()
        self._folder_menu_refresh_after = None

    def _select_all_folders(self) -> None:
        self.scan_folders = None
        self._sync_folder_menu_vars()
        self._update_folder_display()

    def _sync_folder_menu_vars(self) -> None:
        selected = set(self._available_folders if self.scan_folders is None else self.scan_folders)
        for folder, var in self._folder_menu_vars.items():
            var.set(folder in selected)

    def _update_folder_display(self) -> None:
        if self.scan_folders is None or not self._available_folders:
            self.scan_folder_display.set("All folders")
            return
        if not self.scan_folders:
            self.scan_folder_display.set("No folders")
            return
        labels = ["Root files" if folder in (".", "") else folder for folder in sorted(self.scan_folders)]
        self.scan_folder_display.set(", ".join(labels[:4]) + ("…" if len(labels) > 4 else ""))

    def _prune_scan_folders(self) -> None:
        if self.scan_folders is None:
            return
        available_set = set(self._available_folders)
        self.scan_folders = {folder for folder in self.scan_folders if folder in available_set}
        if self.scan_folders and self.scan_folders >= available_set:
            self.scan_folders = None

    def _on_folder_toggle(self, folder: str) -> None:
        var = self._folder_menu_vars.get(folder)
        if not var:
            return
        available_set = set(self._available_folders)
        is_selected = bool(var.get())
        if is_selected:
            if self.scan_folders is not None:
                self.scan_folders.add(folder)
        else:
            if self.scan_folders is None:
                self.scan_folders = set(available_set)
            if self.scan_folders is not None and folder in self.scan_folders:
                self.scan_folders.remove(folder)
        if self.scan_folders is not None and self.scan_folders >= available_set:
            self.scan_folders = None
        self._sync_folder_menu_vars()
        self._update_folder_display()

    def _resolve_selected_folders(self) -> Optional[List[str]]:
        if self.scan_folders is None:
            return None
        return sorted(self.scan_folders)

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

    # In-window settings overlay (no OS pop-up)
    def _build_settings_overlay(self):
        theme = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        self._settings_sidebar_width = 360
        self.settings_scrim = None
        self.settings_sidebar = tk.Frame(
            self,
            bg=theme.get("sel", "#2A2F3A"),
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
        updates = ttk.Frame(container)
        updates.grid(row=row, column=0, sticky="ew")
        updates.columnconfigure(0, weight=1)
        ttk.Label(
            updates,
            textvariable=self._version_display_var,
            font=("TkDefaultFont", 10, "bold"),
        ).grid(row=0, column=0, sticky="w")
        self.check_updates_button = ttk.Button(
            updates,
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
        ctrl = ttk.Frame(theme_section)
        ctrl.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        ctrl.columnconfigure(1, weight=1)
        ttk.Label(ctrl, text="Theme").grid(row=0, column=0, sticky="w")
        self.theme_cb = ttk.Combobox(ctrl, values=list(THEMES.keys()), textvariable=self.theme_name, state="readonly")
        self.theme_cb.grid(row=0, column=1, sticky="ew", padx=(6, 0))
        ttk.Button(ctrl, text="Apply", command=self.on_apply_theme).grid(row=0, column=2, padx=(6, 0))
        self.theme_preview_container = ttk.Frame(theme_section)
        self.theme_preview_container.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        for column in range(max(1, min(2, len(THEMES)))):
            self.theme_preview_container.columnconfigure(column, weight=1)
        self._build_theme_preview_widgets()
        row += 1
        scan = ttk.Frame(container)
        scan.grid(row=row, column=0, sticky="ew", pady=(18, 0))
        scan.columnconfigure(0, weight=1)
        ttk.Label(scan, text="Scanning", font=("TkDefaultFont", 10, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(scan, text="Scan subfolders", variable=self.recurse_var).grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Label(scan, text="Ignore extensions (comma separated)").grid(row=2, column=0, sticky="w", pady=(10, 2))
        ttk.Entry(scan, textvariable=self.ignore_exts_var).grid(row=3, column=0, sticky="ew")
        ttk.Label(scan, text="Ignore names containing (comma separated)").grid(row=4, column=0, sticky="w", pady=(10, 2))
        ttk.Entry(scan, textvariable=self.ignore_names_var).grid(row=5, column=0, sticky="ew")
        ttk.Label(scan, text="File types to include").grid(row=6, column=0, sticky="w", pady=(12, 2))
        type_frame = ttk.Frame(scan)
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
            scan,
            text="Include adult content",
            variable=self.include_adult_var,
        ).grid(row=8, column=0, sticky="w", pady=(12, 0))
        row += 1
        actions = ttk.Frame(container)
        actions.grid(row=row + 1, column=0, sticky="e", pady=(20, 0))
        ttk.Button(actions, text="Done", command=self.hide_settings).grid(row=0, column=0)
        self.settings_sidebar.bind("<Escape>", lambda _e: self.hide_settings())
        self._update_theme_preview_highlight()

    def show_settings(self):
        if not hasattr(self, "settings_sidebar"):
            return
        theme = self._theme_cache or THEMES.get(self.theme_name.get(), THEMES["Dark Mode"])
        scrim = getattr(self, "settings_scrim", None)
        if scrim:
            scrim.configure(bg=_scrim_color(theme.get("bg", "#111316")))
            scrim.place(relx=0, rely=0, relwidth=1, relheight=1)
            scrim.tkraise()
        self.settings_sidebar.configure(bg=theme.get("sel", "#2A2F3A"))
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

    def hide_settings(self):
        if hasattr(self, "settings_sidebar"):
            self.settings_sidebar.place_forget()
        scrim = getattr(self, "settings_scrim", None)
        if scrim:
            scrim.place_forget()

    def show_mod_status_popup(self) -> None:
        if not self.plugin_manager:
            messagebox.showinfo("Plugin Status", "No plugins loaded.", parent=self)
            return
        if self._mod_status_window and self._mod_status_window.winfo_exists():
            self._populate_mod_status_popup()
            self._mod_status_window.deiconify()
            self._mod_status_window.lift()
            return
        palette = getattr(self, "_theme_cache", THEMES["Dark Mode"])
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

    def _build_theme_preview_widgets(self):
        if not hasattr(self, "theme_preview_container"):
            return
        for child in self.theme_preview_container.winfo_children():
            child.destroy()
        self.theme_preview_canvases: Dict[str, tk.Canvas] = {}

        columns = max(1, min(2, len(THEMES)))
        for idx, (name, palette) in enumerate(THEMES.items()):
            row = idx // columns
            col = idx % columns
            cell = ttk.Frame(self.theme_preview_container)
            cell.grid(row=row, column=col, padx=4, pady=4, sticky="nsew")
            cell.columnconfigure(0, weight=1)

            canvas = tk.Canvas(cell, width=120, height=60, highlightthickness=2)
            canvas.grid(row=0, column=0, sticky="ew")
            canvas.create_rectangle(0, 0, 120, 60, fill=palette["bg"], outline="")
            canvas.create_rectangle(0, 0, 120, 24, fill=palette["alt"], outline="")
            canvas.create_rectangle(0, 24, 120, 40, fill=palette["accent"], outline="")
            canvas.create_rectangle(0, 40, 120, 60, fill=palette["sel"], outline="")

            label = ttk.Label(cell, text=name, anchor="center")
            label.grid(row=1, column=0, pady=(4, 0))

            bind_target = lambda widget, theme=name: widget.bind(
                "<Button-1>", lambda _e, theme_name=theme: self._on_theme_preview_click(theme_name)
            )
            bind_target(canvas)
            bind_target(label)

            self.theme_preview_canvases[name] = canvas

        self._update_theme_preview_highlight()

    def _on_theme_preview_click(self, theme_name: str):
        self.theme_name.set(theme_name)
        if hasattr(self, "theme_cb"):
            self.theme_cb.set(theme_name)
        self._update_theme_preview_highlight()

    def _update_theme_preview_highlight(self):
        canvases = getattr(self, "theme_preview_canvases", {})
        if not canvases:
            return
        selected = self.theme_name.get()
        accent = THEMES.get(selected, THEMES["Dark Mode"])["accent"]
        neutral = self._theme_cache.get("alt", "#444444")
        for name, canvas in canvases.items():
            border = accent if name == selected else neutral
            canvas.configure(highlightbackground=border, highlightcolor=border)

    # ---- helpers
    def log(self, msg: str, level: str = "info"):
        normalized = (level or "info").strip().lower()
        if normalized not in {"info", "warn", "warning", "error"}:
            normalized = "info"
        if normalized == "warning":
            normalized = "warn"
        if normalized == "info":
            lowered = msg.lower()
            if "error" in lowered:
                normalized = "error"
            elif lowered.startswith("warn") or "warning" in lowered:
                normalized = "warn"
        timestamp = time.strftime("%H:%M:%S")
        self._log_entries.append((timestamp, normalized, msg))
        tag = normalized.upper()
        if normalized == "info":
            rendered = f"[{timestamp}] {msg}\n"
        else:
            rendered = f"[{timestamp}] [{tag}] {msg}\n"
        self.log_text.configure(state="normal")
        self.log_text.insert("end", rendered)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def on_export_logs(self, *, errors_only: bool) -> None:
        if errors_only:
            entries = [entry for entry in self._log_entries if entry[1] == "error"]
        else:
            entries = list(self._log_entries)
        if not entries:
            message = "No error logs to export." if errors_only else "No logs to export yet."
            messagebox.showinfo("Export Logs", message, parent=self)
            return
        default_name = "modsorter-errors.log" if errors_only else "modsorter.log"
        path = filedialog.asksaveasfilename(
            parent=self,
            title="Export Logs",
            defaultextension=".log",
            filetypes=[("Log files", "*.log"), ("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=default_name,
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as handle:
                for timestamp, level_name, message in entries:
                    tag = level_name.upper()
                    if level_name == "info":
                        handle.write(f"[{timestamp}] {message}\n")
                    else:
                        handle.write(f"[{timestamp}] [{tag}] {message}\n")
        except Exception as exc:
            messagebox.showerror("Export Logs", f"Failed to export logs: {exc}", parent=self)
            self.log(f"Export failed: {exc}", level="error")
            return
        summary = "error" if errors_only else "all"
        self.log(f"Exported {summary} logs to {path}")

    def _refresh_version_display(self):
        if hasattr(self, "_version_display_var"):
            self._version_display_var.set(f"App Version: {APP_VERSION}")

    def _check_updates_on_launch(self):
        self._start_update_check(manual=False)

    def _on_manual_update_check(self):
        self._start_update_check(manual=True)

    def _start_update_check(self, *, manual: bool):
        if self._update_check_in_progress:
            if manual:
                messagebox.showinfo("Update Check", "An update check is already running.", parent=self)
            return
        self._update_check_in_progress = True
        if manual and self.check_updates_button and self.check_updates_button.winfo_exists():
            self.check_updates_button.configure(state="disabled")

        def worker():
            error_message: Optional[str] = None
            result: Optional[UpdateResult]
            try:
                result = check_for_update("app", APP_VERSION)
            except Exception as exc:  # pragma: no cover - defensive
                error_message = f"Update check failed: {exc}"
                result = None
            self.after(0, lambda: self._complete_update_check(result, manual, error_message))

        threading.Thread(target=worker, daemon=True).start()

    def _complete_update_check(self, result: Optional[UpdateResult], manual: bool, error_message: Optional[str]):
        self._update_check_in_progress = False
        if self.check_updates_button and self.check_updates_button.winfo_exists():
            self.check_updates_button.configure(state="normal")
        self._refresh_version_display()

        if error_message:
            if manual:
                messagebox.showerror("Update Check", error_message, parent=self)
            else:
                self.log(error_message)
            return

        if not result:
            return

        if result.message:
            if manual:
                messagebox.showerror("Update Check", result.message, parent=self)
            else:
                self.log(result.message)
            return

        if result.is_newer and result.latest_version:
            self.log(f"Update available: {result.latest_version}")
            prompt = (
                f"Version {result.latest_version} is available (current version is {APP_VERSION}).\n"
                "Would you like to open the download page?"
            )
            if messagebox.askyesno("Update Available", prompt, parent=self):
                if result.download_url:
                    webbrowser.open(result.download_url)
                else:
                    messagebox.showinfo("Update Available", "Download URL is not configured.", parent=self)
        else:
            if manual:
                messagebox.showinfo(
                    "Update Check",
                    f"You are using the latest version ({APP_VERSION}).",
                    parent=self,
                )

    def on_apply_theme(self):
        self._build_style()
        palette = self._theme_cache
        self.log_text.configure(bg=palette["alt"], fg=palette["fg"])
        if hasattr(self, "settings_sidebar"):
            self.settings_sidebar.configure(bg=palette["sel"])
        scrim = getattr(self, "settings_scrim", None)
        if scrim:
            scrim.configure(bg=_scrim_color(palette["bg"]))
        self._update_theme_preview_highlight()
        self.log("Theme applied: " + self.theme_name.get())

    # ---- actions
    def on_browse(self):
        p = filedialog.askdirectory(initialdir=self.mods_root.get(), title="Select Mods folder")
        if p: self.mods_root.set(p)

    def on_scan(self):
        _load_relatedwords_condom()
        mods = self.mods_root.get()
        if not os.path.isdir(mods):
            self.log("Error: folder not found"); self.status_var.set("Folder not found"); return
        self._refresh_folder_menu()
        allowed_exts_preview = self._resolve_allowed_extensions()
        if allowed_exts_preview is not None and not allowed_exts_preview:
            messagebox.showwarning("Scan", "Enable at least one file type before scanning.", parent=self)
            return
        selected_folders_preview = self._resolve_selected_folders()
        if selected_folders_preview is not None and not selected_folders_preview:
            messagebox.showwarning("Scan", "No folders are selected to scan.", parent=self)
            return
        include_adult = self.include_adult_var.get()
        self.status_var.set("Scanning…")
        self.progress.configure(maximum=100, value=0)
        self.items = []
        if hasattr(self, "btn_scan"):
            try:
                self.btn_scan.configure(state="disabled")
            except Exception:
                pass
        folder_desc = "All folders" if not selected_folders_preview else ", ".join(
            "Root files" if f in (".", "") else f for f in selected_folders_preview
        )
        type_desc = "all file types" if allowed_exts_preview is None else ", ".join(sorted(allowed_exts_preview)) or "(none)"
        self.log(
            f"Starting scan in {folder_desc} ({'including' if include_adult else 'excluding'} adult content, {type_desc})."
        )

        def progress_cb(done, total, path, state):
            pct = int((done/total)*100) if total else 0
            self.progress.configure(value=pct, maximum=100)
            if done % 25 == 0 or state == "error":
                self.status_var.set(f"Scanning {done}/{total}: {os.path.basename(path)}")
            if state == "error":
                self.log(f"Scan error: {os.path.basename(path)}", level="error")

        def worker():
            ignore_exts = {e.strip() for e in self.ignore_exts_var.get().split(',')}
            ignore_names = [t.strip() for t in self.ignore_names_var.get().split(',')]
            allowed_exts = None if allowed_exts_preview is None else set(allowed_exts_preview)
            selected_folders = None if selected_folders_preview is None else list(selected_folders_preview)
            include_adult_scan = include_adult
            context = {
                "mods_root": mods,
                "recurse": self.recurse_var.get(),
                "ignore_exts": ignore_exts,
                "ignore_names": ignore_names,
                "folder_map": self.folder_map,
                "allowed_exts": allowed_exts,
                "selected_folders": selected_folders,
                "include_adult": include_adult_scan,
            }
            if self.plugin_manager:
                self.plugin_manager.run_pre_scan(context)
                if isinstance(context.get("ignore_exts"), (list, set, tuple)):
                    ignore_exts = {str(ext).strip() for ext in context["ignore_exts"] if str(ext).strip()}
                if isinstance(context.get("ignore_names"), (list, set, tuple)):
                    ignore_names = [str(name).strip() for name in context["ignore_names"] if str(name).strip()]
                if context.get("mods_root"):
                    context["mods_root"] = str(context["mods_root"])
                if "allowed_exts" in context:
                    ext_value = context["allowed_exts"]
                    if ext_value is None:
                        allowed_exts = None
                    elif isinstance(ext_value, (list, set, tuple)):
                        allowed_exts = {
                            (str(ext).lower() if str(ext).startswith('.') else f".{str(ext).lower()}")
                            for ext in ext_value
                            if str(ext).strip()
                        }
                    else:
                        allowed_exts = None if not ext_value else {
                            (str(ext_value).lower() if str(ext_value).startswith('.') else f".{str(ext_value).lower()}")
                        }
                if "selected_folders" in context:
                    folders_val = context["selected_folders"]
                    if folders_val is None:
                        selected_folders = None
                    elif isinstance(folders_val, (list, tuple, set)):
                        selected_folders = [str(folder) for folder in folders_val]
                    else:
                        selected_folders = [str(folders_val)]
                include_adult_scan = bool(context.get("include_adult", include_adult_scan))
            scan_root = str(context.get("mods_root", mods) or mods)
            if scan_root != mods:
                try:
                    self.mods_root.set(scan_root)
                except Exception:
                    pass
            if allowed_exts is not None and not allowed_exts:
                self.log("Scan cancelled: no file types allowed after plugin adjustments.", level="warning")
                self.status_var.set("Scan cancelled")
                return
            if selected_folders is not None and not selected_folders:
                self.log("Scan cancelled: no folders selected after plugin adjustments.", level="warning")
                self.status_var.set("Scan cancelled")
                return
            try:
                result = scan_folder(
                    scan_root,
                    self.folder_map,
                    recurse=self.recurse_var.get(),
                    ignore_exts=ignore_exts,
                    ignore_name_contains=ignore_names,
                    progress_cb=progress_cb,
                    selected_folders=selected_folders,
                    include_adult=include_adult_scan,
                    allowed_exts=allowed_exts,
                )
                items = list(result.items)
                stats = bundle_scripts_and_packages(items, self.folder_map)
                if self.plugin_manager:
                    context.update({
                        "bundle_stats": stats,
                        "items": items,
                        "scan_root": scan_root,
                    })
                    self.plugin_manager.run_post_scan(items, context)
                    items.sort(
                        key=lambda fi: (
                            CATEGORY_INDEX.get(fi.guess_type, len(CATEGORY_ORDER)),
                            _natural_key(os.path.dirname(fi.relpath) or '.'),
                            _natural_key(fi.name),
                        )
                    )
                self.items = items
                self.disabled_items = list(result.disabled_items)
                self._refresh_tree()
                self._refresh_disabled_tree()
                disabled_count = len(self.disabled_items)
                plan_summary = f"Plan: {len(self.items)} files"
                if disabled_count:
                    plan_summary += f" ({disabled_count} disabled)"
                self.status_var.set(plan_summary)
                self.log(
                    f"Scan complete. Planned {len(self.items)} files (disabled: {disabled_count}). Linked packages: {stats['linked']} across {stats['scripts']} script(s)."
                )
                for warning in result.errors:
                    self.log(f"Scan warning: {warning}", level="warning")
                self._report_mod_runtime_messages()
            finally:
                if hasattr(self, "btn_scan"):
                    self.after(0, lambda: self.btn_scan.configure(state="normal"))

        threading.Thread(target=worker, daemon=True).start()

    def on_select(self, event=None):
        sel = self.tree.selection()
        if not sel: self.sel_label.config(text="None selected"); return
        idx = int(sel[0]); it = self.items[idx]
        self.sel_label.config(text=pretty_display_name(it.name))
        self.type_cb.set(it.guess_type if it.guess_type in CATEGORY_ORDER else "Unknown")
        self.target_entry.delete(0, tk.END); self.target_entry.insert(0, it.target_folder)

    def on_double_click(self, event=None):
        region = self.tree.identify_region(event.x, event.y)
        if region != "cell": self.on_toggle_include(); return
        col = self.tree.identify_column(event.x)
        sel = self.tree.selection()
        if not sel: return
        idx = int(sel[0]); it = self.items[idx]
        if col == "#5":   # Type
            self.type_cb.set(it.guess_type if it.guess_type in CATEGORY_ORDER else "Unknown"); self.type_cb.focus_set()
        elif col == "#6": # Target
            self.target_entry.delete(0, tk.END); self.target_entry.insert(0, it.target_folder); self.target_entry.focus_set()
        else:
            self.on_toggle_include()

    def on_apply_selected(self):
        sel = self.tree.selection()
        if not sel: return
        new_type = self.type_cb.get() or "Unknown"
        new_target = self.target_entry.get().strip() or DEFAULT_FOLDER_MAP.get(new_type, "Unsorted")
        for iid in sel:
            it = self.items[int(iid)]
            it.guess_type = new_type
            it.target_folder = new_target
        self._refresh_tree(preserve_selection=True)

    def on_toggle_include(self):
        sel = self.tree.selection()
        if not sel: return
        for iid in sel:
            idx = int(iid); self.items[idx].include = not self.items[idx].include
        self._refresh_tree(preserve_selection=True)

    def on_batch_assign(self):
        kw = self.batch_keyword.get().strip().lower()
        if not kw: return
        new_type = self.type_cb.get() or "Unknown"; count = 0
        for it in self.items:
            if kw in it.name.lower():
                it.guess_type = new_type
                it.target_folder = DEFAULT_FOLDER_MAP.get(new_type, "Unsorted")
                count += 1
        self._refresh_tree(); self.log(f"Assigned {new_type} to {count} file(s) containing '{kw}'.")

    def on_recalc_targets(self):
        for it in self.items:
            it.target_folder = DEFAULT_FOLDER_MAP.get(it.guess_type, "Unsorted")
        bundle_scripts_and_packages(self.items, self.folder_map)
        self._refresh_tree(preserve_selection=True)

    def on_complete(self):
        if not self.items: return
        mods = self.mods_root.get()
        plan = [it for it in self.items if it.include]
        if not plan: self.log("No files selected to move."); return
        self.log(f"Starting move of {len(plan)} file(s)…")

        def worker():
            self.progress.configure(maximum=len(plan), value=0)
            moved_total = skipped_total = 0; collisions_total = []; moves_log_all = []
            for i, it in enumerate(plan, start=1):
                moved, skipped, collisions, moves_log = perform_moves([it], mods)
                moved_total += moved; skipped_total += skipped
                collisions_total.extend(collisions); moves_log_all.extend(moves_log)
                self.progress.configure(value=i); self.progress.update_idletasks()
            save_moves_log(mods, moves_log_all)
            self.status_var.set("Move complete")
            self.log(f"Move complete. Moved {moved_total}, Skipped {skipped_total}, Issues {len(collisions_total)}")
            for s, d, r in collisions_total[:50]:
                self.log(f"Collision: {os.path.basename(s)} -> {os.path.dirname(d)} ({r})")
            self.on_scan()

        threading.Thread(target=worker, daemon=True).start()

    def on_undo(self):
        mods = self.mods_root.get()
        undone, failed, errs = undo_last_moves(mods)
        self.log(f"Undo: {undone} restored, {failed} failed")
        for e in errs[:50]: self.log(e)
        self.on_scan()

    # Table refresh and responsive widths
    def _refresh_tree(self, preserve_selection: bool = False):
        selected_iids = set(self.tree.selection()) if preserve_selection else set()
        self.tree.delete(*self.tree.get_children())
        total = len(self.items); by_cat: Dict[str, int] = {}
        disabled_count = len(self.disabled_items)
        for idx, it in enumerate(self.items):
            by_cat[it.guess_type] = by_cat.get(it.guess_type, 0) + 1
            inc = "✓" if it.include else ""
            folder_rel = os.path.dirname(it.relpath) if it.relpath else "."
            linked = "🔗" if it.bundle else ""
            vals = (
                inc, folder_rel or ".", pretty_display_name(it.name), f"{it.size_mb:.2f}",
                it.guess_type, it.target_folder, f"{it.confidence:.2f}", linked, it.meta_tags, it.notes,
            )
            iid = str(idx)
            extras = it.extras if isinstance(it.extras, dict) else {}
            adult_tag = (
                "adult"
                if it.guess_type.startswith("Adult")
                or extras.get("adult_keywords")
                or extras.get("adult_note")
                else None
            )
            tags = (adult_tag,) if adult_tag else ()
            self.tree.insert("", "end", iid=iid, values=vals, tags=tags)
            if preserve_selection and iid in selected_iids:
                self.tree.selection_add(iid)
        if total:
            topcats = sorted(by_cat.items(), key=lambda kv: -kv[1])[:4]
            frag = ", ".join(f"{k}: {v}" for k, v in topcats)
            summary = f"Planned {total} files"
            if disabled_count:
                summary += f" (+{disabled_count} disabled)"
            summary += f" | {frag}"
            self.summary_var.set(summary)
        else:
            self.summary_var.set("No plan yet")
        if disabled_count:
            self._disabled_status_var.set(f"Disabled mods: {disabled_count}")
        else:
            self._disabled_status_var.set("No disabled mods")
        self._auto_size_columns()

    def on_disable_selected(self):
        sel = list(self.tree.selection())
        if not sel:
            messagebox.showinfo("Disable Mods", "Select one or more mods to disable.", parent=self)
            return
        disabled = 0
        errors: List[str] = []
        for iid in sel:
            try:
                idx = int(iid)
            except ValueError:
                continue
            if idx < 0 or idx >= len(self.items):
                continue
            item = self.items[idx]
            if item.disabled:
                continue
            path = Path(item.path)
            target = path.with_name(path.name + "OFF")
            if target.exists():
                errors.append(f"Disable failed: {target.name} already exists")
                continue
            try:
                path.rename(target)
                disabled += 1
            except Exception as exc:
                errors.append(f"Disable failed for {path.name}: {exc}")
        if disabled:
            self.log(f"Disabled {disabled} mod(s).")
        for message in errors[:50]:
            self.log(message, level="error")
        if disabled or errors:
            self.on_scan()

    def show_disabled_mods(self):
        window = self._disabled_window
        if window is not None and window.winfo_exists():
            window.lift()
            self._refresh_disabled_tree()
            return
        win = tk.Toplevel(self)
        win.title("Disabled Mods")
        win.geometry("720x360")
        win.transient(self)
        try:
            win.grab_set()
        except tk.TclError:
            pass
        columns = ("name", "folder", "type", "notes")
        tree = ttk.Treeview(win, columns=columns, show="headings", selectmode="extended")
        headings = {
            "name": "File",
            "folder": "Folder",
            "type": "Type",
            "notes": "Notes",
        }
        for column in columns:
            tree.heading(column, text=headings[column])
            tree.column(column, width=160 if column != "notes" else 280, anchor="w")
        tree.tag_configure("adult", foreground="#d12a2a")
        tree.pack(fill="both", expand=True, padx=8, pady=(8, 4))
        button_row = ttk.Frame(win)
        button_row.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(button_row, text="Enable Selected", command=self._enable_disabled_selection).pack(side="left")
        ttk.Button(button_row, text="Close", command=self._close_disabled_window).pack(side="right")
        win.protocol("WM_DELETE_WINDOW", self._close_disabled_window)
        self._disabled_window = win
        self._disabled_tree = tree
        self._refresh_disabled_tree()

    def _close_disabled_window(self):
        window = self._disabled_window
        if window is not None and window.winfo_exists():
            window.destroy()
        self._disabled_window = None
        self._disabled_tree = None

    def _refresh_disabled_tree(self):
        tree = self._disabled_tree
        if tree is None or not tree.winfo_exists():
            return
        tree.delete(*tree.get_children())
        self._disabled_items_by_path = {}
        for item in self.disabled_items:
            path_str = str(item.path)
            self._disabled_items_by_path[path_str] = item
            folder_rel = os.path.dirname(item.relpath) if item.relpath else "."
            extras = item.extras if isinstance(item.extras, dict) else {}
            adult_tag = (
                "adult"
                if item.guess_type.startswith("Adult")
                or extras.get("adult_keywords")
                or extras.get("adult_note")
                else None
            )
            tags = (adult_tag,) if adult_tag else ()
            tree.insert(
                "",
                "end",
                iid=path_str,
                values=(
                    pretty_display_name(item.name),
                    folder_rel or ".",
                    item.guess_type,
                    item.notes,
                ),
                tags=tags,
            )

    def _enable_disabled_selection(self):
        tree = self._disabled_tree
        if tree is None or not tree.winfo_exists():
            return
        selection = tree.selection()
        if not selection:
            messagebox.showinfo("Enable Mods", "Select one or more disabled mods to enable.", parent=self._disabled_window)
            return
        enabled = 0
        errors: List[str] = []
        for iid in selection:
            item = self._disabled_items_by_path.get(iid)
            if not item:
                continue
            path = Path(item.path)
            ext_raw = path.suffix
            normalized_ext, disabled_flag = normalize_extension(ext_raw)
            if not disabled_flag:
                continue
            target = path.with_suffix(normalized_ext)
            if target.exists():
                errors.append(f"Enable failed: {target.name} already exists")
                continue
            try:
                path.rename(target)
                enabled += 1
            except Exception as exc:
                errors.append(f"Enable failed for {path.name}: {exc}")
        if enabled:
            self.log(f"Enabled {enabled} mod(s).")
        for message in errors[:50]:
            self.log(message, level="error")
        if enabled or errors:
            self.on_scan()

# ---------------------------
# Entry
# ---------------------------
def main():
    if "--selftest" in sys.argv:
        _selftest()
        return
    app = Sims4ModSorterApp()
    app.mainloop()

if __name__ == "__main__":
    main()
