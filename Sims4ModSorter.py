"""Sims4 Mod Sorter

Refined single-file application with a cleaned scan pipeline, stable plugin hooks,
thread-safe Tk interactions, and offline heuristics. Python 3.10+ only.
"""
from __future__ import annotations

import importlib.util
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import tkinter as tk
from tkinter import filedialog, ttk


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


@dataclass(slots=True)
class ScanResult:
    items: List[FileItem]
    total_files: int
    errors: List[str] = field(default_factory=list)


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
    "lingerie",
    "nipple",
    "areola",
    "sperm",
    "spermicide",
    "lubricant",
    "aphrodisiac",
    "escort",
    "brothel",
    "latex",
    "polyurethane",
    "polyisoprene",
    "birthcontrol",
    "durex",
    "trojan",
    "std",
    "sti",
)

ADULT_WORDS: set[str] = set(ADULT_WORDS_BASE)


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


_load_adult_words_override()


# ---------------------------------------------------------------------------
# Theme registry
# ---------------------------------------------------------------------------

THEMES: Dict[str, Dict[str, str]] = {
    "Dark Mode": {"bg": "#111316", "fg": "#E6E6E6", "alt": "#161A1E", "accent": "#4C8BF5", "sel": "#2A2F3A"},
    "Slightly Dark Mode": {"bg": "#14161a", "fg": "#EAEAEA", "alt": "#1b1e24", "accent": "#6AA2FF", "sel": "#2f3642"},
    "Light Mode": {"bg": "#FAFAFA", "fg": "#1f2328", "alt": "#FFFFFF", "accent": "#316DCA", "sel": "#E8F0FE"},
    "High Contrast Mode": {"bg": "#000000", "fg": "#FFFFFF", "alt": "#000000", "accent": "#FFD400", "sel": "#333333"},
    "Pink Holiday": {"bg": "#1a1216", "fg": "#FFE7F3", "alt": "#23171e", "accent": "#FF5BA6", "sel": "#3a1f2c"},
}

# ---------------------------------------------------------------------------
# Plugin system
# ---------------------------------------------------------------------------

USER_MODS_DIR: Path = Path(__file__).resolve().with_name("user_mods")


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


class ModAPI:
    """API exposed to user mods."""

    def __init__(self, manager: "PluginManager") -> None:
        self._manager = manager

    def register_pre_scan_hook(self, func: Callable[[Dict[str, object], "ModAPI"], None]) -> None:
        if callable(func):
            self._manager.pre_scan_hooks.append(func)

    def register_post_scan_hook(self, func: Callable[[List[FileItem], Dict[str, object], "ModAPI"], None]) -> None:
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
        self, title: str, builder: Callable[["Sims4ModSorterApp", ttk.Frame, "ModAPI"], None]
    ) -> None:
        if callable(builder) and title:
            self._manager.settings_sections.append((title, builder))

    def request_refresh(self) -> None:
        app = self.app
        if app is not None:
            app.schedule_refresh()


class PluginManager:
    def __init__(self, mods_dir: Path, message_bus: Optional[PluginMessageBus] = None) -> None:
        self.mods_dir = mods_dir
        self.pre_scan_hooks: List[Callable[[Dict[str, object], ModAPI], None]] = []
        self.post_scan_hooks: List[Callable[[List[FileItem], Dict[str, object], ModAPI], None]] = []
        self.message_bus = message_bus or PluginMessageBus()
        self.api = ModAPI(self)
        self.columns: Dict[str, PluginColumn] = {}
        self.column_order: List[str] = []
        self.settings_sections: List[Tuple[str, Callable[["Sims4ModSorterApp", ttk.Frame, ModAPI], None]]] = []
        self.app: Optional["Sims4ModSorterApp"] = None

    def attach_app(self, app: "Sims4ModSorterApp") -> None:
        self.app = app

    def load(self) -> None:
        self.mods_dir.mkdir(parents=True, exist_ok=True)
        for entry in sorted(self.mods_dir.iterdir(), key=lambda p: p.name.lower()):
            manifest: Dict[str, object]
            module_path: Path
            if entry.is_dir():
                manifest_path = entry / "mod.json"
                if manifest_path.exists():
                    try:
                        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    except Exception as exc:
                        self.message_bus.post("boot", "error", f"Failed to parse {manifest_path.name}: {exc}")
                        continue
                else:
                    manifest = {"name": entry.name, "entry": "mod.py", "enabled": True, "callable": "register"}
                module_path = entry / str(manifest.get("entry", "mod.py"))
            else:
                manifest = {"name": entry.stem, "entry": entry.name, "enabled": True, "callable": "register"}
                module_path = entry
            self._load_manifest(manifest, module_path)

    def _load_manifest(self, manifest: Dict[str, object], module_path: Path) -> None:
        name = str(manifest.get("name") or module_path.stem)
        if not manifest.get("enabled", True):
            self.message_bus.post("boot", "info", f"Skipping disabled mod: {name}")
            return
        if not module_path.exists():
            self.message_bus.post("boot", "error", f"Missing entry for {name}: {module_path.name}")
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
            self.message_bus.post("boot", "error", f"Failed to import {name}: {exc}")
            return
        callable_name = str(manifest.get("callable", "register"))
        register = getattr(module, callable_name, None)
        if not callable(register):
            self.message_bus.post("boot", "warn", f"Mod '{name}' missing callable '{callable_name}'")
            return
        try:
            register(self.api)
        except Exception as exc:
            self.message_bus.post("boot", "error", f"Mod '{name}' failed during register: {exc}")
            return
        self.message_bus.post("boot", "info", f"Loaded mod: {name}")

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


def load_user_mods() -> PluginManager:
    manager = PluginManager(USER_MODS_DIR)
    try:
        manager.load()
    except Exception as exc:
        manager.message_bus.post("boot", "error", f"Mod loading aborted: {exc}")
    return manager


def flush_plugin_messages(app, channel: str) -> None:
    manager = getattr(app, "plugin_manager", None)
    if not isinstance(manager, PluginManager):
        return
    prefix = {"error": "Mod error", "warn": "Mod warning", "info": "Mod"}
    for level, message in manager.message_bus.drain(channel):
        if not message:
            continue
        tag = prefix.get(level, "Mod")
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


def _tokenise(name: str) -> Tuple[str, ...]:
    base = re.sub(r"\.[^.]+$", "", name.lower())
    base = re.sub(r"[^a-z0-9]+", " ", base)
    return tuple(token for token in base.split(" ") if token)


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
    if path.suffix.lower() not in PACKAGE_EXTS:
        return category, confidence, notes, tags
    adult_hint = any(token in ADULT_WORDS for token in _tokenise(path.name))
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
    items: List[FileItem] = []
    errors: List[str] = []
    candidates = list(_iter_files(root, recurse))
    total = len(candidates)
    for index, path in enumerate(candidates, start=1):
        name = path.name
        ext = path.suffix.lower()
        lowered = name.lower()
        if ignore_exts_set and ext in ignore_exts_set:
            if progress_cb:
                progress_cb(index, total, path, "ignored")
            continue
        if ignore_names_tokens and any(token in lowered for token in ignore_names_tokens):
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
        initial = _guess_from_name(name, ext)
        refined = refine_with_metadata(path, initial)
        category, confidence, notes, tags = refined
        target_folder = folder_map.get(category, folder_map.get("Unknown", "Unsorted"))
        relpath = str(path.relative_to(root)) if path != root else name
        items.append(
            FileItem(
                path=path,
                name=name,
                ext=ext,
                size_mb=size,
                relpath=relpath,
                guess_type=category,
                confidence=confidence,
                notes=notes,
                target_folder=target_folder,
                include=True,
                meta_tags=", ".join(tags),
            )
        )
        if progress_cb:
            progress_cb(index, total, path, "scanned")
    items.sort(
        key=lambda item: (
            CATEGORY_INDEX.get(item.guess_type, len(CATEGORY_ORDER)),
            _natural_key(os.path.dirname(item.relpath) or "."),
            _natural_key(item.name),
        )
    )
    return ScanResult(items, total, errors)

# ---------------------------------------------------------------------------
# Bundling and move helpers
# ---------------------------------------------------------------------------

LOG_NAME = ".sims4_modsorter_moves.json"


def bundle_scripts_and_packages(items: Sequence[FileItem], folder_map: Dict[str, str]) -> Dict[str, int]:
    script_lookup: Dict[str, FileItem] = {}
    for item in items:
        if item.ext in SCRIPT_EXTS and item.guess_type in {"Script Mod", "Adult Script"}:
            script_lookup[normalize_key(item.name)] = item
    linked = 0
    for item in items:
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
        self.theme_name = tk.StringVar(value="Dark Mode")
        self.mods_root = tk.StringVar(value=get_default_mods_path())

        self.items: List[FileItem] = []
        self.items_by_path: Dict[str, FileItem] = {}
        self.scan_errors: List[str] = []
        self.plugin_manager = load_user_mods()
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

        self._build_style()
        self._build_ui()
        self._build_settings_overlay()
        self.after(16, self._pump_ui_queue)
        self._report_mod_boot_messages()

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
        ttk.Button(top, text="Undo Last", command=self.on_undo).pack(side="right", padx=6)

        mid = ttk.Frame(root_container)
        mid.pack(fill="both", expand=True, padx=12, pady=(6, 8))
        header = ttk.Frame(mid)
        header.pack(fill="x", pady=(0, 6))
        ttk.Label(header, textvariable=self.summary_var).pack(side="left")

        left = ttk.Frame(mid)
        left.pack(side="left", fill="both", expand=True)
        base_columns = ["inc", "rel", "name", "size", "type", "target", "conf", "linked", "meta", "notes"]
        columns = list(base_columns)
        if self._plugin_columns:
            insert_at = columns.index("linked")
            for plugin_column in self._plugin_columns:
                columns.insert(insert_at, plugin_column.column_id)
                insert_at += 1
        self._column_order = columns
        columns = ("inc", "rel", "name", "size", "type", "target", "conf", "linked", "meta", "notes")
        self.tree = ttk.Treeview(left, columns=columns, show="headings", selectmode="extended")
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
        for plugin_column in self._plugin_columns:
            headings[plugin_column.column_id] = plugin_column.heading
        for column in columns:
            self.tree.heading(column, text=headings.get(column, column))
        for column in columns:
            self.tree.heading(column, text=headings[column])
        self.tree.column("inc", width=40, anchor="center")
        self.tree.column("rel", width=220)
        self.tree.column("name", width=360)
        self.tree.column("size", width=70, anchor="e")
        self.tree.column("type", width=170)
        self.tree.column("target", width=200)
        self.tree.column("conf", width=60, anchor="e")
        for plugin_column in self._plugin_columns:
            self.tree.column(plugin_column.column_id, width=plugin_column.width, anchor=plugin_column.anchor, stretch=False)
        self.tree.column("linked", width=80, anchor="center")
        self.tree.column("meta", width=180)
        self.tree.column("notes", width=260)
        ysb = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=ysb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        ysb.pack(side="left", fill="y")

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
        self.overlay = tk.Frame(self, bg=self._theme_cache.get("sel", "#2A2F3A"))
        self.overlay.columnconfigure(0, weight=1)
        self.overlay.rowconfigure(0, weight=1)

        card = ttk.Frame(self.overlay, padding=18)
        card.grid(row=0, column=0, sticky="nsew")
        card.columnconfigure(0, weight=1)

        header = ttk.Frame(card)
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="Settings", font=("TkDefaultFont", 12, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Button(header, text="Close", command=self.hide_settings, width=7).grid(row=0, column=1, sticky="e")

        next_row = 1
        theme_section = ttk.LabelFrame(card, text="Themes")
        theme_section.grid(row=next_row, column=0, sticky="ew", pady=(12, 0))
        theme_section.columnconfigure(0, weight=1)

        theme_controls = ttk.Frame(theme_section)
        theme_controls.grid(row=0, column=0, sticky="ew")
        theme_controls.columnconfigure(1, weight=1)
        ttk.Label(theme_controls, text="Theme").grid(row=0, column=0, sticky="w")
        self.theme_cb = ttk.Combobox(theme_controls, values=list(THEMES.keys()), textvariable=self.theme_name, state="readonly")
        self.theme_cb.grid(row=0, column=1, sticky="ew", padx=6)
        ttk.Button(theme_controls, text="Apply", command=self.on_apply_theme).grid(row=0, column=2, padx=(6, 0))

        self.theme_preview_container = ttk.Frame(theme_section)
        self.theme_preview_container.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        for column in range(3):
            self.theme_preview_container.columnconfigure(column, weight=1)
        self._build_theme_preview_widgets()

        scan_section = ttk.LabelFrame(card, text="Scanning")
        scan_section.grid(row=2, column=0, sticky="ew", pady=(16, 0))
        scan_section.columnconfigure(0, weight=1)
        ttk.Checkbutton(scan_section, text="Scan subfolders", variable=self.recurse_var).grid(row=0, column=0, sticky="w")
        ttk.Label(scan_section, text="Ignore extensions (comma separated)").grid(row=1, column=0, sticky="w", pady=(10, 2))
        ttk.Entry(scan_section, textvariable=self.ignore_exts_var).grid(row=2, column=0, sticky="ew")
        ttk.Label(scan_section, text="Ignore names containing (comma separated)").grid(row=3, column=0, sticky="w", pady=(10, 2))
        ttk.Entry(scan_section, textvariable=self.ignore_names_var).grid(row=4, column=0, sticky="ew")

        actions = ttk.Frame(card)
        actions.grid(row=3, column=0, sticky="e", pady=(18, 0))
        ttk.Button(actions, text="Done", command=self.hide_settings).grid(row=0, column=0)

        self.overlay.bind("<Escape>", lambda _e: self.hide_settings())
        self.overlay.place_forget()
        self._update_theme_preview_highlight()
    # ------------------------------------------------------------------
    # Theme preview helpers
    # ------------------------------------------------------------------
    def _build_theme_preview_widgets(self) -> None:
        for child in self.theme_preview_container.winfo_children():
            child.destroy()
        self.theme_preview_canvases: Dict[str, tk.Canvas] = {}
        columns = 3
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

    def log(self, message: str) -> None:
        timestamp = time.strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{timestamp}] {message}\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    # ------------------------------------------------------------------
    # Settings overlay
    # ------------------------------------------------------------------
    def show_settings(self) -> None:
        self.overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
        self.overlay.tkraise()
        self.overlay.focus_set()
        self._update_theme_preview_highlight()

    def hide_settings(self) -> None:
        self.overlay.place_forget()

    def on_apply_theme(self) -> None:
        self._build_style()
        palette = self._theme_cache
        self.log_text.configure(bg=palette.get("alt", "#1f2328"), fg=palette.get("fg", "#E6E6E6"))
        self.overlay.configure(bg=palette.get("sel", "#2A2F3A"))
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
        }
        if self.plugin_manager:
            self.plugin_manager.run_pre_scan(context)
            ignore_exts = [str(ext).strip() for ext in context.get("ignore_exts", ignore_exts) if str(ext).strip()]
            ignore_names = [str(name).strip() for name in context.get("ignore_names", ignore_names) if str(name).strip()]
            mods_path = Path(context.get("mods_root", mods_path))
        recurse = bool(context.get("recurse", self.recurse_var.get()))

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
            }
            for item in self.items
        ]
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
        self.items_by_path = {str(item.path): item for item in self.items}
        counts: Dict[str, int] = {}
        for item in self.items:
            counts[item.guess_type] = counts.get(item.guess_type, 0) + 1
            values = (
                "✓" if item.include else "",
                os.path.dirname(item.relpath) or ".",
                pretty_display_name(item.name),
                f"{item.size_mb:.2f}",
                item.guess_type,
                item.target_folder,
                f"{item.confidence:.2f}",
                "🔗" if item.bundle else "",
                item.meta_tags,
                item.notes,
            )
            iid = str(item.path)
            self.tree.insert("", "end", iid=iid, values=values)
            if iid in selected:
                self.tree.selection_add(iid)
        if self.items:
            topcats = sorted(counts.items(), key=lambda pair: -pair[1])[:4]
            fragment = ", ".join(f"{name}: {count}" for name, count in topcats)
            self.summary_var.set(f"Planned {len(self.items)} files | {fragment}")
        else:
            self.summary_var.set("No plan yet")
        self._on_resize()

    def _on_resize(self, _event: Optional[tk.Event] = None) -> None:
        total_width = self.tree.winfo_width() or 1200
        fixed = 40 + 220 + 70 + 170 + 200 + 60 + 80 + 180
        dynamic = max(300, total_width - fixed - 60)
        name_width = int(dynamic * 0.6)
        notes_width = int(dynamic * 0.4)
        self.tree.column("name", width=max(220, name_width))
        self.tree.column("notes", width=max(220, notes_width))
# ---------------------------------------------------------------------------
# Entry points and self-test
# ---------------------------------------------------------------------------


def _selftest() -> None:
    samples = {
        "wickedwhims_nude_top.package": "Adult CAS",
        "durex_bundle.zip": "Adult Other",
        "uicheats.ts4script": "Script Mod",
        "fantasyhair.package": "CAS Hair",
        "object_sofa.package": "BuildBuy Object",
    }
    for name, expected in samples.items():
        cat, _ = guess_type_for_name(name)
        assert cat == expected, f"{name} -> {cat}, expected {expected}"
    script_guess = _guess_from_name("awesome_mod.ts4script", ".ts4script")
    assert script_guess[0] == "Script Mod"
    archive_guess = _guess_from_name("poses.zip", ".zip")
    assert archive_guess[0] in {"Archive", "Adult Other"}
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
from tkinter import ttk, filedialog
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
# Modding support
# ---------------------------

USER_MODS_DIR = Path(__file__).with_name("user_mods")


class ModAPI:
    """Lightweight API exposed to external mods."""

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
    def __init__(self, mods_dir: Path):
        self.mods_dir = mods_dir
        self.pre_scan_hooks = []
        self.post_scan_hooks = []
        self.boot_messages: List[Tuple[str, str]] = []
        self.runtime_messages: List[Tuple[str, str]] = []
        self.api = ModAPI(self)

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
        self.mods_dir.mkdir(parents=True, exist_ok=True)
        for entry in sorted(self.mods_dir.iterdir()):
            if entry.is_file() and entry.suffix == ".py":
                manifest = {
                    "name": entry.stem,
                    "entry": entry.name,
                    "enabled": True,
                    "callable": "register",
                }
                self._load_mod(manifest, entry)
            elif entry.is_dir():
                manifest_path = entry / "mod.json"
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
                        "entry": "mod.py",
                        "enabled": True,
                        "callable": "register",
                    }
                self._load_mod(manifest, entry / manifest.get("entry", "mod.py"))

    def _load_mod(self, manifest: Dict[str, object], entry_path: Path):
        name = manifest.get("name") or entry_path.stem
        if not manifest.get("enabled", True):
            self.log_boot(f"Skipping disabled mod: {name}")
            return
        if not entry_path.exists():
            self.log_boot(f"Missing entry file for mod '{name}': {entry_path.name}", level="error")
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
            self.log_boot(f"Failed to import mod '{name}': {exc}", level="error")
            return

        callable_name = manifest.get("callable", "register")
        register = getattr(module, callable_name, None)
        if not callable(register):
            self.log_boot(f"Mod '{name}' missing callable '{callable_name}'", level="warn")
            return
        try:
            register(self.api)
        except Exception as exc:
            self.log_boot(f"Mod '{name}' failed during register: {exc}", level="error")
            return
        self.log_boot(f"Loaded mod: {name}")

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


def load_user_mods() -> PluginManager:
    manager = PluginManager(USER_MODS_DIR)
    try:
        manager.load()
    except Exception as exc:
        manager.log_boot(f"Mod loading aborted: {exc}", level="error")
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
        "error": "Mod error",
        "warn": "Mod warning",
    }
    for level, message in drain():
        if not message:
            continue
        prefix = prefix_map.get(level, "Mod")
        app.log(f"{prefix}: {message}")

# ---------------------------
# Modding support
# ---------------------------

USER_MODS_DIR = Path(__file__).with_name("user_mods")


class ModAPI:
    """Lightweight API exposed to external mods."""

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
    def __init__(self, mods_dir: Path):
        self.mods_dir = mods_dir
        self.pre_scan_hooks = []
        self.post_scan_hooks = []
        self.boot_messages: List[Tuple[str, str]] = []
        self.runtime_messages: List[Tuple[str, str]] = []
        self.api = ModAPI(self)

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
        self.mods_dir.mkdir(parents=True, exist_ok=True)
        for entry in sorted(self.mods_dir.iterdir()):
            if entry.is_file() and entry.suffix == ".py":
                manifest = {
                    "name": entry.stem,
                    "entry": entry.name,
                    "enabled": True,
                    "callable": "register",
                }
                self._load_mod(manifest, entry)
            elif entry.is_dir():
                manifest_path = entry / "mod.json"
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
                        "entry": "mod.py",
                        "enabled": True,
                        "callable": "register",
                    }
                self._load_mod(manifest, entry / manifest.get("entry", "mod.py"))

    def _load_mod(self, manifest: Dict[str, object], entry_path: Path):
        name = manifest.get("name") or entry_path.stem
        if not manifest.get("enabled", True):
            self.log_boot(f"Skipping disabled mod: {name}")
            return
        if not entry_path.exists():
            self.log_boot(f"Missing entry file for mod '{name}': {entry_path.name}", level="error")
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
            self.log_boot(f"Failed to import mod '{name}': {exc}", level="error")
            return

        callable_name = manifest.get("callable", "register")
        register = getattr(module, callable_name, None)
        if not callable(register):
            self.log_boot(f"Mod '{name}' missing callable '{callable_name}'", level="warn")
            return
        try:
            register(self.api)
        except Exception as exc:
            self.log_boot(f"Mod '{name}' failed during register: {exc}", level="error")
            return
        self.log_boot(f"Loaded mod: {name}")

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


def load_user_mods() -> PluginManager:
    manager = PluginManager(USER_MODS_DIR)
    try:
        manager.load()
    except Exception as exc:
        manager.log_boot(f"Mod loading aborted: {exc}", level="error")
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
        "error": "Mod error",
        "warn": "Mod warning",
    }
    for level, message in drain():
        if not message:
            continue
        prefix = prefix_map.get(level, "Mod")
        app.log(f"{prefix}: {message}")

# ---------------------------
# Modding support
# ---------------------------

USER_MODS_DIR = Path(__file__).with_name("user_mods")


class ModAPI:
    """Lightweight API exposed to external mods."""

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
    def __init__(self, mods_dir: Path):
        self.mods_dir = mods_dir
        self.pre_scan_hooks = []
        self.post_scan_hooks = []
        self.boot_messages: List[Tuple[str, str]] = []
        self.runtime_messages: List[Tuple[str, str]] = []
        self.api = ModAPI(self)

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
        self.mods_dir.mkdir(parents=True, exist_ok=True)
        for entry in sorted(self.mods_dir.iterdir()):
            if entry.is_file() and entry.suffix == ".py":
                manifest = {
                    "name": entry.stem,
                    "entry": entry.name,
                    "enabled": True,
                    "callable": "register",
                }
                self._load_mod(manifest, entry)
            elif entry.is_dir():
                manifest_path = entry / "mod.json"
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
                        "entry": "mod.py",
                        "enabled": True,
                        "callable": "register",
                    }
                self._load_mod(manifest, entry / manifest.get("entry", "mod.py"))

    def _load_mod(self, manifest: Dict[str, object], entry_path: Path):
        name = manifest.get("name") or entry_path.stem
        if not manifest.get("enabled", True):
            self.log_boot(f"Skipping disabled mod: {name}")
            return
        if not entry_path.exists():
            self.log_boot(f"Missing entry file for mod '{name}': {entry_path.name}", level="error")
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
            self.log_boot(f"Failed to import mod '{name}': {exc}", level="error")
            return

        callable_name = manifest.get("callable", "register")
        register = getattr(module, callable_name, None)
        if not callable(register):
            self.log_boot(f"Mod '{name}' missing callable '{callable_name}'", level="warn")
            return
        try:
            register(self.api)
        except Exception as exc:
            self.log_boot(f"Mod '{name}' failed during register: {exc}", level="error")
            return
        self.log_boot(f"Loaded mod: {name}")

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


def load_user_mods() -> PluginManager:
    manager = PluginManager(USER_MODS_DIR)
    try:
        manager.load()
    except Exception as exc:
        manager.log_boot(f"Mod loading aborted: {exc}", level="error")
    return manager

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
    "genital", "penis", "vagina", "condom", "dildo", "vibrator", "plug", "cum"
}

def is_ts4script_or_zip_script(path: str) -> bool:
    ext = os.path.splitext(path)[1].lower()
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
        "naked","strip","lapdance","prostitution","erotic","aphrodisiac","genital","penis","vagina"
    }
    condom_terms = _load_relatedwords_condom()     # fetched once per run
    adult_terms = base_adult | condom_terms

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
    ext = os.path.splitext(path)[1].lower()
    filename = os.path.basename(path)
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
def scan_folder(path: str, folder_map: Dict[str, str], recurse: bool = True,
                ignore_exts: Optional[set] = None, ignore_name_contains: Optional[List[str]] = None,
                progress_cb=None) -> List[FileItem]:
    if not os.path.isdir(path):
        return []

    ignore_exts = {
        ext.strip().lower() if ext.strip().startswith('.') else f".{ext.strip().lower()}"
        for ext in (ignore_exts or set()) if ext and ext.strip()
    }
    ignore_name_contains = [tok.strip().lower() for tok in (ignore_name_contains or []) if tok and tok.strip()]

    def iter_candidates():
        if recurse:
            for root, _, files in os.walk(path):
                for fname in files:
                    yield os.path.join(root, fname)
        else:
            with os.scandir(path) as entries:
                for entry in entries:
                    if entry.is_file():
                        yield entry.path

    def count_candidates():
        if recurse:
            total = 0
            for _, _, files in os.walk(path):
                total += len(files)
            return total
        with os.scandir(path) as entries:
            return sum(1 for entry in entries if entry.is_file())

    total = count_candidates()
    items: List[FileItem] = []

    for idx, fpath in enumerate(iter_candidates(), start=1):
        fname = os.path.basename(fpath)
        lowname = fname.lower()
        ext = os.path.splitext(fname)[1].lower()

        if ext in ignore_exts or any(tok in lowname for tok in ignore_name_contains):
            if progress_cb:
                progress_cb(idx, total, fpath, "ignored")
            continue

        try:
            size_mb = human_mb(os.path.getsize(fpath))
            guess = guess_type_for_name(fname, ext)
            cat, conf, notes = guess
            cat, conf, notes, tags = refine_with_metadata(fpath, guess)
            relp = os.path.relpath(fpath, path)
            target = map_type_to_folder(cat, folder_map)
            items.append(FileItem(
                path=fpath,
                name=fname,
                ext=ext,
                size_mb=size_mb,
                relpath=relp,
                guess_type=cat,
                confidence=conf,
                notes=notes,
                include=True,
                target_folder=target,
                meta_tags=tags,
            ))
            if progress_cb:
                progress_cb(idx, total, fpath, "scanned")
        except Exception as e:
            relp = os.path.relpath(fpath, path) if os.path.exists(fpath) else fname
            items.append(FileItem(
                path=fpath,
                name=fname,
                ext=ext,
                size_mb=0.0,
                relpath=relp,
                guess_type="Unknown",
                confidence=0.0,
                notes=f"scan error: {e}",
                include=False,
                target_folder=map_type_to_folder("Unknown", folder_map),
            ))
            if progress_cb:
                progress_cb(idx, total, fpath, "error")

    items.sort(key=lambda fi: (
        CATEGORY_INDEX.get(fi.guess_type, len(CATEGORY_ORDER)),
        _natural_key(os.path.dirname(fi.relpath) or '.'),
        _natural_key(fi.name),
    ))
    return items

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
        if it.ext in SCRIPT_EXTS and it.guess_type in {"Script Mod", "Adult Script"}:
            scripts[normalize_key(it.name)] = it
    linked = 0
    for it in items:
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
        self.theme_name = tk.StringVar(value="Dark Mode")
        self.mods_root = tk.StringVar(value=get_default_mods_path())
        self.items: List[FileItem] = []
        self.plugin_manager = load_user_mods()

        self.status_var = tk.StringVar(value="Ready")
        self.summary_var = tk.StringVar(value="No plan yet")

        self._build_style()
        self._build_ui()
        self._build_settings_overlay()
        self.bind("<Configure>", self._on_resize)
        self._report_mod_boot_messages()

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
        root_container = ttk.Frame(self); root_container.pack(fill="both", expand=True)

        top = ttk.Frame(root_container); top.pack(fill="x", padx=12, pady=10)
        ttk.Label(top, text="Mods folder:").pack(side="left")
        self.entry_path = ttk.Entry(top, textvariable=self.mods_root, width=80); self.entry_path.pack(side="left", padx=8)
        ttk.Button(top, text="Browse", command=self.on_browse).pack(side="left", padx=4)
        ttk.Button(top, text="Scan", command=self.on_scan).pack(side="left", padx=4)
        ttk.Label(top, textvariable=self.status_var).pack(side="left", padx=12)
        ttk.Button(top, text="⚙", width=3, command=self.show_settings).pack(side="right")
        ttk.Button(top, text="Undo Last", command=self.on_undo).pack(side="right", padx=6)

        mid = ttk.Frame(root_container); mid.pack(fill="both", expand=True, padx=12, pady=(6, 8))
        header = ttk.Frame(mid); header.pack(fill="x", pady=(0,6))
        ttk.Label(header, textvariable=self.summary_var).pack(side="left")

        left = ttk.Frame(mid); left.pack(side="left", fill="both", expand=True)
        cols = ("inc", "rel", "name", "size", "type", "target", "conf", "linked", "meta", "notes")
        self.tree = ttk.Treeview(left, columns=cols, show="headings", selectmode="extended")
        for c,t in [("inc","✔"),("rel","Folder"),("name","File"),("size","MB"),("type","Type"),
                    ("target","Target Folder"),("conf","Conf"),("linked","Linked"),("meta","Meta"),("notes","Notes")]:
            self.tree.heading(c, text=t)
        self.tree.column("inc", width=40, anchor="center")
        self.tree.column("rel", width=220)
        self.tree.column("name", width=360)
        self.tree.column("size", width=70, anchor="e")
        self.tree.column("type", width=170)
        self.tree.column("target", width=200)
        self.tree.column("conf", width=60, anchor="e")
        self.tree.column("linked", width=80, anchor="center")
        self.tree.column("meta", width=180)
        self.tree.column("notes", width=260)
        ysb = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=ysb.set); self.tree.pack(side="left", fill="both", expand=True); ysb.pack(side="left", fill="y")

        right = ttk.Frame(mid); right.pack(side="left", fill="y", padx=(10, 0))
        ttk.Label(right, text="Selection").pack(anchor="w")
        self.sel_label = ttk.Label(right, text="None selected"); self.sel_label.pack(anchor="w", pady=(0, 10))
        ttk.Label(right, text="Type").pack(anchor="w")
        self.type_cb = ttk.Combobox(right, values=CATEGORY_ORDER, state="readonly"); self.type_cb.pack(fill="x", pady=(0, 8))
        ttk.Label(right, text="Target Folder").pack(anchor="w")
        self.target_entry = ttk.Entry(right); self.target_entry.pack(fill="x", pady=(0, 8))
        ttk.Button(right, text="Apply to Selected", command=self.on_apply_selected).pack(fill="x", pady=4)
        ttk.Button(right, text="Toggle Include", command=self.on_toggle_include).pack(fill="x", pady=4)
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
        self.log_text = tk.Text(logf, height=6, wrap="word", state="disabled", relief="flat",
                                bg=self._theme_cache["alt"], fg=self._theme_cache["fg"])
        self.log_text.pack(fill="both", expand=False)

        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self.tree.bind("<Double-1>", self.on_double_click)

    # In-window settings overlay (no OS pop-up)
    def _build_settings_overlay(self):
        self.overlay = tk.Frame(self, bg=self._theme_cache["sel"])
        self.overlay.columnconfigure(0, weight=1)
        self.overlay.rowconfigure(0, weight=1)

        self.overlay_card = ttk.Frame(self.overlay, padding=18)
        self.overlay_card.grid(row=0, column=0, sticky="nsew")
        self.overlay_card.columnconfigure(0, weight=1)

        header = ttk.Frame(self.overlay_card)
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="Settings", font=("TkDefaultFont", 12, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Button(header, text="Close", command=self.hide_settings, width=7).grid(row=0, column=1, sticky="e")

        theme_section = ttk.LabelFrame(self.overlay_card, text="Themes")
        theme_section.grid(row=1, column=0, sticky="ew", pady=(12, 0))
        theme_section.columnconfigure(0, weight=1)

        theme_controls = ttk.Frame(theme_section)
        theme_controls.grid(row=0, column=0, sticky="ew")
        theme_controls.columnconfigure(1, weight=1)
        ttk.Label(theme_controls, text="Theme").grid(row=0, column=0, sticky="w")
        self.theme_cb = ttk.Combobox(theme_controls, values=list(THEMES.keys()), textvariable=self.theme_name, state="readonly")
        self.theme_cb.grid(row=0, column=1, sticky="ew", padx=6)
        ttk.Button(theme_controls, text="Apply", command=self.on_apply_theme).grid(row=0, column=2, padx=(6,0))

        self.theme_preview_container = ttk.Frame(theme_section)
        self.theme_preview_container.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        for col in range(3):
            self.theme_preview_container.columnconfigure(col, weight=1)
        self._build_theme_preview_widgets()

        scan_section = ttk.LabelFrame(self.overlay_card, text="Scanning")
        scan_section.grid(row=2, column=0, sticky="ew", pady=(16, 0))
        scan_section.columnconfigure(0, weight=1)

        self.chk_recurse = ttk.Checkbutton(scan_section, text="Scan subfolders", variable=self.recurse_var)
        self.chk_recurse.grid(row=0, column=0, sticky="w")

        ttk.Label(scan_section, text="Ignore extensions (comma separated)").grid(row=1, column=0, sticky="w", pady=(10, 2))
        ttk.Entry(scan_section, textvariable=self.ignore_exts_var).grid(row=2, column=0, sticky="ew")

        ttk.Label(scan_section, text="Ignore names containing (comma separated)").grid(row=3, column=0, sticky="w", pady=(10, 2))
        ttk.Entry(scan_section, textvariable=self.ignore_names_var).grid(row=4, column=0, sticky="ew")

        actions = ttk.Frame(self.overlay_card)
        actions.grid(row=3, column=0, sticky="e", pady=(18, 0))
        ttk.Button(actions, text="Done", command=self.hide_settings).grid(row=0, column=0)

        self.overlay.bind("<Escape>", lambda e: self.hide_settings())
        self.overlay.place_forget()
        self._update_theme_preview_highlight()

    def show_settings(self):
        self.overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
        self.overlay.tkraise()
        self.overlay.focus_set()
        self._update_theme_preview_highlight()

    def hide_settings(self):
        self.overlay.place_forget()

    def _build_theme_preview_widgets(self):
        if not hasattr(self, "theme_preview_container"):
            return
        for child in self.theme_preview_container.winfo_children():
            child.destroy()
        self.theme_preview_canvases: Dict[str, tk.Canvas] = {}

        columns = 3
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
    def log(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{ts}] {msg}\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def on_apply_theme(self):
        self._build_style()
        self.log_text.configure(bg=self._theme_cache["alt"], fg=self._theme_cache["fg"])
        self.overlay.configure(bg=self._theme_cache["sel"])
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
        self.status_var.set("Scanning…")
        self.progress.configure(maximum=100, value=0)
        self.items = []

        def progress_cb(done, total, path, state):
            pct = int((done/total)*100) if total else 0
            self.progress.configure(value=pct, maximum=100)
            if done % 25 == 0 or state == "error":
                self.status_var.set(f"Scanning {done}/{total}: {os.path.basename(path)}")
            if state == "error":
                self.log(f"Scan error: {os.path.basename(path)}")

        def worker():
            ignore_exts = {e.strip() for e in self.ignore_exts_var.get().split(',')}
            ignore_names = [t.strip() for t in self.ignore_names_var.get().split(',')]
            context = {
                "mods_root": mods,
                "recurse": self.recurse_var.get(),
                "ignore_exts": ignore_exts,
                "ignore_names": ignore_names,
                "folder_map": self.folder_map,
            }
            if self.plugin_manager:
                self.plugin_manager.run_pre_scan(context)
                if isinstance(context.get("ignore_exts"), (list, set, tuple)):
                    ignore_exts = {str(ext).strip() for ext in context["ignore_exts"] if str(ext).strip()}
                if isinstance(context.get("ignore_names"), (list, set, tuple)):
                    ignore_names = [str(name).strip() for name in context["ignore_names"] if str(name).strip()]
                if context.get("mods_root"):
                    context["mods_root"] = str(context["mods_root"])
            scan_root = str(context.get("mods_root", mods) or mods)
            if scan_root != mods:
                try:
                    self.mods_root.set(scan_root)
                except Exception:
                    pass

            items = scan_folder(
                scan_root, self.folder_map,
                recurse=self.recurse_var.get(),
                ignore_exts=ignore_exts,
                ignore_name_contains=ignore_names,
                progress_cb=progress_cb,
            )
            stats = bundle_scripts_and_packages(items, self.folder_map)
            if self.plugin_manager:
                context.update({
                    "bundle_stats": stats,
                    "items": items,
                    "scan_root": scan_root,
                })
                self.plugin_manager.run_post_scan(items, context)
                items.sort(key=lambda fi: (
                    CATEGORY_INDEX.get(fi.guess_type, len(CATEGORY_ORDER)),
                    _natural_key(os.path.dirname(fi.relpath) or '.'),
                    _natural_key(fi.name),
                ))
            self.items = items
            self._refresh_tree()
            self.status_var.set(f"Plan: {len(self.items)} files")
            self.log(f"Scan complete. Planned {len(self.items)} files. Linked packages: {stats['linked']} across {stats['scripts']} script(s).")
            self._report_mod_runtime_messages()

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
            self.tree.insert("", "end", iid=iid, values=vals)
            if preserve_selection and iid in selected_iids:
                self.tree.selection_add(iid)
        if total:
            topcats = sorted(by_cat.items(), key=lambda kv: -kv[1])[:4]
            frag = ", ".join(f"{k}: {v}" for k, v in topcats)
            self.summary_var.set(f"Planned {total} files | {frag}")
        else:
            self.summary_var.set("No plan yet")
        self._on_resize()

    def _on_resize(self, event=None):
        total_w = self.tree.winfo_width() or 1200
        fixed = 40 + 220 + 70 + 170 + 200 + 60 + 80 + 180  # fixed widths
        dynamic = max(300, total_w - fixed - 60)
        name_w = int(dynamic * 0.6); notes_w = int(dynamic * 0.4)
        self.tree.column("name", width=max(220, name_w))
        self.tree.column("notes", width=max(220, notes_w))

# ---------------------------
# Entry
# ---------------------------
def main():
    app = Sims4ModSorterApp()
    app.mainloop()

if __name__ == "__main__":
    main()
