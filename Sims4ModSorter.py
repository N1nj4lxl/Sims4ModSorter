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
