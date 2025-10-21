from __future__ import annotations

import json
import os
import re
from collections import Counter
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union


__all__ = [
    "AdultEvidence",
    "CATEGORY_INDEX",
    "CATEGORY_ORDER",
    "DEFAULT_FOLDER_MAP",
    "FileItem",
    "ProgressCallback",
    "ScanResult",
    "bundle_scripts_and_packages",
    "classify_from_types",
    "dbpf_scan_types",
    "get_default_mods_path",
    "guess_type_for_name",
    "human_mb",
    "inspect_adult_content",
    "map_type_to_folder",
    "normalize_extension",
    "normalize_key",
    "pretty_display_name",
    "refine_with_metadata",
    "scan_folder",
    "SUPPORTED_EXTS",
]


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
    "Adult Script": "Adult/Scripts",
    "Adult Gameplay": "Adult/Gameplay",
    "Adult Animation": "Adult/Animations",
    "Adult Pose": "Adult/Poses",
    "Adult CAS": "Adult/CAS",
    "Adult BuildBuy": "Adult/BuildBuy",
    "Adult Override": "Adult/Overrides",
    "Adult Other": "Adult/Misc",
    "Script Mod": "Gameplay/Scripts",
    "Gameplay Tuning": "Gameplay/Tuning",
    "CAS Hair": "CAS/Hair",
    "CAS Clothing": "CAS/Clothing",
    "CAS Makeup": "CAS/Makeup",
    "CAS Skin": "CAS/Skin",
    "CAS Eyes": "CAS/Eyes",
    "CAS Accessories": "CAS/Accessories",
    "BuildBuy Object": "BuildBuy/Objects",
    "BuildBuy Recolour": "BuildBuy/Recolours",
    "Animation": "Visuals/Animations",
    "Preset": "Visuals/Presets",
    "Pose": "Visuals/Poses",
    "Slider": "CAS/Sliders",
    "World": "World",
    "Override": "Overrides",
    "Utility Tool": "Utilities",
    "Archive": "Archives",
    "Other": "Misc",
    "Unknown": "Unsorted",
}

PACKAGE_EXTS = {".package"}
SCRIPT_EXTS = {".ts4script", ".t4script"}
ARCHIVE_EXTS = {".zip", ".rar", ".7z"}
SUPPORTED_EXTS = PACKAGE_EXTS | SCRIPT_EXTS | ARCHIVE_EXTS | {
    ".ts4script.off",
    ".package.off",
    ".zip.off",
    ".rar.off",
    ".7z.off",
    ".ts4script.disabled",
    ".package.disabled",
    ".zip.disabled",
    ".rar.disabled",
    ".7z.disabled",
    ".ts4script.bak",
    ".package.bak",
    ".zip.bak",
    ".rar.bak",
    ".7z.bak",
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
ARCHIVE_SUMMARY_MAX_ENTRIES = 200

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
    _normalise_author_token(name): name for name in ADULT_AUTHORS
}


def _ensure_adult_word_cache(limit: int = ADULT_SCAN_MAX_BYTES) -> None:
    global _ADULT_WORD_CACHE, _ADULT_MAX_WORD_LEN
    _ADULT_WORD_CACHE = tuple(
        sorted((word for word in ADULT_WORDS if word), key=len, reverse=True)
    )
    _ADULT_MAX_WORD_LEN = max((len(word) for word in _ADULT_WORD_CACHE), default=0)


def _load_adult_words_override() -> None:
    override_path = Path(__file__).with_name("adult_words.json")
    if not override_path.is_file():
        return
    try:
        data = json.loads(override_path.read_text(encoding="utf-8"))
    except Exception:
        return
    words = data.get("words") if isinstance(data, dict) else None
    if isinstance(words, list):
        for word in words:
            if isinstance(word, str) and word.strip():
                ADULT_WORDS.add(word.strip().lower())
    _ensure_adult_word_cache()


_load_adult_words_override()


def _ensure_adult_category(category: str) -> str:
    return ADULT_CATEGORY_PROMOTIONS.get(category, "Adult Other")


def _strip_adult_category(category: str) -> str:
    base = ADULT_CATEGORY_DEMOTIONS.get(category)
    if base:
        return base
    return category.replace("Adult ", "", 1) if category.startswith("Adult ") else category


def _scan_stream_for_keywords(stream, limit: int) -> set[str]:
    _ensure_adult_word_cache(limit)
    hits: set[str] = set()
    remaining = limit
    carry = ""
    while remaining > 0:
        chunk = stream.read(min(ADULT_SCAN_CHUNK_SIZE, remaining))
        if not chunk:
            break
        remaining -= len(chunk)
        if isinstance(chunk, bytes):
            lowered = chunk.decode("utf-8", "ignore").lower()
        else:
            lowered = str(chunk).lower()
        window = carry + lowered
        if window:
            for keyword in _ADULT_WORD_CACHE:
                if keyword and keyword in window:
                    hits.add(keyword)
        if _ADULT_MAX_WORD_LEN > 1 and window:
            carry = window[-(_ADULT_MAX_WORD_LEN - 1) :]
        else:
            carry = ""
    return hits


def _scan_file_for_adult_keywords(path: Path, limit: int = ADULT_SCAN_MAX_BYTES) -> set[str]:
    try:
        with path.open("rb") as stream:
            return _scan_stream_for_keywords(stream, limit)
    except Exception:
        return set()


def _scan_text_file_for_adult_keywords(path: Path, limit: int = ADULT_SCAN_MAX_BYTES) -> set[str]:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return set()
    snippet = text.lower()[:limit]
    return {word for word in _ADULT_WORD_CACHE if word and word in snippet}


_ARCHIVE_SUMMARY_EXT_TYPES = {
    ".package": "packages",
}
for _ext in SCRIPT_EXTS:
    _ARCHIVE_SUMMARY_EXT_TYPES[_ext] = "ts4scripts"
for _ext in (".py", ".pyc", ".pyo"):
    _ARCHIVE_SUMMARY_EXT_TYPES[_ext] = "python"
for _ext in TEXT_FILE_EXTS:
    _ARCHIVE_SUMMARY_EXT_TYPES[_ext] = "text"
for _ext in (".png", ".jpg", ".jpeg"):
    _ARCHIVE_SUMMARY_EXT_TYPES[_ext] = "images"
for _ext in ARCHIVE_EXTS:
    _ARCHIVE_SUMMARY_EXT_TYPES[_ext] = "archives"

_ARCHIVE_SUMMARY_LABELS: Dict[str, Tuple[str, str]] = {
    "packages": ("package", "packages"),
    "ts4scripts": ("ts4script", "ts4scripts"),
    "python": ("python file", "python files"),
    "text": ("text file", "text files"),
    "images": ("image", "images"),
    "archives": ("archive", "archives"),
    "other": ("file", "files"),
}


def _summarise_archive_contents(path: Path, limit: int = ARCHIVE_SUMMARY_MAX_ENTRIES) -> Tuple[Counter[str], bool, int]:
    counts: Counter[str] = Counter()
    truncated = False
    total_entries = 0
    try:
        with zipfile.ZipFile(path, "r") as archive:
            names = [name for name in archive.namelist() if name and not name.endswith("/")]
        total_entries = len(names)
        max_entries = limit if limit and limit > 0 else total_entries
        for index, name in enumerate(names):
            if index >= max_entries:
                truncated = True
                break
            ext = Path(name).suffix.lower()
            if not ext:
                continue
            category = _ARCHIVE_SUMMARY_EXT_TYPES.get(ext)
            if category:
                counts[category] += 1
            else:
                counts["other"] += 1
    except Exception:
        return Counter(), False, 0
    return counts, truncated, total_entries


def _scan_archive_for_adult_keywords(path: Path) -> Tuple[set[str], List[str]]:
    hits: set[str] = set()
    sources: List[str] = []
    try:
        with zipfile.ZipFile(path, "r") as archive:
            for index, info in enumerate(archive.infolist()):
                if index >= ADULT_SCAN_MAX_ARCHIVE_ENTRIES:
                    break
                lowered = info.filename.lower()
                name_hits = {word for word in _ADULT_WORD_CACHE if word and word in lowered}
                if name_hits:
                    hits.update(name_hits)
                    if len(sources) < ADULT_SCAN_MAX_SOURCES:
                        sources.append(info.filename)
                    continue
                try:
                    with archive.open(info, "r") as stream:
                        limit = min(getattr(info, "file_size", ADULT_SCAN_MAX_BYTES), ADULT_SCAN_MAX_BYTES)
                        stream_hits = _scan_stream_for_keywords(stream, limit)
                except Exception:
                    continue
                if stream_hits:
                    hits.update(stream_hits)
                    if len(sources) < ADULT_SCAN_MAX_SOURCES:
                        sources.append(info.filename)
    except Exception:
        return set(), []
    return hits, sources


def _format_adult_sources(sources: Sequence[str]) -> str:
    if not sources:
        return ""
    display = ", ".join(sources[:ADULT_SCAN_MAX_SOURCES])
    if len(sources) > ADULT_SCAN_MAX_SOURCES:
        return display + ", …"
    return display


def inspect_adult_content(
    path: Path,
    ext: str,
    tokens: Tuple[str, ...],
    category: str,
    *,
    relpath: Optional[str] = None,
    folder_tokens: Optional[Tuple[str, ...]] = None,
) -> AdultEvidence:
    evidence = AdultEvidence()
    tokens = tokens or tuple()
    folder_tokens = folder_tokens or tuple()
    token_hits = {token for token in tokens if token in ADULT_WORDS}
    if token_hits:
        evidence.add(token_hits, 0.25, "Filename keyword")
    if folder_tokens:
        folder_hits = {token for token in folder_tokens if token in ADULT_WORDS}
        if folder_hits:
            evidence.add(folder_hits, 0.2, "Folder keyword")
    lowered_rel = (relpath or "").lower()
    if lowered_rel and "wicked" in lowered_rel:
        evidence.add({"wicked"}, 0.1, "Folder hint")
    if ext in SCRIPT_EXTS:
        hits = _scan_file_for_adult_keywords(path)
        if hits:
            evidence.add(hits, 0.3, "Script contents")
    elif ext == ".package":
        hits = _scan_file_for_adult_keywords(path)
        if hits:
            evidence.add(hits, 0.35, "Package contents")
    elif ext in ARCHIVE_EXTS:
        hits, sources = _scan_archive_for_adult_keywords(path)
        if hits:
            evidence.add(hits, 0.4, f"Archive contents ({_format_adult_sources(sources)})")
    if not evidence.hits and category.startswith("Adult"):
        text_hits = _scan_text_file_for_adult_keywords(path) if path.suffix.lower() in TEXT_FILE_EXTS else set()
        if text_hits:
            evidence.add(text_hits, 0.2, "Adult text reference")
    author_tokens = set()
    for token in tokens:
        normalised = _normalise_author_token(token)
        if normalised in ADULT_AUTHOR_ALIASES:
            author_tokens.add(ADULT_AUTHOR_ALIASES[normalised])
    if author_tokens:
        evidence.add(author_tokens, 0.2, "Known adult creator")
    return evidence


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
        if keyword == token:
            return True
    return False


def guess_type_for_name(name: str, ext: str) -> Tuple[str, float, str, Tuple[str, ...]]:
    tokens = _tokenise(name)
    joined = " " + " ".join(tokens) + " "
    ext = ext.lower()
    adult = any(token in ADULT_WORDS for token in tokens)
    tags: Tuple[str, ...] = tuple()
    if adult:
        for keyword in ("anim", "animation"):
            if keyword in joined:
                return "Adult Animation", 0.9, "Adult keyword + animation", tags
        if "pose" in joined:
            return "Adult Pose", 0.85, "Adult keyword + pose", tags
        if any(keyword in joined for keyword in ("hair", "dress", "top", "skirt", "tattoo", "freckle")):
            return "Adult CAS", 0.85, "Adult keyword + CAS", tags
        if "override" in joined:
            return "Adult Override", 0.8, "Adult keyword + override", tags
        return "Adult Gameplay", 0.75, "Adult keyword", tags
    if ext in SCRIPT_EXTS:
        return "Script Mod", 0.85, "Script-like extension", tags
    if ext in ARCHIVE_EXTS:
        return "Archive", 0.5, "Archive container", tags
    if ext == ".package":
        lowered = name.lower()
        for keyword, category in _KEYWORD_MAP:
            if _keyword_matches(keyword, tokens, joined):
                adult_category = ADULT_CATEGORY_PROMOTIONS.get(category, "Adult Other")
                category = adult_category if adult else category
                return category, 0.65, f"Keyword '{keyword}'", tags
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


def classify_from_types(
    types: Dict[int, int],
    filename: str,
    adult_hint: bool,
) -> Tuple[str, float, str, Tuple[str, ...]]:
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
        if any(key in lower for key in ("ring", "necklace", "ear", "nail", "piercing")):
            return "CAS Accessories", 0.85, notes, tags
        return "CAS Clothing", 0.8, notes, tags
    if has(0x319E4F1D) or has(0x015A1849) or has(0x01661233) or has(0x01D10F34):
        category = "Adult BuildBuy" if adult_hint else "BuildBuy Object"
        return category, 0.85, notes, tags
    if has(0x0354796A):
        category = "Adult CAS" if adult_hint else "CAS Skin"
        return category, 0.85, notes, tags
    if has(0x02D5DF13):
        category = "Adult Animation" if adult_hint else "Animation"
        return category, 0.85, notes, tags
    if has(0x220557DA):
        category = "Adult Gameplay" if adult_hint else "Gameplay Tuning"
        return category, 0.75, notes, tags
    category = "Adult Other" if adult_hint else "Other"
    return category, 0.6, notes, tags


def guess_type_for_extension(ext: str) -> Tuple[str, float, str]:
    if ext in SCRIPT_EXTS:
        return "Script Mod", 0.85, "Script extension"
    if ext in ARCHIVE_EXTS:
        return "Archive", 0.5, "Archive container"
    if ext == ".package":
        return "Other", 0.4, "Package (no keyword match)"
    if ext in TEXT_FILE_EXTS:
        return "Utility Tool", 0.4, "Utility/config file"
    return "Unknown", 0.3, "Unrecognised extension"


def refine_with_metadata(
    path: Path,
    current: Tuple[str, float, str, Tuple[str, ...]] | Tuple[str, float, str],
) -> Tuple[str, float, str, Tuple[str, ...]]:
    if len(current) == 4:
        category, confidence, notes, tags = current
    elif len(current) == 3:
        category, confidence, notes = current
        tags = tuple()
    else:
        raise ValueError("Unexpected metadata tuple length")
    ext_raw = path.suffix
    ext, disabled_flag = normalize_extension(ext_raw)
    filename = path.name
    if disabled_flag and filename.lower().endswith("off"):
        filename = filename[:-3]
    adult_hint = any(token in ADULT_WORDS for token in _tokenise(filename))
    if ext in SCRIPT_EXTS:
        if is_ts4script_or_zip_script(path):
            return (
                "Adult Script" if adult_hint else "Script Mod",
                max(confidence, 1.0),
                "Zip/ts4script with .py/.pyc",
                tags,
            )
        return (
            "Adult Other" if adult_hint else "Archive",
            max(confidence, 0.6),
            "Zip without Python",
            tags,
        )
    if ext != ".package":
        return category, confidence, notes, tags
    try:
        types = dbpf_scan_types(path)
        if types:
            new_category, new_confidence, new_notes, new_tags = classify_from_types(types, filename, adult_hint)
            if new_confidence >= confidence:
                return new_category, new_confidence, new_notes, new_tags
            merged_notes = f"{notes}; {new_notes}" if notes else new_notes
            merged_tags = new_tags or tags
            return category, confidence, merged_notes, merged_tags
        return category, confidence, notes, tags
    except Exception as exc:
        merged = f"{notes}; dbpf-scan err: {exc}" if notes else f"dbpf-scan err: {exc}"
        return category, confidence, merged, tags


def is_ts4script_or_zip_script(path: Path) -> bool:
    ext, _ = normalize_extension(path.suffix)
    if ext not in SCRIPT_EXTS | {".zip"}:
        return False
    try:
        with zipfile.ZipFile(path, "r") as archive:
            for name in archive.namelist():
                lowered = name.lower()
                if lowered.endswith(".py") or lowered.endswith(".pyc"):
                    return True
    except Exception:
        return False
    return False


def map_type_to_folder(cat: str, folder_map: Dict[str, str]) -> str:
    return folder_map.get(cat, folder_map.get("Unknown", "Unsorted"))


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
    ignore_names_tokens = [
        token.lower() for token in (ignore_names or []) if isinstance(token, str) and token.strip()
    ]
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
        initial_category, confidence, notes, tags = guess_type_for_name(effective_name, ext)
        category, confidence, notes, tags = refine_with_metadata(
            path, (initial_category, confidence, notes, tags)
        )
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
        archive_meta_tags: List[str] = []
        archive_extras: Dict[str, str] = {}
        if not is_disabled_ext and ext in ARCHIVE_EXTS:
            summary_counts, summary_truncated, summary_total = _summarise_archive_contents(path)
            if summary_total:
                summary_parts: List[str] = []
                for key, (singular, plural) in _ARCHIVE_SUMMARY_LABELS.items():
                    count = summary_counts.get(key, 0)
                    if not count:
                        continue
                    label = singular if count == 1 else plural
                    summary_parts.append(f"{count} {label}")
                    archive_meta_tags.append(f"{count} {label}")
                    archive_extras[f"archive_{key}"] = str(count)
                archive_extras["archive_entries"] = str(summary_total)
                if summary_truncated:
                    archive_extras["archive_summary_truncated"] = "true"
                    if summary_parts:
                        summary_parts.append("…")
                if not summary_parts:
                    label = "file" if summary_total == 1 else "files"
                    summary_parts.append(f"{summary_total} {label}")
                archive_note = "Archive contents: " + ", ".join(summary_parts)
                notes = f"{notes}; {archive_note}" if notes else archive_note
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
        for tag in archive_meta_tags:
            if tag not in raw_tags:
                raw_tags.append(tag)
        meta_tags = ", ".join(raw_tags)
        extras: Dict[str, str] = dict(archive_extras)
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
# Bundling helper
# ---------------------------------------------------------------------------


def bundle_scripts_and_packages(items: Sequence[FileItem], folder_map: Dict[str, str]) -> Dict[str, int]:
    script_lookup: Dict[str, FileItem] = {}
    for item in items:
        if item.disabled:
            continue
        if item.ext in SCRIPT_EXTS and item.guess_type in {"Script Mod", "Adult Script"}:
            script_lookup[normalize_key(item.name)] = item
    linked = 0
    for item in items:
        if item.disabled or item.ext != ".package":
            continue
        key = normalize_key(item.name)
        script_item = script_lookup.get(key)
        if not script_item:
            continue
        bundle_key = f"bundle:{key}"
        item.bundle = bundle_key
        script_item.bundle = bundle_key
        if item.target_folder == folder_map.get("Unknown", "Unsorted"):
            item.target_folder = folder_map.get(script_item.guess_type, item.target_folder)
        linked += 1
    return {"linked": linked, "scripts": len(script_lookup)}


def is_ts4script(path: Path) -> bool:
    return path.suffix.lower() in SCRIPT_EXTS


def is_archive(path: Path) -> bool:
    return path.suffix.lower() in ARCHIVE_EXTS


__all__ += ["SCRIPT_EXTS", "ARCHIVE_EXTS", "PACKAGE_EXTS", "TEXT_FILE_EXTS", "is_ts4script", "is_archive"]
