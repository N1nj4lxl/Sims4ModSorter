from __future__ import annotations

import concurrent.futures
import json
import os
import re
import sqlite3
import threading
import time
import zipfile
from collections import Counter
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


def _env_flag(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    value = value.strip().lower()
    return value in {"1", "true", "yes", "on"}


_SCAN_DEBUG = _env_flag("SIMS4_SCANNER_DEBUG")
_debug_lock = threading.Lock()


def _debug_log(message: str) -> None:
    if not _SCAN_DEBUG:
        return
    with _debug_lock:
        print(f"[scanner] {message}")

import zlib


__all__ = [
    "AdultEvidence",
    "CATEGORY_INDEX",
    "CATEGORY_ORDER",
    "DEFAULT_FOLDER_MAP",
    "DUPLICATE_EXTRA_KEY",
    "FINGERPRINT_EXTRA_KEY",
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


FINGERPRINT_EXTRA_KEY = "fingerprint"
DUPLICATE_EXTRA_KEY = "duplicate"
UNKNOWN_DEFAULT_FOLDER = "Mods/NeedsReview"


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
    target_folder: str = UNKNOWN_DEFAULT_FOLDER
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
# Feature flags and metrics
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatureFlags:
    fast_mode: bool = True
    peek_budget_bytes: int = 131072
    enable_folder_bias: bool = True
    treat_package_as_nonfinal: bool = True
    cache_db: str = "scan_cache.db"
    routing_map: str = "DEFAULT_FOLDER_MAP_V2"
    use_legacy_routing: bool = False


@dataclass(slots=True)
class ScanMetrics:
    total_time: float = 0.0
    files_scanned: int = 0
    cache_hits: int = 0
    decisive_headers: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_scan(self, duration: float, decisive: bool) -> None:
        with self.lock:
            self.total_time += max(0.0, duration)
            self.files_scanned += 1
            if decisive:
                self.decisive_headers += 1

    def record_cache_hit(self) -> None:
        with self.lock:
            self.cache_hits += 1

    def average_time_ms(self) -> float:
        with self.lock:
            if not self.files_scanned:
                return 0.0
            return (self.total_time / self.files_scanned) * 1000.0


@dataclass(slots=True)
class PathBiasRule:
    category: str
    score: int
    patterns: Tuple[str, ...]


# ---------------------------------------------------------------------------
# Classification constants
# ---------------------------------------------------------------------------


CATEGORY_ORDER: List[str] = [
    "Script Mod",
    "Adult",
    "CAS",
    "BuildBuy",
    "Pose or Animation",
    "Tuning",
    "Mixed",
    "Resources",
    "Archive",
    "Other",
    "Unknown",
]

CATEGORY_INDEX: Dict[str, int] = {name: idx for idx, name in enumerate(CATEGORY_ORDER)}

DEFAULT_FOLDER_MAP_V2: Dict[str, str] = {
    "Script Mod": "Mods/Scripts/",
    "Adult": "Mods/NeedsReview/Adult/",
    "CAS": "Mods/CAS/",
    "BuildBuy": "Mods/BuildBuy/",
    "Pose or Animation": "Mods/Animations/",
    "Tuning": "Mods/Tuning/",
    "Mixed": "Mods/NeedsReview/",
    "Resources": "Mods/NeedsReview/",
    "Archive": "Mods/NeedsReview/",
    "Other": "Mods/NeedsReview/",
    "Unknown": "Mods/NeedsReview/",
}

DEFAULT_FOLDER_MAP: Dict[str, str] = DEFAULT_FOLDER_MAP_V2

ROUTING_MAPS: Dict[str, Dict[str, str]] = {
    "DEFAULT_FOLDER_MAP_V2": DEFAULT_FOLDER_MAP_V2,
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
    "Script Mod": "Adult",
    "CAS": "Adult",
    "BuildBuy": "Adult",
    "Tuning": "Adult",
    "Mixed": "Adult",
    "Resources": "Adult",
    "Archive": "Adult",
    "Other": "Adult",
    "Unknown": "Adult",
}

ADULT_CATEGORY_DEMOTIONS: Dict[str, str] = {
    "Adult": "Other",
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
    0x2F7D0004: "RLE2",
    0xCD0F1220: "SLOT",
    0x160D0E6A: "RSLT",
    0x0AE3FDE5: "FTPT",
    0x6B20C4F3: "CLIP",
    0xE882D22F: "I7",
    0x03B33DDF: "ITUN",
    0xD1F577C6: "SXML",
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
)

ADULT_WORDS = set(token.lower() for token in ADULT_WORDS_BASE)


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
            tokens.append(token.capitalize())
    return " ".join(tokens)


NORMALISE_KEY_RE = re.compile(r"[^a-z0-9]+")


def normalize_key(value: str) -> str:
    lowered = value.lower()
    collapsed = NORMALISE_KEY_RE.sub("-", lowered)
    return collapsed.strip("-")


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def _effective_extension(path: Path) -> Tuple[str, bool]:
    suffixes = [suffix.lower() for suffix in path.suffixes]
    if not suffixes:
        return "", False
    disabled_suffixes = {".off", ".disabled", ".bak"}
    disabled = False
    ext = suffixes[-1]
    if suffixes[-1] in disabled_suffixes and len(suffixes) >= 2:
        disabled = True
        ext = suffixes[-2]
    return ext, disabled


# ---------------------------------------------------------------------------
# Legacy placeholders (adult scanning no longer used but kept for API)
# ---------------------------------------------------------------------------


def inspect_adult_content(*_args, **_kwargs) -> AdultEvidence:  # type: ignore[override]
    return AdultEvidence()

# ---------------------------------------------------------------------------
# Name heuristics and classification helpers
# ---------------------------------------------------------------------------


TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def _tokenise(value: str) -> Tuple[str, ...]:
    return tuple(token.lower() for token in TOKEN_RE.findall(value))


def _tokenise_path_parts(path: Path) -> Tuple[str, ...]:
    tokens: List[str] = []
    for part in path.parts:
        tokens.extend(_tokenise(part))
    return tuple(tokens)


def _shorten_note(note: str, max_words: int = 10) -> str:
    """Trim notes that become overly long during scanning."""

    words = note.split()
    if len(words) <= max_words:
        return note
    trimmed = " ".join(words[:max_words])
    return trimmed + "\u2026"


DECISIVE_DBPF: Dict[str, set[int]] = {
    "CAS": {0x034AEECB, 0x015A1849, 0x067CAA11, 0x2F7D0004},
    "BuildBuy": {0x319E4F1D, 0xCD0F1220, 0x160D0E6A, 0x0AE3FDE5},
    "Pose or Animation": {0x6B20C4F3, 0xE882D22F},
    "Tuning": {0x03B33DDF, 0xD1F577C6},
}

POSE_HEADER_TYPES: set[int] = {0x6B20C4F3, 0xE882D22F}
TUNING_HEADER_TYPES: set[int] = {0x03B33DDF, 0xD1F577C6}
STBL_TYPE_ID = 0x220557DA


def _score_dbpf_types(types: Dict[int, int]) -> Tuple[Dict[str, int], List[str], bool, Optional[str]]:
    scores: Dict[str, int] = {}
    signals: List[str] = []
    decisive = False
    decisive_type: Optional[str] = None
    for type_id, count in types.items():
        name = TYPE_IDS.get(type_id, hex(type_id))
        weight = max(1, int(count))
        if type_id in DECISIVE_DBPF.get("CAS", set()):
            scores["CAS"] = scores.get("CAS", 0) + 5 * weight
            decisive = True
            decisive_type = name
            signals.append(f"header:{name}")
        elif type_id in DECISIVE_DBPF.get("BuildBuy", set()):
            scores["BuildBuy"] = scores.get("BuildBuy", 0) + 5 * weight
            decisive = True
            decisive_type = name
            signals.append(f"header:{name}")
        elif type_id in POSE_HEADER_TYPES:
            scores["Pose or Animation"] = scores.get("Pose or Animation", 0) + 4 * weight
            signals.append(f"header:{name}")
        elif type_id in TUNING_HEADER_TYPES:
            scores["Tuning"] = scores.get("Tuning", 0) + 4 * weight
            signals.append(f"header:{name}")
        elif type_id == STBL_TYPE_ID:
            scores["Tuning"] = scores.get("Tuning", 0) + 2 * weight
            signals.append(f"header:{name}")
    return scores, signals, decisive, decisive_type


@dataclass(slots=True)
class NameSignal:
    category: str
    confidence: float
    adult_risk: str
    tags: set[str]
    notes: List[str]
    family: Optional[str] = None
    tokens: Tuple[str, ...] = field(default_factory=tuple)
    score: int = 0
    signals: Tuple[str, ...] = field(default_factory=tuple)
    score_map: Dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class HeaderSignal:
    category: Optional[str]
    confidence: float
    decisive: bool
    tags: set[str] = field(default_factory=set)
    notes: List[str] = field(default_factory=list)
    family: Optional[str] = None
    supported: set[str] = field(default_factory=set)
    handler: str = "unknown"
    score: int = 0
    signals: Tuple[str, ...] = field(default_factory=tuple)
    decisive_type: Optional[str] = None

    def supports(self, category: str) -> bool:
        if not category:
            return False
        if self.category and self.category == category:
            return True
        return category in self.supported


@dataclass(slots=True)
class PeekSignal:
    hits: Dict[str, set[str]]


@dataclass(slots=True)
class ScanFinding:
    path: Path
    ext: str
    size: int
    category: str
    confidence: float
    notes: str
    tags: Tuple[str, ...]
    target: str
    needs_enrich: bool
    disabled: bool
    extras: Dict[str, str] = field(default_factory=dict)
    handler: str = ""
    confidence_score: int = 0
    signals: Tuple[str, ...] = field(default_factory=tuple)
    decisive_header: bool = False

    def to_payload(self) -> Dict[str, object]:
        return {
            "path": str(self.path),
            "ext": self.ext,
            "size": self.size,
            "category": self.category,
            "confidence": self.confidence,
            "notes": self.notes,
            "tags": list(self.tags),
            "target": self.target,
            "needs_enrich": self.needs_enrich,
            "disabled": self.disabled,
            "extras": self.extras,
            "handler": self.handler,
            "confidence_score": int(self.confidence_score),
            "signals": list(self.signals),
            "decisive_header": bool(self.decisive_header),
        }

    @classmethod
    def from_payload(cls, payload: Dict[str, object]) -> "ScanFinding":
        return cls(
            path=Path(str(payload["path"])),
            ext=str(payload["ext"]),
            size=int(payload["size"]),
            category=str(payload["category"]),
            confidence=float(payload["confidence"]),
            notes=str(payload.get("notes", "")),
            tags=tuple(str(tag) for tag in payload.get("tags", [])),
            target=str(payload.get("target", UNKNOWN_DEFAULT_FOLDER)),
            needs_enrich=bool(payload.get("needs_enrich", False)),
            disabled=bool(payload.get("disabled", False)),
            extras={str(k): str(v) for k, v in dict(payload.get("extras", {})).items()},
            handler=str(payload.get("handler", "")),
            confidence_score=int(payload.get("confidence_score", 0)),
            signals=tuple(str(sig) for sig in payload.get("signals", [])),
            decisive_header=bool(payload.get("decisive_header", False)),
        )


def _ensure_fingerprint_extra(finding: ScanFinding, fingerprint: str) -> None:
    extras = finding.extras
    if not isinstance(extras, dict):
        extras = {}
        finding.extras = extras
    extras[FINGERPRINT_EXTRA_KEY] = str(fingerprint)


# ---------------------------------------------------------------------------
# Cache layer
# ---------------------------------------------------------------------------


class ScanCache:
    def __init__(self, db_path: str, rules_version: int) -> None:
        self._path = db_path
        self._rules_version = rules_version
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._ensure_schema()
        self._purge_old_versions()

    def _ensure_schema(self) -> None:
        with self._conn:
            self._conn.execute("DROP TABLE IF EXISTS entries")
            self._conn.execute("DROP TABLE IF EXISTS fingerprints")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS files (
                    hash TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    mtime INTEGER NOT NULL,
                    rules_version INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    confidence INTEGER NOT NULL,
                    decisive_header INTEGER NOT NULL,
                    disabled INTEGER NOT NULL,
                    payload TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fingerprints (
                    fingerprint TEXT PRIMARY KEY,
                    hash TEXT NOT NULL,
                    FOREIGN KEY(hash) REFERENCES files(hash) ON DELETE CASCADE
                )
                """
            )

    def _make_hash(self, path: Path, size: int, mtime: float) -> str:
        return f"{path}:{int(size)}:{int(mtime)}"

    def _purge_old_versions(self) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "DELETE FROM files WHERE rules_version<>?",
                (self._rules_version,),
            )

    def lookup(self, path: Path, size: int, mtime: float) -> Optional[ScanFinding]:
        key = self._make_hash(path, size, mtime)
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT rules_version, payload FROM files WHERE hash=?",
                (key,),
            ).fetchone()
        if not row:
            return None
        if int(row[0]) != self._rules_version:
            return None
        payload_raw = row[1]
        if not payload_raw:
            return None
        payload = json.loads(payload_raw)
        finding = ScanFinding.from_payload(payload)
        finding.path = path
        return finding

    def upsert(
        self, finding: ScanFinding, st: os.stat_result, fingerprint: Optional[str]
    ) -> None:
        payload = json.dumps(finding.to_payload(), ensure_ascii=False)
        key = self._make_hash(finding.path, int(st.st_size), float(st.st_mtime))
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO files(hash, size, mtime, rules_version, category, confidence, decisive_header, disabled, payload)
                VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(hash) DO UPDATE SET
                    size=excluded.size,
                    mtime=excluded.mtime,
                    rules_version=excluded.rules_version,
                    category=excluded.category,
                    confidence=excluded.confidence,
                    decisive_header=excluded.decisive_header,
                    disabled=excluded.disabled,
                    payload=excluded.payload
                """,
                (
                    key,
                    int(st.st_size),
                    int(st.st_mtime),
                    self._rules_version,
                    finding.category,
                    int(finding.confidence_score),
                    1 if finding.decisive_header else 0,
                    1 if finding.disabled else 0,
                    payload,
                ),
            )
            if fingerprint:
                self._conn.execute(
                    "INSERT OR REPLACE INTO fingerprints(fingerprint, hash) VALUES(?, ?)",
                    (fingerprint, key),
                )

    def lookup_fingerprint(self, fingerprint: str) -> Optional[ScanFinding]:
        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT files.payload, files.rules_version
                FROM fingerprints
                JOIN files ON files.hash = fingerprints.hash
                WHERE fingerprints.fingerprint=?
                """,
                (fingerprint,),
            ).fetchone()
        if not row:
            return None
        if int(row[1]) != self._rules_version:
            return None
        payload_raw = row[0]
        if not payload_raw:
            return None
        payload = json.loads(payload_raw)
        return ScanFinding.from_payload(payload)

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class FingerprintIndex:
    def __init__(self, cache: ScanCache) -> None:
        self._cache = cache

    def has(self, fingerprint: str) -> bool:
        return self._cache.lookup_fingerprint(fingerprint) is not None

    def get(self, fingerprint: str) -> Optional[ScanFinding]:
        return self._cache.lookup_fingerprint(fingerprint)

    def link(self, fingerprint: str, finding: ScanFinding, st: os.stat_result) -> None:
        self._cache.upsert(finding, st, fingerprint)


# ---------------------------------------------------------------------------
# Context, heuristics, probes, and classifier
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ScanContext:
    rules: Dict[str, object]
    aho: "KeywordAutomaton"
    cache: ScanCache
    seen: FingerprintIndex
    budgets: Dict[str, object]
    thresholds: Dict[str, float]
    pool: concurrent.futures.ThreadPoolExecutor
    semaphore: threading.Semaphore
    features: FeatureFlags
    folder_map: Dict[str, str]
    path_bias: Tuple[PathBiasRule, ...]
    metrics: ScanMetrics
    rules_version: int


class KeywordAutomaton:
    def __init__(self, groups: Dict[str, Iterable[str]]) -> None:
        self._groups: Dict[str, Tuple[str, ...]] = {
            key: tuple(sorted({token.lower() for token in values if token}))
            for key, values in groups.items()
        }

    def search(self, data: bytes) -> Dict[str, set[str]]:
        try:
            text = data.decode("utf-8", errors="ignore").lower()
        except Exception:
            text = ""
        hits: Dict[str, set[str]] = {key: set() for key in self._groups}
        for key, patterns in self._groups.items():
            for pattern in patterns:
                if pattern and pattern in text:
                    hits[key].add(pattern)
        return hits



def load_rules(path: str) -> Dict[str, object]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {"authors": {}, "packs": {}, "folders": {}, "routing": {}}


def load_budgets(path: str) -> Dict[str, object]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {".package": 0, ".ts4script": 262144, ".zip": 0, "zip_sample": {}}


def load_thresholds(path: str) -> Dict[str, float]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return {key: float(value) if isinstance(value, (int, float)) else value for key, value in data.items()}
    except Exception:
        return {"high_conf": 0.8, "decisive_stop": True, "name_conf_stop": 0.7}


def load_keywords(path: str) -> Dict[str, List[str]]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {"adult": [], "script": [], "cas": [], "buildbuy": []}


def load_feature_flags(base_dir: Path) -> Tuple[FeatureFlags, int]:
    cfg_path = base_dir / "update_config.json"
    features = FeatureFlags()
    rules_version = 1
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return features, rules_version
    rules_version = int(data.get("scanner_rules_version", rules_version))
    flags = data.get("scanner_features", {})
    if isinstance(flags, dict):
        features.fast_mode = bool(flags.get("fast_mode", features.fast_mode))
        features.peek_budget_bytes = int(flags.get("peek_budget_bytes", features.peek_budget_bytes))
        features.enable_folder_bias = bool(flags.get("enable_folder_bias", features.enable_folder_bias))
        features.treat_package_as_nonfinal = bool(
            flags.get("treat_package_as_nonfinal", features.treat_package_as_nonfinal)
        )
        cache_db = flags.get("cache_db")
        if isinstance(cache_db, str) and cache_db.strip():
            features.cache_db = cache_db.strip()
        routing_map = flags.get("routing_map")
        if isinstance(routing_map, str) and routing_map.strip():
            features.routing_map = routing_map.strip()
        features.use_legacy_routing = bool(flags.get("use_legacy_routing", features.use_legacy_routing))
    return features, rules_version


class NameHeuristics:
    _name_token_map: Dict[str, Tuple[set[str], int]] = {}
    _cas_tokens: set[str] = set()
    _script_tokens: set[str] = set()
    _adult_tokens: set[str] = set()
    _generic_tokens: set[str] = set()
    _features: FeatureFlags = FeatureFlags()
    _path_bias: Tuple[PathBiasRule, ...] = tuple()

    @classmethod
    def configure(
        cls,
        keywords: Dict[str, List[str]],
        path_bias: Tuple[PathBiasRule, ...],
        features: FeatureFlags,
    ) -> None:
        name_tokens = dict(keywords.get("name_tokens", {}))
        cas_tokens = {token.lower() for token in name_tokens.get("cas", [])}
        build_tokens = {token.lower() for token in name_tokens.get("buildbuy", [])}
        pose_tokens = {token.lower() for token in name_tokens.get("pose", [])}
        tuning_tokens = {token.lower() for token in name_tokens.get("tuning", [])}
        cls._name_token_map = {
            "CAS": (cas_tokens, 3),
            "BuildBuy": (build_tokens, 3),
            "Pose or Animation": (pose_tokens, 4),
            "Tuning": (tuning_tokens, 3),
        }
        cls._cas_tokens = cas_tokens
        cls._generic_tokens = {token.lower() for token in name_tokens.get("generic", [])}
        cls._script_tokens = {token.lower() for token in keywords.get("script", [])}
        cls._adult_tokens = {token.lower() for token in keywords.get("adult", [])}
        cls._path_bias = path_bias
        cls._features = features

    @classmethod
    def cas_tokens(cls) -> set[str]:
        return set(cls._cas_tokens)

    @classmethod
    def guess(cls, path: Path, rules: Dict[str, object]) -> NameSignal:
        name = path.name
        tokens = _tokenise(name)
        token_set = set(tokens)
        folder_tokens = _tokenise_path_parts(path.parent)
        folder_token_set = set(folder_tokens)
        ext, _ = _effective_extension(path)

        notes: List[str] = []
        tags: set[str] = set()
        signals: List[str] = []
        scores: Dict[str, int] = {}
        base_scores: Dict[str, int] = {}
        adult_risk = "low"
        adult_hits: set[str] = set()
        family: Optional[str] = None

        def bump(category: str, amount: int, signal: Optional[str] = None) -> None:
            if not category or amount <= 0:
                return
            scores[category] = scores.get(category, 0) + amount
            if signal:
                signals.append(signal)

        def base(category: str, amount: int) -> None:
            if not category:
                return
            base_scores.setdefault(category, amount)
            scores.setdefault(category, 0)

        if ext in SCRIPT_EXTS:
            base("Script Mod", 10)
            bump("Script Mod", 10, "name:ext:ts4script")
        elif ext in ARCHIVE_EXTS:
            base("Archive", 6)
            bump("Archive", 2, "name:ext:archive")
        elif ext in PACKAGE_EXTS:
            base("Package", 4)
        else:
            base("Other", 3)

        seen_token_hits: Dict[Tuple[str, str], bool] = {}
        for token in token_set:
            for category, (token_pool, amount) in cls._name_token_map.items():
                if token in token_pool and (category, token) not in seen_token_hits:
                    seen_token_hits[(category, token)] = True
                    bump(category, amount, f"name:{token}")
                    tags.add(f"name:{category.lower().replace(' ', '_')}")
                    notes.append(f"Name token: {token}")
            if token in cls._script_tokens:
                bump("Script Mod", 4, f"name:{token}")
                tags.add("name:script")
            if token in cls._adult_tokens:
                adult_risk = "high"
                adult_hits.add(token)
                tags.add("name:adult")

        if adult_risk == "low" and cls._adult_tokens & folder_token_set:
            adult_risk = "medium"
            tags.add("folder:adult")

        for token in folder_token_set:
            if token in cls._script_tokens:
                bump("Script Mod", 3, f"folder:{token}")
                tags.add("folder:script")

        if cls._generic_tokens & token_set:
            bump("Mixed", len(cls._generic_tokens & token_set), "name:generic")

        author_rules = dict(rules.get("authors", {}))
        for token in tokens + folder_tokens:
            bias = author_rules.get(token)
            if not bias:
                continue
            bias_category = str(bias.get("bias", ""))
            base_category = bias_category
            local_family: Optional[str] = None
            if ":" in bias_category:
                base_category, local_family = bias_category.split(":", 1)
            boost = float(bias.get("boost", 0.2))
            if base_category:
                bump(base_category, max(3, int(round(10 * boost))), f"author:{token}")
                tags.add(f"author:{token}")
                if local_family:
                    family = local_family or family
                notes.append(f"Author bias: {token}")

        pack_rules = dict(rules.get("packs", {}))
        for token in token_set:
            bias = pack_rules.get(token)
            if not bias:
                continue
            bias_category = str(bias.get("bias", ""))
            base_category = bias_category
            local_family: Optional[str] = None
            if ":" in bias_category:
                base_category, local_family = bias_category.split(":", 1)
            if base_category:
                bump(base_category, 6, f"pack:{token}")
                tags.add(f"pack:{token}")
                if local_family:
                    family = local_family or family
                notes.append(f"Pack bias: {token}")

        folder_rules = dict(rules.get("folders", {}))
        for token in folder_token_set:
            bias = folder_rules.get(token)
            if not bias:
                continue
            bias_category = str(bias.get("bias", ""))
            base_category = bias_category
            local_family: Optional[str] = None
            if ":" in bias_category:
                base_category, local_family = bias_category.split(":", 1)
            boost = float(bias.get("boost", 0.0))
            if base_category:
                bump(base_category, max(2, int(round(10 * boost))), f"folder:{token}")
                tags.add(f"folder:{token}")
                if local_family:
                    family = local_family or family
                notes.append(f"Folder bias: {token}")

        name_best = max((score for category, score in scores.items() if category != "Package"), default=0)
        if cls._features.enable_folder_bias and name_best < 6:
            folder_text = str(path.parent).lower()
            for rule in cls._path_bias:
                if any(pattern.lower() in folder_text for pattern in rule.patterns):
                    bump(rule.category, rule.score, f"folderbias:{rule.category}")
                    tags.add(f"folderbias:{rule.category}")
                    notes.append(f"Folder bias: {rule.category}")

        if not scores:
            scores["Unknown"] = 0

        best_category = max(
            scores.items(),
            key=lambda kv: (kv[1], -CATEGORY_INDEX.get(kv[0].split(":", 1)[0], len(CATEGORY_ORDER))),
        )[0]

        total_score = scores.get(best_category, 0)
        base_amount = base_scores.get(best_category, 0)
        base_confidence = 0.35
        if best_category == "Script Mod":
            base_confidence = 0.7
        elif best_category == "Archive":
            base_confidence = 0.5
        elif best_category == "CAS" or best_category == "BuildBuy" or best_category == "Pose or Animation":
            base_confidence = 0.45
        elif best_category == "Tuning":
            base_confidence = 0.45
        elif best_category == "Package":
            base_confidence = 0.35
        confidence = min(1.0, base_confidence + max(0, total_score - base_amount) * 0.05)

        if adult_hits:
            notes.append("Adult keywords in name")

        signals = tuple(dict.fromkeys(signals))
        notes.append(f"Tokens: {', '.join(tokens[:6])}" if tokens else "No tokens")

        return NameSignal(
            category=best_category,
            confidence=confidence,
            adult_risk=adult_risk,
            tags=tags,
            notes=notes,
            family=family,
            tokens=tokens,
            score=total_score,
            signals=signals,
            score_map=dict(scores),
        )


class HeaderProbe:
    @staticmethod
    def run(path: Path, ctx: ScanContext) -> HeaderSignal:
        ext, _ = _effective_extension(path)
        if ext == ".package":
            signal = DbpfProbe.inspect(path)
        elif ext in {".ts4script", ".t4script"}:
            signal = Ts4ScriptProbe.inspect(path)
        elif ext == ".zip":
            signal = ZipProbe.inspect(path, ctx)
        else:
            signal = HeaderSignal(
                category=None,
                confidence=0.0,
                decisive=False,
                handler="none",
                score=0,
                signals=tuple(),
            )
        return signal


class DbpfProbe:
    @staticmethod
    def inspect(path: Path) -> HeaderSignal:
        types = dbpf_scan_types(path, limit=10)
        if not types:
            return HeaderSignal(
                category=None,
                confidence=0.0,
                decisive=False,
                notes=["No DBPF index"],
                handler="dbpf",
                score=0,
                signals=tuple(),
            )
        tags = {TYPE_IDS.get(key, hex(key)) for key in types}
        notes = [
            "DBPF types: "
            + ", ".join(f"{TYPE_IDS.get(key, hex(key))}:{count}" for key, count in sorted(types.items()))
        ]
        score_map, raw_signals, decisive, decisive_type = _score_dbpf_types(types)
        supported = {category for category, score in score_map.items() if score > 0}
        if decisive and decisive_type:
            notes.append(f"Decisive header: {decisive_type}")
        if not score_map:
            return HeaderSignal(
                category=None,
                confidence=0.5,
                decisive=False,
                tags=tags,
                notes=notes,
                supported=set(),
                handler="dbpf",
                score=0,
                signals=tuple(),
            )
        best_category = max(
            score_map.items(),
            key=lambda kv: (kv[1], -CATEGORY_INDEX.get(kv[0].split(":", 1)[0], len(CATEGORY_ORDER))),
        )[0]
        score_value = score_map.get(best_category, 0)
        confidence = 0.55 + 0.04 * score_value
        if decisive:
            confidence = max(confidence, 0.92)
        confidence = min(0.99, confidence)
        return HeaderSignal(
            category=best_category,
            confidence=confidence,
            decisive=decisive,
            tags=tags,
            notes=notes,
            supported=supported,
            handler="dbpf",
            score=score_value,
            signals=tuple(dict.fromkeys(raw_signals)),
            decisive_type=decisive_type,
        )


class ZipProbe:
    @staticmethod
    def inspect(path: Path, ctx: ScanContext) -> HeaderSignal:
        try:
            with zipfile.ZipFile(path, "r") as archive:
                infos = archive.infolist()
        except Exception as exc:
            _debug_log(f"Zip probe exception for {path.name}: {exc}")
            return HeaderSignal(
                category=None,
                confidence=0.0,
                decisive=False,
                notes=[f"Zip read error: {exc}"],
                handler="zip",
                score=0,
                signals=tuple(),
            )

        ext_counts = Counter()
        names = []
        for info in infos:
            ext_counts[Path(info.filename).suffix.lower()] += 1
            names.append(info.filename)
        tags = {f"zip:{ext}:{count}" for ext, count in ext_counts.items() if ext}
        notes = [
            "Zip entries: "
            + ", ".join(f"{ext or 'noext'}:{count}" for ext, count in sorted(ext_counts.items(), key=lambda kv: kv[0]))
        ]
        category: Optional[str] = None
        confidence = 0.5
        supported: set[str] = set()
        signals: List[str] = []
        score = 0

        if ext_counts and set(ext_counts) <= {".package"}:
            category = "CAS"
            confidence = 0.75
            supported.add("CAS")
            cas_tokens = {
                token
                for name in names
                for token in _tokenise(name)
                if token in NameHeuristics.cas_tokens()
            }
            if cas_tokens:
                notes.append("CAS-like package names: " + ", ".join(sorted(cas_tokens)))
                signals.extend(f"header:zip:{token}" for token in sorted(cas_tokens))
            score = 3
            signals.append("header:zip:package")
        elif any(ext in SCRIPT_EXTS for ext in ext_counts):
            category = "Script Mod"
            confidence = 0.75
            supported.add("Script Mod")
            score = 4
            signals.append("header:zip:script")
        elif all(ext in TEXT_FILE_EXTS for ext in ext_counts if ext):
            category = "Resources"
            confidence = 0.6
            score = 2
        else:
            supported.update({"CAS", "BuildBuy", "Script Mod"})

        sample_cfg = ctx.budgets.get("zip_sample", {})
        max_files = int(sample_cfg.get("max_files", 0)) if isinstance(sample_cfg, dict) else 0
        sample_notes: List[str] = []
        if max_files and infos:
            preferred_kb = int(sample_cfg.get("prefer_small_under_kb", 0))
            small_infos = sorted(
                infos,
                key=lambda info: (
                    0 if preferred_kb <= 0 else int(info.file_size <= preferred_kb * 1024),
                    info.file_size,
                ),
            )
            sampled = 0
            for info in small_infos:
                if sampled >= max_files:
                    break
                if info.file_size == 0 or (preferred_kb > 0 and info.file_size > preferred_kb * 1024):
                    continue
                try:
                    with ctx.semaphore:
                        with zipfile.ZipFile(path, "r") as archive:
                            data = archive.read(info.filename)
                    hits = ctx.aho.search(data)
                    for group, group_hits in hits.items():
                        if group_hits:
                            sample_notes.append(f"Sample hit {group}: {', '.join(sorted(group_hits))}")
                            supported.add(group.capitalize())
                            for hit in sorted(group_hits):
                                signals.append(f"header:zip:{group}:{hit}")
                    sampled += 1
                except Exception:
                    continue
        if sample_notes:
            notes.extend(sample_notes)

        return HeaderSignal(
            category=category,
            confidence=confidence,
            decisive=False,
            tags=tags,
            notes=notes,
            supported=supported,
            handler="zip",
            score=score,
            signals=tuple(dict.fromkeys(signals)),
        )


class Ts4ScriptProbe:
    KNOWN_FAMILIES = {
        "mccc": "Script Mod:MCCC",
        "mc_command_center": "Script Mod:MCCC",
        "wickedwhims": "Adult:WickedWhims",
        "deviousdesires": "Adult:DeviousDesires",
    }

    @classmethod
    def inspect(cls, path: Path) -> HeaderSignal:
        modules: set[str] = set()
        try:
            with zipfile.ZipFile(path, "r") as archive:
                for name in archive.namelist():
                    if name.endswith("/__init__.pyc"):
                        module = name.split("/")[0]
                        modules.add(module.lower())
                    elif name.endswith(".pyc"):
                        module = name.split(".pyc")[0].split("/")[-1]
                        modules.add(module.lower())
        except Exception as exc:
            _debug_log(f"TS4Script probe exception for {path.name}: {exc}")
            return HeaderSignal(
                category="Script Mod",
                confidence=0.5,
                decisive=False,
                notes=[f"ts4script error: {exc}"],
                handler="ts4script",
                score=0,
                signals=tuple(),
            )

        family: Optional[str] = None
        for module in modules:
            bias = cls.KNOWN_FAMILIES.get(module)
            if bias:
                if ":" in bias:
                    base, fam = bias.split(":", 1)
                    return HeaderSignal(
                        category=base,
                        confidence=0.95,
                        decisive=True,
                        tags={"module:" + module},
                        notes=["Modules: " + ", ".join(sorted(modules))],
                        family=fam,
                        handler="ts4script",
                        score=12,
                        signals=tuple(f"module:{mod}" for mod in sorted(modules)),
                        decisive_type=f"module:{module}",
                    )
                family = bias
        score = 8 if modules else 4
        return HeaderSignal(
            category="Script Mod",
            confidence=0.8 if modules else 0.6,
            decisive=bool(modules),
            tags={"module:" + mod for mod in modules},
            notes=["Modules: " + ", ".join(sorted(modules))] if modules else ["No python modules"],
            family=family,
            handler="ts4script",
            score=score,
            signals=tuple(f"module:{mod}" for mod in sorted(modules)),
            decisive_type=None,
        )


class ContentPeek:
    @staticmethod
    def sample(
        path: Path,
        budgets: Dict[str, object],
        automaton: KeywordAutomaton,
        features: FeatureFlags,
    ) -> Optional[PeekSignal]:
        ext, _ = _effective_extension(path)
        default_budget = int(budgets.get(ext, 0) or 0)
        if ext == ".package":
            max_bytes = max(int(features.peek_budget_bytes), default_budget)
        else:
            max_bytes = default_budget
        if max_bytes <= 0:
            return None
        try:
            size = path.stat().st_size
            with path.open("rb") as fh:
                if size <= max_bytes:
                    data = fh.read(max_bytes)
                else:
                    midpoint = size // 2
                    start = max(0, midpoint - max_bytes // 2)
                    fh.seek(start)
                    data = fh.read(max_bytes)
        except Exception:
            return None
        hits = automaton.search(data)
        if not any(hits.values()):
            return None
        return PeekSignal(hits=hits)


class Deduper:
    HEAD_SIZE = 64 * 1024

    @classmethod
    def make_short_fingerprint(cls, path: Path, st: os.stat_result) -> str:
        size = int(st.st_size)
        head = b""
        tail = b""
        try:
            with path.open("rb") as fh:
                head = fh.read(cls.HEAD_SIZE)
                if size > cls.HEAD_SIZE:
                    fh.seek(max(size - cls.HEAD_SIZE, 0))
                    tail = fh.read(cls.HEAD_SIZE)
        except Exception:
            head = b""
            tail = b""
        head_crc = zlib.crc32(head) & 0xFFFFFFFF
        tail_crc = zlib.crc32(tail) & 0xFFFFFFFF
        return f"{size}:{head_crc:08x}:{tail_crc:08x}"



def should_escalate(name_sig: NameSignal, head_sig: HeaderSignal, thresholds: Dict[str, float]) -> bool:
    decisive_stop = bool(thresholds.get("decisive_stop", True))
    if decisive_stop and head_sig.decisive:
        return False
    name_conf_stop = float(thresholds.get("name_conf_stop", 0.7))
    if name_sig.confidence >= name_conf_stop and head_sig.supports(name_sig.category):
        return False
    adult_risk_low_skip = bool(thresholds.get("adult_risk_low_skip", False))
    if adult_risk_low_skip and name_sig.adult_risk == "low" and head_sig.category == "CAS":
        return False
    high_conf = float(thresholds.get("high_conf", 0.8))
    return min(name_sig.confidence, head_sig.confidence or 0.0) < high_conf


class Classifier:
    @staticmethod
    def merge(
        path: Path,
        st: os.stat_result,
        ext: str,
        disabled: bool,
        name_sig: NameSignal,
        head_sig: HeaderSignal,
        peek_sig: Optional[PeekSignal],
        features: FeatureFlags,
        thresholds: Dict[str, float],
    ) -> ScanFinding:
        notes: List[str] = []
        tags: set[str] = set()
        tags.update(name_sig.tags)
        tags.update(head_sig.tags)
        notes.extend(name_sig.notes)
        notes.extend(head_sig.notes)

        score_board: Dict[str, int] = {}
        signals: List[str] = []
        adult_hits: set[str] = set()

        for category, value in name_sig.score_map.items():
            if category:
                score_board[category] = score_board.get(category, 0) + max(0, value)
        signals.extend(name_sig.signals)

        if head_sig.category:
            score_board[head_sig.category] = score_board.get(head_sig.category, 0) + max(0, head_sig.score)
        for supported in head_sig.supported:
            score_board.setdefault(supported, 0)
        signals.extend(head_sig.signals)

        if peek_sig:
            for group, hits in peek_sig.hits.items():
                if not hits:
                    continue
                tags.add(f"peek:{group}")
                note_hits = ", ".join(sorted(hits))
                notes.append(f"Content hits {group}: {note_hits}")
                if group == "adult":
                    adult_hits.update(hits)
                category_map = {
                    "adult": "Adult",
                    "script": "Script Mod",
                    "cas": "CAS",
                    "buildbuy": "BuildBuy",
                    "pose": "Pose or Animation",
                    "tuning": "Tuning",
                }
                mapped = category_map.get(group)
                if mapped:
                    score_board[mapped] = score_board.get(mapped, 0) + 2 * len(hits)
                for hit in sorted(hits):
                    signals.append(f"peek:{group}:{hit}")

        if not score_board:
            score_board["Unknown"] = 0

        base_category = name_sig.category or "Unknown"
        if head_sig.category:
            base_category = head_sig.category

        if head_sig.decisive and head_sig.category:
            best_category = head_sig.category
            best_score = max(score_board.get(best_category, 0), head_sig.score)
        else:
            best_category, best_score = max(
                score_board.items(),
                key=lambda kv: (kv[1], -CATEGORY_INDEX.get(kv[0].split(":", 1)[0], len(CATEGORY_ORDER))),
            )
            tied = [cat for cat, score in score_board.items() if score == best_score]
            if len(tied) > 1:
                supported = [cat for cat in tied if head_sig.supports(cat)]
                if supported:
                    tied = supported
                elif head_sig.category in tied:
                    tied = [head_sig.category]
                if len(tied) > 1 and {"BuildBuy", "CAS"} <= set(tied):
                    build_has_objd = any(
                        signal.startswith("header:COBJ/OBJD")
                        or signal.startswith("header:SLOT")
                        or signal.startswith("header:FTPT")
                        for signal in head_sig.signals
                    )
                    if build_has_objd:
                        tied = ["BuildBuy"]
                    else:
                        tied = ["CAS"]
                if len(tied) > 1:
                    tied.sort(key=lambda cat: CATEGORY_INDEX.get(cat.split(":", 1)[0], len(CATEGORY_ORDER)))
                best_category = tied[0]
                best_score = score_board.get(best_category, best_score)

        if (
            features.treat_package_as_nonfinal
            and best_category in {"Package", "Unknown"}
            and name_sig.score_map
            and any(cat not in {"Package", "Unknown"} for cat in name_sig.score_map)
        ):
            alternatives = sorted(
                ((cat, score) for cat, score in score_board.items() if cat not in {"Package", "Unknown"}),
                key=lambda kv: (kv[1], -CATEGORY_INDEX.get(kv[0].split(":", 1)[0], len(CATEGORY_ORDER))),
                reverse=True,
            )
            for candidate, value in alternatives:
                if value >= 5:
                    best_category = candidate
                    best_score = value
                    break

        family = head_sig.family or name_sig.family
        adult_flag = name_sig.adult_risk == "high" or bool(adult_hits)
        if head_sig.category == "Adult":
            adult_flag = True

        category = best_category
        if adult_flag and not category.startswith("Adult"):
            base = category
            category = "Adult" if not base else f"Adult:{base}"
            signals.append("adult:promotion")

        if family and not category.startswith("Adult"):
            category = f"{best_category}:{family}" if best_category else family
        elif family and category.startswith("Adult") and ":" not in category:
            category = f"Adult:{family}"

        if adult_hits:
            tags.update(f"adult:{hit}" for hit in adult_hits)

        combined_confidence = max(
            name_sig.confidence,
            head_sig.confidence,
            0.4 + 0.05 * max(0, best_score),
        )
        if head_sig.decisive:
            combined_confidence = max(combined_confidence, float(thresholds.get("high_conf", 0.8)))
        if adult_flag:
            combined_confidence = max(combined_confidence, 0.75 if adult_hits else 0.7)
        combined_confidence = min(0.99, combined_confidence)

        notes_text = "; ".join(_shorten_note(note) for note in notes if note)
        needs_enrich = combined_confidence < float(thresholds.get("high_conf", 0.8))
        extras: Dict[str, str] = {}
        if adult_hits:
            extras["adult_hits"] = ", ".join(sorted(adult_hits))
        extras["confidence_score"] = str(best_score)
        extras["signals"] = ", ".join(dict.fromkeys(signals))

        finding = ScanFinding(
            path=path,
            ext=ext,
            size=int(st.st_size),
            category=category or "Unknown",
            confidence=combined_confidence,
            notes=notes_text,
            tags=tuple(sorted(tags)),
            target=UNKNOWN_DEFAULT_FOLDER,
            needs_enrich=needs_enrich,
            disabled=disabled,
            extras=extras,
            handler=head_sig.handler,
            confidence_score=best_score,
            signals=tuple(dict.fromkeys(signals)),
            decisive_header=head_sig.decisive,
        )
        return finding


class Router:
    @staticmethod
    def apply(finding: ScanFinding, ctx: ScanContext) -> ScanFinding:
        rules = ctx.rules
        features = ctx.features
        folder_map = ctx.folder_map
        routing = dict(rules.get("routing", {})) if features.use_legacy_routing else {}
        category = finding.category
        base = category.split(":", 1)[0] if category else "Unknown"
        dest = routing.get(base)
        if not dest:
            dest = folder_map.get(base, folder_map.get("Unknown", UNKNOWN_DEFAULT_FOLDER))
        if ":" in category:
            fam = category.split(":", 1)[1]
            if fam:
                dest = f"{dest}{'' if dest.endswith('/') else '/'}{fam}"
        if finding.disabled:
            if not dest.endswith("/"):
                dest = dest + "/"
            dest = f"{dest}Disabled/"
        finding.target = dest
        return finding


class Scheduler:
    def __init__(self, ctx: ScanContext) -> None:
        self._ctx = ctx

    def iter_files(self, root: Path, recurse: bool) -> Iterator[Path]:
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

    def scan(
        self,
        paths: Sequence[Path],
        progress_cb: Optional[ProgressCallback],
    ) -> Tuple[List[ScanFinding], List[str]]:
        total = len(paths)
        results: List[Optional[ScanFinding]] = [None] * total
        errors: List[str] = []
        groups: Dict[Path, List[Tuple[int, Path, os.stat_result]]] = {}
        for index, path in enumerate(paths):
            try:
                st = path.stat()
            except OSError as exc:
                errors.append(f"stat failed for {path.name}: {exc}")
                if progress_cb:
                    progress_cb(index + 1, total, path, "error")
                continue
            cached = self._ctx.cache.lookup(path, st.st_size, st.st_mtime)
            if cached:
                fingerprint = Deduper.make_short_fingerprint(path, st)
                _ensure_fingerprint_extra(cached, fingerprint)
                self._ctx.cache.upsert(cached, st, fingerprint)
                results[index] = cached
                self._ctx.metrics.record_cache_hit()
                if progress_cb:
                    progress_cb(index + 1, total, path, "cached")
                continue
            groups.setdefault(path.parent, []).append((index, path, st))

        futures: List[concurrent.futures.Future[Tuple[List[Tuple[int, ScanFinding]], List[str]]]] = []
        for entries in groups.values():
            future = self._ctx.pool.submit(self._scan_directory, entries, progress_cb, total)
            futures.append(future)

        for future in futures:
            try:
                group_results, group_errors = future.result()
                for index, finding in group_results:
                    results[index] = finding
                errors.extend(group_errors)
            except Exception as exc:
                errors.append(str(exc))

        final_results = [finding for finding in results if finding]
        return final_results, errors

    def _scan_directory(
        self,
        entries: List[Tuple[int, Path, os.stat_result]],
        progress_cb: Optional[ProgressCallback],
        total: int,
    ) -> Tuple[List[Tuple[int, ScanFinding]], List[str]]:
        local_results: List[Tuple[int, ScanFinding]] = []
        local_errors: List[str] = []
        for index, path, st in entries:
            start = time.perf_counter()
            try:
                finding = scan_light(path, st, self._ctx)
                duration = time.perf_counter() - start
                self._ctx.metrics.record_scan(duration, finding.decisive_header)
                local_results.append((index, finding))
                if progress_cb:
                    progress_cb(index + 1, total, path, "scanned")
            except Exception as exc:
                local_errors.append(f"scan failed for {path.name}: {exc}")
                if progress_cb:
                    progress_cb(index + 1, total, path, "error")
        return local_results, local_errors


def scan_light(path: Path, st: os.stat_result, ctx: ScanContext) -> ScanFinding:
    fingerprint = Deduper.make_short_fingerprint(path, st)
    cached = ctx.seen.get(fingerprint)
    if cached:
        ext, disabled = _effective_extension(path)
        extras = dict(cached.extras) if isinstance(cached.extras, dict) else {}
        finding = replace(
            cached,
            path=path,
            ext=ext,
            size=int(st.st_size),
            disabled=disabled,
            extras=extras,
        )
        _ensure_fingerprint_extra(finding, fingerprint)
        ctx.cache.upsert(finding, st, fingerprint)
        return finding

    name_sig = NameHeuristics.guess(path, ctx.rules)
    head_sig = HeaderProbe.run(path, ctx)

    ext, disabled = _effective_extension(path)
    preliminary = Classifier.merge(path, st, ext, disabled, name_sig, head_sig, None, ctx.features, ctx.thresholds)

    need_peek = False
    if ctx.features.fast_mode and not head_sig.decisive:
        base_category = (preliminary.category or "Unknown").split(":", 1)[0]
        if base_category in {"Package", "Unknown"}:
            need_peek = True
    if not need_peek and should_escalate(name_sig, head_sig, ctx.thresholds):
        need_peek = True

    peek_sig: Optional[PeekSignal] = None
    finding = preliminary
    if need_peek:
        peek_sig = ContentPeek.sample(path, ctx.budgets, ctx.aho, ctx.features)
        if peek_sig:
            finding = Classifier.merge(path, st, ext, disabled, name_sig, head_sig, peek_sig, ctx.features, ctx.thresholds)
    finding = Router.apply(finding, ctx)
    _ensure_fingerprint_extra(finding, fingerprint)

    ctx.cache.upsert(finding, st, fingerprint)
    return finding


# ---------------------------------------------------------------------------
# Public API compatibility wrappers
# ---------------------------------------------------------------------------


def guess_type_for_name(name: str, ext: str) -> Tuple[str, float, str, Tuple[str, ...]]:
    dummy_path = Path(name)
    signal = NameHeuristics.guess(dummy_path, load_rules(_rules_path()))
    return signal.category, signal.confidence, "; ".join(signal.notes), tuple(sorted(signal.tags))


def classify_from_types(
    types: Dict[int, int],
    filename: str,
    adult_hint: bool,
) -> Tuple[str, float, str, Tuple[str, ...]]:
    if not types:
        category = "Adult" if adult_hint else "Unknown"
        return category, 0.5, "No DBPF index", tuple()
    header_types = {TYPE_IDS.get(key, hex(key)) for key in types}
    notes = "DBPF types: " + ", ".join(
        f"{TYPE_IDS.get(key, hex(key))}:{count}" for key, count in sorted(types.items())
    )
    score_map, _, decisive, decisive_type = _score_dbpf_types(types)
    if not score_map:
        category = "Adult" if adult_hint else "Unknown"
        confidence = 0.6 if category != "Unknown" else 0.5
        return category, confidence, notes, tuple(sorted(header_types))
    category = max(
        score_map.items(),
        key=lambda kv: (kv[1], -CATEGORY_INDEX.get(kv[0].split(":", 1)[0], len(CATEGORY_ORDER))),
    )[0]
    score_value = score_map.get(category, 0)
    confidence = min(0.99, 0.55 + 0.04 * score_value)
    if decisive:
        confidence = max(confidence, 0.9)
        if decisive_type:
            notes += f"; decisive {decisive_type}"
    if adult_hint and not category.startswith("Adult"):
        category = f"Adult:{category}"
        confidence = max(confidence, 0.8)
    tags = tuple(sorted(header_types))
    return category, confidence, notes, tags


def map_type_to_folder(cat: str, folder_map: Dict[str, str]) -> str:
    base = cat.split(":", 1)[0] if cat else "Unknown"
    return folder_map.get(base, folder_map.get("Unknown", UNKNOWN_DEFAULT_FOLDER))


# Placeholder for removed adult scan behaviour

def refine_with_metadata(path: Path, current: Tuple[str, float, str, Tuple[str, ...]]) -> Tuple[str, float, str, Tuple[str, ...]]:
    return current


ProgressCallback = Callable[[int, int, Path, str], None]


def _rules_path() -> str:
    return str(Path(__file__).with_name("rules.json"))


def _budgets_path() -> str:
    return str(Path(__file__).with_name("budgets.json"))


def _thresholds_path() -> str:
    return str(Path(__file__).with_name("thresholds.json"))


def _keywords_path() -> str:
    return str(Path(__file__).with_name("keywords.json"))


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

    ctx = _build_context()
    if folder_map is not None:
        ctx.folder_map = dict(folder_map)
    files: List[Path] = []
    debug_discovery: Optional[Counter[str]] = Counter() if _SCAN_DEBUG else None
    results: List[ScanFinding]
    errors: List[str]
    try:
        scheduler = Scheduler(ctx)
        for path in scheduler.iter_files(root, recurse):
            name = path.name
            suffixes_lower = [suffix.lower() for suffix in path.suffixes]
            actual_ext, is_disabled_ext = _effective_extension(path)
            canonical_ext = actual_ext or (suffixes_lower[-1] if suffixes_lower else "")
            lowered_actual = name.lower()
            if allowed_exts_set is not None and canonical_ext not in allowed_exts_set:
                if progress_cb:
                    progress_cb(len(files) + 1, 0, path, "filtered")
                continue
            if ignore_exts_set and (
                canonical_ext in ignore_exts_set
                or any(suffix in ignore_exts_set for suffix in suffixes_lower)
            ):
                if progress_cb:
                    progress_cb(len(files) + 1, 0, path, "ignored")
                continue
            if ignore_names_tokens and any(token in lowered_actual for token in ignore_names_tokens):
                if progress_cb:
                    progress_cb(len(files) + 1, 0, path, "ignored")
                continue
            if selected_paths is not None:
                include_file = False
                relpath_obj = path.relative_to(root)
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
                        progress_cb(len(files) + 1, 0, path, "filtered")
                    continue
            files.append(path)
            if debug_discovery is not None:
                key = canonical_ext or "<noext>"
                debug_discovery[key] += 1

        results, errors = scheduler.scan(files, progress_cb)
        if debug_discovery is not None:
            summary = ", ".join(
                f"{ext}: {count}" for ext, count in sorted(debug_discovery.items(), key=lambda kv: kv[0])
            )
            _debug_log(f"Discovery counts -> {summary}")
            handler_counts = Counter((finding.handler or "unknown") for finding in results)
            handler_summary = ", ".join(
                f"{handler}: {count}" for handler, count in sorted(handler_counts.items(), key=lambda kv: kv[0])
            )
            _debug_log(f"Handler selection -> {handler_summary}")
    finally:
        ctx.pool.shutdown(wait=True)
        ctx.cache.close()
    items: List[FileItem] = []
    disabled_items: List[FileItem] = []

    for finding in results:
        relpath_obj = finding.path.relative_to(root)
        relpath = str(relpath_obj)
        ext = finding.ext
        size_mb = human_mb(finding.size)
        include = not finding.disabled
        if not include_adult and finding.category.startswith("Adult"):
            continue
        item = FileItem(
            path=finding.path,
            name=finding.path.name,
            ext=ext,
            size_mb=size_mb,
            relpath=relpath,
            guess_type=finding.category,
            confidence=finding.confidence,
            notes=finding.notes,
            include=include,
            target_folder=finding.target,
            meta_tags=", ".join(finding.tags),
            extras=dict(finding.extras),
            disabled=finding.disabled,
            original_ext="".join(suffix.lower() for suffix in finding.path.suffixes),
        )
        if finding.disabled:
            disabled_items.append(item)
        else:
            items.append(item)

    duplicate_groups: Dict[str, List[FileItem]] = {}
    for item in items:
        extras = item.extras if isinstance(item.extras, dict) else {}
        tooltips = item.tooltips if isinstance(item.tooltips, dict) else {}
        if extras:
            extras.pop(DUPLICATE_EXTRA_KEY, None)
        if tooltips:
            tooltips.pop(DUPLICATE_EXTRA_KEY, None)
        fingerprint = extras.get(FINGERPRINT_EXTRA_KEY, "") if extras else ""
        if fingerprint:
            duplicate_groups.setdefault(fingerprint, []).append(item)

    for group in duplicate_groups.values():
        if len(group) <= 1:
            continue
        primary = min(
            group,
            key=lambda entry: (entry.relpath.lower(), entry.name.lower()),
        )
        primary_tooltips = primary.tooltips if isinstance(primary.tooltips, dict) else {}
        if primary_tooltips is not primary.tooltips:
            primary.tooltips = dict(primary_tooltips)
            primary_tooltips = primary.tooltips
        if primary_tooltips is not None:
            primary_tooltips[DUPLICATE_EXTRA_KEY] = (
                f"Primary copy for {len(group) - 1} duplicate file(s)."
            )
        for candidate in group:
            if candidate is primary:
                continue
            extras = candidate.extras if isinstance(candidate.extras, dict) else {}
            if extras is not candidate.extras:
                candidate.extras = dict(extras)
                extras = candidate.extras
            extras[DUPLICATE_EXTRA_KEY] = "⚠"
            tooltips = candidate.tooltips if isinstance(candidate.tooltips, dict) else {}
            if tooltips is not candidate.tooltips:
                candidate.tooltips = dict(tooltips)
                tooltips = candidate.tooltips
            tooltips[DUPLICATE_EXTRA_KEY] = f"Duplicate of {primary.relpath}"

    summary_counts = Counter(finding.category.split(":", 1)[0] if finding.category else "Unknown" for finding in results)
    needs_review_prefix = ctx.folder_map.get("Unknown", UNKNOWN_DEFAULT_FOLDER).rstrip("/")
    needs_review_count = sum(
        1
        for finding in results
        if finding.target.rstrip("/").startswith(needs_review_prefix)
    )
    total_processed = ctx.metrics.files_scanned + ctx.metrics.cache_hits
    hit_rate = ctx.metrics.cache_hits / total_processed if total_processed else 0.0
    avg_ms = ctx.metrics.average_time_ms()
    print(
        "Scan summary: "
        + f"total={len(results)}, needs_review={needs_review_count}, "
        + f"cache_hit_rate={hit_rate:.0%}, avg_ms={avg_ms:.1f}"
    )
    if summary_counts:
        category_summary = ", ".join(
            f"{category}:{count}" for category, count in sorted(summary_counts.items(), key=lambda kv: kv[0])
        )
        print(f"Categories -> {category_summary}")

    if _SCAN_DEBUG and items:
        category_counts = Counter(item.guess_type for item in items)
        route_counts = Counter(item.target_folder for item in items)
        category_summary = ", ".join(
            f"{category}: {count}" for category, count in sorted(category_counts.items(), key=lambda kv: kv[0])
        )
        route_summary = ", ".join(
            f"{route}: {count}" for route, count in sorted(route_counts.items(), key=lambda kv: kv[0])
        )
        _debug_log(f"Classification categories -> {category_summary}")
        _debug_log(f"Routing destinations -> {route_summary}")

    items.sort(
        key=lambda item: (
            CATEGORY_INDEX.get(item.guess_type.split(":", 1)[0], len(CATEGORY_ORDER)),
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

    return ScanResult(items, len(files), errors, disabled_items)


def _build_context() -> ScanContext:
    base_dir = Path(__file__).resolve().parent
    rules = load_rules(str(base_dir / "rules.json"))
    budgets = load_budgets(str(base_dir / "budgets.json"))
    thresholds = load_thresholds(str(base_dir / "thresholds.json"))
    keywords = load_keywords(str(base_dir / "keywords.json"))
    features, rules_version = load_feature_flags(base_dir)

    path_bias_raw = rules.get("path_bias", [])
    path_bias_rules: List[PathBiasRule] = []
    if isinstance(path_bias_raw, list):
        for entry in path_bias_raw:
            if not isinstance(entry, dict):
                continue
            category = str(entry.get("category", "")).strip()
            score = int(entry.get("score", 0))
            patterns_raw = entry.get("patterns", [])
            patterns: Tuple[str, ...] = tuple(
                str(pattern).strip() for pattern in patterns_raw if isinstance(pattern, str) and pattern.strip()
            )
            if category and score and patterns:
                path_bias_rules.append(PathBiasRule(category=category, score=score, patterns=patterns))

    automaton_keywords: Dict[str, List[str]] = {}
    for key, values in keywords.items():
        if isinstance(values, list):
            automaton_keywords[key] = [str(value) for value in values if isinstance(value, str)]
    name_token_groups = keywords.get("name_tokens", {})
    if isinstance(name_token_groups, dict):
        for group, values in name_token_groups.items():
            if isinstance(values, list):
                automaton_keywords[str(group)] = [str(value) for value in values if isinstance(value, str)]

    automaton = KeywordAutomaton(automaton_keywords)

    folder_map_template = ROUTING_MAPS.get(features.routing_map, DEFAULT_FOLDER_MAP_V2)
    folder_map = {key: value for key, value in folder_map_template.items()}

    NameHeuristics.configure(keywords, tuple(path_bias_rules), features)

    cache_path = base_dir / features.cache_db
    cache = ScanCache(str(cache_path), rules_version)
    worker_count = max(1, (os.cpu_count() or 2) - 1)
    metrics = ScanMetrics()
    ctx = ScanContext(
        rules=rules,
        aho=automaton,
        cache=cache,
        seen=FingerprintIndex(cache),
        budgets=budgets,
        thresholds=thresholds,
        pool=concurrent.futures.ThreadPoolExecutor(max_workers=worker_count),
        semaphore=threading.Semaphore(worker_count),
        features=features,
        folder_map=folder_map,
        path_bias=tuple(path_bias_rules),
        metrics=metrics,
        rules_version=rules_version,
    )
    return ctx


# ---------------------------------------------------------------------------
# Legacy helpers retained for plugins/UI
# ---------------------------------------------------------------------------


def bundle_scripts_and_packages(items: Sequence[FileItem], folder_map: Dict[str, str]) -> Dict[str, int]:
    script_lookup: Dict[str, FileItem] = {}
    for item in items:
        if item.disabled:
            continue
        if item.ext in SCRIPT_EXTS and item.guess_type.startswith("Script Mod"):
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
        if item.target_folder == folder_map.get("Unknown", UNKNOWN_DEFAULT_FOLDER):
            item.target_folder = folder_map.get(script_item.guess_type.split(":", 1)[0], item.target_folder)
        linked += 1
    return {"linked": linked, "scripts": len(script_lookup)}


def dbpf_scan_types(path: Path, limit: int = 10) -> Dict[int, int]:
    result: Dict[int, int] = {}
    try:
        with path.open("rb") as fh:
            head = fh.read(96)
            if len(head) < 96 or head[:4] != b"DBPF":
                return result
            count = _u32(head, 0x20)
            index_pos = _u32(head, 0x40)
            if not count or not index_pos:
                return result
            fh.seek(index_pos)
            flags = int.from_bytes(fh.read(4), "little")
            flagged_slots = [idx for idx in range(8) if (flags >> idx) & 1]
            header_vals = [int.from_bytes(fh.read(4), "little") for _ in flagged_slots]
            per_entry = 8 - len(flagged_slots)
            inspected = 0
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
                inspected += 1
                if limit and inspected >= limit:
                    break
    except Exception as exc:
        _debug_log(f"DBPF probe exception for {path.name}: {exc}")
        return {}
    return result


def _u32(buf: bytes, offset: int) -> int:
    return int.from_bytes(buf[offset : offset + 4], "little", signed=False)


__all__ += ["SCRIPT_EXTS", "ARCHIVE_EXTS", "PACKAGE_EXTS", "TEXT_FILE_EXTS", "is_ts4script", "is_archive"]


def is_ts4script(path: Path) -> bool:
    return path.suffix.lower() in SCRIPT_EXTS


def is_archive(path: Path) -> bool:
    return path.suffix.lower() in ARCHIVE_EXTS


