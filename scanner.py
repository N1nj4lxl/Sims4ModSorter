from __future__ import annotations

import concurrent.futures
import json
import os
import re
import sqlite3
import threading
import zipfile
from collections import Counter
from dataclasses import dataclass, field
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
    "Adult",
    "CAS",
    "BuildBuy",
    "Tuning",
    "Mixed",
    "Resources",
    "Archive",
    "Other",
    "Unknown",
]

CATEGORY_INDEX: Dict[str, int] = {name: idx for idx, name in enumerate(CATEGORY_ORDER)}

DEFAULT_FOLDER_MAP: Dict[str, str] = {
    "Script Mod": "Sorted/ScriptMods",
    "Adult": "Sorted/Adult",
    "CAS": "Sorted/CAS",
    "BuildBuy": "Sorted/BuildBuy",
    "Tuning": "Sorted/Tuning",
    "Mixed": "Sorted/Mixed",
    "Resources": "Sorted/Resources",
    "Archive": "Sorted/Archives",
    "Other": "Sorted/Other",
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


DECISIVE_DBPF = {
    "CAS": {"CASP"},
    "BuildBuy": {"OBJD", "RSLT", "SLOT"},
    "Tuning": {"ITUN", "SITN", "CLIP"},
}


@dataclass(slots=True)
class NameSignal:
    category: str
    confidence: float
    adult_risk: str
    tags: set[str]
    notes: List[str]
    family: Optional[str] = None
    tokens: Tuple[str, ...] = field(default_factory=tuple)


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
            target=str(payload.get("target", "Unsorted")),
            needs_enrich=bool(payload.get("needs_enrich", False)),
            disabled=bool(payload.get("disabled", False)),
            extras={str(k): str(v) for k, v in dict(payload.get("extras", {})).items()},
            handler=str(payload.get("handler", "")),
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
    def __init__(self, db_path: str) -> None:
        self._path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS entries (
                    id INTEGER PRIMARY KEY,
                    path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    mtime REAL NOT NULL,
                    payload TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_entries_key
                ON entries(path, size, mtime)
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fingerprints (
                    fingerprint TEXT PRIMARY KEY,
                    entry_id INTEGER NOT NULL,
                    FOREIGN KEY(entry_id) REFERENCES entries(id) ON DELETE CASCADE
                )
                """
            )

    def lookup(self, path: Path, size: int, mtime: float) -> Optional[ScanFinding]:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT payload FROM entries WHERE path=? AND size=? AND mtime=?",
                (str(path), int(size), float(mtime)),
            ).fetchone()
        if not row:
            return None
        payload = json.loads(row[0])
        return ScanFinding.from_payload(payload)

    def get_by_id(self, entry_id: int) -> Optional[ScanFinding]:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT payload FROM entries WHERE id=?",
                (entry_id,),
            ).fetchone()
        if not row:
            return None
        payload = json.loads(row[0])
        return ScanFinding.from_payload(payload)

    def upsert(self, finding: ScanFinding, st: os.stat_result, fingerprint: Optional[str]) -> None:
        payload = json.dumps(finding.to_payload(), ensure_ascii=False)
        with self._lock, self._conn:
            cur = self._conn.execute(
                "SELECT id FROM entries WHERE path=? AND size=? AND mtime=?",
                (str(finding.path), int(st.st_size), float(st.st_mtime)),
            )
            row = cur.fetchone()
            if row:
                entry_id = row[0]
                self._conn.execute(
                    "UPDATE entries SET payload=? WHERE id=?",
                    (payload, entry_id),
                )
            else:
                cur = self._conn.execute(
                    "INSERT INTO entries(path, size, mtime, payload) VALUES(?,?,?,?)",
                    (str(finding.path), int(st.st_size), float(st.st_mtime), payload),
                )
                entry_id = cur.lastrowid
            if fingerprint:
                self._conn.execute(
                    "INSERT OR REPLACE INTO fingerprints(fingerprint, entry_id) VALUES(?, ?)",
                    (fingerprint, entry_id),
                )

    def lookup_fingerprint(self, fingerprint: str) -> Optional[ScanFinding]:
        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT entries.payload
                FROM fingerprints
                JOIN entries ON entries.id = fingerprints.entry_id
                WHERE fingerprints.fingerprint=?
                """,
                (fingerprint,),
            ).fetchone()
        if not row:
            return None
        payload = json.loads(row[0])
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


class NameHeuristics:
    CAS_TOKENS = {
        "hair",
        "hairstyle",
        "preset",
        "eyes",
        "skin",
        "skintone",
        "tattoo",
        "brow",
        "lip",
        "makeup",
        "recolor",
        "recolour",
        "overlay",
        "lashes",
    }
    BUILD_TOKENS = {
        "sofa",
        "clutter",
        "chair",
        "table",
        "bed",
        "counter",
        "window",
        "door",
        "wall",
        "floor",
        "lamp",
        "decor",
    }
    SCRIPT_TOKENS = {
        "script",
        "python",
        "mc_command_center",
        "mccc",
        "wickedwhims",
        "ts4script",
    }
    ADULT_TOKENS = {
        "wickedwhims",
        "deviousdesires",
        "basemental",
        "nsfw",
        "adult",
        "strip",
        "nude",
    }

    @classmethod
    def guess(cls, path: Path, rules: Dict[str, object]) -> NameSignal:
        name = path.name
        tokens = _tokenise(name)
        folder_tokens = _tokenise_path_parts(path.parent)
        ext, _ = _effective_extension(path)

        category = "Archive" if ext in ARCHIVE_EXTS else "Script Mod" if ext in SCRIPT_EXTS else "Package"
        confidence = 0.4 if category == "Package" else 0.5
        notes: List[str] = []
        tags: set[str] = set()
        adult_risk = "low"
        family: Optional[str] = None

        if cls.CAS_TOKENS & set(tokens):
            category = "CAS"
            confidence = max(confidence, 0.55)
            tags.add("name:cas")
        if cls.BUILD_TOKENS & set(tokens):
            category = "BuildBuy"
            confidence = max(confidence, 0.55)
            tags.add("name:buildbuy")
        if cls.SCRIPT_TOKENS & set(tokens):
            category = "Script Mod"
            confidence = max(confidence, 0.65)
            tags.add("name:script")
        if cls.ADULT_TOKENS & set(tokens):
            adult_risk = "high"
            tags.add("name:adult")
            notes.append("Adult keywords in name")
        elif cls.ADULT_TOKENS & set(folder_tokens):
            adult_risk = "medium"
            tags.add("folder:adult")

        author_rules = dict(rules.get("authors", {}))
        for token in tokens + folder_tokens:
            bias = author_rules.get(token)
            if not bias:
                continue
            bias_category = str(bias.get("bias", ""))
            if ":" in bias_category:
                base, fam = bias_category.split(":", 1)
                if base:
                    category = base
                family = fam or family
            elif bias_category:
                category = bias_category
            boost = float(bias.get("boost", 0.2))
            confidence = min(1.0, max(confidence, 0.5 + boost))
            tags.add(f"author:{token}")
            notes.append(f"Author bias: {token}")

        pack_rules = dict(rules.get("packs", {}))
        for token in tokens:
            bias = pack_rules.get(token)
            if not bias:
                continue
            bias_category = str(bias.get("bias", ""))
            if ":" in bias_category:
                base, fam = bias_category.split(":", 1)
                if base:
                    category = base
                family = fam or family
            elif bias_category:
                category = bias_category
            confidence = min(1.0, max(confidence, 0.6))
            tags.add(f"pack:{token}")
            notes.append(f"Pack bias: {token}")

        folder_rules = dict(rules.get("folders", {}))
        for token in folder_tokens:
            bias = folder_rules.get(token)
            if not bias:
                continue
            bias_category = str(bias.get("bias", ""))
            if ":" in bias_category:
                base, fam = bias_category.split(":", 1)
                if base:
                    category = base
                family = fam or family
            elif bias_category:
                category = bias_category
            boost = float(bias.get("boost", 0.0))
            confidence = min(1.0, confidence + boost)
            tags.add(f"folder:{token}")
            notes.append(f"Folder bias: {token}")

        category = category or "Unknown"
        notes.append(f"Tokens: {', '.join(tokens[:6])}" if tokens else "No tokens")

        return NameSignal(
            category=category,
            confidence=min(1.0, confidence),
            adult_risk=adult_risk,
            tags=tags,
            notes=notes,
            family=family,
            tokens=tokens,
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
            signal = HeaderSignal(category=None, confidence=0.0, decisive=False, handler="none")
        return signal


class DbpfProbe:
    @staticmethod
    def inspect(path: Path) -> HeaderSignal:
        types = dbpf_scan_types(path)
        if not types:
            return HeaderSignal(
                category=None,
                confidence=0.0,
                decisive=False,
                notes=["No DBPF index"],
                handler="dbpf",
            )
        tags = {TYPE_IDS.get(key, hex(key)) for key in types}
        notes = [
            "DBPF types: "
            + ", ".join(f"{TYPE_IDS.get(key, hex(key))}:{count}" for key, count in sorted(types.items()))
        ]
        for category, decisive_set in DECISIVE_DBPF.items():
            if decisive_set <= {TYPE_IDS.get(key, hex(key)).split("/")[0] for key in types}:
                return HeaderSignal(
                    category=category,
                    confidence=0.92,
                    decisive=True,
                    tags=tags,
                    notes=notes,
                    handler="dbpf",
                )
        supported = set()
        if any(TYPE_IDS.get(key, "").startswith("CASP") for key in types):
            supported.add("CAS")
        if 0x319E4F1D in types or 0x015A1849 in types:
            supported.add("BuildBuy")
        if 0x220557DA in types:
            supported.add("Tuning")
        return HeaderSignal(
            category=next(iter(supported), None),
            confidence=0.7 if supported else 0.4,
            decisive=False,
            tags=tags,
            notes=notes,
            supported=supported,
            handler="dbpf",
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

        if ext_counts and set(ext_counts) <= {".package"}:
            category = "CAS"
            confidence = 0.75
            supported.add("CAS")
            cas_tokens = {token for name in names for token in _tokenise(name) if token in NameHeuristics.CAS_TOKENS}
            if cas_tokens:
                notes.append("CAS-like package names: " + ", ".join(sorted(cas_tokens)))
        elif any(ext in SCRIPT_EXTS for ext in ext_counts):
            category = "Script Mod"
            confidence = 0.75
            supported.add("Script Mod")
        elif all(ext in TEXT_FILE_EXTS for ext in ext_counts if ext):
            category = "Resources"
            confidence = 0.6
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
                    )
                family = bias
        return HeaderSignal(
            category="Script Mod",
            confidence=0.8 if modules else 0.6,
            decisive=bool(modules),
            tags={"module:" + mod for mod in modules},
            notes=["Modules: " + ", ".join(sorted(modules))] if modules else ["No python modules"],
            family=family,
            handler="ts4script",
        )


class ContentPeek:
    @staticmethod
    def sample(path: Path, budgets: Dict[str, object], automaton: KeywordAutomaton) -> Optional[PeekSignal]:
        ext, _ = _effective_extension(path)
        budget = budgets.get(ext, 0)
        if not budget:
            return None
        max_bytes = int(budget)
        if max_bytes <= 0:
            return None
        try:
            with path.open("rb") as fh:
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
        thresholds: Dict[str, float],
    ) -> ScanFinding:
        notes: List[str] = []
        tags: set[str] = set()
        tags.update(name_sig.tags)
        tags.update(head_sig.tags)
        notes.extend(name_sig.notes)
        notes.extend(head_sig.notes)

        adult_hits: set[str] = set()
        if peek_sig:
            for group, hits in peek_sig.hits.items():
                if hits:
                    tags.add(f"peek:{group}")
                    if group == "adult":
                        adult_hits.update(hits)
                    notes.append(f"Content hits {group}: {', '.join(sorted(hits))}")

        base_category = head_sig.category or name_sig.category or "Unknown"
        confidence = max(name_sig.confidence, head_sig.confidence)
        family = head_sig.family or name_sig.family

        adult_flag = name_sig.adult_risk == "high" or bool(adult_hits)
        if base_category == "Adult":
            adult_flag = True

        if adult_flag:
            base = "Adult"
            if family:
                category = f"Adult:{family}"
            else:
                category = "Adult"
            confidence = max(confidence, 0.85 if adult_hits else 0.75)
            if adult_hits:
                tags.update(f"adult:{hit}" for hit in adult_hits)
        else:
            base = base_category
            if family:
                category = f"{base}:{family}"
            else:
                category = base

        if head_sig.decisive:
            confidence = max(confidence, float(thresholds.get("high_conf", 0.8)))

        needs_enrich = confidence < float(thresholds.get("high_conf", 0.8))
        notes_text = "; ".join(note for note in notes if note)
        target = "Unsorted"

        finding = ScanFinding(
            path=path,
            ext=ext,
            size=int(st.st_size),
            category=category or "Unknown",
            confidence=min(1.0, confidence),
            notes=notes_text,
            tags=tuple(sorted(tags)),
            target=target,
            needs_enrich=needs_enrich,
            disabled=disabled,
            extras={"adult_hits": ", ".join(sorted(adult_hits))} if adult_hits else {},
            handler=head_sig.handler,
        )
        return finding


class Router:
    @staticmethod
    def apply(finding: ScanFinding, rules: Dict[str, object]) -> ScanFinding:
        routing = dict(rules.get("routing", {}))
        category = finding.category
        base = category.split(":", 1)[0] if category else "Unknown"
        dest = routing.get(base, DEFAULT_FOLDER_MAP.get(base, "Unsorted"))
        if ":" in category:
            fam = category.split(":", 1)[1]
            if fam:
                dest = f"{dest}/{fam}"
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
        results: List[Optional[ScanFinding]] = [None] * len(paths)
        errors: List[str] = []
        futures: List[Tuple[int, Path, os.stat_result, concurrent.futures.Future[ScanFinding]]] = []
        for index, path in enumerate(paths):
            try:
                st = path.stat()
            except OSError as exc:
                errors.append(f"stat failed for {path.name}: {exc}")
                if progress_cb:
                    progress_cb(index + 1, len(paths), path, "error")
                continue
            cached = self._ctx.cache.lookup(path, st.st_size, st.st_mtime)
            if cached:
                fingerprint = Deduper.make_short_fingerprint(path, st)
                _ensure_fingerprint_extra(cached, fingerprint)
                self._ctx.cache.upsert(cached, st, fingerprint)
                results[index] = cached
                if progress_cb:
                    progress_cb(index + 1, len(paths), path, "cached")
                continue
            future = self._ctx.pool.submit(scan_light, path, st, self._ctx)
            futures.append((index, path, st, future))

        for index, path, st, future in futures:
            try:
                finding = future.result()
                results[index] = finding
            except Exception as exc:
                errors.append(f"scan failed for {path.name}: {exc}")
                results[index] = None
            if progress_cb:
                progress_cb(index + 1, len(paths), path, "scanned")

        final_results = [finding for finding in results if finding]
        return final_results, errors


def scan_light(path: Path, st: os.stat_result, ctx: ScanContext) -> ScanFinding:
    fingerprint = Deduper.make_short_fingerprint(path, st)
    cached = ctx.seen.get(fingerprint)
    if cached:
        _ensure_fingerprint_extra(cached, fingerprint)
        ctx.cache.upsert(cached, st, fingerprint)
        return cached

    name_sig = NameHeuristics.guess(path, ctx.rules)
    head_sig = HeaderProbe.run(path, ctx)

    if should_escalate(name_sig, head_sig, ctx.thresholds):
        peek_sig = ContentPeek.sample(path, ctx.budgets, ctx.aho)
    else:
        peek_sig = None

    ext, disabled = _effective_extension(path)
    finding = Classifier.merge(path, st, ext, disabled, name_sig, head_sig, peek_sig, ctx.thresholds)
    finding = Router.apply(finding, ctx.rules)
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
    category: Optional[str] = None
    for target, decisive_set in DECISIVE_DBPF.items():
        if decisive_set <= {name.split("/")[0] for name in header_types}:
            category = target
            break
    if not category:
        if any(name.startswith("CASP") for name in header_types):
            category = "CAS"
        elif "COBJ/OBJD" in header_types or "GEOM" in header_types:
            category = "BuildBuy"
        elif "STBL" in header_types:
            category = "Tuning"
        else:
            category = "Other"
    confidence = 0.9 if category in DECISIVE_DBPF else 0.7
    if adult_hint and category != "Adult":
        category = f"Adult:{category}"
        confidence = max(confidence, 0.8)
    tags = tuple(sorted(header_types))
    return category, confidence, notes, tags


def map_type_to_folder(cat: str, folder_map: Dict[str, str]) -> str:
    base = cat.split(":", 1)[0] if cat else "Unknown"
    return folder_map.get(base, folder_map.get("Unknown", "Unsorted"))


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

    ctx = _build_context()
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
    automaton = KeywordAutomaton(keywords)
    cache = ScanCache(str(base_dir / "scan_cache.db"))
    ctx = ScanContext(
        rules=rules,
        aho=automaton,
        cache=cache,
        seen=FingerprintIndex(cache),
        budgets=budgets,
        thresholds=thresholds,
        pool=concurrent.futures.ThreadPoolExecutor(max_workers=8),
        semaphore=threading.Semaphore(8),
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
        if item.target_folder == folder_map.get("Unknown", "Unsorted"):
            item.target_folder = folder_map.get(script_item.guess_type.split(":", 1)[0], item.target_folder)
        linked += 1
    return {"linked": linked, "scripts": len(script_lookup)}


def dbpf_scan_types(path: Path) -> Dict[int, int]:
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


