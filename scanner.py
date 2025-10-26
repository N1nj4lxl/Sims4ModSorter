from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

FINGERPRINT_EXTRA_KEY = "fingerprint"
DUPLICATE_EXTRA_KEY = "duplicate"
UNKNOWN_DEFAULT_FOLDER = "Mods/NeedsReview/"

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
    "Disabled",
]
CATEGORY_INDEX: Dict[str, int] = {name: index for index, name in enumerate(CATEGORY_ORDER)}

INCLUDED_EXTENSIONS: List[str] = [
    ".ts4script",
    ".package",
    ".zip",
    ".rar",
    ".7z",
    ".txt",
    ".cfg",
    ".ini",
    ".log",
    ".rtf",
    ".png",
    ".jpg",
    ".jpeg",
]
SOFT_ROUTE_NAMES: List[str] = ["__MACOSX", ".DS_Store", "Thumbs.db"]
DISABLED_SUFFIXES: List[str] = ["off", "disabled", "bak"]
SOFT_ROUTE_NAME_SET = {name.lower() for name in SOFT_ROUTE_NAMES}

PACKAGE_EXTS = {".package"}
SCRIPT_EXTS = {".ts4script"}
ARCHIVE_EXTS = {".zip", ".rar", ".7z"}
TEXT_FILE_EXTS = {".txt", ".cfg", ".ini", ".log", ".rtf"}
IMAGE_FILE_EXTS = {".png", ".jpg", ".jpeg"}
RESOURCE_EXTS = {".json"}
SUPPORTED_EXTS = set(INCLUDED_EXTENSIONS) | RESOURCE_EXTS
SUPPORTED_DISABLED_SUFFIXES = {f".{suffix}" for suffix in DISABLED_SUFFIXES}

TOKEN_RE = re.compile(r"[a-z0-9]+")
NORMALISE_KEY_RE = re.compile(r"[^a-z0-9]+")
NAT_SORT_RE = re.compile(r"(\d+)")


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
    metrics: "Optional[ScanMetrics]" = None


@dataclass(slots=True)
class ScanMetrics:
    total_files: int
    elapsed_ms: float
    avg_ms_per_file: float
    decisive_headers: int
    category_counts: Dict[str, int]


@dataclass(slots=True)
class ClassificationDecision:
    category: str = "Unknown"
    confidence: float = 0.0
    reason: str = ""


@dataclass(slots=True)
class PipelineEntry:
    path: Path
    ext: str
    disabled: bool
    original_ext: str
    size_bytes: int
    tokens: Tuple[str, ...]
    path_tokens: Tuple[str, ...]
    base_name: str
    normalized_key: str
    decision: ClassificationDecision = field(default_factory=ClassificationDecision)
    header_types: Optional[Dict[int, int]] = None
    soft_routed: bool = False
    decided_by_header: bool = False
    link_target: str = ""


ProgressCallback = Callable[[int, int, Path, str], None]

__all__ = [
    "ARCHIVE_EXTS",
    "CATEGORY_INDEX",
    "CATEGORY_ORDER",
    "DEFAULT_FOLDER_MAP",
    "DUPLICATE_EXTRA_KEY",
    "FINGERPRINT_EXTRA_KEY",
    "FileItem",
    "ProgressCallback",
    "ScanMetrics",
    "ScanResult",
    "SUPPORTED_EXTS",
    "TEXT_FILE_EXTS",
    "bundle_scripts_and_packages",
    "classify_by_header",
    "classify_from_types",
    "dbpf_scan_types",
    "get_default_mods_path",
    "guess_type_for_name",
    "human_mb",
    "normalize_extension",
    "normalize_key",
    "pretty_display_name",
    "read_package_manifest",
    "route_category",
    "run_classification_pipeline",
    "scan_file",
    "scan_folder",
]


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


def normalize_key(value: str) -> str:
    lowered = value.lower()
    collapsed = NORMALISE_KEY_RE.sub("-", lowered)
    return collapsed.strip("-")


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


def _natural_key(value: str) -> Tuple[object, ...]:
    parts = NAT_SORT_RE.split(value.lower())
    out: List[object] = []
    for part in parts:
        out.append(int(part) if part.isdigit() else part)
    return tuple(out)


def _load_json(path: Path) -> Dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return {}


def _module_dir() -> Path:
    return Path(__file__).resolve().parent


def _rules_path() -> Path:
    return _module_dir() / "rules.json"


def _budgets_path() -> Path:
    return _module_dir() / "budgets.json"


def _routing_path() -> Path:
    return _module_dir() / "routing.json"


def _keywords_path() -> Path:
    return _module_dir() / "keywords.json"


def load_rules() -> Dict[str, object]:
    return _load_json(_rules_path())


def load_budgets() -> Dict[str, object]:
    return _load_json(_budgets_path())


def load_routing() -> Dict[str, str]:
    data = _load_json(_routing_path())
    routing = {str(key): str(value) for key, value in data.items()}
    if not routing:
        routing = {
            "Script Mod": "Mods/Scripts/",
            "Adult": "Mods/Adult/",
            "CAS": "Mods/CAS/",
            "BuildBuy": "Mods/Build Mode/",
            "Pose or Animation": "Mods/Animations/",
            "Tuning": "Mods/Gameplay/",
            "Mixed": UNKNOWN_DEFAULT_FOLDER,
            "Resources": UNKNOWN_DEFAULT_FOLDER,
            "Archive": UNKNOWN_DEFAULT_FOLDER,
            "Other": UNKNOWN_DEFAULT_FOLDER,
            "Unknown": UNKNOWN_DEFAULT_FOLDER,
            "Disabled": UNKNOWN_DEFAULT_FOLDER,
        }
    return routing


def load_keywords() -> Dict[str, object]:
    return _load_json(_keywords_path())


DEFAULT_FOLDER_MAP = load_routing()

_KEYWORDS_DATA = load_keywords()


def _prepare_keyword_tokens(values: Iterable[object]) -> Tuple[str, ...]:
    tokens: List[str] = []
    for value in values or []:
        text = str(value).strip().lower()
        if not text:
            continue
        collapsed = re.sub(r"[^a-z0-9]+", "", text)
        if collapsed:
            tokens.append(collapsed)
        tokens.extend(TOKEN_RE.findall(text))
    seen: Dict[str, None] = {}
    ordered: List[str] = []
    for token in tokens:
        if token and token not in seen:
            seen[token] = None
            ordered.append(token)
    return tuple(ordered)


def _merge_tokens(*token_sets: Iterable[str]) -> Tuple[str, ...]:
    seen: Dict[str, None] = {}
    merged: List[str] = []
    for token_set in token_sets:
        for token in token_set:
            if token and token not in seen:
                seen[token] = None
                merged.append(token)
    return tuple(merged)


def _resolve_name_tokens(category: str) -> Tuple[str, ...]:
    data_tokens: List[object] = []
    name_section = _KEYWORDS_DATA.get("name_tokens")
    if isinstance(name_section, dict):
        key_map = {
            "Script Mod": "script",
            "CAS": "cas",
            "BuildBuy": "buildbuy",
            "Pose or Animation": "pose",
            "Tuning": "tuning",
        }
        mapped_key = key_map.get(category)
        if mapped_key:
            maybe_tokens = name_section.get(mapped_key, [])
            if isinstance(maybe_tokens, list):
                data_tokens.extend(maybe_tokens)
        if category != "Script Mod":
            generic_tokens = name_section.get("generic", [])
            if isinstance(generic_tokens, list):
                data_tokens.extend(generic_tokens)
    if category == "Script Mod":
        script_tokens = _KEYWORDS_DATA.get("script", [])
        if isinstance(script_tokens, list):
            data_tokens.extend(script_tokens)
    return _prepare_keyword_tokens(data_tokens)


NAME_KEYWORD_BUCKETS: Tuple[Tuple[str, Tuple[str, ...]], ...] = tuple(
    (category, _resolve_name_tokens(category))
    for category in (
        "Script Mod",
        "CAS",
        "BuildBuy",
        "Pose or Animation",
        "Tuning",
    )
)


NAME_PRIORITY: Tuple[str, ...] = (
    "Script Mod",
    "CAS",
    "BuildBuy",
    "Pose or Animation",
    "Tuning",
)


NAME_REASON_MAP = {
    "Script Mod": "name:script",
    "CAS": "name:cas",
    "BuildBuy": "name:buildbuy",
    "Pose or Animation": "name:pose",
    "Tuning": "name:tuning",
}


def _resolve_script_ids() -> set[str]:
    script_tokens = _KEYWORDS_DATA.get("script", [])
    normalized: set[str] = set()
    if isinstance(script_tokens, list):
        for token in script_tokens:
            norm = normalize_key(str(token))
            if norm:
                normalized.add(norm)
    return normalized


SCRIPT_NORMALIZED_IDS = _resolve_script_ids()


def _resolve_adult_strong_keywords() -> set[str]:
    strong_keywords = _KEYWORDS_DATA.get("adult_strong", [])
    if not isinstance(strong_keywords, list):
        return set()
    return set(_prepare_keyword_tokens(strong_keywords))


ADULT_STRONG_KEYWORDS = _resolve_adult_strong_keywords()


def normalize_extension(ext: str) -> Tuple[str, bool]:
    lowered = ext.lower()
    disabled = False
    for suffix in SUPPORTED_DISABLED_SUFFIXES:
        if lowered.endswith(suffix):
            lowered = lowered[: -len(suffix)]
            disabled = True
            break
    if lowered and not lowered.startswith("."):
        lowered = f".{lowered}"
    return lowered, disabled


def _effective_extension(path: Path) -> Tuple[str, bool]:
    suffixes = [suffix.lower() for suffix in path.suffixes]
    disabled = False
    ext = ""
    for suffix in reversed(suffixes):
        if suffix in SUPPORTED_DISABLED_SUFFIXES:
            disabled = True
            continue
        ext = suffix
        break
    return ext, disabled


def _tokenise(text: str) -> Tuple[str, ...]:
    normalised = text.replace("_", " ").replace("-", " ")
    return tuple(TOKEN_RE.findall(normalised.lower()))


def _tokenise_path(path: Path, depth: int = 3) -> Tuple[str, ...]:
    parts = list(path.parts)[-depth:]
    tokens: List[str] = []
    for part in parts:
        tokens.extend(_tokenise(part))
    return tuple(tokens)


def read_package_manifest(path: Path, limit: int) -> Dict[int, int]:
    try:
        entry_limit = 0
        if limit:
            entry_limit = max(1, limit // 32)
        return dbpf_scan_types(path, limit=entry_limit)
    except Exception:
        return {}


def classify_by_header(types: Dict[int, int]) -> Tuple[str, float, str]:
    if not types:
        return "", 0.0, ""
    cas_ids = {0x034AEECB, 0x067CAA11, 0x2F7D0004}
    buildbuy_ids = {0x319E4F1D, 0xCD0F1220, 0x160D0E6A, 0x0AE3FDE5}
    pose_ids = {0x6B20C4F3, 0xE882D22F}
    tuning_ids = {0x03B33DDF, 0xD1F577C6}

    has_cas = any(type_id in types for type_id in cas_ids)
    has_bb = any(type_id in types for type_id in buildbuy_ids)
    if has_cas and has_bb:
        return "Mixed", 1.0, "header:mixed"
    if has_cas:
        return "CAS", 1.0, "header:CASP"
    if has_bb:
        return "BuildBuy", 1.0, "header:OBJD"
    if any(type_id in types for type_id in pose_ids):
        return "Pose or Animation", 0.9, "header:CLIP"
    if any(type_id in types for type_id in tuning_ids):
        return "Tuning", 0.9, "header:ITUN"
    if 0x220557DA in types:
        return "", 0.0, "header:nondet"
    return "", 0.0, ""


def route_category(category: str, routing_map: Dict[str, str]) -> str:
    target = routing_map.get(category)
    if not target:
        target = routing_map.get("Unknown", UNKNOWN_DEFAULT_FOLDER)
    if not target.endswith("/"):
        target = target + "/"
    return target


def _build_reason(reason: str) -> Dict[str, str]:
    if not reason:
        return {}
    return {"reason": reason}


def _is_text_sidecar(ext: str) -> bool:
    return ext in TEXT_FILE_EXTS


def _is_image(ext: str) -> bool:
    return ext in IMAGE_FILE_EXTS


def _is_resource_ext(ext: str) -> bool:
    return ext in RESOURCE_EXTS



def scan_file(
    path: Path,
    rules: Dict[str, object],
    routing: Dict[str, str],
    budgets: Dict[str, object],
) -> FileItem:
    items, _metrics = run_classification_pipeline([path], routing, budgets, rules)
    return items[0]


def run_classification_pipeline(
    paths: Sequence[Path],
    routing: Dict[str, str],
    budgets: Dict[str, object],
    rules: Optional[Dict[str, object]] = None,
) -> Tuple[List[FileItem], ScanMetrics]:
    start = time.perf_counter()
    entries = _collect_pipeline_entries(paths)
    pair_map = _build_pair_map(entries)
    _apply_name_classification(entries)
    _apply_pair_enforcement(entries, pair_map)
    soft_authors = _prepare_keyword_tokens((rules or {}).get("adult_soft_authors", []))
    _apply_adult_gating(entries, set(soft_authors))
    header_budget = min(int(budgets.get(".package", 131072)), 131072)
    decisive_headers = _apply_fallback_peek(entries, header_budget)
    items = [_finalize_entry(entry, routing) for entry in entries]
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    metrics = _build_metrics(entries, elapsed_ms, decisive_headers)
    return items, metrics


def _collect_pipeline_entries(paths: Sequence[Path]) -> List[PipelineEntry]:
    entries: List[PipelineEntry] = []
    for path in paths:
        ext, disabled = _effective_extension(path)
        original_ext = "".join(suffix.lower() for suffix in path.suffixes)
        try:
            size_bytes = path.stat().st_size
        except OSError:
            size_bytes = 0
        base_name = _base_name_for_entry(path, ext, disabled)
        tokens = _tokenise(base_name)
        path_tokens = _tokenise_path(path.parent)
        normalized_key = normalize_key(base_name) or normalize_key(path.stem)
        soft_routed = _is_soft_routed_entry(path)
        entries.append(
            PipelineEntry(
                path=path,
                ext=ext,
                disabled=disabled,
                original_ext=original_ext,
                size_bytes=size_bytes,
                tokens=tokens,
                path_tokens=path_tokens,
                base_name=base_name,
                normalized_key=normalized_key,
                soft_routed=soft_routed,
            )
        )
    return entries


def _base_name_for_entry(path: Path, ext: str, disabled: bool) -> str:
    name = path.name
    suffixes = list(path.suffixes)
    trimmed = name
    while suffixes:
        suffix = suffixes.pop()
        lowered = suffix.lower()
        if lowered == ext or lowered in SUPPORTED_DISABLED_SUFFIXES:
            trimmed = trimmed[: -len(suffix)]
            continue
        break
    return trimmed or path.stem


def _is_soft_routed_entry(path: Path) -> bool:
    lowered_name = path.name.lower()
    if lowered_name in SOFT_ROUTE_NAME_SET:
        return True
    return any(part.lower() == "__macosx" for part in path.parts)


def _build_pair_map(entries: Sequence[PipelineEntry]) -> Dict[str, Dict[str, List[PipelineEntry]]]:
    pair_map: Dict[str, Dict[str, List[PipelineEntry]]] = {}
    for entry in entries:
        if entry.disabled:
            continue
        if entry.ext not in SCRIPT_EXTS and entry.ext not in PACKAGE_EXTS:
            continue
        key = entry.normalized_key
        info = pair_map.setdefault(key, {"scripts": [], "packages": []})
        if entry.ext in SCRIPT_EXTS:
            info["scripts"].append(entry)
        else:
            info["packages"].append(entry)
    return pair_map


def _apply_name_classification(entries: Sequence[PipelineEntry]) -> None:
    for entry in entries:
        entry.decision = _classify_entry_by_name(entry)


def _classify_entry_by_name(entry: PipelineEntry) -> ClassificationDecision:
    if entry.disabled:
        return ClassificationDecision("Disabled", 1.0, "disabled:ext")
    if entry.soft_routed:
        return ClassificationDecision("Resources", 0.6, "resource")
    if entry.ext in SCRIPT_EXTS:
        return ClassificationDecision("Script Mod", 1.0, NAME_REASON_MAP["Script Mod"])
    if entry.ext in ARCHIVE_EXTS:
        return ClassificationDecision("Archive", 0.6, "archive")
    if entry.ext in TEXT_FILE_EXTS or entry.ext in IMAGE_FILE_EXTS or entry.ext in RESOURCE_EXTS:
        return ClassificationDecision("Resources", 0.6, "resource")
    if entry.ext and entry.ext not in SUPPORTED_EXTS:
        return ClassificationDecision("Other", 0.5, "other")
    if entry.ext not in PACKAGE_EXTS:
        return ClassificationDecision("Unknown", 0.0, "")

    tokens = tuple(token.lower() for token in entry.tokens + entry.path_tokens)
    token_set = set(tokens)

    scores: Dict[str, int] = {}
    for category, keywords in NAME_KEYWORD_BUCKETS:
        hits = sum(1 for keyword in keywords if keyword in token_set)
        scores[category] = hits

    best_category = "Unknown"
    best_score = 0
    for category in NAME_PRIORITY:
        score = scores.get(category, 0)
        if score > best_score:
            best_category = category
            best_score = score

    script_hint = entry.normalized_key in SCRIPT_NORMALIZED_IDS or scores.get("Script Mod", 0) > 0
    if script_hint:
        best_category = "Script Mod"
        best_score = max(best_score, 1)

    if best_score <= 0:
        return ClassificationDecision("Unknown", 0.0, "")

    confidence = 0.7 + min(0.15, 0.05 * (best_score - 1))
    if best_category == "Script Mod":
        confidence = 0.95 if entry.normalized_key in SCRIPT_NORMALIZED_IDS else max(confidence, 0.85)
    reason = NAME_REASON_MAP.get(best_category, "")
    return ClassificationDecision(best_category, confidence, reason)


def _apply_pair_enforcement(
    entries: Sequence[PipelineEntry],
    pair_map: Dict[str, Dict[str, List[PipelineEntry]]],
) -> None:
    for key, info in pair_map.items():
        scripts = info.get("scripts", [])
        packages = info.get("packages", [])
        if scripts:
            for script_entry in scripts:
                script_entry.decision = ClassificationDecision(
                    "Script Mod", 1.0, NAME_REASON_MAP["Script Mod"]
                )
            for package_entry in packages:
                package_entry.decision = ClassificationDecision(
                    "Script Mod", 1.0, "link:script-pair"
                )
                package_entry.link_target = package_entry.base_name or script_entry.base_name
        elif packages and key in SCRIPT_NORMALIZED_IDS:
            for package_entry in packages:
                package_entry.decision = ClassificationDecision(
                    "Script Mod", 0.95, NAME_REASON_MAP["Script Mod"]
                )


def _apply_adult_gating(
    entries: Sequence[PipelineEntry], soft_authors: Optional[set[str]] = None
) -> None:
    for entry in entries:
        decision = entry.decision
        if decision.category in {"Disabled", "Script Mod", "Adult"}:
            continue
        tokens = {token.lower() for token in entry.tokens + entry.path_tokens}
        matched = next((token for token in ADULT_STRONG_KEYWORDS if token in tokens), None)
        if matched:
            entry.decision = ClassificationDecision("Adult", 1.0, f"adult:strong:{matched}")
            continue
        if not soft_authors or "adult" not in entry.path_tokens:
            continue
        soft_match = next((token for token in soft_authors if token in tokens), None)
        if soft_match:
            entry.decision = ClassificationDecision("Adult", 0.8, f"adult:soft:{soft_match}")


def _apply_fallback_peek(entries: Sequence[PipelineEntry], budget: int) -> int:
    decisive = 0
    for entry in entries:
        if entry.ext not in PACKAGE_EXTS:
            continue
        if entry.decision.category not in {"Unknown", "Mixed"}:
            continue
        types = read_package_manifest(entry.path, budget)
        entry.header_types = types
        category, confidence, reason = classify_by_header(types)
        if category:
            entry.decision = ClassificationDecision(category, confidence, reason)
            entry.decided_by_header = True
            if confidence >= 0.9 and reason.startswith("header:"):
                decisive += 1
        elif reason:
            entry.decision.reason = reason
        elif not types:
            entry.decision.reason = "header:fail"
    return decisive


def _finalize_entry(entry: PipelineEntry, routing: Dict[str, str]) -> FileItem:
    decision = entry.decision
    category = decision.category or "Unknown"
    confidence = decision.confidence
    reason = decision.reason
    if not reason and category == "Unknown" and entry.header_types is None:
        reason = ""
    target_folder = route_category(category, routing)
    include = category != "Disabled"
    notes = reason
    tooltips = _build_reason(reason)
    if reason == "link:script-pair":
        tooltips["reason"] = f"Linked to script mod: {entry.link_target or entry.base_name}"
    item = FileItem(
        path=entry.path,
        name=entry.path.name,
        ext=entry.ext,
        size_mb=human_mb(entry.size_bytes),
        relpath="",
        guess_type=category,
        confidence=confidence,
        notes=notes,
        include=include,
        target_folder=target_folder,
        bundle="",
        meta_tags="",
        dependency_status="",
        dependency_detail="",
        extras={},
        tooltips=tooltips,
        disabled=category == "Disabled",
        original_ext=entry.original_ext,
    )
    return item


def _build_metrics(
    entries: Sequence[PipelineEntry], elapsed_ms: float, decisive_headers: int
) -> ScanMetrics:
    total = len(entries)
    avg_ms = elapsed_ms / total if total else 0.0
    counts: Dict[str, int] = {}
    for entry in entries:
        category = entry.decision.category or "Unknown"
        counts[category] = counts.get(category, 0) + 1
    return ScanMetrics(
        total_files=total,
        elapsed_ms=elapsed_ms,
        avg_ms_per_file=avg_ms,
        decisive_headers=decisive_headers,
        category_counts=counts,
    )


@dataclass(frozen=True)
class DependencyRule:
    key: str
    requires: Tuple[str, ...]


def _parse_dependency_rules(rules: Dict[str, object]) -> Tuple[DependencyRule, ...]:
    raw_rules = rules.get("dependency_rules", [])
    parsed: List[DependencyRule] = []
    for entry in raw_rules:
        if not isinstance(entry, dict):
            continue
        key = normalize_key(str(entry.get("key", "")))
        if not key:
            continue
        raw_requires = entry.get("requires", [])
        requires: List[str] = []
        if isinstance(raw_requires, list):
            for raw in raw_requires:
                norm = normalize_key(str(raw))
                if norm:
                    requires.append(norm)
        parsed.append(DependencyRule(key=key, requires=tuple(requires)))
    return tuple(parsed)


def _collect_dependency_keys(item: FileItem) -> set[str]:
    tokens: set[str] = set()
    stem = normalize_key(Path(item.name).stem)
    if stem:
        tokens.add(stem)
    rel_tokens = _tokenise(item.relpath)
    for token in rel_tokens:
        tokens.add(normalize_key(token))
    return {token for token in tokens if token}


def apply_dependency_rules(
    items: Sequence[FileItem],
    disabled_items: Sequence[FileItem],
    rules: Tuple[DependencyRule, ...],
) -> None:
    if not rules:
        return
    seen_keys: set[str] = set()
    for item in list(items) + list(disabled_items):
        seen_keys.update(_collect_dependency_keys(item))
    for item in items:
        item_keys = _collect_dependency_keys(item)
        for rule in rules:
            if rule.key not in item_keys:
                continue
            if not rule.requires:
                item.dependency_status = "ok"
                item.dependency_detail = rule.key
                break
            missing = [req for req in rule.requires if req not in seen_keys]
            if missing:
                item.dependency_status = "missing"
                item.dependency_detail = "Missing: " + ", ".join(sorted(missing))
            else:
                item.dependency_status = "ok"
                item.dependency_detail = "Requires: " + ", ".join(sorted(rule.requires))
            break


def _fingerprint_file(path: Path) -> str:
    try:
        digest = hashlib.sha1()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(65536), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return ""


def _should_ignore(path: Path) -> bool:
    return False


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
    root_path = Path(root)
    if not root_path.is_dir():
        return ScanResult([], 0, ["Folder not found"])

    rules = load_rules()
    budgets = load_budgets()
    routing = dict(folder_map) if folder_map is not None else dict(DEFAULT_FOLDER_MAP)
    dependency_rules = _parse_dependency_rules(rules)

    ignore_exts_set = {ext.lower() if ext.startswith(".") else f".{ext.lower()}" for ext in (ignore_exts or [])}
    ignore_names_tokens = [token.lower() for token in (ignore_names or []) if token]
    if allowed_exts is None:
        allowed_exts_set: Optional[set[str]] = None
    else:
        allowed_exts_set = {
            ext.lower() if ext.startswith(".") else f".{ext.lower()}"
            for ext in allowed_exts
        }

    selected_paths: Optional[List[Path]] = None
    if selected_folders is not None:
        selected_paths = []
        seen: set[Path] = set()
        for folder in selected_folders:
            token = Path(folder)
            if token in seen:
                continue
            seen.add(token)
            selected_paths.append(token)

    files: List[Path] = []
    if recurse:
        iterator: Iterable[Path] = (Path(os.path.join(base, name)) for base, _, names in os.walk(root_path) for name in names)
    else:
        iterator = (entry.path for entry in os.scandir(root_path) if entry.is_file())
        iterator = (Path(path) for path in iterator)

    for index, path in enumerate(iterator):
        if progress_cb:
            progress_cb(index + 1, 0, path, "discover")
        if _should_ignore(path):
            continue
        ext, disabled_ext = _effective_extension(path)
        canonical_ext = ext or path.suffix.lower()
        if allowed_exts_set is not None and canonical_ext not in allowed_exts_set:
            continue
        if ignore_exts_set and canonical_ext in ignore_exts_set:
            continue
        lowered_name = path.name.lower()
        if any(token in lowered_name for token in ignore_names_tokens):
            continue
        if selected_paths is not None:
            rel = path.relative_to(root_path)
            include = False
            for selected in selected_paths:
                if selected in {Path("."), Path("./"), Path("")}:
                    include = True
                    break
                try:
                    rel.relative_to(selected)
                    include = True
                    break
                except ValueError:
                    continue
            if not include:
                continue
        if not canonical_ext and not disabled_ext:
            continue
        files.append(path)

    items: List[FileItem] = []
    disabled_items: List[FileItem] = []
    errors: List[str] = []
    files = sorted(files)
    total = len(files)
    pipeline_items: List[FileItem] = []
    metrics: Optional[ScanMetrics] = None
    if files:
        pipeline_items, metrics = run_classification_pipeline(files, routing, budgets, rules)
    else:
        metrics = ScanMetrics(0, 0.0, 0.0, 0, {})

    for index, (path, item) in enumerate(zip(files, pipeline_items)):
        item.relpath = str(path.relative_to(root_path)).replace(os.sep, "/")
        if not include_adult and item.guess_type == "Adult":
            if progress_cb:
                progress_cb(index + 1, total, path, "scanned")
            continue
        fingerprint = _fingerprint_file(path)
        if fingerprint:
            item.extras[FINGERPRINT_EXTRA_KEY] = fingerprint
        if item.disabled:
            item.include = False
            disabled_items.append(item)
        else:
            items.append(item)
        if progress_cb:
            progress_cb(index + 1, total, path, "scanned")

    fingerprint_groups: Dict[str, List[FileItem]] = {}
    for item in items:
        fingerprint = item.extras.get(FINGERPRINT_EXTRA_KEY, "")
        if fingerprint:
            fingerprint_groups.setdefault(fingerprint, []).append(item)

    for group in fingerprint_groups.values():
        if len(group) <= 1:
            continue
        group.sort(key=lambda entry: _natural_key(entry.relpath))
        primary = group[0]
        for duplicate in group[1:]:
            duplicate.extras[DUPLICATE_EXTRA_KEY] = "⚠"
            duplicate.tooltips[DUPLICATE_EXTRA_KEY] = f"Duplicate of {primary.relpath}"

    apply_dependency_rules(items, disabled_items, dependency_rules)

    items.sort(
        key=lambda item: (
            CATEGORY_INDEX.get(item.guess_type.split(":", 1)[0], len(CATEGORY_ORDER)),
            _natural_key(item.relpath),
            _natural_key(item.name),
        )
    )
    disabled_items.sort(key=lambda item: (_natural_key(item.relpath), _natural_key(item.name)))

    return ScanResult(
        items=items,
        total_files=total,
        errors=errors,
        disabled_items=disabled_items,
        metrics=metrics,
    )


def guess_type_for_name(name: str, ext: str) -> Tuple[str, float, str]:
    norm_ext, disabled = normalize_extension(ext)
    path_obj = Path(name)
    base_name = _base_name_for_entry(path_obj, norm_ext, disabled)
    entry = PipelineEntry(
        path=path_obj,
        ext=norm_ext,
        disabled=disabled,
        original_ext=norm_ext,
        size_bytes=0,
        tokens=_tokenise(base_name),
        path_tokens=(),
        base_name=base_name,
        normalized_key=normalize_key(base_name),
        soft_routed=_is_soft_routed_entry(path_obj),
    )
    decision = _classify_entry_by_name(entry)
    return decision.category, decision.confidence, decision.reason


def classify_from_types(
    types: Dict[int, int],
    filename: str,
    adult_hint: bool = False,
) -> Tuple[str, float, str]:
    category, confidence, reason = classify_by_header(types)
    if category:
        return category, confidence, reason
    path_obj = Path(filename)
    norm_ext, disabled = normalize_extension(path_obj.suffix)
    base_name = _base_name_for_entry(path_obj, norm_ext, disabled)
    entry = PipelineEntry(
        path=path_obj,
        ext=norm_ext,
        disabled=disabled,
        original_ext=norm_ext,
        size_bytes=0,
        tokens=_tokenise(base_name),
        path_tokens=(),
        base_name=base_name,
        normalized_key=normalize_key(base_name),
        soft_routed=_is_soft_routed_entry(path_obj),
    )
    decision = _classify_entry_by_name(entry)
    reason = decision.reason or reason
    return decision.category or "Unknown", decision.confidence or confidence, reason


TYPE_IDS: Dict[int, str] = {
    0x00B2D882: "BONE",  # placeholder entries retained for compatibility
}


def dbpf_scan_types(path: Path, limit: int = 10) -> Dict[int, int]:
    result: Dict[int, int] = {}
    try:
        with path.open("rb") as handle:
            header = handle.read(96)
            if len(header) < 96 or header[:4] != b"DBPF":
                return result
            count = int.from_bytes(header[0x20:0x24], "little")
            index_pos = int.from_bytes(header[0x40:0x44], "little")
            if count <= 0 or index_pos <= 0:
                return result
            handle.seek(index_pos)
            flags = int.from_bytes(handle.read(4), "little")
            flagged_slots = [idx for idx in range(8) if (flags >> idx) & 1]
            header_vals = [int.from_bytes(handle.read(4), "little") for _ in flagged_slots]
            per_entry = 8 - len(flagged_slots)
            inspected = 0
            for _ in range(count):
                entry_vals = [int.from_bytes(handle.read(4), "little") for _ in range(per_entry)]
                values: Dict[int, int] = {}
                hi = 0
                mi = 0
                for slot in range(8):
                    if slot in flagged_slots:
                        values[slot] = header_vals[hi]
                        hi += 1
                    else:
                        values[slot] = entry_vals[mi]
                        mi += 1
                resource_type = values.get(0)
                if resource_type is None:
                    continue
                result[resource_type] = result.get(resource_type, 0) + 1
                inspected += 1
                if limit and inspected >= limit:
                    break
    except Exception:
        return {}
    return result


def bundle_scripts_and_packages(items: Sequence[FileItem], folder_map: Dict[str, str]) -> Dict[str, int]:
    script_lookup: Dict[str, FileItem] = {}
    for item in items:
        if item.disabled:
            continue
        if item.ext in SCRIPT_EXTS and item.guess_type == "Script Mod":
            script_lookup[normalize_key(item.name)] = item
    linked = 0
    for item in items:
        if item.disabled or item.ext not in PACKAGE_EXTS:
            continue
        key = normalize_key(item.name)
        script_item = script_lookup.get(key)
        if not script_item:
            continue
        bundle_key = f"bundle:{key}"
        item.bundle = bundle_key
        script_item.bundle = bundle_key
        linked += 1
        if item.target_folder.rstrip("/") == folder_map.get("Unknown", UNKNOWN_DEFAULT_FOLDER).rstrip("/"):
            item.target_folder = folder_map.get("Script Mod", script_item.target_folder)
    return {"linked": linked, "scripts": len(script_lookup)}


DEFAULT_FOLDER_MAP.setdefault("Disabled", UNKNOWN_DEFAULT_FOLDER)
