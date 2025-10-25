from __future__ import annotations

import hashlib
import json
import os
import re
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

PACKAGE_EXTS = {".package"}
SCRIPT_EXTS = {".ts4script"}
ARCHIVE_EXTS = {".zip", ".rar", ".7z"}
TEXT_FILE_EXTS = {".txt", ".cfg", ".ini", ".log", ".rtf"}
IMAGE_FILE_EXTS = {".png", ".jpg", ".jpeg"}
PRESET_EXTS = {".json"}
SUPPORTED_EXTS = (
    PACKAGE_EXTS
    | SCRIPT_EXTS
    | ARCHIVE_EXTS
    | TEXT_FILE_EXTS
    | IMAGE_FILE_EXTS
    | PRESET_EXTS
)
SUPPORTED_DISABLED_SUFFIXES = {".off", ".disabled", ".bak"}

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
    "ScanResult",
    "SUPPORTED_EXTS",
    "TEXT_FILE_EXTS",
    "bundle_scripts_and_packages",
    "classify_by_header",
    "classify_from_types",
    "classify_adult",
    "dbpf_scan_types",
    "fallback_by_name",
    "get_default_mods_path",
    "guess_type_for_name",
    "human_mb",
    "normalize_extension",
    "normalize_key",
    "pretty_display_name",
    "read_package_manifest",
    "route_category",
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


def _thresholds_path() -> Path:
    return _module_dir() / "thresholds.json"


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


def load_thresholds() -> Dict[str, object]:
    return _load_json(_thresholds_path())


DEFAULT_FOLDER_MAP = load_routing()


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


NAME_WEIGHTS = {
    "CAS": 4,
    "BuildBuy": 4,
    "Pose or Animation": 3,
    "Tuning": 3,
}
REASON_PREFIX = {
    "CAS": "name:cas",
    "BuildBuy": "name:buildbuy",
    "Pose or Animation": "name:pose",
    "Tuning": "name:tuning",
}
NAME_TIE_BREAK = [
    "Script Mod",
    "CAS",
    "BuildBuy",
    "Pose or Animation",
    "Tuning",
    "Resources",
    "Other",
    "Unknown",
    "Disabled",
]


def fallback_by_name(tokens: Iterable[str], rules: Dict[str, object]) -> Tuple[str, float, str]:
    cas_tokens = {token.lower() for token in rules.get("cas_tokens", [])}
    build_tokens = {token.lower() for token in rules.get("buildbuy_tokens", [])}
    pose_tokens = {token.lower() for token in rules.get("pose_tokens", [])}
    tuning_tokens = {token.lower() for token in rules.get("tuning_tokens", [])}
    scores = {
        "CAS": 0,
        "BuildBuy": 0,
        "Pose or Animation": 0,
        "Tuning": 0,
    }
    for token in tokens:
        if token in cas_tokens:
            scores["CAS"] += NAME_WEIGHTS["CAS"]
        if token in build_tokens:
            scores["BuildBuy"] += NAME_WEIGHTS["BuildBuy"]
        if token in pose_tokens:
            scores["Pose or Animation"] += NAME_WEIGHTS["Pose or Animation"]
        if token in tuning_tokens:
            scores["Tuning"] += NAME_WEIGHTS["Tuning"]
    best_score = max(scores.values())
    if best_score <= 0:
        return "Unknown", 0.0, ""
    candidates = [category for category, value in scores.items() if value == best_score]
    best_category = next((category for category in NAME_TIE_BREAK if category in candidates), candidates[0])
    confidence = max(0.70, min(0.85, best_score / 12.0))
    reason = REASON_PREFIX.get(best_category, "")
    return best_category, confidence, reason


def classify_adult(
    tokens: Iterable[str],
    path_tokens: Iterable[str],
    base_category: str,
    rules: Dict[str, object],
) -> Tuple[str, str]:
    name_tokens = [token.lower() for token in tokens]
    context_tokens = [token.lower() for token in path_tokens]
    combined = set(name_tokens) | set(context_tokens)
    adult_strong = [token.lower() for token in rules.get("adult_strong", [])]
    for token in adult_strong:
        if token and token in combined:
            return "Adult", f"adult:strong:{token}"
    adult_soft_authors = {token.lower() for token in rules.get("adult_soft_authors", [])}
    path_token_set = set(context_tokens)
    if "adult" in path_token_set and adult_soft_authors & path_token_set:
        return "Adult", "adult:soft+folder"
    return base_category, ""


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
    return ext in PRESET_EXTS


CLUTTER_NAMES = {".ds_store", "thumbs.db"}


def scan_file(
    path: Path,
    rules: Dict[str, object],
    routing: Dict[str, str],
    budgets: Dict[str, object],
) -> FileItem:
    ext, disabled = _effective_extension(path)
    original_ext = "".join(suffix.lower() for suffix in path.suffixes)
    size_bytes = path.stat().st_size if path.exists() else 0
    size_mb = human_mb(size_bytes)
    tokens = _tokenise(path.name)
    path_tokens = _tokenise_path(path.parent)

    category = "Unknown"
    confidence = 0.0
    reason = ""

    if disabled:
        category = "Disabled"
        confidence = 1.0
        reason = "disabled:ext"
    elif ext in SCRIPT_EXTS:
        category = "Script Mod"
        confidence = 1.0
        reason = "script:ts4script"
    elif ext in ARCHIVE_EXTS:
        category = "Archive"
        confidence = 0.6
        reason = "archive"
    elif ext in PACKAGE_EXTS:
        if size_bytes and size_bytes < 1024:
            category = "Other"
            confidence = 1.0
            reason = "size:tiny"
        else:
            budget = int(budgets.get(".package", 131072))
            types = read_package_manifest(path, budget)
            header_category, header_confidence, header_reason = classify_by_header(types)
            if header_category:
                category = header_category
                confidence = header_confidence
                reason = header_reason
            else:
                if header_reason:
                    reason = header_reason
                fallback_category, fallback_confidence, fallback_reason = fallback_by_name(tokens + path_tokens, rules)
                category = fallback_category
                confidence = max(confidence, fallback_confidence)
                if fallback_reason and not reason:
                    reason = fallback_reason
                if not category:
                    category = "Unknown"
            if not types and not reason:
                reason = "header:fail"
    elif _is_text_sidecar(ext) or _is_image(ext) or _is_resource_ext(ext):
        category = "Resources"
        confidence = 0.6
        reason = "resource"
    elif ext:
        category = "Other"
        confidence = 0.5
        reason = "other"

    base_category = category
    if category != "Disabled":
        adult_category, adult_reason = classify_adult(tokens, path_tokens, category, rules)
        if adult_reason:
            category = adult_category
            reason = adult_reason
    target_folder = route_category(category, routing)
    include = category != "Disabled"
    notes = reason
    tooltips = _build_reason(reason)
    extras: Dict[str, str] = {}

    return FileItem(
        path=path,
        name=path.name,
        ext=ext,
        size_mb=size_mb,
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
        extras=extras,
        tooltips=tooltips,
        disabled=category == "Disabled",
        original_ext=original_ext,
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
    name = path.name.lower()
    if name in CLUTTER_NAMES:
        return True
    return "__macosx" in (part.lower() for part in path.parts)


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
    total = len(files)
    for index, path in enumerate(sorted(files)):
        try:
            item = scan_file(path, rules, routing, budgets)
            item.relpath = str(path.relative_to(root_path)).replace(os.sep, "/")
            if not include_adult and item.guess_type == "Adult":
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
        except Exception as exc:
            errors.append(f"scan failed for {path}: {exc}")
            if progress_cb:
                progress_cb(index + 1, total, path, "error")

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

    return ScanResult(items=items, total_files=total, errors=errors, disabled_items=disabled_items)


def guess_type_for_name(name: str, ext: str) -> Tuple[str, float, str]:
    rules = load_rules()
    tokens = _tokenise(name)
    category, confidence, reason = fallback_by_name(tokens, rules)
    return category, confidence, reason


def classify_from_types(
    types: Dict[int, int],
    filename: str,
    adult_hint: bool = False,
) -> Tuple[str, float, str]:
    category, confidence, reason = classify_by_header(types)
    if not category:
        rules = load_rules()
        tokens = _tokenise(filename)
        category, confidence, name_reason = fallback_by_name(tokens, rules)
        if name_reason:
            reason = name_reason
    return category or "Unknown", confidence, reason


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
