"""Dependency Tracker plugin for Sims4 Mod Sorter (v1.1.0)."""

from __future__ import annotations

import json
import threading
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import tkinter as tk
from tkinter import ttk

from plugin_api import scan_metrics

COLUMN_ID = "dependency"
ICON_MISSING = "\u26a0\ufe0f"
ICON_OK = "\u2705"
SCANNED_EXTS = {".package", ".ts4script"}
DB_PATH = Path(__file__).with_name("known_dependencies.json")

FEATURE_TRACKING = "tracking"
FEATURE_OVERLAY = "overlay"


def _lower_clean(value: str) -> str:
    return value.lower().strip()


DEFAULT_DB: Dict[str, List[Dict[str, object]]] = {
    "mods": [
        {
            "id": "ui_cheats_extension",
            "name": "UI Cheats Extension",
            "patterns": ["ui cheats", "ui_cheats", "uicheats", "ui-cheats"],
            "requires": ["mc_command_center", "ts4_script_loader"],
        },
        {
            "id": "wickedwhims",
            "name": "WickedWhims",
            "patterns": ["wickedwhims", "wicked whims", "turbodriver"],
            "requires": ["basemental_drugs"],
        },
        {
            "id": "better_school_grades",
            "name": "Better School Grades",
            "patterns": ["better school grades"],
            "requires": ["xml_injector"],
        },
        {
            "id": "slice_of_life",
            "name": "Slice of Life",
            "patterns": ["slice of life", "kawaiistacie slice"],
            "requires": ["xml_injector"],
        },
    ],
    "frameworks": [
        {
            "id": "mc_command_center",
            "name": "MC Command Center",
            "patterns": ["mc command center", "mccc", "deaderpool"],
        },
        {
            "id": "xml_injector",
            "name": "XML Injector",
            "patterns": ["xml injector"],
        },
        {
            "id": "basemental_drugs",
            "name": "Basemental Drugs",
            "patterns": ["basemental drugs", "basementaldrugs"],
        },
        {
            "id": "ts4_script_loader",
            "name": "TS4 Script Loader",
            "patterns": ["ts4 script loader", "ts4scriptloader"],
        },
    ],
}


@dataclass(frozen=True)
class DependencyDefinition:
    identifier: str
    name: str
    patterns: Tuple[str, ...]
    requires: Tuple[str, ...] = tuple()

    def matches(self, blob: str) -> bool:
        if not blob:
            return False
        return any(pattern in blob for pattern in self.patterns)


@dataclass(slots=True)
class DependencyResult:
    item: object
    mods: Tuple[str, ...]
    detail: str
    missing: bool


class DependencyRegistry:
    """In-memory dependency definitions and helpers."""

    def __init__(self, api) -> None:
        self.api = api
        self.mods: Tuple[DependencyDefinition, ...] = tuple()
        self.frameworks: Tuple[DependencyDefinition, ...] = tuple()
        self.lookup: Dict[str, DependencyDefinition] = {}
        self._info_labels: List[ttk.Label] = []

    # ------------------------------------------------------------------
    # Loading and parsing
    # ------------------------------------------------------------------
    def load(self) -> None:
        try:
            payload = json.loads(DB_PATH.read_text(encoding="utf-8"))
        except FileNotFoundError:
            payload = DEFAULT_DB
            self.api.log(
                f"[Dependency Tracker] Dependency list missing. Using built-in defaults ({len(DEFAULT_DB['mods'])} mods)."
            )
            try:
                DB_PATH.write_text(json.dumps(DEFAULT_DB, indent=2), encoding="utf-8")
                self.api.log("[Dependency Tracker] Created default known_dependencies.json for future edits.")
            except Exception as exc:
                self.api.log(
                    f"[Dependency Tracker] Unable to write default dependency list ({exc}).",
                    "warn",
                )
        except Exception as exc:
            payload = DEFAULT_DB
            self.api.log(
                f"[Dependency Tracker] Failed to parse dependency list ({exc}). Using built-in defaults ({len(DEFAULT_DB['mods'])} mods).",
                "warn",
            )
            try:
                DB_PATH.write_text(json.dumps(DEFAULT_DB, indent=2), encoding="utf-8")
            except Exception:
                pass
        else:
            self.api.log(
                f"[Dependency Tracker] Loaded {len(payload.get('mods', []))} mod definition(s) and {len(payload.get('frameworks', []))} framework(s)."
            )
        self._parse_payload(payload)

    def _parse_payload(self, payload: Dict[str, object]) -> None:
        mods: List[DependencyDefinition] = []
        frameworks: List[DependencyDefinition] = []
        lookup: Dict[str, DependencyDefinition] = {}

        def normalise(entry: Dict[str, object], *, requires: Sequence[str]) -> Optional[DependencyDefinition]:
            identifier = str(entry.get("id") or entry.get("name") or "").strip()
            if not identifier:
                return None
            name = str(entry.get("name") or identifier).strip()
            patterns = tuple(sorted({_lower_clean(str(pattern)) for pattern in entry.get("patterns", []) if str(pattern).strip()}))
            req = tuple(str(dep).strip() for dep in requires if str(dep).strip())
            definition = DependencyDefinition(identifier=identifier, name=name, patterns=patterns, requires=req)
            return definition

        for entry in payload.get("frameworks", []) or []:
            if not isinstance(entry, dict):
                continue
            definition = normalise(entry, requires=())
            if not definition:
                continue
            frameworks.append(definition)
            lookup[definition.identifier] = definition

        for entry in payload.get("mods", []) or []:
            if not isinstance(entry, dict):
                continue
            requires = entry.get("requires", [])
            requires_seq = requires if isinstance(requires, Sequence) else []
            definition = normalise(entry, requires=requires_seq)
            if not definition:
                continue
            mods.append(definition)
            lookup[definition.identifier] = definition

        self.mods = tuple(mods)
        self.frameworks = tuple(frameworks)
        self.lookup = lookup
        self._update_info_labels()

    # ------------------------------------------------------------------
    # UI helpers
    # ------------------------------------------------------------------
    def register_info_label(self, label: ttk.Label) -> None:
        self._info_labels.append(label)
        self._update_info_labels()

    def _update_info_labels(self) -> None:
        text = f"Definitions: {len(self.mods)} mods, {len(self.frameworks)} frameworks"
        for label in list(self._info_labels):
            if label.winfo_exists():
                label.configure(text=text)
            else:
                self._info_labels.remove(label)


def _contains_any(haystack: str, needles: Iterable[str]) -> bool:
    if not haystack:
        return False
    return any(needle in haystack for needle in needles if needle)


def _read_package_snippet(path: Path) -> str:
    try:
        with path.open("rb") as handle:
            data = handle.read(131072)
        return data.decode("utf-8", "ignore").lower()
    except Exception:
        return ""


def _collect_ts4_names(path: Path) -> str:
    try:
        with zipfile.ZipFile(path) as archive:
            names = [Path(name).name for name in archive.namelist()[:40]]
        return " ".join(name.lower() for name in names if name)
    except Exception:
        return ""


class DependencyAnalyser:
    def __init__(self, registry: DependencyRegistry) -> None:
        self.registry = registry

    # ------------------------------------------------------------------
    def clear_items(self, items: Iterable[object]) -> None:
        for item in items:
            self._clear_item(item)

    @staticmethod
    def _clear_item(item: object) -> None:
        setattr(item, "dependency_status", "")
        setattr(item, "dependency_detail", "")
        extras = getattr(item, "extras", None)
        if isinstance(extras, dict):
            extras.pop(COLUMN_ID, None)
        tooltips = getattr(item, "tooltips", None)
        if isinstance(tooltips, dict):
            tooltips.pop(COLUMN_ID, None)

    # ------------------------------------------------------------------
    def analyse(self, items: Sequence[object]) -> List[DependencyResult]:
        if not items:
            return []

        for item in items:
            self._clear_item(item)

        found_ids: set[str] = set()
        mod_matches: Dict[int, List[str]] = {}

        for item in items:
            ext = str(getattr(item, "ext", "")).lower()
            if ext not in SCANNED_EXTS:
                continue
            path = getattr(item, "path", None)
            path = Path(path) if path else None
            name_lower = str(getattr(item, "name", "")).lower()
            rel_lower = str(getattr(item, "relpath", "")).lower()
            ts4_names = _collect_ts4_names(path) if ext == ".ts4script" and path else ""
            name_blob = " ".join(part for part in (name_lower, rel_lower, ts4_names) if part)
            content_blob = _read_package_snippet(path) if ext == ".package" and path else ""
            combined_blob = "\n".join(part for part in (name_blob, content_blob) if part)

            for definition in self.registry.frameworks:
                if definition.matches(name_blob):
                    found_ids.add(definition.identifier)

            blob_for_mod = combined_blob or name_blob
            for definition in self.registry.mods:
                if definition.matches(blob_for_mod):
                    found_ids.add(definition.identifier)
                    bucket = mod_matches.setdefault(id(item), [])
                    if definition.identifier not in bucket:
                        bucket.append(definition.identifier)

        results: List[DependencyResult] = []
        for item in items:
            entry_ids = mod_matches.get(id(item), [])
            if not entry_ids:
                continue
            result = self._apply_result(item, entry_ids, found_ids)
            if result:
                results.append(result)

        return results

    def _apply_result(self, item: object, entry_ids: Sequence[str], found_ids: Sequence[str]) -> Optional[DependencyResult]:
        lookup = self.registry.lookup
        details: List[str] = []
        missing_any = False
        seen_mods: set[str] = set()

        for entry_id in entry_ids:
            entry = lookup.get(entry_id)
            if not entry:
                continue
            if entry.name in seen_mods:
                continue
            seen_mods.add(entry.name)
            requires = entry.requires
            if not requires:
                continue
            parts: List[str] = []
            for dependency_id in requires:
                dep_entry = lookup.get(dependency_id)
                dep_name = dep_entry.name if dep_entry else dependency_id
                if dep_entry and dependency_id in found_ids:
                    parts.append(f"{dep_name} (found)")
                else:
                    parts.append(f"{dep_name} (missing)")
                    missing_any = True
            if parts:
                details.append(f"{entry.name}: {', '.join(parts)}")

        if not details:
            return None

        status = "missing" if missing_any else "ok"
        detail_text = "Requires: " + "; ".join(details)
        setattr(item, "dependency_status", status)
        setattr(item, "dependency_detail", detail_text)
        extras = getattr(item, "extras", None)
        if isinstance(extras, dict):
            extras[COLUMN_ID] = ICON_MISSING if missing_any else ICON_OK
        tooltips = getattr(item, "tooltips", None)
        if isinstance(tooltips, dict):
            tooltips[COLUMN_ID] = detail_text
        return DependencyResult(
            item=item,
            mods=tuple(sorted(seen_mods)),
            detail=detail_text,
            missing=missing_any,
        )


class DependencyOverlay:
    def __init__(self, api) -> None:
        self.api = api
        self.window: Optional[tk.Toplevel] = None
        self.tree: Optional[ttk.Treeview] = None
        self.status_var: Optional[tk.StringVar] = None
        self._results: List[DependencyResult] = []

    # ------------------------------------------------------------------
    def show(self) -> None:
        window = self._ensure_window()
        if not window:
            return
        window.deiconify()
        window.lift()
        self._populate()

    def sync(self, results: Sequence[DependencyResult]) -> None:
        self._results = list(results)
        app = getattr(self.api, "app", None)
        if app is None:
            return

        def refresh() -> None:
            self._populate()

        app.after(0, refresh)

    # ------------------------------------------------------------------
    def _ensure_window(self) -> Optional[tk.Toplevel]:
        if self.window and self.window.winfo_exists():
            return self.window

        app = getattr(self.api, "app", None)
        if app is None:
            return None

        window = tk.Toplevel(app)
        window.title("Dependency Tracker Summary")
        window.geometry("820x460")
        window.transient(app)

        frame = ttk.Frame(window, padding=12)
        frame.pack(fill="both", expand=True)

        columns = ("file", "mods", "status", "detail")
        tree = ttk.Treeview(frame, columns=columns, show="headings")
        tree.heading("file", text="File")
        tree.heading("mods", text="Detected Mod")
        tree.heading("status", text="Status")
        tree.heading("detail", text="Dependencies")
        tree.column("file", anchor="w", width=200)
        tree.column("mods", anchor="w", width=220)
        tree.column("status", anchor="center", width=100)
        tree.column("detail", anchor="w", width=360)
        tree.pack(fill="both", expand=True)
        self.tree = tree

        self.status_var = tk.StringVar(value="No dependencies detected.")
        ttk.Label(frame, textvariable=self.status_var).pack(anchor="w", pady=(8, 0))

        def on_close() -> None:
            window.withdraw()

        window.protocol("WM_DELETE_WINDOW", on_close)
        self.window = window
        return window

    def _populate(self) -> None:
        if not self.tree or not self.tree.winfo_exists():
            return
        tree = self.tree
        tree.delete(*tree.get_children())
        total = len(self._results)
        missing = 0
        for result in self._results:
            item = result.item
            file_name = getattr(item, "name", "unknown")
            mods = ", ".join(result.mods)
            status = "Missing" if result.missing else "OK"
            if result.missing:
                missing += 1
            detail = result.detail
            tree.insert("", "end", values=(file_name, mods, status, detail))
        if self.status_var is not None:
            try:
                if total:
                    self.status_var.set(f"Tracked {total} mod(s); {missing} with missing dependencies")
                else:
                    self.status_var.set("No dependencies detected.")
            except tk.TclError:
                pass


class DependencyPlugin:
    def __init__(self, api) -> None:
        self.api = api
        self.registry = DependencyRegistry(api)
        self.analyser = DependencyAnalyser(self.registry)
        self.overlay = DependencyOverlay(api)
        self.feature_flags: Dict[str, bool] = {FEATURE_TRACKING: True, FEATURE_OVERLAY: True}
        self.tracking_enabled = True
        self.last_items: List[object] = []
        self.last_results: List[DependencyResult] = []
        self._check_var: Optional[tk.BooleanVar] = None

    # ------------------------------------------------------------------
    def register(self) -> None:
        self.registry.load()
        getter = getattr(self.api, "is_feature_enabled", None)
        if callable(getter):
            self.feature_flags[FEATURE_TRACKING] = bool(getter(FEATURE_TRACKING, default=True))
            self.feature_flags[FEATURE_OVERLAY] = bool(getter(FEATURE_OVERLAY, default=True))
        self.tracking_enabled = self.feature_flags[FEATURE_TRACKING]

        self.api.register_column(COLUMN_ID, "Deps", width=64, anchor="center")
        self.api.register_settings_section("Dependency Tracker", self._build_settings)

        if self.feature_flags[FEATURE_OVERLAY]:
            self.api.register_toolbar_button(
                "dependency-tracker",
                text="Dependencies",
                command=lambda app, _api: self.overlay.show(),
                side="right",
                insert_before="plugin_status",
                padx=6,
            )

        self.api.register_post_scan_hook(self._on_post_scan)
        self.api.log("[Dependency Tracker] Plugin initialised.")

    # ------------------------------------------------------------------
    def _build_settings(self, app: tk.Tk, frame: ttk.Frame, _api) -> None:
        self._check_var = tk.BooleanVar(master=app, value=self.tracking_enabled)

        def on_toggle() -> None:
            self.tracking_enabled = bool(self._check_var.get())
            self.feature_flags[FEATURE_TRACKING] = self.tracking_enabled
            state = "enabled" if self.tracking_enabled else "disabled"
            self.api.log(f"[Dependency Tracker] Tracking {state}.")
            if self.tracking_enabled:
                self._reanalyze_async("Rechecking dependencies after enabling tracking...")
            else:
                self.analyser.clear_items(self.last_items)
                self.last_results.clear()
                self.overlay.sync(self.last_results)
                self.api.request_refresh()

        def on_reload() -> None:
            self.registry.load()
            if self.tracking_enabled:
                self._reanalyze_async("Refreshing dependency markers after reloading definitions...")
            else:
                self.api.request_refresh()

        ttk.Checkbutton(
            frame,
            text="Enable Dependency Tracking",
            variable=self._check_var,
            command=on_toggle,
        ).grid(row=0, column=0, sticky="w")
        ttk.Button(frame, text="Reload Dependency List", command=on_reload).grid(row=1, column=0, sticky="w", pady=(6, 0))
        info = ttk.Label(frame, text="")
        info.grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.registry.register_info_label(info)

    # ------------------------------------------------------------------
    def _on_post_scan(self, items, context, _api) -> None:
        self.last_items = list(items)
        if not self.tracking_enabled:
            self.analyser.clear_items(self.last_items)
            self.last_results.clear()
            self.overlay.sync(self.last_results)
            return
        scan_metrics.start("Dependency Tracker")
        start = time.perf_counter()
        results = self.analyser.analyse(self.last_items)
        duration = time.perf_counter() - start
        missing_total = sum(1 for result in results if result.missing)
        scan_metrics.stop(
            "Dependency Tracker",
            files_processed=len(self.last_items),
            warnings=missing_total,
        )
        self.last_results = list(results)
        self.overlay.sync(self.last_results)
        self._log_summary(results, duration, len(self.last_items))

    # ------------------------------------------------------------------
    def _reanalyze_async(self, reason: str) -> None:
        if not self.last_items:
            return
        items = list(self.last_items)

        def worker() -> None:
            if not self.tracking_enabled:
                self.analyser.clear_items(items)
                self.last_results.clear()
                self.overlay.sync(self.last_results)
                self.api.request_refresh()
                return
            start = time.perf_counter()
            results = self.analyser.analyse(items)
            duration = time.perf_counter() - start
            self.last_results = list(results)
            self.overlay.sync(self.last_results)
            self.api.request_refresh()
            if reason:
                self.api.log(f"[Dependency Tracker] {reason}")
            self._log_summary(results, duration, len(items))

        threading.Thread(target=worker, daemon=True).start()

    # ------------------------------------------------------------------
    def _log_summary(self, results: Sequence[DependencyResult], duration: float, total_items: int) -> None:
        self.api.log(f"[Dependency Tracker] Checking {total_items} mod file(s) for dependencies...")
        if results:
            for result in results:
                item = result.item
                icon = ICON_MISSING if result.missing else ICON_OK
                detail = result.detail
                name = getattr(item, "name", "unknown")
                self.api.log(f"[Dependency Tracker] - {name} \u2192 {detail} {icon}")
        else:
            self.api.log("[Dependency Tracker] No tracked dependencies detected.")
        self.api.log(f"[Dependency Tracker] Finished dependency analysis in {duration:.2f}s")


PLUGIN: Optional[DependencyPlugin] = None


def register(api) -> None:
    global PLUGIN
    PLUGIN = DependencyPlugin(api)
    PLUGIN.register()
