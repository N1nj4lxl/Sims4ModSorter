"""Dependency Tracker plugin for Sims4 Mod Sorter (v1.1.0)."""

from __future__ import annotations

import json
import threading
import time
import webbrowser
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
            "download_url": "https://www.patreon.com/posts/ui-cheats-extension-26240068",
        },
        {
            "id": "wickedwhims",
            "name": "WickedWhims",
            "patterns": ["wickedwhims", "wicked whims", "turbodriver"],
            "requires": ["basemental_drugs"],
            "homepage": "https://wickedwhimsmod.com/",
        },
        {
            "id": "better_school_grades",
            "name": "Better School Grades",
            "patterns": ["better school grades"],
            "requires": ["xml_injector"],
            "homepage": "https://littlemssam.tumblr.com/post/625676532776542208/better-school-grades",
        },
        {
            "id": "slice_of_life",
            "name": "Slice of Life",
            "patterns": ["slice of life", "kawaiistacie slice"],
            "requires": ["xml_injector"],
            "homepage": "https://www.kawaiistaciemods.com/slice-of-life",
        },
    ],
    "frameworks": [
        {
            "id": "mc_command_center",
            "name": "MC Command Center",
            "patterns": ["mc command center", "mccc", "deaderpool"],
            "homepage": "https://deaderpool-mccc.com/#/releases",
        },
        {
            "id": "xml_injector",
            "name": "XML Injector",
            "patterns": ["xml injector"],
            "homepage": "https://scumbumbomods.com/xml-injector/",
        },
        {
            "id": "basemental_drugs",
            "name": "Basemental Drugs",
            "patterns": ["basemental drugs", "basementaldrugs"],
            "homepage": "https://basementalcc.com/drugs",
        },
        {
            "id": "ts4_script_loader",
            "name": "TS4 Script Loader",
            "patterns": ["ts4 script loader", "ts4scriptloader"],
            "homepage": "https://modthesims.info/d/479997/ts4-script-loader.html",
        },
    ],
}


@dataclass(frozen=True)
class DependencyDefinition:
    identifier: str
    name: str
    patterns: Tuple[str, ...]
    requires: Tuple[str, ...] = tuple()
    homepage: Optional[str] = None
    download_url: Optional[str] = None

    def matches(self, blob: str) -> bool:
        if not blob:
            return False
        return any(pattern in blob for pattern in self.patterns)

    @property
    def primary_url(self) -> Optional[str]:
        return self.download_url or self.homepage


@dataclass(frozen=True, slots=True)
class RequirementDetail:
    mod: str
    dependency: str
    missing: bool
    url: Optional[str]


@dataclass(slots=True)
class DependencyResult:
    item: object
    mods: Tuple[str, ...]
    detail: str
    missing: bool
    requirements: Tuple[RequirementDetail, ...]


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

        def _clean_url(value: object) -> Optional[str]:
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        def normalise(entry: Dict[str, object], *, requires: Sequence[str]) -> Optional[DependencyDefinition]:
            identifier = str(entry.get("id") or entry.get("name") or "").strip()
            if not identifier:
                return None
            name = str(entry.get("name") or identifier).strip()
            patterns = tuple(sorted({_lower_clean(str(pattern)) for pattern in entry.get("patterns", []) if str(pattern).strip()}))
            req = tuple(str(dep).strip() for dep in requires if str(dep).strip())
            homepage = _clean_url(entry.get("homepage"))
            download_url = _clean_url(entry.get("download_url"))
            definition = DependencyDefinition(
                identifier=identifier,
                name=name,
                patterns=patterns,
                requires=req,
                homepage=homepage,
                download_url=download_url,
            )
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
        requirement_rows: List[RequirementDetail] = []
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
                dep_url = dep_entry.primary_url if dep_entry else None
                if dep_entry and dependency_id in found_ids:
                    if dep_url:
                        parts.append(f"{dep_name} (found – {dep_url})")
                    else:
                        parts.append(f"{dep_name} (found)")
                    requirement_rows.append(
                        RequirementDetail(mod=entry.name, dependency=dep_name, missing=False, url=dep_url)
                    )
                else:
                    if dep_url:
                        parts.append(f"{dep_name} (missing – {dep_url})")
                    else:
                        parts.append(f"{dep_name} (missing)")
                    missing_any = True
                    requirement_rows.append(
                        RequirementDetail(mod=entry.name, dependency=dep_name, missing=True, url=dep_url)
                    )
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
            requirements=tuple(requirement_rows),
        )


class DependencyOverlay:
    def __init__(self, api) -> None:
        self.api = api
        self.window: Optional[tk.Toplevel] = None
        self.tree: Optional[ttk.Treeview] = None
        self.status_var: Optional[tk.StringVar] = None
        self._results: List[DependencyResult] = []
        self._item_links: Dict[str, List[RequirementDetail]] = {}
        self._link_container: Optional[ttk.Frame] = None
        self._context_menu: Optional[tk.Menu] = None

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

        tree.bind("<<TreeviewSelect>>", lambda _event: self._update_link_buttons())

        links_frame = ttk.Frame(frame)
        links_frame.pack(fill="x", expand=False, pady=(8, 0))
        ttk.Label(links_frame, text="Missing dependency links:").pack(anchor="w")
        container = ttk.Frame(links_frame)
        container.pack(anchor="w", fill="x")
        self._link_container = container

        context_menu = tk.Menu(tree, tearoff=False)
        self._context_menu = context_menu

        def show_context_menu(event) -> None:
            item_id = tree.identify_row(event.y)
            if not item_id:
                return
            links = [req for req in self._item_links.get(item_id, []) if req.url]
            if not links:
                return
            context_menu.delete(0, "end")
            for requirement in links:
                label = requirement.dependency
                if requirement.mod:
                    label = f"{requirement.dependency} (required by {requirement.mod})"
                context_menu.add_command(
                    label=label,
                    command=lambda url=requirement.url: webbrowser.open(url),
                )
            if context_menu.index("end") is None:
                return
            try:
                context_menu.tk_popup(event.x_root, event.y_root)
            finally:
                context_menu.grab_release()

        tree.bind("<Button-3>", show_context_menu, add="+")

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
        self._item_links.clear()
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
            item_id = tree.insert("", "end", values=(file_name, mods, status, detail))
            links = [req for req in result.requirements if req.missing and req.url]
            if links:
                self._item_links[item_id] = links
        if self.status_var is not None:
            try:
                if total:
                    self.status_var.set(f"Tracked {total} mod(s); {missing} with missing dependencies")
                else:
                    self.status_var.set("No dependencies detected.")
            except tk.TclError:
                pass
        self._update_link_buttons()

    def _update_link_buttons(self) -> None:
        container = self._link_container
        tree = self.tree
        if container is None or tree is None or not tree.winfo_exists():
            return
        for child in container.winfo_children():
            child.destroy()
        selection = tree.selection()
        if not selection:
            ttk.Label(container, text="Select a row to view available downloads.").pack(anchor="w")
            return
        item_id = selection[0]
        links = self._item_links.get(item_id, [])
        if not links:
            ttk.Label(container, text="No URLs available for the selected item.").pack(anchor="w")
            return
        for requirement in links:
            label = requirement.dependency
            if requirement.mod:
                label = f"{requirement.dependency} (required by {requirement.mod})"
            ttk.Button(
                container,
                text=label,
                command=lambda url=requirement.url: webbrowser.open(url),
            ).pack(anchor="w", pady=(0, 4))


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
        self._command_center_labels: List[ttk.Label] = []

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
        self.api.register_command_center_panel(
            "dependency-alerts",
            "Dependency alerts",
            self._build_command_center_panel,
            priority=40,
        )

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
            self._refresh_command_center_summary()
            self.api.refresh_command_center()
            return
        scan_metrics.start("Dependency Tracker")
        start = time.perf_counter()
        scan_metrics.start("Dependency Tracker")
        scan_metrics.log(
            "Dependency Tracker scanning files…", plugin="Dependency Tracker"
        )
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
        self._refresh_command_center_summary()
        self.api.refresh_command_center()
        warnings = self._log_summary(results, duration, len(self.last_items), track_metrics=True)
        if warnings:
            detail = ", ".join(sorted({result.detail for result in results if result.missing}))
            if detail:
                scan_metrics.log(
                    f"Missing dependencies detected for {warnings} file(s): {detail}",
                    level="warn",
                    plugin="Dependency Tracker",
                )

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
                self._refresh_command_center_summary()
                self.api.refresh_command_center()
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
            self._log_summary(results, duration, len(items), track_metrics=False)
            self._refresh_command_center_summary()
            self.api.refresh_command_center()

        threading.Thread(target=worker, daemon=True).start()

    # ------------------------------------------------------------------
    def _log_summary(
        self,
        results: Sequence[DependencyResult],
        duration: float,
        total_items: int,
        *,
        track_metrics: bool,
    ) -> int:
        self.api.log(f"[Dependency Tracker] Checking {total_items} mod file(s) for dependencies...")
        missing_count = 0
        if results:
            for result in results:
                item = result.item
                icon = ICON_MISSING if result.missing else ICON_OK
                detail = result.detail
                name = getattr(item, "name", "unknown")
                self.api.log(f"[Dependency Tracker] - {name} \u2192 {detail} {icon}")
                if result.missing:
                    missing_count += 1
        else:
            self.api.log("[Dependency Tracker] No tracked dependencies detected.")
        self.api.log(f"[Dependency Tracker] Finished dependency analysis in {duration:.2f}s")
        if track_metrics:
            scan_metrics.stop(
                "Dependency Tracker",
                files_processed=total_items,
                warnings=missing_count,
                status="Warning" if missing_count else "Done",
            )
            scan_metrics.log(
                f"Dependency analysis complete in {duration:.2f}s", plugin="Dependency Tracker"
            )
        return missing_count

    def _build_command_center_panel(self, app: tk.Tk, frame: ttk.Frame, _api) -> None:
        frame.columnconfigure(0, weight=1)
        summary = ttk.Label(frame, text="", wraplength=360, justify="left")
        summary.grid(row=0, column=0, sticky="w")
        ttk.Button(frame, text="Open dependency overlay", command=self.overlay.show).grid(
            row=1, column=0, sticky="w", pady=(8, 0)
        )
        self._command_center_labels.append(summary)
        self._refresh_command_center_summary()

        def _cleanup(_event: tk.Event) -> None:
            if summary in self._command_center_labels:
                self._command_center_labels.remove(summary)

        frame.bind("<Destroy>", _cleanup, add="+")

    def _refresh_command_center_summary(self) -> None:
        if not self._command_center_labels:
            return
        if not self.last_results:
            message = "Run a scan to audit dependencies."
        else:
            missing = {
                requirement.dependency
                for result in self.last_results
                for requirement in result.requirements
                if requirement.missing
            }
            if missing:
                names = ", ".join(sorted(missing))
                message = f"Missing dependencies detected: {names}"
            else:
                message = "All tracked dependencies are satisfied."
        for label in list(self._command_center_labels):
            if not label.winfo_exists():
                try:
                    self._command_center_labels.remove(label)
                except ValueError:
                    pass
                continue
            label.configure(text=message)


PLUGIN: Optional[DependencyPlugin] = None


def register(api) -> None:
    global PLUGIN
    PLUGIN = DependencyPlugin(api)
    PLUGIN.register()
