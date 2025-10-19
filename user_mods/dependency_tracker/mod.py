"""Dependency Tracker plugin for Sims4 Mod Sorter."""

from __future__ import annotations

import json
import threading
import time
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import tkinter as tk
from tkinter import ttk

COLUMN_ID = "dependency"
ICON_MISSING = "\u26a0\ufe0f"
ICON_OK = "\u2705"
SCANNED_EXTS = {".package", ".ts4script"}
DB_PATH = Path(__file__).with_name("known_dependencies.json")

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

DATABASE = {
    "mods": [],
    "frameworks": [],
    "lookup": {},
}
TRACKING_ENABLED = True
LAST_ITEMS: List[object] = []
LATEST_RESULTS: List[Dict[str, object]] = []
RESULT_LOCK = threading.Lock()
CHECK_VAR: Optional[tk.BooleanVar] = None
INFO_LABELS: List[ttk.Label] = []
VIEW_WINDOW: Optional[tk.Toplevel] = None
VIEW_TREE: Optional[ttk.Treeview] = None
VIEW_STATUS: Optional[ttk.Label] = None


def _normalise(raw: Dict[str, object]) -> Dict[str, object]:
    lookup: Dict[str, Dict[str, object]] = {}
    mods: List[Dict[str, object]] = []
    frameworks: List[Dict[str, object]] = []
    for entry in raw.get("frameworks", []):
        identifier = str(entry.get("id") or entry.get("name") or "").strip()
        if not identifier:
            continue
        patterns = tuple(sorted({str(pat).lower().strip() for pat in entry.get("patterns", []) if str(pat).strip()}))
        normalized = {
            "id": identifier,
            "name": str(entry.get("name") or identifier),
            "patterns": patterns,
            "requires": tuple(),
        }
        frameworks.append(normalized)
        lookup[identifier] = normalized
    for entry in raw.get("mods", []):
        identifier = str(entry.get("id") or entry.get("name") or "").strip()
        if not identifier:
            continue
        patterns = tuple(sorted({str(pat).lower().strip() for pat in entry.get("patterns", []) if str(pat).strip()}))
        requires = tuple(str(dep).strip() for dep in entry.get("requires", []) if str(dep).strip())
        normalized = {
            "id": identifier,
            "name": str(entry.get("name") or identifier),
            "patterns": patterns,
            "requires": requires,
        }
        mods.append(normalized)
        lookup[identifier] = normalized
    return {"mods": mods, "frameworks": frameworks, "lookup": lookup}


def _update_info_labels() -> None:
    text = f"Entries: {len(DATABASE['mods'])} mods, {len(DATABASE['frameworks'])} frameworks"
    for label in INFO_LABELS:
        if label.winfo_exists():
            label.configure(text=text)


def _load_database(api) -> None:
    global DATABASE
    try:
        raw = json.loads(DB_PATH.read_text(encoding="utf-8"))
        DATABASE = _normalise(raw)
        api.log(
            f"[Dependency Tracker] Loaded {len(DATABASE['mods'])} mod definition(s) and {len(DATABASE['frameworks'])} framework(s)."
        )
    except FileNotFoundError:
        DATABASE = _normalise(DEFAULT_DB)
        api.log(
            f"[Dependency Tracker] Dependency list missing. Using built-in defaults ({len(DATABASE['mods'])} mods)."
        )
    except Exception as exc:
        DATABASE = _normalise(DEFAULT_DB)
        api.log(
            f"[Dependency Tracker] Failed to parse dependency list ({exc}). Using built-in defaults ({len(DATABASE['mods'])} mods).",
            "warn",
        )
    _update_info_labels()


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


def _clear_item(item: object) -> None:
    setattr(item, "dependency_status", "")
    setattr(item, "dependency_detail", "")
    extras = getattr(item, "extras", None)
    if isinstance(extras, dict):
        extras.pop(COLUMN_ID, None)
    tooltips = getattr(item, "tooltips", None)
    if isinstance(tooltips, dict):
        tooltips.pop(COLUMN_ID, None)


def _apply_result(item: object, entry_ids: List[str], found_ids: set[str]) -> Optional[Dict[str, object]]:
    if not entry_ids:
        return None
    lookup = DATABASE["lookup"]
    details: List[str] = []
    missing_any = False
    seen_mods: set[str] = set()
    for entry_id in entry_ids:
        entry = lookup.get(entry_id)
        if not entry:
            continue
        mod_name = entry["name"]
        if mod_name in seen_mods:
            continue
        seen_mods.add(mod_name)
        requires = entry.get("requires", ())
        if not requires:
            continue
        parts: List[str] = []
        for dep_id in requires:
            dep_entry = lookup.get(dep_id)
            dep_name = dep_entry["name"] if dep_entry else dep_id
            if dep_entry and dep_id in found_ids:
                parts.append(f"{dep_name} (found)")
            else:
                parts.append(f"{dep_name} (missing)")
                missing_any = True
        if parts:
            details.append(f"{mod_name}: {', '.join(parts)}")
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
    return {
        "item": item,
        "detail": detail_text,
        "missing": missing_any,
        "mods": list(seen_mods),
    }


def _analyse(items: List[object]) -> List[Dict[str, object]]:
    if not items:
        return []
    for item in items:
        _clear_item(item)
    found_ids: set[str] = set()
    mod_matches: Dict[int, List[str]] = {}
    frameworks = DATABASE.get("frameworks", [])
    mods = DATABASE.get("mods", [])
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
        for entry in frameworks:
            if _contains_any(name_blob, entry.get("patterns", ())):
                found_ids.add(entry["id"])
        blob_for_mod = combined_blob or name_blob
        for entry in mods:
            if _contains_any(blob_for_mod, entry.get("patterns", ())):
                found_ids.add(entry["id"])
                bucket = mod_matches.setdefault(id(item), [])
                if entry["id"] not in bucket:
                    bucket.append(entry["id"])
    results: List[Dict[str, object]] = []
    for item in items:
        entry_ids = mod_matches.get(id(item), [])
        result = _apply_result(item, entry_ids, found_ids)
        if result:
            results.append(result)
    return results


def _log_summary(api, results: List[Dict[str, object]], duration: float, total_items: int) -> None:
    api.log(f"[Dependency Tracker] Checking {total_items} mod file(s) for dependencies...")
    if results:
        for result in results:
            item = result["item"]
            icon = ICON_MISSING if result["missing"] else ICON_OK
            detail = result["detail"]
            name = getattr(item, "name", "unknown")
            api.log(f"[Dependency Tracker] - {name} \u2192 {detail} {icon}")
    else:
        api.log("[Dependency Tracker] No tracked dependencies detected.")
    api.log(f"[Dependency Tracker] Finished dependency analysis in {duration:.2f}s")


def _ensure_view_window(app: tk.Tk) -> tuple[tk.Toplevel, ttk.Treeview, ttk.Label]:
    global VIEW_WINDOW, VIEW_TREE, VIEW_STATUS
    if VIEW_WINDOW is None or not VIEW_WINDOW.winfo_exists():
        VIEW_WINDOW = tk.Toplevel(app)
        VIEW_WINDOW.title("Dependency Overview")
        VIEW_WINDOW.geometry("720x420")
        VIEW_WINDOW.transient(app)
        container = ttk.Frame(VIEW_WINDOW, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)
        tree = ttk.Treeview(container, columns=("file", "status", "detail"), show="headings", selectmode="browse")
        tree.heading("file", text="Mod")
        tree.heading("status", text="Status")
        tree.heading("detail", text="Dependencies")
        tree.column("file", width=220, anchor="w")
        tree.column("status", width=120, anchor="center")
        tree.column("detail", width=340, anchor="w")
        yscroll = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=yscroll.set)
        tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        status = ttk.Label(container, text="")
        status.grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))
        tree.tag_configure("missing", foreground="#d9534f")
        tree.tag_configure("ok", foreground="#2e7d32")

        def on_close() -> None:
            if VIEW_WINDOW is not None:
                VIEW_WINDOW.withdraw()

        VIEW_WINDOW.protocol("WM_DELETE_WINDOW", on_close)
        VIEW_TREE = tree
        VIEW_STATUS = status
    else:
        VIEW_WINDOW.deiconify()
        VIEW_WINDOW.lift()
    return VIEW_WINDOW, VIEW_TREE, VIEW_STATUS


def _gather_rows() -> tuple[List[Dict[str, str]], str]:
    with RESULT_LOCK:
        items = list(LAST_ITEMS)
    rows: List[Dict[str, str]] = []
    missing = 0
    for item in items:
        detail = getattr(item, "dependency_detail", "")
        status = getattr(item, "dependency_status", "")
        if not detail:
            continue
        name = getattr(item, "name", "unknown")
        rel = getattr(item, "relpath", "")
        display = name if not rel else f"{name} ({rel})"
        if status == "missing":
            label = f"{ICON_MISSING} Missing"
            missing += 1
        else:
            label = f"{ICON_OK} Satisfied"
            status = "ok"
        rows.append({"display": display, "status": status, "label": label, "detail": detail})
    rows.sort(key=lambda row: (0 if row["status"] == "missing" else 1, row["display"].lower()))
    if not rows:
        if not TRACKING_ENABLED:
            summary = "Dependency tracking is disabled in settings."
        elif not items:
            summary = "No scan results available yet."
        else:
            summary = "No tracked dependencies detected in the last scan."
    else:
        summary = f"Dependencies tracked for {len(rows)} mod(s). Missing: {missing}."
    return rows, summary


def _show_dependency_panel(app: tk.Tk, api) -> None:
    rows, summary = _gather_rows()

    def render() -> None:
        window, tree, status_label = _ensure_view_window(app)
        if tree is None or status_label is None:
            return
        tree.delete(*tree.get_children())
        for row in rows:
            tags = (row["status"],)
            tree.insert("", "end", values=(row["display"], row["label"], row["detail"]), tags=tags)
        status_label.configure(text=summary)
        window.deiconify()
        window.lift()

    app.after(0, render)


def _reanalyze_async(api, reason: str) -> None:
    with RESULT_LOCK:
        if not LAST_ITEMS:
            return
        items = list(LAST_ITEMS)

    def worker() -> None:
        if not TRACKING_ENABLED:
            for item in items:
                _clear_item(item)
            api.log("[Dependency Tracker] Tracking disabled. Dependency markers cleared.")
            with RESULT_LOCK:
                LATEST_RESULTS.clear()
            api.request_refresh()
            return
        start = time.perf_counter()
        results = _analyse(items)
        duration = time.perf_counter() - start
        api.log(f"[Dependency Tracker] {reason}")
        _log_summary(api, results, duration, len(items))
        with RESULT_LOCK:
            LATEST_RESULTS[:] = list(results)
        api.request_refresh()

    threading.Thread(target=worker, daemon=True).start()


def _build_settings(app, frame, api) -> None:
    global CHECK_VAR
    CHECK_VAR = tk.BooleanVar(master=app, value=TRACKING_ENABLED)

    def on_toggle() -> None:
        global TRACKING_ENABLED
        TRACKING_ENABLED = bool(CHECK_VAR.get())
        state = "enabled" if TRACKING_ENABLED else "disabled"
        api.log(f"[Dependency Tracker] Tracking {state}.")
        if TRACKING_ENABLED:
            _reanalyze_async(api, "Rechecking dependencies after enabling tracking...")
        else:
            _reanalyze_async(api, "Clearing dependency markers...")

    def on_reload() -> None:
        _load_database(api)
        if TRACKING_ENABLED:
            _reanalyze_async(api, "Refreshing dependency markers after reloading definitions...")
        else:
            api.request_refresh()

    ttk.Checkbutton(
        frame,
        text="Enable Dependency Tracking",
        variable=CHECK_VAR,
        command=on_toggle,
    ).grid(row=0, column=0, sticky="w")
    ttk.Button(frame, text="Reload Dependency List", command=on_reload).grid(row=1, column=0, sticky="w", pady=(6, 0))
    info = ttk.Label(frame, text="")
    info.grid(row=2, column=0, sticky="w", pady=(6, 0))
    INFO_LABELS.append(info)
    _update_info_labels()


def register(api) -> None:
    _load_database(api)
    api.register_column(COLUMN_ID, "Deps", width=64, anchor="center")
    api.update_column(COLUMN_ID, heading="Dependencies", width=90)
    api.register_settings_section("Dependency Tracker", _build_settings)
    api.register_button(
        "dependency_view",
        "View Dependencies",
        _show_dependency_panel,
        location="right",
        tooltip="Show dependency requirements detected during the last scan.",
    )

    def post_scan(items, context, _api) -> None:
        with RESULT_LOCK:
            LAST_ITEMS[:] = list(items)
            if not TRACKING_ENABLED:
                LATEST_RESULTS.clear()
        if not TRACKING_ENABLED:
            for item in items:
                _clear_item(item)
            return
        start = time.perf_counter()
        results = _analyse(items)
        duration = time.perf_counter() - start
        _log_summary(api, results, duration, len(items))
        with RESULT_LOCK:
            LATEST_RESULTS[:] = list(results)

    api.register_post_scan_hook(post_scan)
    api.log("[Dependency Tracker] Plugin initialised.")
