"""Modern Column Customizer plugin for Sims4 Mod Sorter."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import tkinter as tk
from tkinter import messagebox, ttk

CONFIG_PATH = Path(__file__).with_name("column_layout.json")


@dataclass
class ColumnState:
    column_id: str
    label: str
    visible: bool = True
    width: int = 80

    @classmethod
    def from_dict(cls, payload: Dict[str, object]) -> Optional["ColumnState"]:
        if not isinstance(payload, dict):
            return None
        column_id = str(payload.get("id") or payload.get("column_id") or "").strip()
        if not column_id:
            return None
        label = str(payload.get("label") or column_id)
        visible = bool(payload.get("visible", True))
        try:
            width = int(payload.get("width", 80))
        except (TypeError, ValueError):
            width = 80
        return cls(column_id=column_id, label=label, visible=visible, width=max(20, width))

    def to_dict(self) -> Dict[str, object]:
        return {
            "id": self.column_id,
            "label": self.label,
            "visible": self.visible,
            "width": int(self.width),
        }

    def clone(self) -> "ColumnState":
        return ColumnState(self.column_id, self.label, self.visible, int(self.width))


PROFILE_CHOICES = (
    "Custom",
    "Minimal View",
    "Full View",
    "Compact View",
)


SMALL_COLUMNS = {"inc", "linked", "duplicate"}


class ColumnCustomizerPlugin:
    def __init__(self, api) -> None:
        self.api = api
        self.config_path = CONFIG_PATH
        self.layout: List[ColumnState] = []
        self._default_layout: List[ColumnState] = []
        self._working_layout: List[ColumnState] = []
        self._app: Optional[tk.Tk] = None
        self._window: Optional[tk.Toplevel] = None
        self._tree: Optional[ttk.Treeview] = None
        self._status_var: Optional[tk.StringVar] = None
        self._profile_var: Optional[tk.StringVar] = None
        self._buttons: Dict[str, ttk.Button] = {}
        self._settings_status_var: Optional[tk.StringVar] = None
        self._load_layout()
        self._start_app_monitor()
        self.api.register_toolbar_button(
            "modern-column-customizer",
            text="⚙️ Customize Columns",
            command=self._open_from_toolbar,
            side="sidebar",
            padx=6,
        )
        self.api.register_settings_section("Column Layout", self._build_settings_section)

    def _start_app_monitor(self) -> None:
        def worker() -> None:
            while True:
                app = self.api.app
                if app is not None and hasattr(app, "tree"):
                    self._app = app
                    app.after(0, self._on_app_ready)
                    break
                time.sleep(0.2)

        threading.Thread(target=worker, daemon=True).start()

    def _schedule_on_app(self, callback) -> bool:
        app = self._app or self.api.app
        if app is None:
            return False
        try:
            app.after(0, callback)
        except Exception:
            return False
        return True

    def _on_app_ready(self) -> None:
        self._refresh_layout_from_app()
        self.apply_layout()
        self._capture_current_widths()
        self._update_settings_status("Applied saved column layout.")
        self.api.log("[Column Customizer] Layout initialized.")

    def _build_settings_section(self, app: tk.Tk, frame: ttk.Frame, _api) -> None:
        frame.columnconfigure(0, weight=1)

        ttk.Label(
            frame,
            text="Control which columns appear in the main table and restore defaults if needed.",
            wraplength=360,
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 8))

        ttk.Button(
            frame,
            text="Open Column Customizer",
            command=lambda: self.show_dialog(app),
        ).grid(row=1, column=0, sticky="w")

        ttk.Button(
            frame,
            text="Apply Saved Layout",
            command=self._apply_saved_from_settings,
        ).grid(row=2, column=0, sticky="w", pady=(6, 0))

        ttk.Button(
            frame,
            text="Restore Default Layout",
            command=self._reset_from_settings,
        ).grid(row=3, column=0, sticky="w", pady=(6, 0))

        self._settings_status_var = tk.StringVar(app, value="")
        ttk.Label(frame, textvariable=self._settings_status_var).grid(
            row=4, column=0, sticky="w", pady=(8, 0)
        )

    def _apply_saved_from_settings(self) -> None:
        def _run() -> None:
            self._refresh_layout_from_app()
            if not self.layout:
                self._update_settings_status("No saved layout available yet.")
                return
            self.apply_layout()
            self._capture_current_widths()
            self._update_settings_status("Applied saved column layout.")

        if not self._schedule_on_app(_run):
            self._update_settings_status("App UI is not ready yet.")

    def _reset_from_settings(self) -> None:
        def _run() -> None:
            if not self._default_layout:
                self._refresh_layout_from_app()
            if not self._default_layout:
                self._update_settings_status("Unable to determine default layout.")
                return
            self.layout = [state.clone() for state in self._default_layout]
            self.apply_layout()
            self._capture_current_widths()
            if self._save_layout():
                self._update_settings_status("Restored default column layout.")
            else:
                self._update_settings_status("Unable to save column layout.")

        if not self._schedule_on_app(_run):
            self._update_settings_status("App UI is not ready yet.")

    def _update_settings_status(self, message: str) -> None:
        if self._settings_status_var is not None:
            self._settings_status_var.set(message)
        
    def _load_layout(self) -> None:
        if not self.config_path.exists():
            self.layout = []
            return
        try:
            payload = json.loads(self.config_path.read_text(encoding="utf-8"))
        except Exception as exc:
            self.api.log(f"[Column Customizer] Failed to read layout: {exc}", level="warn")
            self.layout = []
            return
        columns = payload.get("columns") if isinstance(payload, dict) else None
        if not isinstance(columns, list):
            self.layout = []
            return
        parsed: List[ColumnState] = []
        for entry in columns:
            state = ColumnState.from_dict(entry)
            if state:
                parsed.append(state)
        self.layout = parsed

    def _save_layout(self) -> bool:
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            data = {"columns": [state.to_dict() for state in self.layout]}
            self.config_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            return True
        except Exception as exc:
            self.api.log(f"[Column Customizer] Failed to write layout: {exc}", level="error")
            if self._window and self._window.winfo_exists():
                try:
                    messagebox.showerror("Column Customizer", f"Unable to save layout: {exc}", parent=self._window)
                except Exception:
                    pass
            return False

    def _refresh_layout_from_app(self) -> None:
        app = self._app or self.api.app
        if app is None or not hasattr(app, "tree"):
            return
        tree = app.tree
        try:
            columns = list(tree["columns"])
        except Exception:
            return
        try:
            display_columns = list(tree["displaycolumns"])
        except Exception:
            display_columns = list(columns)
        if not display_columns or display_columns == ["#all"]:
            display_columns = list(columns)
        existing = {state.column_id: state for state in self.layout}
        updated: List[ColumnState] = []
        seen = set()
        for column_id in columns:
            heading = tree.heading(column_id).get("text", column_id)
            state = existing.get(column_id)
            try:
                width_value = int(tree.column(column_id).get("width", state.width if state else 80))
            except Exception:
                width_value = state.width if state else 80
            if width_value <= 0 and state:
                width_value = state.width
            visible = state.visible if state else column_id in display_columns
            updated.append(ColumnState(column_id, heading, visible, max(20, width_value or 80)))
            seen.add(column_id)
        for column_id, state in existing.items():
            if column_id not in seen:
                updated.append(state.clone())
        self.layout = updated
        if not self._default_layout:
            base_visible = set(display_columns or columns)
            defaults: List[ColumnState] = []
            for column_id in columns:
                heading = tree.heading(column_id).get("text", column_id)
                try:
                    width_value = int(tree.column(column_id).get("width", 80))
                except Exception:
                    width_value = 80
                defaults.append(ColumnState(column_id, heading, column_id in base_visible, max(20, width_value or 80)))
            self._default_layout = defaults

    def apply_layout(self) -> None:
        app = self._app or self.api.app
        if app is None or not hasattr(app, "tree"):
            return
        tree = app.tree
        if not self.layout:
            self._refresh_layout_from_app()
        if not self.layout:
            return
        columns_in_tree = list(tree["columns"])
        order = [state.column_id for state in self.layout if state.column_id in columns_in_tree]
        if not order:
            order = columns_in_tree
        visible = [state.column_id for state in self.layout if state.visible and state.column_id in order]
        if not visible:
            visible = order
            for state in self.layout:
                if state.column_id in visible:
                    state.visible = True
        headings = {column_id: tree.heading(column_id).get("text", column_id) for column_id in columns_in_tree}
        anchors = dict(getattr(app, "_column_anchors", {}))
        tree.configure(columns=tuple(order), displaycolumns=tuple(visible))
        app._column_order = list(order)
        for state in self.layout:
            if state.column_id not in order:
                continue
            heading_text = headings.get(state.column_id, state.label)
            anchor = anchors.get(state.column_id, tree.column(state.column_id).get("anchor", "w"))
            if state.visible:
                base_min = 36 if state.column_id in SMALL_COLUMNS else 60
                width_value = max(base_min, int(state.width or 0))
                tree.column(
                    state.column_id,
                    width=width_value,
                    minwidth=base_min,
                    stretch=False,
                    anchor=anchor,
                )
            else:
                tree.column(state.column_id, width=0, minwidth=0, stretch=False, anchor=anchor)
            tree.heading(state.column_id, text=heading_text)
        refresh = getattr(app, "_refresh_tree", None)
        if callable(refresh):
            refresh(preserve_selection=True)

    def _capture_current_widths(self) -> None:
        app = self._app or self.api.app
        if app is None or not hasattr(app, "tree"):
            return
        tree = app.tree
        for state in self.layout:
            if state.column_id not in tree["columns"]:
                continue
            try:
                width_value = int(tree.column(state.column_id).get("width", state.width))
            except Exception:
                width_value = state.width
            if width_value > 0:
                state.width = width_value

    def _open_from_toolbar(self, app, _api) -> None:
        self.show_dialog(app)

    def show_dialog(self, app) -> None:
        if self._window and self._window.winfo_exists():
            self._window.lift()
            return
        self._app = app
        self._refresh_layout_from_app()
        self._working_layout = [state.clone() for state in self.layout]

        palette = getattr(app, "_theme_cache", {
            "bg": "#1e1e1e",
            "fg": "#f2f2f7",
            "alt": "#2b2b2b",
            "sel": "#3a3a3a",
        })
        window = tk.Toplevel(app)
        window.title("Customize Columns")
        window.transient(app)
        window.grab_set()
        window.configure(bg=palette.get("bg", "#1e1e1e"))
        window.resizable(False, True)
        window.geometry("520x520")
        window.focus_set()
        window.protocol("WM_DELETE_WINDOW", self._close_dialog)
        self._window = window

        style = ttk.Style(window)
        style.configure("MCC.TFrame", background=palette.get("bg", "#1e1e1e"))
        style.configure("MCC.TLabel", background=palette.get("bg", "#1e1e1e"), foreground=palette.get("fg", "#f2f2f7"))
        style.configure("MCC.Heading.TLabel", background=palette.get("bg", "#1e1e1e"), foreground=palette.get("fg", "#f2f2f7"), font=("Segoe UI", 14, "bold"))
        style.configure("MCC.TButton", background=palette.get("alt", "#2b2b2b"), foreground=palette.get("fg", "#f2f2f7"), padding=6)
        style.map("MCC.TButton", background=[("active", palette.get("sel", "#3a3a3a"))])
        style.configure("MCC.Treeview", background=palette.get("alt", "#2b2b2b"), foreground=palette.get("fg", "#f2f2f7"), fieldbackground=palette.get("alt", "#2b2b2b"), rowheight=28, bordercolor=palette.get("bg", "#1e1e1e"), borderwidth=0)
        style.map("MCC.Treeview", background=[("selected", palette.get("sel", "#3a3a3a"))])
        style.configure("MCC.Treeview.Heading", background=palette.get("bg", "#1e1e1e"), foreground=palette.get("fg", "#f2f2f7"))

        container = ttk.Frame(window, padding=16, style="MCC.TFrame")
        container.pack(fill="both", expand=True)

        ttk.Label(container, text="Manage Visible Columns", style="MCC.Heading.TLabel").pack(anchor="w")
        ttk.Label(
            container,
            text="Customize which columns appear in the mod list and adjust their order.",
            style="MCC.TLabel",
        ).pack(anchor="w", pady=(4, 12))

        tree_frame = ttk.Frame(container, style="MCC.TFrame")
        tree_frame.pack(fill="both", expand=True)
        columns = ("label", "status")
        self._tree = ttk.Treeview(
            tree_frame,
            columns=columns,
            show="headings",
            selectmode="browse",
            height=12,
            style="MCC.Treeview",
        )
        self._tree.heading("label", text="Column")
        self._tree.heading("status", text="Visibility")
        self._tree.column("label", width=260, anchor="w")
        self._tree.column("status", width=120, anchor="center")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        self._tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        self._tree.bind("<<TreeviewSelect>>", lambda _e: self._update_button_states())
        self._tree.bind("<Double-1>", lambda _e: self._toggle_selected())

        controls = ttk.Frame(container, style="MCC.TFrame")
        controls.pack(fill="x", pady=(12, 4))
        self._buttons["move_up"] = ttk.Button(controls, text="🔼 Move Up", command=lambda: self._move_selected(-1), style="MCC.TButton")
        self._buttons["move_up"].pack(side="left")
        self._buttons["move_down"] = ttk.Button(controls, text="🔽 Move Down", command=lambda: self._move_selected(1), style="MCC.TButton")
        self._buttons["move_down"].pack(side="left", padx=(8, 0))
        self._buttons["toggle"] = ttk.Button(controls, text="Toggle Visibility", command=self._toggle_selected, style="MCC.TButton")
        self._buttons["toggle"].pack(side="left", padx=(8, 0))

        profile_frame = ttk.Frame(container, style="MCC.TFrame")
        profile_frame.pack(fill="x", pady=(4, 12))
        ttk.Label(profile_frame, text="Quick Profiles", style="MCC.TLabel").pack(side="left")
        self._profile_var = tk.StringVar(window, value=PROFILE_CHOICES[0])
        profile_combo = ttk.Combobox(
            profile_frame,
            textvariable=self._profile_var,
            state="readonly",
            values=PROFILE_CHOICES,
        )
        profile_combo.pack(side="left", padx=(8, 0))
        profile_combo.bind("<<ComboboxSelected>>", self._on_profile_selected)

        action_frame = ttk.Frame(container, style="MCC.TFrame")
        action_frame.pack(fill="x", pady=(4, 0))
        ttk.Button(action_frame, text="💾 Save Layout", command=self._on_save, style="MCC.TButton").pack(side="right")
        ttk.Button(action_frame, text="Reset to Default", command=self._on_reset, style="MCC.TButton").pack(side="right", padx=(8, 0))
        ttk.Button(action_frame, text="Close", command=self._close_dialog, style="MCC.TButton").pack(side="left")

        self._status_var = tk.StringVar(window, value="")
        ttk.Label(container, textvariable=self._status_var, style="MCC.TLabel").pack(anchor="w", pady=(12, 0))

        self._populate_tree()
        self._update_button_states()

    def _close_dialog(self) -> None:
        if self._window and self._window.winfo_exists():
            self._window.destroy()
        self._window = None
        self._tree = None
        self._status_var = None
        self._profile_var = None
        self._buttons.clear()
        self._working_layout = []

    def _populate_tree(self, *, select: Optional[str] = None) -> None:
        if not self._tree:
            return
        self._tree.delete(*self._tree.get_children())
        for state in self._working_layout:
            status = "🟢 Visible" if state.visible else "🔴 Hidden"
            self._tree.insert("", "end", iid=state.column_id, values=(state.label, status))
        target = select or (self._working_layout[0].column_id if self._working_layout else None)
        if target and target in self._tree.get_children(""):
            self._tree.selection_set(target)
            self._tree.focus(target)

    def _selected_index(self) -> Optional[int]:
        if not self._tree:
            return None
        selection = self._tree.selection()
        if not selection:
            return None
        column_id = selection[0]
        for index, state in enumerate(self._working_layout):
            if state.column_id == column_id:
                return index
        return None

    def _move_selected(self, delta: int) -> None:
        index = self._selected_index()
        if index is None:
            return
        new_index = index + delta
        if new_index < 0 or new_index >= len(self._working_layout):
            return
        self._working_layout[index], self._working_layout[new_index] = (
            self._working_layout[new_index],
            self._working_layout[index],
        )
        self._populate_tree(select=self._working_layout[new_index].column_id)
        if self._profile_var:
            self._profile_var.set(PROFILE_CHOICES[0])
        if self._status_var:
            self._status_var.set("Reordered column. Save to apply changes.")
        self._update_button_states()

    def _toggle_selected(self) -> None:
        index = self._selected_index()
        if index is None:
            return
        state = self._working_layout[index]
        state.visible = not state.visible
        self._populate_tree(select=state.column_id)
        if self._profile_var:
            self._profile_var.set(PROFILE_CHOICES[0])
        if self._status_var:
            status = "visible" if state.visible else "hidden"
            self._status_var.set(f"Marked '{state.label}' as {status}. Save to apply changes.")
        self._update_button_states()

    def _update_button_states(self) -> None:
        index = self._selected_index()
        total = len(self._working_layout)
        for button in self._buttons.values():
            if button and button.winfo_exists():
                button.configure(state="normal")
        if index is None:
            for key in ("move_up", "move_down", "toggle"):
                button = self._buttons.get(key)
                if button:
                    button.configure(state="disabled")
            return
        if index <= 0 and self._buttons.get("move_up"):
            self._buttons["move_up"].configure(state="disabled")
        if index >= total - 1 and self._buttons.get("move_down"):
            self._buttons["move_down"].configure(state="disabled")

    def _on_profile_selected(self, _event) -> None:
        if not self._profile_var:
            return
        choice = self._profile_var.get()
        if choice == PROFILE_CHOICES[0]:
            return
        if choice == "Full View":
            for state in self._working_layout:
                state.visible = True
        elif choice == "Minimal View":
            keep = {"name", "type", "rel"}
            self._reorder_working_layout(["name", "type", "rel"])
            for state in self._working_layout:
                state.visible = state.column_id in keep
        elif choice == "Compact View":
            for state in self._working_layout:
                state.visible = state.column_id != "rel"
        if self._status_var:
            self._status_var.set(f"Applied {choice}. Save to apply changes.")
        self._populate_tree()
        self._update_button_states()

    def _reorder_working_layout(self, priority: List[str]) -> None:
        mapping = {state.column_id: state for state in self._working_layout}
        ordered: List[ColumnState] = []
        seen = set()
        for column_id in priority:
            state = mapping.get(column_id)
            if state:
                ordered.append(state)
                seen.add(column_id)
        for state in self._working_layout:
            if state.column_id not in seen:
                ordered.append(state)
        self._working_layout = ordered

    def _on_reset(self) -> None:
        if not self._default_layout:
            return
        self._working_layout = [state.clone() for state in self._default_layout]
        if self._profile_var:
            self._profile_var.set(PROFILE_CHOICES[0])
        if self._status_var:
            self._status_var.set("Restored default layout. Save to apply changes.")
        self._populate_tree()
        self._update_button_states()

    def _on_save(self) -> None:
        if not self._working_layout:
            return
        self.layout = [state.clone() for state in self._working_layout]
        self.apply_layout()
        self._capture_current_widths()
        if self._save_layout():
            if self._status_var:
                self._status_var.set("Layout saved and applied.")
            self.api.log("[Column Customizer] Saved column layout.")
            if self._profile_var:
                self._profile_var.set(PROFILE_CHOICES[0])
            self._update_settings_status("Layout saved and applied.")
        else:
            self._update_settings_status("Unable to save column layout.")


def register(api) -> None:
    ColumnCustomizerPlugin(api)
