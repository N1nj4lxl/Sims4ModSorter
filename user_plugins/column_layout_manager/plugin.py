from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import tkinter as tk
from tkinter import ttk


@dataclass(frozen=True)
class _ColumnDefaults:
    column_id: str
    heading: str
    width: int
    anchor: str


@dataclass
class _ColumnState:
    column_id: str
    heading: str
    visible: bool
    width: int
    anchor: str


@dataclass
class _LayoutModel:
    order: List[str]
    columns: List[_ColumnState]
    preset: str


_PRESET_COMPACT_ORDER = ["inc", "name", "size", "type", "target", "conf", "linked"]
_ANCHOR_CHOICES = {
    "Left": "w",
    "Centre": "center",
    "Right": "e",
}
_REVERSE_ANCHORS = {value: key for key, value in _ANCHOR_CHOICES.items()}


class ColumnLayoutManagerPlugin:
    """Interactive column layout manager for the Sims 4 Mod Sorter tree view."""

    _CONFIG_VERSION = 1
    _PRESETS = ("Default", "Compact", "Everything")

    def __init__(self, api) -> None:
        self.api = api
        self._config_path = Path(__file__).with_name("layout.json")
        self._state = self._load_state()
        self._defaults: Dict[str, _ColumnDefaults] = {}
        self._default_order: List[str] = []
        self._default_display: List[str] = []
        self._initialised = False
        self._dialog_editor: Optional[_ColumnLayoutEditor] = None
        self._stop_event = threading.Event()
        self._watcher: Optional[threading.Thread] = None
        self._editors: List[_ColumnLayoutEditor] = []

    # ------------------------------------------------------------------
    # Plugin lifecycle
    # ------------------------------------------------------------------
    def register(self) -> None:
        self.api.register_toolbar_button(
            "column-layout-manager",
            text="Columns",
            tooltip="Manage column order and visibility",
            command=lambda app, _api: self._open_dialog(app),
            side="right",
            insert_before="settings",
            padx=4,
        )
        self.api.register_settings_section("Column Layout", self._build_settings_section)
        self._start_app_listener()
        self.api.log("[Column Layout] Manager registered.")

    # ------------------------------------------------------------------
    def _start_app_listener(self) -> None:
        if self._watcher and self._watcher.is_alive():
            return
        self._watcher = threading.Thread(target=self._wait_for_app, name="ColumnLayoutInit", daemon=True)
        self._watcher.start()

    def _wait_for_app(self) -> None:
        while not self._stop_event.is_set():
            app = getattr(self.api, "app", None)
            if app is None:
                time.sleep(0.2)
                continue
            try:
                app.after(0, lambda app=app: self._initialise_with_app(app))
            except Exception:
                self._initialise_with_app(app)
            return

    # ------------------------------------------------------------------
    def _initialise_with_app(self, app) -> None:
        if self._initialised:
            self._ensure_tree_defaults(app)
            self._apply_preferences(app)
            return
        tree = getattr(app, "tree", None)
        if tree is None:
            try:
                app.after(150, lambda app=app: self._initialise_with_app(app))
            except Exception:
                pass
            return
        try:
            tree_columns = list(tree["columns"])
        except Exception:
            try:
                app.after(150, lambda app=app: self._initialise_with_app(app))
            except Exception:
                pass
            return
        if not tree_columns:
            try:
                app.after(150, lambda app=app: self._initialise_with_app(app))
            except Exception:
                pass
            return
        self._ensure_tree_defaults(app)
        self._apply_preferences(app)
        self._initialised = True

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------
    def _load_state(self) -> Dict[str, object]:
        if not self._config_path.exists():
            return {
                "version": self._CONFIG_VERSION,
                "order": [],
                "display": [],
                "widths": {},
                "anchors": {},
                "preset": "Default",
            }
        try:
            payload = json.loads(self._config_path.read_text(encoding="utf-8"))
        except Exception:
            return {
                "version": self._CONFIG_VERSION,
                "order": [],
                "display": [],
                "widths": {},
                "anchors": {},
                "preset": "Default",
            }
        if not isinstance(payload, dict):
            return {
                "version": self._CONFIG_VERSION,
                "order": [],
                "display": [],
                "widths": {},
                "anchors": {},
                "preset": "Default",
            }
        return {
            "version": int(payload.get("version", self._CONFIG_VERSION)),
            "order": list(payload.get("order", [])) if isinstance(payload.get("order"), list) else [],
            "display": list(payload.get("display", [])) if isinstance(payload.get("display"), list) else [],
            "widths": dict(payload.get("widths", {})) if isinstance(payload.get("widths"), dict) else {},
            "anchors": dict(payload.get("anchors", {})) if isinstance(payload.get("anchors"), dict) else {},
            "preset": str(payload.get("preset", "Default")),
        }

    def _save_state(self) -> None:
        payload = {
            "version": self._CONFIG_VERSION,
            "order": self._state.get("order", []),
            "display": self._state.get("display", []),
            "widths": self._state.get("widths", {}),
            "anchors": self._state.get("anchors", {}),
            "preset": self._state.get("preset", "Default"),
        }
        try:
            self._config_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except Exception as exc:
            self.api.log(f"[Column Layout] Failed to write config: {exc}", level="warn")

    # ------------------------------------------------------------------
    # Column discovery & application
    # ------------------------------------------------------------------
    def _ensure_tree_defaults(self, app) -> None:
        tree = getattr(app, "tree", None)
        if tree is None:
            return
        try:
            columns = list(tree["columns"])
        except Exception:
            columns = []
        display = []
        try:
            display_value = tree["displaycolumns"]
            if isinstance(display_value, (list, tuple)):
                display = [col for col in display_value if col in columns]
        except Exception:
            display = []
        if not self._defaults:
            for column in columns:
                heading = self._safe_heading(tree, column)
                width, anchor = self._safe_column_geometry(tree, column)
                self._defaults[column] = _ColumnDefaults(column, heading, width, anchor)
            self._default_order = list(columns)
            self._default_display = list(display) if display else list(columns)
        else:
            for column in columns:
                if column not in self._defaults:
                    heading = self._safe_heading(tree, column)
                    width, anchor = self._safe_column_geometry(tree, column)
                    self._defaults[column] = _ColumnDefaults(column, heading, width, anchor)
                    self._default_order.append(column)
                    if column not in self._default_display:
                        self._default_display.append(column)
            if display:
                for column in display:
                    if column not in self._default_display:
                        self._default_display.append(column)

    def _safe_heading(self, tree, column: str) -> str:
        try:
            heading = tree.heading(column).get("text", column)
        except Exception:
            heading = column
        return heading or column

    def _safe_column_geometry(self, tree, column: str) -> tuple[int, str]:
        width = 80
        anchor = "w"
        try:
            info = tree.column(column)
        except Exception:
            info = {}
        if isinstance(info, dict):
            try:
                width = int(info.get("width", width))
            except (TypeError, ValueError):
                width = 80
            anchor_value = info.get("anchor")
            if isinstance(anchor_value, str) and anchor_value:
                anchor = anchor_value
        return max(24, width), anchor or "w"

    def _normalize_order(self, order: Sequence[str]) -> List[str]:
        known = list(self._defaults.keys())
        normalized: List[str] = [column for column in order if column in self._defaults]
        for column in known:
            if column not in normalized:
                normalized.append(column)
        return normalized

    def _resolve_visible(self, order: Sequence[str]) -> List[str]:
        stored = self._state.get("display")
        visible: List[str]
        if isinstance(stored, list) and stored:
            target = [column for column in stored if column in self._defaults]
            visible = [column for column in order if column in target]
            if not visible:
                visible = list(order)
        else:
            visible = list(order)
        if not visible:
            visible = list(order)
        return visible

    def _resolve_widths(self, order: Iterable[str]) -> Dict[str, int]:
        widths: Dict[str, int] = {}
        stored = self._state.get("widths")
        if isinstance(stored, dict):
            for column, value in stored.items():
                try:
                    widths[column] = max(24, int(value))
                except (TypeError, ValueError):
                    continue
        for column in order:
            if column not in widths:
                default = self._defaults.get(column)
                widths[column] = default.width if default else 80
        return widths

    def _resolve_anchors(self, order: Iterable[str]) -> Dict[str, str]:
        anchors: Dict[str, str] = {}
        stored = self._state.get("anchors")
        if isinstance(stored, dict):
            for column, value in stored.items():
                if isinstance(value, str) and value:
                    anchors[column] = value
        for column in order:
            if column not in anchors:
                default = self._defaults.get(column)
                anchors[column] = default.anchor if default else "w"
        return anchors

    def _apply_preferences(self, app) -> None:
        tree = getattr(app, "tree", None)
        if tree is None or not self._defaults:
            return
        order = self._state.get("order")
        if not isinstance(order, list) or not order:
            order = list(self._default_order)
        order = self._normalize_order(order)
        visible = self._resolve_visible(order)
        widths = self._resolve_widths(order)
        anchors = self._resolve_anchors(order)
        try:
            tree.configure(columns=tuple(order))
            if visible:
                tree.configure(displaycolumns=tuple(visible))
        except Exception:
            pass
        for column in order:
            default = self._defaults.get(column)
            heading = default.heading if default else column
            try:
                tree.heading(column, text=heading)
            except Exception:
                continue
            try:
                tree.column(
                    column,
                    width=max(24, int(widths.get(column, default.width if default else 80))),
                    anchor=anchors.get(column, default.anchor if default else "w") or "w",
                    stretch=False,
                )
            except Exception:
                continue
        try:
            app._column_order = list(order)
        except Exception:
            pass
        anchor_payload: Dict[str, str] = {}
        for column in order:
            if column in anchors and isinstance(anchors[column], str) and anchors[column]:
                anchor_payload[column] = anchors[column]
            else:
                default = self._defaults.get(column)
                anchor_payload[column] = default.anchor if default else "w"
        try:
            app._column_anchors = anchor_payload
        except Exception:
            pass
        self._refresh_editors()

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------
    def update_layout(
        self,
        order: Sequence[str],
        visible: Sequence[str],
        widths: Dict[str, int],
        anchors: Dict[str, str],
        *,
        preset: Optional[str] = None,
    ) -> None:
        normalized_order = self._normalize_order(order)
        visible_set = set(visible)
        visible_columns = [column for column in normalized_order if column in visible_set]
        width_map: Dict[str, int] = {}
        for column in normalized_order:
            fallback = 80
            default = self._defaults.get(column)
            if default is not None:
                fallback = default.width
            value = widths.get(column) if isinstance(widths, dict) else None
            try:
                resolved = int(value if value is not None else fallback)
            except (TypeError, ValueError):
                resolved = fallback
            width_map[column] = max(24, resolved)
        anchor_map: Dict[str, str] = {}
        for column in normalized_order:
            value = anchors.get(column) if isinstance(anchors, dict) else None
            if isinstance(value, str) and value:
                anchor_map[column] = value
            else:
                default = self._defaults.get(column)
                anchor_map[column] = default.anchor if default else "w"
        self._state["order"] = list(normalized_order)
        self._state["display"] = list(visible_columns) if visible_columns else list(normalized_order)
        self._state["widths"] = width_map
        self._state["anchors"] = anchor_map
        chosen = preset if preset in self._PRESETS else "Custom"
        self._state["preset"] = chosen
        self._save_state()
        app = getattr(self.api, "app", None)
        if app is not None:
            try:
                app.after(0, lambda app=app: self._apply_preferences(app))
            except Exception:
                self._apply_preferences(app)
        else:
            self._refresh_editors()

    def apply_preset(self, name: str) -> None:
        if name not in self._PRESETS:
            return
        app = getattr(self.api, "app", None)
        if app is not None:
            self._ensure_tree_defaults(app)
        order = list(self._default_order) if self._default_order else list(self._state.get("order", []))
        if not order:
            order = list(self._defaults.keys())
        if name == "Default":
            visible = list(self._default_display) if self._default_display else list(order)
        elif name == "Compact":
            primary = [column for column in _PRESET_COMPACT_ORDER if column in order]
            order = self._normalize_order(primary)
            visible = [column for column in order if column in primary]
        else:  # Everything
            order = self._normalize_order(order)
            visible = list(order)
        widths: Dict[str, int] = {}
        anchors: Dict[str, str] = {}
        for column in order:
            default = self._defaults.get(column)
            widths[column] = default.width if default else 80
            anchors[column] = default.anchor if default else "w"
        self.update_layout(order, visible, widths, anchors, preset=name)

    def reset_to_defaults(self) -> None:
        self.apply_preset("Default")

    # ------------------------------------------------------------------
    # UI builders
    # ------------------------------------------------------------------
    def _build_settings_section(self, app, container, _api) -> None:
        editor = _ColumnLayoutEditor(self, container, standalone=False)
        self._register_editor(editor)
        editor.frame.pack(fill="both", expand=True)

    def _open_dialog(self, app) -> None:
        def builder(container: ttk.Frame, footer: ttk.Frame) -> None:
            container.columnconfigure(0, weight=1)
            container.rowconfigure(0, weight=1)
            editor = _ColumnLayoutEditor(self, container, standalone=True)
            self._register_editor(editor)
            self._dialog_editor = editor
            editor.frame.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

            ttk.Button(
                footer,
                text="Close",
                command=lambda: self._close_overlay(app, "column_layout_manager"),
            ).pack(side="right")

        def on_hide() -> None:
            if self._dialog_editor is not None:
                self._unregister_editor(self._dialog_editor)
                self._dialog_editor = None

        app._show_overlay_panel(
            "column_layout_manager",
            "Column Layout Manager",
            builder,
            width=720,
            on_hide=on_hide,
        )

    def _close_overlay(self, app, key: str) -> None:
        if self._dialog_editor is not None:
            self._unregister_editor(self._dialog_editor)
            self._dialog_editor = None
        hide = getattr(app, "_hide_overlay_panel", None)
        if callable(hide):
            hide(key)

    def _register_editor(self, editor: "_ColumnLayoutEditor") -> None:
        if editor not in self._editors:
            self._editors.append(editor)
        editor.frame.bind("<Destroy>", lambda _e, ed=editor: self._unregister_editor(ed))
        editor.refresh()

    def _unregister_editor(self, editor: "_ColumnLayoutEditor") -> None:
        try:
            self._editors.remove(editor)
        except ValueError:
            return

    def _refresh_editors(self) -> None:
        for editor in list(self._editors):
            try:
                editor.refresh()
            except Exception:
                continue

    def get_layout_model(self) -> _LayoutModel:
        app = getattr(self.api, "app", None)
        if app is not None and self._defaults:
            self._ensure_tree_defaults(app)
        order = self._state.get("order")
        if not isinstance(order, list) or not order:
            if self._default_order:
                order = list(self._default_order)
            elif self._defaults:
                order = list(self._defaults.keys())
            else:
                order = []
        order = self._normalize_order(order)
        visible = set(self._resolve_visible(order))
        widths = self._resolve_widths(order)
        anchors = self._resolve_anchors(order)
        columns: List[_ColumnState] = []
        for column in order:
            defaults = self._defaults.get(column)
            heading = defaults.heading if defaults else column
            columns.append(
                _ColumnState(
                    column_id=column,
                    heading=heading,
                    visible=column in visible,
                    width=widths.get(column, defaults.width if defaults else 80),
                    anchor=anchors.get(column, defaults.anchor if defaults else "w"),
                )
            )
        preset = self._state.get("preset")
        if not isinstance(preset, str) or preset not in self._PRESETS:
            preset = "Custom"
        return _LayoutModel(order=list(order), columns=columns, preset=preset)


class _ColumnLayoutEditor:
    def __init__(self, plugin: ColumnLayoutManagerPlugin, parent, *, standalone: bool) -> None:
        self.plugin = plugin
        self.frame = ttk.Frame(parent)
        self._standalone = standalone
        self._order: List[str] = []
        self._columns: Dict[str, _ColumnState] = {}
        self._selected: Optional[str] = None
        self._updating = False

        self._visible_var = tk.BooleanVar(value=True)
        self._width_var = tk.StringVar(value="80")
        self._anchor_var = tk.StringVar(value="w")
        self._preset_var = tk.StringVar(value="Default")

        self._build_ui()

    def _build_ui(self) -> None:
        self.frame.columnconfigure(0, weight=1)
        self.frame.columnconfigure(1, weight=1)
        ttk.Label(self.frame, text="Column Order").grid(row=0, column=0, sticky="w")
        list_frame = ttk.Frame(self.frame)
        list_frame.grid(row=1, column=0, rowspan=4, sticky="nsew", pady=(4, 0))
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        self._listbox = tk.Listbox(list_frame, activestyle="dotbox", exportselection=False, height=12)
        self._listbox.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=self._listbox.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self._listbox.configure(yscrollcommand=scrollbar.set)
        self._listbox.bind("<<ListboxSelect>>", lambda _e: self._on_select())

        controls = ttk.Frame(self.frame)
        controls.grid(row=1, column=1, sticky="nw")
        ttk.Button(controls, text="Move Up", command=self._move_up).grid(row=0, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(controls, text="Move Down", command=self._move_down).grid(row=1, column=0, sticky="ew", pady=(0, 8))
        ttk.Checkbutton(
            controls,
            text="Visible",
            variable=self._visible_var,
            command=self._on_visibility_toggle,
        ).grid(row=2, column=0, sticky="w", pady=(0, 8))
        ttk.Label(controls, text="Width (px)").grid(row=3, column=0, sticky="w")
        width_entry = ttk.Entry(controls, textvariable=self._width_var, width=8)
        width_entry.grid(row=4, column=0, sticky="w")
        width_entry.bind("<FocusOut>", lambda _e: self._sync_selected())
        width_entry.bind("<Return>", lambda _e: self._sync_selected())
        ttk.Label(controls, text="Alignment").grid(row=5, column=0, sticky="w", pady=(8, 0))
        self._anchor_cb = ttk.Combobox(
            controls,
            values=list(_ANCHOR_CHOICES.keys()),
            state="readonly",
            width=10,
        )
        self._anchor_cb.grid(row=6, column=0, sticky="w")
        self._anchor_cb.bind("<<ComboboxSelected>>", lambda _e: self._on_anchor_changed())

        preset_frame = ttk.Frame(self.frame)
        preset_frame.grid(row=5, column=0, sticky="ew", pady=(16, 0))
        preset_frame.columnconfigure(1, weight=1)
        ttk.Label(preset_frame, text="Preset").grid(row=0, column=0, sticky="w")
        options = ["Custom"] + list(self.plugin._PRESETS)
        self._preset_cb = ttk.Combobox(preset_frame, textvariable=self._preset_var, values=options, state="readonly")
        self._preset_cb.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        self._preset_cb.bind("<<ComboboxSelected>>", lambda _e: self._on_preset_selected())

        action_frame = ttk.Frame(self.frame)
        action_frame.grid(row=6, column=0, columnspan=2, sticky="ew", pady=(16, 0))
        action_frame.columnconfigure(0, weight=1)
        action_frame.columnconfigure(1, weight=1)
        ttk.Button(action_frame, text="Apply", command=self._apply).grid(row=0, column=0, sticky="ew")
        ttk.Button(action_frame, text="Reset", command=self.plugin.reset_to_defaults).grid(row=0, column=1, sticky="ew", padx=(8, 0))

    # ------------------------------------------------------------------
    def refresh(self) -> None:
        if self._updating:
            return
        self._updating = True
        try:
            model = self.plugin.get_layout_model()
            self._order = list(model.order)
            self._columns = {column.column_id: column for column in model.columns}
            self._preset_var.set(model.preset if model.preset in self._preset_cb["values"] else "Custom")
            self._populate_listbox()
            target = self._selected if self._selected in self._order else (self._order[0] if self._order else None)
            if target is not None:
                self._select_column(target)
            else:
                self._clear_detail()
        finally:
            self._updating = False

    def _populate_listbox(self) -> None:
        self._listbox.delete(0, tk.END)
        for column_id in self._order:
            column = self._columns.get(column_id)
            heading = column.heading if column else column_id
            display = f"{heading} ({column_id})"
            self._listbox.insert(tk.END, display)

    def _select_column(self, column_id: str) -> None:
        if column_id not in self._order:
            return
        index = self._order.index(column_id)
        self._listbox.selection_clear(0, tk.END)
        self._listbox.selection_set(index)
        self._listbox.see(index)
        self._selected = column_id
        column = self._columns.get(column_id)
        if column is None:
            return
        self._visible_var.set(bool(column.visible))
        self._width_var.set(str(column.width))
        label = _REVERSE_ANCHORS.get(column.anchor, None)
        if label is None:
            label = "Left"
            if column.anchor in _REVERSE_ANCHORS:
                label = _REVERSE_ANCHORS[column.anchor]
        self._anchor_cb.set(label)

    def _clear_detail(self) -> None:
        self._selected = None
        self._visible_var.set(True)
        self._width_var.set("80")
        self._anchor_cb.set("Left")

    def _sync_selected(self) -> None:
        if self._selected is None:
            return
        column = self._columns.get(self._selected)
        if column is None:
            return
        try:
            column.width = max(24, int(self._width_var.get()))
        except (TypeError, ValueError):
            self._width_var.set(str(column.width))
        column.visible = bool(self._visible_var.get())
        anchor_label = self._anchor_cb.get()
        column.anchor = _ANCHOR_CHOICES.get(anchor_label, column.anchor)
        self._preset_var.set("Custom")

    def _on_select(self) -> None:
        if self._updating:
            return
        selection = self._listbox.curselection()
        if not selection:
            return
        self._sync_selected()
        index = selection[0]
        if index < 0 or index >= len(self._order):
            return
        column_id = self._order[index]
        self._selected = column_id
        column = self._columns.get(column_id)
        if column is None:
            return
        self._visible_var.set(bool(column.visible))
        self._width_var.set(str(column.width))
        self._anchor_cb.set(_REVERSE_ANCHORS.get(column.anchor, "Left"))

    def _on_visibility_toggle(self) -> None:
        if self._updating:
            return
        self._sync_selected()

    def _on_anchor_changed(self) -> None:
        if self._updating:
            return
        self._sync_selected()

    def _move_up(self) -> None:
        if self._updating:
            return
        selection = self._listbox.curselection()
        if not selection:
            return
        index = selection[0]
        if index <= 0:
            return
        self._sync_selected()
        self._order[index - 1], self._order[index] = self._order[index], self._order[index - 1]
        self._populate_listbox()
        self._listbox.selection_set(index - 1)
        self._listbox.see(index - 1)
        self._selected = self._order[index - 1]
        self._preset_var.set("Custom")

    def _move_down(self) -> None:
        if self._updating:
            return
        selection = self._listbox.curselection()
        if not selection:
            return
        index = selection[0]
        if index >= len(self._order) - 1:
            return
        self._sync_selected()
        self._order[index + 1], self._order[index] = self._order[index], self._order[index + 1]
        self._populate_listbox()
        self._listbox.selection_set(index + 1)
        self._listbox.see(index + 1)
        self._selected = self._order[index + 1]
        self._preset_var.set("Custom")

    def _apply(self) -> None:
        self._sync_selected()
        visible = [column_id for column_id in self._order if self._columns.get(column_id, _ColumnState(column_id, column_id, True, 80, "w")).visible]
        widths = {column_id: self._columns[column_id].width for column_id in self._order if column_id in self._columns}
        anchors = {column_id: self._columns[column_id].anchor for column_id in self._order if column_id in self._columns}
        self.plugin.update_layout(self._order, visible, widths, anchors, preset=self._preset_var.get())

    def _on_preset_selected(self) -> None:
        if self._updating:
            return
        chosen = self._preset_var.get()
        if chosen == "Custom":
            return
        self.plugin.apply_preset(chosen)


PLUGIN: Optional[ColumnLayoutManagerPlugin] = None


def register(api) -> None:
    global PLUGIN
    PLUGIN = ColumnLayoutManagerPlugin(api)
    PLUGIN.register()
