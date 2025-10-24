"""Command Center dashboard displayed on launch for quick actions."""
from __future__ import annotations

from typing import Iterable, Sequence

import tkinter as tk
from tkinter import ttk

_DEFAULT_PALETTE = {
    "bg": "#111316",
    "fg": "#E6E6E6",
    "alt": "#161A1E",
    "accent": "#4C8BF5",
    "sel": "#2A2F3A",
}


def _center(window: tk.Toplevel) -> None:
    try:
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        if width <= 1 or height <= 1:
            geometry = window.geometry()
            size = geometry.split("+", 1)[0]
            if "x" in size:
                width_str, height_str = size.split("x", 1)
                width = int(width_str)
                height = int(height_str)
            else:
                width, height = 720, 540
        screen_w = window.winfo_screenwidth()
        screen_h = window.winfo_screenheight()
        x = max(int((screen_w - width) / 2), 0)
        y = max(int((screen_h - height) / 2), 0)
        window.geometry(f"{width}x{height}+{x}+{y}")
    except Exception:
        return


class CommandCenter:
    """Lightweight modal dashboard that aggregates launch shortcuts."""

    def __init__(self, app: tk.Tk) -> None:
        self.app = app
        self.window: tk.Toplevel | None = None
        self._content: ttk.Frame | None = None
        self._modal: bool = True

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def show(self, *, modal: bool = True) -> None:
        self._modal = modal
        self._ensure_window()
        if self.window is None:
            return
        self.refresh()
        self.window.deiconify()
        _center(self.window)
        if modal:
            try:
                self.window.grab_set()
            except Exception:
                pass
        self.window.focus_set()

    def hide(self) -> None:
        if self.window is None:
            return
        if self.window.winfo_exists():
            try:
                self.window.grab_release()
            except Exception:
                pass
            self.window.destroy()
        self.window = None
        self._content = None

    def refresh(self) -> None:
        if self.window is None or not self.window.winfo_exists() or self._content is None:
            return
        for child in list(self._content.winfo_children()):
            child.destroy()
        self._populate_content(self._content)
        self.window.update_idletasks()

    def refresh_theme(self) -> None:
        if self.window is None or not self.window.winfo_exists():
            return
        self._apply_theme()
        self.refresh()

    def is_visible(self) -> bool:
        return bool(self.window and self.window.winfo_exists())

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    def _ensure_window(self) -> None:
        if self.window is not None and self.window.winfo_exists():
            return
        palette = self._palette()
        self.window = tk.Toplevel(self.app)
        self.window.withdraw()
        self.window.title("Command Center")
        self.window.transient(self.app)
        self.window.resizable(False, False)
        self.window.configure(bg=palette.get("bg", _DEFAULT_PALETTE["bg"]))
        self.window.protocol("WM_DELETE_WINDOW", self.hide)
        self.window.bind("<Escape>", lambda _e: self.hide())

        self._content = ttk.Frame(self.window, padding=(16, 20, 16, 20))
        self._content.pack(fill="both", expand=True)
        self._content.columnconfigure(0, weight=1)

    def _populate_content(self, container: ttk.Frame) -> None:
        row = 0
        header = ttk.Frame(container)
        header.grid(row=row, column=0, sticky="ew")
        ttk.Label(header, text="Command Center", font=("TkDefaultFont", 14, "bold")).pack(side="left")
        ttk.Label(
            header,
            text="Quick shortcuts for common launch actions",
            foreground="#7f8c9a",
        ).pack(side="left", padx=(12, 0))
        row += 1

        ttk.Separator(container).grid(row=row, column=0, sticky="ew", pady=(12, 16))
        row += 1

        quick = ttk.LabelFrame(container, text="Quick actions", padding=(12, 10))
        quick.grid(row=row, column=0, sticky="ew")
        for column in range(3):
            quick.columnconfigure(column, weight=1)
        ttk.Button(quick, text="Start scan", command=self._trigger_scan).grid(row=0, column=0, sticky="ew")
        ttk.Button(quick, text="Open settings", command=self._open_settings).grid(row=0, column=1, sticky="ew", padx=6)
        ttk.Button(quick, text="Plugin manager", command=self._open_plugin_manager).grid(row=0, column=2, sticky="ew")
        ttk.Button(
            quick,
            text="Plugin status",
            command=self._open_plugin_status,
        ).grid(row=1, column=0, sticky="ew", columnspan=3, pady=(8, 0))
        row += 1

        ttk.Separator(container).grid(row=row, column=0, sticky="ew", pady=(16, 12))
        row += 1

        self._build_recent_section(container, row)
        row += 1

        ttk.Separator(container).grid(row=row, column=0, sticky="ew", pady=(16, 12))
        row += 1

        self._build_loadouts_section(container, row)
        row += 1

        manager = getattr(self.app, "plugin_manager", None)
        if manager is not None:
            panels = list(manager.get_command_center_panels())
        else:
            panels = []
        if panels:
            ttk.Separator(container).grid(row=row, column=0, sticky="ew", pady=(16, 12))
            row += 1
        for panel in panels:
            frame = ttk.LabelFrame(container, text=panel.title, padding=(12, 10))
            frame.grid(row=row, column=0, sticky="ew", pady=(0, 12))
            try:
                panel.builder(self.app, frame, manager.api if manager else None)
            except Exception as exc:  # pragma: no cover - defensive
                ttk.Label(frame, text=f"Unable to load panel: {exc}").grid(row=0, column=0, sticky="w")
            row += 1

        actions = ttk.Frame(container)
        actions.grid(row=row, column=0, sticky="e", pady=(8, 0))
        ttk.Button(actions, text="Close", command=self.hide).pack(side="right")

    def _build_recent_section(self, container: ttk.Frame, row: int) -> None:
        frame = ttk.LabelFrame(container, text="Recent Mods folders", padding=(12, 10))
        frame.grid(row=row, column=0, sticky="ew")
        frame.columnconfigure(0, weight=1)
        history = list(self._recent_directories())
        if not history:
            ttk.Label(frame, text="Browse to a Mods folder to populate this list.").grid(row=0, column=0, sticky="w")
            return
        for index, directory in enumerate(history):
            button = ttk.Button(
                frame,
                text=directory,
                command=lambda value=directory: self._select_directory(value),
            )
            button.grid(row=index, column=0, sticky="ew", pady=(0, 6))

    def _build_loadouts_section(self, container: ttk.Frame, row: int) -> None:
        frame = ttk.LabelFrame(container, text="Loadout presets", padding=(12, 10))
        frame.grid(row=row, column=0, sticky="ew")
        frame.columnconfigure(0, weight=1)
        names = list(self._loadout_names())
        if not names:
            ttk.Label(frame, text="Create a loadout to quick load specific plans.").grid(row=0, column=0, sticky="w")
            return
        for index, name in enumerate(names):
            ttk.Button(frame, text=name, command=lambda value=name: self._apply_loadout(value)).grid(
                row=index, column=0, sticky="ew", pady=(0, 6)
            )

    # ------------------------------------------------------------------
    # Action callbacks
    # ------------------------------------------------------------------
    def _trigger_scan(self) -> None:
        self.hide()
        command = getattr(self.app, "on_scan", None)
        if callable(command):
            self.app.after(0, command)

    def _open_settings(self) -> None:
        self.hide()
        command = getattr(self.app, "show_settings", None)
        if callable(command):
            self.app.after(0, command)

    def _open_plugin_manager(self) -> None:
        self.hide()
        command = getattr(self.app, "open_plugin_manager_ui", None)
        if callable(command):
            self.app.after(0, command)

    def _open_plugin_status(self) -> None:
        self.hide()
        command = getattr(self.app, "show_mod_status_popup", None)
        if callable(command):
            self.app.after(0, command)

    def _select_directory(self, value: str) -> None:
        setter = getattr(getattr(self.app, "mods_root", None), "set", None)
        if callable(setter):
            setter(value)

    def _apply_loadout(self, name: str) -> None:
        var = getattr(self.app, "loadout_var", None)
        if hasattr(var, "set"):
            var.set(name)
        command = getattr(self.app, "on_apply_loadout", None)
        if callable(command):
            self.app.after(0, command)

    # ------------------------------------------------------------------
    # Data helpers
    # ------------------------------------------------------------------
    def _palette(self) -> dict[str, str]:
        palette = getattr(self.app, "_theme_cache", None)
        if isinstance(palette, dict) and palette:
            return palette
        return dict(_DEFAULT_PALETTE)

    def _apply_theme(self) -> None:
        if self.window is None:
            return
        palette = self._palette()
        self.window.configure(bg=palette.get("bg", _DEFAULT_PALETTE["bg"]))

    def _recent_directories(self) -> Iterable[str]:
        getter = getattr(self.app, "get_recent_mods_dirs", None)
        if callable(getter):
            try:
                entries = getter()
            except Exception:
                entries = []
            for entry in entries:
                if entry:
                    yield entry

    def _loadout_names(self) -> Sequence[str]:
        loadouts = getattr(self.app, "loadouts", {})
        if isinstance(loadouts, dict):
            return sorted(str(name) for name in loadouts.keys())
        return []

