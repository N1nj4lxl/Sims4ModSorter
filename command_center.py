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


def _parse_hex_color(value: str) -> tuple[int, int, int] | None:
    value = value.strip().lstrip("#")
    if len(value) != 6:
        return None
    try:
        r = int(value[0:2], 16)
        g = int(value[2:4], 16)
        b = int(value[4:6], 16)
    except ValueError:
        return None
    return r, g, b


def _mix_colors(foreground: str, background: str, ratio: float) -> str:
    """Blend two hex colours together."""

    fg = _parse_hex_color(foreground)
    bg = _parse_hex_color(background)
    if fg is None or bg is None:
        return foreground
    ratio = max(0.0, min(1.0, ratio))
    blend = []
    for left, right in zip(fg, bg):
        value = int(round(left * (1.0 - ratio) + right * ratio))
        blend.append(max(0, min(255, value)))
    return f"#{blend[0]:02x}{blend[1]:02x}{blend[2]:02x}"


def _scrim_color(bg_hex: str) -> str:
    """Blend the background colour with black for an overlay scrim."""

    darkened = _mix_colors("#000000", bg_hex, 0.55)
    return darkened or "#000000"


class CommandCenter:
    """Lightweight modal dashboard that aggregates launch shortcuts."""

    def __init__(self, app: tk.Tk) -> None:
        self.app = app
        self.window: tk.Frame | None = None
        self._shell: ttk.Frame | None = None
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
        self.window.place(relx=0, rely=0, relwidth=1, relheight=1)
        self.window.tkraise()
        if modal:
            try:
                self.window.grab_set()
            except Exception:
                pass
        if self._shell is not None:
            self._shell.focus_set()

    def hide(self) -> None:
        if self.window is None:
            return
        if self.window.winfo_exists():
            try:
                self.window.grab_release()
            except Exception:
                pass
            self.window.place_forget()
            self.window.destroy()
        self.window = None
        self._shell = None
        self._content = None

    def refresh(self) -> None:
        if (
            self.window is None
            or not self.window.winfo_exists()
            or self._content is None
            or self._shell is None
        ):
            return
        self._apply_theme()
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
        return bool(
            self.window and self.window.winfo_exists() and self.window.winfo_ismapped()
        )

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    def _ensure_window(self) -> None:
        if self.window is not None and self.window.winfo_exists():
            return
        palette = self._palette()
        overlay = tk.Frame(
            self.app,
            bg=_scrim_color(palette.get("bg", _DEFAULT_PALETTE["bg"])),
            highlightthickness=0,
            bd=0,
        )
        overlay.place_forget()
        overlay.grid_rowconfigure(0, weight=1)
        overlay.grid_columnconfigure(0, weight=1)
        overlay.bind("<Escape>", lambda _e: self.hide())
        overlay.bind(
            "<Button-1>",
            lambda event: self.hide() if event.widget is overlay else None,
        )

        shell = ttk.Frame(
            overlay,
            padding=0,
            style="CommandCenter.OverlayShell.TFrame",
        )
        shell.grid(row=0, column=0, padx=32, pady=32)
        shell.columnconfigure(0, weight=1)
        shell.rowconfigure(0, weight=1)
        shell.bind("<Escape>", lambda _e: self.hide())

        content = ttk.Frame(
            shell,
            padding=(24, 26, 24, 24),
            style="CommandCenter.Container.TFrame",
        )
        content.grid(row=0, column=0, sticky="nsew")
        content.columnconfigure(0, weight=1)
        content.bind("<Escape>", lambda _e: self.hide())

        self.window = overlay
        self._shell = shell
        self._content = content
        self._apply_theme()

    def _populate_content(self, container: ttk.Frame) -> None:
        row = 0
        header = ttk.Frame(container, style="CommandCenter.Header.TFrame")
        header.grid(row=row, column=0, sticky="ew")
        ttk.Label(header, text="Command Center", style="CommandCenter.Title.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            header,
            text="Launch the actions you need as soon as the sorter starts.",
            style="CommandCenter.Subtitle.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))
        row += 1

        quick = ttk.Frame(
            container,
            style="CommandCenter.Card.TFrame",
            padding=(18, 18, 18, 20),
        )
        quick.grid(row=row, column=0, sticky="ew", pady=(18, 0))
        quick.columnconfigure(0, weight=1)
        ttk.Label(quick, text="Quick actions", style="CommandCenter.CardHeading.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        current_mods = getattr(self.app, "mods_root", None)
        mods_value = ""
        if hasattr(current_mods, "get"):
            try:
                mods_value = str(current_mods.get()).strip()
            except Exception:
                mods_value = ""
        mods_display = mods_value or "No Mods folder selected yet"
        ttk.Label(
            quick,
            text=f"Mods folder: {mods_display}",
            style="CommandCenter.Muted.TLabel",
            wraplength=460,
        ).grid(row=1, column=0, sticky="w", pady=(8, 12))
        actions = ttk.Frame(quick, style="CommandCenter.CardBody.TFrame")
        actions.grid(row=2, column=0, sticky="ew")
        buttons = [
            ("Start scan", self._trigger_scan, "CommandCenter.Primary.TButton"),
            ("Change Mods folder", self._change_mods_folder, "CommandCenter.Secondary.TButton"),
            ("Open settings", self._open_settings, "CommandCenter.Secondary.TButton"),
        ]
        if getattr(self.app, "plugin_manager", None) is not None:
            buttons.append(
                ("Manage plugins", self._open_plugin_manager, "CommandCenter.Secondary.TButton")
            )
        for index, (_label, _command, _style) in enumerate(buttons):
            actions.columnconfigure(index, weight=1)
        for index, (label, command, style_name) in enumerate(buttons):
            padx = (0, 0) if index == 0 else (10, 0)
            ttk.Button(actions, text=label, command=command, style=style_name).grid(
                row=0, column=index, sticky="ew", padx=padx
            )
        ttk.Checkbutton(
            quick,
            text="Show the Command Center on launch",
            variable=self.app.show_command_center_var,
            style="CommandCenter.Checkbutton.TCheckbutton",
        ).grid(row=3, column=0, sticky="w", pady=(16, 0))
        row += 1

        self._build_recent_section(container, row)
        row += 1

        self._build_loadouts_section(container, row)
        row += 1

        manager = getattr(self.app, "plugin_manager", None)
        if manager is not None:
            panels = list(manager.get_command_center_panels())
        else:
            panels = []
        for panel in panels:
            frame = ttk.LabelFrame(
                container,
                text=panel.title,
                padding=(16, 14, 16, 16),
                style="CommandCenter.Section.TLabelframe",
            )
            frame.grid(row=row, column=0, sticky="ew", pady=(18, 0))
            try:
                panel.builder(self.app, frame, manager.api if manager else None)
            except Exception as exc:  # pragma: no cover - defensive
                ttk.Label(
                    frame,
                    text=f"Unable to load panel: {exc}",
                    style="CommandCenter.Muted.TLabel",
                ).grid(row=0, column=0, sticky="w")
            row += 1

        actions = ttk.Frame(container, style="CommandCenter.Footer.TFrame")
        actions.grid(row=row, column=0, sticky="e", pady=(20, 0))
        ttk.Button(
            actions,
            text="Close",
            command=self.hide,
            style="CommandCenter.Secondary.TButton",
        ).pack(side="right")

    def _build_recent_section(self, container: ttk.Frame, row: int) -> None:
        frame = ttk.LabelFrame(
            container,
            text="Recent Mods folders",
            padding=(16, 14, 16, 16),
            style="CommandCenter.Section.TLabelframe",
        )
        frame.grid(row=row, column=0, sticky="ew", pady=(18, 0))
        frame.columnconfigure(0, weight=1)
        history = list(self._recent_directories())
        if not history:
            ttk.Label(
                frame,
                text="Browse to a Mods folder to populate this list.",
                style="CommandCenter.Muted.TLabel",
                wraplength=440,
            ).grid(row=0, column=0, sticky="w")
            return
        for index, directory in enumerate(history):
            button = ttk.Button(
                frame,
                text=directory,
                command=lambda value=directory: self._select_directory(value),
                style="CommandCenter.List.TButton",
            )
            button.grid(row=index, column=0, sticky="ew", pady=(0, 6))

    def _build_loadouts_section(self, container: ttk.Frame, row: int) -> None:
        frame = ttk.LabelFrame(
            container,
            text="Loadout presets",
            padding=(16, 14, 16, 16),
            style="CommandCenter.Section.TLabelframe",
        )
        frame.grid(row=row, column=0, sticky="ew", pady=(18, 0))
        frame.columnconfigure(0, weight=1)
        names = list(self._loadout_names())
        if not names:
            ttk.Label(
                frame,
                text="Create a loadout to quick load specific plans.",
                style="CommandCenter.Muted.TLabel",
                wraplength=440,
            ).grid(row=0, column=0, sticky="w")
            return
        active_name = ""
        loadout_var = getattr(self.app, "loadout_var", None)
        if hasattr(loadout_var, "get"):
            try:
                active_name = str(loadout_var.get() or "")
            except Exception:
                active_name = ""
        if active_name:
            ttk.Label(
                frame,
                text=f"Active: {active_name}",
                style="CommandCenter.Muted.TLabel",
            ).grid(row=0, column=0, sticky="w", pady=(0, 10))
            start_row = 1
        else:
            start_row = 0
        for index, name in enumerate(names):
            style_name = "CommandCenter.ActiveList.TButton" if name == active_name else "CommandCenter.List.TButton"
            ttk.Button(
                frame,
                text=name,
                command=lambda value=name: self._apply_loadout(value),
                style=style_name,
            ).grid(row=start_row + index, column=0, sticky="ew", pady=(0, 6))

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

    def _change_mods_folder(self) -> None:
        self.hide()
        command = getattr(self.app, "on_browse", None)
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
        bg = palette.get("bg", _DEFAULT_PALETTE["bg"])
        fg = palette.get("fg", _DEFAULT_PALETTE["fg"])
        alt = palette.get("alt", _DEFAULT_PALETTE["alt"])
        accent = palette.get("accent", _DEFAULT_PALETTE["accent"])
        sel = palette.get("sel", _DEFAULT_PALETTE["sel"])
        muted = _mix_colors(fg, bg, 0.45)
        hover_accent = _mix_colors(accent, bg, 0.25)
        list_hover = _mix_colors(sel, bg, 0.35)
        border = _mix_colors(fg, bg, 0.75)

        style = ttk.Style(self.window)
        style.configure(
            "CommandCenter.OverlayShell.TFrame",
            background=bg,
            borderwidth=1,
            relief="solid",
        )
        style.configure("CommandCenter.Container.TFrame", background=bg)
        style.configure("CommandCenter.Header.TFrame", background=bg)
        style.configure("CommandCenter.Footer.TFrame", background=bg)
        style.configure(
            "CommandCenter.Card.TFrame",
            background=alt,
            borderwidth=1,
            relief="solid",
        )
        style.configure("CommandCenter.CardBody.TFrame", background=alt)
        style.configure(
            "CommandCenter.CardHeading.TLabel",
            background=alt,
            foreground=fg,
            font=("TkDefaultFont", 11, "bold"),
        )
        style.configure(
            "CommandCenter.Title.TLabel",
            background=bg,
            foreground=fg,
            font=("TkDefaultFont", 16, "bold"),
        )
        style.configure(
            "CommandCenter.Subtitle.TLabel",
            background=bg,
            foreground=muted,
            wraplength=460,
        )
        style.configure(
            "CommandCenter.Muted.TLabel",
            background=alt,
            foreground=muted,
            wraplength=460,
        )
        style.configure(
            "CommandCenter.Section.TLabelframe",
            background=alt,
            foreground=fg,
            borderwidth=1,
            relief="solid",
        )
        style.configure(
            "CommandCenter.Section.TLabelframe.Label",
            background=alt,
            foreground=fg,
        )
        style.configure("CommandCenter.TSeparator", background=border)
        style.configure(
            "CommandCenter.Primary.TButton",
            background=accent,
            foreground=fg,
            padding=(16, 10),
        )
        style.map(
            "CommandCenter.Primary.TButton",
            background=[("active", hover_accent), ("pressed", hover_accent)],
            foreground=[("disabled", muted)],
        )
        style.configure(
            "CommandCenter.Secondary.TButton",
            background=sel,
            foreground=fg,
            padding=(14, 10),
        )
        style.map(
            "CommandCenter.Secondary.TButton",
            background=[("active", list_hover), ("pressed", list_hover)],
            foreground=[("disabled", muted)],
        )
        style.configure(
            "CommandCenter.List.TButton",
            background=alt,
            foreground=fg,
            anchor="w",
            padding=(12, 8),
        )
        style.map(
            "CommandCenter.List.TButton",
            background=[("active", list_hover), ("pressed", list_hover)],
            foreground=[("disabled", muted)],
        )
        style.configure(
            "CommandCenter.ActiveList.TButton",
            background=sel,
            foreground=fg,
            anchor="w",
            padding=(12, 8),
        )
        style.map(
            "CommandCenter.ActiveList.TButton",
            background=[("active", hover_accent), ("pressed", hover_accent)],
            foreground=[("disabled", muted)],
        )
        style.configure(
            "CommandCenter.Checkbutton.TCheckbutton",
            background=alt,
            foreground=fg,
            padding=(2, 2),
        )
        style.map(
            "CommandCenter.Checkbutton.TCheckbutton",
            foreground=[("disabled", muted)],
        )

        self.window.configure(bg=_scrim_color(bg))
        if self._shell is not None and self._shell.winfo_exists():
            try:
                self._shell.configure(style="CommandCenter.OverlayShell.TFrame")
            except tk.TclError:
                pass
        if self._content is not None and self._content.winfo_exists():
            try:
                self._content.configure(style="CommandCenter.Container.TFrame")
            except tk.TclError:
                pass

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

