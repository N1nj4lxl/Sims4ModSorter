from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, List, Optional, Tuple

from plugin_api import scan_metrics

BG_COLOR = "#1c1c1c"
TEXT_COLOR = "#e0e0e0"
ALT_ROW = "#232323"
HEADER_BG = "#2a2a2a"
ACCENT = "#3a7bd5"
LOG_BG = "#151515"


class ScanTimelineVisualizerPlugin:
    def __init__(self, api) -> None:
        self.api = api
        self.window: Optional[tk.Toplevel] = None
        self.tree: Optional[ttk.Treeview] = None
        self.log_text: Optional[tk.Text] = None
        self.summary_var = tk.StringVar(value="No scans yet.")
        self.session_var = tk.StringVar(value="")
        self._session_map: Dict[str, Tuple[Optional[Dict[str, Any]], str]] = {}
        self._latest_snapshot: Optional[Dict[str, Any]] = None
        self._history_cache: List[Dict[str, Any]] = scan_metrics.get_history()
        self._pending_refresh = False
        self._listener = scan_metrics.register_listener(self._on_metrics_update)

    # ------------------------------------------------------------------
    def register(self) -> None:
        self.api.register_view_action("Scan Insights", self._open_panel)

    # ------------------------------------------------------------------
    def _on_metrics_update(self, payload: Dict[str, Any]) -> None:
        session = payload.get("session")
        history = payload.get("history") or []
        self._latest_snapshot = session
        self._history_cache = list(history)
        self._schedule_refresh()

    def _schedule_refresh(self) -> None:
        if self._pending_refresh:
            return
        app = getattr(self.api, "app", None)
        if not app:
            return
        self._pending_refresh = True

        def run() -> None:
            self._pending_refresh = False
            if self.window and self.window.winfo_exists():
                self._populate_panel()

        app.after(0, run)

    # ------------------------------------------------------------------
    def _open_panel(self, app, _api) -> None:
        if self.window and self.window.winfo_exists():
            self.window.deiconify()
            self.window.lift()
            self._populate_panel()
            return
        window = tk.Toplevel(app)
        window.title("Scan Insights")
        window.geometry("840x520")
        window.configure(bg=BG_COLOR)
        window.resizable(False, False)
        window.protocol("WM_DELETE_WINDOW", window.withdraw)
        self.window = window

        style = ttk.Style(window)
        style.configure("Insights.TFrame", background=BG_COLOR)
        style.configure("Insights.TLabel", background=BG_COLOR, foreground=TEXT_COLOR)
        style.configure("Insights.TCombobox", fieldbackground=BG_COLOR, foreground=TEXT_COLOR)
        style.configure("Insights.Treeview", background=BG_COLOR, fieldbackground=BG_COLOR, foreground=TEXT_COLOR, borderwidth=0)
        style.configure("Insights.Treeview.Heading", background=HEADER_BG, foreground=TEXT_COLOR)
        style.map("Insights.Treeview", background=[("selected", ACCENT)])
        style.map("Insights.TCombobox", fieldbackground=[("readonly", BG_COLOR)])

        container = ttk.Frame(window, padding=16, style="Insights.TFrame")
        container.pack(fill="both", expand=True)

        header = ttk.Frame(container, style="Insights.TFrame")
        header.pack(fill="x")
        title = ttk.Label(header, text="Scan Insights", style="Insights.TLabel", font=("TkDefaultFont", 14, "bold"))
        title.pack(side="left")
        session_box = ttk.Frame(header, style="Insights.TFrame")
        session_box.pack(side="right")
        ttk.Label(session_box, text="Session:", style="Insights.TLabel").pack(side="left", padx=(0, 6))
        combo = ttk.Combobox(
            session_box,
            textvariable=self.session_var,
            state="readonly",
            width=28,
            style="Insights.TCombobox",
        )
        combo.bind("<<ComboboxSelected>>", lambda _e: self._populate_panel())
        combo.pack(side="left")
        self.session_combo = combo

        tree_frame = ttk.Frame(container, style="Insights.TFrame")
        tree_frame.pack(fill="both", expand=True, pady=(12, 12))
        columns = ("plugin", "time", "files", "warnings", "status")
        tree = ttk.Treeview(
            tree_frame,
            columns=columns,
            show="headings",
            height=8,
            style="Insights.Treeview",
        )
        tree.heading("plugin", text="Plugin")
        tree.heading("time", text="Time (s)")
        tree.heading("files", text="Files")
        tree.heading("warnings", text="Warnings")
        tree.heading("status", text="Status")
        tree.column("plugin", width=240, anchor="w")
        tree.column("time", width=90, anchor="center")
        tree.column("files", width=90, anchor="center")
        tree.column("warnings", width=110, anchor="center")
        tree.column("status", width=130, anchor="center")
        tree.tag_configure("odd", background=ALT_ROW)
        tree.pack(fill="both", expand=True, side="left")
        scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        self.tree = tree

        summary = ttk.Label(container, textvariable=self.summary_var, style="Insights.TLabel")
        summary.pack(anchor="w")

        log_frame = ttk.Frame(container, style="Insights.TFrame")
        log_frame.pack(fill="both", expand=True, pady=(12, 0))
        ttk.Label(log_frame, text="Timeline", style="Insights.TLabel").pack(anchor="w")
        log_text = tk.Text(
            log_frame,
            height=8,
            wrap="word",
            state="disabled",
            bg=LOG_BG,
            fg=TEXT_COLOR,
            highlightthickness=0,
            relief="flat",
        )
        log_text.pack(fill="both", expand=True)
        self.log_text = log_text

        self._populate_panel()

    # ------------------------------------------------------------------
    def _populate_panel(self) -> None:
        if not (self.window and self.window.winfo_exists()):
            return
        snapshot = self._latest_snapshot
        history = list(self._history_cache)
        options: List[Tuple[str, Optional[Dict[str, Any]], str]] = []
        if snapshot:
            label = snapshot.get("label", "Current Scan")
            options.append(("__current__", snapshot, f"Current Scan — {label}"))
        for entry in history:
            ts = entry.get("timestamp", "")
            label = entry.get("label") or ts
            display = label or ts or "Previous Scan"
            options.append((ts or display, entry, display))
        if not options:
            options.append(("", None, "No history"))
        self._session_map = {display: (data, key) for key, data, display in options}
        values = [display for _, _, display in options]
        current_value = self.session_var.get()
        if current_value not in values:
            self.session_var.set(values[0])
        combo = getattr(self, "session_combo", None)
        if combo is not None:
            combo.configure(values=values)

        selection = self.session_var.get()
        data, key = self._session_map.get(selection, (None, ""))
        if data is None:
            self._render_empty()
        elif key == "__current__":
            self._render_snapshot(data)
        else:
            self._render_history_entry(data)

    def _render_empty(self) -> None:
        if self.tree:
            for iid in self.tree.get_children():
                self.tree.delete(iid)
        self.summary_var.set("No scans yet.")
        if self.log_text:
            self.log_text.configure(state="normal")
            self.log_text.delete("1.0", "end")
            self.log_text.insert("end", "No timeline data available.")
            self.log_text.configure(state="disabled")

    def _render_snapshot(self, snapshot: Dict[str, Any]) -> None:
        plugins = snapshot.get("plugins", [])
        duration = snapshot.get("duration", 0.0)
        total_files = snapshot.get("total_files", 0)
        extras = snapshot.get("extras", {}) or {}
        disabled = extras.get("disabled", 0)
        summary = f"Total Time: {duration:.2f} s    Total Files: {total_files}"
        if disabled:
            summary += f" (disabled: {disabled})"
        self.summary_var.set(summary)
        self._render_table(plugins)
        self._render_log(snapshot.get("events", []))

    def _render_history_entry(self, entry: Dict[str, Any]) -> None:
        plugins = entry.get("plugins", [])
        total_time = entry.get("total_time", 0.0)
        total_files = entry.get("total_files", 0)
        summary = f"Total Time: {total_time:.2f} s    Total Files: {total_files}"
        self.summary_var.set(summary)
        formatted_plugins = [
            {
                "name": item.get("name", "Unknown"),
                "time": item.get("time", 0.0),
                "files": item.get("files", 0),
                "warnings": item.get("warnings", 0),
                "status": "Warning" if item.get("warnings") else "Done",
            }
            for item in plugins
        ]
        self._render_table(formatted_plugins)
        self._render_log(entry.get("events", []))

    def _render_table(self, plugins: List[Dict[str, Any]]) -> None:
        if not self.tree:
            return
        tree = self.tree
        tree.delete(*tree.get_children())
        for index, item in enumerate(plugins):
            status = item.get("status", "Done")
            warnings = item.get("warnings", 0) or 0
            icon = self._status_icon(status, warnings)
            tree.insert(
                "",
                "end",
                values=(
                    item.get("name", "Unknown"),
                    f"{float(item.get('time', 0.0)):.2f}",
                    int(item.get("files", 0)),
                    warnings,
                    icon,
                ),
                tags=("odd" if index % 2 else "even",),
            )

    def _render_log(self, events: List[Dict[str, Any]]) -> None:
        if not self.log_text:
            return
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        if not events:
            self.log_text.insert("end", "No timeline events recorded.")
        else:
            for event in events:
                offset = event.get("offset", "[00:00]")
                message = event.get("message", "")
                level = event.get("level", "info")
                prefix = ""
                if level in {"warn", "warning"}:
                    prefix = "[WARN] "
                elif level == "error":
                    prefix = "[ERROR] "
                self.log_text.insert("end", f"{offset} {prefix}{message}\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    @staticmethod
    def _status_icon(status: str, warnings: int) -> str:
        normalized = (status or "").lower()
        if normalized == "warning" or warnings:
            return "⚠️ Warning"
        if normalized == "error":
            return "❌ Error"
        if normalized == "running":
            return "⏳ Running"
        return "✅ Done"


PLUGIN: Optional[ScanTimelineVisualizerPlugin] = None


def register(api) -> None:
    global PLUGIN
    PLUGIN = ScanTimelineVisualizerPlugin(api)
    PLUGIN.register()
