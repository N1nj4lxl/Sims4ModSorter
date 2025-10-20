"""Scan Timeline Visualizer plugin for Sims4 Mod Sorter."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from plugin_api import scan_metrics

_BACKGROUND = "#1c1c1c"
_FOREGROUND = "#e0e0e0"
_TABLE_ALT = "#232323"
_HEADER_FG = "#f5f5f5"
_STATUS_ICONS = {
    "done": "✅ Done",
    "warning": "⚠️ Warning",
    "warn": "⚠️ Warning",
    "error": "❌ Error",
    "failed": "❌ Error",
    "running": "⏳ Running",
}


@dataclass
class _SessionEntry:
    label: str
    key: Tuple[str, int]
    snapshot: Dict[str, object]


class ScanTimelineVisualizerPlugin:
    def __init__(self, api) -> None:
        self.api = api
        self.window: Optional[tk.Toplevel] = None
        self.tree: Optional[ttk.Treeview] = None
        self.log_text: Optional[tk.Text] = None
        self.summary_var = tk.StringVar(value="No scans yet.")
        self.session_var = tk.StringVar(value="Current Scan")
        self._session_entries: List[_SessionEntry] = []
        self._session_lookup: Dict[Tuple[str, int], Dict[str, object]] = {}
        self._current_snapshot: Optional[Dict[str, object]] = None
        self._history: List[Dict[str, object]] = []
        self._listener_token: Optional[int] = None
        getter = getattr(self.api, "is_feature_enabled", None)
        self._live_updates = bool(getter("live_updates", default=True)) if callable(getter) else True
        self._selected_key: Optional[Tuple[str, int]] = None

    # ------------------------------------------------------------------
    def register(self) -> None:
        self.api.register_toolbar_button(
            "scan-timeline-visualizer",
            text="Scan Insights",
            command=lambda app, _api: self._open_window(app),
            side="right",
            insert_before="plugin_status",
            padx=6,
        )
        try:
            self._listener_token = scan_metrics.register_listener(self._on_metrics_event)
        except Exception as exc:
            self.api.log(f"[Scan Timeline] Failed to subscribe to metrics: {exc}", level="error")
        self.api.log("[Scan Timeline] Visualizer initialised.")

    # ------------------------------------------------------------------
    def _open_window(self, app: tk.Tk) -> None:
        window = self._ensure_window(app)
        if not window:
            return
        window.deiconify()
        window.lift()
        self._refresh_sessions()

    def _ensure_window(self, app: tk.Tk) -> Optional[tk.Toplevel]:
        if self.window and self.window.winfo_exists():
            return self.window
        window = tk.Toplevel(app)
        window.title("Scan Insights")
        window.geometry("820x540")
        window.configure(bg=_BACKGROUND)
        window.resizable(False, False)
        window.transient(app)
        window.protocol("WM_DELETE_WINDOW", window.withdraw)
        self.window = window

        style = ttk.Style(window)
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Scan.TFrame", background=_BACKGROUND)
        style.configure("Scan.TLabel", background=_BACKGROUND, foreground=_FOREGROUND)
        style.configure(
            "Scan.Treeview",
            background=_BACKGROUND,
            foreground=_FOREGROUND,
            fieldbackground=_BACKGROUND,
            rowheight=24,
            bordercolor=_BACKGROUND,
            borderwidth=0,
        )
        style.configure("Scan.Treeview.Heading", background=_BACKGROUND, foreground=_HEADER_FG)
        style.map("Scan.Treeview", background=[("selected", "#2f2f2f")])
        style.configure("Scan.TButton", background="#2a2a2a", foreground=_FOREGROUND, padding=6)
        style.map("Scan.TButton", background=[("active", "#3a3a3a")])
        style.configure("Scan.TCombobox", fieldbackground=_BACKGROUND, foreground=_FOREGROUND)

        container = ttk.Frame(window, style="Scan.TFrame", padding=16)
        container.pack(fill="both", expand=True)

        header = ttk.Frame(container, style="Scan.TFrame")
        header.pack(fill="x")
        ttk.Label(
            header,
            text="Scan Insights",
            style="Scan.TLabel",
            font=("TkDefaultFont", 14, "bold"),
        ).pack(side="left")

        controls = ttk.Frame(container, style="Scan.TFrame")
        controls.pack(fill="x", pady=(12, 8))
        ttk.Label(controls, text="Session", style="Scan.TLabel").pack(side="left")
        self.session_cb = ttk.Combobox(
            controls,
            textvariable=self.session_var,
            state="readonly",
            width=28,
        )
        self.session_cb.pack(side="left", padx=(8, 12))
        self.session_cb.bind("<<ComboboxSelected>>", lambda _e: self._on_session_changed())
        ttk.Button(
            controls,
            text="Export JSON",
            style="Scan.TButton",
            command=lambda: self._export_current("json"),
        ).pack(side="right")
        ttk.Button(
            controls,
            text="Export CSV",
            style="Scan.TButton",
            command=lambda: self._export_current("csv"),
        ).pack(side="right", padx=(0, 8))

        table_frame = ttk.Frame(container, style="Scan.TFrame")
        table_frame.pack(fill="x")
        columns = ("plugin", "time", "files", "warnings", "status")
        self.tree = ttk.Treeview(
            table_frame,
            columns=columns,
            show="headings",
            style="Scan.Treeview",
            height=6,
        )
        headings = {
            "plugin": "Plugin",
            "time": "Time (s)",
            "files": "Files",
            "warnings": "Warnings",
            "status": "Status",
        }
        anchors = {"plugin": "w", "time": "e", "files": "center", "warnings": "center", "status": "w"}
        widths = {"plugin": 220, "time": 90, "files": 80, "warnings": 90, "status": 160}
        for column in columns:
            self.tree.heading(column, text=headings[column])
            self.tree.column(column, anchor=anchors[column], width=widths[column])
        self.tree.tag_configure("odd", background=_BACKGROUND)
        self.tree.tag_configure("even", background=_TABLE_ALT)
        self.tree.pack(fill="x")

        summary = ttk.Label(container, textvariable=self.summary_var, style="Scan.TLabel")
        summary.pack(anchor="w", pady=(8, 4))

        log_frame = ttk.Frame(container, style="Scan.TFrame")
        log_frame.pack(fill="both", expand=True)
        ttk.Label(log_frame, text="Timeline", style="Scan.TLabel").pack(anchor="w")
        self.log_text = tk.Text(
            log_frame,
            height=12,
            bg=_BACKGROUND,
            fg=_FOREGROUND,
            insertbackground=_FOREGROUND,
            state="disabled",
            wrap="word",
            relief="flat",
        )
        self.log_text.pack(fill="both", expand=True, pady=(6, 0))

        return window

    # ------------------------------------------------------------------
    def _on_metrics_event(self, payload: Dict[str, object]) -> None:
        event = str(payload.get("event") or "")
        if event == "history":
            history = payload.get("history") or []
            self._schedule(lambda: self._update_history(history))
            return
        session = payload.get("session")
        if not isinstance(session, dict):
            return
        if event == "session_update" and not self._live_updates:
            if not session.get("completed"):
                return
        self._schedule(lambda: self._update_current_session(session))

    def _schedule(self, func) -> None:
        app = getattr(self.api, "app", None)
        if not app:
            return
        try:
            app.after(0, func)
        except Exception:
            pass

    # ------------------------------------------------------------------
    def _update_history(self, history: Sequence[Dict[str, object]]) -> None:
        self._history = list(history)
        self._refresh_sessions()

    def _update_current_session(self, snapshot: Dict[str, object]) -> None:
        self._current_snapshot = snapshot
        if snapshot.get("running"):
            key = ("current", int(snapshot.get("session_id", 0)))
        else:
            key = ("history", int(snapshot.get("session_id", 0)))
        self._session_lookup[key] = snapshot
        if self._selected_key == key:
            self._render_snapshot(snapshot)
        self._refresh_sessions()

    def _refresh_sessions(self) -> None:
        entries: List[_SessionEntry] = []
        lookup: Dict[Tuple[str, int], Dict[str, object]] = {}
        if self._current_snapshot and self._current_snapshot.get("running"):
            key = ("current", int(self._current_snapshot.get("session_id", 0)))
            entries.append(
                _SessionEntry("Current Scan", key, self._current_snapshot)
            )
            lookup[key] = self._current_snapshot
        for idx, snapshot in enumerate(self._history):
            key = ("history", int(snapshot.get("session_id", 0)))
            label = "Last Scan" if idx == 0 else f"Scan #{snapshot.get('session_id', idx + 1)}"
            entries.append(_SessionEntry(label, key, snapshot))
            lookup[key] = snapshot
        if not entries:
            placeholder = {
                "session_id": 0,
                "plugins": [],
                "logs": [],
                "total_time": 0.0,
                "total_files": 0,
                "plugin_count": 0,
                "running": False,
            }
            entries.append(_SessionEntry("No scans", ("empty", 0), placeholder))
            lookup[("empty", 0)] = placeholder
        labels = [entry.label for entry in entries]
        self._session_entries = entries
        self._session_lookup = lookup
        if hasattr(self, "session_cb"):
            self.session_cb["values"] = labels
        selected = None
        if self._selected_key:
            for entry in entries:
                if entry.key == self._selected_key:
                    selected = entry
                    break
        if not selected:
            selected = entries[0]
        self._selected_key = selected.key
        self.session_var.set(selected.label)
        self._render_snapshot(selected.snapshot)
        if hasattr(self, "session_cb"):
            try:
                idx = labels.index(selected.label)
                self.session_cb.current(idx)
            except ValueError:
                pass

    # ------------------------------------------------------------------
    def _on_session_changed(self) -> None:
        if not self._session_entries:
            return
        label = self.session_var.get()
        entry = next((e for e in self._session_entries if e.label == label), None)
        if not entry:
            entry = self._session_entries[0]
        self._selected_key = entry.key
        snapshot = self._session_lookup.get(entry.key)
        if snapshot:
            self._render_snapshot(snapshot)

    def _render_snapshot(self, snapshot: Dict[str, object]) -> None:
        tree = self.tree
        if tree:
            tree.delete(*tree.get_children())
            plugins = snapshot.get("plugins") or []
            for idx, plugin in enumerate(plugins):
                status_text = str(plugin.get("status", "Done")).lower()
                label = _STATUS_ICONS.get(status_text, plugin.get("status", ""))
                if plugin.get("warnings") and status_text not in {"warning", "warn"}:
                    label = _STATUS_ICONS.get("warning")
                tag = "even" if idx % 2 else "odd"
                tree.insert(
                    "",
                    "end",
                    values=(
                        plugin.get("name", "Plugin"),
                        f"{float(plugin.get('time', 0.0)):.2f}",
                        int(plugin.get("files", 0) or 0),
                        int(plugin.get("warnings", 0) or 0),
                        label,
                    ),
                    tags=(tag,),
                )
        total_time = float(snapshot.get("total_time", 0.0) or 0.0)
        total_files = int(snapshot.get("total_files", 0) or 0)
        plugin_count = int(snapshot.get("plugin_count", 0) or 0)
        running = bool(snapshot.get("running"))
        summary_parts = [
            f"Total Time: {total_time:.2f} s",
            f"Total Files: {total_files}",
            f"Plugins: {plugin_count}",
        ]
        if running:
            summary_parts.append("(running)")
        self.summary_var.set(" | ".join(summary_parts))
        logs = snapshot.get("logs") or []
        lines: List[str] = []
        for entry in logs:
            offset = entry.get("offset", "00:00")
            message = str(entry.get("message", ""))
            level = str(entry.get("level", "info")).lower()
            plugin = entry.get("plugin")
            prefix = ""
            if level == "warn":
                prefix = "⚠️ "
            elif level == "error":
                prefix = "❌ "
            elif level == "info":
                prefix = ""
            if plugin:
                message = f"{plugin}: {message}"
            lines.append(f"[{offset}] {prefix}{message}")
        log_widget = self.log_text
        if log_widget:
            log_widget.configure(state="normal")
            log_widget.delete("1.0", "end")
            log_widget.insert("end", "\n".join(lines) if lines else "No events recorded yet.")
            log_widget.configure(state="disabled")

    # ------------------------------------------------------------------
    def _export_current(self, fmt: str) -> None:
        if not self._selected_key:
            messagebox.showinfo("Scan Insights", "No session selected.", parent=self.window)
            return
        snapshot = self._session_lookup.get(self._selected_key)
        if not snapshot:
            messagebox.showinfo("Scan Insights", "No session data available.", parent=self.window)
            return
        if fmt == "json":
            path = filedialog.asksaveasfilename(
                title="Export Scan Session",
                defaultextension=".json",
                filetypes=[("JSON", "*.json"), ("All Files", "*.*")],
            )
            if not path:
                return
            try:
                Path(path).write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
            except Exception as exc:
                messagebox.showerror("Export Failed", str(exc), parent=self.window)
                return
        elif fmt == "csv":
            path = filedialog.asksaveasfilename(
                title="Export Scan Session",
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv"), ("All Files", "*.*")],
            )
            if not path:
                return
            try:
                with open(path, "w", encoding="utf-8", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(["Plugin", "Time (s)", "Files", "Warnings", "Status"])
                    for plugin in snapshot.get("plugins", []):
                        writer.writerow(
                            [
                                plugin.get("name", "Plugin"),
                                f"{float(plugin.get('time', 0.0)):.3f}",
                                int(plugin.get("files", 0) or 0),
                                int(plugin.get("warnings", 0) or 0),
                                plugin.get("status", ""),
                            ]
                        )
            except Exception as exc:
                messagebox.showerror("Export Failed", str(exc), parent=self.window)
                return
        else:
            return
        messagebox.showinfo("Scan Insights", f"Exported session to {path}", parent=self.window)


PLUGIN: Optional[ScanTimelineVisualizerPlugin] = None


def register(api) -> None:
    global PLUGIN
    PLUGIN = ScanTimelineVisualizerPlugin(api)
    PLUGIN.register()
