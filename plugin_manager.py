"""Interactive Plugin Manager for Sims4 Mod Sorter."""
from __future__ import annotations

import json
import os
import re
import shutil
import sys
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

BASE_DIR = Path(__file__).resolve().parent
USER_PLUGINS_DIR = BASE_DIR / "user_plugins"
VERSION_FILE = BASE_DIR / "VERSION"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def sanitize_name(name: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_-]+", "-", name.strip())
    safe = safe.strip("-_")
    return safe or "plugin"


def ensure_plugins_dir() -> None:
    USER_PLUGINS_DIR.mkdir(parents=True, exist_ok=True)


def read_version() -> str:
    try:
        return VERSION_FILE.read_text(encoding="utf-8").strip() or "0.0.0"
    except Exception:
        return "0.0.0"


APP_VERSION = read_version()


def parse_version(value: str) -> List[int]:
    parts: List[int] = []
    for token in re.split(r"[^0-9]+", value):
        if not token:
            continue
        try:
            parts.append(int(token))
        except ValueError:
            parts.append(0)
    return parts or [0]


def compare_versions(current: str, required: str) -> int:
    left = parse_version(current)
    right = parse_version(required)
    for idx in range(max(len(left), len(right))):
        a = left[idx] if idx < len(left) else 0
        b = right[idx] if idx < len(right) else 0
        if a != b:
            return 1 if a > b else -1
    return 0


def load_manifest(plugin_dir: Path) -> Dict[str, object]:
    manifest_path = plugin_dir / "plugin.json"
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {
        "name": plugin_dir.name,
        "entry": "plugin.py",
        "enabled": True,
        "callable": "register",
    }


def parse_features(manifest: Dict[str, object]) -> List[PluginFeature]:
    features: List[PluginFeature] = []
    raw = manifest.get("features")
    if not isinstance(raw, list):
        return features
    for item in raw:
        if not isinstance(item, dict):
            continue
        feature_id = str(item.get("id") or item.get("feature_id") or "").strip()
        if not feature_id:
            continue
        name = str(item.get("name") or feature_id)
        description = str(item.get("description") or "")
        default_value = item.get("default")
        if default_value is None:
            default_enabled = True
        else:
            default_enabled = bool(default_value)
        enabled_value = item.get("enabled")
        if enabled_value is None:
            enabled_flag = default_enabled
        else:
            enabled_flag = bool(enabled_value)
        features.append(
            PluginFeature(
                feature_id=feature_id,
                name=name,
                description=description,
                enabled=enabled_flag,
                default=default_enabled,
            )
        )
    return features


def write_manifest(plugin_dir: Path, manifest: Dict[str, object]) -> None:
    manifest_path = plugin_dir / "plugin.json"
    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)


def copy_directory(src: Path, dest: Path) -> None:
    for item in src.iterdir():
        target = dest / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True)
        else:
            shutil.copy2(item, target)


def determine_version(manifest: Dict[str, object], module_path: Path) -> str:
    version = manifest.get("version")
    if isinstance(version, str) and version.strip():
        return version.strip()
    module_parent = module_path.parent
    version_file = module_parent / "VERSION"
    if version_file.exists():
        try:
            value = version_file.read_text(encoding="utf-8").strip()
            if value:
                return value
        except Exception:
            pass
    return "Unknown"


def compatibility_message(manifest: Dict[str, object]) -> Optional[str]:
    minimum = manifest.get("min_sorter_version") or manifest.get("min_app_version")
    maximum = manifest.get("max_sorter_version") or manifest.get("max_app_version")
    if isinstance(minimum, str) and minimum.strip():
        if compare_versions(APP_VERSION, minimum.strip()) < 0:
            return f"Requires sorter {minimum.strip()} or newer"
    if isinstance(maximum, str) and maximum.strip():
        if compare_versions(APP_VERSION, maximum.strip()) > 0:
            return f"Incompatible with sorter newer than {maximum.strip()}"
    return None


def describe_source(path: Path) -> str:
    if path.is_file():
        return path.name
    return path.name + "/"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class PluginFeature:
    feature_id: str
    name: str
    description: str
    enabled: bool
    default: bool = True


@dataclass
class PluginEntry:
    name: str
    folder: str
    version: str
    enabled: bool
    source: Path
    status: str
    message: str = ""
    features: List[PluginFeature] = field(default_factory=list)


class PluginLibrary:
    """Filesystem operations for plugins."""

    def __init__(self, root: Path) -> None:
        self.root = root
        ensure_plugins_dir()

    def refresh(self) -> List[PluginEntry]:
        entries: List[PluginEntry] = []
        for plugin_dir in sorted(self.root.iterdir(), key=lambda p: p.name.lower()):
            if not plugin_dir.is_dir():
                continue
            manifest = load_manifest(plugin_dir)
            entry_path = plugin_dir / str(manifest.get("entry", "plugin.py"))
            name = str(manifest.get("name") or plugin_dir.name)
            enabled = bool(manifest.get("enabled", True))
            version = determine_version(manifest, entry_path)
            status = "Enabled" if enabled else "Disabled"
            message = ""
            features = parse_features(manifest)
            compat = compatibility_message(manifest)
            if compat:
                status = "Blocked"
                message = compat
            elif not entry_path.exists():
                status = "Error"
                message = f"Missing entry: {entry_path.name}"
            entries.append(
                PluginEntry(
                    name=name,
                    folder=plugin_dir.name,
                    version=version,
                    enabled=enabled,
                    source=plugin_dir,
                    status=status,
                    message=message,
                    features=features,
                )
            )
        return entries

    # Import helpers -----------------------------------------------------
    def import_from_source(
        self,
        source: Path,
        *,
        name: Optional[str] = None,
        entry: Optional[str] = None,
        callable_name: str = "register",
        disable: bool = False,
        overwrite: bool = False,
    ) -> PluginEntry:
        if not source.exists():
            raise FileNotFoundError(str(source))
        ensure_plugins_dir()
        plugin_name = name or source.stem
        folder_name = sanitize_name(plugin_name)
        dest_dir = self.root / folder_name
        if dest_dir.exists():
            if overwrite:
                shutil.rmtree(dest_dir)
            else:
                raise FileExistsError(f"Plugin '{folder_name}' already exists")
        dest_dir.mkdir(parents=True, exist_ok=True)
        chosen_entry = entry
        if source.is_file() and zipfile.is_zipfile(source):
            with zipfile.ZipFile(source, "r") as archive:
                archive.extractall(dest_dir)
            if not chosen_entry:
                default_entry = dest_dir / "plugin.py"
                if default_entry.exists():
                    chosen_entry = "plugin.py"
        elif source.is_file():
            chosen_entry = chosen_entry or source.name
            target = dest_dir / chosen_entry
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        elif source.is_dir():
            copy_directory(source, dest_dir)
            if not chosen_entry:
                default_entry = dest_dir / "plugin.py"
                if default_entry.exists():
                    chosen_entry = "plugin.py"
        else:
            shutil.rmtree(dest_dir, ignore_errors=True)
            raise ValueError(f"Unsupported source type: {source}")
        chosen_entry = chosen_entry or "plugin.py"
        if not (dest_dir / chosen_entry).exists():
            shutil.rmtree(dest_dir, ignore_errors=True)
            raise FileNotFoundError(f"Entry '{chosen_entry}' not found inside plugin")
        manifest = {
            "name": plugin_name,
            "entry": chosen_entry,
            "callable": callable_name,
            "enabled": not disable,
        }
        write_manifest(dest_dir, manifest)
        version = determine_version(manifest, dest_dir / chosen_entry)
        features = parse_features(manifest)
        status = "Enabled" if manifest["enabled"] else "Disabled"
        return PluginEntry(
            name=plugin_name,
            folder=folder_name,
            version=version,
            enabled=manifest["enabled"],
            source=dest_dir,
            status=status,
            message="Imported",
            features=features,
        )

    def set_enabled(self, identifier: str, enabled: bool) -> PluginEntry:
        plugin_dir = self._resolve(identifier)
        manifest = load_manifest(plugin_dir)
        manifest["enabled"] = enabled
        write_manifest(plugin_dir, manifest)
        version = determine_version(manifest, plugin_dir / str(manifest.get("entry", "plugin.py")))
        features = parse_features(manifest)
        status = "Enabled" if enabled else "Disabled"
        return PluginEntry(
            name=str(manifest.get("name") or plugin_dir.name),
            folder=plugin_dir.name,
            version=version,
            enabled=enabled,
            source=plugin_dir,
            status=status,
            message="",
            features=features,
        )

    def set_features(self, identifier: str, feature_states: Dict[str, bool]) -> PluginEntry:
        plugin_dir = self._resolve(identifier)
        manifest = load_manifest(plugin_dir)
        raw = manifest.get("features")
        if not isinstance(raw, list):
            raise ValueError("Plugin does not define any configurable features")
        updated_features: List[Dict[str, object]] = []
        for item in raw:
            if not isinstance(item, dict):
                updated_features.append(item)
                continue
            feature_id = str(item.get("id") or item.get("feature_id") or "").strip()
            payload = dict(item)
            if feature_id and feature_id in feature_states:
                payload["enabled"] = bool(feature_states[feature_id])
            updated_features.append(payload)
        manifest["features"] = updated_features
        write_manifest(plugin_dir, manifest)
        entry_path = plugin_dir / str(manifest.get("entry", "plugin.py"))
        version = determine_version(manifest, entry_path)
        enabled = bool(manifest.get("enabled", True))
        features = parse_features(manifest)
        status = "Enabled" if enabled else "Disabled"
        return PluginEntry(
            name=str(manifest.get("name") or plugin_dir.name),
            folder=plugin_dir.name,
            version=version,
            enabled=enabled,
            source=plugin_dir,
            status=status,
            message="",
            features=features,
        )

    def remove(self, identifier: str) -> None:
        plugin_dir = self._resolve(identifier)
        shutil.rmtree(plugin_dir, ignore_errors=True)

    def open_folder(self) -> None:
        ensure_plugins_dir()
        if sys.platform.startswith("win"):
            os.startfile(self.root)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            os.system(f"open '{self.root}'")
        else:
            os.system(f"xdg-open '{self.root}' >/dev/null 2>&1 &")

    # Internal -----------------------------------------------------------
    def _resolve(self, identifier: str) -> Path:
        folder = sanitize_name(identifier)
        candidate = self.root / folder
        if candidate.exists():
            return candidate
        target_lower = identifier.lower()
        for entry in self.root.iterdir():
            if not entry.is_dir():
                continue
            manifest = load_manifest(entry)
            name = str(manifest.get("name") or entry.name).lower()
            if name == target_lower:
                return entry
        raise FileNotFoundError(identifier)


# ---------------------------------------------------------------------------
# Tkinter UI
# ---------------------------------------------------------------------------


class PluginManagerApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Plugin Manager")
        self.geometry("760x520")
        self.minsize(680, 440)
        self.configure(bg="#1f1f24")

        self.library = PluginLibrary(USER_PLUGINS_DIR)
        self.entries: List[PluginEntry] = []
        self.status_var = tk.StringVar(value="Ready")

        self._build_style()
        self._build_ui()
        self.refresh()

    # UI construction ---------------------------------------------------
    def _build_style(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("TFrame", background="#1f1f24")
        style.configure("TLabel", background="#1f1f24", foreground="#f2f2f7")
        style.configure("TButton", background="#2d2d34", foreground="#f2f2f7", padding=8)
        style.map("TButton", background=[("active", "#3b3b44")])
        style.configure(
            "Treeview",
            background="#2a2a31",
            foreground="#f2f2f7",
            fieldbackground="#2a2a31",
            rowheight=28,
            bordercolor="#1f1f24",
            borderwidth=0,
        )
        style.map("Treeview", background=[("selected", "#454552")])
        style.configure("Treeview.Heading", background="#1f1f24", foreground="#f2f2f7")
        style.configure("Status.TLabel", background="#1f1f24", foreground="#9d9da6")

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=16)
        root.pack(fill="both", expand=True)

        header = ttk.Frame(root)
        header.pack(fill="x")
        ttk.Label(header, text="Installed Plugins", font=("Segoe UI", 14, "bold")).pack(side="left")
        ttk.Button(header, text="Open Folder", command=self.on_open_folder).pack(side="right")

        toolbar = ttk.Frame(root)
        toolbar.pack(fill="x", pady=(12, 8))
        ttk.Button(toolbar, text="Import File", command=self.on_import_file).pack(side="left")
        ttk.Button(toolbar, text="Import Folder", command=self.on_import_folder).pack(side="left", padx=(8, 0))
        ttk.Button(toolbar, text="Refresh", command=self.refresh).pack(side="left", padx=(8, 0))
        self.btn_enable = ttk.Button(toolbar, text="Enable", command=lambda: self.on_toggle(True))
        self.btn_enable.pack(side="left", padx=(24, 0))
        self.btn_disable = ttk.Button(toolbar, text="Disable", command=lambda: self.on_toggle(False))
        self.btn_disable.pack(side="left", padx=(8, 0))
        self.btn_settings = ttk.Button(toolbar, text="Settings", command=self.on_settings)
        self.btn_settings.pack(side="left", padx=(8, 0))
        self.btn_copy_error = ttk.Button(toolbar, text="Copy Error", command=self.on_copy_error)
        self.btn_copy_error.pack(side="left", padx=(8, 0))
        ttk.Button(toolbar, text="Remove", command=self.on_remove).pack(side="left", padx=(8, 0))

        columns = ("name", "folder", "version", "status", "message")
        self.tree = ttk.Treeview(root, columns=columns, show="headings")
        self.tree.heading("name", text="Name")
        self.tree.heading("folder", text="Folder")
        self.tree.heading("version", text="Version")
        self.tree.heading("status", text="Status")
        self.tree.heading("message", text="Details")
        self.tree.column("name", width=200, anchor="w")
        self.tree.column("folder", width=120, anchor="w")
        self.tree.column("version", width=100, anchor="center")
        self.tree.column("status", width=100, anchor="center")
        self.tree.column("message", width=240, anchor="w")
        self.tree.pack(fill="both", expand=True)
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._update_buttons())

        status_bar = ttk.Frame(root, padding=(0, 12, 0, 0))
        status_bar.pack(fill="x")
        ttk.Label(status_bar, textvariable=self.status_var, style="Status.TLabel").pack(side="left")

    # Actions ------------------------------------------------------------
    def refresh(self) -> None:
        try:
            self.entries = self.library.refresh()
            self._reload_tree()
            self.status_var.set(f"Loaded {len(self.entries)} plugin(s)")
        except Exception as exc:
            messagebox.showerror("Refresh failed", str(exc), parent=self)
            self.status_var.set("Refresh failed")
        self._update_buttons()

    def on_open_folder(self) -> None:
        try:
            self.library.open_folder()
        except Exception as exc:
            messagebox.showerror("Unable to open", str(exc), parent=self)

    def on_import_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Import Plugin",
            filetypes=[
                ("Python plugin", "*.py"),
                ("ZIP archive", "*.zip"),
                ("All files", "*.*"),
            ],
        )
        if not path:
            return
        self._handle_import(Path(path))

    def on_import_folder(self) -> None:
        path = filedialog.askdirectory(title="Import Plugin Folder")
        if not path:
            return
        self._handle_import(Path(path))

    def _handle_import(self, path: Path) -> None:
        dialog = ImportDialog(self, default_name=path.stem)
        self.wait_window(dialog)
        if not dialog.result:
            return
        name, entry, callable_name, disable, overwrite = dialog.result
        try:
            entry_obj = self.library.import_from_source(
                path,
                name=name or None,
                entry=entry or None,
                callable_name=callable_name or "register",
                disable=disable,
                overwrite=overwrite,
            )
        except Exception as exc:
            messagebox.showerror("Import failed", str(exc), parent=self)
            return
        self.refresh()
        self.status_var.set(f"Imported {entry_obj.name}")

    def on_toggle(self, enabled: bool) -> None:
        identifier = self._selected_folder()
        if not identifier:
            return
        try:
            entry = self.library.set_enabled(identifier, enabled)
        except Exception as exc:
            messagebox.showerror("Update failed", str(exc), parent=self)
            return
        self.refresh()
        state = "enabled" if enabled else "disabled"
        self.status_var.set(f"Plugin {entry.name} {state}")

    def on_settings(self) -> None:
        identifier = self._selected_folder()
        if not identifier:
            return
        entry = next((e for e in self.entries if e.folder == identifier), None)
        if not entry or not entry.features:
            messagebox.showinfo("Plugin Settings", "Selected plugin does not expose configurable features.", parent=self)
            return
        dialog = PluginSettingsDialog(self, entry)
        self.wait_window(dialog)
        if dialog.result is None:
            return
        try:
            updated = self.library.set_features(identifier, dialog.result)
        except Exception as exc:
            messagebox.showerror("Update failed", str(exc), parent=self)
            return
        self.refresh()
        self.status_var.set(f"Updated settings for {updated.name}")

    def on_remove(self) -> None:
        identifier = self._selected_folder()
        if not identifier:
            return
        entry = next((e for e in self.entries if e.folder == identifier), None)
        name = entry.name if entry else identifier
        if messagebox.askyesno("Remove Plugin", f"Remove '{name}'?", parent=self):
            try:
                self.library.remove(identifier)
            except Exception as exc:
                messagebox.showerror("Remove failed", str(exc), parent=self)
                return
            self.refresh()
            self.status_var.set(f"Removed {name}")

    def on_copy_error(self) -> None:
        identifier = self._selected_folder()
        if not identifier:
            return
        entry = next((e for e in self.entries if e.folder == identifier), None)
        if not entry or not entry.message:
            messagebox.showinfo("Copy Error", "Selected plugin has no error details to copy.", parent=self)
            return
        payload = f"{entry.name} [{entry.status}] - {entry.message}".strip()
        try:
            self.clipboard_clear()
            self.clipboard_append(payload)
        except Exception as exc:
            messagebox.showerror("Copy Error", f"Unable to copy details: {exc}", parent=self)
            return
        self.status_var.set(f"Copied error for {entry.name}")

    # Helpers ------------------------------------------------------------
    def _reload_tree(self) -> None:
        self.tree.delete(*self.tree.get_children())
        for entry in self.entries:
            values = (entry.name, entry.folder, entry.version, entry.status, entry.message)
            self.tree.insert("", "end", iid=entry.folder, values=values)

    def _selected_folder(self) -> Optional[str]:
        selection = self.tree.selection()
        if not selection:
            return None
        return selection[0]

    def _update_buttons(self) -> None:
        folder = self._selected_folder()
        if not folder:
            self.btn_enable.state(["disabled"])
            self.btn_disable.state(["disabled"])
            self.btn_settings.state(["disabled"])
            self.btn_copy_error.state(["disabled"])
            return
        entry = next((e for e in self.entries if e.folder == folder), None)
        if entry and entry.enabled:
            self.btn_enable.state(["disabled"])
            self.btn_disable.state(["!disabled"])
        else:
            self.btn_enable.state(["!disabled"])
            self.btn_disable.state(["disabled"])
        if entry and entry.features:
            self.btn_settings.state(["!disabled"])
        else:
            self.btn_settings.state(["disabled"])
        if entry and entry.message:
            self.btn_copy_error.state(["!disabled"])
        else:
            self.btn_copy_error.state(["disabled"])


class ImportDialog(tk.Toplevel):
    def __init__(self, parent: PluginManagerApp, default_name: str) -> None:
        super().__init__(parent)
        self.title("Import Plugin")
        self.configure(bg="#1f1f24")
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()
        self.result: Optional[tuple[str, str, str, bool, bool]] = None

        ttk.Frame(self, padding=16).pack(fill="both", expand=True)
        container = self.children[list(self.children.keys())[0]]

        ttk.Label(container, text="Plugin name:").grid(row=0, column=0, sticky="w")
        self.name_var = tk.StringVar(value=default_name)
        ttk.Entry(container, textvariable=self.name_var).grid(row=0, column=1, sticky="ew", padx=(8, 0))

        ttk.Label(container, text="Entry file (optional):").grid(row=1, column=0, sticky="w", pady=(10, 0))
        self.entry_var = tk.StringVar()
        ttk.Entry(container, textvariable=self.entry_var).grid(row=1, column=1, sticky="ew", padx=(8, 0), pady=(10, 0))

        ttk.Label(container, text="Callable (default register):").grid(row=2, column=0, sticky="w", pady=(10, 0))
        self.callable_var = tk.StringVar(value="register")
        ttk.Entry(container, textvariable=self.callable_var).grid(row=2, column=1, sticky="ew", padx=(8, 0), pady=(10, 0))

        self.disable_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(container, text="Import disabled", variable=self.disable_var).grid(row=3, column=1, sticky="w", pady=(12, 0))

        self.overwrite_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(container, text="Overwrite if exists", variable=self.overwrite_var).grid(row=4, column=1, sticky="w", pady=(6, 0))

        buttons = ttk.Frame(container)
        buttons.grid(row=5, column=0, columnspan=2, sticky="e", pady=(18, 0))
        ttk.Button(buttons, text="Cancel", command=self.destroy).pack(side="right")
        ttk.Button(buttons, text="Import", command=self._on_accept).pack(side="right", padx=(0, 8))

        container.columnconfigure(1, weight=1)
        self.bind("<Return>", lambda _e: self._on_accept())
        self.bind("<Escape>", lambda _e: self.destroy())

    def _on_accept(self) -> None:
        self.result = (
            self.name_var.get().strip(),
            self.entry_var.get().strip(),
            self.callable_var.get().strip(),
            self.disable_var.get(),
            self.overwrite_var.get(),
        )
        self.destroy()


class PluginSettingsDialog(tk.Toplevel):
    def __init__(self, parent: PluginManagerApp, entry: PluginEntry) -> None:
        super().__init__(parent)
        self.title(f"Settings · {entry.name}")
        self.configure(bg="#1f1f24")
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()
        self.result: Optional[Dict[str, bool]] = None
        self._feature_vars: Dict[str, tk.BooleanVar] = {}

        frame = ttk.Frame(self, padding=16)
        frame.pack(fill="both", expand=True)

        if not entry.features:
            ttk.Label(frame, text="This plugin does not expose configurable features.").pack()
        else:
            for index, feature in enumerate(entry.features):
                var = tk.BooleanVar(value=feature.enabled)
                self._feature_vars[feature.feature_id] = var
                ttk.Checkbutton(frame, text=feature.name, variable=var).grid(row=index * 2, column=0, sticky="w")
                description = feature.description.strip()
                if description:
                    ttk.Label(frame, text=description, wraplength=420, style="Status.TLabel").grid(
                        row=index * 2 + 1, column=0, sticky="w", pady=(0, 8)
                    )
                else:
                    ttk.Frame(frame).grid(row=index * 2 + 1, column=0, pady=(0, 8))

        buttons = ttk.Frame(frame)
        buttons.grid(row=len(entry.features) * 2 + 1, column=0, sticky="e", pady=(12, 0))
        ttk.Button(buttons, text="Cancel", command=self.destroy).pack(side="right")
        ttk.Button(buttons, text="Save", command=self._on_accept).pack(side="right", padx=(0, 8))

        self.bind("<Return>", lambda _e: self._on_accept())
        self.bind("<Escape>", lambda _e: self.destroy())

    def _on_accept(self) -> None:
        if not self._feature_vars:
            self.result = {}
        else:
            self.result = {key: bool(var.get()) for key, var in self._feature_vars.items()}
        self.destroy()


def main() -> None:
    app = PluginManagerApp()
    app.mainloop()


if __name__ == "__main__":
    main()
