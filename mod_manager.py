"""CLI utility to manage Sims4ModSorter user mods."""
from __future__ import annotations

import argparse
import io
import json
import re
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Dict, Optional

USER_MODS_DIR = Path(__file__).resolve().parent / "user_mods"


def sanitize_name(name: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_-]+", "-", name.strip())
    safe = safe.strip("-_")
    return safe or "mod"


def ensure_mods_dir() -> None:
    USER_MODS_DIR.mkdir(parents=True, exist_ok=True)


def load_manifest(mod_dir: Path) -> Dict[str, object]:
    manifest_path = mod_dir / "mod.json"
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {
        "name": mod_dir.name,
        "entry": "mod.py",
        "enabled": True,
        "callable": "register",
    }


def gather_mods() -> list[Dict[str, object]]:
    ensure_mods_dir()
    mods: list[Dict[str, object]] = []
    for mod_dir in sorted(USER_MODS_DIR.iterdir(), key=lambda d: d.name.lower()):
        if not mod_dir.is_dir():
            continue
        manifest = load_manifest(mod_dir)
        mods.append(
            {
                "folder": mod_dir.name,
                "name": str(manifest.get("name", mod_dir.name)),
                "enabled": bool(manifest.get("enabled", True)),
                "entry": str(manifest.get("entry", "mod.py")),
            }
        )
    return mods


def write_manifest(mod_dir: Path, manifest: Dict[str, object]) -> None:
    manifest_path = mod_dir / "mod.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)


def copy_directory(src: Path, dest: Path) -> None:
    for item in src.iterdir():
        target = dest / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True)
        else:
            shutil.copy2(item, target)


def import_mod(args: argparse.Namespace) -> int:
    source = Path(args.source).expanduser().resolve()
    if not source.exists():
        print(f"Source not found: {source}", file=sys.stderr)
        return 1

    ensure_mods_dir()
    mod_name = args.name or source.stem
    folder_name = sanitize_name(mod_name)
    dest_dir = USER_MODS_DIR / folder_name

    if dest_dir.exists():
        if args.overwrite:
            shutil.rmtree(dest_dir)
        else:
            print(f"Mod '{folder_name}' already exists. Use --overwrite to replace it.", file=sys.stderr)
            return 1

    dest_dir.mkdir(parents=True, exist_ok=True)

    entry = args.entry
    callable_name = args.callable

    if source.is_file() and zipfile.is_zipfile(source):
        with zipfile.ZipFile(source, "r") as zf:
            zf.extractall(dest_dir)
        if not entry:
            potential = dest_dir / "mod.py"
            if potential.exists():
                entry = "mod.py"
    elif source.is_file():
        entry = entry or source.name
        target_path = dest_dir / entry
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target_path)
    elif source.is_dir():
        copy_directory(source, dest_dir)
        if not entry:
            potential = dest_dir / "mod.py"
            if potential.exists():
                entry = "mod.py"
    else:
        print(f"Unsupported source type: {source}", file=sys.stderr)
        shutil.rmtree(dest_dir, ignore_errors=True)
        return 1

    entry = entry or "mod.py"
    if not (dest_dir / entry).exists():
        print(f"Entry file '{entry}' not found inside installed mod. Adjust --entry.", file=sys.stderr)
        shutil.rmtree(dest_dir, ignore_errors=True)
        return 1

    manifest = {
        "name": mod_name,
        "entry": entry,
        "enabled": not args.disable,
        "callable": callable_name,
    }
    write_manifest(dest_dir, manifest)
    status = "disabled" if args.disable else "enabled"
    print(f"Imported mod '{mod_name}' into {dest_dir.name} ({status}).")
    return 0


def _capture_cli(func, *args, **kwargs) -> tuple[int, str, str]:
    stdout = sys.stdout
    stderr = sys.stderr
    out_buffer = io.StringIO()
    err_buffer = io.StringIO()
    try:
        sys.stdout = out_buffer
        sys.stderr = err_buffer
        code = func(*args, **kwargs)
    finally:
        sys.stdout = stdout
        sys.stderr = stderr
    return code, out_buffer.getvalue().strip(), err_buffer.getvalue().strip()


def list_mods(_args: argparse.Namespace) -> int:
    ensure_mods_dir()
    entries = [d for d in USER_MODS_DIR.iterdir() if d.is_dir()]
    if not entries:
        print("No mods installed.")
        return 0

    print(f"Mods in {USER_MODS_DIR}:")
    for mod_dir in sorted(entries, key=lambda d: d.name.lower()):
        manifest = load_manifest(mod_dir)
        status = "enabled" if manifest.get("enabled", True) else "disabled"
        entry = manifest.get("entry", "mod.py")
        print(f"- {manifest.get('name', mod_dir.name)} [{status}] -> {mod_dir.name}/{entry}")
    return 0


def find_mod(identifier: str) -> Optional[Path]:
    ensure_mods_dir()
    candidate = USER_MODS_DIR / sanitize_name(identifier)
    if candidate.exists():
        return candidate
    identifier_lower = identifier.lower()
    for mod_dir in USER_MODS_DIR.iterdir():
        if not mod_dir.is_dir():
            continue
        manifest = load_manifest(mod_dir)
        name = str(manifest.get("name", ""))
        if name.lower() == identifier_lower:
            return mod_dir
    return None


def set_enabled(identifier: str, enabled: bool) -> int:
    mod_dir = find_mod(identifier)
    if not mod_dir:
        print(f"Mod '{identifier}' not found.", file=sys.stderr)
        return 1
    manifest = load_manifest(mod_dir)
    manifest["enabled"] = enabled
    write_manifest(mod_dir, manifest)
    state = "enabled" if enabled else "disabled"
    print(f"Mod '{manifest.get('name', mod_dir.name)}' is now {state}.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage Sims4ModSorter user mods.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    imp = subparsers.add_parser("import", help="Import a mod into the user_mods directory")
    imp.add_argument("source", help="Path to a .py file, directory, or .zip archive")
    imp.add_argument("--name", help="Display name for the mod")
    imp.add_argument("--entry", help="Relative entry file inside the mod", default=None)
    imp.add_argument("--callable", help="Registration callable name", default="register")
    imp.add_argument("--disable", action="store_true", help="Import the mod but leave it disabled")
    imp.add_argument("--overwrite", action="store_true", help="Replace an existing mod with the same name")
    imp.set_defaults(func=import_mod)

    ls = subparsers.add_parser("list", help="List installed mods")
    ls.set_defaults(func=list_mods)

    enable = subparsers.add_parser("enable", help="Enable a mod")
    enable.add_argument("identifier", help="Folder name or mod display name")
    enable.set_defaults(func=lambda args: set_enabled(args.identifier, True))

    disable = subparsers.add_parser("disable", help="Disable a mod")
    disable.add_argument("identifier", help="Folder name or mod display name")
    disable.set_defaults(func=lambda args: set_enabled(args.identifier, False))

    return parser


def interactive_main() -> int:
    ensure_mods_dir()
    print("Sims4 Mod Sorter - Mod Manager")
    print("Manage user plugins without needing command-line arguments.")
    while True:
        print("\nOptions:")
        print("  1) Import mod")
        print("  2) List mods")
        print("  3) Enable mod")
        print("  4) Disable mod")
        print("  5) Quit")
        try:
            choice = input("Select an option: ").strip()
        except EOFError:
            print()
            return 0
        if choice == "1":
            source = input("Path to .py/.zip/directory: ").strip()
            name = input("Display name (optional): ").strip() or None
            entry = input("Entry file (optional): ").strip() or None
            callable_name = input("Callable (default register): ").strip() or "register"
            overwrite = input("Overwrite existing? [y/N]: ").strip().lower() == "y"
            disable = input("Import disabled? [y/N]: ").strip().lower() == "y"
            args = argparse.Namespace(
                source=source,
                name=name,
                entry=entry,
                callable=callable_name,
                overwrite=overwrite,
                disable=disable,
            )
            import_mod(args)
        elif choice == "2":
            list_mods(argparse.Namespace())
        elif choice == "3":
            identifier = input("Folder name or display name: ").strip()
            if identifier:
                set_enabled(identifier, True)
        elif choice == "4":
            identifier = input("Folder name or display name: ").strip()
            if identifier:
                set_enabled(identifier, False)
        elif choice == "5" or choice.lower() in {"q", "quit", "exit"}:
            return 0
        else:
            print("Invalid selection. Choose 1-5.")


def run_gui() -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, simpledialog, ttk
    except Exception as exc:
        print(f"GUI mode unavailable: {exc}", file=sys.stderr)
        return interactive_main()

    ensure_mods_dir()
    root = tk.Tk()
    root.title("Sims4 Mod Sorter - Mod Manager")
    root.geometry("560x360")
    root.minsize(520, 320)

    columns = ("name", "status", "entry", "folder")
    tree = ttk.Treeview(root, columns=columns, show="headings", selectmode="browse")
    headings = {
        "name": "Name",
        "status": "Status",
        "entry": "Entry",
        "folder": "Folder",
    }
    widths = {"name": 200, "status": 80, "entry": 140, "folder": 120}
    for column in columns:
        tree.heading(column, text=headings[column])
        tree.column(column, width=widths[column], anchor="w")
    yscroll = ttk.Scrollbar(root, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=yscroll.set)
    tree.grid(row=0, column=0, columnspan=3, sticky="nsew", padx=(10, 0), pady=(10, 0))
    yscroll.grid(row=0, column=3, sticky="ns", pady=(10, 0), padx=(0, 10))
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)

    status_var = tk.StringVar()
    status_label = ttk.Label(root, textvariable=status_var)
    status_label.grid(row=1, column=0, columnspan=4, sticky="w", padx=10, pady=(6, 6))

    def set_status(message: str) -> None:
        status_var.set(message)

    def refresh_tree() -> None:
        tree.delete(*tree.get_children())
        mods = gather_mods()
        if not mods:
            set_status("No mods installed.")
        else:
            set_status(f"Loaded {len(mods)} mod(s).")
        for mod in mods:
            status = "Enabled" if mod["enabled"] else "Disabled"
            tree.insert("", "end", iid=mod["folder"], values=(mod["name"], status, mod["entry"], mod["folder"]))

    def get_selection() -> Optional[str]:
        sel = tree.selection()
        return sel[0] if sel else None

    def gui_import_file() -> None:
        path = filedialog.askopenfilename(
            parent=root,
            title="Select mod file",
            filetypes=[
                ("Python scripts", "*.py"),
                ("Zip archives", "*.zip"),
                ("All files", "*.*"),
            ],
        )
        if not path:
            return
        source = Path(path)
        default_name = source.stem
        name = simpledialog.askstring("Mod Name", "Display name:", initialvalue=default_name, parent=root)
        if name is None:
            return
        overwrite = messagebox.askyesno("Overwrite?", "Overwrite existing mod with the same folder name?", parent=root)
        disable = messagebox.askyesno("Import disabled?", "Import this mod disabled until you enable it?", parent=root)
        args = argparse.Namespace(
            source=str(source),
            name=name,
            entry=None,
            callable="register",
            overwrite=overwrite,
            disable=disable,
        )
        code, out_msg, err_msg = _capture_cli(import_mod, args)
        if code == 0:
            messagebox.showinfo("Import Complete", out_msg or "Mod imported successfully.", parent=root)
            refresh_tree()
        else:
            messagebox.showerror("Import Failed", err_msg or "Unable to import mod.", parent=root)

    def gui_import_folder() -> None:
        path = filedialog.askdirectory(parent=root, title="Select mod folder")
        if not path:
            return
        source = Path(path)
        name = simpledialog.askstring("Mod Name", "Display name:", initialvalue=source.name, parent=root)
        if name is None:
            return
        overwrite = messagebox.askyesno("Overwrite?", "Overwrite existing mod with the same folder name?", parent=root)
        disable = messagebox.askyesno("Import disabled?", "Import this mod disabled until you enable it?", parent=root)
        args = argparse.Namespace(
            source=str(source),
            name=name,
            entry=None,
            callable="register",
            overwrite=overwrite,
            disable=disable,
        )
        code, out_msg, err_msg = _capture_cli(import_mod, args)
        if code == 0:
            messagebox.showinfo("Import Complete", out_msg or "Mod imported successfully.", parent=root)
            refresh_tree()
        else:
            messagebox.showerror("Import Failed", err_msg or "Unable to import mod.", parent=root)

    def gui_toggle(enabled: bool) -> None:
        selection = get_selection()
        if not selection:
            messagebox.showwarning("No Selection", "Select a mod first.", parent=root)
            return
        code, out_msg, err_msg = _capture_cli(set_enabled, selection, enabled)
        if code == 0:
            verb = "enabled" if enabled else "disabled"
            messagebox.showinfo("Success", out_msg or f"Mod {verb}.", parent=root)
            refresh_tree()
        else:
            messagebox.showerror("Error", err_msg or "Unable to update mod.", parent=root)

    button_frame = ttk.Frame(root)
    button_frame.grid(row=2, column=0, columnspan=4, sticky="ew", padx=10, pady=(0, 10))
    button_frame.columnconfigure((0, 1, 2, 3), weight=1)

    ttk.Button(button_frame, text="Import File", command=gui_import_file).grid(row=0, column=0, padx=4)
    ttk.Button(button_frame, text="Import Folder", command=gui_import_folder).grid(row=0, column=1, padx=4)
    ttk.Button(button_frame, text="Enable", command=lambda: gui_toggle(True)).grid(row=0, column=2, padx=4)
    ttk.Button(button_frame, text="Disable", command=lambda: gui_toggle(False)).grid(row=0, column=3, padx=4)
    ttk.Button(button_frame, text="Refresh", command=refresh_tree).grid(row=1, column=0, columnspan=2, pady=(6, 0))
    ttk.Button(button_frame, text="Close", command=root.destroy).grid(row=1, column=2, columnspan=2, pady=(6, 0))

    refresh_tree()
    set_status("Manage your Sims4 Mod Sorter plugins.")
    root.mainloop()
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        print()
        stdin = sys.stdin
        has_tty = bool(getattr(stdin, "isatty", lambda: False)()) if stdin is not None else False
        if not has_tty:
            try:
                return run_gui()
            except Exception as exc:
                print(f"GUI startup failed: {exc}", file=sys.stderr)
                return 1
        try:
            return interactive_main()
        except KeyboardInterrupt:
            print()
            return 0
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
