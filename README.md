# Sims4ModSorter

Sort Sims 4 mods with Modly – a single-file desktop assistant that scans, classifies, and moves packages into tidy folders. The application ships with a modern Tkinter interface, natural sorting, and undo support so you can keep control of large mod libraries.

## Requirements

* Python 3.10 or later
* Windows or macOS with Tkinter available (comes with the standard Python installer)

## Running the app

1. Install Python 3.10+ if it is not already available on your system.
2. Download the repository or clone it:
   ```bash
   git clone https://github.com/your-account/Sims4ModSorter.git
   cd Sims4ModSorter
   ```
3. Launch the sorter UI:
   ```bash
   python Sims4ModSorter.py
   ```

The main window lets you browse to your *Mods* directory, start a scan, and review suggested destinations for every package or script. Use the right-hand inspector to change a file’s category, toggle whether it will be moved, or batch-assign categories based on a keyword. When you are happy with the plan, click **Complete Sorting** to move the selected items into their target folders. The move log is saved in `.sims4_modsorter_moves.json`, enabling the **Undo Last** button to roll back the previous batch.

### Settings and themes

Select the cog button in the toolbar to open the inline settings overlay. From there you can:

* Switch between built-in or custom themes using the preview grid.
* Decide whether scans recurse into sub-folders.
* Provide comma-separated file extensions or filename fragments to ignore during a scan.

Any changes take effect immediately when you click **Apply** or **Done**.

## Modding the sorter

Sims4ModSorter now exposes a lightweight plugin system. Drop Python-based mods into the `user_mods/` directory next to `Sims4ModSorter.py` and they will be imported on launch. Mods receive a `ModAPI` instance and can:

* Register pre-scan hooks to adjust scan parameters (ignored extensions, scan path, etc.).
* Register post-scan hooks to inspect or modify the planned file list.
* Register additional UI themes.
* Emit log messages that appear in the application console.

Each mod directory should contain a `mod.json` manifest (created automatically by the helper script below) that describes the entry script, registration callable, and whether the mod is enabled.

### Managing mods with `mod_manager.py`

Use the bundled CLI script to import, enable, or disable mods safely without editing manifests by hand:

```bash
# List installed mods
python mod_manager.py list

# Import a standalone script (entry callable defaults to `register`)
python mod_manager.py import path/to/my_mod.py --name "My Mod"

# Import a zipped mod but keep it disabled until you have tested it
python mod_manager.py import path/to/my_mod.zip --disable

# Enable or disable a mod by name or folder
python mod_manager.py enable "My Mod"
python mod_manager.py disable my_mod
```

Imported mods live in `user_mods/<folder>`. Disabling a mod toggles the `enabled` flag in its manifest so that the sorter loads without executing the plugin, keeping your main code safe from experimental additions.
