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

### Included example mod

The repository ships with an enabled sample plugin in `user_mods/example_theme_mod`. It demonstrates how to:

* Register a custom "Ocean Breeze" theme that appears in the settings overlay previews.
* Extend the scan by adding `.bak` and `.tmp` files to the ignored extension list.
* Annotate `.package` files that contain "preview" in their filename so they are easy to review in the results grid.

Launch the sorter normally (`python Sims4ModSorter.py`) and you will see log entries confirming that the example mod loaded. You can disable or remove it later with `python mod_manager.py disable example_theme_mod` once you are ready to build your own plugins.

### Dependency Tracker plugin

The `user_mods/dependency_tracker` plugin is bundled and enabled by default. It adds a **Deps** column to the results table, an export payload, and settings controls that help you stay on top of framework requirements:

* During scans it inspects `.package` and `.ts4script` files for phrases that match the local `known_dependencies.json` database. Recognised mods receive a ✅ icon when their dependencies are present or ⚠️ when something is missing.
* Hover the icon to view a tooltip describing which frameworks were found or missing (for example, `Requires: UI Cheats Extension: MC Command Center (found), TS4 Script Loader (missing)`).
* The Export Plan JSON includes `dependency_status` and `dependency_detail` keys for each entry so external tools can audit reports.
* Open **Settings → Plugins → Dependency Tracker** to toggle tracking or reload the dependency list after editing `known_dependencies.json`. Reloading runs the analysis again without forcing a rescan.

The plugin ships with common Sims 4 frameworks such as **MC Command Center**, **XML Injector**, **Basemental Drugs**, and **TS4 Script Loader**, but you can expand the JSON file with your own dependencies whenever you discover a new mod relationship.
