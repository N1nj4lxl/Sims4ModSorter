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

### Command Center dashboard

On launch a modal **Command Center** appears with quick shortcuts for common tasks:

* Start a scan immediately, open the inline settings overlay, or jump straight to the plugin manager.
* Switch between recently used Mods directories without re-opening the folder chooser.
* Apply loadout presets with a single click.
* Review plugin-provided launch panels—by default the bundled dependency tracker highlights missing frameworks and links directly to its overlay.

Prefer to jump straight to the grid? Disable the modal from **Settings → Launch**. The toolbar’s new **Command Center** button keeps the dashboard close at hand even after you dismiss it.

### Loadouts

The toolbar’s **Loadout** selector lets you switch between saved include/exclude profiles. Create, rename, or delete loadouts from the inspector panel; each profile captures the `✔` state of every file and is persisted to `.sims4_modsorter_loadouts.json` inside the Mods directory. Applying a loadout updates the entire plan immediately so you can prepare different sets of mods (for example, “Family Friendly” or “Challenge Run”) without rescanning. Exported plans now bundle loadout metadata and the active selection, and importing a plan can restore both the file choices and any shared loadout definitions.

### Default target layout

Mods are mapped to a concise folder tree that keeps everything to a single level of nesting. The default destinations are:

| Category | Target folder |
| --- | --- |
| Adult Script, Gameplay, Animation, Pose, CAS, BuildBuy, Override, Other | `Adult/…` (e.g. `Adult/Scripts`, `Adult/Gameplay`)
| Script Mod, Gameplay Tuning | `Gameplay/Scripts`, `Gameplay/Tuning`
| CAS Hair, Clothing, Makeup, Skin, Eyes, Accessories, Sliders | `CAS/…`
| BuildBuy Object, Recolour | `BuildBuy/Objects`, `BuildBuy/Recolours`
| Animation, Pose, Preset | `Visuals/Animations`, `Visuals/Poses`, `Visuals/Presets`
| World | `World`
| Override | `Overrides`
| Utility Tool | `Utilities`
| Archive | `Archives`
| Other | `Misc`
| Unknown | `Unsorted`

You can still change any destination in the results grid before finalising a sort, but the rebuilt defaults keep related content together without creating deeply nested folder trees.

### Settings and themes

Select the cog button in the toolbar to open the inline settings overlay. From there you can:

* Switch between built-in or custom themes using the preview grid.
* Decide whether scans recurse into sub-folders.
* Provide comma-separated file extensions or filename fragments to ignore during a scan.

Any changes take effect immediately when you click **Apply** or **Done**.

### Duplicate review tools

Every scanned file now carries a short fingerprint in its `extras` payload so the sorter can detect duplicates without hashing
the entire archive. When two or more entries share the same fingerprint:

* The **Dup** column shows a ⚠️ marker on each secondary copy and the tooltip lists the relative path of the primary file so you
  can review the original quickly.
* Primaries receive a tooltip explaining how many duplicates point at them.
* The **Duplicates only** quick filter above the table hides everything except flagged copies, making bulk clean-up easy. The
  toggle disables itself when no duplicates are detected.
* Exported plans now include the duplicate flag and fingerprint so that external tooling can reconcile records or build reports.

## Modding the sorter

Sims4ModSorter now exposes a lightweight plugin system. Drop Python-based plugins into the `user_plugins/` directory next to `Sims4ModSorter.py` and they will be imported on launch. Plugins receive a `PluginAPI` instance and can:

* Register pre-scan hooks to adjust scan parameters (ignored extensions, scan path, etc.).
* Register post-scan hooks to inspect or modify the planned file list.
* Register additional UI themes.
* Emit log messages that appear in the application console.

Each plugin directory should contain a `plugin.json` manifest (created automatically by the helper script below) that describes the entry script, registration callable, and whether the plugin is enabled.

The plugin API exposes `api.reserved_extra_keys()` and `api.is_reserved_extra()` so extensions can avoid overwriting the sorter’s internal metadata (such as the duplicate marker and fingerprint). When manipulating `item.extras`, leave unknown keys unchanged to preserve built-in functionality.

### Managing plugins with the Plugin Manager

Launch the dedicated Plugin Manager UI to import, enable, or disable plugins without editing manifests by hand:

```bash
python plugin_manager.py
```

The window lists every plugin in `user_plugins/`, highlights whether it is enabled, and shows any compatibility warnings (for example when a plugin requires a newer version of the sorter). Use the toolbar buttons to:

* **Import File** – add a single `.py` file or zipped package as a plugin.
* **Import Folder** – register an unpacked plugin directory.
* **Enable/Disable** – toggle the selected plugin.
* **Remove** – delete the plugin’s folder from disk.

The status bar confirms actions and the **Open Folder** button jumps straight to the plugin directory so you can make manual edits if required.

### Included example plugin

The repository ships with an enabled sample plugin in `user_plugins/example_theme_plugin`. It demonstrates how to:

* Register a custom "Ocean Breeze" theme that appears in the settings overlay previews.
* Extend the scan by adding `.bak` and `.tmp` files to the ignored extension list.
* Annotate `.package` files that contain "preview" in their filename so they are easy to review in the results grid.

Launch the sorter normally (`python Sims4ModSorter.py`) and you will see log entries confirming that the example plugin loaded. You can disable or remove it later with `python plugin_manager.py disable example_theme_plugin` once you are ready to build your own plugins.

### Dependency Tracker plugin

The `user_plugins/dependency_tracker` plugin is bundled and enabled by default. It adds a **Deps** column to the results table, an export payload, and settings controls that help you stay on top of framework requirements:

* During scans it inspects `.package` and `.ts4script` files for phrases that match the local `known_dependencies.json` database. Recognised mods receive a ✅ icon when their dependencies are present or ⚠️ when something is missing.
* Hover the icon to view a tooltip describing which frameworks were found or missing (for example, `Requires: UI Cheats Extension: MC Command Center (found – https://deaderpool-mccc.com/#/releases), TS4 Script Loader (missing – https://modthesims.info/d/479997/ts4-script-loader.html)`).
* The Export Plan JSON includes `dependency_status` and `dependency_detail` keys for each entry so external tools can audit reports.
* The Dependency Summary overlay now lists buttons (and a right-click context menu) for any missing dependencies that include a URL, allowing you to open the download page directly from the sorter.

The plugin ships with common Sims 4 frameworks such as **MC Command Center**, **XML Injector**, **Basemental Drugs**, and **TS4 Script Loader**, but you can expand the JSON file with your own dependencies whenever you discover a new plugin relationship. Entries may include optional `homepage` and/or `download_url` fields; when provided, these links appear in tooltips, exports, and the overlay so you and third-party plugins can jump straight to the official download source. Dependencies without a URL are still supported—the UI simply omits the button and menu options when no link is available.
