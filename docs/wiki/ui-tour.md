# User interface tour

Sims4ModSorter combines a Tkinter-powered grid, an inspector sidebar, and a Command Center modal to streamline sorting. This tour highlights each component so you can make the most of the workflow.

## Command Center dashboard

* Appears on launch by default. Disable it under **Settings → Launch** if you prefer to start directly in the grid.
* Provides one-click actions to start a scan, open settings, or jump to the plugin manager.
* Remembers recent Mods directories so you can switch between setups without re-browsing.
* Hosts plugin tiles. The bundled dependency checker lives here and points to missing frameworks.

## Toolbar

Located at the top of the main window, the toolbar includes:

* **Scan Mods** – Triggers a scan of the active Mods directory.
* **Complete Sorting** – Moves selected files into their target folders.
* **Undo Last** – Reverts the previous move batch using the recorded move log.
* **Loadout selector** – Quickly swap include/exclude profiles.
* **Command Center** – Re-open the dashboard at any time.
* **Settings** – Opens the inline settings overlay.

## Results grid

The central table lists every scanned file. Key columns include:

* **Name** – File basename with natural sorting.
* **Category** – Auto-detected category which determines the target folder. Click to change.
* **Target** – The folder where the file will move. Editable per entry.
* **✔ Toggle** – Whether the file will move when you finalise the sort.
* **Dup marker** – Appears on duplicate fingerprints. Tooltips show the primary file path.

Use the quick filters above the grid to isolate duplicates or search by keyword.

## Inspector pane

On the right, the inspector provides context-sensitive controls:

* Adjust the destination folder for the current selection.
* Batch-assign categories using keyword rules.
* Manage loadouts: create new profiles, rename, delete, or update the current profile’s selection state.
* Review metadata inserted by plugins under the **Extras** section.

## Status bar and log console

* The status bar confirms actions such as completed moves or plugin events.
* The expandable console captures plugin log messages, warnings, and errors to aid debugging.

## Settings overlay

Click the cog icon to open the settings overlay. From here you can:

* Switch between built-in and plugin-provided themes. A preview grid shows how the UI will look.
* Toggle recursive scanning into sub-folders.
* Provide ignored extensions or filename fragments.

All changes apply immediately when you click **Apply** or **Done**.
