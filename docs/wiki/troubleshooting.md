# Troubleshooting

If the sorter is not behaving as expected, use these checklists to diagnose and fix common issues.

## The UI will not launch

* Ensure you are using Python 3.10 or newer. Run `python --version`.
* Confirm Tkinter is available. On Windows/macOS the standard installer includes it. On Linux, install the `python3-tk` package from your distribution.
* If you are using a virtual environment, activate it before launching the app.

## Scans finish instantly or return no files

* Verify the Mods directory is correct. The path appears in the window title and status bar.
* Open **Settings → Scan** and ensure recursive scanning is enabled if your mods are nested.
* Review the ignored extensions list. Remove any entries that match your mod filenames.

## Duplicate markers look wrong

* Duplicates are determined by fingerprints stored in each item’s `extras` payload. If a plugin manipulates `extras`, make sure it avoids keys returned by `api.reserved_extra_keys()`.
* Rescan the Mods directory after updating mods so fingerprints refresh.

## Moves failed or only partially completed

* Check the status bar and console for error messages indicating permission issues.
* Make sure the Mods folder is not open in another program that locks files (such as an archive manager).
* Verify there is enough disk space for temporary copies if you are moving between drives.
* Review `.sims4_modsorter_moves.json` to determine which files moved successfully.

## Undo did not revert everything

* Undo only affects the most recent move batch. Run another sort to reverse earlier changes.
* Ensure `.sims4_modsorter_moves.json` exists and is writable. Without it, the app cannot track completed operations.

## Plugins are missing

* Open `python plugin_manager.py` and confirm the plugin appears and is enabled.
* Inspect `plugin.json` for typos in the `entry` or `callable` fields.
* If the plugin depends on external packages, install them into your environment and restart the sorter.

## Still stuck?

* File an issue with logs, screenshots, and the steps you took. Mention the app version from the `VERSION` file and your operating system.
* Temporarily remove third-party plugins to determine whether the core sorter works without them.
