# Release notes and versioning

Sims4ModSorter follows semantic versioning. This page outlines how to interpret the `VERSION` file and track changes between releases.

## Version format

The `VERSION` file in the repository root contains a string such as `1.4.0`. The segments mean:

* **Major** – Significant feature releases or breaking changes to the plugin API.
* **Minor** – Backwards-compatible improvements such as new UI tools or expanded sorting logic.
* **Patch** – Bug fixes or small tweaks.

## Where to find release notes

* **Git tags** – Releases are tagged in the repository with the version number.
* **CHANGELOG** – If present, describes the enhancements, fixes, and migration notes for each release. (If missing, use commit history or pull requests as references.)
* **Wiki updates** – This page tracks highlights from recent versions.

## Recent highlights

### 1.0.0 – Initial public release

* Introduced the Tkinter grid interface with sorting, filtering, and undo support.
* Added the Command Center dashboard for quick actions.
* Shipped with the plugin system and an example theme plugin.

### 1.1.0 – Loadouts and duplicate detection

* Added loadout management to save per-profile file selections.
* Embedded fingerprints in scan results to detect duplicates.
* Enhanced export/import plans to include duplicate and loadout metadata.

### 1.2.0 – Settings overhaul

* Replaced the modal settings dialog with an inline overlay.
* Added theme previews and recursive scan controls.
* Improved ignored extension handling.

## Updating the app

1. Pull the latest changes from the repository:
   ```bash
   git pull origin main
   ```
2. Review the release notes to understand what changed.
3. Test your plugins against the new version, paying special attention to API updates.

## Reporting regressions

* Open an issue with detailed reproduction steps, logs, and the version you updated from.
* Include screenshots of the UI if visual regressions occur.
* Mention any third-party plugins you had enabled during the regression.
