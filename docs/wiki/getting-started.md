# Getting started

This guide walks you through preparing your system, launching Sims4ModSorter, and performing your first sort.

## Prerequisites

* **Python 3.10+** – The app is developed against Python 3.10 and later. On Windows and macOS, the standard installer ships with Tkinter which powers the UI.
* **Sims 4 Mods directory** – Know where your `Mods/` folder lives (typically `Documents/Electronic Arts/The Sims 4/Mods`).

## Installation

1. Download or clone the repository:
   ```bash
   git clone https://github.com/your-account/Sims4ModSorter.git
   cd Sims4ModSorter
   ```
2. Optional but recommended: create a virtual environment so the sorter runs isolated from other Python projects.
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows use `.venv\\Scripts\\activate`
   ```
3. Install extra packages if your plugins require them. The base app runs with the Python standard library.

## Launching the sorter

Run the main entry point:
```bash
python Sims4ModSorter.py
```
When the window opens, pick your Mods directory from the folder chooser or choose a recent location from the Command Center.

## Running a scan

1. Click **Scan Mods** (or the equivalent shortcut in the Command Center).
2. Wait for the progress indicator to finish. Large libraries may take a few minutes.
3. Review the results grid. Each row represents a file with:
   * Its suggested category and destination folder.
   * A toggle that determines whether it will move when you finalise the plan.
   * Duplicate markers if the same fingerprint appears elsewhere in the scan results.
4. Adjust categories or destinations as needed using the inspector pane on the right.

## Completing the sort

* Use **Complete Sorting** to move the checked files into their category folders.
* A move log is recorded in `.sims4_modsorter_moves.json`. Use **Undo Last** if you need to revert the most recent batch.

## Next steps

* Explore the [User interface tour](ui-tour.md) to understand every panel and control.
* Learn how to [manage loadouts](loadouts.md) for different play styles.
* Discover how to [extend the sorter with plugins](plugins.md).
