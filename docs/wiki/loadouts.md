# Working with loadouts

Loadouts let you maintain multiple mod configurations without juggling separate folder trees. Each loadout stores the enabled/disabled state of every scanned file so you can switch play styles with a couple of clicks.

## Creating a loadout

1. Run a scan so the grid reflects the files in your Mods directory.
2. Use the ✔ toggle column to mark which files should be active.
3. Open the inspector pane and locate the **Loadouts** section.
4. Click **New Loadout**. Provide a descriptive name such as "Challenge Run" or "Family Friendly".
5. The loadout saves immediately and becomes the active profile.

## Switching between loadouts

* Select a different profile from the toolbar’s loadout dropdown.
* The grid updates instantly, enabling and disabling files to match the saved profile.
* Use the Command Center to apply recent loadouts as part of your startup routine.

## Updating a loadout

1. With the desired loadout active, change the ✔ toggles in the grid.
2. In the inspector, choose **Save Loadout** (or the equivalent update action) to capture the new state.

## Renaming or deleting

* Highlight the loadout in the inspector list and click **Rename** to provide a new label.
* Choose **Delete** to remove it permanently. The associated profile is removed from the `.sims4_modsorter_loadouts.json` file in your Mods directory.

## Storage format

* Loadouts live next to your Mods directory in `.sims4_modsorter_loadouts.json`.
* The file persists every profile and its selection state so switching computers or sharing the file restores your setups.

## Tips

* Create a "Baseline" loadout that represents your default everyday mods. Update it whenever you settle on a new normal configuration.
* Use loadouts to create seasonal or challenge-specific setups without rescanning the entire library.
* Exported plans include the active loadout and related metadata so you can share curated configurations with friends.
