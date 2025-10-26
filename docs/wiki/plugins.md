# Plugin development

Sims4ModSorter exposes a lightweight plugin API that lets you customise scans, extend the UI, and add automation. This article explains the plugin structure, lifecycle, and available hooks.

## Directory layout

Plugins live in `user_plugins/` next to `Sims4ModSorter.py`. Each plugin resides in its own folder. A minimal layout looks like:

```
user_plugins/
└── my_plugin/
    ├── plugin.json
    └── main.py
```

The sorter imports enabled plugins automatically on launch.

## Manifest (`plugin.json`)

The manifest describes how the sorter should load the plugin. Core fields include:

* `name` – Identifier shown in the UI.
* `entry` – Path to the module that exposes a registration function.
* `callable` – The function called with the `PluginAPI` instance.
* `enabled` – Whether the plugin should load on startup.

Use the `plugin_manager.py` helper to generate manifests automatically when importing plugins.

## Registration function

Your `callable` receives an instance of `PluginAPI` from `plugin_api.py`. Through it you can:

* Register pre-scan hooks to tweak scan settings (ignored extensions, recursion flags, etc.).
* Register post-scan hooks to inspect or mutate the plan before it appears in the grid.
* Add custom UI themes that show up in the settings overlay.
* Log messages to the console via `api.logger`.

Always check `api.reserved_extra_keys()` or `api.is_reserved_extra()` before writing to `item.extras` so you do not clobber built-in metadata like duplicate markers.

## Example

The repository ships with `user_plugins/example_theme_plugin` which demonstrates how to:

* Add an "Ocean Breeze" theme to the theme picker.
* Extend the ignored extensions list with `.bak` and `.tmp` files.
* Annotate `.package` files containing "preview" in their filename so they stand out in the grid.

Launch the sorter normally and watch the console output confirm that the example plugin loaded successfully.

## Debugging tips

* Run `python plugin_manager.py` to enable, disable, or remove plugins with a UI.
* Use the Command Center to jump straight to the Plugin Manager window.
* Keep logging statements lightweight—Tkinter handles console output on the main thread.
* Wrap plugin code that touches external libraries in try/except blocks so failures surface as warnings instead of crashes.

## Distribution checklist

1. Document dependencies in a `requirements.txt` file so users know what to install.
2. Provide clear instructions for importing the plugin folder or archive via the Plugin Manager.
3. Test against the `VERSION` file at the repository root to ensure compatibility.
4. Use semantic versioning in your manifest and changelog so users can track updates.
