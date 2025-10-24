from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pytest

from user_plugins.column_layout_manager import plugin as column_plugin


class DummyAPI:
    def __init__(self, app) -> None:
        self._app = app
        self.toolbar: List[Dict[str, object]] = []
        self.sections: List[Callable] = []
        self.messages: List[tuple[str, str]] = []

    @property
    def app(self):
        return self._app

    def register_toolbar_button(self, button_id: str, **kwargs) -> None:
        self.toolbar.append({"id": button_id, **kwargs})

    def register_settings_section(self, title: str, builder: Callable) -> None:
        self.sections.append(builder)

    def log(self, message: str, level: str = "info") -> None:
        self.messages.append((level, message))


class FakeTree:
    def __init__(self, columns: List[str]) -> None:
        self._columns = list(columns)
        self._display = list(columns)
        self._headings: Dict[str, str] = {column: column.title() for column in columns}
        self._meta: Dict[str, Dict[str, object]] = {
            column: {"width": 80 + index * 10, "anchor": "w"}
            for index, column in enumerate(columns)
        }

    def __getitem__(self, key: str):
        if key == "columns":
            return tuple(self._columns)
        if key == "displaycolumns":
            return tuple(self._display)
        raise KeyError(key)

    def configure(self, **kwargs) -> None:
        if "columns" in kwargs:
            self._columns = list(kwargs["columns"])
        if "displaycolumns" in kwargs:
            self._display = list(kwargs["displaycolumns"])

    def heading(self, column: str, **kwargs):
        if kwargs:
            text = kwargs.get("text")
            if isinstance(text, str):
                self._headings[column] = text
        return {"text": self._headings.get(column, column)}

    def column(self, column: str, **kwargs):
        info = self._meta.setdefault(column, {"width": 80, "anchor": "w"})
        for key, value in kwargs.items():
            if key in {"width", "anchor"}:
                info[key] = value
        return dict(info)

    def winfo_exists(self) -> bool:  # pragma: no cover - mirrors Tk API
        return True


class FakeApp:
    def __init__(self, columns: List[str]) -> None:
        self.tree = FakeTree(columns)
        self._column_order = list(columns)
        self._column_anchors = {column: "w" for column in columns}

    def after(self, _delay: int, callback: Callable) -> None:
        callback()


@pytest.fixture
def temp_config(tmp_path: Path) -> Path:
    return tmp_path / "layout.json"


def test_plugin_registers_and_initialises(temp_config: Path) -> None:
    app = FakeApp(["inc", "name", "size", "type"])
    api = DummyAPI(app)
    plugin = column_plugin.ColumnLayoutManagerPlugin(api)
    plugin._config_path = temp_config
    plugin._state = plugin._load_state()

    plugin.register()
    if plugin._watcher:
        plugin._watcher.join(timeout=1)

    assert api.toolbar, "toolbar button should be registered"
    assert api.sections, "settings section should be registered"
    assert list(app.tree["columns"]) == ["inc", "name", "size", "type"]
    assert app._column_order == ["inc", "name", "size", "type"]


def test_layout_persistence_round_trip(temp_config: Path) -> None:
    initial_columns = ["inc", "name", "size", "type"]
    app = FakeApp(initial_columns)
    api = DummyAPI(app)
    plugin = column_plugin.ColumnLayoutManagerPlugin(api)
    plugin._config_path = temp_config
    plugin._state = plugin._load_state()
    plugin.register()
    if plugin._watcher:
        plugin._watcher.join(timeout=1)

    new_order = ["name", "inc", "type", "size"]
    visible = ["name", "size"]
    widths = {"name": 120, "inc": 48, "type": 90, "size": 110}
    anchors = {"name": "w", "inc": "center", "type": "e", "size": "e"}
    plugin.update_layout(new_order, visible, widths, anchors, preset="Custom")

    stored = json.loads(temp_config.read_text(encoding="utf-8"))
    assert stored["order"][0] == "name"
    assert stored["display"] == visible
    assert stored["widths"]["name"] == 120
    assert stored["anchors"]["inc"] == "center"

    app2 = FakeApp(initial_columns)
    api2 = DummyAPI(app2)
    plugin2 = column_plugin.ColumnLayoutManagerPlugin(api2)
    plugin2._config_path = temp_config
    plugin2._state = plugin2._load_state()
    plugin2.register()
    if plugin2._watcher:
        plugin2._watcher.join(timeout=1)

    assert list(app2.tree["columns"]) == new_order
    assert tuple(app2.tree["displaycolumns"]) == tuple(visible)
    assert app2.tree.column("name")["width"] == 120
    assert app2.tree.column("inc")["anchor"] == "center"
    assert app2._column_order == new_order
    assert app2._column_anchors["inc"] == "center"
