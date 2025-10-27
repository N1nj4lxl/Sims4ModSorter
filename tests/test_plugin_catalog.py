import json


def test_catalog_bundle_defaults_to_folder(tmp_path):
    from Sims4ModSorter import Sims4ModSorterApp

    app = Sims4ModSorterApp.__new__(Sims4ModSorterApp)

    catalog_path = tmp_path / "plugin_marketplace.json"
    catalog_path.write_text(
        json.dumps(
            {
                "plugins": [
                    {
                        "id": "insight-logger",
                        "name": "Insight Logger",
                        "folder": "insight_logger",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    app._plugin_catalog_path = lambda: catalog_path
    app._load_plugin_catalog()

    assert app.plugin_catalog[0]["bundle"] == "insight_logger"
