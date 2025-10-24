from unittest import TestCase, mock

from Sims4ModSorter import Sims4ModSorterApp


class CommandCenterPreferenceTests(TestCase):
    def _make_app(self) -> Sims4ModSorterApp:
        app = Sims4ModSorterApp.__new__(Sims4ModSorterApp)
        app.show_command_center_var = mock.Mock()
        app.command_center = mock.Mock()
        return app

    def test_maybe_show_displays_when_enabled(self) -> None:
        app = self._make_app()
        app.show_command_center_var.get.return_value = True  # type: ignore[attr-defined]

        Sims4ModSorterApp._maybe_show_command_center(app)

        app.command_center.show.assert_called_once_with()  # type: ignore[attr-defined]
        app.command_center.hide.assert_not_called()  # type: ignore[attr-defined]

    def test_maybe_show_hides_when_disabled(self) -> None:
        app = self._make_app()
        app.show_command_center_var.get.return_value = False  # type: ignore[attr-defined]

        Sims4ModSorterApp._maybe_show_command_center(app)

        app.command_center.hide.assert_called_once()  # type: ignore[attr-defined]
        app.command_center.show.assert_not_called()  # type: ignore[attr-defined]
