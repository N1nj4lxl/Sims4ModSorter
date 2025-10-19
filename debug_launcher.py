"""Explain why Sims4 Mod Sorter or the Plugin Manager failed to open."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, List, Optional

from launch_utils import LAUNCH_LOG_PATH

BASE_DIR = Path(__file__).resolve().parent
ERROR_LOGS = {
    "mod_sorter": BASE_DIR / "Sims4ModSorter_error.log",
    "plugin_manager": BASE_DIR / "plugin_manager_error.log",
}


def load_events(component: Optional[str] = None) -> List[dict]:
    if not LAUNCH_LOG_PATH.exists():
        return []
    events: List[dict] = []
    for line in LAUNCH_LOG_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if component and entry.get("component") != component:
            continue
        events.append(entry)
    return events


def find_last_event(events: Iterable[dict], names: Iterable[str]) -> Optional[dict]:
    lookup = set(names)
    for entry in reversed(list(events)):
        if entry.get("event") in lookup:
            return entry
    return None


def summarize_mod_sorter(show_logs: bool) -> None:
    print("== Mod Sorter ==")
    events = load_events("mod_sorter")
    if not events:
        print("No launch events recorded yet. Launch Sims4ModSorter.py once to collect diagnostics.\n")
        return

    failure = find_last_event(events, ["tk-init-failed", "startup-exception"])
    success = find_last_event(events, ["startup-success"])
    if failure:
        details = failure.get("details", {})
        reason = details.get("error") or details.get("log") or "Unknown"
        print(f"Last startup failed: {failure['event']} -> {reason}")
    elif success:
        print(f"Last startup succeeded with version {success.get('details', {}).get('version', 'unknown')}.")
    else:
        print("No startup success or failure recorded.")

    if show_logs:
        log_path = ERROR_LOGS["mod_sorter"]
        if log_path.exists():
            print("-- Error log excerpt --")
            print(tail_file(log_path))
        else:
            print("No Sims4ModSorter_error.log present.")
    print()


def summarize_plugin_manager(show_logs: bool) -> None:
    print("== Plugin Manager ==")
    events = load_events("plugin_manager")
    if not events:
        print("No plugin manager diagnostics logged yet. Run plugin_manager.py at least once.\n")
        return

    fallback = find_last_event(events, ["fallback-gui", "gui-start-failed", "gui-import-failed"])
    if fallback:
        details = fallback.get("details", {})
        reason = details.get("reason") or details.get("error") or "Unknown"
        print(f"Last fallback triggered: {fallback['event']} -> {reason}")
    else:
        print("No GUI fallback recorded.")

    exception = find_last_event(events, ["unhandled-exception"])
    if exception:
        print(f"Unhandled exception noted: {exception.get('details', {}).get('error', 'Unknown error')}")

    if show_logs:
        log_path = ERROR_LOGS["plugin_manager"]
        if log_path.exists():
            print("-- Plugin manager error log excerpt --")
            print(tail_file(log_path))
        else:
            print("No plugin_manager_error.log present.")
    print()


def tail_file(path: Path, max_lines: int = 40) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception as exc:
        return f"Unable to read log: {exc}"
    if len(lines) <= max_lines:
        return "\n".join(lines)
    return "\n".join(lines[-max_lines:])


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect Mod Sorter and Plugin Manager fallback reasons.")
    parser.add_argument(
        "--component",
        choices=["mod_sorter", "plugin_manager", "all"],
        default="all",
        help="Limit output to a single component.",
    )
    parser.add_argument("--logs", action="store_true", help="Show trailing error log excerpts if available.")
    args = parser.parse_args()

    if args.component in ("mod_sorter", "all"):
        summarize_mod_sorter(args.logs)
    if args.component in ("plugin_manager", "all"):
        summarize_plugin_manager(args.logs)


if __name__ == "__main__":
    main()
