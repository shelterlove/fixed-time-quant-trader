from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from .config import ConfigError, StrategyConfig, load_config
from .pipeline import bootstrap, resume, run


_WINDOW_COMMANDS = {
    "bootstrap": "research",
    "run": "research",
    "validate": "external_2021",
    "forward": "forward_2026_jul_aug",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fixed-time")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("bootstrap", "run", "resume", "validate"):
        command = commands.add_parser(name)
        command.add_argument("--window", required=True)
        if name == "bootstrap":
            command.add_argument("--refresh", action="store_true")
        if name in ("run", "resume"):
            command.add_argument("--offline", action="store_true", required=True)
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--legacy-root", required=True)
    forward = commands.add_parser("forward")
    forward.add_argument("--window", required=True)
    forward.add_argument("--confirm", action="store_true", required=True)
    return parser


def _reconcile(root: Path, legacy_root: Path) -> None:
    """Deliberately isolated legacy access; run/validate never call this path."""
    current = root / "results" / "local" / "research" / "summary.csv"
    legacy = legacy_root / "results" / "summary.csv"
    if not current.exists() or not legacy.exists():
        raise ConfigError(f"reconcile needs {current} and {legacy}")
    with current.open(encoding="utf-8", newline="") as left, legacy.open(encoding="utf-8", newline="") as right:
        current_row, legacy_row = next(csv.DictReader(left)), next(csv.DictReader(right))
    print({"current": current_row, "legacy": legacy_row})


def _require_research_baseline(config: StrategyConfig) -> None:
    path = config.root / "results" / "local" / "research" / "run_manifest.json"
    if not path.exists():
        raise ConfigError("authorized external windows require a completed research run manifest")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("strategy_version") != config.version or manifest.get("window") != "research" or manifest.get("parameters") != config.values:
        raise ConfigError("research baseline does not match the frozen strategy")


def _authorized_window(config: StrategyConfig, command: str, window_id: str):
    window = config.window(window_id)
    expected = _WINDOW_COMMANDS.get(command)
    if expected is not None and window.id != expected:
        raise ConfigError(f"{command} only accepts the {expected} window")
    if command in ("validate", "forward"):
        _require_research_baseline(config)
    if command == "resume" and window.id != "research":
        _require_research_baseline(config)
    return window


def main() -> None:
    args = _parser().parse_args()
    config = load_config()
    if args.command == "reconcile":
        _reconcile(config.root, Path(args.legacy_root))
        return
    window = _authorized_window(config, args.command, args.window)
    if args.command == "validate":
        result = bootstrap(config, window, refresh=False)
    elif args.command == "forward":
        result = bootstrap(config, window, refresh=False)
    elif args.command == "bootstrap":
        result = bootstrap(config, window, args.refresh)
    elif args.command == "run":
        result = run(config, window, offline=True)
    else:
        result = resume(config, window, offline=True)
    print(f"strategy={config.version} window={args.window} result={result}")


if __name__ == "__main__":
    main()
