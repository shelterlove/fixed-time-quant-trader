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
    live_check = commands.add_parser("live-check", help="validate Binance Futures testnet account configuration")
    live_check.add_argument("--root", default=".")
    live_seed = commands.add_parser("live-seed", help="seed rolling P90 history from local frozen results")
    live_seed.add_argument("--root", default=".")
    live_run = commands.add_parser("live-run", help="run the REST-only automated Futures testnet engine")
    live_run.add_argument("--root", default=".")
    live_smoke = commands.add_parser("live-smoke", help="place and close one minimum-size Futures testnet position")
    live_smoke.add_argument("--root", default=".")
    live_smoke.add_argument("--symbol", required=True)
    live_health = commands.add_parser("live-health", help="check whether the live trader heartbeat is fresh")
    live_health.add_argument("--root", default=".")
    live_dashboard = commands.add_parser("live-dashboard", help="serve the read-only live monitoring page")
    live_dashboard.add_argument("--root", default=".")
    live_dashboard.add_argument("--host", default="0.0.0.0")
    live_dashboard.add_argument("--port", type=int, default=8080)
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
    if args.command == "live-dashboard":
        from .live.config import load_live_config
        from .live.dashboard import run_dashboard

        run_dashboard(load_live_config(args.root).database_path, args.host, args.port)
        return
    if args.command == "live-health":
        from datetime import UTC, datetime, timedelta
        from .live.config import load_live_config
        from .live.state import StateStore

        live_config = load_live_config(args.root)
        if not live_config.database_path.exists():
            raise ConfigError("live runtime database does not exist")
        store = StateStore(live_config.database_path)
        try:
            status = store.runtime_status()
        finally:
            store.close()
        if status is None or datetime.fromisoformat(status["heartbeat_at"]) < datetime.now(UTC) - timedelta(seconds=30):
            raise ConfigError("live trader heartbeat is stale")
        print({"status": "healthy", "heartbeat_at": status["heartbeat_at"]})
        return
    if args.command.startswith("live-"):
        from .live.config import load_live_config
        from .live.engine import LiveEngine
        from .live.state import RuntimeLock

        live_config = load_live_config(args.root)
        # These commands can write the durable trade state or submit orders.
        # A read-only account check deliberately remains available while the
        # trader is running.
        lock = RuntimeLock(live_config.database_path) if args.command in {"live-run", "live-seed", "live-smoke"} else None
        engine = None
        try:
            if lock is not None:
                lock.acquire()
            engine = LiveEngine(live_config)
            if args.command == "live-check":
                print(engine.check())
            elif args.command == "live-seed":
                print({"inserted_shadow_records": engine.seed_shadow_history()})
            elif args.command == "live-smoke":
                print(engine.smoke_test(args.symbol))
            else:
                engine.run_forever()
        finally:
            if engine is not None:
                engine.close()
            if lock is not None:
                lock.release()
        return
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
