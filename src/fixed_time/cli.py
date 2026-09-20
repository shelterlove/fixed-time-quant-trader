from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from .config import load_live_config, load_strategy
from .dashboard import serve
from .engine import Engine
from .exchange import Binance
from .state import RuntimeLock, Store


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="fixed-time")
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("strategy-check", "live-check", "live-deploy-check", "live-reconcile", "live-run", "live-health", "live-backup"):
        item = commands.add_parser(name)
        item.add_argument("--root", default=".")
    dashboard = commands.add_parser("live-dashboard")
    dashboard.add_argument("--root", default=".")
    dashboard.add_argument("--host", default="127.0.0.1")
    dashboard.add_argument("--port", type=int, default=8080)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    root = Path(args.root).resolve()
    if args.command == "strategy-check":
        strategy = load_strategy(root)
        print(json.dumps({"strategy_version": strategy.version, "status": "ok"}))
        return 0
    config = load_live_config(root)
    if args.command in {"live-check","live-deploy-check"}:
        result = Engine.check_exchange(Binance(config))
        if args.command == "live-deploy-check":
            if not config.trading_enabled:
                raise ValueError("deploy requires TRADING_ENABLED=true; use live-check for read-only checks")
            if not config.database_path.is_relative_to(root / "runtime"):
                raise ValueError("Docker deployment requires DATABASE_PATH inside /app/runtime")
            legacy = False
            if config.database_path.exists():
                with sqlite3.connect(config.database_path.as_uri()+"?mode=ro",uri=True) as db:
                    tables = {x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    migrated = "v2_meta" in tables and db.execute("SELECT 1 FROM v2_meta WHERE key='legacy_open_positions_imported'").fetchone()
                    legacy = "positions" in tables and not migrated
                    if legacy and db.execute("SELECT 1 FROM positions WHERE status='OPEN' LIMIT 1").fetchone():
                        raise ValueError("legacy upgrade requires flat local positions; finish trades with the previous version first")
            if legacy and any(result[k] for k in ("positions","open_orders","open_algos")):
                raise ValueError("legacy upgrade requires a flat exchange account with no pending orders")
            print("Deployment preflight passed")
        else:
            print(json.dumps(result,default=str))
        return 0
    if args.command == "live-backup":
        with RuntimeLock(config.database_path):
            if not config.database_path.exists():
                print("No existing database to back up")
                return 0
            target = config.database_path.parent / "backups" / (datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ") + ".sqlite3")
            target.parent.mkdir(parents=True,exist_ok=True)
            with sqlite3.connect(config.database_path.as_uri()+"?mode=ro",uri=True) as source, sqlite3.connect(target) as backup:
                source.backup(backup)
            print(f"Database backup: {target}")
        return 0
    if args.command == "live-health":
        from .dashboard import snapshot
        status = snapshot(config.database_path)
        print(json.dumps(status, default=str))
        return 0 if status["healthy"] else 1
    if args.command == "live-dashboard":
        serve(config.database_path, args.host, args.port)
        return 0
    with RuntimeLock(config.database_path):
        engine = Engine(config)
        try:
            if args.command == "live-reconcile":
                engine.check()
                print(json.dumps({"reconciled": engine.reconcile(), "incidents": engine.store.incidents()}, default=str))
            else:
                engine.run_forever()
        finally:
            engine.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
