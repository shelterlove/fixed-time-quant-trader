"""Offline controlled replay; writes separate artifacts, never the frozen baseline.

python scripts/verify_repairs.py --baseline-ref c8d3d1d
Requires the repository's already downloaded public data and feature caches.
"""
from pathlib import Path
from datetime import timedelta
import argparse
import gc
import importlib.util
import json
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import polars as pl
from fixed_time import __version__
from fixed_time.config import load_config
from fixed_time.execution import (execute_long, execute_short, extend_long_trades, extension_requirements,
                                  funding_requirements, SHADOW_HISTORY_COLUMNS)
from fixed_time.pipeline import _history_long_signals, _minute_requirements_for_signals
from fixed_time.portfolio import replay_portfolio
from fixed_time.metrics import summarize
from fixed_time.signals import long_signals, short_signals, enforce_research_subwindow_exit_boundary
from fixed_time.storage import load_minutes, load_funding, KLINE_COLUMNS
from fixed_time.download import download_minutes, download_funding


def short_hourly(root, signals):
    paths = set()
    for row in signals.to_dicts():
        month = (row["entry_time"] - timedelta(hours=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while month < row["planned_exit_time"]:
            paths.add(root / "data/raw/klines_1h" / f"symbol={row['symbol']}" / f"year={month.year:04}" / f"month={month.month:02}" / "part.parquet")
            month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
    return pl.concat([pl.read_parquet(p, columns=KLINE_COLUMNS) for p in sorted(paths)]) if paths else pl.DataFrame(schema={c: pl.Null for c in KLINE_COLUMNS})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-ref", required=True)
    parser.add_argument("--window", choices=["research", "external_2021", "forward_2026_jul_aug"], action="append")
    parser.add_argument("--download-missing", action="store_true")
    args = parser.parse_args()
    config = load_config(ROOT)
    output = ROOT / "results/repairs" / __version__
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "before_portfolio.py"
        path.write_bytes(subprocess.run(["git", "show", f"{args.baseline_ref}:src/fixed_time/portfolio.py"], cwd=ROOT, check=True, capture_output=True).stdout)
        spec = importlib.util.spec_from_file_location("fixed_time._before_portfolio", path)
        before = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(before)
    reports = []
    for name in args.window or ["research", "external_2021", "forward_2026_jul_aug"]:
        print(f"{name}: signals", flush=True)
        window = config.window(name)
        destination = output / name
        destination.mkdir(exist_ok=True)
        features = pl.read_parquet(ROOT / "data/cache" / name / "hourly_features.parquet")
        longs = enforce_research_subwindow_exit_boundary(long_signals(features, window.start, window.end_exclusive, config), window)
        shorts = short_signals(features, window.start, window.end_exclusive, config)
        warmup = _history_long_signals(features, window, config, longs)
        original = pl.read_parquet(ROOT / "data/cache" / name / "signals.parquet").filter(pl.col("signal_scope") == "window")
        assert set(original["trade_id"]) == set(longs["trade_id"]) | set(shorts["trade_id"]), "candidate identities changed"
        del features, original
        signals = pl.concat([warmup, longs], how="vertical_relaxed") if not warmup.is_empty() else longs
        days = _minute_requirements_for_signals(signals, shorts)
        months = funding_requirements(longs)
        print(f"{name}: loading {len(days)} minute partitions", flush=True)
        if args.download_missing:
            download_minutes(config, days)
            download_funding(config, months)
        minutes = load_minutes(ROOT, days)
        funding = load_funding(ROOT, months)
        prior_path = output / "research/base_shadow_history.parquet"
        prior = pl.read_parquet(prior_path) if name.startswith("forward") else None
        base = execute_long(signals, minutes, funding, config, prior, window.start if not warmup.is_empty() else None)
        base.select(SHADOW_HISTORY_COLUMNS).write_parquet(destination / "base_shadow_history.parquet")
        extra_days, extra_months = extension_requirements(base, config, window)
        if args.download_missing:
            download_minutes(config, extra_days - days)
            download_funding(config, extra_months - months)
        if extra_days - days:
            minutes = pl.concat([minutes, load_minutes(ROOT, extra_days - days)])
        if extra_months - months:
            funding = pl.concat([funding, load_funding(ROOT, extra_months - months)])
        # Archives are partitioned by day/month; consumers only receive the authorized range.
        minutes = minutes.filter(pl.col("open_time") < window.end_exclusive)
        funding = funding.filter(pl.col("funding_time") <= window.end_exclusive)
        long_trades, _ = extend_long_trades(base, minutes, funding, config, window)
        hourly = short_hourly(ROOT, shorts)
        short_trades = execute_short(shorts, hourly, config)
        long_trades.write_parquet(destination / "long_trades.parquet")
        short_trades.write_parquet(destination / "short_trades.parquet")
        del base
        candidate_counts = {"long": longs.height, "short": shorts.height}
        print(f"{name}: old accounting, identical bounded paths and config", flush=True)
        old_trades, old_account, old_counts, _ = before.replay_portfolio(long_trades, short_trades, hourly, config, minutes, funding)
        old_summary, _ = summarize(old_trades, old_account, old_counts, candidate_counts)
        old_summary.write_csv(destination / "before_summary.csv")
        old_trades.write_parquet(destination / "before_portfolio.parquet")
        del old_trades, old_account
        gc.collect()
        print(f"{name}: corrected accounting", flush=True)
        trades, account, counts, audit = replay_portfolio(long_trades, short_trades, hourly, config, minutes, funding)
        summary, monthly = summarize(trades, account, counts, candidate_counts)
        for file, frame in (("portfolio_trades", trades), ("account_ledger", account), ("allocation_audit", audit)):
            frame.write_parquet(destination / f"{file}.parquet")
        summary.write_csv(destination / "summary.csv")
        monthly.write_csv(destination / "monthly.csv")
        assert account["event_time"].max() <= window.end_exclusive
        assert account["open_units"].max() <= config.values["portfolio"]["total_units"]
        report = {"window": name, "code_version": __version__, "baseline_ref": args.baseline_ref,
                  "comparison": "same configuration and bounded execution paths; accounting only",
                  "signals_unchanged": True, "boundary_exits": long_trades.filter(pl.col("exit_reason") == "WINDOW_END").height,
                  "before": old_summary.to_dicts()[0], "after": summary.to_dicts()[0]}
        (destination / "comparison.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        (destination / "parameters.json").write_text(json.dumps(config.values, indent=2), encoding="utf-8")
        reports.append(report)
        print(json.dumps(report, default=str), flush=True)
        del minutes, funding, hourly, long_trades, short_trades, trades, account, audit
        gc.collect()
    (output / "comparison.json").write_text(json.dumps(reports, indent=2, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
