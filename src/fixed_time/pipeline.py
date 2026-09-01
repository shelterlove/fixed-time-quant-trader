from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Any

import polars as pl

from .config import StrategyConfig, Window
from .download import download_funding, download_hourly, download_minutes
from .execution import SHADOW_HISTORY_COLUMNS, execute_long_with_funding_diagnostics, execute_short, funding_requirements, minute_requirements
from .features import build_features
from .metrics import report_markdown, summarize
from .portfolio import replay_long_standalone, replay_portfolio, replay_short_standalone
from .signals import enforce_research_subwindow_exit_boundary, long_signals, short_signals
from .storage import atomic_write_frame, atomic_write_json, atomic_write_text, empty_funding_frame, load_funding, load_hourly, load_minutes


@dataclass(frozen=True)
class Prepared:
    hourly: pl.DataFrame
    features: pl.DataFrame
    long_signals: pl.DataFrame
    short_signals: pl.DataFrame
    shadow_long_signals: pl.DataFrame
    prior_shadow_history: pl.DataFrame


_CACHE_SCHEMA_VERSION = 1
_SIGNAL_CACHE_REQUIRED_COLUMNS = {
    "trade_id", "strategy", "symbol", "decision_time", "source_bar_time",
    "entry_time", "planned_exit_time", "requested_units", "priority_score",
    "priority_order", "signal_scope",
}


def _cache_meta(config: StrategyConfig, window: Window, frame: pl.DataFrame) -> dict[str, Any]:
    decision_time = frame.get_column("decision_time") if "decision_time" in frame.columns and not frame.is_empty() else None
    source_time = frame.get_column("source_bar_time") if "source_bar_time" in frame.columns and not frame.is_empty() else None
    history_start = window.protection_history_start or window.start
    source_start = history_start - timedelta(hours=config.values["features"]["hourly_warmup_hours"])
    return {"schema_version": _CACHE_SCHEMA_VERSION, "strategy_version": config.version, "window_id": window.id,
            "download_manifest": "data/manifests/downloads.jsonl", "source_start": source_start.isoformat(),
            "source_end_exclusive": window.end_exclusive.isoformat(), "rows": frame.height,
            "symbols": frame.get_column("symbol").n_unique() if "symbol" in frame.columns else 0,
            "first_time": decision_time.min().isoformat() if decision_time is not None else None,
            "last_time": decision_time.max().isoformat() if decision_time is not None else None,
            "latest_source_time": source_time.max().isoformat() if source_time is not None else None,
            "parameters": config.values}


def _read_cache_meta(config: StrategyConfig, window: Window, name: str) -> dict[str, Any]:
    path = config.root / "data" / "cache" / window.id / f"{name}.meta.json"
    if not path.exists():
        raise ValueError(f"missing cache metadata: {path}")
    meta = json.loads(path.read_text(encoding="utf-8"))
    history_start = window.protection_history_start or window.start
    expected_start = history_start - timedelta(hours=config.values["features"]["hourly_warmup_hours"])
    expected = {
        "schema_version": _CACHE_SCHEMA_VERSION,
        "strategy_version": config.version,
        "window_id": window.id,
        "source_start": expected_start.isoformat(),
        "source_end_exclusive": window.end_exclusive.isoformat(),
        "parameters": config.values,
    }
    mismatched = [key for key, value in expected.items() if meta.get(key) != value]
    if mismatched:
        raise ValueError(f"stale {name} cache metadata: {', '.join(mismatched)}")
    return meta


def _validate_signal_cache(frame: pl.DataFrame, meta: dict[str, Any]) -> None:
    missing = _SIGNAL_CACHE_REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"signals cache is missing columns: {sorted(missing)}")
    observed = {
        "rows": frame.height,
        "symbols": frame.get_column("symbol").n_unique(),
        "first_time": frame.get_column("decision_time").min().isoformat() if not frame.is_empty() else None,
        "last_time": frame.get_column("decision_time").max().isoformat() if not frame.is_empty() else None,
        "latest_source_time": frame.get_column("source_bar_time").max().isoformat() if not frame.is_empty() else None,
    }
    mismatched = [name for name, value in observed.items() if meta.get(name) != value]
    if mismatched:
        raise ValueError(f"signals cache content does not match metadata: {', '.join(mismatched)}")
    if frame.get_column("trade_id").n_unique() != frame.height:
        raise ValueError("signals cache has duplicate trade_id values")
    scopes = set(frame.get_column("signal_scope").unique().to_list())
    if not scopes <= {"window", "protection_history"}:
        raise ValueError(f"signals cache has invalid signal_scope values: {scopes}")
    strategies = set(frame.get_column("strategy").unique().to_list())
    if not strategies <= {"long", "short"}:
        raise ValueError(f"signals cache has invalid strategy values: {strategies}")
    if frame.filter((pl.col("signal_scope") == "protection_history") & (pl.col("strategy") != "long")).height:
        raise ValueError("signals cache protection_history rows must be long")


def _write_cache(config: StrategyConfig, window: Window, name: str, frame: pl.DataFrame) -> None:
    directory = config.root / "data" / "cache" / window.id
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{name}.parquet"
    atomic_write_frame(target, frame)
    atomic_write_json(directory / f"{name}.meta.json", _cache_meta(config, window, frame), indent=2)


def _research_shadow_history(config: StrategyConfig, window: Window) -> pl.DataFrame:
    """Reuse completed research base shadows when a later authorized window begins."""
    research = config.window("research")
    if window.start < research.end_exclusive:
        return pl.DataFrame(schema={name: pl.Null for name in SHADOW_HISTORY_COLUMNS})
    output = config.root / "results" / "local" / research.id
    path, manifest_path = output / "long_trades.parquet", output / "run_manifest.json"
    if not path.exists() or not manifest_path.exists():
        raise ValueError(f"{window.id} requires frozen research shadow history: {output}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("strategy_version") != config.version or manifest.get("window") != research.id or manifest.get("parameters") != config.values:
        raise ValueError("research shadow history does not match the frozen strategy")
    history = pl.read_parquet(path, columns=SHADOW_HISTORY_COLUMNS)
    if history.height != manifest.get("long_trade_rows"):
        raise ValueError("research shadow history row count does not match its run manifest")
    return history


def _history_long_signals(features: pl.DataFrame, window: Window, config: StrategyConfig, current: pl.DataFrame) -> pl.DataFrame:
    if window.protection_history_start is None:
        return current.head(0)
    # The history selector has the final window end so a late-2020 decision
    # whose base shadow completes in early 2021 remains available to P90.
    return long_signals(features, window.protection_history_start, window.end_exclusive, config).filter(
        pl.col("decision_time") < pl.lit(window.start)
    )


def prepare(config: StrategyConfig, window: Window) -> Prepared:
    feature_start = window.protection_history_start or window.start
    hourly = load_hourly(config.root, feature_start, window.end_exclusive, config.values["features"]["hourly_warmup_hours"])
    features = build_features(hourly, config)
    long = enforce_research_subwindow_exit_boundary(long_signals(features, window.start, window.end_exclusive, config), window)
    short = short_signals(features, window.start, window.end_exclusive, config)
    shadow_long = _history_long_signals(features, window, config, long)
    prior_shadow = _research_shadow_history(config, window)
    _write_cache(config, window, "hourly_features", features)
    cached_signals = pl.concat([
        long.with_columns(pl.lit("window").alias("signal_scope")),
        short.with_columns(pl.lit("window").alias("signal_scope")),
        shadow_long.with_columns(pl.lit("protection_history").alias("signal_scope")),
    ], how="diagonal_relaxed")
    _write_cache(config, window, "signals", cached_signals)
    return Prepared(hourly, features, long, short, shadow_long, prior_shadow)


def _complete_from_signals(
    config: StrategyConfig,
    window: Window,
    hourly: pl.DataFrame | None,
    feature_rows: int,
    long_signals_frame: pl.DataFrame,
    short_signals_frame: pl.DataFrame,
    shadow_long_signals: pl.DataFrame,
    prior_shadow_history: pl.DataFrame,
    offline: bool,
) -> dict[str, Any]:
    """Finish an already-frozen signal set without rebuilding its features."""
    long_execution_signals = long_signals_frame if shadow_long_signals.is_empty() else pl.concat(
        [shadow_long_signals, long_signals_frame], how="vertical_relaxed"
    )
    minute_days = minute_requirements(long_execution_signals)
    # Pre-window shadows seed only base-path protection history. Funding never
    # contributes to a base shadow and is therefore not downloaded for them.
    funding_months = funding_requirements(long_signals_frame)
    if not offline:
        # Signals are now frozen in memory; only then is loading future path data allowed.
        download_minutes(config, minute_days)
        download_funding(config, funding_months)
    minutes = load_minutes(config.root, minute_days) if minute_days else pl.DataFrame(schema={})
    funding = load_funding(config.root, funding_months) if funding_months else empty_funding_frame()
    if hourly is None:
        hourly = load_hourly(config.root, window.start, window.end_exclusive, config.values["features"]["hourly_warmup_hours"])
    long_trades, funding_event_detail = execute_long_with_funding_diagnostics(
        long_execution_signals, minutes, funding, config, prior_shadow_history,
        window.start if not shadow_long_signals.is_empty() else None,
    )
    short_trades = execute_short(short_signals_frame, hourly, config)
    long_standalone = replay_long_standalone(long_trades, config)
    short_standalone = replay_short_standalone(short_trades, config)
    portfolio_trades, account, counts, allocation_audit = replay_portfolio(long_trades, short_trades, hourly, config)
    summary, monthly = summarize(portfolio_trades, account, counts, {"long": long_signals_frame.height, "short": short_signals_frame.height})
    output = config.root / "results" / "local" / window.id
    output.mkdir(parents=True, exist_ok=True)
    for name, frame in (
        ("long_trades.parquet", long_trades),
        ("funding_event_detail.parquet", funding_event_detail),
        ("short_trades.parquet", short_trades),
        ("long_standalone.parquet", long_standalone),
        ("short_standalone.parquet", short_standalone),
        ("portfolio_trades.parquet", portfolio_trades),
        ("combo_allocation_audit.csv", allocation_audit),
        ("account_ledger.parquet", account),
        ("summary.csv", summary),
        ("monthly.csv", monthly),
    ):
        atomic_write_frame(output / name, frame)
    atomic_write_text(output / "REPORT.md", report_markdown(summary, portfolio_trades))
    manifest = {"strategy_version": config.version, "window": window.id, "parameters": config.values,
                "hourly_rows": hourly.height, "feature_rows": feature_rows,
                "long_signal_rows": long_signals_frame.height, "short_signal_rows": short_signals_frame.height,
                "shadow_long_signal_rows": shadow_long_signals.height,
                "prior_shadow_history_rows": prior_shadow_history.height,
                "long_trade_rows": long_trades.height, "short_trade_rows": short_trades.height,
                "portfolio_trade_rows": portfolio_trades.height,
                "cache_files": [f"data/cache/{window.id}/hourly_features.parquet", f"data/cache/{window.id}/signals.parquet"]}
    atomic_write_json(output / "run_manifest.json", manifest, indent=2)
    return summary.to_dicts()[0]


def run(config: StrategyConfig, window: Window, offline: bool) -> dict[str, Any]:
    prepared = prepare(config, window)
    return _complete_from_signals(
        config, window, prepared.hourly, prepared.features.height,
        prepared.long_signals, prepared.short_signals, prepared.shadow_long_signals, prepared.prior_shadow_history, offline,
    )


def resume(config: StrategyConfig, window: Window, offline: bool) -> dict[str, Any]:
    """Recover after an execution-data failure without rebuilding frozen signals."""
    directory = config.root / "data" / "cache" / window.id
    signal_path = directory / "signals.parquet"
    if not signal_path.exists():
        raise ValueError("resume requires frozen signals cache")
    feature_meta = _read_cache_meta(config, window, "hourly_features")
    signal_meta = _read_cache_meta(config, window, "signals")
    signals = pl.read_parquet(signal_path)
    _validate_signal_cache(signals, signal_meta)
    current = signals.filter(pl.col("signal_scope") == "window")
    shadow_long = signals.filter(pl.col("signal_scope") == "protection_history")
    return _complete_from_signals(
        config, window, None, int(feature_meta["rows"]),
        current.filter(pl.col("strategy") == "long"), current.filter(pl.col("strategy") == "short"),
        shadow_long, _research_shadow_history(config, window), offline,
    )


def bootstrap(config: StrategyConfig, window: Window, refresh: bool) -> dict[str, Any]:
    download_hourly(config, window, refresh)
    return run(config, window, offline=False)
