from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Any

import polars as pl

from fixed_time.config import StrategyConfig, load_config
from fixed_time.metrics import summarize
from fixed_time.portfolio import replay_portfolio
from fixed_time.storage import (
    KLINE_COLUMNS,
    atomic_write_frame,
    atomic_write_json,
    load_funding,
    load_minutes,
)


class ExperimentError(ValueError):
    pass


@dataclass(frozen=True)
class ExperimentSettings:
    experiment_id: str
    source_window: str
    activation_lookback: timedelta
    maximum_extension: timedelta
    out_of_sample_fraction: float


@dataclass(frozen=True)
class ExtensionOutcome:
    exit_time: datetime
    exit_reference: float
    exit_reason: str
    activated_at: datetime
    maximum_retrace: float | None
    mae: float
    mfe: float


def load_settings(path: Path) -> ExperimentSettings:
    import tomllib

    with path.open("rb") as handle:
        values = tomllib.load(handle)
    if set(values) != {"experiment"} or set(values["experiment"]) != {
        "id", "source_window", "activation_lookback_hours", "maximum_extension_hours", "out_of_sample_fraction",
    }:
        raise ExperimentError("unexpected research configuration shape")
    raw = values["experiment"]
    lookback, extension, fraction = raw["activation_lookback_hours"], raw["maximum_extension_hours"], raw["out_of_sample_fraction"]
    if not isinstance(raw["id"], str) or not isinstance(raw["source_window"], str):
        raise ExperimentError("research id and source_window must be strings")
    if not isinstance(lookback, int) or not isinstance(extension, int) or lookback <= 0 or extension <= 0:
        raise ExperimentError("extension hours must be positive integers")
    if not isinstance(fraction, float) or not 0 < fraction < 1:
        raise ExperimentError("out_of_sample_fraction must be in (0, 1)")
    return ExperimentSettings(raw["id"], raw["source_window"], timedelta(hours=lookback), timedelta(hours=extension), fraction)


def _date_requirements(rows: list[dict[str, Any]], extension: timedelta) -> set[tuple[str, datetime]]:
    required: set[tuple[str, datetime]] = set()
    for row in rows:
        current = (row["entry_time"] - timedelta(minutes=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = row["planned_exit_time"] + extension
        while current < end:
            required.add((row["symbol"], current))
            current += timedelta(days=1)
    return required


def _funding_requirements(rows: list[dict[str, Any]], extension: timedelta) -> set[tuple[str, int, int]]:
    required: set[tuple[str, int, int]] = set()
    for row in rows:
        current = row["entry_time"].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = row["planned_exit_time"] + extension
        while current < end:
            required.add((row["symbol"], current.year, current.month))
            current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
    return required


def _short_hourly_paths(root: Path, short_trades: pl.DataFrame) -> list[Path]:
    """Load only paths that can be consulted for a short eviction.

    The portfolio replay reads hourly data exclusively to value open short
    positions when a later long displaces them. Loading the full historical
    universe would add no information to this experiment and needlessly use
    several times more memory.
    """
    paths: set[Path] = set()
    for row in short_trades.select("symbol", "entry_time", "planned_exit_time").to_dicts():
        current = (row["entry_time"] - timedelta(hours=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while current < row["planned_exit_time"]:
            path = root / "data" / "raw" / "klines_1h" / f"symbol={row['symbol']}" / f"year={current.year:04d}" / f"month={current.month:02d}" / "part.parquet"
            if not path.exists():
                raise ExperimentError(f"missing hourly partition needed for short replay: {path}")
            paths.add(path)
            current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
    return sorted(paths)


def _load_short_hourly_paths(root: Path, short_trades: pl.DataFrame) -> pl.DataFrame:
    paths = _short_hourly_paths(root, short_trades)
    if not paths:
        return pl.DataFrame(schema={name: pl.Null for name in KLINE_COLUMNS})
    return pl.concat([pl.read_parquet(path, columns=KLINE_COLUMNS) for path in paths], how="vertical").sort(["symbol", "open_time"])


def _continuous_bars(minutes_by_symbol: dict[str, pl.DataFrame], row: dict[str, Any], deadline: datetime) -> list[dict[str, Any]]:
    frame = minutes_by_symbol.get(row["symbol"])
    if frame is None:
        raise ExperimentError(f"missing minute data for {row['symbol']}")
    bars = frame.filter(
        (pl.col("open_time") >= row["entry_time"]) & (pl.col("open_time") < deadline)
    ).sort("open_time").to_dicts()
    expected = row["entry_time"]
    for bar in bars:
        if bar["open_time"] != expected:
            raise ExperimentError(f"incomplete minute path for {row['trade_id']} at {expected.isoformat()}")
        expected += timedelta(minutes=1)
    if expected != deadline:
        raise ExperimentError(f"incomplete minute path for {row['trade_id']} through {deadline.isoformat()}")
    return bars


def extend_recently_activated_trade(
    row: dict[str, Any], bars: list[dict[str, Any]], long_rules: dict[str, Any], lookback: timedelta, extension: timedelta,
) -> ExtensionOutcome | None:
    """Return an alternate outcome only for a normal planned exit meeting the hypothesis.

    This is deliberately research-local. It mirrors the frozen long bar ordering:
    hard stop, then existing protection, then peak update. The activation bar itself
    cannot use the trailing protection.
    """
    if row["exit_reason"] != "PLANNED_EXIT" or not bool(row["shadow_activated"]):
        return None
    planned, deadline = row["planned_exit_time"], row["planned_exit_time"] + extension
    expected_count = int((deadline - row["entry_time"]).total_seconds() // 60)
    if len(bars) != expected_count:
        raise ExperimentError(f"unexpected minute count for {row['trade_id']}")
    reference = float(row["entry_reference"])
    stop_price = reference * (1 + long_rules["hard_stop_return"])
    protection = long_rules["protection"]
    activation_price = reference * (1 + protection["activation_return"])
    allowed = row["protection_allowed_retrace"]
    if allowed is None:
        raise ExperimentError(f"missing frozen P90 threshold for {row['trade_id']}")
    active, peak, activated_at, maximum_retrace = False, reference, None, None
    lowest, highest, extending = reference, reference, False
    for bar in bars:
        low, high, opening = float(bar["low"]), float(bar["high"]), float(bar["open"])
        lowest, highest = min(lowest, low), max(highest, high)
        completed_at = bar["open_time"] + timedelta(minutes=1)
        if low <= stop_price:
            if not extending:
                return None
            return ExtensionOutcome(completed_at, min(opening, stop_price), "HARD_STOP", activated_at, maximum_retrace, lowest / reference - 1, highest / reference - 1)
        if active:
            retrace = max(0.0, (peak - low) / peak)
            maximum_retrace = retrace if maximum_retrace is None else max(maximum_retrace, retrace)
            trailing = peak * (1 - float(allowed))
            floor_price = reference * (1 + 2 * (long_rules["slippage_per_side"] + long_rules["taker_fee_per_side"]))
            effective_exit = max(trailing, floor_price)
            if low <= effective_exit:
                if not extending:
                    return None
                return ExtensionOutcome(completed_at, min(opening, effective_exit), "PROTECTION", activated_at, maximum_retrace, lowest / reference - 1, highest / reference - 1)
            peak = max(peak, high)
        elif high >= activation_price:
            active, peak, activated_at = True, max(peak, high), completed_at
        if completed_at == planned:
            if activated_at is None or not planned - lookback < activated_at <= planned:
                return None
            extending = True
    final = bars[-1]
    return ExtensionOutcome(deadline, float(final["close"]), "EXTENSION_CAP", activated_at, maximum_retrace, lowest / reference - 1, highest / reference - 1)


def _funding_return(
    row: dict[str, Any], exit_time: datetime, funding_by_symbol: dict[str, list[dict[str, Any]]], minute_open_by_symbol: dict[str, dict[datetime, float]], long_rules: dict[str, Any],
) -> float:
    entry_fill = float(row["entry_reference"]) * (1 + long_rules["slippage_per_side"])
    total = 0.0
    for event in funding_by_symbol.get(row["symbol"], []):
        if not row["entry_time"] < event["funding_time"] <= exit_time:
            continue
        settlement = event["funding_time"].replace(second=0, microsecond=0)
        price = minute_open_by_symbol[row["symbol"]].get(settlement)
        if price is None:
            raise ExperimentError(f"missing settlement minute for {row['trade_id']} at {settlement.isoformat()}")
        total += -float(event["funding_rate"]) * price / entry_fill
    return total


def build_variant_long_trades(config: StrategyConfig, baseline: pl.DataFrame, settings: ExperimentSettings) -> pl.DataFrame:
    candidates = baseline.filter((pl.col("exit_reason") == "PLANNED_EXIT") & pl.col("shadow_activated")).to_dicts()
    if not candidates:
        return baseline
    minutes = load_minutes(config.root, _date_requirements(candidates, settings.maximum_extension))
    funding = load_funding(config.root, _funding_requirements(candidates, settings.maximum_extension))
    minute_by_symbol = {
        key[0] if isinstance(key, tuple) else key: group.sort("open_time")
        for key, group in minutes.group_by("symbol", maintain_order=True)
    }
    minute_open_by_symbol = {
        symbol: {item["open_time"]: float(item["open"]) for item in frame.select("open_time", "open").to_dicts()}
        for symbol, frame in minute_by_symbol.items()
    }
    funding_by_symbol = {
        key[0] if isinstance(key, tuple) else key: group.to_dicts()
        for key, group in funding.group_by("symbol", maintain_order=True)
    }
    candidate_ids = {row["trade_id"] for row in candidates}
    rows: list[dict[str, Any]] = []
    for original in baseline.sort("entry_time").to_dicts():
        row = dict(original)
        row.update({
            "extension_applied": False,
            "protection_activated_at": None,
            "extension_deadline": None,
            "extension_minutes": 0,
            "baseline_exit_reason": original["exit_reason"],
        })
        if original["trade_id"] not in candidate_ids:
            rows.append(row)
            continue
        deadline = original["planned_exit_time"] + settings.maximum_extension
        outcome = extend_recently_activated_trade(
            original, _continuous_bars(minute_by_symbol, original, deadline), config.values["long"],
            settings.activation_lookback, settings.maximum_extension,
        )
        if outcome is None:
            rows.append(row)
            continue
        funding_return = _funding_return(original, outcome.exit_time, funding_by_symbol, minute_open_by_symbol, config.values["long"])
        entry_fill = float(original["entry_reference"]) * (1 + config.values["long"]["slippage_per_side"])
        exit_fill = outcome.exit_reference * (1 - config.values["long"]["slippage_per_side"])
        ratio = exit_fill / entry_fill
        row.update({
            "exit_time": outcome.exit_time,
            "exit_reference": outcome.exit_reference,
            "exit_reason": outcome.exit_reason,
            "gross_return": ratio - 1,
            "cost_return": -config.values["long"]["taker_fee_per_side"] * (1 + ratio),
            "funding_return": funding_return,
            "net_return": ratio - 1 - config.values["long"]["taker_fee_per_side"] * (1 + ratio) + funding_return,
            "pnl": ratio - 1 - config.values["long"]["taker_fee_per_side"] * (1 + ratio) + funding_return,
            "mae_return": outcome.mae,
            "mfe_return": outcome.mfe,
            "extension_applied": True,
            "protection_activated_at": outcome.activated_at,
            "extension_deadline": original["planned_exit_time"] + settings.maximum_extension,
            "extension_minutes": int((outcome.exit_time - original["planned_exit_time"]).total_seconds() // 60),
        })
        rows.append(row)
    return pl.DataFrame(rows).sort("entry_time")


def _audit_statuses(frame: pl.DataFrame) -> dict[tuple[Any, ...], str]:
    return {
        (row["strategy"], row["symbol"], row["signal_time"], row["entry_time"]): row["status"]
        for row in frame.select("strategy", "symbol", "signal_time", "entry_time", "status").to_dicts()
    }


def _out_of_sample(account: pl.DataFrame, cutoff: datetime) -> dict[str, float]:
    before = account.filter(pl.col("event_time") < cutoff).tail(1)
    after = account.filter(pl.col("event_time") >= cutoff)
    points = pl.concat([before, after], how="vertical") if not before.is_empty() else after
    if points.height < 2:
        raise ExperimentError("not enough account points for out-of-sample summary")
    equity = points.get_column("realized_equity")
    drawdown = float((equity / equity.cum_max() - 1).min() or 0.0)
    return {
        "start_equity": float(equity.head(1).item()),
        "final_equity": float(equity.tail(1).item()),
        "compound_return": float(equity.tail(1).item() / equity.head(1).item() - 1),
        "realized_max_drawdown": drawdown,
    }


def _portfolio_delta_concentration(baseline: pl.DataFrame, variant: pl.DataFrame) -> dict[str, float]:
    joined = baseline.select("trade_id", "pnl").rename({"pnl": "baseline_pnl"}).join(
        variant.select("trade_id", "pnl").rename({"pnl": "variant_pnl"}), on="trade_id", how="full", coalesce=True,
    ).with_columns(
        pl.col("baseline_pnl").fill_null(0.0), pl.col("variant_pnl").fill_null(0.0),
    ).with_columns((pl.col("variant_pnl") - pl.col("baseline_pnl")).alias("delta_pnl"))
    positive = joined.filter(pl.col("delta_pnl") > 0).get_column("delta_pnl")
    return {
        "total_pnl_delta": float(joined.get_column("delta_pnl").sum() or 0.0),
        "largest_positive_pnl_delta": float(positive.max() or 0.0),
        "largest_positive_pnl_delta_share": float(positive.max() / positive.sum()) if positive.len() and positive.sum() else 0.0,
    }


def run_experiment(root: Path, settings: ExperimentSettings, output: Path) -> dict[str, Any]:
    config = load_config(root)
    window = config.window(settings.source_window)
    source = config.root / "results" / "local" / settings.source_window
    long_trades = pl.read_parquet(source / "long_trades.parquet")
    short_trades = pl.read_parquet(source / "short_trades.parquet")
    manifest = json.loads((source / "run_manifest.json").read_text(encoding="utf-8"))
    if manifest.get("strategy_version") != config.version or manifest.get("parameters") != config.values:
        raise ExperimentError("source results do not match the frozen strategy configuration")
    variant_long = build_variant_long_trades(config, long_trades, settings)
    # The production portfolio interface intentionally receives its frozen
    # trade schema. Extension diagnostics remain in the research-only frame.
    variant_long_for_portfolio = variant_long.select(long_trades.columns)
    hourly = _load_short_hourly_paths(config.root, short_trades)
    candidate_counts = {"long": long_trades.height, "short": short_trades.height}
    baseline_portfolio, baseline_account, baseline_counts, baseline_audit = replay_portfolio(long_trades, short_trades, hourly, config)
    variant_portfolio, variant_account, variant_counts, variant_audit = replay_portfolio(variant_long_for_portfolio, short_trades, hourly, config)
    baseline_summary, _ = summarize(baseline_portfolio, baseline_account, baseline_counts, candidate_counts)
    variant_summary, _ = summarize(variant_portfolio, variant_account, variant_counts, candidate_counts)
    delta_concentration = _portfolio_delta_concentration(baseline_portfolio, variant_portfolio)
    baseline_status, variant_status = _audit_statuses(baseline_audit), _audit_statuses(variant_audit)
    newly_unselected = sum(
        1 for key, status in baseline_status.items() if status == "SELECTED" and variant_status.get(key) != "SELECTED"
    )
    cutoff = window.start + (window.end_exclusive - window.start) * (1 - settings.out_of_sample_fraction)
    extended = variant_long.filter(pl.col("extension_applied"))
    output.mkdir(parents=True, exist_ok=True)
    atomic_write_frame(output / "variant_long_trades.parquet", variant_long)
    atomic_write_frame(output / "variant_portfolio_trades.parquet", variant_portfolio)
    atomic_write_frame(output / "variant_account_ledger.parquet", variant_account)
    atomic_write_frame(output / "variant_allocation_audit.csv", variant_audit)
    atomic_write_frame(output / "extended_trades.csv", extended)
    comparison = pl.DataFrame([
        {"scenario": "baseline", **baseline_summary.to_dicts()[0], **_out_of_sample(baseline_account, cutoff)},
        {"scenario": "recent_protection_extension", **variant_summary.to_dicts()[0], **_out_of_sample(variant_account, cutoff)},
    ])
    atomic_write_frame(output / "comparison.csv", comparison)
    result = {
        "experiment_id": settings.experiment_id,
        "strategy_version": config.version,
        "source_window": settings.source_window,
        "source_manifest": str(source / "run_manifest.json"),
        "rule": {
            "activation_lookback_hours": settings.activation_lookback.total_seconds() / 3600,
            "maximum_extension_hours": settings.maximum_extension.total_seconds() / 3600,
            "long_only": True,
            "preemptive_exits": ["HARD_STOP", "PROTECTION"],
            "cap_exit_reason": "EXTENSION_CAP",
        },
        "out_of_sample": {"cutoff": cutoff.isoformat(), "fraction": settings.out_of_sample_fraction},
        "candidate_planned_and_activated": int(((long_trades.get_column("exit_reason") == "PLANNED_EXIT") & long_trades.get_column("shadow_activated")).sum()),
        "extended_trade_count": extended.height,
        "extended_exit_reasons": extended.group_by("exit_reason").len().sort("exit_reason").to_dicts() if not extended.is_empty() else [],
        "newly_unselected_vs_baseline": newly_unselected,
        "portfolio_pnl_delta": delta_concentration,
        "baseline": baseline_summary.to_dicts()[0],
        "variant": variant_summary.to_dicts()[0],
        "baseline_out_of_sample": _out_of_sample(baseline_account, cutoff),
        "variant_out_of_sample": _out_of_sample(variant_account, cutoff),
    }
    atomic_write_json(output / "summary.json", result, indent=2)
    return result
