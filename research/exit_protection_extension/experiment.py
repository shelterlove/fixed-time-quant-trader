from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Any

import polars as pl

from fixed_time.config import StrategyConfig, load_config
from fixed_time.metrics import summarize
from fixed_time.portfolio import _latest_completed_hourly_close, _short_excursions_until_exit, replay_portfolio
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
    long_eviction_after_extension: timedelta
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


@dataclass(frozen=True)
class ExtensionMarketData:
    minutes_by_symbol: dict[str, pl.DataFrame]
    minute_open_by_symbol: dict[str, dict[datetime, float]]
    funding_by_symbol: dict[str, list[dict[str, Any]]]


def load_settings(path: Path) -> ExperimentSettings:
    import tomllib

    with path.open("rb") as handle:
        values = tomllib.load(handle)
    if set(values) != {"experiment"} or set(values["experiment"]) != {
        "id", "source_window", "activation_lookback_hours", "maximum_extension_hours", "long_eviction_after_extension_hours", "out_of_sample_fraction",
    }:
        raise ExperimentError("unexpected research configuration shape")
    raw = values["experiment"]
    lookback, extension, eviction_after, fraction = raw["activation_lookback_hours"], raw["maximum_extension_hours"], raw["long_eviction_after_extension_hours"], raw["out_of_sample_fraction"]
    if not isinstance(raw["id"], str) or not isinstance(raw["source_window"], str):
        raise ExperimentError("research id and source_window must be strings")
    if not isinstance(lookback, int) or not isinstance(extension, int) or not isinstance(eviction_after, int) or lookback <= 0 or extension <= 0 or eviction_after <= 0 or eviction_after >= extension:
        raise ExperimentError("extension hours must be positive integers")
    if not isinstance(fraction, float) or not 0 < fraction < 1:
        raise ExperimentError("out_of_sample_fraction must be in (0, 1)")
    return ExperimentSettings(raw["id"], raw["source_window"], timedelta(hours=lookback), timedelta(hours=extension), timedelta(hours=eviction_after), fraction)


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


def build_variant_long_trades(config: StrategyConfig, baseline: pl.DataFrame, settings: ExperimentSettings) -> tuple[pl.DataFrame, ExtensionMarketData]:
    candidates = baseline.filter((pl.col("exit_reason") == "PLANNED_EXIT") & pl.col("shadow_activated")).to_dicts()
    if not candidates:
        return baseline, ExtensionMarketData({}, {}, {})
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
    market = ExtensionMarketData(minute_by_symbol, minute_open_by_symbol, funding_by_symbol)
    rows: list[dict[str, Any]] = []
    for original in baseline.sort("entry_time").to_dicts():
        row = dict(original)
        row.update({
            "extension_applied": False,
            "protection_activated_at": None,
            "extension_deadline": None,
            "extension_release_time": None,
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
        funding_return = _funding_return(original, outcome.exit_time, market.funding_by_symbol, market.minute_open_by_symbol, config.values["long"])
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
            "extension_release_time": original["planned_exit_time"] + settings.long_eviction_after_extension,
            "extension_minutes": int((outcome.exit_time - original["planned_exit_time"]).total_seconds() // 60),
        })
        rows.append(row)
    return pl.DataFrame(rows).sort("entry_time"), market


def _latest_completed_minute_close(minutes_by_symbol: dict[str, pl.DataFrame], symbol: str, event_time: datetime) -> float:
    frame = minutes_by_symbol.get(symbol)
    if frame is None:
        raise ExperimentError(f"no extension minute path for {symbol}")
    price = frame.filter(pl.col("open_time") <= event_time - timedelta(minutes=1)).tail(1).get_column("close")
    if price.len() != 1:
        raise ExperimentError(f"no completed minute close for {symbol} at {event_time.isoformat()}")
    return float(price.item())


def extension_long_eviction_order(positions: dict[tuple[str, str], dict[str, Any]], event_time: datetime) -> list[dict[str, Any]]:
    """Only post-four-hour extensions are secondary to a newly admitted long."""
    return sorted(
        (
            item for item in positions.values()
            if item["strategy"] == "long" and item.get("extension_applied")
            and item.get("extension_release_time") is not None and item["extension_release_time"] < event_time
        ),
        key=lambda item: (item["extension_release_time"], item["entry_time"], item["symbol"]),
    )


def replay_extension_portfolio(
    long_trades: pl.DataFrame,
    short_trades: pl.DataFrame,
    hourly: pl.DataFrame,
    config: StrategyConfig,
    market: ExtensionMarketData,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, int], pl.DataFrame]:
    """Research-only five-unit replay with a post-four-hour extension eviction.

    It preserves the frozen long-priority sequence. For a newly admitted long,
    open shorts are evicted from worse to better priority first. Only then may
    an extended long, already beyond its four-hour release time, be closed.
    """
    rules, short_rules = config.values["portfolio"], config.values["short"]
    candidates = pl.concat([long_trades, short_trades], how="diagonal_relaxed").to_dicts()
    hourly_by_symbol = {
        key[0] if isinstance(key, tuple) else key: group.sort("open_time")
        for key, group in hourly.group_by("symbol", maintain_order=True)
    }
    events = sorted({row["entry_time"] for row in candidates} | {row["exit_time"] for row in candidates})
    by_entry: dict[datetime, list[dict[str, Any]]] = {}
    for row in candidates:
        by_entry.setdefault(row["entry_time"], []).append(dict(row))
    cash, positions, accepted, account, audit = 1.0, {}, [], [], []
    counts = {name: 0 for name in (
        "LONG_NO_CAPACITY", "LONG_TIME_SLOT_CAP", "SHORT_SKIP_NO_FUNDS", "LONG_DUPLICATE_OPEN",
        "SHORT_DUPLICATE_OPEN", "LONG_PRIORITY_EVICTION", "LONG_EXTENSION_EVICTION",
        "waiting", "cancelled", "expired",
    )}

    def snapshot() -> tuple[int, str]:
        occupied = sum(int(position["units"]) for position in positions.values())
        return occupied, ";".join(
            f"{position['strategy']}:{position['symbol']}:{position['units']}"
            for position in sorted(positions.values(), key=lambda item: (item["strategy"], item["symbol"]))
        )

    def audit_row(row: dict[str, Any], status: str, successful_longs: int, before: tuple[int, str]) -> None:
        occupied, position_text = before
        audit.append({
            "strategy": row["strategy"], "symbol": row["symbol"], "signal_time": row["signal_time"],
            "entry_time": row["entry_time"], "planned_exit_time": row["planned_exit_time"],
            "priority_score": row["priority_score"], "requested_units": row["units"], "status": status,
            "open_units_before": occupied, "free_units_before": rules["total_units"] - occupied,
            "open_positions_before": position_text,
            "successful_long_entries_before": successful_longs if row["strategy"] == "long" else None,
        })

    def close(position: dict[str, Any]) -> None:
        nonlocal cash
        position["pnl"] = position["notional"] * position["net_return"]
        cash += position["notional"] + position["pnl"]
        accepted.append(position.copy())
        del positions[(position["strategy"], position["symbol"])]

    def close_short_for_long(position: dict[str, Any], event_time: datetime) -> None:
        price = _latest_completed_hourly_close(hourly_by_symbol, position["symbol"], event_time)
        position.update({
            "exit_reference": price,
            "exit_reason": "LONG_PRIORITY_EVICTION",
            "exit_time": event_time,
            "gross_return": 1 - price / position["entry_reference"],
            "cost_return": -short_rules["round_trip_stress_cost"],
            "funding_return": 0.0,
        })
        position["net_return"] = position["gross_return"] + position["cost_return"]
        position["mae_return"], position["mfe_return"] = _short_excursions_until_exit(
            hourly_by_symbol, position["symbol"], position["entry_time"], event_time, position["entry_reference"],
        )
        close(position)

    def close_extended_long_for_long(position: dict[str, Any], event_time: datetime) -> None:
        price = _latest_completed_minute_close(market.minutes_by_symbol, position["symbol"], event_time)
        entry_fill = position["entry_reference"] * (1 + config.values["long"]["slippage_per_side"])
        exit_fill = price * (1 - config.values["long"]["slippage_per_side"])
        ratio = exit_fill / entry_fill
        position.update({
            "exit_reference": price,
            "exit_reason": "LONG_EXTENSION_EVICTION",
            "exit_time": event_time,
            "gross_return": ratio - 1,
            "cost_return": -config.values["long"]["taker_fee_per_side"] * (1 + ratio),
            "funding_return": _funding_return(position, event_time, market.funding_by_symbol, market.minute_open_by_symbol, config.values["long"]),
        })
        position["net_return"] = position["gross_return"] + position["cost_return"] + position["funding_return"]
        close(position)

    for event_time in events:
        for position in list(positions.values()):
            if position["exit_time"] <= event_time:
                close(position)
        arriving = by_entry.get(event_time, [])
        longs = sorted((row for row in arriving if row["strategy"] == "long"), key=lambda row: (row.get("priority_order", row["priority_score"]), row["symbol"]))
        shorts = sorted((row for row in arriving if row["strategy"] == "short"), key=lambda row: (row.get("priority_order", row["priority_score"]), row["signal_time"], row["symbol"]))
        successful_longs = 0
        remaining = []
        for row in longs:
            before = snapshot()
            if ("long", row["symbol"]) in positions:
                counts["LONG_DUPLICATE_OPEN"] += 1
                audit_row(row, "LONG_DUPLICATE_OPEN", successful_longs, before)
            else:
                remaining.append(row)
        maximum = config.values["long"]["portfolio"]["max_positions_per_entry_time"]
        admissible, capped = remaining[:maximum], remaining[maximum:]
        for row in capped:
            counts["LONG_TIME_SLOT_CAP"] += 1
            audit_row(row, "LONG_TIME_SLOT_CAP", successful_longs, snapshot())
        requested_units = config.values["long"]["portfolio"]["single_signal_units"] if len(admissible) == 1 else config.values["long"]["portfolio"]["two_signal_units_each"]
        for candidate in admissible:
            row, before = dict(candidate), snapshot()
            row["units"] = requested_units
            free_units = rules["total_units"] - sum(int(position["units"]) for position in positions.values())
            if free_units < requested_units:
                for position in sorted((item for item in positions.values() if item["strategy"] == "short"), key=lambda item: (-item["priority_score"], item["signal_time"], item["symbol"])):
                    if free_units >= requested_units:
                        break
                    close_short_for_long(position, event_time)
                    counts["LONG_PRIORITY_EVICTION"] += 1
                    free_units = rules["total_units"] - sum(int(item["units"]) for item in positions.values())
            if free_units < requested_units:
                for position in extension_long_eviction_order(positions, event_time):
                    if free_units >= requested_units:
                        break
                    close_extended_long_for_long(position, event_time)
                    counts["LONG_EXTENSION_EVICTION"] += 1
                    free_units = rules["total_units"] - sum(int(item["units"]) for item in positions.values())
            granted = min(requested_units, rules["total_units"] - sum(int(position["units"]) for position in positions.values()))
            if granted <= 0:
                counts["LONG_NO_CAPACITY"] += 1
                audit_row(row, "LONG_NO_CAPACITY", successful_longs, before)
                continue
            position = dict(row, units=granted, notional=granted * cash / free_units)
            positions[("long", position["symbol"])] = position
            cash -= position["notional"]
            audit_row(row, "SELECTED", successful_longs, before)
            successful_longs += 1
        for row in shorts:
            before = snapshot()
            if ("short", row["symbol"]) in positions:
                counts["SHORT_DUPLICATE_OPEN"] += 1
                audit_row(row, "SHORT_DUPLICATE_OPEN", successful_longs, before)
                continue
            occupied = sum(int(position["units"]) for position in positions.values())
            short_units = sum(int(position["units"]) for position in positions.values() if position["strategy"] == "short")
            requested = int(row["units"])
            if occupied + requested > rules["total_units"] or short_units + requested > rules["short_unit_cap"]:
                counts["SHORT_SKIP_NO_FUNDS"] += 1
                audit_row(row, "SHORT_SKIP_NO_FUNDS", successful_longs, before)
                continue
            free_units = rules["total_units"] - occupied
            position = dict(row, units=requested, notional=requested * cash / free_units)
            positions[("short", position["symbol"])] = position
            cash -= position["notional"]
            audit_row(row, "SELECTED", successful_longs, before)
        account.append({"event_time": event_time, "realized_equity": cash + sum(position["notional"] for position in positions.values()), "cash": cash,
                        "open_units": sum(int(position["units"]) for position in positions.values()), "open_positions": len(positions)})
    if positions:
        raise ExperimentError("research window ended with open positions")
    accepted_frame = pl.DataFrame(accepted).sort("entry_time") if accepted else pl.DataFrame(schema=long_trades.schema)
    account_frame = pl.DataFrame(account).sort("event_time")
    audit_frame = pl.DataFrame(audit).sort(["entry_time", "strategy", "priority_score", "symbol"])
    return accepted_frame, account_frame, counts, audit_frame


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


def _yearly_account_summary(account: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    prior_equity = 1.0
    for year, frame in account.with_columns(pl.col("event_time").dt.year().alias("year")).group_by("year", maintain_order=True):
        ordered = frame.sort("event_time")
        equity = ordered.get_column("realized_equity")
        values = [prior_equity, *equity.to_list()]
        peak = values[0]
        drawdown = 0.0
        for value in values:
            peak = max(peak, value)
            drawdown = min(drawdown, value / peak - 1)
        ending = float(equity.tail(1).item())
        rows.append({"year": int(year[0] if isinstance(year, tuple) else year), "return": ending / prior_equity - 1,
                     "ending_equity": ending, "realized_max_drawdown": drawdown})
        prior_equity = ending
    return pl.DataFrame(rows).sort("year")


def run_experiment(root: Path, settings: ExperimentSettings, output: Path) -> dict[str, Any]:
    config = load_config(root)
    window = config.window(settings.source_window)
    source = config.root / "results" / "local" / settings.source_window
    long_trades = pl.read_parquet(source / "long_trades.parquet")
    short_trades = pl.read_parquet(source / "short_trades.parquet")
    manifest = json.loads((source / "run_manifest.json").read_text(encoding="utf-8"))
    if manifest.get("strategy_version") != config.version or manifest.get("parameters") != config.values:
        raise ExperimentError("source results do not match the frozen strategy configuration")
    variant_long, market = build_variant_long_trades(config, long_trades, settings)
    hourly = _load_short_hourly_paths(config.root, short_trades)
    candidate_counts = {"long": long_trades.height, "short": short_trades.height}
    baseline_portfolio, baseline_account, baseline_counts, baseline_audit = replay_portfolio(long_trades, short_trades, hourly, config)
    variant_portfolio, variant_account, variant_counts, variant_audit = replay_extension_portfolio(variant_long, short_trades, hourly, config, market)
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
    baseline_yearly = _yearly_account_summary(baseline_account).with_columns(pl.lit("baseline").alias("scenario"))
    variant_yearly = _yearly_account_summary(variant_account).with_columns(pl.lit("recent_protection_extension").alias("scenario"))
    yearly = pl.concat([baseline_yearly, variant_yearly], how="vertical").select("year", "scenario", "return", "ending_equity", "realized_max_drawdown").sort(["year", "scenario"])
    atomic_write_frame(output / "yearly_comparison.csv", yearly)
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
            "long_eviction_after_extension_hours": settings.long_eviction_after_extension.total_seconds() / 3600,
            "long_only": True,
            "preemptive_exits": ["HARD_STOP", "PROTECTION", "NEW_LONG_AFTER_SHORT_EVICTIONS"],
            "cap_exit_reason": "EXTENSION_CAP",
        },
        "out_of_sample": {"cutoff": cutoff.isoformat(), "fraction": settings.out_of_sample_fraction},
        "candidate_planned_and_activated": int(((long_trades.get_column("exit_reason") == "PLANNED_EXIT") & long_trades.get_column("shadow_activated")).sum()),
        "extended_trade_count": extended.height,
        "extended_exit_reasons": extended.group_by("exit_reason").len().sort("exit_reason").to_dicts() if not extended.is_empty() else [],
        "newly_unselected_vs_baseline": newly_unselected,
        "portfolio_pnl_delta": delta_concentration,
        "yearly": yearly.to_dicts(),
        "baseline": baseline_summary.to_dicts()[0],
        "variant": variant_summary.to_dicts()[0],
        "baseline_out_of_sample": _out_of_sample(baseline_account, cutoff),
        "variant_out_of_sample": _out_of_sample(variant_account, cutoff),
    }
    atomic_write_json(output / "summary.json", result, indent=2)
    return result
