from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import floor
from typing import Any

import polars as pl

from .storage import DataError
from .config import StrategyConfig


TRADE_COLUMNS = [
    "trade_id", "strategy", "symbol", "signal_time", "entry_time", "planned_exit_time", "exit_time",
    "entry_reference", "exit_reference", "exit_reason", "units", "notional", "gross_return",
    "cost_return", "funding_return", "net_return", "pnl", "mae_return", "mfe_return", "priority_score",
    "priority_order",
    "visible_hourly_close",
    "shadow_exit_time", "shadow_activated", "shadow_max_retrace", "protection_history_count",
    "protection_activated_history_count", "protection_allowed_retrace",
    "protection_activated_at", "extension_applied", "extension_release_time", "extension_deadline",
]

FUNDING_EVENT_COLUMNS = [
    "trade_id", "strategy", "symbol", "signal_time", "entry_time", "actual_exit_time",
    "funding_time", "funding_rate", "settlement_minute_open", "entry_reference", "entry_fill",
    "funding_return_contribution",
]

SHADOW_HISTORY_COLUMNS = [
    "shadow_exit_time", "shadow_activated", "shadow_max_retrace",
]


@dataclass(frozen=True)
class _LongPath:
    exit_time: datetime
    exit_reference: float
    exit_reason: str
    max_retrace: float | None
    activated: bool
    mae: float
    mfe: float
    activated_at: datetime | None


def _linear_quantile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower, upper = floor(position), min(floor(position) + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _continuous_prefix(
    frame: pl.DataFrame, symbol: str, start: datetime, end_exclusive: datetime, interval: timedelta,
) -> list[dict[str, Any]]:
    """Require an observed path; a missing suffix is not a settlement price."""
    bars = frame.filter((pl.col("open_time") >= start) & (pl.col("open_time") < end_exclusive)).sort("open_time").to_dicts()
    if not bars or bars[0]["open_time"] != start:
        raise DataError(f"missing entry reference for {symbol}: {start.isoformat()}")
    for left, right in zip(bars, bars[1:]):
        if right["open_time"] - left["open_time"] != interval:
            raise DataError(f"non-terminal path gap for {symbol}: {start.isoformat()}..{end_exclusive.isoformat()}")
    if bars[-1]["open_time"] + interval != end_exclusive:
        raise DataError(f"incomplete terminal path for {symbol}: {start.isoformat()}..{end_exclusive.isoformat()}; missing data is not an execution")
    return bars


def _simulate_long(
    bars: list[dict[str, Any]],
    entry_reference: float,
    planned_exit_time: datetime,
    allowed_retrace: float | None,
    rules: dict,
) -> _LongPath:
    """The one long bar engine; `None` means the required base-shadow path."""
    stop_price = entry_reference * (1 + rules["hard_stop_return"])
    protection = rules["protection"]
    activation_price = entry_reference * (1 + protection["activation_return"])
    active, peak, maximum_retrace, activated_at = False, entry_reference, None, None
    lowest, highest = entry_reference, entry_reference
    for bar in bars:
        lowest, highest = min(lowest, float(bar["low"])), max(highest, float(bar["high"]))
        if float(bar["low"]) <= stop_price:  # hard stop always precedes protection and peak update
            return _LongPath(bar["open_time"] + timedelta(minutes=1), min(float(bar["open"]), stop_price), "HARD_STOP", maximum_retrace, active, lowest / entry_reference - 1, highest / entry_reference - 1, activated_at)
        if active:
            retrace = max(0.0, (peak - float(bar["low"])) / peak)
            maximum_retrace = retrace if maximum_retrace is None else max(maximum_retrace, retrace)
            if allowed_retrace is not None:
                trailing = peak * (1 - allowed_retrace)
                floor_price = entry_reference * (1 + 2 * (rules["slippage_per_side"] + rules["taker_fee_per_side"]))
                effective_exit = max(trailing, floor_price)
                if float(bar["low"]) <= effective_exit:
                    return _LongPath(bar["open_time"] + timedelta(minutes=1), min(float(bar["open"]), effective_exit), "PROTECTION", maximum_retrace, active, lowest / entry_reference - 1, highest / entry_reference - 1, activated_at)
            peak = max(peak, float(bar["high"]))
        elif float(bar["high"]) >= activation_price:
            # The activation bar cannot immediately use protection, but its high is
            # the prior peak available to the next complete minute.
            active, peak = True, max(peak, float(bar["high"]))
            activated_at = bar["open_time"] + timedelta(minutes=1)
    final = bars[-1]
    return _LongPath(planned_exit_time, float(final["close"]), "PLANNED_EXIT", maximum_retrace, active, lowest / entry_reference - 1, highest / entry_reference - 1, activated_at)


def minute_requirements(long_signals: pl.DataFrame) -> set[tuple[str, datetime]]:
    required: set[tuple[str, datetime]] = set()
    for signal in long_signals.select("symbol", "entry_time", "planned_exit_time").to_dicts():
        current = signal["entry_time"] - timedelta(minutes=1)
        while current < signal["planned_exit_time"]:
            required.add((signal["symbol"], current.replace(hour=0, minute=0, second=0, microsecond=0)))
            current = (current.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
    return required


def funding_requirements(long_signals: pl.DataFrame) -> set[tuple[str, int, int]]:
    required: set[tuple[str, int, int]] = set()
    for signal in long_signals.select("symbol", "entry_time", "planned_exit_time").to_dicts():
        current = signal["entry_time"].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while current < signal["planned_exit_time"]:
            required.add((signal["symbol"], current.year, current.month))
            current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
    return required


def extension_requirements(trades: pl.DataFrame, config: StrategyConfig) -> tuple[set[tuple[str, datetime]], set[tuple[str, int, int]]]:
    """Return only the extra dates needed by long trades eligible for the selected extension."""
    if trades.is_empty():
        return set(), set()
    policy = config.values["long"]["extension"]
    lookback = timedelta(hours=policy["activation_lookback_hours"])
    eligible = trades.filter(
        (pl.col("exit_reason") == "PLANNED_EXIT")
        & pl.col("protection_activated_at").is_not_null()
        & (pl.col("protection_activated_at") > pl.col("planned_exit_time") - lookback)
        & (pl.col("protection_activated_at") <= pl.col("planned_exit_time"))
    ).select("symbol", "planned_exit_time").to_dicts()
    minute_days: set[tuple[str, datetime]] = set()
    funding_months: set[tuple[str, int, int]] = set()
    extension = timedelta(hours=policy["maximum_extension_hours"])
    for row in eligible:
        current = row["planned_exit_time"].replace(hour=0, minute=0, second=0, microsecond=0)
        deadline = row["planned_exit_time"] + extension
        while current < deadline:
            minute_days.add((row["symbol"], current))
            current += timedelta(days=1)
        current = row["planned_exit_time"].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while current < deadline:
            funding_months.add((row["symbol"], current.year, current.month))
            current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
    return minute_days, funding_months


def extend_long_trades(trades: pl.DataFrame, minutes: pl.DataFrame, funding: pl.DataFrame,
                       config: StrategyConfig) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Apply the frozen 4h/24h extension to eligible completed base trades."""
    if trades.is_empty():
        return trades, pl.DataFrame(schema={name: pl.Null for name in FUNDING_EVENT_COLUMNS})
    rules, policy = config.values["long"], config.values["long"]["extension"]
    lookback = timedelta(hours=policy["activation_lookback_hours"])
    extension = timedelta(hours=policy["maximum_extension_hours"])
    release = timedelta(hours=policy["evict_after_hours"])
    by_symbol = {key[0] if isinstance(key, tuple) else key: group.sort("open_time")
                 for key, group in minutes.group_by("symbol", maintain_order=True)}
    minute_opens = {symbol: {row["open_time"]: float(row["open"]) for row in frame.select("open_time", "open").to_dicts()}
                    for symbol, frame in by_symbol.items()}
    funding_by_symbol = {key[0] if isinstance(key, tuple) else key: group.to_dicts()
                         for key, group in funding.group_by("symbol", maintain_order=True)}
    result, funding_events = [], []
    for original in trades.sort("entry_time").to_dicts():
        row = dict(original)
        activated = row.get("protection_activated_at")
        eligible = (row["exit_reason"] == "PLANNED_EXIT" and activated is not None
                    and row["planned_exit_time"] - lookback < activated <= row["planned_exit_time"])
        if not eligible:
            result.append(row)
            continue
        deadline = row["planned_exit_time"] + extension
        frame = by_symbol.get(row["symbol"])
        if frame is None:
            raise DataError(f"missing extension minute data for {row['symbol']}")
        prefix = _continuous_prefix(frame, row["symbol"], row["entry_time"] - timedelta(minutes=1), deadline, timedelta(minutes=1))
        outcome = _simulate_long(prefix[1:], float(row["entry_reference"]), deadline,
                                 float(row["protection_allowed_retrace"]), rules)
        if outcome.exit_time <= row["planned_exit_time"]:
            raise DataError(f"extension replay disagrees with base path for {row['trade_id']}")
        exit_reason = "EXTENSION_CAP" if outcome.exit_reason == "PLANNED_EXIT" else outcome.exit_reason
        entry_fill = float(row["entry_reference"]) * (1 + rules["slippage_per_side"])
        funding_return = 0.0
        for event in funding_by_symbol.get(row["symbol"], []):
            if not row["entry_time"] < event["funding_time"] <= outcome.exit_time:
                continue
            settlement = event["funding_time"].replace(second=0, microsecond=0)
            price = minute_opens[row["symbol"]].get(settlement)
            if price is None:
                raise DataError(f"missing settlement minute for {row['symbol']} at {settlement.isoformat()}")
            contribution = -float(event["funding_rate"]) * price / entry_fill
            funding_return += contribution
            funding_events.append({"trade_id": row["trade_id"], "strategy": "long", "symbol": row["symbol"],
                "signal_time": row["signal_time"], "entry_time": row["entry_time"], "actual_exit_time": outcome.exit_time,
                "funding_time": event["funding_time"], "funding_rate": event["funding_rate"],
                "settlement_minute_open": price, "entry_reference": row["entry_reference"], "entry_fill": entry_fill,
                "funding_return_contribution": contribution})
        exit_fill = outcome.exit_reference * (1 - rules["slippage_per_side"])
        ratio = exit_fill / entry_fill
        cost = -rules["taker_fee_per_side"] * (1 + ratio)
        row.update({"exit_time": outcome.exit_time, "exit_reference": outcome.exit_reference,
                    "exit_reason": exit_reason, "gross_return": ratio - 1, "cost_return": cost,
                    "funding_return": funding_return, "net_return": ratio - 1 + cost + funding_return,
                    "pnl": ratio - 1 + cost + funding_return, "mae_return": outcome.mae, "mfe_return": outcome.mfe,
                    "extension_applied": True, "extension_release_time": row["planned_exit_time"] + release,
                    "extension_deadline": deadline})
        result.append(row)
    extended = pl.DataFrame(result, infer_schema_length=None).select(TRADE_COLUMNS).sort("entry_time")
    details = (pl.DataFrame(funding_events, infer_schema_length=None).select(FUNDING_EVENT_COLUMNS).sort(["entry_time", "funding_time"])
               if funding_events else pl.DataFrame(schema={name: pl.Null for name in FUNDING_EVENT_COLUMNS}))
    return extended, details


def execute_long(
    signals: pl.DataFrame,
    minutes: pl.DataFrame,
    funding: pl.DataFrame,
    config: StrategyConfig,
    shadow_history: pl.DataFrame | None = None,
    emit_from: datetime | None = None,
) -> pl.DataFrame:
    return execute_long_with_funding_diagnostics(signals, minutes, funding, config, shadow_history, emit_from)[0]


def _completed_shadow_history(shadow_history: pl.DataFrame | None) -> list[tuple[datetime, bool, float | None]]:
    if shadow_history is None or shadow_history.is_empty():
        return []
    missing = set(SHADOW_HISTORY_COLUMNS) - set(shadow_history.columns)
    if missing:
        raise DataError(f"shadow history is missing columns: {sorted(missing)}")
    return [
        (row["shadow_exit_time"], bool(row["shadow_activated"]), row["shadow_max_retrace"])
        for row in shadow_history.select(SHADOW_HISTORY_COLUMNS).drop_nulls("shadow_exit_time").to_dicts()
    ]


def execute_long_with_funding_diagnostics(
    signals: pl.DataFrame,
    minutes: pl.DataFrame,
    funding: pl.DataFrame,
    config: StrategyConfig,
    shadow_history: pl.DataFrame | None = None,
    emit_from: datetime | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Execute long candidates, optionally seeding P90 from prior base shadows.

    Candidates before ``emit_from`` are intentionally base-shadow-only: they
    update later P90 eligibility but neither receive funding data nor enter the
    returned trade set.
    """
    if signals.is_empty():
        return (
            pl.DataFrame(schema={name: pl.Null for name in TRADE_COLUMNS}),
            pl.DataFrame(schema={name: pl.Null for name in FUNDING_EVENT_COLUMNS}),
        )
    rules = config.values["long"]
    minute_by_symbol = {key[0] if isinstance(key, tuple) else key: group.sort("open_time") for key, group in minutes.group_by("symbol", maintain_order=True)}
    minute_open_by_symbol = {
        symbol: {row["open_time"]: float(row["open"]) for row in frame.select("open_time", "open").to_dicts()}
        for symbol, frame in minute_by_symbol.items()
    }
    funding_by_symbol = {
        key[0] if isinstance(key, tuple) else key: group.to_dicts()
        for key, group in funding.group_by("symbol", maintain_order=True)
    }
    base: list[tuple[dict[str, Any], _LongPath, float, list[dict[str, Any]]]] = []
    for signal in signals.sort("entry_time").to_dicts():
        symbol, entry, planned = signal["symbol"], signal["entry_time"], signal["planned_exit_time"]
        if symbol not in minute_by_symbol:
            raise DataError(f"missing minute data for {symbol}")
        prefix = _continuous_prefix(minute_by_symbol[symbol], symbol, entry - timedelta(minutes=1), planned, timedelta(minutes=1))
        if len(prefix) < 2:
            raise DataError(f"missing completed entry minute for {symbol}: {entry.isoformat()}")
        path = prefix[1:]
        # At decision T, the T-to-T+1 minute is the first completed entry
        # minute. Its close is the entry reference; T-1h close is only a
        # visible diagnostic field and is never used as the execution price.
        reference = float(prefix[0]["close"])
        base.append((signal, _simulate_long(path, reference, planned, None, rules), reference, path))
    completed = _completed_shadow_history(shadow_history)
    rows: list[dict[str, Any]] = []
    funding_events: list[dict[str, Any]] = []
    # The protection state is fed only when an already completed base event has
    # arrived; it never receives a future candidate as a threshold sample.
    for signal, base_path, reference, path in base:
        entry = signal["entry_time"]
        protection = rules["protection"]
        available = [item for item in completed if entry - timedelta(days=protection["window_days"]) <= item[0] <= entry]
        activated = [item[2] for item in available if item[1] and item[2] is not None]
        if emit_from is not None and entry < emit_from:
            completed.append((base_path.exit_time, base_path.activated, base_path.max_retrace))
            continue
        allowed = protection["fallback_retrace"] if len(available) < protection["minimum_history"] or len(activated) < protection["minimum_activated_history"] else _linear_quantile(activated, protection["retrace_quantile"])
        outcome = _simulate_long(path, reference, signal["planned_exit_time"], allowed, rules)
        entry_fill = reference * (1 + rules["slippage_per_side"])
        funding_return = 0.0
        for event in funding_by_symbol.get(signal["symbol"], []):
            if not entry < event["funding_time"] <= outcome.exit_time:
                continue
            settlement_minute = event["funding_time"].replace(second=0, microsecond=0)
            price = minute_open_by_symbol[signal["symbol"]].get(settlement_minute)
            if price is None:
                raise DataError(f"missing settlement minute for {signal['symbol']} at {settlement_minute.isoformat()}")
            contribution = -float(event["funding_rate"]) * price / entry_fill
            funding_return += contribution
            funding_events.append({
                "trade_id": signal["trade_id"], "strategy": "long", "symbol": signal["symbol"],
                "signal_time": signal["decision_time"], "entry_time": entry, "actual_exit_time": outcome.exit_time,
                "funding_time": event["funding_time"], "funding_rate": event["funding_rate"],
                "settlement_minute_open": price, "entry_reference": reference,
                "entry_fill": entry_fill, "funding_return_contribution": contribution,
            })
        exit_fill = outcome.exit_reference * (1 - rules["slippage_per_side"])
        ratio = exit_fill / entry_fill
        gross, cost = ratio - 1, -rules["taker_fee_per_side"] * (1 + ratio)
        rows.append({
            "trade_id": signal["trade_id"], "strategy": "long", "symbol": signal["symbol"], "signal_time": signal["decision_time"],
            "entry_time": entry, "planned_exit_time": signal["planned_exit_time"], "exit_time": outcome.exit_time,
            "entry_reference": reference, "exit_reference": outcome.exit_reference, "exit_reason": outcome.exit_reason,
            "visible_hourly_close": signal.get("close"),
            "units": signal["requested_units"], "notional": 1.0, "gross_return": gross, "cost_return": cost,
            "funding_return": funding_return, "net_return": gross + cost + funding_return, "pnl": gross + cost + funding_return,
            "mae_return": outcome.mae, "mfe_return": outcome.mfe, "priority_score": signal["priority_score"],
            "priority_order": signal.get("priority_order", 1),
            "shadow_exit_time": base_path.exit_time, "shadow_activated": base_path.activated,
            "shadow_max_retrace": base_path.max_retrace, "protection_history_count": len(available),
            "protection_activated_history_count": len(activated), "protection_allowed_retrace": allowed,
            "protection_activated_at": outcome.activated_at, "extension_applied": False,
            "extension_release_time": None, "extension_deadline": None,
        })
        completed.append((base_path.exit_time, base_path.activated, base_path.max_retrace))
    diagnostics = pl.DataFrame(funding_events).select(FUNDING_EVENT_COLUMNS).sort(["entry_time", "funding_time"]) if funding_events else pl.DataFrame(schema={name: pl.Null for name in FUNDING_EVENT_COLUMNS})
    trades = pl.DataFrame(rows).select(TRADE_COLUMNS).sort("entry_time") if rows else pl.DataFrame(schema={name: pl.Null for name in TRADE_COLUMNS})
    return trades, diagnostics


def execute_short(signals: pl.DataFrame, hourly: pl.DataFrame, config: StrategyConfig) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    rules = config.values["short"]
    by_symbol = {key[0] if isinstance(key, tuple) else key: group.sort("open_time") for key, group in hourly.group_by("symbol", maintain_order=True)}
    for signal in signals.sort("entry_time").to_dicts():
        symbol, entry, planned = signal["symbol"], signal["entry_time"], signal["planned_exit_time"]
        bars = by_symbol.get(symbol)
        if bars is None:
            raise DataError(f"missing hourly data for {symbol}")
        prefix = _continuous_prefix(bars, symbol, entry - timedelta(hours=1), planned, timedelta(hours=1))
        if len(prefix) < 2:
            raise DataError(f"missing completed entry hour for {symbol}: {entry.isoformat()}")
        entry_reference = float(prefix[0]["close"])
        stop = entry_reference * (1 + rules["hard_stop_return"])
        path = prefix[1:]
        outcome_time, outcome_reference, reason = planned, float(path[-1]["close"]), "PLANNED_EXIT"
        realized_path = path
        for index, bar in enumerate(path):
            if float(bar["high"]) >= stop:
                outcome_time, outcome_reference, reason = bar["open_time"] + timedelta(hours=1), max(float(bar["open"]), stop), "HARD_STOP"
                realized_path = path[:index + 1]
                break
        gross, cost = 1 - outcome_reference / entry_reference, -rules["round_trip_stress_cost"]
        lows, highs = [float(bar["low"]) for bar in realized_path], [float(bar["high"]) for bar in realized_path]
        rows.append({
            "trade_id": signal["trade_id"], "strategy": "short", "symbol": symbol, "signal_time": signal["decision_time"],
            "entry_time": entry, "planned_exit_time": planned, "exit_time": outcome_time, "entry_reference": entry_reference,
            "exit_reference": outcome_reference, "exit_reason": reason,
            "units": signal.get("requested_units", rules["portfolio"]["units_per_signal"]), "notional": 1.0,
            "visible_hourly_close": entry_reference,
            "gross_return": gross, "cost_return": cost, "funding_return": 0.0, "net_return": gross + cost, "pnl": gross + cost,
            "mae_return": min(0.0, 1 - max(highs) / entry_reference), "mfe_return": max(0.0, 1 - min(lows) / entry_reference),
            "priority_score": signal["priority_score"], "priority_order": signal.get("priority_order", 1),
            "shadow_exit_time": None, "shadow_activated": None, "shadow_max_retrace": None,
            "protection_history_count": None, "protection_activated_history_count": None,
            "protection_allowed_retrace": None,
            "protection_activated_at": None, "extension_applied": False,
            "extension_release_time": None, "extension_deadline": None,
        })
    return pl.DataFrame(rows).select(TRADE_COLUMNS).sort("entry_time") if rows else pl.DataFrame(schema={name: pl.Null for name in TRADE_COLUMNS})
