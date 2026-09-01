from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from math import floor
from typing import Any

import polars as pl

from ..config import StrategyConfig
from ..features import build_features
from ..signals import long_signals, short_signals


@dataclass(frozen=True)
class Admission:
    candidate: dict[str, Any]
    units: int
    evict_intent_ids: tuple[str, ...] = ()


def decision_candidates(hourly: pl.DataFrame, decision_time: datetime, config: StrategyConfig) -> list[dict[str, Any]]:
    """Return frozen candidates at one timestamp; only live entry timing changes."""
    features = build_features(hourly, config)
    tomorrow = decision_time.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    rows: list[dict[str, Any]] = []
    if decision_time.hour in config.values["long"]["entry_hours_utc"]:
        long = long_signals(features, decision_time, tomorrow + timedelta(days=1), config).filter(pl.col("decision_time") == pl.lit(decision_time))
        for row in long.to_dicts():
            row["entry_time"] = decision_time  # documented live/testnet execution delta
            row["position_side"] = "LONG"
            row["trade_id"] = f"live:{row['trade_id']}"
            rows.append(row)
    if decision_time.hour in config.values["short"]["entry_hours_utc"]:
        day_start = decision_time.replace(hour=0, minute=0, second=0, microsecond=0)
        short = short_signals(features, day_start, tomorrow, config).filter(pl.col("decision_time") == pl.lit(decision_time))
        for row in short.to_dicts():
            row["position_side"] = "SHORT"
            row["trade_id"] = f"live:{row['trade_id']}"
            rows.append(row)
    return rows


def _open_key(position: dict[str, Any]) -> tuple[str, str]:
    return str(position["strategy"]), str(position["symbol"])


def plan_admissions(candidates: list[dict[str, Any]], open_positions: list[dict[str, Any]], config: StrategyConfig) -> list[Admission]:
    """Frozen LONG_PRIORITY_SKIP capacity ordering without future-price assumptions."""
    rules, long_rules, short_rules = config.values["portfolio"], config.values["long"]["portfolio"], config.values["short"]["portfolio"]
    simulated = [dict(position) for position in open_positions]
    admissions: list[Admission] = []
    occupied = lambda: sum(int(position["units"]) for position in simulated)
    open_keys = lambda: {_open_key(position) for position in simulated}
    longs = sorted((row for row in candidates if row["strategy"] == "long"), key=lambda row: (row["priority_order"], row["symbol"]))
    eligible = [row for row in longs if ("long", row["symbol"]) not in open_keys()][:long_rules["max_positions_per_entry_time"]]
    requested = long_rules["single_signal_units"] if len(eligible) == 1 else long_rules["two_signal_units_each"]
    for row in eligible:
        evictions: list[str] = []
        free_units = rules["total_units"] - occupied()
        if free_units < requested:
            victims = sorted(
                (position for position in simulated if position["strategy"] == "short"),
                key=lambda position: (-float(position["priority_score"]), position["decision_time"], position["symbol"]),
            )
            for victim in victims:
                if free_units >= requested:
                    break
                simulated.remove(victim)
                evictions.append(str(victim["intent_id"]))
                free_units = rules["total_units"] - occupied()
        granted = min(requested, rules["total_units"] - occupied())
        if granted > 0:
            candidate = dict(row, units=granted)
            simulated.append(candidate)
            admissions.append(Admission(candidate, granted, tuple(evictions)))
    shorts = sorted((row for row in candidates if row["strategy"] == "short"), key=lambda row: (row["priority_order"], row["decision_time"], row["symbol"]))
    for row in shorts:
        if ("short", row["symbol"]) in open_keys():
            continue
        short_units = sum(int(position["units"]) for position in simulated if position["strategy"] == "short")
        requested = int(row["requested_units"])
        if occupied() + requested > rules["total_units"] or short_units + requested > rules["short_unit_cap"]:
            continue
        candidate = dict(row, units=requested)
        simulated.append(candidate)
        admissions.append(Admission(candidate, requested))
    return admissions


def unit_notional(safe_available_usdt: Decimal, occupied_units: int, requested_units: int, config: StrategyConfig) -> Decimal:
    total = config.values["portfolio"]["total_units"]
    free = total - occupied_units
    if safe_available_usdt <= 0 or free <= 0 or requested_units <= 0 or requested_units > free:
        return Decimal("0")
    return safe_available_usdt / Decimal(free) * Decimal(requested_units)


def allowed_retrace(history: list[dict[str, Any]], entry_time: datetime, config: StrategyConfig) -> float:
    rules = config.values["long"]["protection"]
    earliest = entry_time - timedelta(days=rules["window_days"])
    available = []
    activated: list[float] = []
    for row in history:
        exit_time = datetime.fromisoformat(str(row["shadow_exit_time"]))
        if earliest <= exit_time <= entry_time:
            available.append(row)
            if bool(row["shadow_activated"]) and row["shadow_max_retrace"] is not None:
                activated.append(float(row["shadow_max_retrace"]))
    if len(available) < rules["minimum_history"] or len(activated) < rules["minimum_activated_history"]:
        return float(rules["fallback_retrace"])
    ordered = sorted(activated)
    position = (len(ordered) - 1) * float(rules["retrace_quantile"])
    lower, upper = floor(position), min(floor(position) + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def long_protection_update(position: dict[str, Any], minute_bar: dict[str, Any], config: StrategyConfig) -> tuple[bool, bool, Decimal]:
    """Return (should_exit, active, updated_peak) using the frozen next-minute activation rule."""
    rules = config.values["long"]
    protection = rules["protection"]
    entry = Decimal(str(position["entry_price"]))
    high, low = Decimal(str(minute_bar["high"])), Decimal(str(minute_bar["low"]))
    active = bool(position["protection_active"])
    peak = Decimal(str(position["protection_peak"] or position["entry_price"]))
    if not active:
        if high >= entry * (Decimal("1") + Decimal(str(protection["activation_return"]))):
            return False, True, max(peak, high)
        return False, False, peak
    raw_allowed = position["protection_allowed_retrace"]
    allowed = Decimal(str(protection["fallback_retrace"] if raw_allowed is None else raw_allowed))
    trailing = peak * (Decimal("1") - allowed)
    floor_price = entry * (Decimal("1") + Decimal("2") * (Decimal(str(rules["slippage_per_side"])) + Decimal(str(rules["taker_fee_per_side"]))))
    should_exit = low <= max(trailing, floor_price)
    return should_exit, True, max(peak, high)
