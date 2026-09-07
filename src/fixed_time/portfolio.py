from __future__ import annotations

from datetime import timedelta
from math import isclose
from typing import Any

import polars as pl

from .config import StrategyConfig


class PortfolioError(ValueError):
    pass


def admitted_long_units(requested: int, free: int, *, single_candidate: bool, strict_fragment: bool) -> int:
    """Use one already-idle unit for a lone two-unit request; never create it by eviction."""
    return 1 if strict_fragment and single_candidate and requested == 2 and free == 1 else requested


def drawdown_multiplier(drawdown: float, sizing: dict[str, Any]) -> float:
    """Apply inclusive tiers without letting binary floating-point move a boundary."""
    for tier in reversed(sizing["tiers"]):
        threshold = float(tier["threshold"])
        if drawdown > threshold or isclose(drawdown, threshold, rel_tol=0.0, abs_tol=1e-12):
            return float(tier["multiplier"])
    return float(sizing["base_multiplier"])


def _latest_completed_hourly_close(hourly_by_symbol: dict[str, pl.DataFrame], symbol: str, entry_time) -> float:
    cutoff = entry_time - timedelta(hours=1)
    hourly = hourly_by_symbol.get(symbol)
    if hourly is None:
        raise PortfolioError(f"no hourly path for eviction: {symbol}")
    price = hourly.filter(pl.col("open_time") <= cutoff).tail(1).get_column("close")
    if price.len() != 1:
        raise PortfolioError(f"no completed hourly close for eviction: {symbol} at {entry_time}")
    return float(price.item())


def _short_excursions_until_exit(
    hourly_by_symbol: dict[str, pl.DataFrame], symbol: str, entry_time, exit_time, entry_reference: float,
) -> tuple[float, float]:
    """Return the short MAE/MFE over fully completed bars through an eviction."""
    hourly = hourly_by_symbol.get(symbol)
    if hourly is None:
        raise PortfolioError(f"no hourly path for eviction: {symbol}")
    last_completed_open = exit_time - timedelta(hours=1)
    path = hourly.filter(
        (pl.col("open_time") >= entry_time) & (pl.col("open_time") <= last_completed_open)
    )
    if path.is_empty():
        raise PortfolioError(f"no completed holding bar for eviction: {symbol} at {exit_time}")
    return (
        min(0.0, 1 - float(path.get_column("high").max()) / entry_reference),
        max(0.0, 1 - float(path.get_column("low").min()) / entry_reference),
    )


def replay_portfolio(long_trades: pl.DataFrame, short_trades: pl.DataFrame, hourly: pl.DataFrame, config: StrategyConfig,
                     minutes: pl.DataFrame | None = None, funding: pl.DataFrame | None = None,
                     ) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, int], pl.DataFrame]:
    """Three-unit shared account with causal completed-minute marking.

    ``cash`` is the realized account value.  ``marked_equity`` additionally
    includes the linear PnL of currently open positions and is the only value
    used for drawdown sizing.  Keeping both values prevents reports from
    describing a marked drawdown as an already-realized drawdown.
    """
    rules, short_rules, sizing = config.values["portfolio"], config.values["short"], config.values["portfolio"]["drawdown_sizing"]
    candidate_frame = pl.concat([long_trades, short_trades], how="vertical_relaxed")
    candidates = candidate_frame.to_dicts()
    hourly_by_symbol = {key[0] if isinstance(key, tuple) else key: group.sort("open_time") for key, group in hourly.group_by("symbol", maintain_order=True)}
    by_entry: dict[Any, list[dict[str, Any]]] = {}
    for row in candidates:
        by_entry.setdefault(row["entry_time"], []).append(dict(row))
    events = {row["entry_time"] for row in candidates} | {row["exit_time"] for row in candidates}
    if candidates and funding is not None and not funding.is_empty():
        lower, upper = min(events), max(events)
        events.update(t.replace(second=0, microsecond=0) + (timedelta(minutes=1) if t.second or t.microsecond else timedelta())
                      for t in funding["funding_time"].to_list() if lower < t <= upper)
    events = sorted(events)
    minute_rows = minutes.to_dicts() if minutes is not None and not minutes.is_empty() else []
    minute_close = {
        (str(row["symbol"]), row["open_time"] + timedelta(minutes=1)): float(row["close"])
        for row in minute_rows
    }
    minute_open = {
        (str(row["symbol"]), row["open_time"]): float(row["open"])
        for row in minute_rows if "open" in row
    }
    minute_by_symbol = ({key[0] if isinstance(key, tuple) else key: group.sort("open_time")
                         for key, group in minutes.group_by("symbol", maintain_order=True)}
                        if minutes is not None and not minutes.is_empty() else {})
    funding_by_symbol = ({key[0] if isinstance(key, tuple) else key: group.to_dicts()
                          for key, group in funding.group_by("symbol", maintain_order=True)}
                         if funding is not None and not funding.is_empty() else {})
    cash, peak, positions, accepted, account, audit = 1.0, 1.0, {}, [], [], []
    counts = {name: 0 for name in ("LONG_NO_CAPACITY", "LONG_TIME_SLOT_CAP", "SHORT_SKIP_NO_FUNDS", "LONG_DUPLICATE_OPEN", "SHORT_DUPLICATE_OPEN", "LONG_PRIORITY_EVICTION", "LONG_EXTENSION_EVICTION", "LONG_SINGLE_UNIT_FRAGMENT", "waiting", "cancelled", "expired")}
    last_mark: Any | None = None

    def occupied() -> int:
        return sum(int(position["units"]) for position in positions.values())

    def register(position: dict[str, Any]) -> None:
        nonlocal cash
        strategy = position["strategy"]
        position["entry_fill"] = float(position.get("entry_fill") or
            float(position["entry_reference"]) * (1 + config.values["long"]["slippage_per_side"] if strategy == "long" else 1))
        cost = position.get("entry_cost_return")
        if cost is None:
            cost = -config.values["long"]["taker_fee_per_side"] if strategy == "long" else position["cost_return"] / 2
        position["entry_cost_return"] = float(cost)
        position["_booked_cashflow"] = position["notional"] * float(cost)
        position["_funding_events"] = sorted(
            (event for event in funding_by_symbol.get(position["symbol"], [])
             if strategy == "long" and position["entry_time"] < event["funding_time"] <= position["exit_time"]),
            key=lambda event: event["funding_time"],
        )
        position["_funding_index"] = 0
        cash += position["_booked_cashflow"]
        positions[(strategy, position["symbol"])] = position

    def settle_funding(when) -> None:
        nonlocal cash
        for position in positions.values():
            if position["strategy"] != "long":
                continue  # Frozen short stress model deliberately excludes funding.
            pending = position["_funding_events"]
            while position["_funding_index"] < len(pending):
                event = pending[position["_funding_index"]]
                if event["funding_time"] > min(when, position["exit_time"]):
                    break
                key = (position["symbol"], event["funding_time"].replace(second=0, microsecond=0))
                boundary = position.get("extension_deadline")
                at_boundary = (boundary is not None and event["funding_time"] == boundary
                               and boundary < position["planned_exit_time"] + timedelta(hours=config.values["long"]["extension"]["maximum_extension_hours"]))
                price = (minute_close.get(key) if at_boundary
                         else minute_open.get(key))
                if price is None:
                    raise PortfolioError(f"missing funding settlement price: {key}")
                amount = -position["notional"] * float(event["funding_rate"]) * price / position["entry_fill"]
                cash += amount
                position["_booked_cashflow"] += amount
                position["_funding_index"] += 1

    def marked_equity(when) -> float:
        value = cash
        for position in positions.values():
            close = (float(position["entry_reference"]) if position["entry_time"] == when
                     else minute_close.get((str(position["symbol"]), when)))
            if close is None:
                if minutes is not None:
                    raise PortfolioError(f"missing completed minute close for {position['symbol']} at {when}")
                continue
            entry = float(position["entry_fill"])
            # USD-M contracts are linear.  A short entered at 100 and marked
            # at 80 earns +20% of its entry notional, not +25%.
            change = close / entry - 1 if position["strategy"] == "long" else 1 - close / entry
            value += float(position["notional"]) * change
        return value

    def audit_snapshot() -> tuple[int, str]:
        return occupied(), ";".join(f"{p['strategy']}:{p['symbol']}:{p['units']}" for p in sorted(positions.values(), key=lambda item: (item["strategy"], item["symbol"])))

    def audit_row(row: dict[str, Any], status: str, successful_longs: int, snapshot: tuple[int, str]) -> None:
        used, text = snapshot
        audit.append({"strategy": row["strategy"], "symbol": row["symbol"], "signal_time": row["signal_time"], "entry_time": row["entry_time"],
                      "planned_exit_time": row["planned_exit_time"], "priority_score": row["priority_score"], "requested_units": row["units"],
                      "status": status, "open_units_before": used, "free_units_before": rules["total_units"] - used,
                      "open_positions_before": text, "successful_long_entries_before": successful_longs if row["strategy"] == "long" else None})

    def close(position: dict[str, Any], exit_time, exit_reference: float | None = None, reason: str | None = None) -> None:
        nonlocal cash
        if exit_reference is not None:
            position.update(exit_reference=exit_reference, exit_reason=reason, exit_time=exit_time)
            if position["strategy"] == "short":
                position["gross_return"] = 1 - exit_reference / position["entry_reference"]
                position["cost_return"], position["funding_return"] = -short_rules["round_trip_stress_cost"], 0.0
                position["net_return"] = position["gross_return"] + position["cost_return"]
                position["mae_return"], position["mfe_return"] = _short_excursions_until_exit(
                    hourly_by_symbol, position["symbol"], position["entry_time"], exit_time, position["entry_reference"])
            else:
                long_rules = config.values["long"]
                entry_fill = float(position["entry_reference"]) * (1 + long_rules["slippage_per_side"])
                ratio = exit_reference * (1 - long_rules["slippage_per_side"]) / entry_fill
                position["gross_return"] = ratio - 1
                position["cost_return"] = -long_rules["taker_fee_per_side"] * (1 + ratio)
                position["funding_return"] = sum(
                    -float(event["funding_rate"]) * minute_open[(position["symbol"], event["funding_time"].replace(second=0, microsecond=0))] / entry_fill
                    for event in funding_by_symbol.get(position["symbol"], [])
                    if position["entry_time"] < event["funding_time"] <= exit_time
                )
                position["net_return"] = position["gross_return"] + position["cost_return"] + position["funding_return"]
                path = minute_by_symbol[position["symbol"]].filter(
                    (pl.col("open_time") >= position["entry_time"]) & (pl.col("open_time") < exit_time)
                )
                position["mae_return"] = min(0.0, float(path.get_column("low").min()) / position["entry_reference"] - 1)
                position["mfe_return"] = max(0.0, float(path.get_column("high").max()) / position["entry_reference"] - 1)
        position["pnl"] = position["notional"] * position["net_return"]
        cash += position["pnl"] - position["_booked_cashflow"]
        accepted.append({key: value for key, value in position.items() if not key.startswith("_")})
        del positions[(position["strategy"], position["symbol"])]

    def mark_through(before) -> None:
        nonlocal last_mark, peak
        if minutes is None or last_mark is None:
            return
        current = last_mark + timedelta(minutes=1)
        while current < before:
            settle_funding(current)
            equity = marked_equity(current)
            peak = max(peak, equity)
            account.append({"event_time": current, "realized_equity": cash, "marked_equity": equity,
                            "cash": cash, "open_units": occupied(), "open_positions": len(positions)})
            current += timedelta(minutes=1)

    for event_time in events:
        mark_through(event_time)
        settle_funding(event_time)
        for position in list(positions.values()):
            if position["exit_time"] <= event_time:
                close(position, position["exit_time"])
        equity = marked_equity(event_time)
        peak = max(peak, equity)
        drawdown = max(0.0, 1 - equity / peak)
        batch_multiplier = drawdown_multiplier(drawdown, sizing)
        base_unit = equity / rules["total_units"]
        arriving = by_entry.get(event_time, [])
        longs = sorted((row for row in arriving if row["strategy"] == "long"), key=lambda row: (row.get("priority_order", row["priority_score"]), row["symbol"]))
        shorts = sorted((row for row in arriving if row["strategy"] == "short"), key=lambda row: (row.get("priority_order", row["priority_score"]), row["signal_time"], row["symbol"]))
        successful_longs, remaining = 0, []
        for row in longs:
            before, key = audit_snapshot(), ("long", row["symbol"])
            if key in positions:
                counts["LONG_DUPLICATE_OPEN"] += 1
                audit_row(row, "LONG_DUPLICATE_OPEN", successful_longs, before)
            else:
                remaining.append(row)
        maximum = config.values["long"]["portfolio"]["max_positions_per_entry_time"]
        admissible, capped = remaining[:maximum], remaining[maximum:]
        for row in capped:
            counts["LONG_TIME_SLOT_CAP"] += 1
            audit_row(row, "LONG_TIME_SLOT_CAP", successful_longs, audit_snapshot())
        long_rules = config.values["long"]["portfolio"]
        requested = long_rules["single_signal_units"] if len(admissible) == 1 else long_rules["two_signal_units_each"]
        for candidate in admissible:
            row, before = dict(candidate, units=requested), audit_snapshot()
            free = rules["total_units"] - occupied()
            actual_units = admitted_long_units(
                requested, free, single_candidate=len(admissible) == 1,
                strict_fragment=bool(long_rules["strict_idle_single_unit_fragment"]),
            )
            fragment = actual_units != requested
            victims: list[dict[str, Any]] = []
            if not fragment and free < requested:
                for victim in sorted((p for p in positions.values() if p["strategy"] == "short"), key=lambda p: (-p["priority_score"], p["signal_time"], p["symbol"])):
                    if free >= actual_units:
                        break
                    victims.append(victim)
                    free += int(victim["units"])
            if not fragment and free < actual_units:
                extensions = sorted((p for p in positions.values()
                    if p["strategy"] == "long" and p.get("extension_applied")
                    and p.get("extension_release_time") is not None and p["extension_release_time"] < event_time),
                    key=lambda p: (p["extension_release_time"], p["entry_time"], p["symbol"]))
                for victim in extensions:
                    if free >= actual_units:
                        break
                    victims.append(victim)
                    free += int(victim["units"])
            if free < actual_units:
                counts["LONG_NO_CAPACITY"] += 1
                audit_row(row, "LONG_NO_CAPACITY", successful_longs, before)
                continue
            for victim in victims:
                if victim["strategy"] == "short":
                    price, reason = _latest_completed_hourly_close(hourly_by_symbol, victim["symbol"], event_time), "LONG_PRIORITY_EVICTION"
                else:
                    price, reason = minute_close[(victim["symbol"], event_time)], "LONG_EXTENSION_EVICTION"
                close(victim, event_time, price, reason)
                counts[reason] += 1
            target = base_unit * actual_units * batch_multiplier
            position = dict(row, units=actual_units, notional=target, base_unit_capital=base_unit, pre_entry_equity=equity, pre_entry_peak=peak,
                            pre_entry_drawdown=drawdown, exposure_multiplier=batch_multiplier, target_notional=target,
                            allocation_mode="SINGLE_UNIT_FRAGMENT" if fragment else "STANDARD")
            register(position)
            if fragment:
                counts["LONG_SINGLE_UNIT_FRAGMENT"] += 1
            audit_row(row, "SELECTED_FRAGMENT" if fragment else "SELECTED", successful_longs, before)
            successful_longs += 1
        for row in shorts:
            before, key = audit_snapshot(), ("short", row["symbol"])
            if key in positions:
                counts["SHORT_DUPLICATE_OPEN"] += 1
                audit_row(row, "SHORT_DUPLICATE_OPEN", successful_longs, before)
                continue
            requested = int(row["units"])
            if occupied() + requested > rules["total_units"] or sum(int(p["units"]) for p in positions.values() if p["strategy"] == "short") + requested > rules["short_unit_cap"]:
                counts["SHORT_SKIP_NO_FUNDS"] += 1
                audit_row(row, "SHORT_SKIP_NO_FUNDS", successful_longs, before)
                continue
            target = base_unit * requested * batch_multiplier
            register(dict(row, notional=target, base_unit_capital=base_unit, pre_entry_equity=equity, pre_entry_peak=peak,
                          pre_entry_drawdown=drawdown, exposure_multiplier=batch_multiplier, target_notional=target))
            audit_row(row, "SELECTED", successful_longs, before)
        account.append({"event_time": event_time, "realized_equity": cash, "marked_equity": marked_equity(event_time),
                        "cash": cash, "open_units": occupied(), "open_positions": len(positions)})
        last_mark = event_time
    if positions:
        raise PortfolioError("window ended with open positions")
    accepted_frame = pl.DataFrame(accepted).sort("entry_time") if accepted else candidate_frame.head(0)
    account_frame = pl.DataFrame(account).sort("event_time") if account else pl.DataFrame(schema={
        "event_time": pl.Datetime("us", "UTC"), "realized_equity": pl.Float64, "marked_equity": pl.Float64,
        "cash": pl.Float64, "open_units": pl.Int64, "open_positions": pl.Int64,
    })
    audit_frame = pl.DataFrame(audit).sort(["entry_time", "strategy", "priority_score", "symbol"]) if audit else pl.DataFrame()
    return accepted_frame, account_frame, counts, audit_frame


def replay_long_standalone(trades: pl.DataFrame, config: StrategyConfig) -> pl.DataFrame:
    """Independent long account with the frozen duplicate/slot/unit sequence."""
    rules = config.values["long"]["portfolio"]
    rows = trades.to_dicts()
    by_entry: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        by_entry.setdefault(row["entry_time"], []).append(row)
    event_times = sorted({row["entry_time"] for row in rows} | {row["exit_time"] for row in rows})
    cash, positions, accepted = 1.0, {}, []
    for time in event_times:
        for key, position in list(positions.items()):
            if position["exit_time"] <= time:
                position["pnl"] = position["notional"] * position["net_return"]
                cash += position["notional"] + position["pnl"]
                accepted.append(position)
                del positions[key]
        arriving = sorted(
            by_entry.get(time, []),
            key=lambda item: (item.get("priority_order", item["priority_score"]), item["symbol"]),
        )
        non_duplicates = [row for row in arriving if row["symbol"] not in positions]
        admissible = non_duplicates[:rules["max_positions_per_entry_time"]]
        requested = rules["single_signal_units"] if len(admissible) == 1 else rules["two_signal_units_each"]
        for row in admissible:
            occupied = sum(item["units"] for item in positions.values())
            if occupied + requested > rules["total_units"]:
                continue
            basis = cash + sum(item["notional"] for item in positions.values())
            position = dict(row, units=requested, notional=basis * requested / rules["total_units"])
            cash -= position["notional"]
            positions[row["symbol"]] = position
    return pl.DataFrame(accepted).sort("entry_time") if accepted else trades.head(0)


def replay_short_standalone(trades: pl.DataFrame, config: StrategyConfig) -> pl.DataFrame:
    """Independent-short UTC-day compounding ledger mandated by the frozen baseline."""
    units = config.values["short"]["portfolio"]["total_daily_units"]
    equity, accepted = 1.0, []
    dated = trades.with_columns(pl.col("entry_time").dt.date().alias("_day"))
    for day, frame in dated.group_by("_day", maintain_order=True):
        start = equity
        for row in frame.to_dicts():
            trade = dict(row, notional=start / units, pnl=start * row["net_return"] / units)
            accepted.append(trade)
        equity *= 1 + sum(row["net_return"] / units for row in frame.to_dicts())
    return pl.DataFrame(accepted).sort("entry_time") if accepted else trades.head(0)
