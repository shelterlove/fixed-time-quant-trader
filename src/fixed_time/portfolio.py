from __future__ import annotations

from datetime import timedelta
from typing import Any

import polars as pl

from .config import StrategyConfig


class PortfolioError(ValueError):
    pass


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
        entry_reference / float(path.get_column("high").max()) - 1,
        entry_reference / float(path.get_column("low").min()) - 1,
    )


def replay_portfolio(long_trades: pl.DataFrame, short_trades: pl.DataFrame, hourly: pl.DataFrame, config: StrategyConfig) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, int], pl.DataFrame]:
    """The sole shared-five-unit account event loop for LONG_PRIORITY_SKIP."""
    rules, short_rules = config.values["portfolio"], config.values["short"]
    candidate_frame = pl.concat([long_trades, short_trades], how="vertical_relaxed")
    candidates = candidate_frame.to_dicts()
    hourly_by_symbol = {
        key[0] if isinstance(key, tuple) else key: group.sort("open_time")
        for key, group in hourly.group_by("symbol", maintain_order=True)
    }
    events = sorted({row["entry_time"] for row in candidates} | {row["exit_time"] for row in candidates})
    by_entry: dict[Any, list[dict[str, Any]]] = {}
    for row in candidates:
        by_entry.setdefault(row["entry_time"], []).append(dict(row))
    cash, positions, accepted, account, audit = 1.0, {}, [], [], []
    counts = {name: 0 for name in ("LONG_NO_CAPACITY", "LONG_TIME_SLOT_CAP", "SHORT_SKIP_NO_FUNDS", "LONG_DUPLICATE_OPEN", "SHORT_DUPLICATE_OPEN", "LONG_PRIORITY_EVICTION", "waiting", "cancelled", "expired")}

    def audit_snapshot() -> tuple[int, str]:
        occupied = sum(int(position["units"]) for position in positions.values())
        return occupied, ";".join(
            f"{position['strategy']}:{position['symbol']}:{position['units']}"
            for position in sorted(positions.values(), key=lambda item: (item["strategy"], item["symbol"]))
        )

    def audit_row(row: dict[str, Any], status: str, successful_longs: int, snapshot: tuple[int, str]) -> None:
        occupied, position_text = snapshot
        audit.append({
            "strategy": row["strategy"], "symbol": row["symbol"], "signal_time": row["signal_time"],
            "entry_time": row["entry_time"], "planned_exit_time": row["planned_exit_time"],
            "priority_score": row["priority_score"], "requested_units": row["units"], "status": status,
            "open_units_before": occupied, "free_units_before": rules["total_units"] - occupied,
            "open_positions_before": position_text,
            "successful_long_entries_before": successful_longs if row["strategy"] == "long" else None,
        })

    def close(position: dict[str, Any], exit_time, exit_reference: float | None = None, reason: str | None = None) -> None:
        nonlocal cash
        if exit_reference is not None:
            position["exit_reference"] = exit_reference
            position["exit_reason"] = reason
            position["exit_time"] = exit_time
            if position["strategy"] != "short":
                raise PortfolioError("only short positions may be evicted")
            position["gross_return"] = 1 - exit_reference / position["entry_reference"]
            position["cost_return"] = -short_rules["round_trip_stress_cost"]
            position["funding_return"] = 0.0
            position["net_return"] = position["gross_return"] + position["cost_return"]
            position["mae_return"], position["mfe_return"] = _short_excursions_until_exit(
                hourly_by_symbol, position["symbol"], position["entry_time"], exit_time,
                position["entry_reference"],
            )
        position["pnl"] = position["notional"] * position["net_return"]
        cash += position["notional"] + position["pnl"]
        accepted.append(position.copy())
        del positions[(position["strategy"], position["symbol"])]

    for event_time in events:
        # 1. all normal exits at this UTC instant
        for position in list(positions.values()):
            if position["exit_time"] <= event_time:
                close(position, position["exit_time"])
        arriving = by_entry.get(event_time, [])
        longs = sorted(
            (row for row in arriving if row["strategy"] == "long"),
            key=lambda row: (row.get("priority_order", row["priority_score"]), row["symbol"]),
        )
        shorts = sorted(
            (row for row in arriving if row["strategy"] == "short"),
            key=lambda row: (row.get("priority_order", row["priority_score"]), row["signal_time"], row["symbol"]),
        )
        # 2. short eviction is part of each long admission, after normal exits.
        successful_longs = 0
        remaining_longs: list[dict[str, Any]] = []
        for row in longs:
            before = audit_snapshot()
            key = ("long", row["symbol"])
            if key in positions:
                counts["LONG_DUPLICATE_OPEN"] += 1
                audit_row(row, "LONG_DUPLICATE_OPEN", successful_longs, before)
                continue
            remaining_longs.append(row)
        maximum = config.values["long"]["portfolio"]["max_positions_per_entry_time"]
        admissible, capped = remaining_longs[:maximum], remaining_longs[maximum:]
        for row in capped:
            counts["LONG_TIME_SLOT_CAP"] += 1
            audit_row(row, "LONG_TIME_SLOT_CAP", successful_longs, audit_snapshot())
        requested_units = (
            config.values["long"]["portfolio"]["single_signal_units"]
            if len(admissible) == 1
            else config.values["long"]["portfolio"]["two_signal_units_each"]
        )
        for candidate in admissible:
            row = dict(candidate, units=requested_units)
            before = audit_snapshot()
            key = ("long", row["symbol"])
            requested = requested_units
            free_units = rules["total_units"] - sum(int(position["units"]) for position in positions.values())
            if free_units < requested:
                shorts_open = sorted((p for p in positions.values() if p["strategy"] == "short"), key=lambda p: (-p["priority_score"], p["signal_time"], p["symbol"]))
                for position in shorts_open:
                    if free_units >= requested:
                        break
                    price = _latest_completed_hourly_close(hourly_by_symbol, position["symbol"], event_time)
                    close(position, event_time, price, "LONG_PRIORITY_EVICTION")
                    counts["LONG_PRIORITY_EVICTION"] += 1
                    free_units = rules["total_units"] - sum(int(current["units"]) for current in positions.values())
            granted = min(requested, rules["total_units"] - sum(int(position["units"]) for position in positions.values()))
            if granted <= 0:
                counts["LONG_NO_CAPACITY"] += 1
                audit_row(row, "LONG_NO_CAPACITY", successful_longs, before)
                continue
            free_units = rules["total_units"] - sum(int(position["units"]) for position in positions.values())
            position = dict(row, units=granted, notional=granted * cash / free_units)
            positions[key] = position
            cash -= position["notional"]
            audit_row(row, "SELECTED", successful_longs, before)
            successful_longs += 1
        # 3. shorts have no waiting list and are admitted only after longs.
        for row in shorts:
            before = audit_snapshot()
            key = ("short", row["symbol"])
            if key in positions:
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
            positions[key] = position
            cash -= position["notional"]
            audit_row(row, "SELECTED", successful_longs, before)
        account.append({"event_time": event_time, "realized_equity": cash + sum(p["notional"] for p in positions.values()), "cash": cash,
                        "open_units": sum(int(p["units"]) for p in positions.values()), "open_positions": len(positions)})
    if positions:
        raise PortfolioError("window ended with open positions")
    accepted_frame = pl.DataFrame(accepted).sort("entry_time") if accepted else candidate_frame.head(0)
    account_frame = pl.DataFrame(account).sort("event_time") if account else pl.DataFrame(schema={
        "event_time": pl.Datetime("us", "UTC"), "realized_equity": pl.Float64, "cash": pl.Float64,
        "open_units": pl.Int64, "open_positions": pl.Int64,
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
            granted = min(requested, rules["total_units"] - occupied)
            if granted <= 0:
                continue
            basis = cash + sum(item["notional"] for item in positions.values())
            position = dict(row, units=granted, notional=basis * granted / rules["total_units"])
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
