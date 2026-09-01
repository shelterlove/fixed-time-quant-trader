from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from fixed_time.portfolio import replay_long_standalone, replay_portfolio
from fixed_time.config import load_config

CONFIG = load_config()


def _trade(strategy: str, symbol: str, entry, exit, units: int, score: int, order: int | None = None) -> dict:
    return {"trade_id": f"{strategy}:{symbol}", "strategy": strategy, "symbol": symbol, "signal_time": entry,
            "entry_time": entry, "planned_exit_time": exit, "exit_time": exit, "entry_reference": 100., "exit_reference": 100.,
            "exit_reason": "PLANNED_EXIT", "units": units, "notional": 1., "gross_return": 0., "cost_return": 0.,
            "funding_return": 0., "net_return": 0., "pnl": 0., "mae_return": 0., "mfe_return": 0.,
            "priority_score": score, "priority_order": order if order is not None else score}


def test_long_evicts_short_and_uses_dynamic_units() -> None:
    t = datetime(2022, 1, 1, 10, tzinfo=UTC)
    short = pl.DataFrame([_trade("short", symbol, t, t + timedelta(hours=3), 1, 9) for symbol in ("S1", "S2", "S3")])
    long = pl.DataFrame([
        _trade("long", "E", t, t + timedelta(hours=4), 1, 1),
        _trade("long", "L", t + timedelta(hours=1), t + timedelta(hours=2), 1, 1),
    ])
    hourly = pl.DataFrame([{ "symbol": symbol, "open_time": t, "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1 } for symbol in ("S1", "S2", "S3")])
    trades, account, counts, _ = replay_portfolio(long, short, hourly, CONFIG)
    assert counts["LONG_PRIORITY_EVICTION"] == 2
    assert trades.filter((pl.col("strategy") == "long") & (pl.col("symbol") == "L")).item(0, "units") == 2
    assert account.get_column("open_units").max() == 5


def test_short_eviction_truncates_mae_and_mfe_to_actual_exit() -> None:
    base = datetime(2022, 1, 1, 6, tzinfo=UTC)
    shorts = pl.DataFrame([
        _trade("short", "S1", base, base + timedelta(hours=6), 1, 1),
        _trade("short", "S2", base, base + timedelta(hours=6), 1, 2),
        _trade("short", "S3", base, base + timedelta(hours=6), 1, 3),
    ]).with_columns(pl.lit(-0.9).alias("mae_return"), pl.lit(0.9).alias("mfe_return"))
    longs = pl.DataFrame([
        _trade("long", "EARLY", base + timedelta(hours=1), base + timedelta(hours=5), 1, 1),
        _trade("long", "LATE", base + timedelta(hours=2), base + timedelta(hours=6), 1, 1),
    ])
    bars = []
    for symbol in ("S1", "S2", "S3"):
        bars.append({"symbol": symbol, "open_time": base - timedelta(hours=1), "open": 100.0, "high": 100.0,
                     "low": 100.0, "close": 100.0, "quote_volume": 1.0, "trade_count": 1})
        for offset, values in enumerate([(100.0, 110.0, 90.0, 100.0), (100.0, 120.0, 80.0, 100.0), (100.0, 200.0, 10.0, 100.0)]):
            bars.append({"symbol": symbol, "open_time": base + timedelta(hours=offset), "open": values[0], "high": values[1],
                         "low": values[2], "close": values[3], "quote_volume": 1.0, "trade_count": 1})
    trades, _, _, _ = replay_portfolio(longs, shorts, pl.DataFrame(bars), CONFIG)
    evicted = trades.filter((pl.col("strategy") == "short") & (pl.col("symbol") == "S3")).to_dicts()[0]
    assert evicted["exit_reason"] == "LONG_PRIORITY_EVICTION"
    assert evicted["mae_return"] == pytest.approx(-1 / 6)
    assert evicted["mfe_return"] == pytest.approx(0.25)


def test_long_slot_cap_counts_successful_entries_after_duplicate_skip() -> None:
    t = datetime(2022, 3, 27, 15, tzinfo=UTC)
    existing = _trade("long", "ZIL", t - timedelta(hours=1), t + timedelta(hours=2), 1, 1)
    candidates = [
        _trade("long", "ZIL", t, t + timedelta(hours=1), 1, 1),
        _trade("long", "VET", t, t + timedelta(hours=1), 1, 2),
        _trade("long", "CHZ", t, t + timedelta(hours=1), 1, 3),
    ]
    trades, _, counts, audit = replay_portfolio(pl.DataFrame([existing, *candidates]), pl.DataFrame(schema=pl.DataFrame([existing]).schema), pl.DataFrame(schema={"symbol": pl.String, "open_time": pl.Datetime("us", "UTC"), "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "quote_volume": pl.Float64, "trade_count": pl.Int64}), CONFIG)
    at_time = audit.filter(pl.col("entry_time") == t).sort("priority_score")
    assert at_time.get_column("status").to_list() == ["LONG_DUPLICATE_OPEN", "SELECTED", "SELECTED"]
    assert counts["LONG_DUPLICATE_OPEN"] == 1
    assert counts["LONG_TIME_SLOT_CAP"] == 0
    assert set(trades.filter(pl.col("entry_time") == t).get_column("symbol")) == {"VET", "CHZ"}


def test_long_units_are_assigned_after_duplicate_and_slot_selection() -> None:
    base = datetime(2022, 4, 10, 14, tzinfo=UTC)
    rows = [
        _trade("long", "DOGE", base, base + timedelta(hours=5), 1, 1),
        _trade("long", "DOGE", base + timedelta(hours=1), base + timedelta(hours=5), 1, 1),
        _trade("long", "1000SHIB", base + timedelta(hours=1), base + timedelta(hours=5), 1, 2),
        _trade("long", "APE", base + timedelta(hours=3), base + timedelta(hours=6), 1, 1),
        _trade("long", "KNC", base + timedelta(hours=3), base + timedelta(hours=6), 1, 2),
    ]
    empty_short = pl.DataFrame(schema=pl.DataFrame([rows[0]]).schema)
    hourly = pl.DataFrame(schema={"symbol": pl.String, "open_time": pl.Datetime("us", "UTC"), "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "quote_volume": pl.Float64, "trade_count": pl.Int64})
    trades, _, counts, audit = replay_portfolio(pl.DataFrame(rows), empty_short, hourly, CONFIG)
    selected = {row["symbol"]: row["units"] for row in trades.to_dicts()}
    assert selected == {"DOGE": 2, "1000SHIB": 2, "APE": 1}
    at_15 = audit.filter(pl.col("entry_time") == base + timedelta(hours=1)).sort("priority_score")
    at_17 = audit.filter(pl.col("entry_time") == base + timedelta(hours=3)).sort("priority_score")
    assert at_15.select("symbol", "status", "requested_units").rows() == [("DOGE", "LONG_DUPLICATE_OPEN", 1), ("1000SHIB", "SELECTED", 2)]
    assert at_17.select("symbol", "status", "requested_units").rows() == [("APE", "SELECTED", 1), ("KNC", "LONG_NO_CAPACITY", 1)]
    assert counts["LONG_NO_CAPACITY"] == 1


def test_portfolio_preserves_frozen_factor_tie_break_order() -> None:
    entry = datetime(2022, 5, 1, 14, tzinfo=UTC)
    rows = [
        _trade("long", "A", entry, entry + timedelta(hours=1), 1, 10, 3),
        _trade("long", "B", entry, entry + timedelta(hours=1), 1, 10, 2),
        _trade("long", "C", entry, entry + timedelta(hours=1), 1, 10, 1),
    ]
    empty = pl.DataFrame(schema=pl.DataFrame([rows[0]]).schema)
    hourly = pl.DataFrame(schema={"symbol": pl.String, "open_time": pl.Datetime("us", "UTC"), "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "quote_volume": pl.Float64, "trade_count": pl.Int64})
    trades, _, _, audit = replay_portfolio(pl.DataFrame(rows), empty, hourly, CONFIG)
    assert set(trades.get_column("symbol")) == {"B", "C"}
    assert audit.filter(pl.col("symbol") == "A").item(0, "status") == "LONG_TIME_SLOT_CAP"


def test_long_standalone_assigns_two_units_to_one_admissible_signal() -> None:
    entry = datetime(2022, 5, 1, 14, tzinfo=UTC)
    trade = _trade("long", "A", entry, entry + timedelta(hours=1), 1, 1)
    accepted = replay_long_standalone(pl.DataFrame([trade]), CONFIG)
    assert accepted.item(0, "units") == 2
    assert accepted.item(0, "notional") == 0.4
