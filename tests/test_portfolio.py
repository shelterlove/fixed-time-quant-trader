from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from fixed_time.portfolio import drawdown_multiplier, replay_long_standalone, replay_portfolio
from fixed_time.config import load_config

CONFIG = load_config()


def _trade(strategy: str, symbol: str, entry, exit, units: int, score: int, order: int | None = None) -> dict:
    return {"trade_id": f"{strategy}:{symbol}", "strategy": strategy, "symbol": symbol, "signal_time": entry,
            "entry_time": entry, "planned_exit_time": exit, "exit_time": exit, "entry_reference": 100., "exit_reference": 100.,
            "exit_reason": "PLANNED_EXIT", "units": units, "notional": 1., "gross_return": 0., "cost_return": 0.,
            "funding_return": 0., "net_return": 0., "pnl": 0., "mae_return": 0., "mfe_return": 0.,
            "priority_score": score, "priority_order": order if order is not None else score,
            "extension_applied": False, "extension_release_time": None, "extension_deadline": None}


def test_single_long_evicts_two_worst_shorts_for_its_two_units() -> None:
    t = datetime(2022, 1, 1, 10, tzinfo=UTC)
    short = pl.DataFrame([_trade("short", symbol, t, t + timedelta(hours=3), 1, 9) for symbol in ("S1", "S2", "S3")])
    long = pl.DataFrame([_trade("long", "L", t + timedelta(hours=1), t + timedelta(hours=2), 1, 1)])
    hourly = pl.DataFrame([{ "symbol": symbol, "open_time": t, "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1 } for symbol in ("S1", "S2", "S3")])
    trades, account, counts, _ = replay_portfolio(long, short, hourly, CONFIG)
    assert counts["LONG_PRIORITY_EVICTION"] == 2
    assert trades.filter((pl.col("strategy") == "long") & (pl.col("symbol") == "L")).item(0, "units") == 2
    assert account.get_column("open_units").max() == 3


def test_strict_d3_uses_an_existing_idle_unit() -> None:
    t = datetime(2022, 1, 1, 10, tzinfo=UTC)
    long = pl.DataFrame([
        _trade("long", "HELD", t, t + timedelta(hours=3), 1, 1),
        _trade("long", "NEW", t + timedelta(hours=1), t + timedelta(hours=2), 1, 1),
    ])
    empty = pl.DataFrame(schema=long.schema)
    hourly = pl.DataFrame(schema={"symbol": pl.String, "open_time": pl.Datetime("us", "UTC"), "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "quote_volume": pl.Float64, "trade_count": pl.Int64})
    trades, _, counts, _ = replay_portfolio(long, empty, hourly, CONFIG)
    assert trades.filter(pl.col("symbol") == "NEW").item(0, "units") == 1
    assert counts["LONG_PRIORITY_EVICTION"] == 0
    assert counts["LONG_SINGLE_UNIT_FRAGMENT"] == 1


def test_new_long_evicts_short_then_released_extension() -> None:
    start = datetime(2022, 1, 1, 10, tzinfo=UTC)
    held = dict(_trade("long", "HELD", start, start+timedelta(hours=10), 1, 1),
                extension_applied=True, extension_release_time=start+timedelta(hours=4))
    new = dict(_trade("long", "NEW", start+timedelta(hours=5), start+timedelta(hours=6), 1, 1),
               extension_applied=False, extension_release_time=None)
    short = _trade("short", "SHORT", start, start+timedelta(hours=8), 1, 9)
    minute_rows = []
    for symbol in ("HELD", "NEW", "SHORT"):
        for offset in range(600):
            minute_rows.append({"symbol": symbol, "open_time": start+timedelta(minutes=offset), "open": 100.,
                                "high": 100., "low": 100., "close": 100.})
    hourly = pl.DataFrame([{"symbol": "SHORT", "open_time": start+timedelta(hours=offset), "open": 100.,
                            "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1}
                           for offset in range(8)])
    funding = pl.DataFrame(schema={"symbol": pl.String, "funding_time": pl.Datetime("us", "UTC"), "funding_rate": pl.Float64})
    trades, _, counts, _ = replay_portfolio(pl.DataFrame([held, new]), pl.DataFrame([short]), hourly, CONFIG,
                                             pl.DataFrame(minute_rows), funding)
    evicted = trades.filter(pl.col("symbol") == "HELD").to_dicts()[0]
    assert evicted["exit_reason"] == "LONG_EXTENSION_EVICTION"
    assert counts["LONG_PRIORITY_EVICTION"] == 1
    assert counts["LONG_EXTENSION_EVICTION"] == 1


@pytest.mark.parametrize(("drawdown", "expected"), [
    (.24999, 1.0), (.25, 1.05), (.30, 1.10), (.35, 1.15),
    (.40, 1.20), (.45, 1.25), (.50, 1.30), (.90, 1.30),
])
def test_backtest_drawdown_sizing_uses_inclusive_tiers_and_fifty_percent_cap(drawdown: float, expected: float) -> None:
    assert drawdown_multiplier(drawdown, CONFIG.values["portfolio"]["drawdown_sizing"]) == pytest.approx(expected)


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
    assert evicted["mae_return"] == pytest.approx(-.1)
    assert evicted["mfe_return"] == pytest.approx(.1)


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
    assert at_time.get_column("status").to_list() == ["LONG_DUPLICATE_OPEN", "SELECTED", "LONG_NO_CAPACITY"]
    assert counts["LONG_DUPLICATE_OPEN"] == 1
    assert counts["LONG_TIME_SLOT_CAP"] == 0
    assert set(trades.filter(pl.col("entry_time") == t).get_column("symbol")) == {"VET"}


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
    assert selected == {"DOGE": 2, "1000SHIB": 1}
    at_15 = audit.filter(pl.col("entry_time") == base + timedelta(hours=1)).sort("priority_score")
    at_17 = audit.filter(pl.col("entry_time") == base + timedelta(hours=3)).sort("priority_score")
    assert at_15.select("symbol", "status", "requested_units").rows() == [("DOGE", "LONG_DUPLICATE_OPEN", 1), ("1000SHIB", "SELECTED_FRAGMENT", 2)]
    assert at_17.select("symbol", "status", "requested_units").rows() == [("APE", "LONG_NO_CAPACITY", 1), ("KNC", "LONG_NO_CAPACITY", 1)]
    assert counts["LONG_NO_CAPACITY"] == 2


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


def test_completed_minute_drawdown_sets_the_next_batch_multiplier() -> None:
    entry = datetime(2022, 5, 2, 14, tzinfo=UTC)
    long = _trade("long", "L", entry, entry + timedelta(minutes=2), 1, 1)
    short = _trade("short", "S", entry + timedelta(minutes=1), entry + timedelta(minutes=2), 1, 1)
    minutes = pl.DataFrame([
        {"symbol": "L", "open_time": entry, "close": 70.0},
        {"symbol": "S", "open_time": entry, "close": 100.0},
    ])
    empty_hourly = pl.DataFrame(schema={"symbol": pl.String, "open_time": pl.Datetime("us", "UTC"), "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "quote_volume": pl.Float64, "trade_count": pl.Int64})
    trades, _, _, _ = replay_portfolio(pl.DataFrame([long]), pl.DataFrame([short]), empty_hourly, CONFIG, minutes)
    opened_short = trades.filter(pl.col("symbol") == "S").to_dicts()[0]
    assert opened_short["pre_entry_drawdown"] == pytest.approx(.2)
    assert opened_short["exposure_multiplier"] == pytest.approx(1.0)
    assert opened_short["target_notional"] == pytest.approx(.8 / 3)


def test_short_mark_to_market_uses_linear_usd_m_pnl() -> None:
    entry = datetime(2022, 5, 2, 14, tzinfo=UTC)
    short = _trade("short", "S", entry, entry + timedelta(minutes=2), 1, 1)
    minutes = pl.DataFrame([{"symbol": "S", "open_time": entry, "close": 80.0}])
    hourly = pl.DataFrame(schema={"symbol": pl.String, "open_time": pl.Datetime("us", "UTC"), "open": pl.Float64,
                                  "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                                  "quote_volume": pl.Float64, "trade_count": pl.Int64})
    _, account, _, _ = replay_portfolio(pl.DataFrame(schema=pl.DataFrame([short]).schema), pl.DataFrame([short]), hourly, CONFIG, minutes)
    marked = account.filter(pl.col("event_time") == entry + timedelta(minutes=1)).item(0, "marked_equity")
    assert marked == pytest.approx(1 + 1 / 3 * .2)


def test_long_standalone_assigns_two_units_to_one_admissible_signal() -> None:
    entry = datetime(2022, 5, 1, 14, tzinfo=UTC)
    trade = _trade("long", "A", entry, entry + timedelta(hours=1), 1, 1)
    accepted = replay_long_standalone(pl.DataFrame([trade]), CONFIG)
    assert accepted.item(0, "units") == 2
    assert accepted.item(0, "notional") == pytest.approx(2 / 3)


def test_long_standalone_never_downsizes_a_two_unit_admission() -> None:
    entry = datetime(2022, 5, 3, 14, tzinfo=UTC)
    first = _trade("long", "A", entry, entry + timedelta(hours=3), 1, 1)
    second = _trade("long", "B", entry + timedelta(hours=1), entry + timedelta(hours=2), 1, 1)
    accepted = replay_long_standalone(pl.DataFrame([first, second]), CONFIG)
    assert accepted.get_column("symbol").to_list() == ["A"]
