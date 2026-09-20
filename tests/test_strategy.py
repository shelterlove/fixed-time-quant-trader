from datetime import UTC, datetime, timedelta
from decimal import Decimal

import polars as pl
import pytest

from fixed_time.config import load_strategy
from fixed_time.strategy import admissions, candidates, planned_exit, requested_total


def test_frozen_strategy_loads():
    assert load_strategy(".").version == "SPEC-20260917-r2"


def test_batch_requests_match_a50_contract():
    b, remaining = Decimal("50"), Decimal("50")
    assert requested_total("long", 14, b, remaining, False) == Decimal("25")
    assert requested_total("long", 15, b, remaining, False) == Decimal(100) / Decimal(3)
    assert requested_total("long", 15, b, remaining, True) == Decimal("50")
    assert requested_total("short", 1, b, remaining, True) == Decimal("25")
    assert requested_total("short", 2, b, remaining, True) == Decimal("50")


def test_batch_is_equal_and_capped_by_equity():
    config = load_strategy(".")
    now = datetime(2026, 9, 19, 14, tzinfo=UTC)
    rows = [{"strategy": "long", "decision_time": now, "symbol": symbol} for symbol in ("A", "B")]
    result = admissions(rows, [], Decimal("100"), Decimal("20"), config)
    assert len(result) == 2
    assert result[0].target_notional == result[1].target_notional
    assert sum(x.target_notional for x in result) < Decimal("20")  # entry cost is reserved


def test_new_short_exit_uses_new_york_dst():
    summer = planned_exit(datetime(2026, 7, 1, 0, tzinfo=UTC), "NEW_SHORT")
    winter = planned_exit(datetime(2026, 1, 1, 0, tzinfo=UTC), "NEW_SHORT")
    assert summer.hour == 14
    assert winter.hour == 15


def test_long_and_original_exits():
    assert planned_exit(datetime(2026, 9, 19, 17, tzinfo=UTC), "MAIN") == datetime(2026, 9, 20, 4, tzinfo=UTC)
    assert planned_exit(datetime(2026, 9, 19, 6, tzinfo=UTC), "ORIGINAL_06") == datetime(2026, 9, 19, 20, tzinfo=UTC)


def test_long_signal_is_rebuilt_from_hourly_market_snapshot():
    config = load_strategy(".")
    decision = datetime(2026, 9, 19, 14, tzinfo=UTC)
    rows = []
    for index in range(100):
        symbol = f"S{index:03}USDT"
        growth = .001 + index * .00002
        for t in range(81):
            close = 100 * (1 + growth) ** t
            rows.append({"symbol": symbol, "open_time": decision - timedelta(hours=81-t),
                         "open": close, "high": close * 1.002, "low": close * .998, "close": close,
                         "quote_volume": float((index + 1) * 1000), "trade_count": 100})
    selected = candidates(pl.DataFrame(rows), decision, config)
    assert any(row["symbol"] == "S099USDT" and "MAIN" in row["source"] for row in selected)
    future = dict(rows[-1],open_time=decision,close=1e9,high=1e9,low=1e9,open=1e9)
    assert candidates(pl.DataFrame(rows+[future]),decision,config) == selected
    with pytest.raises(ValueError,match="duplicate"):
        candidates(pl.DataFrame(rows+[rows[-1]]),decision,config)


def test_new_short_requires_strong_coin_inside_weak_market():
    config = load_strategy(".")
    decision = datetime(2026, 9, 20, 0, tzinfo=UTC)
    rows = []
    for index in range(100):
        symbol = f"W{index:03}USDT"
        for t in range(81):
            if index == 99:
                close = 100 if t < 77 else [110, 130, 160, 200][t - 77]
                volume = 20000.0 if t == 80 else 100.0
                spread = .15 if t >= 77 else .002
            else:
                close = 100 * .998 ** t
                volume = 1000.0 + index
                spread = .002
            rows.append({"symbol": symbol, "open_time": decision - timedelta(hours=81-t),
                         "open": close, "high": close * (1 + spread), "low": close * (1 - spread), "close": close,
                         "quote_volume": volume, "trade_count": 100})
    selected = candidates(pl.DataFrame(rows), decision, config)
    assert any(row["symbol"] == "W099USDT" and row["source"] == "NEW_SHORT" for row in selected)
