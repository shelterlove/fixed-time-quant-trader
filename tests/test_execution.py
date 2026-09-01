from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from fixed_time.execution import execute_long, execute_short
from fixed_time.storage import DataError
from fixed_time.config import load_config

CONFIG = load_config()


def _long_signal() -> pl.DataFrame:
    return pl.DataFrame([{ "trade_id": "long:A", "symbol": "A", "decision_time": datetime(2022, 1, 1, 14, tzinfo=UTC),
        "entry_time": datetime(2022, 1, 1, 14, 1, tzinfo=UTC), "planned_exit_time": datetime(2022, 1, 1, 14, 3, tzinfo=UTC), "requested_units": 1, "priority_score": 1 }])


def test_long_hard_stop_gap_has_priority() -> None:
    start = datetime(2022, 1, 1, 14, tzinfo=UTC)
    minutes = pl.DataFrame([
        {"symbol": "A", "open_time": start, "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": start + timedelta(minutes=1), "open": 60., "high": 150., "low": 60., "close": 80., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": start + timedelta(minutes=2), "open": 80., "high": 80., "low": 80., "close": 80., "quote_volume": 1., "trade_count": 1},
    ])
    funding = pl.DataFrame(schema={"symbol": pl.String, "funding_time": pl.Datetime("us", "UTC"), "funding_rate": pl.Float64})
    trade = execute_long(_long_signal(), minutes, funding, CONFIG).to_dicts()[0]
    assert trade["exit_reason"] == "HARD_STOP"
    assert trade["exit_reference"] == 60.0


def test_long_funding_uses_original_event_boundary_and_settlement_minute_open() -> None:
    start = datetime(2022, 1, 1, 14, tzinfo=UTC)
    minutes = pl.DataFrame([
        {"symbol": "A", "open_time": start, "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": start + timedelta(minutes=1), "open": 101., "high": 101., "low": 101., "close": 101., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": start + timedelta(minutes=2), "open": 120., "high": 120., "low": 120., "close": 120., "quote_volume": 1., "trade_count": 1},
    ])
    funding = pl.DataFrame([{"symbol": "A", "funding_time": start + timedelta(minutes=2, milliseconds=28), "funding_rate": 0.0001}])
    trade = execute_long(_long_signal(), minutes, funding, CONFIG).to_dicts()[0]
    assert trade["funding_return"] == pytest.approx(-0.0001 * 120 / (100 * 1.001))


def test_long_entry_reference_uses_completed_entry_minute_close_not_visible_hourly_close() -> None:
    signal = _long_signal().with_columns(pl.lit(77.0).alias("close"))
    start = datetime(2022, 1, 1, 14, tzinfo=UTC)
    minutes = pl.DataFrame([
        {"symbol": "A", "open_time": start, "open": 100., "high": 100., "low": 100., "close": 101., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": start + timedelta(minutes=1), "open": 101., "high": 101., "low": 101., "close": 101., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": start + timedelta(minutes=2), "open": 101., "high": 101., "low": 101., "close": 101., "quote_volume": 1., "trade_count": 1},
    ])
    funding = pl.DataFrame(schema={"symbol": pl.String, "funding_time": pl.Datetime("us", "UTC"), "funding_rate": pl.Float64})
    trade = execute_long(signal, minutes, funding, CONFIG).to_dicts()[0]
    assert trade["entry_reference"] == 101.0
    assert trade["visible_hourly_close"] == 77.0


def test_short_hourly_high_stop_gap() -> None:
    entry = datetime(2022, 1, 1, 6, tzinfo=UTC)
    signal = pl.DataFrame([{ "trade_id": "short:A", "symbol": "A", "decision_time": entry, "entry_time": entry,
        "planned_exit_time": entry + timedelta(hours=2), "priority_score": 1 }])
    rows = []
    for hour, values in enumerate([(100., 100., 100., 100.), (140., 140., 130., 135.), (135., 135., 130., 132.)]):
        rows.append({"symbol": "A", "open_time": entry - timedelta(hours=1) + timedelta(hours=hour), "open": values[0], "high": values[1], "low": values[2], "close": values[3], "quote_volume": 1., "trade_count": 1})
    trade = execute_short(signal, pl.DataFrame(rows), CONFIG).to_dicts()[0]
    assert trade["exit_reason"] == "HARD_STOP"
    assert trade["exit_reference"] == 140.0


def test_short_hard_stop_excursions_exclude_post_exit_hours() -> None:
    entry = datetime(2022, 1, 1, 6, tzinfo=UTC)
    signal = pl.DataFrame([{ "trade_id": "short:A", "symbol": "A", "decision_time": entry, "entry_time": entry,
        "planned_exit_time": entry + timedelta(hours=3), "priority_score": 1 }])
    hourly = pl.DataFrame([
        {"symbol": "A", "open_time": entry - timedelta(hours=1), "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": entry, "open": 100., "high": 140., "low": 95., "close": 130., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": entry + timedelta(hours=1), "open": 130., "high": 500., "low": 1., "close": 200., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": entry + timedelta(hours=2), "open": 200., "high": 500., "low": 1., "close": 200., "quote_volume": 1., "trade_count": 1},
    ])
    trade = execute_short(signal, hourly, CONFIG).to_dicts()[0]
    assert trade["exit_time"] == entry + timedelta(hours=1)
    assert trade["mae_return"] == pytest.approx(100 / 140 - 1)
    assert trade["mfe_return"] == pytest.approx(100 / 95 - 1)


def test_long_p90_protection_uses_completed_shadow_history_only() -> None:
    base = datetime(2022, 1, 3, tzinfo=UTC)
    signals, minutes = [], []
    for number in range(101):
        entry = base + timedelta(minutes=number * 4 + 1)
        symbol = f"S{number}"
        signals.append({"trade_id": f"long:{symbol}", "symbol": symbol, "decision_time": entry - timedelta(minutes=1),
                        "entry_time": entry, "planned_exit_time": entry + timedelta(minutes=3), "requested_units": 1, "priority_score": 1})
        for offset, values in enumerate([(100., 100., 100., 100.), (100., 130., 100., 120.), (115., 116., 114., 115.), (115., 115., 110., 110.)]):
            minutes.append({"symbol": symbol, "open_time": entry - timedelta(minutes=1) + timedelta(minutes=offset), "open": values[0], "high": values[1], "low": values[2], "close": values[3], "quote_volume": 1., "trade_count": 1})
    funding = pl.DataFrame(schema={"symbol": pl.String, "funding_time": pl.Datetime("us", "UTC"), "funding_rate": pl.Float64})
    final = execute_long(pl.DataFrame(signals), pl.DataFrame(minutes), funding, CONFIG).sort("entry_time").tail(1).to_dicts()[0]
    assert final["exit_reason"] == "PROTECTION"
    assert final["exit_reference"] == 110.0


def test_long_p90_seeds_pre_window_base_shadows_without_emitting_them() -> None:
    entry = datetime(2022, 1, 2, 14, 1, tzinfo=UTC)
    history_entry = entry - timedelta(minutes=5)
    signals = pl.DataFrame([
        {"trade_id": "long:H", "symbol": "H", "decision_time": history_entry - timedelta(minutes=1),
         "entry_time": history_entry, "planned_exit_time": history_entry + timedelta(minutes=3), "requested_units": 1, "priority_score": 1},
        {"trade_id": "long:C", "symbol": "C", "decision_time": entry - timedelta(minutes=1),
         "entry_time": entry, "planned_exit_time": entry + timedelta(minutes=3), "requested_units": 1, "priority_score": 1},
    ])
    minutes = []
    for offset in range(4):
        minutes.append({"symbol": "H", "open_time": history_entry - timedelta(minutes=1) + timedelta(minutes=offset),
                        "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1})
    for offset, values in enumerate([(100., 100., 100., 100.), (100., 130., 100., 120.), (115., 116., 110., 115.), (115., 115., 115., 115.)]):
        minutes.append({"symbol": "C", "open_time": entry - timedelta(minutes=1) + timedelta(minutes=offset),
                        "open": values[0], "high": values[1], "low": values[2], "close": values[3], "quote_volume": 1., "trade_count": 1})
    history = pl.DataFrame([
        {"shadow_exit_time": entry - timedelta(days=365), "shadow_activated": True, "shadow_max_retrace": .1}
        for _ in range(99)
    ] + [
        {"shadow_exit_time": entry + timedelta(minutes=1), "shadow_activated": True, "shadow_max_retrace": .9},
        {"shadow_exit_time": entry - timedelta(days=365, minutes=1), "shadow_activated": True, "shadow_max_retrace": .9},
    ])
    funding = pl.DataFrame(schema={"symbol": pl.String, "funding_time": pl.Datetime("us", "UTC"), "funding_rate": pl.Float64})
    trades = execute_long(signals, pl.DataFrame(minutes), funding, CONFIG, history, entry).to_dicts()
    assert [trade["symbol"] for trade in trades] == ["C"]
    assert trades[0]["protection_history_count"] == 100
    assert trades[0]["protection_allowed_retrace"] == pytest.approx(.1)
    assert trades[0]["exit_reason"] == "PROTECTION"


def test_missing_minute_path_fails_without_mutating_candidates() -> None:
    signal = _long_signal()
    one_bar = pl.DataFrame([{ "symbol": "A", "open_time": datetime(2022, 1, 1, 14, tzinfo=UTC), "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1 }])
    funding = pl.DataFrame(schema={"symbol": pl.String, "funding_time": pl.Datetime("us", "UTC"), "funding_rate": pl.Float64})
    with pytest.raises(DataError):
        execute_long(signal, one_bar, funding, CONFIG)
    assert signal.height == 1


def test_long_terminal_path_forces_exit_at_last_completed_minute_close() -> None:
    signal = _long_signal().with_columns(pl.col("planned_exit_time") + pl.duration(minutes=2))
    start = datetime(2022, 1, 1, 14, tzinfo=UTC)
    minutes = pl.DataFrame([
        {"symbol": "A", "open_time": start, "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": start + timedelta(minutes=1), "open": 100., "high": 105., "low": 100., "close": 104., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": start + timedelta(minutes=2), "open": 104., "high": 106., "low": 103., "close": 105., "quote_volume": 1., "trade_count": 1},
    ])
    funding = pl.DataFrame(schema={"symbol": pl.String, "funding_time": pl.Datetime("us", "UTC"), "funding_rate": pl.Float64})
    trade = execute_long(signal, minutes, funding, CONFIG).to_dicts()[0]
    assert (trade["exit_time"], trade["exit_reference"], trade["exit_reason"]) == (start + timedelta(minutes=3), 105., "DATA_PATH_FORCED_EXIT")


def test_internal_minute_gap_remains_an_error() -> None:
    signal = _long_signal().with_columns(pl.col("planned_exit_time") + pl.duration(minutes=2))
    start = datetime(2022, 1, 1, 14, tzinfo=UTC)
    minutes = pl.DataFrame([
        {"symbol": "A", "open_time": start, "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": start + timedelta(minutes=1), "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": start + timedelta(minutes=3), "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1},
    ])
    funding = pl.DataFrame(schema={"symbol": pl.String, "funding_time": pl.Datetime("us", "UTC"), "funding_rate": pl.Float64})
    with pytest.raises(DataError, match="non-terminal path gap"):
        execute_long(signal, minutes, funding, CONFIG)


def test_short_terminal_path_forces_exit_at_last_completed_hour_close() -> None:
    entry = datetime(2022, 1, 1, 6, tzinfo=UTC)
    signal = pl.DataFrame([{ "trade_id": "short:A", "symbol": "A", "decision_time": entry, "entry_time": entry,
        "planned_exit_time": entry + timedelta(hours=3), "priority_score": 1 }])
    hourly = pl.DataFrame([
        {"symbol": "A", "open_time": entry - timedelta(hours=1), "open": 100., "high": 100., "low": 100., "close": 100., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": entry, "open": 100., "high": 100., "low": 90., "close": 95., "quote_volume": 1., "trade_count": 1},
        {"symbol": "A", "open_time": entry + timedelta(hours=1), "open": 95., "high": 96., "low": 80., "close": 85., "quote_volume": 1., "trade_count": 1},
    ])
    trade = execute_short(signal, hourly, CONFIG).to_dicts()[0]
    assert (trade["exit_time"], trade["exit_reference"], trade["exit_reason"]) == (entry + timedelta(hours=2), 85., "DATA_PATH_FORCED_EXIT")
