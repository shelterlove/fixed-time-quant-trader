from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from fixed_time.features import build_features
from fixed_time.signals import SignalError, enforce_research_subwindow_exit_boundary, long_signals, select_short_hour, short_signals
from fixed_time.config import load_config

CONFIG = load_config()


def _hourly(symbols: list[str], bars: int = 45) -> pl.DataFrame:
    rows = []
    start = datetime(2022, 1, 1, tzinfo=UTC)
    for symbol in symbols:
        for index in range(bars):
            rows.append({"symbol": symbol, "open_time": start + timedelta(hours=index), "open": 100.0,
                         "high": 102.0, "low": 99.0, "close": 100.0 + index,
                         "quote_volume": 1_000.0 + index * 10, "trade_count": 1})
    return pl.DataFrame(rows)


def test_features_are_t_minus_one_and_stably_ranked() -> None:
    features = build_features(_hourly(["BBBUSDT", "AAAUSDT"]), CONFIG)
    assert all(source == decision - timedelta(hours=1) for source, decision in zip(features["source_bar_time"], features["decision_time"]))
    at_time = features.filter(pl.col("decision_time") == features.get_column("decision_time").min()).sort("r1_rank")
    assert at_time.get_column("r1_rank").to_list() == [1, 2]
    assert at_time.get_column("universe_size").unique().to_list() == [2]


def test_gap_invalidates_features_instead_of_dropping_other_symbols() -> None:
    hourly = _hourly(["AAAUSDT", "BBBUSDT"])
    gap = hourly.filter(~((pl.col("symbol") == "AAAUSDT") & (pl.col("open_time") == datetime(2022, 1, 2, 4, tzinfo=UTC))))
    features = build_features(gap, CONFIG)
    target = datetime(2022, 1, 2, 6, tzinfo=UTC)
    assert features.filter((pl.col("decision_time") == target) & (pl.col("symbol") == "AAAUSDT")).is_empty()
    assert features.filter((pl.col("decision_time") == target) & (pl.col("symbol") == "BBBUSDT")).height == 1


def test_zero_latest_completed_hour_excludes_top100_membership_and_ranks() -> None:
    hourly = _hourly(["ACTIVE", "PAUSED"], bars=45)
    target = datetime(2022, 1, 2, 14, tzinfo=UTC)
    hourly = hourly.with_columns(
        pl.when((pl.col("symbol") == "PAUSED") & (pl.col("open_time") == target - timedelta(hours=1)))
        .then(0.0).otherwise(pl.col("quote_volume")).alias("quote_volume"),
        pl.when((pl.col("symbol") == "PAUSED") & (pl.col("open_time") == target - timedelta(hours=1)))
        .then(0).otherwise(pl.col("trade_count")).alias("trade_count"),
    )
    observed = build_features(hourly, CONFIG).filter(pl.col("decision_time") == target)
    assert observed.get_column("symbol").to_list() == ["ACTIVE"]


def test_first_28_hour_warmup_has_historical_r4_rank_change() -> None:
    """T needs exactly source bars T-28h through T-1h, not T-29h."""
    target = datetime(2022, 1, 1, 6, tzinfo=UTC)
    start = target - timedelta(hours=28)
    rows = []
    for multiplier, symbol in [(1.0, "AAAUSDT"), (2.0, "BBBUSDT")]:
        for index in range(29):
            rows.append({"symbol": symbol, "open_time": start + timedelta(hours=index), "open": 100.0,
                         "high": 102.0, "low": 99.0, "close": 100.0 + multiplier * index,
                         "quote_volume": 1_000.0 + multiplier * index, "trade_count": 1})
    observed = build_features(pl.DataFrame(rows), CONFIG).filter(pl.col("decision_time") == target)
    assert observed.height == 2
    assert observed.get_column("r4_rank_change_rank").null_count() == 0


def test_current_features_do_not_require_bar_before_t_minus_25h() -> None:
    target = datetime(2022, 1, 1, 14, tzinfo=UTC)
    required = [target - timedelta(hours=offset) for offset in range(25, 0, -1)]

    def frame(times: list[datetime]) -> pl.DataFrame:
        return pl.DataFrame([
            {"symbol": "AAAUSDT", "open_time": time, "open": 100.0, "high": 102.0, "low": 99.0,
             "close": 100.0 + index, "quote_volume": 1_000.0 + index, "trade_count": 1}
            for index, time in enumerate(times)
        ])

    assert build_features(frame(required), CONFIG).filter(pl.col("decision_time") == target).height == 1
    # T-27h is intentionally disconnected from the required T-25h..T-1h
    # window.  It must not change current feature eligibility.
    assert build_features(frame([target - timedelta(hours=27), *required]), CONFIG).filter(
        pl.col("decision_time") == target
    ).height == 1


def test_historical_top100_does_not_require_t_minus_29h() -> None:
    target = datetime(2022, 1, 1, 6, tzinfo=UTC)
    required = [target - timedelta(hours=offset) for offset in range(28, 0, -1)]

    def frame(times: list[datetime]) -> pl.DataFrame:
        return pl.DataFrame([
            {"symbol": "AAAUSDT", "open_time": time, "open": 100.0, "high": 102.0, "low": 99.0,
             "close": 100.0 + index, "quote_volume": 1_000.0 + index, "trade_count": 1}
            for index, time in enumerate(times)
        ])

    required_rank = build_features(frame(required), CONFIG).filter(pl.col("decision_time") == target).item(
        0, "r4_rank_change_rank"
    )
    # T-30h is disconnected because T-29h is absent.  The T-4h historical
    # Top100 remains valid because its earliest required source bar is T-28h.
    disconnected_rank = build_features(frame([target - timedelta(hours=30), *required]), CONFIG).filter(
        pl.col("decision_time") == target
    ).item(0, "r4_rank_change_rank")
    assert (required_rank, disconnected_rank) == (1, 1)


def test_negative_r4_rank_change_is_signed_before_ranking() -> None:
    target = datetime(2022, 1, 1, 6, tzinfo=UTC)
    start = target - timedelta(hours=28)
    rows = []
    for symbol, closes in {
        "AAAUSDT": [100.0] * 21 + [125.0, 150.0, 175.0, 200.0] + [175.0, 150.0, 125.0, 100.0],
        "BBBUSDT": [100.0] * 21 + [102.5, 105.0, 107.5, 110.0] + [137.5, 165.0, 192.5, 220.0],
    }.items():
        for index, close in enumerate(closes):
            rows.append({"symbol": symbol, "open_time": start + timedelta(hours=index), "open": close,
                         "high": close, "low": close, "close": close,
                         "quote_volume": 1_000.0 + index, "trade_count": 1})
    observed = build_features(pl.DataFrame(rows), CONFIG).filter(
        (pl.col("symbol") == "AAAUSDT") & (pl.col("decision_time") == target)
    ).to_dicts()[0]
    assert observed["r4_rank_change"] == -1
    assert observed["r4_rank_change_rank"] == 2


def test_short_06_selector_cannot_receive_08_rows() -> None:
    frame = pl.DataFrame({"decision_time": [datetime(2022, 1, 1, 6, tzinfo=UTC), datetime(2022, 1, 1, 8, tzinfo=UTC)], "symbol": ["A", "B"]})
    with pytest.raises(SignalError):
        select_short_hour(frame, 6, CONFIG.values["short"], CONFIG.values["universe"]["top_n"])


def test_06_selection_is_invariant_when_08_candidate_is_added() -> None:
    def row(hour: int, symbol: str) -> dict:
        return {"decision_time": datetime(2022, 1, 3, hour, tzinfo=UTC), "symbol": symbol, "r24_rank": 1,
                "r4_rank_change_rank": 100, "volume_diff_v1_rank": 1, "volume_diff_v4_rank": 5,
                "market_r1_p10": -0.01}
    start, end = datetime(2022, 1, 3, tzinfo=UTC), datetime(2022, 1, 4, tzinfo=UTC)
    first = short_signals(pl.DataFrame([row(6, "A")]), start, end, CONFIG)
    combined = short_signals(pl.DataFrame([row(6, "A"), row(8, "B")]), start, end, CONFIG)
    assert first.filter(pl.col("decision_time").dt.hour() == 6).get_column("symbol").to_list() == combined.filter(pl.col("decision_time").dt.hour() == 6).get_column("symbol").to_list()


def test_signal_window_excludes_warmup_decisions() -> None:
    row = {"decision_time": datetime(2021, 12, 31, 6, tzinfo=UTC), "symbol": "A", "r24_rank": 1,
           "r4_rank_change_rank": 100, "volume_diff_v1_rank": 1, "volume_diff_v4_rank": 5,
           "market_r1_p10": -0.01}
    assert short_signals(pl.DataFrame([row]), datetime(2022, 1, 1, tzinfo=UTC), datetime(2022, 1, 2, tzinfo=UTC), CONFIG).is_empty()


def test_boundary_invalid_06_does_not_consume_capacity_from_valid_08() -> None:
    def row(hour: int, symbol: str) -> dict:
        return {"decision_time": datetime(2022, 1, 3, hour, tzinfo=UTC), "symbol": symbol, "r24_rank": 1,
                "r4_rank_change_rank": 100, "volume_diff_v1_rank": 1, "volume_diff_v4_rank": 5,
                "market_r1_p10": -0.01}

    signals = short_signals(
        pl.DataFrame([row(6, "OUTSIDE"), row(8, "AT_BOUNDARY")]),
        datetime(2022, 1, 3, tzinfo=UTC),
        datetime(2022, 1, 3, 17, tzinfo=UTC),
        CONFIG,
    )
    assert signals.select("symbol", "planned_exit_time").rows() == [
        ("AT_BOUNDARY", datetime(2022, 1, 3, 17, tzinfo=UTC))
    ]


def test_long_signal_layer_keeps_all_qualified_candidates() -> None:
    time = datetime(2022, 3, 27, 15, tzinfo=UTC)
    rows = [
        {"decision_time": time, "symbol": symbol, "r1_rank": rank, "r4_rank": rank, "r24_rank": rank,
         "v1_rank": rank, "v4_rank": rank, "market_r24_p10": 0.0}
        for rank, symbol in enumerate(["ZIL", "VET", "CHZ"], start=1)
    ]
    signals = long_signals(pl.DataFrame(rows), time, time + timedelta(days=1), CONFIG)
    assert signals.get_column("symbol").to_list() == ["ZIL", "VET", "CHZ"]
    assert signals.get_column("requested_units").to_list() == [1, 1, 1]


def test_research_subwindow_rejects_cross_boundary_long_exit() -> None:
    window = CONFIG.window("research")
    rows = [
        {"decision_time": datetime(2024, 12, 30, 14, tzinfo=UTC), "planned_exit_time": datetime(2024, 12, 31, 8, 1, tzinfo=UTC), "symbol": "KEEP"},
        {"decision_time": datetime(2024, 12, 31, 14, tzinfo=UTC), "planned_exit_time": datetime(2025, 1, 1, 8, 1, tzinfo=UTC), "symbol": "DROP"},
        {"decision_time": datetime(2026, 6, 30, 14, tzinfo=UTC), "planned_exit_time": datetime(2026, 7, 1, 8, 1, tzinfo=UTC), "symbol": "DROP_H1"},
    ]
    filtered = enforce_research_subwindow_exit_boundary(pl.DataFrame(rows), window)
    assert filtered.get_column("symbol").to_list() == ["KEEP"]
