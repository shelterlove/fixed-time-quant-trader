from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json

import polars as pl
import pytest

from fixed_time.config import load_config
from fixed_time.execution import SHADOW_HISTORY_COLUMNS, execute_long_with_funding_diagnostics
from fixed_time.pipeline import _research_shadow_history, _validate_signal_cache
from fixed_time.storage import empty_funding_frame


def _signals() -> pl.DataFrame:
    time = datetime(2022, 1, 1, 14, tzinfo=UTC)
    return pl.DataFrame([{
        "trade_id": "long:A:2022-01-01", "strategy": "long", "symbol": "A",
        "decision_time": time, "source_bar_time": time - timedelta(hours=1),
        "entry_time": time + timedelta(minutes=1), "planned_exit_time": time + timedelta(hours=1),
        "requested_units": 1, "priority_score": 1, "priority_order": 1, "signal_scope": "window",
    }])


def _meta(frame: pl.DataFrame) -> dict[str, object]:
    return {
        "rows": frame.height,
        "symbols": frame.get_column("symbol").n_unique(),
        "first_time": frame.get_column("decision_time").min().isoformat(),
        "last_time": frame.get_column("decision_time").max().isoformat(),
        "latest_source_time": frame.get_column("source_bar_time").max().isoformat(),
    }


def test_signal_cache_validation_rejects_truncation_and_duplicate_ids() -> None:
    frame = pl.concat([_signals(), _signals().with_columns(pl.lit("long:B:2022-01-01").alias("trade_id"))])
    with pytest.raises(ValueError, match="rows"):
        _validate_signal_cache(frame.head(1), _meta(frame))
    with pytest.raises(ValueError, match="duplicate"):
        _validate_signal_cache(pl.concat([_signals(), _signals()]), _meta(pl.concat([_signals(), _signals()])))


def test_signal_cache_validation_rejects_invalid_scope() -> None:
    frame = _signals().with_columns(pl.lit("unknown").alias("signal_scope"))
    with pytest.raises(ValueError, match="signal_scope"):
        _validate_signal_cache(frame, _meta(frame))


def test_empty_funding_frame_supports_history_only_long_execution() -> None:
    entry = datetime(2022, 1, 1, 14, 1, tzinfo=UTC)
    signals = _signals().with_columns(
        pl.lit(entry - timedelta(minutes=1)).alias("decision_time"),
        pl.lit(entry).alias("entry_time"),
        pl.lit(entry + timedelta(minutes=2)).alias("planned_exit_time"),
    )
    minutes = pl.DataFrame([
        {"symbol": "A", "open_time": entry - timedelta(minutes=1), "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "quote_volume": 1.0, "trade_count": 1},
        {"symbol": "A", "open_time": entry, "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "quote_volume": 1.0, "trade_count": 1},
        {"symbol": "A", "open_time": entry + timedelta(minutes=1), "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "quote_volume": 1.0, "trade_count": 1},
    ])
    trades, funding = execute_long_with_funding_diagnostics(
        signals, minutes, empty_funding_frame(), load_config(), emit_from=entry + timedelta(days=1),
    )
    assert trades.is_empty()
    assert funding.is_empty()


def test_research_shadow_history_requires_manifest_row_match(tmp_path) -> None:
    config = replace(load_config(), root=tmp_path)
    output = tmp_path / "results" / "local" / "research"
    output.mkdir(parents=True)
    pl.DataFrame([{
        "shadow_exit_time": datetime(2022, 1, 1, tzinfo=UTC),
        "shadow_activated": False,
        "shadow_max_retrace": None,
    }]).write_parquet(output / "long_trades.parquet")
    (output / "run_manifest.json").write_text(json.dumps({
        "strategy_version": config.version, "window": "research", "parameters": config.values,
        "long_trade_rows": 2,
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="row count"):
        _research_shadow_history(config, config.window("forward_2026_jul_aug"))
