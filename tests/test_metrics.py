import polars as pl
import pytest
from datetime import datetime, UTC, timedelta

from fixed_time.metrics import summarize


def test_summary_exposes_trade_and_account_profit_factors() -> None:
    trades = pl.DataFrame([
        {"strategy": "long", "net_return": 0.1, "pnl": 10.0, "mae_return": 0.0, "mfe_return": 0.0, "exit_reason": "PLANNED_EXIT"},
        {"strategy": "short", "net_return": -0.1, "pnl": -1.0, "mae_return": 0.0, "mfe_return": 0.0, "exit_reason": "PLANNED_EXIT"},
    ])
    account = pl.DataFrame(schema={"event_time": pl.Datetime, "realized_equity": pl.Float64, "open_units": pl.Int64, "open_positions": pl.Int64})
    summary, _ = summarize(trades, account, {}, {})
    row = summary.to_dicts()[0]
    assert row["trade_return_pf"] == 1.0
    assert row["account_pnl_pf"] == 10.0


def test_realized_drawdown_includes_initial_capital_peak() -> None:
    trades = pl.DataFrame(schema={"strategy": pl.String, "net_return": pl.Float64, "pnl": pl.Float64,
                                  "mae_return": pl.Float64, "mfe_return": pl.Float64, "exit_reason": pl.String})
    start = datetime(2022, 1, 1, tzinfo=UTC)
    account = pl.DataFrame([
        {"event_time": start, "realized_equity": 0.9, "open_units": 0, "open_positions": 0},
        {"event_time": start + timedelta(hours=1), "realized_equity": 0.95, "open_units": 0, "open_positions": 0},
    ])
    summary, _ = summarize(trades, account, {}, {})
    assert summary.item(0, "realized_max_drawdown") == pytest.approx(-0.1)
