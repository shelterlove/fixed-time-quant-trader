from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from fixed_time.live.dashboard import read_status
from fixed_time.live.state import StateStore


def test_dashboard_reads_runtime_positions_and_decisions(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "runtime.sqlite3")
    now = datetime(2026, 9, 1, 14, tzinfo=UTC).isoformat()
    store.update_runtime_status("1.3.0", now, "100", 0, 0, reconciled=True)
    store.start_decision(now)
    store.finish_decision(now, 10, [{"symbol": "AAAUSDT"}], [{"symbol": "AAAUSDT", "units": 1}])
    store.close()

    status = read_status(tmp_path / "runtime.sqlite3")
    assert status["runtime"]["available_usdt"] == "100"
    assert status["positions"] == []
    assert status["decisions"][0]["detail"]["candidates"][0]["symbol"] == "AAAUSDT"


def test_dashboard_reports_auditable_account_and_trade_returns(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "runtime.sqlite3")
    first = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store.update_runtime_status("1.3.0", first.isoformat(), "105", 0, 0, reconciled=True)
    store.record_equity_minute(first.isoformat(), Decimal("100"), Decimal("0"), Decimal("100"), Decimal("100"), Decimal("0"))
    store.record_equity_minute((first.replace(minute=1)).isoformat(), Decimal("105"), Decimal("0"), Decimal("105"), Decimal("105"), Decimal("0"))
    store.create_intent({"intent_id": "trade", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG",
                         "decision_time": first.isoformat(), "planned_exit_time": first.isoformat(), "units": 1,
                         "priority_score": 1.0, "client_order_id": "entry"})
    store.record_execution("trade", "entry", "ENTRY", {"orderId": 10, "status": "FILLED", "executedQty": "1", "avgPrice": "100"})
    store.open_position("trade", "1", "100")
    store.record_trade_fills([{"symbol": "AAAUSDT", "id": 1, "orderId": 10, "positionSide": "LONG", "side": "BUY",
                               "qty": "1", "price": "100", "quoteQty": "100", "realizedPnl": "5",
                               "commission": "1", "commissionAsset": "USDT", "time": int(first.timestamp() * 1000)}])
    store.close()

    status = read_status(tmp_path / "runtime.sqlite3")
    assert Decimal(status["performance"]["total_net_pnl"]) == Decimal("5")
    assert status["trades"][0]["fill_count"] == 1
    assert status["trades"][0]["net_pnl"] == 4.0
    store = StateStore(tmp_path / "runtime.sqlite3")
    store.record_execution("trade", "exit", "EXIT", {"orderId": 11, "status": "FILLED", "executedQty": "1", "avgPrice": "110"})
    store.close_position("trade")
    store.close()
    partial = read_status(tmp_path / "runtime.sqlite3")["trades"][0]
    assert partial["fills_complete"] is False
    assert partial["net_pnl"] is None


def test_account_returns_ignore_transfers_after_latest_equity(tmp_path):
    path = tmp_path / "state.sqlite3"
    store = StateStore(path)
    first = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    for offset, value in ((0, "100"), (1, "115")):
        amount = Decimal(value)
        store.record_equity_minute((first + timedelta(minutes=offset)).isoformat(), amount, Decimal(0), amount, amount, Decimal(0))
    store.record_income_events([
        {"incomeType": "TRANSFER", "tranId": i, "income": "10", "asset": "USDT",
         "time": int((first + timedelta(seconds=seconds)).timestamp() * 1000)}
        for i, seconds in ((1, 30), (2, 90))
    ])
    status = read_status(path)["performance"]
    assert Decimal(status["total_net_pnl"]) == Decimal(5)
    assert Decimal(status["today_net_pnl"]) == Decimal(5)
    store.close()
