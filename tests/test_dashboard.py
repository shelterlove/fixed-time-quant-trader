from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fixed_time.dashboard import snapshot
from fixed_time.state import Store
from test_state_and_engine import add_lot


def test_dashboard_aggregates_ledger_without_changing_it(tmp_path):
    path = tmp_path / "state.db"
    store = Store(path)
    now = datetime.now(UTC)
    store.record_equity(now-timedelta(minutes=5), Decimal("1000"))
    store.record_equity(now, Decimal("1010"))
    store.heartbeat(now)
    add_lot(store, strategy="long")
    store.arm_profit("lot", now)
    store.begin_algo("stop", store.lot("lot"), "stop", Decimal("70"))
    store.update_algo("stop", "1", "ACKNOWLEDGED")
    store.set_algos("lot", stop="1")
    store.begin_algo("cap", store.lot("lot"), "cap", Decimal("500"))
    before = store.connection.total_changes
    data = snapshot(path)
    assert data["equity_change"] == "10"
    assert data["occupied"] == {"long":"200", "short":"0"}
    assert len(data["equity_history"]) == 2
    assert data["positions"][0]["stop_is_floor"] is False  # Armed is not confirmation of replacement.
    assert data["pending_orders"][0]["role"] == "PROTECTION"
    assert store.connection.total_changes == before
    store.close()


def test_dashboard_does_not_report_old_equity_as_healthy(tmp_path):
    path = tmp_path / "state.db"
    assert not snapshot(path)["healthy"]
    assert not path.exists()
    store = Store(path)
    now = datetime.now(UTC)
    store.record_equity(now-timedelta(minutes=10), Decimal("1000"))
    store.heartbeat(now)
    data = snapshot(path)
    assert not data["healthy"]
    assert data["reason"] == "权益数据缺失或已过期"
    store.close()
