from datetime import UTC, datetime, timedelta
from decimal import Decimal
from io import BytesIO
import json

import pytest

from fixed_time import dashboard
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


def test_dashboard_equity_periods_filter_and_bucket_history(tmp_path):
    path = tmp_path / "state.db"
    store = Store(path)
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    for days, amount in [(20, "900"), (5, "950"), (0, "1000")]:
        store.record_equity(now-timedelta(days=days), Decimal(amount))
    for minute in range(12):
        store.record_equity(now-timedelta(minutes=minute), Decimal(1000+minute))
    assert {x["equity"] for x in snapshot(path, "1d")["equity_history"]} == {
        str(Decimal(1000+minute)) for minute in range(12)}
    assert "950" in {x["equity"] for x in snapshot(path, "7d")["equity_history"]}
    assert "900" in {x["equity"] for x in snapshot(path, "30d")["equity_history"]}
    assert len(snapshot(path, "30d")["equity_history"]) < 15
    with pytest.raises(ValueError, match="unsupported"):
        snapshot(path, "365d")
    store.close()


def test_dashboard_shows_each_candidate_and_partial_exit_gross_pnl(tmp_path):
    path = tmp_path / "state.db"
    store = Store(path)
    add_lot(store)
    decision_time = datetime(2026, 9, 19, tzinfo=UTC).isoformat()
    store.save_decision(decision_time, "COMPLETE", {
        "candidates": [
            {"trade_id": "lot", "symbol": "ABCUSDT", "strategy": "short", "source": "NEW_SHORT"},
            {"trade_id": "other", "symbol": "XYZUSDT", "strategy": "short",
             "source": "NEW_SHORT", "rejection": "SAME_SHORT_OPEN"},
        ],
        "plan": [{"candidate": {"trade_id": "lot"}, "target_notional": "200"}],
        "admissions": [{"symbol": "ABCUSDT", "outcome": "ALREADY_OPEN"}],
    })
    first = datetime(2026, 9, 19, 2, tzinfo=UTC)
    for client, price, when, reason in [
        ("exit1", "80", first, "EXCHANGE_ADL"),
        ("exit2", "90", first+timedelta(hours=1), "PLANNED_EXIT"),
    ]:
        store.begin_order({"client_id": client, "lot_id": "lot", "role": "EXIT",
            "symbol": "ABCUSDT", "side": "BUY", "position_side": "SHORT",
            "requested_quantity": "1", "reason": reason})
        store.apply_order(client, {"status": "FILLED", "executedQty": "1", "avgPrice": price,
            "orderId": client, "updateTime": int(when.timestamp()*1000)}, when)
    data = snapshot(path)
    assert [x["result"] for x in data["decisions"][0]["items"]] == ["OPENED", "SAME_SHORT_OPEN"]
    assert [x["gross_pnl"] for x in data["closed"]] == ["10", "20"]
    assert [x["gross_return_pct"] for x in data["closed"]] == ["10.0", "20.0"]
    assert data["closed"][0]["exit_time"] == (first+timedelta(hours=1)).isoformat()
    assert all(not x["time_is_ledger"] for x in data["closed"])
    assert store.lot("lot")["status"] == "CLOSED"
    store.close()


def test_quote_cache_drops_prices_when_testnet_quote_is_unavailable(monkeypatch):
    calls = []
    def available(url, timeout):
        calls.append(url)
        return BytesIO(json.dumps([{"symbol": "ABCUSDT", "price": "0.035"},
                                   {"symbol": "OTHERUSDT", "price": "5"}]).encode())
    monkeypatch.setattr(dashboard, "urlopen", available)
    cache = dashboard.TickerCache()
    assert cache.get({"ABCUSDT"})["prices"] == {"ABCUSDT": "0.035"}
    assert cache.get({"ABCUSDT"})["prices"] == {"ABCUSDT": "0.035"}
    assert len(calls) == 1
    cache.expires = datetime.now(UTC)-timedelta(seconds=1)
    def unavailable(*_args, **_kwargs):
        raise OSError("offline")
    monkeypatch.setattr(dashboard, "urlopen", unavailable)
    assert cache.get({"ABCUSDT"}) == {"prices": {}, "observed_at": None}
