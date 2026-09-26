from datetime import UTC, datetime, timedelta
from decimal import Decimal
from http.server import ThreadingHTTPServer
from threading import Event, Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import json

import pytest

from fixed_time.dashboard import snapshot
from fixed_time import dashboard
from fixed_time.engine import Engine
from fixed_time.manual import ManualActionError, submit_action
from fixed_time.state import Store
from test_state_and_engine import FlatExchange, add_lot, live_config


def future_lot(tmp_path, strategy="long"):
    store = Store(tmp_path / "state.db")
    add_lot(store, strategy=strategy)
    scheduled = (datetime.now(UTC) + timedelta(hours=8)).replace(microsecond=0).isoformat()
    store.connection.execute("UPDATE v2_lots SET planned_exit_time=?,scheduled_exit_time=? WHERE lot_id='lot'",
                             (scheduled, scheduled))
    store.connection.commit()
    return store, scheduled


def test_manual_extension_is_one_time_and_preserves_automatic_extension(tmp_path):
    store, scheduled = future_lot(tmp_path)
    command = submit_action(tmp_path / "state.db", lot_id="lot", action="EXTEND_4H",
                            expected_exit_time=scheduled)
    with pytest.raises(ManualActionError, match="already pending"):
        submit_action(tmp_path / "state.db", lot_id="lot", action="SELL_NOW",
                      expected_exit_time=scheduled)
    engine = Engine(live_config(tmp_path), FlatExchange(), store)
    engine.reconcile = lambda: True
    engine.process_manual_actions()
    lot = store.lot("lot")
    assert lot["scheduled_exit_time"] == (datetime.fromisoformat(scheduled)+timedelta(hours=4)).isoformat()
    assert lot["manual_extended_at"]
    assert store.pending_manual_actions() == []
    assert snapshot(tmp_path / "state.db")["positions"][0]["manual_action"]["status"] == "APPLIED"
    with pytest.raises(ManualActionError, match="unavailable"):
        submit_action(tmp_path / "state.db", lot_id="lot", action="EXTEND_4H",
                      expected_exit_time=lot["scheduled_exit_time"])
    activation = datetime.fromisoformat(scheduled)-timedelta(hours=2)
    store.arm_extension("lot", activation)
    exchange = engine.exchange
    exchange.latest_price = lambda symbol: Decimal("100")
    engine._manage_lot(store.lot("lot"), datetime.fromisoformat(lot["scheduled_exit_time"]), {})
    assert store.lot("lot")["scheduled_exit_time"] == (datetime.fromisoformat(scheduled)+timedelta(hours=28)).isoformat()
    engine.close()


def test_manual_sell_uses_existing_exit_flow_and_closes_only_requested_lot(tmp_path):
    store, scheduled = future_lot(tmp_path)
    add_lot(store, lot_id="sibling", strategy="long")
    store.connection.execute("UPDATE v2_lots SET scheduled_exit_time=? WHERE lot_id='sibling'", (scheduled,))
    store.connection.commit()
    submit_action(tmp_path / "state.db", lot_id="lot", action="SELL_NOW", expected_exit_time=scheduled)
    exchange = FlatExchange()
    exposure = ["4"]
    exchange.positions = lambda: [{"symbol": "ABCUSDT", "positionSide": "LONG", "positionAmt": exposure[0]}]
    def close_order(*_args):
        exposure[0] = "2"
        return {"status": "FILLED", "executedQty": "2", "avgPrice": "90",
                "orderId": "123", "updateTime": int(datetime.now(UTC).timestamp()*1000)}
    exchange.market_order = close_order
    engine = Engine(live_config(tmp_path), exchange, store)
    engine.reconcile = lambda: True
    engine.process_manual_actions()
    assert store.lot("lot")["exit_reason"] == "MANUAL_EXIT"
    assert store.pending_manual_actions() == []
    engine.manage_positions()
    assert store.lot("lot")["status"] == "CLOSED"
    assert store.lot("lot")["exit_reason"] == "MANUAL_EXIT"
    assert store.lot("sibling")["status"] == "OPEN"
    assert snapshot(tmp_path / "state.db")["closed"][0]["exit_reason"] == "MANUAL_EXIT"
    engine.close()


def test_manual_action_rejects_stale_position_without_changing_it(tmp_path):
    store, scheduled = future_lot(tmp_path)
    with pytest.raises(ManualActionError, match="refresh"):
        submit_action(tmp_path / "state.db", lot_id="lot", action="SELL_NOW",
                      expected_exit_time=(datetime.fromisoformat(scheduled)-timedelta(hours=1)).isoformat())
    assert store.pending_manual_actions() == []
    assert store.lot("lot")["exit_reason"] is None
    store.close()


def test_dashboard_requires_token_and_secure_origin_for_manual_action(tmp_path, monkeypatch):
    store, scheduled = future_lot(tmp_path)
    ready = Event()
    server = {}
    class LocalServer(ThreadingHTTPServer):
        def __init__(self, address, handler):
            super().__init__(("127.0.0.1", 0), handler)
            server["instance"] = self
            ready.set()
    monkeypatch.setattr(dashboard, "ThreadingHTTPServer", LocalServer)
    token = "a" * 64
    worker = Thread(target=dashboard.serve, args=(tmp_path / "state.db", "127.0.0.1", 0, token), daemon=True)
    worker.start()
    assert ready.wait(3)
    port = server["instance"].server_address[1]
    url = f"http://127.0.0.1:{port}/api/manual-action"
    body = json.dumps({"lot_id": "lot", "action": "EXTEND_4H", "expected_exit_time": scheduled}).encode()
    def request(origin, key):
        headers = {"Content-Type": "application/json", "X-Control-Token": key}
        if origin:
            headers["Origin"] = origin
        return urlopen(Request(url, data=body, headers=headers), timeout=3)
    try:
        with pytest.raises(HTTPError) as unauthorized:
            request(f"http://127.0.0.1:{port}", "wrong")
        assert unauthorized.value.code == 403
        with pytest.raises(HTTPError) as insecure:
            request(None, token)
        assert insecure.value.code == 403
        with request(f"http://127.0.0.1:{port}", token) as response:
            assert response.status == 202
        assert len(store.pending_manual_actions()) == 1
    finally:
        server["instance"].shutdown()
        server["instance"].server_close()
        worker.join(3)
        store.close()
