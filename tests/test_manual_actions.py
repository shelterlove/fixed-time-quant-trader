from datetime import UTC, datetime, timedelta
from decimal import Decimal
from http.server import ThreadingHTTPServer
from hashlib import sha256
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


@pytest.mark.parametrize("strategy,stop,take,stop_side,cap_side", [
    ("long", "95.01", "120.01", "SELL", "SELL"),
    ("short", "110.09", "90.09", "BUY", "BUY"),
])
def test_custom_extension_sets_exchange_stop_and_take_profit(tmp_path, strategy, stop, take, stop_side, cap_side):
    store, scheduled = future_lot(tmp_path, strategy)
    submit_action(tmp_path / "state.db", lot_id="lot", action="EXTEND", expected_exit_time=scheduled,
                  extension_hours=6, stop_loss_price=stop, take_profit_price=take)
    exchange = FlatExchange()
    exchange.positions = lambda: [{"symbol": "ABCUSDT", "positionSide": strategy.upper(),
                                    "positionAmt": "2" if strategy == "long" else "-2"}]
    placed = []
    exchange.conditional_order = lambda *args, **kwargs: placed.append(args) or {"algoId": f"algo{len(placed)}"}
    engine = Engine(live_config(tmp_path), exchange, store)
    engine.reconcile = lambda: True
    engine.process_manual_actions()
    lot = store.lot("lot")
    assert lot["scheduled_exit_time"] == (datetime.fromisoformat(scheduled)+timedelta(hours=6)).isoformat()
    assert lot["manual_extension_hours"] == 6
    assert lot["manual_stop_price"] == stop
    assert lot["manual_take_profit_price"] == take
    engine.reconcile = Engine.reconcile.__get__(engine)
    assert engine.reconcile()
    assert [(call[1], call[4], call[5]) for call in placed] == [
        (stop_side, engine._trigger(lot, "stop"), "STOP_MARKET"),
        (cap_side, engine._trigger(lot, "cap"), "TAKE_PROFIT_MARKET"),
    ]
    assert snapshot(tmp_path / "state.db")["positions"][0]["take_profit_trigger"] is not None
    engine.close()


def test_custom_extension_rejects_triggered_price_without_extending(tmp_path):
    store, scheduled = future_lot(tmp_path)
    submit_action(tmp_path / "state.db", lot_id="lot", action="EXTEND", expected_exit_time=scheduled,
                  stop_loss_price="105")
    engine = Engine(live_config(tmp_path), FlatExchange(), store)
    engine.reconcile = lambda: True
    engine.process_manual_actions()
    assert store.lot("lot")["scheduled_exit_time"] == scheduled
    assert store.lot("lot")["manual_extended_at"] is None
    assert store.connection.execute("SELECT status FROM v2_manual_actions").fetchone()[0] == "REJECTED"
    engine.close()


def test_custom_take_profit_supersedes_default_profit_cap(tmp_path):
    store, _ = future_lot(tmp_path)
    store.connection.execute("UPDATE v2_lots SET manual_take_profit_price='600', manual_stop_price='90' WHERE lot_id='lot'")
    store.connection.commit()
    exchange = FlatExchange()
    exchange.latest_price = lambda symbol: Decimal("500")
    engine = Engine(live_config(tmp_path), exchange, store)
    engine._manage_lot(store.lot("lot"), datetime.now(UTC), {})
    assert store.lot("lot")["status"] == "OPEN"
    engine.close()


def test_exit_fill_backfill_restores_price_and_gross_pnl(tmp_path):
    store = Store(tmp_path / "state.db")
    add_lot(store)
    store.begin_order({"client_id": "exit", "lot_id": "lot", "role": "EXIT", "symbol": "ABCUSDT",
                       "side": "BUY", "position_side": "SHORT", "requested_quantity": "2", "reason": "PLANNED_EXIT"})
    store.apply_order("exit", {"status": "FILLED", "executedQty": "2", "avgPrice": "0", "orderId": "42"},
                      datetime.now(UTC))
    assert snapshot(tmp_path / "state.db")["closed"][0]["gross_pnl"] is None
    exchange = FlatExchange()
    exchange.user_trades = lambda symbol, order_id: [
        {"orderId": "42", "side": "BUY", "positionSide": "SHORT", "qty": "1", "price": "90"},
        {"orderId": "42", "side": "BUY", "positionSide": "SHORT", "qty": "1", "price": "80"},
    ]
    engine = Engine(live_config(tmp_path), exchange, store)
    assert engine.backfill_exit_prices(force=True) == 1
    closed = snapshot(tmp_path / "state.db")["closed"][0]
    assert closed["exit_price"] == "85"
    assert closed["gross_pnl"] == "30"
    assert engine.backfill_exit_prices(force=True) == 0
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
    token = sha256(b"test-only-password").hexdigest()
    worker = Thread(target=dashboard.serve, args=(tmp_path / "state.db", "127.0.0.1", 0, token), daemon=True)
    worker.start()
    assert ready.wait(3)
    port = server["instance"].server_address[1]
    url = f"http://127.0.0.1:{port}/api/manual-action"
    body = json.dumps({"lot_id": "lot", "action": "EXTEND", "expected_exit_time": scheduled,
                       "extension_hours": 6, "stop_loss_price": "90", "take_profit_price": "120"}).encode()
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
        assert store.pending_manual_actions()[0]["extension_hours"] == 6
        for _ in range(10):
            with pytest.raises(HTTPError) as incorrect:
                request(f"http://127.0.0.1:{port}", "wrong")
            assert incorrect.value.code == 403
        with pytest.raises(HTTPError) as limited:
            request(f"http://127.0.0.1:{port}", "wrong")
        assert limited.value.code == 429
    finally:
        server["instance"].shutdown()
        server["instance"].server_close()
        worker.join(3)
        store.close()
