from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
import sqlite3

import pytest

from fixed_time.dashboard import snapshot
from fixed_time.engine import Engine
from fixed_time.exchange import ExchangeError
from fixed_time.state import RuntimeLock, StateError, Store
from fixed_time.strategy import Admission
from test_state_and_engine import FlatExchange, add_lot, live_config


NOW = datetime(2026,9,19,14,1,tzinfo=UTC)


def intent(store, role="ENTRY", client="entry", lot="lot", reason=None):
    return store.begin_order({"client_id":client,"lot_id":lot,"role":role,"symbol":"ABCUSDT",
        "side":"BUY" if role == "ENTRY" else "SELL","position_side":"LONG","requested_quantity":"2","reason":reason,
        "metadata":{"lot_id":lot,"strategy":"long","source":"MAIN","decision_time":NOW.isoformat(),
                    "planned_exit_time":(NOW+timedelta(hours=18)).isoformat(),"entry_reference":"100"}})


def receipt(quantity="2", status="FILLED", average="100"):
    return {"status":status,"orderId":"1001","executedQty":quantity,"avgPrice":average,
            "updateTime":int(NOW.timestamp()*1000)}


def test_entry_receipt_and_lot_commit_together_and_replay_is_idempotent(tmp_path):
    path = tmp_path/"state.db"
    store = Store(path)
    intent(store)
    with pytest.raises(StateError,match="average price"):
        store.apply_order("entry",receipt(average="0"),NOW)
    assert store.order("entry")["status"] == "SUBMITTED"
    assert store.lot("lot") is None
    store.apply_order("entry",receipt(),NOW)
    store.close()
    store = Store(path)
    store.apply_order("entry",receipt(),NOW)
    assert Decimal(store.lot("lot")["quantity"]) == 2
    assert store.pending_orders() == []
    store.close()


def test_partial_entry_and_exit_only_apply_incremental_fills(tmp_path):
    store = Store(tmp_path/"state.db")
    intent(store)
    store.apply_order("entry",receipt("1","PARTIALLY_FILLED"),NOW)
    store.apply_order("entry",receipt("1","PARTIALLY_FILLED"),NOW)
    assert Decimal(store.lot("lot")["quantity"]) == 1
    assert len(store.pending_orders()) == 1
    store.apply_order("entry",receipt("2","FILLED","110"),NOW)
    assert Decimal(store.lot("lot")["entry_notional"]) == 220
    intent(store,"EXIT","exit",reason="PLANNED_EXIT")
    store.apply_order("exit",receipt("0.5","PARTIALLY_FILLED"),NOW)
    store.apply_order("exit",receipt("0.5","PARTIALLY_FILLED"),NOW)
    assert Decimal(store.lot("lot")["quantity"]) == Decimal("1.5")
    store.apply_order("exit",receipt("2"),NOW)
    assert store.lot("lot")["status"] == "CLOSED"
    store.close()


def test_canceled_partially_filled_entry_is_not_lost(tmp_path):
    store = Store(tmp_path/"state.db")
    intent(store)
    store.apply_order("entry",receipt("0.7","CANCELED"),NOW)
    assert Decimal(store.lot("lot")["quantity"]) == Decimal("0.7")
    assert store.pending_orders() == []
    store.close()


def test_exit_overfill_rolls_back_instead_of_silently_consuming_another_lot(tmp_path):
    store = Store(tmp_path/"state.db")
    add_lot(store,strategy="long")
    intent(store,"EXIT","exit")
    with pytest.raises(StateError,match="exceeds recorded"):
        store.apply_order("exit",receipt("3"),NOW)
    assert store.lot("lot")["quantity"] == "2"
    assert store.order("exit")["applied_quantity"] == "0"
    store.close()


def test_hard_stop_locks_execution_day_not_original_entry_day(tmp_path):
    store = Store(tmp_path/"state.db")
    add_lot(store,strategy="long")
    intent(store,"EXIT","exit",reason="HARD_STOP")
    tomorrow = NOW+timedelta(days=1)
    store.apply_order("exit",dict(receipt(),updateTime=int(tomorrow.timestamp()*1000)),tomorrow)
    assert "ABCUSDT" not in store.long_day_locks(NOW.date())[1]
    assert "ABCUSDT" in store.long_day_locks(tomorrow.date())[1]
    store.close()


def test_missing_exit_fill_is_recovered_once_on_next_reconcile(tmp_path):
    store = Store(tmp_path/"state.db")
    add_lot(store)
    store.set_algos("lot",stop="stop")
    exchange = FlatExchange()
    engine = Engine(live_config(tmp_path),exchange,store)
    assert not engine.reconcile()
    exchange.query_algo_id = lambda *args: {"algoStatus":"FINISHED","actualOrderId":"12"}
    exchange.query_order_id = lambda *args: receipt()
    assert engine.reconcile()
    assert store.lot("lot")["exit_reason"] == "HARD_STOP"
    assert engine.reconcile()
    engine.close()


def test_adl_exit_is_verified_and_reconciled_once(tmp_path):
    store = Store(tmp_path / "state.db")
    add_lot(store)
    store.begin_algo("stop-client", store.lot("lot"), "stop", Decimal("130"))
    store.update_algo("stop-client", "stop-id", "ACKNOWLEDGED")
    store.set_algos("lot", stop="stop-id")
    exchange = FlatExchange()
    open_stops = [{"algoId": "stop-id", "symbol": "ABCUSDT"}]
    exchange.open_algos = lambda: list(open_stops)
    exchange.cancel_algo = lambda symbol, algo_id: open_stops.clear()
    forced_at = datetime(2026, 9, 19, 11, 11, 29, tzinfo=UTC)
    order = {"orderId": 345, "clientOrderId": "adl_autoclose", "symbol": "ABCUSDT",
             "side": "BUY", "positionSide": "SHORT", "status": "FILLED",
             "executedQty": "2", "avgPrice": "80", "updateTime": int(forced_at.timestamp() * 1000)}
    exchange.force_orders = lambda symbol: [order]
    exchange.query_order_id = lambda symbol, order_id: order
    exchange.user_trades = lambda symbol, order_id: [
        {"orderId": 345, "side": "BUY", "positionSide": "SHORT", "qty": "2"}]
    engine = Engine(live_config(tmp_path), exchange, store)
    assert engine.reconcile()
    lot = store.lot("lot")
    assert lot["status"] == "CLOSED"
    assert lot["quantity"] == "0"
    assert lot["exit_reason"] == "EXCHANGE_ADL"
    assert lot["closed_at"] == forced_at.isoformat()
    assert store.order("force:ABCUSDT:345")["applied_quantity"] == "2"
    assert open_stops == []
    assert engine.reconcile()
    assert store.connection.execute("SELECT COUNT(*) FROM v2_orders WHERE role='EXIT'").fetchone()[0] == 1
    engine.close()


def test_adl_quantity_disagreement_keeps_ledger_blocked(tmp_path):
    store = Store(tmp_path / "state.db")
    add_lot(store)
    exchange = FlatExchange()
    exchange.force_orders = lambda symbol: [{"orderId": 345, "clientOrderId": "adl_autoclose",
        "symbol": "ABCUSDT", "side": "BUY", "positionSide": "SHORT", "status": "FILLED",
        "executedQty": "1", "updateTime": int(NOW.timestamp() * 1000)}]
    exchange.query_order_id = lambda *args: pytest.fail("ambiguous forced close must not be applied")
    engine = Engine(live_config(tmp_path), exchange, store)
    assert not engine.reconcile()
    assert store.lot("lot")["status"] == "OPEN"
    assert any(x["code"] == "POSITION_QUANTITY_MISMATCH" for x in store.incidents())
    engine.close()


def test_partial_adl_keeps_remaining_lot_open_and_resizes_stop(tmp_path):
    store = Store(tmp_path / "state.db")
    add_lot(store)
    store.begin_algo("old-client", store.lot("lot"), "stop", Decimal("130"))
    store.update_algo("old-client", "old-stop", "ACKNOWLEDGED")
    store.set_algos("lot", stop="old-stop")
    exchange = FlatExchange()
    exchange.positions = lambda: [{"symbol": "ABCUSDT", "positionSide": "SHORT", "positionAmt": "-1.5"}]
    open_stops = [{"algoId": "old-stop", "symbol": "ABCUSDT", "triggerPrice": "130", "quantity": "2"}]
    exchange.open_algos = lambda: list(open_stops)
    exchange.cancel_algo = lambda symbol, algo_id: open_stops.__setitem__(slice(None),
        [x for x in open_stops if x["algoId"] != algo_id])
    exchange.query_algo_id = lambda symbol, algo_id: {
        "actualOrderId": "0", "algoStatus": "CANCELED" if algo_id == "old-stop" else "NEW"}
    exchange.query_algo_client = lambda symbol, client: {
        "algoId": "old-stop" if client == "old-client" else "new-stop",
        "algoStatus": "CANCELED" if client == "old-client" else "NEW"}
    def place(*args):
        open_stops.append({"algoId": "new-stop", "symbol": "ABCUSDT",
                           "triggerPrice": str(args[4]), "quantity": str(args[3])})
        return {"algoId": "new-stop"}
    exchange.conditional_order = place
    forced_at = datetime(2026, 9, 19, 11, 11, 29, tzinfo=UTC)
    order = {"orderId": 346, "clientOrderId": "adl_autoclose", "symbol": "ABCUSDT",
             "side": "BUY", "positionSide": "SHORT", "status": "FILLED",
             "executedQty": "0.5", "avgPrice": "80", "updateTime": int(forced_at.timestamp() * 1000)}
    exchange.force_orders = lambda symbol: [order]
    exchange.query_order_id = lambda symbol, order_id: order
    exchange.user_trades = lambda symbol, order_id: [
        {"orderId": 346, "side": "BUY", "positionSide": "SHORT", "qty": "0.5"}]
    engine = Engine(live_config(tmp_path), exchange, store)
    assert engine.reconcile()
    lot = store.lot("lot")
    assert lot["status"] == "OPEN"
    assert lot["quantity"] == "1.5"
    assert Decimal(lot["entry_notional"]) == Decimal("150")
    assert lot["exit_reason"] is None
    assert lot["stop_algo_id"] == "new-stop"
    assert open_stops[0]["quantity"] == "1.5"
    assert engine.reconcile()
    assert store.connection.execute("SELECT COUNT(*) FROM v2_orders WHERE role='EXIT'").fetchone()[0] == 1
    engine.close()


def test_adl_receipt_recovers_after_crash_between_intent_and_apply(tmp_path):
    store = Store(tmp_path / "state.db")
    add_lot(store)
    store.begin_order({"client_id": "force:ABCUSDT:347", "lot_id": "lot", "role": "EXIT",
                       "symbol": "ABCUSDT", "side": "BUY", "position_side": "SHORT",
                       "requested_quantity": "2", "reason": "EXCHANGE_ADL"})
    exchange = FlatExchange()
    order = {"orderId": 347, "clientOrderId": "adl_autoclose", "symbol": "ABCUSDT",
             "side": "BUY", "positionSide": "SHORT", "status": "FILLED",
             "executedQty": "2", "avgPrice": "80", "updateTime": int(NOW.timestamp() * 1000)}
    exchange.query_order_id = lambda symbol, order_id: order
    exchange.user_trades = lambda symbol, order_id: [
        {"orderId": 347, "side": "BUY", "positionSide": "SHORT", "qty": "2"}]
    engine = Engine(live_config(tmp_path), exchange, store)
    assert engine.reconcile()
    assert store.lot("lot")["status"] == "CLOSED"
    assert store.pending_orders() == []
    assert engine.reconcile()
    engine.close()


def test_partial_algo_fill_reconciliation_does_not_double_subtract(tmp_path):
    store = Store(tmp_path/"state.db")
    add_lot(store)
    store.set_algos("lot",stop="stop")
    exchange = FlatExchange()
    exchange.query_algo_id = lambda *args: {"actualOrderId":"12"}
    exchange.query_order_id = lambda *args: receipt("0.5","PARTIALLY_FILLED")
    engine = Engine(live_config(tmp_path),exchange,store)
    for _ in range(3):
        engine._algo_fill(store.lot("lot"),"stop_algo_id","HARD_STOP")
    assert Decimal(store.lot("lot")["quantity"]) == Decimal("1.5")
    engine.close()


def test_pending_exit_is_queried_not_submitted_again(tmp_path):
    store = Store(tmp_path/"state.db")
    add_lot(store)
    intent(store,"EXIT","exit")
    exchange = FlatExchange()
    exchange.market_order = lambda *args: pytest.fail("duplicate market close")
    engine = Engine(live_config(tmp_path),exchange,store)
    engine._close_lot(store.lot("lot"),"PLANNED_EXIT")
    engine.close()


def test_pending_conditional_after_restart_reuses_saved_client_id(tmp_path):
    store = Store(tmp_path/"state.db")
    add_lot(store)
    store.begin_algo("known",store.lot("lot"),"stop",Decimal("130"))
    exchange = FlatExchange()
    exchange.query_algo_client = lambda symbol,client: {"algoId":"15","algoStatus":"NEW"} if client == "known" else pytest.fail()
    engine = Engine(live_config(tmp_path),exchange,store)
    engine._recover_pending()
    assert store.lot("lot")["stop_algo_id"] == "15"
    assert store.algos()[0]["status"] == "ACKNOWLEDGED"
    engine.close()


def test_crash_after_algo_ack_before_lot_binding_is_recovered(tmp_path):
    store = Store(tmp_path/"state.db")
    add_lot(store)
    store.begin_algo("known",store.lot("lot"),"stop",Decimal("130"))
    store.update_algo("known","15","ACKNOWLEDGED")
    exchange = FlatExchange()
    exchange.query_algo_client = lambda *args: {"algoId":"15","algoStatus":"NEW"}
    engine = Engine(live_config(tmp_path),exchange,store)
    engine._recover_pending()
    assert store.lot("lot")["stop_algo_id"] == "15"
    engine.close()


def test_retry_keeps_frozen_batch_allocation(tmp_path):
    store = Store(tmp_path/"state.db")
    exchange = FlatExchange()
    exchange.now = lambda: NOW
    engine = Engine(live_config(tmp_path),exchange,store)
    decision = NOW.replace(minute=0)
    row = {"symbol":"ABCUSDT","source":"MAIN","strategy":"long","decision_time":decision,
           "entry_time":NOW,"planned_exit_time":NOW+timedelta(hours=18)}
    store.save_decision(decision.isoformat(),"RUNNING",{"plan":[{"candidate":row,"target_notional":"123.45"}]})
    targets=[]
    engine._open = lambda item: targets.append(item.target_notional) or "OPEN"
    engine.process_decision(decision,[])
    assert targets == [Decimal("123.45")]
    engine.close()


def test_day_reference_cannot_use_future_days_or_reclassify_late_start(tmp_path):
    store = Store(tmp_path/"state.db")
    store.record_equity(NOW+timedelta(days=1),Decimal("900"))
    late = NOW.replace(hour=0,minute=59)
    assert store.day_reference(NOW.date(),Decimal("100"),late) == 100
    assert store.connection.execute("SELECT recovered_late FROM v2_day_reference").fetchone()[0] == 1
    assert store.day_reference(NOW.date(),Decimal("200"),NOW) == 100
    store.close()


def test_health_requires_recent_heartbeat_and_equity(tmp_path):
    path = tmp_path/"state.db"
    store = Store(path)
    assert not snapshot(path)["healthy"]
    now = datetime.now(UTC)
    store.record_equity(now,Decimal("100"))
    store.heartbeat(now-timedelta(minutes=3))
    assert not snapshot(path)["healthy"]
    store.heartbeat(now)
    assert snapshot(path)["healthy"]
    store.close()


def test_engine_construction_is_not_a_deployment(tmp_path):
    engine = Engine(live_config(tmp_path),FlatExchange())
    assert engine.store.connection.execute("SELECT COUNT(*) FROM v2_deployments").fetchone()[0] == 0
    engine.close()


def test_runtime_lock_rejects_second_writer_and_releases_afterwards(tmp_path):
    path = tmp_path/"state.db"
    with RuntimeLock(path):
        with pytest.raises(StateError,match="locked"):
            with RuntimeLock(path):
                pytest.fail()
    with RuntimeLock(path):
        pass


def test_failed_lot_does_not_prevent_other_lots_being_managed(tmp_path):
    store = Store(tmp_path/"state.db")
    add_lot(store,"one")
    add_lot(store,"two")
    engine = Engine(live_config(tmp_path),FlatExchange(),store)
    visited=[]
    def manage(lot,*args):
        visited.append(lot["lot_id"])
        if lot["lot_id"] == "one":
            raise ExchangeError("temporarily unavailable")
    engine._manage_lot=manage
    engine.manage_positions()
    assert visited == ["one","two"]
    engine.close()
