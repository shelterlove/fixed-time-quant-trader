from concurrent.futures import Future
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json

import polars as pl
import pytest

from fixed_time.execution import _simulate_long
from fixed_time.features import build_features
from fixed_time.live.binance import BinanceError, BinanceRest
from fixed_time.live.engine import LiveEngine
from fixed_time.live.shadows import advance_shadow
from fixed_time.live.state import EXECUTION_VERSION, SHADOW_VERSION, StateError, StateStore
from fixed_time.live.strategy import Admission
from fixed_time.signals import short_signals
from test_live import _config, _candidate, _Client, _RecoveryClient, _CatchupProtectionClient


TIME = datetime(2026, 8, 1, 14, tzinfo=UTC)


def opened(tmp_path, client=None, *, version=EXECUTION_VERSION):
    store = StateStore(tmp_path / "state.sqlite3")
    store.create_intent({"intent_id": "held", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG",
                         "decision_time": TIME.isoformat(), "planned_exit_time": (TIME + timedelta(hours=18)).isoformat(),
                         "units": 1, "priority_score": 1., "client_order_id": "entry"})
    store.open_position("held", ".1", "100", .1, "hard", execution_version=version, filled_at=TIME.isoformat())
    return LiveEngine(_config(tmp_path), client=client or _Client(), store=store)


class HostedClient(_RecoveryClient):
    def __init__(self):
        super().__init__()
        self.exchange_positions = [{"symbol": "AAAUSDT", "positionSide": "LONG", "positionAmt": ".1"}]
        self.algo_orders = [{"symbol": "AAAUSDT", "algoId": "hard", "clientAlgoId": "hard-client"}]
        self.events = []
        self.trades = []
        self.fail_post = False
        self.unknown_post = False
        self.query_fails = False
        self.canceled = set()

    def now(self):
        return TIME + timedelta(seconds=5)

    def latest_price(self, symbol):
        return Decimal("200")

    def aggregate_trades(self, symbol, start, end, cursor=None):
        return [row for row in self.trades if (cursor is None or row["a"] > cursor) and start.timestamp() * 1000 <= row["T"] <= end.timestamp() * 1000]

    def stop_market(self, symbol, side, position_side, trigger, client_id, *, quantity=None):
        self.events.append(("post", trigger, quantity))
        if self.fail_post:
            raise BinanceError("rejected", -4000)
        order = {"symbol": symbol, "algoId": str(len(self.events)), "clientAlgoId": client_id}
        self.algo_orders.append(order)
        if self.unknown_post:
            raise BinanceError("timeout after accepted write")
        return order

    def cancel_algo(self, symbol, algo_id):
        self.events.append(("cancel", algo_id))
        self.canceled.add(algo_id)
        self.algo_orders = [row for row in self.algo_orders if row["algoId"] != algo_id]

    def query_algo(self, symbol, client_id):
        if self.query_fails:
            raise BinanceError("query timeout")
        return next(row for row in self.algo_orders if row["clientAlgoId"] == client_id)

    def query_algo_by_id(self, symbol, algo_id):
        return {"algoId": algo_id, "algoStatus": "CANCELED" if algo_id in self.canceled else "NEW", "actualOrderId": "0"}


def trade(trade_id, seconds, price):
    return {"a": trade_id, "T": int((TIME + timedelta(seconds=seconds)).timestamp() * 1000), "p": str(price)}


def test_high_between_polls_tightens_exchange_stop_before_cancel(tmp_path):
    client = HostedClient()
    engine = opened(tmp_path, client)
    client.trades = [trade(1, 1, 150), trade(2, 2, 140)]
    engine.process_long_protection(TIME + timedelta(seconds=5))
    assert client.events == [("post", Decimal("135"), Decimal(".1"))]
    assert engine.store.open_positions()[0]["protection_peak"] == "150"
    client.trades += [trade(3, 6, 160), trade(4, 7, 140)]
    engine.process_long_protection(TIME + timedelta(seconds=10))
    assert client.events[1:] == [("post", Decimal("144"), Decimal(".1")), ("cancel", "1")]
    assert "hard" not in client.canceled
    engine.process_long_protection(TIME + timedelta(seconds=15))
    assert len(client.events) == 3  # no new high/tick: no order write


def test_failed_replacement_keeps_old_stop_and_retries_saved_target(tmp_path):
    client = HostedClient()
    engine = opened(tmp_path, client)
    client.trades = [trade(1, 1, 150)]
    engine.process_long_protection(TIME + timedelta(seconds=5))
    client.fail_post = True
    client.trades.append(trade(2, 6, 160))
    with pytest.raises(BinanceError, match="rejected"):
        engine.process_long_protection(TIME + timedelta(seconds=10))
    assert not client.canceled
    assert engine.store.open_positions()[0]["target_stop"] == "144.0"
    client.fail_post = False
    engine.process_long_protection(TIME + timedelta(seconds=15))
    assert client.events[-1] == ("cancel", "1")


def test_unknown_protection_post_is_queried_after_restart_without_second_post(tmp_path):
    client = HostedClient()
    engine = opened(tmp_path, client)
    client.trades = [trade(1, 1, 150)]
    client.unknown_post = True
    with pytest.raises(BinanceError):
        engine.process_long_protection(TIME + timedelta(seconds=5))
    path = tmp_path / "state.sqlite3"
    engine.close()
    engine = LiveEngine(_config(tmp_path), client=client, store=StateStore(path))
    engine.reconcile()
    engine.process_long_protection(TIME + timedelta(seconds=10))
    assert len(client.events) == 1
    assert engine.store.protection_orders()[0]["status"] == "ACTIVE"


def test_filled_exit_is_recovered_before_stop_attribution(tmp_path):
    client = _RecoveryClient({"status": "FILLED", "executedQty": ".1", "avgPrice": "110"})
    engine = opened(tmp_path, client)
    engine.store.require_exit("held", "PROTECTION")
    engine.store.begin_exit_attempt("held", ".1", "PROTECTION", "exit-1")
    engine.reconcile()
    assert engine.store.open_positions() == []
    assert client.orders == []
    assert engine.store.connection.execute("SELECT reason FROM executions WHERE role = 'EXIT'").fetchone()[0] == "PROTECTION"


def test_cumulative_partial_fill_is_atomic_and_idempotent(tmp_path):
    engine = opened(tmp_path)
    store = engine.store
    attempt = store.begin_exit_attempt("held", ".1", "PROTECTION", "exit")
    response = {"status": "PARTIALLY_FILLED", "executedQty": ".04", "avgPrice": "101"}
    store.apply_exit(attempt, response)
    store.apply_exit(attempt, response)
    assert Decimal(store.open_positions()[0]["quantity"]) == Decimal(".06")
    assert len(store.unsettled_exits()) == 1
    with pytest.raises(StateError):
        store.apply_exit(attempt, dict(response, executedQty=".11"))
    assert store.connection.execute("SELECT quantity FROM executions WHERE client_order_id = 'exit'").fetchone()[0] == ".04"
    store.apply_exit(attempt, dict(response, status="CANCELED", executedQty=".06"))
    assert Decimal(store.open_positions()[0]["quantity"]) == Decimal(".04")
    assert not store.unsettled_exits()


def test_legacy_exit_signal_survives_failed_post_and_processed_bar(tmp_path):
    bar = {"symbol": "AAAUSDT", "open_time": TIME, "open": 110., "high": 111., "low": 100., "close": 110., "quote_volume": 1., "trade_count": 1}
    client = _CatchupProtectionClient(pl.DataFrame([bar]))
    client.market_order = lambda *args: (_ for _ in ()).throw(BinanceError("rejected", -4000))
    engine = opened(tmp_path, client, version="minute-v1")
    engine.store.update_protection("held", True, "150", (TIME - timedelta(minutes=1)).isoformat())
    with pytest.raises(BinanceError):
        engine.process_long_protection(TIME + timedelta(minutes=1))
    assert engine.store.open_positions()[0]["exit_required"] == "PROTECTION"
    client.market_order = _Client().market_order
    engine.process_due_exits(TIME + timedelta(minutes=1))
    assert engine.store.open_positions() == []


def test_shadow_reference_and_chunked_path_match_frozen_engine(tmp_path):
    rules = _config(tmp_path).strategy.values["long"]
    task = {"entry_time": (TIME + timedelta(minutes=1)).isoformat(), "planned_exit_time": (TIME + timedelta(minutes=4)).isoformat()}
    bars = [{"open_time": TIME, "open": 80., "close": 100.},
            {"open_time": TIME + timedelta(minutes=1), "open": 105., "low": 99., "high": 140., "close": 130.},
            {"open_time": TIME + timedelta(minutes=2), "open": 130., "low": 110., "high": 160., "close": 150.},
            {"open_time": TIME + timedelta(minutes=3), "open": 150., "low": 100., "high": 155., "close": 120.}]
    progress, error = advance_shadow(task, bars[:2], TIME + timedelta(minutes=2), rules)
    assert error is None and progress["reference"] == 100.
    task["progress_json"] = json.dumps(progress)
    progress, error = advance_shadow(task, bars[2:], TIME + timedelta(minutes=4), rules)
    baseline = _simulate_long(bars[1:], 100., TIME + timedelta(minutes=4), None, rules)
    assert (progress["active"], progress["maximum"], progress["exit_time"]) == (baseline.activated, baseline.max_retrace, baseline.exit_time.isoformat())


def test_delayed_shadow_uses_exact_range_and_hard_stop_completes_early(tmp_path):
    bars = pl.DataFrame([{"symbol": "AAAUSDT", "open_time": TIME, "open": 80., "close": 100., "low": 80., "high": 100.},
                         {"symbol": "AAAUSDT", "open_time": TIME + timedelta(minutes=1), "open": 100., "close": 65., "low": 60., "high": 100.}])
    client = _CatchupProtectionClient(bars)
    engine = LiveEngine(_config(tmp_path), client=client, store=StateStore(tmp_path / "state.sqlite3"))
    engine.store.add_shadow_task("shadow", "AAAUSDT", (TIME + timedelta(minutes=1)).isoformat(), (TIME + timedelta(hours=18)).isoformat())
    engine.process_due_shadows(TIME + timedelta(minutes=20))
    assert client.requests[0]["start_time"] == TIME
    assert client.requests[0]["end_time"] == TIME + timedelta(minutes=20, milliseconds=-1)
    assert engine.store.shadow_history()[0]["shadow_exit_time"] == (TIME + timedelta(minutes=2)).isoformat()
    assert engine.store.shadow_history_stats() == (1, 0)


def test_shadow_gap_does_not_complete_or_block_risk_loop(tmp_path):
    bars = pl.DataFrame([{"symbol": "AAAUSDT", "open_time": TIME, "open": 100., "close": 100., "low": 100., "high": 100.}])
    engine = LiveEngine(_config(tmp_path), client=_CatchupProtectionClient(bars), store=StateStore(tmp_path / "state.sqlite3"))
    engine.store.add_shadow_task("shadow", "AAAUSDT", (TIME + timedelta(minutes=1)).isoformat(), (TIME + timedelta(hours=1)).isoformat())
    engine.process_due_shadows(TIME + timedelta(hours=2))
    task = engine.store.due_shadow_tasks((TIME + timedelta(hours=2)).isoformat())[0]
    assert "missing shadow minute" in task["last_error"]
    assert json.loads(task["progress_json"])["next_time"] == (TIME + timedelta(minutes=1)).isoformat()
    assert engine.store.shadow_history_stats() == (0, 0)


def test_slow_research_jobs_do_not_wait_in_risk_thread(tmp_path):
    engine = opened(tmp_path)
    engine._shadow_job = Future()
    engine._decision_job = (TIME, Future())
    engine._background_work(TIME)
    assert not engine._shadow_job.done() and not engine._decision_job[1].done()
    engine.process_due_exits(TIME + timedelta(hours=18))
    assert engine.store.open_positions() == []


def test_market_filters_and_price_use_execution_venue_and_cache_catalogue(tmp_path):
    calls = []
    def transport(method, url, params, headers, timeout):
        calls.append(url)
        if url.endswith("ticker/price"):
            return {"price": "123"}
        return {"symbols": [{"symbol": "AAAUSDT", "filters": [
            {"filterType": "LOT_SIZE", "stepSize": ".01", "minQty": ".01", "maxQty": "100"},
            {"filterType": "MARKET_LOT_SIZE", "stepSize": ".05", "minQty": ".1", "maxQty": "10"},
            {"filterType": "PRICE_FILTER", "tickSize": ".1"}, {"filterType": "MIN_NOTIONAL", "notional": "5"}]}]}
    client = BinanceRest(_config(tmp_path), transport)
    filters = client.symbol_filters("AAAUSDT")
    assert (filters["step_size"], filters["min_qty"], filters["max_qty"]) == (Decimal(".05"), Decimal(".1"), Decimal("10"))
    assert client.symbol_filters("AAAUSDT") == filters
    assert client.latest_price("AAAUSDT") == Decimal("123")
    assert len(calls) == 2 and all(url.startswith("https://demo-fapi.binance.com/") for url in calls)


def test_first_short_selection_count_survives_failed_execution_and_restart(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    engine = LiveEngine(_config(tmp_path), client=_Client(), store=store)
    six = TIME.replace(hour=6)
    engine._now = lambda: six
    engine.reconcile = lambda: None
    candidates = [dict(_candidate("short", symbol, six), testnet_eligible=False) for symbol in ("A", "B")]
    engine.process_decision(six, collected=(["A", "B"], candidates, 2))
    assert store.short_count(six.isoformat()) == 2 and store.units_open() == 0
    engine.close()
    store = StateStore(tmp_path / "state.sqlite3")
    count = store.short_count(six.isoformat())
    eight = six.replace(hour=8)
    # Even if the reconstructed 06:00 feature set is empty, its persisted
    # selection count still leaves only one of the two eligible 08:00 shorts.
    features = pl.DataFrame([{"symbol": symbol, "decision_time": eight, "r24_rank": 1, "r4_rank_change_rank": 100,
                              "volume_diff_v1_rank": 1, "volume_diff_v4_rank": 1, "market_r1_p10": 0.} for symbol in ("C", "D")])
    selected = short_signals(features, six.replace(hour=0), six.replace(hour=0) + timedelta(days=1), _config(tmp_path).strategy,
                             first_selected={six.date(): count})
    assert selected.height == 1


def test_decision_retry_reuses_admission_units_and_frozen_sample(tmp_path):
    client = _Client()
    client.ensure_symbol_config = lambda symbol: None
    engine = LiveEngine(_config(tmp_path), client=client, store=StateStore(tmp_path / "state.sqlite3"))
    engine._now = lambda: TIME
    engine.reconcile = lambda: None
    candidates = [dict(_candidate("long", symbol, TIME, rank), testnet_eligible=True) for rank, symbol in enumerate(("AAAUSDT", "BBBUSDT"), 1)]
    original_open = engine._open
    fail = True
    def open_once(admission, **kwargs):
        if admission.candidate["symbol"] == "BBBUSDT" and fail:
            raise BinanceError("temporary second entry failure")
        original_open(admission, **kwargs)
    engine._open = open_once
    with pytest.raises(BinanceError):
        engine.process_decision(TIME, collected=(["AAAUSDT", "BBBUSDT"], candidates, None))
    assert engine.store.units_open() == 1
    fail = False
    engine.process_decision(TIME)
    assert [row["units"] for row in engine.store.open_positions()] == [1, 1]
    assert len(client.orders) == 2
    assert {row["protection_allowed_retrace"] for row in engine.store.open_positions()} == {.3}
    assert engine.store.decision_done(TIME.isoformat())


def test_recovered_entry_uses_sample_frozen_before_submission(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    store.create_intent({"intent_id": "pending", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG",
                         "decision_time": TIME.isoformat(), "planned_exit_time": (TIME + timedelta(hours=18)).isoformat(),
                         "units": 1, "priority_score": 1., "client_order_id": "pending-entry"}, protection=(.123, 100, 40), execution_version=EXECUTION_VERSION)
    client = _RecoveryClient({"status": "FILLED", "executedQty": ".1", "avgPrice": "101", "updateTime": int((TIME + timedelta(seconds=20)).timestamp() * 1000)})
    engine = LiveEngine(_config(tmp_path), client=client, store=store)
    engine._recover_pending_entries()
    position = store.open_positions()[0]
    assert (position["protection_allowed_retrace"], position["protection_history_count"], position["protection_activated_count"]) == (.123, 100, 40)
    assert position["filled_at"] == (TIME + timedelta(seconds=20)).isoformat()
    assert position["execution_version"] == EXECUTION_VERSION


def test_gap_does_not_advance_market_cursor_or_loosen_stop(tmp_path):
    client = HostedClient()
    engine = opened(tmp_path, client)
    client.trades = [trade(1, 1, 150)]
    engine.process_long_protection(TIME + timedelta(seconds=5))
    client.trades.append(trade(3, 6, 200))
    with pytest.raises(BinanceError, match="trade gap"):
        engine.process_long_protection(TIME + timedelta(seconds=10))
    position = engine.store.open_positions()[0]
    assert (position["trade_cursor"], position["protection_peak"], position["target_stop"]) == (1, "150", "135.0")
    assert len(client.events) == 1


def test_old_trade_observed_after_deadline_does_not_retroactively_extend(tmp_path):
    client = HostedClient()
    engine = opened(tmp_path, client)
    planned = TIME + timedelta(hours=18)
    client.trades = [trade(1, 17 * 3600, 150)]
    now = planned + timedelta(seconds=5)
    engine.process_long_protection(now)
    position = engine.store.open_positions()[0]
    assert position["protection_activated_at"] == now.isoformat()
    assert not engine._activate_extension_if_qualified(position, now)


def test_canceled_profit_stop_is_reinstalled_with_hard_stop_intact(tmp_path):
    client = HostedClient()
    engine = opened(tmp_path, client)
    client.trades = [trade(1, 1, 150)]
    engine.process_long_protection(TIME + timedelta(seconds=5))
    client.cancel_algo("AAAUSDT", "1")
    engine.reconcile()
    engine.process_long_protection(TIME + timedelta(seconds=10))
    assert len([event for event in client.events if event[0] == "post"]) == 2
    assert "hard" not in client.canceled


def test_cancel_trigger_race_retains_fill_attribution(tmp_path):
    client = HostedClient()
    engine = opened(tmp_path, client)
    order = engine.store.begin_protection_order("held", "135", ".1")
    engine.store.set_protection_order(order["id"], "ACTIVE", "profit")
    order = engine.store.protection_orders()[0]
    client.cancel_algo = lambda *args: (_ for _ in ()).throw(BinanceError("no order", -2011))
    client.query_algo_by_id = lambda symbol, algo: {"actualOrderId": "filled" if algo == "profit" else "0", "algoStatus": "FINISHED"}
    engine._cancel_protection(engine.store.open_positions()[0], order)
    assert engine.store.protection_orders()[0]["status"] == "TRIGGERED"
    client.query_order_by_id = lambda *args: {"status": "FILLED", "executedQty": ".1", "avgPrice": "135", "clientOrderId": "profit-fill"}
    client.exchange_positions = []
    engine.reconcile()
    assert not engine.store.open_positions()
    assert engine.store.connection.execute("SELECT reason FROM executions WHERE client_order_id = 'profit-fill'").fetchone()[0] == "PROTECTION"


def test_thirty_closed_hours_recover_six_oclock_history_at_eight(tmp_path):
    eight = TIME.replace(hour=8)
    rows = [{"symbol": "A", "open_time": eight - timedelta(hours=30 - index), "open": 100., "high": 100., "low": 100.,
             "close": 100., "quote_volume": 1., "trade_count": 1} for index in range(30)]
    config = _config(tmp_path).strategy
    thirty = build_features(pl.DataFrame(rows), config).filter(pl.col("decision_time") == eight.replace(hour=6))
    twenty_nine = build_features(pl.DataFrame(rows[1:]), config).filter(pl.col("decision_time") == eight.replace(hour=6))
    assert thirty.get_column("r4_rank_change_rank").to_list() == [1]
    assert twenty_nine.get_column("r4_rank_change_rank").to_list() == [None]


def test_legacy_partial_commit_is_reconstructed_from_entry_and_exit_fills(tmp_path):
    engine = opened(tmp_path, _RecoveryClient({"status": "CANCELED", "executedQty": ".06", "avgPrice": "101"}))
    store = engine.store
    store.record_execution("held", "entry", "ENTRY", {"status": "FILLED", "executedQty": ".1", "avgPrice": "100"})
    store.begin_exit_attempt("held", ".1", "PROTECTION", "partial")
    store.record_execution("held", "partial", "EXIT", {"status": "PARTIALLY_FILLED", "executedQty": ".04", "avgPrice": "101"})
    store.finish_exit_attempt("partial", "PARTIAL")
    # Simulate old code crashing between recording a partial exit and updating quantity.
    with store.transaction() as connection:
        connection.execute("ALTER TABLE exit_attempts DROP COLUMN applied_quantity")
    store.close()
    engine.store = StateStore(tmp_path / "state.sqlite3")
    assert Decimal(engine.store.open_positions()[0]["quantity"]) == Decimal(".06")
    assert engine.store.open_positions()[0]["exit_required"] == "PROTECTION"
    engine._recover_pending_exits()
    assert Decimal(engine.store.open_positions()[0]["quantity"]) == Decimal(".04")


def test_unversioned_live_shadow_is_rebuilt_once_and_seed_deduplicates(tmp_path):
    path = tmp_path / "state.sqlite3"
    store = StateStore(path)
    store.add_shadow_task("old-live-id", "AAAUSDT", TIME.isoformat(), (TIME + timedelta(hours=1)).isoformat())
    store.complete_shadow_task("old-live-id", (TIME + timedelta(hours=1)).isoformat(), True, .2)
    with store.transaction() as connection:
        connection.execute("UPDATE shadow_tasks SET definition_version = 'unverified'")
        connection.execute("UPDATE shadow_history SET definition_version = 'unverified'")
    store.close()
    store = StateStore(path)
    task = store.due_shadow_tasks((TIME + timedelta(hours=2)).isoformat())[0]
    assert task["entry_time"] == (TIME + timedelta(minutes=1)).isoformat()
    assert task["shadow_id"] == f"long:AAAUSDT:{TIME.isoformat()}"
    assert store.shadow_history_stats() == (0, 0)
    record = (task["shadow_id"], (TIME + timedelta(hours=1)).isoformat(), True, .1)
    store.seed_shadow_history([record])
    store.complete_shadow_task(*record)
    assert store.shadow_history_stats() == (1, 1)
    store.close()
    store = StateStore(path)
    assert store.connection.execute("SELECT entry_time, definition_version FROM shadow_tasks").fetchone()[:] == ((TIME + timedelta(minutes=1)).isoformat(), SHADOW_VERSION)


def test_busy_trade_feed_paginates_without_refetching_previous_page(tmp_path):
    requests = []
    def transport(method, url, params, headers, timeout):
        requests.append(dict(params))
        if len(requests) == 1:
            return [trade(index, 1, 100) for index in range(1000)]
        return [trade(1000, 2, 150)]
    client = BinanceRest(_config(tmp_path), transport)
    rows = client.aggregate_trades("AAAUSDT", TIME, TIME + timedelta(seconds=5))
    assert len(rows) == 1001 and rows[-1]["p"] == "150"
    assert requests[1] == {"symbol": "AAAUSDT", "limit": "1000", "fromId": "1000"}


def test_lost_hard_stop_response_is_cleaned_after_fallback_exit(tmp_path):
    client = _Client(stop_fails=True)
    engine = opened(tmp_path, client)
    with pytest.raises(BinanceError, match="stop unavailable"):
        engine._install_stop(engine.store.open_positions()[0])
    assert engine.store.open_positions() == []
    assert len(engine.store.pending_hard_stops()) == 1
    canceled = []
    client.query_algo = lambda *args: {"algoId": "late-hard"}
    client.cancel_algo = lambda symbol, algo: canceled.append(algo)
    engine._recover_hard_stops()
    assert canceled == ["late-hard"] and not engine.store.pending_hard_stops()


def test_sizing_delay_cannot_send_entry_after_deadline(tmp_path):
    client = _Client()
    engine = LiveEngine(_config(tmp_path), client=client, store=StateStore(tmp_path / "state.sqlite3"))
    engine._now = lambda: TIME + timedelta(seconds=120)
    engine._open(Admission(_candidate("long", "AAAUSDT", TIME), 1), deadline=TIME + timedelta(seconds=120))
    assert not client.orders and not engine.store.pending_intents()


def test_late_snapshot_keeps_shadow_candidates_without_entering(tmp_path):
    client = _Client()
    engine = LiveEngine(_config(tmp_path), client=client, store=StateStore(tmp_path / "state.sqlite3"))
    engine._now = lambda: TIME + timedelta(seconds=125)
    engine.reconcile = lambda: None
    candidate = dict(_candidate("long", "AAAUSDT", TIME), testnet_eligible=True)
    assert engine.process_decision(TIME, collected=(["AAAUSDT"], [candidate], None)) == []
    assert engine.store.decision_done(TIME.isoformat())
    assert len(engine.store.due_shadow_tasks((TIME + timedelta(minutes=3)).isoformat())) == 1
    assert not client.orders


def test_prepared_entry_rechecks_deadline_and_capacity(tmp_path):
    client = _Client()
    engine = opened(tmp_path, client)
    with engine.store.transaction() as connection:
        connection.execute("UPDATE positions SET sizing_version = 'drawdown-2x-v2'")
    engine._now = lambda: TIME
    admission = Admission(_candidate("long", "AAAUSDT", TIME), 1)
    prepared = engine._prepare_open(admission)
    assert prepared is not None
    assert engine._open(admission, prepared=prepared) == "SKIPPED"
    engine.store.close_position("held")
    engine._now = lambda: TIME + timedelta(seconds=120)
    assert engine._open(admission, prepared=prepared, deadline=TIME + timedelta(seconds=120)) == "SKIPPED"
    assert not client.orders


def test_unknown_exchange_position_does_not_prevent_known_stop_recovery(tmp_path):
    client = _RecoveryClient()
    engine = opened(tmp_path, client)
    with engine.store.transaction() as connection:
        connection.execute("UPDATE positions SET stop_algo_id = NULL WHERE intent_id = 'held'")
    client.exchange_positions = [
        {"symbol": "AAAUSDT", "positionSide": "LONG", "positionAmt": ".1"},
        {"symbol": "BTCUSDT", "positionSide": "LONG", "positionAmt": "1"},
    ]
    assert engine.reconcile() is False
    assert client.stop_calls == 1
    assert engine.store.open_positions()[0]["stop_algo_id"] == "recovered-stop"
    with pytest.raises(StateError, match="quantities are unresolved"):
        engine._completed_equity(TIME)
    assert engine.store.latest_equity_minute() is None


def test_state_error_in_one_exit_does_not_skip_other_positions(tmp_path):
    engine = opened(tmp_path)
    held = engine.store.open_positions()[0]
    engine.store.open_positions = lambda: [held, dict(held, intent_id="second")]
    visited = []
    def close(position, reason):
        visited.append(position["intent_id"])
        if position["intent_id"] == "held":
            raise StateError("unresolved first position")
    engine._close = close
    with pytest.raises(BinanceError, match="unresolved first position"):
        engine.process_due_exits(TIME + timedelta(days=1))
    assert visited == ["held", "second"]


def test_trade_fill_linking_is_scoped_to_symbol_in_both_arrival_orders(tmp_path):
    engine = opened(tmp_path)
    store = engine.store
    store.record_execution("held", "entry", "ENTRY", {"orderId": 7, "status": "FILLED", "executedQty": ".1", "avgPrice": "100"})
    fills = [{"symbol": symbol, "id": 1, "orderId": 7, "positionSide": "LONG", "side": "BUY",
              "qty": ".1", "price": "100", "commission": "0", "commissionAsset": "USDT",
              "time": int(TIME.timestamp() * 1000)} for symbol in ("AAAUSDT", "BBBUSDT")]
    store.record_trade_fills(fills)
    assert [(r["symbol"], r["intent_id"]) for r in store.connection.execute("SELECT * FROM trade_fills ORDER BY symbol")] == [("AAAUSDT", "held"), ("BBBUSDT", None)]
    with store.transaction() as connection:
        connection.execute("UPDATE trade_fills SET intent_id = NULL")
    store.record_execution("held", "entry", "ENTRY", {"orderId": 7, "status": "FILLED", "executedQty": ".1", "avgPrice": "100"})
    assert [(r["symbol"], r["intent_id"]) for r in store.connection.execute("SELECT * FROM trade_fills ORDER BY symbol")] == [("AAAUSDT", "held"), ("BBBUSDT", None)]
