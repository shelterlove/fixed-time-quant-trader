from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
from shutil import copyfile
import sqlite3

import pytest
import polars as pl

from fixed_time.config import load_config
from fixed_time.live.binance import BinanceError, BinanceRest, quantize_down, stop_trigger_price
from fixed_time.live.config import LiveConfig, LongExtensionConfig, load_live_config
from fixed_time.live.dashboard import read_status
from fixed_time.live.engine import LiveEngine
from fixed_time.live.state import RuntimeLock, StateError, StateStore
from fixed_time.live.strategy import Admission, allowed_retrace, entry_notional, exposure_multiplier, long_protection_update, plan_admissions


ROOT = Path(__file__).resolve().parents[1]


def _config(tmp_path: Path) -> LiveConfig:
    return LiveConfig(
        root=ROOT, strategy=load_config(ROOT), market_data_base_url="https://fapi.binance.com",
        trading_base_url="https://demo-fapi.binance.com", api_key="key", api_secret="secret", trading_enabled=True,
        database_path=tmp_path / "runtime.sqlite3",
        long_extension=LongExtensionConfig(True, 4, 24, 4), account_poll_seconds=5, idle_reconcile_seconds=60, decision_deadline_seconds=120,
        request_timeout_seconds=1, max_attempts=3, max_concurrent_market_requests=1, leverage=2,
    )


def _candidate(strategy: str, symbol: str, time: datetime, priority: int = 1) -> dict:
    return {
        "trade_id": f"live:{strategy}:{symbol}:{time.isoformat()}", "strategy": strategy, "symbol": symbol,
        "position_side": "LONG" if strategy == "long" else "SHORT", "decision_time": time,
        "entry_time": time, "planned_exit_time": time + timedelta(hours=1), "requested_units": 1,
        "priority_score": priority, "priority_order": priority,
    }


def _position(intent_id: str, strategy: str, symbol: str, units: int, priority: int, time: datetime, **extra) -> dict:
    return {
        "intent_id": intent_id, "strategy": strategy, "symbol": symbol, "position_side": "LONG" if strategy == "long" else "SHORT",
        "units": units, "priority_score": priority, "decision_time": time.isoformat(),
        **extra,
    }


def test_live_config_defaults_to_disabled(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
    monkeypatch.setenv("TRADING_ENABLED", "false")
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "state.sqlite3"))
    config = load_live_config(ROOT)
    assert config.trading_enabled is False
    assert config.trading_base_url == "https://demo-fapi.binance.com"
    assert config.long_extension == LongExtensionConfig(True, 4, 24, 4)


def test_runtime_lock_blocks_a_second_state_writer(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    first, second = RuntimeLock(path), RuntimeLock(path)
    first.acquire()
    try:
        with pytest.raises(StateError, match="already locked"):
            second.acquire()
    finally:
        first.release()
    second.acquire()
    second.release()


def test_state_preserves_logical_units(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC).isoformat()
    store.create_intent({"intent_id": "i1", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": time,
                         "planned_exit_time": time, "units": 2, "priority_score": 1.0, "client_order_id": "i1"})
    store.open_position("i1", "1", "100", .3)
    assert store.units_open() == 2
    assert store.units_open("long") == 2
    assert store.open_positions()[0]["protection_allowed_retrace"] == pytest.approx(.3)
    store.close_position("i1")
    assert store.units_open() == 0


def test_state_migrates_existing_execution_ledger_without_losing_rows(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("""CREATE TABLE executions (
        client_order_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL, role TEXT NOT NULL, reason TEXT,
        exchange_order_id TEXT, status TEXT NOT NULL, quantity TEXT NOT NULL, average_price TEXT NOT NULL,
        recorded_at TEXT NOT NULL
    )""")
    connection.execute("INSERT INTO executions VALUES ('old', 'intent', 'ENTRY', NULL, '1', 'FILLED', '.1', '100', '2026-09-01T00:00:00+00:00')")
    connection.commit()
    connection.close()
    store = StateStore(path)
    columns = {row[1] for row in store.connection.execute("PRAGMA table_info(executions)")}
    row = store.connection.execute("SELECT client_order_id, recorded_at FROM executions WHERE client_order_id = 'old'").fetchone()
    assert "executed_at" in columns
    assert tuple(row) == ("old", "2026-09-01T00:00:00+00:00")


def test_state_migrates_existing_positions_to_the_original_scheduled_exit(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("""CREATE TABLE positions (
        intent_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, strategy TEXT NOT NULL, position_side TEXT NOT NULL,
        units INTEGER NOT NULL, quantity TEXT NOT NULL, entry_price TEXT NOT NULL, planned_exit_time TEXT NOT NULL,
        stop_algo_id TEXT, protection_active INTEGER NOT NULL DEFAULT 0, protection_peak TEXT,
        protection_allowed_retrace REAL, protection_last_bar_time TEXT, status TEXT NOT NULL,
        opened_at TEXT NOT NULL, updated_at TEXT NOT NULL
    )""")
    planned = "2026-09-02T08:01:00+00:00"
    connection.execute("INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       ("legacy", "AAAUSDT", "long", "LONG", 1, ".1", "100", planned, "stop", 1, "130", .3, planned, "OPEN", planned, planned))
    connection.commit()
    connection.close()
    store = StateStore(path)
    row = store.connection.execute("SELECT scheduled_exit_time, protection_activated_at, extension_active FROM positions WHERE intent_id = 'legacy'").fetchone()
    assert tuple(row) == (planned, None, 0)


def test_seeded_shadow_history_is_idempotent(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    records = [("research:1", "2026-09-01T00:00:00+00:00", True, .1)]
    assert store.seed_shadow_history(records) == 1
    assert store.seed_shadow_history(records) == 0
    assert store.shadow_history_stats() == (1, 1)


def test_exchange_trade_and_income_sync_is_idempotent(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    now = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store.create_intent({"intent_id": "trade", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG",
                         "decision_time": now.isoformat(), "planned_exit_time": now.isoformat(), "units": 1,
                         "priority_score": 1.0, "client_order_id": "entry"})
    store.record_execution("trade", "entry", "ENTRY", {"orderId": 10, "status": "FILLED", "executedQty": "1", "avgPrice": "100"})
    fill = {"symbol": "AAAUSDT", "id": 7, "orderId": 10, "positionSide": "LONG", "side": "BUY", "qty": "1",
            "price": "100", "quoteQty": "100", "realizedPnl": "0", "commission": ".05", "commissionAsset": "USDT",
            "time": int(now.timestamp() * 1000)}
    income = {"incomeType": "FUNDING_FEE", "tranId": 8, "symbol": "AAAUSDT", "tradeId": "",
              "income": ".1", "asset": "USDT", "time": int(now.timestamp() * 1000)}
    assert store.record_trade_fills([fill]) == 1
    assert store.record_trade_fills([fill]) == 0
    assert store.record_income_events([income]) == 1
    assert store.record_income_events([income]) == 0
    assert store.usdt_income_between((now-timedelta(seconds=1)).isoformat(), now.isoformat()) == Decimal(".1")


def test_live_seed_uses_packaged_history_when_research_outputs_are_absent(tmp_path: Path) -> None:
    seed_dir = tmp_path / "seed"
    seed_dir.mkdir()
    copyfile(ROOT / "seed" / "initial_shadow_history.csv", seed_dir / "initial_shadow_history.csv")
    store = StateStore(tmp_path / "state.sqlite3")
    engine = LiveEngine(replace(_config(tmp_path), root=tmp_path), store=store)
    assert engine.seed_shadow_history(datetime(2026, 9, 1, tzinfo=UTC)) == 164
    assert store.shadow_history_stats() == (164, 48)


def test_live_seed_accepts_sufficient_existing_runtime_history_without_a_fresh_seed(tmp_path: Path) -> None:
    cutoff = datetime(2028, 9, 1, tzinfo=UTC)
    store = StateStore(tmp_path / "state.sqlite3")
    records = [
        (f"runtime:{index}", (cutoff - timedelta(days=1, minutes=index)).isoformat(), index < 30, .1 if index < 30 else None)
        for index in range(100)
    ]
    store.seed_shadow_history(records)
    engine = LiveEngine(replace(_config(tmp_path), root=tmp_path), store=store)
    assert engine.seed_shadow_history(cutoff) == 0
    assert store.shadow_history_stats() == (100, 30)


def test_one_long_uses_two_units_and_two_longs_use_one_each(tmp_path: Path) -> None:
    config = _config(tmp_path).strategy
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    one = plan_admissions([_candidate("long", "AAAUSDT", time)], [], config)
    two = plan_admissions([_candidate("long", "AAAUSDT", time), _candidate("long", "BBBUSDT", time, 2)], [], config)
    assert [item.units for item in one] == [2]
    assert [item.units for item in two] == [1, 1]


def test_long_evicts_worst_short_to_make_capacity(tmp_path: Path) -> None:
    config = _config(tmp_path).strategy
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    positions = [
        _position("s1", "short", "S1USDT", 1, 10, time),
        _position("s2", "short", "S2USDT", 1, 20, time),
        _position("s3", "short", "S3USDT", 1, 30, time),
    ]
    admissions = plan_admissions([_candidate("long", "NEWUSDT", time)], positions, config)
    assert admissions[0].units == 2
    assert admissions[0].evict_intent_ids == ("s3", "s2")


def test_strict_d3_uses_only_an_already_idle_unit_without_eviction(tmp_path: Path) -> None:
    config = _config(tmp_path).strategy
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    positions = [
        _position("long", "long", "HELDUSDT", 2, 1, time),
    ]
    admission = plan_admissions([_candidate("long", "NEWUSDT", time)], positions, config)[0]
    assert admission.units == 1
    assert admission.candidate["allocation_mode"] == "SINGLE_UNIT_FRAGMENT"
    assert admission.evict_intent_ids == ()


def test_strict_d3_can_use_one_unit_left_by_two_shorts(tmp_path: Path) -> None:
    config = _config(tmp_path).strategy
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    positions = [
        _position("s1", "short", "S1USDT", 1, 10, time),
        _position("s2", "short", "S2USDT", 1, 20, time),
    ]
    admission = plan_admissions([_candidate("long", "NEWUSDT", time)], positions, config)[0]
    assert admission.units == 1
    assert admission.evict_intent_ids == ()


def test_long_evicts_shorts_before_a_post_four_hour_extension(tmp_path: Path) -> None:
    config = _config(tmp_path).strategy
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    positions = [
        _position("short", "short", "SUSDT", 1, 20, time),
        _position("extended", "long", "EUSDT", 2, 1, time - timedelta(hours=8), extension_active=1,
                  extension_release_time=(time - timedelta(minutes=1)).isoformat()),
    ]
    admissions = plan_admissions([_candidate("long", "NEWUSDT", time)], positions, config)
    assert admissions[0].units == 2
    assert admissions[0].evict_intent_ids == ("short", "extended")


def test_infeasible_new_long_does_not_evict_existing_shorts(tmp_path: Path) -> None:
    decision = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store = StateStore(tmp_path / "state.sqlite3")
    for index in range(3):
        intent_id = f"short-{index}"
        store.create_intent({"intent_id": intent_id, "strategy": "short", "symbol": f"S{index}USDT",
                             "position_side": "SHORT", "decision_time": decision.isoformat(),
                             "planned_exit_time": (decision+timedelta(hours=2)).isoformat(), "units": 1,
                             "priority_score": float(index), "client_order_id": intent_id})
        store.open_position(intent_id, "1", "100", stop_algo_id=f"stop-{index}")
    client = _Client()
    client.balance = lambda: Decimal("0")  # type: ignore[method-assign]
    engine = LiveEngine(_config(tmp_path), client=client, store=store)
    engine._now = lambda: decision
    engine.reconcile = lambda: True  # type: ignore[method-assign]
    store.record_equity_minute(decision.isoformat(), Decimal("100"), Decimal("0"), Decimal("100"), Decimal("100"), Decimal("0"))
    candidate = dict(_candidate("long", "AAAUSDT", decision), testnet_eligible=True)

    assert engine.process_decision(decision, collected=(["AAAUSDT"], [candidate], None)) == []
    assert {row["intent_id"] for row in store.open_positions()} == {"short-0", "short-1", "short-2"}


def test_reconciliation_block_still_records_long_shadow_candidate(tmp_path: Path) -> None:
    decision = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store = StateStore(tmp_path / "state.sqlite3")
    engine = LiveEngine(_config(tmp_path), client=_Client(), store=store)
    engine._now = lambda: decision
    engine.reconcile = lambda: False  # type: ignore[method-assign]
    candidate = dict(_candidate("long", "AAAUSDT", decision), testnet_eligible=True)

    assert engine.process_decision(decision, collected=(["AAAUSDT"], [candidate], None)) == []
    assert store.connection.execute("SELECT COUNT(*) FROM shadow_tasks").fetchone()[0] == 1
    run = store.connection.execute("SELECT candidate_count, admission_count FROM decision_runs").fetchone()
    assert tuple(run) == (1, 0)


def test_drawdown_sizing_and_p90_fallback(tmp_path: Path) -> None:
    config = _config(tmp_path).strategy
    assert exposure_multiplier(Decimal(".24999"), config) == Decimal("1.0")
    assert exposure_multiplier(Decimal(".25"), config) == Decimal("1.05")
    assert exposure_multiplier(Decimal(".30"), config) == Decimal("1.10")
    assert exposure_multiplier(Decimal(".35"), config) == Decimal("1.15")
    assert exposure_multiplier(Decimal(".40"), config) == Decimal("1.20")
    assert exposure_multiplier(Decimal(".45"), config) == Decimal("1.25")
    assert exposure_multiplier(Decimal(".50"), config) == Decimal("1.30")
    assert exposure_multiplier(Decimal(".90"), config) == Decimal("1.30")
    assert entry_notional(Decimal("60"), 2, Decimal("1.1"), config) == Decimal("44.0")
    entry = datetime(2026, 9, 1, tzinfo=UTC)
    assert allowed_retrace([], entry, config) == pytest.approx(.3)


def test_long_protection_activates_then_checks_following_minute(tmp_path: Path) -> None:
    config = _config(tmp_path).strategy
    position = {"entry_price": "100", "protection_active": 0, "protection_peak": "100", "protection_allowed_retrace": .1}
    should_exit, active, peak = long_protection_update(position, {"high": 130, "low": 100}, config)
    assert (should_exit, active, peak) == (False, True, Decimal("130"))
    position.update({"protection_active": int(active), "protection_peak": str(peak)})
    should_exit, active, peak = long_protection_update(position, {"high": 129, "low": 116}, config)
    assert should_exit is True
    assert active is True


def test_order_rounding_is_protective() -> None:
    assert quantize_down(Decimal("1.239"), Decimal(".01")) == Decimal("1.23")
    assert stop_trigger_price(Decimal("70.01"), Decimal(".1"), "SELL") == Decimal("70.1")
    assert stop_trigger_price(Decimal("129.99"), Decimal(".1"), "BUY") == Decimal("129.9")


def test_private_production_request_is_rejected(tmp_path: Path) -> None:
    client = BinanceRest(_config(tmp_path), transport=lambda *_: {})
    with pytest.raises(BinanceError, match="restricted"):
        client._request("GET", "https://fapi.binance.com", "/fapi/v1/account", signed=True)


def test_tradable_symbols_are_the_public_and_testnet_intersection(tmp_path: Path) -> None:
    public = {
        "symbols": [
            {"symbol": "BTCUSDT", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
            {"symbol": "UAIUSDT", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
        ]
    }
    testnet = {
        "symbols": [
            {"symbol": "BTCUSDT", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
            {"symbol": "PAUSEDUSDT", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "BREAK"},
        ]
    }

    def transport(_method, url, _params, _headers, _timeout):
        return testnet if url.startswith("https://demo-fapi.binance.com") else public

    assert BinanceRest(_config(tmp_path), transport=transport).tradable_symbols() == ["BTCUSDT"]


def test_order_filters_come_from_testnet(tmp_path: Path) -> None:
    public = {"symbols": [{"symbol": "BTCUSDT", "filters": []}]}
    testnet = {"symbols": [{"symbol": "BTCUSDT", "filters": [
        {"filterType": "LOT_SIZE", "stepSize": ".01", "minQty": ".01"},
        {"filterType": "PRICE_FILTER", "tickSize": ".1"},
        {"filterType": "MIN_NOTIONAL", "notional": "5"},
    ]}]}

    def transport(_method, url, _params, _headers, _timeout):
        return testnet if url.startswith("https://demo-fapi.binance.com") else public

    filters = BinanceRest(_config(tmp_path), transport=transport).symbol_filters("BTCUSDT")
    assert filters == {"max_qty": Decimal("Infinity"), "step_size": Decimal(".01"), "min_qty": Decimal(".01"), "tick_size": Decimal(".1"), "min_notional": Decimal("5")}


def test_market_post_is_not_retried_after_transport_error(tmp_path: Path) -> None:
    calls = []

    def broken(*_args):
        calls.append(1)
        raise BinanceError("timeout")

    client = BinanceRest(_config(tmp_path), transport=broken)
    with pytest.raises(BinanceError, match="timeout"):
        client.market_order("BTCUSDT", "BUY", "LONG", Decimal(".001"), "test")
    assert len(calls) == 1


def test_minute_kline_query_can_request_an_exact_closed_interval(tmp_path: Path) -> None:
    captured: dict[str, str] = {}
    start = datetime(2026, 9, 1, 14, tzinfo=UTC)

    def transport(_method, _url, params, _headers, _timeout):
        captured.update(params)
        return [[int(start.timestamp() * 1000), "100", "101", "99", "100", "0", 0, "1", 1]]

    client = BinanceRest(_config(tmp_path), transport=transport)
    client.klines("AAAUSDT", "1m", 1, start_time=start, end_time=start + timedelta(minutes=1, milliseconds=-1))
    assert captured == {
        "symbol": "AAAUSDT", "interval": "1m", "limit": "1",
        "startTime": str(int(start.timestamp() * 1000)),
        "endTime": str(int((start + timedelta(minutes=1, milliseconds=-1)).timestamp() * 1000)),
    }


def test_configure_symbol_sets_isolated_two_x_only_when_needed(tmp_path: Path) -> None:
    symbol_config_calls = 0
    paths: list[str] = []

    def transport(_method, url, _params, _headers, _timeout):
        nonlocal symbol_config_calls
        paths.append(url)
        if url.endswith("/symbolConfig"):
            symbol_config_calls += 1
            return [{"symbol": "AAAUSDT", "marginType": "crossed" if symbol_config_calls == 1 else "isolated", "leverage": 20 if symbol_config_calls == 1 else 2}]
        return {"code": 200, "msg": "success"}

    client = BinanceRest(_config(tmp_path), transport=transport)
    client.configure_symbol("AAAUSDT")
    assert any(path.endswith("/marginType") for path in paths)
    assert any(path.endswith("/leverage") for path in paths)


def test_completed_mark_and_wallet_queries_use_the_testnet_profile(tmp_path: Path) -> None:
    minute_end = datetime(2026, 9, 1, 14, 1, tzinfo=UTC)
    calls: list[tuple[str, str, dict[str, str], dict[str, str]]] = []

    def transport(method, url, params, headers, _timeout):
        calls.append((method, url, dict(params), dict(headers)))
        if url.endswith("/fapi/v3/account"):
            return {"assets": [{"asset": "USDT", "walletBalance": "123.45"}]}
        if url.endswith("/fapi/v1/markPriceKlines"):
            return [[int((minute_end - timedelta(minutes=1)).timestamp() * 1000), "0", "0", "0", "101.25"]]
        raise AssertionError(url)

    client = BinanceRest(_config(tmp_path), transport=transport)
    assert client.wallet_balance() == Decimal("123.45")
    assert client.mark_price_close("AAAUSDT", minute_end) == Decimal("101.25")
    account_call, mark_call = calls
    assert account_call[1].startswith("https://demo-fapi.binance.com") and "signature" in account_call[2]
    assert mark_call[1].startswith("https://demo-fapi.binance.com")
    assert mark_call[2] == {
        "symbol": "AAAUSDT", "interval": "1m", "limit": "1",
        "startTime": str(int((minute_end - timedelta(minutes=1)).timestamp() * 1000)),
        "endTime": str(int((minute_end - timedelta(milliseconds=1)).timestamp() * 1000)),
    }


class _Client:
    def __init__(self, stop_fails: bool = False):
        self.stop_fails = stop_fails
        self.orders: list[tuple[str, str, str, Decimal]] = []

    def ensure_symbol_config(self, symbol: str) -> None:
        assert symbol == "AAAUSDT"

    def configure_symbol(self, symbol: str) -> None:
        self.ensure_symbol_config(symbol)

    def balance(self) -> Decimal:
        return Decimal("100")

    def wallet_balance(self) -> Decimal:
        return Decimal("100")

    def symbol_filters(self, symbol: str):
        return {"step_size": Decimal(".001"), "min_qty": Decimal(".001"), "tick_size": Decimal(".1"), "min_notional": Decimal("5")}

    def latest_price(self, symbol: str) -> Decimal:
        return Decimal("100")

    def market_order(self, symbol: str, side: str, position_side: str, quantity: Decimal, client_order_id: str):
        self.orders.append((symbol, side, position_side, quantity))
        return {"status": "FILLED", "executedQty": format(quantity, "f"), "avgPrice": "100"}

    def stop_market(self, *_args):
        if self.stop_fails:
            raise BinanceError("stop unavailable")
        return {"algoId": "99"}

    def query_algo(self, *_args):
        raise BinanceError("not found")

    def cancel_algo(self, *_args) -> None:
        return None

    def positions(self) -> list[dict]:
        return []


class _EquityClient(_Client):
    def __init__(self, marks: dict[datetime, Decimal]):
        super().__init__()
        self.marks = marks
        self.mark_requests: list[datetime] = []

    def wallet_balance(self) -> Decimal:
        return Decimal("100")

    def mark_price_close(self, _symbol: str, minute_end: datetime) -> Decimal:
        self.mark_requests.append(minute_end)
        return self.marks[minute_end]


def test_completed_equity_persists_peak_idempotently_and_blocks_a_gap(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    store = StateStore(path)
    first = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store.create_intent({"intent_id": "equity", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG",
                         "decision_time": first.isoformat(), "planned_exit_time": (first + timedelta(hours=1)).isoformat(),
                         "units": 1, "priority_score": 1.0, "client_order_id": "equity"})
    store.open_position("equity", "1", "100", .3, "stop", filled_at=(first - timedelta(minutes=1)).isoformat())
    client = _EquityClient({
        first: Decimal("120"), first + timedelta(minutes=1): Decimal("90"), first + timedelta(minutes=3): Decimal("90"),
    })
    engine = LiveEngine(_config(tmp_path), client=client, store=store)
    first_row = engine._completed_equity(first)
    assert (first_row["equity"], first_row["peak_equity"], first_row["drawdown"]) == ("120", "120", "0")
    assert engine._completed_equity(first) == first_row
    assert client.mark_requests == [first]

    engine.close()
    restarted = LiveEngine(_config(tmp_path), client=client, store=StateStore(path))
    second = restarted._completed_equity(first + timedelta(minutes=1))
    assert (second["equity"], second["peak_equity"], second["drawdown"]) == ("90", "120", "0.25")
    assert restarted._sizing_snapshot(first + timedelta(minutes=1))["exposure_multiplier"] == "1.05"
    restarted._completed_equity(first + timedelta(minutes=3))
    assert [row["code"] for row in restarted.store.active_entry_blocks()] == ["EQUITY_GAP"]
    assert restarted.store.latest_equity_minute()["minute_end"] == (first + timedelta(minutes=1)).isoformat()


def test_completed_equity_repairs_a_short_restart_gap_when_marks_are_complete(tmp_path: Path) -> None:
    first = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store = StateStore(tmp_path / "state.sqlite3")
    store.create_intent({"intent_id": "equity", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG",
                         "decision_time": first.isoformat(), "planned_exit_time": (first+timedelta(hours=1)).isoformat(),
                         "units": 1, "priority_score": 1.0, "client_order_id": "equity"})
    store.open_position("equity", "1", "100", filled_at=(first-timedelta(minutes=1)).isoformat())
    marks = {first+timedelta(minutes=offset): Decimal(str(100+offset)) for offset in range(4)}
    engine = LiveEngine(_config(tmp_path), client=_EquityClient(marks), store=store)
    engine._completed_equity(first)
    repaired = engine._completed_equity(first+timedelta(minutes=3))
    assert repaired["equity"] == "103"
    assert store.connection.execute("SELECT COUNT(*) FROM equity_minutes").fetchone()[0] == 4
    assert store.active_entry_blocks() == []


def test_completed_equity_removes_post_cutoff_wallet_income(tmp_path: Path) -> None:
    cutoff = datetime(2026, 9, 1, 14, tzinfo=UTC)

    class IncomeClient(_Client):
        def wallet_balance(self) -> Decimal:
            return Decimal("110")
        def user_trades(self, _symbol: str, **_kwargs):
            return []
        def income_history(self, **_kwargs):
            return [{"incomeType": "TRANSFER", "tranId": 1, "symbol": "", "tradeId": "", "income": "10",
                     "asset": "USDT", "time": int((cutoff+timedelta(seconds=20)).timestamp()*1000)}]

    engine = LiveEngine(_config(tmp_path), client=IncomeClient(), store=StateStore(tmp_path / "state.sqlite3"))
    engine._now = lambda: cutoff + timedelta(seconds=30)
    row = engine._completed_equity(cutoff)
    assert (row["wallet_balance"], row["equity"]) == ("100", "100")


def test_position_at_minute_cutoff_is_rebuilt_from_exchange_execution_times(tmp_path: Path) -> None:
    cutoff = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store = StateStore(tmp_path / "state.sqlite3")
    store.create_intent({"intent_id": "trade", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG",
                         "decision_time": cutoff.isoformat(), "planned_exit_time": cutoff.isoformat(), "units": 1,
                         "priority_score": 1.0, "client_order_id": "entry"})
    store.record_execution("trade", "entry", "ENTRY", {"orderId": 1, "status": "FILLED", "executedQty": "1",
                           "avgPrice": "100", "updateTime": int((cutoff-timedelta(seconds=10)).timestamp()*1000)})
    store.open_position("trade", "1", "100")
    store.record_execution("trade", "exit", "EXIT", {"orderId": 2, "status": "FILLED", "executedQty": "1",
                           "avgPrice": "110", "updateTime": int((cutoff+timedelta(seconds=10)).timestamp()*1000)})
    store.close_position("trade")
    assert store.positions_at(cutoff.isoformat())[0]["quantity"] == Decimal("1")
    assert store.positions_at((cutoff+timedelta(minutes=1)).isoformat()) == []


def test_completed_equity_never_uses_a_live_price_as_a_historical_mark(tmp_path: Path) -> None:
    first = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store = StateStore(tmp_path / "state.sqlite3")
    store.create_intent({"intent_id": "held", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG",
                         "decision_time": first.isoformat(), "planned_exit_time": (first + timedelta(hours=1)).isoformat(),
                         "units": 1, "priority_score": 1.0, "client_order_id": "held"})
    store.open_position("held", ".1", "100", .3, "stop", filled_at=(first - timedelta(minutes=1)).isoformat())

    class NoHistoricalMarkClient(_Client):
        mark_price_close = None

    with pytest.raises(StateError, match="completed mark-price"):
        LiveEngine(_config(tmp_path), client=NoHistoricalMarkClient(), store=store)._completed_equity(first)


def test_legacy_position_can_install_a_stop_without_a_two_x_reconfiguration(tmp_path: Path) -> None:
    first = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store = StateStore(tmp_path / "state.sqlite3")
    store.create_intent({"intent_id": "legacy", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG",
                         "decision_time": first.isoformat(), "planned_exit_time": (first + timedelta(hours=1)).isoformat(),
                         "units": 1, "priority_score": 1.0, "client_order_id": "legacy"})
    store.open_position("legacy", ".1", "100", .3)

    class LegacyClient(_Client):
        def ensure_symbol_config(self, _symbol: str) -> None:
            raise AssertionError("stop recovery must not reconfigure a held position")

    engine = LiveEngine(_config(tmp_path), client=LegacyClient(), store=store)
    engine._install_stop(store.open_positions()[0])
    assert store.open_positions()[0]["stop_algo_id"] == "99"


def test_decision_batch_reuses_one_persisted_sizing_snapshot(tmp_path: Path) -> None:
    decision = datetime(2026, 9, 1, 14, tzinfo=UTC)
    client = _DecisionClient(decision)
    store = StateStore(tmp_path / "state.sqlite3")
    engine = LiveEngine(_config(tmp_path), client=client, store=store)
    candidates = [
        dict(_candidate("long", "AAAUSDT", decision), testnet_eligible=True),
        dict(_candidate("long", "BBBUSDT", decision, 2), testnet_eligible=True),
    ]
    admissions = engine.process_decision(decision, collected=(["AAAUSDT", "BBBUSDT"], candidates, None))
    assert [item.units for item in admissions] == [1, 1]
    positions = store.open_positions()
    assert len(positions) == 2
    assert {row["pre_entry_equity"] for row in positions} == {"100"}
    assert {row["pre_entry_drawdown"] for row in positions} == {"0"}
    assert {row["exposure_multiplier"] for row in positions} == {"1.0"}
    assert len({row["target_notional"] for row in positions}) == 1
    assert {Decimal(str(row["filled_notional"])) for row in positions} == {Decimal("33.2")}


def test_configuration_failure_cannot_send_an_unprotected_entry(tmp_path: Path) -> None:
    class ConfigurationFailureClient(_Client):
        def configure_symbol(self, _symbol: str) -> None:
            raise BinanceError("cannot set isolated 2x")

    store = StateStore(tmp_path / "state.sqlite3")
    client = ConfigurationFailureClient()
    engine = LiveEngine(_config(tmp_path), client=client, store=store)
    with pytest.raises(BinanceError, match="isolated 2x"):
        engine._open(Admission(_candidate("long", "AAAUSDT", datetime(2026, 9, 1, 14, tzinfo=UTC)), 2))
    assert client.orders == []
    assert store.pending_intents() == []


def test_reconciliation_blocks_new_entries_on_existing_leverage_mismatch(tmp_path: Path) -> None:
    class ConfigurationMismatchClient(_RecoveryClient):
        def ensure_symbol_config(self, _symbol: str) -> None:
            raise BinanceError("AAAUSDT must be isolated at 2x leverage")

    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC).isoformat()
    store.create_intent({"intent_id": "legacy", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG",
                         "decision_time": time, "planned_exit_time": time, "units": 1, "priority_score": 1.0,
                         "client_order_id": "legacy"})
    store.open_position("legacy", ".1", "100", .3, "stop")
    client = ConfigurationMismatchClient()
    client.exchange_positions = [{"symbol": "AAAUSDT", "positionSide": "LONG", "positionAmt": ".1"}]
    client.algo_orders = [{"symbol": "AAAUSDT", "algoId": "stop"}]
    engine = LiveEngine(_config(tmp_path), client=client, store=store)
    assert engine.reconcile() is False
    assert store.active_entry_blocks()[0]["code"] == "ACCOUNT_CONFIGURATION"


def test_entry_blocks_are_deduplicated_and_visible_to_the_dashboard(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    store = StateStore(path)
    store.block_entry("UNKNOWN_EXCHANGE_POSITION", "unknown exchange positions: [('BTCUSDT', 'LONG')]")
    store.block_entry("UNKNOWN_EXCHANGE_POSITION", "unknown exchange positions: [('BTCUSDT', 'LONG')]")
    block = store.active_entry_blocks()[0]
    assert (block["code"], block["occurrences"]) == ("UNKNOWN_EXCHANGE_POSITION", 2)
    assert read_status(path)["active_blocks"][0]["code"] == "UNKNOWN_EXCHANGE_POSITION"
    store.resolve_entry_block("UNKNOWN_EXCHANGE_POSITION")
    assert store.active_entry_blocks() == []


class _RecoveryClient(_Client):
    def __init__(self, order: dict | None = None):
        super().__init__()
        self.order = order
        self.exchange_positions: list[dict] = []
        self.algo_orders: list[dict] = []
        self.stop_calls = 0

    def query_order(self, *_args) -> dict:
        if self.order is None:
            raise BinanceError("not found")
        return self.order

    def positions(self) -> list[dict]:
        return self.exchange_positions

    def open_orders(self) -> list[dict]:
        return []

    def open_algo_orders(self) -> list[dict]:
        return list(self.algo_orders)

    def stop_market(self, symbol: str, _side: str, _position_side: str, _trigger: Decimal, client_algo_id: str) -> dict:
        self.stop_calls += 1
        self.algo_orders.append({"algoId": "recovered-stop", "clientAlgoId": client_algo_id, "symbol": symbol})
        return {"algoId": "recovered-stop"}


class _ProtectionClient(_Client):
    def __init__(self, bar_time: datetime):
        super().__init__()
        self.bar_time = bar_time

    def klines(self, _symbol: str, _interval: str, _limit: int, **_kwargs) -> pl.DataFrame:
        return pl.DataFrame([{
            "symbol": "AAAUSDT", "open_time": self.bar_time, "open": 100., "high": 130., "low": 100., "close": 120.,
            "quote_volume": 1., "trade_count": 1,
        }])


class _CatchupProtectionClient(_Client):
    def __init__(self, frame: pl.DataFrame):
        super().__init__()
        self.frame = frame
        self.requests: list[dict] = []

    def klines(self, _symbol: str, _interval: str, _limit: int, **kwargs) -> pl.DataFrame:
        self.requests.append(kwargs)
        return self.frame


class _ExchangeClockClient:
    def __init__(self, now: datetime):
        self._now = now

    def now(self) -> datetime:
        return self._now


class _DecisionClient(_RecoveryClient):
    def __init__(self, now: datetime):
        super().__init__()
        self._now = now

    def now(self) -> datetime:
        return self._now

    def market_data_symbols(self) -> list[str]:
        return ["BTCUSDT", "UAIUSDT"]

    def trading_symbols(self) -> list[str]:
        return ["BTCUSDT"]

    def ensure_symbol_config(self, _symbol: str) -> None:
        return None


class _LifecycleDecisionClient(_DecisionClient):
    def __init__(self, now: datetime):
        super().__init__(now)
        self.exchange_positions: list[dict] = []
        self.algo_orders: list[dict] = []

    def market_order(self, symbol: str, side: str, position_side: str, quantity: Decimal, client_order_id: str) -> dict:
        self.orders.append((symbol, side, position_side, quantity))
        if position_side != "SHORT":
            raise AssertionError("this lifecycle fixture only models a short")
        if side == "SELL":
            self.exchange_positions = [{"symbol": symbol, "positionSide": "SHORT", "positionAmt": format(-quantity, "f")}]
        elif side == "BUY":
            self.exchange_positions = []
        else:
            raise AssertionError(f"unexpected short order side: {side}")
        return {"status": "FILLED", "executedQty": format(quantity, "f"), "avgPrice": "100"}

    def stop_market(self, symbol: str, _side: str, _position_side: str, _trigger: Decimal, client_algo_id: str) -> dict:
        self.algo_orders.append({"algoId": "lifecycle-stop", "clientAlgoId": client_algo_id, "symbol": symbol})
        return {"algoId": "lifecycle-stop"}

    def cancel_algo(self, _symbol: str, algo_id: str) -> None:
        self.algo_orders = [order for order in self.algo_orders if order["algoId"] != algo_id]


class _PartialCloseClient(_Client):
    def __init__(self):
        super().__init__()
        self.exchange_quantity = Decimal(".1")
        self.exit_ids: list[str] = []

    def market_order(self, symbol: str, side: str, position_side: str, quantity: Decimal, client_order_id: str) -> dict:
        self.orders.append((symbol, side, position_side, quantity))
        if side == "SELL":
            self.exit_ids.append(client_order_id)
            filled = min(quantity, Decimal(".05"))
            self.exchange_quantity -= filled
            return {"orderId": str(len(self.exit_ids)), "status": "FILLED", "executedQty": format(filled, "f"), "avgPrice": "99"}
        return {"status": "FILLED", "executedQty": format(quantity, "f"), "avgPrice": "100"}

    def positions(self) -> list[dict]:
        if self.exchange_quantity == 0:
            return []
        return [{"symbol": "AAAUSDT", "positionSide": "LONG", "positionAmt": format(self.exchange_quantity, "f")}]


class _TriggeredStopClient(_RecoveryClient):
    def query_algo_by_id(self, _symbol: str, algo_id: str) -> dict:
        assert algo_id == "stop-1"
        return {"algoId": algo_id, "actualOrderId": "fill-1", "status": "FINISHED"}

    def query_order_by_id(self, _symbol: str, order_id: str) -> dict:
        assert order_id == "fill-1"
        return {
            "orderId": order_id, "clientOrderId": "exchange-stop-fill-1", "status": "FILLED",
            "executedQty": ".1", "avgPrice": "70", "updateTime": 1788271200000,
        }


class _IdleReconcileClient:
    def __init__(self):
        self.position_calls = 0
        self.open_order_calls = 0
        self.open_algo_order_calls = 0

    def positions(self) -> list[dict]:
        self.position_calls += 1
        return []

    def open_orders(self, _symbol: str | None = None) -> list[dict]:
        self.open_order_calls += 1
        return []

    def open_algo_orders(self, _symbol: str | None = None) -> list[dict]:
        self.open_algo_order_calls += 1
        return []


class _SmokeFailureClient(_Client):
    def __init__(self):
        super().__init__()
        self.cancelled: list[str] = []
        self.sell_attempts = 0

    def market_order(self, symbol: str, side: str, position_side: str, quantity: Decimal, client_order_id: str) -> dict:
        self.orders.append((symbol, side, position_side, quantity))
        if side == "SELL":
            self.sell_attempts += 1
            if self.sell_attempts == 1:
                return {"status": "NEW", "executedQty": "0", "avgPrice": "0"}
        return {"status": "FILLED", "executedQty": format(quantity, "f"), "avgPrice": "100"}

    def cancel_algo(self, _symbol: str, algo_id: str) -> None:
        self.cancelled.append(algo_id)

    def cancel_order(self, _symbol: str, _client_id: str) -> dict:
        return {"status": "CANCELED", "executedQty": "0", "avgPrice": "0"}


class _MissingAverageSmokeClient(_Client):
    def __init__(self):
        super().__init__()
        self.queries: list[str] = []
        self.cancelled: list[str] = []

    def market_order(self, symbol: str, side: str, position_side: str, quantity: Decimal, client_order_id: str) -> dict:
        self.orders.append((symbol, side, position_side, quantity))
        return {"status": "FILLED", "executedQty": format(quantity, "f")}

    def query_order(self, _symbol: str, client_order_id: str) -> dict:
        self.queries.append(client_order_id)
        return {"status": "FILLED", "executedQty": ".05", "avgPrice": "100"}

    def cancel_algo(self, _symbol: str, algo_id: str) -> None:
        self.cancelled.append(algo_id)


def test_entry_creates_exchange_stop_and_persistent_position(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    client = _Client()
    engine = LiveEngine(config, client=client, store=store)
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    engine._open(Admission(_candidate("long", "AAAUSDT", time), 2))
    position = store.open_positions()[0]
    assert position["units"] == 2
    assert position["stop_algo_id"] == "99"
    assert client.orders == [("AAAUSDT", "BUY", "LONG", Decimal(".664"))]


def test_stop_setup_failure_flattens_filled_entry(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    client = _Client(stop_fails=True)
    engine = LiveEngine(config, client=client, store=store)
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    with pytest.raises(BinanceError, match="stop unavailable"):
        engine._open(Admission(_candidate("long", "AAAUSDT", time), 2))
    assert store.open_positions() == []
    assert [(side, position_side) for _, side, position_side, _ in client.orders] == [("BUY", "LONG"), ("SELL", "LONG")]


def test_reconcile_recovers_pending_filled_entry_and_installs_stop(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC).isoformat()
    store.create_intent({"intent_id": "pending", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": time,
                         "planned_exit_time": time, "units": 1, "priority_score": 1.0, "client_order_id": "ft-e-l-AAAUSDT-2609011400"})
    client = _RecoveryClient({"orderId": "1", "status": "FILLED", "executedQty": ".1", "avgPrice": "100"})
    client.exchange_positions = [{"symbol": "AAAUSDT", "positionSide": "LONG", "positionAmt": ".1"}]
    engine = LiveEngine(config, client=client, store=store)
    engine.reconcile()
    position = store.open_positions()[0]
    assert position["quantity"] == "0.1"
    assert position["stop_algo_id"] == "recovered-stop"
    assert client.stop_calls == 1


def test_reconcile_adopts_exchange_stop_created_before_local_persistence(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store.create_intent({"intent_id": "open", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": time.isoformat(),
                         "planned_exit_time": time.isoformat(), "units": 1, "priority_score": 1.0, "client_order_id": "ft-e-l-AAAUSDT-2609011400"})
    store.open_position("open", ".1", "100", .3)
    client = _RecoveryClient()
    client.exchange_positions = [{"symbol": "AAAUSDT", "positionSide": "LONG", "positionAmt": ".1"}]
    engine = LiveEngine(config, client=client, store=store)
    client.algo_orders = [{"algoId": "existing-stop", "clientAlgoId": engine._stop_client_id(store.open_positions()[0]), "symbol": "AAAUSDT"}]
    engine.reconcile()
    assert store.open_positions()[0]["stop_algo_id"] == "existing-stop"
    assert client.stop_calls == 0


def test_reconcile_records_actual_exchange_stop_fill(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC).isoformat()
    store.create_intent({"intent_id": "stopped", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": time,
                         "planned_exit_time": time, "units": 1, "priority_score": 1.0, "client_order_id": "ft-e-l-AAAUSDT-2609011400"})
    store.open_position("stopped", ".1", "100", .3, "stop-1")
    client = _TriggeredStopClient()
    engine = LiveEngine(config, client=client, store=store)
    engine.reconcile()
    assert store.open_positions() == []
    execution = store.connection.execute("SELECT * FROM executions WHERE intent_id = 'stopped'").fetchone()
    assert execution is not None
    assert dict(execution)["reason"] == "EXCHANGE_STOP"
    assert dict(execution)["quantity"] == ".1"
    assert dict(execution)["average_price"] == "70"
    assert dict(execution)["executed_at"] == "2026-09-01T14:00:00+00:00"


def test_light_reconcile_skips_global_open_order_queries_when_idle(tmp_path: Path) -> None:
    client = _IdleReconcileClient()
    engine = LiveEngine(_config(tmp_path), client=client, store=StateStore(tmp_path / "state.sqlite3"))
    engine.reconcile(full=False)
    assert client.position_calls == 1
    assert client.open_order_calls == 0
    assert client.open_algo_order_calls == 0


def test_long_protection_does_not_reprocess_activation_bar_after_restart(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store.create_intent({"intent_id": "open", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": time.isoformat(),
                         "planned_exit_time": (time + timedelta(hours=1)).isoformat(), "units": 1, "priority_score": 1.0, "client_order_id": "ft-e-l-AAAUSDT-2609011400"})
    store.open_position("open", ".1", "100", .1, "stop")
    store.update_protection("open", False, "100", time.isoformat())
    bar_time = time + timedelta(minutes=1)
    client = _ProtectionClient(bar_time)
    LiveEngine(config, client=client, store=store).process_long_protection(bar_time + timedelta(minutes=1))
    assert store.open_positions()[0]["protection_last_bar_time"] == bar_time.isoformat()
    LiveEngine(config, client=client, store=store).process_long_protection(bar_time + timedelta(minutes=1))
    assert client.orders == []


def test_long_protection_replays_every_unprocessed_completed_minute_after_a_gap(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store.create_intent({"intent_id": "catchup", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": time.isoformat(),
                         "planned_exit_time": (time + timedelta(hours=1)).isoformat(), "units": 1, "priority_score": 1.0, "client_order_id": "ft-e-l-AAAUSDT-2609011400"})
    store.open_position("catchup", ".1", "100", .1, "stop")
    store.update_protection("catchup", True, "130", time.isoformat())
    frame = pl.DataFrame([
        {"symbol": "AAAUSDT", "open_time": time + timedelta(minutes=1), "open": 125., "high": 129., "low": 120., "close": 125., "quote_volume": 1., "trade_count": 1},
        {"symbol": "AAAUSDT", "open_time": time + timedelta(minutes=2), "open": 120., "high": 129., "low": 115., "close": 120., "quote_volume": 1., "trade_count": 1},
        {"symbol": "AAAUSDT", "open_time": time + timedelta(minutes=3), "open": 120., "high": 129., "low": 119., "close": 120., "quote_volume": 1., "trade_count": 1},
    ])
    client = _CatchupProtectionClient(frame)
    engine = LiveEngine(config, client=client, store=store)
    engine.process_long_protection(time + timedelta(minutes=4))
    assert store.open_positions() == []
    assert [(side, position_side) for _, side, position_side, _ in client.orders] == [("SELL", "LONG")]
    assert client.requests == [{"start_time": time + timedelta(minutes=1), "end_time": time + timedelta(minutes=4, milliseconds=-1)}]


def test_long_protection_uses_the_following_bar_after_activation_during_catchup(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store.create_intent({"intent_id": "activate-then-exit", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": time.isoformat(),
                         "planned_exit_time": (time + timedelta(hours=1)).isoformat(), "units": 1, "priority_score": 1.0, "client_order_id": "activate-then-exit"})
    store.open_position("activate-then-exit", ".1", "100", .1, "stop")
    frame = pl.DataFrame([
        {"symbol": "AAAUSDT", "open_time": time, "open": 100., "high": 130., "low": 100., "close": 125., "quote_volume": 1., "trade_count": 1},
        {"symbol": "AAAUSDT", "open_time": time + timedelta(minutes=1), "open": 120., "high": 129., "low": 110., "close": 112., "quote_volume": 1., "trade_count": 1},
    ])
    engine = LiveEngine(config, client=_CatchupProtectionClient(frame), store=store)
    engine.process_long_protection(time + timedelta(minutes=2))
    assert store.open_positions() == []


def test_due_long_extends_only_after_a_recent_persisted_activation(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    planned = datetime(2026, 9, 2, 8, 1, tzinfo=UTC)
    store.create_intent({"intent_id": "extend", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": (planned - timedelta(hours=18)).isoformat(),
                         "planned_exit_time": planned.isoformat(), "units": 1, "priority_score": 1.0, "client_order_id": "extend"})
    store.open_position("extend", ".1", "100", .1, "stop")
    store.update_protection("extend", True, "130", (planned - timedelta(minutes=1)).isoformat(), (planned - timedelta(hours=1)).isoformat())
    LiveEngine(config, client=_Client(), store=store).process_due_exits(planned)
    position = store.open_positions()[0]
    assert position["extension_active"] == 1
    assert position["extension_release_time"] == (planned + timedelta(hours=4)).isoformat()
    assert position["scheduled_exit_time"] == (planned + timedelta(hours=24)).isoformat()


def test_due_extension_closes_at_its_24_hour_cap(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    planned = datetime(2026, 9, 2, 8, 1, tzinfo=UTC)
    store.create_intent({"intent_id": "cap", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": (planned - timedelta(hours=18)).isoformat(),
                         "planned_exit_time": planned.isoformat(), "units": 1, "priority_score": 1.0, "client_order_id": "cap"})
    store.open_position("cap", ".1", "100", .1, "stop")
    store.activate_extension("cap", (planned + timedelta(hours=24)).isoformat(), (planned + timedelta(hours=4)).isoformat())
    engine = LiveEngine(config, client=_Client(), store=store)
    engine.process_due_exits(planned + timedelta(hours=24))
    assert store.open_positions() == []
    reason = store.connection.execute("SELECT reason FROM executions WHERE intent_id = 'cap' AND role = 'EXIT'").fetchone()[0]
    assert reason == "EXTENSION_CAP"


def test_decision_deadline_uses_the_exchange_aligned_clock(tmp_path: Path) -> None:
    decision = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store = StateStore(tmp_path / "state.sqlite3")
    engine = LiveEngine(_config(tmp_path), client=_ExchangeClockClient(decision + timedelta(seconds=121)), store=store)
    assert engine.process_decision(decision) == []
    assert store.decision_done(decision.isoformat())


def test_decision_preserves_public_ranks_then_filters_unorderable_testnet_candidate(monkeypatch, tmp_path: Path) -> None:
    decision = datetime(2026, 9, 2, 8, tzinfo=UTC)
    store = StateStore(tmp_path / "state.sqlite3")
    client = _DecisionClient(decision)
    engine = LiveEngine(_config(tmp_path), client=client, store=store)
    hourly = pl.DataFrame([{
        "symbol": "BTCUSDT", "open_time": decision - timedelta(hours=1), "open": 100., "high": 101., "low": 99., "close": 100.,
        "quote_volume": 1., "trade_count": 1,
    }, {
        "symbol": "UAIUSDT", "open_time": decision - timedelta(hours=1), "open": 100., "high": 101., "low": 99., "close": 100.,
        "quote_volume": 1., "trade_count": 1,
    }])
    seen: list[str] = []

    def candidates(snapshot, _decision_time, _config, **kwargs):
        seen.extend(snapshot.get_column("symbol").unique().sort().to_list())
        return [_candidate("long", "UAIUSDT", decision)], 0

    monkeypatch.setattr("fixed_time.live.engine.decision_candidates", candidates)
    assert engine.process_decision(decision, hourly=hourly) == []
    assert seen == ["BTCUSDT", "UAIUSDT"]
    row = store.connection.execute("SELECT universe_size, candidate_count, admission_count, status, detail_json FROM decision_runs").fetchone()
    assert tuple(row)[:4] == (2, 1, 0, "COMPLETE")
    assert json.loads(row[4])["candidates"][0]["testnet_eligible"] is False
    assert store.connection.execute("SELECT COUNT(*) FROM shadow_tasks").fetchone()[0] == 1


def test_full_short_decision_path_filters_testnet_only_after_public_signal_ranking(tmp_path: Path) -> None:
    decision = datetime(2026, 9, 2, 8, tzinfo=UTC)
    symbols = [f"S{index:03d}USDT" for index in range(99)] + ["UAIUSDT"]
    rows: list[dict] = []
    first = decision - timedelta(hours=30)
    for symbol in symbols:
        for index in range(30):
            close, volume = 100., 10.
            if symbol == "UAIUSDT":
                close, volume = 100., 1.
                if index == 20:
                    close = 100.
                elif index in {24, 25, 26, 27, 28}:
                    close = 200.
                elif index == 29:
                    close, volume = 150., 1000.
            elif symbol in symbols[:10] and index == 29:
                close = 99.
            rows.append({
                "symbol": symbol, "open_time": first + timedelta(hours=index), "open": close, "high": close,
                "low": close, "close": close, "quote_volume": volume, "trade_count": 1,
            })
    snapshot = pl.DataFrame(rows)

    blocked_client = _DecisionClient(decision)
    blocked_client.market_data_symbols = lambda: symbols  # type: ignore[method-assign]
    blocked_client.trading_symbols = lambda: [symbol for symbol in symbols if symbol != "UAIUSDT"]  # type: ignore[method-assign]
    blocked_store = StateStore(tmp_path / "blocked.sqlite3")
    blocked = LiveEngine(_config(tmp_path), client=blocked_client, store=blocked_store)
    assert blocked.process_decision(decision, hourly=snapshot) == []
    blocked_run = blocked_store.connection.execute("SELECT candidate_count, admission_count, status FROM decision_runs").fetchone()
    assert tuple(blocked_run) == (1, 0, "COMPLETE")
    assert blocked_client.orders == []

    supported_client = _LifecycleDecisionClient(decision)
    supported_client.market_data_symbols = lambda: symbols  # type: ignore[method-assign]
    supported_client.trading_symbols = lambda: symbols  # type: ignore[method-assign]
    supported_store = StateStore(tmp_path / "supported.sqlite3")
    supported = LiveEngine(_config(tmp_path), client=supported_client, store=supported_store)
    admissions = supported.process_decision(decision, hourly=snapshot)
    assert [(item.candidate["strategy"], item.candidate["symbol"], item.units) for item in admissions] == [("short", "UAIUSDT", 1)]
    position = supported_store.open_positions()[0]
    assert (position["symbol"], position["position_side"], position["stop_algo_id"]) == ("UAIUSDT", "SHORT", "lifecycle-stop")
    assert [(symbol, side, position_side) for symbol, side, position_side, _ in supported_client.orders] == [("UAIUSDT", "SELL", "SHORT")]
    supported.process_due_exits(decision + timedelta(hours=9))
    assert supported_store.open_positions() == []
    assert supported_client.exchange_positions == []
    assert supported_client.algo_orders == []
    exits = supported_store.connection.execute("SELECT role, reason FROM executions ORDER BY recorded_at").fetchall()
    assert [tuple(row) for row in exits] == [("ENTRY", None), ("EXIT", "PLANNED_EXIT")]


def test_decision_collection_window_includes_the_second_minute(tmp_path: Path) -> None:
    decision = datetime(2026, 9, 1, 14, tzinfo=UTC)
    engine = LiveEngine(_config(tmp_path), client=_ExchangeClockClient(decision), store=StateStore(tmp_path / "state.sqlite3"))
    assert engine._due_decision_time(decision + timedelta(seconds=90)) == decision
    assert engine._due_decision_time(decision + timedelta(seconds=120)) is None
    assert engine._due_decision_time(decision.replace(hour=13) + timedelta(seconds=90)) is None


def test_partial_exit_uses_a_new_persistent_client_order_id_for_the_remaining_position(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC).isoformat()
    store.create_intent({"intent_id": "partial", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": time,
                         "planned_exit_time": time, "units": 1, "priority_score": 1.0, "client_order_id": "ft-e-l-AAAUSDT-2609011400"})
    store.open_position("partial", ".1", "100", .3, "stop-1")
    client = _PartialCloseClient()
    engine = LiveEngine(config, client=client, store=store)
    with pytest.raises(BinanceError, match="remains pending"):
        engine._close(store.open_positions()[0], "PLANNED_EXIT")
    assert store.open_positions()[0]["quantity"] == "0.05"
    engine._close(store.open_positions()[0], "PLANNED_EXIT")
    assert store.open_positions() == []
    assert len(client.exit_ids) == 2
    assert client.exit_ids[0] != client.exit_ids[1]
    attempts = store.connection.execute("SELECT sequence, status FROM exit_attempts WHERE intent_id = 'partial' ORDER BY sequence").fetchall()
    assert [tuple(row) for row in attempts] == [(1, "PARTIAL"), (2, "SETTLED")]


def test_smoke_failure_settles_exit_and_cancels_stop_after_cleanup(tmp_path: Path) -> None:
    config = _config(tmp_path)
    client = _SmokeFailureClient()
    engine = LiveEngine(config, client=client, store=StateStore(tmp_path / "state.sqlite3"))
    engine.check = lambda: {"positions": [], "open_orders": [], "open_algo_orders": []}  # type: ignore[method-assign]
    with pytest.raises(BinanceError, match="SMOKE_EXIT exit remains pending"):
        engine.smoke_test("AAAUSDT")
    assert client.sell_attempts == 2
    assert client.cancelled == ["99"]


def test_smoke_fetches_average_price_before_creating_stop(tmp_path: Path) -> None:
    config = _config(tmp_path)
    client = _MissingAverageSmokeClient()
    engine = LiveEngine(config, client=client, store=StateStore(tmp_path / "state.sqlite3"))
    engine.check = lambda: {"positions": [], "open_orders": [], "open_algo_orders": []}  # type: ignore[method-assign]
    result = engine.smoke_test("AAAUSDT")
    assert result["entry_price"] == "100"
    assert len(client.queries) == 2  # Both entry and exit fills need an auditable price.
    assert client.cancelled == ["99"]
