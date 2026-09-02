from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from shutil import copyfile
import sqlite3

import pytest
import polars as pl

from fixed_time.config import load_config
from fixed_time.live.binance import BinanceError, BinanceRest, quantize_down, stop_trigger_price
from fixed_time.live.config import LiveConfig, LongExtensionConfig, load_live_config
from fixed_time.live.engine import LiveEngine
from fixed_time.live.state import StateStore
from fixed_time.live.strategy import Admission, allowed_retrace, long_protection_update, plan_admissions, unit_notional


ROOT = Path(__file__).resolve().parents[1]


def _config(tmp_path: Path) -> LiveConfig:
    return LiveConfig(
        root=ROOT, strategy=load_config(ROOT), market_data_base_url="https://fapi.binance.com",
        trading_base_url="https://demo-fapi.binance.com", api_key="key", api_secret="secret", trading_enabled=True,
        database_path=tmp_path / "runtime.sqlite3",
        long_extension=LongExtensionConfig(True, 4, 24, 4), account_poll_seconds=5, idle_reconcile_seconds=60, decision_deadline_seconds=60,
        request_timeout_seconds=1, max_attempts=3, max_concurrent_market_requests=1,
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
        _position("l1", "long", "L1USDT", 2, 1, time),
    ]
    admissions = plan_admissions([_candidate("long", "NEWUSDT", time)], positions, config)
    assert admissions[0].units == 2
    assert admissions[0].evict_intent_ids == ("s2",)


def test_long_evicts_shorts_before_a_post_four_hour_extension(tmp_path: Path) -> None:
    config = _config(tmp_path).strategy
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    positions = [
        _position("short", "short", "SUSDT", 1, 20, time),
        _position("regular", "long", "RUSDT", 2, 1, time),
        _position("extended", "long", "EUSDT", 2, 1, time - timedelta(hours=8), extension_active=1,
                  extension_release_time=(time - timedelta(minutes=1)).isoformat()),
    ]
    admissions = plan_admissions([_candidate("long", "NEWUSDT", time)], positions, config)
    assert admissions[0].units == 2
    assert admissions[0].evict_intent_ids == ("short", "extended")


def test_dynamic_unit_notional_and_p90_fallback(tmp_path: Path) -> None:
    config = _config(tmp_path).strategy
    assert unit_notional(Decimal("60"), 2, 2, config) == Decimal("40")
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


def test_configure_symbol_sets_isolated_one_x_only_when_needed(tmp_path: Path) -> None:
    symbol_config_calls = 0
    paths: list[str] = []

    def transport(_method, url, _params, _headers, _timeout):
        nonlocal symbol_config_calls
        paths.append(url)
        if url.endswith("/symbolConfig"):
            symbol_config_calls += 1
            return [{"symbol": "AAAUSDT", "marginType": "crossed" if symbol_config_calls == 1 else "isolated", "leverage": 20 if symbol_config_calls == 1 else 1}]
        return {"code": 200, "msg": "success"}

    client = BinanceRest(_config(tmp_path), transport=transport)
    client.configure_symbol("AAAUSDT")
    assert any(path.endswith("/marginType") for path in paths)
    assert any(path.endswith("/leverage") for path in paths)


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
    assert client.orders == [("AAAUSDT", "BUY", "LONG", Decimal(".4"))]


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
    engine = LiveEngine(_config(tmp_path), client=_ExchangeClockClient(decision + timedelta(seconds=61)), store=store)
    assert engine.process_decision(decision) == []
    assert store.decision_done(decision.isoformat())


def test_partial_exit_uses_a_new_persistent_client_order_id_for_the_remaining_position(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC).isoformat()
    store.create_intent({"intent_id": "partial", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": time,
                         "planned_exit_time": time, "units": 1, "priority_score": 1.0, "client_order_id": "ft-e-l-AAAUSDT-2609011400"})
    store.open_position("partial", ".1", "100", .3, "stop-1")
    client = _PartialCloseClient()
    engine = LiveEngine(config, client=client, store=store)
    with pytest.raises(BinanceError, match="did not fully fill"):
        engine._close(store.open_positions()[0], "PLANNED_EXIT")
    assert store.open_positions()[0]["quantity"] == "0.05"
    engine._close(store.open_positions()[0], "PLANNED_EXIT")
    assert store.open_positions() == []
    assert len(client.exit_ids) == 2
    assert client.exit_ids[0] != client.exit_ids[1]
    attempts = store.connection.execute("SELECT sequence, status FROM exit_attempts WHERE intent_id = 'partial' ORDER BY sequence").fetchall()
    assert [tuple(row) for row in attempts] == [(1, "PARTIAL"), (2, "SETTLED")]


def test_smoke_failure_retries_same_exit_and_cancels_stop_after_cleanup(tmp_path: Path) -> None:
    config = _config(tmp_path)
    client = _SmokeFailureClient()
    engine = LiveEngine(config, client=client, store=StateStore(tmp_path / "state.sqlite3"))
    engine.check = lambda: {"positions": [], "open_orders": [], "open_algo_orders": []}  # type: ignore[method-assign]
    with pytest.raises(BinanceError, match="smoke exit did not fill"):
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
    assert len(client.queries) == 1
    assert client.cancelled == ["99"]
