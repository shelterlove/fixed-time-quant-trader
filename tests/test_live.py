from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import polars as pl

from fixed_time.config import load_config
from fixed_time.live.binance import BinanceError, BinanceRest, quantize_down, stop_trigger_price
from fixed_time.live.config import LiveConfig, load_live_config
from fixed_time.live.engine import LiveEngine
from fixed_time.live.state import StateStore
from fixed_time.live.strategy import Admission, allowed_retrace, long_protection_update, plan_admissions, unit_notional


ROOT = Path(__file__).resolve().parents[1]


def _config(tmp_path: Path) -> LiveConfig:
    return LiveConfig(
        root=ROOT, strategy=load_config(ROOT), market_data_base_url="https://fapi.binance.com",
        trading_base_url="https://demo-fapi.binance.com", api_key="key", api_secret="secret", trading_enabled=True,
        database_path=tmp_path / "runtime.sqlite3", account_poll_seconds=5, decision_deadline_seconds=60,
        request_timeout_seconds=1, max_attempts=3, max_concurrent_market_requests=1,
    )


def _candidate(strategy: str, symbol: str, time: datetime, priority: int = 1) -> dict:
    return {
        "trade_id": f"live:{strategy}:{symbol}:{time.isoformat()}", "strategy": strategy, "symbol": symbol,
        "position_side": "LONG" if strategy == "long" else "SHORT", "decision_time": time,
        "entry_time": time, "planned_exit_time": time + timedelta(hours=1), "requested_units": 1,
        "priority_score": priority, "priority_order": priority,
    }


def _position(intent_id: str, strategy: str, symbol: str, units: int, priority: int, time: datetime) -> dict:
    return {
        "intent_id": intent_id, "strategy": strategy, "symbol": symbol, "position_side": "LONG" if strategy == "long" else "SHORT",
        "units": units, "priority_score": priority, "decision_time": time.isoformat(),
    }


def test_live_config_defaults_to_disabled(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
    monkeypatch.setenv("TRADING_ENABLED", "false")
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "state.sqlite3"))
    config = load_live_config(ROOT)
    assert config.trading_enabled is False
    assert config.trading_base_url == "https://demo-fapi.binance.com"


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


def test_seeded_shadow_history_is_idempotent(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    records = [("research:1", "2026-09-01T00:00:00+00:00", True, .1)]
    assert store.seed_shadow_history(records) == 1
    assert store.seed_shadow_history(records) == 0
    assert store.shadow_history_stats() == (1, 1)


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


class _Client:
    def __init__(self, stop_fails: bool = False):
        self.stop_fails = stop_fails
        self.orders: list[tuple[str, str, str, Decimal]] = []

    def ensure_symbol_config(self, symbol: str) -> None:
        assert symbol == "AAAUSDT"

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

    def klines(self, _symbol: str, _interval: str, _limit: int) -> pl.DataFrame:
        return pl.DataFrame([{
            "symbol": "AAAUSDT", "open_time": self.bar_time, "open": 100., "high": 130., "low": 100., "close": 120.,
            "quote_volume": 1., "trade_count": 1,
        }])


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


def test_long_protection_does_not_reprocess_activation_bar_after_restart(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    time = datetime(2026, 9, 1, 14, tzinfo=UTC)
    store.create_intent({"intent_id": "open", "strategy": "long", "symbol": "AAAUSDT", "position_side": "LONG", "decision_time": time.isoformat(),
                         "planned_exit_time": (time + timedelta(hours=1)).isoformat(), "units": 1, "priority_score": 1.0, "client_order_id": "ft-e-l-AAAUSDT-2609011400"})
    store.open_position("open", ".1", "100", .1, "stop")
    bar_time = time + timedelta(minutes=1)
    client = _ProtectionClient(bar_time)
    LiveEngine(config, client=client, store=store).process_long_protection(bar_time + timedelta(minutes=1))
    assert store.open_positions()[0]["protection_last_bar_time"] == bar_time.isoformat()
    LiveEngine(config, client=client, store=store).process_long_protection(bar_time + timedelta(minutes=1))
    assert client.orders == []


def test_smoke_failure_retries_same_exit_and_cancels_stop_after_cleanup(tmp_path: Path) -> None:
    config = _config(tmp_path)
    client = _SmokeFailureClient()
    engine = LiveEngine(config, client=client, store=StateStore(tmp_path / "state.sqlite3"))
    engine.check = lambda: {"positions": [], "open_orders": [], "open_algo_orders": []}  # type: ignore[method-assign]
    with pytest.raises(BinanceError, match="smoke exit did not fill"):
        engine.smoke_test("AAAUSDT")
    assert client.sell_attempts == 2
    assert client.cancelled == ["99"]
