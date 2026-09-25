from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from fixed_time.config import LiveConfig, StrategyConfig
from fixed_time.engine import Engine
from fixed_time.exchange import ExchangeError
from fixed_time.state import Store


def live_config(tmp_path: Path) -> LiveConfig:
    values = {
        "strategy_version": "SPEC-20260917-r2",
        "execution": {"slippage_per_side": .001, "taker_fee_per_side": .0005, "entry_delay_minutes": 1},
        "features": {"strategy_decision_hours_utc": [0, 1, 2, 6, 14, 15, 17], "hourly_warmup_hours": 80},
        "long": {"hard_stop_return": -.3, "extension": {"activation_return": .3},
                 "profit_guard": {"arm_return": 3., "floor_return": 2.7, "cap_return": 4.}},
        "short": {"hard_stop_return": .3}, "allocation": {"direction_budget_fraction": .5},
    }
    return LiveConfig(tmp_path, StrategyConfig(tmp_path, values), "k", "s", True, tmp_path / "state.db",
                      5, 60, 60, 180, 10, 2, 2, 2)


class FlatExchange:
    def __init__(self):
        self.configured = set()
    def now(self): return datetime(2026, 9, 19, 12, tzinfo=UTC)
    def positions(self): return []
    def open_algos(self): return []
    def open_orders(self): return []
    def force_orders(self, symbol): return []
    def cancel_algo(self, *args): pass
    def cancel_order(self, *args): pass
    def query_algo_id(self, *args): return {"actualOrderId": "0"}
    def symbol_filters(self, symbol):
        return {"tick_size": Decimal("0.1"), "step_size": Decimal("0.01"), "min_qty": Decimal("0.01"),
                "max_qty": Decimal("1000"), "min_notional": Decimal("5")}
    def conditional_order(self, *args, **kwargs): return {"algoId": "new"}
    def latest_price(self, symbol): return Decimal("100")


def add_lot(store: Store, lot_id="lot", strategy="short"):
    store.create_lot({"lot_id": lot_id, "strategy": strategy, "source": "NEW_SHORT", "symbol": "ABCUSDT",
        "position_side": strategy.upper(), "decision_time": datetime(2026, 9, 19, 0, tzinfo=UTC).isoformat(),
        "entry_time": datetime(2026, 9, 19, 0, 1, tzinfo=UTC).isoformat(),
        "planned_exit_time": datetime(2026, 9, 19, 14, tzinfo=UTC).isoformat(), "quantity": "2",
        "entry_price": "100", "entry_reference": "100", "entry_notional": "200"})


def test_exchange_flat_without_fill_preserves_ledger_and_blocks_new_entries(tmp_path):
    store = Store(tmp_path / "state.db")
    add_lot(store)
    engine = Engine(live_config(tmp_path), FlatExchange(), store)
    assert not engine.reconcile()
    lot = store.lot("lot")
    assert lot["status"] == "OPEN"
    assert lot["exit_reason"] is None
    assert store.blocked()
    store.record_equity(exchange_time := engine.now(),Decimal("1000"))
    assert store.day_reference(exchange_time.date(),Decimal("1000"),exchange_time) == 1000
    engine.close()


def test_client_id_is_ascii_even_for_localized_symbol():
    value = Engine.client_id("e", "live:short:牛来USDT:2026-09-19")
    assert value.isascii() and len(value) <= 36


def test_profit_arm_replaces_hard_stop_with_fixed_270_floor(tmp_path):
    store = Store(tmp_path / "state.db")
    add_lot(store, strategy="long")
    exchange = FlatExchange()
    exchange.positions = lambda: [{"symbol": "ABCUSDT", "positionSide": "LONG", "positionAmt": "2"}]
    exchange.latest_price = lambda symbol: Decimal("410")
    engine = Engine(live_config(tmp_path), exchange, store)
    placed = []
    exchange.conditional_order = lambda *args, **kwargs: placed.append(args) or {"algoId": f"a{len(placed)}"}
    engine.manage_positions(datetime(2026, 9, 19, 1, tzinfo=UTC))
    lot = store.lot("lot")
    assert lot["profit_armed_at"] is not None
    assert placed[-1][4] == Decimal("370.0")
    engine.close()


def test_reduce_rejection_waits_for_fill_evidence_when_exchange_is_flat(tmp_path):
    store = Store(tmp_path / "state.db")
    add_lot(store)
    exchange = FlatExchange()
    observations = iter([
        [{"symbol": "ABCUSDT", "positionSide": "SHORT", "positionAmt": "-2"}],
        [],
    ])
    exchange.positions = lambda: next(observations)
    exchange.market_order = lambda *args: (_ for _ in ()).throw(ExchangeError("ReduceOnly Order is rejected", -2022))
    engine = Engine(live_config(tmp_path), exchange, store)
    engine._close_lot(store.lot("lot"), "PLANNED_EXIT")
    assert store.lot("lot")["status"] == "OPEN"
    assert store.blocked()
    assert store.pending_orders() == []
    engine.close()
