from datetime import UTC, datetime, timedelta
from decimal import Decimal
from dataclasses import replace
import json
from pathlib import Path

import polars as pl
import pytest

from fixed_time.config import load_config, Window
from fixed_time.execution import execute_long, extend_long_trades, extension_requirements
from fixed_time.portfolio import replay_portfolio
from fixed_time.storage import empty_funding_frame
from fixed_time.live.engine import LiveEngine
from fixed_time.live.state import StateStore
from fixed_time.live.binance import BinanceError
from fixed_time.live.provenance import deployment_snapshot
from fixed_time.live.dashboard import read_status
from fixed_time.live.strategy import Admission
from fixed_time.signals import long_signals, short_signals
from test_live import _config, _candidate, _Client
from test_portfolio import _trade

T = datetime(2026, 8, 1, 14, tzinfo=UTC)


def swap_engine(tmp_path, *, final_balance="100"):
    store = StateStore(tmp_path / "state.sqlite3")
    for i in range(3):
        store.create_intent({"intent_id": f"s{i}", "strategy": "short", "symbol": f"S{i}", "position_side": "SHORT",
            "decision_time": T.isoformat(), "planned_exit_time": (T + timedelta(hours=2)).isoformat(),
            "units": 1, "priority_score": i, "client_order_id": f"s{i}"})
        store.open_position(f"s{i}", "1", "100", stop_algo_id=f"hard{i}")
    store.record_equity_minute(T.isoformat(), Decimal(100), Decimal(0), Decimal(100), Decimal(100), Decimal(0))
    client = _Client()
    closed = []
    client.balance = lambda: Decimal(final_balance) if len(closed) == 2 else Decimal("0")
    client.latest_price = lambda _: Decimal(200 if closed else 100)
    client.positions = lambda: [{"symbol": p["symbol"], "positionSide": p["position_side"], "positionAmt": p["quantity"],
        "isolatedWallet": "50", "unRealizedProfit": "0", "markPrice": "100"} for p in store.open_positions()]
    engine = LiveEngine(_config(tmp_path), client=client, store=store)
    engine._now = lambda: T
    engine.reconcile = lambda: True
    def close(position, reason):
        closed.append(position["intent_id"])
        store.close_position(position["intent_id"], reason)
    engine._close = close
    candidate = dict(_candidate("long", "AAAUSDT", T), testnet_eligible=True)
    return engine, candidate, closed


@pytest.mark.parametrize("balance,expected", [("100", "OPEN"), ("0", "SKIPPED_AFTER_EVICTION")])
def test_swap_releases_margin_then_refreshes_price_and_balance(tmp_path, balance, expected):
    engine, candidate, closed = swap_engine(tmp_path, final_balance=balance)
    admissions = engine.process_decision(T, collected=(["AAAUSDT"], [candidate], None))
    assert closed == ["s2", "s1"]
    detail = json.loads(engine.store.recent_decisions()[0]["detail_json"])
    assert detail["admissions"][0]["outcome"] == expected
    if expected == "OPEN":
        assert len(admissions) == 1
        assert engine.client.orders[0][3] == Decimal(".332")  # refreshed price 200, frozen target 66.66
        assert engine.store.intent(candidate["trade_id"])["pre_entry_equity"] == "100"
    else:
        assert not admissions and not engine.client.orders
        assert engine.store.recent_decisions()[0]["admission_count"] == 0
    engine.close()


def test_swap_retry_preserves_victims_after_partial_exit(tmp_path):
    engine, candidate, closed = swap_engine(tmp_path)
    close = engine._close
    failed = False
    def partial(position, reason):
        nonlocal failed
        if not failed:
            failed = True
            engine.store.require_exit(position["intent_id"], reason)
            raise BinanceError("partial exit unresolved")
        close(position, reason)
    engine._close = partial
    with pytest.raises(BinanceError):
        engine.process_decision(T, collected=(["AAAUSDT"], [candidate], None))
    assert not engine.client.orders
    assert engine.store.decision_plan(T.isoformat()) is not None
    engine.process_decision(T)
    assert closed == ["s2", "s1"] and len(engine.client.orders) == 1
    engine.close()


def test_swap_deadline_stops_remaining_evictions(tmp_path):
    engine, candidate, closed = swap_engine(tmp_path)
    close = engine._close
    def delayed(position, reason):
        close(position, reason)
        engine._now = lambda: T + timedelta(seconds=121)
    engine._close = delayed
    engine.process_decision(T, collected=(["AAAUSDT"], [candidate], None))
    assert closed == ["s2"] and not engine.client.orders
    engine.close()


@pytest.mark.parametrize("subwindow", [False, True])
@pytest.mark.parametrize("last_stop", [False, True])
def test_extension_stops_at_window_without_loading_next_day(subwindow, last_stop):
    config = load_config()
    start = datetime(2026, 6, 30, 23, 55, tzinfo=UTC)
    boundary = start.replace(hour=0, minute=0) + timedelta(days=1)
    signal = pl.DataFrame([{"trade_id": "long:A", "symbol": "A", "decision_time": start,
        "entry_time": start + timedelta(minutes=1), "planned_exit_time": start + timedelta(minutes=3),
        "requested_units": 1, "priority_score": 1}])
    rows = [{"symbol": "A", "open_time": start + timedelta(minutes=i), "open": 120. if i > 1 else 100.,
             "high": 130. if i >= 2 else 100., "low": 110. if i > 2 else 100., "close": 120. if i >= 2 else 100.}
            for i in range(5)]
    if last_stop:
        rows[-1]["low"] = 60.
    base = execute_long(signal, pl.DataFrame(rows), empty_funding_frame(), config)
    window = Window("research" if subwindow else "test", start, boundary + timedelta(days=3) if subwindow else boundary,
                    subwindows=((start, boundary), (boundary, boundary + timedelta(days=3))))
    days, _ = extension_requirements(base, config, window)
    assert days == {("A", start.replace(hour=0, minute=0))}
    # Boundary funding uses the last completed close, never the next window's open.
    funding = pl.DataFrame([{"symbol": "A", "funding_time": boundary, "funding_rate": .01}])
    extended, _ = extend_long_trades(base, pl.DataFrame(rows), funding, config, window)
    row = extended.to_dicts()[0]
    assert row["exit_time"] == boundary and row["exit_reason"] == ("HARD_STOP" if last_stop else "WINDOW_END")
    assert row["funding_return"] == pytest.approx(-.01 * 120 / 100.1)
    hourly = pl.DataFrame(schema={"symbol": pl.String, "open_time": pl.Datetime("us", "UTC")})
    trades, ledger, _, _ = replay_portfolio(extended, extended.head(0), hourly, config, pl.DataFrame(rows), funding)
    assert ledger.tail(1).item(0, "cash") == pytest.approx(1 + trades.item(0, "pnl"))


def test_costs_and_fractional_funding_change_next_batch_without_double_charge():
    config = load_config()
    long = dict(_trade("long", "L", T, T + timedelta(minutes=3), 1, 1),
                entry_fill=100., entry_cost_return=-.001, cost_return=-.002, funding_return=-.4, net_return=-.402)
    short = _trade("short", "S", T + timedelta(minutes=2), T + timedelta(minutes=3), 1, 1)
    minutes = pl.DataFrame([{"symbol": s, "open_time": T + timedelta(minutes=i), "open": 100., "close": 100.}
                            for s in ("L", "S") for i in range(3)])
    hourly = pl.DataFrame(schema={"symbol": pl.String, "open_time": pl.Datetime("us", "UTC")})
    funding = pl.DataFrame([{"symbol": "L", "funding_time": T + timedelta(minutes=1, milliseconds=28), "funding_rate": .4}])
    trades, account, _, _ = replay_portfolio(pl.DataFrame([long]), pl.DataFrame([short]), hourly, config, minutes, funding)
    opened_short = trades.filter(pl.col("symbol") == "S").to_dicts()[0]
    expected = 1 - (2 / 3) * .401
    assert opened_short["pre_entry_equity"] == pytest.approx(expected)
    assert opened_short["exposure_multiplier"] == 1.05
    assert account.tail(1).item(0, "cash") == pytest.approx(1 - (2 / 3) * .402)
    assert account.item(0, "cash") == pytest.approx(1 - (2 / 3) * .001)


def test_long_marks_use_entry_fill_not_reference():
    long = dict(_trade("long", "L", T, T + timedelta(minutes=2), 1, 1), entry_fill=101., entry_cost_return=-.001)
    frame = pl.DataFrame([long])
    minutes = pl.DataFrame([{"symbol": "L", "open_time": T, "close": 101.}])
    hourly = pl.DataFrame(schema={"symbol": pl.String, "open_time": pl.Datetime("us", "UTC")})
    _, account, _, _ = replay_portfolio(frame, frame.head(0), hourly, load_config(), minutes)
    assert account.item(1, "marked_equity") == pytest.approx(1 - (2 / 3) * .001)


def test_deployment_snapshot_does_not_include_credentials(tmp_path):
    config = replace(_config(tmp_path), api_key="DO_NOT_RECORD_KEY", api_secret="DO_NOT_RECORD_SECRET")
    snapshot = deployment_snapshot(config)
    assert "DO_NOT_RECORD" not in json.dumps(snapshot)
    assert snapshot["strategy"]["portfolio"]["total_units"] == 3
    engine = LiveEngine(config, client=_Client())
    engine._now = lambda: T
    engine._open(Admission(_candidate("long", "AAAUSDT", T), 1))
    intent = engine.store.pending_intents() or [engine.store.intent(_candidate("long", "AAAUSDT", T)["trade_id"])]
    assert intent[0]["run_id"] == engine.run_id
    timing = engine.store.connection.execute("SELECT * FROM execution_timing").fetchone()
    assert timing["planned_at"] == T.isoformat() and timing["submitted_at"] is not None
    engine.close()
    status = read_status(config.database_path)
    assert status["trades"][0]["portfolio_units"] == 3
    assert status["execution_timing"][0]["role"] == "ENTRY"


def test_observed_three_trades_keep_signals_and_do_not_trigger_protection():
    case = json.loads((Path(__file__).parent / "fixtures/observed_prices_20260905.json").read_text(encoding="utf-8"))
    features = []
    for row in case["trades"]:
        row["features"]["decision_time"] = datetime.fromisoformat(row["features"]["decision_time"])
        features.append(row["features"])
    frame = pl.DataFrame(features)
    config = load_config()
    start = datetime(2026, 9, 5, tzinfo=UTC)
    longs = long_signals(frame, start, start + timedelta(days=2), config)
    shorts = short_signals(frame, start, start + timedelta(days=1), config)
    assert longs["symbol"].to_list() == ["BULLAUSDT"]
    assert shorts["symbol"].to_list() == ["DASHUSDT", "AKEUSDT"]
    assert longs.item(0, "planned_exit_time") == start + timedelta(days=1, hours=8, minutes=1)
    assert shorts["planned_exit_time"].to_list() == [start + timedelta(hours=17)] * 2
    for row in case["trades"]:
        entry = float(row["entry_price"])
        if row["position_side"] == "LONG":
            assert row["minimum_low"] > entry * .7
            assert row["maximum_high"] < entry * 1.3
        else:
            assert row["maximum_high"] < entry * 1.3


def test_dashboard_funding_uses_same_cutoff_as_account_return(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    for minute in (0, 2):
        store.record_equity_minute((T + timedelta(minutes=minute)).isoformat(), Decimal(100), Decimal(0), Decimal(100), Decimal(100), Decimal(0))
    store.record_income_events([{"incomeType": "FUNDING_FEE", "tranId": i, "income": "-2", "asset": "USDT",
                                "time": int((T + timedelta(minutes=i)).timestamp() * 1000)} for i in (1, 3)])
    assert read_status(tmp_path / "state.sqlite3")["performance"]["funding_pnl"] == "-2"
    store.close()


def test_migration_preserves_legacy_exposure_without_inventing_a_version(tmp_path):
    path = tmp_path / "old.sqlite3"
    store = StateStore(path)
    store.create_intent({"intent_id": "legacy", "strategy": "long", "symbol": "A", "position_side": "LONG",
        "decision_time": T.isoformat(), "planned_exit_time": (T + timedelta(hours=1)).isoformat(),
        "units": 2, "priority_score": 1, "client_order_id": "legacy"})
    store.open_position("legacy", "10", "100", .2, "old-stop")
    original = store.open_positions()[0]
    with store.transaction() as connection:
        connection.execute("ALTER TABLE intents DROP COLUMN run_id")
        connection.execute("DROP TABLE deployment_runs")
        connection.execute("DROP TABLE execution_timing")
    store.close()
    migrated = StateStore(path)
    assert migrated.open_positions()[0] == original
    assert migrated.intent("legacy")["run_id"] is None
    assert migrated.connection.execute("SELECT count(*) FROM deployment_runs").fetchone()[0] == 0
    migrated.close()
