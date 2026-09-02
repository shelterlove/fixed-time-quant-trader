from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_CEILING
import signal
from threading import Event
import time
from typing import Any

import polars as pl

from .. import __version__
from .binance import BinanceError, BinanceRest, quantize_down, stop_trigger_price
from .config import LiveConfig
from .state import StateError, StateStore
from .strategy import Admission, allowed_retrace, decision_candidates, long_protection_update, plan_admissions, unit_notional


class LiveEngine:
    def __init__(self, config: LiveConfig, client: BinanceRest | None = None, store: StateStore | None = None):
        self.config = config
        self.client = client or BinanceRest(config)
        self.store = store or StateStore(config.database_path)
        self._stop_requested = Event()
        self._started_at = self._iso(self._now())

    def close(self) -> None:
        self.store.close()

    def request_stop(self) -> None:
        self._stop_requested.set()

    @staticmethod
    def _iso(value: datetime) -> str:
        return value.astimezone(UTC).isoformat()

    def _now(self) -> datetime:
        clock = getattr(self.client, "now", None)
        value = clock() if callable(clock) else datetime.now(UTC)
        if value.tzinfo is None:
            raise StateError("exchange clock must be timezone-aware")
        return value.astimezone(UTC)

    def _sync_clock(self) -> None:
        sync = getattr(self.client, "sync_time", None)
        if callable(sync):
            sync()

    @staticmethod
    def _client_id(prefix: str, strategy: str, symbol: str, when: datetime) -> str:
        return f"ft-{prefix}-{strategy[0]}-{symbol}-{when.strftime('%y%m%d%H%M')}"[:36]

    @staticmethod
    def _exit_client_id(position: dict[str, Any], sequence: int) -> str:
        prefix = f"ft-x-{str(position['strategy'])[0]}-"
        suffix = f"-{datetime.fromisoformat(str(position['decision_time'])).strftime('%y%m%d%H%M')}-{sequence}"
        symbol_limit = 36 - len(prefix) - len(suffix)
        return f"{prefix}{str(position['symbol'])[:symbol_limit]}{suffix}"

    def check(self) -> dict[str, Any]:
        account = self.client.account_check()
        for position in account["positions"]:
            self.client.ensure_symbol_config(str(position["symbol"]))
        return account

    def reconcile(self, *, full: bool = True) -> None:
        self._recover_pending_entries()
        exchange = {(str(row["symbol"]), str(row["positionSide"])): abs(Decimal(str(row["positionAmt"]))) for row in self.client.positions()}
        local = self.store.open_positions()
        local_keys = {(str(row["symbol"]), str(row["position_side"])): row for row in local}
        unknown = set(exchange) - set(local_keys)
        if unknown:
            self.store.record_reconciliation("BLOCKED", f"unknown exchange positions: {sorted(unknown)}")
            raise StateError(f"unknown exchange positions: {sorted(unknown)}")
        for key, position in local_keys.items():
            observed = exchange.get(key, Decimal("0"))
            expected = Decimal(str(position["quantity"]))
            if observed == 0:
                completed_exit = self.store.filled_exit_attempt(str(position["intent_id"]))
                if completed_exit is not None:
                    self._finalize_exchange_exit(position, str(completed_exit["reason"]))
                else:
                    self._record_exchange_stop(position)
                continue
            if observed != expected:
                self.store.record_reconciliation("BLOCKED", f"quantity mismatch {key}: exchange={observed} local={expected}")
                raise StateError(f"quantity mismatch for {key}")
        local = self.store.open_positions()
        if not full and not local:
            return
        if full:
            open_orders = self.client.open_orders()
            if open_orders:
                self.store.record_reconciliation("BLOCKED", "unexpected open normal orders")
                raise StateError("unexpected open normal orders")
        algo_orders = self._tracked_algo_orders(local, full)
        algo_by_client_id = {
            str(row["clientAlgoId"]): str(row["algoId"])
            for row in algo_orders if row.get("clientAlgoId") and row.get("algoId")
        }
        for position in local:
            if position.get("stop_algo_id"):
                continue
            algo_id = algo_by_client_id.get(self._stop_client_id(position))
            if algo_id is not None:
                self.store.set_stop_algo(str(position["intent_id"]), algo_id)
        local = self.store.open_positions()
        active_stop_ids = {str(row["stop_algo_id"]) for row in local if row.get("stop_algo_id")}
        known_stops = self.store.known_stop_ids()
        algo_ids = {str(row.get("algoId")) for row in algo_orders}
        for algo_id in algo_ids & (known_stops - active_stop_ids):
            order = next(row for row in algo_orders if str(row.get("algoId")) == algo_id)
            self.client.cancel_algo(str(order["symbol"]), algo_id)
        algo_ids -= known_stops - active_stop_ids
        unknown_algos = algo_ids - known_stops
        if unknown_algos:
            self.store.record_reconciliation("BLOCKED", f"unknown algo orders: {sorted(unknown_algos)}")
            raise StateError(f"unknown algo orders: {sorted(unknown_algos)}")
        missing_stops = [row for row in local if not row.get("stop_algo_id")]
        for position in missing_stops:
            self._install_stop(position)
        local = self.store.open_positions()
        protected_algos = self._tracked_algo_orders(local, full) if missing_stops else algo_orders
        algo_ids = {str(row.get("algoId")) for row in protected_algos}
        unprotected = [row for row in local if not row.get("stop_algo_id") or str(row["stop_algo_id"]) not in algo_ids]
        for position in unprotected:
            self._close(position, "UNPROTECTED_RECOVERY")
        if unprotected:
            self.store.record_reconciliation("RECOVERED", f"flattened unprotected positions: {[row['intent_id'] for row in unprotected]}")
            return
        return

    def _tracked_algo_orders(self, positions: list[dict[str, Any]], full: bool) -> list[dict[str, Any]]:
        if full:
            return self.client.open_algo_orders()
        return [
            order
            for symbol in sorted({str(position["symbol"]) for position in positions})
            for order in self.client.open_algo_orders(symbol)
        ]

    def _finalize_exchange_exit(self, position: dict[str, Any], reason: str) -> None:
        if position.get("stop_algo_id"):
            try:
                self.client.cancel_algo(str(position["symbol"]), str(position["stop_algo_id"]))
            except BinanceError:
                # A stop that has just triggered or been cancelled is already inactive.
                pass
        self.store.close_position(str(position["intent_id"]), reason)

    def _record_exchange_stop(self, position: dict[str, Any]) -> None:
        algo_id = position.get("stop_algo_id")
        if not algo_id:
            message = f"exchange closed unprotected position {position['intent_id']}"
            self.store.record_reconciliation("BLOCKED", message)
            raise StateError(message)
        try:
            algo = self.client.query_algo_by_id(str(position["symbol"]), str(algo_id))
            actual_order_id = str(algo.get("actualOrderId", ""))
            if not actual_order_id or actual_order_id == "0":
                raise StateError(f"stop {algo_id} has no actual order")
            response = self.client.query_order_by_id(str(position["symbol"]), actual_order_id)
            executed = Decimal(str(response.get("executedQty", "0")))
            if executed <= 0:
                raise StateError(f"stop {algo_id} actual order has no fill")
            response = self._with_average_price_by_id(str(position["symbol"]), actual_order_id, response)
            client_order_id = str(response.get("clientOrderId") or f"ft-a-{algo_id}")
            self.store.record_execution(str(position["intent_id"]), client_order_id, "EXIT", response, "EXCHANGE_STOP")
            self._finalize_exchange_exit(position, "EXCHANGE_STOP")
        except (BinanceError, StateError) as exc:
            message = f"cannot reconcile exchange stop for {position['intent_id']}: {exc}"
            self.store.record_reconciliation("BLOCKED", message)
            raise StateError(message) from exc

    def _stop_client_id(self, position: dict[str, Any]) -> str:
        return self._client_id(
            "s", str(position["strategy"]), str(position["symbol"]),
            datetime.fromisoformat(str(position["decision_time"])),
        )

    def _install_stop(self, position: dict[str, Any]) -> None:
        symbol, strategy = str(position["symbol"]), str(position["strategy"])
        self.client.ensure_symbol_config(symbol)
        filters = self.client.symbol_filters(symbol)
        close_side = self._side(strategy, False)
        entry_price = Decimal(str(position["entry_price"]))
        raw_trigger = entry_price * (Decimal("1") + Decimal(str(self.config.strategy.values[strategy]["hard_stop_return"])))
        trigger = stop_trigger_price(raw_trigger, filters["tick_size"], close_side)
        client_id = self._stop_client_id(position)
        try:
            stop = self.client.stop_market(symbol, close_side, str(position["position_side"]), trigger, client_id)
        except BinanceError as exc:
            try:
                stop = self.client.query_algo(symbol, client_id)
            except BinanceError:
                protected = next(item for item in self.store.open_positions() if item["intent_id"] == position["intent_id"])
                self._close(protected, "STOP_SETUP_FAILED")
                raise exc
        self.store.set_stop_algo(str(position["intent_id"]), str(stop["algoId"]))

    def _recover_pending_entries(self) -> None:
        for intent in self.store.pending_intents():
            try:
                response = self.client.query_order(str(intent["symbol"]), str(intent["client_order_id"]))
            except BinanceError as exc:
                self.store.record_reconciliation("BLOCKED", f"cannot resolve pending entry {intent['intent_id']}: {exc}")
                raise StateError(f"cannot resolve pending entry {intent['intent_id']}") from exc
            filled = Decimal(str(response.get("executedQty", "0")))
            if filled <= 0:
                self.store.set_intent_status(str(intent["intent_id"]), "ENTRY_UNFILLED")
                continue
            entry_price = Decimal(str(response.get("avgPrice", "0")))
            if entry_price <= 0:
                raise StateError(f"pending entry has no average price: {intent['intent_id']}")
            self.store.record_execution(str(intent["intent_id"]), str(intent["client_order_id"]), "ENTRY", response)
            decision = datetime.fromisoformat(str(intent["decision_time"]))
            retrace = allowed_retrace(self.store.shadow_history(), decision, self.config.strategy) if intent["strategy"] == "long" else None
            self.store.open_position(str(intent["intent_id"]), format(filled, "f"), format(entry_price, "f"), retrace)

    def _side(self, strategy: str, opening: bool) -> str:
        if strategy == "long":
            return "BUY" if opening else "SELL"
        return "SELL" if opening else "BUY"

    def _close(self, position: dict[str, Any], reason: str) -> None:
        requested = Decimal(str(position["quantity"]))
        sequence = self.store.next_exit_sequence(str(position["intent_id"]))
        attempt = self.store.begin_exit_attempt(
            str(position["intent_id"]), format(requested, "f"), reason, self._exit_client_id(position, sequence),
        )
        order_id = str(attempt["client_order_id"])
        if str(attempt["status"]) == "FILLED":
            self._confirm_filled_exit(position, str(attempt["reason"]))
            return
        if Decimal(str(attempt["requested_quantity"])) != requested:
            raise StateError(f"exit attempt quantity does not match local position: {position['intent_id']}")
        response = self._market_or_query(
            str(position["symbol"]), self._side(str(position["strategy"]), False), str(position["position_side"]), requested, order_id,
        )
        executed = Decimal(str(response.get("executedQty", "0")))
        if executed < 0 or executed > requested:
            raise StateError(f"invalid exit quantity for {position['intent_id']}: {executed}")
        if executed > 0:
            response = self._with_average_price(str(position["symbol"]), order_id, response)
        self.store.record_execution(str(position["intent_id"]), order_id, "EXIT", response, str(attempt["reason"]))
        if executed == requested:
            self.store.finish_exit_attempt(order_id, "FILLED")
            self._confirm_filled_exit(position, str(attempt["reason"]))
            return
        self.store.finish_exit_attempt(order_id, "PARTIAL" if executed > 0 else "NO_FILL")
        if executed > 0:
            self.store.update_position_quantity(str(position["intent_id"]), format(requested - executed, "f"))
        raise BinanceError(f"{reason} exit did not fully fill {position['intent_id']}")

    def _confirm_filled_exit(self, position: dict[str, Any], reason: str) -> None:
        key = (str(position["symbol"]), str(position["position_side"]))
        exchange = {
            (str(row["symbol"]), str(row["positionSide"])): abs(Decimal(str(row["positionAmt"])))
            for row in self.client.positions()
        }
        remaining = exchange.get(key, Decimal("0"))
        if remaining != 0:
            message = f"filled exit still has exchange position {key}: {remaining}"
            self.store.record_reconciliation("BLOCKED", message)
            raise StateError(message)
        self._finalize_exchange_exit(position, reason)

    def _open(self, admission: Admission) -> None:
        row = admission.candidate
        symbol, strategy, position_side = str(row["symbol"]), str(row["strategy"]), str(row["position_side"])
        self.client.configure_symbol(symbol)
        occupied = self.store.units_open()
        available = self.client.balance()
        notional = unit_notional(available, occupied, admission.units, self.config.strategy)
        filters = self.client.symbol_filters(symbol)
        price = self.client.latest_price(symbol)
        quantity = quantize_down(notional / price, filters["step_size"])
        if quantity < filters["min_qty"] or quantity * price < filters["min_notional"]:
            return
        decision = row["decision_time"]
        intent_id = str(row["trade_id"])
        client_order_id = self._client_id("e", strategy, symbol, decision)
        if self.store.intent(intent_id) is not None:
            raise StateError(f"intent already exists: {intent_id}")
        self.store.create_intent({
            "intent_id": intent_id, "strategy": strategy, "symbol": symbol, "position_side": position_side,
            "decision_time": self._iso(decision), "planned_exit_time": self._iso(row["planned_exit_time"]),
            "units": admission.units, "priority_score": float(row["priority_score"]), "client_order_id": client_order_id,
        })
        try:
            response = self.client.market_order(symbol, self._side(strategy, True), position_side, quantity, client_order_id)
        except BinanceError as exc:
            try:
                response = self.client.query_order(symbol, client_order_id)
            except BinanceError:
                raise exc
        filled = Decimal(str(response.get("executedQty", "0")))
        if filled <= 0:
            self.store.set_intent_status(intent_id, "ENTRY_UNFILLED")
            raise BinanceError(f"entry did not fill: {intent_id}")
        response = self._with_average_price(symbol, client_order_id, response)
        self.store.record_execution(intent_id, client_order_id, "ENTRY", response)
        entry_price = Decimal(str(response.get("avgPrice", "0")))
        retrace = allowed_retrace(self.store.shadow_history(), decision, self.config.strategy) if strategy == "long" else None
        self.store.open_position(intent_id, format(filled, "f"), format(entry_price, "f"), retrace)
        protected = next(item for item in self.store.open_positions() if item["intent_id"] == intent_id)
        self._install_stop(protected)

    def process_decision(self, decision_time: datetime, hourly: pl.DataFrame | None = None) -> list[Admission]:
        decision_time = decision_time.astimezone(UTC).replace(second=0, microsecond=0)
        if decision_time.hour not in self.config.strategy.values["features"]["strategy_decision_hours_utc"]:
            return []
        if self.store.decision_done(self._iso(decision_time)):
            return []
        if self._now() > decision_time + timedelta(seconds=self.config.decision_deadline_seconds):
            self.store.mark_decision_done(self._iso(decision_time))
            return []
        decision_key = self._iso(decision_time)
        self.store.start_decision(decision_key)
        symbols: list[str] = []
        candidates: list[dict[str, Any]] = []
        admissions: list[Admission] = []
        try:
            self.reconcile()
            symbols = self.client.tradable_symbols()
            snapshot = hourly if hourly is not None else self.client.hourly_snapshot(symbols, 30)
            snapshot = snapshot.filter(pl.col("open_time") < pl.lit(decision_time))
            if self._now() > decision_time + timedelta(seconds=self.config.decision_deadline_seconds):
                self.store.mark_decision_done(decision_key)
                self.store.finish_decision(decision_key, len(symbols), candidates, [], error="decision deadline exceeded")
                return []
            candidates = decision_candidates(snapshot, decision_time, self.config.strategy)
            for candidate in candidates:
                if candidate["strategy"] == "long":
                    self.store.add_shadow_task(str(candidate["trade_id"]), str(candidate["symbol"]), self._iso(candidate["entry_time"]), self._iso(candidate["planned_exit_time"]))
            admissions = plan_admissions(candidates, self.store.open_positions(), self.config.strategy)
            for admission in admissions:
                for victim_id in admission.evict_intent_ids:
                    victim = next(row for row in self.store.open_positions() if row["intent_id"] == victim_id)
                    reason = "LONG_EXTENSION_EVICTION" if victim["strategy"] == "long" and bool(victim.get("extension_active")) else "LONG_PRIORITY_EVICTION"
                    self._close(victim, reason)
                self._open(admission)
            self.store.mark_decision_done(decision_key)
            self.store.finish_decision(
                decision_key, len(symbols), candidates,
                [{"symbol": item.candidate["symbol"], "strategy": item.candidate["strategy"], "units": item.units,
                  "evicted": list(item.evict_intent_ids)} for item in admissions],
            )
            return admissions
        except Exception as exc:
            self.store.finish_decision(decision_key, len(symbols), candidates, [], error=str(exc))
            raise

    def process_due_exits(self, now: datetime | None = None) -> None:
        now = now or self._now()
        for position in self.store.open_positions():
            scheduled = datetime.fromisoformat(str(position["scheduled_exit_time"] or position["planned_exit_time"]))
            if scheduled > now:
                continue
            if self._activate_extension_if_qualified(position, now):
                continue
            self._close(position, "EXTENSION_CAP" if bool(position.get("extension_active")) else "PLANNED_EXIT")

    def _activate_extension_if_qualified(self, position: dict[str, Any], now: datetime) -> bool:
        policy = self.config.long_extension
        if not policy.enabled or position["strategy"] != "long" or bool(position.get("extension_active")):
            return False
        if not bool(position.get("protection_active")) or not position.get("protection_activated_at"):
            return False
        planned = datetime.fromisoformat(str(position["planned_exit_time"]))
        activated = datetime.fromisoformat(str(position["protection_activated_at"]))
        release = planned + timedelta(hours=policy.evict_after_hours)
        deadline = planned + timedelta(hours=policy.maximum_extension_hours)
        if now >= deadline or not planned - timedelta(hours=policy.activation_lookback_hours) < activated <= planned:
            return False
        self.store.activate_extension(str(position["intent_id"]), self._iso(deadline), self._iso(release))
        return True

    def process_long_protection(self, now: datetime | None = None) -> None:
        now = now or self._now()
        for position in self.store.open_positions():
            if position["strategy"] != "long":
                continue
            for bar in self._unprocessed_protection_bars(position, now):
                was_active = bool(position["protection_active"])
                should_exit, active, peak = long_protection_update(position, bar, self.config.strategy)
                activated_at = self._iso(bar["open_time"] + timedelta(minutes=1)) if active and not was_active else None
                last_bar_time = self._iso(bar["open_time"])
                self.store.update_protection(str(position["intent_id"]), active, format(peak, "f"), last_bar_time, activated_at)
                # A delayed loop may process several completed bars at once.
                # Keep the in-memory state aligned with the durable state so
                # the bar immediately after activation can enforce P90.
                position.update({
                    "protection_active": int(active), "protection_peak": format(peak, "f"),
                    "protection_last_bar_time": last_bar_time,
                    "protection_activated_at": activated_at or position.get("protection_activated_at"),
                })
                if should_exit:
                    self._close(position, "PROTECTION")
                    break

    def _unprocessed_protection_bars(self, position: dict[str, Any], now: datetime) -> list[dict[str, Any]]:
        last = position.get("protection_last_bar_time")
        start = (
            datetime.fromisoformat(str(last)) + timedelta(minutes=1)
            if last else datetime.fromisoformat(str(position["decision_time"]))
        )
        end_exclusive = now.replace(second=0, microsecond=0)
        if start >= end_exclusive:
            return []
        expected = int((end_exclusive - start).total_seconds() // 60)
        if expected > 1_500:
            raise StateError(f"protection catch-up exceeds Binance limit for {position['intent_id']}")
        frame = self.client.klines(
            str(position["symbol"]), "1m", expected, start_time=start,
            end_time=end_exclusive - timedelta(milliseconds=1),
        )
        closed = frame.filter(
            (pl.col("open_time") >= pl.lit(start)) & (pl.col("open_time") < pl.lit(end_exclusive))
        ).sort("open_time")
        bars = closed.to_dicts()
        if (
            len(bars) != expected or not bars or bars[0]["open_time"] != start
            or any(right["open_time"] - left["open_time"] != timedelta(minutes=1) for left, right in zip(bars, bars[1:]))
        ):
            raise StateError(f"incomplete protection minute path for {position['intent_id']}")
        return bars

    def process_due_shadows(self, now: datetime | None = None) -> None:
        now = now or self._now()
        for task in self.store.due_shadow_tasks(self._iso(now)):
            entry = datetime.fromisoformat(str(task["entry_time"]))
            planned = datetime.fromisoformat(str(task["planned_exit_time"]))
            expected = int((planned - entry).total_seconds() // 60)
            if expected <= 0 or expected > 1_499:
                raise StateError(f"invalid shadow holding period for {task['shadow_id']}")
            frame = self.client.klines(str(task["symbol"]), "1m", expected + 2).filter(
                (pl.col("open_time") >= pl.lit(entry)) & (pl.col("open_time") < pl.lit(planned))
            ).sort("open_time")
            bars = frame.to_dicts()
            if len(bars) != expected or any(right["open_time"] - left["open_time"] != timedelta(minutes=1) for left, right in zip(bars, bars[1:])):
                raise StateError(f"incomplete minute path for shadow {task['shadow_id']}")
            reference = Decimal(str(bars[0]["open"]))
            hard_stop = reference * (Decimal("1") + Decimal(str(self.config.strategy.values["long"]["hard_stop_return"])))
            activation = reference * (Decimal("1") + Decimal(str(self.config.strategy.values["long"]["protection"]["activation_return"])))
            active, peak, maximum = False, reference, None
            exit_time = planned
            for bar in bars:
                low, high = Decimal(str(bar["low"])), Decimal(str(bar["high"]))
                if low <= hard_stop:
                    exit_time = bar["open_time"] + timedelta(minutes=1)
                    break
                if active:
                    retrace = float(max(Decimal("0"), (peak - low) / peak))
                    maximum = retrace if maximum is None else max(maximum, retrace)
                    peak = max(peak, high)
                elif high >= activation:
                    active, peak = True, max(peak, high)
            self.store.complete_shadow_task(str(task["shadow_id"]), self._iso(exit_time), active, maximum)
        earliest = now - timedelta(days=self.config.strategy.values["long"]["protection"]["window_days"])
        self.store.prune_shadow_history(self._iso(earliest))

    def smoke_test(self, symbol: str) -> dict[str, str]:
        """A deliberately tiny, testnet-only round trip for credential validation."""
        account = self.check()
        if account["positions"] or account["open_orders"] or account["open_algo_orders"]:
            raise StateError("smoke test requires an empty dedicated testnet account")
        self.client.ensure_symbol_config(symbol)
        filters = self.client.symbol_filters(symbol)
        price = self.client.latest_price(symbol)
        minimum = max(filters["min_qty"], filters["min_notional"] / price)
        quantity = (minimum / filters["step_size"]).to_integral_value(rounding=ROUND_CEILING) * filters["step_size"]
        now = self._now()
        entry_id = self._client_id("m", "long", symbol, now)
        entry = self._market_or_query(symbol, "BUY", "LONG", quantity, entry_id)
        if str(entry.get("status")) != "FILLED" or Decimal(str(entry.get("executedQty", "0"))) <= 0:
            raise BinanceError("smoke entry did not fill")
        filled = Decimal(str(entry["executedQty"]))
        stop_id = self._client_id("q", "long", symbol, now)
        stop: dict[str, Any] | None = None
        exit_id = entry_id.replace("ft-m-", "ft-z-", 1)
        exited = False
        try:
            entry = self._with_average_price(symbol, entry_id, entry)
            average = Decimal(str(entry["avgPrice"]))
            trigger = stop_trigger_price(average * (Decimal("1") + Decimal(str(self.config.strategy.values["long"]["hard_stop_return"]))), filters["tick_size"], "SELL")
            try:
                stop = self.client.stop_market(symbol, "SELL", "LONG", trigger, stop_id)
            except BinanceError as exc:
                try:
                    stop = self.client.query_algo(symbol, stop_id)
                except BinanceError:
                    raise exc
            exit_order = self._market_or_query(symbol, "SELL", "LONG", filled, exit_id)
            if str(exit_order.get("status")) != "FILLED" or Decimal(str(exit_order.get("executedQty", "0"))) != filled:
                raise BinanceError("smoke exit did not fill")
            exited = True
            return {"symbol": symbol, "quantity": format(filled, "f"), "entry_price": format(average, "f"), "stop_algo_id": str(stop["algoId"])}
        finally:
            if not exited:
                try:
                    cleanup = self._market_or_query(symbol, "SELL", "LONG", filled, exit_id)
                    exited = str(cleanup.get("status")) == "FILLED" and Decimal(str(cleanup.get("executedQty", "0"))) == filled
                except BinanceError:
                    pass
            if stop is None and exited:
                try:
                    stop = self.client.query_algo(symbol, stop_id)
                except BinanceError:
                    pass
            if stop is not None and exited:
                try:
                    self.client.cancel_algo(symbol, str(stop["algoId"]))
                except BinanceError:
                    pass

    def _market_or_query(self, symbol: str, side: str, position_side: str, quantity: Decimal, client_order_id: str) -> dict[str, Any]:
        try:
            return self.client.market_order(symbol, side, position_side, quantity, client_order_id)
        except BinanceError as exc:
            try:
                return self.client.query_order(symbol, client_order_id)
            except BinanceError:
                raise exc

    def _with_average_price(self, symbol: str, client_order_id: str, response: dict[str, Any]) -> dict[str, Any]:
        if Decimal(str(response.get("avgPrice", "0"))) > 0:
            return response
        authoritative = self.client.query_order(symbol, client_order_id)
        if Decimal(str(authoritative.get("avgPrice", "0"))) <= 0:
            raise BinanceError(f"filled order has no average price: {client_order_id}")
        return authoritative

    def _with_average_price_by_id(self, symbol: str, order_id: str, response: dict[str, Any]) -> dict[str, Any]:
        if Decimal(str(response.get("avgPrice", "0"))) > 0:
            return response
        authoritative = self.client.query_order_by_id(symbol, order_id)
        if Decimal(str(authoritative.get("avgPrice", "0"))) <= 0:
            raise BinanceError(f"filled order has no average price: {order_id}")
        return authoritative

    def seed_shadow_history(self, cutoff: datetime | None = None) -> int:
        cutoff = cutoff or self._now()
        earliest = cutoff - timedelta(days=self.config.strategy.values["long"]["protection"]["window_days"])
        records: list[tuple[str, str, bool, float | None]] = []
        source_paths = (
            self.config.root / "results" / "local" / "research" / "long_trades.parquet",
            self.config.root / "results" / "local" / "forward_2026_jul_aug" / "long_trades.parquet",
        )
        for path in source_paths:
            if not path.exists():
                continue
            frame = pl.read_parquet(path)
            required = {"shadow_exit_time", "shadow_activated", "shadow_max_retrace"}
            if not required.issubset(frame.columns):
                raise StateError(f"{path} cannot seed P90 history")
            for index, row in enumerate(frame.select(*required).drop_nulls("shadow_exit_time").to_dicts()):
                exit_time = row["shadow_exit_time"].astimezone(UTC)
                if earliest <= exit_time <= cutoff:
                    source_id = f"seed:{path.parent.name}:{index}"
                    records.append((source_id, self._iso(exit_time), bool(row["shadow_activated"]), row["shadow_max_retrace"]))
        if not records:
            path = self.config.root / "seed" / "initial_shadow_history.csv"
            if path.exists():
                frame = pl.read_csv(path, try_parse_dates=True)
                required = {"shadow_exit_time", "shadow_activated", "shadow_max_retrace"}
                if not required.issubset(frame.columns):
                    raise StateError(f"{path} cannot seed P90 history")
                for index, row in enumerate(frame.select(*required).drop_nulls("shadow_exit_time").to_dicts()):
                    exit_time = row["shadow_exit_time"]
                    if not isinstance(exit_time, datetime):
                        exit_time = datetime.fromisoformat(str(exit_time))
                    exit_time = exit_time.astimezone(UTC)
                    if earliest <= exit_time <= cutoff:
                        records.append((f"seed:initial:{index}", self._iso(exit_time), bool(row["shadow_activated"]), row["shadow_max_retrace"]))
        self.store.prune_shadow_history(self._iso(earliest))
        inserted = self.store.seed_shadow_history(records) if records else 0
        total, activated = self.store.shadow_history_stats()
        rules = self.config.strategy.values["long"]["protection"]
        if total < rules["minimum_history"] or activated < rules["minimum_activated_history"]:
            raise StateError(f"insufficient P90 warm-up: total={total}, activated={activated}")
        return inserted

    def _heartbeat(self, *, reconciled: bool = False, error: str | None = None) -> None:
        available = None
        if error is None:
            try:
                available = format(self.client.balance(), "f")
            except BinanceError:
                available = None
        positions = self.store.open_positions()
        self.store.update_runtime_status(
            __version__, self._started_at, available, len(positions), self.store.units_open(),
            reconciled=reconciled, error=error,
        )

    def run_forever(self) -> None:
        self.check()
        next_poll = self._now()
        next_full_reconcile = next_poll
        failures = 0
        previous_handlers: dict[int, Any] = {}
        if signal.getsignal(signal.SIGTERM) is not None:
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, lambda *_: self.request_stop())
        try:
            while not self._stop_requested.is_set():
                try:
                    now = self._now()
                    if now >= next_poll:
                        full_reconcile = now >= next_full_reconcile
                        self.reconcile(full=full_reconcile)
                        self.process_long_protection(now)
                        self.process_due_exits(now)
                        self.process_due_shadows(now)
                        self._heartbeat(reconciled=full_reconcile)
                        next_poll = now + timedelta(seconds=self.config.account_poll_seconds)
                        if full_reconcile:
                            next_full_reconcile = now + timedelta(seconds=self.config.idle_reconcile_seconds)
                    if now.minute == 0 and now.second < self.config.decision_deadline_seconds:
                        self._sync_clock()
                        now = self._now()
                        if now.minute == 0 and now.second < self.config.decision_deadline_seconds:
                            self.process_decision(now.replace(second=0, microsecond=0))
                            next_full_reconcile = now + timedelta(seconds=self.config.idle_reconcile_seconds)
                    failures = 0
                    self._stop_requested.wait(1)
                except StateError as exc:
                    self._heartbeat(error=str(exc))
                    raise
                except BinanceError as exc:
                    failures += 1
                    self._heartbeat(error=str(exc))
                    if failures >= 5:
                        raise
                    self._stop_requested.wait((5, 15, 30)[min(failures - 1, 2)])
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
