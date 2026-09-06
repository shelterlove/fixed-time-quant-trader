from __future__ import annotations

from datetime import UTC, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, Future
from decimal import Decimal, ROUND_CEILING
import json
import signal
from threading import Event
from typing import Any

import polars as pl

from .. import __version__
from .binance import BinanceError, BinanceRest, quantize_down, stop_trigger_price
from .config import LiveConfig
from .state import EXECUTION_VERSION, StateError, StateStore, _exchange_time
from .strategy import Admission, allowed_retrace, protection_sample, decision_candidates, entry_notional, exposure_multiplier, long_protection_update, plan_admissions
from .shadows import advance_shadow, shadow_start


class LiveEngine:
    def __init__(self, config: LiveConfig, client: BinanceRest | None = None, store: StateStore | None = None):
        self.config = config
        self.client = client or BinanceRest(config)
        self.store = store or StateStore(config.database_path)
        self._stop_requested = Event()
        self._started_at = self._iso(self._now())
        self._workers = ThreadPoolExecutor(max_workers=2, thread_name_prefix="market-read")
        self._decision_job: tuple[datetime, Future] | None = None
        self._shadow_job: Future | None = None
        self._shadow_minute: datetime | None = None
        self._market_workers = ThreadPoolExecutor(max_workers=5, thread_name_prefix="position-read")
        self._market_jobs: dict[str, tuple[datetime, Future]] = {}
        self._last_equity_minute: datetime | None = None
        self._last_ledger_sync: datetime | None = None

    def close(self) -> None:
        self._workers.shutdown(wait=True, cancel_futures=True)
        self._market_workers.shutdown(wait=True, cancel_futures=True)
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
        valid = True
        for position in account["positions"]:
            symbol = str(position["symbol"])
            try:
                self._verify_held_symbol_config(symbol, {1, self.config.leverage})
            except BinanceError as exc:
                valid = False
                self.store.block_entry("ACCOUNT_CONFIGURATION", f"{symbol}: {exc}")
        if valid:
            self.store.resolve_entry_block("ACCOUNT_CONFIGURATION")
        return account

    def _verify_held_symbol_config(self, symbol: str, allowed_leverages: set[int]) -> None:
        """Held positions are checked, never reconfigured by a recovery loop."""
        verify = getattr(self.client, "verify_symbol_config", None)
        if callable(verify):
            verify(symbol, allowed_leverages)
            return
        # Test adapters from older releases only expose the stricter new-entry
        # check.  The production client always implements the method above.
        self.client.ensure_symbol_config(symbol)

    def _position_allowed_leverages(self, position: dict[str, Any]) -> set[int]:
        return {self.config.leverage} if position.get("sizing_version") in {"drawdown-2x-v1", "drawdown-2x-v2"} else {1}

    def reconcile(self, *, full: bool = True) -> bool:
        self._recover_pending_entries()
        self._recover_pending_exits()
        self._recover_hard_stops()
        self._recover_protection_orders()
        exchange = {(str(row["symbol"]), str(row["positionSide"])): abs(Decimal(str(row["positionAmt"]))) for row in self.client.positions()}
        local = self.store.open_positions()
        local_keys = {(str(row["symbol"]), str(row["position_side"])): row for row in local}
        unknown = set(exchange) - set(local_keys)
        if unknown:
            self.store.block_entry("UNKNOWN_EXCHANGE_POSITION", f"unknown exchange positions: {sorted(unknown)}")
        else:
            self.store.resolve_entry_block("UNKNOWN_EXCHANGE_POSITION")
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
                self._record_exchange_stop(position, finalize=False)
                current = next(row for row in self.store.open_positions() if row["intent_id"] == position["intent_id"])
                if Decimal(current["quantity"]) == observed:
                    continue
                self.store.block_entry("POSITION_QUANTITY_MISMATCH", f"quantity mismatch {key}: exchange={observed} local={expected}")
                return False
        self.store.resolve_entry_block("POSITION_QUANTITY_MISMATCH")
        local = self.store.open_positions()
        if not full and not local:
            return not self.store.entry_blocked()
        if full:
            open_orders = self.client.open_orders()
            pending_ids = {row["client_order_id"] for row in self.store.unsettled_exits()}
            if any(str(row.get("clientOrderId")) not in pending_ids for row in open_orders):
                self.store.block_entry("UNKNOWN_EXCHANGE_ORDER", "unexpected open normal orders")
            else:
                self.store.resolve_entry_block("UNKNOWN_EXCHANGE_ORDER")
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
        active_intents = {row["intent_id"] for row in local}
        protection_orders = self.store.protection_orders()
        active_stop_ids.update(str(row["algo_id"]) for row in protection_orders if row["intent_id"] in active_intents and row["algo_id"])
        algo_ids = {str(row.get("algoId")) for row in algo_orders}
        known_stops = self.store.known_stop_ids(algo_ids)
        for algo_id in algo_ids & (known_stops - active_stop_ids):
            order = next(row for row in algo_orders if str(row.get("algoId")) == algo_id)
            try:
                self.client.cancel_algo(str(order["symbol"]), algo_id)
            except BinanceError as exc:
                if not exc.not_found:
                    raise
        algo_ids -= known_stops - active_stop_ids
        unknown_algos = algo_ids - known_stops
        if unknown_algos:
            self.store.block_entry("UNKNOWN_EXCHANGE_ALGO", f"unknown algo orders: {sorted(unknown_algos)}")
        else:
            self.store.resolve_entry_block("UNKNOWN_EXCHANGE_ALGO")
        positions_by_id = {row["intent_id"]: row for row in local}
        for order in protection_orders:
            if order["status"] == "ACTIVE" and order["algo_id"] not in algo_ids:
                position = positions_by_id.get(order["intent_id"])
                if position is not None:
                    result = self.client.query_algo_by_id(position["symbol"], str(order["algo_id"]))
                    if str(result.get("actualOrderId", "0")) not in {"", "0", "None"}:
                        self.store.set_protection_order(order["id"], "TRIGGERED")
                        raise BinanceError("protection triggered during reconciliation; refresh position before trading")
                    if result.get("algoStatus") in {"CANCELED", "EXPIRED", "REJECTED"}:
                        self.store.set_protection_order(order["id"], result["algoStatus"])
                    else:
                        raise BinanceError("protection order is not yet visible in open orders")
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
            return False
        if full:
            position_mode = getattr(self.client, "position_mode", None)
            multi_asset_mode = getattr(self.client, "multi_asset_mode", None)
            if ((callable(position_mode) and not position_mode())
                    or (callable(multi_asset_mode) and multi_asset_mode())):
                self.store.block_entry("ACCOUNT_CONFIGURATION", "account must remain in Hedge and Single-Asset modes")
                return False
            configuration_ok = True
            for position in local:
                try:
                    self._verify_held_symbol_config(str(position["symbol"]), self._position_allowed_leverages(position))
                except BinanceError as exc:
                    configuration_ok = False
                    self.store.block_entry("ACCOUNT_CONFIGURATION", f"{position['symbol']}: {exc}")
            if not configuration_ok:
                return False
            self.store.resolve_entry_block("ACCOUNT_CONFIGURATION")
        return not self.store.entry_blocked()

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
            except BinanceError as exc:
                if not exc.not_found:
                    raise
        for order in self.store.protection_orders(str(position["intent_id"])):
            if order["status"] == "SUBMITTED":
                raise BinanceError("cannot finalize before protection order is resolved")
            self._cancel_protection(position, order)
        self.store.close_position(str(position["intent_id"]), reason)

    def _record_exchange_stop(self, position: dict[str, Any], *, finalize: bool = True) -> None:
        algo_id = position.get("stop_algo_id")
        if not algo_id:
            message = f"exchange closed unprotected position {position['intent_id']}"
            self.store.record_reconciliation("BLOCKED", message)
            raise StateError(message)
        try:
            stops = [(algo_id, "EXCHANGE_STOP"), *[(o["algo_id"], "PROTECTION") for o in self.store.protection_orders(position["intent_id"]) if o["algo_id"]]]
            reason = "EXCHANGE_STOP"
            for stop_id, stop_reason in stops:
                algo = self.client.query_algo_by_id(str(position["symbol"]), str(stop_id))
                actual_order_id = str(algo.get("actualOrderId", "0"))
                if actual_order_id in {"", "0", "None"}:
                    continue
                response = self.client.query_order_by_id(str(position["symbol"]), actual_order_id)
                quantity = Decimal(str(response.get("executedQty", "0")))
                if quantity <= 0:
                    continue
                response = self._with_average_price_by_id(str(position["symbol"]), actual_order_id, response)
                client_order_id = str(response.get("clientOrderId") or f"ft-a-{stop_id}")
                response = self._terminal_order(position["symbol"], client_order_id, response)
                self.store.apply_stop_fill(str(position["intent_id"]), client_order_id, response, stop_reason)
                reason = stop_reason
            if finalize:
                remaining = next(row for row in self.store.open_positions() if row["intent_id"] == position["intent_id"])
                if Decimal(remaining["quantity"]) != 0:
                    raise StateError("exchange position disappeared without sufficient exit fills")
                self._finalize_exchange_exit(remaining, reason)
        except (BinanceError, StateError) as exc:
            message = f"cannot reconcile exchange stop for {position['intent_id']}: {exc}"
            self.store.record_reconciliation("BLOCKED", message)
            raise StateError(message) from exc

    def _stop_client_id(self, position: dict[str, Any]) -> str:
        return self._client_id(
            "s", str(position["strategy"]), str(position["symbol"]),
            datetime.fromisoformat(str(position["decision_time"])),
        )

    def _recover_hard_stops(self) -> None:
        for position in self.store.pending_hard_stops():
            try:
                stop = self.client.query_algo(position["symbol"], self._stop_client_id(position))
            except BinanceError as exc:
                if exc.not_found and self._now() - datetime.fromisoformat(position["updated_at"]) > timedelta(seconds=60):
                    self.store.set_hard_stop_pending(position["intent_id"], False)
                    continue
                raise
            algo_id = str(stop["algoId"])
            self.store.set_stop_algo(position["intent_id"], algo_id)
            if position["status"] != "OPEN":
                try:
                    self.client.cancel_algo(position["symbol"], algo_id)
                except BinanceError as exc:
                    if not exc.not_found:
                        raise

    def _install_stop(self, position: dict[str, Any], filters: dict[str, Decimal] | None = None) -> None:
        symbol, strategy = str(position["symbol"]), str(position["strategy"])
        if filters is None:
            filters = self.client.symbol_filters(symbol)
        close_side = self._side(strategy, False)
        entry_price = Decimal(str(position["entry_price"]))
        raw_trigger = entry_price * (Decimal("1") + Decimal(str(self.config.strategy.values[strategy]["hard_stop_return"])))
        trigger = stop_trigger_price(raw_trigger, filters["tick_size"], close_side)
        client_id = self._stop_client_id(position)
        self.store.set_hard_stop_pending(position["intent_id"], True)
        try:
            stop = self.client.stop_market(symbol, close_side, str(position["position_side"]), trigger, client_id)
        except BinanceError as exc:
            if exc.rejected:
                self.store.set_hard_stop_pending(position["intent_id"], False)
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
                if exc.not_found and self._now() - datetime.fromisoformat(intent["created_at"]) > timedelta(seconds=60):
                    self.store.set_intent_status(str(intent["intent_id"]), "ENTRY_UNFILLED")
                    continue
                raise BinanceError(f"cannot resolve pending entry {intent['intent_id']}: {exc}") from exc
            response = self._terminal_order(str(intent["symbol"]), str(intent["client_order_id"]), response)
            filled = Decimal(str(response.get("executedQty", "0")))
            if filled <= 0:
                self.store.set_intent_status(str(intent["intent_id"]), "ENTRY_UNFILLED")
                continue
            response = self._with_average_price(str(intent["symbol"]), str(intent["client_order_id"]), response)
            entry_price = Decimal(str(response.get("avgPrice", "0")))
            if entry_price <= 0:
                raise StateError(f"pending entry has no average price: {intent['intent_id']}")
            self.store.record_execution(str(intent["intent_id"]), str(intent["client_order_id"]), "ENTRY", response)
            decision = datetime.fromisoformat(str(intent["decision_time"]))
            sample = json.loads(intent["protection_json"]) if intent.get("protection_json") else None
            retrace = sample[0] if sample else allowed_retrace(self.store.shadow_history(), decision, self.config.strategy) if intent["strategy"] == "long" else None
            self.store.open_position(str(intent["intent_id"]), format(filled, "f"), format(entry_price, "f"), retrace,
                                     execution_version=intent["execution_version"], filled_at=_exchange_time(response) or intent["created_at"])

    def _side(self, strategy: str, opening: bool) -> str:
        if strategy == "long":
            return "BUY" if opening else "SELL"
        return "SELL" if opening else "BUY"

    def _recover_pending_exits(self) -> None:
        positions = {row["intent_id"]: row for row in self.store.open_positions()}
        for attempt in self.store.unsettled_exits():
            position = positions[attempt["intent_id"]]
            self._resolve_exit(position, attempt)

    def _resolve_exit(self, position: dict[str, Any], attempt: dict[str, Any], response: dict[str, Any] | None = None) -> None:
        if response is None:
            try:
                response = self.client.query_order(position["symbol"], attempt["client_order_id"])
            except BinanceError as exc:
                # A request that cannot still pass recvWindow may be released
                # after authoritative absence. Reconciliation checks exposure
                # before a new attempt can be made.
                if exc.not_found and self._now() - datetime.fromisoformat(attempt["created_at"]) > timedelta(seconds=60):
                    self.store.finish_exit_attempt(attempt["client_order_id"], "NO_FILL")
                    return
                raise
        response = self._terminal_order(position["symbol"], attempt["client_order_id"], response)
        if Decimal(str(response.get("executedQty", "0"))) > 0:
            response = self._with_average_price(position["symbol"], attempt["client_order_id"], response)
        self.store.apply_exit(attempt, response)

    def _close(self, position: dict[str, Any], reason: str) -> None:
        self.store.require_exit(position["intent_id"], reason)
        current = next((row for row in self.store.open_positions() if row["intent_id"] == position["intent_id"]), None)
        if current is None:
            return
        position = current
        reason = position["exit_required"]
        requested = Decimal(str(position["quantity"]))
        if requested == 0:
            self._confirm_filled_exit(position, reason)
            return
        pending = next((row for row in self.store.unsettled_exits() if row["intent_id"] == position["intent_id"]), None)
        if pending is not None:
            self._resolve_exit(position, pending)
        else:
            sequence = self.store.next_exit_sequence(position["intent_id"])
            requested = min(requested, self.client.symbol_filters(position["symbol"]).get("max_qty", requested))
            attempt = self.store.begin_exit_attempt(position["intent_id"], format(requested, "f"), reason, self._exit_client_id(position, sequence), sequence=sequence)
            try:
                response = self.client.market_order(position["symbol"], self._side(position["strategy"], False), position["position_side"], requested, attempt["client_order_id"])
            except BinanceError as exc:
                if exc.rejected:
                    self.store.finish_exit_attempt(attempt["client_order_id"], "NO_FILL")
                    raise
                self._resolve_exit(position, attempt)
            else:
                self._resolve_exit(position, attempt, response)
        remaining = next(row for row in self.store.open_positions() if row["intent_id"] == position["intent_id"])
        if Decimal(remaining["quantity"]) == 0:
            self._confirm_filled_exit(remaining, reason)
        else:
            raise BinanceError(f"{reason} exit remains pending for {position['intent_id']}")

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

    def _sync_account_ledger(self, now: datetime, *, force: bool = False) -> None:
        """Incrementally copy exchange fills and cash events into the local audit ledger."""
        trade_reader = getattr(self.client, "user_trades", None)
        income_reader = getattr(self.client, "income_history", None)
        if not callable(trade_reader) or not callable(income_reader):
            return
        if not force and self._last_ledger_sync is not None and now - self._last_ledger_sync < timedelta(minutes=1):
            return
        symbols = (self.store.known_trade_symbols() if self._last_ledger_sync is None else
                   self.store.active_trade_symbols(self._iso(self._last_ledger_sync - timedelta(minutes=2))))
        for symbol in symbols:
            cursor_key = f"trades:{symbol}"
            cursor = self.store.sync_cursor(cursor_key)
            from_id = int(cursor) + 1 if cursor is not None else None
            page_count = 0
            while True:
                page_count += 1
                if page_count > 100:
                    raise StateError(f"{symbol} trade history pagination exceeded the audit limit")
                rows = trade_reader(symbol, from_id=from_id)
                self.store.record_trade_fills(rows)
                if not rows:
                    if cursor is None:
                        self.store.set_sync_cursor(cursor_key, "0")
                    break
                last_id = max(int(row["id"]) for row in rows)
                self.store.set_sync_cursor(cursor_key, str(last_id))
                if len(rows) < 1000:
                    break
                from_id = last_id + 1
        income_cursor = self.store.sync_cursor("income")
        start = datetime.fromisoformat(income_cursor) - timedelta(minutes=10) if income_cursor else now - timedelta(days=89)
        latest = datetime.fromisoformat(income_cursor) if income_cursor else start
        page = 1
        while True:
            rows = income_reader(start_time=start, end_time=now, page=page)
            self.store.record_income_events(rows)
            if rows:
                latest = max(latest, *(datetime.fromtimestamp(int(row["time"]) / 1000, UTC) for row in rows))
            if len(rows) < 1000:
                break
            page += 1
            if page > 100:
                raise StateError("income history pagination exceeded the audit limit")
        self.store.set_sync_cursor("income", self._iso(max(latest, now)))
        self.store.resolve_entry_block("LEDGER_SYNC")
        self._last_ledger_sync = now

    def _completed_equity(self, minute_end: datetime) -> dict[str, Any]:
        """Durably value completed minutes from one wallet observation and historical marks."""
        minute_end = minute_end.astimezone(UTC).replace(second=0, microsecond=0)
        if minute_end > self._now().replace(second=0, microsecond=0):
            raise StateError("cannot value an unfinished equity minute")
        unsafe = {"UNKNOWN_EXCHANGE_POSITION", "POSITION_QUANTITY_MISMATCH", "ACCOUNT_RECONCILIATION"}
        if any(row["code"] in unsafe for row in self.store.active_entry_blocks()):
            raise StateError("cannot value equity while exchange quantities are unresolved")
        key = self._iso(minute_end)
        existing = self.store.equity_minute(key)
        if existing is not None:
            self._last_equity_minute = minute_end
            return existing
        previous = self.store.latest_equity_minute()
        first = datetime.fromisoformat(str(previous["minute_end"])) + timedelta(minutes=1) if previous else minute_end
        missing = int((minute_end - first).total_seconds() // 60) + 1
        if missing > 1440:
            detail = f"equity gap exceeds one-day automatic repair: {first.isoformat()} to {minute_end.isoformat()}"
            self.store.block_entry("EQUITY_GAP", detail)
            return previous or {"minute_end": key, "equity": "0", "peak_equity": "0", "drawdown": "0"}
        wallet_reader = getattr(self.client, "wallet_balance", None)
        if not callable(wallet_reader):
            raise StateError("client does not provide a wallet-balance snapshot")
        wallet_now = wallet_reader()
        observed_at = self._now()
        try:
            self._sync_account_ledger(observed_at, force=True)
        except (BinanceError, StateError) as exc:
            self.store.block_entry("LEDGER_SYNC", str(exc))
            raise
        close_reader = getattr(self.client, "mark_price_close", None)
        latest = previous
        current = first
        try:
            while current <= minute_end:
                cutoff = self._iso(current)
                wallet = wallet_now
                if callable(getattr(self.client, "income_history", None)):
                    wallet -= self.store.usdt_income_between(cutoff, self._iso(observed_at))
                unrealized = Decimal("0")
                position_marks: dict[str, tuple[Decimal, Decimal]] = {}
                for position in self.store.positions_at(cutoff):
                    if not callable(close_reader):
                        raise StateError("client does not provide completed mark-price minutes")
                    mark = close_reader(str(position["symbol"]), current)
                    quantity = Decimal(str(position["quantity"]))
                    entry = Decimal(str(position["entry_price"]))
                    position_pnl = quantity * (mark - entry) if position["position_side"] == "LONG" else quantity * (entry - mark)
                    unrealized += position_pnl
                    position_marks[str(position["intent_id"])] = (mark, position_pnl)
                equity = wallet + unrealized
                if equity <= 0:
                    raise StateError(f"non-positive completed equity at {cutoff}")
                old_peak = Decimal(str(latest["peak_equity"])) if latest is not None else equity
                peak = max(old_peak, equity)
                drawdown = max(Decimal("0"), Decimal("1") - equity / peak)
                multiplier = exposure_multiplier(drawdown, self.config.strategy)
                self.store.record_equity_minute(cutoff, wallet, unrealized, equity, peak, drawdown, multiplier)
                if current == minute_end:
                    self.store.update_position_marks(cutoff, position_marks)
                latest = self.store.equity_minute(cutoff)
                current += timedelta(minutes=1)
        except (BinanceError, KeyError) as exc:
            self.store.block_entry("EQUITY_GAP", f"cannot rebuild equity minute {current.isoformat()}: {exc}")
            latest = self.store.latest_equity_minute()
            if latest is None:
                raise StateError(str(exc)) from exc
            return latest
        self.store.resolve_entry_block("EQUITY_GAP")
        row = self.store.equity_minute(key)
        assert row is not None
        self._last_equity_minute = minute_end
        return row

    def _sizing_snapshot(self, decision_time: datetime) -> dict[str, str]:
        row = self._completed_equity(decision_time)
        equity, peak, drawdown = (Decimal(str(row[name])) for name in ("equity", "peak_equity", "drawdown"))
        multiplier = exposure_multiplier(drawdown, self.config.strategy)
        total = Decimal(int(self.config.strategy.values["portfolio"]["total_units"]))
        return {
            "minute_end": str(row["minute_end"]), "pre_entry_equity": format(equity, "f"),
            "pre_entry_peak": format(peak, "f"), "pre_entry_drawdown": format(drawdown, "f"),
            "exposure_multiplier": format(multiplier, "f"), "base_unit_capital": format(equity / total, "f"),
            "sizing_version": "drawdown-2x-v2",
        }

    def _prepare_open(self, admission: Admission, *, deadline: datetime | None = None) -> dict[str, Any] | None:
        row = admission.candidate
        symbol, strategy, position_side = str(row["symbol"]), str(row["strategy"]), str(row["position_side"])
        existing = self.store.intent(str(row["trade_id"]))
        if existing is not None:
            return {"existing": existing}
        if self.store.entry_blocked():
            return None
        if any(
            str(position["symbol"]) == symbol and position.get("sizing_version") not in {"drawdown-2x-v1", "drawdown-2x-v2"}
            for position in self.store.open_positions()
        ):
            self.store.record_reconciliation("ENTRY_SKIPPED", f"{symbol}: legacy 1x position prevents a 2x reconfiguration")
            return None
        self.client.configure_symbol(symbol)
        sizing = dict(row.get("sizing") or self._sizing_snapshot(row["decision_time"]))
        pre_entry_equity = Decimal(sizing["pre_entry_equity"])
        multiplier = Decimal(sizing["exposure_multiplier"])
        notional = entry_notional(pre_entry_equity, admission.units, multiplier, self.config.strategy)
        sizing["target_notional"] = format(notional, "f")
        filters = self.client.symbol_filters(symbol)
        price = self.client.latest_price(symbol)
        reserve = Decimal(str(self.config.strategy.values[strategy].get("round_trip_stress_cost", .003)))
        if notional <= 0:
            return None
        available = self.client.balance()
        leverage = Decimal(str(self.config.leverage))
        if available <= 0 or notional * (Decimal("1") + reserve) / leverage > available:
            self.store.record_reconciliation("ENTRY_SKIPPED", f"{symbol}: insufficient available margin for fixed target notional")
            return None
        quantity = quantize_down(min(notional / (price * (1 + reserve)), filters.get("max_qty", Decimal("Infinity"))), filters["step_size"])
        if quantity < filters["min_qty"] or quantity * price < filters["min_notional"]:
            self.store.record_reconciliation("ENTRY_SKIPPED", f"{symbol}: below exchange minimum after sizing")
            return None
        decision = row["decision_time"]
        intent_id = str(row["trade_id"])
        client_order_id = self._client_id("e", strategy, symbol, decision)
        sample = row.get("protection_sample")
        if strategy == "long" and sample is None:
            sample = protection_sample(self.store.shadow_history(), decision, self.config.strategy)
        if deadline is not None and self._now() >= deadline:
            return None
        return {"sizing": sizing, "filters": filters, "quantity": quantity, "sample": sample,
                "symbol": symbol, "strategy": strategy, "position_side": position_side,
                "intent_id": intent_id, "client_order_id": client_order_id, "decision": decision}

    def _open(self, admission: Admission, *, deadline: datetime | None = None,
              prepared: dict[str, Any] | None = None) -> str:
        row = admission.candidate
        prepared = prepared if prepared is not None else self._prepare_open(admission, deadline=deadline)
        if prepared is None:
            return "SKIPPED"
        if "existing" in prepared:
            existing = prepared["existing"]
            if existing["status"] == "PENDING":
                self._recover_pending_entries()
            return str((self.store.intent(str(row["trade_id"])) or existing)["status"])
        if self.store.entry_blocked() or (deadline is not None and self._now() >= deadline):
            return "SKIPPED"
        positions = self.store.open_positions()
        if (sum(int(p["units"]) for p in positions) + admission.units > self.config.strategy.values["portfolio"]["total_units"]
                or any(p["symbol"] == row["symbol"] and p["strategy"] == row["strategy"] for p in positions)):
            return "SKIPPED"
        sizing, filters, quantity, sample = (prepared[key] for key in ("sizing", "filters", "quantity", "sample"))
        symbol, strategy, position_side = (str(prepared[key]) for key in ("symbol", "strategy", "position_side"))
        intent_id, client_order_id, decision = prepared["intent_id"], prepared["client_order_id"], prepared["decision"]
        self.store.create_intent({
            "intent_id": intent_id, "strategy": strategy, "symbol": symbol, "position_side": position_side,
            "decision_time": self._iso(decision), "planned_exit_time": self._iso(row["planned_exit_time"]),
            "units": admission.units, "priority_score": float(row["priority_score"]), "client_order_id": client_order_id,
        }, protection=sample, execution_version=EXECUTION_VERSION, sizing=sizing)
        try:
            response = self.client.market_order(symbol, self._side(strategy, True), position_side, quantity, client_order_id)
        except BinanceError as exc:
            if exc.rejected:
                self.store.set_intent_status(intent_id, "ENTRY_REJECTED")
                self.store.record_reconciliation("ENTRY_REJECTED", f"{symbol}: {exc}")
                return "ENTRY_REJECTED"
            try:
                response = self.client.query_order(symbol, client_order_id)
            except BinanceError:
                raise exc
        response = self._terminal_order(symbol, client_order_id, response)
        filled = Decimal(str(response.get("executedQty", "0")))
        if filled <= 0:
            self.store.set_intent_status(intent_id, "ENTRY_UNFILLED")
            raise BinanceError(f"entry did not fill: {intent_id}")
        response = self._with_average_price(symbol, client_order_id, response)
        self.store.record_execution(intent_id, client_order_id, "ENTRY", response)
        entry_price = Decimal(str(response.get("avgPrice", "0")))
        retrace = sample[0] if sample is not None else None
        self.store.open_position(intent_id, format(filled, "f"), format(entry_price, "f"), retrace,
                                 execution_version=EXECUTION_VERSION, filled_at=_exchange_time(response) or self._iso(self._now()))
        protected = next(item for item in self.store.open_positions() if item["intent_id"] == intent_id)
        self._install_stop(protected, filters)
        return "OPEN"

    def _collect_decision(self, decision: datetime, hourly: pl.DataFrame | None, first_count: int | None) -> tuple[list[str], list[dict], int | None]:
        self._sync_clock()
        symbols = self.client.market_data_symbols()
        tradable = set(self.client.trading_symbols())
        # Explicit endTime excludes the unfinished hour. Thirty completed hours
        # also reconstruct 06:00 ranks at 08:00 after a restart.
        snapshot = hourly if hourly is not None else self.client.hourly_snapshot(symbols, 30, end_time=decision - timedelta(milliseconds=1))
        snapshot = snapshot.filter(pl.col("open_time") < pl.lit(decision))
        candidates, first_count = decision_candidates(snapshot, decision, self.config.strategy, first_short_count=first_count)
        for row in candidates:
            row["testnet_eligible"] = row["symbol"] in tradable
        return symbols, candidates, first_count

    def process_decision(self, decision_time: datetime, hourly: pl.DataFrame | None = None, *, collected: tuple | None = None) -> list[Admission]:
        decision_time = decision_time.astimezone(UTC).replace(second=0, microsecond=0)
        decision_key = self._iso(decision_time)
        if decision_time.hour not in self.config.strategy.values["features"]["strategy_decision_hours_utc"] or self.store.decision_done(decision_key):
            return []
        if collected is None and self._now() >= decision_time + timedelta(seconds=self.config.decision_deadline_seconds):
            self.store.mark_decision_done(decision_key)
            return []
        self.store.start_decision(decision_key)
        candidates, admissions, symbols = [], [], []
        try:
            self.process_due_exits(decision_time)
            saved = self.store.decision_plan(decision_key)
            if saved is None:
                first_key = self._iso(decision_time.replace(hour=6))
                first_count = self.store.short_count(first_key) if decision_time.hour == 8 else None
                symbols, candidates, first_count = collected or self._collect_decision(decision_time, hourly, first_count)
                if first_count is not None:
                    self.store.save_short_count(first_key, first_count)
                for candidate in candidates:
                    if candidate["strategy"] == "long":
                        shadow_entry = decision_time + timedelta(minutes=self.config.strategy.values["long"]["entry_delay_minutes"])
                        self.store.add_shadow_task(self._shadow_id(candidate["symbol"], decision_time), candidate["symbol"], self._iso(shadow_entry), self._iso(candidate["planned_exit_time"]))
                if self._now() >= decision_time + timedelta(seconds=self.config.decision_deadline_seconds):
                    self.store.mark_decision_done(decision_key)
                    self.store.finish_decision(decision_key, len(symbols), candidates, [], error="decision deadline exceeded")
                    return []
            else:
                candidates, serialized = saved
                for item in serialized:
                    row = item["candidate"]
                    for field in ("decision_time", "entry_time", "planned_exit_time"):
                        row[field] = datetime.fromisoformat(row[field])
                    admissions.append(Admission(row, item["units"], tuple(item["evicted"])))

            if self.reconcile() is False or self.store.entry_blocked():
                self.store.mark_decision_done(decision_key)
                self.store.finish_decision(
                    decision_key, len(symbols) or len({row["symbol"] for row in candidates}), candidates, [],
                    error="new entries are blocked by reconciliation",
                )
                return []

            if saved is None:
                sizing = self._sizing_snapshot(decision_time)
                if self.store.entry_blocked():
                    self.store.mark_decision_done(decision_key)
                    self.store.finish_decision(decision_key, len(symbols), candidates, [], error="new entries are blocked by equity continuity")
                    return []
                admissions = plan_admissions([row for row in candidates if row["testnet_eligible"]], self.store.open_positions(), self.config.strategy)
                for item in admissions:
                    item.candidate["sizing"] = sizing
                if any(item.candidate["strategy"] == "long" for item in admissions):
                    sample = protection_sample(self.store.shadow_history(), decision_time, self.config.strategy)
                    for item in admissions:
                        if item.candidate["strategy"] == "long":
                            item.candidate["protection_sample"] = sample
                serialized = [{"candidate": item.candidate, "units": item.units, "evicted": list(item.evict_intent_ids)} for item in admissions]
                self.store.save_decision_plan(decision_key, candidates, serialized)

            completed: list[Admission] = []
            for admission in admissions:
                # The saved plan is immutable: a retry cannot turn the second
                # of two one-unit entries into a new two-unit single entry.
                if self._now() >= decision_time + timedelta(seconds=self.config.decision_deadline_seconds):
                    break
                prepared = self._prepare_open(admission, deadline=decision_time + timedelta(seconds=self.config.decision_deadline_seconds))
                if prepared is None:
                    admission.candidate["execution_outcome"] = "SKIPPED"
                    continue
                for victim_id in (() if "existing" in prepared else admission.evict_intent_ids):
                    victim = next((row for row in self.store.open_positions() if row["intent_id"] == victim_id), None)
                    if victim is not None:
                        reason = "LONG_EXTENSION_EVICTION" if victim["strategy"] == "long" and bool(victim.get("extension_active")) else "LONG_PRIORITY_EVICTION"
                        self._close(victim, reason)
                outcome = self._open(admission, deadline=decision_time + timedelta(seconds=self.config.decision_deadline_seconds), prepared=prepared)
                admission.candidate["execution_outcome"] = outcome
                if outcome == "OPEN":
                    completed.append(admission)
            self.store.mark_decision_done(decision_key)
            self.store.finish_decision(decision_key, len(symbols) or len({row["symbol"] for row in candidates}), candidates,
                [{"symbol": item.candidate["symbol"], "strategy": item.candidate["strategy"], "units": item.units,
                  "evicted": list(item.evict_intent_ids), "outcome": item.candidate.get("execution_outcome")} for item in completed])
            return completed
        except Exception as exc:
            self.store.finish_decision(decision_key, len(symbols), candidates, [], error=str(exc))
            raise

    @staticmethod
    def _shadow_id(symbol: str, decision: datetime) -> str:
        return f"long:{symbol}:{decision.isoformat()}"

    def process_due_exits(self, now: datetime | None = None) -> None:
        now = now or self._now()
        errors = []
        for position in self.store.open_positions():
            try:
                scheduled = datetime.fromisoformat(position["scheduled_exit_time"] or position["planned_exit_time"])
                reason = position.get("exit_required")
                if reason is None:
                    if scheduled > now or self._activate_extension_if_qualified(position, now):
                        continue
                    reason = "EXTENSION_CAP" if position.get("extension_active") else "PLANNED_EXIT"
                self._close(position, reason)
            except (BinanceError, StateError) as exc:
                errors.append(str(exc))
        if errors:
            raise BinanceError("; ".join(errors))

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

    def process_long_protection(self, now: datetime | None = None, *, background: bool = False) -> None:
        now = now or self._now()
        positions = self.store.open_positions()
        open_ids = {row["intent_id"] for row in positions}
        for intent_id in set(self._market_jobs) - open_ids:
            self._market_jobs.pop(intent_id)[1].cancel()
        errors = []
        for position in positions:
            try:
                self._protect_position(position, now, background)
            except (BinanceError, StateError) as exc:
                errors.append(str(exc))
        if errors:
            raise BinanceError("; ".join(errors))

    def _protect_position(self, position: dict[str, Any], now: datetime, background: bool) -> None:
        if position["strategy"] != "long":
            return
        if position.get("exit_required"):
            return
        if position.get("execution_version") == EXECUTION_VERSION:
            if background:
                intent_id = position["intent_id"]
                pending = self._market_jobs.get(intent_id)
                if pending is not None and pending[1].done():
                    del self._market_jobs[intent_id]
                    self._update_exchange_protection(position, now, trades=pending[1].result(), requested_at=pending[0])
                    position = next((row for row in self.store.open_positions() if row["intent_id"] == intent_id), None)
                if position is not None and intent_id not in self._market_jobs:
                    self._market_jobs[intent_id] = (now, self._market_workers.submit(self.client.aggregate_trades, position["symbol"], self._market_start(position), now, position.get("trade_cursor")))
            else:
                self._update_exchange_protection(position, now)
            return
        for bar in self._unprocessed_protection_bars(position, now):
            was_active = bool(position["protection_active"])
            should_exit, active, peak = long_protection_update(position, bar, self.config.strategy)
            activated_at = self._iso(bar["open_time"] + timedelta(minutes=1)) if active and not was_active else None
            last_bar_time = self._iso(bar["open_time"])
            if should_exit:
                self.store.require_exit(position["intent_id"], "PROTECTION")
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

    def _recover_protection_orders(self) -> None:
        positions = {row["intent_id"]: row for row in self.store.open_positions()}
        for order in self.store.protection_orders():
            if order["status"] != "SUBMITTED":
                continue
            position = positions.get(order["intent_id"])
            if position is None:
                continue
            try:
                result = self.client.query_algo(position["symbol"], order["client_order_id"])
            except BinanceError as exc:
                if exc.not_found and self._now() - datetime.fromisoformat(order["created_at"]) > timedelta(seconds=60):
                    self.store.set_protection_order(order["id"], "ABSENT")
                    continue
                raise
            status = "TRIGGERED" if str(result.get("actualOrderId", "0")) not in {"", "0", "None"} else "ACTIVE"
            self.store.set_protection_order(order["id"], status, str(result["algoId"]))

    def _cancel_protection(self, position: dict[str, Any], order: dict[str, Any]) -> None:
        try:
            self.client.cancel_algo(position["symbol"], str(order["algo_id"]))
        except BinanceError as exc:
            if not exc.not_found:
                raise
            result = self.client.query_algo_by_id(position["symbol"], str(order["algo_id"]))
            if str(result.get("actualOrderId", "0")) not in {"", "0", "None"}:
                self.store.set_protection_order(order["id"], "TRIGGERED")
                return
            if result.get("algoStatus") not in {"CANCELED", "EXPIRED", "REJECTED"}:
                raise BinanceError("cancellation is unresolved") from exc
        self.store.set_protection_order(order["id"], "CANCELED")

    @staticmethod
    def _market_start(position: dict[str, Any]) -> datetime:
        return datetime.fromisoformat(position.get("market_time") or position["filled_at"]) + (timedelta(0) if position.get("trade_cursor") is not None else timedelta(milliseconds=1))

    def _update_exchange_protection(self, position: dict[str, Any], now: datetime, *, trades: list[dict] | None = None, requested_at: datetime | None = None) -> None:
        """Read unseen trades; publish a tighter exchange stop at most once per poll."""
        start = self._market_start(position)
        if start >= now:
            return
        if trades is None:
            trades = self.client.aggregate_trades(position["symbol"], start, now, position.get("trade_cursor"))
        peak = Decimal(position["protection_peak"] or position["entry_price"])
        entry = Decimal(position["entry_price"])
        rules = self.config.strategy.values["long"]
        activation_price = entry * (1 + Decimal(str(rules["protection"]["activation_return"])))
        activated_at = position.get("protection_activated_at")
        cursor = position.get("trade_cursor")
        for trade in trades:
            trade_id = int(trade["a"])
            if cursor is not None and trade_id != cursor + 1:
                raise BinanceError(f"aggregate trade gap for {position['symbol']}: {cursor} -> {trade_id}")
            cursor = trade_id
            peak = max(peak, Decimal(str(trade["p"])))
            if activated_at is None and peak >= activation_price:
                # The decision is made when this information is observed, not
                # retrospectively at a pre-restart trade timestamp.
                activated_at = self._iso(now)
        target = Decimal(position["target_stop"]) if position.get("target_stop") else None
        if activated_at is not None and (target is None or peak > Decimal(position["protection_peak"])):
            allowed = Decimal(str(position["protection_allowed_retrace"]))
            floor = entry * (1 + 2 * (Decimal(str(rules["slippage_per_side"])) + Decimal(str(rules["taker_fee_per_side"])) ))
            target = max(peak * (1 - allowed), floor, Decimal(position.get("target_stop") or "0"))
            target = stop_trigger_price(target, self.client.symbol_filters(position["symbol"])["tick_size"], "SELL")
        if trades:
            market_time = datetime.fromtimestamp(int(trades[-1]["T"]) / 1000, UTC)
            self.store.update_market(position["intent_id"], cursor, self._iso(market_time), format(peak, "f"), activated_at, format(target, "f") if target else None)
        elif cursor is None:
            # Advance a confirmed empty initial interval; otherwise a quiet
            # first hour would be fetched forever after a restart.
            through = min(requested_at or now, start + timedelta(minutes=59))
            self.store.update_market(position["intent_id"], None, self._iso(through), format(peak, "f"), activated_at, format(target, "f") if target else None)
        if target is None:
            return
        orders = self.store.protection_orders(position["intent_id"])
        if any(order["status"] in {"SUBMITTED", "TRIGGERED"} for order in orders):
            return  # resolve the outstanding write before issuing another one
        active = sorted(orders, key=lambda order: Decimal(order["trigger_price"]), reverse=True)
        if active and Decimal(active[0]["trigger_price"]) >= target and Decimal(active[0]["quantity"]) == Decimal(position["quantity"]):
            for old in active[1:]:
                self._cancel_protection(position, old)
            return
        latest = self.client.latest_price(position["symbol"])
        if latest <= target:
            self._close(position, "PROTECTION")
            return
        order = self.store.begin_protection_order(position["intent_id"], format(target, "f"), position["quantity"])
        try:
            response = self.client.stop_market(position["symbol"], "SELL", "LONG", target, order["client_order_id"], quantity=Decimal(position["quantity"]))
        except BinanceError as exc:
            if exc.rejected:
                self.store.set_protection_order(order["id"], "REJECTED")
                if exc.code == -2021:
                    self._close(position, "PROTECTION")
                    return
            # The old protection and hard stop remain live on any failed update.
            raise
        self.store.set_protection_order(order["id"], "ACTIVE", str(response["algoId"]))
        for old in active:
            self._cancel_protection(position, old)

    def _collect_shadows(self, tasks: list[dict], now: datetime) -> list[tuple[str, dict | None, str | None]]:
        grouped: dict[str, list[tuple[dict, datetime, datetime]]] = {}
        for task in tasks:
            start = shadow_start(task)
            end = min(now.replace(second=0, microsecond=0), datetime.fromisoformat(task["planned_exit_time"]), start + timedelta(minutes=1500))
            if start < end:
                grouped.setdefault(task["symbol"], []).append((task, start, end))
        updates = []
        for symbol, intervals in grouped.items():
            batches: list[tuple[datetime, datetime, list[dict]]] = []
            for task, start, end in sorted(intervals, key=lambda item: item[1]):
                if batches and start <= batches[-1][1] and end - batches[-1][0] <= timedelta(minutes=1500):
                    left, right, group = batches[-1]
                    batches[-1] = (left, max(right, end), [*group, task])
                else:
                    batches.append((start, end, [task]))
            for start, end, group in batches:
                try:
                    # Merge overlapping intervals, but let newer candidates
                    # progress even when an older disjoint path has a gap.
                    bars = self.client.klines(symbol, "1m", int((end - start).total_seconds() // 60), start_time=start, end_time=end - timedelta(milliseconds=1)).to_dicts()
                except BinanceError as exc:
                    updates.extend((task["shadow_id"], None, str(exc)) for task in group)
                    continue
                for task in group:
                    progress, error = advance_shadow(task, bars, end, self.config.strategy.values["long"])
                    updates.append((task["shadow_id"], progress, error))
        return updates

    def _apply_shadows(self, updates: list[tuple[str, dict | None, str | None]], now: datetime) -> None:
        for shadow_id, progress, error in updates:
            if progress and progress.get("exit_time"):
                self.store.complete_shadow_task(shadow_id, progress["exit_time"], progress["active"], progress["maximum"])
            else:
                self.store.save_shadow_progress(shadow_id, progress, error)
        earliest = now - timedelta(days=self.config.strategy.values["long"]["protection"]["window_days"])
        self.store.prune_shadow_history(self._iso(earliest))

    def process_due_shadows(self, now: datetime | None = None) -> None:
        now = now or self._now()
        self._apply_shadows(self._collect_shadows(self.store.due_shadow_tasks(self._iso(now)), now), now)

    def _background_work(self, now: datetime) -> None:
        # Workers only fetch/compute. All state mutations and exchange writes
        # remain serialized on this thread.
        if self._shadow_job is not None and self._shadow_job.done():
            self._apply_shadows(self._shadow_job.result(), now)
            self._shadow_job = None
        minute = now.replace(second=0, microsecond=0)
        if self._shadow_job is None and minute != self._shadow_minute:
            tasks = self.store.due_shadow_tasks(self._iso(minute))
            if tasks:
                self._shadow_job = self._workers.submit(self._collect_shadows, tasks, now)
            self._shadow_minute = minute
        if self._decision_job is not None and self._decision_job[1].done():
            decision, job = self._decision_job
            try:
                collected = job.result()
            except Exception:
                self._decision_job = None
                raise
            self.process_decision(decision, collected=collected)
            self._decision_job = None
        decision = self._due_decision_time(now)
        if decision is not None and self._decision_job is None and not self.store.decision_done(self._iso(decision)):
            if self.store.decision_plan(self._iso(decision)) is not None:
                self.process_decision(decision)
            else:
                first = self.store.short_count(self._iso(decision.replace(hour=6))) if decision.hour == 8 else None
                self._decision_job = (decision, self._workers.submit(self._collect_decision, decision, None, first))

    def smoke_test(self, symbol: str) -> dict[str, str]:
        """A tiny testnet round trip using the same durable recovery as normal orders."""
        account = self.check()
        if (account["positions"] or account["open_orders"] or account["open_algo_orders"]
                or self.store.open_positions() or self.store.pending_intents() or self.store.pending_hard_stops()):
            raise StateError("smoke test requires an empty dedicated testnet account")
        self.client.configure_symbol(symbol)
        filters = self.client.symbol_filters(symbol)
        price = self.client.latest_price(symbol)
        minimum = max(filters["min_qty"], filters["min_notional"] / price)
        quantity = (minimum / filters["step_size"]).to_integral_value(rounding=ROUND_CEILING) * filters["step_size"]
        if quantity > filters.get("max_qty", quantity):
            raise BinanceError("smoke quantity exceeds exchange maximum")
        now = self._now()
        # One minute identity prevents a repeated invocation from duplicating a test.
        intent_id = self._client_id("m", "long", symbol, now)
        if self.store.intent(intent_id) is not None:
            raise StateError("smoke test already attempted in this minute")
        candidate = {"trade_id": intent_id, "strategy": "long", "symbol": symbol, "position_side": "LONG",
                     "decision_time": now, "planned_exit_time": now, "priority_score": 0.0}
        prepared = {"sizing": {"sizing_version": "drawdown-2x-v2"}, "filters": filters, "quantity": quantity,
                    "sample": (self.config.strategy.values["long"]["protection"]["fallback_retrace"], 0, 0),
                    "symbol": symbol, "strategy": "long", "position_side": "LONG",
                    "intent_id": intent_id, "client_order_id": intent_id, "decision": now}
        try:
            outcome = self._open(Admission(candidate, 1), prepared=prepared)
            if outcome != "OPEN":
                raise BinanceError(f"smoke entry failed: {outcome}")
            position = next(row for row in self.store.open_positions() if row["intent_id"] == intent_id)
            self._close(position, "SMOKE_EXIT")
            return {"symbol": symbol, "quantity": position["quantity"], "entry_price": position["entry_price"],
                    "stop_algo_id": str(position["stop_algo_id"])}
        finally:
            position = next((row for row in self.store.open_positions() if row["intent_id"] == intent_id), None)
            if position is not None:
                try:
                    self._close(position, "SMOKE_EXIT")
                except (BinanceError, StateError) as exc:
                    self.store.record_reconciliation("BLOCKED", f"smoke cleanup pending: {exc}")

    def _terminal_order(self, symbol: str, client_id: str, response: dict[str, Any]) -> dict[str, Any]:
        if "status" not in response:
            response = self.client.query_order(symbol, client_id)
        if response.get("status") in {"NEW", "PARTIALLY_FILLED"}:
            try:
                response = self.client.cancel_order(symbol, client_id)
            except BinanceError as exc:
                if not exc.not_found:
                    raise
                response = self.client.query_order(symbol, client_id)
        if response.get("status") not in {"FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"}:
            raise BinanceError(f"order remains unresolved: {client_id}")
        return response

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

    def seed_shadow_history(self, cutoff: datetime | None = None, *, require_minimum: bool = True) -> int:
        cutoff = cutoff or self._now()
        earliest = cutoff - timedelta(days=self.config.strategy.values["long"]["protection"]["window_days"])
        records: dict[str, tuple] = {}
        columns = ["symbol", "signal_time", "shadow_exit_time", "shadow_activated", "shadow_max_retrace"]
        for window in ("research", "forward_2026_jul_aug"):
            path = self.config.root / "results" / "local" / window / "long_trades.parquet"
            if path.exists():
                frame = pl.read_parquet(path, columns=columns).filter(pl.col("shadow_exit_time").is_between(earliest, cutoff))
                for row in frame.to_dicts():
                    source_id = self._shadow_id(row["symbol"], row["signal_time"])
                    records[source_id] = (source_id, self._iso(row["shadow_exit_time"]), bool(row["shadow_activated"]), row["shadow_max_retrace"])
        if not records:
            path = self.config.root / "seed" / "initial_shadow_history.csv"
            if path.exists():
                frame = pl.read_csv(path, try_parse_dates=True)
                if "source_id" not in frame.columns:
                    raise StateError("P90 seed requires canonical candidate identities")
                for row in frame.filter(pl.col("shadow_exit_time").is_between(earliest, cutoff)).to_dicts():
                    source_id = row["source_id"]
                    records[source_id] = (source_id, self._iso(row["shadow_exit_time"]), bool(row["shadow_activated"]), row["shadow_max_retrace"])
        self.store.prune_shadow_history(self._iso(earliest))
        inserted = self.store.seed_shadow_history(list(records.values())) if records else 0
        total, activated = self.store.shadow_history_stats()
        rules = self.config.strategy.values["long"]["protection"]
        if total < rules["minimum_history"] or activated < rules["minimum_activated_history"]:
            message = f"insufficient P90 warm-up: total={total}, activated={activated}"
            if require_minimum:
                raise StateError(message)
            self.store.record_reconciliation("P90_FALLBACK", message)
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

    def _due_decision_time(self, now: datetime) -> datetime | None:
        """Return the current hour's decision time while its collection window is open."""
        decision_time = now.replace(minute=0, second=0, microsecond=0)
        decision_hours = self.config.strategy.values["features"]["strategy_decision_hours_utc"]
        if decision_time.hour not in decision_hours:
            return None
        if now < decision_time or now >= decision_time + timedelta(seconds=self.config.decision_deadline_seconds):
            return None
        return decision_time

    def run_forever(self) -> None:
        self.check()
        self.seed_shadow_history(require_minimum=False)
        next_poll = self._now()
        next_full_reconcile = next_poll
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
                        errors: list[str] = []
                        reconciled = False
                        try:
                            reconciled = self.reconcile(full=full_reconcile)
                            self.store.resolve_entry_block("ACCOUNT_RECONCILIATION")
                        except (BinanceError, StateError) as exc:
                            errors.append(f"reconciliation: {exc}")
                            self.store.block_entry("ACCOUNT_RECONCILIATION", str(exc))
                        try:
                            self.process_long_protection(self._now(), background=True)
                        except (BinanceError, StateError) as exc:
                            errors.append(f"protection: {exc}")
                        try:
                            self.process_due_exits(self._now())
                        except (BinanceError, StateError) as exc:
                            errors.append(f"exits: {exc}")
                        minute_end = now.replace(second=0, microsecond=0)
                        if minute_end != self._last_equity_minute:
                            try:
                                self._completed_equity(minute_end)
                                self.store.resolve_entry_block("EQUITY_SNAPSHOT")
                            except (BinanceError, StateError) as exc:
                                errors.append(f"equity: {exc}")
                                self.store.block_entry("EQUITY_SNAPSHOT", str(exc))
                        self._heartbeat(reconciled=full_reconcile and reconciled, error="; ".join(errors) or None)
                        next_poll = now + timedelta(seconds=self.config.account_poll_seconds)
                        if full_reconcile:
                            next_full_reconcile = now + timedelta(seconds=self.config.idle_reconcile_seconds)
                    self._background_work(self._now())
                    self._stop_requested.wait(1)
                except StateError as exc:
                    self._heartbeat(error=str(exc))
                    raise
                except BinanceError as exc:
                    self._heartbeat(error=str(exc))
                    self._stop_requested.wait(self.config.account_poll_seconds)
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
