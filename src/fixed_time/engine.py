from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import hashlib
import json
import signal
from threading import Event
from typing import Any
from uuid import uuid4

from . import __version__
from .config import LiveConfig
from .exchange import Binance, ExchangeError, protective_price, quantize_down
from .state import StateError, Store
from .strategy import Admission, admissions, candidates, entry_rejection


class Engine:
    def __init__(self, config: LiveConfig, exchange: Binance | None = None, store: Store | None = None):
        self.config = config
        self.exchange = exchange or Binance(config)
        self.store = store or Store(config.database_path)
        self.stop_requested = Event()
        self.run_id = uuid4().hex
        self._last_reconcile: datetime | None = None
        self._last_equity_minute: datetime | None = None

    def close(self) -> None:
        self.store.close()

    @staticmethod
    def _iso(value: datetime) -> str:
        return value.astimezone(UTC).isoformat()

    def now(self) -> datetime:
        return self.exchange.now().astimezone(UTC)

    @staticmethod
    def client_id(role: str, lot_id: str, sequence: int = 0) -> str:
        # The digest is deliberately the only symbol-derived content. It is
        # ASCII, legal under Binance's regex, stable and always under 36 chars.
        digest = hashlib.sha256(f"{lot_id}:{role}:{sequence}".encode("utf-8")).hexdigest()[:24]
        return f"ft2-{role}-{digest}"[:36]

    @staticmethod
    def _side(strategy: str, opening: bool) -> str:
        return "BUY" if (strategy == "long") == opening else "SELL"

    @staticmethod
    def check_exchange(exchange: Binance) -> dict[str, Any]:
        exchange.sync_time()
        if not exchange.position_mode():
            raise StateError("account must use Hedge Mode")
        if exchange.multi_asset_mode():
            raise StateError("account must use Single-Asset Mode")
        equity = exchange.equity()
        if equity <= 0:
            raise StateError("account equity must be positive")
        return {"equity": format(equity, "f"), "positions": exchange.positions(),
                "open_orders": exchange.open_orders(), "open_algos": exchange.open_algos()}

    def check(self) -> dict[str, Any]:
        return self.check_exchange(self.exchange)

    @staticmethod
    def _position_map(rows: list[dict]) -> dict[tuple[str, str], Decimal]:
        return {(str(x["symbol"]), str(x["positionSide"])): abs(Decimal(str(x["positionAmt"]))) for x in rows if Decimal(str(x["positionAmt"])) != 0}

    @staticmethod
    def _lot_groups(lots: list[dict]) -> dict[tuple[str, str], list[dict]]:
        result: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for lot in lots:
            result[(str(lot["symbol"]), str(lot["position_side"]))].append(lot)
        return result

    def _cancel_algos(self, lot: dict, *, keep_stop: bool = False) -> None:
        for field in ("cap_algo_id",) if keep_stop else ("stop_algo_id", "cap_algo_id"):
            if lot.get(field):
                self.exchange.cancel_algo(str(lot["symbol"]), str(lot[field]))
                reason = "PROFIT_CAP400" if field == "cap_algo_id" else ("PROFIT_FLOOR270" if lot.get("profit_armed_at") else "HARD_STOP")
                self._algo_fill(lot, field, reason)
                response = self.exchange.query_algo_id(str(lot["symbol"]), str(lot[field]))
                if response.get("algoStatus") not in {"CANCELED", "EXPIRED", "REJECTED", "FINISHED"}:
                    raise StateError(f"conditional cancellation unresolved: {lot[field]}")
        if not keep_stop:
            self.store.set_algos(str(lot["lot_id"]), stop=None, cap=None)
        if any(x["lot_id"] == lot["lot_id"] for x in self.store.pending_orders()):
            raise StateError(f"conditional fill still pending: {lot['lot_id']}")

    def _trigger(self, lot: dict, kind: str) -> Decimal:
        reference = Decimal(str(lot["entry_reference"]))
        rules = self.config.strategy.values
        if kind == "cap":
            raw = reference * (Decimal(1) + Decimal(str(rules["long"]["profit_guard"]["cap_return"])))
            side = "SELL"
        elif lot["strategy"] == "long":
            loss = (Decimal(str(rules["long"]["profit_guard"]["floor_return"]))
                    if lot.get("profit_armed_at") else Decimal(str(rules["long"]["hard_stop_return"])))
            raw = reference * (Decimal(1) + loss)
            side = "SELL"
        else:
            raw = reference * (Decimal(1) + Decimal(str(rules["short"]["hard_stop_return"])))
            side = "BUY"
        return protective_price(raw, self.exchange.symbol_filters(str(lot["symbol"]))["tick_size"], side)

    def _place_stop(self, lot: dict, *, replace: bool = False) -> str:
        old = str(lot.get("stop_algo_id") or "")
        role = "f" if lot.get("profit_armed_at") else "s"
        if old:
            self.exchange.cancel_algo(str(lot["symbol"]), old)
            self._algo_fill(lot, "stop_algo_id", self._stop_reason(lot))
            response = self.exchange.query_algo_id(str(lot["symbol"]), old)
            if response.get("algoStatus") not in {"CANCELED","EXPIRED","REJECTED","FINISHED"}:
                raise StateError(f"old stop unresolved: {old}")
            self.store.set_algos(str(lot["lot_id"]), stop=None)
            lot = self.store.lot(str(lot["lot_id"])) or lot
            if lot["status"] != "OPEN":
                return ""
            if any(x["lot_id"] == lot["lot_id"] for x in self.store.pending_orders()):
                raise StateError(f"stop fill still pending: {lot['lot_id']}")
        sequence = len(self.store.algos(str(lot["lot_id"])))
        client = self.client_id(role, str(lot["lot_id"]), sequence)
        result = self._conditional(lot, self._side(str(lot["strategy"]), False), self._trigger(lot, "stop"), "STOP_MARKET", client)
        algo_id = str(result["algoId"])
        self.store.set_algos(str(lot["lot_id"]), stop=algo_id)
        return algo_id

    def _place_cap(self, lot: dict) -> str:
        old = lot.get("cap_algo_id")
        if old:
            self.exchange.cancel_algo(str(lot["symbol"]),str(old))
            self._algo_fill(lot,"cap_algo_id","PROFIT_CAP400")
            response = self.exchange.query_algo_id(str(lot["symbol"]),str(old))
            if response.get("algoStatus") not in {"CANCELED","EXPIRED","REJECTED","FINISHED"}:
                raise StateError(f"old cap unresolved: {old}")
            self.store.set_algos(str(lot["lot_id"]),cap=None)
            lot = self.store.lot(str(lot["lot_id"])) or lot
            if lot["status"] != "OPEN":
                return ""
            if any(x["lot_id"] == lot["lot_id"] for x in self.store.pending_orders()):
                raise StateError(f"cap fill still pending: {lot['lot_id']}")
        sequence = len(self.store.algos(str(lot["lot_id"])))
        result = self._conditional(lot, "SELL", self._trigger(lot, "cap"), "TAKE_PROFIT_MARKET",
                                   self.client_id("c", str(lot["lot_id"]), sequence))
        algo_id = str(result["algoId"])
        self.store.set_algos(str(lot["lot_id"]), cap=algo_id)
        return algo_id

    def _conditional(self, lot: dict, side: str, trigger: Decimal, order_type: str, client: str) -> dict:
        kind = "cap" if order_type == "TAKE_PROFIT_MARKET" else "stop"
        if any(x["status"] == "SUBMITTED" for x in self.store.algos(str(lot["lot_id"]))):
            raise StateError(f"conditional submission unresolved: {lot['lot_id']}")
        self.store.begin_algo(client, lot, kind, trigger)
        try:
            result = self.exchange.conditional_order(str(lot["symbol"]), side, str(lot["position_side"]),
                Decimal(str(lot["quantity"])), trigger, order_type, client)
        except ExchangeError as exc:
            # A timed-out write or a duplicate client ID is resolved by the
            # idempotency key before another conditional order is attempted.
            try:
                result = self.exchange.query_algo_client(str(lot["symbol"]), client)
            except ExchangeError:
                if exc.code in {-1100,-1111,-2019,-2021,-2022,-4003,-4004,-4014,-4023}:
                    self.store.update_algo(client, None, "REJECTED")
                raise exc
        self.store.update_algo(client, str(result["algoId"]), "ACKNOWLEDGED")
        return result

    def _stop_reason(self, lot: dict) -> str:
        records = [x for x in self.store.algos(str(lot["lot_id"])) if x["algo_id"] == lot.get("stop_algo_id")]
        if records:
            return "PROFIT_FLOOR270" if Decimal(records[-1]["trigger_price"]) > Decimal(lot["entry_reference"]) and lot["strategy"] == "long" else "HARD_STOP"
        return "PROFIT_FLOOR270" if lot.get("profit_armed_at") else "HARD_STOP"

    def _algo_fill(self, lot: dict, field: str, reason: str) -> bool:
        algo_id = lot.get(field)
        if not algo_id:
            return False
        try:
            algo = self.exchange.query_algo_id(str(lot["symbol"]), str(algo_id))
        except ExchangeError as exc:
            if exc.not_found:
                return False
            raise
        actual = str(algo.get("actualOrderId", "0"))
        if actual in {"", "0", "None"}:
            return False
        order = self.exchange.query_order_id(str(lot["symbol"]), actual)
        filled = Decimal(str(order.get("executedQty", "0")))
        if filled <= 0:
            trades = self.exchange.user_trades(str(lot["symbol"]), actual)
            filled = sum((Decimal(str(x.get("qty", "0"))) for x in trades), Decimal())
        client = f"algo:{lot['symbol']}:{actual}"
        self.store.begin_order({"client_id":client,"lot_id":lot["lot_id"],"role":"EXIT","symbol":lot["symbol"],
            "side":self._side(lot["strategy"],False),"position_side":lot["position_side"],
            "requested_quantity":lot["quantity"],"reason":reason})
        order = dict(order, executedQty=str(filled), orderId=actual)
        self.store.apply_order(client, order, self.now())
        return filled > 0

    def _assume_exchange_flat(self, lots: list[dict]) -> None:
        for lot in lots:
            self.store.block(f"UNCONFIRMED_EXIT:{lot['lot_id']}",
                             "exchange exposure is zero but exit fills are missing; keep ledger for reconciliation")

    def _recover_pending(self) -> None:
        for order in self.store.pending_orders():
            try:
                response = (self.exchange.query_order_id(str(order["symbol"]), str(order["exchange_order_id"]))
                            if order.get("exchange_order_id") else self.exchange.query_order(str(order["symbol"]), str(order["client_id"])))
            except ExchangeError as exc:
                if exc.not_found and self.now() - datetime.fromisoformat(str(order["created_at"])) > timedelta(minutes=2):
                    if Decimal(order["applied_quantity"]) > 0:
                        raise StateError(f"part-filled order disappeared: {order['client_id']}") from exc
                    self.store.apply_order(str(order["client_id"]), {"status":"NOT_FOUND"}, self.now())
                    continue
                raise
            self.store.apply_order(str(order["client_id"]), response, self.now())
            if order["role"] == "ENTRY" and response.get("status") in {"NEW","PARTIALLY_FILLED"}:
                self.exchange.cancel_order(str(order["symbol"]),str(order["client_id"]))
                self.store.apply_order(str(order["client_id"]),self.exchange.query_order(str(order["symbol"]),str(order["client_id"])),self.now())
        for order in self.store.algos():
            lot = self.store.lot(order["lot_id"])
            field = order["kind"] + "_algo_id"
            unbound = order["status"] == "ACKNOWLEDGED" and str(lot.get(field)) != str(order["algo_id"])
            if order["status"] != "SUBMITTED" and not unbound:
                continue
            try:
                result = self.exchange.query_algo_client(lot["symbol"], order["client_id"])
            except ExchangeError as exc:
                if exc.not_found and self.now() - datetime.fromisoformat(order["created_at"]) > timedelta(minutes=2):
                    self.store.update_algo(order["client_id"], None, "NOT_FOUND")
                    continue
                raise
            state = str(result.get("algoStatus","NEW"))
            self.store.update_algo(order["client_id"], str(result["algoId"]),
                                   "ACKNOWLEDGED" if state in {"NEW","TRIGGERING","TRIGGERED"} else state)
            if lot["status"] == "OPEN" and not lot.get(field):
                self.store.set_algos(order["lot_id"], **{order["kind"]:str(result["algoId"])})
            elif unbound:
                self._algo_fill(dict(lot,**{field:str(result["algoId"])}),field,
                                "PROFIT_CAP400" if order["kind"] == "cap" else self._stop_reason(dict(lot,stop_algo_id=str(result["algoId"]))))

    def reconcile(self) -> bool:
        self._recover_pending()
        for lot in self.store.open_lots():
            self._algo_fill(lot, "stop_algo_id", self._stop_reason(lot))
            self._algo_fill(lot, "cap_algo_id", "PROFIT_CAP400")
        exchange_positions = self._position_map(self.exchange.positions())
        lots = self.store.open_lots()
        groups = self._lot_groups(lots)
        unknown = set(exchange_positions) - set(groups)
        if unknown:
            self.store.block("UNKNOWN_EXCHANGE_POSITION", f"unknown exchange positions: {sorted(unknown)}")
        else:
            self.store.resolve("UNKNOWN_EXCHANGE_POSITION")

        mismatch = []
        for key, group in groups.items():
            observed = exchange_positions.get(key, Decimal())
            expected = sum((Decimal(str(x["quantity"])) for x in group), Decimal())
            if observed != expected:
                for lot in group:
                    stop_reason = self._stop_reason(lot)
                    self._algo_fill(lot, "stop_algo_id", stop_reason)
                    self._algo_fill(lot, "cap_algo_id", "PROFIT_CAP400")
                fresh = [x for x in self.store.open_lots() if (x["symbol"], x["position_side"]) == key]
                expected = sum((Decimal(str(x["quantity"])) for x in fresh), Decimal())
                if observed == 0 and expected > 0:
                    self._assume_exchange_flat(fresh)
                if observed != expected:
                    mismatch.append(f"{key}: exchange={observed} local={expected}")
        if mismatch:
            self.store.block("POSITION_QUANTITY_MISMATCH", "; ".join(mismatch))
        else:
            self.store.resolve("POSITION_QUANTITY_MISMATCH")
        for incident in self.store.incidents():
            if incident["code"].startswith("UNCONFIRMED_EXIT:"):
                lot = self.store.lot(incident["code"].split(":",1)[1])
                if lot and lot["status"] == "CLOSED":
                    self.store.resolve(incident["code"])

        open_algos = self.exchange.open_algos()
        open_algo_ids = {str(x.get("algoId")) for x in open_algos}
        for lot in self.store.open_lots():
            group = self._lot_groups(self.store.open_lots())[(lot["symbol"],lot["position_side"])]
            if sum((Decimal(x["quantity"]) for x in group),Decimal()) != exchange_positions.get((lot["symbol"],lot["position_side"]),Decimal()):
                continue
            if any(x["lot_id"] == lot["lot_id"] and x["role"] == "EXIT" for x in self.store.pending_orders()):
                continue
            records = self.store.algos(str(lot["lot_id"]))
            stop = next((x for x in records if x["algo_id"] == lot.get("stop_algo_id")), None)
            if stop is None:
                observed_stop = next((x for x in open_algos if str(x.get("algoId")) == str(lot.get("stop_algo_id"))),None)
                if observed_stop and "triggerPrice" in observed_stop and "quantity" in observed_stop:
                    stop = {"trigger_price":observed_stop["triggerPrice"],"quantity":observed_stop["quantity"]}
            if lot.get("protection_version") != "r2":
                self._place_stop(lot, replace=True)
                lot = self.store.lot(str(lot["lot_id"])) or lot
            elif (not lot.get("stop_algo_id") or str(lot["stop_algo_id"]) not in open_algo_ids
                  or (stop and (Decimal(stop["quantity"]) != Decimal(lot["quantity"]) or Decimal(stop["trigger_price"]) != self._trigger(lot,"stop")))):
                self._place_stop(lot, replace=bool(lot.get("stop_algo_id")))
            lot = self.store.lot(str(lot["lot_id"])) or lot
            if lot["status"] != "OPEN":
                continue
            cap = next((x for x in records if x["algo_id"] == lot.get("cap_algo_id")),None)
            if lot["strategy"] == "long" and (not lot.get("cap_algo_id") or str(lot["cap_algo_id"]) not in open_algo_ids
                                               or (cap and Decimal(cap["quantity"]) != Decimal(lot["quantity"]))):
                self._place_cap(lot)
            self.store.set_protection_version(str(lot["lot_id"]))
        tracked = {str(x[field]) for x in self.store.open_lots() for field in ("stop_algo_id", "cap_algo_id") if x.get(field)}
        unknown_algos = []
        for order in open_algos:
            algo_id, client = str(order.get("algoId")), str(order.get("clientAlgoId", ""))
            if algo_id in tracked:
                continue
            record = next((x for x in self.store.algos() if x["algo_id"] == algo_id), None)
            if record and self.store.lot(record["lot_id"])["status"] == "CLOSED":
                self.exchange.cancel_algo(str(order["symbol"]), algo_id)
            else:
                unknown_algos.append(algo_id)
        if unknown_algos:
            self.store.block("UNKNOWN_EXCHANGE_ALGO", f"unknown exchange algo orders: {sorted(unknown_algos)}")
        else:
            self.store.resolve("UNKNOWN_EXCHANGE_ALGO")
        unknown_orders = []
        known_orders = self.store.known_order_ids()
        for order in self.exchange.open_orders():
            client = str(order.get("clientOrderId", ""))
            if client not in known_orders:
                unknown_orders.append(client)
        if unknown_orders:
            self.store.block("UNKNOWN_EXCHANGE_ORDER", f"unknown exchange orders: {sorted(unknown_orders)}")
        else:
            self.store.resolve("UNKNOWN_EXCHANGE_ORDER")
        if self.store.pending_orders() or any(x["status"] == "SUBMITTED" for x in self.store.algos()):
            self.store.block("PENDING_ORDER", "waiting for exchange order settlement")
        else:
            self.store.resolve("PENDING_ORDER")
        for incident in self.store.incidents():
            if incident["code"].startswith("LOT:"):
                lot = self.store.lot(incident["code"][4:])
                if lot and lot["status"] == "CLOSED":
                    self.store.resolve(incident["code"])
        return not self.store.blocked()

    def _eligible(self, rows: list[dict], tradable: set[str], now: datetime) -> list[dict]:
        lots = self.store.open_lots()
        bought, stopped = self.store.long_day_locks(now.date())
        eligible = []
        for row in rows:
            symbol, direction = row["symbol"], row["strategy"]
            if symbol not in tradable:
                row["rejection"] = "NOT_ON_TESTNET"
                continue
            rejection = entry_rejection(row, lots, bought, stopped)
            if rejection:
                row["rejection"] = rejection
                continue
            eligible.append(row)
        return eligible

    def _open(self, admission: Admission) -> str:
        row, target = admission.candidate, admission.target_notional
        lot_id = str(row["trade_id"])
        if self.store.lot(lot_id):
            return "ALREADY_OPEN"
        client = self.client_id("e", lot_id)
        existing = self.store.order(client)
        if existing:
            return str(existing["status"])
        if not self.config.trading_enabled:
            return "TRADING_DISABLED"
        if self.store.pending_orders() or self.store.blocked():
            return "BLOCKED"
        lots = self.store.open_lots()
        equity = self.exchange.equity()
        e0 = self.store.day_reference(self.now().date(), equity, self.now())
        occupied = sum((Decimal(x["entry_notional"]) for x in lots),Decimal())
        direction_used = sum((Decimal(x["entry_notional"]) for x in lots if x["strategy"] == row["strategy"]),Decimal())
        reserve = Decimal(1) + Decimal(str(self.config.strategy.values["execution"]["slippage_per_side"])) + Decimal(str(self.config.strategy.values["execution"]["taker_fee_per_side"]))
        target = max(Decimal(), min(target, e0*Decimal("0.5")-direction_used, (min(e0,equity)-occupied)/reserve))
        self.exchange.configure_symbol(str(row["symbol"]))
        filters = self.exchange.symbol_filters(str(row["symbol"]))
        price = self.exchange.latest_price(str(row["symbol"]))
        quantity = quantize_down(min(target / price, filters["max_qty"]), filters["step_size"])
        if quantity < filters["min_qty"] or quantity * price < filters["min_notional"]:
            return "BELOW_EXCHANGE_MINIMUM"
        if target / Decimal(self.config.leverage) + target*(reserve-1) > self.exchange.available_balance():
            return "INSUFFICIENT_MARGIN"
        metadata = {"lot_id": lot_id, "client_id": client, "strategy": row["strategy"], "source": row["source"],
                    "symbol": row["symbol"], "position_side": row["position_side"],
                    "decision_time": self._iso(row["decision_time"]), "planned_exit_time": self._iso(row["planned_exit_time"]),
                    "entry_reference": str(row["reference_price"])}
        self.store.begin_order({"client_id": client, "lot_id": lot_id, "role": "ENTRY", "symbol": row["symbol"],
            "side": self._side(row["strategy"], True), "position_side": row["position_side"],
            "requested_quantity": quantity, "metadata": metadata})
        try:
            response = self.exchange.market_order(str(row["symbol"]), self._side(row["strategy"], True),
                                                  str(row["position_side"]), quantity, client)
        except ExchangeError as exc:
            if exc.code in {-1100,-1111,-2019,-2021,-2022,-4003,-4004,-4014,-4023}:
                self.store.apply_order(client, {"status":"REJECTED"}, self.now())
                self.store.event("ERROR", "ENTRY_REJECTED", f"{row['symbol']}: {exc}")
                return "REJECTED"
            response = self.exchange.query_order(str(row["symbol"]), client)
        self.store.apply_order(client, response, self.now())
        if response.get("status") in {"NEW","PARTIALLY_FILLED"}:
            self.exchange.cancel_order(str(row["symbol"]),client)
            response = self.exchange.query_order(str(row["symbol"]),client)
            self.store.apply_order(client,response,self.now())
        lot = self.store.lot(lot_id)
        if lot:
            self._place_stop(lot)
            if lot["strategy"] == "long":
                self._place_cap(lot)
        return "OPEN" if response.get("status") == "FILLED" else str(response.get("status", "UNRESOLVED"))

    def collect_candidates(self, decision: datetime) -> list[dict]:
        # Separate read-only adapter keeps market collection off the exit loop.
        exchange = Binance(self.config)
        frame = exchange.hourly_snapshot(exchange.market_symbols(), self.config.strategy.values["features"]["hourly_warmup_hours"] + 1, decision)
        return candidates(frame, decision, self.config.strategy)

    def process_decision(self, decision: datetime, rows: list[dict] | None = None) -> list[Admission]:
        decision = decision.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
        key = self._iso(decision)
        existing = self.store.decision(key)
        if existing and existing["status"] in {"COMPLETE", "EXPIRED"}:
            return []
        if self.now() >= decision + timedelta(seconds=self.config.decision_deadline_seconds):
            self.store.save_decision(key, "EXPIRED", {"candidates": rows, "reason": "decision deadline exceeded"})
            return []
        detail = json.loads(existing["detail_json"]) if existing else {}
        if "plan" in detail:
            plan = []
            for saved in detail["plan"]:
                row = dict(saved["candidate"])
                for field in ("decision_time","entry_time","planned_exit_time"):
                    row[field] = datetime.fromisoformat(row[field])
                plan.append(Admission(row,Decimal(saved["target_notional"])))
        else:
            rows = self.collect_candidates(decision) if rows is None else rows
            eligible = self._eligible(rows, self.exchange.trading_symbols(), decision)
            equity = self.exchange.equity()
            e0 = self.store.day_reference(decision.date(), equity, self.now())
            plan = admissions(eligible, self.store.open_lots(), e0, equity, self.config.strategy)
            detail = {"candidates":rows,"E0":str(e0),"equity":str(equity),
                      "plan":[{"candidate":x.candidate,"target_notional":str(x.target_notional)} for x in plan]}
            self.store.save_decision(key,"RUNNING",detail)
        if self.store.blocked() or self.store.pending_orders():
            return plan
        outcomes = detail.setdefault("admissions",[])
        for item in plan:
            if self.now() >= decision + timedelta(seconds=self.config.decision_deadline_seconds):
                break
            if self.store.blocked() or self.store.pending_orders():
                return plan
            outcome = self._open(item)
            outcomes.append({"symbol":item.candidate["symbol"],"target_notional":str(item.target_notional),"outcome":outcome})
            self.store.save_decision(key,"RUNNING",detail)
        self.store.save_decision(key, "COMPLETE", detail)
        return plan

    def _exchange_quantity(self, lot: dict) -> Decimal:
        return self._position_map(self.exchange.positions()).get((str(lot["symbol"]), str(lot["position_side"])), Decimal())

    def _close_lot(self, lot: dict, reason: str) -> None:
        if any(x["lot_id"] == lot["lot_id"] for x in self.store.pending_orders()):
            return
        observed = self._exchange_quantity(lot)
        group = self._lot_groups(self.store.open_lots())[(lot["symbol"],lot["position_side"])]
        if observed != sum((Decimal(x["quantity"]) for x in group),Decimal()):
            if observed == 0:
                self._assume_exchange_flat(group)
                return
            raise StateError(f"cannot close unresolved quantity: {lot['lot_id']}")
        # Resolve conditional fills after cancellation before issuing a market
        # close. Otherwise a triggered stop can consume a sibling lot's quantity.
        had_algos = bool(lot.get("stop_algo_id") or lot.get("cap_algo_id"))
        self._cancel_algos(lot)
        lot = self.store.lot(str(lot["lot_id"])) or lot
        if lot["status"] != "OPEN":
            return
        observed = self._exchange_quantity(lot) if had_algos else observed
        if observed <= 0:
            self._assume_exchange_flat([lot])
            return
        group = self._lot_groups(self.store.open_lots())[(lot["symbol"],lot["position_side"])]
        if observed != sum((Decimal(x["quantity"]) for x in group),Decimal()):
            raise StateError(f"cannot close unresolved quantity: {lot['lot_id']}")
        quantity = min(Decimal(str(lot["quantity"])), self.exchange.symbol_filters(str(lot["symbol"]))["max_qty"])
        client = self.client_id("x", str(lot["lot_id"]), len(self.store.known_order_ids()))
        order = self.store.begin_order({"client_id": client, "lot_id": lot["lot_id"], "role": "EXIT", "symbol": lot["symbol"],
            "side": self._side(str(lot["strategy"]), False), "position_side": lot["position_side"],
            "requested_quantity": quantity, "reason": reason})
        if order["status"] != "SUBMITTED":
            return
        try:
            response = self.exchange.market_order(str(lot["symbol"]), self._side(str(lot["strategy"]), False),
                                                  str(lot["position_side"]), quantity, client)
        except ExchangeError as exc:
            if exc.code == -2022:
                # A concurrently triggered stop can remove the position between
                # preflight and submission. Refresh exposure before deciding.
                if self._exchange_quantity(lot) == 0:
                    self.store.apply_order(client, {"status":"REJECTED"}, self.now())
                    self._assume_exchange_flat([lot])
                    return
                self.store.apply_order(client, {"status":"REJECTED"}, self.now())
                self.store.event("WARN", "REDUCE_ONLY_REJECTED", f"{lot['lot_id']}: refreshed for next reconciliation")
                return
            try:
                response = self.exchange.query_order(str(lot["symbol"]), client)
            except ExchangeError:
                raise exc
        self.store.apply_order(client, response, self.now())

    def manage_positions(self, now: datetime | None = None) -> None:
        now = (now or self.now()).astimezone(UTC)
        prices: dict[str, Decimal] = {}
        for lot in self.store.open_lots():
            try:
                self._manage_lot(lot, now, prices)
                self.store.resolve(f"LOT:{lot['lot_id']}")
            except (ExchangeError,StateError,ValueError) as exc:
                self.store.block(f"LOT:{lot['lot_id']}",str(exc))

    def _manage_lot(self, lot: dict, now: datetime, prices: dict) -> None:
        symbol = str(lot["symbol"])
        if symbol not in prices:
            prices[symbol] = self.exchange.latest_price(symbol)
        price = prices[symbol]
        reference = Decimal(str(lot["entry_reference"]))
        stop = self._trigger(lot,"stop")
        if (lot["strategy"] == "long" and price <= stop) or (lot["strategy"] == "short" and price >= stop):
            self._close_lot(lot, "PROFIT_FLOOR270" if lot.get("profit_armed_at") else "HARD_STOP")
            return
        if lot["strategy"] == "long":
            if not lot.get("first_extension_activation") and price >= reference * Decimal("1.3"):
                self.store.arm_extension(str(lot["lot_id"]), now)
                lot["first_extension_activation"] = now.isoformat()
            if not lot.get("profit_armed_at") and price >= reference * Decimal("4"):
                self.store.arm_profit(str(lot["lot_id"]), now)
                lot["profit_armed_at"] = now.isoformat()
                self._place_stop(lot, replace=True)
                lot = self.store.lot(str(lot["lot_id"])) or lot
                if lot["status"] != "OPEN":
                    return
            if price >= reference * Decimal("5"):
                self._close_lot(lot, "PROFIT_CAP400")
                return
        scheduled = datetime.fromisoformat(str(lot["scheduled_exit_time"]))
        if now < scheduled:
            return
        if lot["strategy"] == "long" and not bool(lot["extended"]) and lot.get("first_extension_activation"):
            activation = datetime.fromisoformat(str(lot["first_extension_activation"]))
            planned = datetime.fromisoformat(str(lot["planned_exit_time"]))
            if planned - timedelta(hours=4) < activation <= planned:
                self.store.extend(str(lot["lot_id"]), planned + timedelta(hours=24))
                if now < planned + timedelta(hours=24):
                    return
                lot["extended"] = 1
        self._close_lot(lot, "EXTENSION_CAP" if lot.get("extended") else "PLANNED_EXIT")

    def _due_decision(self, now: datetime) -> datetime | None:
        decision = now.replace(minute=0, second=0, microsecond=0)
        seconds = (now - decision).total_seconds()
        if decision.hour not in self.config.strategy.values["features"]["strategy_decision_hours_utc"]:
            return None
        if not self.config.decision_delay_seconds <= seconds < self.config.decision_deadline_seconds:
            return None
        return decision

    def run_forever(self) -> None:
        self.check()
        if not self.config.trading_enabled:
            raise StateError("live-run requires TRADING_ENABLED=true; use live-check for read-only validation")
        self.store.deployment(self.run_id, __version__)
        pool = ThreadPoolExecutor(max_workers=1)
        collection = None
        collecting_decision = None
        previous = {}
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, lambda *_: self.stop_requested.set())
        try:
            while not self.stop_requested.is_set():
                now = self.now()
                stages = []
                if self._last_reconcile is None or now - self._last_reconcile >= timedelta(seconds=self.config.reconcile_seconds):
                    stages.append(("RECONCILE",self.reconcile))
                    self._last_reconcile = now
                stages.append(("MANAGE",lambda: self.manage_positions(now)))
                def sample_equity():
                    minute = now.replace(second=0, microsecond=0)
                    if minute != self._last_equity_minute:
                        equity = self.exchange.equity()
                        self.store.record_equity(now, equity)
                        self.store.day_reference(now.date(), equity, now)
                        self._last_equity_minute = minute
                stages.append(("EQUITY",sample_equity))
                for code, action in stages:
                    try:
                        action()
                        self.store.resolve(code)
                    except (ExchangeError, StateError, ValueError) as exc:
                        self.store.block(code, str(exc))
                try:
                    if collection is not None and collection.done():
                        job, collection = collection, None
                        self.store.resolve("SIGNALS")
                        self.process_decision(collecting_decision, job.result())
                    decision = self._due_decision(now)
                    if decision is not None:
                        saved = self.store.decision(self._iso(decision))
                        if saved and saved["status"] == "RUNNING":
                            self.store.resolve("SIGNALS")
                            self.process_decision(decision, [])
                        elif not saved and collection is None:
                            collecting_decision = decision
                            collection = pool.submit(self.collect_candidates, decision)
                except (ExchangeError, StateError, ValueError) as exc:
                    self.store.block("SIGNALS", str(exc))
                self.store.heartbeat(self.now())
                self.stop_requested.wait(self.config.account_poll_seconds)
        finally:
            pool.shutdown(wait=False,cancel_futures=True)
            for signum, handler in previous.items():
                signal.signal(signum, handler)
