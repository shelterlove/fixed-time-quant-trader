from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import hmac
import json
from pathlib import Path
import sqlite3
from threading import Lock
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from .config import TESTNET_FUTURES_URL
from .manual import ManualActionError, submit_action


PERIODS = {"1d": (timedelta(days=1), 60), "7d": (timedelta(days=7), 600),
           "30d": (timedelta(days=30), 3600)}


def _history(db: sqlite3.Connection, now: datetime, period: str) -> list[dict]:
    duration, bucket_seconds = PERIODS[period]
    return [dict(x) for x in db.execute("""SELECT e.observed_at,e.equity FROM v2_equity e
        JOIN (SELECT MAX(observed_at) AS observed_at FROM v2_equity WHERE observed_at>=?
              GROUP BY CAST(strftime('%s',observed_at) AS INTEGER)/?) b
        ON e.observed_at=b.observed_at ORDER BY e.observed_at""",
        ((now-duration).isoformat(), bucket_seconds))]


def _decisions(db: sqlite3.Connection) -> list[dict]:
    results = []
    for row in db.execute("SELECT * FROM v2_decisions ORDER BY decision_time DESC LIMIT 8"):
        try:
            detail = json.loads(row["detail_json"])
        except (ValueError, TypeError):
            detail = {}
        if not isinstance(detail, dict):
            detail = {}
        candidates = [x for x in detail.get("candidates") or [] if isinstance(x, dict)]
        plans = [x for x in detail.get("plan") or [] if isinstance(x, dict)]
        outcomes = [x for x in detail.get("admissions") or [] if isinstance(x, dict)]
        ids = [str(x.get("trade_id")) for x in candidates if x.get("trade_id")]
        lots = {}
        if ids:
            placeholders = ",".join("?" for _ in ids)
            lots = {x["lot_id"]: dict(x) for x in db.execute(
                f"SELECT lot_id,entry_time FROM v2_lots WHERE lot_id IN ({placeholders})", ids)}
        candidate_rows = []
        for candidate in candidates:
            trade_id = candidate.get("trade_id")
            plan = next((x for x in plans if isinstance(x.get("candidate"), dict)
                         and x["candidate"].get("trade_id") == trade_id), None)
            outcome = next((x for x in outcomes if x.get("trade_id") == trade_id), None)
            if outcome is None and sum(x.get("symbol") == candidate.get("symbol") for x in candidates) == 1:
                outcome = next((x for x in outcomes if x.get("symbol") == candidate.get("symbol")), None)
            rejection = candidate.get("rejection")
            result = ("OPENED" if trade_id in lots else rejection or
                      (outcome.get("outcome") if outcome else
                       "EXPIRED" if row["status"] == "EXPIRED" else
                       "WAITING" if row["status"] == "RUNNING" else
                       "NOT_EXECUTED" if plan else "NO_CAPACITY"))
            candidate_rows.append({"symbol": candidate.get("symbol"), "strategy": candidate.get("strategy"),
                "source": candidate.get("source"), "reference_price": candidate.get("reference_price"),
                "target_notional": plan.get("target_notional") if plan else None,
                "result": result, "entry_time": lots.get(trade_id, {}).get("entry_time")})
        reason = detail.get("reason")
        results.append({"decision_time": row["decision_time"], "status": row["status"],
            "candidates": len(candidates), "planned": len(plans), "items": candidate_rows,
            "reason": reason or ("NO_SIGNALS" if not candidates else None)})
    return results


def _exits(db: sqlite3.Connection) -> list[dict]:
    order_columns = {x[1] for x in db.execute("PRAGMA table_info(v2_orders)")}
    quantity_column = "o.applied_quantity" if "applied_quantity" in order_columns else "o.executed_quantity"
    fill_time_column = "o.filled_at" if "filled_at" in order_columns else "NULL"
    lot_columns = {x[1] for x in db.execute("PRAGMA table_info(v2_lots)")}
    closed_time_column = "l.closed_at" if "closed_at" in lot_columns else "NULL"
    orders = [dict(x) for x in db.execute(f"""SELECT o.client_id,o.lot_id,o.symbol,o.position_side,
        o.reason AS exit_reason,o.average_price AS exit_price,{quantity_column} AS quantity,
        o.updated_at AS ledger_time,{fill_time_column} AS filled_at,
        l.strategy,l.source,l.entry_price,l.entry_time,l.status AS lot_status,
        {closed_time_column} AS closed_at
        FROM v2_orders o LEFT JOIN v2_lots l ON l.lot_id=o.lot_id
        WHERE o.role='EXIT' AND CAST({quantity_column} AS REAL)>0
        ORDER BY o.updated_at DESC LIMIT 40""")]
    latest_order_by_lot = {x["lot_id"]: x["client_id"] for x in reversed(orders) if x["lot_id"]}
    exits = []
    for order in orders:
        try:
            quantity = Decimal(order["quantity"])
            entry = Decimal(order["entry_price"])
            price = Decimal(order["exit_price"])
            valid = all(x.is_finite() and x > 0 for x in (quantity, entry, price))
            gross = ((price-entry) * quantity * (1 if order["strategy"] == "long" else -1)) if valid else None
            percent = gross / (entry * quantity) * 100 if gross is not None else None
        except (ValueError, TypeError, ArithmeticError):
            gross = percent = None
        exact_close = (order["lot_status"] == "CLOSED" and
                       latest_order_by_lot.get(order["lot_id"]) == order["client_id"])
        exit_time = order["filled_at"] or (order["closed_at"] if exact_close else None) or order["ledger_time"]
        exits.append({**order, "exit_time": exit_time,
            "time_is_ledger": not bool(order["filled_at"] or (exact_close and order["closed_at"])),
            "gross_pnl": str(gross) if gross is not None else None,
            "gross_return_pct": str(percent) if percent is not None else None})
    # Old closed lots can lack a linked exit receipt; keep them visible with unknown PnL.
    closed_time = "COALESCE(l.closed_at,l.updated_at)" if "closed_at" in lot_columns else "l.updated_at"
    for row in db.execute(f"""SELECT l.lot_id,l.symbol,l.position_side,l.strategy,l.source,l.entry_price,
        l.entry_time,l.exit_reason,{closed_time_column} AS closed_at,{closed_time} AS exit_time FROM v2_lots l
        WHERE l.status='CLOSED' AND NOT EXISTS
        (SELECT 1 FROM v2_orders o WHERE o.lot_id=l.lot_id AND o.role='EXIT' AND CAST({quantity_column} AS REAL)>0)
        ORDER BY exit_time DESC LIMIT 10"""):
        exits.append({**dict(row), "client_id": None, "quantity": None, "exit_price": None,
            "gross_pnl": None, "gross_return_pct": None, "time_is_ledger": not bool(row["closed_at"])})
    return sorted(exits, key=lambda x: x["exit_time"] or "", reverse=True)[:10]


class TickerCache:
    """Read only public testnet quotes; a missing quote is never treated as current."""

    def __init__(self):
        self.lock = Lock()
        self.expires = datetime.min.replace(tzinfo=UTC)
        self.prices: dict[str, str] = {}
        self.observed_at: str | None = None

    def get(self, symbols: set[str]) -> dict:
        if not symbols:
            return {"prices": {}, "observed_at": None}
        with self.lock:
            now = datetime.now(UTC)
            if now >= self.expires:
                try:
                    with urlopen(f"{TESTNET_FUTURES_URL}/fapi/v1/ticker/price", timeout=3) as response:
                        rows = json.load(response)
                    if not isinstance(rows, list):
                        raise ValueError("invalid ticker response")
                    prices = {str(x["symbol"]): str(x["price"]) for x in rows
                              if isinstance(x, dict) and x.get("symbol") in symbols
                              and Decimal(str(x.get("price", "0"))).is_finite()
                              and Decimal(str(x.get("price", "0"))) > 0}
                    self.prices = prices
                    self.observed_at = now.isoformat()
                    self.expires = now + timedelta(seconds=10)
                except (HTTPError, URLError, OSError, ValueError, KeyError, ArithmeticError):
                    self.prices = {}
                    self.observed_at = None
                    self.expires = now + timedelta(seconds=5)
            return {"prices": {symbol: self.prices[symbol] for symbol in symbols if symbol in self.prices},
                    "observed_at": self.observed_at}


def snapshot(database: Path, period: str = "1d") -> dict:
    if period not in PERIODS:
        raise ValueError(f"unsupported equity period: {period}")
    if not database.exists():
        return {"healthy": False, "reason": "database missing", "positions": [], "incidents": []}
    db = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN")  # All cards describe the same ledger snapshot.
        tables = {x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "v2_lots" not in tables:
            return {"healthy": False, "reason": "v2 schema not initialized", "positions": [], "incidents": []}
        positions = [dict(x) for x in db.execute("SELECT * FROM v2_lots WHERE status='OPEN' ORDER BY opened_at")]
        if "v2_manual_actions" in tables:
            actions = {x["lot_id"]: dict(x) for x in db.execute("""SELECT a.* FROM v2_manual_actions a
                WHERE a.requested_at=(SELECT MAX(b.requested_at) FROM v2_manual_actions b WHERE b.lot_id=a.lot_id)""")}
            for position in positions:
                position["manual_action"] = actions.get(position["lot_id"])
        algo_records = {str(x["algo_id"]):dict(x) for x in db.execute("SELECT * FROM v2_algos WHERE algo_id IS NOT NULL")} if "v2_algos" in tables else {}
        for position in positions:
            stop = algo_records.get(str(position.get("stop_algo_id")))
            position["stop_trigger"] = stop["trigger_price"] if stop else None
            position["stop_is_floor"] = bool(stop and position["strategy"] == "long"
                                             and Decimal(stop["trigger_price"]) > Decimal(position["entry_reference"]))
        incidents = [dict(x) for x in db.execute("SELECT * FROM v2_incidents WHERE resolved_at IS NULL ORDER BY first_seen")]
        deployment = db.execute("SELECT * FROM v2_deployments ORDER BY started_at DESC LIMIT 1").fetchone()
        equity = db.execute("SELECT * FROM v2_equity ORDER BY observed_at DESC LIMIT 1").fetchone()
        heartbeat = db.execute("SELECT value FROM v2_meta WHERE key='heartbeat'").fetchone()
        age = (datetime.now(UTC)-datetime.fromisoformat(heartbeat[0])).total_seconds() if heartbeat else None
        alive = age is not None and -10 <= age < 120
        now = datetime.now(UTC)
        equity_age = (now-datetime.fromisoformat(equity["observed_at"])).total_seconds() if equity else None
        equity_fresh = equity_age is not None and -10 <= equity_age < 180
        first_equity = db.execute("SELECT * FROM v2_equity ORDER BY observed_at LIMIT 1").fetchone()
        history = _history(db, now, period)
        day_reference = db.execute("SELECT * FROM v2_day_reference WHERE utc_day=?", (now.date().isoformat(),)).fetchone()
        columns = {x[1] for x in db.execute("PRAGMA table_info(v2_orders)")}
        pending = [dict(x) for x in db.execute("SELECT client_id,lot_id,symbol,role,status,requested_quantity,executed_quantity,updated_at FROM v2_orders WHERE "
                   + ("settled=0" if "settled" in columns else "status IN ('SUBMITTED','NEW','PARTIALLY_FILLED','UNKNOWN')") + " ORDER BY created_at")]
        if "v2_algos" in tables:
            pending.extend(dict(x) for x in db.execute("""SELECT a.client_id,a.lot_id,l.symbol,'PROTECTION' AS role,a.status,
                a.quantity AS requested_quantity,NULL AS executed_quantity,a.created_at AS updated_at
                FROM v2_algos a JOIN v2_lots l USING(lot_id) WHERE a.status='SUBMITTED'"""))
        decisions = _decisions(db)
        closed = _exits(db)
        occupied = {side:str(sum((Decimal(x["entry_notional"]) for x in positions if x["strategy"] == side),Decimal())) for side in ("long","short")}
        change = str(Decimal(equity["equity"])-Decimal(first_equity["equity"])) if equity and first_equity else None
        reason = "运行正常" if alive and equity_fresh and not incidents else (
            "尚无运行心跳" if age is None else "运行心跳已过期" if not alive else
            "权益数据缺失或已过期" if not equity_fresh else "存在待处理异常，暂停新增仓位")
        return {"healthy": bool(alive and equity_fresh and not incidents), "reason":reason,
                "heartbeat_age_seconds":age,"equity_age_seconds":equity_age,"generated_at":now.isoformat(),
                "deployment": dict(deployment) if deployment else None,
                "equity": dict(equity) if equity else None, "positions": positions, "incidents": incidents,
                "first_equity":dict(first_equity) if first_equity else None,"equity_change":change,
                "equity_history":history,"equity_period":period,
                "day_reference":dict(day_reference) if day_reference else None,
                "occupied":occupied,"pending_orders":pending,"decisions":decisions,"closed":closed}
    except (sqlite3.Error,ValueError) as exc:
        return {"healthy":False,"reason":"暂时无法读取运行账本","positions":[],"incidents":[],"error":type(exc).__name__}
    finally:
        db.close()


def serve(database: Path, host: str, port: int, control_token: str | None = None) -> None:
    from urllib.parse import parse_qs, urlsplit

    if control_token and len(control_token) < 32:
        raise ValueError("DASHBOARD_CONTROL_TOKEN must have at least 32 characters")
    quotes = TickerCache()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlsplit(self.path)
            if url.path in {"/healthz", "/api/status"}:
                period = parse_qs(url.query).get("period", ["1d"])[0]
                if period not in PERIODS:
                    self.send_error(400, "invalid equity period")
                    return
                data = snapshot(database, period)
                if url.path == "/api/status":
                    data["quotes"] = quotes.get({x["symbol"] for x in data.get("positions", [])})
                    data["controls_enabled"] = bool(control_token)
                body = json.dumps(data, default=str).encode()
                # HTTP health is dashboard liveness; trading health is explicit
                # in the payload and live-health command, so incidents stay visible.
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
            elif url.path in {"/","/index.html"}:
                body = Path(__file__).with_name("dashboard.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
            else:
                self.send_error(404)
                return
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            if self.path != "/api/manual-action":
                self.send_error(404)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = 0
            if not 0 < length <= 2048:
                self.send_error(400, "invalid request size")
                return
            body_bytes = self.rfile.read(length)
            origin = self.headers.get("Origin", "")
            parsed = urlsplit(origin)
            if (not control_token or not origin or parsed.netloc != self.headers.get("Host") or
                    (parsed.scheme != "https" and not (parsed.scheme == "http" and
                    parsed.hostname in {"localhost", "127.0.0.1", "::1"}))):
                self.send_error(403, "manual controls require HTTPS or a local SSH tunnel")
                return
            if not hmac.compare_digest(self.headers.get("X-Control-Token", ""), control_token):
                self.send_error(403, "invalid control token")
                return
            try:
                if self.headers.get("Content-Type", "").split(";")[0] != "application/json":
                    raise ManualActionError("invalid request body")
                payload = json.loads(body_bytes)
                if not isinstance(payload, dict) or set(payload) != {"lot_id", "action", "expected_exit_time"}:
                    raise ManualActionError("invalid request fields")
                result = submit_action(database, **payload)
                status = 202
            except (ManualActionError, ValueError, TypeError, json.JSONDecodeError) as exc:
                result = {"error": str(exc)}
                status = 409
            except sqlite3.Error:
                result = {"error": "ledger is temporarily unavailable"}
                status = 503
            body = json.dumps(result).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            return

    ThreadingHTTPServer((host, port), Handler).serve_forever()
