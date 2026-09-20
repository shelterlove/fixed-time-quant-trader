from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import sqlite3


def snapshot(database: Path) -> dict:
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
        history = [dict(x) for x in db.execute(
            "SELECT * FROM v2_equity WHERE observed_at>=? ORDER BY observed_at DESC LIMIT 1441",
            ((now-timedelta(hours=24)).isoformat(),))][::-1]
        day_reference = db.execute("SELECT * FROM v2_day_reference WHERE utc_day=?", (now.date().isoformat(),)).fetchone()
        columns = {x[1] for x in db.execute("PRAGMA table_info(v2_orders)")}
        pending = [dict(x) for x in db.execute("SELECT client_id,lot_id,symbol,role,status,requested_quantity,executed_quantity,updated_at FROM v2_orders WHERE "
                   + ("settled=0" if "settled" in columns else "status IN ('SUBMITTED','NEW','PARTIALLY_FILLED','UNKNOWN')") + " ORDER BY created_at")]
        if "v2_algos" in tables:
            pending.extend(dict(x) for x in db.execute("""SELECT a.client_id,a.lot_id,l.symbol,'PROTECTION' AS role,a.status,
                a.quantity AS requested_quantity,NULL AS executed_quantity,a.created_at AS updated_at
                FROM v2_algos a JOIN v2_lots l USING(lot_id) WHERE a.status='SUBMITTED'"""))
        decisions = []
        for row in db.execute("SELECT * FROM v2_decisions ORDER BY decision_time DESC LIMIT 8"):
            try:
                detail = json.loads(row["detail_json"])
            except (ValueError,TypeError):
                detail = {}
            decisions.append({"decision_time":row["decision_time"],"status":row["status"],
                "candidates":len(detail.get("candidates") or []),"planned":len(detail.get("plan") or []),
                "outcomes":detail.get("admissions") or [],"reason":detail.get("reason")})
        closed = [dict(x) for x in db.execute("SELECT symbol,strategy,source,entry_price,entry_time,updated_at,exit_reason FROM v2_lots WHERE status='CLOSED' ORDER BY updated_at DESC LIMIT 10")]
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
                "equity_history":history,"day_reference":dict(day_reference) if day_reference else None,
                "occupied":occupied,"pending_orders":pending,"decisions":decisions,"closed":closed}
    except (sqlite3.Error,ValueError) as exc:
        return {"healthy":False,"reason":"暂时无法读取运行账本","positions":[],"incidents":[],"error":type(exc).__name__}
    finally:
        db.close()


def serve(database: Path, host: str, port: int) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in {"/healthz", "/api/status"}:
                data = snapshot(database)
                body = json.dumps(data, default=str).encode()
                # HTTP health is dashboard liveness; trading health is explicit
                # in the payload and live-health command, so incidents stay visible.
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
            elif self.path in {"/","/index.html"}:
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

        def log_message(self, *_):
            return

    ThreadingHTTPServer((host, port), Handler).serve_forever()
