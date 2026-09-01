from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sqlite3
from typing import Any


_PAGE = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Fixed Time Monitor</title><style>
body{font:15px system-ui,sans-serif;background:#10131a;color:#e9edf5;margin:0;padding:24px}h1{margin-top:0}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:16px}.card{background:#1b202b;padding:16px;border-radius:8px}
pre{white-space:pre-wrap;overflow:auto;margin:0;font-size:12px;color:#cbd5e1}.ok{color:#6ee7b7}.bad{color:#fda4af}
</style></head><body><h1>Fixed Time 测试网监控</h1><div id="updated"></div><div class="grid">
<section class="card"><h2>运行状态</h2><pre id="runtime"></pre></section>
<section class="card"><h2>当前持仓</h2><pre id="positions"></pre></section>
<section class="card"><h2>最近决策</h2><pre id="decisions"></pre></section>
<section class="card"><h2>最近订单</h2><pre id="executions"></pre></section>
<section class="card"><h2>对账/异常</h2><pre id="events"></pre></section>
</div><script>
async function refresh(){try{const r=await fetch('/api/status');const x=await r.json();
for(const k of ['runtime','positions','decisions','executions','events'])document.getElementById(k).textContent=JSON.stringify(x[k],null,2);
document.getElementById('updated').textContent='页面刷新：'+new Date().toLocaleTimeString();}catch(e){document.getElementById('updated').textContent='读取失败：'+e}}
refresh();setInterval(refresh,5000);
</script></body></html>"""


def _rows(connection: sqlite3.Connection, statement: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(statement, parameters)]


def read_status(database_path: Path) -> dict[str, Any]:
    uri = f"{database_path.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        runtime = connection.execute("SELECT * FROM runtime_status WHERE singleton = 1").fetchone()
        decisions = _rows(connection, "SELECT * FROM decision_runs ORDER BY decision_time DESC LIMIT 10")
        for row in decisions:
            row["detail"] = json.loads(row.pop("detail_json"))
        return {
            "runtime": dict(runtime) if runtime is not None else None,
            "positions": _rows(connection, """SELECT positions.*, intents.priority_score, intents.decision_time
                FROM positions JOIN intents USING(intent_id) WHERE positions.status = 'OPEN' ORDER BY opened_at"""),
            "decisions": decisions,
            "executions": _rows(connection, "SELECT * FROM executions ORDER BY recorded_at DESC LIMIT 30"),
            "events": _rows(connection, "SELECT * FROM reconciliation ORDER BY checked_at DESC LIMIT 30"),
        }
    finally:
        connection.close()


class DashboardHandler(BaseHTTPRequestHandler):
    server: "DashboardServer"

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/":
            self._send(HTTPStatus.OK, _PAGE.encode("utf-8"), "text/html; charset=utf-8")
            return
        try:
            status = read_status(self.server.database_path)
        except (OSError, sqlite3.Error) as exc:
            self._send(HTTPStatus.SERVICE_UNAVAILABLE, json.dumps({"error": str(exc)}).encode("utf-8"), "application/json")
            return
        if self.path == "/api/status":
            self._send(HTTPStatus.OK, json.dumps(status, default=str).encode("utf-8"), "application/json")
            return
        if self.path == "/healthz":
            runtime = status["runtime"]
            fresh = runtime is not None and datetime.fromisoformat(runtime["heartbeat_at"]) >= datetime.now(UTC) - timedelta(seconds=30)
            self._send(HTTPStatus.OK if fresh else HTTPStatus.SERVICE_UNAVAILABLE, b"ok" if fresh else b"stale", "text/plain")
            return
        self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")


class DashboardServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], database_path: Path):
        super().__init__(address, DashboardHandler)
        self.database_path = database_path


def run_dashboard(database_path: Path, host: str = "0.0.0.0", port: int = 8080) -> None:
    DashboardServer((host, port), database_path).serve_forever()
