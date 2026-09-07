from __future__ import annotations

from datetime import UTC, datetime, timedelta
from math import ceil, isclose
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sqlite3
from decimal import Decimal
from threading import Lock
from time import monotonic
from typing import Any
from urllib.parse import parse_qs, urlparse


_PAGE = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Fixed Time 策略监控</title><style>
:root{color-scheme:dark;--bg:#0b1018;--panel:#131b27;--line:#253247;--text:#eef3fa;--muted:#8fa1b8;--green:#3ddc97;--red:#ff6b7a;--amber:#ffc857;--blue:#55a7ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px system-ui,"Microsoft YaHei",sans-serif}.wrap{max-width:1440px;margin:auto;padding:22px}
header{display:flex;justify-content:space-between;align-items:flex-start;gap:20px;margin-bottom:18px}h1{font-size:22px;margin:0 0 7px}.sub,.muted{color:var(--muted)}.bad{color:var(--red)}.good{color:var(--green)}.warn{color:var(--amber)}
.pill{display:inline-block;padding:4px 9px;border-radius:99px;background:#202b3b;margin:0 4px 4px 0}.grid{display:grid;gap:12px}.kpis{grid-template-columns:repeat(6,minmax(140px,1fr));margin-bottom:12px}.two{grid-template-columns:2fr 1fr}.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:15px;overflow:hidden}.label{color:var(--muted);font-size:12px}.value{font-size:22px;font-weight:650;margin-top:6px}.section{margin-top:12px}h2{font-size:15px;margin:0 0 12px}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:9px 7px;border-bottom:1px solid var(--line);white-space:nowrap}th{color:var(--muted);font-weight:500}tbody tr:last-child td{border-bottom:0}.scroll{overflow:auto}.empty{padding:22px;text-align:center;color:var(--muted)}
svg{width:100%;height:220px;display:block}.chart-tools{float:right}button{background:#1c2838;color:var(--muted);border:0;padding:5px 8px;border-radius:5px;cursor:pointer}button.on{color:white;background:#315a88}.event{padding:9px 0;border-bottom:1px solid var(--line)}.event:last-child{border:0}.event small{display:block;color:var(--muted);margin-top:4px}.status{font-size:13px;text-align:right}
@media(max-width:1050px){.kpis{grid-template-columns:repeat(3,1fr)}.two{grid-template-columns:1fr}}@media(max-width:600px){.wrap{padding:12px}.kpis{grid-template-columns:repeat(2,1fr)}header{display:block}.status{text-align:left;margin-top:10px}}
</style></head><body><div class="wrap">
<header><div><h1>Fixed Time 策略监控</h1><div class="sub">测试网 · 所有时间默认显示为浏览器本地时间</div></div><div id="status" class="status">正在读取…</div></header>
<div class="grid kpis" id="kpis"></div>
<div class="grid two"><section class="card"><h2>账户净值与回撤 <span class="chart-tools"><button data-n="1440" class="on">1天</button><button data-n="10080">7天</button><button data-n="43200">30天</button><button data-n="0">全部</button></span></h2><svg id="chart" viewBox="0 0 900 220" preserveAspectRatio="none"></svg><div id="chart-note" class="muted"></div></section>
<section class="card"><h2>交易状态</h2><div id="blocks"></div></section></div>
<section class="card section"><h2>当前持仓</h2><div class="scroll"><table><thead><tr><th>方向 / 标的</th><th>单位</th><th>数量</th><th>入场 / 估值</th><th>浮盈亏</th><th>止损保护</th><th>计划退出</th><th>开仓倍率</th></tr></thead><tbody id="positions"></tbody></table></div></section>
<section class="card section"><h2>历史交易</h2><div class="scroll"><table><thead><tr><th>开仓时间</th><th>方向 / 标的</th><th>数量</th><th>成交名义</th><th>实现盈亏</th><th>手续费</th><th>净收益（不含资金费）</th><th>状态 / 原因</th></tr></thead><tbody id="trades"></tbody></table></div><div id="trade-note" class="muted"></div></section>
<section class="card section"><h2>最近订单时间</h2><div id="timings"></div></section>
<div class="grid two section"><section class="card"><h2>最近决策</h2><div id="decisions"></div></section><section class="card"><h2>运行事件</h2><div id="events"></div></section></div>
</div><script>
const $=id=>document.getElementById(id), esc=x=>String(x??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=(x,d=2)=>x==null?'—':Number(x).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d});
const money=x=>x==null?'—':num(x,2)+' USDT', dt=x=>x?new Date(x).toLocaleString():'—', pnl=x=>x==null?'—':`<span class="${Number(x)>=0?'good':'bad'}">${money(x)}</span>`;
function decisionReason(d){const c=d.detail?.candidates||[],a=d.detail?.admissions||[];if(!c.length)return '该时点没有策略信号';if(!a.length)return c.some(v=>!v.testnet_eligible)?'候选不支持测试网交易':'候选未获容量准入或执行被跳过';return a.map(v=>`${v.symbol} ${v.units}单位 ${v.outcome||''}`).join('；')}
function render(x){const r=x.runtime||{},p=x.performance||{},alive=r.heartbeat_at&&Date.now()-Date.parse(r.heartbeat_at)<30000,ready=alive&&!r.last_error&&r.reconciled_at&&!x.active_blocks.length;
$('status').innerHTML=`<span class="pill ${alive?'good':'bad'}">进程${alive?'在线':'离线'}</span><span class="pill ${ready?'good':'warn'}">${ready?'允许新仓':'暂停新仓'}</span><br><span class="muted">心跳 ${dt(r.heartbeat_at)} · 版本 ${esc(r.version)}</span>`;
const cards=[['账户净值',money(p.equity)],['累计净收益',p.total_net_pnl==null?'统计中':pnl(p.total_net_pnl)+`<div class="muted label">其中资金费 ${money(p.funding_pnl)}</div>`],['今日净收益',p.today_net_pnl==null?'统计中':pnl(p.today_net_pnl)],['持仓浮盈亏',p.unrealized_pnl==null?'—':pnl(p.unrealized_pnl)],['当前回撤 / 新仓倍率',p.drawdown==null?'—':num(100*p.drawdown,2)+'% · '+num(p.exposure_multiplier,2)+'x'],['逻辑仓位',`${r.open_units??0} / 3 单位`]];
$('kpis').innerHTML=cards.map(c=>`<section class="card"><div class="label">${c[0]}</div><div class="value">${c[1]}</div></section>`).join('');
$('blocks').innerHTML=x.active_blocks.length?x.active_blocks.map(b=>`<div class="event bad"><b>${esc(b.code)}</b> · ${esc(b.detail)}<small>首次 ${dt(b.first_seen)} · 最近 ${dt(b.last_seen)} · ${b.occurrences} 次</small></div>`).join(''):`<div class="empty ${ready?'good':'warn'}">${ready?'对账正常，等待策略信号':esc(r.last_error||'等待进程和对账状态恢复')}</div>`;
$('positions').innerHTML=x.positions.length?x.positions.map(v=>`<tr><td>${v.position_side==='LONG'?'多':'空'} · ${esc(v.symbol)}${v.extension_active?' <span class="pill">延长</span>':''}</td><td>${v.units}</td><td>${esc(v.quantity)}</td><td>${num(v.entry_price)} / ${num(v.last_mark_price)}</td><td>${pnl(v.unrealized_pnl)}</td><td>${v.stop_algo_id?'<span class="good">硬止损已确认</span>':'<span class="bad">硬止损待确认</span>'}<br><span class="muted">${esc(v.target_stop||v.active_trigger_price)}</span></td><td>${dt(v.scheduled_exit_time)}</td><td>${num(v.exposure_multiplier,2)}x<br><span class="muted">DD ${num(100*Number(v.pre_entry_drawdown||0),2)}%</span></td></tr>`).join(''):`<tr><td colspan="8" class="empty">当前空仓；请在最近决策中查看无交易原因</td></tr>`;
$('trades').innerHTML=x.trades.length?x.trades.map(t=>`<tr><td>${dt(t.opened_at)}</td><td>${t.position_side==='LONG'?'多':'空'} · ${esc(t.symbol)}<br><small class="muted">${t.code_version?esc(t.code_version)+' · '+esc(t.portfolio_units)+'仓':'历史版本未记录'}</small></td><td>${esc(t.entry_quantity||t.quantity)}</td><td>${money(t.filled_notional)}</td><td>${t.fills_complete?pnl(t.realized_pnl):'待同步'}</td><td>${!t.fills_complete?'待同步':t.non_usdt_fee_count?'待换算':money(t.commission)}</td><td>${!t.fills_complete?'待同步':t.non_usdt_fee_count?'待换算':pnl(t.net_pnl)}</td><td>${esc(t.status)}${t.exit_reason&&t.exit_reason!==t.status?'<br><span class="muted">'+esc(t.exit_reason)+'</span>':''}</td></tr>`).join(''):`<tr><td colspan="8" class="empty">尚无本地交易记录</td></tr>`;
$('trade-note').textContent=p.accounting_note||'';
$('timings').innerHTML=(x.execution_timing||[]).slice(0,6).map(t=>`<div class="event">${esc(t.symbol)} · ${t.role==='EXIT'?'平仓':'开仓'}<small>计划 ${dt(t.planned_at)} · 开始 ${dt(t.started_at)} · 提交 ${dt(t.submitted_at)} · 成交 ${dt(t.exchange_at)} · 收到响应 ${dt(t.response_at)}</small></div>`).join('')||'<div class="empty">尚无订单时间记录</div>';
$('decisions').innerHTML=x.decisions.length?x.decisions.map(d=>`<div class="event"><b>${dt(d.decision_time)}</b> · ${d.status} · 候选 ${d.candidate_count??0} · 成交准入 ${d.admission_count??0}<small>${esc(d.error||decisionReason(d))}</small></div>`).join(''):`<div class="empty">尚无决策记录</div>`;
$('events').innerHTML=x.events.length?x.events.map(e=>`<div class="event ${e.status==='BLOCKED'?'bad':''}">${esc(e.status)} · ${esc(e.detail)}<small>${dt(e.checked_at)}</small></div>`).join(''):`<div class="empty">尚无运行事件</div>`;if(chartRange===1440)draw(x.equity||[])}
function draw(rows){const svg=$('chart');if(rows.length<2){svg.innerHTML='';$('chart-note').textContent=rows.length?'等待更多分钟净值':'尚无完整分钟净值';return}const W=900,H=220,pad=14,ys=rows.map(r=>Number(r.equity)),lo=Math.min(...ys),hi=Math.max(...ys),span=hi-lo||1,pts=ys.map((y,i)=>`${pad+i*(W-2*pad)/(ys.length-1)},${pad+(hi-y)*(H-2*pad)/span}`).join(' ');svg.innerHTML=`<line x1="0" y1="205" x2="900" y2="205" stroke="#253247"/><polyline points="${pts}" fill="none" stroke="#55a7ff" stroke-width="2" vector-effect="non-scaling-stroke"/>`;$('chart-note').textContent=`${dt(rows[0].minute_end)} 至 ${dt(rows.at(-1).minute_end)} · 区间 ${num(lo)} — ${num(hi)} USDT`}
let running=false,timer,chartRange=1440,chartMinute=null,chartRequest=0;
async function refreshChart(){const request=++chartRequest,range=chartRange;try{const r=await fetch('/api/equity?limit='+range);if(!r.ok)throw Error(await r.text());const data=await r.json();if(request===chartRequest&&range===chartRange)draw(data.equity)}catch(e){if(request===chartRequest)$('chart-note').textContent='图表读取失败：'+e.message}}
async function refresh(){if(running)return;running=true;try{const r=await fetch('/api/status',{cache:'no-store'});if(!r.ok)throw Error(await r.text());const x=await r.json();render(x);const minute=x.equity?.at(-1)?.minute_end;if(chartRange!==1440&&minute!==chartMinute)await refreshChart();chartMinute=minute}catch(e){$('status').innerHTML=`<span class="bad">读取失败：${esc(e.message)}</span>`}finally{running=false;timer=setTimeout(refresh,document.hidden?30000:5000)}}
document.querySelectorAll('button[data-n]').forEach(b=>b.onclick=()=>{document.querySelectorAll('button[data-n]').forEach(x=>x.classList.remove('on'));b.classList.add('on');chartRange=Number(b.dataset.n);refreshChart()});document.addEventListener('visibilitychange',()=>{clearTimeout(timer);refresh()});refresh();
</script></body></html>"""


def _rows(connection: sqlite3.Connection, statement: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(statement, parameters)]


def _open_database(database_path: Path) -> sqlite3.Connection:
    uri = f"{database_path.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def read_equity(database_path: Path, limit: int = 1440) -> list[dict[str, Any]]:
    connection = _open_database(database_path)
    try:
        connection.execute("BEGIN")
        cutoff = None
        if limit > 0:
            row = connection.execute("SELECT minute_end FROM equity_minutes ORDER BY minute_end DESC LIMIT 1 OFFSET ?",
                                     (max(0, limit - 1),)).fetchone()
            cutoff = row["minute_end"] if row else None
        where, parameters = ("WHERE minute_end >= ?", (cutoff,)) if cutoff else ("", ())
        count = connection.execute(f"SELECT COUNT(*) FROM equity_minutes {where}", parameters).fetchone()[0]
        cursor = connection.execute(
            f"SELECT minute_end,equity,drawdown FROM equity_minutes {where} ORDER BY minute_end", parameters)
        if count <= 2000:
            return [dict(row) for row in cursor]
        sampled: list[dict[str, Any]] = []
        size = ceil(count / 1000)
        while batch := cursor.fetchmany(size):
            low = min(batch, key=lambda row: Decimal(row["equity"]))
            high = max(batch, key=lambda row: Decimal(row["equity"]))
            for row in sorted({low["minute_end"]: low, high["minute_end"]: high}.values(), key=lambda item: item["minute_end"]):
                sampled.append(dict(row))
        return sampled
    finally:
        connection.close()


def read_health(database_path: Path) -> dict[str, Any] | None:
    connection = _open_database(database_path)
    try:
        row = connection.execute("SELECT heartbeat_at, last_error FROM runtime_status WHERE singleton = 1").fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def read_status(database_path: Path) -> dict[str, Any]:
    connection = _open_database(database_path)
    try:
        connection.execute("BEGIN")
        runtime = connection.execute("SELECT * FROM runtime_status WHERE singleton = 1").fetchone()
        latest = connection.execute("SELECT * FROM equity_minutes ORDER BY minute_end DESC LIMIT 1").fetchone()
        first = connection.execute("SELECT * FROM equity_minutes ORDER BY minute_end LIMIT 1").fetchone()
        day_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        today_first = connection.execute("SELECT * FROM equity_minutes WHERE minute_end >= ? ORDER BY minute_end LIMIT 1", (day_start,)).fetchone()
        def external_flows(since: str) -> Decimal:
            return sum((Decimal(row[0]) for row in connection.execute("""SELECT income FROM income_events
                WHERE asset='USDT' AND income_type IN ('TRANSFER','INTERNAL_TRANSFER')
                  AND occurred_at > ? AND occurred_at <= ?""", (since, latest["minute_end"]))), Decimal("0"))

        transfer = external_flows(first["minute_end"]) if first else Decimal("0")
        today_transfer = external_flows(today_first["minute_end"]) if today_first else Decimal("0")
        performance: dict[str, Any] = {
            "funding_pnl": format(sum((Decimal(row[0]) for row in connection.execute(
                "SELECT income FROM income_events WHERE asset='USDT' AND income_type='FUNDING_FEE' AND occurred_at > ? AND occurred_at <= ?",
                (first["minute_end"], latest["minute_end"]))), Decimal("0")), "f") if first and latest else None,
            "equity": latest["equity"] if latest else None, "unrealized_pnl": latest["unrealized_pnl"] if latest else None,
            "drawdown": latest["drawdown"] if latest else None, "exposure_multiplier": latest["exposure_multiplier"] if latest else None,
            "total_net_pnl": format(Decimal(latest["equity"])-Decimal(first["equity"])-Decimal(str(transfer)), "f") if latest and first else None,
            "today_net_pnl": format(Decimal(latest["equity"])-Decimal(today_first["equity"])-today_transfer, "f") if latest and today_first else None,
            "accounting_note": "累计收益从首个完整分钟净值开始，今日按 UTC 日界统计；逐笔净收益包含完整同步的实现盈亏与 USDT 手续费，资金费在账户收益中体现。",
        }
        decisions = _rows(connection, "SELECT * FROM decision_runs ORDER BY decision_time DESC LIMIT 10")
        for row in decisions:
            try:
                row["detail"] = json.loads(row.pop("detail_json"))
            except json.JSONDecodeError:
                row["detail"] = {}
        positions = _rows(connection, """SELECT positions.*, intents.priority_score, intents.decision_time,
                (SELECT trigger_price FROM protection_orders po WHERE po.intent_id=positions.intent_id
                 AND po.status IN ('SUBMITTED','ACTIVE') ORDER BY po.id DESC LIMIT 1) AS active_trigger_price
            FROM positions JOIN intents USING(intent_id) WHERE positions.status='OPEN' ORDER BY opened_at""")
        trades = _rows(connection, """SELECT positions.intent_id,positions.symbol,positions.position_side,positions.units,
                positions.quantity,positions.filled_notional,positions.status,positions.opened_at,
                (SELECT run_id FROM intents WHERE intent_id=positions.intent_id) AS run_id,
                (SELECT quantity FROM executions e WHERE e.intent_id=positions.intent_id AND e.role='ENTRY' LIMIT 1) AS entry_quantity,
                (SELECT reason FROM executions e WHERE e.intent_id=positions.intent_id AND e.role='EXIT' ORDER BY recorded_at DESC LIMIT 1) AS exit_reason,
                COUNT(tf.trade_id) AS fill_count,
                SUM(CAST(tf.quantity AS REAL)) AS synced_quantity,
                (SELECT SUM(CAST(e.quantity AS REAL)) FROM executions e WHERE e.intent_id=positions.intent_id) AS executed_quantity,
                SUM(CASE WHEN tf.trade_id IS NOT NULL AND tf.commission_asset!='USDT' THEN 1 ELSE 0 END) AS non_usdt_fee_count,
                CASE WHEN COUNT(tf.trade_id)>0 THEN SUM(CAST(tf.realized_pnl AS REAL)) END AS realized_pnl,
                CASE WHEN COUNT(tf.trade_id)>0 AND SUM(CASE WHEN tf.commission_asset!='USDT' THEN 1 ELSE 0 END)=0 THEN SUM(CAST(tf.commission AS REAL)) END AS commission,
                CASE WHEN COUNT(tf.trade_id)>0 AND SUM(CASE WHEN tf.commission_asset!='USDT' THEN 1 ELSE 0 END)=0 THEN SUM(CAST(tf.realized_pnl AS REAL)-CAST(tf.commission AS REAL)) END AS net_pnl
            FROM (SELECT * FROM positions ORDER BY opened_at DESC LIMIT 50) AS positions
            LEFT JOIN trade_fills tf USING(intent_id) GROUP BY positions.intent_id
            ORDER BY positions.opened_at DESC LIMIT 50""")
        for trade in trades:
            deployment = connection.execute("SELECT snapshot_json FROM deployment_runs WHERE run_id = ?", (trade["run_id"],)).fetchone()
            snapshot = json.loads(deployment[0]) if deployment else {}
            trade.update(code_version=snapshot.get("code_version"), revision=snapshot.get("revision"),
                         portfolio_units=snapshot.get("strategy", {}).get("portfolio", {}).get("total_units"))
            trade["fills_complete"] = bool(trade["fill_count"] and trade["executed_quantity"] is not None
                and isclose(trade["synced_quantity"], trade["executed_quantity"], rel_tol=1e-9, abs_tol=0.0))
            if not trade["fills_complete"]:
                trade.update(realized_pnl=None, commission=None, net_pnl=None)
        timings = _rows(connection, """SELECT t.*, i.symbol FROM execution_timing t JOIN intents i USING(intent_id)
            ORDER BY COALESCE(t.submitted_at,t.response_at) DESC LIMIT 20""")
        return {"generated_at": datetime.now(UTC).isoformat(), "runtime": dict(runtime) if runtime else None,
                "execution_timing": timings,
                "performance": performance, "active_blocks": _rows(connection, "SELECT * FROM entry_blocks WHERE resolved_at IS NULL ORDER BY first_seen"),
                "positions": positions, "trades": trades, "decisions": decisions,
                "events": _rows(connection, "SELECT * FROM reconciliation ORDER BY checked_at DESC LIMIT 20"),
                "equity": list(reversed(_rows(connection, "SELECT minute_end,equity,drawdown FROM equity_minutes ORDER BY minute_end DESC LIMIT 1440")))}
    finally:
        connection.close()


class DashboardHandler(BaseHTTPRequestHandler):
    server: "DashboardServer"

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, value: Any) -> None:
        self._send(HTTPStatus.OK, json.dumps(value, default=str, separators=(",", ":")).encode(), "application/json")

    def do_GET(self) -> None:  # noqa: N802
        request = urlparse(self.path)
        if request.path == "/":
            self._send(HTTPStatus.OK, _PAGE.encode(), "text/html; charset=utf-8")
            return
        try:
            if request.path == "/healthz":
                runtime = read_health(self.server.database_path)
                fresh = runtime is not None and datetime.fromisoformat(runtime["heartbeat_at"]) >= datetime.now(UTC)-timedelta(seconds=30)
                self._send(HTTPStatus.OK if fresh else HTTPStatus.SERVICE_UNAVAILABLE,
                           b"ok" if fresh else b"stale", "text/plain")
                return
            if request.path == "/api/status":
                self._json(self.server.status())
                return
            if request.path == "/api/equity":
                raw = parse_qs(request.query).get("limit", ["1440"])[0]
                self._json({"equity": read_equity(self.server.database_path, int(raw))})
                return
        except (OSError, sqlite3.Error, ValueError) as exc:
            self._send(HTTPStatus.SERVICE_UNAVAILABLE, json.dumps({"error": str(exc)}).encode(), "application/json")
            return
        self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")


class DashboardServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], database_path: Path):
        super().__init__(address, DashboardHandler)
        self.database_path = database_path
        self._cache: tuple[float, dict[str, Any]] | None = None
        self._cache_lock = Lock()

    def status(self) -> dict[str, Any]:
        with self._cache_lock:
            if self._cache is None or monotonic() - self._cache[0] >= 4:
                self._cache = monotonic(), read_status(self.database_path)
            return self._cache[1]


def run_dashboard(database_path: Path, host: str = "0.0.0.0", port: int = 8080) -> None:
    DashboardServer((host, port), database_path).serve_forever()
