from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Iterator


class StateError(RuntimeError):
    pass


class RuntimeLock:
    def __init__(self, database: Path):
        self.path = database.with_suffix(database.suffix + ".lock")
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+")
        try:
            try:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except ImportError:
                import msvcrt
                self.handle.seek(0)
                if not self.handle.read(1):
                    self.handle.write(" ")
                    self.handle.flush()
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
        except (BlockingIOError, OSError) as exc:
            self.handle.close()
            raise StateError(f"runtime already locked: {self.path}") from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(f"pid={os.getpid()} started={utc_now()}\n")
        self.handle.flush()
        return self

    def __exit__(self, *_):
        if not self.handle:
            return
        try:
            try:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            except ImportError:
                import msvcrt
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self.handle.close()


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class Store:
    """Durable v2 lot ledger. Existing v1 tables remain read-only migration input."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self._migrate()

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            yield self.connection
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def _migrate(self) -> None:
        with self.transaction() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS v2_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS v2_lots (
                    lot_id TEXT PRIMARY KEY, strategy TEXT NOT NULL, source TEXT NOT NULL,
                    symbol TEXT NOT NULL, position_side TEXT NOT NULL,
                    decision_time TEXT NOT NULL, entry_time TEXT NOT NULL,
                    planned_exit_time TEXT NOT NULL, scheduled_exit_time TEXT NOT NULL,
                    quantity TEXT NOT NULL, entry_price TEXT NOT NULL, entry_reference TEXT NOT NULL,
                    entry_notional TEXT NOT NULL, stop_algo_id TEXT, cap_algo_id TEXT,
                    protection_version TEXT,
                    first_extension_activation TEXT, profit_armed_at TEXT,
                    extended INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'OPEN',
                    exit_reason TEXT, closed_at TEXT, opened_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS v2_orders (
                    client_id TEXT PRIMARY KEY, lot_id TEXT, role TEXT NOT NULL,
                    symbol TEXT NOT NULL, side TEXT NOT NULL, position_side TEXT NOT NULL,
                    requested_quantity TEXT NOT NULL, reason TEXT, status TEXT NOT NULL,
                    exchange_order_id TEXT, executed_quantity TEXT NOT NULL DEFAULT '0',
                    average_price TEXT NOT NULL DEFAULT '0', metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS v2_decisions (
                    decision_time TEXT PRIMARY KEY, status TEXT NOT NULL,
                    detail_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS v2_incidents (
                    code TEXT PRIMARY KEY, detail TEXT NOT NULL, first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL, occurrences INTEGER NOT NULL, resolved_at TEXT
                );
                CREATE TABLE IF NOT EXISTS v2_events (
                    event_time TEXT NOT NULL, level TEXT NOT NULL, code TEXT NOT NULL, detail TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS v2_equity (
                    observed_at TEXT PRIMARY KEY, equity TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS v2_day_reference (
                    utc_day TEXT PRIMARY KEY, equity TEXT NOT NULL, observed_at TEXT NOT NULL, recovered_late INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS v2_day_locks (
                    utc_day TEXT NOT NULL, symbol TEXT NOT NULL, kind TEXT NOT NULL,
                    created_at TEXT NOT NULL, PRIMARY KEY (utc_day, symbol, kind)
                );
                CREATE TABLE IF NOT EXISTS v2_deployments (
                    run_id TEXT PRIMARY KEY, version TEXT NOT NULL, started_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS v2_lots_open ON v2_lots(status, symbol, position_side);
                CREATE INDEX IF NOT EXISTS v2_orders_pending ON v2_orders(status);
                CREATE INDEX IF NOT EXISTS v2_equity_time ON v2_equity(observed_at);
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(v2_lots)")}
            if "protection_version" not in columns:
                db.execute("ALTER TABLE v2_lots ADD COLUMN protection_version TEXT")
            if "closed_at" not in columns:
                db.execute("ALTER TABLE v2_lots ADD COLUMN closed_at TEXT")
            order_columns = {row[1] for row in db.execute("PRAGMA table_info(v2_orders)")}
            if "applied_quantity" not in order_columns:
                db.execute("ALTER TABLE v2_orders ADD COLUMN applied_quantity TEXT NOT NULL DEFAULT '0'")
                db.execute("ALTER TABLE v2_orders ADD COLUMN settled INTEGER NOT NULL DEFAULT 0")
                # Old completed exits cannot be replayed safely. They were already
                # applied separately; quantity reconciliation detects any crash gap.
                db.execute("""UPDATE v2_orders SET applied_quantity=executed_quantity,settled=1
                    WHERE status NOT IN ('SUBMITTED','NEW','PARTIALLY_FILLED','UNKNOWN')
                    AND (role='EXIT' OR lot_id IN (SELECT lot_id FROM v2_lots))""")
            if "filled_at" not in order_columns:
                db.execute("ALTER TABLE v2_orders ADD COLUMN filled_at TEXT")
            db.execute("""CREATE TABLE IF NOT EXISTS v2_algos (
                client_id TEXT PRIMARY KEY,lot_id TEXT NOT NULL,kind TEXT NOT NULL,
                quantity TEXT NOT NULL,trigger_price TEXT NOT NULL,algo_id TEXT,
                status TEXT NOT NULL,created_at TEXT NOT NULL)""")
            migrated = db.execute("SELECT value FROM v2_meta WHERE key='legacy_open_positions_imported'").fetchone()
            if migrated is None:
                tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if {"positions", "intents"} <= tables:
                    rows = db.execute("""SELECT p.*, i.decision_time, i.planned_exit_time AS intent_exit
                        FROM positions p JOIN intents i USING(intent_id) WHERE p.status='OPEN'""").fetchall()
                    if rows:
                        raise StateError("v1 upgrade requires a flat legacy ledger; finish existing trades with the old version first")
                    # Preserve same-day long admissions on a flat upgrade.
                    db.execute("""INSERT OR IGNORE INTO v2_day_locks
                        SELECT substr(i.decision_time,1,10),p.symbol,'LONG_BOUGHT',p.opened_at
                        FROM positions p JOIN intents i USING(intent_id) WHERE p.strategy='long'""")
                db.execute("INSERT INTO v2_meta VALUES ('legacy_open_positions_imported', ?)", (utc_now(),))

    def deployment(self, run_id: str, version: str) -> None:
        with self.transaction() as db:
            db.execute("INSERT INTO v2_deployments VALUES (?,?,?)", (run_id, version, utc_now()))

    def open_lots(self) -> list[dict[str, Any]]:
        return [dict(x) for x in self.connection.execute("SELECT * FROM v2_lots WHERE status='OPEN' ORDER BY opened_at,lot_id")]

    def lot(self, lot_id: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM v2_lots WHERE lot_id=?", (lot_id,)).fetchone()
        return dict(row) if row else None

    def create_lot(self, row: dict[str, Any]) -> None:
        required = {"lot_id", "strategy", "source", "symbol", "position_side", "decision_time", "entry_time",
                    "planned_exit_time", "quantity", "entry_price", "entry_reference", "entry_notional"}
        if set(row) != required:
            raise StateError(f"lot keys mismatch: {sorted(set(row) ^ required)}")
        now = utc_now()
        with self.transaction() as db:
            db.execute("""INSERT INTO v2_lots
                (lot_id,strategy,source,symbol,position_side,decision_time,entry_time,planned_exit_time,scheduled_exit_time,
                 quantity,entry_price,entry_reference,entry_notional,protection_version,status,opened_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'r2','OPEN',?,?)""",
                (*[str(row[k]) for k in ("lot_id", "strategy", "source", "symbol", "position_side", "decision_time", "entry_time",
                                          "planned_exit_time", "planned_exit_time", "quantity", "entry_price", "entry_reference", "entry_notional")], now, now))

    def set_algos(self, lot_id: str, *, stop: str | None | object = ..., cap: str | None | object = ...) -> None:
        updates, params = [], []
        if stop is not ...:
            updates.append("stop_algo_id=?")
            params.append(stop)
        if cap is not ...:
            updates.append("cap_algo_id=?")
            params.append(cap)
        if not updates:
            return
        with self.transaction() as db:
            db.execute(f"UPDATE v2_lots SET {','.join(updates)},updated_at=? WHERE lot_id=?", (*params, utc_now(), lot_id))

    def set_protection_version(self, lot_id: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE v2_lots SET protection_version='r2',updated_at=? WHERE lot_id=?", (utc_now(), lot_id))

    def arm_extension(self, lot_id: str, when: datetime) -> None:
        with self.transaction() as db:
            db.execute("UPDATE v2_lots SET first_extension_activation=COALESCE(first_extension_activation,?),updated_at=? WHERE lot_id=? AND status='OPEN'",
                       (when.isoformat(), utc_now(), lot_id))

    def arm_profit(self, lot_id: str, when: datetime) -> None:
        with self.transaction() as db:
            db.execute("UPDATE v2_lots SET profit_armed_at=COALESCE(profit_armed_at,?),updated_at=? WHERE lot_id=? AND status='OPEN'",
                       (when.isoformat(), utc_now(), lot_id))

    def extend(self, lot_id: str, scheduled: datetime) -> None:
        with self.transaction() as db:
            db.execute("UPDATE v2_lots SET extended=1,scheduled_exit_time=?,updated_at=? WHERE lot_id=? AND status='OPEN'",
                       (scheduled.isoformat(), utc_now(), lot_id))

    def begin_order(self, row: dict[str, Any]) -> dict[str, Any]:
        now = utc_now()
        with self.transaction() as db:
            db.execute("""INSERT OR IGNORE INTO v2_orders
                (client_id,lot_id,role,symbol,side,position_side,requested_quantity,reason,status,metadata_json,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?, 'SUBMITTED',?,?,?)""",
                (row["client_id"], row.get("lot_id"), row["role"], row["symbol"], row["side"], row["position_side"],
                 str(row["requested_quantity"]), row.get("reason"), json.dumps(row.get("metadata", {}), default=str), now, now))
            return dict(db.execute("SELECT * FROM v2_orders WHERE client_id=?", (row["client_id"],)).fetchone())

    def pending_orders(self) -> list[dict[str, Any]]:
        return [dict(x) for x in self.connection.execute("SELECT * FROM v2_orders WHERE settled=0 ORDER BY created_at")]

    def order(self, client_id: str) -> dict | None:
        row = self.connection.execute("SELECT * FROM v2_orders WHERE client_id=?", (client_id,)).fetchone()
        return dict(row) if row else None

    def apply_order(self, client_id: str, response: dict, when: datetime) -> None:
        """Commit the cumulative fill delta and order receipt in one transaction."""
        with self.transaction() as db:
            order = db.execute("SELECT * FROM v2_orders WHERE client_id=?", (client_id,)).fetchone()
            if order is None:
                raise StateError(f"unknown order {client_id}")
            filled = Decimal(str(response.get("executedQty", "0")))
            applied = Decimal(order["applied_quantity"])
            if filled < applied or not filled.is_finite():
                raise StateError(f"nonmonotonic fill: {client_id}")
            delta = filled - applied
            average = Decimal(str(response.get("avgPrice", "0")))
            status = str(response.get("status", "UNKNOWN"))
            timestamp = response.get("updateTime") or response.get("transactTime")
            executed_at = datetime.fromtimestamp(int(timestamp) / 1000, UTC) if timestamp else when
            lot = db.execute("SELECT * FROM v2_lots WHERE lot_id=?", (order["lot_id"],)).fetchone()
            if delta > 0 and order["role"] == "ENTRY":
                if average <= 0 or not average.is_finite():
                    raise StateError(f"entry has no average price: {client_id}")
                metadata = json.loads(order["metadata_json"])
                if lot is None:
                    db.execute("""INSERT INTO v2_lots
                        (lot_id,strategy,source,symbol,position_side,decision_time,entry_time,planned_exit_time,
                         scheduled_exit_time,quantity,entry_price,entry_reference,entry_notional,protection_version,opened_at,updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'r2',?,?)""",
                        (order["lot_id"],metadata["strategy"],metadata["source"],order["symbol"],order["position_side"],
                         metadata["decision_time"],executed_at.isoformat(),metadata["planned_exit_time"],metadata["planned_exit_time"],
                         str(filled),str(average),metadata["entry_reference"],str(filled*average),utc_now(),utc_now()))
                else:
                    if lot["status"] != "OPEN":
                        raise StateError(f"late entry fill on closed lot: {client_id}")
                    db.execute("UPDATE v2_lots SET quantity=?,entry_price=?,entry_notional=?,updated_at=? WHERE lot_id=?",
                               (str(Decimal(lot["quantity"])+delta),str(average),str(filled*average),utc_now(),order["lot_id"]))
            elif delta > 0:
                if lot is None or lot["status"] != "OPEN" or delta > Decimal(lot["quantity"]):
                    raise StateError(f"exit fill exceeds recorded lot: {client_id}")
                remaining = Decimal(lot["quantity"])-delta
                remaining_notional = (Decimal(lot["entry_notional"]) * remaining / Decimal(lot["quantity"])
                                      if remaining else Decimal(lot["entry_notional"]))
                db.execute("UPDATE v2_lots SET quantity=?,entry_notional=?,status=?,exit_reason=?,closed_at=?,updated_at=? WHERE lot_id=?",
                           (str(remaining),str(remaining_notional),"OPEN" if remaining else "CLOSED",
                            None if remaining else order["reason"],
                            executed_at.isoformat() if not remaining else None,utc_now(),order["lot_id"]))
                if lot["strategy"] == "long" and order["reason"] == "HARD_STOP":
                    db.execute("INSERT OR IGNORE INTO v2_day_locks VALUES (?,?,'LONG_STOP',?)",
                               (executed_at.date().isoformat(),lot["symbol"],utc_now()))
            settled = int(status in {"FILLED","CANCELED","EXPIRED","EXPIRED_IN_MATCH","REJECTED","NOT_FOUND"})
            db.execute("""UPDATE v2_orders SET status=?,exchange_order_id=?,executed_quantity=?,average_price=?,
                applied_quantity=?,settled=?,filled_at=COALESCE(?,filled_at),updated_at=? WHERE client_id=?""",
                (status,str(response.get("orderId", "")),str(filled),str(average),str(filled),settled,
                 executed_at.isoformat() if delta > 0 else None,utc_now(),client_id))

    def algos(self, lot_id: str | None = None) -> list[dict]:
        return [dict(x) for x in self.connection.execute(
            "SELECT * FROM v2_algos" + (" WHERE lot_id=?" if lot_id else ""), (lot_id,) if lot_id else ())]

    def begin_algo(self, client: str, lot: dict, kind: str, trigger: Decimal) -> None:
        with self.transaction() as db:
            db.execute("INSERT INTO v2_algos VALUES (?,?,?,?,?,NULL,'SUBMITTED',?)",
                       (client,lot["lot_id"],kind,lot["quantity"],str(trigger),utc_now()))

    def update_algo(self, client: str, algo_id: str | None, status: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE v2_algos SET algo_id=COALESCE(?,algo_id),status=? WHERE client_id=?", (algo_id,status,client))

    def heartbeat(self, when: datetime) -> None:
        with self.transaction() as db:
            db.execute("INSERT OR REPLACE INTO v2_meta VALUES ('heartbeat',?)", (when.isoformat(),))

    def known_order_ids(self) -> set[str]:
        return {str(x[0]) for x in self.connection.execute("SELECT client_id FROM v2_orders")}

    def decision(self, when: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM v2_decisions WHERE decision_time=?", (when,)).fetchone()
        return dict(row) if row else None

    def save_decision(self, when: str, status: str, detail: dict) -> None:
        now = utc_now()
        with self.transaction() as db:
            db.execute("""INSERT INTO v2_decisions VALUES (?,?,?,?,?)
                ON CONFLICT(decision_time) DO UPDATE SET status=excluded.status,detail_json=excluded.detail_json,updated_at=excluded.updated_at""",
                (when, status, json.dumps(detail, default=str, separators=(",", ":")), now, now))

    def block(self, code: str, detail: str) -> None:
        now = utc_now()
        with self.transaction() as db:
            current = db.execute("SELECT 1 FROM v2_incidents WHERE code=? AND resolved_at IS NULL", (code,)).fetchone()
            db.execute("""INSERT INTO v2_incidents VALUES (?,?,?,?,1,NULL)
                ON CONFLICT(code) DO UPDATE SET detail=excluded.detail,last_seen=excluded.last_seen,
                occurrences=CASE WHEN v2_incidents.resolved_at IS NULL THEN v2_incidents.occurrences+1 ELSE 1 END,
                first_seen=CASE WHEN v2_incidents.resolved_at IS NULL THEN v2_incidents.first_seen ELSE excluded.first_seen END,resolved_at=NULL""",
                (code, detail, now, now))
            if current is None:
                db.execute("INSERT INTO v2_events VALUES (?,?,?,?)", (now, "BLOCKED", code, detail))

    def resolve(self, code: str) -> None:
        now = utc_now()
        with self.transaction() as db:
            row = db.execute("SELECT detail FROM v2_incidents WHERE code=? AND resolved_at IS NULL", (code,)).fetchone()
            if row:
                db.execute("UPDATE v2_incidents SET resolved_at=? WHERE code=?", (now, code))
                db.execute("INSERT INTO v2_events VALUES (?,?,?,?)", (now, "RESOLVED", code, row["detail"]))

    def event(self, level: str, code: str, detail: str) -> None:
        with self.transaction() as db:
            db.execute("INSERT INTO v2_events VALUES (?,?,?,?)", (utc_now(), level, code, detail))

    def blocked(self) -> bool:
        return self.connection.execute("SELECT 1 FROM v2_incidents WHERE resolved_at IS NULL LIMIT 1").fetchone() is not None

    def incidents(self) -> list[dict[str, Any]]:
        return [dict(x) for x in self.connection.execute("SELECT * FROM v2_incidents WHERE resolved_at IS NULL ORDER BY first_seen")]

    def record_equity(self, when: datetime, equity: Decimal) -> None:
        with self.transaction() as db:
            db.execute("INSERT OR REPLACE INTO v2_equity VALUES (?,?)", (when.astimezone(UTC).replace(second=0, microsecond=0).isoformat(), format(equity, "f")))

    def day_reference(self, day: date, current: Decimal, observed_at: datetime) -> Decimal:
        key = day.isoformat()
        row = self.connection.execute("SELECT equity FROM v2_day_reference WHERE utc_day=?", (key,)).fetchone()
        if row:
            return Decimal(row["equity"])
        midnight = datetime.combine(day, datetime.min.time(), UTC).isoformat()
        tomorrow = datetime.combine(day + timedelta(days=1), datetime.min.time(), UTC).isoformat()
        sample = self.connection.execute("SELECT equity,observed_at FROM v2_equity WHERE observed_at>=? AND observed_at<? ORDER BY observed_at LIMIT 1", (midnight,tomorrow)).fetchone()
        value = Decimal(sample["equity"]) if sample else current
        sample_time = datetime.fromisoformat(sample["observed_at"]) if sample else observed_at
        late = int(sample_time.hour != 0 or sample_time.minute != 0)
        with self.transaction() as db:
            db.execute("INSERT INTO v2_day_reference VALUES (?,?,?,?)", (key, format(value, "f"), sample_time.isoformat(), late))
            if late:
                db.execute("INSERT INTO v2_events VALUES (?,?,?,?)", (utc_now(), "WARN", "E0_RECOVERED_LATE", f"{key}={value}"))
        return value

    def long_day_locks(self, day: date) -> tuple[set[str], set[str]]:
        key = day.isoformat()
        bought = {str(x[0]) for x in self.connection.execute(
            "SELECT symbol FROM v2_lots WHERE strategy='long' AND substr(decision_time,1,10)=?", (key,))}
        bought.update(str(x[0]) for x in self.connection.execute(
            "SELECT symbol FROM v2_day_locks WHERE utc_day=? AND kind='LONG_BOUGHT'", (key,)))
        stopped = {str(x[0]) for x in self.connection.execute(
            "SELECT symbol FROM v2_day_locks WHERE utc_day=? AND kind='LONG_STOP'", (key,))}
        return bought, stopped
