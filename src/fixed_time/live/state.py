from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Iterator

EXECUTION_VERSION = "exchange-5s-v2"
SHADOW_VERSION = "base-minute-v1"

class StateError(RuntimeError):
    pass


class RuntimeLock:
    """An advisory, process-lifetime lock for a live runtime database.

    The dashboard opens the database read-only and intentionally does not take
    this lock.  Commands that can change the trading ledger take it so a
    second trader (or a seed/smoke command) cannot operate on the same account
    state concurrently.  The operating-system lock is released automatically
    if the owning process dies.
    """

    def __init__(self, database_path: Path):
        self.path = database_path.with_suffix(f"{database_path.suffix}.lock")
        self._handle: Any | None = None
        self._platform: str | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            try:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._platform = "fcntl"
            except ImportError:
                import msvcrt

                handle.seek(0)
                if not handle.read(1):
                    handle.seek(0)
                    handle.write(" ")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                self._platform = "msvcrt"
        except (BlockingIOError, OSError) as exc:
            handle.close()
            raise StateError(f"live runtime is already locked: {self.path}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()} started_at={_utc_now()}\n")
        handle.flush()
        self._handle = handle

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            if self._platform == "fcntl":
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            elif self._platform == "msvcrt":
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._handle.close()
            self._handle = None
            self._platform = None

    def __enter__(self) -> "RuntimeLock":
        self.acquire()
        return self

    def __exit__(self, *_args: Any) -> None:
        self.release()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _next_reconciliation_time(connection: sqlite3.Connection) -> str:
    """Keep distinct incident transitions even when one poll resolves immediately."""
    value = datetime.now(UTC)
    while connection.execute("SELECT 1 FROM reconciliation WHERE checked_at = ?", (value.isoformat(),)).fetchone() is not None:
        value += timedelta(microseconds=1)
    return value.isoformat()


def _exchange_time(response: dict[str, Any]) -> str | None:
    value = response.get("updateTime", response.get("transactTime", response.get("time")))
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000, UTC).isoformat()
    except (TypeError, ValueError, OSError):
        return str(value)


class StateStore:
    """Small durable ledger. Exchange data remains the source of trade truth."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute("PRAGMA foreign_keys=ON")
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
        with self.transaction() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS intents (
                    intent_id TEXT PRIMARY KEY,
                    strategy TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    position_side TEXT NOT NULL,
                    decision_time TEXT NOT NULL,
                    planned_exit_time TEXT NOT NULL,
                    units INTEGER NOT NULL,
                    priority_score REAL NOT NULL,
                    status TEXT NOT NULL,
                    client_order_id TEXT UNIQUE NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS positions (
                    intent_id TEXT PRIMARY KEY REFERENCES intents(intent_id),
                    symbol TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    position_side TEXT NOT NULL,
                    units INTEGER NOT NULL,
                    quantity TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    planned_exit_time TEXT NOT NULL,
                    scheduled_exit_time TEXT NOT NULL,
                    stop_algo_id TEXT,
                    protection_active INTEGER NOT NULL DEFAULT 0,
                    protection_peak TEXT,
                    protection_allowed_retrace REAL,
                    protection_last_bar_time TEXT,
                    protection_activated_at TEXT,
                    extension_active INTEGER NOT NULL DEFAULT 0,
                    extension_release_time TEXT,
                    extension_deadline_time TEXT,
                    status TEXT NOT NULL,
                    opened_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS executions (
                    client_order_id TEXT PRIMARY KEY,
                    intent_id TEXT NOT NULL REFERENCES intents(intent_id),
                    role TEXT NOT NULL,
                    reason TEXT,
                    exchange_order_id TEXT,
                    status TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    average_price TEXT NOT NULL,
                    executed_at TEXT,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS exit_attempts (
                    client_order_id TEXT PRIMARY KEY,
                    intent_id TEXT NOT NULL REFERENCES intents(intent_id),
                    sequence INTEGER NOT NULL,
                    requested_quantity TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(intent_id, sequence)
                );
                CREATE TABLE IF NOT EXISTS shadow_history (
                    source_id TEXT PRIMARY KEY,
                    shadow_exit_time TEXT NOT NULL,
                    shadow_activated INTEGER NOT NULL,
                    shadow_max_retrace REAL
                );
                CREATE TABLE IF NOT EXISTS shadow_tasks (
                    shadow_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    entry_time TEXT NOT NULL,
                    planned_exit_time TEXT NOT NULL,
                    status TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS processed_decisions (
                    decision_time TEXT PRIMARY KEY,
                    completed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS reconciliation (
                    checked_at TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    detail TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runtime_status (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    version TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    reconciled_at TEXT,
                    last_error TEXT,
                    available_usdt TEXT,
                    open_positions INTEGER NOT NULL,
                    open_units INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS decision_runs (
                    decision_time TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    universe_size INTEGER,
                    candidate_count INTEGER,
                    admission_count INTEGER,
                    status TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS protection_orders (
                    id INTEGER PRIMARY KEY,
                    intent_id TEXT NOT NULL REFERENCES intents(intent_id),
                    client_order_id TEXT UNIQUE,
                    trigger_price TEXT NOT NULL,
                    algo_id TEXT,
                    status TEXT NOT NULL DEFAULT 'SUBMITTED',
                    created_at TEXT NOT NULL,
                    confirmed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS decision_plans (
                    decision_time TEXT PRIMARY KEY,
                    candidates_json TEXT NOT NULL,
                    admissions_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS short_budgets (
                    decision_time TEXT PRIMARY KEY,
                    selected_count INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS equity_minutes (
                    minute_end TEXT PRIMARY KEY,
                    wallet_balance TEXT NOT NULL,
                    unrealized_pnl TEXT NOT NULL,
                    equity TEXT NOT NULL,
                    peak_equity TEXT NOT NULL,
                    drawdown TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS trade_fills (
                    symbol TEXT NOT NULL,
                    trade_id INTEGER NOT NULL,
                    order_id TEXT NOT NULL,
                    intent_id TEXT,
                    position_side TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    price TEXT NOT NULL,
                    quote_quantity TEXT NOT NULL,
                    realized_pnl TEXT NOT NULL,
                    commission TEXT NOT NULL,
                    commission_asset TEXT NOT NULL,
                    traded_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, trade_id)
                );
                CREATE TABLE IF NOT EXISTS income_events (
                    income_type TEXT NOT NULL,
                    transaction_id INTEGER NOT NULL,
                    symbol TEXT,
                    trade_id TEXT,
                    income TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    PRIMARY KEY (income_type, transaction_id)
                );
                CREATE TABLE IF NOT EXISTS sync_cursors (
                    stream TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS entry_blocks (
                    code TEXT PRIMARY KEY,
                    detail TEXT NOT NULL,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    occurrences INTEGER NOT NULL,
                    resolved_at TEXT
                );
            """)
            connection.execute("BEGIN")
            columns = {row[1] for row in connection.execute("PRAGMA table_info(positions)")}
            for name, declaration in (
                ("protection_active", "INTEGER NOT NULL DEFAULT 0"),
                ("protection_peak", "TEXT"),
                ("protection_allowed_retrace", "REAL"),
                ("protection_last_bar_time", "TEXT"),
                ("scheduled_exit_time", "TEXT"),
                ("protection_activated_at", "TEXT"),
                ("extension_active", "INTEGER NOT NULL DEFAULT 0"),
                ("extension_release_time", "TEXT"),
                ("extension_deadline_time", "TEXT"),
                ("execution_version", "TEXT NOT NULL DEFAULT 'minute-v1'"),
                ("filled_at", "TEXT"),
                ("exit_required", "TEXT"),
                ("trade_cursor", "INTEGER"),
                ("market_time", "TEXT"),
                ("target_stop", "TEXT"),
                ("hard_stop_pending", "INTEGER NOT NULL DEFAULT 0"),
                ("protection_history_count", "INTEGER"),
                ("protection_activated_count", "INTEGER"),
                ("base_unit_capital", "TEXT"),
                ("pre_entry_equity", "TEXT"),
                ("pre_entry_peak", "TEXT"),
                ("pre_entry_drawdown", "TEXT"),
                ("exposure_multiplier", "TEXT"),
                ("target_notional", "TEXT"),
                ("filled_notional", "TEXT"),
                ("sizing_version", "TEXT NOT NULL DEFAULT 'legacy-1x'"),
                ("last_mark_price", "TEXT"),
                ("last_mark_time", "TEXT"),
                ("unrealized_pnl", "TEXT"),
            ):
                if name not in columns:
                    connection.execute(f"ALTER TABLE positions ADD COLUMN {name} {declaration}")
            connection.execute("UPDATE positions SET scheduled_exit_time = planned_exit_time WHERE scheduled_exit_time IS NULL")
            intent_columns = {row[1] for row in connection.execute("PRAGMA table_info(intents)")}
            for name, declaration in (
                ("protection_json", "TEXT"), ("execution_version", "TEXT NOT NULL DEFAULT 'minute-v1'"),
                ("base_unit_capital", "TEXT"), ("pre_entry_equity", "TEXT"), ("pre_entry_peak", "TEXT"),
                ("pre_entry_drawdown", "TEXT"), ("exposure_multiplier", "TEXT"), ("target_notional", "TEXT"),
                ("sizing_version", "TEXT NOT NULL DEFAULT 'legacy-1x'"),
            ):
                if name not in intent_columns:
                    connection.execute(f"ALTER TABLE intents ADD COLUMN {name} {declaration}")
            for row in connection.execute("""SELECT intent_id, quantity, entry_price, units
                FROM positions WHERE filled_notional IS NULL""").fetchall():
                filled = Decimal(str(row["quantity"])) * Decimal(str(row["entry_price"]))
                unit = filled / Decimal(int(row["units"]))
                connection.execute(
                    """UPDATE positions SET filled_notional = ?, target_notional = COALESCE(target_notional, ?),
                       base_unit_capital = COALESCE(base_unit_capital, ?), exposure_multiplier = COALESCE(exposure_multiplier, '1')
                       WHERE intent_id = ?""",
                    (format(filled, "f"), format(filled, "f"), format(unit, "f"), row["intent_id"]),
                )
            protection_columns = {row[1] for row in connection.execute("PRAGMA table_info(protection_orders)")}
            if "quantity" not in protection_columns:
                connection.execute("ALTER TABLE protection_orders ADD COLUMN quantity TEXT")
            execution_columns = {row[1] for row in connection.execute("PRAGMA table_info(executions)")}
            if "executed_at" not in execution_columns:
                connection.execute("ALTER TABLE executions ADD COLUMN executed_at TEXT")
            equity_columns = {row[1] for row in connection.execute("PRAGMA table_info(equity_minutes)")}
            if "exposure_multiplier" not in equity_columns:
                connection.execute("ALTER TABLE equity_minutes ADD COLUMN exposure_multiplier TEXT NOT NULL DEFAULT '1.0'")
            attempt_columns = {row[1] for row in connection.execute("PRAGMA table_info(exit_attempts)")}
            if "applied_quantity" not in attempt_columns:
                connection.execute("ALTER TABLE exit_attempts ADD COLUMN applied_quantity TEXT NOT NULL DEFAULT '0'")
                # Older releases committed the fill and remaining quantity
                # separately. Rebuild open balances from their execution ledger.
                connection.execute("""UPDATE exit_attempts SET applied_quantity = COALESCE(
                    (SELECT quantity FROM executions WHERE executions.client_order_id = exit_attempts.client_order_id), '0')""")
                for row in connection.execute("""SELECT positions.intent_id, executions.quantity FROM positions
                    JOIN executions USING(intent_id) WHERE positions.status = 'OPEN' AND executions.role = 'ENTRY'
                    AND (EXISTS (SELECT 1 FROM exit_attempts WHERE exit_attempts.intent_id = positions.intent_id)
                         OR EXISTS (SELECT 1 FROM executions AS exits WHERE exits.intent_id = positions.intent_id AND exits.role = 'EXIT'))""").fetchall():
                    attempts = connection.execute("SELECT * FROM exit_attempts WHERE intent_id = ? ORDER BY sequence", (row["intent_id"],)).fetchall()
                    exits = connection.execute("SELECT quantity, reason FROM executions WHERE intent_id = ? AND role = 'EXIT' ORDER BY recorded_at", (row["intent_id"],)).fetchall()
                    remaining = Decimal(row["quantity"]) - sum((Decimal(item["quantity"]) for item in exits), Decimal(0))
                    if remaining < 0:
                        raise StateError("legacy exit ledger exceeds entry quantity")
                    reason = attempts[-1]["reason"] if attempts else exits[-1]["reason"] or "EXCHANGE_STOP"
                    connection.execute("UPDATE positions SET quantity = ?, exit_required = ? WHERE intent_id = ?", (str(remaining), reason, row["intent_id"]))
                    connection.execute("UPDATE exit_attempts SET status = 'SUBMITTED' WHERE intent_id = ? AND status != 'SETTLED'", (row["intent_id"],))
            history_columns = {row[1] for row in connection.execute("PRAGMA table_info(shadow_history)")}
            if "source_id" not in history_columns:
                connection.execute("ALTER TABLE shadow_history RENAME TO shadow_history_legacy")
                connection.execute("""CREATE TABLE shadow_history (
                    source_id TEXT PRIMARY KEY,
                    shadow_exit_time TEXT NOT NULL,
                    shadow_activated INTEGER NOT NULL,
                    shadow_max_retrace REAL
                )""")
                connection.execute("""INSERT INTO shadow_history
                    SELECT 'legacy:' || rowid, shadow_exit_time, shadow_activated, shadow_max_retrace
                    FROM shadow_history_legacy""")
            # Unversioned live labels used a different entry reference. Retain
            # them for audit, but never mix them into the canonical sample pool.
            for table, additions in {
                "shadow_history": [("definition_version", "TEXT NOT NULL DEFAULT 'unverified'")],
                "shadow_tasks": [("definition_version", "TEXT NOT NULL DEFAULT 'unverified'"),
                                 ("progress_json", "TEXT"), ("last_error", "TEXT")],
            }.items():
                present = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
                for name, declaration in additions:
                    if name not in present:
                        connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")
            connection.execute("""UPDATE shadow_tasks SET shadow_id = 'long:' || symbol || ':' || entry_time,
                entry_time = strftime('%Y-%m-%dT%H:%M:00+00:00', entry_time, '+1 minute'),
                definition_version = ?, status = 'PENDING', progress_json = NULL WHERE definition_version = 'unverified'""", (SHADOW_VERSION,))
            for name, target in (("pending_shadows", "shadow_tasks(status, entry_time)"),
                                 ("pending_entries", "intents(status)"),
                                 ("open_positions", "positions(status)"),
                                 ("pending_hard_stops", "positions(hard_stop_pending)"),
                                 ("pending_exits", "exit_attempts(status, intent_id)"),
                                  ("active_protection", "protection_orders(status, intent_id)"),
                                  ("protection_algo", "protection_orders(algo_id)"),
                                  ("position_stop", "positions(stop_algo_id)"),
                                  ("equity_minutes_time", "equity_minutes(minute_end)"),
                                  ("active_entry_blocks", "entry_blocks(resolved_at)"),
                                  ("execution_recorded", "executions(recorded_at)"),
                                  ("execution_intent", "executions(intent_id, role)"),
                                  ("trade_fill_time", "trade_fills(traded_at)"),
                                  ("trade_fill_intent", "trade_fills(intent_id, traded_at)"),
                                  ("income_time", "income_events(occurred_at)")):
                connection.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {target}")

    def create_intent(self, values: dict[str, Any], *, protection: tuple[float, int, int] | None = None,
                      execution_version: str = "minute-v1", sizing: dict[str, str] | None = None) -> None:
        now = _utc_now()
        required = {"intent_id", "strategy", "symbol", "position_side", "decision_time", "planned_exit_time", "units", "priority_score", "client_order_id"}
        if set(values) != required:
            raise StateError(f"intent keys mismatch: {sorted(set(values) ^ required)}")
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO intents (intent_id, strategy, symbol, position_side, decision_time, planned_exit_time,
                   units, priority_score, status, client_order_id, created_at, updated_at, protection_json, execution_version,
                   base_unit_capital, pre_entry_equity, pre_entry_peak, pre_entry_drawdown, exposure_multiplier, target_notional, sizing_version)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (*[values[key] for key in ("intent_id", "strategy", "symbol", "position_side", "decision_time", "planned_exit_time", "units", "priority_score")], values["client_order_id"], now, now, json.dumps(protection) if protection is not None else None, execution_version,
                 *[(sizing or {}).get(key) for key in ("base_unit_capital", "pre_entry_equity", "pre_entry_peak", "pre_entry_drawdown", "exposure_multiplier", "target_notional")],
                 (sizing or {}).get("sizing_version", "legacy-1x")),
            )

    def intent(self, intent_id: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM intents WHERE intent_id = ?", (intent_id,)).fetchone()
        return dict(row) if row is not None else None

    def set_intent_status(self, intent_id: str, status: str) -> None:
        with self.transaction() as connection:
            cursor = connection.execute("UPDATE intents SET status = ?, updated_at = ? WHERE intent_id = ?", (status, _utc_now(), intent_id))
            if cursor.rowcount != 1:
                raise StateError(f"unknown intent {intent_id}")

    def record_execution(self, intent_id: str, client_order_id: str, role: str, response: dict[str, Any], reason: str | None = None) -> None:
        with self.transaction() as connection:
            self._record_execution(connection, intent_id, client_order_id, role, response, reason)

    @staticmethod
    def _record_execution(connection: sqlite3.Connection, intent_id: str, client_order_id: str, role: str, response: dict[str, Any], reason: str | None) -> None:
        exchange_order_id = str(response.get("orderId", ""))
        connection.execute(
            """INSERT OR REPLACE INTO executions
               (client_order_id, intent_id, role, reason, exchange_order_id, status, quantity, average_price, executed_at, recorded_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                client_order_id, intent_id, role, reason, exchange_order_id, str(response.get("status", "UNKNOWN")),
                str(response.get("executedQty", "0")), str(response.get("avgPrice", "0")), _exchange_time(response), _utc_now(),
            ),
        )
        if exchange_order_id:
            connection.execute(
                """UPDATE trade_fills SET intent_id = ? WHERE order_id = ? AND intent_id IS NULL
                   AND symbol = (SELECT symbol FROM intents WHERE intent_id = ?)""",
                (intent_id, exchange_order_id, intent_id),
            )

    def begin_exit_attempt(self, intent_id: str, requested_quantity: str, reason: str, client_order_id: str, *, sequence: int | None = None) -> dict[str, Any]:
        """Persist an idempotency key before a close order can reach Binance."""
        with self.transaction() as connection:
            existing = connection.execute(
                """SELECT * FROM exit_attempts
                   WHERE intent_id = ? AND status IN ('SUBMITTED', 'FILLED')
                   ORDER BY sequence DESC LIMIT 1""",
                (intent_id,),
            ).fetchone()
            if existing is not None:
                return dict(existing)
            if sequence is None:
                sequence = self.next_exit_sequence(intent_id)
            now = _utc_now()
            connection.execute(
                "INSERT INTO exit_attempts (client_order_id, intent_id, sequence, requested_quantity, reason, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'SUBMITTED', ?, ?)",
                (client_order_id, intent_id, sequence, requested_quantity, reason, now, now),
            )
            row = connection.execute("SELECT * FROM exit_attempts WHERE client_order_id = ?", (client_order_id,)).fetchone()
            assert row is not None
            return dict(row)

    def next_exit_sequence(self, intent_id: str) -> int:
        row = self.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS value FROM exit_attempts WHERE intent_id = ?", (intent_id,)
        ).fetchone()
        return int(row["value"])

    def finish_exit_attempt(self, client_order_id: str, status: str) -> None:
        if status not in {"PARTIAL", "NO_FILL", "FILLED", "SETTLED"}:
            raise StateError(f"invalid exit attempt status: {status}")
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE exit_attempts SET status = ?, updated_at = ? WHERE client_order_id = ?",
                (status, _utc_now(), client_order_id),
            )
            if cursor.rowcount != 1:
                raise StateError(f"unknown exit attempt {client_order_id}")

    def filled_exit_attempt(self, intent_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            """SELECT * FROM exit_attempts
               WHERE intent_id = ? AND status = 'FILLED'
               ORDER BY sequence DESC LIMIT 1""",
            (intent_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    def open_position(self, intent_id: str, quantity: str, entry_price: str, allowed_retrace: float | None = None, stop_algo_id: str | None = None,
                      *, execution_version: str = "minute-v1", filled_at: str | None = None) -> None:
        intent = self.intent(intent_id)
        if intent is None:
            raise StateError(f"unknown intent {intent_id}")
        now = _utc_now()
        filled_notional = format(Decimal(str(quantity)) * Decimal(str(entry_price)), "f")
        with self.transaction() as connection:
            connection.execute(
                """INSERT OR REPLACE INTO positions
                   (intent_id, symbol, strategy, position_side, units, quantity, entry_price, planned_exit_time, scheduled_exit_time, stop_algo_id,
                     protection_active, protection_peak, protection_allowed_retrace, status, opened_at, updated_at, execution_version, filled_at,
                     base_unit_capital, pre_entry_equity, pre_entry_peak, pre_entry_drawdown, exposure_multiplier, target_notional, filled_notional, sizing_version)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (intent_id, intent["symbol"], intent["strategy"], intent["position_side"], intent["units"], quantity, entry_price,
                 intent["planned_exit_time"], intent["planned_exit_time"], stop_algo_id, entry_price if intent["strategy"] == "long" else None, allowed_retrace, now, now, execution_version, filled_at or now,
                 intent.get("base_unit_capital"), intent.get("pre_entry_equity"), intent.get("pre_entry_peak"), intent.get("pre_entry_drawdown"),
                 intent.get("exposure_multiplier"), intent.get("target_notional"), filled_notional, intent.get("sizing_version", "legacy-1x")),
            )
            connection.execute("UPDATE intents SET status = 'OPEN', updated_at = ? WHERE intent_id = ?", (now, intent_id))
            if intent.get("protection_json"):
                sample = json.loads(intent["protection_json"])
                connection.execute("UPDATE positions SET protection_history_count = ?, protection_activated_count = ? WHERE intent_id = ?", (sample[1], sample[2], intent_id))

    def set_stop_algo(self, intent_id: str, algo_id: str) -> None:
        with self.transaction() as connection:
            cursor = connection.execute("UPDATE positions SET stop_algo_id = ?, hard_stop_pending = 0, updated_at = ? WHERE intent_id = ?", (algo_id, _utc_now(), intent_id))
            if cursor.rowcount != 1:
                raise StateError(f"cannot add stop to {intent_id}")

    def set_hard_stop_pending(self, intent_id: str, pending: bool) -> None:
        with self.transaction() as connection:
            connection.execute("UPDATE positions SET hard_stop_pending = ?, updated_at = ? WHERE intent_id = ?", (int(pending), _utc_now(), intent_id))

    def pending_hard_stops(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("""SELECT positions.*, intents.decision_time
            FROM positions JOIN intents USING(intent_id) WHERE positions.hard_stop_pending = 1""")]

    def update_protection(self, intent_id: str, active: bool, peak: str, last_bar_time: str, activated_at: str | None = None) -> None:
        with self.transaction() as connection:
            cursor = connection.execute(
                """UPDATE positions
                   SET protection_active = ?, protection_peak = ?, protection_last_bar_time = ?,
                       protection_activated_at = COALESCE(protection_activated_at, ?), updated_at = ?
                   WHERE intent_id = ? AND status = 'OPEN'""",
                (int(active), peak, last_bar_time, activated_at, _utc_now(), intent_id),
            )
            if cursor.rowcount != 1:
                raise StateError(f"cannot update protection for {intent_id}")

    def activate_extension(self, intent_id: str, scheduled_exit_time: str, release_time: str) -> None:
        with self.transaction() as connection:
            cursor = connection.execute(
                """UPDATE positions
                   SET extension_active = 1, scheduled_exit_time = ?, extension_release_time = ?, extension_deadline_time = ?, updated_at = ?
                   WHERE intent_id = ? AND status = 'OPEN' AND extension_active = 0""",
                (scheduled_exit_time, release_time, scheduled_exit_time, _utc_now(), intent_id),
            )
            if cursor.rowcount != 1:
                raise StateError(f"cannot activate extension for {intent_id}")

    def open_positions(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            """SELECT positions.*, intents.priority_score, intents.decision_time, intents.client_order_id AS entry_client_order_id
               FROM positions JOIN intents USING(intent_id)
               WHERE positions.status = 'OPEN' ORDER BY opened_at"""
        )]

    def pending_intents(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM intents WHERE status = 'PENDING' ORDER BY created_at"
        )]

    def known_stop_ids(self, observed: set[str] | None = None) -> set[str]:
        if observed is not None and not observed:
            return set()
        placeholders = ",".join("?" for _ in observed) if observed else ""
        condition = f"IN ({placeholders})" if observed else "IS NOT NULL"
        params = tuple(observed) * 2 if observed else ()
        return {str(row[0]) for row in self.connection.execute(
            f"SELECT stop_algo_id FROM positions WHERE stop_algo_id {condition} UNION SELECT algo_id FROM protection_orders WHERE algo_id {condition}", params)}

    def close_position(self, intent_id: str, status: str = "CLOSED") -> None:
        with self.transaction() as connection:
            cursor = connection.execute("UPDATE positions SET status = ?, updated_at = ? WHERE intent_id = ? AND status = 'OPEN'", (status, _utc_now(), intent_id))
            if cursor.rowcount != 1:
                raise StateError(f"cannot close {intent_id}")
            connection.execute("UPDATE intents SET status = ?, updated_at = ? WHERE intent_id = ?", (status, _utc_now(), intent_id))
            connection.execute(
                "UPDATE exit_attempts SET status = 'SETTLED', updated_at = ? WHERE intent_id = ? AND status = 'FILLED'",
                (_utc_now(), intent_id),
            )

    def units_open(self, strategy: str | None = None) -> int:
        query, params = "SELECT COALESCE(SUM(units), 0) AS value FROM positions WHERE status = 'OPEN'", ()
        if strategy is not None:
            query += " AND strategy = ?"
            params = (strategy,)
        return int(self.connection.execute(query, params).fetchone()["value"])

    def decision_done(self, decision_time: str) -> bool:
        return self.connection.execute("SELECT 1 FROM processed_decisions WHERE decision_time = ?", (decision_time,)).fetchone() is not None

    def mark_decision_done(self, decision_time: str) -> None:
        with self.transaction() as connection:
            connection.execute("INSERT OR IGNORE INTO processed_decisions VALUES (?, ?)", (decision_time, _utc_now()))

    def record_reconciliation(self, status: str, detail: str) -> None:
        with self.transaction() as connection:
            connection.execute("INSERT INTO reconciliation VALUES (?, ?, ?)", (_next_reconciliation_time(connection), status, detail))

    def block_entry(self, code: str, detail: str) -> None:
        """Persist one active incident instead of appending the same failure every poll."""
        now = _utc_now()
        with self.transaction() as connection:
            active = connection.execute("SELECT occurrences FROM entry_blocks WHERE code = ? AND resolved_at IS NULL", (code,)).fetchone()
            if active is None:
                connection.execute(
                    "INSERT INTO entry_blocks VALUES (?, ?, ?, ?, 1, NULL) ON CONFLICT(code) DO UPDATE SET detail = excluded.detail, first_seen = excluded.first_seen, last_seen = excluded.last_seen, occurrences = 1, resolved_at = NULL",
                    (code, detail, now, now),
                )
                connection.execute("INSERT INTO reconciliation VALUES (?, 'BLOCKED', ?)", (_next_reconciliation_time(connection), detail))
            else:
                connection.execute(
                    "UPDATE entry_blocks SET detail = ?, last_seen = ?, occurrences = occurrences + 1 WHERE code = ?",
                    (detail, now, code),
                )

    def resolve_entry_block(self, code: str) -> None:
        now = _utc_now()
        with self.transaction() as connection:
            row = connection.execute("SELECT detail FROM entry_blocks WHERE code = ? AND resolved_at IS NULL", (code,)).fetchone()
            if row is None:
                return
            connection.execute("UPDATE entry_blocks SET resolved_at = ? WHERE code = ?", (now, code))
            connection.execute("INSERT INTO reconciliation VALUES (?, 'RESOLVED', ?)", (_next_reconciliation_time(connection), row["detail"]))

    def entry_blocked(self) -> bool:
        return self.connection.execute("SELECT 1 FROM entry_blocks WHERE resolved_at IS NULL LIMIT 1").fetchone() is not None

    def active_entry_blocks(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM entry_blocks WHERE resolved_at IS NULL ORDER BY first_seen"
        )]

    def equity_minute(self, minute_end: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM equity_minutes WHERE minute_end = ?", (minute_end,)).fetchone()
        return dict(row) if row is not None else None

    def latest_equity_minute(self) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM equity_minutes ORDER BY minute_end DESC LIMIT 1").fetchone()
        return dict(row) if row is not None else None

    def record_equity_minute(self, minute_end: str, wallet_balance: Decimal, unrealized_pnl: Decimal,
                             equity: Decimal, peak_equity: Decimal, drawdown: Decimal,
                             exposure_multiplier: Decimal = Decimal("1.0")) -> bool:
        with self.transaction() as connection:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO equity_minutes
                   (minute_end, wallet_balance, unrealized_pnl, equity, peak_equity, drawdown, recorded_at, exposure_multiplier)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (minute_end, format(wallet_balance, "f"), format(unrealized_pnl, "f"), format(equity, "f"),
                 format(peak_equity, "f"), format(drawdown, "f"), _utc_now(), format(exposure_multiplier, "f")),
            )
            return cursor.rowcount == 1

    def update_position_marks(self, minute_end: str, marks: dict[str, tuple[Decimal, Decimal]]) -> None:
        if not marks:
            return
        with self.transaction() as connection:
            for intent_id, (mark, unrealized) in marks.items():
                connection.execute(
                    """UPDATE positions SET last_mark_price = ?, last_mark_time = ?, unrealized_pnl = ?
                       WHERE intent_id = ? AND status = 'OPEN'""",
                    (format(mark, "f"), minute_end, format(unrealized, "f"), intent_id),
                )

    def known_trade_symbols(self) -> list[str]:
        return [str(row[0]) for row in self.connection.execute(
            "SELECT DISTINCT symbol FROM intents ORDER BY symbol"
        )]

    def active_trade_symbols(self, since: str) -> list[str]:
        return [str(row[0]) for row in self.connection.execute(
            """SELECT symbol FROM positions WHERE status='OPEN'
               UNION
               SELECT intents.symbol FROM executions JOIN intents USING(intent_id) WHERE executions.recorded_at >= ?
               ORDER BY symbol""",
            (since,),
        )]

    def sync_cursor(self, stream: str) -> str | None:
        row = self.connection.execute("SELECT value FROM sync_cursors WHERE stream = ?", (stream,)).fetchone()
        return str(row["value"]) if row is not None else None

    def set_sync_cursor(self, stream: str, value: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO sync_cursors VALUES (?, ?, ?)
                   ON CONFLICT(stream) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at""",
                (stream, value, _utc_now()),
            )

    def record_trade_fills(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        inserted = 0
        with self.transaction() as connection:
            for row in rows:
                intent = connection.execute(
                    """SELECT executions.intent_id FROM executions JOIN intents USING(intent_id)
                       WHERE exchange_order_id = ? AND intents.symbol = ? LIMIT 1""",
                    (str(row["orderId"]), str(row["symbol"])),
                ).fetchone()
                cursor = connection.execute(
                    """INSERT OR IGNORE INTO trade_fills
                       (symbol, trade_id, order_id, intent_id, position_side, side, quantity, price,
                        quote_quantity, realized_pnl, commission, commission_asset, traded_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (str(row["symbol"]), int(row["id"]), str(row["orderId"]),
                     intent["intent_id"] if intent is not None else None,
                     str(row["positionSide"]), str(row["side"]), str(row["qty"]), str(row["price"]),
                     str(row.get("quoteQty", "0")), str(row.get("realizedPnl", "0")),
                     str(row.get("commission", "0")), str(row.get("commissionAsset", "")),
                     datetime.fromtimestamp(int(row["time"]) / 1000, UTC).isoformat()),
                )
                inserted += cursor.rowcount
        return inserted

    def record_income_events(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        inserted = 0
        with self.transaction() as connection:
            for row in rows:
                cursor = connection.execute(
                    """INSERT OR IGNORE INTO income_events
                       (income_type, transaction_id, symbol, trade_id, income, asset, occurred_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (str(row["incomeType"]), int(row["tranId"]), str(row.get("symbol") or ""),
                     str(row.get("tradeId") or ""), str(row["income"]), str(row["asset"]),
                     datetime.fromtimestamp(int(row["time"]) / 1000, UTC).isoformat()),
                )
                inserted += cursor.rowcount
        return inserted

    def usdt_income_between(self, start_exclusive: str, end_inclusive: str) -> Decimal:
        rows = self.connection.execute(
            """SELECT income FROM income_events
               WHERE asset = 'USDT' AND occurred_at > ? AND occurred_at <= ?""",
            (start_exclusive, end_inclusive),
        )
        return sum((Decimal(str(row["income"])) for row in rows), Decimal("0"))

    def positions_at(self, at: str) -> list[dict[str, Any]]:
        """Reconstruct quantities at an exchange timestamp from cumulative order executions."""
        rows = self.connection.execute(
            """SELECT positions.intent_id, positions.symbol, positions.position_side,
                      positions.entry_price, positions.quantity, positions.filled_at, positions.status,
                      entry.quantity AS entry_quantity, entry.average_price AS execution_entry_price,
                      COALESCE(entry.executed_at, entry.recorded_at) AS entry_time
               FROM positions
               LEFT JOIN executions AS entry
                 ON entry.intent_id = positions.intent_id AND entry.role = 'ENTRY'"""
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            if row["entry_quantity"] is None:
                if row["status"] != "OPEN" or (row["filled_at"] and str(row["filled_at"]) >= at):
                    continue
                quantity = Decimal(str(row["quantity"]))
                entry_price = Decimal(str(row["entry_price"]))
            else:
                if not row["entry_time"] or str(row["entry_time"]) > at:
                    continue
                quantity = Decimal(str(row["entry_quantity"]))
                exits = self.connection.execute(
                    """SELECT quantity FROM executions
                       WHERE intent_id = ? AND role = 'EXIT'
                         AND COALESCE(executed_at, recorded_at) <= ?""",
                    (row["intent_id"], at),
                )
                quantity -= sum((Decimal(str(item["quantity"])) for item in exits), Decimal("0"))
                entry_price = Decimal(str(row["execution_entry_price"]))
            if quantity > 0:
                result.append({"intent_id": row["intent_id"], "symbol": row["symbol"], "position_side": row["position_side"],
                               "quantity": quantity, "entry_price": entry_price})
        return result

    def update_runtime_status(
        self, version: str, started_at: str, available_usdt: str | None, open_positions: int, open_units: int,
        *, reconciled: bool = False, error: str | None = None,
    ) -> None:
        now = _utc_now()
        with self.transaction() as connection:
            current = connection.execute("SELECT reconciled_at FROM runtime_status WHERE singleton = 1").fetchone()
            reconciled_at = now if reconciled else (current["reconciled_at"] if current else None)
            connection.execute(
                """INSERT INTO runtime_status
                   VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(singleton) DO UPDATE SET
                     version = excluded.version, started_at = excluded.started_at, heartbeat_at = excluded.heartbeat_at,
                     reconciled_at = excluded.reconciled_at, last_error = excluded.last_error,
                     available_usdt = excluded.available_usdt, open_positions = excluded.open_positions,
                     open_units = excluded.open_units""",
                (version, started_at, now, reconciled_at, error, available_usdt, open_positions, open_units),
            )

    def runtime_status(self) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM runtime_status WHERE singleton = 1").fetchone()
        return dict(row) if row is not None else None

    def start_decision(self, decision_time: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO decision_runs VALUES (?, ?, NULL, NULL, NULL, NULL, 'RUNNING', '{}', NULL)
                   ON CONFLICT(decision_time) DO NOTHING""",
                (decision_time, _utc_now()),
            )

    def finish_decision(
        self, decision_time: str, universe_size: int, candidates: list[dict[str, Any]], admissions: list[dict[str, Any]],
        *, error: str | None = None,
    ) -> None:
        detail = json.dumps({"candidates": candidates, "admissions": admissions}, default=str, separators=(",", ":"))
        with self.transaction() as connection:
            cursor = connection.execute(
                """UPDATE decision_runs
                   SET completed_at = ?, universe_size = ?, candidate_count = ?, admission_count = ?,
                       status = ?, detail_json = ?, error = ?
                   WHERE decision_time = ?""",
                (_utc_now(), universe_size, len(candidates), len(admissions), "FAILED" if error else "COMPLETE", detail, error, decision_time),
            )
            if cursor.rowcount != 1:
                raise StateError(f"decision was not started: {decision_time}")

    def recent_decisions(self, limit: int = 20) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM decision_runs ORDER BY decision_time DESC LIMIT ?", (limit,)
        )]

    def add_shadow_task(self, shadow_id: str, symbol: str, entry_time: str, planned_exit_time: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO shadow_tasks (shadow_id, symbol, entry_time, planned_exit_time, status, definition_version) VALUES (?, ?, ?, ?, 'PENDING', ?)",
                (shadow_id, symbol, entry_time, planned_exit_time, SHADOW_VERSION),
            )

    def due_shadow_tasks(self, now: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM shadow_tasks WHERE status = 'PENDING' AND entry_time < ? ORDER BY planned_exit_time", (now,)
        )]

    def complete_shadow_task(self, shadow_id: str, exit_time: str, activated: bool, max_retrace: float | None) -> None:
        with self.transaction() as connection:
            connection.execute("INSERT OR REPLACE INTO shadow_history VALUES (?, ?, ?, ?, ?)", (shadow_id, exit_time, int(activated), max_retrace, SHADOW_VERSION))
            cursor = connection.execute("UPDATE shadow_tasks SET status = 'COMPLETE' WHERE shadow_id = ? AND status = 'PENDING'", (shadow_id,))
            if cursor.rowcount != 1:
                raise StateError(f"cannot complete shadow task {shadow_id}")

    def shadow_history(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT shadow_exit_time, shadow_activated, shadow_max_retrace FROM shadow_history WHERE definition_version = ? ORDER BY shadow_exit_time", (SHADOW_VERSION,))]

    def shadow_history_stats(self) -> tuple[int, int]:
        row = self.connection.execute(
            "SELECT COUNT(*) AS total, COALESCE(SUM(shadow_activated AND shadow_max_retrace IS NOT NULL), 0) AS activated FROM shadow_history WHERE definition_version = ?", (SHADOW_VERSION,)
        ).fetchone()
        return int(row["total"]), int(row["activated"])

    def prune_shadow_history(self, earliest_exit_time: str) -> None:
        with self.transaction() as connection:
            connection.execute("DELETE FROM shadow_history WHERE shadow_exit_time < ?", (earliest_exit_time,))

    def seed_shadow_history(self, records: list[tuple[str, str, bool, float | None]]) -> int:
        with self.transaction() as connection:
            before = connection.execute("SELECT COUNT(*) AS value FROM shadow_history").fetchone()["value"]
            connection.executemany(
                "INSERT OR IGNORE INTO shadow_history VALUES (?, ?, ?, ?, ?)",
                [(source_id, exit_time, int(activated), retrace, SHADOW_VERSION) for source_id, exit_time, activated, retrace in records],
            )
            after = connection.execute("SELECT COUNT(*) AS value FROM shadow_history").fetchone()["value"]
        return int(after - before)

    def require_exit(self, intent_id: str, reason: str) -> None:
        with self.transaction() as connection:
            connection.execute("UPDATE positions SET exit_required = COALESCE(exit_required, ?) WHERE intent_id = ? AND status = 'OPEN'", (reason, intent_id))

    def unsettled_exits(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("""SELECT exit_attempts.* FROM exit_attempts
            JOIN positions USING(intent_id) WHERE positions.status = 'OPEN' AND exit_attempts.status = 'SUBMITTED'""")]

    def apply_exit(self, attempt: dict[str, Any], response: dict[str, Any]) -> bool:
        """Apply cumulative fills once, atomically with the remaining quantity."""
        filled = Decimal(str(response.get("executedQty", "0")))
        terminal = response.get("status") in {"FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"}
        with self.transaction() as connection:
            applied = Decimal(connection.execute("SELECT applied_quantity FROM exit_attempts WHERE client_order_id = ?", (attempt["client_order_id"],)).fetchone()[0])
            quantity = Decimal(connection.execute("SELECT quantity FROM positions WHERE intent_id = ?", (attempt["intent_id"],)).fetchone()[0])
            delta = filled - applied
            if delta < 0 or delta > quantity or filled > Decimal(attempt["requested_quantity"]):
                raise StateError("invalid cumulative exit fill")
            remaining = quantity - delta
            self._record_execution(connection, attempt["intent_id"], attempt["client_order_id"], "EXIT", response, attempt["reason"])
            status = "FILLED" if remaining == 0 else ("PARTIAL" if filled else "NO_FILL") if terminal else "SUBMITTED"
            connection.execute("UPDATE positions SET quantity = ? WHERE intent_id = ?", (format(remaining, "f"), attempt["intent_id"]))
            connection.execute("UPDATE exit_attempts SET applied_quantity = ?, status = ?, updated_at = ? WHERE client_order_id = ?",
                               (format(filled, "f"), status, _utc_now(), attempt["client_order_id"]))
        return remaining == 0

    def apply_stop_fill(self, intent_id: str, client_id: str, response: dict[str, Any], reason: str) -> None:
        with self.transaction() as connection:
            previous = connection.execute("SELECT quantity FROM executions WHERE client_order_id = ?", (client_id,)).fetchone()
            quantity = Decimal(connection.execute("SELECT quantity FROM positions WHERE intent_id = ?", (intent_id,)).fetchone()[0])
            filled = Decimal(str(response["executedQty"]))
            delta = filled - (Decimal(previous[0]) if previous else 0)
            if delta < 0 or delta > quantity:
                raise StateError("invalid cumulative stop fill")
            self._record_execution(connection, intent_id, client_id, "EXIT", response, reason)
            connection.execute("UPDATE positions SET quantity = ?, exit_required = COALESCE(exit_required, ?) WHERE intent_id = ?", (format(quantity - delta, "f"), reason, intent_id))

    def protection_orders(self, intent_id: str | None = None, *, include_inactive: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM protection_orders WHERE " + ("1 = 1" if include_inactive else "status IN ('SUBMITTED', 'ACTIVE', 'TRIGGERED')")
        return [dict(row) for row in self.connection.execute(sql + (" AND intent_id = ?" if intent_id else "") + " ORDER BY id", (intent_id,) if intent_id else ())]

    def begin_protection_order(self, intent_id: str, trigger: str, quantity: str) -> dict[str, Any]:
        with self.transaction() as connection:
            cursor = connection.execute("INSERT INTO protection_orders (intent_id, trigger_price, quantity, created_at) VALUES (?, ?, ?, ?)", (intent_id, trigger, quantity, _utc_now()))
            order_id = cursor.lastrowid
            client_id = f"ft-p-{datetime.now(UTC):%y%m%d%H%M%S}-{order_id}"
            connection.execute("UPDATE protection_orders SET client_order_id = ? WHERE id = ?", (client_id, order_id))
            return dict(connection.execute("SELECT * FROM protection_orders WHERE id = ?", (order_id,)).fetchone())

    def set_protection_order(self, order_id: int, status: str, algo_id: str | None = None) -> None:
        with self.transaction() as connection:
            connection.execute("UPDATE protection_orders SET status = ?, algo_id = COALESCE(?, algo_id), confirmed_at = ? WHERE id = ?", (status, algo_id, _utc_now(), order_id))

    def update_market(self, intent_id: str, cursor: int | None, market_time: str, peak: str, activated_at: str | None, target: str | None) -> None:
        with self.transaction() as connection:
            connection.execute("""UPDATE positions SET trade_cursor = ?, market_time = ?, protection_peak = ?,
                protection_active = CASE WHEN ? IS NOT NULL THEN 1 ELSE protection_active END,
                protection_activated_at = COALESCE(protection_activated_at, ?), target_stop = COALESCE(?, target_stop)
                WHERE intent_id = ?""",
                (cursor, market_time, peak, activated_at, activated_at, target, intent_id))

    def save_shadow_progress(self, shadow_id: str, progress: dict[str, Any] | None, error: str | None = None) -> None:
        with self.transaction() as connection:
            connection.execute("UPDATE shadow_tasks SET progress_json = COALESCE(?, progress_json), last_error = ? WHERE shadow_id = ?", (json.dumps(progress) if progress is not None else None, error, shadow_id))

    def short_count(self, decision_time: str) -> int | None:
        row = self.connection.execute("SELECT selected_count FROM short_budgets WHERE decision_time = ?", (decision_time,)).fetchone()
        return int(row[0]) if row else None

    def save_short_count(self, decision_time: str, count: int) -> None:
        with self.transaction() as connection:
            connection.execute("INSERT OR IGNORE INTO short_budgets VALUES (?, ?)", (decision_time, count))

    def decision_plan(self, decision_time: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
        row = self.connection.execute("SELECT candidates_json, admissions_json FROM decision_plans WHERE decision_time = ?", (decision_time,)).fetchone()
        return (json.loads(row[0]), json.loads(row[1])) if row else None

    def save_decision_plan(self, decision_time: str, candidates: list[dict[str, Any]], admissions: list[dict[str, Any]]) -> None:
        with self.transaction() as connection:
            connection.execute("INSERT OR IGNORE INTO decision_plans VALUES (?, ?, ?)", (decision_time, json.dumps(candidates, default=str), json.dumps(admissions, default=str)))
