from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterator


class StateError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


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
            """)
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
            ):
                if name not in columns:
                    connection.execute(f"ALTER TABLE positions ADD COLUMN {name} {declaration}")
            connection.execute("UPDATE positions SET scheduled_exit_time = planned_exit_time WHERE scheduled_exit_time IS NULL")
            execution_columns = {row[1] for row in connection.execute("PRAGMA table_info(executions)")}
            if "executed_at" not in execution_columns:
                connection.execute("ALTER TABLE executions ADD COLUMN executed_at TEXT")
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

    def create_intent(self, values: dict[str, Any]) -> None:
        now = _utc_now()
        required = {"intent_id", "strategy", "symbol", "position_side", "decision_time", "planned_exit_time", "units", "priority_score", "client_order_id"}
        if set(values) != required:
            raise StateError(f"intent keys mismatch: {sorted(set(values) ^ required)}")
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO intents VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)""",
                (*[values[key] for key in ("intent_id", "strategy", "symbol", "position_side", "decision_time", "planned_exit_time", "units", "priority_score")], values["client_order_id"], now, now),
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
            connection.execute(
                """INSERT OR REPLACE INTO executions
                   (client_order_id, intent_id, role, reason, exchange_order_id, status, quantity, average_price, executed_at, recorded_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    client_order_id, intent_id, role, reason, str(response.get("orderId", "")), str(response.get("status", "UNKNOWN")),
                    str(response.get("executedQty", "0")), str(response.get("avgPrice", "0")), _exchange_time(response), _utc_now(),
                ),
            )

    def begin_exit_attempt(self, intent_id: str, requested_quantity: str, reason: str, client_order_id: str) -> dict[str, Any]:
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
            sequence = int(connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS value FROM exit_attempts WHERE intent_id = ?", (intent_id,)
            ).fetchone()["value"])
            now = _utc_now()
            connection.execute(
                "INSERT INTO exit_attempts VALUES (?, ?, ?, ?, ?, 'SUBMITTED', ?, ?)",
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

    def open_position(self, intent_id: str, quantity: str, entry_price: str, allowed_retrace: float | None = None, stop_algo_id: str | None = None) -> None:
        intent = self.intent(intent_id)
        if intent is None:
            raise StateError(f"unknown intent {intent_id}")
        now = _utc_now()
        with self.transaction() as connection:
            connection.execute(
                """INSERT OR REPLACE INTO positions
                   (intent_id, symbol, strategy, position_side, units, quantity, entry_price, planned_exit_time, scheduled_exit_time, stop_algo_id,
                    protection_active, protection_peak, protection_allowed_retrace, status, opened_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 'OPEN', ?, ?)""",
                (intent_id, intent["symbol"], intent["strategy"], intent["position_side"], intent["units"], quantity, entry_price,
                 intent["planned_exit_time"], intent["planned_exit_time"], stop_algo_id, entry_price if intent["strategy"] == "long" else None, allowed_retrace, now, now),
            )
            connection.execute("UPDATE intents SET status = 'OPEN', updated_at = ? WHERE intent_id = ?", (now, intent_id))

    def set_stop_algo(self, intent_id: str, algo_id: str) -> None:
        with self.transaction() as connection:
            cursor = connection.execute("UPDATE positions SET stop_algo_id = ?, updated_at = ? WHERE intent_id = ? AND status = 'OPEN'", (algo_id, _utc_now(), intent_id))
            if cursor.rowcount != 1:
                raise StateError(f"cannot add stop to {intent_id}")

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

    def update_position_quantity(self, intent_id: str, quantity: str) -> None:
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE positions SET quantity = ?, updated_at = ? WHERE intent_id = ? AND status = 'OPEN'",
                (quantity, _utc_now(), intent_id),
            )
            if cursor.rowcount != 1:
                raise StateError(f"cannot update quantity for {intent_id}")

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

    def known_stop_ids(self) -> set[str]:
        return {
            str(row["stop_algo_id"])
            for row in self.connection.execute("SELECT stop_algo_id FROM positions WHERE stop_algo_id IS NOT NULL")
        }

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
            connection.execute("INSERT INTO reconciliation VALUES (?, ?, ?)", (_utc_now(), status, detail))

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
                "INSERT OR IGNORE INTO shadow_tasks VALUES (?, ?, ?, ?, 'PENDING')",
                (shadow_id, symbol, entry_time, planned_exit_time),
            )

    def due_shadow_tasks(self, now: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM shadow_tasks WHERE status = 'PENDING' AND planned_exit_time <= ? ORDER BY planned_exit_time", (now,)
        )]

    def complete_shadow_task(self, shadow_id: str, exit_time: str, activated: bool, max_retrace: float | None) -> None:
        with self.transaction() as connection:
            connection.execute("INSERT OR IGNORE INTO shadow_history VALUES (?, ?, ?, ?)", (shadow_id, exit_time, int(activated), max_retrace))
            cursor = connection.execute("UPDATE shadow_tasks SET status = 'COMPLETE' WHERE shadow_id = ? AND status = 'PENDING'", (shadow_id,))
            if cursor.rowcount != 1:
                raise StateError(f"cannot complete shadow task {shadow_id}")

    def shadow_history(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT shadow_exit_time, shadow_activated, shadow_max_retrace FROM shadow_history ORDER BY shadow_exit_time")]

    def shadow_history_stats(self) -> tuple[int, int]:
        row = self.connection.execute(
            "SELECT COUNT(*) AS total, COALESCE(SUM(shadow_activated), 0) AS activated FROM shadow_history"
        ).fetchone()
        return int(row["total"]), int(row["activated"])

    def prune_shadow_history(self, earliest_exit_time: str) -> None:
        with self.transaction() as connection:
            connection.execute("DELETE FROM shadow_history WHERE shadow_exit_time < ?", (earliest_exit_time,))

    def seed_shadow_history(self, records: list[tuple[str, str, bool, float | None]]) -> int:
        with self.transaction() as connection:
            before = connection.execute("SELECT COUNT(*) AS value FROM shadow_history").fetchone()["value"]
            connection.executemany(
                "INSERT OR IGNORE INTO shadow_history VALUES (?, ?, ?, ?)",
                [(source_id, exit_time, int(activated), retrace) for source_id, exit_time, activated, retrace in records],
            )
            after = connection.execute("SELECT COUNT(*) AS value FROM shadow_history").fetchone()["value"]
        return int(after - before)
