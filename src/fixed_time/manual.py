from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sqlite3
from uuid import uuid4


class ManualActionError(ValueError):
    pass


def submit_action(database: Path, *, lot_id: str, action: str, expected_exit_time: str) -> dict:
    if action not in {"SELL_NOW", "EXTEND_4H"} or not isinstance(lot_id, str) or not lot_id or len(lot_id) > 200:
        raise ManualActionError("invalid manual action")
    if not isinstance(expected_exit_time, str) or len(expected_exit_time) > 64:
        raise ManualActionError("invalid expected exit time")
    try:
        parsed_exit_time = datetime.fromisoformat(expected_exit_time)
        if parsed_exit_time.tzinfo is None or parsed_exit_time.utcoffset() is None:
            raise ValueError("timezone required")
    except ValueError as exc:
        raise ManualActionError("invalid expected exit time") from exc
    now = datetime.now(UTC).isoformat()
    db = sqlite3.connect(database.resolve().as_uri() + "?mode=rw", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN IMMEDIATE")
        lot = db.execute("SELECT status,exit_reason,scheduled_exit_time,manual_extended_at FROM v2_lots WHERE lot_id=?",
                         (lot_id,)).fetchone()
        if lot is None or lot["status"] != "OPEN" or lot["exit_reason"]:
            raise ManualActionError("position is no longer available")
        if lot["scheduled_exit_time"] != expected_exit_time:
            raise ManualActionError("position changed; refresh before submitting")
        if action == "EXTEND_4H" and (lot["manual_extended_at"] or expected_exit_time <= now):
            raise ManualActionError("four-hour extension is unavailable")
        if db.execute("SELECT 1 FROM v2_manual_actions WHERE lot_id=? AND status='PENDING'", (lot_id,)).fetchone():
            raise ManualActionError("an action is already pending for this position")
        action_id = uuid4().hex
        db.execute("INSERT INTO v2_manual_actions VALUES (?,?,?,?,?,?,NULL,NULL)",
                   (action_id, lot_id, action, expected_exit_time, "PENDING", now))
        db.commit()
        return {"action_id": action_id, "status": "PENDING"}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
