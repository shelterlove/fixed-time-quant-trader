from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
import sqlite3
from uuid import uuid4


class ManualActionError(ValueError):
    pass


def submit_action(database: Path, *, lot_id: str, action: str, expected_exit_time: str,
                  extension_hours: int | None = None, stop_loss_price: str | None = None,
                  take_profit_price: str | None = None) -> dict:
    extending = action in {"EXTEND", "EXTEND_4H"}
    if action not in {"SELL_NOW", "EXTEND", "EXTEND_4H"} or not isinstance(lot_id, str) or not lot_id or len(lot_id) > 200:
        raise ManualActionError("invalid manual action")
    if not extending and any(value is not None for value in (extension_hours, stop_loss_price, take_profit_price)):
        raise ManualActionError("sell action cannot include extension settings")
    if extending:
        extension_hours = 4 if extension_hours is None else extension_hours
        if isinstance(extension_hours, bool) or not isinstance(extension_hours, int) or not 1 <= extension_hours <= 168:
            raise ManualActionError("extension hours must be an integer from 1 to 168")
        def valid_price(value: str | None) -> str | None:
            if value is None:
                return None
            if not isinstance(value, str) or len(value) > 40:
                raise ManualActionError("invalid price")
            try:
                price = Decimal(value)
            except InvalidOperation as exc:
                raise ManualActionError("invalid price") from exc
            if not price.is_finite() or price <= 0:
                raise ManualActionError("price must be positive")
            return format(price, "f")
        stop_loss_price = valid_price(stop_loss_price)
        take_profit_price = valid_price(take_profit_price)
    if not isinstance(expected_exit_time, str) or len(expected_exit_time) > 64:
        raise ManualActionError("invalid expected exit time")
    try:
        parsed_exit_time = datetime.fromisoformat(expected_exit_time)
        if parsed_exit_time.tzinfo is None or parsed_exit_time.utcoffset() is None:
            raise ValueError("timezone required")
    except ValueError as exc:
        raise ManualActionError("invalid expected exit time") from exc
    now_time = datetime.now(UTC)
    now = now_time.isoformat()
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
        if extending and (lot["manual_extended_at"] or parsed_exit_time <= now_time):
            raise ManualActionError("extension is unavailable")
        if db.execute("SELECT 1 FROM v2_manual_actions WHERE lot_id=? AND status='PENDING'", (lot_id,)).fetchone():
            raise ManualActionError("an action is already pending for this position")
        action_id = uuid4().hex
        db.execute("""INSERT INTO v2_manual_actions
            (action_id,lot_id,action,expected_exit_time,status,requested_at,
             extension_hours,stop_loss_price,take_profit_price) VALUES (?,?,?,?,?,?,?,?,?)""",
            (action_id, lot_id, action, expected_exit_time, "PENDING", now,
             extension_hours, stop_loss_price, take_profit_price))
        db.commit()
        return {"action_id": action_id, "status": "PENDING"}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
