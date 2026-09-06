from __future__ import annotations

from datetime import datetime, timedelta
import json
from typing import Any


def shadow_start(task: dict[str, Any]) -> datetime:
    progress = json.loads(task["progress_json"]) if task.get("progress_json") else None
    return datetime.fromisoformat(progress["next_time"]) if progress else datetime.fromisoformat(task["entry_time"]) - timedelta(minutes=1)


def advance_shadow(task: dict[str, Any], bars: list[dict[str, Any]], end: datetime, rules: dict) -> tuple[dict, str | None]:
    """Consume each completed public minute once, with the frozen base rules."""
    entry = datetime.fromisoformat(task["entry_time"])
    planned = datetime.fromisoformat(task["planned_exit_time"])
    state = json.loads(task["progress_json"]) if task.get("progress_json") else {
        "next_time": (entry - timedelta(minutes=1)).isoformat(), "active": False, "maximum": None,
    }
    next_time = datetime.fromisoformat(state["next_time"])
    end = min(end, planned)
    for bar in bars:
        when = bar["open_time"]
        if when < next_time:
            continue
        if when >= end:
            break
        if when != next_time:
            return state, f"missing shadow minute: {next_time.isoformat()}"
        next_time += timedelta(minutes=1)
        state["next_time"] = next_time.isoformat()
        if when < entry:
            state["reference"] = state["peak"] = float(bar["close"])
            continue
        low, high, reference = float(bar["low"]), float(bar["high"]), state["reference"]
        if low <= reference * (1 + rules["hard_stop_return"]):
            state["exit_time"] = next_time.isoformat()
            return state, None
        if state["active"]:
            retrace = max(0.0, (state["peak"] - low) / state["peak"])
            state["maximum"] = retrace if state["maximum"] is None else max(state["maximum"], retrace)
            state["peak"] = max(state["peak"], high)
        elif high >= reference * (1 + rules["protection"]["activation_return"]):
            state["active"], state["peak"] = True, max(state["peak"], high)
    if next_time == planned:
        state["exit_time"] = planned.isoformat()
    return state, None if next_time >= end else f"missing shadow minute: {next_time.isoformat()}"
