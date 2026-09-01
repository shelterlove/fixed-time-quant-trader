from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from fixed_time.live.dashboard import read_status
from fixed_time.live.state import StateStore


def test_dashboard_reads_runtime_positions_and_decisions(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "runtime.sqlite3")
    now = datetime(2026, 9, 1, 14, tzinfo=UTC).isoformat()
    store.update_runtime_status("1.3.0", now, "100", 0, 0, reconciled=True)
    store.start_decision(now)
    store.finish_decision(now, 10, [{"symbol": "AAAUSDT"}], [{"symbol": "AAAUSDT", "units": 1}])
    store.close()

    status = read_status(tmp_path / "runtime.sqlite3")
    assert status["runtime"]["available_usdt"] == "100"
    assert status["positions"] == []
    assert status["decisions"][0]["detail"]["candidates"][0]["symbol"] == "AAAUSDT"
