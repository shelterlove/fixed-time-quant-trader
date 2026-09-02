from datetime import UTC, datetime, timedelta

from fixed_time.config import load_config
from research.exit_protection_extension.experiment import extend_recently_activated_trade, extension_long_eviction_order


CONFIG = load_config()


def _bar(time, open_, high, low, close):
    return {"open_time": time, "open": open_, "high": high, "low": low, "close": close}


def _row(entry, planned):
    return {
        "trade_id": "long:A", "entry_time": entry, "planned_exit_time": planned,
        "entry_reference": 100.0, "exit_reason": "PLANNED_EXIT", "shadow_activated": True,
        "protection_allowed_retrace": 0.10,
    }


def test_recent_activation_extends_then_uses_existing_protection() -> None:
    entry = datetime(2022, 1, 1, 14, 1, tzinfo=UTC)
    planned = entry + timedelta(minutes=3)
    bars = [
        _bar(entry, 100.0, 130.0, 100.0, 125.0),
        _bar(entry + timedelta(minutes=1), 125.0, 130.0, 124.0, 128.0),
        _bar(entry + timedelta(minutes=2), 128.0, 130.0, 127.0, 129.0),
        _bar(planned, 120.0, 121.0, 110.0, 112.0),
        _bar(planned + timedelta(minutes=1), 112.0, 112.0, 112.0, 112.0),
        _bar(planned + timedelta(minutes=2), 112.0, 112.0, 112.0, 112.0),
        _bar(planned + timedelta(minutes=3), 112.0, 112.0, 112.0, 112.0),
    ]
    outcome = extend_recently_activated_trade(_row(entry, planned), bars, CONFIG.values["long"], timedelta(hours=4), timedelta(minutes=4))
    assert outcome is not None
    assert outcome.activated_at == entry + timedelta(minutes=1)
    assert outcome.exit_reason == "PROTECTION"
    assert outcome.exit_time == planned + timedelta(minutes=1)
    assert outcome.exit_reference == 117.0


def test_old_activation_does_not_extend_at_planned_exit() -> None:
    entry = datetime(2022, 1, 1, 0, 1, tzinfo=UTC)
    planned = entry + timedelta(hours=5)
    bars = []
    for offset in range(9 * 60):
        time = entry + timedelta(minutes=offset)
        bars.append(_bar(time, 130.0, 130.0, 130.0, 130.0))
    bars[0] = _bar(entry, 100.0, 130.0, 100.0, 130.0)
    outcome = extend_recently_activated_trade(_row(entry, planned), bars, CONFIG.values["long"], timedelta(hours=4), timedelta(hours=4))
    assert outcome is None


def test_only_post_four_hour_extensions_are_eligible_for_secondary_eviction() -> None:
    event = datetime(2022, 1, 2, 12, tzinfo=UTC)
    positions = {
        ("long", "RECENT"): {"strategy": "long", "symbol": "RECENT", "entry_time": event - timedelta(hours=10), "extension_applied": True,
                              "extension_release_time": event - timedelta(minutes=1)},
        ("long", "OLDER"): {"strategy": "long", "symbol": "OLDER", "entry_time": event - timedelta(hours=12), "extension_applied": True,
                             "extension_release_time": event - timedelta(hours=2)},
        ("long", "NOT_EXTENDED"): {"strategy": "long", "symbol": "NOT_EXTENDED", "entry_time": event - timedelta(hours=12), "extension_applied": False,
                                    "extension_release_time": None},
    }
    assert [item["symbol"] for item in extension_long_eviction_order(positions, event)] == ["OLDER", "RECENT"]
