from datetime import UTC, datetime

import pytest

from fixed_time.config import ConfigError, load_config


def test_research_subwindows_are_contiguous_and_cover_the_window() -> None:
    config = load_config()
    research = config.window("research")
    assert research.subwindows == (
        (datetime(2022, 1, 1, tzinfo=UTC), datetime(2025, 1, 1, tzinfo=UTC)),
        (datetime(2025, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
        (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 7, 1, tzinfo=UTC)),
    )


def test_research_subwindows_reject_a_gap(tmp_path) -> None:
    source = (load_config().root / "strategy.toml").read_text(encoding="utf-8")
    broken = source.replace('end_exclusive = "2025-01-01T00:00:00+00:00" },', 'end_exclusive = "2024-12-31T00:00:00+00:00" },', 1)
    (tmp_path / "strategy.toml").write_text(broken, encoding="utf-8")
    with pytest.raises(ConfigError, match="contiguous"):
        load_config(tmp_path)


@pytest.mark.parametrize(
    ("needle", "replacement", "message"),
    [
        ("single_signal_units = 2", "single_signal_units = 1", "long three-unit allocation"),
        ("{ threshold = 0.25, multiplier = 1.05 }", "{ threshold = 0.24, multiplier = 1.05 }", "drawdown sizing"),
    ],
)
def test_three_unit_and_drawdown_rules_are_frozen(tmp_path, needle, replacement, message) -> None:
    source = (load_config().root / "strategy.toml").read_text(encoding="utf-8")
    (tmp_path / "strategy.toml").write_text(source.replace(needle, replacement, 1), encoding="utf-8")
    with pytest.raises(ConfigError, match=message):
        load_config(tmp_path)
