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
