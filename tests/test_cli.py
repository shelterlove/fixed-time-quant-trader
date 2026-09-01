import sys

import pytest

from fixed_time.cli import _parser, main
from fixed_time.config import ConfigError


def test_run_cannot_open_external_window(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["fixed-time", "run", "--window", "external_2021", "--offline"])
    with pytest.raises(ConfigError, match="run only accepts"):
        main()


def test_validate_cannot_open_forward_window(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["fixed-time", "validate", "--window", "forward_2026_jul_aug"])
    with pytest.raises(ConfigError, match="validate only accepts"):
        main()


def test_forward_requires_explicit_confirmation() -> None:
    parser = _parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["forward", "--window", "forward_2026_jul_aug"])
    assert parser.parse_args(["forward", "--window", "forward_2026_jul_aug", "--confirm"]).confirm is True


def test_resume_requires_explicit_offline_mode() -> None:
    parser = _parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["resume", "--window", "research"])
    assert parser.parse_args(["resume", "--window", "research", "--offline"]).offline is True
