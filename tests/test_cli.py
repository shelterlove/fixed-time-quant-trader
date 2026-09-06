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


def test_live_smoke_requires_explicit_symbol() -> None:
    parser = _parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["live-smoke"])
    assert parser.parse_args(["live-smoke", "--symbol", "BTCUSDT"]).symbol == "BTCUSDT"


def test_live_dashboard_defaults() -> None:
    parser = _parser()
    args = parser.parse_args(["live-dashboard"])
    assert (args.host, args.port) == ("0.0.0.0", 8080)


@pytest.mark.parametrize("command", ["live-check", "live-health"])
def test_live_read_commands_do_not_open_a_writable_state_store(monkeypatch, tmp_path, command):
    from datetime import UTC, datetime
    from fixed_time.live.state import StateStore
    from test_live import _config

    config = _config(tmp_path)
    store = StateStore(config.database_path)
    store.update_runtime_status("test", datetime.now(UTC).isoformat(), "100", 0, 0)
    store.close()
    monkeypatch.setattr("fixed_time.live.config.load_live_config", lambda root: config)
    monkeypatch.setattr("fixed_time.live.binance.BinanceRest.account_check", lambda self: {"positions": [], "open_orders": [], "open_algo_orders": []})
    def forbidden(*args, **kwargs):
        raise AssertionError("read command opened writable state")
    monkeypatch.setattr(StateStore, "__init__", forbidden)
    monkeypatch.setattr(sys, "argv", ["fixed-time", command])
    main()
