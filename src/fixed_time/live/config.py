from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import tomllib

from ..config import ConfigError, StrategyConfig, load_config


PUBLIC_FUTURES_URL = "https://fapi.binance.com"
TESTNET_FUTURES_URL = "https://testnet.binancefuture.com"


@dataclass(frozen=True)
class LiveConfig:
    root: Path
    strategy: StrategyConfig
    market_data_base_url: str
    trading_base_url: str
    api_key: str
    api_secret: str
    trading_enabled: bool
    database_path: Path
    account_poll_seconds: int
    decision_deadline_seconds: int
    request_timeout_seconds: int
    max_attempts: int
    max_concurrent_market_requests: int


def _bool(value: str | None, name: str, default: bool | None = None) -> bool:
    if value is None:
        if default is None:
            raise ConfigError(f"{name} is required")
        return default
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ConfigError(f"{name} must be true or false")


def _load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ConfigError(f"{path.name}:{number} must be NAME=value")
        name, value = line.split("=", 1)
        name, value = name.strip(), value.strip()
        if not name or not name.replace("_", "").isalnum():
            raise ConfigError(f"{path.name}:{number} has an invalid variable name")
        values[name] = value.strip("\"'")
    return values


def _env(name: str, dotenv: dict[str, str]) -> str | None:
    return os.environ.get(name, dotenv.get(name))


def load_live_config(root: Path | str = ".") -> LiveConfig:
    root_path = Path(root).resolve()
    strategy = load_config(root_path)
    try:
        with (root_path / "testnet.toml").open("rb") as handle:
            values = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read testnet.toml: {exc}") from exc
    expected = {"environment", "account", "runtime"}
    if set(values) != expected:
        raise ConfigError("testnet.toml must contain environment, account, and runtime")
    environment, account, runtime = values["environment"], values["account"], values["runtime"]
    if environment != {
        "market_data_base_url": PUBLIC_FUTURES_URL,
        "trading_base_url": TESTNET_FUTURES_URL,
        "trading_environment": "testnet",
    }:
        raise ConfigError("testnet environment URLs are fixed; signed production trading is not supported")
    if account != {"position_mode": "hedge", "margin_type": "isolated", "leverage": 1, "single_asset_mode": True}:
        raise ConfigError("only hedge, isolated, single-asset, 1x testnet execution is supported")
    required_runtime = {"account_poll_seconds", "decision_deadline_seconds", "request_timeout_seconds", "max_attempts", "max_concurrent_market_requests"}
    if set(runtime) != required_runtime or not all(isinstance(runtime[name], int) and runtime[name] > 0 for name in required_runtime):
        raise ConfigError("runtime settings must be positive integers")
    dotenv = _load_dotenv(root_path / ".env")
    key, secret = _env("BINANCE_TESTNET_API_KEY", dotenv), _env("BINANCE_TESTNET_API_SECRET", dotenv)
    enabled = _bool(_env("TRADING_ENABLED", dotenv), "TRADING_ENABLED", default=False)
    database = _env("DATABASE_PATH", dotenv) or "runtime/testnet.sqlite3"
    poll = _env("ACCOUNT_POLL_SECONDS", dotenv)
    poll_seconds = int(poll) if poll is not None else runtime["account_poll_seconds"]
    if poll_seconds <= 0:
        raise ConfigError("ACCOUNT_POLL_SECONDS must be positive")
    if enabled and (not key or not secret):
        raise ConfigError("TRADING_ENABLED requires Binance testnet API credentials")
    return LiveConfig(
        root=root_path,
        strategy=strategy,
        market_data_base_url=PUBLIC_FUTURES_URL,
        trading_base_url=TESTNET_FUTURES_URL,
        api_key=key or "",
        api_secret=secret or "",
        trading_enabled=enabled,
        database_path=(root_path / database).resolve(),
        account_poll_seconds=poll_seconds,
        decision_deadline_seconds=runtime["decision_deadline_seconds"],
        request_timeout_seconds=runtime["request_timeout_seconds"],
        max_attempts=runtime["max_attempts"],
        max_concurrent_market_requests=runtime["max_concurrent_market_requests"],
    )
