from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import tomllib
from typing import Any


PUBLIC_FUTURES_URL = "https://fapi.binance.com"
TESTNET_FUTURES_URL = "https://demo-fapi.binance.com"


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class StrategyConfig:
    root: Path
    values: dict[str, Any]

    @property
    def version(self) -> str:
        return str(self.values["strategy_version"])


@dataclass(frozen=True)
class LiveConfig:
    root: Path
    strategy: StrategyConfig
    api_key: str
    api_secret: str
    trading_enabled: bool
    database_path: Path
    account_poll_seconds: int
    reconcile_seconds: int
    decision_delay_seconds: int
    decision_deadline_seconds: int
    request_timeout_seconds: int
    max_attempts: int
    max_market_workers: int
    leverage: int
    market_data_base_url: str = PUBLIC_FUTURES_URL
    trading_base_url: str = TESTNET_FUTURES_URL


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ConfigError(message)


def _keys(table: dict[str, Any], expected: set[str], name: str) -> None:
    missing, unknown = expected - set(table), set(table) - expected
    _require(not missing and not unknown, f"{name} keys: missing={sorted(missing)}, unknown={sorted(unknown)}")


def _validate_strategy(v: dict[str, Any]) -> None:
    _keys(v, {"schema_version", "strategy_version", "status", "timezone", "execution", "universe", "features", "long", "short", "allocation"}, "root")
    _require(v["schema_version"] == 2 and v["strategy_version"] == "SPEC-20260917-r2", "unsupported strategy version")
    _require(v["status"] == "production" and v["timezone"] == "UTC", "strategy must be production UTC")
    _require(v["execution"] == {"slippage_per_side": .001, "taker_fee_per_side": .0005, "entry_delay_minutes": 1}, "execution costs changed")
    _require(v["universe"] == {"venue": "BINANCE_UM", "quote_asset": "USDT", "top_n": 100,
                               "rank_method": "descending_ordinal", "rank_tie_break": "symbol_asc"}, "universe changed")
    _require(v["features"] == {"strategy_decision_hours_utc": [0, 1, 2, 6, 14, 15, 17],
                               "hourly_warmup_hours": 80, "market_quantile_interpolation": "nearest"}, "feature clock changed")
    _require(v["long"] == {
        "entry_hours_utc": [14, 15, 17], "hard_stop_return": -.30,
        "legs": {"14": {"exit_hour_utc": 8}, "15": {"exit_hour_utc": 8}, "17": {"exit_hour_utc": 4}},
        "extension": {"enabled": True, "activation_return": .30, "activation_lookback_hours": 4, "maximum_extension_hours": 24},
        "profit_guard": {"arm_return": 3.0, "floor_return": 2.7, "cap_return": 4.0},
    }, "long rules changed")
    _require(v["short"] == {"entry_hours_utc": [0, 1, 2, 6], "hard_stop_return": .30,
                            "new_exit_local_hour": 10, "new_exit_timezone": "America/New_York",
                            "original_exit_hour_utc": 20}, "short rules changed")
    _require(v["allocation"] == {"direction_budget_fraction": .5, "cross_direction_borrowing": False,
                                 "drawdown_leverage": False, "eviction": False}, "allocation rules changed")


def load_strategy(root: Path | str = ".") -> StrategyConfig:
    root_path = Path(root).resolve()
    try:
        with (root_path / "strategy.toml").open("rb") as handle:
            values = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read strategy.toml: {exc}") from exc
    _validate_strategy(values)
    return StrategyConfig(root_path, values)


def _dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    result: dict[str, str] = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ConfigError(f"{path.name}:{number} must be NAME=value")
        name, value = line.split("=", 1)
        result[name.strip()] = value.strip().strip("\"'")
    return result


def load_live_config(root: Path | str = ".") -> LiveConfig:
    root_path = Path(root).resolve()
    strategy = load_strategy(root_path)
    try:
        with (root_path / "testnet.toml").open("rb") as handle:
            values = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read testnet.toml: {exc}") from exc
    _keys(values, {"environment", "account", "runtime"}, "testnet")
    _require(values["environment"] == {"market_data_base_url": PUBLIC_FUTURES_URL,
                                       "trading_base_url": TESTNET_FUTURES_URL,
                                       "trading_environment": "testnet"}, "only Binance Futures testnet trading is supported")
    account = values["account"]
    _require(account == {"position_mode": "hedge", "margin_type": "isolated", "leverage": 2, "single_asset_mode": True}, "account must be isolated Hedge Mode at 2x")
    runtime = values["runtime"]
    expected = {"account_poll_seconds", "reconcile_seconds", "decision_delay_seconds", "decision_deadline_seconds",
                "request_timeout_seconds", "max_attempts", "max_market_workers"}
    _keys(runtime, expected, "runtime")
    _require(all(isinstance(runtime[k], int) and runtime[k] > 0 for k in expected), "runtime values must be positive integers")
    _require(runtime["decision_delay_seconds"] < runtime["decision_deadline_seconds"], "decision deadline must follow delay")
    env_file = _dotenv(root_path / ".env")
    get = lambda name, default=None: os.environ.get(name, env_file.get(name, default))
    flag = str(get("TRADING_ENABLED", "false")).strip().lower()
    _require(flag in {"1", "true", "yes", "0", "false", "no"}, "invalid TRADING_ENABLED boolean")
    enabled = flag in {"1", "true", "yes"}
    key, secret = str(get("BINANCE_TESTNET_API_KEY", "")), str(get("BINANCE_TESTNET_API_SECRET", ""))
    _require(not enabled or (key and secret), "TRADING_ENABLED requires testnet credentials")
    database = Path(str(get("DATABASE_PATH", "runtime/testnet.sqlite3")))
    if not database.is_absolute():
        database = root_path / database
    poll_seconds = int(str(get("ACCOUNT_POLL_SECONDS", runtime["account_poll_seconds"])))
    _require(poll_seconds > 0, "ACCOUNT_POLL_SECONDS must be positive")
    selected_runtime = dict(runtime, account_poll_seconds=poll_seconds)
    return LiveConfig(root_path, strategy, key, secret, enabled, database.resolve(), leverage=2, **selected_runtime)


# Kept as the public loader name used by a few deployment checks.
load_config = load_strategy
