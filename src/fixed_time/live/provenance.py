"""Non-secret, immutable deployment metadata for audit and recovery."""
from functools import lru_cache
import os
from pathlib import Path
import subprocess

from .. import __version__
from .config import LiveConfig


@lru_cache(maxsize=8)
def revision(root: Path) -> str:
    packaged = os.environ.get("SOURCE_REVISION")
    if packaged and packaged != "unknown":
        return packaged
    try:
        commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True,
                                text=True, timeout=5, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=root,
                               capture_output=True, text=True, timeout=5, check=True).stdout.strip()
        return commit + ("-dirty" if dirty else "")
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def deployment_snapshot(config: LiveConfig) -> dict:
    # Explicit whitelist: never serialize LiveConfig/API keys or the environment.
    return {
        "code_version": __version__, "revision": revision(config.root),
        "strategy_version": config.strategy.version, "strategy": config.strategy.values,
        "environment": {"market_data_base_url": config.market_data_base_url,
                        "trading_base_url": config.trading_base_url},
        "account": {"position_mode": "hedge", "margin_type": "isolated", "leverage": config.leverage,
                    "single_asset_mode": True},
        "long_extension": {name: getattr(config.long_extension, name) for name in (
            "enabled", "activation_lookback_hours", "maximum_extension_hours", "evict_after_hours")},
        "runtime": {name: getattr(config, name) for name in (
            "account_poll_seconds", "idle_reconcile_seconds", "decision_deadline_seconds",
            "request_timeout_seconds", "max_attempts", "max_concurrent_market_requests")},
    }
