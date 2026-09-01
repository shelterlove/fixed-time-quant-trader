from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import tomllib
from typing import Any


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Window:
    id: str
    start: datetime
    end_exclusive: datetime
    protection_history_start: datetime | None = None
    subwindows: tuple[tuple[datetime, datetime], ...] = ()


@dataclass(frozen=True)
class StrategyConfig:
    root: Path
    values: dict[str, Any]
    windows: dict[str, Window]

    @property
    def version(self) -> str:
        return self.values["strategy_version"]

    def window(self, window_id: str) -> Window:
        try:
            return self.windows[window_id]
        except KeyError as exc:
            raise ConfigError(f"unknown window: {window_id}") from exc


_SHAPE = {
    "schema_version": None, "strategy_version": None, "status": None, "timezone": None,
    "execution": {"terminal_data_path_exit": None},
    "data": {"archive_base_url": None, "archive_listing_url": None, "futures_api_base_url": None,
             "max_concurrent_requests": None, "max_attempts_per_object": None,
             "keep_downloaded_zip": None, "download_checksum_files": None},
    "universe": {"venue": None, "quote_asset": None, "top_n": None, "liquidity_factor": None,
                 "rank_method": None, "rank_tie_break": None},
    "features": {"strategy_decision_hours_utc": None, "rank_history_hours_utc": None,
                 "hourly_warmup_hours": None, "return_horizons": None, "volume_horizons": None,
                 "market_quantile_interpolation": None},
    "long": {"entry_hours_utc": None, "entry_delay_minutes": None, "rank_max": None,
             "rank_factors": None, "market_factor": None, "market_lower_exclusive": None,
             "hard_stop_return": None, "slippage_per_side": None, "taker_fee_per_side": None,
             "funding_boundary": None, "funding_price_proxy": None,
             "legs": {"14": {"exit_hour_utc": None, "next_day": None},
                      "15": {"exit_hour_utc": None, "next_day": None},
                      "17": {"exit_hour_utc": None, "next_day": None}},
             "protection": {"method": None, "activation_return": None, "window_days": None,
                            "retrace_quantile": None, "minimum_history": None,
                            "minimum_activated_history": None, "fallback_retrace": None,
                            "break_even_floor": None, "history_source": None},
             "portfolio": {"total_units": None, "max_positions_per_entry_time": None,
                           "single_signal_units": None, "two_signal_units_each": None,
                           "same_strategy_open_symbol": None}},
    "short": {"entry_hours_utc": None, "r24_rank_min": None, "r24_rank_max": None,
              "r4_rank_change_rank_min": None, "r4_rank_change_rank_max": None,
              "volume_diff_rank_min": None, "volume_diff_rank_max": None, "volume_diff_logic": None,
              "market_factor": None, "market_lower_inclusive": None, "market_upper_inclusive": None,
              "hard_stop_return": None, "stop_trigger": None, "gap_fill": None,
              "round_trip_stress_cost": None, "funding_model": None,
              "legs": {"6": {"exit_hour_utc": None, "hold_hours": None},
                       "8": {"exit_hour_utc": None, "hold_hours": None}},
              "portfolio": {"selection": None, "total_daily_units": None,
                            "max_positions_per_entry_hour": None, "units_per_signal": None,
                            "same_symbol_same_day": None}},
    "portfolio": {"mode": None, "total_units": None, "long_unit_cap": None, "short_unit_cap": None,
                  "unit_value": None, "long_priority": None, "short_no_funds": None,
                  "short_eviction_order": None, "short_eviction_price": None,
                  "same_strategy_open_symbol": None, "cross_strategy_same_symbol": None,
                  "same_timestamp_order": None},
    "windows": {"research": {"start": None, "end_exclusive": None, "subwindows": None},
                "external_2021": {"start": None, "end_exclusive": None, "protection_history_start": None},
                "forward_2026_jul_aug": {"start": None, "end_exclusive": None},
                "reserved_forward": {"start": None, "authorized": None}},
}


def _check_shape(actual: dict[str, Any], expected: dict[str, Any], where: str = "") -> None:
    unknown, missing = set(actual) - set(expected), set(expected) - set(actual)
    if unknown or missing:
        raise ConfigError(f"{where or 'root'} keys: missing={sorted(missing)}, unknown={sorted(unknown)}")
    for name, nested in expected.items():
        if nested is not None:
            if not isinstance(actual[name], dict):
                raise ConfigError(f"{where}{name} must be a table")
            _check_shape(actual[name], nested, f"{where}{name}.")


def _utc(value: str, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ConfigError(f"{name} is not ISO-8601: {value}") from exc
    if parsed.tzinfo != UTC:
        raise ConfigError(f"{name} must use explicit UTC (+00:00)")
    return parsed


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ConfigError(message)


def _validate(values: dict[str, Any]) -> dict[str, Window]:
    _check_shape(values, _SHAPE)
    _require(values["schema_version"] == 1, "unsupported schema_version")
    _require(isinstance(values["strategy_version"], str) and values["strategy_version"], "strategy_version is required")
    _require(values["status"] == "frozen", "strategy status must be frozen")
    _require(values["timezone"] == "UTC", "timezone must be UTC")
    _require(values["execution"]["terminal_data_path_exit"] == "last_completed_bar_close", "unsupported terminal data-path exit rule")
    data, universe, features = values["data"], values["universe"], values["features"]
    _require(data["max_concurrent_requests"] > 0 and data["max_attempts_per_object"] > 0, "download limits must be positive")
    _require(data["keep_downloaded_zip"] is False and data["download_checksum_files"] is False, "ZIP retention and checksum downloads must remain disabled")
    _require(universe["top_n"] == 100, "only the frozen Top100 universe is supported")
    _require(universe["liquidity_factor"] == "v24" and universe["rank_method"] == "descending_ordinal" and universe["rank_tie_break"] == "symbol_asc", "unsupported universe ranking semantics")
    _require(features["market_quantile_interpolation"] == "nearest", "unsupported market quantile interpolation")
    _require(features["return_horizons"] == [1, 4, 24] and features["volume_horizons"] == [1, 4, 24], "unsupported feature horizons")
    for name in ("strategy_decision_hours_utc", "rank_history_hours_utc"):
        hours = features[name]
        _require(all(isinstance(hour, int) and 0 <= hour < 24 for hour in hours), f"invalid {name}")
        _require(len(set(hours)) == len(hours), f"duplicate {name}")
    _require(features["hourly_warmup_hours"] == 28, "hourly warm-up must remain 28 hours")
    long, short, portfolio = values["long"], values["short"], values["portfolio"]
    for name, hours in (("long.entry_hours_utc", long["entry_hours_utc"]), ("short.entry_hours_utc", short["entry_hours_utc"])):
        _require(all(isinstance(hour, int) and 0 <= hour < 24 for hour in hours), f"invalid {name}")
        _require(len(set(hours)) == len(hours), f"duplicate {name}")
    _require(long["entry_hours_utc"] == [14, 15, 17] and short["entry_hours_utc"] == [6, 8], "unsupported entry-hour layout")
    _require(features["strategy_decision_hours_utc"] == [6, 8, 14, 15, 17] and features["rank_history_hours_utc"] == [2, 4], "unsupported decision/history-hour layout")
    _require(long["rank_factors"] == ["r1", "r4", "r24", "v1", "v4"], "unsupported long rank factors")
    _require(all(1 <= value <= universe["top_n"] for value in [
        long["rank_max"], short["r24_rank_min"], short["r24_rank_max"],
        short["r4_rank_change_rank_min"], short["r4_rank_change_rank_max"],
        short["volume_diff_rank_min"], short["volume_diff_rank_max"],
    ]), "rank range exceeds Top100")
    _require(short["r24_rank_min"] <= short["r24_rank_max"] and short["r4_rank_change_rank_min"] <= short["r4_rank_change_rank_max"] and short["volume_diff_rank_min"] <= short["volume_diff_rank_max"], "invalid short rank range")
    _require(-1 < long["hard_stop_return"] < 0 and long["slippage_per_side"] >= 0 and long["taker_fee_per_side"] >= 0, "invalid long execution parameter")
    _require(short["hard_stop_return"] > 0 and short["round_trip_stress_cost"] >= 0, "invalid short execution parameter")
    protection = long["protection"]
    _require(0 < protection["activation_return"] and 0 <= protection["retrace_quantile"] <= 1 and protection["window_days"] > 0 and protection["minimum_history"] >= protection["minimum_activated_history"] > 0, "invalid protection parameter")
    _require(long["funding_boundary"] == "entry_exclusive_exit_inclusive" and long["funding_price_proxy"] == "settlement_minute_open", "unsupported long funding semantics")
    _require(protection["method"] == "ROLLING_P90_BREAKEVEN" and protection["break_even_floor"] is True and protection["history_source"] == "all_completed_base_shadow_candidates", "unsupported long protection semantics")
    _require(short["volume_diff_logic"] == "OR" and short["stop_trigger"] == "hourly_high" and short["gap_fill"] == "max_hourly_open_or_stop" and short["funding_model"] == "none_in_v1_reproduction", "unsupported short execution semantics")
    for hour in long["entry_hours_utc"]:
        leg = long["legs"][str(hour)]
        _require(isinstance(leg["next_day"], bool) and 0 <= leg["exit_hour_utc"] < 24, f"invalid long leg {hour}")
        hold_hours = leg["exit_hour_utc"] - hour + 24 * int(leg["next_day"])
        _require(hold_hours > 0, f"long leg {hour} does not exit after entry")
    for hour in short["entry_hours_utc"]:
        leg = short["legs"][str(hour)]
        _require(leg["hold_hours"] > 0 and (hour + leg["hold_hours"]) % 24 == leg["exit_hour_utc"], f"short leg {hour} hold/exit mismatch")
    _require(portfolio["total_units"] > 0 and 0 < portfolio["short_unit_cap"] <= portfolio["total_units"] and 0 < portfolio["long_unit_cap"] <= portfolio["total_units"], "invalid portfolio capacity")
    _require(0 < long["portfolio"]["max_positions_per_entry_time"] <= long["portfolio"]["total_units"], "invalid long entry-slot capacity")
    _require(0 < long["portfolio"]["two_signal_units_each"] <= long["portfolio"]["single_signal_units"] <= long["portfolio"]["total_units"], "invalid long requested units")
    _require(0 < short["portfolio"]["max_positions_per_entry_hour"] <= short["portfolio"]["total_daily_units"], "invalid short entry-hour capacity")
    _require(short["portfolio"]["units_per_signal"] == 1, "short units_per_signal must remain 1")
    _require(long["portfolio"]["total_units"] == portfolio["total_units"] == portfolio["long_unit_cap"], "long/combined unit capacities disagree")
    _require(short["portfolio"]["total_daily_units"] == portfolio["short_unit_cap"], "short/combined unit capacities disagree")
    _require(short["portfolio"]["selection"] == "SEQUENTIAL_06_THEN_08" and short["portfolio"]["same_symbol_same_day"] == "allow_in_standalone", "unsupported short portfolio semantics")
    _require(long["portfolio"]["same_strategy_open_symbol"] == "skip", "unsupported long duplicate semantics")
    _require(
        portfolio["mode"] == "LONG_PRIORITY_SKIP"
        and portfolio["unit_value"] == "free_cash_divided_by_free_units"
        and portfolio["long_priority"] is True
        and portfolio["short_no_funds"] == "skip"
        and portfolio["short_eviction_order"] == "worst_priority_then_signal_time_then_symbol"
        and portfolio["short_eviction_price"] == "latest_completed_hourly_close"
        and portfolio["same_strategy_open_symbol"] == "skip"
        and portfolio["cross_strategy_same_symbol"] == "allow"
        and portfolio["same_timestamp_order"] == "exits_evictions_long_short",
        "unsupported combined portfolio semantics",
    )
    windows: dict[str, Window] = {}
    for name, raw in values["windows"].items():
        if name == "reserved_forward":
            _require(raw["authorized"] is False, "reserved window must remain closed")
            continue
        start, end = _utc(raw["start"], f"windows.{name}.start"), _utc(raw["end_exclusive"], f"windows.{name}.end_exclusive")
        _require(start < end, f"windows.{name} is empty")
        history = _utc(raw["protection_history_start"], f"windows.{name}.protection_history_start") if "protection_history_start" in raw else None
        if history is not None:
            _require(history < start, "protection history must precede window")
        subwindows: tuple[tuple[datetime, datetime], ...] = ()
        if name == "research":
            raw_subwindows = raw["subwindows"]
            _require(isinstance(raw_subwindows, list) and raw_subwindows, "research subwindows are required")
            parsed_subwindows: list[tuple[datetime, datetime]] = []
            for index, item in enumerate(raw_subwindows):
                _require(isinstance(item, dict), f"invalid research subwindow {index}")
                _require(set(item) == {"start", "end_exclusive"}, f"invalid research subwindow {index}")
                sub_start = _utc(item["start"], f"windows.research.subwindows[{index}].start")
                sub_end = _utc(item["end_exclusive"], f"windows.research.subwindows[{index}].end_exclusive")
                _require(sub_start < sub_end, f"research subwindow {index} is empty")
                parsed_subwindows.append((sub_start, sub_end))
            _require(parsed_subwindows[0][0] == start and parsed_subwindows[-1][1] == end, "research subwindows must cover the research window")
            _require(all(left[1] == right[0] for left, right in zip(parsed_subwindows, parsed_subwindows[1:])), "research subwindows must be contiguous")
            subwindows = tuple(parsed_subwindows)
        windows[name] = Window(name, start, end, history, subwindows)
    return windows


def load_config(root: Path | str = ".") -> StrategyConfig:
    root_path = Path(root).resolve()
    try:
        with (root_path / "strategy.toml").open("rb") as handle:
            values = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read strategy.toml: {exc}") from exc
    return StrategyConfig(root_path, values, _validate(values))
