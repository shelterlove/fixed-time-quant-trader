from __future__ import annotations

from datetime import datetime

import polars as pl

from .config import StrategyConfig, Window


class SignalError(ValueError):
    pass


def _within_window(frame: pl.DataFrame, start: datetime, end_exclusive: datetime) -> pl.DataFrame:
    return frame.filter(
        (pl.col("decision_time") >= pl.lit(start))
        & (pl.col("decision_time") < pl.lit(end_exclusive))
        & (pl.col("planned_exit_time") <= pl.lit(end_exclusive))
    )


def enforce_research_subwindow_exit_boundary(signals: pl.DataFrame, window: Window) -> pl.DataFrame:
    """Keep research candidates inside their frozen discovery/confirmation/H1 subwindow."""
    if window.id != "research" or signals.is_empty():
        return signals
    allowed = [
        (pl.col("decision_time") >= pl.lit(start))
        & (pl.col("decision_time") < pl.lit(end_exclusive))
        & (pl.col("planned_exit_time") <= pl.lit(end_exclusive))
        for start, end_exclusive in window.subwindows
    ]
    return signals.filter(pl.any_horizontal(allowed))


def long_signals(features: pl.DataFrame, start: datetime, end_exclusive: datetime, config: StrategyConfig) -> pl.DataFrame:
    rules = config.values["long"]
    factor_ranks = [f"{factor}_rank" for factor in rules["rank_factors"]]
    rank_filter = pl.all_horizontal([pl.col(name) <= rules["rank_max"] for name in factor_ranks])
    eligible = features.filter(
        pl.col("decision_time").dt.hour().is_in(rules["entry_hours_utc"])
        & rank_filter & (pl.col(rules["market_factor"]) > rules["market_lower_exclusive"])
    ).with_columns(
        pl.sum_horizontal(factor_ranks).alias("priority_score")
    ).sort(["decision_time", "priority_score", *factor_ranks, "symbol"])
    legs = rules["legs"]
    result = eligible.with_columns(
        pl.col("symbol").cum_count().over("decision_time").alias("priority_order"),
        pl.lit("long").alias("strategy"),
        (pl.col("decision_time") + pl.duration(minutes=rules["entry_delay_minutes"])).alias("entry_time"),
        pl.when(pl.col("decision_time").dt.hour() == 14).then(pl.col("decision_time") + pl.duration(hours=legs["14"]["exit_hour_utc"] - 14 + 24 * int(legs["14"]["next_day"]), minutes=rules["entry_delay_minutes"]))
        .when(pl.col("decision_time").dt.hour() == 15).then(pl.col("decision_time") + pl.duration(hours=legs["15"]["exit_hour_utc"] - 15 + 24 * int(legs["15"]["next_day"]), minutes=rules["entry_delay_minutes"]))
        .otherwise(pl.col("decision_time") + pl.duration(hours=legs["17"]["exit_hour_utc"] - 17 + 24 * int(legs["17"]["next_day"]), minutes=rules["entry_delay_minutes"])).alias("planned_exit_time"),
    ).with_columns(
        # Shadow execution is independent of account sizing. The portfolio event
        # loop assigns the frozen single-/two-signal request after de-duplication.
        pl.lit(1).alias("requested_units"),
        pl.concat_str([pl.lit("long:"), pl.col("symbol"), pl.lit(":"), pl.col("decision_time").cast(pl.String)]).alias("trade_id"),
    )
    return _within_window(result, start, end_exclusive)


def select_short_hour(features_at_one_hour: pl.DataFrame, hour: int, rules: dict, top_n: int) -> pl.DataFrame:
    """Selects one hour in isolation; callers must never pass another hour."""
    actual_hours = features_at_one_hour.get_column("decision_time").dt.hour().unique().to_list() if not features_at_one_hour.is_empty() else [hour]
    if any(value != hour for value in actual_hours):
        raise SignalError(f"short {hour:02d}:00 selection received another hour")
    eligible = features_at_one_hour.filter(
        (pl.col("r24_rank") >= rules["r24_rank_min"]) & (pl.col("r24_rank") <= rules["r24_rank_max"])
        & (pl.col("r4_rank_change_rank") >= rules["r4_rank_change_rank_min"]) & (pl.col("r4_rank_change_rank") <= rules["r4_rank_change_rank_max"])
        & (((pl.col("volume_diff_v1_rank") >= rules["volume_diff_rank_min"]) & (pl.col("volume_diff_v1_rank") <= rules["volume_diff_rank_max"]))
           | ((pl.col("volume_diff_v4_rank") >= rules["volume_diff_rank_min"]) & (pl.col("volume_diff_v4_rank") <= rules["volume_diff_rank_max"])))
        & (pl.col(rules["market_factor"]) >= rules["market_lower_inclusive"]) & (pl.col(rules["market_factor"]) <= rules["market_upper_inclusive"])
    ).with_columns(
        pl.min_horizontal("volume_diff_v1_rank", "volume_diff_v4_rank").alias("volume_rank_best")
    ).with_columns(
        (pl.col("r24_rank") + (top_n - pl.col("r4_rank_change_rank")) + pl.col("volume_rank_best")).alias("priority_score")
    ).sort(["decision_time", "priority_score", "r24_rank", "r4_rank_change_rank", "volume_rank_best", "symbol"], descending=[False, False, False, True, False, False])
    return eligible.with_columns(pl.col("symbol").cum_count().over("decision_time").alias("_selection_rank")).filter(pl.col("_selection_rank") <= rules["portfolio"]["max_positions_per_entry_hour"])


def short_signals(features: pl.DataFrame, start: datetime, end_exclusive: datetime, config: StrategyConfig) -> pl.DataFrame:
    rules, top_n = config.values["short"], config.values["universe"]["top_n"]
    first_hour, second_hour = rules["entry_hours_utc"]
    eligible_features = features.filter(
        (pl.col("decision_time") >= pl.lit(start))
        & (pl.col("decision_time") < pl.lit(end_exclusive))
        & pl.col("decision_time").dt.hour().is_in(rules["entry_hours_utc"])
    ).with_columns(
        pl.when(pl.col("decision_time").dt.hour() == first_hour)
        .then(pl.col("decision_time") + pl.duration(hours=rules["legs"][str(first_hour)]["hold_hours"]))
        .otherwise(pl.col("decision_time") + pl.duration(hours=rules["legs"][str(second_hour)]["hold_hours"]))
        .alias("_planned_exit_time")
    ).filter(pl.col("_planned_exit_time") <= pl.lit(end_exclusive))
    day_frames: list[pl.DataFrame] = []
    daily_groups = eligible_features.with_columns(pl.col("decision_time").dt.date().alias("_day")).partition_by(
        "_day", maintain_order=True,
    )
    for daily in daily_groups:
        first = select_short_hour(daily.filter(pl.col("decision_time").dt.hour() == first_hour), first_hour, rules, top_n)
        # 08:00 only sees remaining daily capacity after the frozen 06:00 result.
        remaining = rules["portfolio"]["total_daily_units"] - first.height
        second = select_short_hour(daily.filter(pl.col("decision_time").dt.hour() == second_hour), second_hour, rules, top_n).head(max(remaining, 0))
        day_frames.extend([first, second])
    selected = pl.concat(day_frames, how="diagonal_relaxed") if day_frames else eligible_features.with_columns(
        pl.lit(None, dtype=pl.UInt32).alias("_selection_rank")
    ).head(0)
    if selected.is_empty():
        return selected
    result = selected.with_columns(
        pl.col("_selection_rank").alias("priority_order"),
        pl.lit("short").alias("strategy"), pl.col("decision_time").alias("entry_time"),
        pl.col("_planned_exit_time").alias("planned_exit_time"),
        pl.lit(rules["portfolio"]["units_per_signal"]).alias("requested_units"),
        pl.concat_str([pl.lit("short:"), pl.col("symbol"), pl.lit(":"), pl.col("decision_time").cast(pl.String)]).alias("trade_id"),
    )
    return _within_window(result, start, end_exclusive).drop("_selection_rank", "_planned_exit_time", "_day")
