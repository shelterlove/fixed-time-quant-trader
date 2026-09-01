from __future__ import annotations

from datetime import timedelta

import polars as pl

from .config import StrategyConfig


class FeatureError(ValueError):
    pass


def _rank(frame: pl.DataFrame, value: str, output: str) -> pl.DataFrame:
    ordered = frame.sort(["decision_time", value, "symbol"], descending=[False, True, False])
    return ordered.with_columns(pl.col("symbol").cum_count().over("decision_time").alias(output))


def _top(frame: pl.DataFrame, top_n: int) -> pl.DataFrame:
    ranked = _rank(frame, "v24", "qv_rank").filter(pl.col("qv_rank") <= top_n)
    return ranked.with_columns(pl.len().over("decision_time").alias("universe_size"))


def _strategy_ranks(frame: pl.DataFrame, rank_factors: list[str]) -> pl.DataFrame:
    result = frame
    for factor in rank_factors:
        result = _rank(result, factor, f"{factor}_rank")
    return result.with_columns(
        pl.col("r1").quantile(0.1, interpolation="nearest").over("decision_time").alias("market_r1_p10"),
        pl.col("r24").quantile(0.1, interpolation="nearest").over("decision_time").alias("market_r24_p10"),
    )


def build_features(hourly: pl.DataFrame, config: StrategyConfig) -> pl.DataFrame:
    """Builds all cross-sectional fields from one in-memory hourly table.

    Historical 02/04 rows deliberately compute only v24 and r4 rank.  They are
    used solely for the 06/08 r4-rank change, keeping the frozen 28-hour warm-up.
    """
    rules, universe = config.values["features"], config.values["universe"]
    decision_hours, history_hours, top_n = rules["strategy_decision_hours_utc"], rules["rank_history_hours_utc"], universe["top_n"]
    h1, h4, h24 = rules["return_horizons"]
    required = {"symbol", "open_time", "open", "high", "low", "close", "quote_volume", "trade_count"}
    if set(hourly.columns) != required:
        raise FeatureError("hourly input has an invalid schema")
    source = hourly.sort(["symbol", "open_time"]).with_columns(
        pl.col("close").shift(h1).over("symbol").alias("_close_1"),
        pl.col("close").shift(h4).over("symbol").alias("_close_4"),
        pl.col("close").shift(h24).over("symbol").alias("_close_24"),
        pl.col("open_time").shift(h24).over("symbol").alias("_current_window_start"),
        pl.col("open_time").shift(h24 - 1).over("symbol").alias("_history_window_start"),
        pl.col("quote_volume").rolling_sum(h4).over("symbol").alias("v4"),
        pl.col("quote_volume").rolling_sum(h24).over("symbol").alias("v24"),
        pl.col("quote_volume").shift(1).over("symbol").alias("_v1_prev"),
        pl.col("quote_volume").rolling_sum(h4).shift(h4).over("symbol").alias("_v4_prev"),
        (pl.col("open_time") + pl.duration(hours=1)).alias("decision_time"),
        pl.col("open_time").alias("source_bar_time"),
    )
    # Dynamic membership is decided entirely at T from the latest completed
    # source bar (T-1h).  A suspended contract must not retain a Top100 slot
    # merely because its trailing 24-hour quote volume is still large.
    active_at_source = (pl.col("quote_volume") > 0) & (pl.col("trade_count") > 0)
    current = source.filter(
        pl.col("decision_time").dt.hour().is_in(decision_hours)
        # Current factors need exactly T-25h through T-1h: 25 bars and 24
        # one-hour intervals.  Do not inspect T-26h.
        & ((pl.col("open_time") - pl.col("_current_window_start")) == pl.duration(hours=h24))
        & active_at_source
    ).with_columns(
        (pl.col("close") / pl.col("_close_1") - 1).alias("r1"),
        (pl.col("close") / pl.col("_close_4") - 1).alias("r4"),
        (pl.col("close") / pl.col("_close_24") - 1).alias("r24"),
        pl.col("quote_volume").alias("v1"),
        (pl.col("quote_volume") - pl.col("_v1_prev")).alias("volume_diff_v1"),
        (pl.col("v4") - pl.col("_v4_prev")).alias("volume_diff_v4"),
    )
    current = _strategy_ranks(_top(current, top_n), config.values["long"]["rank_factors"])

    historical = source.filter(
        pl.col("decision_time").dt.hour().is_in(history_hours)
        # The historical Top100 used by T's rank change needs exactly
        # T-28h through T-5h: 24 bars and 23 intervals.  It must not depend
        # on T-29h, which is outside the frozen 28-hour warm-up.
        & ((pl.col("open_time") - pl.col("_history_window_start")) == pl.duration(hours=h24 - 1))
        & active_at_source
    ).with_columns(
        (pl.col("close") / pl.col("_close_4") - 1).alias("r4"),
    ).select("symbol", "decision_time", "r4", "v24")
    historical = _rank(_top(historical, top_n), "r4", "history_r4_rank")

    current = current.with_columns((pl.col("decision_time") - pl.duration(hours=4)).alias("_history_time"))
    current = current.join(
        historical.select("symbol", pl.col("decision_time").alias("_history_time"), "history_r4_rank"),
        on=["symbol", "_history_time"], how="left",
    ).with_columns(
        (pl.col("history_r4_rank").cast(pl.Int64) - pl.col("r4_rank").cast(pl.Int64)).alias("r4_rank_change")
    )
    changed = current.filter(pl.col("r4_rank_change").is_not_null())
    changed = _rank(changed, "r4_rank_change", "r4_rank_change_rank").select("symbol", "decision_time", "r4_rank_change", "r4_rank_change_rank")
    result = current.drop(["r4_rank_change", "_history_time"]).join(changed, on=["symbol", "decision_time"], how="left")
    for factor in ("volume_diff_v1", "volume_diff_v4"):
        ranked = _rank(result, factor, f"{factor}_rank").select("symbol", "decision_time", f"{factor}_rank")
        result = result.join(ranked, on=["symbol", "decision_time"], how="left")
    result = result.with_columns(
        (pl.col("source_bar_time") == pl.col("decision_time") - pl.duration(hours=1)).alias("_causal_source")
    )
    if not result.get_column("_causal_source").all():
        raise FeatureError("feature source violates T-1h causality")
    return result.drop([name for name in result.columns if name.startswith("_")]).sort(["decision_time", "symbol"])
