from __future__ import annotations

from typing import Any

import polars as pl


def _profit_factor(values: pl.Series) -> float | None:
    profit = float(values.filter(values > 0).sum() or 0.0)
    loss = abs(float(values.filter(values < 0).sum() or 0.0))
    return profit / loss if loss else None


def summarize(trades: pl.DataFrame, account: pl.DataFrame, counts: dict[str, int], candidate_counts: dict[str, int]) -> tuple[pl.DataFrame, pl.DataFrame]:
    net = trades.get_column("net_return") if not trades.is_empty() else pl.Series("net_return", [], dtype=pl.Float64)
    pnl = trades.get_column("pnl") if not trades.is_empty() else pl.Series("pnl", [], dtype=pl.Float64)
    equity = account.get_column("realized_equity") if not account.is_empty() else pl.Series([], dtype=pl.Float64)
    # Initial capital is a realized-equity peak even when the first exits lose.
    drawdown = (equity / equity.cum_max().clip(lower_bound=1.0) - 1).min() if equity.len() else 0.0
    final_equity = float(equity.tail(1).item()) if equity.len() else 1.0
    summary: dict[str, Any] = {
        "candidates": sum(candidate_counts.values()), "long_candidates": candidate_counts.get("long", 0), "short_candidates": candidate_counts.get("short", 0),
        "trades": trades.height, "long_trades": trades.filter(pl.col("strategy") == "long").height if not trades.is_empty() else 0,
        "short_trades": trades.filter(pl.col("strategy") == "short").height if not trades.is_empty() else 0,
        "mean_trade_return": float(net.mean() or 0.0), "median_trade_return": float(net.median() or 0.0),
        "win_rate": float((net > 0).mean() or 0.0),
        "trade_return_pf": _profit_factor(net), "account_pnl_pf": _profit_factor(pnl),
        "min_trade_return": float(net.min() or 0.0), "max_trade_return": float(net.max() or 0.0),
        "long_pnl": float(trades.filter(pl.col("strategy") == "long").get_column("pnl").sum() or 0.0) if not trades.is_empty() else 0.0,
        "short_pnl": float(trades.filter(pl.col("strategy") == "short").get_column("pnl").sum() or 0.0) if not trades.is_empty() else 0.0,
        "final_equity": final_equity, "compound_return": final_equity - 1, "realized_max_drawdown": float(drawdown or 0.0),
        "mae_p10": float(trades.get_column("mae_return").quantile(.1, "nearest") or 0.0) if not trades.is_empty() else 0.0,
        "mfe_p90": float(trades.get_column("mfe_return").quantile(.9, "nearest") or 0.0) if not trades.is_empty() else 0.0,
        "hard_stops": trades.filter(pl.col("exit_reason") == "HARD_STOP").height if not trades.is_empty() else 0,
        "max_open_units": int(account.get_column("open_units").max() or 0) if not account.is_empty() else 0,
        "max_open_positions": int(account.get_column("open_positions").max() or 0) if not account.is_empty() else 0,
        "head_profit_concentration": float(pnl.filter(pnl > 0).max() / pnl.filter(pnl > 0).sum()) if (pnl > 0).any() else 0.0,
        **counts,
    }
    monthly = _monthly(account)
    summary["positive_month_ratio"] = float((monthly.get_column("return") > 0).mean() or 0.0) if not monthly.is_empty() else 0.0
    return pl.DataFrame([summary]), monthly


def _monthly(account: pl.DataFrame) -> pl.DataFrame:
    if account.is_empty():
        return pl.DataFrame({"month": [], "return": [], "ending_equity": []})
    points = account.with_columns(pl.col("event_time").dt.truncate("1mo").alias("month")).group_by("month").agg(pl.col("realized_equity").sort_by("event_time").last().alias("ending_equity")).sort("month")
    return points.with_columns((pl.col("ending_equity") / pl.col("ending_equity").shift(1).fill_null(1.0) - 1).alias("return"))


def report_markdown(summary: pl.DataFrame, trades: pl.DataFrame) -> str:
    row = summary.to_dicts()[0]
    reasons = trades.group_by("exit_reason").len().sort("exit_reason").to_dicts() if not trades.is_empty() else []
    return "\n".join([
        "# Fixed Time Portfolio Report", "", "该报告仅描述历史研究结果，不构成收益承诺。", "",
        "## 汇总", "", f"- 候选 / 成交：{row['candidates']} / {row['trades']}",
        f"- 最终权益：{row['final_equity']:.8f}", f"- 复合收益：{row['compound_return']:.4%}",
        f"- 已实现最大回撤：{row['realized_max_drawdown']:.4%}", f"- 胜率：{row['win_rate']:.4%}",
        f"- 账户 PnL PF：{row['account_pnl_pf'] if row['account_pnl_pf'] is not None else 'n/a'}",
        f"- 单笔收益率 PF（诊断）：{row['trade_return_pf'] if row['trade_return_pf'] is not None else 'n/a'}", "",
        "## 退出原因", "", *(f"- {item['exit_reason']}：{item['len']}" for item in reasons), "",
    ])
