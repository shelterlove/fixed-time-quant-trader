from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from .data import write_json


def report(run: Path) -> None:
    root=run/"refined"
    s=json.loads((root/"summary.json").read_text(encoding="utf-8"))
    hourly=json.loads((run/"hourly/summary.json").read_text(encoding="utf-8"))
    trades=pl.read_parquet(root/"trades.parquet")
    eq=pl.read_parquet(root/"equity_hourly.parquet")
    rows=[]
    prior=s["initial_equity"]
    for year in sorted(eq["time"].dt.year().unique().to_list()):
        annual=eq.filter(pl.col("time").dt.year()==year)
        selected=trades.filter(pl.col("entry_time").dt.year()==year)
        peak=prior; dd=0.
        for value in annual["equity"]:
            peak=max(peak,value); dd=max(dd,1-value/peak)
        final=annual["equity"][-1]
        rows.append({"year":year,"account_return":final/prior-1,"within_year_close_drawdown":dd,
                     "entries":selected.height,"entry_cohort_win_rate":selected.select((pl.col("net_pnl")>0).mean()).item(),
                     "entry_cohort_hard_stops":selected.filter(pl.col("exit_reason")=="HARD_STOP").height})
        prior=final
    pl.DataFrame(rows).write_csv(root/"annual.csv")
    grouped=trades.group_by(["strategy","source"]).agg(pl.len().alias("trades"),
        (pl.col("net_pnl")>0).mean().alias("win_rate"),pl.col("net_return").mean().alias("mean_trade_return"),
        (pl.col("exit_reason")=="HARD_STOP").sum().alias("hard_stops"),pl.col("net_pnl").sum().alias("net_pnl"))
    grouped.sort(["strategy","source"]).write_csv(root/"by_source.csv")
    lines=["# 当前策略研究基线", "",f"区间：{s['start']} 至 {s['end']}；初始权益 {s['initial_equity']:,.2f} USDT。",
        "", "本次实际运行当前共享信号、准入与资金分配代码。离线成交使用历史 OHLC 状态机，不能替代测试网订单执行验收。",
        "", "## 全期结果", "", "|指标|小时悲观初跑|必要分钟细化|", "|---|---:|---:|"]
    for key,label,fmt in [("trades","成交笔数",",.0f"),("equity_multiple","期末净值倍数",",.4f"),
        ("win_rate","胜率",".2%"),("hard_stops","硬止损笔数",",.0f"),
        ("max_close_drawdown","小时收盘最大回撤",".2%"),("max_envelope_drawdown","非同步盘中风险包络",".2%")]:
        lines.append(f"|{label}|{format(hourly[key],fmt)}|{format(s[key],fmt)}|")
    lines.extend(["", "## 分年度", "", "账户从 2021 年连续运行，年度不重置持仓或权益；胜率按入场年份分组。末年不完整。", "",
        "|年份|账户收益|年内收盘回撤|入场笔数|胜率|硬止损|", "|---|---:|---:|---:|---:|---:|"])
    lines.extend(f"|{r['year']}|{r['account_return']:.2%}|{r['within_year_close_drawdown']:.2%}|{r['entries']}|{r['entry_cohort_win_rate']:.2%}|{r['entry_cohort_hard_stops']}|" for r in rows)
    lines.extend(["", "## 数据与核验", "",f"- 最终路径产生 {s['minute_unique_hours']} 个唯一币种小时的分钟需求；分钟细化 {s['minute_lot_hours_used']} 个 lot-小时，未接受 {s['minute_lot_hours_unavailable']} 个。",
        f"- 分钟内仍有 {s['ambiguous_minute_lot_bars']} 个 lot-分钟先后不明，按局部悲观可行路径处理。",
        f"- 小时账本最大相对误差 {s['max_ledger_relative_error']:.3g}；终点剩余持仓 {s['remaining_lots']}；终点模拟清算 {s['terminal_exits']} 笔。",
        "- `minute_requirements_all.json` 保存历轮需求并集；`refined/minute_audit.json` 保存本地复用、下载、聚合核验与缺失记录。",
        "- `run.json` 记录数据、代码、配置哈希及环境；数据清单包含逐文件来源和校验值。",
        "", "## 解释边界", "", "- 已计每边 0.10% 不利滑点、0.05% 手续费；未计资金费、容量冲击、交易所最小下单量、维持保证金与强平。",
        "- 信号只使用已完成小时线；入场以信号收盘价加滑点近似，T+1 分钟是记录时间，不是真实第 1 分钟价格。",
        "- 仅对退出顺序影响结果的小时补分钟；首小时未剔除入场前的首分钟，延长激活沿用小时决策时间。",
        "- 历史币种元数据不完整；当前交易所名单仅用于补充近期数据，不用于剔除历史退市币。",
        "- 风险包络合并了不同币种非同步极值；局部悲观路径不是复利账户的严格数学下界。",
        "- 与旧结果不一致时，以来源、信号、成交与账本差异解释，不以复现巨额收益为验收目标。",
        "- 本基线可用于后续受控研究；不将这些结果解释为可实现的实盘收益或无过拟合证明。",""])
    (run/"REPORT.md").write_text("\n".join(lines),encoding="utf-8")
    write_json(run/"baseline.json",{"status":"complete","summary":s,"report":"REPORT.md","annual":"refined/annual.csv","trade_attribution":"refined/by_source.csv"})
