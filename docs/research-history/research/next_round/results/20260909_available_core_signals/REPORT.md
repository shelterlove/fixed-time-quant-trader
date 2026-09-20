# 空仓可用的完整核心信号研究

候选没有放宽任何现有门槛：Top100、P10 > -5%、五项均 Top10；仅取非 14/15/17 UTC，且基线账户在 T+1 有容量、未持有同币的信号。收益为完成小时路径，扣双边费率与滑点；组合回放只会在因子通过 2025 和 2026 上半年固定验证后进行。

## 因子验证

| 通过 2022-24 训练 | 通过 2025 / 2026H1 验证 | 数量 |
|---|---|---:|
| breadth_r1_above_minus_1pct | False | 690 |
| liquidity_top50 | False | 1440 |
| momentum_accelerating | False | 749 |
| momentum_accelerating+volume_continuing | False | 513 |
| rank_sum_25 | False | 1077 |
| rank_sum_25+liquidity_top50 | False | 1073 |
| rank_sum_25+trend_all_positive | False | 1077 |
| rank_sum_25+volume_continuing | False | 624 |
| trend_all_positive | False | 1443 |
| trend_all_positive+breadth_r1_above_minus_1pct | False | 690 |
| trend_all_positive+volume_continuing | False | 774 |
| volume_continuing | False | 775 |

通过组合回放门槛的筛选：无。

完整小时、年份、容量和市场区间表在 `available_by_hour_year_capacity.csv`、`available_by_market_band.csv`；因子分年结果在 `factor_screen_by_year.csv`。
