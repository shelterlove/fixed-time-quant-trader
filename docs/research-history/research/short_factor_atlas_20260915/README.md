# 空头广谱因子图谱

研究专用，不修改生产策略。默认只使用本地小时行情，覆盖 2021-01-01 至 2026-09-13 13:00 UTC。

```powershell
python -m pytest research/short_factor_atlas_20260915/test_atlas.py -q
python -m research.short_factor_atlas_20260915 run
```

也可分别运行 `prepare`、`single`、`combine`、`report`。市场状态统计属于 `single` 阶段；报告只读取统计产物。
默认产物目录是 `artifacts_unique_paths/`；标签按事件与真实退出时刻去重，期限名称另存映射表。

两组固定时间路径的聚焦研究使用独立入口，不覆盖全量图谱：

```powershell
python -m research.short_factor_atlas_20260915.focused run
```

也可按 `prepare`、`pairs`、`compare`、`new-factors`、`report` 分阶段执行。产物写入
`artifacts_focus_v1/`；原始行情扫描按 25 个币分区落盘，重跑时跳过已完成分区。

UTC00/01/02 统一多因子候选（C1—C8）在同一底表上测试：

```powershell
python -m research.short_factor_atlas_20260915.multi_candidates
```

结果写入 `multi_candidate_atlas.parquet`、`multi_candidate_marginals.parquet` 和
`MULTI_CANDIDATE_RESULTS.md`。

多窗口强势条件候选（`ATR高 + 相对成交额高 + accel4高`，再叠加
`r24/r12/r8/r4` 或联合条件）使用同一聚焦底表：

```powershell
python -m research.short_factor_atlas_20260915.multi_window_candidates
```

结果写入 `multi_window_atlas.parquet` 和 `MULTI_WINDOW_RESULTS.md`。

只将上述条件中的 `r24高` 替换为 `r48高`：

```powershell
python -m research.short_factor_atlas_20260915.r48_candidate
```

结果写入 `r48_candidate_atlas.parquet` 和 `R48_CANDIDATE_RESULTS.md`，不会覆盖r24版本。

检验r48高但短窗口排名没有同步的版本：

```powershell
python -m research.short_factor_atlas_20260915.r48_gap_candidate
```

结果写入 `r48_gap_atlas.parquet` 和 `R48_GAP_RESULTS.md`。

查看r48候选中24h/48h最高点距今时长和回撤的分层差异：

```powershell
python -m research.short_factor_atlas_20260915.r48_extreme_profile
```

结果写入 `r48_extreme_atlas.parquet`、`r48_extreme_events.parquet` 和 `R48_EXTREME_RESULTS.md`。

按当前 r48 四因子候选回放 UTC00/01/02 分批账户、30%硬止损和资金分配规则：

```powershell
python -m research.short_factor_atlas_20260915.short_account
```

结果写入 `artifacts_focus_v1/short_account_v2/`；同一币已有空仓时，重叠信号按生产规则跳过。

比较降低日内暴露和收紧硬止损的三组账户版本（一次读取原始行情）：

```powershell
python -m research.short_factor_atlas_20260915.short_account_risk
```

结果写入 `artifacts_focus_v1/short_account_risk_v1/`。

广谱好坏交易扫描完成后，使用固定的少量质量因子与市场状态组合，并在同一批路径上做账户重放：

```powershell
python -m research.short_factor_atlas_20260915.short_quality_scan
python -m research.short_factor_atlas_20260915.short_quality_combine
```

结果写入 `artifacts_focus_v1/short_quality_scan_v1/` 和
`artifacts_focus_v1/short_quality_combine_v1/`；组合阶段只做一次特征读取和一次原始路径读取。

检查整条账户权益曲线中所有大幅回撤区间的市场与交易特征：

```powershell
python -m research.short_factor_atlas_20260915.short_drawdown_profile
```

结果写入 `artifacts_focus_v1/short_drawdown_profile_v1/`。
