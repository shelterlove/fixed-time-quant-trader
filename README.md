# Fixed Time Portfolio

一个固定时点、多空组合的因果回测工程。唯一策略规格为
[`STRATEGY_DEVELOPMENT_WHITEPAPER.md`](STRATEGY_DEVELOPMENT_WHITEPAPER.md)；
[`strategy.toml`](strategy.toml) 保存已冻结的参数、窗口和运行权限。
策略版本为 `1.1.0`，研究引擎版本为 `1.1.1`；冻结结果见
[`BASELINE.md`](BASELINE.md)。

## 架构

```text
官方归档 / REST
        │
        ▼
data/raw/ ── storage.py ──► features.py ──► signals.py
                                            │
                                            ▼
                              execution.py ─► portfolio.py ─► metrics.py
                                            │
                                            ▼
                         data/cache/                 results/local/<window>/
```

- `config.py`：严格校验冻结配置、UTC 时间和授权窗口。
- `download.py` / `storage.py`：下载、断点恢复、原始分区和原子写入；原始数据只落在 `data/raw/`。
- `features.py`：只用决策时刻已完成的小时线生成动态 Top100 与因子。
- `signals.py`：生成合格候选；不承担持仓、容量或资金判断。
- `execution.py`：分钟级多头路径、资金费与小时级空头执行；影子候选仅服务滚动保护。
- `portfolio.py`：唯一的共享五份资金事件循环，负责去重、时段容量、做多优先和空头挤出。
- `pipeline.py`：单向编排、缓存和结果写入；不把旧项目带入正常运行。

`legacy_research/` 仅供只读对账；`results/reconciliation/` 保存已完成的对账证据。它们不参与回测数据流。

## 运行

```powershell
# 研究窗口：下载缺失原始数据并完整运行
python -m fixed_time.cli bootstrap --window research

# 研究窗口：只使用本地原始数据完整重建
python -m fixed_time.cli run --window research --offline

# 已冻结的信号缓存：只恢复执行、账户和报告
python -m fixed_time.cli resume --window research --offline

# 外部 2021 验证：先要求已完成的研究基线
python -m fixed_time.cli validate --window external_2021

# 2026 年 7–8 月前向窗口：显式确认后才允许运行
python -m fixed_time.cli forward --window forward_2026_jul_aug --confirm
```

`reserved_forward` 始终关闭；程序不得读取 `2026-09-01 UTC` 及之后的数据。
`run` 和 `resume` 都要求 `--offline`，不会访问网络。需要补数据时使用对应的
`bootstrap`、`validate` 或 `forward` 入口，下载器会复用本地分区和清单。

## 结果与检查

每个窗口输出至 `results/local/<window>/`：

- `summary.csv`、`monthly.csv` 与 `REPORT.md`：账户级表现。
- `portfolio_trades.parquet`、`account_ledger.parquet`：组合成交与权益路径。
- `long_trades.parquet`、`short_trades.parquet`、`funding_event_detail.parquet`：执行与资金费诊断。
- `combo_allocation_audit.csv`：候选的接纳、去重、容量和挤出审计。
- `run_manifest.json`：冻结配置、缓存和行数证明。

测试仅覆盖因果性、关键执行边界、配置权限、组合容量和数据分区。运行：

```powershell
pytest -q
```

策略或经济规则有歧义时，不应通过改代码猜测；应先更新白皮书并明确确认口径。

## 已验证环境

- Python 3.12.10
- Polars 1.32.0
- pytest 9.1.1
