# Fixed Time Portfolio

固定时点、多空组合策略的研究与 Binance USDⓈ-M Futures 测试网执行仓库。

仓库包含两条明确分离的路径：

- **冻结研究基线**：以 [`strategy.toml`](strategy.toml) 为唯一参数来源，使用公开历史数据重建特征、信号、执行和组合结果。
- **测试网执行层**：以相同的信号规则在 Binance 测试网即时市价执行，并通过 SQLite 保存运行状态、订单与持仓。

当前代码发布版本为 `v1.3.5`；冻结研究策略版本为 `1.1.0`，对应冻结研究引擎 `1.1.1`。二者不是同一个版本号：前者描述运行代码发布，后者描述不再改动的研究基线。

## 文档导航

| 文档 | 用途 |
|---|---|
| [`STRATEGY_DEVELOPMENT_WHITEPAPER.md`](STRATEGY_DEVELOPMENT_WHITEPAPER.md) | 冻结研究基线的完整数据、信号、执行与组合规格。 |
| [`BASELINE.md`](BASELINE.md) | 冻结研究结果和验收口径。 |
| [`research/exit_protection_extension/README.md`](research/exit_protection_extension/README.md) | 近期保护激活后延长持仓的独立研究方法。 |
| [`research/exit_protection_extension/CAP_COMPARISON.md`](research/exit_protection_extension/CAP_COMPARISON.md) | 12h、24h、48h 上限的研究比较与选择。 |
| [`LIVE_EXTENSION.md`](LIVE_EXTENSION.md) | 已部署到测试网执行层的 24h 多头延长规则。 |
| [`OPERATIONS.md`](OPERATIONS.md) | VPS 首次部署、升级、监控、停止与故障处理。 |

代码和 `strategy.toml` / `testnet.toml` 是运行行为的最终依据；文档只解释已实现、已冻结的行为，不构成收益承诺。

## 策略与运行架构

```text
公开历史数据 → 特征 → 信号 → 执行路径 → 五份资金组合 → 指标与报告

测试网：完成小时线 → 信号候选 → 容量接纳 → 市价单 → 交易所硬止损
                                      ↓
                           分钟级 P90 保护 / 时间退出 / 对账
```

- 仅处理 USDT 报价永续合约，所有内部时间使用 UTC。
- 冻结研究在 06:00、08:00、14:00、15:00、17:00 UTC 决策；只读取 `open_time < 决策时刻` 的已完成小时线。
- 账户统一分为五份。多头优先；单个多头信号使用两份、两个同时多头各使用一份；容量不足时新多头可按优先级让较差空头退出。
- 测试网多头在信号计算完成后立即市价入场。这与冻结研究的下一分钟入场约定不同，属于已记录的实时执行差异。
- 硬止损由 Binance 条件单托管；多头盈利保护由程序读取已完成的一分钟K线执行。P90 历史只使用已完成的基础影子候选，不使用延长持仓退出结果。

## 本地研究

以下命令在仓库根目录运行：

```powershell
# 下载研究窗口缺失的公开原始数据并完整运行。
python -m fixed_time.cli bootstrap --window research

# 只用本地数据重建冻结研究结果。
python -m fixed_time.cli run --window research --offline

# 从已缓存的冻结信号恢复执行、组合和报告。
python -m fixed_time.cli resume --window research --offline

# 基线完成后，运行授权的外部 2021 验证。
python -m fixed_time.cli validate --window external_2021

# 显式确认后，运行授权的 2026-07 至 2026-08 前向窗口。
python -m fixed_time.cli forward --window forward_2026_jul_aug --confirm
```

`reserved_forward` 未授权，程序不会读取 `2026-09-01 UTC` 及之后的数据。研究输出写入 `results/local/<window>/`；原始数据、结果和运行缓存均不提交到 Git。

## 测试网执行

测试网部署和日常操作见 [`OPERATIONS.md`](OPERATIONS.md)。本地只读检查可运行：

```powershell
Copy-Item .env.example .env
python -m fixed_time.cli live-check
```

`live-smoke` 会在空的专用测试网账户中实际开仓、设置硬止损、平仓和撤销保护单；它不是只读命令，也不能与正在运行的交易程序并发执行。

```powershell
python -m fixed_time.cli live-smoke --symbol BTCUSDT
```

## 测试与版本管理

```powershell
pytest -q
git status
git describe --tags --always
```

定向测试覆盖因果边界、容量、P90、止损、订单恢复、对账、24h 延长与测试网配置限制。不要提交 `.env`、`runtime/`、`data/` 或 `results/`。
