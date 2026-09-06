# Fixed Time Portfolio

固定时点、多空组合策略的研究与 Binance USDⓈ-M Futures 测试网执行仓库。

仓库包含两条明确分离的路径：

- **冻结研究基线**：以 [`strategy.toml`](strategy.toml) 为唯一参数来源，使用公开历史数据重建特征、信号、执行和组合结果。
- **测试网执行层**：以相同的信号规则在 Binance 测试网即时市价执行，并通过 SQLite 保存运行状态、订单与持仓。

当前代码版本为 `v1.6.0`；当前冻结策略版本为 `1.3.0`。`BASELINE.md` 保留原始 `1.1.0` 研究结果，不能与三单位、回撤倍率账户账本直接比较。本次执行修复、验收结果与迁移边界见 [`REPAIR_REVIEW.md`](REPAIR_REVIEW.md)。

## 文档导航

| 文档 | 用途 |
|---|---|
| [`STRATEGY_DEVELOPMENT_WHITEPAPER.md`](STRATEGY_DEVELOPMENT_WHITEPAPER.md) | 冻结研究基线的完整数据、信号、执行与组合规格。 |
| [`BASELINE.md`](BASELINE.md) | 冻结研究结果和验收口径。 |
| [`LIVE_EXTENSION.md`](LIVE_EXTENSION.md) | 已部署到测试网执行层的 24h 多头延长规则。 |
| [`OPERATIONS.md`](OPERATIONS.md) | VPS 首次部署、升级、监控、停止与故障处理。 |

代码和 `strategy.toml` / `testnet.toml` 是运行行为的最终依据；文档只解释已实现、已冻结的行为，不构成收益承诺。

## 策略与运行架构

```text
公开历史数据 → 特征 → 信号 → 执行路径 → 三单位盯市组合 → 指标与报告

测试网：完成小时线 → 信号候选 → 容量接纳 → 市价单 → 交易所硬止损
                                      ↓
                           交易所 P90 保护 / 时间退出 / 对账
```

- 仅处理 USDT 报价永续合约，所有内部时间使用 UTC。实时信号排名与多头 P90 影子历史使用完整正式网行情宇宙；候选生成后才过滤 Binance 测试网不支持下单的合约，避免测试网合约差异改变横截面排名或 P90 历史。
- 冻结研究在 06:00、08:00、14:00、15:00、17:00 UTC 决策；只读取 `open_time < 决策时刻` 的已完成小时线。
- 账户逻辑容量为三单位。多头优先；单个多头信号使用两单位、两个同时多头各使用一单位；容量不足时新多头先让较差空头退出，仍不足才让已过释放时刻的延长多头退出。空头不会挤出仓位。
- 每个决策先处理到期退出，再用最近完成的一分钟盯市净值冻结本批新仓的基础单位资金和倍率：回撤低于 25% 为 1.0 倍；25%、30%、35%、40%、45%、50%及以上分别为 1.05、1.10、1.15、1.20、1.25、1.30 倍。倍率只影响新订单名义金额；已有仓位、逻辑单位和退出规则不变。
- 单一新多头通常申请两单位；若决策前组合恰有一个空闲单位，则严格 D3 只使用该空闲单位，不会为凑足第二个单位而挤出任何仓位。空闲单位可能来自一个两单位多头、两个一单位仓位或其他合法组合。
- 测试网多头在信号计算完成后立即市价入场。这与冻结研究的下一分钟入场约定不同，属于已记录的实时执行差异。
- 新仓硬止损与盈利保护均由 Binance 条件单托管；程序通常每 5 秒根据测试网新增成交的累计高点收紧保护价。公共分钟线仅用于基础影子样本和旧版持仓兼容；P90 不使用延长持仓退出结果。

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

定向测试覆盖因果边界、容量、P90、止损、订单恢复、对账、收益账本、24h 延长与测试网配置限制。监控页通过本地只读账本展示净值、收益、回撤、持仓保护和决策原因，不会因浏览器刷新额外访问交易所。不要提交 `.env`、`runtime/`、`data/`、`results/` 或 `research/`。
