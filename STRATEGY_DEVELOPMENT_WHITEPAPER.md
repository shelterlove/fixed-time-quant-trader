# 固定时间多空策略：从零开发白皮书

状态：冻结开发规格 v1.0  
日期：2026-08-31  
用途：作为新项目从零实现、审查和验收的唯一开发说明。本文不是新的参数研究，也不授权实盘交易。

## 1. 项目目标

新项目只实现已经冻结的固定时间多头、固定时间空头和最终选定的账户组合规则。它必须从原始行情重新产生特征、信号、成交和账户结果，不复制或导入当前仓库的 Python 实现、派生特征、信号表或逐笔结果。

新项目连数据也从零开始：只从 Binance 官方公开源重新发现合约并下载原始行情，不读取当前仓库的 PostgreSQL、Parquet、下载清单或任何派生数据。允许复用的只有已经写入本文的市场定义和冻结策略规则。旧研究结果只能在新项目独立完成输出后，由单独的对账命令读取。

项目首先是一套研究和回测系统，不包括交易所密钥、自动下单、仓位同步、故障恢复和实盘风控。实盘化必须在复现验收通过后另立项目。

## 2. 核心工程原则

### 2.1 简洁优雅

- 每个策略参数只在 `strategy.toml` 出现一次，业务代码不得另设默认策略参数。
- 数据流固定为“原始数据 → 特征 → 信号 → 执行 → 组合 → 报告”，禁止反向依赖。
- 特征、信号和执行尽量写成输入表到输出表的纯函数；网络和文件读写只在下载、存储和流水线层发生。
- 多头和空头共享通用特征表，但信号条件、执行规则和逐笔账本彼此独立，直到组合账户层才合并。
- 只保留一套执行引擎和一套组合事件循环，不建立第二套完整回测实现。
- 不使用动态导入、插件框架、ORM、任务编排框架、依赖注入容器或为未来需求预建抽象层。
- 首版依赖仅限 Python 3.11+、Polars 和 pytest；HTTP、ZIP、CSV 使用 Python 标准库。除非出现明确需求，不增加 pandas、数据库、ORM 或分布式计算组件。

### 2.2 不做多余工作

- 不重新搜索时间、因子、排名边界、市场阈值、止损或盈利保护参数。
- 不计算当前策略未使用的 `r12`、`v12`、其他市场分位数或额外技术指标。
- 不拉取空头分钟线；空头冻结基线按小时 high 近似止损。
- 多头分钟线只读取多头信号的必要持仓区间。由于滚动 P90 使用全部候选的影子路径，即使候选最终未被账户选中，它的必要分钟路径仍需读取。
- 不做全库测试、全量数据扫描、参数排列测试、性能基准测试或大规模快照测试。
- 官方 ZIP 只作为流式或临时下载载体；转换成规范 Parquet 后删除，不同时长期保存 ZIP、CSV 和 Parquet 三份相同数据。

### 2.3 不做无意义哈希

- 不计算 Parquet、CSV、行情表或结果文件的内容哈希。
- 不移植现有 `source_files.source_sha256` 作为策略缓存依赖。
- 缓存有效性只比较可读元数据：模式版本、窗口边界、策略版本、参数原文、行数、首末时间和最新源时间。
- 本地原始数据和缓存默认视为一次运行中冻结的输入。需要更新时由操作者显式使用 `--refresh`；普通回测不访问网络检查新版本。

### 2.4 不重复读取和计算

- 每个运行窗口的小时行情最多读取一次，并在内存中供共同特征、多头和空头使用。
- 同一阶段的 Parquet 最多读取一次；流水线通过对象传递结果，不让下游模块自行重新打开文件。
- 所有横截面排名一次生成。多头和空头从同一特征结果选择各自字段，不分别重算 Top100。
- 分钟路径先按币种和 UTC 日期合并，再批量下载缺失分区；禁止同一信号反复请求相同区间。
- 资金费按候选币种和月份批量下载一次并建立内存索引，禁止逐笔网络请求。
- 报告只消费已经完成的逐笔账本和账户账本，不重新执行策略。

## 3. 明确不在首版范围内的事项

- 原始 `src/momentumlock/` 动量策略。
- 旧的单因子、时间网格、阈值和止盈止损探索。
- PostgreSQL、数据库建表迁移和对当前仓库数据层的兼容适配。
- 实盘、模拟盘、API 密钥、订单状态机和交易所账户同步。
- 杠杆、保证金、强平、维持保证金和跨币种保证金模型。
- 精确盘口冲击、限价单、maker 成交和部分成交。
- 账户逐分钟盯市最大回撤。首版为复现旧结果，只报告退出事件形成的“已实现最大回撤”。
- 2026-09-01 UTC 之后的保留前向数据，除非用户明确授权打开。

## 4. 市场、时间和原始数据契约

### 4.1 市场口径

- 交易场所：Binance USDⓈ-M Futures。
- 合约：USDT 报价永续合约。
- 全部时间：UTC，内部使用带 UTC 时区的 `datetime`。
- 决策时刻记为 `T`。只能使用 `open_time < T` 的完整小时线，最新可见小时线必须是 `T-1h`。
- 排名并列时，先按数值降序，再按 `symbol` 升序产生从 1 开始的 ordinal rank。
- 历史时点是否可交易由该币种在决策前是否存在完整原始路径决定。当前交易所状态不能删除历史上曾有效的合约。

### 4.2 唯一外部数据源

历史 K 线优先来自 Binance 官方公共归档 `data.binance.vision`；当前合约元数据和归档缺口补取使用 USDⓈ-M Futures 公共 REST API。具体入口以 Binance 官方公开数据仓库和官方 Market Data 文档为准：

- [Binance Public Data](https://github.com/binance/binance-public-data)
- [USDⓈ-M Futures Market Data API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)

新项目不导入官方仓库的下载脚本，也不复制当前 MomentumLock 的下载代码；只依据公开文件格式和接口契约自行写最小下载器。

### 4.3 下载范围和优先级

1. 用官方归档前缀枚举 USDⓈ-M 历史 1h K 线币种，并与一次 `GET /fapi/v1/exchangeInfo` 的当前永续合约集合取并集。
2. 只保留普通 USDT 永续符号。历史退市币种不能因当前 `exchangeInfo` 不再出现而丢失；是否进入某个决策横截面最终由当时连续 1h 数据决定。
3. 完整历史月份使用 monthly 1h K 线 ZIP；不完整边界月份使用 daily ZIP。一个 `(symbol, interval, open_time)` 只允许一个来源。
4. 小时信号完成后，才确定多头所需分钟路径。按 `(symbol, UTC date)` 合并路径，优先下载对应 daily 1m ZIP；尚未发布或缺失的日期才调用 `GET /fapi/v1/klines`，并按接口上限分页。
5. 资金费只下载多头候选覆盖的币种和月份，优先官方 fundingRate 月度归档，缺口使用 `GET /fapi/v1/fundingRate`。资金费价格仍取对应结算分钟 1m open，不改用接口返回的 mark price。
6. 空头不下载任何额外分钟数据。

下载器采用有限并发和有上限重试，不做复杂调度：默认最多 8 个并发请求、每个对象最多 3 次指数退避重试。HTTP 404 只在该对象按规则允许缺失时转入 daily/REST 备用路径；必要数据最终仍缺失则失败，唯一例外是第 6.2.1 节定义的已入场终止性路径。

### 4.4 本地原始数据合同

下载后直接规范化为以下 Parquet 分区；ZIP 解压和 CSV 转换成功后删除临时文件：

```text
data/raw/symbols.parquet
data/raw/klines_1h/symbol=<SYMBOL>/year=<YYYY>/month=<MM>/part.parquet
data/raw/klines_1m/symbol=<SYMBOL>/date=<YYYY-MM-DD>/part.parquet
data/raw/funding/symbol=<SYMBOL>/year=<YYYY>/month=<MM>/part.parquet
data/manifests/downloads.jsonl
```

| 数据 | 唯一键 | 必要字段 |
|---|---|---|
| symbols | `symbol` | `symbol`, `quote_asset`, `contract_type`, `first_bar_time`, `last_bar_time` |
| 1h/1m K线 | `symbol, open_time` | `open`, `high`, `low`, `close`, `quote_volume`, `trade_count` |
| funding | `symbol, funding_time` | `funding_rate` |

`downloads.jsonl` 每个远端对象只记录 URL、官方对象大小、下载时间、解析行数和首末时间，不保存内容哈希。完整性只检查 HTTP 长度、ZIP 自带 CRC、CSV 列数、唯一键、价格关系和时间连续性；不下载 `.CHECKSUM`，不额外计算 SHA256。

加载时只做高价值校验：主键不重复、价格为正、`low <= min(open, close)`、`high >= max(open, close)`、要求的时间连续。内部缺口、入场参考价缺失或未形成入场分钟/小时线的必要路径必须报错，不能删除信号；已入场后的终止性后缀缺失按第 6.2.1 节处理。

## 5. 通用特征的精确定义

对每个决策时刻 `T` 和币种 `s`，记小时线 `C_s(t)` 为开盘时间 `t` 的 close，`Q_s(t)` 为 quote volume。最新可见小时线为 `t=T-1h`。

```text
r1(T)  = C(T-1h) / C(T-2h)  - 1
r4(T)  = C(T-1h) / C(T-5h)  - 1
r24(T) = C(T-1h) / C(T-25h) - 1

v1(T)  = Q(T-1h)
v4(T)  = Σ Q(T-i), i=1..4
v24(T) = Σ Q(T-i), i=1..24
```

### 5.1 动态 Top100

1. 每个 `T` 先在当时有效、数据连续的 USDT 永续合约中按 `v24(T)` 降序排名。
2. 取 `qv_rank <= 100`。不足 100 个合约时使用实际可用数量，记录 `universe_size`，不伪造名次。
3. `r1/r4/r24/v1/v4` 的策略排名只在这个同一时刻的 Top100 内计算。

### 5.2 空头使用的排名变化与成交量变化

```text
r4_rank_change(T) = r4_rank(T-4h) - r4_rank(T)
volume_diff_v1(T) = v1(T) - v1(T-1h)
volume_diff_v4(T) = v4(T) - v4(T-4h)
```

- `r4_rank_change` 为正表示排名改善，为负表示排名下降。
- 只有币种在 `T` 和 `T-4h` 都属于各自 Top100 时才计算 `r4_rank_change`；缺失保持 null，不能填成 101。
- 再在当前 `T` 的 Top100 内对 `r4_rank_change` 降序排名，得到 `r4_rank_change_rank`。因此 91–100 是排名变化最弱的一档。
- `volume_diff_v1/v4` 是成交量绝对差值，不是比例变化，也不是成交量排名变化。
- 历史 `v1/v4` 值在 Top100 筛选前从币种连续小时线取得；差值形成后，只在当前 Top100 内降序排名。
- `market_r1_p10(T)` 是当前 Top100 的 `r1` 第 10 百分位，插值方式固定为 `nearest`。

首版只需为 06、08、14、15、17 UTC 生成策略特征，并为 06/08 的 `r4_rank_change` 额外生成 02/04 UTC 的历史横截面。原始小时线仍需保留连续窗口。

小时特征的最小原始 warm-up 为 28 小时：它同时覆盖当前 `r24`、当前 `v24` 以及 06/08 信号所需的 `T-4h` 历史 Top100。盈利保护的历史 warm-up 与小时特征分开处理：为匹配现有基线，2022 研究窗口从 2022 年候选开始积累保护历史；2021 外部回放使用 2020 年影子候选为 2021 提供滚动历史。

## 6. 固定时间多头规则

### 6.1 信号

决策时间：每日 14:00、15:00、17:00 UTC。

信号必须同时满足：

- 当前动态 `v24` Top100；
- `r1_rank <= 10`；
- `r4_rank <= 10`；
- `r24_rank <= 10`；
- `v1_rank <= 10`；
- `v4_rank <= 10`；
- 当前 Top100 的 `r24` P10 严格大于 `-5%`。

同一时刻的选择优先级从小到大为：

```text
signal_score = r1_rank + r4_rank + r24_rank + v1_rank + v4_rank
tie_break = signal_score, r1_rank, r4_rank, r24_rank, v1_rank, v4_rank, symbol
```

### 6.2 时间与成交

| 决策时间 | 入场 | 计划退出 |
|---|---|---|
| 14:00 | 14:01，使用 14:00 分钟线 close 作为参考价 | 次日 08:01，使用 08:00 分钟线 close |
| 15:00 | 15:01，使用 15:00 分钟线 close 作为参考价 | 次日 08:01，使用 08:00 分钟线 close |
| 17:00 | 17:01，使用 17:00 分钟线 close 作为参考价 | 次日 04:01，使用 04:00 分钟线 close |

多头成交与净收益：

```text
entry_fill = entry_reference × (1 + 0.001)
exit_fill  = exit_reference  × (1 - 0.001)
ratio      = exit_fill / entry_fill
net_long   = ratio - 1 - 0.0005 × (1 + ratio) + funding_return
```

资金费事件边界为 `entry_time < funding_time <= exit_time`。正 funding rate 对多头为支出，结算名义价格使用结算分钟的 1 分钟 open 代理。

#### 6.2.1 终止性数据路径强制退出

此规则统一适用于多头分钟路径和空头小时路径。若入场参考价和首个持有周期K线均已形成，随后官方数据以连续前缀终止（退市/下架后不再有后续K线），则不删除既有信号：

- 以最后一根连续、已完成K线的 `close` 作为退出参考价；
- 退出时间为该K线 `open_time + interval`；
- 退出原因固定为 `DATA_PATH_FORCED_EXIT`；
- 多头资金费仍只累计 `entry_time < funding_time <= actual_exit_time`；
- 多头基础影子路径同样采用该退出，因此其P90历史与实际执行一致。

若缺失发生在入场参考价、首个持有周期K线之前，或连续前缀中间出现缺口，则仍是必要数据缺失并直接失败。该规则不以当前交易所状态回填或判断历史数据。

### 6.3 -30% 硬止损

- 止损参考价为原始入场参考价的 `70%`。
- 每分钟先检查 low。若 `low <= stop_price`，该分钟触发止损。
- 如果分钟 open 已低于止损价，按 `min(open, stop_price)` 作为退出参考价，否则按止损价。
- 退出时间为该分钟完成后的 `bar_time + 1 minute`。
- 同一分钟同时存在硬止损、保护价和有利高点时，硬止损优先。

### 6.4 滚动 P90 盈利保护

- 当价格相对入场参考价上涨 30% 后，盈利保护从下一根完整分钟线开始生效。
- 每笔候选在不考虑账户是否实际持有的情况下，维护一条“基础影子路径”：按计划退出或 -30% 硬止损结束。
- 在新候选入场时，只能使用 `current_entry_time - 365 days <= base_exit_time <= current_entry_time` 的已完成影子候选。
- 历史候选总数至少 100，且其中至少 30 个曾达到 +30% 激活；否则允许回撤固定为 30%。
- 满足样本要求时，允许回撤取这些已激活候选“激活之后、相对先前峰值的最大回撤”的 P90。
- 当前候选、尚未完成的候选以及未来候选不能进入阈值样本。
- 保护生效后，每分钟先用进入该分钟前的峰值计算：

```text
trailing_price = prior_peak × (1 - allowed_retrace)
cost_floor     = entry_reference × (1 + 2 × (0.001 + 0.0005))
effective_exit = max(trailing_price, cost_floor)
```

- 若该分钟 low 触及 `effective_exit`，按 `min(open, effective_exit)` 退出。
- 只有未退出时才用当前分钟 high 更新峰值。激活分钟不能依靠同一分钟内未知的 high/low 顺序立即触发保护退出。

### 6.5 多头独立账户规则

- 五个逻辑资金单位。
- 每个买入时刻最多两个信号。
- 若该时刻只有一个可接受信号，申请两份；若有两个，各申请一份。
- 同一多头币种尚未退出时，后续多头信号跳过。
- 同一时刻先退出旧仓，再处理新入场。
- 独立多头复现时，分配基数为 `free_cash + Σ open_notional`，不包含未实现盈亏；每份名义金额为该基数的 `1/5`，已有仓位名义金额不重算。

## 7. 固定时间空头规则

### 7.1 信号

决策时间：每日 06:00、08:00 UTC。信号必须同时满足：

- 当前动态 `v24` Top100；
- `r24_rank` 在 1–10；
- `r4_rank_change_rank` 在 91–100；
- `volume_diff_v1_rank` 在 1–5，或 `volume_diff_v4_rank` 在 1–5；
- `market_r1_p10` 位于闭区间 `[-1.5%, 0%]`。

优先级从小到大为：

```text
volume_rank_best = min(volume_diff_v1_rank, volume_diff_v4_rank)
priority_score   = r24_rank + (100 - r4_rank_change_rank) + volume_rank_best
tie_break        = priority_score, r24_rank, r4_rank_change_rank(desc),
                   volume_rank_best, entry_hour, symbol
```

### 7.2 06/08 因果选仓

- 每日总计最多三份，每个入场小时最多两份，每笔一份。
- 必须先仅用 06:00 当时的信息选出最多两笔。
- 到 08:00 后，只使用当天剩余容量选择最多两笔。
- 禁止把 06:00 和 08:00 候选放在一起全局排序后反向决定 06:00 持仓。
- 独立空头基线允许同一币种分别出现在 06:00 和 08:00 的当日信号中。
- 空余份额留作现金。
- 独立空头收益按 UTC 日结算：每日开始权益分成三份，当日每笔使用一份，当日收益为 `Σ(net_short / 3)`，未使用份额为现金，再进入下一日复利。

### 7.3 时间、止损和成本

| 决策时间 | 入场参考价 | 计划退出 |
|---|---|---|
| 06:00 | 05:00 小时线 close | 20:00，使用 19:00 小时线 close |
| 08:00 | 07:00 小时线 close | 17:00，使用 16:00 小时线 close |

- 持有区间使用从决策小时开始的完整小时线。
- 硬止损价为入场参考价的 `130%`。
- 若某小时 high 触及止损，按 `max(hourly_open, stop_price)` 退出，退出时间记为该小时完成时。
- 定时退出或止损后的冻结净收益为：

```text
net_short = 1 - exit_reference / entry_reference - 0.003
```

当前空头基线的 0.30% 是统一往返压力成本，未单独拆分滑点、手续费和资金费。首版若目标是精确复现，不得擅自加入空头资金费；这属于需要新版本和重新验证的经济口径变更。

## 8. 多空组合账户

最终冻结账户为 `LONG_PRIORITY_SKIP`：多空共享五份，做多优先，空头没有资金时直接跳过，不等待补仓。

### 8.1 资金单位

- 总容量五份，多头最多占五份，空头最多占三份。
- 每次入场前按当前空闲现金重新计算一份的金额：

```text
occupied_units = 所有当前持仓占用的逻辑份数之和
free_units     = 5 - occupied_units
unit_value     = free_cash / free_units
new_notional   = requested_units × unit_value
```

- 只改变新仓金额，已有仓位名义金额不重算。
- 多头单信号申请两份、双信号各一份；空头每笔一份。

### 8.2 做多优先

- 同一时刻先处理所有正常退出，再处理多头，最后处理空头。
- 多头入场所需空闲份数不足时，从当前空头仓位中按“最差优先级、较早信号、symbol”顺序强制退出，直到容量足够或已经没有空头可退。
- 强制退出价格使用多头入场时最新已完成小时线 close；不得读取包含多头入场时刻之后价格的小时线。
- 强制退出按空头公式计算截至该价格的净收益，退出原因记录为 `LONG_PRIORITY_EVICTION`。
- 若全部空头退出后仍不足，多头只使用实际剩余份数；没有份数则记录 `LONG_NO_CAPACITY`。

### 8.3 空头无资金直接跳过

- 新空头到达时，若总容量已满或空头已占三份，立即记录 `SHORT_SKIP_NO_FUNDS`。
- 跳过的空头不建立待入场订单，不在后续空闲时重新尝试，也不需要等待期间的 10% 价格检查。
- 其他组合方案仅属于研究历史；新项目不为它们保留代码、配置分支或测试。

### 8.4 重叠和事件顺序

- 同一策略、同一币种已有持仓时跳过新的同策略信号。
- 多头和空头允许同时持有同一币种，视为两个独立策略仓位。
- 同一时刻固定顺序：正常退出 → 为多头让位 → 多头入场 → 新空头入场 → 记录已实现权益。

## 9. 新项目目录

建议项目名暂用 `fixed-time-portfolio`，目录保持扁平：

```text
fixed-time-portfolio/
├─ README.md
├─ pyproject.toml
├─ strategy.toml                 唯一策略和窗口配置
├─ src/fixed_time/
│  ├─ __init__.py
│  ├─ cli.py                     薄命令入口
│  ├─ config.py                  TOML 读取、不可变 dataclass、参数校验
│  ├─ download.py                Binance 官方归档/API 的最小下载器
│  ├─ storage.py                 规范 Parquet、清单和单次加载
│  ├─ features.py                通用 Top100 与必要因子
│  ├─ signals.py                 多头/空头信号和稳定排序
│  ├─ execution.py               多头分钟与空头小时执行
│  ├─ portfolio.py               唯一账户事件循环
│  ├─ metrics.py                 逐笔、月度和账户统计
│  └─ pipeline.py                单次读取和阶段编排
├─ tests/
│  ├─ test_data.py
│  ├─ test_causality.py
│  ├─ test_execution.py
│  └─ test_portfolio.py
├─ data/                         本地可重建缓存，Git 忽略
└─ results/                      紧凑结果与报告
```

不建立 `utils.py` 大杂烩。重复两次以上且语义完全相同的小函数再提取；业务含义不同的相似代码不要为了“复用率”强行合并。

### 9.1 依赖方向

```text
cli
 └─ pipeline
     ├─ download → storage → features → signals → execution → portfolio → metrics
     └─ config
```

- `features/signals/execution/portfolio/metrics` 不访问网络、不读取文件。
- `download.py` 只理解远端对象、分页和重试，不包含因子、信号或仓位规则。
- `storage.py` 只理解规范数据模式、分区和清单，不包含排名阈值或入场时间。
- `metrics.py` 不修改交易和账户状态。
- `pipeline.py` 是唯一允许组织阶段和决定缓存复用的模块。

### 9.2 `strategy.toml` 合同骨架

下列配置是已经确认的开发合同，不得在代码中覆盖其中任何值。

```toml
schema_version = 1
strategy_version = "1.1.0"
status = "frozen"
timezone = "UTC"

[execution]
terminal_data_path_exit = "last_completed_bar_close"

[data]
archive_base_url = "https://data.binance.vision/data"
archive_listing_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
futures_api_base_url = "https://fapi.binance.com"
max_concurrent_requests = 8
max_attempts_per_object = 3
keep_downloaded_zip = false
download_checksum_files = false

[universe]
venue = "BINANCE_UM"
quote_asset = "USDT"
top_n = 100
liquidity_factor = "v24"
rank_method = "descending_ordinal"
rank_tie_break = "symbol_asc"

[features]
strategy_decision_hours_utc = [6, 8, 14, 15, 17]
rank_history_hours_utc = [2, 4]
hourly_warmup_hours = 28
return_horizons = [1, 4, 24]
volume_horizons = [1, 4, 24]
market_quantile_interpolation = "nearest"

[long]
entry_hours_utc = [14, 15, 17]
entry_delay_minutes = 1
rank_max = 10
rank_factors = ["r1", "r4", "r24", "v1", "v4"]
market_factor = "market_r24_p10"
market_lower_exclusive = -0.05
hard_stop_return = -0.30
slippage_per_side = 0.001
taker_fee_per_side = 0.0005
funding_boundary = "entry_exclusive_exit_inclusive"
funding_price_proxy = "settlement_minute_open"

[long.legs]
"14" = { exit_hour_utc = 8, next_day = true }
"15" = { exit_hour_utc = 8, next_day = true }
"17" = { exit_hour_utc = 4, next_day = true }

[long.protection]
method = "ROLLING_P90_BREAKEVEN"
activation_return = 0.30
window_days = 365
retrace_quantile = 0.90
minimum_history = 100
minimum_activated_history = 30
fallback_retrace = 0.30
break_even_floor = true
history_source = "all_completed_base_shadow_candidates"

[long.portfolio]
total_units = 5
max_positions_per_entry_time = 2
single_signal_units = 2
two_signal_units_each = 1
same_strategy_open_symbol = "skip"

[short]
entry_hours_utc = [6, 8]
r24_rank_min = 1
r24_rank_max = 10
r4_rank_change_rank_min = 91
r4_rank_change_rank_max = 100
volume_diff_rank_min = 1
volume_diff_rank_max = 5
volume_diff_logic = "OR"
market_factor = "market_r1_p10"
market_lower_inclusive = -0.015
market_upper_inclusive = 0.0
hard_stop_return = 0.30
stop_trigger = "hourly_high"
gap_fill = "max_hourly_open_or_stop"
round_trip_stress_cost = 0.003
funding_model = "none_in_v1_reproduction"

[short.legs]
"6" = { exit_hour_utc = 20, hold_hours = 14 }
"8" = { exit_hour_utc = 17, hold_hours = 9 }

[short.portfolio]
selection = "SEQUENTIAL_06_THEN_08"
total_daily_units = 3
max_positions_per_entry_hour = 2
units_per_signal = 1
same_symbol_same_day = "allow_in_standalone"

[portfolio]
mode = "LONG_PRIORITY_SKIP"
total_units = 5
long_unit_cap = 5
short_unit_cap = 3
unit_value = "free_cash_divided_by_free_units"
long_priority = true
short_no_funds = "skip"
short_eviction_order = "worst_priority_then_signal_time_then_symbol"
short_eviction_price = "latest_completed_hourly_close"
same_strategy_open_symbol = "skip"
cross_strategy_same_symbol = "allow"
same_timestamp_order = "exits_evictions_long_short"

[windows.research]
start = "2022-01-01T00:00:00+00:00"
end_exclusive = "2026-07-01T00:00:00+00:00"

[windows.external_2021]
start = "2021-01-01T00:00:00+00:00"
end_exclusive = "2022-01-01T00:00:00+00:00"
protection_history_start = "2020-01-01T00:00:00+00:00"

[windows.forward_2026_jul_aug]
start = "2026-07-01T00:00:00+00:00"
end_exclusive = "2026-08-28T15:00:00+00:00"

[windows.reserved_forward]
start = "2026-09-01T00:00:00+00:00"
authorized = false
```

配置读取必须是严格模式：未知键、重复时段、非法边界、非 UTC 时间、排名范围超出 Top100、腿的持有时长与退出时间不一致均直接失败。首版所有数据源均为公开匿名接口，不读取交易所 API Key。

## 10. 单次流水线

### 10.1 `bootstrap`

1. 解析并完整校验 `strategy.toml`。
2. 若本地没有该窗口的下载清单，则从官方归档一次发现币种和所需 1h 对象。
3. 只下载缺失的小时分区，直接规范化为 Parquet，完成后不保留 ZIP。
4. 下载结束后一次读取本地小时分区，计算动态 Top100、必要特征和多空信号。
5. 根据信号合并多头分钟日期和资金费月份，只下载尚未存在的必要分区。
6. 分钟与资金费齐全后直接继续执行、组合和报告，不重新读取小时分区或重算特征。

`bootstrap` 中断后可以重启：它根据可读下载清单跳过已经完整落盘的分区。恢复过程不重新下载完整对象，也不依赖文件哈希。

### 10.2 `prepare`

1. 从本地规范数据一次读取窗口所需小时线及 28 小时 warm-up。
2. 一次计算动态 Top100 和策略所需字段。
3. 写入 `hourly_features.parquet` 和可读元数据。
4. 分别生成多头、空头信号；此时不得读取任何持仓后路径。

### 10.3 `execute`

1. 由多头信号合并全部必要分钟区间。
2. 一次读取已经下载的分钟分区，并验证每条路径连续。
3. 一次读取已经下载的多头资金费，并从同一批分钟数据取得结算分钟 open 代理。
4. 按时间事件维护多头影子历史，执行硬止损与滚动 P90 保护。
5. 使用已经在内存中的小时线执行空头小时止损。
6. 写出独立的 `long_trades.parquet` 与 `short_trades.parquet`。

### 10.4 `portfolio`

1. 读取或直接接收同次运行生成的两份逐笔账本。
2. 按唯一事件循环执行共享五份、做多优先、空头无资金跳过规则。
3. 写出本地详细账本和紧凑汇总。

### 10.5 `report`

只读取组合账本和汇总，不调用特征、信号或执行代码。输出 `REPORT.md`、`summary.csv` 和 `monthly.csv`。

建议 CLI：

```powershell
python -m fixed_time.cli bootstrap --window research
python -m fixed_time.cli validate --window external_2021
python -m fixed_time.cli validate --window forward_2026_jul_aug
python -m fixed_time.cli run --window research --offline
python -m fixed_time.cli reconcile --legacy-root <只读旧仓库路径>
```

首次从零运行使用 `bootstrap`；它在一个进程内完成下载、特征、信号、执行、组合和报告。`run --offline` 只在本地原始数据已经齐全时使用。内部阶段函数可测试和恢复，但不提供一串要求操作者反复调用的日常命令。

## 11. 缓存设计

`data/raw/` 是新项目自己下载的唯一规范原始数据，不是旧仓库缓存。派生层只缓存计算昂贵且会跨命令复用的两类对象：

```text
data/cache/<window>/hourly_features.parquet
data/cache/<window>/signals.parquet
```

分钟线和资金费直接从 `data/raw/` 的必要分区一次加载，不再复制成第二份“路径缓存”。执行账本写入 `results/local/`，组合阶段在同一进程中直接接收内存对象。

每个缓存旁只放一个 `.meta.json`，字段为：

```json
{
  "schema_version": 1,
  "strategy_version": "1.1.0",
  "window_id": "research",
  "download_manifest": "data/manifests/downloads.jsonl",
  "source_start": "...",
  "source_end_exclusive": "...",
  "rows": 0,
  "symbols": 0,
  "first_time": "...",
  "last_time": "...",
  "latest_source_time": "...",
  "parameters": {}
}
```

不含哈希。缓存不匹配时整阶段重建，不写复杂的局部修补逻辑。写入使用临时文件后原子替换，避免中断留下半个 Parquet。普通运行不访问远端判断缓存新旧；只有显式 `--refresh` 才更新下载清单。

每次运行另写一个紧凑 `run_manifest.json`，记录实际使用的策略参数、窗口、各阶段行数和缓存文件名；同样不包含内容哈希。

详细逐笔和账户审计属于结果，不属于输入缓存；它们可本地保留并由 Git 忽略。Git 只保留配置、代码、紧凑 CSV、报告和必要审计结论。

## 12. 窗口和冻结纪律

| 角色 | 窗口 | 用法 |
|---|---|---|
| 发现记录 | 2022-01-01 至 2024-12-31 | 仅复现冻结规则，不再选择参数 |
| 确认记录 | 2025 全年 | 已看过，只用于一致性 |
| 研究扩展 | 2026-01-01 至 2026-06-30 | 已看过，只用于一致性 |
| 外部历史 | 2021 全年 | 规则冻结后的历史验证；多头 P90 使用 2020 影子候选作 warm-up |
| 已捕获前向 | 2026-07-01 至 2026-08-28 15:00 | 已看过，只用于实现核对 |
| 保留前向 | 2026-09-01 起 | 未经明确授权不得读取 |

所有窗口要求计划退出不超过窗口末端。跨窗口信号直接不进入候选，不使用窗口外价格补全交易。

`research` 命令不得接受外部窗口 ID；`validate` 命令要求 `strategy.toml` 状态为 frozen 且研究输出已经存在。`forward` 窗口使用单独命令并要求显式确认，避免误开。

## 13. 防未来函数的结构性约束

这些约束不是报告检查，而是代码结构本身必须做到：

1. `features.py` 的输出必须包含 `source_bar_time`，并满足 `source_bar_time == decision_time - 1h`。
2. Top100 和全部 rank 都按同一 `decision_time` 分组；任何跨时点全局排序视为错误。
3. `r4_rank_change` 通过精确的 `(symbol, T-4h)` 左连接生成，历史缺失保持 null。
4. 信号表完成并冻结后，流水线才允许调用分钟路径加载函数。
5. 分钟路径是否完整不能改变信号成员；内部缺口或未形成入场的缺失必须失败并列出缺失区间，已入场后的终止性后缀按第 6.2.1 节强制退出。
6. 06:00 空头选择函数的输入中不得出现 08:00 行。
7. 多头保护状态只接收已经到达基础退出事件的历史标签；不能把未来已预计算路径直接传入阈值函数。
8. 多头 P90 使用全部已完成影子候选，不因账户是否选中而改变历史样本。
9. 硬止损和保护退出使用 low/high 触发及明确 gap 规则，不能用收盘价判断是否触发。
10. 旧仓库结果路径只允许出现在 `reconcile` 命令，不能出现在 `run` 或 `validate` 的依赖图中。

## 14. 唯一账户事件循环

事件按 UTC 时间升序处理。同一时间固定顺序为：

1. 正常计划退出和策略止损/保护退出；
2. 处理为多头让位的空头强制退出；
3. 多头新入场；
4. 新空头入场；容量不足立即跳过；
5. 记录事件后的已实现权益。

事件循环只维护：现金、持仓字典、占用逻辑份数、各策略占用份数、同策略已开币种集合和退出事件堆。首版没有待入场队列。

不把“一个仓位”误认为“一份”：多头单信号可能是一个仓位占两份。容量约束永远按 `units` 求和。

## 15. 输出和指标

### 15.1 必要逐笔字段

```text
trade_id, strategy, symbol, signal_time, entry_time, planned_exit_time,
exit_time, entry_reference, exit_reference, exit_reason, units, notional,
gross_return, cost_return, funding_return, net_return, pnl,
mae_return, mfe_return, priority_score
```

### 15.2 必要汇总

- 候选数、成交数、多头/空头成交数；
- 单笔均值、中位数、胜率、PF、最小/最大单笔；
- 多头 PnL、空头 PnL、最终权益、复合收益；
- 已实现最大回撤，字段名必须是 `realized_max_drawdown`；
- MFE/MAE 分位数、硬止损次数、退出原因分布；
- 最大同时持仓份数和仓位数；
- 按月收益和正收益月份比例；
- 头部盈利集中度；
- 所有容量跳过、重复币种跳过、等待、取消、过期和让位计数。

不输出伪精确年化收益，不把历史巨额复利描述为未来收益预期。

## 16. 最小但必要的测试

首版只保留以下定向测试，使用手工构造的小数据集：

1. 用内存 ZIP/CSV 样本验证官方 K 线解析、时间单位、唯一键和损坏 ZIP 失败；不做真实网络集成测试。
2. `T` 只能看见 `T-1h`，且小时缺口使相应特征失效。
3. Top100、稳定并列排序和不足 100 个币种时的实际横截面。
4. `r4_rank_change` 精确回看四小时，历史非 Top100 时保持 null。
5. 多头分钟 low 硬止损、跳空 `min(open, stop)`、硬止损优先。
6. 多头激活从下一分钟生效，滚动 P90 不读取未退出和当前候选。
7. 空头小时 high 硬止损和跳空 `max(open, stop)`。
8. 06:00 选择结果在添加任意 08:00 候选后保持不变。
9. 共享五份的动态单位公式、做多让位、空头无资金跳过、退出先于入场和一个仓位多份。
10. 内部缺失分钟或小时路径时失败且信号数量不被静默减少；已入场后的终止性路径以最后完成K线close强制退出。

不为简单 getter、CSV 排版、第三方库行为和私有小函数编写测试。不跑旧仓库全量测试，不复制第二套执行引擎做“交叉验证”。

## 17. 开发阶段与验收

### 阶段 A：项目骨架与配置

- 建立目录和依赖。
- 写完 `strategy.toml`、不可变配置对象和全部参数校验。
- 验收：启动时打印策略版本和窗口；任何缺失、重复或非法参数立即失败。

### 阶段 B：从零数据下载

- 独立实现官方归档枚举、K 线/资金费下载、解析和规范 Parquet 存储。
- 先完成研究窗口 1h 数据；信号出现后再下载必要 1m 和 funding 分区。
- 验收：本地数据不依赖当前仓库；下载清单无哈希；唯一键、时间和价格校验通过。

### 阶段 C：小时特征和信号

- 一次读取原始小时线。
- 生成通用特征、多头信号和因果 06/08 空头信号。
- 验收：在读取旧结果前保存候选键；因果定向测试通过。

### 阶段 D：执行账本

- 批量读取必要多头分钟路径与资金费。
- 生成全部多头影子/保护账本和空头小时执行账本。
- 验收：每条候选都有且只有一条执行记录；路径缺失为零；执行定向测试通过。

### 阶段 E：独立账户与组合账户

- 先重放独立多头和独立空头，确认策略本身。
- 再实现共享五份、做多优先、空头无资金跳过的唯一组合模式。
- 验收：容量、重复、时间边界和事件顺序不变量通过。

### 阶段 F：结果冻结与事后对账

- 先写新项目完整结果和运行元数据。
- 然后单独运行 `reconcile`，读取旧仓库紧凑结果。
- 候选键、选中键、退出时间和退出原因要求完全一致；价格和收益允许浮点误差 `1e-9`。
- 若不一致，只修复口径或实现错误，不依据旧窗口表现调参数。

## 18. 当前结果基线（只用于事后验收）

### 18.1 独立多头

| 范围 | 成交 | 复合收益 | 已实现最大回撤 |
|---|---:|---:|---:|
| 2022–2024 | 1,235 | 16,326.29% | -29.55% |
| 2025–2026-07 | 241 | 9,686.46% | -36.29% |
| 2022–2026-07 | 1,476 | 1,607,452.73% | -36.29% |

### 18.2 因果顺序空头

| 范围 | 成交 | 单笔均值 | PF | 复合收益 | 已实现最大回撤 |
|---|---:|---:|---:|---:|---:|
| 2022–2024 | 376 | 1.4699% | 1.837 | 457.7652% | -19.5233% |
| 2025 | 93 | 1.6142% | 1.431 | 53.8396% | -23.1575% |
| 2021 | 66 | 1.8894% | 2.141 | 48.7187% | -13.0139% |
| 2026 H1 | 32 | 4.1217% | 1.976 | 48.6857% | -11.2427% |
| 2026 Jul–Aug | 21 | 4.6497% | 1.843 | 33.5112% | -20.0618% |

### 18.3 组合研究

这里只保留已经选定的 `LONG_PRIORITY_SKIP`：共享5份、做多优先、做空无资金立即跳过。

| 范围 | 成交 | 复合收益 | 已实现最大回撤 |
|---|---:|---:|---:|
| 2022–2026H1 | 1,952 | 7,533,732.90% | -34.08% |
| 2022–2024 | 1,593 | 43,604.54% | -33.79% |
| 2025 | 263 | 2,445.45% | -33.65% |
| 2026 H1 | 96 | 577.21% | -34.08% |
| 2021 | 344 | 4,065.55% | -23.91% |
| 2026 Jul–Aug | 28 | 26.40% | -12.22% |

以上数值只作为从零实现的结果复现与验收基线，不是收益承诺。新项目不实现或保留其他组合模式。

## 19. 代码审查清单

提交实现前只审查这些高价值问题：

- 策略参数是否只有 `strategy.toml` 一个来源；
- 下载和本地读取是否集中，是否重复请求同一对象或逐信号下载路径；
- 是否计算了未使用的因子或重复排名；
- 信号生成是否完全早于未来路径读取；
- 是否有全局 06/08 排序、历史 rank 填 101 等泄露；
- 多头 P90 是否只使用基础退出已完成的影子候选；
- 多空止损方向、gap 成交和同一 K 线路径优先级是否正确；
- 资金份数、仓位数量和名义金额是否被混淆；
- 缺路径是否失败，而不是静默删除；
- 报告是否明确写“已实现最大回撤”；
- 旧研究结果是否只在事后 `reconcile` 中出现；
- 是否引入了哈希、重复缓存、第二套引擎或与首版无关的框架。

## 20. 已确认的开发决策

1. 首版唯一组合账户是 `LONG_PRIORITY_SKIP`：共享五份、做多优先、空头无资金直接跳过。
2. 空头首版严格复现 `net = 1 - exit/entry - 0.30%`，不加入资金费；精确成本模型属于复现通过后的 v2。
3. 数据完全从零：新项目自行从 Binance 官方公开源重新发现和下载，不读取当前仓库数据库、缓存或清单。
4. 同一策略内部禁止同币种重叠；多头和空头允许同时持有同一币种。
5. 新项目从空目录开始，不复制旧代码。代码以清晰、短路径和单向数据流为第一优先；不通过连续补丁堆积研究逻辑，不实现未冻结分支，不做无意义测试或哈希，不重复读取、下载和计算。

以上内容构成 v1.0 冻结开发规格。后续从零开发只做实现、因果审查和结果核对，不再做参数选择。
