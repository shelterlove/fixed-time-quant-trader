# 可复现研究流水线

## 固定顺序

1. 盘点本地全市场小时行情，冻结起止点、预热期、来源和 SHA-256；缺口单列。禁止只用旧成交涉及的币种重建全市场排名。
2. 复用生产策略的纯函数，按月批量重建全量候选；每月带 81 小时预热。账户始终连续运行，不按年重置。
3. 完整小时回放，对每个持仓比较可行的 OHLC 路径。只有退出价格、退出原因或持续持仓状态确实不同，才登记分钟需求。
4. 优先读取本地对应日期分钟线，仅保留需求小时。要求完整 60 根，时间连续，OHLC、成交额与笔数聚合与小时线一致。
5. 缺失时只请求该币该小时的 60 根分钟；退市币 REST 不可用时尝试官方日归档并验证 CHECKSUM。归档为上游最小文件粒度，不扩大到全市场分钟下载。
6. 以相同信号重新运行连续账户。分钟内部仍有歧义则比较可行路径，取该 lot 当前分钟末价值更低者；不是选择有利高低顺序。没有接受的分钟数据明确记录并回退小时悲观模型。
7. 后续持仓可能因先前退出改变，所以分钟需求在细化回放中继续按需发现、持久化、补充。最终保存需求并集和实际使用审计。
8. 核对逐时账户、最终清仓现金流、跨年结果与来源贡献，保存报告，再将本轮标记为完成。

## 命令

```bash
python -m fixed_time.research prepare --source D:/Quant_research --snapshot data/snapshots/baseline_20260919 --start 2021-01-01T00:00:00Z --end 2026-09-19T04:00:00Z --supplement-hours
python -m fixed_time.research repair-hours --snapshot data/snapshots/baseline_20260919 --target data/snapshots/<new_snapshot_id> --local-source D:/Quant_research
python -m fixed_time.research run --snapshot data/snapshots/<new_snapshot_id> --run artifacts/runs/<new_run_id> --minute-source D:/Quant_research --download-minutes
```

`--supplement-hours` 与 `--download-minutes` 显式启用公共数据补充；不提供时只读本地。首次构建快照后不覆盖；失败的下载可复用响应缓存。信号缓存必须匹配代码、配置与数据哈希。完成的回放不能覆盖；修改实现后使用新运行目录。

REST 对历史合约可能返回成功但为空或只返回部分 K 线。修复器逐小时核对响应，并对 REST 未返回的小时继续查询官方逐日归档。两种官方来源均无 K 线的时段标记为 `verified_no_published_kline`；这表示没有可取得的官方行情，不等同于推断了停牌或合约迁移原因。

## 下载约束

下载器只允许公共 GET，不加载 `.env`、API key 或运行数据库。全局最多 2 个并发请求，请求起点间隔至少 0.35 秒；klines 单页 499 条以兼顾权重与吞吐。读取 `exchangeInfo.rateLimits` 和 `X-MBX-USED-WEIGHT-1M`，接近所知上限 70% 时主动休息。429 服从 Retry-After，网络/服务错误有限指数退避；403/418 停止，不绕过封禁。长于 60 秒的服务端冷却交由后续恢复，不无限挂起重试。

这是本研究进程的配额管理；相同公网 IP 下其他应用共享限额，不能认为本地并发低就不会限流。优先缓存、按需补数，不重复请求已成功的对象。

依据：[Binance General Info](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)、[Klines](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/market-data/rest-api/Kline-Candlestick-Data)、[官方数据归档与校验说明](https://github.com/binance/binance-public-data)。

## 基线口径

策略参数冻结，不搜索阈值。基础对照保留现有每边 10bp 滑点、5bp 手续费；资金费、成交容量、最小订单与强平模型均显式列为未计项，不将理论复利当实盘收益。资金费数据存在不等于已正确进入账户模型；后续新增这些模型应建独立实验，与本基线同数据同信号对比。

小时入场沿用信号收盘价加滑点，T+1 分钟是记录近似。分钟用于退出顺序消歧，延长激活资格仍沿用原小时边界口径。实时端 5 秒采样与离线 OHLC 触及不完全等价，需要分别验收。

分钟可能仍缺失或与小时聚合冲突；报告必须列数量及受影响交易，不隐藏。持仓缺少小时线则直接失败并要求修复，不按未来路径完整性筛掉候选，也不擅自用前值填充。

## 仓库边界

生产包中 `strategy.py` 是共享信号与分配层；`engine.py/exchange.py/state.py` 负责在线执行，`research/` 负责数据与离线撮合；研究禁止导入线上配置、交易接口或运行数据库。`data/` 与 `artifacts/` 不纳入 Git 和 Docker 上下文；`reports/` 保存可审阅基线，`docs/research-history/` 是历史结论，不充当当前可执行入口。`runtime/` 仅供测试网服务，不能作为研究输出目录。

运行日志按月份或阶段输出。启动确认后等完成/失败，不做秒级状态轮询。验证只围绕因果性、价格顺序、预算及费用现金流，不为了测试数量制造重复用例。
