# v2：独立实现与验证

2026-09-18 更新：已补 BULLA/TUT/LSK 所需分钟数据，小时行情延长至北京时间 **2026-09-18 09:00**，分钟内不明确按悲观可行路径。最新明细为 [2026年7—9月84笔交易](../outputs/quant_trades_20260918/交易明细_2026年7至9月.md)，[更新核验](LATEST_RESULTS.md)。新数据和回放独立保存在 `outputs/updated_20260918/`，未覆盖下方历史结果。

当前版本为 **r2：多空统一 30% 止损 + 多头 300/270/400 盈利保护 + 原盈利延长**。规则见 [当前策略补充](PROFIT_GUARD_SPEC.md)，六账户全流程复核见 [小时结果](PROFIT_GUARD_RESULTS.md)，追加三账户见 [本地分钟细化结果](PROFIT_GUARD_MINUTE_RESULTS.md)。按顺序运行 `python -m v2.profit_guard_study`、`python -m v2.minute_guard_study`；已完成结果禁止覆盖。`python -m v2 replay` 等下方旧入口仍用于历史 r1 复现。75 项测试通过。

以下内容记录 r1 历史基线，不是当前盈利退出配置。

状态：2026-09-17 已完成独立实现、固定数据、信号差异核验和完整双版本回放。41 个定向测试通过；逐小时及期末账本对账通过。详见 [验收结果](RESULTS.md)。

主版本：**原 L2_S1_A50__PROP 资金规则 + 多头 30% 硬止损 + 盈利延长**。

不启用盈利保护、回撤加杠杆、闲置加仓、为多头清退空头或剩余份额资金分配。核验对照只关闭盈利延长，保留其他规则。

## 阅读顺序

1. [策略说明](STRATEGY_SPEC.md)：信号、资金、持仓、退出、成本、时序与风险口径。
2. [实施与验收计划](IMPLEMENTATION_PLAN.md)：复用边界、模块划分、分阶段工作、必要验证和模型建议。
3. [完整结果与差异归因](RESULTS.md)：新旧比较、逐年表现、延长占资及风险边界。

## 运行

从仓库根目录运行；依赖见 `v2/requirements.txt`，不修改原项目入口。

```powershell
python -m pytest v2/tests -q
python -m v2 validate
```

已经保存的输入和结果在 `v2/outputs/frozen_20260917/`。`freeze` 对已完成冻结的目录会拒绝覆盖。需要有依据地重做时，按顺序指定一个新目录；其余阶段默认读取上述固定目录。

```powershell
python -m v2 freeze --out v2/outputs/new_run
python -m v2 signals --out v2/outputs/new_run
python -m v2 audit --out v2/outputs/new_run
python -m v2 explain --out v2/outputs/new_run
python -m v2 prefix --out v2/outputs/new_run
python -m v2 replay --out v2/outputs/new_run
python -m v2 validate --out v2/outputs/new_run
python -m v2 report --out v2/outputs/new_run
```

`audit/explain/report` 可读取旧研究作证据；数据、信号、账户引擎和指标不 import 旧策略。`report` 从已保存轨迹生成统计，不重跑账户。`validate` 由已保存成交独立重建账本和本金占用，并核验连续小时、日初预算、净值和终点结算。

两版回撤：小时收盘 **44.41%**，盘中 OHLC 保守包络 **54.82%**。尚不能认定满足约 50% 的盘中风险偏好；资金费、容量和实盘强平模型未纳入。

## 原则

- 从策略说明独立实现，不复制、包装或调用 v1 / research 的策略逻辑。
- 基础行情、文件读写等可以复用；因子、信号、资金分配、退出与账户核算重新实现。
- 旧结果是对照证据，不是真值；从第一处差异定位原因，不能为了对齐收益而迁就错误。
- 先完成固定策略的正确性验证，不同时优化参数或扩大研究范围。
- 保留 v1、旧结果和原入口；所有新代码与新产物留在 `v2/`。
- 本实现仅运行离线回测。

策略组合已由用户确认。说明中的小时成交近似、盘中风险近似和现有数据限制均须在最终报告保留，不得包装成精确实盘收益。
