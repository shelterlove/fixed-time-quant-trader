# Fixed-Time Portfolio v2

这是当前策略 `SPEC-20260917-r2` 的 Binance USD-M Futures 测试网执行与可复现研究仓库。在线执行和离线研究共用信号、准入及资金分配规则；研究数据和回测产物独立保存，不进入交易部署镜像。历史结论文档保存在 `docs/research-history/`。

当前策略包括：多头 Main/A/C、新空头 00/01/02、原 06 空头、A50 双方向独立预算、全类型 30% 硬止损、多头末四小时盈利延长，以及多头 +300% 激活、+270% 固定保护线、+400% 全平。

详细规则见 [策略说明](docs/STRATEGY_SPEC.md)，部署及恢复流程见 [运维说明](OPERATIONS.md)，代码边界和故障修复见 [实现说明](docs/IMPLEMENTATION.md)。

## 历史研究

[研究流水线](docs/RESEARCH_WORKFLOW.md)固定执行：小时数据冻结与缺口检查 → 当前代码重建信号 → 完整小时回放 → 按需分钟消歧 → 完整账户核验。分钟内仍有歧义时采用局部悲观可行路径，不扫描参数寻找最高收益。

```text
src/fixed_time/strategy.py   共享策略规则
src/fixed_time/research/     离线数据、撮合、账本和报告
data/                       冻结行情与按需分钟缓存（不入 Git）
artifacts/runs/<run_id>/     候选、成交、权益及审计产物（不入 Git）
reports/<run_id>/           版本管理的基线报告与运行清单
runtime/                    测试网状态（与研究隔离，不入 Git）
tests/                      规则、账本与故障恢复的必要验证
```

每次研究固定数据清单、代码和配置哈希；已完成的基线不覆盖。当前已完成 [baseline_20260919](reports/baseline_20260919/REPORT.md)，数据缺口、与旧结果的差异及耗时见[基线复核](reports/baseline_20260919/REVIEW.md)。运行命令与模型边界见研究流水线文档。

## 本地验证

```bash
python -m pip install -e ".[test]"
python -m fixed_time.cli strategy-check --root .
python -m pytest -q
```

## 测试网启动

```bash
cp .env.example .env
# 填入 Binance Futures testnet key/secret，并显式设置 TRADING_ENABLED=true
./deploy.sh
```

签名交易被代码限制在 `https://demo-fapi.binance.com`。`runtime/` 保存 SQLite 状态并由 Docker 挂载。首次从旧版本升级要求旧策略仓位结束、账户无挂单；后续 v2 更新沿用原数据库恢复。旧表保留供审计，不删除运行数据。

已有 VPS 在代码推送到 GitHub 后，首次执行 `git pull --ff-only` 再执行 `sh deploy.sh`；以后可直接执行 `sh deploy.sh --pull`。脚本沿用 Docker Compose，完成测试、账户预检、数据库备份和容器更新。API 密钥仍放在 VPS 的 `.env` 中，不随 Git 同步。

## 生产代码

- `strategy.py`：R/P 币池、因子、信号、纽约时区退出和 A50 分配。
- `exchange.py`：Binance REST、精度、校时和订单接口。
- `state.py`：lot、订单、日参考权益、事故和旧库迁移。
- `engine.py`：对账、准入、开平仓、止损、盈利保护和调度。
- `dashboard.py`：只读健康状态。
