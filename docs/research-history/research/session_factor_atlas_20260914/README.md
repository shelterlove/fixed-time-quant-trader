# Session factor atlas

独立研究入口，协议见 [PROTOCOL.md](PROTOCOL.md)。不修改生产策略。

策略解读和现有多头改进设计见 [STRATEGY_DESIGN.md](STRATEGY_DESIGN.md)。其中同入场退出配对、原策略追加因子分组、信号重叠与时点诊断可用 `python -m research.session_factor_atlas_20260914.strategy_synthesis` 复现；这是看过Atlas后的解释性研究，不改动原冻结名单，不是新的独立样本外实验。

从仓库根目录运行：

```powershell
python -m pytest research/session_factor_atlas_20260914/test_atlas.py -q
python -m research.session_factor_atlas_20260914 run
```

默认覆盖2021-01-01至2026-09-13 13:00 UTC，先准备并必要补数，再单因子、多因子、报告。可分别指定`prepare`、`single`、`combine`、`report`；参数需要与该输出目录既有运行一致。`--no-repair`仅用于本地小样本验证，完整研究默认启用官方必要缺口补数。依赖现有polars，另外需要numpy和IANA tzdata（本机已装）；记录在requirements.txt。

后台启动（隐藏窗口，日志分流）：

```powershell
Start-Process -FilePath (Get-Command python).Source -ArgumentList '-u','-m','research.session_factor_atlas_20260914','run' -WorkingDirectory 'D:\Quant_develop' -WindowStyle Hidden -RedirectStandardOutput 'D:\Quant_develop\research\session_factor_atlas_20260914\run.stdout.log' -RedirectStandardError 'D:\Quant_develop\research\session_factor_atlas_20260914\run.stderr.log'
```

报告为 [artifacts/REPORT.md](artifacts/REPORT.md)，覆盖见 [artifacts/COVERAGE.md](artifacts/COVERAGE.md)，状态见`artifacts/status.json`；进度行记录阶段和每月/Session耗时。失败后修复原因，用相同参数续跑；不要同时启动两个写入进程。
