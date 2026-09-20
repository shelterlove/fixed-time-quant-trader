# 新上市合约过滤研究

本研究只读复用仓库行情，并将新增下载和全部结果写在本目录。它不读取交易账户、不提交订单，也不修改产品策略。

预先定义四组：

- `baseline`：当前规则；
- `exclude_age_gt2d_le7d`：排除决策时上市年龄 `>= 2天且 <= 7天` 的合约；
- `exclude_age_le7d`：排除上市年龄 `<= 7天` 的合约；
- `exclude_age_le30d`：排除上市年龄 `<= 30天` 的合约。

上市时间使用 `data/raw/symbols.parquet` 的首根 USDⓈ-M 永续合约小时线。过滤在 Top100 和全部横截面排名之前执行；滚动因子仍使用原始连续行情，过滤期不会人为破坏后续 24 小时窗口。

运行：

```powershell
python research/listing_age_filter/study.py
python -m pytest -q research/listing_age_filter/test_study.py
```

结果写入 `results/`，结论见运行后生成的 `REPORT.md`。
