# UTC00/01/02 空头信号质量研究结果

全部5,615个当前候选统一按20%硬止损和0.3%往返成本重算。阈值仅由2022—2024生成；2021和2025—2026用于跨期观察。账户仍在下一纽约10:00计划退出，未计funding。

## 训练期基础质量

|事件|平均净收益|日期等权收益|胜率|严重坏交易|20%止损|亏损≤-10%|
|---:|---:|---:|---:|---:|---:|---:|
|2901|0.75%|0.72%|61.50%|16.55%|6.00%|8.82%|

## 由训练期单条件固定的短名单

币级条件：directed_body4__LOW, upper_shadow4__HIGH, range_position4__LOW  
市场条件：REGIME_R48_NEG_R1_NEG, market_r4_positive__LOW

|条件|事件|平均净收益|日期等权|胜率|严重坏交易|止损率|利润因子|
|---|---:|---:|---:|---:|---:|---:|---:|
|upper_shadow4__HIGH|581|1.51%|1.45%|66.44%|13.43%|5.16%|1.7123|
|range_position4__LOW|581|1.38%|1.28%|65.58%|13.77%|5.51%|1.6250|
|directed_body4__LOW|581|1.45%|1.52%|64.72%|12.91%|4.30%|1.6964|
|market_r4_positive__LOW|532|1.19%|1.26%|64.47%|14.10%|4.32%|1.5421|
|REGIME_R48_NEG_R1_NEG|584|1.34%|1.43%|63.36%|14.04%|4.28%|1.5990|

## 固定组合的训练期结果

|条件|事件|平均净收益|日期等权|严重坏交易|止损率|通过质量目标|
|---|---:|---:|---:|---:|---:|---:|
|directed_body4__LOW + REGIME_R48_NEG_R1_NEG|186|1.22%|1.10%|16.13%|4.30%|否|
|directed_body4__LOW + market_r4_positive__LOW|244|1.27%|1.16%|13.11%|3.69%|否|
|upper_shadow4__HIGH + REGIME_R48_NEG_R1_NEG|163|1.36%|1.25%|15.95%|3.68%|否|
|upper_shadow4__HIGH + market_r4_positive__LOW|198|1.32%|1.47%|14.14%|4.04%|否|
|range_position4__LOW + REGIME_R48_NEG_R1_NEG|166|1.00%|0.86%|16.87%|4.82%|否|
|range_position4__LOW + market_r4_positive__LOW|200|1.14%|1.18%|14.50%|4.50%|否|
|directed_body4__LOW + upper_shadow4__HIGH|391|1.54%|1.42%|12.28%|3.84%|是|
|directed_body4__LOW + range_position4__LOW|396|1.40%|1.28%|12.63%|4.29%|是|
|upper_shadow4__HIGH + range_position4__LOW|568|1.44%|1.34%|13.73%|5.28%|否|

严格达到预设质量目标的版本：directed_body4__LOW + upper_shadow4__HIGH, directed_body4__LOW, directed_body4__LOW + range_position4__LOW, REGIME_R48_NEG_R1_NEG, market_r4_positive__LOW。

## 通过版本的跨期结果

|条件|时期|事件|平均净收益|日期等权|止损率|严重坏交易|
|---|---|---:|---:|---:|---:|---:|
|directed_body4__LOW|2021|228|0.68%|0.13%|9.65%|24.12%|
|directed_body4__LOW|2025_2026|331|1.06%|1.34%|19.94%|29.61%|
|directed_body4__LOW|2026H1|102|0.26%|0.16%|19.61%|32.35%|
|directed_body4__LOW|2026JA|24|4.71%|3.14%|12.50%|29.17%|
|directed_body4__LOW|2026SEP|1|18.93%|18.93%|0.00%|0.00%|
|REGIME_R48_NEG_R1_NEG|2021|279|0.30%|-0.28%|10.04%|23.30%|
|REGIME_R48_NEG_R1_NEG|2025_2026|332|2.64%|2.99%|18.67%|27.71%|
|REGIME_R48_NEG_R1_NEG|2026H1|104|2.68%|3.18%|22.12%|31.73%|
|REGIME_R48_NEG_R1_NEG|2026JA|19|12.17%|13.12%|5.26%|21.05%|
|REGIME_R48_NEG_R1_NEG|2026SEP|3|9.61%|9.04%|0.00%|0.00%|
|market_r4_positive__LOW|2021|334|0.33%|-0.06%|11.68%|23.65%|
|market_r4_positive__LOW|2025_2026|254|1.43%|2.00%|18.90%|29.13%|
|market_r4_positive__LOW|2026H1|78|3.08%|4.10%|19.23%|29.49%|
|market_r4_positive__LOW|2026JA|10|-1.68%|-1.68%|30.00%|50.00%|
|market_r4_positive__LOW|2026SEP|5|1.46%|-0.86%|20.00%|20.00%|
|directed_body4__LOW + upper_shadow4__HIGH|2021|142|0.39%|-0.53%|11.27%|26.06%|
|directed_body4__LOW + upper_shadow4__HIGH|2025_2026|249|0.98%|0.78%|20.88%|30.12%|
|directed_body4__LOW + upper_shadow4__HIGH|2026H1|81|1.82%|1.54%|17.28%|25.93%|
|directed_body4__LOW + upper_shadow4__HIGH|2026JA|20|4.70%|2.29%|15.00%|35.00%|
|directed_body4__LOW + upper_shadow4__HIGH|2026SEP|0|—|—|—|—|
|directed_body4__LOW + range_position4__LOW|2021|144|0.17%|-0.82%|11.11%|27.08%|
|directed_body4__LOW + range_position4__LOW|2025_2026|255|1.00%|0.79%|20.78%|30.20%|
|directed_body4__LOW + range_position4__LOW|2026H1|84|1.22%|1.15%|19.05%|28.57%|
|directed_body4__LOW + range_position4__LOW|2026JA|21|4.96%|2.79%|14.29%|33.33%|
|directed_body4__LOW + range_position4__LOW|2026SEP|0|—|—|—|—|

## 固定账户回放

|条件|配置|成交|最终权益|最大回撤|止损成交|
|---|---|---:|---:|---:|---:|
|BASE|10仓/每时点3仓|3936|12.3868x|37.97%|449|
|BASE|5仓/每时点2仓|3804|77.5776x|61.83%|439|
|directed_body4__LOW + upper_shadow4__HIGH|10仓/每时点3仓|686|1.7046x|16.51%|74|
|directed_body4__LOW + upper_shadow4__HIGH|5仓/每时点2仓|681|2.8130x|31.86%|73|
|directed_body4__LOW|10仓/每时点3仓|986|2.8848x|16.63%|96|
|directed_body4__LOW|5仓/每时点2仓|978|7.2843x|32.18%|95|
|directed_body4__LOW + range_position4__LOW|10仓/每时点3仓|693|1.6999x|17.91%|74|
|directed_body4__LOW + range_position4__LOW|5仓/每时点2仓|689|2.7231x|34.13%|73|
|REGIME_R48_NEG_R1_NEG|10仓/每时点3仓|1027|3.8252x|16.79%|104|
|REGIME_R48_NEG_R1_NEG|5仓/每时点2仓|992|13.8566x|30.09%|100|
|market_r4_positive__LOW|10仓/每时点3仓|925|2.6108x|13.26%|93|
|market_r4_positive__LOW|5仓/每时点2仓|883|6.7345x|28.17%|88|

## 10仓账户分期

|条件|时期|收益|最大回撤|
|---|---|---:|---:|
|BASE|2021|-0.12%|37.97%|
|BASE|2022|108.67%|9.84%|
|BASE|2023|62.89%|14.58%|
|BASE|2024|21.74%|30.97%|
|BASE|2025|60.08%|23.22%|
|BASE|2026H1|45.08%|22.42%|
|BASE|2026JA|38.11%|10.87%|
|BASE|2026SEP|-6.55%|9.39%|
|directed_body4__LOW + upper_shadow4__HIGH|2021|-0.02%|16.51%|
|directed_body4__LOW + upper_shadow4__HIGH|2022|23.94%|6.73%|
|directed_body4__LOW + upper_shadow4__HIGH|2023|15.22%|4.31%|
|directed_body4__LOW + upper_shadow4__HIGH|2024|11.94%|3.10%|
|directed_body4__LOW + upper_shadow4__HIGH|2025|-8.22%|16.42%|
|directed_body4__LOW + upper_shadow4__HIGH|2026H1|13.44%|11.71%|
|directed_body4__LOW + upper_shadow4__HIGH|2026JA|2.43%|5.06%|
|directed_body4__LOW|2021|11.29%|16.63%|
|directed_body4__LOW|2022|42.01%|4.57%|
|directed_body4__LOW|2023|22.52%|4.93%|
|directed_body4__LOW|2024|18.86%|6.37%|
|directed_body4__LOW|2025|14.68%|12.97%|
|directed_body4__LOW|2026H1|2.76%|13.05%|
|directed_body4__LOW|2026JA|4.38%|5.06%|
|directed_body4__LOW|2026SEP|1.89%|1.38%|
|directed_body4__LOW + range_position4__LOW|2021|-3.02%|17.91%|
|directed_body4__LOW + range_position4__LOW|2022|24.77%|5.99%|
|directed_body4__LOW + range_position4__LOW|2023|14.97%|4.33%|
|directed_body4__LOW + range_position4__LOW|2024|8.67%|4.96%|
|directed_body4__LOW + range_position4__LOW|2025|-1.77%|13.82%|
|directed_body4__LOW + range_position4__LOW|2026H1|10.63%|12.15%|
|directed_body4__LOW + range_position4__LOW|2026JA|3.48%|5.06%|
|REGIME_R48_NEG_R1_NEG|2021|-5.34%|16.79%|
|REGIME_R48_NEG_R1_NEG|2022|54.14%|4.25%|
|REGIME_R48_NEG_R1_NEG|2023|28.18%|11.26%|
|REGIME_R48_NEG_R1_NEG|2024|3.55%|11.55%|
|REGIME_R48_NEG_R1_NEG|2025|28.58%|14.43%|
|REGIME_R48_NEG_R1_NEG|2026H1|28.29%|12.06%|
|REGIME_R48_NEG_R1_NEG|2026JA|16.37%|5.29%|
|REGIME_R48_NEG_R1_NEG|2026SEP|2.90%|1.98%|
|market_r4_positive__LOW|2021|6.47%|13.26%|
|market_r4_positive__LOW|2022|44.89%|5.06%|
|market_r4_positive__LOW|2023|10.11%|5.49%|
|market_r4_positive__LOW|2024|10.31%|8.03%|
|market_r4_positive__LOW|2025|10.80%|9.86%|
|market_r4_positive__LOW|2026H1|27.13%|5.07%|
|market_r4_positive__LOW|2026JA|-1.77%|4.87%|
|market_r4_positive__LOW|2026SEP|0.69%|3.89%|

## 口径与判断

质量目标要求训练期平均净收益提高至少0.3个百分点、日期等权改善、止损率相对下降至少25%、严重坏交易不增加、至少300事件和100日期，并要求2022—2024至少两年平均收益改善。未通过的版本不因账户收益较高而称为质量升级。
路径定价缺失0个，计划收益不一致0个；特征分区一次读取后复用。总耗时173.59秒。