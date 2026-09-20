# Market P10 versus good and bad current short candidates

Scope: current r48 + accel4 + ATR4/price + relative-volume TOP10 candidate events (5,615). Returns use the 30% hard stop and 0.3% round-trip cost. Good means net return > 0 (3,453, 61.50%); bad means net return <= 0 (2,162). market_r8_p10 was recomputed from full dynamic Top100 r8 cross-sections at each entry time; the other metrics are read from the prepared feature table.

## Distribution medians

|metric|good median|bad median|good minus bad|
|---|---:|---:|---:|
|market_r1_p10|-0.893%|-0.894%|+0.001%|
|market_r4_p10|-1.361%|-1.300%|-0.061%|
|market_r8_p10|-1.833%|-1.806%|-0.027%|
|market_r12_p10|-2.671%|-2.591%|-0.080%|
|market_r24_p10|-3.641%|-3.627%|-0.014%|

## Fixed training Q20/Q80 groups

Thresholds are computed from 2022-2024 candidate values and fixed for the full sample. Low means a more negative lower-tail market return; high means closer to zero or positive.

|metric|Q20|Q80|low good rate|high good rate|low mean net|high mean net|low severe bad|high severe bad|
|---|---:|---:|---:|---:|---:|---:|---:|---:|
|market_r1_p10|-1.423%|-0.115%|61.08%|60.83%|+1.11%|+0.95%|22.91%|17.41%|
|market_r4_p10|-1.989%|0.061%|62.55%|58.45%|+1.67%|-0.11%|22.63%|18.58%|
|market_r8_p10|-2.871%|0.239%|61.27%|60.12%|+1.34%|+0.22%|22.76%|16.16%|
|market_r12_p10|-4.181%|0.172%|61.63%|58.66%|+1.68%|-0.18%|22.62%|18.34%|
|market_r24_p10|-6.021%|-0.020%|60.61%|59.91%|+1.55%|-0.38%|23.17%|18.62%|

## Retrospective original r1 filter

|state|events|good rate|severe bad|stop rate|mean net|
|---|---:|---:|---:|---:|---:|
|pass [-1.5%,0%]|3442|61.88%|18.65%|4.79%|+0.95%|
|fail|2173|60.88%|21.21%|5.89%|+1.01%|

Exploratory descriptive output; no significance or tradability claim.