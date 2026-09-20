# All-hour entry-event study

This result uses the same Top100, five Top10 ranks, market filter, long fee and slippage as v1.7.0. It uses completed one-hour OHLC paths for a common all-hour event study; funding is intentionally not estimated in this first event screen because hourly bars cannot reproduce the strategy's settlement-minute proxy.

The 1/4/8/12/24h return columns are net of entry/exit slippage and taker fees. MFE/MAE are one-hour-bar extrema, and repeated signals are reported separately from first appearances.

| UTC hour | events | mean 11h | mean 17h | mean 18h |
|---:|---:|---:|---:|
| 0 | 139 | 1.274% | 1.446% | 1.857% |
| 1 | 173 | 0.559% | 0.967% | 0.624% |
| 2 | 110 | -0.388% | -0.170% | -0.256% |
| 3 | 102 | 0.670% | 2.941% | 3.575% |
| 4 | 98 | 0.284% | 0.706% | 0.931% |
| 5 | 111 | -0.340% | -0.389% | -0.021% |
| 6 | 94 | 0.445% | 1.889% | 2.999% |
| 7 | 119 | 0.068% | 0.844% | 1.385% |
| 8 | 98 | -0.050% | 2.357% | 1.841% |
| 9 | 96 | -0.822% | 0.287% | 0.038% |
| 10 | 119 | 0.483% | 1.819% | 1.830% |
| 11 | 109 | 0.509% | 6.254% | 5.339% |
| 12 | 121 | 1.491% | 4.973% | 5.754% |
| 13 | 108 | 1.084% | 2.426% | 3.125% |
| 14 | 101 | 5.549% | 7.998% | 7.858% |
| 15 | 122 | 3.436% | 5.282% | 4.637% |
| 16 | 130 | 1.988% | 3.150% | 3.290% |
| 17 | 130 | 4.418% | 4.043% | 5.160% |
| 18 | 106 | 1.839% | 1.839% | 2.554% |
| 19 | 102 | 3.782% | 3.991% | 4.660% |
| 20 | 111 | 1.813% | 3.099% | 3.286% |
| 21 | 108 | 2.980% | 4.447% | 4.760% |
| 22 | 136 | 2.453% | 3.505% | 3.671% |
| 23 | 139 | 1.307% | 3.869% | 3.453% |
