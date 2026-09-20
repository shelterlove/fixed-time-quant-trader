# All-hour entry-event study

This result uses the same Top100, five Top10 ranks, market filter, long fee and slippage as v1.7.0. It uses completed one-hour OHLC paths for a common all-hour event study; funding is intentionally not estimated in this first event screen because hourly bars cannot reproduce the strategy's settlement-minute proxy.

The 1/4/8/12/24h return columns are net of entry/exit slippage and taker fees. MFE/MAE are one-hour-bar extrema, and repeated signals are reported separately from first appearances.

| UTC hour | events | mean 11h | mean 17h | mean 18h |
|---:|---:|---:|---:|
| 0 | 2 | -47.409% | -47.463% | -47.112% |
| 1 | 4 | -9.625% | -4.778% | -5.749% |
| 2 | 5 | -9.554% | -7.128% | -6.629% |
| 3 | 3 | -24.011% | -6.206% | -9.103% |
| 4 | 5 | -7.905% | 6.657% | 8.066% |
| 5 | 5 | -25.629% | -16.481% | -14.623% |
| 6 | 3 | 7.083% | 6.564% | 6.894% |
| 7 | 10 | -4.425% | 5.385% | 4.146% |
| 8 | 5 | -9.506% | -14.311% | -15.090% |
| 9 | 2 | 37.284% | 39.981% | 49.123% |
| 10 | 3 | 21.991% | 29.262% | 30.910% |
| 11 | 3 | 16.463% | 20.806% | 22.230% |
| 12 | 6 | 8.515% | 15.574% | 13.504% |
| 13 | 2 | -4.600% | -3.795% | -7.586% |
| 14 | 5 | 5.730% | 8.253% | 6.126% |
| 15 | 2 | -4.182% | -9.426% | -11.573% |
| 16 | 1 | -8.921% | 4.269% | 4.166% |
| 17 | 3 | 5.316% | 10.815% | 12.936% |
| 18 | 6 | 5.962% | 1.908% | 4.239% |
| 19 | 4 | 18.855% | 10.529% | 4.143% |
| 20 | 9 | -3.643% | -5.792% | -4.630% |
| 21 | 3 | 2.411% | 9.810% | 8.017% |
| 22 | 2 | 46.934% | 31.699% | 29.061% |
| 23 | 2 | 6.845% | 3.938% | 6.770% |
