# All-hour entry-event study

This result uses the same Top100, five Top10 ranks, market filter, long fee and slippage as v1.7.0. It uses completed one-hour OHLC paths for a common all-hour event study; funding is intentionally not estimated in this first event screen because hourly bars cannot reproduce the strategy's settlement-minute proxy.

The 1/4/8/12/24h return columns are net of entry/exit slippage and taker fees. MFE/MAE are one-hour-bar extrema, and repeated signals are reported separately from first appearances.

| UTC hour | events | mean 11h | mean 17h | mean 18h |
|---:|---:|---:|---:|
| 0 | 561 | -0.912% | -1.190% | -1.300% |
| 1 | 609 | -1.441% | -2.161% | -1.958% |
| 2 | 536 | -1.116% | -0.977% | -0.982% |
| 3 | 524 | -1.639% | -1.889% | -1.976% |
| 4 | 515 | -0.036% | 1.064% | 1.397% |
| 5 | 527 | -0.684% | -0.537% | -0.396% |
| 6 | 579 | -1.375% | -0.565% | -0.579% |
| 7 | 563 | -0.396% | 0.470% | 0.260% |
| 8 | 584 | -0.278% | -0.195% | -0.258% |
| 9 | 583 | 0.135% | 0.357% | 0.421% |
| 10 | 598 | 0.505% | 0.771% | 0.912% |
| 11 | 603 | 1.549% | 1.911% | 1.906% |
| 12 | 627 | 0.097% | 0.948% | 0.789% |
| 13 | 635 | 0.821% | 1.224% | 1.202% |
| 14 | 625 | 2.025% | 2.687% | 2.763% |
| 15 | 641 | 1.696% | 2.689% | 2.468% |
| 16 | 691 | 0.850% | 1.710% | 1.555% |
| 17 | 654 | 1.905% | 1.798% | 2.141% |
| 18 | 627 | 0.815% | -0.075% | -0.405% |
| 19 | 585 | 0.628% | 0.427% | 0.161% |
| 20 | 538 | 0.782% | 0.745% | 0.483% |
| 21 | 540 | -0.735% | -1.776% | -1.773% |
| 22 | 529 | -0.069% | -0.586% | -0.369% |
| 23 | 529 | -0.693% | -1.296% | -1.244% |
