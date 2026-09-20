# V1 14:00/15:00 allocation change — 2025 replay

All timestamps are UTC. This replay uses the cached hybrid trade paths and keeps the existing V1 three-unit limit, two-candidate hourly cap, 17:00 Main rule, protection logic, and same-day re-entry blocks.

## Results

| Variant | Final equity* | Max drawdown proxy | Lots | Units | Mean net return | Median | Win rate |
|---|---:|---:|---:|---:|---:|---:|---:|
| Existing V1 Union | 79.09× | 66.69% | 261 | 429 | 7.69% | −0.20% | 48.66% |
| 14/15 reallocation only | **92.23×** | **55.66%** | 268 | 362 | 7.65% | −0.12% | 49.25% |
| Full proposed change | 52.73× | 63.55% | 325 | 408 | 6.43% | −0.97% | 46.77% |
| Full change, no 14 top-ups | 73.31× | 61.72% | 279 | 367 | 7.71% | −0.48% | 48.03% |
| Full change, no cross-source adds | 70.89× | 56.04% | 314 | 403 | 6.34% | −0.63% | 47.77% |

*Final equity is from the realized-event cash sizing proxy, not a minute-marked account NAV. V1’s 79.09× means a simulated net return of 7,808.6%; the reallocation-only variant is 92.23×, or 9,123.2%.

## What was tested

- At 14:00, one structurally eligible candidate receives one unit; multiple candidates receive one each, up to two.
- At 15:00, one ordinary candidate requests two units; multiple candidates receive one each. If the coin already has an open 14:00 lot and the 15:00 signal adds a source that was absent at 14:00, it gets a separate one-unit lot.
- If the market has no 15:00 candidate at all, a still-open one-unit lot from a singleton 14:00 signal may receive a separate one-unit top-up at 15:00.
- At 17:00, the existing V1 Main-only rules remain in place, including same-symbol adds when capacity is free.

There were 51 possible no-15:00 top-ups; 50 were admitted and one parent lot was not open at 15:00. The 50 top-ups averaged −0.31% net, had a 44% win rate, and contributed −4.21 units of notional PnL in the account proxy. Of these, 10 hourly-ambiguous top-ups received targeted minute replay; all replayed successfully. The remaining top-ups use the hourly path approximation.

The 16 cross-source adds averaged +26.97% net, but their median was −5.40% and only 37.5% won. The mean is driven by a right tail. More importantly, the full version missed five trades that the existing V1 had selected, including four Main trades. Four of those misses were explicitly audited as `NO_CAPACITY`: at the arrival time all three units were already occupied. For example, on Apr 19, VOXEL's 14:00 lot, AERGO's 14:00 lot, and VOXEL's 15:00 cross-source add used all three units, so the 17:00 Main add to VOXEL could not enter. On Sep 12, HIFI 14:00, ARIA 14:00, and HIFI 15:00 cross-source add similarly filled the cap before the 17:00 Main add. On Mar 22, FARTCOIN 14:00, API3 14:00, and the API3 15:00 cross-source add filled the cap before UMA's 17:00 Main signal. Three missed Main trades were especially large winners: VOXELUSDT (+116.4%), HIFIUSDT (+171.0%), and ASTERUSDT (+49.9%).

The fifth missed trade, FUNUSDT A+C at Jun 21 14:00, was blocked as `DUPLICATE_OPEN`, not by the unit cap. The previous day's discretionary no-signal top-up had kept a FUN lot open until Jun 21 15:18 after its profit-protection extension; that top-up returned +35.2%, while the missed 14:00 candidate would have returned +2.5% net. This is a real same-symbol substitution caused by the top-up's longer holding period.

With both discretionary add rules removed, the 14/15 reallocation-only variant retained all 261 trades selected by existing V1 and admitted seven additional Main signals. It also used 67 fewer units over the year. In this replay, that produced the best result and lower drawdown; the gains came from reallocating the existing budget, not adding more 14:00/15:00 exposure.

## Interpretation and limits

The 2025 evidence supports testing the basic allocation change: reduce a singleton 14:00 entry to one unit and allow a singleton 15:00 signal to request two. It does not support automatically adding to the 14:00 lot when 15:00 is quiet, or adding again to the same coin at 15:00 on a new source. Both rules consumed capacity that later excluded stronger Main trades.

The unchanged V1 admission replay matched the saved V1 baseline exactly: 261 selected trade IDs and identical admission counts. All primary candidates had labels. The full variant has 216 hourly-proxy paths and 109 minute-exact paths; the proxy account uses realized event exits and does not represent minute-by-minute drawdown, and hourly labels omit actual funding on non-minute paths. This is a 2025 retrospective comparison, not a production change or an independent out-of-sample validation.
