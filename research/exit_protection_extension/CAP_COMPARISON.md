# 12h / 24h / 48h cap comparison

All three variants use the same frozen signals, P90 threshold, fees, slippage, funding treatment, unified five-unit portfolio, and the same priority rule: a new long first evicts shorts from worse to better priority, then may evict an extension only after `E + 4h`. Only the maximum extension time changes.

## Portfolio comparison

| Maximum extension | Final equity | Final 20% return | Realized maximum drawdown | Positive-month ratio | Baseline-selected signals lost | Extension evictions |
|---|---:|---:|---:|---:|---:|---:|
| Baseline | 76,751.15 | 2634.36% | -35.705% | 79.63% | 0 | 0 |
| 12h | 113,721.28 | 4497.43% | -35.780% | 75.93% | 8 | 3 |
| 24h | 167,410.97 | 4553.23% | -35.780% | 75.93% | 8 | 3 |
| 48h | 107,054.98 | 3267.89% | -36.075% | 75.93% | 11 | 9 |

## Calendar-year return comparison

| Year | Baseline | 12h | 24h | 48h |
|---|---:|---:|---:|---:|
| 2022 | 358.27% | 355.20% | 399.41% | 409.84% |
| 2023 | 820.68% | 848.63% | 1218.73% | 1051.78% |
| 2024 | 997.72% | 845.19% | 801.39% | 793.19% |
| 2025 | 2445.45% | 2859.23% | 2658.46% | 2406.09% |
| 2026 through June | 551.03% | 841.55% | 922.34% | 714.46% |

## Decision

`24h` is the preferred cap among the tested choices. It has the highest final equity and final-20%-period return, the same drawdown and capacity cost as 12h, and beats the baseline in every available calendar year except 2024.

`48h` is a useful negative result: extending beyond 24h lowers final equity and final-period return, adds six more extension evictions and three more displaced baseline signals, worsens drawdown, and also trails the baseline in 2025. Do not test longer fixed caps without a new hypothesis.

This selects a research candidate only. It does not authorize or implement a testnet strategy change.
