# Peak-profit P90 ladders

Baseline path reproduction: passed (1,922 long paths, zero mismatch).

| variant | cost | final equity change vs D3 | marked drawdown change |
|---|---:|---:|---:|
| P4_pure_80_60 | 1.0x | +2.49% | -3.02% |
| P4_pure_80_60 | 1.5x | +8.77% | -3.08% |
| P4_pure_80_60 | 2.0x | +10.75% | -2.79% |
| P5_80_60_with_locks | 1.0x | +8.97% | -3.02% |
| P5_80_60_with_locks | 1.5x | +16.26% | -3.08% |
| P5_80_60_with_locks | 2.0x | +16.02% | -2.79% |
| P6_waterfall | 1.0x | +17.18% | -1.83% |
| P6_waterfall | 1.5x | +22.58% | -2.24% |
| P6_waterfall | 2.0x | +22.65% | -1.91% |
| P7_locks_only | 1.0x | +18.70% | +0.00% |
| P7_locks_only | 1.5x | +21.04% | +0.00% |
| P7_locks_only | 2.0x | +19.72% | +0.00% |

All triggers use the previous completed-minute peak; tiers only tighten and do not relax. Research-only: no strategy.toml, production source, or testnet configuration changes.