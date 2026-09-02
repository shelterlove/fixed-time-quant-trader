# Latest result: recent-protection extension (24h)

This is a research result only. It has not changed the testnet trader, P90 history, production code, or deployment configuration.

## Frozen inputs

- Code release: `v1.3.3`; frozen strategy configuration: `1.1.0`.
- Historical research window: 2022-01-01 through 2026-07-01 UTC.
- Baseline: 1,922 long candidates, 502 short candidates, and the existing unified five-unit portfolio replay.
- Variant: a long is extended only if it normally reaches its planned exit and first activates protection in the preceding four hours. It remains subject to the existing hard stop and frozen P90 protection, with a forced exit after 24 additional hours.
- When a new long needs capacity, open shorts are still evicted from worse to better priority first. Only if more capacity is required can an extended long that is already beyond `E + 4h` be closed; the oldest eligible extension gives way first.

## Result

- 129 long candidates had both a normal planned exit and a completed base-shadow activation; 41 met the recent-activation condition and were actually extended.
- Of those 41, ten subsequently exited by P90 protection and 31 reached the 24-hour cap. Three were closed early to admit later longs after short evictions had been exhausted. No extended trade hit its hard stop.
- Full-sample final equity changed from `76,751.15` to `167,410.97` (+118.12% relative). Realized maximum drawdown changed from `-35.7052%` to `-35.7802%` (0.075 percentage points worse).
- In the final chronological 20% of the research period, account return was `26.34x` for the baseline and `45.53x` for the variant; realized drawdown was effectively equal at `-35.7052%`.
- Eight signals selected in the baseline were no longer selected under the variant. This is the observed capacity cost of holding the extensions longer.
- The largest positive per-trade portfolio PnL difference was 17.91% of all positive PnL differences; the gain was not dominated by a single trade under this check.

The reproducible local artifacts are ignored under `results/recent_protection_extension_24h/`. Re-run `python research/exit_protection_extension/run_experiment.py` to regenerate them.

## Interpretation

Annual results are mixed: the variant trails in 2024, while improving 2022, 2023, 2025, and the available 2026 period. It also slightly worsens full-sample realized drawdown and reduces the positive-month ratio. It is therefore a candidate for further validation, not an automatic strategy change.
