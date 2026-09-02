# Latest result: recent-protection extension (4h)

This is a research result only. It has not changed the testnet trader, P90 history, production code, or deployment configuration.

## Frozen inputs

- Code release: `v1.3.3`; frozen strategy configuration: `1.1.0`.
- Historical research window: 2022-01-01 through 2026-07-01 UTC.
- Baseline: 1,922 long candidates, 502 short candidates, and the existing unified five-unit portfolio replay.
- Variant: a long is extended only if it normally reaches its planned exit and first activates protection in the preceding four hours. It is then still subject to the existing hard stop and frozen P90 protection, with a forced exit after four additional hours.

## Result

- 129 long candidates had both a normal planned exit and a completed base-shadow activation; 41 met the recent-activation condition and were actually extended.
- Of those 41, four subsequently exited by P90 protection and 37 reached the four-hour cap. No extended trade hit its hard stop.
- Full-sample final equity changed from `76,751.15` to `96,804.30` (+26.13% relative). Realized maximum drawdown changed from `-35.7052%` to `-35.7802%` (0.075 percentage points worse).
- In the final chronological 20% of the research period, account return was `26.34x` for the baseline and `36.93x` for the variant; realized drawdown was effectively equal at `-35.7052%`.
- Exactly one signal selected in the baseline was no longer selected under the variant, so observed capacity cost was low in this sample.
- The largest positive per-trade portfolio PnL difference was 18.81% of all positive PnL differences; the gain was not dominated by a single trade under this check.

The reproducible local artifacts are ignored under `results/recent_protection_extension_4h/`. Re-run `python research/exit_protection_extension/run_experiment.py` to regenerate them.

## Interpretation

The fixed, single 4h variant passes the planned directional checks in this historical run, while slightly worsening full-sample realized drawdown and reducing the positive-month ratio. It is therefore a candidate for a separate production-integration decision, not an automatic strategy change.
