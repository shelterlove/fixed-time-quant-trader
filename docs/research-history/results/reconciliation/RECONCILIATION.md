# Research reconciliation ? after signed rank-change repair

Scope: `RESEARCH_2022_2026H1` / `LONG_PRIORITY_SKIP`.

## Signal semantic keys

- Legacy allocation audit: 2,436 rows, comprising 2,423 unique semantic keys and 13 duplicate rows.
- New frozen signals: 2,347 unique keys.
- Common keys: 2,345; legacy-only: 78; new-only: 2.
- Short signals are now reconciled: all 501 unique legacy short keys are common. New has one additional short key (`OGNUSDT`, 2023-07-29 06:00 UTC).
- Long signals: 1,844 common, 78 legacy-only, and 1 new-only. Of the 78 legacy-only rows, 38 are explicitly labelled `LONG_TIME_SLOT_CAP`; the new implementation prefilters this condition before writing its signal set, so those rows are not semantically equivalent to new frozen candidate rows.

## Trade semantic keys

- Legacy trades: 1,952; new trades: 1,940.
- Common: 1,930; legacy-only: 22; new-only: 10.
- All common trades have identical entry time, planned exit time, and entry reference price.
- Actual exit time differs for 6 common trades and exit price for 8. Exit reason labels differ mostly by naming convention, but 1,452 common trades have a different net return and all common trade PnLs/notionals differ after their account paths diverge.

## Capacity events

- Short duplicate-open counts now match exactly: 13 legacy and 13 new.
- Remaining capacity differences are: long duplicate-open 399 vs 386, long no-capacity 13 vs 2, short no-funds 8 vs 6, and short evictions 13 vs 8.
- The legacy-only `LONG_TIME_SLOT_CAP` status has no persisted new-side counterpart because the new signal layer already applies the time-slot cap.

## Headline metrics: not attributed

Signal and trade key sets are close but not identical, so monthly attribution remains gated under the requested sequence. The raw headline values are retained for reference only:

| Metric | Legacy | New |
|---|---:|---:|
| Final equity | 75,338.328973 | 66,653.380813 |
| Profit factor | 2.074575 | 1.871439 |
| Realized maximum drawdown | -34.0844% | -34.2121% |

## Conclusion and recommendation

The unsigned-underflow bug in `r4_rank_change` was the primary short-signal discrepancy: it is fixed, and short semantic keys now reconcile except for one extra new candidate.

The remaining mismatch begins with long audit/candidate convention (`LONG_TIME_SLOT_CAP`) and a small residual set of long candidates, then propagates into admission, notional, and PnL differences. It is not evidence that the shared-capital account engine was the original fault.

Do not change strategy parameters or account logic to force equality. If exact legacy reproduction is required, the next focused read-only investigation should compare the 40 non-time-slot-cap legacy-only long signals and the two new-only signals at their factor/selection stage, then compare the 6 common-trade exit-time and 8 exit-price differences before any monthly attribution.

No code, parameters, raw data, or external windows were changed by this reconciliation.
