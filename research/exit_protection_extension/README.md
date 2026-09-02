# Recent-protection exit extension study

This directory is intentionally isolated from the production strategy. It does not read `.env`, contact Binance, alter `src/`, or change the running testnet trader.

## Hypothesis

For a long that reaches its original planned exit `E`, extend the position only when its first completed-minute activation of the existing drawdown protection falls in `(E - 4h, E]`.

The extended position keeps the frozen hard-stop, P90 protection threshold, peak-update order, fees, slippage, funding treatment, and five-unit portfolio rules. It exits at the first of:

1. hard stop;
2. P90 protection exit;
3. `E + 12h` (`EXTENSION_CAP`).

When a later long needs capacity, the frozen long-priority sequence first evicts open shorts from worse to better priority. If capacity remains insufficient, it then closes an extended long only after that long has exceeded `E + 4h`, selecting the oldest eligible extension first. If no later long needs capacity, the extended long remains open until its normal protection exit or `E + 12h`.

Extended exits are not used as P90 training observations.

## Run

From the repository root, after the frozen research result and raw historical data are present:

```powershell
python research/exit_protection_extension/run_experiment.py
```

The run reads `results/local/research/` and writes only ignored files under `research/exit_protection_extension/results/recent_protection_extension_12h/`:

- `summary.json`: frozen inputs, rule, full-sample and final-20%-period metrics;
- `comparison.csv`: baseline versus variant portfolio metrics;
- `yearly_comparison.csv`: baseline versus variant returns and realized drawdown for each calendar year;
- `extended_trades.csv`: each actually extended trade and its final reason;
- variant trade, allocation-audit, and account-ledger files for inspection.

## Decision rule

Consider production work only if the variant improves the final chronological 20% after costs, does not materially worsen realized drawdown, and is not supported only by one calendar year or one trade. Capacity cost is reported as the number of signals selected in the baseline but no longer selected by the variant.

This study uses the frozen historical research entry convention. It does not change the separate, already accepted immediate-market-entry convention used by the live testnet engine.
