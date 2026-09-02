# Live 24-hour long-extension rule

This document applies only to the Binance Futures testnet execution layer. `strategy.toml` and the frozen historical research baseline remain unchanged.

## Rule

For a new live long position, the engine records the first completed minute that activates the existing +30% drawdown-protection state.

At its original planned exit `E`, the engine first processes every completed one-minute protection bar. It extends the long only when all of the following hold:

1. P90 protection has not already exited it;
2. the recorded first activation lies in `(E - 4h, E]`;
3. the exchange-managed hard stop has not closed it.

The extension changes the scheduled time exit to `E + 24h`. P90 protection continues minute-by-minute throughout the extension; the original exchange hard stop remains active. At `E + 24h`, any remaining position is closed by market order with reason `EXTENSION_CAP`.

## Capacity order

When a new long needs capacity, the engine closes positions in this order:

1. open shorts from worse to better priority;
2. only if capacity is still insufficient, extended longs whose release time `E + 4h` is already past, oldest release first.

Normal longs and extensions that have not passed `E + 4h` are not displaced by this rule.

## Upgrade behavior

The SQLite migration adds durable scheduling and activation fields. A position already open at the time of upgrade keeps its original scheduled exit. If it was already in protection, its first activation time is unknowable, so it is deliberately **not** extended. The rule applies fully to positions opened after deployment.

## Testnet deployment

Deploy only at a non-decision minute:

```bash
cd ~/fixed-time-quant-trader
git pull --ff-only
git describe --tags --always
./deploy.sh
docker compose ps
```

The dashboard's current-position JSON now includes `scheduled_exit_time`, `protection_activated_at`, `extension_active`, `extension_release_time`, and `extension_deadline_time`.
