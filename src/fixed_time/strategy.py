from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
from numpy.lib.stride_tricks import sliding_window_view

from .config import StrategyConfig


BAR_COLUMNS = ["symbol", "open_time", "open", "high", "low", "close", "quote_volume", "trade_count"]


@dataclass(frozen=True)
class Admission:
    candidate: dict
    target_notional: Decimal


def _lag(a: np.ndarray, k: int) -> np.ndarray:
    out = np.full_like(a, np.nan, dtype=float)
    out[k:] = a[:-k]
    return out


def _window(a: np.ndarray, n: int, operation: str = "sum") -> np.ndarray:
    out = np.full_like(a, np.nan, dtype=float)
    if len(a) >= n:
        out[n - 1:] = getattr(np, operation)(sliding_window_view(a, n, axis=0), axis=-1)
    return out


def _ratio(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(np.isfinite(b) & (b > 0), a / b, np.nan)


def _ordinal(values: np.ndarray, eligible: np.ndarray) -> np.ndarray:
    """Descending ordinal rank. Columns must already be symbol ascending."""
    valid = eligible & np.isfinite(values)
    ordering = np.argsort(np.where(valid, -values, np.inf), axis=1, kind="stable")
    ranks = np.argsort(ordering, axis=1) + 1
    return np.where(valid, ranks, 0)


def _quantile(values: np.ndarray, pool: np.ndarray, q: float, *, half_even: bool = False) -> np.ndarray:
    out = np.full(len(values), np.nan)
    for i, (row, mask) in enumerate(zip(values, pool)):
        valid = np.sort(row[mask & np.isfinite(row)])
        if len(valid):
            raw = q * (len(valid) - 1)
            index = int(round(raw)) if half_even else int(np.floor(raw + .5))
            out[i] = valid[index]
    return out


def _dense(frame: pl.DataFrame) -> tuple[np.ndarray, list[str], dict[str, np.ndarray]]:
    if frame.is_empty():
        raise ValueError("hourly snapshot is empty")
    clean = frame.select(BAR_COLUMNS).sort(["open_time", "symbol"])
    if clean.select(pl.struct("symbol", "open_time").is_duplicated().any()).item():
        raise ValueError("duplicate hourly bar")
    invalid = clean.filter(
        ~pl.all_horizontal([pl.col(c).is_finite().fill_null(False) for c in BAR_COLUMNS[2:]])
        | (pl.col("low") <= 0) | (pl.col("high") < pl.max_horizontal("open", "close", "low"))
        | (pl.col("low") > pl.min_horizontal("open", "close"))
        | (pl.col("quote_volume") < 0) | (pl.col("trade_count") < 0)
        | (pl.col("open_time").dt.epoch("us") % 3_600_000_000 != 0)
    )
    if invalid.height:
        raise ValueError("invalid or unaligned hourly bars")
    hours_raw = clean["open_time"].dt.epoch("s").to_numpy() // 3600 + 1
    lo, hi = int(hours_raw.min()), int(hours_raw.max()) + 1
    hours = np.arange(lo, hi, dtype=np.int64)
    symbols = sorted(clean["symbol"].unique().to_list())
    smap = {symbol: i for i, symbol in enumerate(symbols)}
    ti = hours_raw - lo
    si = np.fromiter((smap[s] for s in clean["symbol"]), dtype=np.int32)
    arrays: dict[str, np.ndarray] = {}
    for column in BAR_COLUMNS[2:]:
        values = np.full((len(hours), len(symbols)), np.nan)
        values[ti, si] = clean[column].to_numpy()
        arrays[column] = values
    return hours, symbols, arrays


def _features(grids: dict[str, np.ndarray]) -> tuple[dict, dict, dict, dict]:
    c, v, high, low = (grids[k] for k in ("close", "quote_volume", "high", "low"))
    finite = np.isfinite(c).astype(float)
    f: dict[str, np.ndarray] = {"v1": v}
    for k in (1, 4, 24, 48):
        f[f"r{k}"] = np.where(_window(finite, k + 1) == k + 1, _ratio(c, _lag(c, k)) - 1, np.nan)
    for k in (4, 24):
        f[f"v{k}"] = _window(v, k)
    logs = np.log(c)
    f["slope24"] = np.full_like(c, np.nan)
    if len(c) >= 25:
        f["slope24"][24:] = np.einsum("ijk,k->ij", sliding_window_view(logs, 25, axis=0), np.arange(25) - 12) / 1300
    f["v1_vs23"] = _ratio(v, _window(_lag(v, 1), 23) / 23)
    f["volume_vs_prev24_median"] = _ratio(v, _window(_lag(v, 1), 24, "median"))
    f["accel4"] = np.where(_window(finite, 9) == 9, logs - 2 * _lag(logs, 4) + _lag(logs, 8), np.nan)
    tr = np.maximum(high - low, np.maximum(abs(high - _lag(c, 1)), abs(low - _lag(c, 1))))
    f["atr_ratio4"] = _ratio(_window(tr, 4) / 4, c)
    f["volume_diff_v1"] = v - _lag(v, 1)
    f["volume_diff_v4"] = f["v4"] - _lag(f["v4"], 4)
    basic = np.isfinite(f["v24"]) & (v > 0) & (grids["trade_count"] > 0) & np.isfinite(c)
    pools = {}
    for name, eligible in (("R", basic), ("P", basic & np.isfinite(f["r24"])), ("H", basic & np.isfinite(f["r4"]))):
        rank = _ordinal(f["v24"], eligible)
        pools[name] = (rank > 0) & (rank <= 100)
    ranks = {name: {factor: _ordinal(values, pool) for factor, values in f.items()} for name, pool in pools.items() if name != "H"}
    historical_r4 = _ordinal(f["r4"], pools["H"]).astype(float)
    historical_r4[historical_r4 == 0] = np.nan
    f["r4_rank_change"] = _lag(historical_r4, 4) - np.where(ranks["P"]["r4"] > 0, ranks["P"]["r4"], np.nan)
    ranks["P"]["r4_rank_change"] = _ordinal(f["r4_rank_change"], pools["P"])
    market = {f"r{k}_p50": _quantile(f[f"r{k}"], pools["R"], .5, half_even=True) for k in (1, 4, 48)}
    market.update({f"r{k}_p10": _quantile(f[f"r{k}"], pools["P"], .1) for k in (1, 24)})
    return f, pools, ranks, market


def planned_exit(decision: datetime, source: str) -> datetime:
    decision = decision.astimezone(UTC)
    if source in {"MAIN", "A", "C", "MAIN|A", "MAIN|C", "A|C", "MAIN|A|C"}:
        hour = 4 if decision.hour == 17 else 8
        return (decision.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).replace(hour=hour)
    if source == "ORIGINAL_06":
        return decision.replace(hour=20, minute=0, second=0, microsecond=0)
    local = decision.astimezone(ZoneInfo("America/New_York"))
    target = local.replace(hour=10, minute=0, second=0, microsecond=0)
    if target <= local:
        target += timedelta(days=1)
    return target.astimezone(UTC)


def candidates(hourly: pl.DataFrame, decision: datetime, config: StrategyConfig) -> list[dict]:
    decision = decision.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    hours, symbols, grids = _dense(hourly.filter(pl.col("open_time") < pl.lit(decision)))
    target_h = int(decision.timestamp() // 3600)
    matches = np.flatnonzero(hours == target_h)
    if len(matches) != 1:
        raise ValueError(f"snapshot does not contain the completed hour ending {decision.isoformat()}")
    i = int(matches[0])
    f, pools, ranks, market = _features(grids)
    return _select_candidates(i, symbols, grids, ranks, market, decision, config)


def candidate_history(hourly: pl.DataFrame, start: datetime, end: datetime, config: StrategyConfig) -> list[dict]:
    """Evaluate the live signal kernel in bounded, causal historical batches."""
    hours, symbols, grids = _dense(hourly.filter(pl.col("open_time") < pl.lit(end)))
    _, _, ranks, market = _features(grids)
    result = []
    for i, hour in enumerate(hours):
        decision = datetime.fromtimestamp(int(hour) * 3600, UTC)
        if start <= decision < end and decision.hour in config.values["features"]["strategy_decision_hours_utc"]:
            result.extend(_select_candidates(i, symbols, grids, ranks, market, decision, config))
    return result


def _select_candidates(i, symbols, grids, ranks, market, decision, config) -> list[dict]:

    def top(pool: str, factors: tuple[str, ...], n: int) -> np.ndarray:
        return np.logical_and.reduce([(ranks[pool][factor][i] > 0) & (ranks[pool][factor][i] <= n) for factor in factors])

    def row(j: int, direction: int, source: str, priority: tuple) -> dict:
        strategy = "long" if direction == 1 else "short"
        identity = f"{config.version}:{strategy}:{source}:{symbols[j]}:{decision.isoformat()}"
        return {"trade_id": identity, "strategy": strategy, "position_side": strategy.upper(),
                "symbol": symbols[j], "source": source, "decision_time": decision,
                "entry_time": decision + timedelta(minutes=config.values["execution"]["entry_delay_minutes"]),
                "planned_exit_time": planned_exit(decision, source), "reference_price": float(grids["close"][i, j]),
                "priority": tuple(x.item() if isinstance(x, np.generic) else x for x in priority)}

    result: list[dict] = []
    if decision.hour in (14, 15, 17):
        main = top("P", ("r1", "r4", "r24", "v1", "v4"), 10) & (market["r24_p10"][i] > -.05)
        a = top("R", ("slope24", "v1", "v1_vs23"), 5) & (decision.hour != 17)
        c = top("R", ("r4", "slope24", "v1"), 5) & (decision.hour != 17)
        for j in np.flatnonzero(main | a | c):
            source = "|".join(name for name, mask in (("MAIN", main), ("A", a), ("C", c)) if mask[j])
            result.append(row(int(j), 1, source, (symbols[j],)))
    elif decision.hour in (0, 1, 2):
        hit = top("R", ("r48", "accel4", "atr_ratio4", "volume_vs_prev24_median"), 10)
        passed = market["r48_p50"][i] < 0 and (market["r1_p50"][i] < 0 or market["r4_p50"][i] < 0)
        if passed:
            for j in np.flatnonzero(hit):
                result.append(row(int(j), -1, "NEW_SHORT", (symbols[j],)))
    elif decision.hour == 6:
        r = ranks["P"]
        hit = top("P", ("r24",), 10) & (r["r4_rank_change"][i] >= 91) & (r["r4_rank_change"][i] <= 100)
        hit &= top("P", ("volume_diff_v1",), 5) | top("P", ("volume_diff_v4",), 5)
        hit &= -.015 <= market["r1_p10"][i] <= 0
        def priority(j: int) -> tuple:
            volume = min(r["volume_diff_v1"][i, j], r["volume_diff_v4"][i, j])
            return (r["r24"][i, j] + 100 - r["r4_rank_change"][i, j] + volume,
                    r["r24"][i, j], -r["r4_rank_change"][i, j], volume, symbols[j])
        for j in sorted((int(x) for x in np.flatnonzero(hit)), key=priority)[:2]:
            result.append(row(j, -1, "ORIGINAL_06", priority(j)))
    return sorted(result, key=lambda item: item["priority"])


def entry_rejection(row: dict, lots: list[dict], bought: set[str], stopped: set[str]) -> str | None:
    """One position-admission policy for live execution and historical replay."""
    symbol, direction = row["symbol"], row["strategy"]
    same = any(x["symbol"] == symbol and x["strategy"] == direction for x in lots)
    if direction == "short":
        return "SAME_SHORT_OPEN" if same else None
    add17 = row["decision_time"].hour == 17 and "MAIN" in row["source"].split("|") and same
    if symbol in stopped:
        return "LONG_STOP_DAY_LOCK"
    if same and not add17:
        return "SAME_LONG_OPEN"
    if symbol in bought and not add17:
        return "LONG_BOUGHT_DAY_LOCK"
    return None


def requested_total(direction: str, hour: int, budget: Decimal, remaining: Decimal, has_positions: bool) -> Decimal:
    if (direction == "long" and hour == 14) or (direction == "short" and hour == 0):
        request = budget / Decimal(2)
    elif not has_positions:
        request = budget * Decimal(2) / Decimal(3)
    elif direction == "short" and hour == 1:
        request = budget / Decimal(2)
    else:
        request = budget
    return min(request, remaining)


def admissions(eligible: list[dict], open_lots: list[dict], e0: Decimal, equity: Decimal, config: StrategyConfig) -> list[Admission]:
    if not eligible or e0 <= 0 or equity <= 0:
        return []
    direction = str(eligible[0]["strategy"])
    if any(row["strategy"] != direction for row in eligible):
        raise ValueError("one decision batch must contain one direction")
    occupied_direction = sum((Decimal(str(p["entry_notional"])) for p in open_lots if p["strategy"] == direction), Decimal())
    occupied_total = sum((Decimal(str(p["entry_notional"])) for p in open_lots), Decimal())
    budget = e0 * Decimal(str(config.values["allocation"]["direction_budget_fraction"]))
    remaining = max(Decimal(), budget - occupied_direction)
    free = max(Decimal(), min(e0, equity) - occupied_total)
    request = requested_total(direction, eligible[0]["decision_time"].hour, budget, remaining,
                              any(p["strategy"] == direction for p in open_lots))
    fee = Decimal(str(config.values["execution"]["taker_fee_per_side"]))
    slip = Decimal(str(config.values["execution"]["slippage_per_side"]))
    sign = Decimal(1 if direction == "long" else -1)
    entry_drag = fee - sign * (Decimal(1) / (Decimal(1) + sign * slip) - Decimal(1))
    actual = min(request, free / (Decimal(1) + entry_drag))
    each = actual / Decimal(len(eligible))
    return [Admission(row, each) for row in eligible] if each > 0 else []
