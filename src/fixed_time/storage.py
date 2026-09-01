from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterable

import polars as pl


class DataError(ValueError):
    pass


KLINE_COLUMNS = ["symbol", "open_time", "open", "high", "low", "close", "quote_volume", "trade_count"]
FUNDING_COLUMNS = ["symbol", "funding_time", "funding_rate"]


def raw_path(root: Path, kind: str) -> Path:
    return root / "data" / "raw" / kind


def atomic_write_frame(target: Path, frame: pl.DataFrame) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=target.parent, suffix=target.suffix, delete=False) as temp:
        temporary = Path(temp.name)
    try:
        if target.suffix == ".parquet":
            frame.write_parquet(temporary)
        elif target.suffix == ".csv":
            frame.write_csv(temporary)
        else:
            raise DataError(f"unsupported frame output: {target}")
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_json(target: Path, payload: dict[str, Any], *, indent: int | None = None) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=target.parent, mode="w", encoding="utf-8", suffix=".json", delete=False) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, indent=indent, separators=None if indent else (",", ":"), default=str)
    try:
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_text(target: Path, content: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=target.parent, mode="w", encoding="utf-8", suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(content)
    try:
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def _check_klines(frame: pl.DataFrame, expected_symbol: str | None = None) -> None:
    if frame.columns != KLINE_COLUMNS:
        raise DataError(f"unexpected kline schema: {frame.columns}")
    if frame.is_empty():
        raise DataError("empty kline partition")
    if expected_symbol is not None and frame.get_column("symbol").unique().to_list() != [expected_symbol]:
        raise DataError(f"partition symbol mismatch: expected {expected_symbol}")
    duplicate = frame.select(pl.struct(["symbol", "open_time"]).is_duplicated().any()).item()
    if duplicate:
        raise DataError("duplicate (symbol, open_time)")
    invalid = frame.select(
        ((pl.col("open") <= 0) | (pl.col("high") <= 0) | (pl.col("low") <= 0) | (pl.col("close") <= 0)
         | (pl.col("low") > pl.min_horizontal("open", "close"))
         | (pl.col("high") < pl.max_horizontal("open", "close"))).any()
    ).item()
    if invalid:
        raise DataError("invalid OHLC relationship")


def write_kline_partition(root: Path, interval: str, symbol: str, frame: pl.DataFrame) -> Path:
    _check_klines(frame, symbol)
    first = frame.get_column("open_time").min()
    if interval == "1h":
        target = raw_path(root, "klines_1h") / f"symbol={symbol}" / f"year={first.year:04d}" / f"month={first.month:02d}" / "part.parquet"
    elif interval == "1m":
        target = raw_path(root, "klines_1m") / f"symbol={symbol}" / f"date={first.date().isoformat()}" / "part.parquet"
    else:
        raise DataError(f"unsupported interval {interval}")
    atomic_write_frame(target, frame.sort("open_time"))
    return target


def no_kline_marker(root: Path, interval: str, symbol: str, day: datetime) -> Path:
    if interval == "1m":
        return raw_path(root, "klines_1m") / f"symbol={symbol}" / f"date={day.date().isoformat()}" / "no_data.json"
    raise DataError(f"no-data markers are unsupported for {interval}")


def write_no_kline_marker(root: Path, interval: str, symbol: str, day: datetime) -> Path:
    target = no_kline_marker(root, interval, symbol, day)
    atomic_write_json(target, {"symbol": symbol, "interval": interval, "date": day.date().isoformat(), "reason": "official_no_klines"})
    return target


def funding_no_data_marker(root: Path, symbol: str, year: int, month: int) -> Path:
    return raw_path(root, "funding") / f"symbol={symbol}" / f"year={year:04d}" / f"month={month:02d}" / "no_data.json"


def write_funding_no_data_marker(root: Path, symbol: str, year: int, month: int) -> Path:
    target = funding_no_data_marker(root, symbol, year, month)
    atomic_write_json(target, {"symbol": symbol, "year": year, "month": month, "reason": "official_no_funding_events"})
    return target


def write_funding_partition(root: Path, symbol: str, frame: pl.DataFrame) -> Path:
    if frame.columns != FUNDING_COLUMNS or frame.is_empty():
        raise DataError("invalid funding schema")
    if frame.select(pl.struct(["symbol", "funding_time"]).is_duplicated().any()).item():
        raise DataError("duplicate (symbol, funding_time)")
    first = frame.get_column("funding_time").min()
    target = raw_path(root, "funding") / f"symbol={symbol}" / f"year={first.year:04d}" / f"month={first.month:02d}" / "part.parquet"
    atomic_write_frame(target, frame.sort("funding_time"))
    return target


def write_symbols(root: Path, frame: pl.DataFrame) -> Path:
    required = ["symbol", "quote_asset", "contract_type", "first_bar_time", "last_bar_time"]
    if frame.columns != required or frame.get_column("symbol").n_unique() != frame.height:
        raise DataError("invalid symbols table")
    target = raw_path(root, "symbols.parquet")
    atomic_write_frame(target, frame.sort("symbol"))
    return target


def append_manifest(root: Path, record: dict[str, object]) -> None:
    required = {"url", "object_size", "downloaded_at", "rows", "first_time", "last_time"}
    if set(record) != required:
        raise DataError(f"manifest fields must be {sorted(required)}")
    path = root / "data" / "manifests" / "downloads.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def load_hourly_no_data_urls(root: Path) -> set[str]:
    path = root / "data" / "manifests" / "hourly_no_data.jsonl"
    if not path.exists():
        return set()
    with path.open(encoding="utf-8") as handle:
        return {json.loads(line)["url"] for line in handle if line.strip()}


def append_hourly_no_data_url(root: Path, url: str) -> None:
    path = root / "data" / "manifests" / "hourly_no_data.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({"url": url, "reason": "official_no_klines"}, separators=(",", ":")) + "\n")


def _read_paths(paths: Iterable[Path], columns: list[str]) -> pl.DataFrame:
    resolved = list(paths)
    if not resolved:
        raise DataError("required raw partitions are missing")
    frame = pl.concat([pl.read_parquet(path, columns=columns) for path in resolved], how="vertical")
    _check_klines(frame)
    return frame.sort(["symbol", "open_time"])


def load_hourly(root: Path, start: datetime, end_exclusive: datetime, warmup_hours: int) -> pl.DataFrame:
    # Python datetime arithmetic keeps UTC and avoids an implicit local timezone.
    lower = start - timedelta(hours=warmup_hours)
    paths = []
    for path in raw_path(root, "klines_1h").glob("symbol=*/year=*/month=*/part.parquet"):
        year = int(path.parents[1].name.removeprefix("year="))
        month = int(path.parent.name.removeprefix("month="))
        month_start = datetime(year, month, 1, tzinfo=UTC)
        month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        if month_end > lower and month_start < end_exclusive:
            paths.append(path)
    frame = _read_paths(paths, KLINE_COLUMNS).filter((pl.col("open_time") >= lower) & (pl.col("open_time") < end_exclusive))
    if frame.is_empty():
        raise DataError("no hourly rows in requested window")
    return frame


def load_minutes(root: Path, requirements: set[tuple[str, datetime]]) -> pl.DataFrame:
    available, absent = [], []
    for symbol, day in sorted(requirements):
        path = raw_path(root, "klines_1m") / f"symbol={symbol}" / f"date={day.date().isoformat()}" / "part.parquet"
        if path.exists():
            available.append(path)
        elif not no_kline_marker(root, "1m", symbol, day).exists():
            absent.append(str(path))
    if absent:
        raise DataError("missing minute partitions without official no-data markers: " + ", ".join(absent))
    return _read_paths(available, KLINE_COLUMNS) if available else pl.DataFrame(schema={name: pl.Null for name in KLINE_COLUMNS})


def load_funding(root: Path, requirements: set[tuple[str, int, int]]) -> pl.DataFrame:
    paths, absent = [], []
    for symbol, year, month in sorted(requirements):
        path = raw_path(root, "funding") / f"symbol={symbol}" / f"year={year:04d}" / f"month={month:02d}" / "part.parquet"
        if path.exists():
            paths.append(path)
        elif not funding_no_data_marker(root, symbol, year, month).exists():
            absent.append(str(path))
    if absent:
        raise DataError("missing funding partitions: " + ", ".join(absent))
    if not paths:
        return pl.DataFrame(schema={"symbol": pl.String, "funding_time": pl.Datetime("us", "UTC"), "funding_rate": pl.Float64})
    frame = pl.concat([pl.read_parquet(path, columns=FUNDING_COLUMNS) for path in paths], how="vertical").sort(["symbol", "funding_time"])
    if frame.select(pl.struct(["symbol", "funding_time"]).is_duplicated().any()).item():
        raise DataError("duplicate (symbol, funding_time) across funding partitions")
    return frame
