from __future__ import annotations

import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from http.client import IncompleteRead
from io import BytesIO, StringIO
import json
from pathlib import Path
from threading import Lock
import time
from typing import Any, Iterable
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlencode, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile

import polars as pl

from .config import StrategyConfig, Window
from .storage import (
    FUNDING_COLUMNS,
    KLINE_COLUMNS,
    append_hourly_no_data_url,
    append_manifest,
    funding_no_data_marker,
    load_hourly_no_data_urls,
    no_kline_marker,
    raw_path,
    write_funding_partition,
    write_funding_no_data_marker,
    write_kline_partition,
    write_no_kline_marker,
    write_symbols,
)


class DownloadError(RuntimeError):
    pass


class NoKlinesError(DownloadError):
    pass


def _get(url: str, attempts: int) -> tuple[bytes | None, int | None]:
    for attempt in range(attempts):
        try:
            request = Request(url, headers={"User-Agent": "fixed-time-portfolio/1.0"})
            with urlopen(request, timeout=60) as response:
                payload = response.read()
                expected_size = int(response.headers.get("Content-Length", "0") or 0)
                if expected_size and len(payload) != expected_size:
                    raise OSError(f"content length mismatch: expected {expected_size}, got {len(payload)}")
                return payload, expected_size
        except HTTPError as exc:
            if exc.code == 404:
                return None, None
            retryable = exc.code in {408, 429} or exc.code >= 500
            if not retryable or attempt == attempts - 1:
                detail = exc.read().decode("utf-8", errors="replace")
                raise DownloadError(f"HTTP {exc.code}: {detail}: {url}") from exc
        except (OSError, IncompleteRead) as exc:
            if attempt == attempts - 1:
                raise DownloadError(f"download failed: {url}: {exc}") from exc
        time.sleep(2**attempt)
    raise AssertionError("unreachable")


def _utc_millis(value: str | int | float) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC)


def _kline_frame(symbol: str, payload: bytes) -> pl.DataFrame:
    try:
        with ZipFile(BytesIO(payload)) as archive:
            if archive.testzip() is not None:
                raise DownloadError("ZIP CRC failed")
            names = archive.namelist()
            if len(names) != 1:
                raise DownloadError("archive must contain exactly one CSV")
            text = archive.read(names[0]).decode("utf-8")
    except BadZipFile as exc:
        raise DownloadError("invalid ZIP") from exc
    rows = list(csv.reader(StringIO(text)))
    if rows and rows[0] and rows[0][0] == "open_time":
        rows = rows[1:]
    if not rows or any(len(row) != 12 for row in rows):
        raise DownloadError("unexpected Binance kline CSV column count")
    records = [{"symbol": symbol, "open_time": _utc_millis(row[0]), "open": float(row[1]), "high": float(row[2]), "low": float(row[3]), "close": float(row[4]), "quote_volume": float(row[7]), "trade_count": int(row[8])} for row in rows]
    return pl.DataFrame(records, schema=KLINE_COLUMNS)


def _funding_frame(symbol: str, payload: bytes) -> pl.DataFrame:
    try:
        with ZipFile(BytesIO(payload)) as archive:
            if archive.testzip() is not None:
                raise DownloadError("ZIP CRC failed")
            names = archive.namelist()
            if len(names) != 1:
                raise DownloadError("archive must contain exactly one CSV")
            text = archive.read(names[0]).decode("utf-8")
    except BadZipFile as exc:
        raise DownloadError("invalid funding ZIP") from exc
    rows = list(csv.reader(StringIO(text)))
    time_index, rate_index = 0, 1
    if rows and rows[0][0].lower() in {"calc_time", "calctime", "funding_time", "fundingtime"}:
        header = [name.strip().lower() for name in rows.pop(0)]
        try:
            time_index = next(index for index, name in enumerate(header) if name in {"calc_time", "calctime", "funding_time", "fundingtime"})
            rate_index = next(index for index, name in enumerate(header) if name in {"last_funding_rate", "funding_rate"})
        except StopIteration as exc:
            raise DownloadError("funding CSV has no recognized time/rate columns") from exc
    if not rows or any(len(row) <= max(time_index, rate_index) for row in rows):
        raise DownloadError("unexpected Binance funding CSV")
    return pl.DataFrame([{"symbol": symbol, "funding_time": _utc_millis(row[time_index]), "funding_rate": float(row[rate_index])} for row in rows], schema=FUNDING_COLUMNS)


def _funding_in_range(frame: pl.DataFrame, start: datetime, end: datetime) -> pl.DataFrame:
    """Keep an archive month half-open so a boundary settlement has one owner."""
    return frame.filter((pl.col("funding_time") >= start) & (pl.col("funding_time") < end))


def _rest_klines(config: StrategyConfig, symbol: str, interval: str, start: datetime, end: datetime) -> pl.DataFrame:
    rows: list[list[Any]] = []
    current = start
    while current < end:
        query = urlencode({"symbol": symbol, "interval": interval, "startTime": int(current.timestamp() * 1000), "endTime": int(end.timestamp() * 1000), "limit": 1500})
        try:
            payload, _ = _get(config.values["data"]["futures_api_base_url"] + "/fapi/v1/klines?" + query, config.values["data"]["max_attempts_per_object"])
        except DownloadError as exc:
            # Delisted contracts can be absent from the current REST symbol
            # registry even though they remain in the historical archive.
            message = str(exc)
            if message.startswith("HTTP 400:") and ("-1121" in message or "Invalid symbol" in message):
                raise NoKlinesError(f"REST returned no klines for {symbol}") from exc
            raise
        if payload is None:
            raise DownloadError(f"REST kline 404 for {symbol}")
        batch = json.loads(payload)
        if not batch:
            break
        rows.extend(batch)
        current = _utc_millis(batch[-1][0]) + (timedelta(hours=1) if interval == "1h" else timedelta(minutes=1))
    if not rows:
        raise NoKlinesError(f"REST returned no klines for {symbol}")
    frame = pl.DataFrame([{"symbol": symbol, "open_time": _utc_millis(row[0]), "open": float(row[1]), "high": float(row[2]), "low": float(row[3]), "close": float(row[4]), "quote_volume": float(row[7]), "trade_count": int(row[8])} for row in rows], schema=KLINE_COLUMNS).filter(
        (pl.col("open_time") >= start) & (pl.col("open_time") < end)
    ).unique(["symbol", "open_time"]).sort("open_time")
    if frame.is_empty():
        raise NoKlinesError(f"REST returned no in-range klines for {symbol}")
    return frame


def _rest_funding(config: StrategyConfig, symbol: str, start: datetime, end: datetime) -> pl.DataFrame:
    query = urlencode({"symbol": symbol, "startTime": int(start.timestamp() * 1000), "endTime": int(end.timestamp() * 1000), "limit": 1000})
    payload, _ = _get(config.values["data"]["futures_api_base_url"] + "/fapi/v1/fundingRate?" + query, config.values["data"]["max_attempts_per_object"])
    if payload is None:
        raise DownloadError(f"REST funding 404 for {symbol}")
    return _funding_in_range(
        pl.DataFrame([{"symbol": symbol, "funding_time": _utc_millis(row["fundingTime"]), "funding_rate": float(row["fundingRate"])} for row in json.loads(payload)], schema=FUNDING_COLUMNS),
        start,
        end,
    )


def _archive_symbols(config: StrategyConfig) -> set[str]:
    """Enumerate symbol directories once without scanning every interval object."""
    base, prefix, token = config.values["data"]["archive_listing_url"], "data/futures/um/monthly/klines/", None
    namespace, found = "{http://s3.amazonaws.com/doc/2006-03-01/}", set()
    while True:
        query: dict[str, str] = {"list-type": "2", "prefix": prefix, "delimiter": "/"}
        if token:
            query["continuation-token"] = token
        payload, _ = _get(base + "?" + urlencode(query), config.values["data"]["max_attempts_per_object"])
        if payload is None:
            raise DownloadError("monthly archive listing unavailable")
        root = ElementTree.fromstring(payload)
        for item in root.findall(namespace + "CommonPrefixes"):
            key = item.findtext(namespace + "Prefix", "")
            parts = key.rstrip("/").split("/")
            if len(parts) >= 6:
                found.add(parts[-1])
        token = root.findtext(namespace + "NextContinuationToken")
        if not token:
            break
    return found


def _exchange_symbols(config: StrategyConfig) -> set[str]:
    payload, _ = _get(config.values["data"]["futures_api_base_url"] + "/fapi/v1/exchangeInfo", config.values["data"]["max_attempts_per_object"])
    if payload is None:
        raise DownloadError("exchangeInfo unavailable")
    return {row["symbol"] for row in json.loads(payload)["symbols"] if row.get("quoteAsset") == "USDT" and row.get("contractType") == "PERPETUAL"}


def _months(start: datetime, end: datetime) -> Iterable[tuple[datetime, bool]]:
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current < end:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield current, start <= current and next_month <= end
        current = next_month


def _archive_url(config: StrategyConfig, kind: str, symbol: str, interval: str | None, stamp: str) -> str:
    base = config.values["data"]["archive_base_url"]
    encoded = quote(symbol, safe="")
    if kind == "klines":
        return f"{base}/futures/um/{'monthly' if len(stamp) == 7 else 'daily'}/klines/{encoded}/{interval}/{encoded}-{interval}-{stamp}.zip"
    return f"{base}/futures/um/monthly/fundingRate/{encoded}/{encoded}-fundingRate-{stamp}.zip"


def _manifest(root: Path, url: str, size: int | None, frame: pl.DataFrame, time_column: str) -> None:
    append_manifest(root, {"url": url, "object_size": size or 0, "downloaded_at": datetime.now(UTC).isoformat(), "rows": frame.height,
                           "first_time": frame.get_column(time_column).min().isoformat(), "last_time": frame.get_column(time_column).max().isoformat()})


def _hourly_manifest_state(root: Path) -> tuple[set[str], dict[str, tuple[datetime, datetime]]]:
    path = root / "data" / "manifests" / "downloads.jsonl"
    urls: set[str] = set()
    bounds: dict[str, tuple[datetime, datetime]] = {}
    if not path.exists():
        return urls, bounds
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            record = json.loads(line)
            source_url = record["url"].split("#", 1)[0]
            urls.add(source_url)
            parts = unquote(urlparse(source_url).path).strip("/").split("/")
            if len(parts) != 8 or parts[0:3] != ["data", "futures", "um"] or parts[4:5] != ["klines"] or parts[6] != "1h":
                continue
            symbol = parts[5]
            first, last = datetime.fromisoformat(record["first_time"]), datetime.fromisoformat(record["last_time"])
            previous = bounds.get(symbol)
            bounds[symbol] = (min(first, previous[0]) if previous else first, max(last, previous[1]) if previous else last)
    return urls, bounds


def _write_symbols_from_bounds(root: Path, bounds: dict[str, tuple[datetime, datetime]]) -> None:
    if bounds:
        write_symbols(root, pl.DataFrame([
            {"symbol": symbol, "quote_asset": "USDT", "contract_type": "PERPETUAL", "first_bar_time": first, "last_bar_time": last}
            for symbol, (first, last) in bounds.items()
        ]))


def download_hourly(config: StrategyConfig, window: Window, refresh: bool = False) -> None:
    root = config.root
    # One delimiter-based archive listing discovers historical symbols. Direct
    # object requests plus the local manifest determine month/day completion;
    # no per-symbol directory listing is needed.
    archive_symbols = _archive_symbols(config)
    downloaded_urls, symbol_bounds = _hourly_manifest_state(root)
    no_data_urls = load_hourly_no_data_urls(root)
    quote = config.values["universe"]["quote_asset"]
    symbols = sorted(symbol for symbol in (archive_symbols | _exchange_symbols(config)) if symbol.endswith(quote))
    feature_start = window.protection_history_start or window.start
    start = feature_start - timedelta(hours=config.values["features"]["hourly_warmup_hours"])
    manifest_lock = Lock()

    def download_symbol(symbol: str) -> None:
        for month, full in _months(start, window.end_exclusive):
            target = raw_path(root, "klines_1h") / f"symbol={symbol}" / f"year={month.year:04d}" / f"month={month.month:02d}" / "part.parquet"
            stamp = month.strftime("%Y-%m")
            if full:
                url = _archive_url(config, "klines", symbol, "1h", stamp)
                if target.exists() and url in downloaded_urls and not refresh:
                    continue
                if url in no_data_urls and not refresh:
                    continue
                payload, size = _get(url, config.values["data"]["max_attempts_per_object"])
                if payload is None:
                    with manifest_lock:
                        if url not in no_data_urls:
                            append_hourly_no_data_url(root, url)
                            no_data_urls.add(url)
                    continue
                objects = [(url, size, _kline_frame(symbol, payload))]
                frame = objects[0][2]
            else:
                month_end = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
                # A terminal, still-open month has no monthly archive yet.
                # Its daily archives remain the required primary source.
                lower, upper = max(start, month), min(window.end_exclusive, month_end)
                day, objects, fetched_days = lower.replace(hour=0, minute=0, second=0, microsecond=0), [], []
                while day < upper:
                    url = _archive_url(config, "klines", symbol, "1h", day.date().isoformat())
                    if target.exists() and url in downloaded_urls and not refresh:
                        day += timedelta(days=1)
                        continue
                    if url in no_data_urls and not refresh:
                        day += timedelta(days=1)
                        continue
                    payload, size = _get(url, config.values["data"]["max_attempts_per_object"])
                    if payload is not None:
                        objects.append((url, size, _kline_frame(symbol, payload)))
                        fetched_days.append(day)
                    else:
                        try:
                            fallback = _rest_klines(config, symbol, "1h", day, day + timedelta(days=1))
                        except NoKlinesError:
                            # A historic contract may have no bar at all on a
                            # current boundary day.  Its absence cannot create
                            # an eligible cross-sectional member.
                            with manifest_lock:
                                if url not in no_data_urls:
                                    append_hourly_no_data_url(root, url)
                                    no_data_urls.add(url)
                        else:
                            objects.append((url + "#REST", size, fallback))
                            fetched_days.append(day)
                    day += timedelta(days=1)
                if not objects:
                    continue
                new_rows = pl.concat([item[2] for item in objects], how="vertical")
                if target.exists():
                    existing = pl.read_parquet(target, columns=KLINE_COLUMNS)
                    for fetched_day in fetched_days:
                        existing = existing.filter(
                            (pl.col("open_time") < fetched_day)
                            | (pl.col("open_time") >= fetched_day + timedelta(days=1))
                        )
                    frame = pl.concat([existing, new_rows], how="vertical")
                else:
                    frame = new_rows
            frame = frame.unique(["symbol", "open_time"]).sort("open_time")
            write_kline_partition(root, "1h", symbol, frame)
            with manifest_lock:
                for url, size, object_frame in objects:
                    _manifest(root, url, size, object_frame, "open_time")
                    downloaded_urls.add(url.split("#", 1)[0])
                    first, last = object_frame.get_column("open_time").min(), object_frame.get_column("open_time").max()
                    previous = symbol_bounds.get(symbol)
                    symbol_bounds[symbol] = (min(first, previous[0]) if previous else first, max(last, previous[1]) if previous else last)

    with ThreadPoolExecutor(max_workers=config.values["data"]["max_concurrent_requests"]) as pool:
        for future in as_completed([pool.submit(download_symbol, symbol) for symbol in symbols]):
            future.result()
    _write_symbols_from_bounds(root, symbol_bounds)


def download_minutes(config: StrategyConfig, requirements: set[tuple[str, datetime]]) -> None:
    root = config.root
    def fetch(item: tuple[str, datetime]):
        symbol, day = item
        target = raw_path(root, "klines_1m") / f"symbol={symbol}" / f"date={day.date().isoformat()}" / "part.parquet"
        if target.exists() or no_kline_marker(root, "1m", symbol, day).exists(): return None
        url = _archive_url(config, "klines", symbol, "1m", day.date().isoformat())
        payload, size = _get(url, config.values["data"]["max_attempts_per_object"])
        if payload is not None:
            frame = _kline_frame(symbol, payload)
        else:
            try:
                frame = _rest_klines(config, symbol, "1m", day, day + timedelta(days=1))
            except NoKlinesError:
                write_no_kline_marker(root, "1m", symbol, day)
                return None
        return url if payload is not None else url + "#REST", size, frame
    with ThreadPoolExecutor(max_workers=config.values["data"]["max_concurrent_requests"]) as pool:
        for future in as_completed([pool.submit(fetch, item) for item in requirements]):
            result = future.result()
            if result:
                url, size, frame = result
                write_kline_partition(root, "1m", frame.get_column("symbol")[0], frame)
                _manifest(root, url, size, frame, "open_time")


def download_funding(config: StrategyConfig, requirements: set[tuple[str, int, int]], refresh: bool = False) -> None:
    root = config.root
    def fetch(item: tuple[str, int, int]):
        symbol, year, month = item
        target = raw_path(root, "funding") / f"symbol={symbol}" / f"year={year:04d}" / f"month={month:02d}" / "part.parquet"
        if (target.exists() or funding_no_data_marker(root, symbol, year, month).exists()) and not refresh: return None
        start = datetime(year, month, 1, tzinfo=UTC)
        end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        url = _archive_url(config, "funding", symbol, None, start.strftime("%Y-%m"))
        payload, size = _get(url, config.values["data"]["max_attempts_per_object"])
        frame = _funding_in_range(_funding_frame(symbol, payload), start, end) if payload is not None else _rest_funding(config, symbol, start, end)
        if frame.is_empty():
            write_funding_no_data_marker(root, symbol, year, month)
            return None
        return url if payload is not None else url + "#REST", size, frame
    with ThreadPoolExecutor(max_workers=config.values["data"]["max_concurrent_requests"]) as pool:
        for future in as_completed([pool.submit(fetch, item) for item in requirements]):
            result = future.result()
            if result:
                url, size, frame = result
                write_funding_partition(root, frame.get_column("symbol")[0], frame)
                _manifest(root, url, size, frame, "funding_time")
