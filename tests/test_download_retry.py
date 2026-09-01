from http.client import IncompleteRead
from dataclasses import replace
from datetime import UTC, datetime
import json
from unittest.mock import patch

import polars as pl
import pytest

from fixed_time.config import Window, load_config
from fixed_time.download import DownloadError, NoKlinesError, _get, _rest_funding, _rest_klines, download_hourly


def test_incomplete_response_retries_once() -> None:
    with patch("fixed_time.download.urlopen", side_effect=[IncompleteRead(b"x", 1), _Response(b"ok")]):
        payload, _ = _get("https://example.test/object", attempts=2)
    assert payload == b"ok"


def test_content_length_mismatch_retries_once() -> None:
    with patch("fixed_time.download.urlopen", side_effect=[_Response(b"x", declared_size=2), _Response(b"ok")]):
        payload, _ = _get("https://example.test/object", attempts=2)
    assert payload == b"ok"


def test_hourly_bootstrap_uses_one_shared_archive_index(tmp_path) -> None:
    config = replace(load_config(), root=tmp_path)
    window = Window("test", datetime(2022, 1, 1, tzinfo=UTC), datetime(2022, 2, 1, tzinfo=UTC))
    with patch("fixed_time.download._archive_symbols", return_value=set()) as archive_index, \
         patch("fixed_time.download._exchange_symbols", return_value=set()) as exchange_symbols:
        download_hourly(config, window)
    archive_index.assert_called_once_with(config)
    exchange_symbols.assert_called_once_with(config)


def test_terminal_partial_month_uses_daily_fallback_without_monthly_archive(tmp_path) -> None:
    config = replace(load_config(), root=tmp_path)
    window = Window("test", datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 2, tzinfo=UTC))
    frame = pl.DataFrame([{"symbol": "AAAUSDT", "open_time": datetime(2026, 8, 1, tzinfo=UTC),
                           "open": 1., "high": 1., "low": 1., "close": 1., "quote_volume": 1., "trade_count": 1}])
    with patch("fixed_time.download._archive_symbols", return_value={"AAAUSDT"}), \
         patch("fixed_time.download._exchange_symbols", return_value=set()), \
         patch("fixed_time.download._get", return_value=(None, None)), \
         patch("fixed_time.download._rest_klines", return_value=frame) as rest:
        download_hourly(config, window)
    rest.assert_any_call(config, "AAAUSDT", "1h", datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 2, tzinfo=UTC))


def test_full_month_is_not_skipped_when_only_partial_partition_exists(tmp_path) -> None:
    config = replace(load_config(), root=tmp_path)
    window = Window("test", datetime(2021, 12, 1, tzinfo=UTC), datetime(2022, 1, 1, tzinfo=UTC))
    frame = pl.DataFrame([
        {"symbol": "AAAUSDT", "open_time": datetime(2021, 12, 1, tzinfo=UTC),
         "open": 1., "high": 1., "low": 1., "close": 1., "quote_volume": 1., "trade_count": 1}
    ])
    target = tmp_path / "data/raw/klines_1h/symbol=AAAUSDT/year=2021/month=12/part.parquet"
    target.parent.mkdir(parents=True)
    frame.with_columns(pl.col("open_time") + pl.duration(days=29)).write_parquet(target)
    with patch("fixed_time.download._archive_symbols", return_value={"AAAUSDT"}), \
         patch("fixed_time.download._exchange_symbols", return_value=set()), \
         patch("fixed_time.download._get", return_value=(b"monthly", 7)) as get, \
         patch("fixed_time.download._kline_frame", return_value=frame):
        download_hourly(config, window)
    assert any("AAAUSDT-1h-2021-12.zip" in call.args[0] for call in get.call_args_list)
    assert pl.read_parquet(target).item(0, "open_time") == datetime(2021, 12, 1, tzinfo=UTC)


def test_delisted_rest_symbol_is_treated_as_no_klines() -> None:
    config = load_config()
    start = datetime(2026, 8, 1, tzinfo=UTC)
    with patch("fixed_time.download._get", side_effect=DownloadError('HTTP 400: {"code":-1121,"msg":"Invalid symbol."}: example')):
        with pytest.raises(NoKlinesError):
            _rest_klines(config, "DELISTEDUSDT", "1h", start, start.replace(day=2))


def test_other_rest_400_is_not_silenced_as_delisting() -> None:
    config = load_config()
    start = datetime(2026, 8, 1, tzinfo=UTC)
    with patch("fixed_time.download._get", side_effect=DownloadError('HTTP 400: {"code":-1100,"msg":"Bad request"}: example')):
        with pytest.raises(DownloadError, match="-1100"):
            _rest_klines(config, "AAAUSDT", "1h", start, start.replace(day=2))


def test_rest_responses_exclude_end_boundary() -> None:
    config = load_config()
    start, end = datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 2, tzinfo=UTC)
    millis = lambda value: int(value.timestamp() * 1000)
    kline = lambda value: [millis(value), "1", "1", "1", "1", "1", millis(value), "1", 1, "0", "0", "0"]
    with patch("fixed_time.download._get", return_value=(json.dumps([kline(start), kline(end)]).encode(), 0)):
        assert _rest_klines(config, "AAAUSDT", "1h", start, end).get_column("open_time").to_list() == [start]
    funding = [{"fundingTime": millis(start), "fundingRate": "0.001"}, {"fundingTime": millis(end), "fundingRate": "0.002"}]
    with patch("fixed_time.download._get", return_value=(json.dumps(funding).encode(), 0)):
        assert _rest_funding(config, "AAAUSDT", start, end).get_column("funding_time").to_list() == [start]


class _Response:
    def __init__(self, payload: bytes, declared_size: int | None = None) -> None:
        self.payload = payload
        self.headers = {"Content-Length": str(declared_size if declared_size is not None else len(payload))}

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self) -> bytes:
        return self.payload
