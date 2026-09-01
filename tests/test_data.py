from datetime import UTC, datetime
from copy import deepcopy
from io import BytesIO
from zipfile import ZipFile

import pytest

from fixed_time.config import ConfigError, _validate, load_config
from fixed_time.download import DownloadError, _archive_url, _funding_frame, _funding_in_range, _kline_frame
from fixed_time.storage import DataError, load_funding, load_minutes, write_funding_no_data_marker, write_no_kline_marker


def _zip_csv(content: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("sample.csv", content)
    return buffer.getvalue()


def test_official_kline_zip_parsing_and_time_unit() -> None:
    payload = _zip_csv("1640995200000,1,2,0.5,1.5,3,4,5,6,7,8,9\n")
    frame = _kline_frame("AAAUSDT", payload)
    assert frame.height == 1
    assert frame.item(0, "open_time") == datetime(2022, 1, 1, tzinfo=UTC)
    assert frame.item(0, "quote_volume") == 5.0


def test_official_kline_zip_header_is_accepted() -> None:
    payload = _zip_csv("open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1640995200000,1,2,0.5,1.5,3,4,5,6,7,8,9\n")
    assert _kline_frame("AAAUSDT", payload).height == 1


def test_official_funding_zip_calc_time_header_is_accepted() -> None:
    payload = _zip_csv("calc_time,funding_interval_hours,last_funding_rate\n1640995200000,8,0.0001\n")
    frame = _funding_frame("AAAUSDT", payload)
    assert frame.item(0, "funding_time") == datetime(2022, 1, 1, tzinfo=UTC)
    assert frame.item(0, "funding_rate") == 0.0001


def test_funding_archive_month_excludes_next_month_boundary_event() -> None:
    start, end = datetime(2022, 1, 1, tzinfo=UTC), datetime(2022, 2, 1, tzinfo=UTC)
    frame = _funding_frame("AAAUSDT", _zip_csv(
        "calc_time,last_funding_rate\n1640995200000,0.0001\n1643673600000,0.0002\n"
    ))
    assert _funding_in_range(frame, start, end).select("funding_time", "funding_rate").rows() == [
        (start, 0.0001),
    ]


def test_bad_zip_and_bad_csv_fail() -> None:
    with pytest.raises(DownloadError):
        _kline_frame("AAAUSDT", b"not a zip")
    with pytest.raises(DownloadError):
        _kline_frame("AAAUSDT", _zip_csv("1,2\n"))


def test_archive_url_encodes_non_ascii_symbol() -> None:
    url = _archive_url(load_config(), "klines", "币安人生USDT", "1h", "2026-01")
    assert "%E5%B8%81" in url


def test_missing_minute_partition_requires_official_no_data_marker(tmp_path) -> None:
    day = datetime(2022, 1, 1, tzinfo=UTC)
    requirements = {("DELISTEDUSDT", day)}
    with pytest.raises(DataError, match="without official no-data markers"):
        load_minutes(tmp_path, requirements)
    write_no_kline_marker(tmp_path, "1m", "DELISTEDUSDT", day)
    assert load_minutes(tmp_path, requirements).is_empty()


def test_empty_funding_month_requires_official_no_data_marker(tmp_path) -> None:
    requirements = {("NEWUSDT", 2022, 1)}
    with pytest.raises(DataError, match="missing funding partitions"):
        load_funding(tmp_path, requirements)
    write_funding_no_data_marker(tmp_path, "NEWUSDT", 2022, 1)
    assert load_funding(tmp_path, requirements).is_empty()


def test_config_rejects_short_hold_exit_mismatch() -> None:
    values = deepcopy(load_config().values)
    values["short"]["legs"]["6"]["hold_hours"] = 13
    with pytest.raises(ConfigError, match="hold/exit mismatch"):
        _validate(values)


def test_config_rejects_unimplemented_funding_semantics() -> None:
    values = deepcopy(load_config().values)
    values["long"]["funding_boundary"] = "different_boundary"
    with pytest.raises(ConfigError, match="funding semantics"):
        _validate(values)


def test_config_rejects_invalid_short_rank_and_units() -> None:
    values = deepcopy(load_config().values)
    values["short"]["r24_rank_min"] = 0
    with pytest.raises(ConfigError, match="rank range"):
        _validate(values)
    values = deepcopy(load_config().values)
    values["short"]["portfolio"]["units_per_signal"] = 0
    with pytest.raises(ConfigError, match="units_per_signal"):
        _validate(values)
