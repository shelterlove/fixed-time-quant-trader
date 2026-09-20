from datetime import UTC, datetime
from dataclasses import replace
from pathlib import Path

import pytest

from fixed_time.config import LiveConfig, StrategyConfig
from fixed_time.exchange import Binance, ExchangeError


def config(tmp_path: Path) -> LiveConfig:
    strategy = StrategyConfig(tmp_path, {"strategy_version": "test"})
    return LiveConfig(tmp_path, strategy, "key", "secret", True, tmp_path / "state.db",
                      5, 60, 60, 180, 10, 2, 2, 2)


def test_timestamp_rejection_resyncs_and_retries(tmp_path):
    calls = []
    def transport(method, url, params, headers, timeout):
        calls.append((method, url, dict(params)))
        if url.endswith("/time"):
            return {"serverTime": int(datetime.now(UTC).timestamp() * 1000) - 2000}
        signed = [x for x in calls if x[1].endswith("/balance")]
        if len(signed) == 1:
            raise ExchangeError('HTTP 400: {"code":-1021}', -1021)
        return []
    client = Binance(config(tmp_path), transport)
    assert client._request("GET", client.config.trading_base_url, "/fapi/v2/balance", signed=True) == []
    assert sum(url.endswith("/time") for _, url, _ in calls) == 1
    assert len([1 for _, url, _ in calls if url.endswith("/balance")]) == 2


def test_order_ids_reject_unicode_before_submission(tmp_path):
    client = Binance(config(tmp_path), lambda *_: pytest.fail("transport must not be called"))
    with pytest.raises(ExchangeError, match="invalid client order id"):
        client.market_order("BTCUSDT", "BUY", "LONG", 1, "牛来USDT")


def test_hedge_close_does_not_send_reduce_only(tmp_path):
    captured = {}
    def transport(method, url, params, headers, timeout):
        captured.update(params)
        return {"status": "FILLED", "executedQty": "1", "avgPrice": "10"}
    client = Binance(config(tmp_path), transport)
    client.market_order("BTCUSDT", "SELL", "LONG", 1, "ft2-x-abc")
    assert "reduceOnly" not in captured
    assert captured["positionSide"] == "LONG"


@pytest.mark.parametrize("method",["open_orders","open_algos","user_trades"])
def test_malformed_account_order_lists_are_errors_not_empty_accounts(tmp_path,method):
    client=Binance(config(tmp_path),lambda *args: {"unexpected":True})
    with pytest.raises(ExchangeError,match="invalid"):
        getattr(client,method)(*( ["BTCUSDT"] if method == "user_trades" else [] ))


def test_read_only_mode_also_prevents_configuration_and_cancellation(tmp_path):
    client=Binance(replace(config(tmp_path),trading_enabled=False),lambda *args: pytest.fail("read-only wrote to exchange"))
    for action in (lambda:client.configure_symbol("BTCUSDT"),lambda:client.cancel_algo("BTCUSDT","1")):
        with pytest.raises(ExchangeError,match="TRADING_ENABLED"):
            action()


def test_network_timeout_on_order_write_is_not_blindly_retried(tmp_path):
    calls=[]
    def transport(*args):
        calls.append(args)
        raise ExchangeError("network timeout")
    client=Binance(config(tmp_path),transport)
    with pytest.raises(ExchangeError):
        client.market_order("BTCUSDT","BUY","LONG",1,"ft2-e-safe")
    assert len(calls) == 1
