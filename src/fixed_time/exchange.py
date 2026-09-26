from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from http.client import HTTPException
import hashlib
import hmac
import json
from math import lcm
import re
from time import sleep
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

import polars as pl

from .config import LiveConfig, TESTNET_FUTURES_URL
from .strategy import BAR_COLUMNS


class ExchangeError(RuntimeError):
    def __init__(self, message: str, code: int | None = None):
        super().__init__(message)
        self.code = code

    @property
    def not_found(self) -> bool:
        return self.code in {-2011, -2013}


Transport = Callable[[str, str, dict[str, str], dict[str, str], int], Any]
CLIENT_ID = re.compile(r"^[.A-Z:/a-z0-9_-]{1,36}$")


def _transport(method: str, url: str, params: dict[str, str], headers: dict[str, str], timeout: int) -> Any:
    query = urlencode(params)
    request = Request(f"{url}?{query}" if query else url, method=method, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            code = int(json.loads(body).get("code"))
        except (ValueError, TypeError, AttributeError):
            code = None
        raise ExchangeError(f"HTTP {exc.code}: {body}", code) from exc
    except URLError as exc:
        raise ExchangeError(f"network error contacting {urlsplit(url).hostname}: {exc.reason}") from exc
    except (OSError, HTTPException, ValueError) as exc:
        raise ExchangeError(f"incomplete exchange response: {exc}") from exc


def quantize_down(value: Decimal, step: Decimal) -> Decimal:
    if value <= 0 or step <= 0:
        return Decimal()
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def protective_price(value: Decimal, tick: Decimal, close_side: str) -> Decimal:
    rounding = ROUND_CEILING if close_side == "SELL" else ROUND_DOWN
    return (value / tick).to_integral_value(rounding=rounding) * tick


class Binance:
    """Small USD-M adapter. Signed calls are hard-restricted to testnet."""

    def __init__(self, config: LiveConfig, transport: Transport | None = None):
        self.config = config
        self.transport = transport or _transport
        self.server_offset_ms = 0
        self._catalogues: dict[str, tuple[datetime, dict]] = {}
        self._filters: dict[str, dict[str, Decimal]] = {}

    def _raw_time(self) -> int:
        result = self.transport("GET", f"{self.config.trading_base_url}/fapi/v1/time", {}, {}, self.config.request_timeout_seconds)
        if not isinstance(result, dict) or "serverTime" not in result:
            raise ExchangeError("invalid server time response")
        return int(result["serverTime"])

    def sync_time(self) -> int:
        before = int(datetime.now(UTC).timestamp() * 1000)
        server = self._raw_time()
        after = int(datetime.now(UTC).timestamp() * 1000)
        self.server_offset_ms = server - (before + after) // 2
        return self.server_offset_ms

    def now(self) -> datetime:
        return datetime.now(UTC) + timedelta(milliseconds=self.server_offset_ms)

    def _request(self, method: str, base: str, path: str, params: dict[str, str] | None = None, *, signed: bool = False) -> Any:
        if signed and base != TESTNET_FUTURES_URL:
            raise ExchangeError("signed requests are restricted to Binance Futures testnet")
        if signed and (not self.config.api_key or not self.config.api_secret):
            raise ExchangeError("testnet credentials are required")
        if signed and method != "GET" and not self.config.trading_enabled:
            raise ExchangeError("TRADING_ENABLED is false")
        attempts = self.config.max_attempts if method == "GET" else 1
        timestamp_retry = signed
        last: ExchangeError | None = None
        for attempt in range(attempts + int(timestamp_retry)):
            payload = dict(params or {})
            headers: dict[str, str] = {}
            if signed:
                # Bias slightly behind server time. Being behind within recvWindow is
                # accepted; being 1000 ms ahead is rejected by Binance.
                payload["recvWindow"] = "5000"
                payload["timestamp"] = str(int(datetime.now(UTC).timestamp() * 1000) + self.server_offset_ms - 250)
                encoded = urlencode(payload)
                payload["signature"] = hmac.new(self.config.api_secret.encode(), encoded.encode(), hashlib.sha256).hexdigest()
                headers["X-MBX-APIKEY"] = self.config.api_key
            try:
                result = self.transport(method, f"{base}{path}", payload, headers, self.config.request_timeout_seconds)
                if isinstance(result, dict) and int(result.get("code", 0)) < 0:
                    raise ExchangeError(f"Binance {result['code']}: {result.get('msg', '')}", int(result["code"]))
                return result
            except ExchangeError as exc:
                last = exc
                if exc.code == -1021 and timestamp_retry:
                    timestamp_retry = False
                    self.sync_time()
                    continue
                if method != "GET":
                    raise
                if attempt + 1 < attempts and exc.code is None:
                    sleep(min(2 ** attempt, 4))
        raise last or ExchangeError("request failed")

    def _catalogue(self, base: str) -> dict:
        cached = self._catalogues.get(base)
        if cached and self.now() - cached[0] < timedelta(minutes=5):
            return cached[1]
        result = self._request("GET", base, "/fapi/v1/exchangeInfo")
        if not isinstance(result, dict) or not isinstance(result.get("symbols"), list):
            raise ExchangeError("invalid exchangeInfo response")
        self._catalogues[base] = (self.now(), result)
        if base == self.config.trading_base_url:
            self._filters.clear()
        return result

    @staticmethod
    def _symbols(catalogue: dict) -> set[str]:
        return {str(x["symbol"]) for x in catalogue["symbols"] if x.get("quoteAsset") == "USDT"
                and x.get("contractType") == "PERPETUAL" and x.get("status") == "TRADING"}

    def market_symbols(self) -> list[str]:
        return sorted(self._symbols(self._catalogue(self.config.market_data_base_url)))

    def trading_symbols(self) -> set[str]:
        return self._symbols(self._catalogue(self.config.trading_base_url))

    def symbol_filters(self, symbol: str) -> dict[str, Decimal]:
        catalogue = self._catalogue(self.config.trading_base_url)
        if symbol in self._filters:
            return self._filters[symbol]
        item = next((x for x in catalogue["symbols"] if x.get("symbol") == symbol), None)
        if item is None:
            raise ExchangeError(f"unknown testnet symbol {symbol}")
        f = {x["filterType"]: x for x in item.get("filters", [])}
        try:
            lots = [f["LOT_SIZE"], f.get("MARKET_LOT_SIZE", f["LOT_SIZE"])]
            steps = [Decimal(x["stepSize"]) for x in lots if Decimal(x["stepSize"]) > 0]
            scale = 10 ** max(-x.as_tuple().exponent for x in steps)
            step = Decimal(lcm(*(int(x * scale) for x in steps))) / scale
            result = {"step_size": step, "min_qty": max(Decimal(x["minQty"]) for x in lots),
                      "max_qty": min(Decimal(x.get("maxQty", "Infinity")) for x in lots),
                      "tick_size": Decimal(f["PRICE_FILTER"]["tickSize"]),
                      "min_notional": Decimal(f["MIN_NOTIONAL"]["notional"])}
        except KeyError as exc:
            raise ExchangeError(f"incomplete filters for {symbol}") from exc
        self._filters[symbol] = result
        return result

    def klines(self, symbol: str, limit: int, end: datetime) -> pl.DataFrame:
        raw = self._request("GET", self.config.market_data_base_url, "/fapi/v1/klines", {
            "symbol": symbol, "interval": "1h", "limit": str(limit),
            "endTime": str(int((end - timedelta(milliseconds=1)).timestamp() * 1000)),
        })
        if not isinstance(raw, list):
            raise ExchangeError(f"invalid klines for {symbol}")
        if not raw or int(raw[-1][0]) != int((end-timedelta(hours=1)).timestamp()*1000):
            raise ExchangeError(f"missing latest closed hourly bar: {symbol}")
        rows = [{"symbol": symbol, "open_time": datetime.fromtimestamp(int(x[0]) / 1000, UTC),
                 "open": float(x[1]), "high": float(x[2]), "low": float(x[3]), "close": float(x[4]),
                 "quote_volume": float(x[7]), "trade_count": int(x[8])} for x in raw]
        return pl.DataFrame(rows, schema={"symbol": pl.String, "open_time": pl.Datetime("us", "UTC"),
            "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
            "quote_volume": pl.Float64, "trade_count": pl.Int64}).select(BAR_COLUMNS)

    def hourly_snapshot(self, symbols: list[str], limit: int, end: datetime) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        with ThreadPoolExecutor(max_workers=self.config.max_market_workers) as pool:
            jobs = {pool.submit(self.klines, symbol, limit, end): symbol for symbol in symbols}
            for job in as_completed(jobs):
                frames.append(job.result())
        return pl.concat(frames, how="vertical")

    def position_mode(self) -> bool:
        row = self._request("GET", self.config.trading_base_url, "/fapi/v1/positionSide/dual", signed=True)
        return str(row.get("dualSidePosition", "false")).lower() == "true"

    def multi_asset_mode(self) -> bool:
        row = self._request("GET", self.config.trading_base_url, "/fapi/v1/multiAssetsMargin", signed=True)
        if not isinstance(row,dict) or "multiAssetsMargin" not in row:
            raise ExchangeError("invalid multi-asset mode response")
        return str(row["multiAssetsMargin"]).lower() == "true"

    def account(self) -> dict:
        row = self._request("GET", self.config.trading_base_url, "/fapi/v3/account", signed=True)
        if not isinstance(row, dict):
            raise ExchangeError("invalid account response")
        return row

    def equity(self) -> Decimal:
        row = self.account()
        return Decimal(str(row["totalWalletBalance"])) + Decimal(str(row["totalUnrealizedProfit"]))

    def available_balance(self) -> Decimal:
        return Decimal(str(self.account()["availableBalance"]))

    def positions(self) -> list[dict]:
        rows = self._request("GET", self.config.trading_base_url, "/fapi/v2/positionRisk", signed=True)
        if not isinstance(rows, list):
            raise ExchangeError("invalid position response")
        return [x for x in rows if Decimal(str(x.get("positionAmt", "0"))) != 0]

    def latest_price(self, symbol: str) -> Decimal:
        row = self._request("GET", self.config.trading_base_url, "/fapi/v1/ticker/price", {"symbol": symbol})
        value = Decimal(str(row.get("price", "0")))
        if value <= 0:
            raise ExchangeError(f"invalid price for {symbol}")
        return value

    def configure_symbol(self, symbol: str) -> None:
        try:
            self._request("POST", self.config.trading_base_url, "/fapi/v1/marginType", {"symbol": symbol, "marginType": "ISOLATED"}, signed=True)
        except ExchangeError as exc:
            if exc.code != -4046:
                raise
        self._request("POST", self.config.trading_base_url, "/fapi/v1/leverage", {"symbol": symbol, "leverage": str(self.config.leverage)}, signed=True)

    @staticmethod
    def _check_id(value: str) -> None:
        if not CLIENT_ID.fullmatch(value):
            raise ExchangeError(f"invalid client order id: {value!r}")

    def market_order(self, symbol: str, side: str, position_side: str, quantity: Decimal, client_id: str) -> dict:
        if not self.config.trading_enabled:
            raise ExchangeError("TRADING_ENABLED is false")
        self._check_id(client_id)
        return self._request("POST", self.config.trading_base_url, "/fapi/v1/order", {
            "symbol": symbol, "side": side, "positionSide": position_side, "type": "MARKET",
            "quantity": format(quantity, "f"), "newClientOrderId": client_id, "newOrderRespType": "RESULT",
        }, signed=True)

    def conditional_order(self, symbol: str, side: str, position_side: str, quantity: Decimal,
                          trigger: Decimal, order_type: str, client_id: str) -> dict:
        if not self.config.trading_enabled:
            raise ExchangeError("TRADING_ENABLED is false")
        if order_type not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
            raise ExchangeError("unsupported conditional order type")
        self._check_id(client_id)
        row = self._request("POST", self.config.trading_base_url, "/fapi/v1/algoOrder", {
            "algoType": "CONDITIONAL", "symbol": symbol, "side": side, "positionSide": position_side,
            "type": order_type, "triggerPrice": format(trigger, "f"), "quantity": format(quantity, "f"),
            "workingType": "CONTRACT_PRICE", "clientAlgoId": client_id,
        }, signed=True)
        if not isinstance(row, dict) or "algoId" not in row:
            raise ExchangeError("invalid conditional order response")
        return row

    def query_order(self, symbol: str, client_id: str) -> dict:
        return self._request("GET", self.config.trading_base_url, "/fapi/v1/order", {"symbol": symbol, "origClientOrderId": client_id}, signed=True)

    def query_order_id(self, symbol: str, order_id: str) -> dict:
        return self._request("GET", self.config.trading_base_url, "/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)

    def query_algo_id(self, symbol: str, algo_id: str) -> dict:
        return self._request("GET", self.config.trading_base_url, "/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id}, signed=True)

    def query_algo_client(self, symbol: str, client_id: str) -> dict:
        return self._request("GET", self.config.trading_base_url, "/fapi/v1/algoOrder",
                             {"symbol": symbol, "clientAlgoId": client_id}, signed=True)

    def open_orders(self) -> list[dict]:
        row = self._request("GET", self.config.trading_base_url, "/fapi/v1/openOrders", signed=True)
        if not isinstance(row,list):
            raise ExchangeError("invalid open orders response")
        return row

    def cancel_order(self, symbol: str, client_id: str) -> None:
        try:
            self._request("DELETE", self.config.trading_base_url, "/fapi/v1/order",
                          {"symbol": symbol, "origClientOrderId": client_id}, signed=True)
        except ExchangeError as exc:
            if not exc.not_found:
                raise

    def open_algos(self) -> list[dict]:
        row = self._request("GET", self.config.trading_base_url, "/fapi/v1/openAlgoOrders", signed=True)
        if not isinstance(row,list):
            raise ExchangeError("invalid open algo orders response")
        return row

    def cancel_algo(self, symbol: str, algo_id: str) -> None:
        try:
            self._request("DELETE", self.config.trading_base_url, "/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id}, signed=True)
        except ExchangeError as exc:
            if not exc.not_found:
                raise

    def user_trades(self, symbol: str, order_id: str | None = None) -> list[dict]:
        params = {"symbol": symbol, "limit": "1000"}
        if order_id:
            params["orderId"] = order_id
        row = self._request("GET", self.config.trading_base_url, "/fapi/v1/userTrades", params, signed=True)
        if not isinstance(row,list):
            raise ExchangeError("invalid user trades response")
        return row

    def force_orders(self, symbol: str) -> list[dict]:
        row = self._request("GET", self.config.trading_base_url, "/fapi/v1/forceOrders",
                            {"symbol": symbol, "limit": "100"}, signed=True)
        if not isinstance(row, list):
            raise ExchangeError("invalid force orders response")
        return row
