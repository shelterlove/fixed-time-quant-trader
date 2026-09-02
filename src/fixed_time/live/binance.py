from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
import hashlib
import hmac
import json
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl

from ..storage import KLINE_COLUMNS
from .config import LiveConfig, PUBLIC_FUTURES_URL, TESTNET_FUTURES_URL


class BinanceError(RuntimeError):
    pass


Transport = Callable[[str, str, dict[str, str], dict[str, str], int], Any]


def _default_transport(method: str, url: str, params: dict[str, str], headers: dict[str, str], timeout: int) -> Any:
    query = urlencode(params)
    target = f"{url}?{query}" if query else url
    request = Request(target, method=method, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise BinanceError(f"HTTP {exc.code}: {body}") from exc
    except URLError as exc:
        raise BinanceError(f"network error: {exc.reason}") from exc


def quantize_down(value: Decimal, step: Decimal) -> Decimal:
    if value <= 0 or step <= 0:
        raise BinanceError("value and step must be positive")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def stop_trigger_price(value: Decimal, tick: Decimal, close_side: str) -> Decimal:
    """Round toward an earlier protective trigger, never a looser one."""
    rounding = ROUND_CEILING if close_side == "SELL" else ROUND_DOWN
    return (value / tick).to_integral_value(rounding=rounding) * tick


class BinanceRest:
    """REST-only USD-M adapter. Private requests can never target production."""

    def __init__(self, config: LiveConfig, transport: Transport | None = None):
        self.config = config
        self.transport = transport or _default_transport
        self.server_offset_ms = 0

    def _request(self, method: str, base_url: str, path: str, params: dict[str, str] | None = None, *, signed: bool = False) -> Any:
        if signed and base_url != TESTNET_FUTURES_URL:
            raise BinanceError("signed requests are restricted to Binance Futures testnet")
        payload = dict(params or {})
        headers: dict[str, str] = {}
        if signed:
            if not self.config.api_key or not self.config.api_secret:
                raise BinanceError("Binance testnet credentials are required")
            payload["timestamp"] = str(int(datetime.now(UTC).timestamp() * 1000) + self.server_offset_ms)
            encoded = urlencode(payload)
            payload["signature"] = hmac.new(self.config.api_secret.encode("utf-8"), encoded.encode("utf-8"), hashlib.sha256).hexdigest()
            headers["X-MBX-APIKEY"] = self.config.api_key
        last_error: Exception | None = None
        attempts = self.config.max_attempts if method == "GET" else 1
        for _ in range(attempts):
            try:
                result = self.transport(method, f"{base_url}{path}", payload, headers, self.config.request_timeout_seconds)
                if isinstance(result, dict) and "code" in result and int(result["code"]) < 0:
                    raise BinanceError(f"Binance {result['code']}: {result.get('msg', '')}")
                return result
            except BinanceError as exc:
                last_error = exc
        raise BinanceError(str(last_error) if last_error else "request failed")

    def sync_time(self) -> int:
        before = int(datetime.now(UTC).timestamp() * 1000)
        result = self._request("GET", PUBLIC_FUTURES_URL, "/fapi/v1/time")
        after = int(datetime.now(UTC).timestamp() * 1000)
        if not isinstance(result, dict) or "serverTime" not in result:
            raise BinanceError("invalid server time response")
        self.server_offset_ms = int(result["serverTime"]) - (before + after) // 2
        return self.server_offset_ms

    def now(self) -> datetime:
        """Current Binance-aligned UTC time for scheduling and bar completion."""
        return datetime.now(UTC) + timedelta(milliseconds=self.server_offset_ms)

    def exchange_info(self) -> dict[str, Any]:
        result = self._request("GET", self.config.market_data_base_url, "/fapi/v1/exchangeInfo")
        if not isinstance(result, dict) or not isinstance(result.get("symbols"), list):
            raise BinanceError("invalid exchangeInfo response")
        return result

    def trading_exchange_info(self) -> dict[str, Any]:
        """Return the public contract catalogue of the configured testnet."""
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/exchangeInfo")
        if not isinstance(result, dict) or not isinstance(result.get("symbols"), list):
            raise BinanceError("invalid testnet exchangeInfo response")
        return result

    @staticmethod
    def _perpetual_usdt_symbols(exchange_info: dict[str, Any]) -> set[str]:
        return {
            str(item["symbol"])
            for item in exchange_info["symbols"]
            if item.get("quoteAsset") == "USDT" and item.get("contractType") == "PERPETUAL" and item.get("status") == "TRADING"
        }

    def tradable_symbols(self) -> list[str]:
        """Symbols with public price history that can also be ordered on testnet."""
        public = self._perpetual_usdt_symbols(self.exchange_info())
        testnet = self._perpetual_usdt_symbols(self.trading_exchange_info())
        return sorted(public & testnet)

    def symbol_filters(self, symbol: str) -> dict[str, Decimal]:
        # Quantity and stop-price filters must be those accepted by the venue
        # that receives the order, rather than the public-data venue.
        entry = next((item for item in self.trading_exchange_info()["symbols"] if item.get("symbol") == symbol), None)
        if entry is None:
            raise BinanceError(f"unknown testnet symbol {symbol}")
        filters = {item["filterType"]: item for item in entry.get("filters", [])}
        try:
            lot = filters["LOT_SIZE"]
            price = filters["PRICE_FILTER"]
            notional = filters["MIN_NOTIONAL"]
            return {"step_size": Decimal(lot["stepSize"]), "min_qty": Decimal(lot["minQty"]), "tick_size": Decimal(price["tickSize"]), "min_notional": Decimal(notional["notional"])}
        except KeyError as exc:
            raise BinanceError(f"{symbol} has incomplete order filters") from exc

    def klines(
        self, symbol: str, interval: str, limit: int, *, start_time: datetime | None = None, end_time: datetime | None = None,
    ) -> pl.DataFrame:
        params = {"symbol": symbol, "interval": interval, "limit": str(limit)}
        if start_time is not None:
            params["startTime"] = str(int(start_time.timestamp() * 1000))
        if end_time is not None:
            params["endTime"] = str(int(end_time.timestamp() * 1000))
        raw = self._request("GET", self.config.market_data_base_url, "/fapi/v1/klines", params)
        if not isinstance(raw, list):
            raise BinanceError(f"invalid kline response for {symbol}")
        rows = []
        for item in raw:
            if not isinstance(item, list) or len(item) < 9:
                raise BinanceError(f"malformed kline for {symbol}")
            rows.append({
                "symbol": symbol,
                "open_time": datetime.fromtimestamp(int(item[0]) / 1000, UTC),
                "open": float(item[1]), "high": float(item[2]), "low": float(item[3]), "close": float(item[4]),
                "quote_volume": float(item[7]), "trade_count": int(item[8]),
            })
        return pl.DataFrame(rows, schema={
            "symbol": pl.String, "open_time": pl.Datetime("us", "UTC"), "open": pl.Float64, "high": pl.Float64,
            "low": pl.Float64, "close": pl.Float64, "quote_volume": pl.Float64, "trade_count": pl.Int64,
        }).select(KLINE_COLUMNS).sort("open_time")

    def hourly_snapshot(self, symbols: list[str], limit: int) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        with ThreadPoolExecutor(max_workers=self.config.max_concurrent_market_requests) as executor:
            futures = {executor.submit(self.klines, symbol, "1h", limit): symbol for symbol in symbols}
            for future in as_completed(futures):
                frames.append(future.result())
        return pl.concat(frames, how="vertical") if frames else pl.DataFrame(schema={name: pl.Null for name in KLINE_COLUMNS})

    def position_mode(self) -> bool:
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/positionSide/dual", signed=True)
        if not isinstance(result, dict) or "dualSidePosition" not in result:
            raise BinanceError("invalid position mode response")
        return str(result["dualSidePosition"]).lower() == "true"

    def multi_asset_mode(self) -> bool:
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/multiAssetsMargin", signed=True)
        if not isinstance(result, dict) or "multiAssetsMargin" not in result:
            raise BinanceError("invalid multi-assets mode response")
        return str(result["multiAssetsMargin"]).lower() == "true"

    def balance(self) -> Decimal:
        result = self._request("GET", self.config.trading_base_url, "/fapi/v2/balance", signed=True)
        if not isinstance(result, list):
            raise BinanceError("invalid balance response")
        usdt = next((item for item in result if item.get("asset") == "USDT"), None)
        if usdt is None or "availableBalance" not in usdt:
            raise BinanceError("USDT available balance is missing")
        return Decimal(str(usdt["availableBalance"]))

    def positions(self) -> list[dict[str, Any]]:
        result = self._request("GET", self.config.trading_base_url, "/fapi/v2/positionRisk", signed=True)
        if not isinstance(result, list):
            raise BinanceError("invalid position risk response")
        return [item for item in result if Decimal(str(item.get("positionAmt", "0"))) != 0]

    def symbol_config(self, symbol: str) -> dict[str, Any]:
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/symbolConfig", {"symbol": symbol}, signed=True)
        if isinstance(result, list):
            result = next((item for item in result if item.get("symbol") == symbol), None)
        if not isinstance(result, dict):
            raise BinanceError(f"invalid symbol configuration for {symbol}")
        return result

    def ensure_symbol_config(self, symbol: str) -> None:
        item = self.symbol_config(symbol)
        if str(item.get("marginType", "")).lower() != "isolated" or int(item.get("leverage", 0)) != 1:
            raise BinanceError(f"{symbol} must be isolated at 1x leverage")

    def configure_symbol(self, symbol: str) -> None:
        try:
            self.ensure_symbol_config(symbol)
            return
        except BinanceError:
            pass
        try:
            self._request("POST", self.config.trading_base_url, "/fapi/v1/marginType", {
                "symbol": symbol, "marginType": "ISOLATED",
            }, signed=True)
        except BinanceError as exc:
            if "-4046" not in str(exc) and "No need to change" not in str(exc):
                raise
        self._request("POST", self.config.trading_base_url, "/fapi/v1/leverage", {
            "symbol": symbol, "leverage": "1",
        }, signed=True)
        self.ensure_symbol_config(symbol)

    def latest_price(self, symbol: str) -> Decimal:
        result = self._request("GET", self.config.market_data_base_url, "/fapi/v1/ticker/price", {"symbol": symbol})
        if not isinstance(result, dict) or "price" not in result:
            raise BinanceError(f"invalid ticker price for {symbol}")
        price = Decimal(str(result["price"]))
        if price <= 0:
            raise BinanceError(f"invalid ticker price for {symbol}")
        return price

    def open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol} if symbol is not None else None
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/openOrders", params, signed=True)
        if not isinstance(result, list):
            raise BinanceError("invalid open orders response")
        return result

    def open_algo_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol} if symbol is not None else None
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/openAlgoOrders", params, signed=True)
        if not isinstance(result, list):
            raise BinanceError("invalid open algo orders response")
        return result

    def account_check(self) -> dict[str, Any]:
        self.sync_time()
        if not self.position_mode():
            raise BinanceError("account must use Hedge Mode")
        if self.multi_asset_mode():
            raise BinanceError("account must use Single-Asset Mode")
        return {"available_usdt": str(self.balance()), "positions": self.positions(), "open_orders": self.open_orders(), "open_algo_orders": self.open_algo_orders()}

    def market_order(self, symbol: str, side: str, position_side: str, quantity: Decimal, client_order_id: str) -> dict[str, Any]:
        if not self.config.trading_enabled:
            raise BinanceError("TRADING_ENABLED is false")
        return self._request("POST", self.config.trading_base_url, "/fapi/v1/order", {
            "symbol": symbol, "side": side, "positionSide": position_side, "type": "MARKET",
            "quantity": format(quantity, "f"), "newClientOrderId": client_order_id, "newOrderRespType": "RESULT",
        }, signed=True)

    def query_order(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/order", {"symbol": symbol, "origClientOrderId": client_order_id}, signed=True)
        if not isinstance(result, dict):
            raise BinanceError("invalid order response")
        return result

    def query_order_by_id(self, symbol: str, order_id: str) -> dict[str, Any]:
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)
        if not isinstance(result, dict):
            raise BinanceError("invalid order response")
        return result

    def stop_market(self, symbol: str, side: str, position_side: str, trigger_price: Decimal, client_algo_id: str) -> dict[str, Any]:
        if not self.config.trading_enabled:
            raise BinanceError("TRADING_ENABLED is false")
        result = self._request("POST", self.config.trading_base_url, "/fapi/v1/algoOrder", {
            "algoType": "CONDITIONAL", "symbol": symbol, "side": side, "positionSide": position_side,
            "type": "STOP_MARKET", "triggerPrice": format(trigger_price, "f"), "workingType": "CONTRACT_PRICE",
            "closePosition": "true", "clientAlgoId": client_algo_id,
        }, signed=True)
        if not isinstance(result, dict) or "algoId" not in result:
            raise BinanceError("invalid stop algo response")
        return result

    def query_algo(self, symbol: str, client_algo_id: str) -> dict[str, Any]:
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/algoOrder", {"symbol": symbol, "clientAlgoId": client_algo_id}, signed=True)
        if not isinstance(result, dict):
            raise BinanceError("invalid algo order response")
        return result

    def query_algo_by_id(self, symbol: str, algo_id: str) -> dict[str, Any]:
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id}, signed=True)
        if not isinstance(result, dict):
            raise BinanceError("invalid algo order response")
        return result

    def cancel_algo(self, symbol: str, algo_id: str) -> None:
        self._request("DELETE", self.config.trading_base_url, "/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id}, signed=True)
