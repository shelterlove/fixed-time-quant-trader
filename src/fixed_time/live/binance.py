from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from math import lcm
from http.client import HTTPException
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
    def __init__(self, message: str, code: int | None = None):
        super().__init__(message)
        self.code = code

    @property
    def not_found(self) -> bool:
        return self.code in {-2011, -2013}

    @property
    def rejected(self) -> bool:
        return self.code is not None and self.code <= -1100 and self.code not in {-4111, -4115, -4116} and "duplicate" not in str(self).lower()


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
        try:
            code = json.loads(body).get("code")
        except (ValueError, AttributeError):
            code = None
        raise BinanceError(f"HTTP {exc.code}: {body}", code) from exc
    except URLError as exc:
        raise BinanceError(f"network error: {exc.reason}") from exc
    except (OSError, HTTPException, ValueError) as exc:
        raise BinanceError(f"incomplete exchange response: {exc}") from exc


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
        self._catalogues: dict[str, tuple[datetime, dict[str, Any]]] = {}
        self._filters: dict[str, dict[str, Decimal]] = {}

    def _request(self, method: str, base_url: str, path: str, params: dict[str, str] | None = None, *, signed: bool = False) -> Any:
        if signed and base_url != TESTNET_FUTURES_URL:
            raise BinanceError("signed requests are restricted to Binance Futures testnet")
        if signed:
            if not self.config.api_key or not self.config.api_secret:
                raise BinanceError("Binance testnet credentials are required")
        last_error: Exception | None = None
        attempts = self.config.max_attempts if method == "GET" else 1
        for _ in range(attempts):
            payload = dict(params or {})
            headers: dict[str, str] = {}
            if signed:
                payload["timestamp"] = str(int(datetime.now(UTC).timestamp() * 1000) + self.server_offset_ms)
                encoded = urlencode(payload)
                payload["signature"] = hmac.new(
                    self.config.api_secret.encode("utf-8"), encoded.encode("utf-8"), hashlib.sha256
                ).hexdigest()
                headers["X-MBX-APIKEY"] = self.config.api_key
            try:
                result = self.transport(method, f"{base_url}{path}", payload, headers, self.config.request_timeout_seconds)
                if isinstance(result, dict) and "code" in result and int(result["code"]) < 0:
                    raise BinanceError(f"Binance {result['code']}: {result.get('msg', '')}", int(result["code"]))
                return result
            except BinanceError as exc:
                if exc.rejected:
                    raise
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
        return self._catalogue(self.config.market_data_base_url)

    def trading_exchange_info(self) -> dict[str, Any]:
        """Return the public contract catalogue of the configured testnet."""
        return self._catalogue(self.config.trading_base_url)

    def _catalogue(self, base_url: str) -> dict[str, Any]:
        cached = self._catalogues.get(base_url)
        if cached and self.now() - cached[0] < timedelta(minutes=5):
            return cached[1]
        result = self._request("GET", base_url, "/fapi/v1/exchangeInfo")
        if not isinstance(result, dict) or not isinstance(result.get("symbols"), list):
            raise BinanceError("invalid exchangeInfo response")
        self._catalogues[base_url] = (self.now(), result)
        if base_url == self.config.trading_base_url:
            self._filters.clear()
        return result

    @staticmethod
    def _perpetual_usdt_symbols(exchange_info: dict[str, Any]) -> set[str]:
        return {
            str(item["symbol"])
            for item in exchange_info["symbols"]
            if item.get("quoteAsset") == "USDT" and item.get("contractType") == "PERPETUAL" and item.get("status") == "TRADING"
        }

    def market_data_symbols(self) -> list[str]:
        """The full public USDⓈ-M universe used for frozen signal rankings."""
        return sorted(self._perpetual_usdt_symbols(self.exchange_info()))

    def trading_symbols(self) -> list[str]:
        """USDⓈ-M contracts that Binance currently accepts on testnet."""
        return sorted(self._perpetual_usdt_symbols(self.trading_exchange_info()))

    def tradable_symbols(self) -> list[str]:
        """Symbols with both public price history and testnet order support."""
        return sorted(set(self.market_data_symbols()) & set(self.trading_symbols()))

    def symbol_filters(self, symbol: str) -> dict[str, Decimal]:
        # Quantity and stop-price filters must be those accepted by the venue
        # that receives the order, rather than the public-data venue.
        catalogue = self.trading_exchange_info()
        if symbol in self._filters:
            return self._filters[symbol]
        entry = next((item for item in catalogue["symbols"] if item.get("symbol") == symbol), None)
        if entry is None:
            raise BinanceError(f"unknown testnet symbol {symbol}")
        filters = {item["filterType"]: item for item in entry.get("filters", [])}
        try:
            lots = [filters["LOT_SIZE"]]
            if "MARKET_LOT_SIZE" in filters:
                lots.append(filters["MARKET_LOT_SIZE"])
            price = filters["PRICE_FILTER"]
            notional = filters["MIN_NOTIONAL"]
            steps = [Decimal(lot["stepSize"]) for lot in lots if Decimal(lot["stepSize"]) > 0]
            scale = 10 ** max(-step.as_tuple().exponent for step in steps)
            step = Decimal(lcm(*(int(value * scale) for value in steps))) / scale
            result = {"step_size": step, "min_qty": max(Decimal(lot["minQty"]) for lot in lots),
                    "max_qty": min(Decimal(lot.get("maxQty", "Infinity")) for lot in lots),
                    "tick_size": Decimal(price["tickSize"]), "min_notional": Decimal(notional["notional"])}
            self._filters[symbol] = result
            return result
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

    def hourly_snapshot(self, symbols: list[str], limit: int, *, end_time: datetime | None = None) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        with ThreadPoolExecutor(max_workers=self.config.max_concurrent_market_requests) as executor:
            futures = {executor.submit(self.klines, symbol, "1h", limit, end_time=end_time): symbol for symbol in symbols}
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

    def wallet_balance(self) -> Decimal:
        """USDT wallet balance, deliberately distinct from leverage-sensitive available balance."""
        result = self._request("GET", self.config.trading_base_url, "/fapi/v3/account", signed=True)
        if not isinstance(result, dict) or not isinstance(result.get("assets"), list):
            raise BinanceError("invalid account snapshot")
        usdt = next((item for item in result["assets"] if item.get("asset") == "USDT"), None)
        if usdt is None or "walletBalance" not in usdt:
            raise BinanceError("USDT wallet balance is missing")
        return Decimal(str(usdt["walletBalance"]))

    def user_trades(self, symbol: str, *, from_id: int | None = None, order_id: str | None = None) -> list[dict[str, Any]]:
        """Return account fills in stable trade-id order for incremental ledger sync."""
        params = {"symbol": symbol, "limit": "1000"}
        if from_id is not None:
            params["fromId"] = str(from_id)
        if order_id is not None:
            params["orderId"] = str(order_id)
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/userTrades", params, signed=True)
        if not isinstance(result, list):
            raise BinanceError(f"invalid user trade response for {symbol}")
        return sorted(result, key=lambda row: int(row["id"]))

    def income_history(self, *, start_time: datetime | None = None, end_time: datetime | None = None,
                       page: int = 1) -> list[dict[str, Any]]:
        params = {"limit": "1000", "page": str(page)}
        if start_time is not None:
            params["startTime"] = str(int(start_time.timestamp() * 1000))
        if end_time is not None:
            params["endTime"] = str(int(end_time.timestamp() * 1000))
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/income", params, signed=True)
        if not isinstance(result, list):
            raise BinanceError("invalid income history response")
        return result

    def mark_price_close(self, symbol: str, minute_end: datetime) -> Decimal:
        """Return exactly one completed testnet mark-price minute, never a live tick."""
        start = minute_end.astimezone(UTC) - timedelta(minutes=1)
        raw = self._request("GET", self.config.trading_base_url, "/fapi/v1/markPriceKlines", {
            "symbol": symbol, "interval": "1m", "limit": "1",
            "startTime": str(int(start.timestamp() * 1000)),
            "endTime": str(int((minute_end - timedelta(milliseconds=1)).timestamp() * 1000)),
        })
        if not isinstance(raw, list) or len(raw) != 1 or not isinstance(raw[0], list) or len(raw[0]) < 5:
            raise BinanceError(f"missing completed mark-price minute for {symbol}")
        if int(raw[0][0]) != int(start.timestamp() * 1000):
            raise BinanceError(f"unexpected mark-price minute for {symbol}")
        close = Decimal(str(raw[0][4]))
        if close <= 0:
            raise BinanceError(f"invalid mark-price close for {symbol}")
        return close

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
        self.verify_symbol_config(symbol, {getattr(self.config, "leverage", 2)})

    def verify_symbol_config(self, symbol: str, allowed_leverages: set[int]) -> None:
        """Check a held position without silently changing its leverage."""
        item = self.symbol_config(symbol)
        leverage = int(item.get("leverage", 0))
        if str(item.get("marginType", "")).lower() != "isolated" or leverage not in allowed_leverages:
            allowed = "/".join(f"{value}x" for value in sorted(allowed_leverages))
            raise BinanceError(f"{symbol} must be isolated at {allowed} leverage")

    def configure_symbol(self, symbol: str) -> None:
        item = self.symbol_config(symbol)
        target = getattr(self.config, "leverage", 2)
        if str(item.get("marginType", "")).lower() == "isolated" and int(item.get("leverage", 0)) == target:
            return
        try:
            self._request("POST", self.config.trading_base_url, "/fapi/v1/marginType", {
                "symbol": symbol, "marginType": "ISOLATED",
            }, signed=True)
        except BinanceError as exc:
            if "-4046" not in str(exc) and "No need to change" not in str(exc):
                raise
        self._request("POST", self.config.trading_base_url, "/fapi/v1/leverage", {
            "symbol": symbol, "leverage": str(target),
        }, signed=True)
        self.ensure_symbol_config(symbol)

    def latest_price(self, symbol: str) -> Decimal:
        result = self._request("GET", self.config.trading_base_url, "/fapi/v1/ticker/price", {"symbol": symbol})
        if not isinstance(result, dict) or "price" not in result:
            raise BinanceError(f"invalid ticker price for {symbol}")
        price = Decimal(str(result["price"]))
        if price <= 0:
            raise BinanceError(f"invalid ticker price for {symbol}")
        return price

    def aggregate_trades(self, symbol: str, start: datetime, end: datetime, cursor: int | None = None) -> list[dict[str, Any]]:
        """One bounded incremental batch. The next poll continues at its last ID."""
        params = {"symbol": symbol, "limit": "1000"}
        if cursor is None:
            params.update(startTime=str(int(start.timestamp() * 1000)),
                          endTime=str(int(min(end, start + timedelta(minutes=59)).timestamp() * 1000)))
        else:
            params["fromId"] = str(cursor + 1)
        lower, upper = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        result = []
        # A bounded catch-up batch runs on the position's read worker. Normal
        # polls need one page; a busy symbol can drain up to 10,000 trades.
        for _ in range(10):
            rows = self._request("GET", self.config.trading_base_url, "/fapi/v1/aggTrades", params)
            if not isinstance(rows, list):
                raise BinanceError("invalid aggregate trade response")
            result.extend(row for row in rows if (cursor is not None or lower <= int(row["T"])) and int(row["T"]) <= upper)
            if len(rows) < 1000 or int(rows[-1]["T"]) >= upper:
                break
            params = {"symbol": symbol, "limit": "1000", "fromId": str(int(rows[-1]["a"]) + 1)}
        return result

    def cancel_order(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        return self._request("DELETE", self.config.trading_base_url, "/fapi/v1/order", {"symbol": symbol, "origClientOrderId": client_order_id}, signed=True)

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

    def stop_market(self, symbol: str, side: str, position_side: str, trigger_price: Decimal, client_algo_id: str, *, quantity: Decimal | None = None) -> dict[str, Any]:
        if not self.config.trading_enabled:
            raise BinanceError("TRADING_ENABLED is false")
        result = self._request("POST", self.config.trading_base_url, "/fapi/v1/algoOrder", {
            "algoType": "CONDITIONAL", "symbol": symbol, "side": side, "positionSide": position_side,
            "type": "STOP_MARKET", "triggerPrice": format(trigger_price, "f"), "workingType": "CONTRACT_PRICE",
            **({"closePosition": "true"} if quantity is None else {"quantity": format(quantity, "f")}),
            "clientAlgoId": client_algo_id,
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
