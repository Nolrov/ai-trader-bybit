from __future__ import annotations
from urllib.parse import urlencode

import hashlib
import hmac
import json
import math
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Dict

import requests


BOT_ORDER_LINK_PREFIX = "AI_TRADER"


@dataclass
class ExecutionResult:
    ok: bool
    mode: str
    side: str | None
    qty: float
    symbol: str
    order_type: str
    message: str
    raw: Dict[str, Any] | None = None


@dataclass
class ExchangeSyncResult:
    ok: bool
    symbol: str
    category: str
    position: Dict[str, Any] | None
    active_orders: list[Dict[str, Any]]
    message: str


class BybitExecutor:
    def __init__(self, settings):
        self.settings = settings
        self.base_url = "https://api-testnet.bybit.com" if settings.testnet else "https://api.bybit.com"
        self._instrument_cache: dict[tuple[str, str], dict[str, Any]] = {}

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _decimals_from_step(step: float) -> int:
        step_str = f"{step:.16f}".rstrip("0")
        if "." not in step_str:
            return 0
        return len(step_str.split(".")[1])

    @staticmethod
    def is_managed_order_link_id(order_link_id: Any) -> bool:
        value = str(order_link_id or "").strip()
        return value.startswith(BOT_ORDER_LINK_PREFIX)

    @staticmethod
    def build_order_link_id(action: str) -> str:
        suffix = uuid.uuid4().hex[:20].upper()
        clean_action = str(action or "ORD").upper()
        return f"{BOT_ORDER_LINK_PREFIX}_{clean_action}_{suffix}"[:36]

    def _get_instrument_info(self, symbol: str, category: str = "linear") -> dict[str, Any]:
        cache_key = (category, symbol)
        if cache_key in self._instrument_cache:
            return self._instrument_cache[cache_key]

        url = f"{self.base_url}/v5/market/instruments-info"
        params = {
            "category": category,
            "symbol": symbol,
        }

        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit instruments-info error: {data}")

        items = data.get("result", {}).get("list", [])
        if not items:
            raise RuntimeError(f"Instrument info not found for symbol={symbol}, category={category}")

        instrument = items[0]
        self._instrument_cache[cache_key] = instrument
        return instrument

    def get_last_price(self, symbol: str, category: str = "linear") -> float:
        url = f"{self.base_url}/v5/market/tickers"
        params = {
            "category": category,
            "symbol": symbol,
        }

        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit tickers error: {data}")

        items = data.get("result", {}).get("list", [])
        if not items:
            raise RuntimeError(f"Ticker not found for symbol={symbol}, category={category}")

        last_price = self._safe_float(items[0].get("lastPrice"), 0.0)
        if last_price <= 0:
            raise RuntimeError(f"Invalid last price for symbol={symbol}, category={category}")

        return last_price

    def _normalize_qty(
        self,
        qty: float,
        price: float,
        symbol: str,
        category: str,
    ) -> tuple[float, str | None, dict[str, Any] | None]:
        instrument = self._get_instrument_info(symbol=symbol, category=category)
        lot = instrument.get("lotSizeFilter", {})

        qty_step = self._safe_float(lot.get("qtyStep"), 0.0)
        min_qty = self._safe_float(lot.get("minOrderQty"), 0.0)
        min_notional = self._safe_float(lot.get("minNotionalValue"), 0.0)

        if qty <= 0:
            return 0.0, "invalid_qty", instrument

        normalized_qty = qty

        if qty_step > 0:
            normalized_qty = math.floor(qty / qty_step) * qty_step
            decimals = self._decimals_from_step(qty_step)
            normalized_qty = round(normalized_qty, decimals)
        else:
            normalized_qty = round(normalized_qty, 6)

        if normalized_qty <= 0:
            return 0.0, "qty_below_step_after_normalization", instrument

        if min_qty > 0 and normalized_qty < min_qty:
            return normalized_qty, f"qty_below_min_order_qty: min={min_qty}", instrument

        if price > 0 and min_notional > 0:
            notional = normalized_qty * price
            if notional < min_notional:
                return normalized_qty, f"notional_below_min_notional: min={min_notional}", instrument

        return normalized_qty, None, instrument

    def _normalize_price(
        self,
        price: float,
        symbol: str,
        category: str,
    ) -> tuple[float, str | None, dict[str, Any] | None]:
        instrument = self._get_instrument_info(symbol=symbol, category=category)
        price_filter = instrument.get("priceFilter", {})

        tick_size = self._safe_float(price_filter.get("tickSize"), 0.0)
        min_price = self._safe_float(price_filter.get("minPrice"), 0.0)
        max_price = self._safe_float(price_filter.get("maxPrice"), 0.0)

        if price <= 0:
            return 0.0, "invalid_price", instrument

        normalized_price = price

        if tick_size > 0:
            normalized_price = math.floor(price / tick_size) * tick_size
            decimals = self._decimals_from_step(tick_size)
            normalized_price = round(normalized_price, decimals)
        else:
            normalized_price = round(normalized_price, 8)

        if normalized_price <= 0:
            return 0.0, "price_below_tick_after_normalization", instrument

        if min_price > 0 and normalized_price < min_price:
            return normalized_price, f"price_below_min_price: min={min_price}", instrument

        if max_price > 0 and normalized_price > max_price:
            return normalized_price, f"price_above_max_price: max={max_price}", instrument

        return normalized_price, None, instrument

    def _ensure_credentials(self) -> None:
        if not self.settings.api_key or not self.settings.api_secret:
            raise RuntimeError("missing_bybit_credentials")

    def _build_query_string(self, params: dict[str, Any]) -> str:
        clean = {k: v for k, v in params.items() if v is not None}
        return urlencode(clean)

    def _build_signed_headers(self, query_string: str) -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        sign_payload = f"{timestamp}{self.settings.api_key}{recv_window}{query_string}"
        signature = hmac.new(
            self.settings.api_secret.encode("utf-8"),
            sign_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return {
            "X-BAPI-API-KEY": self.settings.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }

    def _signed_get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        self._ensure_credentials()

        query_string = self._build_query_string(params)
        headers = self._build_signed_headers(query_string)
        url = f"{self.base_url}{endpoint}"

        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit private GET error: {data}")

        return data

    def get_open_position(self, symbol: str, category: str = "linear") -> Dict[str, Any] | None:
        if self.settings.mode == "paper":
            return None

        data = self._signed_get(
            endpoint="/v5/position/list",
            params={
                "category": category,
                "symbol": symbol,
            },
        )

        items = data.get("result", {}).get("list", [])
        if not items:
            return None

        for item in items:
            size = self._safe_float(item.get("size"), 0.0)
            side = str(item.get("side") or "").strip()
            if size > 0 and side:
                if side.lower() == "buy":
                    current_position = 1
                elif side.lower() == "sell":
                    current_position = -1
                else:
                    current_position = 0

                take_profit = self._safe_float(item.get("takeProfit"), 0.0)
                stop_loss = self._safe_float(item.get("stopLoss"), 0.0)

                return {
                    "symbol": item.get("symbol", symbol),
                    "category": category,
                    "side": side,
                    "current_position": current_position,
                    "position_qty": size,
                    "entry_price": self._safe_float(item.get("avgPrice"), 0.0),
                    "mark_price": self._safe_float(item.get("markPrice"), 0.0),
                    "unrealized_pnl": self._safe_float(item.get("unrealisedPnl"), 0.0),
                    "liq_price": self._safe_float(item.get("liqPrice"), 0.0),
                    "take_profit": take_profit if take_profit > 0 else None,
                    "stop_loss": stop_loss if stop_loss > 0 else None,
                    "has_protection": bool(take_profit > 0 and stop_loss > 0),
                    "raw": item,
                }

        return None

    def get_active_orders(self, symbol: str, category: str = "linear") -> list[Dict[str, Any]]:
        if self.settings.mode == "paper":
            return []

        data = self._signed_get(
            endpoint="/v5/order/realtime",
            params={
                "category": category,
                "symbol": symbol,
                "openOnly": 0,
            },
        )

        items = data.get("result", {}).get("list", [])
        active_orders: list[Dict[str, Any]] = []

        for item in items:
            status = str(item.get("orderStatus") or "").strip()
            if status in {"New", "PartiallyFilled", "Untriggered"}:
                order_link_id = str(item.get("orderLinkId") or "").strip()
                row = dict(item)
                row["is_managed"] = self.is_managed_order_link_id(order_link_id)
                active_orders.append(row)

        return active_orders

    def sync_exchange_state(self, symbol: str, category: str = "linear") -> ExchangeSyncResult:
        if self.settings.mode == "paper":
            return ExchangeSyncResult(
                ok=True,
                symbol=symbol,
                category=category,
                position=None,
                active_orders=[],
                message="paper_mode_no_exchange_state",
            )

        try:
            position = self.get_open_position(symbol=symbol, category=category)
            active_orders = self.get_active_orders(symbol=symbol, category=category)
            return ExchangeSyncResult(
                ok=True,
                symbol=symbol,
                category=category,
                position=position,
                active_orders=active_orders,
                message="exchange_state_synced",
            )
        except Exception as exc:
            return ExchangeSyncResult(
                ok=False,
                symbol=symbol,
                category=category,
                position=None,
                active_orders=[],
                message=f"exchange_sync_error: {exc}",
            )

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        category: str = "linear",
        order_type: str = "Market",
        reduce_only: bool = False,
        limit_price: float | None = None,
        take_profit: float | None = None,
        stop_loss: float | None = None,
        order_link_id: str | None = None,
        require_tpsl_on_open: bool = True,
    ) -> ExecutionResult:
        try:
            normalized_qty, qty_error, instrument = self._normalize_qty(
                qty=qty,
                price=price,
                symbol=symbol,
                category=category,
            )
        except Exception as exc:
            return ExecutionResult(
                ok=False,
                mode=self.settings.mode,
                side=side,
                qty=qty,
                symbol=symbol,
                order_type=order_type,
                message=f"instrument_info_error: {exc}",
                raw=None,
            )

        if qty_error is not None:
            return ExecutionResult(
                ok=False,
                mode=self.settings.mode,
                side=side,
                qty=normalized_qty,
                symbol=symbol,
                order_type=order_type,
                message=qty_error,
                raw={"instrument": instrument},
            )

        normalized_limit_price = None
        if order_type == "Limit":
            if limit_price is None:
                return ExecutionResult(
                    ok=False,
                    mode=self.settings.mode,
                    side=side,
                    qty=normalized_qty,
                    symbol=symbol,
                    order_type=order_type,
                    message="missing_limit_price",
                    raw=None,
                )

            try:
                normalized_limit_price, price_error, instrument = self._normalize_price(
                    price=limit_price,
                    symbol=symbol,
                    category=category,
                )
            except Exception as exc:
                return ExecutionResult(
                    ok=False,
                    mode=self.settings.mode,
                    side=side,
                    qty=normalized_qty,
                    symbol=symbol,
                    order_type=order_type,
                    message=f"instrument_info_error: {exc}",
                    raw=None,
                )

            if price_error is not None:
                return ExecutionResult(
                    ok=False,
                    mode=self.settings.mode,
                    side=side,
                    qty=normalized_qty,
                    symbol=symbol,
                    order_type=order_type,
                    message=price_error,
                    raw={"instrument": instrument},
                )

        normalized_tp = None
        normalized_sl = None

        is_opening_order = not reduce_only

        if is_opening_order and require_tpsl_on_open:
            if take_profit is None or stop_loss is None:
                return ExecutionResult(
                    ok=False,
                    mode=self.settings.mode,
                    side=side,
                    qty=normalized_qty,
                    symbol=symbol,
                    order_type=order_type,
                    message="missing_required_tp_sl_for_open",
                    raw=None,
                )

        if take_profit is not None:
            normalized_tp, tp_error, instrument = self._normalize_price(
                price=take_profit,
                symbol=symbol,
                category=category,
            )
            if tp_error is not None:
                return ExecutionResult(
                    ok=False,
                    mode=self.settings.mode,
                    side=side,
                    qty=normalized_qty,
                    symbol=symbol,
                    order_type=order_type,
                    message=f"invalid_take_profit: {tp_error}",
                    raw={"instrument": instrument},
                )

        if stop_loss is not None:
            normalized_sl, sl_error, instrument = self._normalize_price(
                price=stop_loss,
                symbol=symbol,
                category=category,
            )
            if sl_error is not None:
                return ExecutionResult(
                    ok=False,
                    mode=self.settings.mode,
                    side=side,
                    qty=normalized_qty,
                    symbol=symbol,
                    order_type=order_type,
                    message=f"invalid_stop_loss: {sl_error}",
                    raw={"instrument": instrument},
                )

        if self.settings.mode == "paper":
            raw_payload = {
                "symbol": symbol,
                "side": side,
                "qty": normalized_qty,
                "category": category,
                "orderType": order_type,
                "reduceOnly": reduce_only,
                "price_reference": price,
                "orderLinkId": order_link_id,
                "takeProfit": normalized_tp,
                "stopLoss": normalized_sl,
            }
            if normalized_limit_price is not None:
                raw_payload["price"] = normalized_limit_price

            return ExecutionResult(
                ok=True,
                mode="paper",
                side=side,
                qty=normalized_qty,
                symbol=symbol,
                order_type=order_type,
                message="paper_order_emitted",
                raw=raw_payload,
            )

        if not self.settings.api_key or not self.settings.api_secret:
            return ExecutionResult(
                ok=False,
                mode=self.settings.mode,
                side=side,
                qty=normalized_qty,
                symbol=symbol,
                order_type=order_type,
                message="missing_bybit_credentials",
                raw=None,
            )

        if order_link_id is None:
            order_link_id = self.build_order_link_id("OPEN" if is_opening_order else "CLOSE")

        endpoint = "/v5/order/create"
        url = f"{self.base_url}{endpoint}"

        payload = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": str(normalized_qty),
            "timeInForce": "GTC",
            "reduceOnly": reduce_only,
            "orderLinkId": order_link_id,
        }

        if normalized_limit_price is not None:
            payload["price"] = str(normalized_limit_price)

        if normalized_tp is not None:
            payload["takeProfit"] = str(normalized_tp)

        if normalized_sl is not None:
            payload["stopLoss"] = str(normalized_sl)

        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        body = json.dumps(payload, separators=(",", ":"))

        sign_payload = f"{timestamp}{self.settings.api_key}{recv_window}{body}"
        signature = hmac.new(
            self.settings.api_secret.encode("utf-8"),
            sign_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.settings.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(url, headers=headers, data=body, timeout=15)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            return ExecutionResult(
                ok=False,
                mode=self.settings.mode,
                side=side,
                qty=normalized_qty,
                symbol=symbol,
                order_type=order_type,
                message=f"http_error: {exc}",
                raw=None,
            )

        if data.get("retCode") != 0:
            return ExecutionResult(
                ok=False,
                mode=self.settings.mode,
                side=side,
                qty=normalized_qty,
                symbol=symbol,
                order_type=order_type,
                message=f"bybit_error: {data.get('retMsg')}",
                raw=data,
            )

        enriched_raw = dict(data)
        enriched_raw["submitted_order_link_id"] = order_link_id
        enriched_raw["submitted_take_profit"] = normalized_tp
        enriched_raw["submitted_stop_loss"] = normalized_sl

        return ExecutionResult(
            ok=True,
            mode=self.settings.mode,
            side=side,
            qty=normalized_qty,
            symbol=symbol,
            order_type=order_type,
            message="order_sent",
            raw=enriched_raw,
        )

    @staticmethod
    def to_dict(result: ExecutionResult) -> Dict[str, Any]:
        return asdict(result)
