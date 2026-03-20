from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict

import requests


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


class BybitExecutor:
    def __init__(self, settings):
        self.settings = settings
        self.base_url = "https://api-testnet.bybit.com" if settings.testnet else "https://api.bybit.com"

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        category: str = "linear",
        order_type: str = "Market",
        reduce_only: bool = False,
    ) -> ExecutionResult:
        if self.settings.mode == "paper":
            return ExecutionResult(
                ok=True,
                mode="paper",
                side=side,
                qty=qty,
                symbol=symbol,
                order_type=order_type,
                message="paper_order_emitted",
                raw={
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "category": category,
                    "orderType": order_type,
                    "reduceOnly": reduce_only,
                },
            )

        if not self.settings.api_key or not self.settings.api_secret:
            return ExecutionResult(
                ok=False,
                mode=self.settings.mode,
                side=side,
                qty=qty,
                symbol=symbol,
                order_type=order_type,
                message="missing_bybit_credentials",
                raw=None,
            )

        endpoint = "/v5/order/create"
        url = f"{self.base_url}{endpoint}"

        payload = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": str(qty),
            "timeInForce": self.settings.time_in_force,
            "reduceOnly": reduce_only,
        }

        timestamp = str(int(time.time() * 1000))
        recv_window = self.settings.recv_window
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
                qty=qty,
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
                qty=qty,
                symbol=symbol,
                order_type=order_type,
                message=f"bybit_error: {data.get('retMsg')}",
                raw=data,
            )

        return ExecutionResult(
            ok=True,
            mode=self.settings.mode,
            side=side,
            qty=qty,
            symbol=symbol,
            order_type=order_type,
            message="order_sent",
            raw=data,
        )

    @staticmethod
    def to_dict(result: ExecutionResult) -> Dict[str, Any]:
        return asdict(result)