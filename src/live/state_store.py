from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from execution.bybit_executor import BybitExecutor


DEFAULT_STATE = {
    "current_position": 0,
    "position_qty": 0.0,
    "entry_price": None,
    "entry_time": None,
    "last_signal_timestamp": None,
    "last_decision_timestamp": None,
    "daily_pnl_pct": 0.0,
    "realized_pnl_pct_total": 0.0,
    "consecutive_losses": 0,
    "last_trade_day_utc": None,
    "circuit_breaker": False,
    "last_order": None,
    "exchange_last_sync_at": None,
    "exchange_last_sync_ok": False,
    "exchange_last_sync_error": None,
    "exchange_position_snapshot": None,
    "exchange_active_orders": [],
    "exchange_active_orders_count": 0,
    "exchange_managed_orders": [],
    "exchange_external_orders": [],
    "exchange_managed_orders_count": 0,
    "exchange_external_orders_count": 0,
    "bot_position_managed": False,
    "bot_position_pending_open": False,
    "bot_position_pending_close": False,
    "bot_last_open_order_link_id": None,
    "bot_last_close_order_link_id": None,
    "bot_position_take_profit": None,
    "bot_position_stop_loss": None,
    "external_position_detected": False,
    "external_position_snapshot": None,
}


class StateStore:
    def __init__(self, path: Path):
        self.path = path

    def _ensure_parent(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return deepcopy(DEFAULT_STATE)

        with self.path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        state = deepcopy(DEFAULT_STATE)
        state.update(data)
        return state

    def save(self, state: Dict[str, Any]) -> None:
        self._ensure_parent()
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, default=str)

    def reset_daily_if_needed(self, state: Dict[str, Any]) -> Dict[str, Any]:
        now_day = datetime.now(timezone.utc).date().isoformat()
        last_trade_day = state.get("last_trade_day_utc")

        if last_trade_day != now_day:
            state["daily_pnl_pct"] = 0.0
            state["last_trade_day_utc"] = now_day

        return state

    def reconcile_with_exchange(
        self,
        state: Dict[str, Any],
        exchange_position: Dict[str, Any] | None,
        active_orders: list[Dict[str, Any]],
        sync_ok: bool,
        sync_message: str,
    ) -> Dict[str, Any]:
        now_iso = datetime.now(timezone.utc).isoformat()

        managed_orders = []
        external_orders = []

        for order in active_orders:
            order_link_id = str(order.get("orderLinkId") or "").strip()
            if BybitExecutor.is_managed_order_link_id(order_link_id):
                managed_orders.append(order)
            else:
                external_orders.append(order)

        state["exchange_last_sync_at"] = now_iso
        state["exchange_last_sync_ok"] = bool(sync_ok)
        state["exchange_last_sync_error"] = None if sync_ok else sync_message
        state["exchange_position_snapshot"] = exchange_position
        state["exchange_active_orders"] = active_orders
        state["exchange_active_orders_count"] = len(active_orders)
        state["exchange_managed_orders"] = managed_orders
        state["exchange_external_orders"] = external_orders
        state["exchange_managed_orders_count"] = len(managed_orders)
        state["exchange_external_orders_count"] = len(external_orders)

        if not sync_ok:
            return state

        was_managed = bool(state.get("bot_position_managed", False))
        pending_open = bool(state.get("bot_position_pending_open", False))

        has_exchange_position = exchange_position is not None

        def clear_bot_position() -> None:
            state["current_position"] = 0
            state["position_qty"] = 0.0
            state["entry_price"] = None
            state["entry_time"] = None
            state["bot_position_managed"] = False
            state["bot_position_pending_open"] = False
            state["bot_position_pending_close"] = False
            state["bot_position_take_profit"] = None
            state["bot_position_stop_loss"] = None
            state["bot_last_open_order_link_id"] = None
            state["bot_last_close_order_link_id"] = None
            state["external_position_detected"] = False
            state["external_position_snapshot"] = None

        def set_managed_position_from_exchange() -> None:
            state["external_position_detected"] = False
            state["external_position_snapshot"] = None
            state["current_position"] = int(exchange_position.get("current_position", 0))
            state["position_qty"] = float(exchange_position.get("position_qty", 0.0))

            entry_price = float(exchange_position.get("entry_price", 0.0))
            state["entry_price"] = entry_price if entry_price > 0 else None

            if state.get("entry_time") is None:
                state["entry_time"] = now_iso

            state["bot_position_managed"] = True
            state["bot_position_pending_open"] = False
            state["bot_position_pending_close"] = False
            state["bot_position_take_profit"] = exchange_position.get("take_profit")
            state["bot_position_stop_loss"] = exchange_position.get("stop_loss")

        if not has_exchange_position:
            clear_bot_position()
            return state

        if pending_open:
            set_managed_position_from_exchange()
            return state

        if was_managed:
            set_managed_position_from_exchange()
            return state

        state["external_position_detected"] = True
        state["external_position_snapshot"] = exchange_position
        state["current_position"] = 0
        state["position_qty"] = 0.0
        state["entry_price"] = None
        state["entry_time"] = None
        state["bot_position_managed"] = False
        state["bot_position_pending_open"] = False
        state["bot_position_pending_close"] = False
        state["bot_position_take_profit"] = None
        state["bot_position_stop_loss"] = None

        return state
