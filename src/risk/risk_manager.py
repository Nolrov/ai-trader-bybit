from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class RiskDecision:
    approved: bool
    reason: str
    target_position: int
    order_qty: float
    order_side: str | None
    reduce_only: bool


class RiskManager:
    def __init__(self, settings):
        self.settings = settings

    def _round_qty(self, qty: float) -> float:
        return round(max(qty, 0.0), 6)

    def _calc_open_qty_from_usdt(self, price: float) -> float:
        if price <= 0:
            return 0.0
        qty = self.settings.max_position_usdt / price
        return self._round_qty(qty)

    def evaluate(
        self,
        desired_position: int,
        current_position: int,
        price: float,
        state: Dict[str, Any],
    ) -> RiskDecision:

        # --- SAFETY ---
        if state.get("circuit_breaker", False):
            return RiskDecision(False, "circuit_breaker_active", current_position, 0.0, None, False)

        daily_pnl_pct = float(state.get("daily_pnl_pct", 0.0))
        if daily_pnl_pct <= -abs(self.settings.max_daily_loss_pct):
            return RiskDecision(False, "daily_loss_limit_hit", current_position, 0.0, None, False)

        consecutive_losses = int(state.get("consecutive_losses", 0))
        if consecutive_losses >= int(self.settings.max_consecutive_losses):
            return RiskDecision(False, "max_consecutive_losses_hit", current_position, 0.0, None, False)

        if desired_position > 0 and not self.settings.allow_long:
            return RiskDecision(False, "longs_disabled", current_position, 0.0, None, False)

        if desired_position < 0 and not self.settings.allow_short:
            return RiskDecision(False, "shorts_disabled", current_position, 0.0, None, False)

        # --- NO CHANGE ---
        if desired_position == current_position:
            return RiskDecision(False, "no_position_change", current_position, 0.0, None, False)

        current_qty = self._round_qty(float(state.get("position_qty", 0.0)))
        open_qty = self._calc_open_qty_from_usdt(price)

        # --- OPEN ---
        if current_position == 0:
            if desired_position > 0:
                if open_qty <= 0:
                    return RiskDecision(False, "qty_zero_or_invalid", current_position, 0.0, None, False)
                return RiskDecision(True, "open_long", 1, open_qty, "Buy", False)

            if desired_position < 0:
                if open_qty <= 0:
                    return RiskDecision(False, "qty_zero_or_invalid", current_position, 0.0, None, False)
                return RiskDecision(True, "open_short", -1, open_qty, "Sell", False)

        # --- CLOSE ---
        if current_position > 0 and desired_position == 0:
            if current_qty <= 0:
                return RiskDecision(False, "missing_position_qty_for_close", current_position, 0.0, None, False)
            return RiskDecision(True, "close_long", 0, current_qty, "Sell", True)

        if current_position < 0 and desired_position == 0:
            if current_qty <= 0:
                return RiskDecision(False, "missing_position_qty_for_close", current_position, 0.0, None, False)
            return RiskDecision(True, "close_short", 0, current_qty, "Buy", True)

        # --- REVERSE ---
        if self.settings.one_position_only and current_position != 0 and desired_position != 0:
            if current_qty <= 0:
                return RiskDecision(False, "missing_position_qty_for_reverse", current_position, 0.0, None, False)

            return RiskDecision(
                True,
                "close_before_reverse",
                0,
                current_qty,
                "Sell" if current_position > 0 else "Buy",
                True,
            )

        return RiskDecision(False, "unsupported_transition", current_position, 0.0, None, False)
