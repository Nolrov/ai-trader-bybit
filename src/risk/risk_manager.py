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
        if state.get("circuit_breaker", False):
            return RiskDecision(
                approved=False,
                reason="circuit_breaker_active",
                target_position=current_position,
                order_qty=0.0,
                order_side=None,
                reduce_only=False,
            )

        daily_pnl_pct = float(state.get("daily_pnl_pct", 0.0))
        if daily_pnl_pct <= -abs(self.settings.max_daily_loss_pct):
            return RiskDecision(
                approved=False,
                reason="daily_loss_limit_hit",
                target_position=current_position,
                order_qty=0.0,
                order_side=None,
                reduce_only=False,
            )

        consecutive_losses = int(state.get("consecutive_losses", 0))
        if consecutive_losses >= int(self.settings.max_consecutive_losses):
            return RiskDecision(
                approved=False,
                reason="max_consecutive_losses_hit",
                target_position=current_position,
                order_qty=0.0,
                order_side=None,
                reduce_only=False,
            )

        if desired_position > 0 and not self.settings.allow_long:
            return RiskDecision(
                approved=False,
                reason="longs_disabled",
                target_position=current_position,
                order_qty=0.0,
                order_side=None,
                reduce_only=False,
            )

        if desired_position < 0 and not self.settings.allow_short:
            return RiskDecision(
                approved=False,
                reason="shorts_disabled",
                target_position=current_position,
                order_qty=0.0,
                order_side=None,
                reduce_only=False,
            )

        if desired_position == current_position:
            return RiskDecision(
                approved=False,
                reason="no_position_change",
                target_position=current_position,
                order_qty=0.0,
                order_side=None,
                reduce_only=False,
            )

        current_qty = self._round_qty(float(state.get("position_qty", 0.0)))
        open_qty = self._calc_open_qty_from_usdt(price)

        if current_position == 0:
            if desired_position > 0:
                if open_qty <= 0:
                    return RiskDecision(
                        approved=False,
                        reason="qty_zero_or_invalid",
                        target_position=current_position,
                        order_qty=0.0,
                        order_side=None,
                        reduce_only=False,
                    )
                return RiskDecision(
                    approved=True,
                    reason="open_long",
                    target_position=1,
                    order_qty=open_qty,
                    order_side="Buy",
                    reduce_only=False,
                )

            if desired_position < 0:
                if open_qty <= 0:
                    return RiskDecision(
                        approved=False,
                        reason="qty_zero_or_invalid",
                        target_position=current_position,
                        order_qty=0.0,
                        order_side=None,
                        reduce_only=False,
                    )
                return RiskDecision(
                    approved=True,
                    reason="open_short",
                    target_position=-1,
                    order_qty=open_qty,
                    order_side="Sell",
                    reduce_only=False,
                )

        if current_position > 0 and desired_position == 0:
            if current_qty <= 0:
                return RiskDecision(
                    approved=False,
                    reason="missing_position_qty_for_close",
                    target_position=current_position,
                    order_qty=0.0,
                    order_side=None,
                    reduce_only=False,
                )
            return RiskDecision(
                approved=True,
                reason="close_long",
                target_position=0,
                order_qty=current_qty,
                order_side="Sell",
                reduce_only=True,
            )

        if current_position < 0 and desired_position == 0:
            if current_qty <= 0:
                return RiskDecision(
                    approved=False,
                    reason="missing_position_qty_for_close",
                    target_position=current_position,
                    order_qty=0.0,
                    order_side=None,
                    reduce_only=False,
                )
            return RiskDecision(
                approved=True,
                reason="close_short",
                target_position=0,
                order_qty=current_qty,
                order_side="Buy",
                reduce_only=True,
            )

        if self.settings.one_position_only and current_position != 0 and desired_position != 0:
            if current_qty <= 0:
                return RiskDecision(
                    approved=False,
                    reason="missing_position_qty_for_reverse",
                    target_position=current_position,
                    order_qty=0.0,
                    order_side=None,
                    reduce_only=False,
                )
            return RiskDecision(
                approved=True,
                reason="close_before_reverse",
                target_position=0,
                order_qty=current_qty,
                order_side="Sell" if current_position > 0 else "Buy",
                reduce_only=True,
            )

        return RiskDecision(
            approved=False,
            reason="unsupported_transition",
            target_position=current_position,
            order_qty=0.0,
            order_side=None,
            reduce_only=False,
        )