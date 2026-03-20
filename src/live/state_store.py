from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


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