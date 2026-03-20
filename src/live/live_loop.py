from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import load_settings
from data.bybit_loader import download_and_save
from execution.bybit_executor import BybitExecutor
from live.state_store import StateStore
from processing.data_processor import process
from research.alpha_miner import apply_candidate, prepare_pa_features
from research.rule_builder import build_rule_candidates
from risk.risk_manager import RiskManager


def parse_args():
    parser = argparse.ArgumentParser(description="AI Trader live loop")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument(
        "--refresh-data",
        action="store_true",
        help="Refresh market data from Bybit using project settings before processing",
    )
    return parser.parse_args()


def refresh_market_data(settings) -> None:
    print("Refreshing market data from Bybit using project settings...")

    download_and_save(
        symbol=settings.data.symbol,
        interval=settings.data.interval_main,
        total=settings.data.bars_15m,
        category=settings.data.category,
    )

    download_and_save(
        symbol=settings.data.symbol,
        interval=settings.data.interval_htf,
        total=settings.data.bars_30m,
        category=settings.data.category,
    )

    print("Market data refreshed.")
    print()


def get_candidate(settings) -> dict:
    candidates = build_rule_candidates()

    candidate_id = settings.strategy.candidate_id
    if candidate_id < 1 or candidate_id > len(candidates):
        raise ValueError(
            f"candidate_id must be between 1 and {len(candidates)}, got {candidate_id}"
        )

    return candidates[candidate_id - 1]


def build_signal_snapshot(settings) -> dict:
    df = process()
    df = prepare_pa_features(df)

    candidate = get_candidate(settings)
    df_signal = apply_candidate(df, candidate)

    if df_signal.empty:
        raise RuntimeError("Signal dataframe is empty after processing/apply_candidate")

    last_row = df_signal.iloc[-1]
    recent = df_signal.tail(100)

    signals_count = int((recent["entry_signal"] != 0).sum())

    return {
        "candidate": candidate,
        "timestamp": str(last_row["timestamp"]),
        "price": float(last_row["close_15m"]),
        "entry_signal": int(last_row.get("entry_signal", 0)),
        "desired_position": int(last_row.get("position", 0)),
        "signals_last_100_bars": signals_count,
    }


def apply_fill_to_state(
    state: dict,
    previous_position: int,
    new_target_position: int,
    fill_price: float,
    fill_qty: float,
    signal_ts: str,
    execution_mode: str,
    order_side: str,
) -> dict:
    now_iso = datetime.now(timezone.utc).isoformat()

    if previous_position == 0 and new_target_position != 0:
        state["current_position"] = new_target_position
        state["position_qty"] = fill_qty
        state["entry_price"] = fill_price
        state["entry_time"] = signal_ts

    elif previous_position != 0 and new_target_position == 0:
        entry_price = state.get("entry_price")
        entry_qty = float(state.get("position_qty", 0.0))

        if entry_price is not None and entry_qty > 0:
            entry_price = float(entry_price)

            if previous_position > 0:
                pnl_pct = ((fill_price / entry_price) - 1.0) * 100.0
            else:
                pnl_pct = ((entry_price / fill_price) - 1.0) * 100.0

            state["daily_pnl_pct"] = float(state.get("daily_pnl_pct", 0.0)) + pnl_pct
            state["realized_pnl_pct_total"] = float(state.get("realized_pnl_pct_total", 0.0)) + pnl_pct

            if pnl_pct < 0:
                state["consecutive_losses"] = int(state.get("consecutive_losses", 0)) + 1
            else:
                state["consecutive_losses"] = 0

        state["current_position"] = 0
        state["position_qty"] = 0.0
        state["entry_price"] = None
        state["entry_time"] = None

    state["last_signal_timestamp"] = signal_ts
    state["last_decision_timestamp"] = now_iso
    state["last_order"] = {
        "side": order_side,
        "qty": fill_qty,
        "target_position": new_target_position,
        "price_reference": fill_price,
        "signal_timestamp": signal_ts,
        "mode": execution_mode,
        "updated_at": now_iso,
    }

    return state


def run_cycle(settings, args):
    state_store = StateStore(settings.runtime.state_file)
    state = state_store.load()
    state = state_store.reset_daily_if_needed(state)

    if args.refresh_data:
        refresh_market_data(settings)

    snap = build_signal_snapshot(settings)

    print("\n=== SIGNAL DEBUG ===")
    print(f"symbol: {settings.data.symbol}")
    print(f"time: {snap['timestamp']}")
    print(f"price: {snap['price']}")
    print(f"entry_signal: {snap['entry_signal']}")
    print(f"desired_position: {snap['desired_position']}")
    print(f"signals_last_100_bars: {snap['signals_last_100_bars']}")
    print(f"current_position: {state.get('current_position', 0)}")
    print(f"position_qty: {state.get('position_qty', 0.0)}")
    print(f"entry_price: {state.get('entry_price')}")
    print(f"daily_pnl_pct: {state.get('daily_pnl_pct', 0.0)}")
    print(f"consecutive_losses: {state.get('consecutive_losses', 0)}")
    print("====================\n")

    signal_ts = snap["timestamp"]

    if state.get("last_signal_timestamp") == signal_ts:
        print("SKIP: already processed this signal\n")
        return

    previous_position = int(state.get("current_position", 0))

    risk = RiskManager(settings.risk)
    decision = risk.evaluate(
        desired_position=snap["desired_position"],
        current_position=previous_position,
        price=snap["price"],
        state=state,
    )

    print("RISK DECISION:", decision)

    state["last_decision_timestamp"] = datetime.now(timezone.utc).isoformat()

    if not decision.approved:
        state["last_signal_timestamp"] = signal_ts
        state_store.save(state)
        return

    executor = BybitExecutor(settings.execution)
    result = executor.place_order(
        symbol=settings.data.symbol,
        side=decision.order_side,
        qty=decision.order_qty,
        price=snap["price"],
        category=settings.data.category,
        reduce_only=decision.reduce_only,
    )

    print("EXEC RESULT:", result)

    if not result.ok:
        print("ORDER FAILED")
        state["last_signal_timestamp"] = signal_ts
        state["last_order"] = {
            "side": decision.order_side,
            "qty": decision.order_qty,
            "target_position": decision.target_position,
            "price_reference": snap["price"],
            "signal_timestamp": signal_ts,
            "mode": settings.execution.mode,
            "error": result.message,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        state_store.save(state)
        return

    state = apply_fill_to_state(
        state=state,
        previous_position=previous_position,
        new_target_position=decision.target_position,
        fill_price=snap["price"],
        fill_qty=decision.order_qty,
        signal_ts=signal_ts,
        execution_mode=settings.execution.mode,
        order_side=decision.order_side,
    )

    state_store.save(state)


def main():
    args = parse_args()
    settings = load_settings()

    print("AI Trader live loop started")
    print(f"mode={settings.execution.mode}")
    print(f"candidate_id={settings.strategy.candidate_id}")
    print(f"symbol={settings.data.symbol}")
    print(f"interval_main={settings.data.interval_main}")
    print(f"interval_htf={settings.data.interval_htf}")
    print(f"bars_15m={settings.data.bars_15m}")
    print(f"bars_30m={settings.data.bars_30m}")
    print(f"state_file={settings.runtime.state_file}")
    print()

    if args.once:
        run_cycle(settings, args)
        return

    while True:
        try:
            run_cycle(settings, args)
        except KeyboardInterrupt:
            print("Stopped by user")
            return
        except Exception as exc:
            print(f"cycle_failed: {exc}")

        time.sleep(settings.runtime.poll_seconds)


if __name__ == "__main__":
    main()