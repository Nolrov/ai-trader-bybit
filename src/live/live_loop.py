from __future__ import annotations

import argparse
import sys
import time
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
    print("====================\n")

    signal_ts = snap["timestamp"]

    if state.get("last_signal_timestamp") == signal_ts:
        print("SKIP: already processed this signal\n")
        return

    risk = RiskManager(settings.risk)

    decision = risk.evaluate(
        desired_position=snap["desired_position"],
        current_position=int(state.get("current_position", 0)),
        price=snap["price"],
        state=state,
    )

    print("RISK DECISION:", decision)

    if not decision.approved:
        state["last_signal_timestamp"] = signal_ts
        state_store.save(state)
        return

    executor = BybitExecutor(settings.execution)

    result = executor.place_order(
        symbol=settings.data.symbol,
        side=decision.order_side,
        qty=decision.order_qty,
        category=settings.data.category,
    )

    print("EXEC RESULT:", result)

    if result.ok:
        state["current_position"] = decision.target_position
        state["last_signal_timestamp"] = signal_ts
        state["last_order"] = {
            "symbol": settings.data.symbol,
            "side": decision.order_side,
            "qty": decision.order_qty,
            "target_position": decision.target_position,
            "price_reference": snap["price"],
            "signal_timestamp": signal_ts,
            "mode": settings.execution.mode,
        }
    else:
        print("ORDER FAILED")

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