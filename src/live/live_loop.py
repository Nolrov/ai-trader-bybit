from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict
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


def setup_logger(name: str, path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


def parse_args():
    parser = argparse.ArgumentParser(description="AI Trader live loop for paper/testnet")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument("--refresh-data", action="store_true", help="Refresh CSV candles from Bybit before decision")
    return parser.parse_args()


def refresh_market_data(settings) -> None:
    download_and_save(
        symbol=settings.strategy.symbol,
        interval=settings.strategy.interval_main,
        total=settings.strategy.data_total_bars,
        category=settings.strategy.category,
    )
    download_and_save(
        symbol=settings.strategy.symbol,
        interval=settings.strategy.interval_htf,
        total=settings.strategy.data_total_bars,
        category=settings.strategy.category,
    )


def get_candidate(settings):
    candidates = build_rule_candidates()
    candidate_id = settings.strategy.candidate_id

    if candidate_id < 1 or candidate_id > len(candidates):
        raise ValueError(f"candidate_id out of range: {candidate_id}, total candidates: {len(candidates)}")

    return candidates[candidate_id - 1]


def build_signal_snapshot(settings):
    df = process()
    df = prepare_pa_features(df)
    candidate = get_candidate(settings)
    df_signal = apply_candidate(df, candidate)

    last_row = df_signal.iloc[-1].copy()

    return {
        "candidate": candidate,
        "timestamp": str(last_row["timestamp"]),
        "close_15m": float(last_row["close_15m"]),
        "entry_signal": int(last_row.get("entry_signal", 0)),
        "desired_position": int(last_row.get("position", 0)),
    }


def maybe_reset_daily_state(state_store: StateStore, state: dict) -> dict:
    return state_store.reset_daily_if_needed(state)


def same_signal_already_processed(state: dict, signal_ts: str) -> bool:
    return state.get("last_signal_timestamp") == signal_ts


def update_state_after_fill(
    state: dict,
    current_position: int,
    target_position: int,
    fill_price: float,
    qty: float,
    signal_ts: str,
):
    now_iso = datetime.now(timezone.utc).isoformat()

    if current_position == 0 and target_position != 0:
        state["current_position"] = target_position
        state["position_qty"] = qty
        state["entry_price"] = fill_price
        state["entry_time"] = signal_ts

    elif current_position != 0 and target_position == 0:
        entry_price = state.get("entry_price")
        if entry_price is not None:
            entry_price = float(entry_price)

            if current_position > 0:
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
    return state


def run_cycle(settings, args, decision_logger, orders_logger):
    state_store = StateStore(settings.runtime.state_file)
    state = state_store.load()
    state = maybe_reset_daily_state(state_store, state)

    if args.refresh_data:
        refresh_market_data(settings)

    snapshot = build_signal_snapshot(settings)
    signal_ts = snapshot["timestamp"]
    desired_position = snapshot["desired_position"]
    price = snapshot["close_15m"]
    candidate = snapshot["candidate"]

    if same_signal_already_processed(state, signal_ts):
        decision_logger.info(
            json.dumps(
                {
                    "event": "skip",
                    "reason": "same_signal_already_processed",
                    "signal_timestamp": signal_ts,
                    "desired_position": desired_position,
                    "state_current_position": state.get("current_position", 0),
                },
                ensure_ascii=False,
            )
        )
        return

    risk_manager = RiskManager(settings.risk)
    risk_decision = risk_manager.evaluate(
        desired_position=desired_position,
        current_position=int(state.get("current_position", 0)),
        price=price,
        state=state,
    )

    decision_payload = {
        "event": "decision",
        "signal_timestamp": signal_ts,
        "price": price,
        "candidate": candidate,
        "desired_position": desired_position,
        "current_position": int(state.get("current_position", 0)),
        "risk_decision": asdict(risk_decision),
        "state": state,
    }
    decision_logger.info(json.dumps(decision_payload, ensure_ascii=False, default=str))

    if not risk_decision.approved:
        state["last_signal_timestamp"] = signal_ts
        state["last_decision_timestamp"] = datetime.now(timezone.utc).isoformat()
        state_store.save(state)
        return

    executor = BybitExecutor(settings.execution)

    reduce_only = (
        (int(state.get("current_position", 0)) > 0 and risk_decision.target_position == 0)
        or (int(state.get("current_position", 0)) < 0 and risk_decision.target_position == 0)
    )

    result = executor.place_order(
        symbol=settings.strategy.symbol,
        side=risk_decision.order_side,
        qty=risk_decision.order_qty,
        category=settings.strategy.category,
        order_type=settings.execution.order_type,
        reduce_only=reduce_only,
    )

    orders_logger.info(json.dumps(result.raw if result.raw is not None else {"message": result.message}, ensure_ascii=False, default=str))

    if not result.ok:
        state["last_signal_timestamp"] = signal_ts
        state["last_decision_timestamp"] = datetime.now(timezone.utc).isoformat()
        state_store.save(state)
        return

    state = update_state_after_fill(
        state=state,
        current_position=int(state.get("current_position", 0)),
        target_position=risk_decision.target_position,
        fill_price=price,
        qty=risk_decision.order_qty,
        signal_ts=signal_ts,
    )
    state["last_order"] = {
        "side": risk_decision.order_side,
        "qty": risk_decision.order_qty,
        "target_position": risk_decision.target_position,
        "price_reference": price,
        "mode": settings.execution.mode,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    state_store.save(state)


def main():
    args = parse_args()
    settings = load_settings()

    decision_logger = setup_logger("live_decisions", settings.runtime.decisions_log_file)
    orders_logger = setup_logger("live_orders", settings.runtime.orders_log_file)

    print("AI Trader live loop started")
    print(f"mode={settings.execution.mode}")
    print(f"candidate_id={settings.strategy.candidate_id}")
    print(f"symbol={settings.strategy.symbol}")
    print(f"state_file={settings.runtime.state_file}")

    if args.once:
        run_cycle(settings, args, decision_logger, orders_logger)
        return

    while True:
        try:
            run_cycle(settings, args, decision_logger, orders_logger)
        except KeyboardInterrupt:
            print("Stopped by user")
            return
        except Exception as exc:
            decision_logger.exception(f"cycle_failed: {exc}")

        time.sleep(settings.runtime.poll_seconds)


if __name__ == "__main__":
    main()