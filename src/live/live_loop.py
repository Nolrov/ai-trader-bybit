from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import load_settings
from data.bybit_loader import fetch_runtime_market_data
from execution.bybit_executor import BybitExecutor
from live.state_store import StateStore
from processing.data_processor import process_frames
from research.alpha_miner import apply_candidate, prepare_pa_features
from research.rule_builder import build_rule_candidates
from risk.risk_manager import RiskManager


def parse_args():
    parser = argparse.ArgumentParser(description="AI Trader live loop")
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def get_candidate(settings) -> dict:
    candidates = build_rule_candidates()
    cid = settings.strategy.candidate_id

    if cid < 1 or cid > len(candidates):
        raise ValueError(f"candidate_id must be between 1 and {len(candidates)}")

    return candidates[cid - 1]


def build_signal_snapshot(settings) -> dict:
    df_15, df_30 = fetch_runtime_market_data(settings=settings)

    df = process_frames(
        df_15=df_15,
        df_30=df_30,
        settings=settings,
        enforce_freshness=True,
    )

    df = prepare_pa_features(df)

    candidate = get_candidate(settings)
    df_signal = apply_candidate(df, candidate)

    if df_signal.empty:
        raise RuntimeError("signal_dataframe_is_empty_after_processing")

    last = df_signal.iloc[-1]
    recent = df_signal.tail(100)
    signals_count = int((recent["entry_signal"] != 0).sum()) if "entry_signal" in recent.columns else 0

    return {
        "timestamp": str(last["timestamp"]),
        "price": float(last["close_15m"]),
        "desired_position": int(last.get("position", 0)),
        "entry_signal": int(last.get("entry_signal", 0)),
        "candidate_id": settings.strategy.candidate_id,
        "signals_last_100_bars": signals_count,
    }


def log_warning(msg: str):
    print(f"WARNING {msg}")


def get_exchange_position_snapshot(state: dict) -> dict:
    snapshot = state.get("exchange_position_snapshot")
    if isinstance(snapshot, dict):
        return snapshot
    return {}


def compute_protection_prices(settings, side: str, entry_price: float):
    tp_pct = float(settings.risk.take_profit_pct)
    sl_pct = float(settings.risk.stop_loss_pct)

    if tp_pct <= 0 or sl_pct <= 0:
        raise RuntimeError("invalid_tp_sl_settings")

    if entry_price <= 0:
        raise RuntimeError("invalid_entry_price_for_tp_sl")

    normalized_side = str(side).strip().lower()

    if normalized_side == "buy":
        take_profit = entry_price * (1.0 + tp_pct)
        stop_loss = entry_price * (1.0 - sl_pct)
    elif normalized_side == "sell":
        take_profit = entry_price * (1.0 - tp_pct)
        stop_loss = entry_price * (1.0 + sl_pct)
    else:
        raise RuntimeError(f"unsupported_side_for_tp_sl: {side}")

    if take_profit <= 0 or stop_loss <= 0:
        raise RuntimeError("computed_nonpositive_tp_sl")

    return float(take_profit), float(stop_loss)


def reconcile_position(state: dict, snap: dict):
    snapshot = get_exchange_position_snapshot(state)

    exchange_pos = int(snapshot.get("current_position", 0) or 0)
    exchange_qty = abs(float(snapshot.get("position_qty", 0.0) or 0.0))
    desired = int(snap["desired_position"])

    if exchange_pos == 0 or exchange_qty <= 0:
        return None

    if exchange_pos == desired:
        return None

    log_warning(
        f"position_mismatch exchange_position={exchange_pos} desired={desired} "
        f"qty={exchange_qty} action=force_close signal_ts={snap['timestamp']}"
    )

    return {
        "action": "close",
        "side": "Sell" if exchange_pos > 0 else "Buy",
        "qty": exchange_qty,
    }


def run_cycle(settings):
    state_store = StateStore(settings.runtime.state_file)
    state = state_store.load()
    state = state_store.reset_daily_if_needed(state)

    executor = BybitExecutor(settings.execution)

    sync = executor.sync_exchange_state(
        symbol=settings.data.symbol,
        category=settings.data.category,
    )

    state = state_store.reconcile_with_exchange(
        state=state,
        exchange_position=sync.position,
        active_orders=sync.active_orders,
        sync_ok=sync.ok,
        sync_message=sync.message,
    )

    state_store.save(state)

    print("=== STARTUP / SYNC ===")
    print(f"sync_ok: {sync.ok}")
    print(f"sync_message: {sync.message}")
    print(f"exchange_position_snapshot: {state.get('exchange_position_snapshot')}")
    print(f"current_position_state: {state.get('current_position')}")
    print(f"position_qty_state: {state.get('position_qty')}")
    print("======================")

    if not sync.ok:
        log_warning("exchange_sync_failed")
        return

    snap = build_signal_snapshot(settings)

    print("=== SIGNAL ===")
    print(f"timestamp: {snap['timestamp']}")
    print(f"price: {snap['price']}")
    print(f"entry_signal: {snap['entry_signal']}")
    print(f"desired_position: {snap['desired_position']}")
    print(f"candidate_id: {snap['candidate_id']}")
    print(f"signals_last_100_bars: {snap['signals_last_100_bars']}")
    print("==============")

    action = reconcile_position(state, snap)

    if action:
        result = executor.place_order(
            symbol=settings.data.symbol,
            side=action["side"],
            qty=action["qty"],
            price=snap["price"],
            category=settings.data.category,
            reduce_only=True,
        )

        print("=== FORCE CLOSE RESULT ===")
        print(result)
        print("==========================")
        return

    current_position = int(state.get("current_position", 0))
    desired = int(snap["desired_position"])

    risk = RiskManager(settings.risk)

    decision = risk.evaluate(
        desired_position=desired,
        current_position=current_position,
        price=snap["price"],
        state=state,
    )

    print("=== RISK DECISION ===")
    print(decision)
    print("=====================")

    if not decision.approved:
        return

    place_order_kwargs = {
        "symbol": settings.data.symbol,
        "side": decision.order_side,
        "qty": decision.order_qty,
        "price": snap["price"],
        "category": settings.data.category,
        "reduce_only": decision.reduce_only,
    }

    is_open_order = (not decision.reduce_only) and int(decision.target_position) != 0

    if is_open_order:
        take_profit, stop_loss = compute_protection_prices(
            settings=settings,
            side=decision.order_side,
            entry_price=snap["price"],
        )
        place_order_kwargs["take_profit"] = take_profit
        place_order_kwargs["stop_loss"] = stop_loss
        place_order_kwargs["require_tpsl_on_open"] = True

    result = executor.place_order(**place_order_kwargs)

    print("=== EXEC RESULT ===")
    print(result)
    print("===================")


def main():
    args = parse_args()
    settings = load_settings()

    print("AI Trader started (online mode)")
    print(f"mode={settings.execution.mode}")
    print(f"testnet={settings.execution.testnet}")
    print(f"symbol={settings.data.symbol}")
    print(f"interval_main={settings.data.interval_main}")
    print(f"interval_htf={settings.data.interval_htf}")
    print(f"take_profit_pct={settings.risk.take_profit_pct}")
    print(f"stop_loss_pct={settings.risk.stop_loss_pct}")

    if args.once:
        run_cycle(settings)
        return

    while True:
        try:
            run_cycle(settings)
        except Exception as e:
            print(f"cycle_failed: {e}")

        time.sleep(settings.runtime.poll_seconds)


if __name__ == "__main__":
    main()
