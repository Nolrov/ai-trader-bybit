from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import AppSettings, load_settings
from data.bybit_loader import refresh_project_market_data
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
        help="Deprecated: market data refresh is always performed before signal calculation",
    )
    return parser.parse_args()


def refresh_market_data(settings: AppSettings) -> None:
    print("Refreshing market data from Bybit using project settings...")
    refresh_project_market_data(settings)
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
    df = process(settings=settings, enforce_freshness=True)
    df = prepare_pa_features(df)

    candidate = get_candidate(settings)
    df_signal = apply_candidate(df, candidate)

    if df_signal.empty:
        raise RuntimeError("signal_dataframe_is_empty_after_processing")

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
        state["bot_position_managed"] = True

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
        state["bot_position_managed"] = False
        state["bot_position_take_profit"] = None
        state["bot_position_stop_loss"] = None

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


def sync_state_with_exchange(settings: AppSettings, state_store: StateStore, state: dict) -> tuple[dict, object]:
    executor = BybitExecutor(settings.execution)
    sync_result = executor.sync_exchange_state(
        symbol=settings.data.symbol,
        category=settings.data.category,
    )

    state = state_store.reconcile_with_exchange(
        state=state,
        exchange_position=sync_result.position,
        active_orders=sync_result.active_orders,
        sync_ok=sync_result.ok,
        sync_message=sync_result.message,
    )

    print("=== EXCHANGE SYNC ===")
    print(f"sync_ok: {sync_result.ok}")
    print(f"sync_message: {sync_result.message}")
    print(f"exchange_position: {sync_result.position}")
    print(f"exchange_active_orders_count: {len(sync_result.active_orders)}")
    print(f"bot_position_managed: {state.get('bot_position_managed')}")
    print(f"external_position_detected: {state.get('external_position_detected')}")
    print(f"exchange_managed_orders_count: {state.get('exchange_managed_orders_count', 0)}")
    print(f"exchange_external_orders_count: {state.get('exchange_external_orders_count', 0)}")
    print("=====================\n")

    return state, sync_result


def should_block_due_to_external_state(state: dict) -> tuple[bool, str]:
    if state.get("external_position_detected"):
        return True, "external_manual_position_detected"

    if int(state.get("exchange_external_orders_count", 0)) > 0:
        return True, "external_manual_orders_detected"

    return False, ""


def should_block_new_open_from_exchange_state(state: dict, desired_position: int) -> tuple[bool, str]:
    current_position = int(state.get("current_position", 0))
    position_qty = float(state.get("position_qty", 0.0))
    bot_position_managed = bool(state.get("bot_position_managed"))

    if not bot_position_managed and (current_position != 0 or position_qty > 0):
        return True, "unmanaged_position_blocks_new_open"

    if current_position == 0 or position_qty <= 0:
        return False, ""

    if desired_position != 0:
        return True, "existing_managed_position_blocks_new_open"

    return False, ""


def should_block_for_active_orders(state: dict, desired_position: int) -> tuple[bool, str]:
    active_managed_orders_count = int(state.get("exchange_managed_orders_count", 0))
    if active_managed_orders_count > 0 and desired_position != 0:
        return True, "active_managed_orders_block_new_open"
    return False, ""


def get_protection_pct(settings: AppSettings, names: tuple[str, ...]) -> float | None:
    containers = [
        getattr(settings, "execution", None),
        getattr(settings, "risk", None),
        getattr(settings, "strategy", None),
    ]

    for container in containers:
        if container is None:
            continue
        for name in names:
            value = getattr(container, name, None)
            if value is None:
                continue
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value

    return None


def compute_protection_prices(settings: AppSettings, side: str, entry_price: float) -> tuple[float | None, float | None, str | None]:
    tp_pct = get_protection_pct(settings, ("take_profit_pct", "tp_pct"))
    sl_pct = get_protection_pct(settings, ("stop_loss_pct", "sl_pct"))

    if tp_pct is None or sl_pct is None:
        return None, None, "tp_sl_pct_not_configured_in_settings"

    side = str(side).strip().lower()
    if entry_price <= 0:
        return None, None, "invalid_entry_price_for_tp_sl"

    if side == "buy":
        take_profit = entry_price * (1.0 + tp_pct)
        stop_loss = entry_price * (1.0 - sl_pct)
    elif side == "sell":
        take_profit = entry_price * (1.0 - tp_pct)
        stop_loss = entry_price * (1.0 + sl_pct)
    else:
        return None, None, f"unsupported_side_for_tp_sl: {side}"

    if take_profit <= 0 or stop_loss <= 0:
        return None, None, "computed_nonpositive_tp_sl"

    return float(take_profit), float(stop_loss), None


def mark_block(state: dict, state_store: StateStore, settings: AppSettings, signal_ts: str, reason: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    print(f"SAFE BLOCK: {reason}")
    state["last_signal_timestamp"] = signal_ts
    state["last_decision_timestamp"] = now_iso
    state["last_order"] = {
        "mode": settings.execution.mode,
        "signal_timestamp": signal_ts,
        "action": "blocked",
        "reason": reason,
        "updated_at": now_iso,
    }
    state_store.save(state)


def run_cycle(settings, args):
    state_store = StateStore(settings.runtime.state_file)
    state = state_store.load()
    state = state_store.reset_daily_if_needed(state)

    refresh_market_data(settings)

    state, sync_result = sync_state_with_exchange(settings, state_store, state)
    state_store.save(state)

    if not sync_result.ok:
        print("SAFE BLOCK: exchange sync failed, trading aborted for this cycle\n")
        return

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
    print(f"exchange_last_sync_ok: {state.get('exchange_last_sync_ok')}")
    print(f"exchange_active_orders_count: {state.get('exchange_active_orders_count', 0)}")
    print(f"exchange_managed_orders_count: {state.get('exchange_managed_orders_count', 0)}")
    print(f"exchange_external_orders_count: {state.get('exchange_external_orders_count', 0)}")
    print(f"bot_position_managed: {state.get('bot_position_managed')}")
    print(f"external_position_detected: {state.get('external_position_detected')}")
    print("====================\n")

    signal_ts = snap["timestamp"]

    if state.get("last_signal_timestamp") == signal_ts:
        print("SKIP: already processed this signal\n")
        return

    block_external, block_external_reason = should_block_due_to_external_state(state)
    if block_external:
        mark_block(state, state_store, settings, signal_ts, block_external_reason)
        return

    block_open, block_reason = should_block_new_open_from_exchange_state(
        state=state,
        desired_position=snap["desired_position"],
    )
    if not block_open:
        block_open, block_reason = should_block_for_active_orders(
            state=state,
            desired_position=snap["desired_position"],
        )

    if block_open:
        mark_block(state, state_store, settings, signal_ts, block_reason)
        return

    previous_position = int(state.get("current_position", 0))
    bot_position_managed = bool(state.get("bot_position_managed"))

    if previous_position != 0 and not bot_position_managed:
        mark_block(state, state_store, settings, signal_ts, "unmanaged_position_close_forbidden")
        return

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

    if decision.reduce_only and not bot_position_managed:
        mark_block(state, state_store, settings, signal_ts, "reduce_only_close_for_unmanaged_position_forbidden")
        return

    executor = BybitExecutor(settings.execution)

    take_profit = None
    stop_loss = None
    order_link_id = None

    is_opening_order = (not decision.reduce_only) and int(decision.target_position) != 0

    if is_opening_order:
        open_side = str(decision.order_side)
        take_profit, stop_loss, protection_error = compute_protection_prices(
            settings=settings,
            side=open_side,
            entry_price=snap["price"],
        )

        if protection_error is not None:
            mark_block(state, state_store, settings, signal_ts, protection_error)
            return

        order_link_id = executor.build_order_link_id("OPEN")
        state["bot_position_pending_open"] = True
        state["bot_position_pending_close"] = False
        state["bot_last_open_order_link_id"] = order_link_id
        state["bot_position_take_profit"] = take_profit
        state["bot_position_stop_loss"] = stop_loss

    elif decision.reduce_only:
        if not bot_position_managed:
            mark_block(state, state_store, settings, signal_ts, "managed_close_required_but_position_not_managed")
            return
        order_link_id = executor.build_order_link_id("CLOSE")
        state["bot_position_pending_close"] = True
        state["bot_last_close_order_link_id"] = order_link_id

    result = executor.place_order(
        symbol=settings.data.symbol,
        side=decision.order_side,
        qty=decision.order_qty,
        price=snap["price"],
        category=settings.data.category,
        reduce_only=decision.reduce_only,
        take_profit=take_profit,
        stop_loss=stop_loss,
        order_link_id=order_link_id,
        require_tpsl_on_open=True,
    )

    print("EXEC RESULT:", result)

    if not result.ok:
        print("ORDER FAILED")
        state["bot_position_pending_open"] = False
        state["bot_position_pending_close"] = False
        state["last_signal_timestamp"] = signal_ts
        state["last_order"] = {
            "side": decision.order_side,
            "qty": decision.order_qty,
            "target_position": decision.target_position,
            "price_reference": snap["price"],
            "signal_timestamp": signal_ts,
            "mode": settings.execution.mode,
            "error": result.message,
            "order_link_id": order_link_id,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        state_store.save(state)
        return

    post_state, post_sync_result = sync_state_with_exchange(settings, state_store, state)
    if post_sync_result.ok:
        post_state["last_signal_timestamp"] = signal_ts
        post_state["last_decision_timestamp"] = datetime.now(timezone.utc).isoformat()
        post_state["last_order"] = {
            "side": decision.order_side,
            "qty": decision.order_qty,
            "target_position": decision.target_position,
            "price_reference": snap["price"],
            "signal_timestamp": signal_ts,
            "mode": settings.execution.mode,
            "exchange_confirmed": True,
            "order_link_id": order_link_id,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        if decision.reduce_only and post_state.get("current_position", 0) == 0:
            post_state["bot_position_managed"] = False
            post_state["bot_position_pending_close"] = False
            post_state["bot_position_take_profit"] = None
            post_state["bot_position_stop_loss"] = None

        state_store.save(post_state)
        return

    print("WARNING: post-order exchange sync failed, falling back to local state update")
    state["bot_position_pending_open"] = False
    state["bot_position_pending_close"] = False
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
    print(f"testnet={settings.execution.testnet}")
    print(f"candidate_id={settings.strategy.candidate_id}")
    print(f"symbol={settings.data.symbol}")
    print(f"interval_main={settings.data.interval_main}")
    print(f"interval_htf={settings.data.interval_htf}")
    print(f"bars_15m={settings.data.bars_15m}")
    print(f"bars_30m={settings.data.bars_30m}")
    print(f"state_file={settings.runtime.state_file}")
    print("data_refresh_policy=always_before_signal")
    print("ownership_policy=managed_only")
    print("open_policy=require_tp_sl")
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
