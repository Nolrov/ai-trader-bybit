from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import AppSettings, LOGS_DIR, load_settings
from data.market_data_manager import get_processed_market_data
from execution.bybit_executor import BybitExecutor
from live.state_store import StateStore
from research.alpha_miner import apply_candidate, prepare_pa_features
from research.rule_builder import build_rule_candidates
from risk.risk_manager import RiskManager
from utils.runtime_logger import RuntimeLogger


def parse_args():
    parser = argparse.ArgumentParser(description="AI Trader live loop")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    return parser.parse_args()


def get_candidate(settings) -> dict:
    candidates = build_rule_candidates()

    candidate_id = settings.strategy.candidate_id
    if candidate_id < 1 or candidate_id > len(candidates):
        raise ValueError(
            f"candidate_id must be between 1 and {len(candidates)}, got {candidate_id}"
        )

    return candidates[candidate_id - 1]


def build_signal_snapshot(settings) -> dict:
    df = get_processed_market_data(settings)
    df = prepare_pa_features(df)

    candidate = get_candidate(settings)
    df_signal = apply_candidate(df, candidate)

    if df_signal.empty:
        raise RuntimeError("signal_dataframe_is_empty_after_processing")

    last_row = df_signal.iloc[-1]
    recent = df_signal.tail(100)

    signals_count = int((recent["entry_signal"] != 0).sum()) if "entry_signal" in recent.columns else 0

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


def sync_state_with_exchange(
    settings: AppSettings,
    state_store: StateStore,
    state: dict,
    logger: RuntimeLogger,
) -> tuple[dict, object]:
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

    logger.info(
        f"exchange_sync sync_ok={sync_result.ok} "
        f"message={sync_result.message} "
        f"managed_orders={state.get('exchange_managed_orders_count', 0)} "
        f"external_orders={state.get('exchange_external_orders_count', 0)}"
    )
    logger.event(
        "exchange_sync",
        {
            "sync_ok": sync_result.ok,
            "message": sync_result.message,
            "exchange_position": sync_result.position,
            "exchange_active_orders_count": len(sync_result.active_orders),
            "bot_position_managed": state.get("bot_position_managed"),
            "external_position_detected": state.get("external_position_detected"),
            "exchange_managed_orders_count": state.get("exchange_managed_orders_count", 0),
            "exchange_external_orders_count": state.get("exchange_external_orders_count", 0),
        },
    )

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


def compute_protection_prices(settings: AppSettings, side: str, entry_price: float) -> tuple[float | None, float | None, str | None]:
    tp_pct = float(settings.risk.take_profit_pct)
    sl_pct = float(settings.risk.stop_loss_pct)

    if tp_pct <= 0 or sl_pct <= 0:
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


def mark_block(
    state: dict,
    state_store: StateStore,
    settings: AppSettings,
    signal_ts: str,
    reason: str,
    logger: RuntimeLogger,
) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    logger.warning(f"safe_block reason={reason} signal_ts={signal_ts}")
    logger.event(
        "safe_block",
        {
            "reason": reason,
            "signal_timestamp": signal_ts,
            "mode": settings.execution.mode,
        },
    )
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


def run_cycle(settings, args, logger: RuntimeLogger):
    state_store = StateStore(settings.runtime.state_file)
    state = state_store.load()
    state = state_store.reset_daily_if_needed(state)

    state, sync_result = sync_state_with_exchange(settings, state_store, state, logger)
    state_store.save(state)

    if not sync_result.ok:
        logger.warning("exchange sync failed, trading aborted for this cycle")
        return

    snap = build_signal_snapshot(settings)
    signal_ts = snap["timestamp"]

    logger.info(
        f"signal symbol={settings.data.symbol} time={signal_ts} "
        f"price={snap['price']} entry_signal={snap['entry_signal']} "
        f"desired_position={snap['desired_position']} signals_last_100={snap['signals_last_100_bars']}"
    )
    logger.event(
        "signal_snapshot",
        {
            "symbol": settings.data.symbol,
            "time": signal_ts,
            "price": snap["price"],
            "entry_signal": snap["entry_signal"],
            "desired_position": snap["desired_position"],
            "signals_last_100_bars": snap["signals_last_100_bars"],
            "candidate_id": settings.strategy.candidate_id,
        },
    )

    exchange_snapshot = state.get("exchange_position_snapshot") or {}
    exchange_pos = int(exchange_snapshot.get("current_position", 0) or 0)
    exchange_qty = float(exchange_snapshot.get("position_qty", 0.0) or 0.0)

    if exchange_pos != 0 and exchange_qty > 0:
        desired = int(snap["desired_position"])

        if exchange_pos != desired:
            logger.warning(
                f"position_mismatch_detected exchange_position={exchange_pos} "
                f"desired_position={desired} qty={exchange_qty} action=force_close "
                f"signal_ts={signal_ts}"
            )

            executor = BybitExecutor(settings.execution)
            close_side = "Sell" if exchange_pos > 0 else "Buy"

            result = executor.place_order(
                symbol=settings.data.symbol,
                side=close_side,
                qty=abs(exchange_qty),
                price=snap["price"],
                category=settings.data.category,
                reduce_only=True,
            )

            logger.event(
                "forced_close",
                {
                    "reason": "position_mismatch_detected",
                    "exchange_position": exchange_pos,
                    "desired_position": desired,
                    "qty": abs(exchange_qty),
                    "result_ok": result.ok,
                    "result_message": result.message,
                    "signal_timestamp": signal_ts,
                },
            )

            state["last_signal_timestamp"] = signal_ts
            state["last_decision_timestamp"] = datetime.now(timezone.utc).isoformat()
            state["last_order"] = {
                "mode": settings.execution.mode,
                "signal_timestamp": signal_ts,
                "action": "forced_close",
                "reason": "position_mismatch_detected",
                "exchange_position": exchange_pos,
                "desired_position": desired,
                "qty": abs(exchange_qty),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            state_store.save(state)
            return

    if state.get("last_signal_timestamp") == signal_ts:
        logger.info(f"skip already_processed_signal signal_ts={signal_ts}")
        return

    block_external, block_external_reason = should_block_due_to_external_state(state)
    if block_external:
        mark_block(state, state_store, settings, signal_ts, block_external_reason, logger)
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
        mark_block(state, state_store, settings, signal_ts, block_reason, logger)
        return

    previous_position = int(state.get("current_position", 0))
    bot_position_managed = bool(state.get("bot_position_managed"))

    if previous_position != 0 and not bot_position_managed:
        mark_block(state, state_store, settings, signal_ts, "unmanaged_position_close_forbidden", logger)
        return

    risk = RiskManager(settings.risk)
    decision = risk.evaluate(
        desired_position=snap["desired_position"],
        current_position=previous_position,
        price=snap["price"],
        state=state,
    )

    logger.info(f"risk_decision approved={decision.approved} reason={decision.reason}")
    logger.event(
        "risk_decision",
        {
            "approved": decision.approved,
            "reason": decision.reason,
            "target_position": decision.target_position,
            "order_qty": decision.order_qty,
            "order_side": decision.order_side,
            "reduce_only": decision.reduce_only,
            "signal_timestamp": signal_ts,
        },
    )

    state["last_decision_timestamp"] = datetime.now(timezone.utc).isoformat()

    if not decision.approved:
        state["last_signal_timestamp"] = signal_ts
        state_store.save(state)
        return

    if decision.reduce_only and not bot_position_managed:
        mark_block(state, state_store, settings, signal_ts, "reduce_only_close_for_unmanaged_position_forbidden", logger)
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
            mark_block(state, state_store, settings, signal_ts, protection_error, logger)
            return

        order_link_id = executor.build_order_link_id("OPEN")
        state["bot_position_pending_open"] = True
        state["bot_position_pending_close"] = False
        state["bot_last_open_order_link_id"] = order_link_id
        state["bot_position_take_profit"] = take_profit
        state["bot_position_stop_loss"] = stop_loss

    elif decision.reduce_only:
        if not bot_position_managed:
            mark_block(state, state_store, settings, signal_ts, "managed_close_required_but_position_not_managed", logger)
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

    logger.info(
        f"order_result ok={result.ok} message={result.message} "
        f"side={decision.order_side} qty={decision.order_qty} reduce_only={decision.reduce_only}"
    )
    logger.event(
        "order_result",
        {
            "ok": result.ok,
            "message": result.message,
            "side": decision.order_side,
            "qty": decision.order_qty,
            "target_position": decision.target_position,
            "reduce_only": decision.reduce_only,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "signal_timestamp": signal_ts,
        },
    )

    if not result.ok:
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

    post_state, post_sync_result = sync_state_with_exchange(settings, state_store, state, logger)
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

    logger.warning("post-order exchange sync failed, falling back to local state update")
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
    logger = RuntimeLogger(LOGS_DIR)

    logger.info(
        f"startup mode={settings.execution.mode} testnet={settings.execution.testnet} "
        f"candidate_id={settings.strategy.candidate_id} symbol={settings.data.symbol} "
        f"interval_main={settings.data.interval_main} interval_htf={settings.data.interval_htf}"
    )

    if args.once:
        run_cycle(settings, args, logger)
        return

    while True:
        try:
            run_cycle(settings, args, logger)
        except KeyboardInterrupt:
            logger.info("stopped_by_user")
            return
        except Exception as exc:
            logger.warning(f"cycle_failed error={exc}")

        time.sleep(settings.runtime.poll_seconds)


if __name__ == "__main__":
    main()