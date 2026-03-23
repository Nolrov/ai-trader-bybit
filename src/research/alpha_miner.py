# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from backtest.engine import apply_position_logic, calculate_metrics, run_backtest
from config.settings import REPORTS_DIR, load_settings
from data.market_data_manager import get_processed_market_data
from research.rule_builder import build_rule_candidates
from research.strategies.atr_breakout import compute_atr
from research.strategies.registry import STRATEGY_REGISTRY


QUALITY_GATE = {
    "min_test_return": 0.0,
    "min_test_trades": 20,
    "min_test_sharpe": 0.0,
    "max_test_drawdown_abs": 8.0,
    "max_train_test_gap": 5.0,
    "min_train_return": -2.0,
    "max_abs_return": 200.0,
}


def _compute_confirmed_swings(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    high = df["high_15m"]
    low = df["low_15m"]

    prev_bar_confirmed_swing_high = (
        (high.shift(1) > high.shift(2))
        & (high.shift(1) >= high)
    )
    prev_bar_confirmed_swing_low = (
        (low.shift(1) < low.shift(2))
        & (low.shift(1) <= low)
    )

    swing_high_price = high.shift(1).where(prev_bar_confirmed_swing_high)
    swing_low_price = low.shift(1).where(prev_bar_confirmed_swing_low)
    return swing_high_price, swing_low_price


def prepare_pa_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["range_15m"] = df["high_15m"] - df["low_15m"]
    df["body_15m"] = (df["close_15m"] - df["open_15m"]).abs()
    df["body_ratio_15m"] = df["body_15m"] / df["range_15m"].replace(0, pd.NA)
    df["upper_wick_15m"] = df["high_15m"] - df[["open_15m", "close_15m"]].max(axis=1)
    df["lower_wick_15m"] = df[["open_15m", "close_15m"]].min(axis=1) - df["low_15m"]
    df["upper_wick_ratio_15m"] = df["upper_wick_15m"] / df["range_15m"].replace(0, pd.NA)
    df["lower_wick_ratio_15m"] = df["lower_wick_15m"] / df["range_15m"].replace(0, pd.NA)
    df["close_location_15m"] = (df["close_15m"] - df["low_15m"]) / df["range_15m"].replace(0, pd.NA)
    df["range_pct_15m"] = df["range_15m"] / df["close_15m"].replace(0, pd.NA)
    df["range_median_20"] = df["range_15m"].rolling(20).median()
    df["range_pct_median_20"] = df["range_pct_15m"].rolling(20).median()
    df["range_width_pct_20"] = (
        (df["high_15m"].rolling(20).max() - df["low_15m"].rolling(20).min()) / df["close_15m"].replace(0, pd.NA)
    )

    df["bullish_15m"] = df["close_15m"] > df["open_15m"]
    df["bearish_15m"] = df["close_15m"] < df["open_15m"]
    df["inside_bar_15m"] = (df["high_15m"] <= df["high_15m"].shift(1)) & (df["low_15m"] >= df["low_15m"].shift(1))

    df["ret_15m"] = df["close_15m"].pct_change()
    df["ret_mean_20"] = df["ret_15m"].rolling(20).mean()
    df["ret_std_20"] = df["ret_15m"].rolling(20).std()
    df["zscore_ret_20"] = (df["ret_15m"] - df["ret_mean_20"]) / df["ret_std_20"].replace(0, pd.NA)

    df["ema_gap_15m"] = (df["close_15m"] - df["ema_fast_30m"]) / df["ema_fast_30m"].replace(0, pd.NA)
    df["ema_gap_fast_15m"] = (df["close_15m"] - df["ema_fast_15m"]) / df["ema_fast_15m"].replace(0, pd.NA)

    df["atr_14"] = compute_atr(df)
    df["atr_pct_14"] = df["atr_14"] / df["close_15m"].replace(0, pd.NA)

    df["ema_trend_strength_30m"] = (
        (df["ema_fast_30m"] - df["ema_slow_30m"]).abs() / df["ema_slow_30m"].replace(0, pd.NA)
    )
    df["ema_slope_fast_30m"] = df["ema_fast_30m"].pct_change(3)
    df["rsi_distance_15m"] = (df["rsi_15m"] - 50.0).abs()
    df["roc_4_15m"] = df["close_15m"].pct_change(4)
    df["roc_8_15m"] = df["close_15m"].pct_change(8)

    df["swing_high_price"], df["swing_low_price"] = _compute_confirmed_swings(df)
    df["recent_swing_high"] = df["swing_high_price"].ffill()
    df["recent_swing_low"] = df["swing_low_price"].ffill()
    df["structure_high_10"] = df["high_15m"].rolling(10).max().shift(1)
    df["structure_low_10"] = df["low_15m"].rolling(10).min().shift(1)
    df["structure_high_20"] = df["high_15m"].rolling(20).max().shift(1)
    df["structure_low_20"] = df["low_15m"].rolling(20).min().shift(1)

    df["volatility_median_50"] = df["volatility_15m"].rolling(50).median()
    df["regime_high_vol"] = (df["volatility_15m"] > df["volatility_median_50"]).astype(int)
    df["regime_trend"] = (df["ema_trend_strength_30m"] > 0.004).astype(int)
    df["regime_flat"] = (df["regime_trend"] == 0).astype(int)

    return df


def split_df(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    split = int(len(df) * 0.7)
    return df.iloc[:split].copy(), df.iloc[split:].copy()


def normalize_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    out = dict(candidate)
    out.setdefault("regime_tag", "all")
    out.setdefault("min_hold_bars", 1)
    out.setdefault("hold_bars", out.get("max_hold_bars", 6))
    out.setdefault("max_hold_bars", out.get("hold_bars", 6))
    out.setdefault("exit_style", "time_stop")
    encoded = json.dumps(out, sort_keys=True, ensure_ascii=False)
    out["candidate_key"] = hashlib.md5(encoded.encode("utf-8")).hexdigest()[:12]
    return out


def apply_candidate(df: pd.DataFrame, candidate: dict) -> pd.DataFrame:
    normalized_candidate = normalize_candidate(candidate)
    family = normalized_candidate["family"]
    func = STRATEGY_REGISTRY[family]["apply"]

    df = func(df.copy(), normalized_candidate)
    if "entry_signal" not in df.columns:
        raise ValueError(f"strategy_apply_missing_entry_signal:{family}")
    if "exit_signal" not in df.columns:
        df["exit_signal"] = 0

    df["position"] = apply_position_logic(
        df["entry_signal"],
        normalized_candidate.get("hold_bars"),
        normalized_candidate["direction"],
        exit_signal=df["exit_signal"],
        min_hold_bars=normalized_candidate.get("min_hold_bars", 1),
        max_hold_bars=normalized_candidate.get("max_hold_bars", normalized_candidate.get("hold_bars", 6)),
    )
    return df


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return default
    return float(value)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None or pd.isna(value):
        return default
    return int(value)


def classify_candidate(train_m: dict, test_m: dict) -> tuple[bool, bool, list[str]]:
    reasons: list[str] = []

    train_return = _safe_float(train_m.get("total_return_pct"))
    test_return = _safe_float(test_m.get("total_return_pct"))
    test_sharpe = _safe_float(test_m.get("sharpe_approx"))
    test_dd = abs(_safe_float(test_m.get("max_drawdown_pct")))
    test_trades = _safe_int(test_m.get("trades"))
    gap = abs(test_return - train_return)

    if train_return <= QUALITY_GATE["min_train_return"]:
        reasons.append("negative_train")
    if test_return <= QUALITY_GATE["min_test_return"]:
        reasons.append("low_test")
    if gap > QUALITY_GATE["max_train_test_gap"]:
        reasons.append("train_test_gap")
    if test_trades < QUALITY_GATE["min_test_trades"]:
        reasons.append("few_trades")
    if test_sharpe < QUALITY_GATE["min_test_sharpe"]:
        reasons.append("low_sharpe")
    if test_dd > QUALITY_GATE["max_test_drawdown_abs"]:
        reasons.append("deep_drawdown")
    if abs(train_return) > QUALITY_GATE["max_abs_return"] or abs(test_return) > QUALITY_GATE["max_abs_return"]:
        reasons.append("absurd_return")

    is_valid = not reasons
    is_promising = (
        test_return > -0.25
        and test_trades >= 15
        and test_dd <= 10
        and test_sharpe >= -0.10
        and abs(test_return) <= 200
    )
    return is_valid, is_promising, reasons


def calculate_candidate_score(train_m: dict, test_m: dict, is_valid: bool, is_promising: bool) -> float:
    train_return = _safe_float(train_m.get("total_return_pct"))
    test_return = _safe_float(test_m.get("total_return_pct"))
    test_sharpe = _safe_float(test_m.get("sharpe_approx"))
    test_dd = abs(_safe_float(test_m.get("max_drawdown_pct")))
    test_trades = max(1, _safe_int(test_m.get("trades"), 1))
    gap_penalty = abs(test_return - train_return) * 1.2
    trade_bonus = math.log1p(test_trades) * 2.5

    score = test_return * 1.8 + train_return * 0.25 + test_sharpe * 6.0 + trade_bonus - test_dd * 0.85 - gap_penalty
    if is_promising:
        score += 4
    if is_valid:
        score += 10
    return round(score, 4)


def _candidate_description(candidate: dict[str, Any]) -> str:
    family = candidate.get("family", "unknown")
    direction = candidate.get("direction", "n/a")
    parts = [str(family), str(direction), f"regime={candidate.get('regime_tag', 'all')}"]
    for key in sorted(candidate.keys()):
        if key in {"family", "direction", "regime_tag", "candidate_key", "score", "test_return", "test_sharpe", "test_drawdown", "test_trades", "is_valid", "is_promising", "description", "selection_source"}:
            continue
        parts.append(f"{key}={candidate[key]}")
    return " | ".join(parts)


def build_active_candidates(df_res: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    filtered = df_res[(df_res["is_valid"]) | (df_res["is_promising"])].copy()
    if filtered.empty:
        filtered = df_res.copy()

    filtered["bucket"] = filtered["family"].astype(str) + "|" + filtered["direction"].astype(str) + "|" + filtered["regime_tag"].astype(str)
    filtered = filtered.sort_values(
        by=["is_valid", "score", "test_sharpe", "test_trades", "test_return"],
        ascending=[False, False, False, False, False],
    )

    selected_rows = []
    selected_keys: set[str] = set()
    per_family: dict[str, int] = {}
    max_per_family = max(1, int(math.floor(limit * 0.4)))

    def can_take(row: pd.Series) -> bool:
        if row["candidate_key"] in selected_keys:
            return False
        if per_family.get(str(row["family"]), 0) >= max_per_family:
            return False
        return True

    def take_row(row: pd.Series, source: str) -> None:
        selected_rows.append(row.copy())
        selected_rows[-1]["selection_source"] = source
        selected_keys.add(str(row["candidate_key"]))
        fam = str(row["family"])
        per_family[fam] = per_family.get(fam, 0) + 1

    for _, group in filtered.groupby("bucket", sort=False):
        count = 0
        for _, row in group.iterrows():
            if can_take(row):
                take_row(row, "bucket_top")
                count += 1
            if count >= 2 or len(selected_rows) >= limit:
                break
        if len(selected_rows) >= limit:
            break

    if len(selected_rows) < limit:
        for _, row in filtered.iterrows():
            if can_take(row):
                take_row(row, "global_fill")
            if len(selected_rows) >= limit:
                break

    candidates: list[dict[str, Any]] = []
    for row in selected_rows[:limit]:
        candidate = json.loads(row["candidate_json"])
        candidate.update(
            {
                "candidate_key": row["candidate_key"],
                "score": float(row["score"]),
                "test_return": float(row["test_return"]),
                "test_sharpe": float(row["test_sharpe"]),
                "test_drawdown": float(row["test_drawdown"]),
                "test_trades": int(row["test_trades"]),
                "is_valid": bool(row["is_valid"]),
                "is_promising": bool(row["is_promising"]),
                "selection_source": row.get("selection_source", "unknown"),
                "family_share_cap": max_per_family,
            }
        )
        candidate["description"] = _candidate_description(candidate)
        candidates.append(candidate)
    return candidates




def assign_candidate_state(row: pd.Series, active_keys: set[str]) -> str:
    key = str(row.get("candidate_key", ""))
    if key in active_keys:
        return "active"
    if bool(row.get("is_valid", False)):
        return "validated"
    if bool(row.get("is_promising", False)):
        return "dormant"
    return "rejected_on_current_window"


def build_strategy_state_table(df_res: pd.DataFrame, active_candidates: list[dict[str, Any]]) -> pd.DataFrame:
    state_df = df_res.copy()
    active_keys = {str(item.get("candidate_key", "")) for item in active_candidates}
    state_df["bank_state"] = state_df.apply(lambda row: assign_candidate_state(row, active_keys), axis=1)
    state_df["in_active_bank"] = state_df["candidate_key"].astype(str).isin(active_keys)
    state_df["library_state"] = "library"
    state_df["status_reason"] = state_df.apply(
        lambda row: "selected_for_active_bank"
        if row["bank_state"] == "active"
        else "passed_validation"
        if row["bank_state"] == "validated"
        else "promising_but_not_selected"
        if row["bank_state"] == "dormant"
        else str(row.get("reasons", "")),
        axis=1,
    )
    return state_df


def build_strategy_bank_summary(state_df: pd.DataFrame) -> dict[str, Any]:
    state_counts = state_df["bank_state"].value_counts().to_dict()
    family_summary = (
        state_df.groupby(["family", "direction", "bank_state"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["family", "direction", "count"], ascending=[True, True, False])
    )
    return {
        "library_candidates": int(len(state_df)),
        "state_counts": {str(k): int(v) for k, v in state_counts.items()},
        "active_families": sorted(state_df.loc[state_df["bank_state"] == "active", "family"].astype(str).unique().tolist()),
        "active_directions": sorted(state_df.loc[state_df["bank_state"] == "active", "direction"].astype(str).unique().tolist()),
        "family_state_breakdown": family_summary.to_dict(orient="records"),
        "active_family_counts": state_df.loc[state_df["bank_state"] == "active", "family"].astype(str).value_counts().to_dict(),
    }


def run_alpha_miner() -> pd.DataFrame:
    settings = load_settings()
    df = prepare_pa_features(get_processed_market_data(settings))
    train_df, test_df = split_df(df)

    results = []
    for candidate in build_rule_candidates():
        normalized_candidate = normalize_candidate(candidate)
        try:
            train_bt = run_backtest(apply_candidate(train_df, normalized_candidate))
            test_bt = run_backtest(apply_candidate(test_df, normalized_candidate))

            train_m = calculate_metrics(train_bt)
            test_m = calculate_metrics(test_bt)
            is_valid, is_promising, reasons = classify_candidate(train_m, test_m)
            score = calculate_candidate_score(train_m, test_m, is_valid, is_promising)

            results.append(
                {
                    "candidate_key": normalized_candidate["candidate_key"],
                    "family": normalized_candidate["family"],
                    "direction": normalized_candidate["direction"],
                    "hold_bars": normalized_candidate["hold_bars"],
                    "regime_tag": normalized_candidate.get("regime_tag", "all"),
                    "candidate_json": json.dumps(normalized_candidate, sort_keys=True, ensure_ascii=False),
                    "train_return": train_m["total_return_pct"],
                    "test_return": test_m["total_return_pct"],
                    "train_sharpe": train_m["sharpe_approx"],
                    "test_sharpe": test_m["sharpe_approx"],
                    "test_drawdown": test_m["max_drawdown_pct"],
                    "test_trades": test_m["trades"],
                    "score": score,
                    "is_valid": is_valid,
                    "is_promising": is_promising,
                    "reasons": "|".join(reasons),
                }
            )
        except Exception as e:
            results.append(
                {
                    "candidate_key": normalized_candidate["candidate_key"],
                    "family": normalized_candidate["family"],
                    "direction": normalized_candidate["direction"],
                    "hold_bars": normalized_candidate["hold_bars"],
                    "regime_tag": normalized_candidate.get("regime_tag", "all"),
                    "candidate_json": json.dumps(normalized_candidate, sort_keys=True, ensure_ascii=False),
                    "train_return": None,
                    "test_return": None,
                    "train_sharpe": None,
                    "test_sharpe": None,
                    "test_drawdown": None,
                    "test_trades": None,
                    "score": -1e9,
                    "is_valid": False,
                    "is_promising": False,
                    "reasons": f"backtest_error:{e}",
                }
            )

    df_res = pd.DataFrame(results).sort_values(by=["is_valid", "is_promising", "score", "test_return"], ascending=[False, False, False, False])

    REPORTS_DIR.mkdir(exist_ok=True)
    df_res.to_csv(REPORTS_DIR / "alpha_miner_wf.csv", index=False)
    df_res[df_res["is_valid"]].to_csv(REPORTS_DIR / "validated_alphas.csv", index=False)
    df_res[df_res["is_promising"]].to_csv(REPORTS_DIR / "promising_alphas.csv", index=False)
    df_res[df_res["is_valid"]].head(20).to_csv(REPORTS_DIR / "top_alphas.csv", index=False)

    active_candidates = build_active_candidates(df_res, limit=20)
    with (REPORTS_DIR / "active_candidates.json").open("w", encoding="utf-8") as f:
        json.dump(active_candidates, f, ensure_ascii=False, indent=2)

    state_df = build_strategy_state_table(df_res, active_candidates)
    state_df.to_csv(REPORTS_DIR / "strategy_bank_states.csv", index=False)
    summary = build_strategy_bank_summary(state_df)
    with (REPORTS_DIR / "strategy_bank_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(df_res[["candidate_key", "family", "direction", "regime_tag", "score", "test_return", "test_sharpe", "test_trades", "is_valid"]].head(25))
    print()
    print("=== Strategy bank states ===")
    print(state_df["bank_state"].value_counts().to_string())
    return df_res


if __name__ == "__main__":
    run_alpha_miner()
