# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
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


def prepare_pa_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["range_15m"] = df["high_15m"] - df["low_15m"]
    df["body_15m"] = (df["close_15m"] - df["open_15m"]).abs()
    df["body_ratio_15m"] = df["body_15m"] / df["range_15m"].replace(0, pd.NA)
    df["upper_wick_15m"] = df["high_15m"] - df[["open_15m", "close_15m"]].max(axis=1)
    df["lower_wick_15m"] = df[["open_15m", "close_15m"]].min(axis=1) - df["low_15m"]

    df["bullish_15m"] = df["close_15m"] > df["open_15m"]
    df["bearish_15m"] = df["close_15m"] < df["open_15m"]

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
    encoded = json.dumps(out, sort_keys=True, ensure_ascii=False)
    out["candidate_key"] = hashlib.md5(encoded.encode("utf-8")).hexdigest()[:12]
    return out


def apply_candidate(df: pd.DataFrame, candidate: dict) -> pd.DataFrame:
    normalized_candidate = normalize_candidate(candidate)
    family = normalized_candidate["family"]
    func = STRATEGY_REGISTRY[family]["apply"]

    df = func(df.copy(), normalized_candidate)
    df["position"] = apply_position_logic(
        df["entry_signal"],
        normalized_candidate["hold_bars"],
        normalized_candidate["direction"],
    )
    return df


def classify_candidate(train_m: dict, test_m: dict) -> tuple[bool, bool, list[str]]:
    reasons: list[str] = []

    train_return = train_m["total_return_pct"]
    test_return = test_m["total_return_pct"]
    test_sharpe = test_m["sharpe_approx"]
    test_dd = test_m["max_drawdown_pct"]
    test_trades = test_m["trades"]
    gap = abs(test_return - train_return)

    if train_return <= -3:
        reasons.append("negative_train")
    if test_return <= 0:
        reasons.append("low_test")
    if gap > 6.0:
        reasons.append("train_test_gap")
    if test_trades < 20:
        reasons.append("few_trades")
    if test_sharpe < 0.0:
        reasons.append("low_sharpe")
    if test_dd < -8:
        reasons.append("deep_drawdown")
    if abs(train_return) > 200 or abs(test_return) > 200:
        reasons.append("absurd_return")

    is_valid = (
        train_return > -3
        and test_return > 0
        and gap <= 4.0
        and test_trades >= 20
        and test_sharpe >= 0.0
        and test_dd >= -8
        and abs(train_return) <= 200
        and abs(test_return) <= 200
    )

    is_promising = (
        test_return > 0
        and test_trades >= 20
        and test_dd >= -10
        and test_sharpe >= -0.1
        and abs(test_return) <= 200
    )

    return is_valid, is_promising, reasons


def calculate_candidate_score(train_m: dict, test_m: dict, is_valid: bool, is_promising: bool) -> float:
    train_return = train_m["total_return_pct"]
    test_return = test_m["total_return_pct"]
    test_sharpe = test_m["sharpe_approx"]
    test_dd = abs(test_m["max_drawdown_pct"])
    gap_penalty = abs(test_return - train_return) * 1.5

    score = test_return * 2.0 + train_return * 0.15 + test_sharpe * 3.0 - test_dd * 0.8 - gap_penalty
    if is_promising:
        score += 10
    if is_valid:
        score += 20
    return round(score, 4)


def build_active_candidates(df_res: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    filtered = df_res[(df_res["is_valid"]) | (df_res["is_promising"])].copy()
    filtered = filtered.sort_values(by=["score", "test_return"], ascending=[False, False]).head(limit)

    candidates: list[dict[str, Any]] = []
    for _, row in filtered.iterrows():
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
            }
        )
        candidates.append(candidate)
    return candidates


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

    df_res = pd.DataFrame(results).sort_values(
        by=["is_valid", "is_promising", "score", "test_return"],
        ascending=[False, False, False, False],
    )

    REPORTS_DIR.mkdir(exist_ok=True)
    df_res.to_csv(REPORTS_DIR / "alpha_miner_wf.csv", index=False)

    valid_top = df_res[df_res["is_valid"]].head(10)
    promising_top = df_res[df_res["is_promising"]].head(15)
    valid_top.to_csv(REPORTS_DIR / "top_alphas.csv", index=False)
    promising_top.to_csv(REPORTS_DIR / "promising_alphas.csv", index=False)

    active_candidates = build_active_candidates(df_res, limit=20)
    with (REPORTS_DIR / "active_candidates.json").open("w", encoding="utf-8") as f:
        json.dump(active_candidates, f, ensure_ascii=False, indent=2)

    print("\n=== VALID TOP ===")
    print(valid_top[["candidate_key", "family", "direction", "regime_tag", "test_return", "test_sharpe", "test_drawdown", "test_trades", "score"]])

    print("\n=== PROMISING TOP ===")
    print(promising_top[["candidate_key", "family", "direction", "regime_tag", "test_return", "test_sharpe", "test_drawdown", "test_trades", "score", "reasons"]].head(15))

    print("\n=== ACTIVE CANDIDATES SAVED ===")
    print(REPORTS_DIR / "active_candidates.json")

    return df_res


if __name__ == "__main__":
    run_alpha_miner()
