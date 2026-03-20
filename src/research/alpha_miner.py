# -*- coding: utf-8 -*-
import sys
from pathlib import Path
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from processing.data_processor import process
from research.rule_builder import build_rule_candidates
from research.strategies.atr_breakout import compute_atr
from backtest.engine import apply_position_logic, run_backtest, calculate_metrics
from research.strategies.registry import STRATEGY_REGISTRY

BASE_DIR = Path(__file__).resolve().parent.parent.parent
REPORTS_DIR = BASE_DIR / "reports"


def prepare_pa_features(df):
    df = df.copy()

    df["range_15m"] = df["high_15m"] - df["low_15m"]
    df["body_15m"] = (df["close_15m"] - df["open_15m"]).abs()
    df["body_ratio_15m"] = df["body_15m"] / df["range_15m"].replace(0, pd.NA)

    df["bullish_15m"] = df["close_15m"] > df["open_15m"]
    df["bearish_15m"] = df["close_15m"] < df["open_15m"]

    df["ret_15m"] = df["close_15m"].pct_change()
    df["ret_mean_20"] = df["ret_15m"].rolling(20).mean()
    df["ret_std_20"] = df["ret_15m"].rolling(20).std()
    df["zscore_ret_20"] = (
        (df["ret_15m"] - df["ret_mean_20"]) / df["ret_std_20"].replace(0, pd.NA)
    )

    df["ema_gap_15m"] = (
        (df["close_15m"] - df["ema_fast_30m"])
        / df["ema_fast_30m"].replace(0, pd.NA)
    )

    df["atr_14"] = compute_atr(df)
    df["atr_pct_14"] = df["atr_14"] / df["close_15m"].replace(0, pd.NA)

    df["ema_trend_strength_30m"] = (
        (df["ema_fast_30m"] - df["ema_slow_30m"]).abs()
        / df["ema_slow_30m"].replace(0, pd.NA)
    )

    df["volatility_median_50"] = df["volatility_15m"].rolling(50).median()

    df["regime_high_vol"] = (df["volatility_15m"] > df["volatility_median_50"]).astype(int)
    df["regime_trend"] = (df["ema_trend_strength_30m"] > 0.004).astype(int)
    df["regime_flat"] = (df["regime_trend"] == 0).astype(int)

    return df


def split_df(df):
    split = int(len(df) * 0.7)
    return df.iloc[:split].copy(), df.iloc[split:].copy()


def apply_candidate(df, candidate):
    family = candidate["family"]
    func = STRATEGY_REGISTRY[family]["apply"]

    df = func(df, candidate)

    df["position"] = apply_position_logic(
        df["entry_signal"],
        candidate["hold_bars"],
        candidate["direction"]
    )

    return df


def classify_candidate(train_m, test_m):
    reasons = []

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

    is_valid = (
        train_return > -3
        and test_return > 0
        and gap <= 4.0
        and test_trades >= 20
        and test_sharpe >= 0.0
        and test_dd >= -8
    )

    is_promising = (
        test_return > 0
        and test_trades >= 20
        and test_dd >= -10
        and test_sharpe >= -0.1
    )

    return is_valid, is_promising, reasons


def calculate_candidate_score(train_m, test_m, is_valid, is_promising):
    train_return = train_m["total_return_pct"]
    test_return = test_m["total_return_pct"]
    test_sharpe = test_m["sharpe_approx"]
    test_dd = abs(test_m["max_drawdown_pct"])
    gap_penalty = abs(test_return - train_return) * 1.5

    score = (
        test_return * 2.0
        + train_return * 0.15
        + test_sharpe * 3.0
        - test_dd * 0.8
        - gap_penalty
    )

    if is_promising:
        score += 10

    if is_valid:
        score += 20

    return round(score, 4)


def run_alpha_miner():
    df = prepare_pa_features(process())
    train_df, test_df = split_df(df)

    results = []

    for i, c in enumerate(build_rule_candidates(), 1):
        train_bt = run_backtest(apply_candidate(train_df, c))
        test_bt = run_backtest(apply_candidate(test_df, c))

        train_m = calculate_metrics(train_bt)
        test_m = calculate_metrics(test_bt)

        is_valid, is_promising, reasons = classify_candidate(train_m, test_m)
        score = calculate_candidate_score(train_m, test_m, is_valid, is_promising)

        results.append({
            "id": i,
            "family": c["family"],
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
        })

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

    print("\n=== VALID TOP ===")
    print(valid_top)

    print("\n=== PROMISING TOP ===")
    print(promising_top[[
        "id", "family", "train_return", "test_return",
        "test_sharpe", "test_drawdown", "test_trades",
        "score", "reasons"
    ]].head(15))


if __name__ == "__main__":
    run_alpha_miner()