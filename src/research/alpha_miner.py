# -*- coding: utf-8 -*-
import sys
import argparse
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

import pandas as pd

from data.bybit_loader import download_and_save
from processing.data_processor import process
from research.rule_builder import build_rule_candidates
from backtest.engine import apply_position_logic, run_backtest, calculate_metrics

BASE_DIR = Path(__file__).resolve().parent.parent.parent
REPORTS_DIR = BASE_DIR / "reports"


def parse_args():
    parser = argparse.ArgumentParser(description="Run alpha miner")
    parser.add_argument(
        "--refresh-data",
        action="store_true",
        help="Refresh BTCUSDT 15m/30m candles from Bybit before processing",
    )
    return parser.parse_args()


def refresh_market_data():
    print("Refreshing market data from Bybit...")
    download_and_save(symbol="BTCUSDT", interval="15", total=15000)
    download_and_save(symbol="BTCUSDT", interval="30", total=10000)
    print("Market data refreshed.\n")


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

    return df


def split_df(df):
    split = int(len(df) * 0.7)
    return df.iloc[:split].copy(), df.iloc[split:].copy()


# ================= APPLY STRATEGIES =================


from research.strategies.registry import STRATEGY_REGISTRY

def apply_candidate(df, candidate):
    family = candidate["family"]

    if family not in STRATEGY_REGISTRY:
        raise ValueError(f"Unknown strategy: {family}")

    func = STRATEGY_REGISTRY[family]["apply"]

    df = func(df, candidate)

    df["position"] = apply_position_logic(
        df["entry_signal"],
        candidate["hold_bars"],
        candidate["direction"]
    )

    return df



# ================= FILTER =================

def passes_candidate_filters(train_m, test_m):
    reasons = []

    train_return = train_m["total_return_pct"]
    test_return = test_m["total_return_pct"]

    if train_return <= 0:
        reasons.append("negative_train")

    if test_return <= 0.5:
        reasons.append("low_test")

    if abs(test_return - train_return) > 3:
        reasons.append("train_test_gap")

    if test_m["trades"] < 15:
        reasons.append("few_trades")

    if test_m["sharpe_approx"] < 0.2:
        reasons.append("low_sharpe")

    return len(reasons) == 0, reasons


def calculate_candidate_score(train_m, test_m, is_valid):
    score = (
        test_m["total_return_pct"]
        + train_m["total_return_pct"] * 0.25
        + test_m["sharpe_approx"] * 3
        - abs(test_m["max_drawdown_pct"]) * 0.75
    )

    if not is_valid:
        score -= 100

    return round(score, 4)


def evaluate_candidate(train_df, test_df, candidate):
    train_bt = run_backtest(apply_candidate(train_df, candidate))
    test_bt = run_backtest(apply_candidate(test_df, candidate))

    train_m = calculate_metrics(train_bt)
    test_m = calculate_metrics(test_bt)

    is_valid, reasons = passes_candidate_filters(train_m, test_m)
    score = calculate_candidate_score(train_m, test_m, is_valid)

    return train_m, test_m, is_valid, reasons, score


# ================= MAIN =================

def run_alpha_miner(refresh_data=False):
    if refresh_data:
        refresh_market_data()

    df = prepare_pa_features(process())
    train_df, test_df = split_df(df)

    results = []

    for i, c in enumerate(build_rule_candidates(), 1):
        train_m, test_m, valid, reasons, score = evaluate_candidate(train_df, test_df, c)

        results.append({
            "id": i,
            "family": c["family"],
            "test_return": test_m["total_return_pct"],
            "score": score,
            "is_valid": valid,
            "reasons": "|".join(reasons),
        })

    df_res = pd.DataFrame(results).sort_values(
        by=["is_valid", "score", "test_return"],
        ascending=[False, False, False],
    )

    REPORTS_DIR.mkdir(exist_ok=True)

    df_res.to_csv(REPORTS_DIR / "alpha_miner_wf.csv", index=False)

    top = df_res[df_res["is_valid"]].head(10)
    top.to_csv(REPORTS_DIR / "top_alphas.csv", index=False)

    print("\n=== VALID TOP ===")
    print(top)
    print()

    return df_res


if __name__ == "__main__":
    args = parse_args()
    run_alpha_miner(refresh_data=args.refresh_data)