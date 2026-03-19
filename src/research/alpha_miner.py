# -*- coding: utf-8 -*-

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

import pandas as pd

from processing.data_processor import process
from research.rule_builder import build_rule_candidates
from backtest.engine import apply_position_logic, run_backtest, calculate_metrics


BASE_DIR = Path(__file__).resolve().parent.parent.parent
REPORTS_DIR = BASE_DIR / "reports"


def prepare_pa_features(df):
    df = df.copy()

    df["range_15m"] = df["high_15m"] - df["low_15m"]
    df["body_15m"] = (df["close_15m"] - df["open_15m"]).abs()
    df["body_ratio_15m"] = df["body_15m"] / df["range_15m"].replace(0, pd.NA)

    df["bullish_15m"] = df["close_15m"] > df["open_15m"]
    df["bearish_15m"] = df["close_15m"] < df["open_15m"]

    return df


def apply_candidate(df, candidate):
    df = df.copy()

    lookback = candidate["breakout_lookback"]
    body_ratio_threshold = candidate["body_ratio_threshold"]
    direction = candidate["direction"]

    prev_high = df["high_15m"].rolling(lookback).max().shift(1)
    prev_low = df["low_15m"].rolling(lookback).min().shift(1)
    vol_threshold = df["volatility_15m"].rolling(50).median()

    if direction == "long":
        entry = (
            df["bullish_15m"]
            & (df["body_ratio_15m"] >= body_ratio_threshold)
            & (df["close_15m"] > prev_high)
        )
        if candidate["use_trend_filter"]:
            entry = entry & (df["ema_fast_30m"] > df["ema_slow_30m"])
    else:
        entry = (
            df["bearish_15m"]
            & (df["body_ratio_15m"] >= body_ratio_threshold)
            & (df["close_15m"] < prev_low)
        )
        if candidate["use_trend_filter"]:
            entry = entry & (df["ema_fast_30m"] < df["ema_slow_30m"])

    if candidate["use_vol_filter"]:
        entry = entry & (df["volatility_15m"] > vol_threshold)

    df["entry_signal"] = entry.fillna(False).astype(int)

    df["position"] = apply_position_logic(
        df["entry_signal"],
        candidate["hold_bars"],
        candidate["direction"],
    )

    return df


def split_df(df):
    split = int(len(df) * 0.7)
    return df.iloc[:split].copy(), df.iloc[split:].copy()


def run_alpha_miner():
    df = process()
    df = prepare_pa_features(df)

    train_df, test_df = split_df(df)

    candidates = build_rule_candidates()
    results = []

    for i, candidate in enumerate(candidates, 1):
        train_df_c = apply_candidate(train_df, candidate)
        test_df_c = apply_candidate(test_df, candidate)

        train_bt = run_backtest(train_df_c)
        test_bt = run_backtest(test_df_c)

        train_m = calculate_metrics(train_bt)
        test_m = calculate_metrics(test_bt)

        stable = (
            test_m["total_return_pct"] > -5
            and test_m["trades"] > 10
            and abs(test_m["total_return_pct"] - train_m["total_return_pct"]) < 10
        )

        description = (
            f"{candidate['direction'].upper()} | "
            f"breakout {candidate['breakout_lookback']} | "
            f"body >= {candidate['body_ratio_threshold']} | "
            f"hold {candidate['hold_bars']} | "
            f"trend={candidate['use_trend_filter']} | "
            f"vol={candidate['use_vol_filter']}"
        )

        results.append(
            {
                "id": i,
                "description": description,
                **candidate,
                "train_return": train_m["total_return_pct"],
                "test_return": test_m["total_return_pct"],
                "train_sharpe": train_m["sharpe_approx"],
                "test_sharpe": test_m["sharpe_approx"],
                "train_trades": train_m["trades"],
                "test_trades": test_m["trades"],
                "stable": stable,
            }
        )

    df_res = pd.DataFrame(results)
    df_res = df_res.sort_values(by="test_return", ascending=False)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "alpha_miner_wf.csv"
    df_res.to_csv(path, index=False)

    print(f"Saved: {path}")
    print()
    print("=== TOP STRATEGIES ===")
    print()

    for _, row in df_res.head(10).iterrows():
        print(f"[ID {row['id']}] {row['description']}")
        print(
            f"  train: {row['train_return']:.2f}% | "
            f"test: {row['test_return']:.2f}%"
        )
        print(
            f"  trades: train={row['train_trades']} "
            f"test={row['test_trades']}"
        )
        print(f"  stable: {row['stable']}")
        print()

    return df_res


if __name__ == "__main__":
    run_alpha_miner()