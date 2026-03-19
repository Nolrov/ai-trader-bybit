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
    download_and_save(symbol="BTCUSDT", interval="15", total=2000, category="linear")
    download_and_save(symbol="BTCUSDT", interval="30", total=2000, category="linear")
    print("Market data refreshed.")
    print()


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


def build_description(candidate):
    family = candidate["family"]

    if family == "breakout":
        return (
            f"{candidate['family']} | "
            f"{candidate['direction']} | "
            f"breakout={candidate['breakout_lookback']} | "
            f"body>={candidate['body_ratio_threshold']} | "
            f"hold={candidate['hold_bars']} | "
            f"trend={candidate['use_trend_filter']} | "
            f"vol={candidate['use_vol_filter']}"
        )

    if family == "mean_reversion":
        return (
            f"{candidate['family']} | "
            f"{candidate['direction']} | "
            f"zscore>={candidate['zscore_threshold']} | "
            f"hold={candidate['hold_bars']} | "
            f"trend={candidate['use_trend_filter']}"
        )

    if family == "trend_pullback":
        return (
            f"{candidate['family']} | "
            f"{candidate['direction']} | "
            f"pullback>={candidate['pullback_threshold']} | "
            f"hold={candidate['hold_bars']} | "
            f"trend={candidate['use_trend_filter']} | "
            f"vol={candidate['use_vol_filter']}"
        )

    return str(candidate)


def apply_breakout_candidate(df, candidate):
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


def apply_mean_reversion_candidate(df, candidate):
    df = df.copy()

    z = df["zscore_ret_20"]
    direction = candidate["direction"]

    if direction == "long":
        entry = z <= -candidate["zscore_threshold"]
        if candidate["use_trend_filter"]:
            entry = entry & (df["ema_fast_30m"] > df["ema_slow_30m"])
    else:
        entry = z >= candidate["zscore_threshold"]
        if candidate["use_trend_filter"]:
            entry = entry & (df["ema_fast_30m"] < df["ema_slow_30m"])

    df["entry_signal"] = entry.fillna(False).astype(int)
    df["position"] = apply_position_logic(
        df["entry_signal"],
        candidate["hold_bars"],
        candidate["direction"],
    )
    return df


def apply_trend_pullback_candidate(df, candidate):
    df = df.copy()

    pullback = candidate["pullback_threshold"]
    direction = candidate["direction"]
    vol_threshold = df["volatility_15m"].rolling(50).median()

    if direction == "long":
        trend_ok = df["ema_fast_30m"] > df["ema_slow_30m"]
        pullback_ok = df["ema_gap_15m"] <= -pullback
        reentry_ok = df["bullish_15m"]
        entry = trend_ok & pullback_ok & reentry_ok
    else:
        trend_ok = df["ema_fast_30m"] < df["ema_slow_30m"]
        pullback_ok = df["ema_gap_15m"] >= pullback
        reentry_ok = df["bearish_15m"]
        entry = trend_ok & pullback_ok & reentry_ok

    if candidate["use_vol_filter"]:
        entry = entry & (df["volatility_15m"] > vol_threshold)

    df["entry_signal"] = entry.fillna(False).astype(int)
    df["position"] = apply_position_logic(
        df["entry_signal"],
        candidate["hold_bars"],
        candidate["direction"],
    )
    return df


def apply_candidate(df, candidate):
    family = candidate["family"]

    if family == "breakout":
        return apply_breakout_candidate(df, candidate)

    if family == "mean_reversion":
        return apply_mean_reversion_candidate(df, candidate)

    if family == "trend_pullback":
        return apply_trend_pullback_candidate(df, candidate)

    raise ValueError(f"Unknown candidate family: {family}")


def split_df(df):
    split = int(len(df) * 0.7)
    return df.iloc[:split].copy(), df.iloc[split:].copy()


def passes_candidate_filters(train_m, test_m):
    reasons = []

    train_trades = train_m.get("trades", 0)
    test_trades = test_m.get("trades", 0)

    train_return = train_m.get("total_return_pct", 0.0)
    test_return = test_m.get("total_return_pct", 0.0)

    train_dd = train_m.get("max_drawdown_pct", 0.0)
    test_dd = test_m.get("max_drawdown_pct", 0.0)

    test_sharpe = test_m.get("sharpe_approx", 0.0)

    if train_trades < 20:
        reasons.append("too_few_train_trades")

    if test_trades < 10:
        reasons.append("too_few_test_trades")

    if test_return <= 0:
        reasons.append("non_positive_test_return")

    if test_sharpe <= 0:
        reasons.append("non_positive_test_sharpe")

    if test_dd < -10:
        reasons.append("test_drawdown_too_large")

    if abs(test_return - train_return) > 10:
        reasons.append("train_test_gap_too_large")

    if abs(test_dd - train_dd) > 10:
        reasons.append("train_test_dd_gap_too_large")

    return len(reasons) == 0, reasons


def calculate_candidate_score(train_m, test_m, is_valid):
    score = 0.0

    score += test_m["total_return_pct"] * 1.0
    score += train_m["total_return_pct"] * 0.25
    score += test_m["sharpe_approx"] * 3.0
    score -= abs(test_m["total_return_pct"] - train_m["total_return_pct"]) * 0.5
    score -= abs(test_m["max_drawdown_pct"]) * 0.75

    if test_m["trades"] < 10:
        score -= 10.0

    if not is_valid:
        score -= 100.0

    return round(score, 4)


def evaluate_candidate(train_df, test_df, candidate):
    train_df_c = apply_candidate(train_df, candidate)
    test_df_c = apply_candidate(test_df, candidate)

    train_bt = run_backtest(train_df_c)
    test_bt = run_backtest(test_df_c)

    train_m = calculate_metrics(train_bt)
    test_m = calculate_metrics(test_bt)

    is_valid, rejection_reasons = passes_candidate_filters(train_m, test_m)
    score = calculate_candidate_score(train_m, test_m, is_valid)

    return {
        "candidate": candidate,
        "description": build_description(candidate),
        "train_metrics": train_m,
        "test_metrics": test_m,
        "is_valid": is_valid,
        "rejection_reasons": rejection_reasons,
        "score": score,
    }


def run_alpha_miner(refresh_data=False):
    if refresh_data:
        refresh_market_data()

    df = process()
    df = prepare_pa_features(df)

    train_df, test_df = split_df(df)
    candidates = build_rule_candidates()

    results = []

    for i, candidate in enumerate(candidates, 1):
        evaluation = evaluate_candidate(train_df, test_df, candidate)
        train_m = evaluation["train_metrics"]
        test_m = evaluation["test_metrics"]

        results.append(
            {
                "id": i,
                "family": candidate["family"],
                "description": evaluation["description"],
                **candidate,
                "train_return": train_m["total_return_pct"],
                "test_return": test_m["total_return_pct"],
                "train_sharpe": train_m["sharpe_approx"],
                "test_sharpe": test_m["sharpe_approx"],
                "train_dd": train_m["max_drawdown_pct"],
                "test_dd": test_m["max_drawdown_pct"],
                "train_trades": train_m["trades"],
                "test_trades": test_m["trades"],
                "is_valid": evaluation["is_valid"],
                "rejection_reasons": "|".join(evaluation["rejection_reasons"]),
                "score": evaluation["score"],
            }
        )

    df_res = pd.DataFrame(results)
    df_res = df_res.sort_values(
        by=["is_valid", "score", "test_return"],
        ascending=[False, False, False],
    )

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "alpha_miner_wf.csv"
    df_res.to_csv(path, index=False)

    print(f"Saved: {path}")
    print()
    print("=== TOP STRATEGIES ===")
    print()

    for _, row in df_res.head(15).iterrows():
        print(f"[ID {row['id']}] {row['description']}")
        print(f" family: {row['family']}")
        print(
            f" score: {row['score']:.2f} | "
            f"train: {row['train_return']:.2f}% | "
            f"test: {row['test_return']:.2f}%"
        )
        print(
            f" sharpe: train={row['train_sharpe']:.2f} "
            f"test={row['test_sharpe']:.2f}"
        )
        print(
            f" dd: train={row['train_dd']:.2f}% "
            f"test={row['test_dd']:.2f}%"
        )
        print(
            f" trades: train={row['train_trades']} "
            f"test={row['test_trades']}"
        )
        print(f" valid: {row['is_valid']}")
        if row["rejection_reasons"]:
            print(f" rejection_reasons: {row['rejection_reasons']}")
        print()

    print("=== FAMILY SUMMARY ===")
    print()
    family_summary = (
        df_res.groupby("family")
        .agg(
            candidates=("id", "count"),
            avg_test_return=("test_return", "mean"),
            avg_score=("score", "mean"),
            valid_count=("is_valid", "sum"),
        )
        .sort_values(by="avg_score", ascending=False)
    )
    print(family_summary)
    print()

    return df_res


if __name__ == "__main__":
    args = parse_args()
    run_alpha_miner(refresh_data=args.refresh_data)