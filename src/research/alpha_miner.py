import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

import pandas as pd

from processing.data_processor import process
from research.rule_builder import build_rule_candidates
from research.validators import calculate_backtest_metrics, is_candidate_valid


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
        entry_condition = (
            df["bullish_15m"] &
            (df["body_ratio_15m"] >= body_ratio_threshold) &
            (df["close_15m"] > prev_high)
        )

        if candidate["use_trend_filter"]:
            entry_condition = entry_condition & (df["ema_fast_30m"] > df["ema_slow_30m"])

    else:
        entry_condition = (
            df["bearish_15m"] &
            (df["body_ratio_15m"] >= body_ratio_threshold) &
            (df["close_15m"] < prev_low)
        )

        if candidate["use_trend_filter"]:
            entry_condition = entry_condition & (df["ema_fast_30m"] < df["ema_slow_30m"])

    if candidate["use_vol_filter"]:
        entry_condition = entry_condition & (df["volatility_15m"] > vol_threshold)

    df["entry_signal"] = entry_condition.fillna(False).astype(int)

    position = []
    hold_bars = candidate["hold_bars"]
    current_pos = 0
    bars_left = 0

    for row in df.itertuples(index=False):
        signal = int(row.entry_signal)

        if current_pos == 0 and signal == 1:
            current_pos = 1 if direction == "long" else -1
            bars_left = hold_bars
        elif current_pos != 0:
            bars_left -= 1
            if bars_left <= 0:
                current_pos = 0
                bars_left = 0

        position.append(current_pos)

    df["position"] = position
    return df


def backtest_candidate(df, fee_per_trade=0.0006):
    df = df.copy()

    df["return"] = df["close_15m"].pct_change().fillna(0.0)
    df["trade"] = df["position"].diff().abs().fillna(0)
    df["fee"] = df["trade"] * fee_per_trade
    df["strategy_return"] = (df["position"].shift(1).fillna(0) * df["return"]) - df["fee"]
    df["equity"] = (1 + df["strategy_return"]).cumprod()

    return df


def run_alpha_miner():
    df = process()
    df = prepare_pa_features(df)

    candidates = build_rule_candidates()
    results = []

    for idx, candidate in enumerate(candidates, start=1):
        candidate_df = apply_candidate(df, candidate)
        candidate_df = backtest_candidate(candidate_df)

        metrics = calculate_backtest_metrics(candidate_df)
        row = {**candidate, **metrics, "candidate_id": idx}
        row["is_valid"] = is_candidate_valid(metrics)

        results.append(row)

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values(
        by=["total_return_pct", "sharpe_approx"],
        ascending=False
    ).reset_index(drop=True)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / "alpha_miner_results.csv"
    results_df.to_csv(output_path, index=False, sep=";")

    print(f"Saved alpha miner results: {output_path}")
    print()
    print(results_df.head(20).to_string(index=False))

    return results_df


if __name__ == "__main__":
    run_alpha_miner()
