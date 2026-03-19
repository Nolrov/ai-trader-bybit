import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

import pandas as pd

from processing.data_processor import process
from research.rule_builder import build_rule_candidates
from research.validators import calculate_backtest_metrics


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
            df["bullish_15m"] &
            (df["body_ratio_15m"] >= body_ratio_threshold) &
            (df["close_15m"] > prev_high)
        )
        if candidate["use_trend_filter"]:
            entry = entry & (df["ema_fast_30m"] > df["ema_slow_30m"])
    else:
        entry = (
            df["bearish_15m"] &
            (df["body_ratio_15m"] >= body_ratio_threshold) &
            (df["close_15m"] < prev_low)
        )
        if candidate["use_trend_filter"]:
            entry = entry & (df["ema_fast_30m"] < df["ema_slow_30m"])

    if candidate["use_vol_filter"]:
        entry = entry & (df["volatility_15m"] > vol_threshold)

    df["entry_signal"] = entry.fillna(False).astype(int)

    position = []
    hold = candidate["hold_bars"]
    pos = 0
    bars = 0

    for row in df.itertuples(index=False):
        if pos == 0 and row.entry_signal == 1:
            pos = 1 if direction == "long" else -1
            bars = hold
        elif pos != 0:
            bars -= 1
            if bars <= 0:
                pos = 0

        position.append(pos)

    df["position"] = position
    return df


def backtest(df, fee=0.0006):
    df = df.copy()

    df["ret"] = df["close_15m"].pct_change().fillna(0)
    df["trade"] = df["position"].diff().abs().fillna(0)
    df["fee"] = df["trade"] * fee
    df["strategy_return"] = df["position"].shift(1).fillna(0) * df["ret"] - df["fee"]
    df["equity"] = (1 + df["strategy_return"]).cumprod()

    return df


def split_df(df):
    split = int(len(df) * 0.7)
    return df.iloc[:split], df.iloc[split:]


def run_alpha_miner():
    df = process()
    df = prepare_pa_features(df)

    train_df, test_df = split_df(df)

    candidates = build_rule_candidates()
    results = []

    for i, candidate in enumerate(candidates, 1):
        train_bt = backtest(apply_candidate(train_df, candidate))
        test_bt = backtest(apply_candidate(test_df, candidate))

        train_m = calculate_backtest_metrics(train_bt)
        test_m = calculate_backtest_metrics(test_bt)

        stable = (
            test_m["total_return_pct"] > -5 and
            test_m["trades"] > 10 and
            abs(test_m["total_return_pct"] - train_m["total_return_pct"]) < 10
        )

        results.append({
            "id": i,
            **candidate,
            "train_return": train_m["total_return_pct"],
            "test_return": test_m["total_return_pct"],
            "train_sharpe": train_m["sharpe_approx"],
            "test_sharpe": test_m["sharpe_approx"],
            "train_trades": train_m["trades"],
            "test_trades": test_m["trades"],
            "stable": stable
        })

    df_res = pd.DataFrame(results)
    df_res = df_res.sort_values(by="test_return", ascending=False)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "alpha_miner_wf.csv"
    df_res.to_csv(path, index=False)

    print(f"Saved: {path}")
    print()
    print(df_res.head(20).to_string(index=False))


if __name__ == "__main__":
    run_alpha_miner()
