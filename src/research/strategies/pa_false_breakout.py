import pandas as pd


def get_pa_false_breakout_candidates():
    candidates = []
    for lookback in [10, 20, 30]:
        for sweep_ratio in [0.0004, 0.0008, 0.0012]:
            for wick_ratio in [0.22, 0.32, 0.42]:
                for reclaim_ratio in [0.40, 0.50]:
                    for hold in [3, 6, 10]:
                        for direction in ["long", "short"]:
                            candidates.append(
                                {
                                    "family": "pa_false_breakout",
                                    "range_lookback": lookback,
                                    "sweep_ratio": sweep_ratio,
                                    "wick_ratio_threshold": wick_ratio,
                                    "reclaim_close_ratio": reclaim_ratio,
                                    "min_hold_bars": 1,
                                    "hold_bars": hold,
                                    "direction": direction,
                                    "exit_style": "range_reentry",
                                    "regime_tag": "flat",
                                }
                            )
    return candidates


def apply_pa_false_breakout(df, c):
    df = df.copy()
    lookback = int(c.get("range_lookback", 20))
    sweep_ratio = c.get("sweep_ratio", 0.0008)
    wick_ratio = c.get("wick_ratio_threshold", 0.3)
    reclaim_ratio = c.get("reclaim_close_ratio", 0.45)

    range_high = df["high_15m"].rolling(lookback).max().shift(1)
    range_low = df["low_15m"].rolling(lookback).min().shift(1)
    range_mid = (range_high + range_low) / 2.0
    range_width_pct = (range_high - range_low) / df["close_15m"].replace(0, pd.NA)
    baseline_width = df["range_width_pct_20"].rolling(20).median().fillna(df["range_width_pct_20"].median())
    range_ok = range_width_pct <= baseline_width * 1.55

    flat_trend = (df["regime_flat"] == 1) | (df["ema_trend_strength_30m"] <= 0.005)
    tame_vol = (df["regime_high_vol"] == 0) | (df["atr_pct_14"] <= df["atr_pct_14"].rolling(30).median().fillna(df["atr_pct_14"].median()) * 1.2)
    flat_context = flat_trend & tame_vol & range_ok.fillna(False)

    long_entry = (
        (df["low_15m"] < range_low * (1 - sweep_ratio))
        & (df["close_15m"] > range_low)
        & (df["close_15m"] <= range_mid * 1.01)
        & (df["lower_wick_ratio_15m"] >= wick_ratio)
        & (df["close_location_15m"] >= reclaim_ratio)
    )
    short_entry = (
        (df["high_15m"] > range_high * (1 + sweep_ratio))
        & (df["close_15m"] < range_high)
        & (df["close_15m"] >= range_mid * 0.99)
        & (df["upper_wick_ratio_15m"] >= wick_ratio)
        & (df["close_location_15m"] <= (1 - reclaim_ratio))
    )

    long_exit = (
        (df["close_15m"] >= range_mid)
        | (df["close_15m"] < range_low * (1 - sweep_ratio * 0.5))
        | (df["rsi_15m"] >= 57)
    )
    short_exit = (
        (df["close_15m"] <= range_mid)
        | (df["close_15m"] > range_high * (1 + sweep_ratio * 0.5))
        | (df["rsi_15m"] <= 43)
    )

    if c["direction"] == "long":
        entry = long_entry & flat_context
        exit_signal = long_exit
    else:
        entry = short_entry & flat_context
        exit_signal = short_exit

    df["entry_signal"] = entry.fillna(False).astype(int)
    df["exit_signal"] = exit_signal.fillna(False).astype(int)
    return df
