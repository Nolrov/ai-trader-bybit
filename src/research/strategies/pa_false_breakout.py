import pandas as pd


def get_pa_false_breakout_candidates():
    candidates = []
    for lookback in [10, 20, 30]:
        for sweep_ratio in [0.0006, 0.0012]:
            for wick_ratio in [0.28, 0.4]:
                for hold in [3, 6, 10]:
                    for direction in ["long", "short"]:
                        candidates.append(
                            {
                                "family": "pa_false_breakout",
                                "range_lookback": lookback,
                                "sweep_ratio": sweep_ratio,
                                "wick_ratio_threshold": wick_ratio,
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
    sweep_ratio = c.get("sweep_ratio", 0.001)
    wick_ratio = c.get("wick_ratio_threshold", 0.3)

    range_high = df["high_15m"].rolling(lookback).max().shift(1)
    range_low = df["low_15m"].rolling(lookback).min().shift(1)
    range_mid = (range_high + range_low) / 2.0
    range_width_pct = (range_high - range_low) / df["close_15m"].replace(0, pd.NA)
    range_ok = range_width_pct <= df["range_width_pct_20"].rolling(10).median().fillna(range_width_pct.median()) * 1.35

    long_entry = (
        (df["low_15m"] < range_low * (1 - sweep_ratio))
        & (df["close_15m"] > range_low)
        & (df["close_15m"] < range_mid)
        & (df["lower_wick_ratio_15m"] >= wick_ratio)
        & (df["close_location_15m"] >= 0.45)
    )
    short_entry = (
        (df["high_15m"] > range_high * (1 + sweep_ratio))
        & (df["close_15m"] < range_high)
        & (df["close_15m"] > range_mid)
        & (df["upper_wick_ratio_15m"] >= wick_ratio)
        & (df["close_location_15m"] <= 0.55)
    )

    long_exit = (df["close_15m"] >= range_mid) | (df["close_15m"] < range_low) | (df["rsi_15m"] >= 56)
    short_exit = (df["close_15m"] <= range_mid) | (df["close_15m"] > range_high) | (df["rsi_15m"] <= 44)

    flat_context = (df["regime_flat"] == 1) & (df["regime_high_vol"] == 0) & range_ok.fillna(False)

    if c["direction"] == "long":
        entry = long_entry & flat_context
        exit_signal = long_exit
    else:
        entry = short_entry & flat_context
        exit_signal = short_exit

    df["entry_signal"] = entry.fillna(False).astype(int)
    df["exit_signal"] = exit_signal.fillna(False).astype(int)
    return df
