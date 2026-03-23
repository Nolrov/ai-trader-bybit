def get_pa_range_rejection_candidates():
    candidates = []
    for lookback in [12, 20, 30]:
        for edge_buffer in [0.08, 0.12]:
            for wick_ratio in [0.22, 0.32]:
                for hold in [3, 6, 10]:
                    for direction in ["long", "short"]:
                        candidates.append(
                            {
                                "family": "pa_range_rejection",
                                "range_lookback": lookback,
                                "edge_buffer_ratio": edge_buffer,
                                "wick_ratio_threshold": wick_ratio,
                                "min_hold_bars": 1,
                                "hold_bars": hold,
                                "direction": direction,
                                "exit_style": "range_mid_reversion",
                                "regime_tag": "flat",
                            }
                        )
    return candidates


def apply_pa_range_rejection(df, c):
    df = df.copy()
    lookback = int(c.get("range_lookback", 20))
    edge_buffer_ratio = c.get("edge_buffer_ratio", 0.1)
    wick_ratio = c.get("wick_ratio_threshold", 0.25)

    range_high = df["high_15m"].rolling(lookback).max().shift(1)
    range_low = df["low_15m"].rolling(lookback).min().shift(1)
    range_width = (range_high - range_low)
    range_mid = range_low + range_width / 2.0
    edge_buffer = range_width * edge_buffer_ratio

    compressed = (df["range_width_pct_20"] <= df["range_width_pct_20"].rolling(20).median() * 1.2).fillna(False)
    flat_context = (df["regime_flat"] == 1) & compressed

    long_entry = (
        (df["low_15m"] <= range_low + edge_buffer)
        & (df["close_15m"] > range_low + edge_buffer * 0.3)
        & (df["lower_wick_ratio_15m"] >= wick_ratio)
        & (df["close_location_15m"] >= 0.45)
    )
    short_entry = (
        (df["high_15m"] >= range_high - edge_buffer)
        & (df["close_15m"] < range_high - edge_buffer * 0.3)
        & (df["upper_wick_ratio_15m"] >= wick_ratio)
        & (df["close_location_15m"] <= 0.55)
    )

    long_exit = (df["close_15m"] >= range_mid) | (df["close_15m"] < range_low) | (df["rsi_15m"] >= 56)
    short_exit = (df["close_15m"] <= range_mid) | (df["close_15m"] > range_high) | (df["rsi_15m"] <= 44)

    if c["direction"] == "long":
        entry = long_entry & flat_context
        exit_signal = long_exit
    else:
        entry = short_entry & flat_context
        exit_signal = short_exit

    df["entry_signal"] = entry.fillna(False).astype(int)
    df["exit_signal"] = exit_signal.fillna(False).astype(int)
    return df
