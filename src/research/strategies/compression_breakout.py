def get_compression_breakout_candidates():
    candidates = []

    for compression in [0.75, 0.9]:
        for lookback in [12, 20]:
            for hold in [4, 8, 12]:
                for direction in ["long", "short"]:
                    candidates.append(
                        {
                            "family": "compression_breakout",
                            "compression_threshold": compression,
                            "breakout_lookback": lookback,
                            "min_hold_bars": 2,
                            "hold_bars": hold,
                            "direction": direction,
                            "exit_style": "compression_failure",
                            "regime_tag": "trend_high_vol",
                        }
                    )

    return candidates


def apply_compression_breakout(df, c):
    df = df.copy()
    prev_high = df["high_15m"].rolling(c["breakout_lookback"]).max().shift(1)
    prev_low = df["low_15m"].rolling(c["breakout_lookback"]).min().shift(1)
    compressed = df["range_pct_15m"] <= (df["range_pct_median_20"] * c["compression_threshold"])
    compressed_recently = compressed.rolling(4).max().fillna(0).astype(bool)

    if c["direction"] == "long":
        entry = (
            compressed_recently
            & (df["close_15m"] > prev_high)
            & (df["ema_fast_30m"] > df["ema_slow_30m"])
            & (df["rsi_15m"] >= 54)
        )
        exit_signal = (
            (df["close_15m"] < prev_high)
            | (df["close_15m"] < df["ema_fast_15m"])
            | (df["rsi_15m"] <= 49)
        )
    else:
        entry = (
            compressed_recently
            & (df["close_15m"] < prev_low)
            & (df["ema_fast_30m"] < df["ema_slow_30m"])
            & (df["rsi_15m"] <= 46)
        )
        exit_signal = (
            (df["close_15m"] > prev_low)
            | (df["close_15m"] > df["ema_fast_15m"])
            | (df["rsi_15m"] >= 51)
        )

    entry &= (df["regime_trend"] == 1) & (df["regime_high_vol"] == 1)

    df["entry_signal"] = entry.fillna(False).astype(int)
    df["exit_signal"] = exit_signal.fillna(False).astype(int)
    return df
