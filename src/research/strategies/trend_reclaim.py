def get_trend_reclaim_candidates():
    candidates = []

    for gap in [0.0005, 0.0010, 0.0015]:
        for hold in [4, 8, 12]:
            for direction in ["long", "short"]:
                candidates.append(
                    {
                        "family": "trend_reclaim",
                        "reclaim_gap": gap,
                        "min_hold_bars": 2,
                        "hold_bars": hold,
                        "direction": direction,
                        "exit_style": "trend_failure",
                        "regime_tag": "trend",
                    }
                )

    return candidates


def apply_trend_reclaim(df, c):
    df = df.copy()
    gap = c["reclaim_gap"]

    if c["direction"] == "long":
        entry = (
            (df["regime_trend"] == 1)
            & (df["ema_fast_30m"] > df["ema_slow_30m"])
            & (df["close_15m"] > df["ema_fast_15m"])
            & (df["close_15m"].shift(1) <= df["ema_fast_15m"].shift(1))
            & (df["ema_gap_fast_15m"] >= gap)
            & (df["rsi_15m"] >= 52)
        )
        exit_signal = (
            (df["close_15m"] < df["ema_fast_15m"])
            | (df["ema_slope_fast_30m"] <= 0)
            | (df["rsi_15m"] <= 48)
        )
    else:
        entry = (
            (df["regime_trend"] == 1)
            & (df["ema_fast_30m"] < df["ema_slow_30m"])
            & (df["close_15m"] < df["ema_fast_15m"])
            & (df["close_15m"].shift(1) >= df["ema_fast_15m"].shift(1))
            & (df["ema_gap_fast_15m"] <= -gap)
            & (df["rsi_15m"] <= 48)
        )
        exit_signal = (
            (df["close_15m"] > df["ema_fast_15m"])
            | (df["ema_slope_fast_30m"] >= 0)
            | (df["rsi_15m"] >= 52)
        )

    df["entry_signal"] = entry.fillna(False).astype(int)
    df["exit_signal"] = exit_signal.fillna(False).astype(int)
    return df
