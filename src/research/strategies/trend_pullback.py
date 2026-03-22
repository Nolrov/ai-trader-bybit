def get_trend_pullback_candidates():
    candidates = []

    for pullback in [0.0015, 0.0025, 0.0035, 0.005, 0.0065]:
        for hold in [2, 4, 6, 8]:
            for vol in [False, True]:
                for direction in ["long", "short"]:
                    candidates.append(
                        {
                            "family": "trend_pullback",
                            "pullback_threshold": pullback,
                            "hold_bars": hold,
                            "use_vol_filter": vol,
                            "direction": direction,
                            "regime_tag": "trend_high_vol" if vol else "trend",
                        }
                    )

    return candidates


def apply_trend_pullback(df, c):
    df = df.copy()
    pullback = c["pullback_threshold"]

    if c["direction"] == "long":
        entry = (
            (df["ema_fast_30m"] > df["ema_slow_30m"])
            & (df["ema_gap_15m"] <= -pullback)
            & df["bullish_15m"]
            & (df["ema_slope_fast_30m"] > 0)
        )
    else:
        entry = (
            (df["ema_fast_30m"] < df["ema_slow_30m"])
            & (df["ema_gap_15m"] >= pullback)
            & df["bearish_15m"]
            & (df["ema_slope_fast_30m"] < 0)
        )

    entry &= df["regime_trend"] == 1
    if c["use_vol_filter"]:
        entry &= df["regime_high_vol"] == 1

    df["entry_signal"] = entry.fillna(False).astype(int)
    return df
