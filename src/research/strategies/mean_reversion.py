def get_mean_reversion_candidates():
    candidates = []

    for z in [0.75, 1.0, 1.25, 1.5, 1.75]:
        for hold in [2, 4, 6]:
            for trend in [False, True]:
                for vol in [False, True]:
                    for direction in ["long", "short"]:
                        candidates.append(
                            {
                                "family": "mean_reversion",
                                "zscore_threshold": z,
                                "hold_bars": hold,
                                "use_trend_filter": trend,
                                "use_vol_filter": vol,
                                "direction": direction,
                                "regime_tag": "flat",
                            }
                        )

    return candidates


def apply_mean_reversion(df, c):
    df = df.copy()
    z = df["zscore_ret_20"]

    if c["direction"] == "long":
        entry = (z <= -c["zscore_threshold"]) & (df["rsi_15m"] <= 45)
        if c["use_trend_filter"]:
            entry &= df["ema_fast_30m"] > df["ema_slow_30m"]
    else:
        entry = (z >= c["zscore_threshold"]) & (df["rsi_15m"] >= 55)
        if c["use_trend_filter"]:
            entry &= df["ema_fast_30m"] < df["ema_slow_30m"]

    entry &= df["regime_flat"] == 1
    if c.get("use_vol_filter", False):
        entry &= df["regime_high_vol"] == 0

    df["entry_signal"] = entry.fillna(False).astype(int)
    return df
