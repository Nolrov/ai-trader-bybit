def get_mean_reversion_candidates():
    candidates = []

    for z in [1.5, 2.0]:
        for hold in [2, 4]:
            for trend in [True, False]:

                candidates.append({
                    "family": "mean_reversion",
                    "zscore_threshold": z,
                    "hold_bars": hold,
                    "use_trend_filter": trend,
                    "direction": "long",
                })

                candidates.append({
                    "family": "mean_reversion",
                    "zscore_threshold": z,
                    "hold_bars": hold,
                    "use_trend_filter": trend,
                    "direction": "short",
                })

    return candidates


def apply_mean_reversion(df, c):
    df = df.copy()

    z = df["zscore_ret_20"]

    if c["direction"] == "long":
        entry = z <= -c["zscore_threshold"]
    else:
        entry = z >= c["zscore_threshold"]

    if c["use_trend_filter"]:
        if c["direction"] == "long":
            entry &= df["ema_fast_30m"] > df["ema_slow_30m"]
        else:
            entry &= df["ema_fast_30m"] < df["ema_slow_30m"]

    df["entry_signal"] = entry.fillna(False).astype(int)
    return df
