def get_breakout_candidates():
    candidates = []

    for lookback in [3, 5, 10]:
        for body in [0.5, 0.6, 0.7]:
            for hold in [2, 4, 8]:
                for trend in [True, False]:
                    for vol in [True, False]:

                        candidates.append({
                            "family": "breakout",
                            "breakout_lookback": lookback,
                            "body_ratio_threshold": body,
                            "hold_bars": hold,
                            "use_trend_filter": trend,
                            "use_vol_filter": vol,
                            "direction": "long",
                        })

                        candidates.append({
                            "family": "breakout",
                            "breakout_lookback": lookback,
                            "body_ratio_threshold": body,
                            "hold_bars": hold,
                            "use_trend_filter": trend,
                            "use_vol_filter": vol,
                            "direction": "short",
                        })

    return candidates


def apply_breakout(df, c):
    df = df.copy()

    prev_high = df["high_15m"].rolling(c["breakout_lookback"]).max().shift(1)
    prev_low = df["low_15m"].rolling(c["breakout_lookback"]).min().shift(1)

    body_ok = df["body_ratio_15m"] >= c["body_ratio_threshold"]

    if c["direction"] == "long":
        entry = (df["close_15m"] > prev_high) & body_ok
        if c["use_trend_filter"]:
            entry &= df["ema_fast_30m"] > df["ema_slow_30m"]
    else:
        entry = (df["close_15m"] < prev_low) & body_ok
        if c["use_trend_filter"]:
            entry &= df["ema_fast_30m"] < df["ema_slow_30m"]

    df["entry_signal"] = entry.fillna(False).astype(int)
    return df
