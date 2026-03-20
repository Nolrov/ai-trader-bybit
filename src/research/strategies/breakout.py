def get_breakout_candidates():
    candidates = []

    for lookback in [10]:
        for body in [0.7]:
            for hold in [2, 4]:
                for trend in [False, True]:
                    for vol in [False, True]:
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

    if c["use_vol_filter"]:
        entry &= df["regime_high_vol"] == 1

    df["entry_signal"] = entry.fillna(False).astype(int)
    return df