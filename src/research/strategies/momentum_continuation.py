def get_momentum_continuation_candidates():
    candidates = []

    for roc_window in [4, 8]:
        for min_roc in [0.003, 0.005, 0.0075]:
            for hold in [2, 4, 6]:
                for vol in [False, True]:
                    for direction in ["long", "short"]:
                        candidates.append(
                            {
                                "family": "momentum_continuation",
                                "roc_window": roc_window,
                                "min_roc": min_roc,
                                "hold_bars": hold,
                                "use_vol_filter": vol,
                                "direction": direction,
                                "regime_tag": "trend_high_vol" if vol else "trend",
                            }
                        )

    return candidates


def apply_momentum_continuation(df, c):
    df = df.copy()
    roc = df["close_15m"].pct_change(c["roc_window"])

    if c["direction"] == "long":
        entry = (
            (roc >= c["min_roc"])
            & (df["close_15m"] > df["ema_fast_15m"])
            & (df["ema_fast_30m"] > df["ema_slow_30m"])
            & (df["rsi_15m"] >= 55)
            & (df["rsi_15m"] <= 78)
        )
    else:
        entry = (
            (roc <= -c["min_roc"])
            & (df["close_15m"] < df["ema_fast_15m"])
            & (df["ema_fast_30m"] < df["ema_slow_30m"])
            & (df["rsi_15m"] <= 45)
            & (df["rsi_15m"] >= 22)
        )

    entry &= df["regime_trend"] == 1
    if c.get("use_vol_filter", False):
        entry &= df["regime_high_vol"] == 1

    df["entry_signal"] = entry.fillna(False).astype(int)
    return df
