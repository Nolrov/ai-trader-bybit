def get_momentum_continuation_candidates():
    candidates = []

    for roc_window in [4, 8]:
        for min_roc in [0.003, 0.005, 0.0075]:
            for hold in [2, 4, 6]:
                for vol in [False, True]:
                    candidates.append(
                        {
                            "family": "momentum_continuation",
                            "roc_window": roc_window,
                            "min_roc": min_roc,
                            "hold_bars": hold,
                            "use_vol_filter": vol,
                            "direction": "long",
                            "regime_tag": "trend_high_vol" if vol else "trend",
                        }
                    )

    for roc_window in [4, 8]:
        for min_roc in [0.003, 0.0045, 0.006]:
            for hold in [3, 4, 6]:
                for vol in [False, True]:
                    for rsi_floor in [28, 32]:
                        for rsi_ceiling in [46, 52]:
                            for ema_gap_cap in [0.0, 0.002]:
                                for trend_slope_max in [-0.0001, -0.0004]:
                                    candidates.append(
                                        {
                                            "family": "momentum_continuation",
                                            "roc_window": roc_window,
                                            "min_roc": min_roc,
                                            "hold_bars": hold,
                                            "use_vol_filter": vol,
                                            "direction": "short",
                                            "rsi_floor": rsi_floor,
                                            "rsi_ceiling": rsi_ceiling,
                                            "ema_gap_cap": ema_gap_cap,
                                            "trend_slope_max": trend_slope_max,
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
        exit_signal = (
            ((df["close_15m"] < df["ema_fast_15m"]) & df["bearish_15m"])
            | (df["rsi_15m"] <= 48)
        )
    else:
        rsi_floor = float(c.get("rsi_floor", 28))
        rsi_ceiling = float(c.get("rsi_ceiling", 48))
        ema_gap_cap = float(c.get("ema_gap_cap", 0.0))
        trend_slope_max = float(c.get("trend_slope_max", -0.0001))

        entry = (
            (roc <= -c["min_roc"])
            & (df["close_15m"] < df["ema_fast_15m"] * (1 + ema_gap_cap))
            & (df["ema_fast_30m"] < df["ema_slow_30m"])
            & (df["ema_slope_fast_30m"] <= trend_slope_max)
            & (df["rsi_15m"] <= rsi_ceiling)
            & (df["rsi_15m"] >= rsi_floor)
        )
        exit_signal = (
            ((df["close_15m"] > df["ema_fast_15m"]) & df["bullish_15m"])
            | (df["rsi_15m"] >= max(52, rsi_ceiling + 2))
        )

    entry &= df["regime_trend"] == 1
    if c.get("use_vol_filter", False):
        entry &= df["regime_high_vol"] == 1

    df["entry_signal"] = entry.fillna(False).astype(int)
    df["exit_signal"] = exit_signal.fillna(False).astype(int)
    return df
