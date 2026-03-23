def get_pa_breakout_retest_candidates():
    candidates = []
    for lookback in [12, 20, 30]:
        for retest_tolerance in [0.0006, 0.0010, 0.0015]:
            for body_ratio in [0.35, 0.45, 0.55]:
                for hold in [4, 8, 12]:
                    for direction in ["long", "short"]:
                        candidates.append(
                            {
                                "family": "pa_breakout_retest",
                                "range_lookback": lookback,
                                "retest_tolerance": retest_tolerance,
                                "trigger_body_ratio": body_ratio,
                                "min_hold_bars": 2,
                                "hold_bars": hold,
                                "direction": direction,
                                "exit_style": "retest_failure",
                                "regime_tag": "trend_high_vol",
                            }
                        )
    return candidates


def apply_pa_breakout_retest(df, c):
    df = df.copy()
    lookback = int(c.get("range_lookback", 20))
    tolerance = c.get("retest_tolerance", 0.001)
    body_ratio = c.get("trigger_body_ratio", 0.45)

    range_high = df["high_15m"].rolling(lookback).max().shift(2)
    range_low = df["low_15m"].rolling(lookback).min().shift(2)
    bullish_trend = (df["ema_fast_30m"] > df["ema_slow_30m"]) & (df["ema_slope_fast_30m"] > -0.0005)
    bearish_trend = (df["ema_fast_30m"] < df["ema_slow_30m"]) & (df["ema_slope_fast_30m"] < 0.0005)

    breakout_long_prev = (df["close_15m"].shift(1) > range_high) | (df["high_15m"].shift(1) > range_high * (1 + tolerance * 0.5))
    breakout_short_prev = (df["close_15m"].shift(1) < range_low) | (df["low_15m"].shift(1) < range_low * (1 - tolerance * 0.5))

    long_retest = (
        breakout_long_prev
        & (df["low_15m"] <= range_high * (1 + tolerance))
        & (df["close_15m"] >= range_high * (1 - tolerance * 0.25))
        & (df["close_location_15m"] >= 0.52)
        & (df["body_ratio_15m"] >= body_ratio)
        & (df["lower_wick_ratio_15m"] >= 0.14)
    )
    short_retest = (
        breakout_short_prev
        & (df["high_15m"] >= range_low * (1 - tolerance))
        & (df["close_15m"] <= range_low * (1 + tolerance * 0.25))
        & (df["close_location_15m"] <= 0.48)
        & (df["body_ratio_15m"] >= body_ratio)
        & (df["upper_wick_ratio_15m"] >= 0.14)
    )

    long_exit = (
        (df["close_15m"] < range_high * (1 - tolerance * 0.5))
        | ((df["close_15m"] < df["low_15m"].shift(1)) & df["bearish_15m"])
        | (df["rsi_15m"] <= 48)
    )
    short_exit = (
        (df["close_15m"] > range_low * (1 + tolerance * 0.5))
        | ((df["close_15m"] > df["high_15m"].shift(1)) & df["bullish_15m"])
        | (df["rsi_15m"] >= 52)
    )

    if c["direction"] == "long":
        entry = long_retest & bullish_trend & (df["regime_high_vol"] == 1)
        exit_signal = long_exit
    else:
        entry = short_retest & bearish_trend & (df["regime_high_vol"] == 1)
        exit_signal = short_exit

    df["entry_signal"] = entry.fillna(False).astype(int)
    df["exit_signal"] = exit_signal.fillna(False).astype(int)
    return df
