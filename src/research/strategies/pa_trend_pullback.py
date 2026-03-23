def get_pa_trend_pullback_candidates():
    candidates = []
    for pullback_gap in [0.0015, 0.0025, 0.0035]:
        for reclaim_body in [0.45, 0.55]:
            for swing_lookback in [3, 5]:
                for hold in [4, 8, 12]:
                    for direction in ["long", "short"]:
                        candidates.append(
                            {
                                "family": "pa_trend_pullback",
                                "pullback_gap": pullback_gap,
                                "reclaim_body_ratio": reclaim_body,
                                "swing_lookback": swing_lookback,
                                "min_hold_bars": 2,
                                "hold_bars": hold,
                                "direction": direction,
                                "exit_style": "pa_structure_failure",
                                "regime_tag": "trend",
                            }
                        )
    return candidates


def apply_pa_trend_pullback(df, c):
    df = df.copy()
    swing_lb = int(c.get("swing_lookback", 3))
    reclaim_body = c.get("reclaim_body_ratio", 0.5)
    pullback_gap = c.get("pullback_gap", 0.0025)

    trend_up = (df["ema_fast_30m"] > df["ema_slow_30m"]) & (df["ema_slope_fast_30m"] > 0)
    trend_down = (df["ema_fast_30m"] < df["ema_slow_30m"]) & (df["ema_slope_fast_30m"] < 0)

    pullback_long = (
        (df["close_15m"].shift(1) <= df["ema_fast_15m"].shift(1))
        | (df["low_15m"].rolling(2).min().shift(1) <= df["ema_fast_15m"].shift(1) * (1 - pullback_gap))
        | (df["ema_gap_fast_15m"].shift(1) <= -pullback_gap)
    )
    pullback_short = (
        (df["close_15m"].shift(1) >= df["ema_fast_15m"].shift(1))
        | (df["high_15m"].rolling(2).max().shift(1) >= df["ema_fast_15m"].shift(1) * (1 + pullback_gap))
        | (df["ema_gap_fast_15m"].shift(1) >= pullback_gap)
    )

    reclaim_long = (
        (df["close_15m"] > df["ema_fast_15m"])
        & (df["close_15m"] > df["high_15m"].shift(1))
        & (df["body_ratio_15m"] >= reclaim_body)
        & df["bullish_15m"]
        & (df["close_location_15m"] >= 0.62)
    )
    reclaim_short = (
        (df["close_15m"] < df["ema_fast_15m"])
        & (df["close_15m"] < df["low_15m"].shift(1))
        & (df["body_ratio_15m"] >= reclaim_body)
        & df["bearish_15m"]
        & (df["close_location_15m"] <= 0.38)
    )

    structure_fail_long = (
        (df["close_15m"] < df["recent_swing_low"].rolling(swing_lb).min().shift(1))
        | ((df["close_15m"] < df["ema_fast_15m"]) & df["bearish_15m"])
        | (df["rsi_15m"] <= 47)
    )
    structure_fail_short = (
        (df["close_15m"] > df["recent_swing_high"].rolling(swing_lb).max().shift(1))
        | ((df["close_15m"] > df["ema_fast_15m"]) & df["bullish_15m"])
        | (df["rsi_15m"] >= 53)
    )

    if c["direction"] == "long":
        entry = trend_up & pullback_long & reclaim_long & (df["regime_trend"] == 1)
        exit_signal = structure_fail_long.fillna(False)
    else:
        entry = trend_down & pullback_short & reclaim_short & (df["regime_trend"] == 1)
        exit_signal = structure_fail_short.fillna(False)

    df["entry_signal"] = entry.fillna(False).astype(int)
    df["exit_signal"] = exit_signal.astype(int)
    return df
