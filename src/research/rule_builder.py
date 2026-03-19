from itertools import product


def build_rule_candidates():
    candidates = []

    body_ratio_thresholds = [0.5, 0.6, 0.7]
    breakout_lookbacks = [3, 5, 10]
    hold_bars_options = [2, 4, 8]
    directions = ["long", "short"]
    use_vol_filter_options = [True, False]
    use_trend_filter_options = [True, False]

    for direction, body_ratio, breakout_lookback, hold_bars, use_vol_filter, use_trend_filter in product(
        directions,
        body_ratio_thresholds,
        breakout_lookbacks,
        hold_bars_options,
        use_vol_filter_options,
        use_trend_filter_options,
    ):
            candidate = {
                "direction": direction,
                "body_ratio_threshold": body_ratio,
                "breakout_lookback": breakout_lookback,
                "hold_bars": hold_bars,
                "use_vol_filter": use_vol_filter,
                "use_trend_filter": use_trend_filter,
            }
            candidates.append(candidate)

    return candidates
