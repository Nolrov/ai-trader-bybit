from itertools import product


def build_breakout_candidates():
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
        candidates.append(
            {
                "family": "breakout",
                "direction": direction,
                "body_ratio_threshold": body_ratio,
                "breakout_lookback": breakout_lookback,
                "hold_bars": hold_bars,
                "use_vol_filter": use_vol_filter,
                "use_trend_filter": use_trend_filter,
            }
        )

    return candidates


def build_mean_reversion_candidates():
    candidates = []

    zscore_thresholds = [1.5, 2.0, 2.5]
    hold_bars_options = [2, 4, 8]
    directions = ["long", "short"]
    use_trend_filter_options = [True, False]

    for direction, zscore_threshold, hold_bars, use_trend_filter in product(
        directions,
        zscore_thresholds,
        hold_bars_options,
        use_trend_filter_options,
    ):
        candidates.append(
            {
                "family": "mean_reversion",
                "direction": direction,
                "zscore_threshold": zscore_threshold,
                "hold_bars": hold_bars,
                "use_trend_filter": use_trend_filter,
                "use_vol_filter": False,
            }
        )

    return candidates


def build_trend_pullback_candidates():
    candidates = []

    pullback_thresholds = [0.003, 0.005, 0.008]
    hold_bars_options = [2, 4, 8]
    directions = ["long", "short"]
    use_vol_filter_options = [True, False]

    for direction, pullback_threshold, hold_bars, use_vol_filter in product(
        directions,
        pullback_thresholds,
        hold_bars_options,
        use_vol_filter_options,
    ):
        candidates.append(
            {
                "family": "trend_pullback",
                "direction": direction,
                "pullback_threshold": pullback_threshold,
                "hold_bars": hold_bars,
                "use_trend_filter": True,
                "use_vol_filter": use_vol_filter,
            }
        )

    return candidates


def build_rule_candidates():
    candidates = []
    candidates.extend(build_breakout_candidates())
    candidates.extend(build_mean_reversion_candidates())
    candidates.extend(build_trend_pullback_candidates())
    return candidates
