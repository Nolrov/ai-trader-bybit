import pandas as pd


def compute_atr(df, period=14):
    high = df["high_15m"]
    low = df["low_15m"]
    close = df["close_15m"]

    prev_close = close.shift(1)

    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(period).mean()
    return atr


def get_atr_breakout_candidates():
    candidates = []

    for atr_mult in [1.0, 1.5]:
        for hold in [2, 4]:
            for trend in [False, True]:
                for vol in [False, True]:
                    candidates.append({
                        "family": "atr_breakout",
                        "atr_mult": atr_mult,
                        "hold_bars": hold,
                        "use_trend_filter": trend,
                        "use_vol_filter": vol,
                        "direction": "long",
                    })

                    candidates.append({
                        "family": "atr_breakout",
                        "atr_mult": atr_mult,
                        "hold_bars": hold,
                        "use_trend_filter": trend,
                        "use_vol_filter": vol,
                        "direction": "short",
                    })

    return candidates


def apply_atr_breakout(df, c):
    df = df.copy()

    df["atr_14"] = compute_atr(df)

    prev_close = df["close_15m"].shift(1)

    upper = prev_close + df["atr_14"] * c["atr_mult"]
    lower = prev_close - df["atr_14"] * c["atr_mult"]

    if c["direction"] == "long":
        entry = df["close_15m"] > upper

        if c["use_trend_filter"]:
            entry &= df["ema_fast_30m"] > df["ema_slow_30m"]

    else:
        entry = df["close_15m"] < lower

        if c["use_trend_filter"]:
            entry &= df["ema_fast_30m"] < df["ema_slow_30m"]

    if c.get("use_vol_filter", False):
        entry &= df["regime_high_vol"] == 1

    df["entry_signal"] = entry.fillna(False).astype(int)
    return df