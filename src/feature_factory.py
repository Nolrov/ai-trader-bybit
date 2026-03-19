import pandas as pd


def add_features(df):
    df = df.copy()

    df["ema_fast"] = df["close"].ewm(span=10).mean()
    df["ema_slow"] = df["close"].ewm(span=30).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()

    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    df["volatility"] = df["close"].pct_change().rolling(20).std()

    return df