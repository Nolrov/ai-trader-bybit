import pandas as pd
from feature_factory import add_features


def load_data():
    df_15 = pd.read_csv("../data/btcusdt_15m.csv")
    df_30 = pd.read_csv("../data/btcusdt_30m.csv")

    df_15["timestamp"] = pd.to_datetime(df_15["timestamp"])
    df_30["timestamp"] = pd.to_datetime(df_30["timestamp"])

    return df_15, df_30


def process():
    df_15, df_30 = load_data()

    df_15 = add_features(df_15)
    df_30 = add_features(df_30)

    df_15["timestamp_30m"] = df_15["timestamp"].dt.floor("30min")
    df_30 = df_30.rename(columns={"timestamp": "timestamp_30m"})

    df = df_15.merge(
        df_30,
        on="timestamp_30m",
        suffixes=("_15m", "_30m")
    )

    return df


if __name__ == "__main__":
    df = process()

    print(df.tail())
    print(f"\nRows: {len(df)}")
