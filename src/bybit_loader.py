import time
from pathlib import Path

import pandas as pd
import requests


BASE_URL = "https://api.bybit.com"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def get_klines_full(symbol="BTCUSDT", interval="15", total=2000):
    all_data = []
    end = None

    while len(all_data) < total:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": 200
        }

        if end is not None:
            params["end"] = end

        response = requests.get(
            f"{BASE_URL}/v5/market/kline",
            params=params,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()

        if data["retCode"] != 0:
            raise Exception(f"Bybit API error: {data}")

        klines = data["result"]["list"]

        if not klines:
            break

        all_data.extend(klines)
        end = klines[-1][0]

        time.sleep(0.1)

    df = pd.DataFrame(all_data, columns=[
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover"
    ])

    df = df.astype({
        "timestamp": "int64",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "turnover": "float64"
    })

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)

    return df.tail(total)


def save_data(df, filename):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / filename
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")


def download_and_save(symbol="BTCUSDT", interval="15", total=2000):
    df = get_klines_full(symbol=symbol, interval=interval, total=total)
    filename = f"{symbol.lower()}_{interval}m.csv"

    print(f"\n=== {symbol} {interval}m ===")
    print(df.tail())

    save_data(df, filename)
    return df


if __name__ == "__main__":
    download_and_save(symbol="BTCUSDT", interval="15", total=2000)
    download_and_save(symbol="BTCUSDT", interval="30", total=2000)
