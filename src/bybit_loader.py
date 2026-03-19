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
    df["timestamp_msk"] = df["timestamp"].dt.tz_convert("Europe/Moscow")
    return df


def validate_klines(df, interval_minutes, symbol="BTCUSDT", interval="15"):
    report = {
        "symbol": symbol,
        "interval": interval,
        "rows_before": len(df),
        "duplicates_removed": 0,
        "time_gaps_found": 0,
        "bad_ohlc_rows": 0,
        "negative_volume_rows": 0,
        "rows_after": 0
    }

    df = df.sort_values("timestamp").reset_index(drop=True)

    duplicate_mask = df.duplicated(subset=["timestamp"], keep="last")
    report["duplicates_removed"] = int(duplicate_mask.sum())
    if report["duplicates_removed"] > 0:
        df = df.loc[~duplicate_mask].copy()

    df = df.sort_values("timestamp").reset_index(drop=True)

    bad_ohlc_mask = (
        (df["high"] < df["open"]) |
        (df["high"] < df["close"]) |
        (df["low"] > df["open"]) |
        (df["low"] > df["close"]) |
        (df["high"] < df["low"])
    )
    report["bad_ohlc_rows"] = int(bad_ohlc_mask.sum())
    if report["bad_ohlc_rows"] > 0:
        df = df.loc[~bad_ohlc_mask].copy()

    negative_volume_mask = (df["volume"] < 0) | (df["turnover"] < 0)
    report["negative_volume_rows"] = int(negative_volume_mask.sum())
    if report["negative_volume_rows"] > 0:
        df = df.loc[~negative_volume_mask].copy()

    df = df.sort_values("timestamp").reset_index(drop=True)

    expected_delta = pd.Timedelta(minutes=interval_minutes)
    time_diff = df["timestamp"].diff()
    report["time_gaps_found"] = int(((time_diff.notna()) & (time_diff != expected_delta)).sum())

    report["rows_after"] = len(df)
    return df, report


def print_validation_report(report):
    print()
    print(f"=== Validation report: {report['symbol']} {report['interval']}m ===")
    print(f"Rows before:           {report['rows_before']}")
    print(f"Duplicates removed:    {report['duplicates_removed']}")
    print(f"Bad OHLC rows removed: {report['bad_ohlc_rows']}")
    print(f"Negative vol removed:  {report['negative_volume_rows']}")
    print(f"Time gaps found:       {report['time_gaps_found']}")
    print(f"Rows after:            {report['rows_after']}")


def save_data(df, filename):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / filename
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")


def download_and_save(symbol="BTCUSDT", interval="15", total=2000):
    raw_df = get_klines_full(symbol=symbol, interval=interval, total=total)
    clean_df, report = validate_klines(
        raw_df,
        interval_minutes=int(interval),
        symbol=symbol,
        interval=interval
    )

    filename = f"{symbol.lower()}_{interval}m.csv"

    print(f"\n=== {symbol} {interval}m tail ===")
    print(clean_df[["timestamp_msk", "open", "high", "low", "close"]].tail())
    print_validation_report(report)

    save_data(clean_df, filename)
    return clean_df, report


if __name__ == "__main__":
    download_and_save(symbol="BTCUSDT", interval="15", total=2000)
    download_and_save(symbol="BTCUSDT", interval="30", total=2000)
