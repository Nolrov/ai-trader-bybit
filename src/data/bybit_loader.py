from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
import requests


BASE_URL_MAINNET = "https://api.bybit.com"
BASE_URL_TESTNET = "https://api-testnet.bybit.com"
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
SRC_DIR = BASE_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from config.settings import AppSettings, load_settings  # noqa: E402


def resolve_market_base_url(settings: AppSettings | None = None) -> str:
    if settings is None:
        settings = load_settings()
    return BASE_URL_TESTNET if settings.execution.testnet else BASE_URL_MAINNET


def build_data_filename(symbol: str, interval: str) -> str:
    return f"{symbol.lower()}_{interval}m.csv"


def get_data_path(symbol: str, interval: str) -> Path:
    return DATA_DIR / build_data_filename(symbol=symbol, interval=interval)


def get_klines_full(
    symbol: str,
    interval: str,
    total: int,
    category: str,
    *,
    base_url: str,
) -> pd.DataFrame:
    if total <= 0:
        raise ValueError(f"total must be > 0, got {total}")

    all_data: list[list] = []
    end = None

    while len(all_data) < total:
        limit = min(200, total - len(all_data))

        params = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
        if end is not None:
            params["end"] = end

        response = requests.get(
            f"{base_url}/v5/market/kline",
            params=params,
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit API error: {data}")

        klines = data.get("result", {}).get("list", [])
        if not klines:
            break

        all_data.extend(klines)
        end = klines[-1][0]
        time.sleep(0.1)

    all_data = all_data[:total]

    if not all_data:
        raise RuntimeError(
            f"No kline data returned for symbol={symbol}, interval={interval}, category={category}"
        )

    df = pd.DataFrame(
        all_data,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
        ],
    )

    df = df.astype(
        {
            "timestamp": "int64",
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "volume": "float64",
            "turnover": "float64",
        }
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["timestamp_msk"] = df["timestamp"].dt.tz_convert("Europe/Moscow")

    return df


def validate_klines(
    df: pd.DataFrame,
    interval_minutes: int,
    symbol: str,
    interval: str,
) -> tuple[pd.DataFrame, dict]:
    report = {
        "symbol": symbol,
        "interval": interval,
        "rows_before": len(df),
        "duplicates_removed": 0,
        "time_gaps_found": 0,
        "bad_ohlc_rows": 0,
        "negative_volume_rows": 0,
        "rows_after": 0,
    }

    df = df.sort_values("timestamp").reset_index(drop=True)

    duplicate_mask = df.duplicated(subset=["timestamp"], keep="last")
    report["duplicates_removed"] = int(duplicate_mask.sum())
    if report["duplicates_removed"] > 0:
        df = df.loc[~duplicate_mask].copy()

    df = df.sort_values("timestamp").reset_index(drop=True)

    bad_ohlc_mask = (
        (df["high"] < df["open"])
        | (df["high"] < df["close"])
        | (df["low"] > df["open"])
        | (df["low"] > df["close"])
        | (df["high"] < df["low"])
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


def compute_freshness(df: pd.DataFrame) -> dict:
    if df.empty:
        raise RuntimeError("cannot compute freshness for empty dataframe")

    now_utc = pd.Timestamp.now(tz="UTC")
    last_open_utc = pd.to_datetime(df["timestamp"].max(), utc=True)
    age = now_utc - last_open_utc

    return {
        "now_utc": now_utc,
        "last_open_utc": last_open_utc,
        "age": age,
    }


def assert_fresh_enough(df: pd.DataFrame, interval_minutes: int, *, multiplier: int = 2) -> dict:
    freshness = compute_freshness(df)
    max_age = pd.Timedelta(minutes=interval_minutes * multiplier)

    if freshness["age"] > max_age:
        raise RuntimeError(
            "stale_market_data: "
            f"last_open={freshness['last_open_utc']} age={freshness['age']} max_allowed={max_age}"
        )

    freshness["max_allowed_age"] = max_age
    return freshness


def print_validation_report(report: dict) -> None:
    print()
    print(f"=== Validation report: {report['symbol']} {report['interval']}m ===")
    print(f"Rows before: {report['rows_before']}")
    print(f"Duplicates removed: {report['duplicates_removed']}")
    print(f"Bad OHLC rows removed: {report['bad_ohlc_rows']}")
    print(f"Negative vol removed: {report['negative_volume_rows']}")
    print(f"Time gaps found: {report['time_gaps_found']}")
    print(f"Rows after: {report['rows_after']}")


def print_freshness_report(df: pd.DataFrame, symbol: str, interval: str) -> None:
    if df.empty:
        print("No data returned.")
        return

    freshness = compute_freshness(df)

    print()
    print(f"=== Freshness report: {symbol} {interval}m ===")
    print(f"Now UTC           : {freshness['now_utc']}")
    print(f"Last candle open  : {freshness['last_open_utc']}")
    print(f"Last candle MSK   : {freshness['last_open_utc'].tz_convert('Europe/Moscow')}")
    print(f"Age               : {freshness['age']}")


def save_data(df: pd.DataFrame, output_path: Path) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")
    return output_path


def download_and_save(
    symbol: str,
    interval: str,
    total: int,
    category: str,
    *,
    settings: AppSettings | None = None,
):
    if settings is None:
        settings = load_settings()

    base_url = resolve_market_base_url(settings)

    raw_df = get_klines_full(
        symbol=symbol,
        interval=interval,
        total=total,
        category=category,
        base_url=base_url,
    )

    clean_df, report = validate_klines(
        raw_df,
        interval_minutes=int(interval),
        symbol=symbol,
        interval=interval,
    )

    print(f"\n=== {symbol} {interval}m tail ===")
    print(clean_df[["timestamp_msk", "open", "high", "low", "close"]].tail())

    print_validation_report(report)
    print_freshness_report(clean_df, symbol=symbol, interval=interval)
    assert_fresh_enough(clean_df, interval_minutes=int(interval), multiplier=2)

    output_path = get_data_path(symbol=symbol, interval=interval)
    save_data(clean_df, output_path)
    return clean_df, report, output_path


def refresh_project_market_data(settings: AppSettings | None = None):
    if settings is None:
        settings = load_settings()

    results = []

    results.append(
        download_and_save(
            symbol=settings.data.symbol,
            interval=settings.data.interval_main,
            total=settings.data.bars_15m,
            category=settings.data.category,
            settings=settings,
        )
    )

    results.append(
        download_and_save(
            symbol=settings.data.symbol,
            interval=settings.data.interval_htf,
            total=settings.data.bars_30m,
            category=settings.data.category,
            settings=settings,
        )
    )

    return results


def parse_args():
    parser = argparse.ArgumentParser(description="Download Bybit kline data to project data/ folder")
    parser.add_argument("--symbol", type=str, default=None)
    parser.add_argument("--category", type=str, default=None)
    parser.add_argument("--interval", type=str, default=None, help="Single interval to download, e.g. 15 or 30")
    parser.add_argument("--total", type=int, default=None, help="Bars to download for --interval")
    parser.add_argument(
        "--use-settings",
        action="store_true",
        help="Use settings.py as the source of truth for symbol, intervals and bar counts",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print(f"BASE_DIR: {BASE_DIR}")
    print(f"DATA_DIR: {DATA_DIR}")

    settings = load_settings()
    print(f"market_data_endpoint: {resolve_market_base_url(settings)}")
    print(f"execution_testnet: {settings.execution.testnet}")

    if args.use_settings or (args.interval is None and args.total is None):
        refresh_project_market_data(settings)
        return

    if args.interval is None or args.total is None:
        raise ValueError("For manual mode you must provide both --interval and --total")

    symbol = args.symbol or settings.data.symbol
    category = args.category or settings.data.category

    download_and_save(
        symbol=symbol,
        interval=args.interval,
        total=args.total,
        category=category,
        settings=settings,
    )


if __name__ == "__main__":
    main()
