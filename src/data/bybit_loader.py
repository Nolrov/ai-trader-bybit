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
from data.candle_utils import inspect_last_candle_status, prepare_closed_analytics_frame  # noqa: E402


HTTP_TIMEOUT_SECONDS = 20
RATE_LIMIT_MAX_RETRIES = 8
RATE_LIMIT_BASE_SLEEP_SECONDS = 2.0
PAGE_SLEEP_SECONDS = 0.15


def resolve_market_base_url(settings: AppSettings | None = None) -> str:
    # Рыночные данные всегда берём с mainnet.
    return BASE_URL_MAINNET


def build_data_filename(symbol: str, interval: str) -> str:
    return f"{symbol.lower()}_{interval}m.csv"


def get_data_path(symbol: str, interval: str) -> Path:
    return DATA_DIR / build_data_filename(symbol=symbol, interval=interval)


def _request_json_with_retry(
    *,
    url: str,
    params: dict,
    timeout: int = HTTP_TIMEOUT_SECONDS,
) -> dict:
    last_error: Exception | None = None

    for attempt in range(RATE_LIMIT_MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            if data.get("retCode") == 0:
                return data

            if data.get("retCode") == 10006:
                sleep_s = RATE_LIMIT_BASE_SLEEP_SECONDS * (2 ** attempt)
                print(
                    f"Bybit rate limit hit (attempt {attempt + 1}/{RATE_LIMIT_MAX_RETRIES}), "
                    f"sleeping {sleep_s:.1f}s..."
                )
                time.sleep(sleep_s)
                continue

            raise RuntimeError(f"Bybit API error: {data}")

        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            sleep_s = RATE_LIMIT_BASE_SLEEP_SECONDS * (2 ** attempt)
            print(
                f"Request failed (attempt {attempt + 1}/{RATE_LIMIT_MAX_RETRIES}), "
                f"sleeping {sleep_s:.1f}s... error={exc}"
            )
            time.sleep(sleep_s)

    if last_error is not None:
        raise RuntimeError(f"Bybit request failed after retries: {last_error}")

    raise RuntimeError("Bybit request failed after retries due to repeated rate limiting")


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

        data = _request_json_with_retry(
            url=f"{base_url}/v5/market/kline",
            params=params,
            timeout=HTTP_TIMEOUT_SECONDS,
        )

        klines = data.get("result", {}).get("list", [])
        if not klines:
            break

        all_data.extend(klines)

        oldest_ts = int(klines[-1][0])
        end = oldest_ts - 1

        time.sleep(PAGE_SLEEP_SECONDS)

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


def print_freshness_report(raw_df: pd.DataFrame, analytics_df: pd.DataFrame, symbol: str, interval: str, closed_status: dict) -> None:
    if raw_df.empty:
        print("No data returned.")
        return

    freshness = compute_freshness(raw_df)
    raw_status = inspect_last_candle_status(raw_df, interval=interval, now_utc=freshness["now_utc"])
    closed_last_open = pd.to_datetime(analytics_df["timestamp"].max(), utc=True) if not analytics_df.empty else None

    print()
    print(f"=== Freshness report: {symbol} {interval}m ===")
    print(f"Now UTC           : {freshness['now_utc']}")
    print(f"Raw last open     : {raw_status['last_open_utc']}")
    print(f"Raw last open MSK : {raw_status['last_open_utc'].tz_convert('Europe/Moscow')}")
    print(f"Age               : {freshness['age']}")
    print(f"Expected close    : {raw_status['expected_close_utc']}")
    print(f"Raw last closed   : {raw_status['is_last_bar_closed']}")
    print(f"Closed last open  : {closed_last_open}")
    print(f"Dropped open bars : {closed_status['bars_dropped_as_incomplete']}")


def save_data(df: pd.DataFrame, output_path: Path) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")
    return output_path


def fetch_klines_prepared(
    symbol: str,
    interval: str,
    total: int,
    category: str,
    *,
    settings: AppSettings | None = None,
    freshness_multiplier: int = 2,
    enforce_freshness: bool = True,
) -> tuple[pd.DataFrame, dict, dict]:
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

    freshness = compute_freshness(clean_df)
    if enforce_freshness:
        freshness = assert_fresh_enough(
            clean_df,
            interval_minutes=int(interval),
            multiplier=freshness_multiplier,
        )

    return clean_df, report, freshness


def fetch_runtime_market_data(
    settings: AppSettings | None = None,
    *,
    enforce_freshness: bool = True,
    freshness_multiplier: int = 2,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if settings is None:
        settings = load_settings()

    df_15, _, _ = fetch_klines_prepared(
        symbol=settings.data.symbol,
        interval=settings.data.interval_main,
        total=settings.data.bars_15m,
        category=settings.data.category,
        settings=settings,
        freshness_multiplier=freshness_multiplier,
        enforce_freshness=enforce_freshness,
    )

    df_30, _, _ = fetch_klines_prepared(
        symbol=settings.data.symbol,
        interval=settings.data.interval_htf,
        total=settings.data.bars_30m,
        category=settings.data.category,
        settings=settings,
        freshness_multiplier=freshness_multiplier,
        enforce_freshness=enforce_freshness,
    )

    return df_15, df_30


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

    raw_df, report, _ = fetch_klines_prepared(
        symbol=symbol,
        interval=interval,
        total=total,
        category=category,
        settings=settings,
        freshness_multiplier=2,
        enforce_freshness=True,
    )

    analytics_df, closed_status = prepare_closed_analytics_frame(raw_df, interval=interval)

    print(f"\n=== {symbol} {interval}m tail (closed only) ===")
    print(analytics_df[["timestamp_msk", "open", "high", "low", "close"]].tail())

    print_validation_report(report)
    print_freshness_report(raw_df, analytics_df, symbol=symbol, interval=interval, closed_status=closed_status)

    output_path = get_data_path(symbol=symbol, interval=interval)
    save_data(analytics_df, output_path)
    return analytics_df, report, output_path


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