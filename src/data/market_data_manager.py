from __future__ import annotations

from pathlib import Path
import json
import os
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import AppSettings, REPORTS_DIR, load_settings
from data.bybit_loader import (
    assert_fresh_enough,
    compute_freshness,
    download_and_save,
    fetch_runtime_market_data,
    get_data_path,
)
from data.candle_utils import inspect_last_candle_status, prepare_closed_analytics_frame
from processing.data_processor import process_frames


MARKET_DATA_STATUS_PATH = REPORTS_DIR / "market_data_status.json"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"market_data_file_not_found: {path}")

    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        raise RuntimeError(f"timestamp_column_missing_in: {path}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if "timestamp_msk" in df.columns:
        df["timestamp_msk"] = pd.to_datetime(df["timestamp_msk"], utc=True, errors="coerce")
    return df.sort_values("timestamp").reset_index(drop=True)


def _skip_data_refresh() -> bool:
    return str(os.getenv("AI_TRADER_SKIP_DATA_REFRESH", "0")).lower() in {"1", "true", "yes"}


def _freshness_payload(df: pd.DataFrame, interval: str) -> dict:
    freshness = compute_freshness(df)
    raw_status = inspect_last_candle_status(df, interval=interval, now_utc=freshness["now_utc"])
    analytics_df, closed_status = prepare_closed_analytics_frame(df, interval=interval, now_utc=freshness["now_utc"])
    return {
        "interval": str(interval),
        "raw_rows": int(len(df)),
        "analytics_rows": int(len(analytics_df)),
        "raw_last_open_utc": str(freshness["last_open_utc"]),
        "analytics_last_open_utc": str(pd.to_datetime(analytics_df["timestamp"].max(), utc=True)),
        "age_seconds": round(float(freshness["age"].total_seconds()), 2),
        "expected_close_utc": str(raw_status["expected_close_utc"]),
        "is_last_bar_closed": bool(raw_status["is_last_bar_closed"]),
        "bars_dropped_as_incomplete": int(closed_status["bars_dropped_as_incomplete"]),
    }


def _write_market_data_status(status: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    MARKET_DATA_STATUS_PATH.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")


def _ensure_single_interval_current(
    *,
    settings: AppSettings,
    interval: str,
    total: int,
) -> dict:
    path = get_data_path(settings.data.symbol, interval)
    skip_refresh = _skip_data_refresh()
    refresh_before_run = bool(getattr(settings.data, "refresh_before_run", True)) and not skip_refresh
    allow_stale_fallback = bool(getattr(settings.data, "allow_stale_fallback", True))

    status: dict = {
        "symbol": settings.data.symbol,
        "interval": str(interval),
        "path": str(path),
        "total_requested": int(total),
        "refresh_requested": bool(refresh_before_run),
        "skip_refresh_flag": bool(skip_refresh),
        "source": "unknown",
    }

    if refresh_before_run:
        try:
            df, _, _ = download_and_save(
                symbol=settings.data.symbol,
                interval=interval,
                total=total,
                category=settings.data.category,
                settings=settings,
            )
            status["source"] = "exchange_refresh"
            status["freshness"] = _freshness_payload(df, interval)
            _, closed_status = prepare_closed_analytics_frame(df, interval)
            status["closed_candle_filter"] = closed_status
            status["ok"] = True
            return status
        except Exception as exc:
            status["refresh_error"] = str(exc)
            if not allow_stale_fallback:
                raise RuntimeError(f"market_data_refresh_failed_no_fallback:{interval}:{exc}") from exc

    try:
        df = _read_csv(path)
        assert_fresh_enough(df, interval_minutes=int(interval), multiplier=2)
        status["source"] = "local_fallback"
        status["freshness"] = _freshness_payload(df, interval)
        _, closed_status = prepare_closed_analytics_frame(df, interval)
        status["closed_candle_filter"] = closed_status
        status["ok"] = True
        return status
    except (FileNotFoundError, RuntimeError) as exc:
        status["fallback_error"] = str(exc)

    try:
        df, _, _ = download_and_save(
            symbol=settings.data.symbol,
            interval=interval,
            total=total,
            category=settings.data.category,
            settings=settings,
        )
        status["source"] = "exchange_recovery"
        status["freshness"] = _freshness_payload(df, interval)
        _, closed_status = prepare_closed_analytics_frame(df, interval)
        status["closed_candle_filter"] = closed_status
        status["ok"] = True
        return status
    except Exception as exc:
        status["recovery_error"] = str(exc)
        raise RuntimeError(f"market_data_unavailable:{interval}:{exc}") from exc


def ensure_local_market_data_current(
    settings: AppSettings | None = None,
) -> dict:
    if settings is None:
        settings = load_settings()

    status = {
        "symbol": settings.data.symbol,
        "refresh_before_run": bool(getattr(settings.data, "refresh_before_run", True)),
        "skip_refresh_flag": bool(_skip_data_refresh()),
        "allow_stale_fallback": bool(getattr(settings.data, "allow_stale_fallback", True)),
        "closed_candles_only_for_analytics": True,
        "intervals": {},
    }

    status["intervals"][settings.data.interval_main] = _ensure_single_interval_current(
        settings=settings,
        interval=settings.data.interval_main,
        total=settings.data.bars_15m,
    )

    status["intervals"][settings.data.interval_htf] = _ensure_single_interval_current(
        settings=settings,
        interval=settings.data.interval_htf,
        total=settings.data.bars_30m,
    )

    _write_market_data_status(status)
    return status


def load_local_market_data(
    settings: AppSettings | None = None,
    *,
    closed_only: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if settings is None:
        settings = load_settings()

    path_15 = get_data_path(settings.data.symbol, settings.data.interval_main)
    path_30 = get_data_path(settings.data.symbol, settings.data.interval_htf)

    df_15 = _read_csv(path_15)
    df_30 = _read_csv(path_30)

    if closed_only:
        df_15, _ = prepare_closed_analytics_frame(df_15, settings.data.interval_main)
        df_30, _ = prepare_closed_analytics_frame(df_30, settings.data.interval_htf)

    return df_15, df_30


def get_processed_market_data(
    settings: AppSettings | None = None,
    *,
    closed_only: bool = True,
) -> pd.DataFrame:
    if settings is None:
        settings = load_settings()

    ensure_local_market_data_current(settings)

    df_15, df_30 = load_local_market_data(settings, closed_only=closed_only)

    return process_frames(
        df_15=df_15,
        df_30=df_30,
        settings=settings,
        enforce_freshness=True,
    )


def get_runtime_market_frames(
    settings: AppSettings | None = None,
    *,
    enforce_freshness: bool = True,
    freshness_multiplier: int = 2,
    closed_only: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if settings is None:
        settings = load_settings()

    df_15, df_30 = fetch_runtime_market_data(
        settings=settings,
        enforce_freshness=enforce_freshness,
        freshness_multiplier=freshness_multiplier,
    )

    if closed_only:
        df_15, _ = prepare_closed_analytics_frame(df_15, settings.data.interval_main)
        df_30, _ = prepare_closed_analytics_frame(df_30, settings.data.interval_htf)

    return df_15, df_30
