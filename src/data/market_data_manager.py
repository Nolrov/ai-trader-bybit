from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import AppSettings, load_settings
from data.bybit_loader import (
    assert_fresh_enough,
    download_and_save,
    fetch_runtime_market_data,
    get_data_path,
)
from processing.data_processor import process_frames


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"market_data_file_not_found: {path}")

    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        raise RuntimeError(f"timestamp_column_missing_in: {path}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)


def _ensure_single_interval_current(
    *,
    settings: AppSettings,
    interval: str,
    total: int,
) -> None:
    path = get_data_path(settings.data.symbol, interval)

    try:
        df = _read_csv(path)
        assert_fresh_enough(df, interval_minutes=int(interval), multiplier=2)
        return
    except (FileNotFoundError, RuntimeError):
        pass

    download_and_save(
        symbol=settings.data.symbol,
        interval=interval,
        total=total,
        category=settings.data.category,
        settings=settings,
    )


def ensure_local_market_data_current(
    settings: AppSettings | None = None,
) -> None:
    if settings is None:
        settings = load_settings()

    _ensure_single_interval_current(
        settings=settings,
        interval=settings.data.interval_main,
        total=settings.data.bars_15m,
    )

    _ensure_single_interval_current(
        settings=settings,
        interval=settings.data.interval_htf,
        total=settings.data.bars_30m,
    )


def load_local_market_data(
    settings: AppSettings | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if settings is None:
        settings = load_settings()

    path_15 = get_data_path(settings.data.symbol, settings.data.interval_main)
    path_30 = get_data_path(settings.data.symbol, settings.data.interval_htf)

    df_15 = _read_csv(path_15)
    df_30 = _read_csv(path_30)

    return df_15, df_30


def get_processed_market_data(
    settings: AppSettings | None = None,
) -> pd.DataFrame:
    if settings is None:
        settings = load_settings()

    ensure_local_market_data_current(settings)

    df_15, df_30 = load_local_market_data(settings)

    return process_frames(
        df_15=df_15,
        df_30=df_30,
        settings=settings,
        enforce_freshness=True,
    )


def get_runtime_market_frames(
    settings: AppSettings | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if settings is None:
        settings = load_settings()

    df_15, df_30 = fetch_runtime_market_data(
        settings=settings,
        enforce_freshness=True,
        freshness_multiplier=2,
    )

    return df_15, df_30