from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import AppSettings, load_settings
from data.bybit_loader import assert_fresh_enough, get_data_path
from features.feature_factory import add_features


BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"market_data_file_not_found: {path}")

    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        raise RuntimeError(f"timestamp_column_missing_in: {path}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)


def load_data(settings: AppSettings | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if settings is None:
        settings = load_settings()

    path_15 = get_data_path(settings.data.symbol, settings.data.interval_main)
    path_30 = get_data_path(settings.data.symbol, settings.data.interval_htf)

    df_15 = _read_csv(path_15)
    df_30 = _read_csv(path_30)

    return df_15, df_30


def validate_loaded_data_freshness(settings: AppSettings, df_15: pd.DataFrame, df_30: pd.DataFrame) -> None:
    assert_fresh_enough(df_15, interval_minutes=int(settings.data.interval_main), multiplier=2)
    assert_fresh_enough(df_30, interval_minutes=int(settings.data.interval_htf), multiplier=2)


def process(
    settings: AppSettings | None = None,
    *,
    enforce_freshness: bool = True,
) -> pd.DataFrame:
    if settings is None:
        settings = load_settings()

    df_15, df_30 = load_data(settings)

    if enforce_freshness:
        validate_loaded_data_freshness(settings, df_15, df_30)

    df_15 = add_features(df_15.copy())
    df_30 = add_features(df_30.copy())

    df_15["timestamp_30m"] = df_15["timestamp"].dt.floor("30min")

    df_30 = df_30.rename(columns={"timestamp": "timestamp_30m"})
    df_30 = df_30.sort_values("timestamp_30m").copy()

    cols_to_shift = [c for c in df_30.columns if c != "timestamp_30m"]
    df_30[cols_to_shift] = df_30[cols_to_shift].shift(1)

    df = df_15.merge(
        df_30,
        on="timestamp_30m",
        suffixes=("_15m", "_30m"),
        how="inner",
    )

    if df.empty:
        raise RuntimeError("processed_dataframe_is_empty_after_merge")

    return df.sort_values("timestamp").reset_index(drop=True)


if __name__ == "__main__":
    app_settings = load_settings()
    df = process(settings=app_settings, enforce_freshness=True)

    print(f"symbol={app_settings.data.symbol}")
    print(f"interval_main={app_settings.data.interval_main}")
    print(f"interval_htf={app_settings.data.interval_htf}")
    print(df.tail())
    print(f"\nRows: {len(df)}")
