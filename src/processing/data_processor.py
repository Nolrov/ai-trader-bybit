from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import AppSettings
from data.bybit_loader import assert_fresh_enough
from features.feature_factory import add_features


def process_frames(
    df_15: pd.DataFrame,
    df_30: pd.DataFrame,
    settings: AppSettings,
    *,
    enforce_freshness: bool = True,
) -> pd.DataFrame:
    if df_15.empty or df_30.empty:
        raise RuntimeError("input_dataframes_empty")

    if enforce_freshness:
        assert_fresh_enough(
            df_15,
            interval_minutes=int(settings.data.interval_main),
            multiplier=2,
        )
        assert_fresh_enough(
            df_30,
            interval_minutes=int(settings.data.interval_htf),
            multiplier=2,
        )

    df_15 = add_features(df_15.copy())
    df_30 = add_features(df_30.copy())

    df_15 = df_15.sort_values("timestamp").reset_index(drop=True)
    df_30 = df_30.sort_values("timestamp").reset_index(drop=True)

    df_15["timestamp_30m"] = df_15["timestamp"].dt.floor("30min")

    df_30 = df_30.rename(columns={"timestamp": "timestamp_30m"}).copy()

    # Сдвигаем только HTF-фичи, а не OHLCV-цены.
    base_ohlcv_cols = {
        "timestamp_30m",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
    }
    feature_cols = [c for c in df_30.columns if c not in base_ohlcv_cols]

    if feature_cols:
        df_30[feature_cols] = df_30[feature_cols].shift(1)

    df = df_15.merge(
        df_30,
        on="timestamp_30m",
        suffixes=("_15m", "_30m"),
        how="inner",
    )

    if df.empty:
        raise RuntimeError("processed_dataframe_is_empty_after_merge")

    df = df.dropna().reset_index(drop=True)

    if df.empty:
        raise RuntimeError("processed_dataframe_is_empty_after_dropna")

    return df.sort_values("timestamp").reset_index(drop=True)