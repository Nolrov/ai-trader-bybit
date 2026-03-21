from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import AppSettings, load_settings
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
        assert_fresh_enough(df_15, interval_minutes=int(settings.data.interval_main), multiplier=2)
        assert_fresh_enough(df_30, interval_minutes=int(settings.data.interval_htf), multiplier=2)

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


# --- BACKWARD COMPAT (можно удалить позже) ---

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"market_data_file_not_found: {path}")

    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        raise RuntimeError(f"timestamp_column_missing_in: {path}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)


def process(
    settings: AppSettings | None = None,
    *,
    enforce_freshness: bool = True,
) -> pd.DataFrame:
    """
    DEPRECATED PATH — только для совместимости.
    Runtime должен использовать process_frames().
    """

    if settings is None:
        settings = load_settings()

    from data.bybit_loader import get_data_path

    path_15 = get_data_path(settings.data.symbol, settings.data.interval_main)
    path_30 = get_data_path(settings.data.symbol, settings.data.interval_htf)

    df_15 = _read_csv(path_15)
    df_30 = _read_csv(path_30)

    return process_frames(
        df_15=df_15,
        df_30=df_30,
        settings=settings,
        enforce_freshness=enforce_freshness,
    )


if __name__ == "__main__":
    settings = load_settings()
    df = process(settings=settings, enforce_freshness=True)

    print(df.tail())
    print(f"\nRows: {len(df)}")
