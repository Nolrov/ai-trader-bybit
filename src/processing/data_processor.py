from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import AppSettings, load_settings
from data.bybit_loader import (
    assert_fresh_enough,
    download_and_save,
    get_data_path,
)
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

    # Сдвигаем только фичи старшего ТФ, чтобы не было lookahead bias.
    # OHLCV 30m не сдвигаем — иначе ломается ценовой ряд после merge.
    base_ohlcv_cols = {"timestamp_30m", "open", "high", "low", "close", "volume", "turnover"}
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


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"market_data_file_not_found: {path}")

    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        raise RuntimeError(f"timestamp_column_missing_in: {path}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)


def _refresh_interval_csv(
    *,
    settings: AppSettings,
    interval: str,
    total: int,
) -> None:
    download_and_save(
        symbol=settings.data.symbol,
        interval=interval,
        total=total,
        category=settings.data.category,
        settings=settings,
    )


def _load_or_refresh_interval(
    *,
    settings: AppSettings,
    interval: str,
    total: int,
    enforce_freshness: bool,
) -> pd.DataFrame:
    path = get_data_path(settings.data.symbol, interval)

    need_refresh = False

    try:
        df = _read_csv(path)

        if enforce_freshness:
            assert_fresh_enough(
                df,
                interval_minutes=int(interval),
                multiplier=2,
            )

        return df

    except (FileNotFoundError, RuntimeError):
        need_refresh = True

    if need_refresh:
        _refresh_interval_csv(
            settings=settings,
            interval=interval,
            total=total,
        )

    df = _read_csv(path)

    if enforce_freshness:
        assert_fresh_enough(
            df,
            interval_minutes=int(interval),
            multiplier=2,
        )

    return df


def process(
    settings: AppSettings | None = None,
    *,
    enforce_freshness: bool = True,
) -> pd.DataFrame:
    """
    Compatibility path for research/backtest.

    Поведение постоянное:
    - если локальных CSV нет -> скачать
    - если CSV устарели -> скачать
    - затем обработать уже актуальные данные
    """

    if settings is None:
        settings = load_settings()

    df_15 = _load_or_refresh_interval(
        settings=settings,
        interval=settings.data.interval_main,
        total=settings.data.bars_15m,
        enforce_freshness=enforce_freshness,
    )

    df_30 = _load_or_refresh_interval(
        settings=settings,
        interval=settings.data.interval_htf,
        total=settings.data.bars_30m,
        enforce_freshness=enforce_freshness,
    )

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