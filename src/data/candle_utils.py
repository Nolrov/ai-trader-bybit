from __future__ import annotations

import pandas as pd


def timeframe_to_timedelta(interval: str | int) -> pd.Timedelta:
    return pd.Timedelta(minutes=int(interval))


def inspect_last_candle_status(
    df: pd.DataFrame,
    interval: str | int,
    now_utc: pd.Timestamp | None = None,
) -> dict:
    if df.empty:
        raise RuntimeError("cannot_inspect_last_candle_for_empty_dataframe")

    if now_utc is None:
        now_utc = pd.Timestamp.now(tz="UTC")
    else:
        now_utc = pd.to_datetime(now_utc, utc=True)

    last_open_utc = pd.to_datetime(df["timestamp"].max(), utc=True)
    candle_delta = timeframe_to_timedelta(interval)
    expected_close_utc = last_open_utc + candle_delta
    is_closed = bool(now_utc >= expected_close_utc)

    return {
        "last_open_utc": last_open_utc,
        "expected_close_utc": expected_close_utc,
        "now_utc": now_utc,
        "is_last_bar_closed": is_closed,
        "bars_dropped_as_incomplete": 0 if is_closed else 1,
    }


def filter_to_closed_candles(
    df: pd.DataFrame,
    interval: str | int,
    now_utc: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, dict]:
    if df.empty:
        now = pd.Timestamp.now(tz="UTC") if now_utc is None else pd.to_datetime(now_utc, utc=True)
        return df.copy(), {
            "last_open_utc": None,
            "expected_close_utc": None,
            "now_utc": str(now),
            "is_last_bar_closed": False,
            "bars_dropped_as_incomplete": 0,
            "rows_before": 0,
            "rows_after": 0,
            "last_closed_open_utc": None,
            "last_closed_open_msk": None,
        }

    status = inspect_last_candle_status(df, interval=interval, now_utc=now_utc)
    rows_before = int(len(df))
    filtered = df.copy()
    if not status["is_last_bar_closed"]:
        filtered = filtered.iloc[:-1].copy()

    if filtered.empty:
        raise RuntimeError(f"no_closed_candles_after_filter: interval={interval}")

    filtered = filtered.sort_values("timestamp").reset_index(drop=True)
    last_closed_open_utc = pd.to_datetime(filtered["timestamp"].max(), utc=True)
    status["rows_before"] = rows_before
    status["rows_after"] = int(len(filtered))
    status["last_closed_open_utc"] = str(last_closed_open_utc)
    status["last_closed_open_msk"] = str(last_closed_open_utc.tz_convert("Europe/Moscow"))
    status["last_open_utc"] = str(status["last_open_utc"])
    status["expected_close_utc"] = str(status["expected_close_utc"])
    status["now_utc"] = str(status["now_utc"])
    return filtered, status
