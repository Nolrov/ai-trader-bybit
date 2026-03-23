import numpy as np
import pandas as pd


MAX_ABS_RET_15M = 0.30  # 30% за 15m для BTC — аварийный порог качества данных


def apply_position_logic(
    entry_signal,
    hold_bars=None,
    direction="long",
    exit_signal=None,
    min_hold_bars=1,
    max_hold_bars=None,
):
    if hold_bars is None and max_hold_bars is None:
        raise ValueError("hold_bars or max_hold_bars must be provided")

    if max_hold_bars is None:
        max_hold_bars = hold_bars
    if hold_bars is None:
        hold_bars = max_hold_bars

    max_hold_bars = int(max_hold_bars)
    min_hold_bars = max(1, int(min_hold_bars))

    if max_hold_bars <= 0:
        raise ValueError("max_hold_bars must be > 0")
    if min_hold_bars > max_hold_bars:
        raise ValueError("min_hold_bars must be <= max_hold_bars")

    entry_series = entry_signal.fillna(0).astype(int)
    if exit_signal is None:
        exit_series = pd.Series(0, index=entry_series.index, dtype="int8")
    else:
        exit_series = exit_signal.fillna(0).astype(int)

    position = []
    pos = 0
    bars_in_trade = 0
    side = 1 if str(direction).lower() == "long" else -1

    for raw_entry, raw_exit in zip(entry_series, exit_series, strict=False):
        entry = 1 if raw_entry == 1 else 0
        exit_now = 1 if raw_exit == 1 else 0

        if pos == 0:
            if entry == 1:
                pos = side
                bars_in_trade = 1
        else:
            allow_exit = bars_in_trade >= min_hold_bars
            must_exit = bars_in_trade >= max_hold_bars
            if must_exit or (allow_exit and exit_now == 1):
                pos = 0
                bars_in_trade = 0
            else:
                bars_in_trade += 1

        position.append(pos)

    return pd.Series(position, index=entry_series.index, dtype="int8")


def _resolve_time_col(df: pd.DataFrame) -> str | None:
    for col in ("timestamp_15m", "timestamp", "open_time", "datetime"):
        if col in df.columns:
            return col
    return None


def _prepare_backtest_frame(df: pd.DataFrame) -> pd.DataFrame:
    if "close_15m" not in df.columns:
        raise ValueError("run_backtest: required column close_15m is missing")
    if "position" not in df.columns:
        raise ValueError("run_backtest: required column position is missing")

    out = df.copy()

    time_col = _resolve_time_col(out)
    if time_col is not None:
        out = out.sort_values(time_col).reset_index(drop=True)

    out["close_15m"] = pd.to_numeric(out["close_15m"], errors="coerce")
    out["position"] = pd.to_numeric(out["position"], errors="coerce").fillna(0)

    bad_close = out["close_15m"].isna() | (out["close_15m"] <= 0)
    if bad_close.any():
        raise ValueError(f"run_backtest: invalid close_15m rows={int(bad_close.sum())}")

    out["position"] = out["position"].clip(-1, 1).astype("int8")

    if time_col is not None:
        dupes = out.duplicated(subset=[time_col]).sum()
        if dupes > 0:
            raise ValueError(f"run_backtest: duplicate timestamps detected: {int(dupes)}")

    return out


def run_backtest(df, fee=0.0006):
    out = _prepare_backtest_frame(df)

    out["ret"] = out["close_15m"].pct_change()
    out["ret"] = out["ret"].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    extreme_mask = out["ret"].abs() > MAX_ABS_RET_15M
    if extreme_mask.any():
        sample = out.loc[extreme_mask, ["close_15m", "ret"]].head(5).to_dict("records")
        raise ValueError(
            f"run_backtest: extreme 15m returns detected count={int(extreme_mask.sum())}, sample={sample}"
        )

    out["trade"] = out["position"].ne(out["position"].shift(1).fillna(0)).astype(int)
    out["fee"] = out["trade"] * fee

    prev_position = out["position"].shift(1).fillna(0).astype("int8")
    out["strategy_return"] = prev_position * out["ret"] - out["fee"]

    invalid_sr = (~np.isfinite(out["strategy_return"])) | (out["strategy_return"] <= -1.0)
    if invalid_sr.any():
        raise ValueError(
            f"run_backtest: invalid strategy_return rows={int(invalid_sr.sum())}"
        )

    out["equity"] = (1.0 + out["strategy_return"]).cumprod()

    if (out["equity"] <= 0).any():
        raise ValueError("run_backtest: non-positive equity detected")

    return out


def calculate_metrics(df):
    if df.empty:
        return {
            "total_return_pct": 0.0,
            "sharpe_approx": 0.0,
            "max_drawdown_pct": 0.0,
            "trades": 0,
        }

    equity = pd.to_numeric(df["equity"], errors="coerce")
    returns = pd.to_numeric(df["strategy_return"], errors="coerce").fillna(0.0)

    if equity.isna().any() or (~np.isfinite(equity)).any():
        raise ValueError("calculate_metrics: invalid equity series")

    total_return = (float(equity.iloc[-1]) - 1.0) * 100.0

    if abs(total_return) > 10000:
        raise ValueError(f"calculate_metrics: absurd total_return_pct={total_return:.4f}")

    sharpe = 0.0
    ret_std = float(returns.std())
    if ret_std > 0:
        sharpe = (float(returns.mean()) / ret_std) * (96 ** 0.5)

    drawdown = equity / equity.cummax() - 1.0
    max_dd = float(drawdown.min() * 100.0)

    trades = int(df["trade"].sum())

    return {
        "total_return_pct": round(total_return, 4),
        "sharpe_approx": round(float(sharpe), 4),
        "max_drawdown_pct": round(max_dd, 4),
        "trades": trades,
    }
