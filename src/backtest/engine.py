import numpy as np
import pandas as pd


MAX_ABS_RET_15M = 0.30  # 30% за 15m для BTC — аварийный порог качества данных


def apply_position_logic(entry_signal, hold_bars, direction):
    if hold_bars <= 0:
        raise ValueError("hold_bars must be > 0")

    position = []
    pos = 0
    bars_left = 0
    side = 1 if direction == "long" else -1

    for raw_signal in entry_signal.fillna(0).astype(int):
        signal = 1 if raw_signal == 1 else 0

        if pos == 0 and signal == 1:
            pos = side
            bars_left = hold_bars
        elif pos != 0:
            bars_left -= 1
            if bars_left <= 0:
                pos = 0
                bars_left = 0

        position.append(pos)

    return pd.Series(position, index=entry_signal.index, dtype="int8")


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