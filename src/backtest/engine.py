import pandas as pd


def apply_position_logic(entry_signal, hold_bars, direction):
    position = []
    pos = 0
    bars_left = 0

    for signal in entry_signal:
        if pos == 0 and signal == 1:
            pos = 1 if direction == "long" else -1
            bars_left = hold_bars
        elif pos != 0:
            bars_left -= 1
            if bars_left <= 0:
                pos = 0
                bars_left = 0

        position.append(pos)

    return position


def run_backtest(df, fee=0.0006):
    df = df.copy()

    df["ret"] = df["close_15m"].pct_change().fillna(0.0)
    df["trade"] = df["position"].diff().abs().fillna(0)
    df["fee"] = df["trade"] * fee
    df["strategy_return"] = (
        df["position"].shift(1).fillna(0) * df["ret"] - df["fee"]
    )
    df["equity"] = (1 + df["strategy_return"]).cumprod()

    return df


def calculate_metrics(df):
    equity = df["equity"]
    returns = df["strategy_return"]

    total_return = (equity.iloc[-1] - 1) * 100

    sharpe = 0.0
    if returns.std() > 0:
        sharpe = (returns.mean() / returns.std()) * (96 ** 0.5)

    drawdown = equity / equity.cummax() - 1

    return {
        "total_return_pct": round(float(total_return), 4),
        "sharpe_approx": round(float(sharpe), 4),
        "max_drawdown_pct": round(float(drawdown.min() * 100), 4),
        "trades": int((df["trade"] > 0).sum()),
    }
