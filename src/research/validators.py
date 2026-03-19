import pandas as pd


def calculate_backtest_metrics(df):
    equity = df["equity"]
    strategy_returns = df["strategy_return"]

    total_return = float(equity.iloc[-1] - 1)
    avg_return = float(strategy_returns.mean())
    std_return = float(strategy_returns.std())

    sharpe = 0.0
    if std_return > 0:
        sharpe = (avg_return / std_return) * (96 ** 0.5)

    rolling_max = equity.cummax()
    drawdown = equity / rolling_max - 1
    max_drawdown = float(drawdown.min())

    trades = int((df["trade"] > 0).sum())

    return {
        "total_return_pct": round(total_return * 100, 4),
        "sharpe_approx": round(sharpe, 4),
        "max_drawdown_pct": round(max_drawdown * 100, 4),
        "trades": trades,
    }


def is_candidate_valid(metrics, min_trades=20, min_total_return=-100, max_drawdown_limit=-30):
    if metrics["trades"] < min_trades:
        return False

    if metrics["total_return_pct"] < min_total_return:
        return False

    if metrics["max_drawdown_pct"] < max_drawdown_limit:
        return False

    return True
