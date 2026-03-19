from pathlib import Path
import logging

import pandas as pd
from data_processor import process


BASE_DIR = Path(__file__).resolve().parent.parent
REPORTS_DIR = BASE_DIR / "reports"
LOGS_DIR = BASE_DIR / "logs"


def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOGS_DIR / "strategy.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    logging.info("Strategy run started")
    return log_path


def generate_signals(df):
    df = df.copy()

    df["signal"] = 0
    df["position"] = 0

    vol_threshold = df["volatility_15m"].rolling(50).median()
    in_position = 0

    for i in range(1, len(df)):
        row = df.iloc[i]
        enough_vol = pd.notna(vol_threshold.iloc[i]) and row["volatility_15m"] > vol_threshold.iloc[i]

        if in_position == 0:
            long_entry = (
                row["ema_fast_15m"] > row["ema_slow_15m"] and
                row["ema_fast_30m"] > row["ema_slow_30m"] and
                row["rsi_15m"] < 65 and
                enough_vol
            )

            if long_entry:
                in_position = 1
                df.at[df.index[i], "signal"] = 1

        elif in_position == 1:
            long_exit = (
                row["rsi_15m"] > 55 or
                row["ema_fast_15m"] < row["ema_slow_15m"]
            )

            if long_exit:
                in_position = 0
                df.at[df.index[i], "signal"] = 0

        df.at[df.index[i], "position"] = in_position

    return df


def backtest(df, fee_per_trade=0.0006):
    df = df.copy()

    df["return"] = df["close_15m"].pct_change().fillna(0.0)
    df["trade"] = df["position"].diff().abs().fillna(0)
    df["fee"] = df["trade"] * fee_per_trade

    df["strategy_return"] = (df["position"].shift(1).fillna(0) * df["return"]) - df["fee"]
    df["equity"] = (1 + df["strategy_return"]).cumprod()

    return df


def calculate_metrics(df):
    equity = df["equity"]
    strategy_returns = df["strategy_return"]

    total_return = equity.iloc[-1] - 1
    avg_return = strategy_returns.mean()
    std_return = strategy_returns.std()

    sharpe = 0.0
    if std_return and std_return > 0:
        sharpe = (avg_return / std_return) * (96 ** 0.5)

    rolling_max = equity.cummax()
    drawdown = equity / rolling_max - 1
    max_drawdown = drawdown.min()

    trades = int((df["trade"] > 0).sum())
    long_signals = int((df["signal"] == 1).sum())
    flat_signals = int((df["signal"] == 0).sum())

    metrics = {
        "total_return_pct": round(total_return * 100, 2),
        "sharpe_approx": round(sharpe, 3),
        "max_drawdown_pct": round(max_drawdown * 100, 2),
        "trades": trades,
        "long_signals": long_signals,
        "flat_signals": flat_signals,
    }

    return metrics


def build_trade_log(df, fee_per_trade=0.0006):
    trades = []
    current_trade = None

    for row in df.itertuples(index=False):
        current_position = int(row.position)
        current_time = row.timestamp
        current_price = float(row.close_15m)

        if current_trade is None and current_position == 1:
            current_trade = {
                "side": "LONG",
                "entry_time": current_time,
                "entry_price": current_price,
                "entry_reason": "signal_change",
                "bars_held": 0,
                "fee_paid": fee_per_trade,
            }
            continue

        if current_trade is not None:
            current_trade["bars_held"] += 1

            if current_position == 0:
                gross_return = (current_price / current_trade["entry_price"]) - 1
                net_return = gross_return - current_trade["fee_paid"] - fee_per_trade

                trade_row = {
                    "side": current_trade["side"],
                    "entry_time": current_trade["entry_time"],
                    "exit_time": current_time,
                    "entry_price": round(current_trade["entry_price"], 6),
                    "exit_price": round(current_price, 6),
                    "bars_held": current_trade["bars_held"],
                    "gross_return_pct": round(gross_return * 100, 4),
                    "net_return_pct": round(net_return * 100, 4),
                    "entry_reason": current_trade["entry_reason"],
                    "exit_reason": "flat",
                }
                trades.append(trade_row)
                current_trade = None

    if current_trade is not None:
        last_row = df.iloc[-1]
        final_price = float(last_row["close_15m"])
        gross_return = (final_price / current_trade["entry_price"]) - 1
        net_return = gross_return - current_trade["fee_paid"] - fee_per_trade

        trade_row = {
            "side": current_trade["side"],
            "entry_time": current_trade["entry_time"],
            "exit_time": last_row["timestamp"],
            "entry_price": round(current_trade["entry_price"], 6),
            "exit_price": round(final_price, 6),
            "bars_held": current_trade["bars_held"],
            "gross_return_pct": round(gross_return * 100, 4),
            "net_return_pct": round(net_return * 100, 4),
            "entry_reason": current_trade["entry_reason"],
            "exit_reason": "end_of_backtest",
        }
        trades.append(trade_row)

    return pd.DataFrame(trades)


def save_backtest_report(df):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / "strategy_backtest.csv"
    df.to_csv(output_path, index=False, sep=";")
    logging.info(f"Saved backtest report: {output_path}")
    return output_path


def save_trade_log(trade_log):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / "trade_log.csv"
    trade_log.to_csv(output_path, index=False, sep=";")
    logging.info(f"Saved trade log: {output_path}")
    return output_path


def save_summary(metrics):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / "backtest_summary.csv"
    pd.DataFrame([metrics]).to_csv(output_path, index=False, sep=";")
    logging.info(f"Saved summary: {output_path}")
    return output_path


if __name__ == "__main__":
    log_path = setup_logging()

    df = process()
    logging.info(f"Processed dataset rows: {len(df)}")

    df = generate_signals(df)
    df = backtest(df)

    metrics = calculate_metrics(df)
    trade_log = build_trade_log(df)

    backtest_path = save_backtest_report(df)
    trade_log_path = save_trade_log(trade_log)
    summary_path = save_summary(metrics)

    print()
    print("Backtest metrics:")
    for key, value in metrics.items():
        print(f"{key}: {value}")

    print()
    print("Last 10 trades:")
    if len(trade_log) > 0:
        print(trade_log.tail(10).to_string(index=False))
    else:
        print("No trades found")

    print()
    print(f"Backtest report: {backtest_path}")
    print(f"Trade log: {trade_log_path}")
    print(f"Summary: {summary_path}")
    print(f"Log file: {log_path}")
