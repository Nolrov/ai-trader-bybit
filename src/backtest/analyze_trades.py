from pathlib import Path
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent.parent
REPORTS_DIR = BASE_DIR / "reports"


def load_trade_log():
    path = REPORTS_DIR / "trade_log.csv"
    df = pd.read_csv(path, sep=";")
    df["entry_time"] = pd.to_datetime(df["entry_time"])
    df["exit_time"] = pd.to_datetime(df["exit_time"])
    return df


def analyze_trades(df):
    print("\n=== Trade Summary ===")
    print(f"Total trades: {len(df)}")

    if len(df) == 0:
        print("No trades found")
        return

    print("\nNet return by side:")
    print(df.groupby("side")["net_return_pct"].agg(["count", "mean", "sum"]))

    print("\nBars held:")
    print(df["bars_held"].describe())

    print("\nTop 10 worst trades:")
    print(
        df.sort_values("net_return_pct", ascending=True)[
            ["side", "entry_time", "exit_time", "bars_held", "entry_price", "exit_price", "net_return_pct"]
        ].head(10).to_string(index=False)
    )

    print("\nTop 10 best trades:")
    print(
        df.sort_values("net_return_pct", ascending=False)[
            ["side", "entry_time", "exit_time", "bars_held", "entry_price", "exit_price", "net_return_pct"]
        ].head(10).to_string(index=False)
    )


if __name__ == "__main__":
    df = load_trade_log()
    analyze_trades(df)
