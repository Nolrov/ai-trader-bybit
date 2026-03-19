# -*- coding: utf-8 -*-
import sys
import argparse
import json
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from data.bybit_loader import download_and_save
from processing.data_processor import process
from research.rule_builder import build_rule_candidates
from research.alpha_miner import prepare_pa_features, apply_candidate, split_df
from backtest.engine import run_backtest, calculate_metrics

BASE_DIR = Path(__file__).resolve().parent.parent.parent
REPORTS_DIR = BASE_DIR / "reports"


def parse_args():
    parser = argparse.ArgumentParser(description="Run one candidate from alpha miner candidate list")
    parser.add_argument(
        "--candidate-id",
        type=int,
        required=True,
        help="Candidate ID from alpha_miner enumeration (starts from 1)"
    )
    parser.add_argument(
        "--save-report",
        action="store_true",
        help="Save one-candidate JSON report to reports/"
    )
    parser.add_argument(
        "--refresh-data",
        action="store_true",
        help="Refresh BTCUSDT 15m/30m candles from Bybit before processing"
    )
    return parser.parse_args()


def refresh_market_data():
    print("Refreshing market data from Bybit...")
    download_and_save(symbol="BTCUSDT", interval="15", total=2000, category="linear")
    download_and_save(symbol="BTCUSDT", interval="30", total=2000, category="linear")
    print("Market data refreshed.")
    print()


def build_description(candidate):
    family = candidate["family"]

    if family == "breakout":
        return (
            f"{candidate['family']} | "
            f"{candidate['direction']} | "
            f"breakout={candidate['breakout_lookback']} | "
            f"body>={candidate['body_ratio_threshold']} | "
            f"hold={candidate['hold_bars']} | "
            f"trend={candidate['use_trend_filter']} | "
            f"vol={candidate['use_vol_filter']}"
        )

    if family == "mean_reversion":
        return (
            f"{candidate['family']} | "
            f"{candidate['direction']} | "
            f"zscore>={candidate['zscore_threshold']} | "
            f"hold={candidate['hold_bars']} | "
            f"trend={candidate['use_trend_filter']}"
        )

    if family == "trend_pullback":
        return (
            f"{candidate['family']} | "
            f"{candidate['direction']} | "
            f"pullback>={candidate['pullback_threshold']} | "
            f"hold={candidate['hold_bars']} | "
            f"trend={candidate['use_trend_filter']} | "
            f"vol={candidate['use_vol_filter']}"
        )

    return str(candidate)


def is_stable_candidate(train_m, test_m):
    return (
        test_m["total_return_pct"] > -5
        and test_m["trades"] > 10
        and abs(test_m["total_return_pct"] - train_m["total_return_pct"]) < 10
    )


def main():
    args = parse_args()

    if args.refresh_data:
        refresh_market_data()

    df = process()
    df = prepare_pa_features(df)
    train_df, test_df = split_df(df)

    candidates = build_rule_candidates()

    if args.candidate_id < 1 or args.candidate_id > len(candidates):
        raise ValueError(
            f"candidate-id must be between 1 and {len(candidates)}, got {args.candidate_id}"
        )

    candidate = candidates[args.candidate_id - 1]

    train_df_c = apply_candidate(train_df, candidate)
    test_df_c = apply_candidate(test_df, candidate)

    train_bt = run_backtest(train_df_c)
    test_bt = run_backtest(test_df_c)

    train_m = calculate_metrics(train_bt)
    test_m = calculate_metrics(test_bt)

    stable = is_stable_candidate(train_m, test_m)
    description = build_description(candidate)

    print()
    print("=" * 88)
    print("RUN CANDIDATE REPORT")
    print("=" * 88)
    print(f"candidate_id        : {args.candidate_id}")
    print(f"description         : {description}")
    print(f"rows_total          : {len(df)}")
    print(f"rows_train          : {len(train_df)}")
    print(f"rows_test           : {len(test_df)}")

    if "timestamp" in train_df.columns:
        print(f"train_start         : {train_df['timestamp'].iloc[0]}")
        print(f"train_end           : {train_df['timestamp'].iloc[-1]}")
        print(f"test_start          : {test_df['timestamp'].iloc[0]}")
        print(f"test_end            : {test_df['timestamp'].iloc[-1]}")

    print("=" * 88)

    print()
    print("[CANDIDATE PARAMS]")
    for k, v in candidate.items():
        print(f"{k:20}: {v}")

    print()
    print("[TRAIN METRICS]")
    for k, v in train_m.items():
        print(f"{k:20}: {v}")

    print()
    print("[TEST METRICS]")
    for k, v in test_m.items():
        print(f"{k:20}: {v}")

    print()
    print(f"{'stable':20}: {stable}")

    if args.save_report:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = REPORTS_DIR / f"run_candidate_{args.candidate_id}.json"

        payload = {
            "candidate_id": args.candidate_id,
            "description": description,
            "candidate": candidate,
            "train_metrics": train_m,
            "test_metrics": test_m,
            "stable": stable,
        }

        with out_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

        print()
        print(f"saved_report        : {out_path}")


if __name__ == "__main__":
    main()
