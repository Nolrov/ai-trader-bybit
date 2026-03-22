# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import load_settings
from data.market_data_manager import get_processed_market_data
from research.alpha_miner import prepare_pa_features, apply_candidate, split_df
from backtest.engine import run_backtest, calculate_metrics

BASE_DIR = Path(__file__).resolve().parent.parent.parent
REPORTS_DIR = BASE_DIR / "reports"
DEFAULT_ACTIVE_BANK = REPORTS_DIR / "active_candidates.json"
EXCLUDED_META_KEYS = {
    "candidate_key",
    "score",
    "test_return",
    "test_sharpe",
    "test_drawdown",
    "test_trades",
    "is_valid",
    "is_promising",
    "rank",
    "description",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one candidate from active candidate bank"
    )
    parser.add_argument(
        "--candidate-key",
        type=str,
        help="Candidate key from active_candidates.json. Unique prefix is accepted.",
    )
    parser.add_argument(
        "--active-bank-file",
        type=Path,
        default=None,
        help="Path to active candidate bank JSON. Defaults to settings.policy.active_candidates_file or reports/active_candidates.json.",
    )
    parser.add_argument(
        "--list-active",
        action="store_true",
        help="List active candidates and exit.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max rows to show with --list-active.",
    )
    parser.add_argument(
        "--save-report",
        action="store_true",
        help="Save one-candidate JSON report to reports/.",
    )
    args = parser.parse_args()

    if not args.list_active and not args.candidate_key:
        parser.error("--candidate-key is required unless --list-active is used")

    return args


def get_active_bank_path(args: argparse.Namespace, settings: Any) -> Path:
    if args.active_bank_file is not None:
        return args.active_bank_file

    policy = getattr(settings, "policy", None)
    if policy is not None:
        active_candidates_file = getattr(policy, "active_candidates_file", None)
        if active_candidates_file:
            return Path(active_candidates_file)

    return DEFAULT_ACTIVE_BANK


def load_active_candidates(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(
            f"active candidate bank not found: {path}. Run alpha_miner.py first."
        )

    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, list):
        raise ValueError(f"active candidate bank must be a JSON list, got: {type(payload)!r}")

    candidates: list[dict[str, Any]] = []
    for idx, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"candidate #{idx} is not an object")
        if "candidate_key" not in item:
            raise ValueError(f"candidate #{idx} has no candidate_key")
        candidates.append(item)

    if not candidates:
        raise ValueError(f"active candidate bank is empty: {path}")

    return candidates


def format_value(value: Any) -> str:
    if isinstance(value, float):
        text = f"{value:.6f}".rstrip("0").rstrip(".")
        return text if text else "0"
    return str(value)


def build_description(candidate: dict[str, Any]) -> str:
    if candidate.get("description"):
        return str(candidate["description"])

    family = candidate.get("family", "unknown")
    direction = candidate.get("direction", "n/a")
    parts = [str(family), str(direction)]

    for key in sorted(candidate.keys()):
        if key in EXCLUDED_META_KEYS or key in {"family", "direction"}:
            continue
        parts.append(f"{key}={format_value(candidate[key])}")

    return " | ".join(parts)


def print_active_candidates(candidates: list[dict[str, Any]], limit: int) -> None:
    print()
    print("ACTIVE CANDIDATES")
    print("=" * 120)
    print(
        f"{'candidate_key':14}  {'family':22} {'dir':5} {'regime':14} {'score':10} {'trades':8}  description"
    )
    print("-" * 120)

    for candidate in candidates[: max(limit, 0)]:
        print(
            f"{str(candidate.get('candidate_key', ''))[:14]:14}  "
            f"{str(candidate.get('family', ''))[:22]:22} "
            f"{str(candidate.get('direction', ''))[:5]:5} "
            f"{str(candidate.get('regime_tag', ''))[:14]:14} "
            f"{format_value(candidate.get('score', '')):10} "
            f"{format_value(candidate.get('test_trades', '')):8}  "
            f"{build_description(candidate)}"
        )

    print("=" * 120)
    print(f"total_candidates: {len(candidates)}")


def resolve_candidate(candidates: list[dict[str, Any]], candidate_key: str) -> dict[str, Any]:
    exact = [c for c in candidates if c.get("candidate_key") == candidate_key]
    if exact:
        return exact[0]

    prefix = [c for c in candidates if str(c.get("candidate_key", "")).startswith(candidate_key)]
    if len(prefix) == 1:
        return prefix[0]
    if len(prefix) > 1:
        matches = ", ".join(str(c.get("candidate_key")) for c in prefix[:10])
        raise ValueError(
            f"candidate_key prefix is ambiguous: {candidate_key}. Matches: {matches}"
        )

    raise ValueError(f"candidate_key not found in active bank: {candidate_key}")


def is_stable_candidate(train_m: dict[str, Any], test_m: dict[str, Any]) -> bool:
    return (
        test_m["total_return_pct"] > -5
        and test_m["trades"] > 10
        and abs(test_m["total_return_pct"] - train_m["total_return_pct"]) < 10
    )


def sanitize_filename(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    return sanitized.strip("._") or "candidate"


def main() -> None:
    args = parse_args()
    settings = load_settings()
    active_bank_path = get_active_bank_path(args, settings)
    candidates = load_active_candidates(active_bank_path)

    if args.list_active:
        print_active_candidates(candidates, args.limit)
        return

    candidate = resolve_candidate(candidates, args.candidate_key)
    candidate_key = str(candidate["candidate_key"])

    df = get_processed_market_data(settings)
    df = prepare_pa_features(df)
    train_df, test_df = split_df(df)

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
    print(f"candidate_key       : {candidate_key}")
    print(f"active_bank_file    : {active_bank_path}")
    print(f"description         : {description}")
    print(f"symbol              : {settings.data.symbol}")
    print(f"interval_main       : {settings.data.interval_main}")
    print(f"interval_htf        : {settings.data.interval_htf}")
    print(f"bars_15m            : {settings.data.bars_15m}")
    print(f"bars_30m            : {settings.data.bars_30m}")
    print(f"rows_total          : {len(df)}")
    print(f"rows_train          : {len(train_df)}")
    print(f"rows_test           : {len(test_df)}")

    if "timestamp" in train_df.columns and len(train_df) > 0 and len(test_df) > 0:
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
        out_path = REPORTS_DIR / f"run_candidate_{sanitize_filename(candidate_key)}.json"

        payload = {
            "candidate_key": candidate_key,
            "active_bank_file": str(active_bank_path),
            "description": description,
            "symbol": settings.data.symbol,
            "interval_main": settings.data.interval_main,
            "interval_htf": settings.data.interval_htf,
            "bars_15m": settings.data.bars_15m,
            "bars_30m": settings.data.bars_30m,
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
