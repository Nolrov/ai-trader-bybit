from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import REPORTS_DIR
from policy.policy_replay import build_runtime_window
from config.settings import load_settings
from data.market_data_manager import get_processed_market_data
from policy.policy_manager import PolicyManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit short-side participation across recent policy decisions")
    parser.add_argument("--bars", type=int, default=400, help="How many most recent decision bars to inspect")
    parser.add_argument("--output-prefix", type=str, default="short_side_audit", help="Prefix for report files")
    return parser.parse_args()


def classify_bottleneck(row: pd.Series) -> str:
    if float(row.get("regime_match_rate", 0.0)) < 0.35:
        return "regime_mismatch"
    if float(row.get("avg_activity_factor", 0.0)) < 0.18:
        return "low_activity"
    if int(row.get("selected_count", 0)) == 0 and int(row.get("raw_entry_count", 0)) > 0:
        return "underweighted"
    if int(row.get("raw_entry_count", 0)) == 0 and int(row.get("soft_vote_count", 0)) > 0:
        return "soft_only"
    if int(row.get("hard_position_count", 0)) == 0 and int(row.get("raw_entry_count", 0)) == 0:
        return "no_trigger"
    if float(row.get("selection_rate", 0.0)) >= 0.08 and float(row.get("short_contribution", 0.0)) > 0:
        return "healthy"
    return "mixed"


def main() -> None:
    args = parse_args()
    settings = load_settings()
    base_df = get_processed_market_data(settings)
    live_window_bars = max(
        int(settings.policy.live_window_bars),
        int(settings.policy.recent_bars_for_evaluation) + 100,
        300,
    )
    policy = PolicyManager(settings)
    start_idx = max(0, len(base_df) - max(1, int(args.bars)))

    rows: list[dict] = []
    family_rows: list[dict] = []
    decisions = {"bars": 0, "short_decisions": 0, "bars_with_short_selected": 0}

    for i in range(start_idx, len(base_df)):
        runtime_df = build_runtime_window(base_df, i, live_window_bars)
        if runtime_df.empty:
            continue
        decision = policy.decide(runtime_df)
        decisions["bars"] += 1
        if int(decision.desired_position) < 0:
            decisions["short_decisions"] += 1

        evaluated = decision.diagnostics.get("evaluated_candidates", [])
        selected = {str(r.get("candidate_key")): r for r in decision.selected_candidates if str(r.get("direction", "")).lower() == "short"}
        if selected:
            decisions["bars_with_short_selected"] += 1

        if not evaluated:
            continue

        eval_df = pd.DataFrame(evaluated)
        if eval_df.empty:
            continue
        eval_df = eval_df[eval_df["direction"].astype(str).str.lower() == "short"].copy()
        if eval_df.empty:
            continue

        eval_df["selected"] = eval_df["candidate_key"].astype(str).isin(selected.keys()).astype(int)
        eval_df["raw_entry"] = eval_df["candidate_key"].astype(str).map(lambda k: int(selected.get(k, {}).get("entry_signal", 0) != 0)).astype(int)
        eval_df["hard_position"] = eval_df["candidate_key"].astype(str).map(lambda k: int(selected.get(k, {}).get("desired_position", 0) < 0)).astype(int)
        eval_df["soft_vote"] = eval_df["candidate_key"].astype(str).map(lambda k: int(selected.get(k, {}).get("soft_direction", 0) < 0 and float(selected.get(k, {}).get("soft_weight", 0.0)) > 0)).astype(int)
        eval_df["weight"] = eval_df["candidate_key"].astype(str).map(lambda k: float(selected.get(k, {}).get("weight", 0.0)))
        eval_df["soft_weight"] = eval_df["candidate_key"].astype(str).map(lambda k: float(selected.get(k, {}).get("soft_weight", 0.0)))
        eval_df["timestamp"] = str(runtime_df.iloc[-1]["timestamp"])
        rows.extend(eval_df.to_dict(orient="records"))

    audit_df = pd.DataFrame(rows)
    if audit_df.empty:
        raise RuntimeError("short_side_audit_empty:no_short_candidates_seen")

    short_candidate_df = (
        audit_df.groupby(["candidate_key", "family", "regime_tag"], dropna=False)
        .agg(
            evaluated_count=("candidate_key", "count"),
            regime_match_count=("regime_factor", lambda s: int((pd.Series(s) > 0).sum())),
            selected_count=("selected", "sum"),
            raw_entry_count=("raw_entry", "sum"),
            hard_position_count=("hard_position", "sum"),
            soft_vote_count=("soft_vote", "sum"),
            effective_weight_total=("effective_weight", "sum"),
            weight_total=("weight", "sum"),
            soft_weight_total=("soft_weight", "sum"),
            avg_activity_factor=("activity_factor", "mean"),
            avg_regime_factor=("regime_factor", "mean"),
            avg_direction_factor=("direction_factor", "mean"),
            avg_bars_since_last_entry=("bars_since_last_entry", "mean"),
            avg_bars_since_last_position=("bars_since_last_position", "mean"),
            last_seen_timestamp=("timestamp", "max"),
        )
        .reset_index()
    )

    short_candidate_df[[
        "effective_weight_total",
        "weight_total",
        "soft_weight_total",
        "avg_activity_factor",
        "avg_regime_factor",
        "avg_direction_factor",
        "avg_bars_since_last_entry",
        "avg_bars_since_last_position",
    ]] = short_candidate_df[[
        "effective_weight_total",
        "weight_total",
        "soft_weight_total",
        "avg_activity_factor",
        "avg_regime_factor",
        "avg_direction_factor",
        "avg_bars_since_last_entry",
        "avg_bars_since_last_position",
    ]].round(4)
    short_candidate_df["selection_rate"] = (short_candidate_df["selected_count"] / short_candidate_df["evaluated_count"].replace(0, 1)).round(4)
    short_candidate_df["regime_match_rate"] = (short_candidate_df["regime_match_count"] / short_candidate_df["evaluated_count"].replace(0, 1)).round(4)
    short_candidate_df["bottleneck"] = short_candidate_df.apply(classify_bottleneck, axis=1)
    short_candidate_df = short_candidate_df.sort_values(["hard_position_count", "raw_entry_count", "selection_rate", "effective_weight_total"], ascending=[False, False, False, False]).reset_index(drop=True)

    short_family_df = (
        short_candidate_df.groupby(["family", "regime_tag"], dropna=False)
        .agg(
            candidates=("candidate_key", "count"),
            evaluated_count=("evaluated_count", "sum"),
            selected_count=("selected_count", "sum"),
            raw_entry_count=("raw_entry_count", "sum"),
            hard_position_count=("hard_position_count", "sum"),
            soft_vote_count=("soft_vote_count", "sum"),
            effective_weight_total=("effective_weight_total", "sum"),
            weight_total=("weight_total", "sum"),
            avg_activity_factor=("avg_activity_factor", "mean"),
            avg_regime_factor=("avg_regime_factor", "mean"),
        )
        .reset_index()
        .sort_values(["hard_position_count", "raw_entry_count", "effective_weight_total"], ascending=[False, False, False])
        .reset_index(drop=True)
    )
    short_family_df["selection_rate"] = (short_family_df["selected_count"] / short_family_df["evaluated_count"].replace(0, 1)).round(4)
    short_family_df[["effective_weight_total", "weight_total", "avg_activity_factor", "avg_regime_factor"]] = short_family_df[["effective_weight_total", "weight_total", "avg_activity_factor", "avg_regime_factor"]].round(4)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    prefix = args.output_prefix.strip() or "short_side_audit"
    candidate_csv = REPORTS_DIR / f"{prefix}_candidates.csv"
    family_csv = REPORTS_DIR / f"{prefix}_families.csv"
    summary_json = REPORTS_DIR / f"{prefix}_summary.json"

    short_candidate_df.to_csv(candidate_csv, index=False)
    short_family_df.to_csv(family_csv, index=False)

    summary = {
        "bars_audited": int(decisions["bars"]),
        "short_decisions": int(decisions["short_decisions"]),
        "short_decision_ratio": round(decisions["short_decisions"] / max(1, decisions["bars"]), 4),
        "bars_with_short_selected": int(decisions["bars_with_short_selected"]),
        "bars_with_short_selected_ratio": round(decisions["bars_with_short_selected"] / max(1, decisions["bars"]), 4),
        "top_short_candidates": short_candidate_df.head(10).to_dict(orient="records"),
        "top_short_families": short_family_df.head(10).to_dict(orient="records"),
        "bottlenecks": short_candidate_df["bottleneck"].value_counts().to_dict(),
        "files": {
            "candidate_csv": str(candidate_csv),
            "family_csv": str(family_csv),
            "summary_json": str(summary_json),
        },
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved: {candidate_csv}")
    print(f"Saved: {family_csv}")
    print(f"Saved: {summary_json}")
    print(f"Bars audited: {decisions['bars']} | short decisions: {decisions['short_decisions']} | bars with short selected: {decisions['bars_with_short_selected']}")
    print(short_family_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
