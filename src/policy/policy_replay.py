from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import REPORTS_DIR, load_settings
from data.market_data_manager import get_processed_market_data
from policy.policy_manager import PolicyManager
from research.alpha_miner import prepare_pa_features


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay policy decisions over recent bars")
    parser.add_argument("--bars", type=int, default=300, help="How many most recent decision bars to replay")
    parser.add_argument(
        "--output-prefix",
        type=str,
        default="policy_replay",
        help="Prefix for report files written to reports/",
    )
    return parser.parse_args()


def build_runtime_window(df: pd.DataFrame, end_idx: int, live_window_bars: int) -> pd.DataFrame:
    window = df.iloc[: end_idx + 1].tail(live_window_bars).copy()
    window = prepare_pa_features(window)
    return window


def main() -> None:
    args = parse_args()
    settings = load_settings()

    base_df = get_processed_market_data(settings)
    live_window_bars = max(
        int(settings.policy.live_window_bars),
        int(settings.policy.recent_bars_for_evaluation) + 100,
        300,
    )

    if len(base_df) < 50:
        raise RuntimeError("not_enough_market_data_for_policy_replay")

    policy = PolicyManager(settings)

    last_n = max(1, int(args.bars))
    start_idx = max(0, len(base_df) - last_n)

    replay_rows: list[dict] = []
    candidate_stats: dict[str, dict] = defaultdict(lambda: {
        "candidate_key": None,
        "family": None,
        "direction": None,
        "regime_tag": None,
        "evaluated_count": 0,
        "selected_count": 0,
        "regime_match_count": 0,
        "raw_entry_count": 0,
        "hard_position_count": 0,
        "soft_vote_count": 0,
        "inactive_count": 0,
        "effective_weight_total": 0.0,
        "weight_total": 0.0,
        "soft_weight_total": 0.0,
        "avg_activity_factor": 0.0,
        "avg_regime_factor": 0.0,
        "avg_direction_factor": 0.0,
        "avg_family_factor": 0.0,
        "avg_bars_since_last_entry": 0.0,
        "avg_bars_since_last_position": 0.0,
        "long_contribution": 0.0,
        "short_contribution": 0.0,
        "last_seen_timestamp": None,
        "last_selected_timestamp": None,
    })
    regime_counts: dict[str, int] = defaultdict(int)
    decisions_nonzero = 0
    long_decisions = 0
    short_decisions = 0

    for i in range(start_idx, len(base_df)):
        runtime_df = build_runtime_window(base_df, i, live_window_bars)
        if runtime_df.empty:
            continue

        decision = policy.decide(runtime_df)
        ts = str(runtime_df.iloc[-1]["timestamp"])
        regime_counts[decision.market_regime] += 1

        if decision.desired_position != 0:
            decisions_nonzero += 1
            if decision.desired_position > 0:
                long_decisions += 1
            else:
                short_decisions += 1

        evaluated = decision.diagnostics.get("evaluated_candidates", [])
        selected_map = {str(row.get("candidate_key")): row for row in decision.selected_candidates}

        for item in evaluated:
            key = str(item.get("candidate_key"))
            stat = candidate_stats[key]
            stat["candidate_key"] = key
            stat["family"] = item.get("family")
            stat["direction"] = item.get("direction")
            stat["regime_tag"] = item.get("regime_tag")
            stat["evaluated_count"] += 1
            if float(item.get("regime_factor", 0.0)) > 0:
                stat["regime_match_count"] += 1
            stat["effective_weight_total"] += float(item.get("effective_weight", 0.0))
            stat["avg_activity_factor"] += float(item.get("activity_factor", 0.0))
            stat["avg_regime_factor"] += float(item.get("regime_factor", 0.0))
            stat["avg_direction_factor"] += float(item.get("direction_factor", 0.0))
            stat["avg_family_factor"] += float(item.get("family_factor", 1.0))
            bars_since_last_entry = item.get("bars_since_last_entry")
            if bars_since_last_entry is not None:
                stat["avg_bars_since_last_entry"] += float(bars_since_last_entry)
            bars_since_last_position = item.get("bars_since_last_position")
            if bars_since_last_position is not None:
                stat["avg_bars_since_last_position"] += float(bars_since_last_position)
            if int(item.get("entry_signal", 0)) == 0 and int(item.get("desired_position", 0)) == 0 and int(item.get("soft_direction", 0)) == 0:
                stat["inactive_count"] += 1
            stat["last_seen_timestamp"] = ts

        for key, row in selected_map.items():
            stat = candidate_stats[key]
            stat["candidate_key"] = key
            stat["family"] = row.get("family")
            stat["direction"] = row.get("direction")
            stat["regime_tag"] = row.get("regime_tag")
            stat["selected_count"] += 1
            stat["last_selected_timestamp"] = ts
            stat["weight_total"] += float(row.get("weight", 0.0))
            stat["soft_weight_total"] += float(row.get("soft_weight", 0.0))
            if int(row.get("entry_signal", 0)) != 0:
                stat["raw_entry_count"] += 1
            if int(row.get("desired_position", 0)) != 0:
                stat["hard_position_count"] += 1
            if int(row.get("soft_direction", 0)) != 0 and float(row.get("soft_weight", 0.0)) > 0:
                stat["soft_vote_count"] += 1
            if int(row.get("desired_position", 0)) > 0 or int(row.get("soft_direction", 0)) > 0:
                stat["long_contribution"] += float(row.get("weight", 0.0)) + float(row.get("soft_weight", 0.0))
            if int(row.get("desired_position", 0)) < 0 or int(row.get("soft_direction", 0)) < 0:
                stat["short_contribution"] += float(row.get("weight", 0.0)) + float(row.get("soft_weight", 0.0))

        top_contributors = []
        for row in decision.selected_candidates[:3]:
            top_contributors.append({
                "candidate_key": row.get("candidate_key"),
                "family": row.get("family"),
                "direction": row.get("direction"),
                "entry_signal": int(row.get("entry_signal", 0)),
                "desired_position": int(row.get("desired_position", 0)),
                "weight": float(row.get("weight", 0.0)),
                "soft_direction": int(row.get("soft_direction", 0)),
                "soft_weight": float(row.get("soft_weight", 0.0)),
            })

        replay_rows.append({
            "timestamp": ts,
            "price": float(decision.price),
            "market_regime": decision.market_regime,
            "desired_position": int(decision.desired_position),
            "entry_signal": int(decision.entry_signal),
            "confidence": float(decision.confidence),
            "votes_long": float(decision.vote_long),
            "votes_short": float(decision.vote_short),
            "signals_last_100_bars": int(decision.signals_last_100_bars),
            "selected_candidates_count": int(decision.selected_candidates_count),
            "active_candidates_count": int(decision.active_candidates_count),
            "bank_loaded": int(decision.diagnostics.get("bank_loaded", 0)),
            "after_direction_filter": int(decision.diagnostics.get("after_direction_filter", 0)),
            "strict_regime_candidates": int(decision.diagnostics.get("strict_regime_candidates", 0)),
            "regime_candidates": int(decision.diagnostics.get("regime_candidates", 0)),
            "evaluated_primary": int(decision.diagnostics.get("evaluated_primary", 0)),
            "selected_primary": int(decision.diagnostics.get("selected_primary", 0)),
            "fallback_used": bool(decision.diagnostics.get("fallback_used", False)),
            "fallback_scope": str(decision.diagnostics.get("fallback_scope", "")),
            "raw_long_signals": int(decision.diagnostics.get("primary_vote_breakdown", {}).get("raw_long_signals", 0)),
            "raw_short_signals": int(decision.diagnostics.get("primary_vote_breakdown", {}).get("raw_short_signals", 0)),
            "soft_long_votes": int(decision.diagnostics.get("primary_vote_breakdown", {}).get("soft_long_votes", 0)),
            "soft_short_votes": int(decision.diagnostics.get("primary_vote_breakdown", {}).get("soft_short_votes", 0)),
            "top_contributors": json.dumps(top_contributors, ensure_ascii=False),
        })

    replay_df = pd.DataFrame(replay_rows)
    candidate_df = pd.DataFrame(candidate_stats.values())
    if not candidate_df.empty:
        for avg_col in [
            "avg_activity_factor",
            "avg_regime_factor",
            "avg_direction_factor",
            "avg_family_factor",
            "avg_bars_since_last_entry",
            "avg_bars_since_last_position",
        ]:
            candidate_df[avg_col] = (candidate_df[avg_col] / candidate_df["evaluated_count"].replace(0, 1)).round(4)
        for col in [
            "effective_weight_total",
            "weight_total",
            "soft_weight_total",
            "long_contribution",
            "short_contribution",
        ]:
            candidate_df[col] = candidate_df[col].round(4)
        candidate_df["selection_rate"] = (candidate_df["selected_count"] / candidate_df["evaluated_count"].replace(0, 1)).round(4)
        candidate_df["regime_match_rate"] = (candidate_df["regime_match_count"] / candidate_df["evaluated_count"].replace(0, 1)).round(4)
        candidate_df["inactive_rate"] = (candidate_df["inactive_count"] / candidate_df["evaluated_count"].replace(0, 1)).round(4)
        candidate_df = candidate_df.sort_values(
            ["selected_count", "raw_entry_count", "evaluated_count", "long_contribution", "short_contribution"],
            ascending=[False, False, False, False, False],
        ).reset_index(drop=True)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    prefix = args.output_prefix.strip() or "policy_replay"
    replay_csv = REPORTS_DIR / f"{prefix}.csv"
    candidate_csv = REPORTS_DIR / f"{prefix}_candidates.csv"
    summary_json = REPORTS_DIR / f"{prefix}_summary.json"

    replay_df.to_csv(replay_csv, index=False)
    if not candidate_df.empty:
        candidate_df.to_csv(candidate_csv, index=False)
    else:
        pd.DataFrame(columns=["candidate_key"]).to_csv(candidate_csv, index=False)

    summary = {
        "bars_replayed": int(len(replay_df)),
        "window_bars": int(live_window_bars),
        "decision_nonzero_count": int(decisions_nonzero),
        "decision_nonzero_ratio": round(float(decisions_nonzero / len(replay_df)), 4) if len(replay_df) else 0.0,
        "long_decisions": int(long_decisions),
        "short_decisions": int(short_decisions),
        "regime_counts": dict(sorted(regime_counts.items())),
        "top_selected_candidates": candidate_df.head(10).to_dict(orient="records") if not candidate_df.empty else [],
        "files": {
            "replay_csv": str(replay_csv),
            "candidate_csv": str(candidate_csv),
            "summary_json": str(summary_json),
        },
    }

    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved: {replay_csv}")
    print(f"Saved: {candidate_csv}")
    print(f"Saved: {summary_json}")
    print(f"Bars replayed: {len(replay_df)} | nonzero decisions: {decisions_nonzero} | long: {long_decisions} | short: {short_decisions}")
    if regime_counts:
        print("Regimes:", dict(sorted(regime_counts.items())))
    if not candidate_df.empty:
        print("Top candidate activity:")
        cols = [
            "candidate_key",
            "family",
            "direction",
            "regime_tag",
            "evaluated_count",
            "selected_count",
            "raw_entry_count",
            "hard_position_count",
            "soft_vote_count",
        ]
        print(candidate_df[cols].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
