from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SOFT_SIGNAL_LOOKBACK_BARS = 8
SOFT_SIGNAL_WEIGHT_FACTOR = 0.15

import pandas as pd

from research.alpha_miner import apply_candidate


@dataclass
class PolicyDecision:
    timestamp: str
    price: float
    desired_position: int
    entry_signal: int
    market_regime: str
    confidence: float
    signals_last_100_bars: int
    selected_candidates_count: int
    active_candidates_count: int
    vote_long: float
    vote_short: float
    selected_candidates: list[dict[str, Any]]
    diagnostics: dict[str, Any] = field(default_factory=dict)


class PolicyManager:
    def __init__(self, settings):
        self.settings = settings
        self.policy_settings = settings.policy
        self.risk_settings = settings.risk

    def _load_active_candidates(self) -> list[dict[str, Any]]:
        path = Path(self.policy_settings.active_candidates_file)
        if not path.exists():
            raise FileNotFoundError(
                f"active_candidates_file_not_found:{path}. Run alpha_miner.py first to build the candidate bank."
            )

        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        if not isinstance(payload, list) or not payload:
            raise RuntimeError("active_candidates_file_is_empty")

        candidates: list[dict[str, Any]] = []
        for item in payload:
            if float(item.get("score", -1e9)) < float(self.policy_settings.min_candidate_score):
                continue
            candidates.append(item)

        if not candidates:
            raise RuntimeError("no_active_candidates_after_score_filter")

        candidates.sort(key=lambda x: float(x.get("score", -1e9)), reverse=True)
        max_active = int(self.policy_settings.max_active_candidates)
        if len(candidates) <= max_active:
            return candidates

        selected: list[dict[str, Any]] = []
        used_keys: set[str] = set()

        def add_candidate(candidate: dict[str, Any]) -> None:
            key = str(candidate.get("candidate_key") or "")
            if key in used_keys:
                return
            used_keys.add(key)
            selected.append(candidate)

        short_candidates = [c for c in candidates if str(c.get("direction", "")).lower() == "short"]
        long_candidates = [c for c in candidates if str(c.get("direction", "")).lower() == "long"]

        min_short = 0
        if self.risk_settings.allow_short and short_candidates:
            min_short = min(max(2, max_active // 4), len(short_candidates), max_active)
            for candidate in short_candidates[:min_short]:
                add_candidate(candidate)

        # Keep the rest score-first, regardless of direction, after preserving short coverage.
        for candidate in candidates:
            if len(selected) >= max_active:
                break
            add_candidate(candidate)

        return selected[:max_active]

    def _detect_market_regime(self, df: pd.DataFrame) -> str:
        last = df.iloc[-1]

        if int(last.get("regime_high_vol", 0)) == 1 and int(last.get("regime_trend", 0)) == 1:
            return "trend_high_vol"
        if int(last.get("regime_trend", 0)) == 1:
            return "trend"
        if int(last.get("regime_high_vol", 0)) == 1:
            return "high_vol"
        return "flat"

    def _candidate_matches_regime(self, regime_tag: str, market_regime: str) -> bool:
        regime_tag = str(regime_tag or "all")
        market_regime = str(market_regime or "flat")

        if regime_tag == "all":
            return True

        compatibility_map = {
            "flat": {"flat", "all"},
            "trend": {"trend", "trend_high_vol", "all"},
            "high_vol": {"high_vol", "trend_high_vol", "all"},
            "trend_high_vol": {"trend_high_vol", "trend", "high_vol", "all"},
        }

        allowed_regimes = compatibility_map.get(market_regime, {market_regime, "all"})
        return regime_tag in allowed_regimes

    def _filter_direction(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        for candidate in candidates:
            direction = str(candidate.get("direction", "")).lower()
            if direction == "long" and not self.risk_settings.allow_long:
                continue
            if direction == "short" and not self.risk_settings.allow_short:
                continue
            filtered.append(candidate)
        return filtered

    def _run_candidate_vote(self, scoped: pd.DataFrame, candidate: dict[str, Any], market_regime: str) -> dict[str, Any] | None:
        df_signal = apply_candidate(scoped, candidate)
        if df_signal.empty:
            return None

        last = df_signal.iloc[-1]
        entry_series = df_signal["entry_signal"].fillna(0).astype(int)
        position_series = df_signal["position"].fillna(0).astype(int)

        desired_position = int(last.get("position", 0))
        entry_signal = int(last.get("entry_signal", 0))
        recent_signals = int((entry_series.tail(100) != 0).sum())
        recent_signals_short = int((entry_series.tail(SOFT_SIGNAL_LOOKBACK_BARS) != 0).sum())

        last_entry_idx = entry_series[entry_series != 0].index
        last_position_idx = position_series[position_series != 0].index
        bars_since_last_entry = None
        bars_since_last_position = None
        if len(last_entry_idx) > 0:
            bars_since_last_entry = int(len(df_signal) - 1 - df_signal.index.get_loc(last_entry_idx[-1]))
        if len(last_position_idx) > 0:
            bars_since_last_position = int(len(df_signal) - 1 - df_signal.index.get_loc(last_position_idx[-1]))

        raw_score = float(candidate.get("score", 0.0))
        base_weight = max(0.0, raw_score)
        if str(candidate.get("regime_tag", "all")) == market_regime:
            base_weight *= 1.15
        hard_weight = base_weight * (1.1 if entry_signal != 0 else 1.0)

        soft_direction = 0
        soft_reason = None
        soft_weight = 0.0
        if desired_position == 0 and entry_signal == 0:
            if recent_signals_short > 0 and bars_since_last_entry is not None and bars_since_last_entry <= SOFT_SIGNAL_LOOKBACK_BARS:
                last_recent_nonzero = int(entry_series[entry_series != 0].iloc[-1])
                soft_direction = 1 if last_recent_nonzero > 0 else -1
                soft_weight = max(0.05, base_weight * SOFT_SIGNAL_WEIGHT_FACTOR)
                soft_reason = f"recent_entry_within_{SOFT_SIGNAL_LOOKBACK_BARS}_bars"
            elif bars_since_last_position is not None and bars_since_last_position <= min(5, SOFT_SIGNAL_LOOKBACK_BARS):
                last_recent_position = int(position_series[position_series != 0].iloc[-1])
                soft_direction = 1 if last_recent_position > 0 else -1
                soft_weight = max(0.03, base_weight * (SOFT_SIGNAL_WEIGHT_FACTOR * 0.75))
                soft_reason = "recent_position_decay"

        return {
            "candidate_key": candidate.get("candidate_key"),
            "family": candidate.get("family"),
            "direction": candidate.get("direction"),
            "regime_tag": candidate.get("regime_tag"),
            "score": raw_score,
            "entry_signal": entry_signal,
            "desired_position": desired_position,
            "weight": round(hard_weight, 4),
            "recent_signals": recent_signals,
            "recent_signals_short": recent_signals_short,
            "bars_since_last_entry": bars_since_last_entry,
            "bars_since_last_position": bars_since_last_position,
            "soft_direction": soft_direction,
            "soft_weight": round(soft_weight, 4),
            "soft_reason": soft_reason,
        }

    def _aggregate_votes(self, vote_rows: list[dict[str, Any]]) -> tuple[float, float, int, list[dict[str, Any]], dict[str, Any]]:
        vote_long = 0.0
        vote_short = 0.0
        recent_signal_count = 0
        selected_candidates: list[dict[str, Any]] = []
        raw_long_signals = 0
        raw_short_signals = 0
        hard_long_positions = 0
        hard_short_positions = 0
        soft_long_votes = 0
        soft_short_votes = 0

        for row in vote_rows:
            recent_signal_count += int(row.get("recent_signals", 0))
            desired_position = int(row.get("desired_position", 0))
            entry_signal = int(row.get("entry_signal", 0))
            weight = float(row.get("weight", 0.0))
            soft_direction = int(row.get("soft_direction", 0))
            soft_weight = float(row.get("soft_weight", 0.0))

            if entry_signal > 0:
                raw_long_signals += 1
            elif entry_signal < 0:
                raw_short_signals += 1

            if desired_position > 0:
                vote_long += weight
                hard_long_positions += 1
            elif desired_position < 0:
                vote_short += weight
                hard_short_positions += 1
            elif soft_direction > 0 and soft_weight > 0:
                vote_long += soft_weight
                soft_long_votes += 1
            elif soft_direction < 0 and soft_weight > 0:
                vote_short += soft_weight
                soft_short_votes += 1

            if desired_position != 0 or entry_signal != 0 or (soft_direction != 0 and soft_weight > 0):
                selected_candidates.append(row)

        breakdown = {
            "raw_long_signals": raw_long_signals,
            "raw_short_signals": raw_short_signals,
            "hard_long_positions": hard_long_positions,
            "hard_short_positions": hard_short_positions,
            "soft_long_votes": soft_long_votes,
            "soft_short_votes": soft_short_votes,
        }
        return vote_long, vote_short, recent_signal_count, selected_candidates, breakdown

    def decide(self, df: pd.DataFrame) -> PolicyDecision:
        if df.empty:
            raise RuntimeError("policy_input_dataframe_is_empty")

        market_regime = self._detect_market_regime(df)
        loaded_candidates = self._load_active_candidates()
        active_candidates = self._filter_direction(loaded_candidates)
        if not active_candidates:
            raise RuntimeError("no_active_candidates_after_direction_filter")

        scoped = df.tail(int(self.policy_settings.recent_bars_for_evaluation)).copy()
        strict_regime_candidates = [
            c for c in active_candidates if str(c.get("regime_tag", "all")) == market_regime
        ]
        compatible_regime_candidates = [
            c for c in active_candidates if self._candidate_matches_regime(str(c.get("regime_tag", "all")), market_regime)
        ]
        candidates_for_regime = compatible_regime_candidates if compatible_regime_candidates else active_candidates

        diagnostics: dict[str, Any] = {
            "market_regime": market_regime,
            "bank_loaded": len(loaded_candidates),
            "after_direction_filter": len(active_candidates),
            "shorts_enabled": bool(self.risk_settings.allow_short),
            "strict_regime_candidates": len(strict_regime_candidates),
            "regime_candidates": len(compatible_regime_candidates),
            "fallback_used": False,
            "fallback_reason": None,
            "fallback_scope": "compatible_regime" if compatible_regime_candidates else "global",
        }

        diagnostics["evaluated_candidates"] = [
            {
                "candidate_key": c.get("candidate_key"),
                "family": c.get("family"),
                "direction": c.get("direction"),
                "regime_tag": c.get("regime_tag"),
                "score": round(float(c.get("score", 0.0)), 4),
            }
            for c in candidates_for_regime[:10]
        ]

        primary_vote_rows: list[dict[str, Any]] = []
        for candidate in candidates_for_regime:
            vote_row = self._run_candidate_vote(scoped, candidate, market_regime)
            if vote_row is not None:
                primary_vote_rows.append(vote_row)

        vote_long, vote_short, recent_signal_count, selected_candidates, vote_breakdown = self._aggregate_votes(primary_vote_rows)
        diagnostics["evaluated_primary"] = len(primary_vote_rows)
        diagnostics["selected_primary"] = len(selected_candidates)
        diagnostics["primary_vote_breakdown"] = vote_breakdown

        if not selected_candidates:
            fallback_candidates = active_candidates[: min(5, len(active_candidates))]
            fallback_vote_rows: list[dict[str, Any]] = []
            for candidate in fallback_candidates:
                vote_row = self._run_candidate_vote(scoped, candidate, market_regime)
                if vote_row is not None:
                    fallback_vote_rows.append(vote_row)

            diagnostics["fallback_candidates"] = [
                {
                    "candidate_key": c.get("candidate_key"),
                    "family": c.get("family"),
                    "direction": c.get("direction"),
                    "regime_tag": c.get("regime_tag"),
                    "score": round(float(c.get("score", 0.0)), 4),
                }
                for c in fallback_candidates
            ]
            fallback_vote_long, fallback_vote_short, fallback_recent_signals, fallback_selected, fallback_breakdown = self._aggregate_votes(
                fallback_vote_rows
            )
            diagnostics["evaluated_fallback"] = len(fallback_vote_rows)
            diagnostics["selected_fallback"] = len(fallback_selected)
            diagnostics["fallback_vote_breakdown"] = fallback_breakdown

            if fallback_selected:
                diagnostics["fallback_used"] = True
                diagnostics["fallback_reason"] = "no_active_signals_in_regime_scope"
                diagnostics["fallback_scope"] = "global_top"
                vote_long = fallback_vote_long
                vote_short = fallback_vote_short
                recent_signal_count = fallback_recent_signals
                selected_candidates = fallback_selected
        else:
            diagnostics["evaluated_fallback"] = 0
            diagnostics["selected_fallback"] = 0
            diagnostics["fallback_vote_breakdown"] = {
                "raw_long_signals": 0,
                "raw_short_signals": 0,
                "hard_long_positions": 0,
                "hard_short_positions": 0,
                "soft_long_votes": 0,
                "soft_short_votes": 0,
            }
            diagnostics["fallback_candidates"] = []

        total_votes = vote_long + vote_short
        confidence = 0.0 if total_votes <= 0 else abs(vote_long - vote_short) / total_votes

        desired_position = 0
        entry_signal = 0
        threshold = float(self.policy_settings.decision_threshold)

        if total_votes > 0 and confidence >= threshold:
            if vote_long > vote_short:
                desired_position = 1
                entry_signal = 1
            elif vote_short > vote_long:
                desired_position = -1
                entry_signal = -1

        diagnostics["threshold"] = threshold
        diagnostics["total_votes"] = round(total_votes, 4)
        diagnostics["confidence"] = round(confidence, 4)
        diagnostics["decision"] = desired_position
        diagnostics["confidence_breakdown"] = {
            "vote_long": round(vote_long, 4),
            "vote_short": round(vote_short, 4),
            "total_votes": round(total_votes, 4),
            "imbalance": round(abs(vote_long - vote_short), 4),
        }

        last_row = scoped.iloc[-1]
        return PolicyDecision(
            timestamp=str(last_row["timestamp"]),
            price=float(last_row["close_15m"]),
            desired_position=desired_position,
            entry_signal=entry_signal,
            market_regime=market_regime,
            confidence=round(confidence, 4),
            signals_last_100_bars=int(recent_signal_count),
            selected_candidates_count=len(selected_candidates),
            active_candidates_count=len(candidates_for_regime),
            vote_long=round(vote_long, 4),
            vote_short=round(vote_short, 4),
            selected_candidates=sorted(
                selected_candidates,
                key=lambda x: (abs(int(x["desired_position"])), float(x["weight"])),
                reverse=True,
            )[:10],
            diagnostics=diagnostics,
        )
