from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SOFT_SIGNAL_LOOKBACK_BARS = 8
SOFT_SIGNAL_WEIGHT_FACTOR = 0.12
ACTIVITY_TARGET_SIGNALS = 3
COMPATIBLE_REGIME_FACTOR = 0.75
FALLBACK_REGIME_FACTOR = 0.5
DOMINANT_DIRECTION_FACTOR = 0.85
NEUTRAL_DIRECTION_FACTOR = 1.0

REGIME_FAMILY_FACTORS = {
    "flat": {
        "mean_reversion": 1.25,
        "pa_false_breakout": 1.15,
        "pa_range_rejection": 1.1,
        "breakout": 0.8,
        "atr_breakout": 0.85,
        "compression_breakout": 0.9,
        "momentum_continuation": 0.8,
        "pa_trend_pullback": 0.85,
        "trend_pullback": 0.85,
        "trend_reclaim": 0.9,
    },
    "trend": {
        "breakout": 1.2,
        "pa_trend_pullback": 1.15,
        "trend_pullback": 1.15,
        "trend_reclaim": 1.1,
        "momentum_continuation": 1.15,
        "atr_breakout": 1.05,
        "mean_reversion": 0.8,
        "pa_false_breakout": 0.9,
        "pa_range_rejection": 0.9,
    },
    "high_vol": {
        "atr_breakout": 1.25,
        "breakout": 1.15,
        "compression_breakout": 1.15,
        "pa_false_breakout": 1.0,
        "momentum_continuation": 1.0,
        "mean_reversion": 0.9,
        "pa_trend_pullback": 0.95,
        "trend_pullback": 0.95,
    },
    "trend_high_vol": {
        "breakout": 1.25,
        "atr_breakout": 1.2,
        "compression_breakout": 1.15,
        "pa_trend_pullback": 1.1,
        "trend_pullback": 1.1,
        "momentum_continuation": 1.1,
        "trend_reclaim": 1.05,
        "mean_reversion": 0.8,
        "pa_false_breakout": 0.9,
    },
}
DEFAULT_FAMILY_FACTOR = 1.0


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

    def _regime_factor(self, regime_tag: str, market_regime: str, fallback: bool = False) -> float:
        regime_tag = str(regime_tag or "all")
        market_regime = str(market_regime or "flat")
        if regime_tag == market_regime:
            return 1.0
        if fallback:
            return FALLBACK_REGIME_FACTOR
        if self._candidate_matches_regime(regime_tag, market_regime):
            return COMPATIBLE_REGIME_FACTOR
        return 0.0

    def _activity_factor(self, recent_signals_short: int, recent_signals: int, bars_since_last_entry: int | None, bars_since_last_position: int | None) -> float:
        # recent true entries dominate; fresh positions keep some influence; silent candidates fade naturally.
        entry_component = min(1.0, recent_signals_short / ACTIVITY_TARGET_SIGNALS) if recent_signals_short > 0 else 0.0
        recent_component = min(1.0, recent_signals / max(8, ACTIVITY_TARGET_SIGNALS * 2)) if recent_signals > 0 else 0.0
        recency_bonus = 0.0
        if bars_since_last_entry is not None and bars_since_last_entry <= SOFT_SIGNAL_LOOKBACK_BARS:
            recency_bonus = max(recency_bonus, 0.35)
        if bars_since_last_position is not None and bars_since_last_position <= min(5, SOFT_SIGNAL_LOOKBACK_BARS):
            recency_bonus = max(recency_bonus, 0.2)
        return min(1.0, max(entry_component, recent_component, recency_bonus))

    def _direction_factor(self, direction: str, direction_bias: str | None) -> float:
        direction = str(direction or "").lower()
        if not direction_bias:
            return NEUTRAL_DIRECTION_FACTOR
        if direction == direction_bias:
            return DOMINANT_DIRECTION_FACTOR
        return NEUTRAL_DIRECTION_FACTOR

    def _family_factor(self, family: str, market_regime: str, regime_tag: str, fallback: bool = False) -> float:
        family = str(family or "").lower()
        market_regime = str(market_regime or "flat")
        regime_tag = str(regime_tag or "all")
        base = float(REGIME_FAMILY_FACTORS.get(market_regime, {}).get(family, DEFAULT_FAMILY_FACTOR))
        if fallback:
            return round(max(0.7, min(base, 1.05)), 4)
        if regime_tag == market_regime:
            return round(base, 4)
        if self._candidate_matches_regime(regime_tag, market_regime):
            return round(max(0.75, base * 0.95), 4)
        return round(max(0.6, base * 0.85), 4)

    def _run_candidate_vote(
        self,
        scoped: pd.DataFrame,
        candidate: dict[str, Any],
        market_regime: str,
        direction_bias: str | None = None,
        fallback: bool = False,
    ) -> dict[str, Any] | None:
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
        candidate_regime_tag = str(candidate.get("regime_tag", "all"))
        candidate_family = str(candidate.get("family", ""))
        regime_factor = self._regime_factor(candidate_regime_tag, market_regime, fallback=fallback)
        activity_factor = self._activity_factor(recent_signals_short, recent_signals, bars_since_last_entry, bars_since_last_position)
        direction_factor = self._direction_factor(str(candidate.get("direction", "")), direction_bias)
        family_factor = self._family_factor(candidate_family, market_regime, candidate_regime_tag, fallback=fallback)
        effective_weight = base_weight * regime_factor * activity_factor * direction_factor * family_factor
        hard_weight = effective_weight * (1.1 if entry_signal != 0 else 1.0)

        soft_direction = 0
        soft_reason = None
        soft_weight = 0.0
        if desired_position == 0 and entry_signal == 0 and effective_weight > 0:
            if recent_signals_short > 0 and bars_since_last_entry is not None and bars_since_last_entry <= SOFT_SIGNAL_LOOKBACK_BARS:
                last_recent_nonzero = int(entry_series[entry_series != 0].iloc[-1])
                soft_direction = 1 if last_recent_nonzero > 0 else -1
                soft_weight = effective_weight * SOFT_SIGNAL_WEIGHT_FACTOR
                soft_reason = f"recent_entry_within_{SOFT_SIGNAL_LOOKBACK_BARS}_bars"
            elif bars_since_last_position is not None and bars_since_last_position <= min(5, SOFT_SIGNAL_LOOKBACK_BARS):
                last_recent_position = int(position_series[position_series != 0].iloc[-1])
                soft_direction = 1 if last_recent_position > 0 else -1
                soft_weight = effective_weight * (SOFT_SIGNAL_WEIGHT_FACTOR * 0.75)
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
            "base_weight": round(base_weight, 4),
            "effective_weight": round(effective_weight, 4),
            "activity_factor": round(activity_factor, 4),
            "regime_factor": round(regime_factor, 4),
            "direction_factor": round(direction_factor, 4),
            "family_factor": round(family_factor, 4),
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

        diagnostics["evaluated_candidates"] = []

        direction_counts = {"long": 0, "short": 0}
        for candidate in candidates_for_regime:
            direction = str(candidate.get("direction", "")).lower()
            if direction in direction_counts:
                direction_counts[direction] += 1
        direction_bias = None
        if direction_counts["long"] >= direction_counts["short"] * 2 and direction_counts["long"] >= 3:
            direction_bias = "long"
        elif direction_counts["short"] >= direction_counts["long"] * 2 and direction_counts["short"] >= 3:
            direction_bias = "short"

        diagnostics["direction_counts"] = direction_counts
        diagnostics["direction_bias"] = direction_bias
        diagnostics["regime_family_factors"] = REGIME_FAMILY_FACTORS.get(market_regime, {})

        primary_vote_rows: list[dict[str, Any]] = []
        for candidate in candidates_for_regime:
            vote_row = self._run_candidate_vote(scoped, candidate, market_regime, direction_bias=direction_bias, fallback=False)
            if vote_row is not None:
                primary_vote_rows.append(vote_row)
                diagnostics["evaluated_candidates"].append(vote_row)

        vote_long, vote_short, recent_signal_count, selected_candidates, vote_breakdown = self._aggregate_votes(primary_vote_rows)
        diagnostics["evaluated_primary"] = len(primary_vote_rows)
        diagnostics["selected_primary"] = len(selected_candidates)
        diagnostics["primary_vote_breakdown"] = vote_breakdown
        diagnostics["primary_effective_weight_total"] = round(sum(float(r.get("effective_weight", 0.0)) for r in primary_vote_rows), 4)

        if not selected_candidates:
            fallback_candidates = active_candidates[: min(5, len(active_candidates))]
            fallback_vote_rows: list[dict[str, Any]] = []
            for candidate in fallback_candidates:
                vote_row = self._run_candidate_vote(scoped, candidate, market_regime, direction_bias=direction_bias, fallback=True)
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
            diagnostics["fallback_effective_weight_total"] = round(sum(float(r.get("effective_weight", 0.0)) for r in fallback_vote_rows), 4)

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
            diagnostics["fallback_effective_weight_total"] = 0.0

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
            "direction_bias": direction_bias,
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
