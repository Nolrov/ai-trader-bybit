from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
        return candidates[: int(self.policy_settings.max_active_candidates)]

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
        if not regime_tag or regime_tag == "all":
            return True
        if regime_tag == market_regime:
            return True
        if regime_tag == "trend" and market_regime == "trend_high_vol":
            return True
        if regime_tag == "high_vol" and market_regime == "trend_high_vol":
            return True
        return False

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

    def decide(self, df: pd.DataFrame) -> PolicyDecision:
        if df.empty:
            raise RuntimeError("policy_input_dataframe_is_empty")

        market_regime = self._detect_market_regime(df)
        active_candidates = self._filter_direction(self._load_active_candidates())
        if not active_candidates:
            raise RuntimeError("no_active_candidates_after_direction_filter")

        scoped = df.tail(int(self.policy_settings.recent_bars_for_evaluation)).copy()
        candidates_for_regime = [
            c for c in active_candidates if self._candidate_matches_regime(str(c.get("regime_tag", "all")), market_regime)
        ]
        if not candidates_for_regime:
            candidates_for_regime = active_candidates

        vote_long = 0.0
        vote_short = 0.0
        selected_candidates: list[dict[str, Any]] = []
        recent_signal_count = 0

        for candidate in candidates_for_regime:
            df_signal = apply_candidate(scoped, candidate)
            if df_signal.empty:
                continue

            last = df_signal.iloc[-1]
            desired_position = int(last.get("position", 0))
            entry_signal = int(last.get("entry_signal", 0))
            recent_signal_count += int((df_signal.tail(100)["entry_signal"] != 0).sum())

            raw_score = float(candidate.get("score", 0.0))
            weight = max(0.1, raw_score)
            if str(candidate.get("regime_tag", "all")) == market_regime:
                weight *= 1.15

            if desired_position > 0:
                vote_long += weight
            elif desired_position < 0:
                vote_short += weight

            if desired_position != 0 or entry_signal != 0:
                selected_candidates.append(
                    {
                        "candidate_key": candidate.get("candidate_key"),
                        "family": candidate.get("family"),
                        "direction": candidate.get("direction"),
                        "regime_tag": candidate.get("regime_tag"),
                        "score": raw_score,
                        "entry_signal": entry_signal,
                        "desired_position": desired_position,
                        "weight": round(weight, 4),
                    }
                )

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
        )
