from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"


def _get(name: str, default):
    return os.getenv(name, default)


def _get_int(name: str, default: int):
    return int(_get(name, default))


def _get_float(name: str, default: float):
    return float(_get(name, default))


def _get_bool(name: str, default: bool):
    return str(_get(name, str(default))).lower() in ("1", "true", "yes")


@dataclass
class DataSettings:
    symbol: str = "BTCUSDT"
    category: str = "linear"
    interval_main: str = "15"
    interval_htf: str = "30"
    bars_15m: int = 2880
    bars_30m: int = 1440
    refresh_before_run: bool = True
    allow_stale_fallback: bool = True


@dataclass
class PolicySettings:
    active_candidates_file: Path = REPORTS_DIR / "active_candidates.json"
    max_active_candidates: int = 12
    decision_threshold: float = 0.15
    recent_bars_for_evaluation: int = 400
    live_window_bars: int = 800
    min_candidate_score: float = -10.0


@dataclass
class RiskSettings:
    max_position_usdt: float = 50.0
    max_daily_loss_pct: float = 2.0
    max_consecutive_losses: int = 3
    allow_long: bool = True
    allow_short: bool = True
    one_position_only: bool = True
    take_profit_pct: float = 0.01
    stop_loss_pct: float = 0.005


@dataclass
class ExecutionSettings:
    mode: str = "paper"
    testnet: bool = True
    api_key: str = ""
    api_secret: str = ""


@dataclass
class RuntimeSettings:
    poll_seconds: int = 20
    state_file: Path = DATA_DIR / "live_state.json"


@dataclass
class AppSettings:
    data: DataSettings
    policy: PolicySettings
    risk: RiskSettings
    execution: ExecutionSettings
    runtime: RuntimeSettings


def load_settings() -> AppSettings:
    return AppSettings(
        data=DataSettings(
            symbol=_get("AI_TRADER_SYMBOL", "BTCUSDT"),
            bars_15m=_get_int("AI_TRADER_BARS_15M", 2880),
            bars_30m=_get_int("AI_TRADER_BARS_30M", 1440),
            refresh_before_run=_get_bool("AI_TRADER_REFRESH_BEFORE_RUN", True),
            allow_stale_fallback=_get_bool("AI_TRADER_ALLOW_STALE_FALLBACK", True),
        ),
        policy=PolicySettings(
            active_candidates_file=Path(
                _get("AI_TRADER_ACTIVE_CANDIDATES_FILE", str(REPORTS_DIR / "active_candidates.json"))
            ),
            max_active_candidates=_get_int("AI_TRADER_MAX_ACTIVE_CANDIDATES", 12),
            decision_threshold=_get_float("AI_TRADER_DECISION_THRESHOLD", 0.15),
            recent_bars_for_evaluation=_get_int("AI_TRADER_RECENT_BARS_FOR_EVALUATION", 400),
            live_window_bars=_get_int("AI_TRADER_LIVE_WINDOW_BARS", 800),
            min_candidate_score=_get_float("AI_TRADER_MIN_CANDIDATE_SCORE", -10.0),
        ),
        risk=RiskSettings(
            max_position_usdt=_get_float("AI_TRADER_MAX_POSITION_USDT", 50.0),
            max_daily_loss_pct=_get_float("AI_TRADER_MAX_DAILY_LOSS_PCT", 2.0),
            max_consecutive_losses=_get_int("AI_TRADER_MAX_CONSECUTIVE_LOSSES", 3),
            allow_long=_get_bool("AI_TRADER_ALLOW_LONG", True),
            allow_short=_get_bool("AI_TRADER_ALLOW_SHORT", True),
            one_position_only=_get_bool("AI_TRADER_ONE_POSITION_ONLY", True),
            take_profit_pct=_get_float("AI_TRADER_TAKE_PROFIT_PCT", 0.01),
            stop_loss_pct=_get_float("AI_TRADER_STOP_LOSS_PCT", 0.005),
        ),
        execution=ExecutionSettings(
            mode=_get("AI_TRADER_MODE", "paper"),
            testnet=_get_bool("AI_TRADER_TESTNET", True),
            api_key=_get("BYBIT_API_KEY", ""),
            api_secret=_get("BYBIT_API_SECRET", ""),
        ),
        runtime=RuntimeSettings(),
    )
