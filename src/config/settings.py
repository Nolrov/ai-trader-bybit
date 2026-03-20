from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"


def _get(name: str, default):
    return os.getenv(name, default)


def _get_int(name: str, default: int):
    return int(_get(name, default))


def _get_float(name: str, default: float):
    return float(_get(name, default))


@dataclass
class DataSettings:
    symbol: str = "BTCUSDT"
    category: str = "linear"

    interval_main: str = "15"
    interval_htf: str = "30"

    bars_15m: int = 15000
    bars_30m: int = 10000


@dataclass
class StrategySettings:
    candidate_id: int = 39


@dataclass
class RiskSettings:
    max_position_usdt: float = 50.0
    max_daily_loss_pct: float = 2.0
    max_consecutive_losses: int = 3
    allow_long: bool = True
    allow_short: bool = False
    one_position_only: bool = True


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
    strategy: StrategySettings
    risk: RiskSettings
    execution: ExecutionSettings
    runtime: RuntimeSettings


def load_settings() -> AppSettings:
    return AppSettings(
        data=DataSettings(
            symbol=_get("AI_TRADER_SYMBOL", "BTCUSDT"),
            bars_15m=_get_int("AI_TRADER_BARS_15M", 15000),
            bars_30m=_get_int("AI_TRADER_BARS_30M", 10000),
        ),
        strategy=StrategySettings(
            candidate_id=_get_int("AI_TRADER_CANDIDATE_ID", 39),
        ),
        risk=RiskSettings(),
        execution=ExecutionSettings(
            mode=_get("AI_TRADER_MODE", "paper"),
            api_key=_get("BYBIT_API_KEY", ""),
            api_secret=_get("BYBIT_API_SECRET", ""),
        ),
        runtime=RuntimeSettings(),
    )