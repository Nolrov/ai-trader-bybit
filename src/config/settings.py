from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    return float(raw)


def _get_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


@dataclass
class StrategySettings:
    candidate_id: int = 39
    symbol: str = "BTCUSDT"
    category: str = "linear"
    interval_main: str = "15"
    interval_htf: str = "30"
    data_total_bars: int = 2000


@dataclass
class RiskSettings:
    max_position_usdt: float = 50.0
    max_daily_loss_pct: float = 2.0
    max_consecutive_losses: int = 3
    allow_long: bool = True
    allow_short: bool = False
    one_position_only: bool = True
    min_signal_interval_minutes: int = 15


@dataclass
class ExecutionSettings:
    mode: str = "paper"  # paper | testnet
    testnet: bool = True
    order_type: str = "Market"
    time_in_force: str = "GTC"
    recv_window: str = "5000"
    api_key: str = ""
    api_secret: str = ""


@dataclass
class RuntimeSettings:
    poll_seconds: int = 20
    fee_rate: float = 0.0006
    state_file: Path = DATA_DIR / "live_state.json"
    decisions_log_file: Path = LOGS_DIR / "live_decisions.log"
    orders_log_file: Path = LOGS_DIR / "live_orders.log"


@dataclass
class AppSettings:
    strategy: StrategySettings
    risk: RiskSettings
    execution: ExecutionSettings
    runtime: RuntimeSettings


def load_settings() -> AppSettings:
    strategy = StrategySettings(
        candidate_id=_get_int("AI_TRADER_CANDIDATE_ID", 39),
        symbol=_get_str("AI_TRADER_SYMBOL", "BTCUSDT"),
        category=_get_str("AI_TRADER_CATEGORY", "linear"),
        interval_main=_get_str("AI_TRADER_INTERVAL_MAIN", "15"),
        interval_htf=_get_str("AI_TRADER_INTERVAL_HTF", "30"),
        data_total_bars=_get_int("AI_TRADER_DATA_TOTAL_BARS", 2000),
    )

    risk = RiskSettings(
        max_position_usdt=_get_float("AI_TRADER_MAX_POSITION_USDT", 50.0),
        max_daily_loss_pct=_get_float("AI_TRADER_MAX_DAILY_LOSS_PCT", 2.0),
        max_consecutive_losses=_get_int("AI_TRADER_MAX_CONSECUTIVE_LOSSES", 3),
        allow_long=_get_bool("AI_TRADER_ALLOW_LONG", True),
        allow_short=_get_bool("AI_TRADER_ALLOW_SHORT", False),
        one_position_only=_get_bool("AI_TRADER_ONE_POSITION_ONLY", True),
        min_signal_interval_minutes=_get_int("AI_TRADER_MIN_SIGNAL_INTERVAL_MINUTES", 15),
    )

    execution = ExecutionSettings(
        mode=_get_str("AI_TRADER_MODE", "paper"),
        testnet=_get_bool("AI_TRADER_TESTNET", True),
        order_type=_get_str("AI_TRADER_ORDER_TYPE", "Market"),
        time_in_force=_get_str("AI_TRADER_TIME_IN_FORCE", "GTC"),
        recv_window=_get_str("AI_TRADER_RECV_WINDOW", "5000"),
        api_key=_get_str("BYBIT_API_KEY", ""),
        api_secret=_get_str("BYBIT_API_SECRET", ""),
    )

    runtime = RuntimeSettings(
        poll_seconds=_get_int("AI_TRADER_POLL_SECONDS", 20),
        fee_rate=_get_float("AI_TRADER_FEE_RATE", 0.0006),
        state_file=Path(_get_str("AI_TRADER_STATE_FILE", str(DATA_DIR / "live_state.json"))),
        decisions_log_file=Path(_get_str("AI_TRADER_DECISIONS_LOG", str(LOGS_DIR / "live_decisions.log"))),
        orders_log_file=Path(_get_str("AI_TRADER_ORDERS_LOG", str(LOGS_DIR / "live_orders.log"))),
    )

    return AppSettings(
        strategy=strategy,
        risk=risk,
        execution=execution,
        runtime=runtime,
    )