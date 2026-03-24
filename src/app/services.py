from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config.settings import REPORTS_DIR, AppSettings
from src.research.alpha_miner import run_alpha_miner


@dataclass
class BankEnsureResult:
    rebuilt: bool
    reason: str
    active_candidates_file: Path
    active_candidates_count: int


def _count_active_candidates(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        import json
        payload = json.loads(path.read_text(encoding="utf-8"))
        return len(payload) if isinstance(payload, list) else 0
    except Exception:
        return 0


def ensure_candidate_bank(settings: AppSettings, logger: Any | None = None, *, force_rebuild: bool = False) -> BankEnsureResult:
    path = Path(settings.policy.active_candidates_file)
    reason = 'existing_bank_ok'
    rebuilt = False

    if force_rebuild or not path.exists() or _count_active_candidates(path) == 0:
        if force_rebuild:
            reason = 'forced_rebuild'
        elif not path.exists():
            reason = 'missing_active_candidates_bank'
        else:
            reason = 'empty_active_candidates_bank'

        if logger is not None:
            logger.info(f'candidate_bank_rebuild_started reason={reason} path={path}')
        run_alpha_miner()
        rebuilt = True
        if logger is not None:
            logger.info(f'candidate_bank_rebuild_finished path={path}')

    count = _count_active_candidates(path)
    if logger is not None:
        logger.info(
            f'candidate_bank_ready rebuilt={rebuilt} reason={reason} '
            f'active_candidates={count} path={path}'
        )
    return BankEnsureResult(
        rebuilt=rebuilt,
        reason=reason,
        active_candidates_file=path,
        active_candidates_count=count,
    )
