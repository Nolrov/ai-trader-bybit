from __future__ import annotations

import argparse

from src.config.settings import LOGS_DIR, load_settings
from src.live.live_loop import run_live_loop
from src.utils.runtime_logger import RuntimeLogger
from src.app.services import ensure_candidate_bank


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Unified AI Trader app runner')
    parser.add_argument('--once', action='store_true', help='Run one live cycle and exit')
    parser.add_argument('--rebuild-bank', action='store_true', help='Force alpha bank rebuild before starting')
    parser.add_argument('--bank-only', action='store_true', help='Only refresh data and rebuild/validate bank, then exit')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings()
    logger = RuntimeLogger(LOGS_DIR)

    logger.info(
        f'app_start mode={settings.execution.mode} testnet={settings.execution.testnet} '
        f'symbol={settings.data.symbol}'
    )
    ensure_candidate_bank(settings, logger=logger, force_rebuild=args.rebuild_bank)

    if args.bank_only:
        logger.info('app_exit reason=bank_only_completed')
        return

    run_live_loop(settings=settings, logger=logger, once=args.once)


if __name__ == '__main__':
    main()
