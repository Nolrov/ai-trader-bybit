from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import load_settings
from execution.bybit_executor import BybitExecutor


def main():
    settings = load_settings()

    print("=== BYBIT TESTNET SMOKE TEST ===")
    print(f"mode: {settings.execution.mode}")
    print(f"symbol: {settings.data.symbol}")
    print(f"category: {settings.data.category}")
    print(f"testnet: {settings.execution.testnet}")
    print(f"max_position_usdt: {settings.risk.max_position_usdt}")
    print()

    if settings.execution.mode != "testnet":
        raise RuntimeError("Smoke test requires AI_TRADER_MODE=testnet")

    if not settings.execution.api_key or not settings.execution.api_secret:
        raise RuntimeError("Missing BYBIT_API_KEY / BYBIT_API_SECRET for testnet smoke test")

    executor = BybitExecutor(settings.execution)

    last_price = executor.get_last_price(
        symbol=settings.data.symbol,
        category=settings.data.category,
    )

    raw_qty = settings.risk.max_position_usdt / last_price
    limit_price = last_price * 0.995  # ставим покупку чуть ниже рынка

    print(f"last_price: {last_price}")
    print(f"raw_qty_from_usdt: {raw_qty}")
    print(f"limit_price_before_normalization: {limit_price}")
    print()

    result = executor.place_order(
        symbol=settings.data.symbol,
        side="Buy",
        qty=raw_qty,
        price=last_price,
        category=settings.data.category,
        order_type="Limit",
        reduce_only=False,
        limit_price=limit_price,
    )

    print("=== EXECUTION RESULT ===")
    print(f"ok: {result.ok}")
    print(f"mode: {result.mode}")
    print(f"symbol: {result.symbol}")
    print(f"side: {result.side}")
    print(f"qty: {result.qty}")
    print(f"order_type: {result.order_type}")
    print(f"message: {result.message}")
    print("raw:")
    print(result.raw)

    if not result.ok:
        raise RuntimeError(f"Smoke test failed: {result.message}")

    print()
    print("Smoke test limit order sent successfully.")


if __name__ == "__main__":
    main()