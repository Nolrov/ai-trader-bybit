# AI Crypto Trader (Bybit, BTCUSDT)

## Overview
Исследовательская торговая система для BTC/USDT на Bybit с основными таймфреймами 15m и 30m.

Цель проекта — не писать стратегии вручную под текущий рынок, а автоматически:
1. генерировать гипотезы,
2. тестировать их,
3. отбирать устойчивые,
4. использовать лучшие идеи в торговле.

## Core Idea
Система не должна зависеть от одной стратегии.

Правильный цикл:
- data -> features -> research -> validation -> alpha bank -> policy -> execution

То есть проект строится как исследовательский трейдер, а не как один фиксированный торговый алгоритм.

## Architecture

### A. Data & Features
- загрузка OHLCV с Bybit
- работа с 15m и 30m
- очистка и валидация свечей
- индикаторы и признаки
- price action признаки

### B. Research
- генерация кандидатов стратегий
- бэктест гипотез
- расчёт метрик
- логирование результатов
- alpha miner v1

### C. Selection & Execution
Пока не реализовано полностью:
- walk-forward validation
- alpha bank
- strategy/policy selection
- risk manager
- paper trading
- live execution через Bybit API

## Current Status
Done:
- data loader for Bybit
- candle validation
- feature engineering
- 15m/30m merged dataset
- backtest prototype
- trade log and summary reports
- alpha miner v1 for price action candidates

In progress:
- project refactoring
- walk-forward validation

Not implemented yet:
- alpha bank
- policy manager
- risk manager
- paper trading
- testnet trading
- live trading

## Current Limitations
Текущие результаты alpha miner пока не подтверждены walk-forward проверкой.

Это значит:
- найденные кандидаты могут быть переобучены,
- текущие стратегии нельзя считать готовыми к real trading,
- проект находится на research stage.

## Roadmap
1. Refactor project structure
2. Add walk-forward validation
3. Filter stable candidates
4. Build alpha bank
5. Add policy manager
6. Add paper trading
7. Connect Bybit testnet
8. Move to controlled live trading

## Project Structure
```text
src/
  data/        # data loading
  features/    # indicators and feature generation
  processing/  # dataset preparation
  research/    # alpha miner and validators
  backtest/    # backtest and trade analysis
