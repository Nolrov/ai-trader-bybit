# AI Crypto Trader (Bybit, BTCUSDT, 15m/30m)

## Mission
Собрать систему:
data → features → alpha → decision → execution  
и довести её до Bybit testnet.

---

## Current Operating Mode

- Архитектура: multi-alpha (НЕ отменяется)
- Текущий режим: single-alpha execution
- Research: временно заморожен
- Цель: запустить paper/testnet цикл

Активная альфа:
candidate_id = 254 (trend_pullback long)

---

## What Works Now

### Data
- bybit_loader.py — загружает BTCUSDT 15m/30m
- пишет в /data
- есть validation + freshness

### Processing
- data_processor.py — собирает dataset
- feature_factory.py — считает фичи

### Research
- alpha_miner.py
  - генерирует кандидатов
  - считает метрики
  - поддерживает --refresh-data

- run_candidate.py
  - тест одного кандидата
  - поддерживает --refresh-data

### Backtest
- engine.py
- комиссия учитывается

---

## What DOES NOT Exist Yet

- execution (биржа)
- paper trading loop
- risk manager
- alpha bank (как система)
- policy manager (дирижёр)

---

## Entrypoints

### Обновить данные
python src/data/bybit_loader.py

### Alpha miner
python src/research/alpha_miner.py
python src/research/alpha_miner.py --refresh-data

### Один кандидат
python src/research/run_candidate.py --candidate-id 254
python src/research/run_candidate.py --candidate-id 254 --refresh-data

---

## Active Alpha (temporary)

ID: 254  
family: trend_pullback  
direction: long  

Используется для первого запуска execution.

---

## Next Step (ONLY)

Сделать вертикальный срез:

1. live_loop.py
   - обновляет данные
   - считает сигнал (1 альфа)
   - принимает решение

2. risk_manager.py
   - 1 позиция
   - фикс размер
   - запрет дублирования

3. bybit_executor.py
   - testnet
   - market order
   - close position

---

## Rules

- НЕ добавлять новые стратегии
- НЕ менять alpha_miner
- НЕ делать дирижёр
- НЕ делать DRL

Пока не работает:

data → signal → order → close

---

## Future (после testnet)

- добавить 2–3 альфы
- включить alpha bank
- добавить простой policy manager
- затем DRL

---

## Core Principle

Сейчас:

multi-alpha architecture  
single-alpha execution  

Это временно.

## System Map (entry files)

### Project state
- README.md
  https://github.com/Nolrov/ai-trader-bybit/blob/main/README.md

### Data
- bybit_loader.py
  https://github.com/Nolrov/ai-trader-bybit/blob/main/src/data/bybit_loader.py

### Processing
- data_processor.py
  https://github.com/Nolrov/ai-trader-bybit/blob/main/src/processing/data_processor.py
- feature_factory.py
  https://github.com/Nolrov/ai-trader-bybit/blob/main/src/features/feature_factory.py

### Backtest
- engine.py
  https://github.com/Nolrov/ai-trader-bybit/blob/main/src/backtest/engine.py
- analyze_trades.py
  https://github.com/Nolrov/ai-trader-bybit/blob/main/src/backtest/analyze_trades.py

### Research
- rule_builder.py
  https://github.com/Nolrov/ai-trader-bybit/blob/main/src/research/rule_builder.py
- validators.py
  https://github.com/Nolrov/ai-trader-bybit/blob/main/src/research/validators.py
- alpha_miner.py
  https://github.com/Nolrov/ai-trader-bybit/blob/main/src/research/alpha_miner.py
- run_candidate.py
  https://github.com/Nolrov/ai-trader-bybit/blob/main/src/research/run_candidate.py

### Legacy / ignore
- strategy.py (obsolete, do not use)
  https://github.com/Nolrov/ai-trader-bybit/blob/main/src/backtest/strategy.py