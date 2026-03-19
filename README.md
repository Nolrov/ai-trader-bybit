# AI Crypto Trader (Bybit, BTCUSDT, 15m/30m)

## Mission

Построить систему:

data → features → strategies → alpha mining → decision → execution

и довести её до стабильной торговли на Bybit (сначала testnet).

---

## Current State (реальное состояние проекта)

Система находится на этапе **research + честная оценка стратегий**.

Что уже есть:

* корректные данные (без leakage)
* multi-timeframe (15m + 30m)
* alpha miner (перебор стратегий)
* стратегии вынесены в отдельные модули (plugins)
* честный train/test split
* базовая фильтрация кандидатов

Что важно:

* система больше **не переоценивает стратегии**
* теперь большинство кандидатов отваливается — это нормально

---

## Architecture

### Data

* `bybit_loader.py`

  * загружает BTCUSDT (15m / 30m)
  * чистит данные (дубликаты, OHLC)
  * проверяет свежесть

---

### Processing

* `data_processor.py`

  * объединяет 15m + 30m
  * устраняет lookahead leakage (30m сдвиг)

* `feature_factory.py`

  * базовые фичи (body, breakout и т.д.)

---

### Strategies (НОВАЯ АРХИТЕКТУРА)

Путь:

```
src/research/strategies/
```

Каждая стратегия — отдельный файл:

* breakout.py
* trend_pullback.py
* mean_reversion.py

Регистрация:

* `registry.py`

Важно:

* стратегии больше НЕ зашиты в коде
* rule_builder генерирует параметры, но не содержит логики

---

### Research

* `alpha_miner.py`

  * перебирает кандидатов
  * считает метрики
  * выводит TOP
  * поддерживает `--refresh-data`

* `run_candidate.py`

  * детальный анализ одной стратегии

---

### Backtest

* `engine.py`

  * считает PnL
  * учитывает комиссию
  * строит equity

---

## Entrypoints

### Обновить данные

```bash
python src/research/alpha_miner.py --refresh-data
```

---

### Найти лучшие стратегии

```bash
python src/research/alpha_miner.py
```

---

### Проверить стратегию

```bash
python src/research/run_candidate.py --candidate-id 270
```

---

## Important Realization

Если раньше были хорошие результаты, а сейчас:

```
VALID TOP = пусто
```

Это означает:

* раньше был leakage
* сейчас система стала честной

Это **хорошо**, а не плохо.

---

## Current Problem

Сейчас стратегии:

* слишком простые
* не учитывают режим рынка

Поэтому:

* либо не проходят фильтр
* либо нестабильны

---

## What Needs To Be Improved

### 1. Market Regime (КРИТИЧНО)

Добавить:

* volatility regime (ATR / range)
* trend strength (EMA distance)

Использование:

* breakout → только high vol
* mean reversion → только low vol
* trend → только сильный тренд

---

### 2. Улучшение стратегий

Не добавлять много, а улучшать текущие:

* breakout с фильтром ATR
* trend_pullback с фильтром тренда
* убрать шумовые комбинации

---

### 3. Нормальный скоринг

Сейчас можно усилить:

* штраф за drawdown
* штраф за малое число трейдов
* штраф за нестабильность

---

### 4. Rolling / Walk-forward (позже)

Сейчас простой split:

```
train → test
```

Дальше нужно:

```
train → validate → test
или rolling окна
```

---

## What NOT To Do

Пока не нужно:

* DRL
* execution
* сложный policy manager
* overengineering

---

## Next Steps (по порядку)

1. Добавить **trend strength filter**
2. Добавить **volatility regime**
3. Улучшить breakout и trend_pullback
4. Добиться появления стабильных кандидатов
5. Только после этого → execution

---

## Core Principle

Система должна:

```
не искать идеальные стратегии,
а отбрасывать плохие
```

Если после фильтрации почти ничего не осталось:

→ фильтр работает правильно

---

## Summary

Сейчас у тебя:

* честные данные
* честный бэктест
* нормальная архитектура

Следующий этап:

```
сделать стратегии умнее, а не больше
```
