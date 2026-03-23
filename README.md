# AI Trader — Bybit BTCUSDT (15m / 30m)

## Назначение

Система алгоритмической торговли для BTCUSDT на Bybit. Это не single-strategy бот, а **research-driven multi-strategy system**:

- Research Layer генерирует, тестирует и отбирает стратегии
- Alpha Bank хранит рабочие кандидаты в нескольких состояниях
- Policy Layer управляет их влиянием в зависимости от рынка
- Risk + Execution исполняют только разрешённые решения

Инструмент:
- Bybit
- BTCUSDT (USDT perpetual)

Таймфреймы:
- 15m — основной
- 30m — старший

---

## Ключевая идея проекта

Целевая схема проекта:

`research → validated/active alpha bank → policy decision → risk check → execution`

Система не должна:
- жить на одном `candidate_id`
- вручную переключать стратегии
- зависеть от ручного включения/выключения гипотез

Система должна:
- хранить широкий research pool
- поднимать или понижать влияние стратегий автоматически
- адаптироваться к рынку через policy, а не через ручные тумблеры

---

## Текущий статус

Проект уже не находится в стадии “инфраструктура без логики”. На текущий момент:

Подтверждено:
- единый data layer работает
- автоматическое обновление market data работает
- alpha miner работает
- run_candidate работает через `candidate_key`, а не через `candidate_id`
- active bank формируется автоматически
- policy layer существует и используется в live
- live loop стабилен в `paper` режиме
- execution и risk manager встроены в pipeline
- logging и state работают
- есть diagnostics для policy
- есть `policy_replay` для анализа решений на истории

Не готово:
- quality short-side всё ещё слабее long-side
- часть PA-стратегий слабая или редкая
- банк ещё не идеально сбалансирован по режимам и направлениям
- live trade observability (PnL / execution detail) ещё не доведена до финального вида
- mainnet/live trading ещё не включён

Главное:
> архитектурный переход от single-strategy к adaptive alpha bank уже сделан.

---

## Что изменилось относительно старой версии

Старая модель проекта опиралась на один выбранный кандидат и ручной `candidate_id` в настройках. Это больше не актуально. По историческому README это был временный переходный механизм fileciteturn5file0.

Текущая модель:
- `candidate_id` больше не является центром live-логики
- research и live работают через банк кандидатов
- policy выбирает и взвешивает стратегии динамически
- стратегии не удаляются вручную из системы, их влияние меняется автоматически

---

## Архитектура

### 1. Data Layer

Единая точка входа в market data pipeline:

`market_data_manager`

Контракт:
- проверяет локальный кэш
- проверяет freshness
- при необходимости обновляет данные через `bybit_loader`
- возвращает готовые dataframes для работы

Жёсткие инварианты:
- CSV/parquet — это кэш, а не источник истины
- источник истины для market data — биржа
- торговая логика не должна напрямую читать market CSV
- торговая логика не должна напрямую вызывать низкоуровневый loader

Основные модули:
- `src/data/bybit_loader.py`
- `src/data/market_data_manager.py`
- `src/data/data_processor.py`

---

### 2. Research Layer

Research Layer отвечает за генерацию и оценку гипотез.

Основные задачи:
- строить признаки
- генерировать кандидатов стратегий
- прогонять backtest / walk-forward
- считать метрики качества
- формировать банк кандидатов

Основные точки:
- `src/research/alpha_miner.py`
- `src/research/run_candidate.py`
- `src/research/strategies/*`

Текущая направленность research:
- Price Action family
- breakout / momentum / mean reversion / ATR breakout
- PA pullback / false breakout / range rejection / breakout retest

---

### 3. Alpha Bank

У банка три уровня:

#### Raw
Все сгенерированные гипотезы.

#### Validated
Кандидаты, прошедшие quality gate.

#### Active
Кандидаты, которые доступны policy для live influence.

Важно:
- стратегия не должна удаляться вручную только потому, что сейчас не активна
- research pool должен оставаться широким
- policy должен регулировать влияние кандидата автоматически

Типовые файлы отчётов:
- `reports/alpha_miner_wf.csv`
- `reports/validated_alphas.csv`
- `reports/validated_candidates.json`
- `reports/active_candidates.json`

---

### 4. Policy Layer

Policy Layer уже существует и является центральным управляющим механизмом.

Основная идея:
> policy не удаляет и не “включает” стратегии руками, а считает их **effective influence**.

Текущая формула по смыслу:

`effective_weight = base_weight * regime_factor * activity_factor * direction_factor`

Где:
- `base_weight` — качество стратегии из research
- `regime_factor` — соответствие текущему рыночному режиму
- `activity_factor` — недавняя реальная активность стратегии
- `direction_factor` — мягкая коррекция перекоса направления

Это означает:
- стратегия не исчезает из research
- стратегия не требует ручного re-enable в будущем
- если рынок ей подходит, её влияние растёт автоматически
- если рынок ей не подходит, её влияние падает автоматически

Дополнительно в policy уже реализовано:
- diagnostics candidate-level
- compatible regime matching
- soft-vote / hard-signal breakdown
- replay-анализ на окне последних N баров

Основные модули:
- `src/policy/policy_manager.py`
- `src/policy/policy_replay.py`

---

### 5. Risk + Execution

Execution не генерирует идеи и не выбирает стратегию. Он исполняет только то, что прошло policy и risk.

Pipeline:
`exchange_sync → market_data_manager → data_processor → policy → desired_position → risk_manager → execution → state → logging`

Risk manager:
- принимает `desired_position`
- сравнивает его с текущим состоянием
- либо разрешает изменение
- либо запрещает изменение

Жёсткий инвариант:
> любое изменение позиции проходит через risk manager

Основные модули:
- `src/live/live_loop.py`
- `src/risk/risk_manager.py`
- `src/execution/bybit_executor.py`
- `src/live/state_store.py`

---

## Текущее реальное поведение системы

### Live
Live работает в `paper` режиме и корректно проходит весь pipeline.

### Policy
Policy уже не “немой”.
Replay по последним барам показал, что policy принимает ненулевые решения на заметной доле баров. Это значит:
- система не сломана
- adaptive bank используется
- проблема уже не в отсутствии policy как класса

### Bias
Главный текущий research-проблемный узел:
- long-side сильнее short-side
- short присутствует, но менее конкурентоспособен
- перекос теперь объясняется качеством стратегий, а не архитектурной блокировкой short

### Price Action
PA-направление уже интегрировано и является базовым направлением исследования, но часть PA-family нуждается в доработке по качеству сигналов и реальной применимости.

---

## Что уже исправлено

Ниже перечислены изменения, которые считаются уже внедрёнными и не должны быть “откачены” в новом диалоге:

- убран ручной `candidate_id` как центр live-решений
- `run_candidate` переведён на `candidate_key`
- сформирован active bank
- добавлен policy manager
- добавлена прозрачная diagnostics-логика policy
- добавлен `policy_replay`
- добавлена совместимость режимов, а не только strict equality
- short больше не режется архитектурно
- добавлен dynamic effective weight
- live отделён от full-history research window
- убран lookahead в части PA swing-features
- live работает через ограниченное runtime window

---

## Что НЕ надо делать

Следующие вещи считаются ошибочными направлениями:

- не возвращать проект к single-strategy execution
- не возвращать ручной `candidate_id` в центр live
- не удалять стратегии вручную из research pool
- не выключать стратегии руками “на время”
- не форсить баланс long/short искусственно
- не пытаться лечить всё через policy, если проблема в качестве research-layer
- не включать mainnet, пока paper/testnet и observability не закрыты

---

## Главные текущие проблемы

### 1. Short-side слабее long-side
Short уже есть, но по качеству и вкладу в решения он отстаёт.

### 2. Часть PA-family слабая или редкая
Некоторые PA-кандидаты либо дают мало сигналов, либо слишком слабо участвуют в policy.

### 3. Soft-vote всё ещё влияет на поведение
Он нужен, но его вклад должен оставаться вторичным относительно реальных сигналов.

### 4. Наблюдаемость live ещё не завершена
Полноценный live PnL / fill / execution observability ещё не является финализированным слоем.

---

## Следующие этапы

### Этап 1 — Усиление research-layer
Приоритет:
- усилить short-side
- улучшить реальные PA-short setups
- улучшить качество кандидатов, а не просто их количество

Фокус:
- `pa_false_breakout short`
- `pa_breakout_retest short`
- `momentum_continuation short`
- усиление flat/high_vol short-логики

Цель:
> сделать short конкурентоспособным, а не просто присутствующим

---

### Этап 2 — Доработка active bank и replay-driven цикла
После каждого research-изменения обязательно:
- пересобрать банк
- прогнать replay
- проверить contribution и activity кандидатов

Команды:
```bash
python src/research/alpha_miner.py
python src/research/run_candidate.py --list-active
python src/policy/policy_replay.py --bars 800
```

Цель:
> принимать решения по данным replay, а не по одному последнему бару

---

### Этап 3 — Paper trading as validation gate
Когда research-layer станет устойчивее:
- регулярно гонять live в `paper`
- смотреть частоту решений
- проверять, не ломает ли policy хороший research
- дорабатывать observability и execution detail

Цель:
> доказать, что система стабильно ведёт себя как трейдер в paper-runtime, а не только в backtest

---

### Этап 4 — Testnet trading
После устойчивого paper:
- подключить testnet execution
- проверить ордера, состояние, восстановление после ошибок
- проверить risk / order lifecycle / state sync
- подтвердить, что runtime переживает реальные API-циклы

Цель:
> перейти от paper-сигналов к реальному исполнению в безопасной среде

---

### Этап 5 — Mainnet с минимальным риском
Только после успешного testnet:
- минимальный размер позиции
- жёсткие risk limits
- расширенное логирование
- circuit-breaker / stop conditions
- пошаговое расширение объёма

Цель:
> не “запустить торговлю любой ценой”, а выйти в реальную торговлю контролируемо и обратимо

---

## Принцип движения проекта

План проекта не должен заканчиваться только исследованием стратегий. Конечная цель — живая торговая система.

Правильная цепочка развития:

`research quality → policy stability → paper validation → testnet execution → mainnet rollout`

То есть:
- сначала строим рабочий банк
- потом подтверждаем устойчивость на replay/paper
- потом выходим в testnet
- потом в реальную торговлю

---

## Метрики, которые сейчас важны

### Research
- test_return
- test_sharpe
- test_drawdown
- trades count
- profit factor
- train/test stability

### Policy
- nonzero decisions ratio
- long/short split
- candidate contribution
- raw entry count vs soft vote count
- regime coverage

### Live/Paper
- стабильность runtime
- отсутствие stale/future leakage
- корректность state sync
- корректность risk decisions

---

## Команды

### Обновить research и банк кандидатов
```bash
python src/research/alpha_miner.py
```

### Посмотреть active bank
```bash
python src/research/run_candidate.py --list-active
```

### Прогнать одного кандидата
```bash
python src/research/run_candidate.py --candidate-key <KEY> --save-report
```

### Один цикл live
```bash
python src/live/live_loop.py --once
```

### Replay policy на недавнем окне
```bash
python src/policy/policy_replay.py --bars 800
```

---

## Инварианты проекта

- market data входят в систему через единый data layer
- live не должен использовать future leakage
- без успешного `exchange_sync` торговля запрещена
- risk manager всегда стоит перед execution
- отсутствие сигнала — нормальное поведение
- биржа — источник истины по позиции
- state — производный слой
- стратегии не удаляются вручную из research pool
- influence стратегий должен регулироваться автоматически

---

## Статус по контурам

### Data
Готово.

### Research
Работает, но требует дальнейшего усиления качества short-side и PA-family.

### Policy
Существует, встроен в live и уже является рабочим управляющим слоем.

### Execution
Готов для paper/testnet цикла.

### Trading
Paper — активен.  
Testnet — следующий реальный операционный этап после стабилизации research/policy.  
Mainnet — только после testnet.

---

## Ключевой вывод

Проект больше не является “инфраструктурой без торгового мозга”.

Сейчас это:
> multi-strategy adaptive trading system в стадии research/paper validation

Следующий приоритет:
> усилить quality research-layer, удержать adaptive policy, затем довести систему до testnet и только после этого до реальной торговли.
