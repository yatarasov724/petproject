# Ticker Quality System — Design Spec
**Date:** 2026-05-26  
**Status:** Approved  

## Problem
Тикеры в публикуемых сигналах не соответствовали упоминаемой компании:
- МГКЛ → MTSS (загрязнение через кластеризацию + отсутствие MGKL в списке)
- Русснефть → LKOH,ROSN (commodity-stem срабатывал на имя компании)
- Озон Фармацевтика → OZON (нет OZPH в списке)

Уже сделано (2026-05-26): добавлены новые тикеры, исправлен word-boundary матчинг,
защита от загрязнения тикеров в clusterer.py.

## Goal
Системно предотвращать публикацию неверных тикеров на трёх уровнях:
1. Валидация до публикации
2. Алерт при обнаружении незнакомой компании
3. Ручное исправление через бот

## Components

### 1. TickerValidator (новый модуль)
**Файл:** `app/pipeline/ticker_validator.py`  
**Когда:** сразу перед `tg.send()` в orchestrator.py  

Логика:
- Commodity-тикеры (LKOH, ROSN, GAZP, NVTK, PLZL) — всегда валидны
- Остальные — только если keyword компании найден в `canonical_title` кластера
- Сомнительный тикер: убирается из отображения, WARNING в лог

### 2. Unknown Company Detector (в orchestrator.py)
**Когда:** при публикации события без тикеров  

Паттерны-триггеры:
- `СД [А-ЯЁ]{2,6} рекомендовал`
- `[А-ЯЁ]{2,6}: ДИВИДЕНДЫ =`
- `акции [А-ЯЁ]{2,6} (выросли|упали)`

Действие: `alerting.send_ops("unknown_company: <title>")` → admin Telegram

### 3. /fix Bot Command (в commands.py)
**Синтаксис:** `/fix <cluster_id> <TICKER>`  
**Доступ:** только admin user_id  

Действия:
1. Валидировать ticker ∈ VALID_TICKERS
2. Обновить `event_clusters.tickers` в БД
3. Найти последний `telegram_sends` для кластера
4. Отредактировать сообщение в Telegram (re-format с новым тикером)
5. Ответить: «✅ Тикер кластера #N обновлён: MGKL»

### 4. Regression Test Suite
**Файл:** `tests/test_ticker_extraction.py`  
**Запуск:** `pytest tests/test_ticker_extraction.py` при деплое  

Покрывает: 14 известных кейсов + краевые случаи commodity/conflict-resolution.

## Out of Scope
- MOEX API sync
- Изменение алгоритма кластеризации (уже реализовано)
- AI-валидация тикеров (достаточно keyword-проверки)

## Success Criteria
- Ни одна новость с МГКЛ/НМТП/Русснефть не получает чужой тикер
- Admin получает alert в течение 1 минуты при обнаружении незнакомой компании
- `/fix` корректирует опубликованное сообщение за < 5 секунд
- `pytest` проходит при каждом деплое
