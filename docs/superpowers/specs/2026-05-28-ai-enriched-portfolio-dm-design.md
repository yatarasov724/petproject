# AI-Enriched Portfolio DM Alerts — Design Spec

_Дата: 2026-05-28_

---

## Контекст

Личные DM-алерты о портфеле сейчас содержат только заголовок новости и тикеры. Канальные посты проходят AI-обогащение (emoji, нормализованный заголовок, summary, market_effect) — но до личных уведомлений эти данные не доходят. Цель: перенести AI-анализ в личный DM, убрав правку канального поста.

---

## Новый формат DM

```
🔴 *Газпром снижает дивиденды*

СД рекомендовал 15 руб. — вдвое ниже ожиданий рынка

📌 На фоне двух недель давления из-за падения цен на газ

⚡ Давление на акции, возможна коррекция

$GAZP
```

**Поля:**
- `{emoji} *{ai_title}*` — 🟢/🔴 + нормализованный заголовок
- `{summary}` — что именно произошло (1 предложение)
- `📌 {context}` — почему важно сейчас (RAG, опционально — только если непустой)
- `⚡ {market_effect}` — прямой эффект на позицию
- `$tickers` — AI-тикеры если есть, иначе keyword-тикеры

---

## Архитектура

### Изменение потока

**Сейчас:**
```
tg.send()           → канал (plain)
_ai_enrich()        → edit_message → канал (AI)
_notify_portfolio() → DM (plain, без AI)
```

**После:**
```
tg.send()    → канал (plain, финально — без правок)
_ai_enrich() → RAG query → AI call
               → notify_with_ai() → DM (AI-обогащённый)
               → fallback: notify() если AI упал
```

### RAG-запрос

Перед AI-вызовом: SQL за последними 5 кластерами по тикерам события за 14 дней.

```sql
SELECT canonical_title FROM event_clusters
WHERE (tickers ILIKE '%GAZP%' OR tickers ILIKE '%NVTK%')
  AND last_updated_at > %s
  AND status IN ('published', 'updated')
ORDER BY last_updated_at DESC
LIMIT 5
```

`tickers` в БД — TEXT (`"GAZP,SBER"`). Запрос строится динамически: одно `ILIKE %s` на каждый тикер.

---

## Затронутые файлы

| Файл | Что меняется |
|------|-------------|
| `app/db/queries.py` | +`get_recent_cluster_titles_for_tickers()` |
| `app/ai/analyzer.py` | +`context` в `AIAnalysis`; +`recent_context` param; новая секция промпта |
| `app/pipeline/orchestrator.py` | убрать `edit_message`; убрать немедленный `_notify_portfolio`; RAG + portfolio в `_ai_enrich` |
| `app/bot/portfolio.py` | +`notify_with_ai()` |
| `tests/test_portfolio.py` | тесты для `notify_with_ai` и нового query |

---

## Граничные случаи

| Случай | Поведение |
|--------|-----------|
| AI недоступен / упал | Fallback: простой DM через `_notify_portfolio` |
| `openrouter_api_key` не задан | Простой DM сразу (elif ветка в orchestrator) |
| `context` пуст | Строка 📌 не включается в DM |
| AI вернул тикеры | Используем AI-тикеры вместо keyword |
| Нет подписчиков | DM не отправляется |
| Необработанное исключение | Catch + fallback DM |

---

## Верификация

1. Подписаться на тикер: `/portfolio → SBER`
2. Дождаться публикации новости с $SBER
3. Убедиться что DM содержит: 🟢/🔴, AI-заголовок, summary, ⚡ market_effect
4. Строка 📌 появляется только если есть свежие кластеры по тикеру
5. Канальный пост НЕ редактируется после публикации
6. `docker exec backend-backend-1 python -m pytest tests/test_portfolio.py -v`
