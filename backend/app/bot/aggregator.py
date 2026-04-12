"""
Агрегатор сигналов:
- Дедупликация одинаковых новостей из разных источников (similarity >= 70%)
- Объединяет несколько новостей об одном тикере за 30 минут
- Cooldown: один сигнал на тикер раз в 60 минут
- Фильтр значимости: confidence >= 65%, credibility >= 60%
- HOLD отправляется только при confidence >= 80%
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
from difflib import SequenceMatcher

MIN_CONFIDENCE = 65
MIN_CREDIBILITY = 60
MIN_CONFIDENCE_HOLD = 80
AGGREGATION_WINDOW_MIN = 30
COOLDOWN_MIN = 60
DEDUP_SIMILARITY = 0.70  # порог похожести заголовков (0..1)


def _similarity(a: str, b: str) -> float:
    """Возвращает коэффициент схожести двух строк от 0 до 1."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


@dataclass
class PendingSignal:
    ticker: str
    action: str
    confidence: int
    credibility: int
    explanation: str
    risk_factors: list
    timeframe: str
    news_title: str
    sources: list[str]  # все источники одной новости
    created_at: datetime = field(default_factory=datetime.utcnow)


class SignalAggregator:
    def __init__(self):
        self._pending: dict[str, list[PendingSignal]] = defaultdict(list)
        self._last_sent: dict[str, datetime] = {}

    def add(self, signal: dict, news_title: str, source: str) -> None:
        for ticker in signal.get("tickers", []):
            existing = self._find_duplicate(ticker, news_title)
            if existing:
                # Та же новость из другого источника — объединяем
                if source not in existing.sources:
                    existing.sources.append(source)
                # Берём максимальные оценки
                existing.confidence = max(existing.confidence, signal.get("confidence", 0))
                existing.credibility = max(existing.credibility, signal.get("credibility", 0))
            else:
                self._pending[ticker].append(PendingSignal(
                    ticker=ticker,
                    action=signal.get("action", ""),
                    confidence=signal.get("confidence", 0),
                    credibility=signal.get("credibility", 0),
                    explanation=signal.get("explanation", ""),
                    risk_factors=signal.get("risk_factors", []),
                    timeframe=signal.get("timeframe", ""),
                    news_title=news_title,
                    sources=[source],
                ))

    def _find_duplicate(self, ticker: str, title: str) -> PendingSignal | None:
        """Ищет похожую новость в буфере по тикеру."""
        for s in self._pending.get(ticker, []):
            if _similarity(s.news_title, title) >= DEDUP_SIMILARITY:
                return s
        return None

    def flush(self) -> list[dict]:
        """Возвращает финальные сигналы готовые к отправке, очищает буфер."""
        now = datetime.utcnow()
        window = timedelta(minutes=AGGREGATION_WINDOW_MIN)
        cooldown = timedelta(minutes=COOLDOWN_MIN)
        results = []

        for ticker, signals in list(self._pending.items()):
            recent = [s for s in signals if now - s.created_at <= window]
            self._pending[ticker] = recent

            if not recent:
                continue

            last = self._last_sent.get(ticker)
            if last and now - last < cooldown:
                continue

            # Агрегация: взвешенные голоса по action
            votes: dict[str, int] = defaultdict(int)
            for s in recent:
                votes[s.action] += s.confidence

            dominant_action = max(votes, key=lambda a: votes[a])
            candidates = [s for s in recent if s.action == dominant_action]
            best = max(candidates, key=lambda s: s.confidence)

            # Фильтр значимости
            if best.confidence < MIN_CONFIDENCE:
                continue
            if best.credibility < MIN_CREDIBILITY:
                continue
            if dominant_action == "HOLD" and best.confidence < MIN_CONFIDENCE_HOLD:
                continue

            # Конфликт сигналов
            conflict_note = ""
            if len(set(s.action for s in recent)) > 1:
                summary = ", ".join(f"{a}: {v}%" for a, v in sorted(votes.items()))
                conflict_note = f"Мнения разделились ({summary}). Показан доминирующий сигнал."

            # Все источники (включая дубликаты из разных изданий)
            all_sources = []
            for s in recent:
                for src in s.sources:
                    if src not in all_sources:
                        all_sources.append(src)

            results.append({
                "ticker": ticker,
                "action": dominant_action,
                "confidence": best.confidence,
                "credibility": best.credibility,
                "explanation": best.explanation,
                "risk_factors": best.risk_factors,
                "timeframe": best.timeframe,
                "news_title": best.news_title,
                "source": ", ".join(all_sources),
                "news_count": len(recent),
                "conflict_note": conflict_note,
            })

            self._last_sent[ticker] = now
            self._pending[ticker] = []

        return results
