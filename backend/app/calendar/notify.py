"""
Calendar DM text formatting and dispatch.

format_calendar_dm()        — pure function, no I/O
send_calendar_notify()      — sends one DM, returns True on success
format_portfolio_calendar() — upcoming-events section for /portfolio command
"""

import json
import logging
from datetime import date

from app.telegram.client import send_dm

logger = logging.getLogger(__name__)

_EVENT_LABELS: dict[str, str] = {
    "dividend_cutoff":  "💰 Отсечка по дивидендам",
    "dividend_payment": "💸 Выплата дивидендов",
    "earnings":         "📊 Публикация отчёта",
    "buyback":          "🔄 Обратный выкуп",
    "offer":            "📋 Оферта",
}

# Short labels used in /portfolio upcoming-events section
_SHORT_LABELS: dict[str, str] = {
    "dividend_cutoff":  "💰 Отсечка",
    "dividend_payment": "💸 Выплата",
    "earnings":         "📊 Отчёт",
    "buyback":          "🔄 Выкуп",
    "offer":            "📋 Оферта",
}

_MONTHS_RU = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]

_MONTHS_SHORT = [
    "янв", "фев", "мар", "апр", "май", "июн",
    "июл", "авг", "сен", "окт", "ноя", "дек",
]


def _esc(text: str) -> str:
    """Escape Telegram MarkdownV2 special characters."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, "\\" + ch)
    return text


def _fmt_date_long(d: date) -> str:
    return f"{d.day} {_MONTHS_RU[d.month - 1]}"


def _fmt_date_short(d: date) -> str:
    return f"{d.day} {_MONTHS_SHORT[d.month - 1]}"


def _days_label(n: int) -> str:
    if n == 1:
        return "1 день"
    if n in (2, 3, 4):
        return f"{n} дня"
    return f"{n} дней"


def format_calendar_dm(
    ticker: str,
    event_type: str,
    event_date: date,
    details: dict,
    days_ahead: int,
) -> str:
    """Format a calendar event DM as Telegram MarkdownV2. Pure function, no I/O."""
    label    = _EVENT_LABELS.get(event_type, "📅 Корпоративное событие")
    date_str = _fmt_date_long(event_date)
    days_str = _days_label(days_ahead)

    lines = [
        "📅 *Корпоративное событие*",
        "",
        f"*{_esc(ticker)}* — {label}",
        f"Дата: {date_str} \\(через {days_str}\\)",
    ]

    if event_type in ("dividend_cutoff", "dividend_payment"):
        amount   = details.get("amount")
        currency = details.get("currency", "RUB")
        if amount is not None:
            lines.append(f"Размер: *{_esc(str(amount))} {_esc(currency)}*/акцию")
        if event_type == "dividend_cutoff":
            lines += [
                "",
                "_Чтобы получить дивиденд, держите акции до этой даты включительно\\._",
            ]

    elif event_type == "earnings":
        rt = details.get("report_type")
        if rt:
            lines.append(f"Тип отчёта: {_esc(rt)}")

    elif event_type in ("buyback", "offer"):
        price = details.get("price")
        if price:
            label_word = "выкупа" if event_type == "buyback" else "оферты"
            lines.append(f"Цена {label_word}: *{_esc(str(price))} руб\\.*/акцию")

    return "\n".join(lines)


async def send_calendar_notify(
    telegram_id: int,
    ticker: str,
    event_type: str,
    event_date: date,
    details: dict,
    days_ahead: int,
) -> bool:
    """Send a calendar DM. Returns True if Telegram accepted it."""
    text   = format_calendar_dm(ticker, event_type, event_date, details, days_ahead)
    msg_id = await send_dm(telegram_id, text)
    ok     = msg_id is not None
    logger.info(
        "calendar notify %s: telegram_id=%d ticker=%s event_type=%s",
        "ok" if ok else "failed",
        telegram_id, ticker, event_type,
        extra={
            "event":       "calendar_notify_ok" if ok else "calendar_notify_failed",
            "telegram_id": telegram_id,
            "ticker":      ticker,
            "event_type":  event_type,
        },
    )
    return ok


def format_portfolio_calendar(events: list, label: str = "7 дней") -> str:
    """
    Format upcoming events section for /portfolio command.
    Returns empty string when there are no events. Pure function.
    """
    if not events:
        return ""

    lines = ["", f"📅 *Ближайшие события \\({_esc(label)}\\):*"]
    for ev in events:
        label_str = _SHORT_LABELS.get(ev["event_type"], "📅")
        details = ev["details"] if isinstance(ev["details"], dict) \
                  else json.loads(ev["details"]) if ev["details"] else {}
        suffix  = ""
        if ev["event_type"] in ("dividend_cutoff", "dividend_payment"):
            amt = details.get("amount")
            if amt:
                suffix = f" — {_esc(str(amt))} руб\\."
        elif ev["event_type"] == "earnings":
            rt = details.get("report_type", "")
            if rt:
                suffix = f" \\({_esc(rt)}\\)"
        lines.append(
            f"• *{_esc(ev['ticker'])}* {label_str} {_fmt_date_short(ev['event_date'])}{suffix}"
        )
    return "\n".join(lines)
