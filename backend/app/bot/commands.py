"""
Telegram bot command handler.

Processes updates from getUpdates (polled every 10 s by bot_commands_job).

Commands:
  /start                    — welcome + usage
  /portfolio GAZP SBER      — subscribe to tickers (replaces previous list)
  /portfolio                — show current subscriptions
  /unsubscribe              — remove all subscriptions
"""

import logging
import sqlite3

from app.ai.filter import VALID_TICKERS
from app.db import queries
from app.telegram.client import send_dm
from app.telegram.formatter import _esc

logger = logging.getLogger(__name__)

_WELCOME = (
    "Привет\\! Я бот MOEX\\.news\\.\n\n"
    "Подписывайтесь на тикеры, чтобы получать личные уведомления по вашему портфелю\\.\n\n"
    "Команды:\n"
    "*/portfolio GAZP SBER* — подписаться на тикеры\n"
    "*/portfolio* — показать текущие подписки\n"
    "*/unsubscribe* — отменить все подписки"
)


async def handle_update(db: sqlite3.Connection, update: dict) -> None:
    """Dispatch one Telegram update to the appropriate command handler."""
    message = update.get("message")
    if not message:
        return

    user_id = message.get("from", {}).get("id")
    text = (message.get("text") or "").strip()
    if not user_id or not text:
        return

    if text.startswith("/start"):
        await send_dm(user_id, _WELCOME)
    elif text.startswith("/portfolio"):
        await _handle_portfolio(db, user_id, text)
    elif text.startswith("/unsubscribe"):
        queries.clear_user_tickers(db, user_id)
        await send_dm(user_id, "Все подписки удалены\\.")
    else:
        await send_dm(
            user_id,
            "Неизвестная команда\\. Напишите */portfolio GAZP SBER* чтобы подписаться\\.",
        )

    logger.info(
        "bot command handled",
        extra={"event": "bot_command", "user_id": user_id, "cmd": text.split()[0]},
    )


async def _handle_portfolio(db: sqlite3.Connection, user_id: int, text: str) -> None:
    parts = text.split()
    raw_args = parts[1:]

    if not raw_args:
        tickers = queries.get_user_tickers(db, user_id)
        if not tickers:
            await send_dm(
                user_id,
                "У вас нет активных подписок\\.\n\nПример: */portfolio GAZP SBER LKOH*",
            )
        else:
            tickers_str = " · ".join(f"\\${t}" for t in tickers)
            await send_dm(user_id, f"Ваши подписки: {tickers_str}")
        return

    # Normalise: uppercase, strip leading $
    args = [p.upper().lstrip("$") for p in raw_args]
    valid   = [t for t in args if t in VALID_TICKERS]
    unknown = [t for t in args if t not in VALID_TICKERS]

    if not valid:
        unknown_str = ", ".join(_esc(t) for t in unknown)
        await send_dm(
            user_id,
            f"Тикеры не найдены: {unknown_str}\\.\n\n"
            "Пример: */portfolio GAZP SBER LKOH*",
        )
        return

    queries.set_user_tickers(db, user_id, valid)
    tickers_str = " · ".join(f"\\${t}" for t in sorted(valid))
    reply = f"Подписки обновлены: {tickers_str}"
    if unknown:
        unknown_str = ", ".join(_esc(t) for t in unknown)
        reply += f"\n\n⚠️ Не распознаны \\(пропущены\\): {unknown_str}"
    await send_dm(user_id, reply)
