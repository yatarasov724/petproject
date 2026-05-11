"""
Telegram bot command handler.

Commands:
  /start           — welcome + usage
  /portfolio       — show ticker keyboard (toggle subscriptions)
  /unsubscribe     — remove all subscriptions

Inline keyboard flow:
  /portfolio → keyboard with all MOEX tickers grouped by sector
  tap ticker → toggle ✅ / off, keyboard updates in place
  tap "Готово" → shows subscription summary, closes keyboard
"""

import logging
import sqlite3

from app.db import queries
from app.telegram.client import answer_callback_query, edit_dm, send_dm

logger = logging.getLogger(__name__)

_WELCOME = (
    "Привет\\! Я бот MOEX\\.news\\.\n\n"
    "Получайте личные уведомления по тикерам вашего портфеля\\.\n\n"
    "*/portfolio* — выбрать тикеры\n"
    "*/unsubscribe* — отменить все подписки"
)

# Tickers grouped by sector for the keyboard
_SECTORS: list[tuple[str, list[str]]] = [
    ("🛢 Нефть / Газ",           ["GAZP", "LKOH", "ROSN", "NVTK", "TATN", "SNGS", "ENPG", "TRNFP", "BANEP"]),
    ("🏦 Банки / Финансы",       ["SBER", "VTBR", "TCSG", "BSPB", "CBOM", "AFKS", "SVCB", "SPBE", "RENI"]),
    ("⚙️ Металлы / Добыча",      ["GMKN", "CHMF", "NLMK", "MAGN", "PLZL", "ALRS", "POLY", "MTLR", "SELG", "RUAL", "RASP"]),
    ("⚡️ Энергетика",            ["IRAO", "HYDR", "FEES"]),
    ("💻 IT / Телеком",          ["YNDX", "MTSS", "RTKM", "VKCO", "POSI", "HHRU", "OZON"]),
    ("🚢 Транспорт",             ["FLOT", "AFLT"]),
    ("🏗 Недвижимость",          ["SMLT", "PIKK", "LSRG", "ETLN"]),
    ("🛒 Ритейл / Прочие",       ["MGNT", "FIVE", "FIXP", "PHOR", "AGRO", "MOEX", "SGZH"]),
]

_KEYBOARD_HEADER = "🗂 *Ваш портфель*\n\nВыберите тикеры \\(нажмите чтобы добавить / убрать\\):"
_TICKERS_PER_ROW = 4


def _build_keyboard(subscribed: set[str]) -> dict:
    """Build the inline keyboard with ✅ on subscribed tickers."""
    rows = []
    for idx, (sector_name, tickers) in enumerate(_SECTORS):
        all_selected = all(t in subscribed for t in tickers)
        sector_label = f"✅ {sector_name}" if all_selected else sector_name
        rows.append([{"text": sector_label, "callback_data": f"s:{idx}"}])
        row = []
        for ticker in tickers:
            label = f"✅ {ticker}" if ticker in subscribed else ticker
            row.append({"text": label, "callback_data": f"t:{ticker}"})
            if len(row) == _TICKERS_PER_ROW:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
    rows.append([{"text": "✔️ Готово", "callback_data": "done"}])
    return {"inline_keyboard": rows}


def _summary_text(tickers: list[str]) -> str:
    if not tickers:
        return "У вас нет активных подписок\\.\n\nНапишите */portfolio* чтобы выбрать тикеры\\."
    tickers_str = " · ".join(f"\\${t}" for t in tickers)
    return f"Подписки сохранены:\n{tickers_str}"


# ── update routing ────────────────────────────────────────────────────────────

async def handle_update(db: sqlite3.Connection, update: dict) -> None:
    if "callback_query" in update:
        await _handle_callback(db, update["callback_query"])
        return

    message = update.get("message")
    if not message:
        return

    user_id = message.get("from", {}).get("id")
    text = (message.get("text") or "").strip()
    if not user_id or not text:
        return

    cmd = text.split()[0].split("@")[0]  # strip @botusername suffix

    if cmd == "/start":
        await send_dm(user_id, _WELCOME)

    elif cmd == "/portfolio":
        subscribed = set(queries.get_user_tickers(db, user_id))
        await send_dm(user_id, _KEYBOARD_HEADER, reply_markup=_build_keyboard(subscribed))

    elif cmd == "/unsubscribe":
        queries.clear_user_tickers(db, user_id)
        await send_dm(user_id, "Все подписки удалены\\.")

    else:
        await send_dm(
            user_id,
            "Напишите */portfolio* чтобы выбрать тикеры\\.",
        )

    logger.info(
        "bot command handled",
        extra={"event": "bot_command", "user_id": user_id, "cmd": cmd},
    )


# ── callback handler ──────────────────────────────────────────────────────────

async def _handle_callback(db: sqlite3.Connection, cbq: dict) -> None:
    cbq_id   = cbq["id"]
    data     = cbq.get("data", "")
    user_id  = cbq["from"]["id"]
    message  = cbq.get("message", {})
    chat_id  = message.get("chat", {}).get("id", user_id)
    msg_id   = message.get("message_id")

    await answer_callback_query(cbq_id)

    if data.startswith("s:"):
        idx = int(data[2:])
        _, sector_tickers = _SECTORS[idx]
        subscribed = set(queries.get_user_tickers(db, user_id))
        if all(t in subscribed for t in sector_tickers):
            subscribed -= set(sector_tickers)
        else:
            subscribed |= set(sector_tickers)
        queries.set_user_tickers(db, user_id, list(subscribed))
        await edit_dm(chat_id, msg_id, _KEYBOARD_HEADER, reply_markup=_build_keyboard(subscribed))
        return

    if data == "done":
        tickers = queries.get_user_tickers(db, user_id)
        await edit_dm(chat_id, msg_id, _summary_text(tickers), reply_markup={"inline_keyboard": []})
        logger.info(
            "portfolio saved via keyboard",
            extra={"event": "portfolio_saved", "user_id": user_id, "tickers": tickers},
        )
        return

    if data.startswith("t:"):
        ticker = data[2:]
        subscribed = set(queries.get_user_tickers(db, user_id))
        if ticker in subscribed:
            subscribed.discard(ticker)
        else:
            subscribed.add(ticker)
        queries.set_user_tickers(db, user_id, list(subscribed))
        await edit_dm(chat_id, msg_id, _KEYBOARD_HEADER, reply_markup=_build_keyboard(subscribed))
