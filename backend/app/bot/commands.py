"""
Telegram bot command handler.

Commands:
  /start           — welcome + usage
  /portfolio       — show ticker keyboard (toggle subscriptions)
  /settings        — настройки алертов (Pro)
  /unsubscribe     — remove all subscriptions

Inline keyboard flow:
  /portfolio → keyboard with all MOEX tickers grouped by sector
  tap ticker → toggle ✅ / off, keyboard updates in place
  tap "Готово" → shows subscription summary, closes keyboard

  /settings → main menu (score + quiet hours)
  tap option → sub-picker, tap value → save + back to main
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.db.database import DBConnection
from app.db import queries
from app.telegram.client import answer_callback_query, edit_dm, send_dm
from app.calendar.notify import format_portfolio_calendar

logger = logging.getLogger(__name__)

# Characters that must be escaped in Telegram MarkdownV2 (backslash first).
_MD_SPECIAL = ["\\", "_", "*", "[", "]", "(", ")", "~", "`", ">",
               "#", "+", "-", "=", "|", "{", "}", ".", "!"]


def _md_escape(text: str) -> str:
    for ch in _MD_SPECIAL:
        text = text.replace(ch, "\\" + ch)
    return text


def _welcome(first_name: str) -> str:
    name = _md_escape(first_name)
    greeting = f"Привет, {name}\\!" if name else "Привет\\!"
    return (
        f"{greeting} Я *MOEX\\.news* — бот для инвесторов\\.\n\n"
        "📡 Слежу за 15\\+ источниками \\(ТАСС, Интерфакс, Ведомости и др\\.\\) "
        "и присылаю важные новости по российскому рынку\\.\n\n"
        "Что умею:\n"
        "• */portfolio* — выбрать тикеры и получать личные алерты\n"
        "• */settings* — настроить порог важности и тихие часы \\(Pro\\)\n"
        "• */status* — статус подписки\n"
        "• */subscribe* — перейти на Pro \\(299 ₽/мес\\)\n"
        "• */unsubscribe* — отменить все подписки\n\n"
        "🆓 *Бесплатно* — до 3 тикеров в портфеле\\."
    )


# ── /status + /subscribe ──────────────────────────────────────────────────────

_MONTHS_RU = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]
_PLAN_LABELS = {"monthly": "ежемесячный", "annual": "ежегодный"}


def _fmt_date(iso: str) -> str:
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return f"{dt.day} {_MONTHS_RU[dt.month - 1]} {dt.year}"


def _status_free_text() -> str:
    return (
        "🆓 *Бесплатный план*\n\n"
        "• До 3 тикеров в портфеле\n"
        "• Алерты из публичного канала\n\n"
        "*/subscribe* — перейти на Pro \\(299 ₽/мес\\)"
    )


def _status_pro_text(plan: str, expires_at: str) -> str:
    label = _PLAN_LABELS.get(plan, plan)
    return (
        "⭐ *Pro\\-план активен*\n\n"
        f"Тариф: {label}\n"
        f"Действует до: {_fmt_date(expires_at)}\n\n"
        "*/subscribe* — продлить подписку"
    )


def _status_grace_text(expires_at: str) -> str:
    expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    grace_end = expires_dt + timedelta(days=3)
    grace_str = f"{grace_end.day} {_MONTHS_RU[grace_end.month - 1]} {grace_end.year}"
    return (
        "⚠️ *Подписка истекла*\n\n"
        f"Pro\\-план действует до {grace_str} \\(льготный период\\)\\.\n"
        "Продлите сейчас, чтобы не потерять доступ\\.\n\n"
        "*/subscribe* — продлить"
    )


def _subscribe_text() -> str:
    return (
        "⭐ *MOEX\\.news Pro*\n\n"
        "Что открывает Pro:\n"
        "• Неограниченный портфель тикеров\n"
        "• AI\\-контекст в портфельных алертах\n"
        "• /ask — вопросы по тикеру \\(10/день\\)\n"
        "• Приоритетная доставка алертов\n\n"
        "Выберите тариф:"
    )


_SUBSCRIBE_KEYBOARD = {
    "inline_keyboard": [
        [{"text": "📅 Месяц — 299 ₽", "callback_data": "sub:monthly"}],
        [{"text": "🗓 Год — 2 490 ₽  ·  207 ₽/мес", "callback_data": "sub:annual"}],
    ]
}

_PAYMENT_PENDING = "⏳ Оплата через Telegram Stars подключается — следите за анонсом\\!"

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

FREE_TICKER_LIMIT = 3
_GATE_ALERT = (
    "🔒 Бесплатный план — максимум 3 тикера. "
    "Напишите /subscribe чтобы получить Pro."
)
_TICKERS_PER_ROW = 4


def _keyboard_header(plan: str, count: int) -> str:
    if plan == "free":
        return (
            f"🗂 *Ваш портфель* \\({count}/{FREE_TICKER_LIMIT} тикера\\)\n\n"
            "Выберите тикеры \\(нажмите чтобы добавить / убрать\\):"
        )
    return "🗂 *Ваш портфель*\n\nВыберите тикеры \\(нажмите чтобы добавить / убрать\\):"


def _sector_badge(count: int, total: int) -> str:
    if count == total:
        return f"✅ {count}/{total}"
    if count == 0:
        return f"0/{total}"
    return f"{count}/{total}"


def _build_keyboard(subscribed: set[str], open_sector: int = -1) -> dict:
    """Accordion keyboard: sectors collapse/expand in place.

    open_sector — index of the currently expanded sector, -1 means all collapsed.
    The open_sector index is encoded into every callback_data so we never need
    server-side session state.
    """
    rows = []
    for idx, (sector_name, tickers) in enumerate(_SECTORS):
        count = sum(1 for t in tickers if t in subscribed)
        total = len(tickers)
        is_open = idx == open_sector

        arrow = "▼" if is_open else "▶"
        badge = _sector_badge(count, total)
        rows.append([{
            "text": f"{arrow} {sector_name}  {badge}",
            "callback_data": f"s:{idx}:{open_sector}",
        }])

        if is_open:
            row: list[dict] = []
            for ticker in tickers:
                label = f"✅ {ticker}" if ticker in subscribed else ticker
                row.append({"text": label, "callback_data": f"t:{ticker}:{idx}"})
                if len(row) == _TICKERS_PER_ROW:
                    rows.append(row)
                    row = []
            if row:
                rows.append(row)

            all_selected = all(t in subscribed for t in tickers)
            sa_label = "🗑 Снять сектор" if all_selected else "✅ Выбрать сектор"
            rows.append([{"text": sa_label, "callback_data": f"sa:{idx}:{idx}"}])

    rows.append([
        {"text": "✅ Выбрать всё", "callback_data": "all_on"},
        {"text": "🗑 Снять всё",   "callback_data": "all_off"},
    ])
    rows.append([{"text": "✔️ Готово", "callback_data": "done"}])
    return {"inline_keyboard": rows}


def _summary_text(tickers: list[str]) -> str:
    if not tickers:
        return "У вас нет активных подписок\\.\n\nНапишите */portfolio* чтобы выбрать тикеры\\."
    tickers_str = " · ".join(f"\\${t}" for t in tickers)
    return f"Подписки сохранены:\n{tickers_str}"


# ── /settings ────────────────────────────────────────────────────────────────

_SCORE_OPTIONS = [10, 20, 30, 50, 70]

_QUIET_PRESETS: list[tuple[str, tuple[int, int] | None]] = [
    ("Выкл", None),
    ("22–08", (22, 8)),
    ("23–08", (23, 8)),
    ("23–09", (23, 9)),
]


def _settings_header(s: Any) -> str:
    score = s["min_score"]
    qf, qt = s["quiet_from"], s["quiet_to"]
    quiet_str = f"{qf:02d}:00–{qt:02d}:00" if qf is not None and qt is not None else "выкл"
    return (
        "⚙️ *Настройки алертов*\n\n"
        f"Порог важности: *{score}*\n"
        f"Тихие часы: *{quiet_str}*"
    )


def _build_settings_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "📊 Порог важности  ▶", "callback_data": "cfg:score"}],
            [{"text": "🌙 Тихие часы  ▶",    "callback_data": "cfg:quiet"}],
            [{"text": "✔️ Готово",             "callback_data": "cfg:done"}],
        ]
    }


def _build_score_keyboard(current: int) -> dict:
    row = []
    for v in _SCORE_OPTIONS:
        label = f"✅ {v}" if v == current else str(v)
        row.append({"text": label, "callback_data": f"cfg:sc:{v}"})
    return {
        "inline_keyboard": [
            row,
            [{"text": "← Назад", "callback_data": "cfg:main"}],
        ]
    }


def _build_quiet_keyboard(current_from: int | None, current_to: int | None) -> dict:
    row = []
    for label, preset in _QUIET_PRESETS:
        pf = preset[0] if preset else None
        pt = preset[1] if preset else None
        active = (pf == current_from and pt == current_to)
        btn_label = f"✅ {label}" if active else label
        data = "cfg:qt:off" if preset is None else f"cfg:qt:{preset[0]}:{preset[1]}"
        row.append({"text": btn_label, "callback_data": data})
    return {
        "inline_keyboard": [
            row,
            [{"text": "← Назад", "callback_data": "cfg:main"}],
        ]
    }


def _get_internal_user_id(db: DBConnection, telegram_id: int) -> int | None:
    row = queries.get_user(db, telegram_id)
    return row["id"] if row else None


# ── update routing ────────────────────────────────────────────────────────────

async def handle_update(db: DBConnection, update: dict) -> None:
    if "callback_query" in update:
        await _handle_callback(db, update["callback_query"])
        return

    message = update.get("message")
    if not message:
        return

    from_data = message.get("from", {})
    user_id = from_data.get("id")
    text = (message.get("text") or "").strip()
    if not user_id or not text:
        return

    first_name = from_data.get("first_name", "")
    plan = queries.upsert_user(db, user_id, from_data.get("username"), first_name)

    cmd = text.split()[0].split("@")[0]  # strip @botusername suffix

    if cmd == "/start":
        await send_dm(user_id, _welcome(first_name))

    elif cmd == "/portfolio":
        subscribed = set(queries.get_user_tickers(db, user_id))
        await send_dm(user_id, _keyboard_header(plan, len(subscribed)), reply_markup=_build_keyboard(subscribed))

    elif cmd == "/status":
        sub = queries.get_active_subscription(db, user_id)
        if sub is None:
            await send_dm(user_id, _status_free_text())
        elif sub["status"] == "grace":
            await send_dm(user_id, _status_grace_text(sub["expires_at"]))
        else:
            await send_dm(user_id, _status_pro_text(sub["plan"], sub["expires_at"]))

    elif cmd == "/subscribe":
        await send_dm(user_id, _subscribe_text(), reply_markup=_SUBSCRIBE_KEYBOARD)

    elif cmd == "/settings":
        from app.bot.guards import require_pro
        if not await require_pro(db, user_id):
            return
        internal_id = _get_internal_user_id(db, user_id)
        s = queries.get_user_settings(db, internal_id) if internal_id else queries._SETTINGS_DEFAULTS
        await send_dm(user_id, _settings_header(s), reply_markup=_build_settings_keyboard())

    elif cmd == "/unsubscribe":
        queries.clear_user_tickers(db, user_id)
        await send_dm(user_id, "Все подписки удалены\\.")

    else:
        await send_dm(
            user_id,
            "Команды: */portfolio*, */settings*, */status*, */subscribe*, */unsubscribe*\\.",
        )

    logger.info(
        "bot command handled",
        extra={"event": "bot_command", "user_id": user_id, "cmd": cmd},
    )


# ── callback handler ──────────────────────────────────────────────────────────

async def _handle_callback(db: DBConnection, cbq: dict) -> None:
    cbq_id  = cbq["id"]
    data    = cbq.get("data", "")
    user_id = cbq["from"]["id"]
    message = cbq.get("message", {})
    chat_id = message.get("chat", {}).get("id", user_id)
    msg_id  = message.get("message_id")

    plan       = queries.get_user_plan(db, user_id)
    subscribed = set(queries.get_user_tickers(db, user_id))

    # sub:monthly / sub:annual — payment initiation (Stars flow wired in ticket #13)
    if data.startswith("sub:"):
        await answer_callback_query(cbq_id)
        await send_dm(user_id, _PAYMENT_PENDING)
        return

    # s:{idx}:{current_open} — toggle accordion (no write, no limit check)
    if data.startswith("s:"):
        await answer_callback_query(cbq_id)
        parts = data.split(":")
        idx, current_open = int(parts[1]), int(parts[2])
        new_open = -1 if idx == current_open else idx
        header = _keyboard_header(plan, len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed, new_open))
        return

    # t:{ticker}:{open_sector} — toggle individual ticker
    if data.startswith("t:"):
        parts = data.split(":")
        ticker, open_sector = parts[1], int(parts[2])
        adding = ticker not in subscribed
        if adding and plan == "free" and len(subscribed) >= FREE_TICKER_LIMIT:
            await answer_callback_query(cbq_id, _GATE_ALERT, show_alert=True)
            return
        await answer_callback_query(cbq_id)
        subscribed ^= {ticker}
        queries.set_user_tickers(db, user_id, list(subscribed))
        header = _keyboard_header(plan, len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed, open_sector))
        return

    # sa:{idx}:{open_sector} — toggle entire sector
    if data.startswith("sa:"):
        parts = data.split(":")
        idx, open_sector = int(parts[1]), int(parts[2])
        _, sector_tickers = _SECTORS[idx]
        all_selected = all(t in subscribed for t in sector_tickers)
        if not all_selected:
            new_count = len(subscribed | set(sector_tickers))
            if plan == "free" and new_count > FREE_TICKER_LIMIT:
                await answer_callback_query(cbq_id, _GATE_ALERT, show_alert=True)
                return
        await answer_callback_query(cbq_id)
        if all_selected:
            subscribed -= set(sector_tickers)
        else:
            subscribed |= set(sector_tickers)
        queries.set_user_tickers(db, user_id, list(subscribed))
        header = _keyboard_header(plan, len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed, open_sector))
        return

    if data == "all_on":
        if plan == "free":
            await answer_callback_query(cbq_id, _GATE_ALERT, show_alert=True)
            return
        await answer_callback_query(cbq_id)
        all_tickers = [t for _, tickers in _SECTORS for t in tickers]
        queries.set_user_tickers(db, user_id, all_tickers)
        subscribed = set(all_tickers)
        header = _keyboard_header(plan, len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed))
        return

    if data == "all_off":
        await answer_callback_query(cbq_id)
        queries.set_user_tickers(db, user_id, [])
        header = _keyboard_header(plan, 0)
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(set()))
        return

    if data == "done":
        await answer_callback_query(cbq_id)
        tickers = queries.get_user_tickers(db, user_id)
        today = datetime.now(timezone.utc).date()
        upcoming = queries.get_portfolio_events_for_user(
            db,
            telegram_id=user_id,
            from_date=today,
            to_date=today + timedelta(days=7),
        )
        calendar_section = format_portfolio_calendar(upcoming)
        text = _summary_text(tickers) + calendar_section
        await edit_dm(chat_id, msg_id, text, reply_markup={"inline_keyboard": []})
        logger.info(
            "portfolio saved via keyboard",
            extra={"event": "portfolio_saved", "user_id": user_id, "tickers": tickers},
        )
        return

    # ── /settings callbacks ───────────────────────────────────────────────────

    if data.startswith("cfg:"):
        await _handle_settings_callback(db, cbq_id, data, user_id, chat_id, msg_id)
        return


async def _handle_settings_callback(
    db: DBConnection,
    cbq_id: str,
    data: str,
    user_id: int,
    chat_id: int,
    msg_id: int,
) -> None:
    internal_id = _get_internal_user_id(db, user_id)

    def _s() -> Any:
        return queries.get_user_settings(db, internal_id) if internal_id else queries._SETTINGS_DEFAULTS

    # cfg:main — re-render main menu
    if data == "cfg:main":
        await answer_callback_query(cbq_id)
        s = _s()
        await edit_dm(chat_id, msg_id, _settings_header(s), reply_markup=_build_settings_keyboard())
        return

    # cfg:done — dismiss keyboard
    if data == "cfg:done":
        await answer_callback_query(cbq_id)
        s = _s()
        await edit_dm(chat_id, msg_id, _settings_header(s), reply_markup={"inline_keyboard": []})
        return

    # cfg:score — show score picker
    if data == "cfg:score":
        await answer_callback_query(cbq_id)
        s = _s()
        text = "📊 *Порог важности*\n\nАлерты с оценкой ниже этого значения не придут\\."
        await edit_dm(chat_id, msg_id, text, reply_markup=_build_score_keyboard(s["min_score"]))
        return

    # cfg:sc:{value} — set score, back to main
    if data.startswith("cfg:sc:"):
        await answer_callback_query(cbq_id)
        value = int(data.split(":")[2])
        if internal_id:
            s = _s()
            queries.save_user_settings(
                db, internal_id,
                min_score=value,
                quiet_from=s["quiet_from"],
                quiet_to=s["quiet_to"],
            )
        s = _s()
        await edit_dm(chat_id, msg_id, _settings_header(s), reply_markup=_build_settings_keyboard())
        logger.info("settings score set", extra={"event": "settings_score", "user_id": user_id, "value": value})
        return

    # cfg:quiet — show quiet hours picker
    if data == "cfg:quiet":
        await answer_callback_query(cbq_id)
        s = _s()
        text = "🌙 *Тихие часы* \\(UTC\\)\n\nАлерты не будут приходить в выбранное время\\."
        await edit_dm(chat_id, msg_id, text, reply_markup=_build_quiet_keyboard(s["quiet_from"], s["quiet_to"]))
        return

    # cfg:qt:off — disable quiet hours
    if data == "cfg:qt:off":
        await answer_callback_query(cbq_id)
        if internal_id:
            s = _s()
            queries.save_user_settings(db, internal_id, min_score=s["min_score"], quiet_from=None, quiet_to=None)
        s = _s()
        await edit_dm(chat_id, msg_id, _settings_header(s), reply_markup=_build_settings_keyboard())
        logger.info("settings quiet disabled", extra={"event": "settings_quiet_off", "user_id": user_id})
        return

    # cfg:qt:{from}:{to} — set quiet hours
    if data.startswith("cfg:qt:"):
        await answer_callback_query(cbq_id)
        parts = data.split(":")
        qf, qt = int(parts[2]), int(parts[3])
        if internal_id:
            s = _s()
            queries.save_user_settings(db, internal_id, min_score=s["min_score"], quiet_from=qf, quiet_to=qt)
        s = _s()
        await edit_dm(chat_id, msg_id, _settings_header(s), reply_markup=_build_settings_keyboard())
        logger.info(
            "settings quiet set", extra={"event": "settings_quiet", "user_id": user_id, "from": qf, "to": qt}
        )
        return
