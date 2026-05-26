"""
Telegram bot command handler.

Commands:
  /start           — welcome + usage
  /portfolio       — show ticker keyboard (toggle subscriptions)
  /settings        — настройки алертов (порог важности + тихие часы)

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

# Admin-only commands. Add your Telegram user_id here.
# Find it in server logs after sending /start: "user_id": <number>
ADMIN_USER_IDS: frozenset[int] = frozenset({402652773})

# Characters that must be escaped in Telegram MarkdownV2 (backslash first).
_MD_SPECIAL = ["\\", "_", "*", "[", "]", "(", ")", "~", "`", ">",
               "#", "+", "-", "=", "|", "{", "}", ".", "!"]


def _md_escape(text: str) -> str:
    for ch in _MD_SPECIAL:
        text = text.replace(ch, "\\" + ch)
    return text


async def _handle_status(user_id: int) -> None:
    """Send pipeline health metrics to an admin user."""
    from app.core import metrics as _metrics

    m = _metrics.snapshot()
    uptime_h = _metrics.uptime_seconds() // 3600
    fetched = m.get("articles_fetched", 0)
    noise = m.get("articles_noise", 0)
    noise_pct = int(noise / fetched * 100) if fetched else 0
    sent_ok = m.get("tg_sent_ok", 0)
    sent_fail = m.get("tg_sent_fail", 0)
    pipeline_err = m.get("pipeline_errors", 0)

    text = (
        "*📊 Pipeline Status*\n\n"
        f"⏱ Uptime: `{uptime_h}h`\n"
        f"📥 Fetched: `{fetched:,}`\n"
        f"🚫 Noise: `{noise:,}` \\({noise_pct}%\\)\n"
        f"📤 Sent OK: `{sent_ok}`\n"
        f"❌ Send errors: `{sent_fail}`\n"
        f"⚙️ Pipeline errors: `{pipeline_err}`"
    )
    await send_dm(user_id, text)


async def _handle_calendar(db: DBConnection, user_id: int) -> None:
    """Handle /calendar command — upcoming events for the next 30 days."""
    today = datetime.now(timezone.utc).date()
    tickers = queries.get_user_tickers(db, user_id)
    if not tickers:
        await send_dm(
            user_id,
            "📭 Твой портфель пуст\\. Добавь тикеры через */portfolio*",
        )
        return
    events = queries.get_portfolio_events_for_user(
        db, user_id,
        from_date=today,
        to_date=today + timedelta(days=29),
    )
    if not events:
        await send_dm(
            user_id,
            "📭 Нет событий по твоим тикерам на ближайшие 30 дней\\.",
        )
        return
    text = format_portfolio_calendar(events, label="30 дней").lstrip("\n")
    await send_dm(user_id, text)


# Tickers grouped by sector for the keyboard
_SECTORS: list[tuple[str, list[str]]] = [
    ("🛢 Нефть/Газ",    ["GAZP", "LKOH", "ROSN", "NVTK", "TATN", "SNGS", "ENPG", "TRNFP", "BANEP"]),
    ("🏦 Банки",         ["SBER", "VTBR", "TCSG", "BSPB", "CBOM", "AFKS", "SVCB", "SPBE", "RENI"]),
    ("⚙️ Металлы",       ["GMKN", "CHMF", "NLMK", "MAGN", "PLZL", "ALRS", "POLY", "MTLR", "SELG", "RUAL", "RASP"]),
    ("⚡️ Энергетика",   ["IRAO", "HYDR", "FEES"]),
    ("💻 IT/Телеком",    ["YNDX", "MTSS", "RTKM", "VKCO", "POSI", "HHRU", "OZON"]),
    ("🚢 Транспорт",     ["FLOT", "AFLT"]),
    ("🏗 Недвижимость",  ["SMLT", "PIKK", "LSRG", "ETLN"]),
    ("🛒 Ритейл",        ["MGNT", "FIVE", "FIXP", "PHOR", "AGRO", "MOEX", "SGZH"]),
]

_TICKERS_PER_ROW = 3


def _keyboard_header(count: int) -> str:
    if count == 0:
        return "🗂 *Портфель пуст*\n\nВыберите тикеры для получения личных алертов:"
    return f"🗂 *Портфель*: {count} тик\\.\n\nНажмите сектор → выберите тикеры:"


def _sector_badge(count: int, total: int) -> str:
    if count == 0:
        return "·"
    if count == total:
        return "✅"
    return f"{count}/{total}"


def _build_keyboard(subscribed: set[str], open_sector: int = -1, done_callback: str = "done") -> dict:
    """Accordion keyboard: sectors collapse/expand in place."""
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
    rows.append([{"text": "✔️ Готово", "callback_data": done_callback}])
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

# ── Reply keyboard (persistent bottom button) ─────────────────────────────────

_REPLY_KEYBOARD: dict = {
    "keyboard":        [[{"text": "☰ Меню"}]],
    "resize_keyboard": True,
    "is_persistent":   True,
}

# ── Main menu ─────────────────────────────────────────────────────────────────

def _build_main_menu_keyboard(is_admin: bool = False) -> dict:
    rows = [
        [
            {"text": "📋 Портфель",  "callback_data": "menu:portfolio"},
            {"text": "📅 Календарь", "callback_data": "menu:calendar"},
        ],
        [
            {"text": "⚙️ Настройки", "callback_data": "menu:settings"},
            {"text": "ℹ️ Помощь",    "callback_data": "menu:help"},
        ],
    ]
    if is_admin:
        rows.append([{"text": "📊 Статус", "callback_data": "menu:status"}])
    return {"inline_keyboard": rows}


def _help_text() -> str:
    return (
        "ℹ️ *MOEX\\.news — справка*\n\n"
        "📋 *Портфель* — /portfolio\n"
        "Выбери тикеры \\($SBER, $GAZP\\.\\.\\.\\)\\. Когда выйдет важная новость — получишь личный алерт\\.\n\n"
        "📅 *Календарь* — /calendar\n"
        "Ближайшие дивиденды, отчёты и оферты по твоим тикерам на 30 дней вперёд\\.\n\n"
        "⚙️ *Настройки* — /settings\n"
        "Порог важности \\(фильтр новостей\\) и тихие часы\\.\n\n"
        "📡 *Канал*\n"
        "Все значимые новости публикуются в общем канале\\."
    )


async def _send_menu(user_id: int, is_admin: bool = False) -> None:
    """Send the main inline menu to a user."""
    await send_dm(
        user_id,
        "📊 *MOEX\\.news*",
        reply_markup=_build_main_menu_keyboard(is_admin),
    )


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


# ── Onboarding wizard ─────────────────────────────────────────────────────────

def _onb_step2_text() -> str:
    return (
        "📋 *Шаг 1 из 3 — Портфель*\n\n"
        "Выбери тикеры для личных алертов\\.\n"
        "Когда выйдет важная новость — сразу напишу\\."
    )


def _onb_step3_text() -> str:
    return (
        "📊 *Шаг 2 из 3 — Порог важности*\n\n"
        "Каждой новости я ставлю оценку от 0 до 100 — насколько она важна для рынка\\.\n\n"
        "• 10 — много новостей, в том числе мелкие\n"
        "• 30 — только заметные события \\(рекомендую\\)\n"
        "• 50 — только крупные: санкции, решения ЦБ, отчёты"
    )


def _build_onb_score_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "10",    "callback_data": "onb:score:10"},
                {"text": "20",    "callback_data": "onb:score:20"},
                {"text": "✅ 30", "callback_data": "onb:score:30"},
                {"text": "50",    "callback_data": "onb:score:50"},
                {"text": "70",    "callback_data": "onb:score:70"},
            ],
            [{"text": "Пропустить →", "callback_data": "onb:skip:score"}],
        ]
    }


def _onb_step4_text() -> str:
    return (
        "🌙 *Шаг 3 из 3 — Тихие часы*\n\n"
        "Алерты ночью? В тихие часы уведомления не придут \\(время UTC\\)\\."
    )


def _build_onb_quiet_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "Выкл",  "callback_data": "onb:quiet:off"},
                {"text": "22–08", "callback_data": "onb:quiet:22:8"},
                {"text": "23–08", "callback_data": "onb:quiet:23:8"},
                {"text": "23–09", "callback_data": "onb:quiet:23:9"},
            ],
            [{"text": "Пропустить →", "callback_data": "onb:skip:quiet"}],
        ]
    }


def _onb_step5_text(n_tickers: int, score: int, quiet_from: int | None, quiet_to: int | None) -> str:
    n_str     = _md_escape(str(n_tickers))
    score_str = _md_escape(str(score))
    if quiet_from is not None and quiet_to is not None:
        quiet_str = _md_escape(f"{quiet_from:02d}:00–{quiet_to:02d}:00 UTC")
    else:
        quiet_str = "выкл"
    return (
        "🎉 *Всё готово\\!*\n\n"
        f"Подписан на *{n_str}* тикеров · Порог: *{score_str}* · Тихие часы: *{quiet_str}*\n\n"
        "Теперь я буду присылать важные новости по твоим акциям\\.\n"
        "Управляй ботом через кнопку *☰ Меню* внизу\\."
    )


_ONB_QUIET_CALLBACKS: frozenset[str] = frozenset({
    "onb:skip:quiet", "onb:quiet:off",
    "onb:quiet:22:8", "onb:quiet:23:8", "onb:quiet:23:9",
})


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
    queries.upsert_user(db, user_id, from_data.get("username"), first_name)

    # ── Reply keyboard shortcut ───────────────────────────────────────────────
    if text == "☰ Меню":
        await _send_menu(user_id, is_admin=user_id in ADMIN_USER_IDS)
        logger.info(
            "bot command handled",
            extra={"event": "bot_command", "user_id": user_id, "cmd": "menu"},
        )
        return

    cmd = text.split()[0].split("@")[0]  # strip @botusername suffix

    if cmd == "/start":
        tickers = queries.get_user_tickers(db, user_id)
        if tickers:
            # Returning user — show welcome + reply keyboard + inline menu
            tickers_str = " · ".join(f"\\${_md_escape(t)}" for t in tickers)
            welcome_back = (
                f"С возвращением, {_md_escape(first_name)}\\!\n\n"
                f"Твой портфель: {tickers_str}"
            )
            await send_dm(user_id, welcome_back, reply_markup=_REPLY_KEYBOARD)
            await _send_menu(user_id, is_admin=user_id in ADMIN_USER_IDS)
        else:
            # New user — wizard step 1
            name = _md_escape(first_name)
            greeting = f"Привет, {name}\\!" if name else "Привет\\!"
            body = (
                f"{greeting} Я *MOEX\\.news* — бот для инвесторов\\.\n\n"
                "📡 Слежу за 15\\+ источниками и присылаю важные новости "
                "по российскому рынку прямо в личку\\.\n\n"
                "Настроим бота под тебя — займёт 1 минуту\\."
            )
            await send_dm(
                user_id,
                body,
                reply_markup={
                    "inline_keyboard": [[
                        {"text": "Начать настройку 🚀", "callback_data": "onb:start"}
                    ]]
                },
            )

    elif cmd == "/portfolio":
        subscribed = set(queries.get_user_tickers(db, user_id))
        await send_dm(user_id, _keyboard_header(len(subscribed)), reply_markup=_build_keyboard(subscribed))

    elif cmd == "/settings":
        internal_id = _get_internal_user_id(db, user_id)
        s = queries.get_user_settings(db, internal_id) if internal_id else queries.DEFAULT_SETTINGS
        await send_dm(user_id, _settings_header(s), reply_markup=_build_settings_keyboard())

    elif cmd == "/status":
        if user_id in ADMIN_USER_IDS:
            await _handle_status(user_id)
        else:
            await send_dm(user_id, "⛔ Нет доступа\\.")

    elif cmd == "/calendar":
        await _handle_calendar(db, user_id)

    elif cmd == "/help":
        await send_dm(user_id, _help_text())

    else:
        await send_dm(
            user_id,
            "Команды: */portfolio*, */calendar*, */settings*, */help*\\.",
        )

    logger.info(
        "bot command handled",
        extra={"event": "bot_command", "user_id": user_id, "cmd": cmd},
    )


# ── callback handler ──────────────────────────────────────────────────────────

async def _handle_callback(db: DBConnection, cbq: dict) -> None:
    cbq_id    = cbq["id"]
    data      = cbq.get("data", "")
    from_data = cbq["from"]
    user_id   = from_data["id"]
    message   = cbq.get("message", {})
    chat_id   = message.get("chat", {}).get("id", user_id)
    msg_id    = message.get("message_id")

    queries.upsert_user(db, user_id, from_data.get("username"), from_data.get("first_name", ""))

    subscribed = set(queries.get_user_tickers(db, user_id))

    # ── Onboarding wizard ─────────────────────────────────────────────────────

    # onb:start — show step 2 (portfolio keyboard with onb:2 done button)
    if data == "onb:start":
        await answer_callback_query(cbq_id)
        await edit_dm(chat_id, msg_id, _onb_step2_text().split("\n")[0],
                      reply_markup={"inline_keyboard": []})
        await send_dm(user_id, _onb_step2_text(),
                      reply_markup=_build_keyboard(subscribed, done_callback="onb:2"))
        return

    # onb:2 — tickers saved via t: callbacks; show step 3 (score)
    if data == "onb:2":
        await answer_callback_query(cbq_id)
        await edit_dm(chat_id, msg_id, _onb_step2_text().split("\n")[0],
                      reply_markup={"inline_keyboard": []})
        await send_dm(user_id, _onb_step3_text(), reply_markup=_build_onb_score_keyboard())
        return

    # onb:score:{v} or onb:skip:score — save score (or keep default), show step 4
    if data.startswith("onb:score:") or data == "onb:skip:score":
        await answer_callback_query(cbq_id)
        await edit_dm(chat_id, msg_id, _onb_step3_text().split("\n")[0],
                      reply_markup={"inline_keyboard": []})
        if data.startswith("onb:score:"):
            score = int(data.split(":")[2])
            internal_id = _get_internal_user_id(db, user_id)
            if internal_id:
                s = queries.get_user_settings(db, internal_id)
                queries.save_user_settings(
                    db, internal_id,
                    min_score=score,
                    quiet_from=s["quiet_from"],
                    quiet_to=s["quiet_to"],
                )
        await send_dm(user_id, _onb_step4_text(), reply_markup=_build_onb_quiet_keyboard())
        return

    # onb:quiet:* or onb:skip:quiet — save quiet hours (if any), show step 5
    if data in _ONB_QUIET_CALLBACKS:
        await answer_callback_query(cbq_id)
        await edit_dm(chat_id, msg_id, _onb_step4_text().split("\n")[0],
                      reply_markup={"inline_keyboard": []})
        internal_id = _get_internal_user_id(db, user_id)
        if internal_id and data.startswith("onb:quiet:") and data != "onb:quiet:off":
            parts = data.split(":")
            qf, qt = int(parts[2]), int(parts[3])
            s = queries.get_user_settings(db, internal_id)
            queries.save_user_settings(
                db, internal_id,
                min_score=s["min_score"],
                quiet_from=qf,
                quiet_to=qt,
            )
        tickers = queries.get_user_tickers(db, user_id)
        s = queries.get_user_settings(db, internal_id) if internal_id else queries.DEFAULT_SETTINGS
        await send_dm(
            user_id,
            _onb_step5_text(len(tickers), s["min_score"], s["quiet_from"], s["quiet_to"]),
            reply_markup=_REPLY_KEYBOARD,
        )
        return

    # s:{idx}:{current_open} — toggle accordion
    if data.startswith("s:"):
        await answer_callback_query(cbq_id)
        parts = data.split(":")
        idx, current_open = int(parts[1]), int(parts[2])
        new_open = -1 if idx == current_open else idx
        header = _keyboard_header(len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed, new_open))
        return

    # t:{ticker}:{open_sector} — toggle individual ticker (single SQL op)
    if data.startswith("t:"):
        parts = data.split(":")
        ticker, open_sector = parts[1], int(parts[2])
        adding = ticker not in subscribed
        await answer_callback_query(cbq_id)
        queries.toggle_user_ticker(db, user_id, ticker, adding)
        if adding:
            subscribed.add(ticker)
        else:
            subscribed.discard(ticker)
        header = _keyboard_header(len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed, open_sector))
        return

    # sa:{idx}:{open_sector} — toggle entire sector
    if data.startswith("sa:"):
        parts = data.split(":")
        idx, open_sector = int(parts[1]), int(parts[2])
        _, sector_tickers = _SECTORS[idx]
        all_selected = all(t in subscribed for t in sector_tickers)
        await answer_callback_query(cbq_id)
        if all_selected:
            subscribed -= set(sector_tickers)
        else:
            subscribed |= set(sector_tickers)
        queries.set_user_tickers(db, user_id, list(subscribed))
        header = _keyboard_header(len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed, open_sector))
        return

    if data == "all_on":
        await answer_callback_query(cbq_id)
        all_tickers = [t for _, tickers in _SECTORS for t in tickers]
        queries.set_user_tickers(db, user_id, all_tickers)
        subscribed = set(all_tickers)
        header = _keyboard_header(len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed))
        return

    if data == "all_off":
        await answer_callback_query(cbq_id)
        queries.set_user_tickers(db, user_id, [])
        header = _keyboard_header(0)
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
        return queries.get_user_settings(db, internal_id) if internal_id else queries.DEFAULT_SETTINGS

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
