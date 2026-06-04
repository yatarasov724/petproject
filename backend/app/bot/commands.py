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
from app.core.alerting import send_ops as _send_ops
from app.core.config import settings
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


async def _handle_fix(db: DBConnection, user_id: int, args: list[str]) -> None:
    """
    /fix <cluster_id> <TICKER[,TICKER2]>
    Admin-only. Updates the cluster's tickers in DB.
    Usage examples:
      /fix 690 MGKL
      /fix 690 MGKL,SBER
      /fix 690 ""        ← clear tickers
    """
    from app.ai.filter import VALID_TICKERS
    from app.db import queries

    if len(args) < 2:
        await send_dm(
            user_id,
            "❌ Использование: `/fix <cluster_id> <TICKER>` или `/fix <cluster_id> \"\"`",
        )
        return

    # Parse cluster_id
    try:
        cluster_id = int(args[0])
    except ValueError:
        await send_dm(user_id, f"❌ Неверный cluster\\_id: `{_md_escape(args[0])}`")
        return

    # Parse tickers (empty string = clear)
    raw_tickers = args[1].strip().strip('"').strip("'")
    if raw_tickers:
        ticker_list = [t.strip().upper() for t in raw_tickers.split(",") if t.strip()]
        invalid = [t for t in ticker_list if t not in VALID_TICKERS]
        if invalid:
            invalid_str = ", ".join(_md_escape(t) for t in invalid)
            await send_dm(user_id, f"❌ Неизвестные тикеры: `{invalid_str}`")
            return
        new_tickers = ",".join(ticker_list)
    else:
        new_tickers = ""

    # Check cluster exists
    cluster = queries.get_cluster_by_id(db, cluster_id)
    if cluster is None:
        await send_dm(user_id, f"❌ Кластер `\\#{cluster_id}` не найден")
        return

    old_tickers = cluster["tickers"] or "(нет)"
    queries.update_cluster_tickers(db, cluster_id, new_tickers)

    # Confirm to admin
    old_display   = _md_escape(old_tickers)
    new_display   = _md_escape(new_tickers or "(нет)")
    title_display = _md_escape(cluster["canonical_title"][:80])
    await send_dm(
        user_id,
        f"✅ *Кластер \\#{cluster_id}* обновлён\n\n"
        f"Новость: _{title_display}_\n\n"
        f"Было: `{old_display}`\n"
        f"Стало: `{new_display}`",
    )

    logger.info(
        "fix command: cluster_id=%d %s → %s by user_id=%d",
        cluster_id, old_tickers, new_tickers, user_id,
        extra={
            "event":      "fix_ticker",
            "cluster_id": cluster_id,
            "old":        old_tickers,
            "new":        new_tickers,
            "admin":      user_id,
        },
    )


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
        # Encode done_callback into every interactive button so keyboard rebuilds preserve it
        dc = done_callback
        rows.append([{
            "text": f"{arrow} {sector_name}  {badge}",
            "callback_data": f"s:{idx}:{open_sector}:{dc}",
        }])

        if is_open:
            row: list[dict] = []
            for ticker in tickers:
                label = f"✅ {ticker}" if ticker in subscribed else ticker
                row.append({"text": label, "callback_data": f"t:{ticker}:{idx}:{dc}"})
                if len(row) == _TICKERS_PER_ROW:
                    rows.append(row)
                    row = []
            if row:
                rows.append(row)

            all_selected = all(t in subscribed for t in tickers)
            sa_label = "🗑 Снять сектор" if all_selected else "✅ Выбрать сектор"
            rows.append([{"text": sa_label, "callback_data": f"sa:{idx}:{idx}:{dc}"}])

    rows.append([
        {"text": "✅ Выбрать всё", "callback_data": f"all_on:{done_callback}"},
        {"text": "🗑 Снять всё",   "callback_data": f"all_off:{done_callback}"},
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
        "ℹ️ *Бычок — справка*\n\n"
        "📋 *Портфель* — /portfolio\n"
        "Выбери тикеры \\($SBER, $GAZP\\.\\.\\.\\)\\. Когда выйдет важная новость — получишь личный алерт\\.\n\n"
        "📅 *Календарь* — /calendar\n"
        "Ближайшие дивиденды, отчёты и оферты по твоим тикерам на 30 дней вперёд\\.\n\n"
        "⚙️ *Настройки* — /settings\n"
        "Порог важности \\(фильтр новостей\\) и тихие часы\\.\n\n"
        "📡 *Канал* — [t\\.me/geomoexnews](https://t.me/geomoexnews)\n"
        "Все значимые новости публикуются в реальном времени\\.\n\n"
        "💬 *Обратная связь* — /feedback\n"
        "Нашёл баг или есть пожелание? Напиши `/feedback текст` — мы прочитаем\\."
    )


async def _send_menu(user_id: int, is_admin: bool = False) -> None:
    """Send the main inline menu to a user."""
    await send_dm(
        user_id,
        "📊 *Бычок*",
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
        "Управляй ботом через команду /menu или кнопки ниже\\."
    )


_ONB_QUIET_CALLBACKS: frozenset[str] = frozenset({
    "onb:skip:quiet", "onb:quiet:off",
    "onb:quiet:22:8", "onb:quiet:23:8", "onb:quiet:23:9",
})


def _get_internal_user_id(db: DBConnection, telegram_id: int) -> int | None:
    row = queries.get_user(db, telegram_id)
    return row["id"] if row else None


# ── update routing ────────────────────────────────────────────────────────────


async def _handle_stats(db: "DBConnection", user_id: int, period: str) -> None:
    hours = 168 if period == "week" else 24
    label = "7 \u0434\u043d\u0435\u0439" if period == "week" else "24\u0447"

    delta = queries.get_metrics_delta(db, hours)
    user_stats = queries.get_user_stats(db, hours)

    if delta.get("snapshot_count", 0) < 2:
        n = delta.get("snapshot_count", 0)
        await send_dm(user_id, f"\u23f3 \u0414\u0430\u043d\u043d\u044b\u0445 \u043f\u043e\u043a\u0430 \u043d\u0435\u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e \u2014 \u043d\u0443\u0436\u043d\u043e \u0445\u043e\u0442\u044f \u0431\u044b 2 \u0441\u043d\u0438\u043c\u043a\u0430 \u043d\u0430\u043a\u043e\u043f\u043b\u0435\u043d\u043e {n}\\.") 
        return

    fetched = delta.get("fetched") or 0
    exact_dup = delta.get("exact_dup") or 0
    near_dup = delta.get("near_dup") or 0
    noise = delta.get("noise") or 0
    published = delta.get("published") or 0
    tg_ok = delta.get("tg_ok") or 0
    tg_fail = delta.get("tg_fail") or 0
    rate_limited = delta.get("rate_limited") or 0

    def pct(n, total):
        return f" \\({int(n / total * 100)}%\\)" if total else ""

    text = (
        f"\U0001f4ca *\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u0437\u0430 {_md_escape(label)}*\n\n"
        f"*Pipeline:*\n"
        f"  \u041f\u043e\u043b\u0443\u0447\u0435\u043d\u043e: `{fetched:,}`\n"
        f"  \u0414\u0443\u0431\u043b\u0435\u0439 \u0442\u043e\u0447\u043d\u044b\u0445: `{exact_dup:,}`{pct(exact_dup, fetched)}\n"
        f"  \u0414\u0443\u0431\u043b\u0435\u0439 \u043f\u043e\u0445\u043e\u0436\u0438\u0445: `{near_dup:,}`{pct(near_dup, fetched)}\n"
        f"  \u0428\u0443\u043c\u0430: `{noise:,}`{pct(noise, fetched)}\n"
        f"  \u041e\u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u043d\u043e: `{published:,}`\n\n"
        f"*Telegram:*\n"
        f"  \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: `{tg_ok:,}` \u2705\n"
        f"  \u041e\u0448\u0438\u0431\u043e\u043a: `{tg_fail:,}` \u274c\n"
        f"  Rate\\-limit: `{rate_limited:,}`\n\n"
        f"*\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438:*\n"
        f"  \u0412\u0441\u0435\u0433\u043e: `{user_stats['total']:,}`\n"
        f"  \u041d\u043e\u0432\u044b\u0445: `{user_stats['new']:,}`\n"
        f"  \u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0445: `{user_stats['active']:,}`\n"
        f"  \u041a\u043e\u043c\u0430\u043d\u0434: `{user_stats['commands']:,}`"
    )
    await send_dm(user_id, text)

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
                f"{greeting} Я Бычок 🐂 — слежу за новостями российского рынка и присылаю важное прямо в личку\\.\n\n"
                "Настроим под тебя — займёт 1 минуту\."
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

    elif cmd == "/fix":
        if user_id in ADMIN_USER_IDS:
            args = text.split()[1:]
            await _handle_fix(db, user_id, args)
        else:
            await send_dm(user_id, "⛔ Нет доступа\\.")


    elif cmd == "/stats":
        if user_id in ADMIN_USER_IDS:
            period = text.split()[1] if len(text.split()) > 1 else "day"
            await _handle_stats(db, user_id, period)
        else:
            await send_dm(user_id, "\u26d4 \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430\\.")  # ⛔ Нет доступа.
    elif cmd == "/calendar":
        await _handle_calendar(db, user_id)

    elif cmd == "/unsubscribe":
        queries.set_user_tickers(db, user_id, [])
        await send_dm(user_id, "Вы отписались от всех уведомлений\\. Управление подпиской: */portfolio*")

    elif cmd == "/help":
        await send_dm(user_id, _help_text())

    elif cmd == "/feedback":
        body = text[len("/feedback"):].strip()
        if not body:
            await send_dm(user_id, "Напиши сообщение после команды: `/feedback текст`")
        else:
            from app.core.alerting import send_ops
            username = from_data.get("username") or str(user_id)
            await send_ops(
                f"💬 Фидбэк от @{username} (id={user_id}):\n{body}"
            )
            await send_dm(user_id, "✅ Спасибо, мы получили твоё сообщение\\!")
            logger.info(
                "feedback received user_id=%d", user_id,
                extra={"event": "feedback", "user_id": user_id},
            )

    else:
        await send_dm(
            user_id,
            "Команды: */portfolio*, */calendar*, */settings*, */help*\\.",
        )

    try:
        queries.log_bot_command(db, user_id, cmd)
    except Exception:
        pass

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

    # ── Main menu callbacks ───────────────────────────────────────────────────

    if data == "menu:portfolio":
        await answer_callback_query(cbq_id)
        await send_dm(user_id, _keyboard_header(len(subscribed)), reply_markup=_build_keyboard(subscribed))
        return

    if data == "menu:calendar":
        await answer_callback_query(cbq_id)
        await _handle_calendar(db, user_id)
        return

    if data == "menu:settings":
        await answer_callback_query(cbq_id)
        internal_id = _get_internal_user_id(db, user_id)
        s = queries.get_user_settings(db, internal_id) if internal_id else queries.DEFAULT_SETTINGS
        await send_dm(user_id, _settings_header(s), reply_markup=_build_settings_keyboard())
        return

    if data == "menu:help":
        await answer_callback_query(cbq_id)
        await send_dm(user_id, _help_text())
        return

    if data == "menu:status":
        await answer_callback_query(cbq_id)
        if user_id in ADMIN_USER_IDS:
            await _handle_status(user_id)
        return

    # s:{idx}:{current_open}:{done_cb} — toggle accordion
    if data.startswith("s:"):
        await answer_callback_query(cbq_id)
        parts = data.split(":")
        idx, current_open = int(parts[1]), int(parts[2])
        done_cb = ":".join(parts[3:]) if len(parts) > 3 else "done"
        new_open = -1 if idx == current_open else idx
        header = _keyboard_header(len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed, new_open, done_callback=done_cb))
        return

    # t:{ticker}:{open_sector}:{done_cb} — toggle individual ticker
    if data.startswith("t:"):
        parts = data.split(":")
        ticker, open_sector = parts[1], int(parts[2])
        done_cb = ":".join(parts[3:]) if len(parts) > 3 else "done"
        adding = ticker not in subscribed
        await answer_callback_query(cbq_id)
        queries.toggle_user_ticker(db, user_id, ticker, adding)
        if adding:
            subscribed.add(ticker)
        else:
            subscribed.discard(ticker)
        header = _keyboard_header(len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed, open_sector, done_callback=done_cb))
        return

    # sa:{idx}:{open_sector}:{done_cb} — toggle entire sector
    if data.startswith("sa:"):
        parts = data.split(":")
        idx, open_sector = int(parts[1]), int(parts[2])
        done_cb = ":".join(parts[3:]) if len(parts) > 3 else "done"
        _, sector_tickers = _SECTORS[idx]
        all_selected = all(t in subscribed for t in sector_tickers)
        await answer_callback_query(cbq_id)
        if all_selected:
            subscribed -= set(sector_tickers)
        else:
            subscribed |= set(sector_tickers)
        queries.set_user_tickers(db, user_id, list(subscribed))
        header = _keyboard_header(len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed, open_sector, done_callback=done_cb))
        return

    # all_on:{done_cb} — select all tickers
    if data == "all_on" or data.startswith("all_on:"):
        done_cb = data.split(":", 1)[1] if ":" in data else "done"
        await answer_callback_query(cbq_id)
        all_tickers = [t for _, tickers in _SECTORS for t in tickers]
        queries.set_user_tickers(db, user_id, all_tickers)
        subscribed = set(all_tickers)
        header = _keyboard_header(len(subscribed))
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(subscribed, done_callback=done_cb))
        return

    # all_off:{done_cb} — deselect all tickers
    if data == "all_off" or data.startswith("all_off:"):
        done_cb = data.split(":", 1)[1] if ":" in data else "done"
        await answer_callback_query(cbq_id)
        queries.set_user_tickers(db, user_id, [])
        header = _keyboard_header(0)
        await edit_dm(chat_id, msg_id, header, reply_markup=_build_keyboard(set(), done_callback=done_cb))
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
