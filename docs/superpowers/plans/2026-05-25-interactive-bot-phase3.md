# Interactive Bot Phase 3 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить онбординг-wizard (5 шагов, inline keyboards, без состояния в БД) и постоянное главное меню (reply keyboard + inline) в Telegram-бот.

**Architecture:** Вся UI-логика в `app/bot/commands.py`. Состояние wizard закодировано в `callback_data` — явного state machine нет. Reply keyboard (`[☰ Меню]`) передаётся через существующий параметр `reply_markup` в `send_dm()` как `ReplyKeyboardMarkup` dict. Schema БД не меняется.

**Tech Stack:** Python async, Telegram Bot API (raw HTTP через aiohttp), pytest + pytest-asyncio, AsyncMock.

---

## Файловая карта

| Файл | Изменение |
|---|---|
| `app/bot/commands.py` | Основная работа: wizard, меню, help, callbacks |
| `app/main.py` | Добавить `_set_my_commands()` при старте |
| `tests/test_interactive_bot.py` | Новый файл тестов |

---

### Task 1: `setMyCommands` при старте

**Files:**
- Modify: `app/main.py`
- Test: `tests/test_interactive_bot.py`

- [ ] **Step 1: Написать падающий тест**

```python
# tests/test_interactive_bot.py
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import aiohttp


@pytest.mark.asyncio
async def test_set_my_commands_sends_correct_commands():
    """_set_my_commands() POST-ит правильный список команд в Telegram API."""
    posted_payloads: list[dict] = []

    async def mock_post(url, json=None, **kwargs):
        posted_payloads.append(json or {})
        mock_resp = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        return mock_resp

    mock_session = MagicMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    mock_session.post = mock_post

    with patch("aiohttp.ClientSession", return_value=mock_session):
        from app.main import _set_my_commands
        await _set_my_commands()

    assert len(posted_payloads) == 1
    commands = {c["command"] for c in posted_payloads[0]["commands"]}
    assert commands == {"portfolio", "calendar", "settings", "help"}
```

- [ ] **Step 2: Запустить тест, убедиться что падает**

```bash
cd /opt/newsparser/backend
python -m pytest tests/test_interactive_bot.py::test_set_my_commands_sends_correct_commands -v
```

Ожидаем: `FAILED` — `ImportError: cannot import name '_set_my_commands'`

- [ ] **Step 3: Реализовать `_set_my_commands` в `app/main.py`**

Добавить функцию ПЕРЕД блоком `@app.on_event("startup")`:

```python
async def _set_my_commands() -> None:
    """Register bot commands in Telegram so they appear in the / menu."""
    import aiohttp as _aiohttp
    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/setMyCommands"
    commands = [
        {"command": "portfolio", "description": "Мой портфель"},
        {"command": "calendar",  "description": "Ближайшие события"},
        {"command": "settings",  "description": "Настройки"},
        {"command": "help",      "description": "Помощь"},
    ]
    try:
        async with _aiohttp.ClientSession() as session:
            await session.post(url, json={"commands": commands})
        logger.info("bot commands registered", extra={"event": "set_my_commands_ok"})
    except Exception as exc:
        logger.warning("setMyCommands failed: %s", exc)
```

Добавить вызов в `startup()` после `seed_sources`:

```python
@app.on_event("startup")
async def startup() -> None:
    init_db()

    db = get_db()
    try:
        seed_sources(db)
    finally:
        db.close()

    await _tg_client.connect()
    await _set_my_commands()          # ← новая строка
    runner.start()
    logger.info("app started", extra={"event": "app_started"})
```

- [ ] **Step 4: Запустить тест, убедиться что проходит**

```bash
python -m pytest tests/test_interactive_bot.py::test_set_my_commands_sends_correct_commands -v
```

Ожидаем: `PASSED`

- [ ] **Step 5: Коммит**

```bash
git add app/main.py tests/test_interactive_bot.py
git commit -m "feat(bot): register bot commands via setMyCommands on startup"
```

---

### Task 2: Константы и вспомогательные функции (меню, help)

**Files:**
- Modify: `app/bot/commands.py`
- Test: `tests/test_interactive_bot.py`

- [ ] **Step 1: Написать падающие тесты**

```python
# добавить в tests/test_interactive_bot.py

def test_build_main_menu_keyboard_regular_user():
    from app.bot.commands import _build_main_menu_keyboard
    kb = _build_main_menu_keyboard(is_admin=False)
    all_data = {btn["callback_data"] for row in kb["inline_keyboard"] for btn in row}
    assert "menu:portfolio" in all_data
    assert "menu:calendar"  in all_data
    assert "menu:settings"  in all_data
    assert "menu:help"      in all_data
    assert "menu:status" not in all_data


def test_build_main_menu_keyboard_admin():
    from app.bot.commands import _build_main_menu_keyboard
    kb = _build_main_menu_keyboard(is_admin=True)
    all_data = {btn["callback_data"] for row in kb["inline_keyboard"] for btn in row}
    assert "menu:status" in all_data


def test_help_text_contains_sections():
    from app.bot.commands import _help_text
    text = _help_text()
    assert "portfolio" in text.lower() or "портфель" in text.lower()
    assert "calendar"  in text.lower() or "календарь" in text.lower()
    assert "settings"  in text.lower() or "настройки" in text.lower()


def test_reply_keyboard_constant_structure():
    from app.bot.commands import _REPLY_KEYBOARD
    assert "keyboard" in _REPLY_KEYBOARD
    assert _REPLY_KEYBOARD["keyboard"] == [[{"text": "☰ Меню"}]]
    assert _REPLY_KEYBOARD.get("resize_keyboard") is True
```

- [ ] **Step 2: Запустить тесты, убедиться что падают**

```bash
python -m pytest tests/test_interactive_bot.py::test_build_main_menu_keyboard_regular_user tests/test_interactive_bot.py::test_build_main_menu_keyboard_admin tests/test_interactive_bot.py::test_help_text_contains_sections tests/test_interactive_bot.py::test_reply_keyboard_constant_structure -v
```

Ожидаем: `FAILED` — `ImportError: cannot import name '_build_main_menu_keyboard'`

- [ ] **Step 3: Добавить константы и функции в `app/bot/commands.py`**

Добавить ПОСЛЕ блока `_QUIET_PRESETS` (перед функциями):

```python
# ── Reply keyboard (persistent bottom button) ─────────────────────────────────

_REPLY_KEYBOARD: dict = {
    "keyboard":       [[{"text": "☰ Меню"}]],
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
```

- [ ] **Step 4: Запустить тесты, убедиться что проходят**

```bash
python -m pytest tests/test_interactive_bot.py::test_build_main_menu_keyboard_regular_user tests/test_interactive_bot.py::test_build_main_menu_keyboard_admin tests/test_interactive_bot.py::test_help_text_contains_sections tests/test_interactive_bot.py::test_reply_keyboard_constant_structure -v
```

Ожидаем: `PASSED` (все 4)

- [ ] **Step 5: Коммит**

```bash
git add app/bot/commands.py tests/test_interactive_bot.py
git commit -m "feat(bot): add main menu keyboard, help text, reply keyboard constant"
```

---

### Task 3: Обновить `/start`, добавить `☰ Меню` и `/help`

**Files:**
- Modify: `app/bot/commands.py` (функция `handle_update`)
- Test: `tests/test_interactive_bot.py`

- [ ] **Step 1: Написать падающие тесты**

```python
# добавить в tests/test_interactive_bot.py

def _make_update(user_id: int, text: str, first_name: str = "Test", update_id: int = 1) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "from": {"id": user_id, "first_name": first_name},
            "text": text,
        },
    }


@pytest.mark.asyncio
async def test_start_new_user_shows_wizard_step1(db):
    """Новый пользователь (нет тикеров) видит приветствие с кнопкой 'Начать настройку'."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"text": text, "reply_markup": reply_markup})
        return 1

    with patch("app.bot.commands.send_dm", side_effect=capture_dm):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(111, "/start"))

    assert len(sent) == 1
    kb = sent[0]["reply_markup"]
    assert kb is not None
    all_data = [btn["callback_data"] for row in kb["inline_keyboard"] for btn in row]
    assert "onb:start" in all_data


@pytest.mark.asyncio
async def test_start_returning_user_shows_menu(db):
    """Вернувшийся пользователь (есть тикеры) получает приветствие + inline-меню."""
    from app.db import queries
    queries.set_user_tickers(db, 222, ["SBER"])

    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"text": text, "reply_markup": reply_markup})
        return 1

    with patch("app.bot.commands.send_dm", side_effect=capture_dm):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(222, "/start"))

    # Первое сообщение — приветствие с reply keyboard
    assert any(
        msg.get("reply_markup", {}).get("keyboard") == [[{"text": "☰ Меню"}]]
        for msg in sent
    )
    # Второе сообщение — inline меню
    all_callbacks = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "menu:portfolio" in all_callbacks


@pytest.mark.asyncio
async def test_menu_button_sends_inline_menu(db):
    """Нажатие '☰ Меню' открывает inline-меню."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with patch("app.bot.commands.send_dm", side_effect=capture_dm):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(333, "☰ Меню"))

    all_data = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "menu:portfolio" in all_data
    assert "menu:calendar"  in all_data


@pytest.mark.asyncio
async def test_help_command_sends_help_text(db):
    """/help отправляет текст справки."""
    sent_texts: list[str] = []

    async def capture_dm(user_id, text, **kwargs):
        sent_texts.append(text)
        return 1

    with patch("app.bot.commands.send_dm", side_effect=capture_dm):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(444, "/help"))

    assert len(sent_texts) == 1
    assert "portfolio" in sent_texts[0].lower() or "портфель" in sent_texts[0].lower()
```

- [ ] **Step 2: Запустить тесты, убедиться что падают**

```bash
python -m pytest tests/test_interactive_bot.py::test_start_new_user_shows_wizard_step1 tests/test_interactive_bot.py::test_start_returning_user_shows_menu tests/test_interactive_bot.py::test_menu_button_sends_inline_menu tests/test_interactive_bot.py::test_help_command_sends_help_text -v
```

Ожидаем: `FAILED`

- [ ] **Step 3: Обновить `handle_update()` в `app/bot/commands.py`**

Найти блок `if cmd == "/start":` и заменить на:

```python
    if cmd == "/start":
        tickers = queries.get_user_tickers(db, user_id)
        if tickers:
            # Returning user — show welcome + reply keyboard + menu
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
            text = (
                f"{greeting} Я *MOEX\\.news* — бот для инвесторов\\.\n\n"
                "📡 Слежу за 15\\+ источниками и присылаю важные новости "
                "по российскому рынку прямо в личку\\.\n\n"
                "Настроим бота под тебя — займёт 1 минуту\\."
            )
            await send_dm(
                user_id,
                text,
                reply_markup={
                    "inline_keyboard": [[
                        {"text": "Начать настройку 🚀", "callback_data": "onb:start"}
                    ]]
                },
            )
```

Добавить обработчик `☰ Меню` в `handle_update()` ПЕРЕД блоком `cmd = text.split()[0]...`:

```python
    # ── Reply keyboard: "☰ Меню" ──────────────────────────────────────────────
    if text == "☰ Меню":
        await _send_menu(user_id, is_admin=user_id in ADMIN_USER_IDS)
        logger.info(
            "bot command handled",
            extra={"event": "bot_command", "user_id": user_id, "cmd": "menu"},
        )
        return
```

Добавить `/help` в блоке `elif cmd == "..."` после `/calendar`:

```python
    elif cmd == "/help":
        await send_dm(user_id, _help_text())
```

Обновить fallback-ответ на неизвестные команды:

```python
    else:
        await send_dm(
            user_id,
            "Команды: */portfolio*, */calendar*, */settings*, */help*\\.",
        )
```

- [ ] **Step 4: Запустить тесты, убедиться что проходят**

```bash
python -m pytest tests/test_interactive_bot.py::test_start_new_user_shows_wizard_step1 tests/test_interactive_bot.py::test_start_returning_user_shows_menu tests/test_interactive_bot.py::test_menu_button_sends_inline_menu tests/test_interactive_bot.py::test_help_command_sends_help_text -v
```

Ожидаем: `PASSED` (все 4)

- [ ] **Step 5: Убедиться что старые тесты не сломались**

```bash
python -m pytest tests/test_portfolio.py -v
```

Ожидаем: все `PASSED`

- [ ] **Step 6: Коммит**

```bash
git add app/bot/commands.py tests/test_interactive_bot.py
git commit -m "feat(bot): update /start for wizard/returning flow, add /help and menu button"
```

---

### Task 4: Wizard callbacks (`onb:*`) и параметр `done_callback`

**Files:**
- Modify: `app/bot/commands.py` (`_build_keyboard`, `_handle_callback`)
- Test: `tests/test_interactive_bot.py`

- [ ] **Step 1: Написать падающие тесты**

```python
# добавить в tests/test_interactive_bot.py

def _make_callback(user_id: int, data: str, message_id: int = 1, update_id: int = 1) -> dict:
    return {
        "update_id": update_id,
        "callback_query": {
            "id": "cbq_test",
            "from": {"id": user_id, "first_name": "Test"},
            "data": data,
            "message": {
                "message_id": message_id,
                "chat": {"id": user_id},
            },
        },
    }


def test_build_keyboard_custom_done_callback():
    """_build_keyboard() с done_callback='onb:2' ставит нужный callback_data."""
    from app.bot.commands import _build_keyboard
    kb = _build_keyboard(set(), done_callback="onb:2")
    all_data = [btn["callback_data"] for row in kb["inline_keyboard"] for btn in row]
    assert "onb:2" in all_data
    assert "done" not in all_data


def test_build_keyboard_default_done_callback():
    """_build_keyboard() без done_callback сохраняет поведение по умолчанию."""
    from app.bot.commands import _build_keyboard
    kb = _build_keyboard(set())
    all_data = [btn["callback_data"] for row in kb["inline_keyboard"] for btn in row]
    assert "done" in all_data


@pytest.mark.asyncio
async def test_onb_start_sends_portfolio_keyboard(db):
    """`onb:start` отправляет новое сообщение с портфельным accordion keyboard."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.edit_dm", AsyncMock(return_value=True)),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "onb:start"))

    # Должен быть отправлен keyboard с onb:2 в кнопке "Готово"
    all_data = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "onb:2" in all_data


@pytest.mark.asyncio
async def test_onb_2_sends_score_keyboard(db):
    """`onb:2` отправляет шаг 3 — выбор порога важности."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"text": text, "reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.edit_dm", AsyncMock(return_value=True)),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "onb:2"))

    all_data = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "onb:score:30" in all_data
    assert "onb:skip:score" in all_data


@pytest.mark.asyncio
async def test_onb_score_saves_and_sends_quiet_keyboard(db):
    """`onb:score:50` сохраняет порог и показывает шаг 4 (тихие часы)."""
    from app.db import queries
    queries.upsert_user(db, 111, None, "Test")

    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.edit_dm", AsyncMock(return_value=True)),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "onb:score:50"))

    all_data = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "onb:quiet:off" in all_data
    assert "onb:skip:quiet" in all_data

    # Проверяем что порог сохранился в БД
    user_row = queries.get_user(db, 111)
    s = queries.get_user_settings(db, user_row["id"])
    assert s["min_score"] == 50


@pytest.mark.asyncio
async def test_onb_quiet_saves_and_shows_done(db):
    """`onb:quiet:22:8` сохраняет тихие часы и показывает шаг 5 с reply keyboard."""
    from app.db import queries
    queries.upsert_user(db, 111, None, "Test")
    queries.set_user_tickers(db, 111, ["SBER"])

    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.edit_dm", AsyncMock(return_value=True)),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "onb:quiet:22:8"))

    # Reply keyboard должен появиться
    reply_keyboards = [
        msg["reply_markup"]
        for msg in sent
        if msg.get("reply_markup", {}).get("keyboard") == [[{"text": "☰ Меню"}]]
    ]
    assert len(reply_keyboards) == 1

    # Тихие часы сохранились
    user_row = queries.get_user(db, 111)
    s = queries.get_user_settings(db, user_row["id"])
    assert s["quiet_from"] == 22
    assert s["quiet_to"] == 8


@pytest.mark.asyncio
async def test_onb_skip_quiet_shows_done_without_saving(db):
    """`onb:skip:quiet` показывает шаг 5, не меняя тихие часы."""
    from app.db import queries
    queries.upsert_user(db, 111, None, "Test")
    queries.set_user_tickers(db, 111, ["GAZP"])

    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.edit_dm", AsyncMock(return_value=True)),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "onb:skip:quiet"))

    reply_keyboards = [
        msg for msg in sent
        if msg.get("reply_markup", {}).get("keyboard") == [[{"text": "☰ Меню"}]]
    ]
    assert len(reply_keyboards) == 1
```

- [ ] **Step 2: Запустить тесты, убедиться что падают**

```bash
python -m pytest tests/test_interactive_bot.py::test_build_keyboard_custom_done_callback tests/test_interactive_bot.py::test_onb_start_sends_portfolio_keyboard tests/test_interactive_bot.py::test_onb_2_sends_score_keyboard tests/test_interactive_bot.py::test_onb_score_saves_and_sends_quiet_keyboard tests/test_interactive_bot.py::test_onb_quiet_saves_and_shows_done tests/test_interactive_bot.py::test_onb_skip_quiet_shows_done_without_saving -v
```

Ожидаем: `FAILED`

- [ ] **Step 3: Добавить `done_callback` в `_build_keyboard()`**

Найти строку с `"✔️ Готово"` в `_build_keyboard` и добавить параметр:

```python
def _build_keyboard(subscribed: set[str], open_sector: int = -1, done_callback: str = "done") -> dict:
    """Accordion keyboard: sectors collapse/expand in place."""
    rows = []
    # ... (весь существующий код без изменений) ...
    rows.append([
        {"text": "✅ Выбрать всё", "callback_data": "all_on"},
        {"text": "🗑 Снять всё",   "callback_data": "all_off"},
    ])
    rows.append([{"text": "✔️ Готово", "callback_data": done_callback}])  # ← done_callback
    return {"inline_keyboard": rows}
```

- [ ] **Step 4: Добавить wizard-тексты и keyboard-функции в `app/bot/commands.py`**

Добавить ПЕРЕД функцией `_get_internal_user_id`:

```python
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
                {"text": "10",     "callback_data": "onb:score:10"},
                {"text": "20",     "callback_data": "onb:score:20"},
                {"text": "✅ 30",  "callback_data": "onb:score:30"},
                {"text": "50",     "callback_data": "onb:score:50"},
                {"text": "70",     "callback_data": "onb:score:70"},
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
                {"text": "Выкл",   "callback_data": "onb:quiet:off"},
                {"text": "22–08",  "callback_data": "onb:quiet:22:8"},
                {"text": "23–08",  "callback_data": "onb:quiet:23:8"},
                {"text": "23–09",  "callback_data": "onb:quiet:23:9"},
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
```

- [ ] **Step 5: Добавить ветки `onb:*` в `_handle_callback()`**

Добавить В НАЧАЛО функции `_handle_callback`, сразу после строк получения `cbq_id`, `data`, `user_id`, `message`, `chat_id`, `msg_id`:

```python
    # ── Onboarding wizard ─────────────────────────────────────────────────────

    # onb:start — show step 2 (portfolio keyboard with onb:2 done button)
    if data == "onb:start":
        await answer_callback_query(cbq_id)
        await edit_dm(chat_id, msg_id, _onb_step2_text().split("\n")[0],
                      reply_markup={"inline_keyboard": []})
        subscribed = set(queries.get_user_tickers(db, user_id))
        await send_dm(user_id, _onb_step2_text(), reply_markup=_build_keyboard(subscribed, done_callback="onb:2"))
        return

    # onb:2 — tickers already saved via t: callbacks; show step 3 (score)
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
        s       = queries.get_user_settings(db, internal_id) if internal_id else queries._SETTINGS_DEFAULTS
        await send_dm(
            user_id,
            _onb_step5_text(len(tickers), s["min_score"], s["quiet_from"], s["quiet_to"]),
            reply_markup=_REPLY_KEYBOARD,
        )
        return
```

- [ ] **Step 6: Запустить тесты, убедиться что проходят**

```bash
python -m pytest tests/test_interactive_bot.py::test_build_keyboard_custom_done_callback tests/test_interactive_bot.py::test_build_keyboard_default_done_callback tests/test_interactive_bot.py::test_onb_start_sends_portfolio_keyboard tests/test_interactive_bot.py::test_onb_2_sends_score_keyboard tests/test_interactive_bot.py::test_onb_score_saves_and_sends_quiet_keyboard tests/test_interactive_bot.py::test_onb_quiet_saves_and_shows_done tests/test_interactive_bot.py::test_onb_skip_quiet_shows_done_without_saving -v
```

Ожидаем: `PASSED` (все 7)

- [ ] **Step 7: Убедиться что старые тесты не сломались**

```bash
python -m pytest tests/test_portfolio.py -v
```

Ожидаем: все `PASSED`

- [ ] **Step 8: Коммит**

```bash
git add app/bot/commands.py tests/test_interactive_bot.py
git commit -m "feat(bot): onboarding wizard steps 1-5 with onb:* callbacks"
```

---

### Task 5: Menu callbacks (`menu:*`)

**Files:**
- Modify: `app/bot/commands.py` (`_handle_callback`)
- Test: `tests/test_interactive_bot.py`

- [ ] **Step 1: Написать падающие тесты**

```python
# добавить в tests/test_interactive_bot.py

@pytest.mark.asyncio
async def test_menu_portfolio_opens_keyboard(db):
    """`menu:portfolio` открывает accordion keyboard тикеров."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "menu:portfolio"))

    all_data = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    # Keyboard должен содержать тикер-кнопки (sectors) и кнопку "Готово"
    assert "done" in all_data or any("s:" in d for d in all_data)


@pytest.mark.asyncio
async def test_menu_help_sends_help_text(db):
    """`menu:help` отправляет текст справки."""
    sent_texts: list[str] = []

    async def capture_dm(user_id, text, **kwargs):
        sent_texts.append(text)
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "menu:help"))

    assert len(sent_texts) == 1
    assert "портфель" in sent_texts[0].lower() or "portfolio" in sent_texts[0].lower()


@pytest.mark.asyncio
async def test_menu_settings_opens_settings_keyboard(db):
    """`menu:settings` открывает keyboard настроек."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "menu:settings"))

    all_data = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "cfg:score" in all_data
    assert "cfg:quiet" in all_data


@pytest.mark.asyncio
async def test_menu_status_blocked_for_non_admin(db):
    """`menu:status` не отправляет ничего обычному пользователю."""
    sent_texts: list[str] = []

    async def capture_dm(user_id, text, **kwargs):
        sent_texts.append(text)
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(99999, "menu:status"))

    assert len(sent_texts) == 0
```

- [ ] **Step 2: Запустить тесты, убедиться что падают**

```bash
python -m pytest tests/test_interactive_bot.py::test_menu_portfolio_opens_keyboard tests/test_interactive_bot.py::test_menu_help_sends_help_text tests/test_interactive_bot.py::test_menu_settings_opens_settings_keyboard tests/test_interactive_bot.py::test_menu_status_blocked_for_non_admin -v
```

Ожидаем: `FAILED`

- [ ] **Step 3: Добавить ветки `menu:*` в `_handle_callback()`**

Добавить ПОСЛЕ блока `onb:*` callbacks (перед секцией `# s:{idx}:...`):

```python
    # ── Main menu callbacks ───────────────────────────────────────────────────

    if data == "menu:portfolio":
        await answer_callback_query(cbq_id)
        subscribed = set(queries.get_user_tickers(db, user_id))
        await send_dm(user_id, _keyboard_header(len(subscribed)), reply_markup=_build_keyboard(subscribed))
        return

    if data == "menu:calendar":
        await answer_callback_query(cbq_id)
        await _handle_calendar(db, user_id)
        return

    if data == "menu:settings":
        await answer_callback_query(cbq_id)
        internal_id = _get_internal_user_id(db, user_id)
        s = queries.get_user_settings(db, internal_id) if internal_id else queries._SETTINGS_DEFAULTS
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
```

- [ ] **Step 4: Запустить тесты, убедиться что проходят**

```bash
python -m pytest tests/test_interactive_bot.py::test_menu_portfolio_opens_keyboard tests/test_interactive_bot.py::test_menu_help_sends_help_text tests/test_interactive_bot.py::test_menu_settings_opens_settings_keyboard tests/test_interactive_bot.py::test_menu_status_blocked_for_non_admin -v
```

Ожидаем: `PASSED` (все 4)

- [ ] **Step 5: Финальный прогон всех тестов**

```bash
python -m pytest tests/ -v --tb=short 2>&1 | tail -40
```

Ожидаем: все `PASSED`, 0 `FAILED`

- [ ] **Step 6: Финальный коммит**

```bash
git add app/bot/commands.py tests/test_interactive_bot.py
git commit -m "feat(bot): add menu:* callbacks for main menu navigation"
```

---

## Итог изменений

| Файл | Что изменено |
|---|---|
| `app/main.py` | `_set_my_commands()` + вызов в `startup()` |
| `app/bot/commands.py` | `_REPLY_KEYBOARD`, `_build_main_menu_keyboard`, `_help_text`, `_send_menu`, wizard-тексты/keyboard, `done_callback` в `_build_keyboard`, `onb:*` и `menu:*` в `_handle_callback`, обновлён `/start` |
| `tests/test_interactive_bot.py` | 16 новых тестов |
