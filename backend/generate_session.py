"""
Run once to create/refresh the Telethon session file (telegram_session.session).

  python generate_session.py

The script:
  1. Asks for phone number and Telegram auth code.
  2. Writes telegram_session.session in the current directory.
  3. Prints the equivalent TELEGRAM_SESSION_STRING as a backup for .env.

After running, restart the service — it will pick up the new session file automatically.
"""

import asyncio
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

API_ID   = 30883658
API_HASH = "fce6de9381ecc0ead5008249180c09f6"

SESSION_FILE = Path("telegram_session")


async def main() -> None:
    phone = input("Введите номер телефона (+7XXXXXXXXXX): ").strip()

    # Auth via StringSession first, then transfer to file session.
    tmp = TelegramClient(StringSession(), API_ID, API_HASH)
    await tmp.connect()
    await tmp.send_code_request(phone)

    code = input("Введите код из Telegram: ").strip()
    try:
        await tmp.sign_in(phone, code)
    except SessionPasswordNeededError:
        password = input("Введите пароль двухфакторной аутентификации: ").strip()
        await tmp.sign_in(password=password)

    # Transfer auth to file session
    dst = TelegramClient(str(SESSION_FILE), API_ID, API_HASH)
    dst.session.set_dc(tmp.session.dc_id, tmp.session.server_address, tmp.session.port)  # type: ignore[union-attr]
    dst.session.auth_key = tmp.session.auth_key  # type: ignore[union-attr]
    dst.session.save()  # type: ignore[union-attr]

    backup_string = tmp.session.save()  # type: ignore[union-attr]
    await tmp.disconnect()  # type: ignore[misc]

    print(f"\n✅ Файл сессии создан: {SESSION_FILE.with_suffix('.session')}")
    print("   Перезапустите сервис — он подхватит сессию автоматически.\n")
    print("Резервная строка (для TELEGRAM_SESSION_STRING в .env):")
    print(backup_string)


asyncio.run(main())
