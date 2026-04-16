"""
Запустить один раз для получения StringSession.
Полученную строку вставить в .env как TELEGRAM_SESSION_STRING
"""
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

API_ID = 30883658
API_HASH = "fce6de9381ecc0ead5008249180c09f6"


async def main():
    phone = input("Введите номер телефона (+7XXXXXXXXXX): ").strip()

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    await client.send_code_request(phone)
    code = input("Введите код из Telegram: ").strip()

    try:
        await client.sign_in(phone, code)
    except SessionPasswordNeededError:
        password = input("Введите пароль двухфакторной аутентификации: ").strip()
        await client.sign_in(password=password)

    print("\n✅ Успешно! Скопируйте строку ниже в .env как TELEGRAM_SESSION_STRING=")
    print("\n" + client.session.save() + "\n")
    await client.disconnect()


asyncio.run(main())
