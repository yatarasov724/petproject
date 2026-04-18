#!/usr/bin/env python3
"""
Pre-flight check — run before first launch to verify configuration.

Usage:
    cd backend
    python scripts/check_setup.py

What is checked:
    1. Required env vars present
    2. Telegram bot token valid (getMe API call)
    3. Telegram channel accessible (bot is member/admin)
    4. Database path is writable
    5. At least one RSS feed responds with 200
"""

import asyncio
import os
import sys
from pathlib import Path

# Allow running from any working directory
sys.path.insert(0, str(Path(__file__).parent.parent))

import aiohttp


PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
WARN = "\033[93m[WARN]\033[0m"
INFO = "\033[94m[INFO]\033[0m"

_errors = 0


def ok(msg: str) -> None:
    print(f"{PASS} {msg}")


def fail(msg: str) -> None:
    global _errors
    _errors += 1
    print(f"{FAIL} {msg}")


def warn(msg: str) -> None:
    print(f"{WARN} {msg}")


def info(msg: str) -> None:
    print(f"{INFO} {msg}")


# ── 1. env vars ───────────────────────────────────────────────────────────────

def check_env() -> tuple[str, str]:
    print("\n── Environment variables ────────────────────────────────────────────")
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    channel = os.environ.get("TELEGRAM_CHANNEL_ID", "")

    if not token:
        fail("TELEGRAM_BOT_TOKEN is not set")
    elif len(token) < 20 or ":" not in token:
        fail(f"TELEGRAM_BOT_TOKEN looks wrong: {token[:15]}...")
    else:
        ok(f"TELEGRAM_BOT_TOKEN is set ({token[:10]}...)")

    if not channel:
        fail("TELEGRAM_CHANNEL_ID is not set")
    else:
        ok(f"TELEGRAM_CHANNEL_ID = {channel}")

    db_url = os.environ.get("DATABASE_URL", "sqlite:///./moex_assistant.db")
    info(f"DATABASE_URL = {db_url}")

    dry_run = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")
    if dry_run:
        warn("DRY_RUN=true — Telegram sends will be skipped (log only)")
    else:
        info("DRY_RUN=false — messages will be sent to real channel")

    return token, channel


# ── 2. telegram token ─────────────────────────────────────────────────────────

async def check_telegram_token(session: aiohttp.ClientSession, token: str) -> bool:
    print("\n── Telegram bot token ───────────────────────────────────────────────")
    if not token:
        fail("Cannot check token — TELEGRAM_BOT_TOKEN not set")
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/getMe"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            body = await resp.json()
            if resp.status == 200 and body.get("ok"):
                bot = body["result"]
                ok(f"Bot valid: @{bot.get('username')} (id={bot.get('id')})")
                return True
            else:
                fail(f"getMe returned {resp.status}: {body.get('description', '')}")
                return False
    except Exception as exc:
        fail(f"getMe request failed: {exc}")
        return False


# ── 3. telegram channel ───────────────────────────────────────────────────────

async def check_telegram_channel(
    session: aiohttp.ClientSession, token: str, channel: str
) -> None:
    print("\n── Telegram channel access ──────────────────────────────────────────")
    if not token or not channel:
        fail("Skipping — token or channel_id not set")
        return
    try:
        url = f"https://api.telegram.org/bot{token}/getChat"
        async with session.get(
            url, params={"chat_id": channel}, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            body = await resp.json()
            if resp.status == 200 and body.get("ok"):
                chat = body["result"]
                chat_type = chat.get("type", "unknown")
                title = chat.get("title", chat.get("username", "?"))
                ok(f"Channel accessible: {title!r} ({chat_type})")

                # Check bot permissions for channel
                if chat_type == "channel":
                    info("For channels, bot must be an admin with 'Post messages' permission")
                elif chat_type in ("group", "supergroup"):
                    info("For groups, bot must be a member")
            else:
                desc = body.get("description", "")
                if "not found" in desc.lower() or "chat not found" in desc.lower():
                    fail(
                        f"Channel {channel!r} not found — check TELEGRAM_CHANNEL_ID. "
                        "For private channels use numeric ID like -1001234567890"
                    )
                elif "bot is not a member" in desc.lower():
                    fail(f"Bot is not a member of channel {channel!r} — add the bot first")
                else:
                    fail(f"getChat returned {resp.status}: {desc}")
    except Exception as exc:
        fail(f"getChat request failed: {exc}")


# ── 4. database path ──────────────────────────────────────────────────────────

def check_database() -> None:
    print("\n── Database ─────────────────────────────────────────────────────────")
    db_url = os.environ.get("DATABASE_URL", "sqlite:///./moex_assistant.db")
    db_path_str = db_url.replace("sqlite:///", "")
    db_path = Path(db_path_str)

    if db_path.is_absolute():
        parent = db_path.parent
    else:
        parent = Path.cwd() / db_path.parent

    if not parent.exists():
        try:
            parent.mkdir(parents=True, exist_ok=True)
            ok(f"Created directory: {parent}")
        except Exception as exc:
            fail(f"Cannot create DB directory {parent}: {exc}")
            return

    # Try write test
    test_file = parent / ".write_test"
    try:
        test_file.write_text("test")
        test_file.unlink()
        ok(f"DB directory is writable: {parent}")
    except Exception as exc:
        fail(f"DB directory not writable: {parent}: {exc}")
        return

    if db_path.exists() if db_path.is_absolute() else (Path.cwd() / db_path).exists():
        info(f"Database file already exists — will not be overwritten")
    else:
        info(f"Database file will be created on first startup: {db_path_str}")


# ── 5. rss feeds ─────────────────────────────────────────────────────────────

async def check_rss_feeds(session: aiohttp.ClientSession) -> None:
    print("\n── RSS feeds (spot check) ───────────────────────────────────────────")
    feeds = [
        ("RBC",        "https://rss.rbc.ru/finances/news.rss"),
        ("TASS",       "https://tass.ru/rss/v2.xml"),
        ("Interfax",   "https://www.interfax.ru/rss.asp"),
    ]
    for name, url in feeds:
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=8),
                headers={"User-Agent": "MOEXNewsBot/1.0"},
            ) as resp:
                if resp.status == 200:
                    content_type = resp.headers.get("Content-Type", "")
                    ok(f"{name}: HTTP 200 ({content_type.split(';')[0].strip()})")
                elif resp.status == 304:
                    ok(f"{name}: HTTP 304 (Not Modified — feed supports ETag)")
                else:
                    warn(f"{name}: HTTP {resp.status} — feed may be blocked or moved")
        except asyncio.TimeoutError:
            warn(f"{name}: timeout — may be slow or geo-blocked")
        except Exception as exc:
            warn(f"{name}: error — {exc}")


# ── main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    print("=" * 60)
    print("  MOEX News Assistant — Pre-flight Check")
    print("=" * 60)

    # Load .env if present (same logic as pydantic-settings)
    _load_dotenv()

    token, channel = check_env()
    check_database()

    connector = aiohttp.TCPConnector(ssl=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        token_ok = await check_telegram_token(session, token)
        if token_ok:
            await check_telegram_channel(session, token, channel)
        await check_rss_feeds(session)

    print("\n" + "=" * 60)
    if _errors == 0:
        print(f"\033[92m  All checks passed — ready to launch!\033[0m")
        print("\n  Start the service:")
        print("    uvicorn app.main:app --reload")
        print("  Or with Docker:")
        print("    docker compose up --build")
    else:
        print(f"\033[91m  {_errors} check(s) failed — fix the issues above first.\033[0m")
    print("=" * 60 + "\n")

    sys.exit(0 if _errors == 0 else 1)


def _load_dotenv() -> None:
    """Minimal .env loader — reads key=value lines, doesn't override existing env."""
    for candidate in ("../.env", ".env"):
        path = Path(candidate)
        if path.exists():
            for line in path.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
            info(f"Loaded env from {path}")
            break


if __name__ == "__main__":
    asyncio.run(main())
