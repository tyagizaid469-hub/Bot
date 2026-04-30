# ==========================================================
# FINAL PRODUCTION TELEGRAM MULTI SESSION USERBOT
# Stable | Railway Ready | Formatting Fix Included
# requirements.txt:
# telethon==1.41.1
# aiosqlite==0.20.0
# ==========================================================

import os
import re
import time
import asyncio
import logging
import aiosqlite

from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ==========================================================
# CONFIG
# ==========================================================

API_ID = int(os.getenv("API_ID", "36180474"))
API_HASH = os.getenv("API_HASH", "1f4ecc2133837a8a3c307f676cb95f88")

SOURCE_BOT = os.getenv("SOURCE_BOT", "@GmailFarmerBot")
DB_PATH = "bot.db"

SESSION_STRINGS = [
    (os.getenv("SESSION1") or "").strip(),
    (os.getenv("SESSION2") or "").strip(),
    (os.getenv("SESSION3") or "").strip(),
    (os.getenv("SESSION4") or "").strip(),
    (os.getenv("SESSION5") or "").strip(),
    (os.getenv("SESSION6") or "").strip(),
]

SESSION_STRINGS = [x for x in SESSION_STRINGS if x]

FETCH_DELAY = 1
JOB_DELAY = 2
CLEANUP_AFTER = 300
RETRY_BUSY_AFTER = 5

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ==========================================================
# GLOBALS
# ==========================================================

clients = []
locks = []
client_index = 0

CLIENT_STATE = {}
CLICKED = {}

# ==========================================================
# DATABASE
# ==========================================================

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
        CREATE TABLE IF NOT EXISTS registrations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            password TEXT,
            recovery_email TEXT,
            task_id TEXT,
            msg_id INTEGER,
            created_at INTEGER,
            state TEXT
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS jobs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            job_type TEXT,
            payload TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            created_at INTEGER,
            updated_at INTEGER,
            error TEXT DEFAULT ''
        )
        """)

        await db.commit()

# ==========================================================
# HELPERS
# ==========================================================

def now():
    return int(time.time())

def get_next_client():
    global client_index

    if not clients:
        return None, None

    idx = client_index % len(clients)
    client_index += 1
    return idx, clients[idx]

# ==========================================================
# FORMAT FIX
# ==========================================================

def clean_value(v):
    if not v:
        return ""

    v = v.strip()
    v = v.strip("'").strip('"')
    v = re.sub(r"\s+", " ", v)

    return v.strip()

def parse_task(text):

    text = text.replace("`", "").replace("’", "'")

    first = re.search(
        r"First name:\s*['\"]?(.+?)['\"]?\s*(?:\n|$)",
        text, re.I
    )

    last = re.search(
        r"Last name:\s*['\"]?(.+?)['\"]?\s*(?:\n|$)",
        text, re.I
    )

    email = re.search(
        r"Email:\s*['\"]?([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})['\"]?",
        text, re.I
    )

    password = re.search(
        r"Password:\s*['\"]?(.+?)['\"]?\s*(?:\n|$)",
        text, re.I
    )

    recovery = re.search(
        r"Recovery email[:\s]*\n?\s*([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
        text, re.I
    )

    if not recovery:
        recovery = re.search(
            r"add Recovery email\s*\n?\s*([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
            text, re.I
        )

    return {
        "first_name": clean_value(first.group(1)) if first else "",
        "last_name": clean_value(last.group(1)) if last else "",
        "email": clean_value(email.group(1)) if email else "",
        "password": clean_value(password.group(1)) if password else "",
        "recovery_email": clean_value(recovery.group(1)) if recovery else "Not Provided"
    }

# ==========================================================
# BUTTON CLICKER
# ==========================================================

async def click_button(msg, keyword):

    if not msg.buttons:
        return False

    msg_id = msg.id
    CLICKED.setdefault(msg_id, set())

    for row in msg.buttons:
        for btn in row:

            txt = (btn.text or "").lower().strip()

            if keyword in txt and keyword not in CLICKED[msg_id]:
                try:
                    await msg.click(text=btn.text)
                    CLICKED[msg_id].add(keyword)

                    logging.info(f"Clicked [{keyword}] on {msg_id}")
                    return True

                except Exception as e:
                    logging.error(f"Click error: {e}")

    return False

async def smart_click(msg):

    order = [
        "done",
        "complete",
        "confirm",
        "next",
        "continue",
        "start"
    ]

    for key in order:
        ok = await click_button(msg, key)
        if ok:
            await asyncio.sleep(1)
            return True

    return False

# ==========================================================
# SAVE
# ==========================================================

async def save_registration(user_id, msg_id, data):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
        INSERT INTO registrations(
            user_id,
            first_name,
            last_name,
            email,
            password,
            recovery_email,
            task_id,
            msg_id,
            created_at,
            state
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            data["first_name"],
            data["last_name"],
            data["email"],
            data["password"],
            data["recovery_email"],
            f"{user_id}_{msg_id}",
            msg_id,
            now(),
            "fetched"
        ))

        await db.commit()

# ==========================================================
# HANDLER
# ==========================================================

async def auto_handler(event):

    msg = event.message

    if not msg:
        return

    msg_id = msg.id
    text = (msg.text or "").lower()

    task = CLIENT_STATE.get(msg_id)

    if not task:
        return

    try:
        # Save when full data available
        if "email:" in text and "password:" in text:

            data = parse_task(msg.text)

            if data["email"]:
                await save_registration(
                    task["user_id"],
                    msg_id,
                    data
                )

                logging.info(f"Saved {data['email']}")

                CLIENT_STATE.pop(msg_id, None)
                CLICKED.pop(msg_id, None)
                return

        # Busy retry
        if "server busy" in text or "5 sec" in text:

            logging.warning("Busy server retry")

            await asyncio.sleep(RETRY_BUSY_AFTER)

            asyncio.create_task(
                fetch_task(task["user_id"])
            )

            CLIENT_STATE.pop(msg_id, None)
            CLICKED.pop(msg_id, None)
            return

        # Auto click
        await smart_click(msg)

    except Exception as e:
        logging.error(f"HANDLER ERROR: {e}")

# ==========================================================
# FETCH
# ==========================================================

async def fetch_task(user_id):

    idx, client = get_next_client()

    if client is None:
        logging.warning("No active clients")
        return

    async with locks[idx]:

        try:
            logging.info(f"Fetching task for {user_id}")

            async with client.conversation(
                SOURCE_BOT,
                timeout=30
            ) as conv:

                await conv.send_message(
                    "➕ Register a new Gmail"
                )

                msg = await conv.get_response()

            CLIENT_STATE[msg.id] = {
                "user_id": user_id,
                "client": idx,
                "created": time.time()
            }

            logging.info(f"Tracking {msg.id}")

            await smart_click(msg)

        except Exception as e:
            logging.error(f"FETCH ERROR: {e}")

# ==========================================================
# JOB LOOP
# ==========================================================

async def job_loop():

    while True:

        try:
            async with aiosqlite.connect(DB_PATH) as db:

                db.row_factory = aiosqlite.Row

                cur = await db.execute("""
                SELECT *
                FROM jobs
                WHERE status='pending'
                ORDER BY id ASC
                LIMIT 1
                """)

                job = await cur.fetchone()

                if not job:
                    await asyncio.sleep(JOB_DELAY)
                    continue

                await db.execute("""
                UPDATE jobs
                SET status='processing',
                    updated_at=?
                WHERE id=?
                """, (now(), job["id"]))

                await db.commit()

            if job["job_type"] == "fetch":
                await fetch_task(job["user_id"])

            async with aiosqlite.connect(DB_PATH) as db:

                await db.execute("""
                UPDATE jobs
                SET status='done',
                    updated_at=?
                WHERE id=?
                """, (now(), job["id"]))

                await db.commit()

        except Exception as e:
            logging.error(f"JOB LOOP ERROR: {e}")
            await asyncio.sleep(2)

# ==========================================================
# CLEANUP
# ==========================================================

async def cleanup_loop():

    while True:

        try:
            current = time.time()
            remove = []

            for msg_id, data in CLIENT_STATE.items():
                if current - data["created"] > CLEANUP_AFTER:
                    remove.append(msg_id)

            for msg_id in remove:
                CLIENT_STATE.pop(msg_id, None)
                CLICKED.pop(msg_id, None)

                logging.info(f"Cleanup {msg_id}")

        except Exception as e:
            logging.error(f"CLEANUP ERROR: {e}")

        await asyncio.sleep(30)

# ==========================================================
# CLIENTS
# ==========================================================

async def start_clients():

    for i, session in enumerate(SESSION_STRINGS):

        try:
            client = TelegramClient(
                StringSession(session),
                API_ID,
                API_HASH
            )

            await client.connect()

            if not await client.is_user_authorized():
                logging.warning(f"Unauthorized {i}")
                continue

            client.add_event_handler(
                auto_handler,
                events.NewMessage(from_users=SOURCE_BOT)
            )

            client.add_event_handler(
                auto_handler,
                events.MessageEdited(from_users=SOURCE_BOT)
            )

            clients.append(client)
            locks.append(asyncio.Lock())

            logging.info(f"Client {i} Ready")

        except Exception as e:
            logging.error(f"Client {i} failed: {e}")

# ==========================================================
# MAIN
# ==========================================================

async def main():

    await init_db()
    await start_clients()

    if not clients:
        logging.error("No clients started")
        return

    asyncio.create_task(job_loop())
    asyncio.create_task(cleanup_loop())

    logging.info("System Started Successfully")

    await asyncio.Event().wait()

# ==========================================================
# RUN
# ==========================================================

if __name__ == "__main__":
    asyncio.run(main())
