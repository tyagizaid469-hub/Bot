import os
import time
import asyncio
import sqlite3
import re
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ========= CONFIG =========
api_id = 36180474
api_hash = "1f4ecc2133837a8a3c307f676cb95f88"
SOURCE = "@GmailFarmerBot"
DB_PATH = "bot.db"

SESSION_STRINGS = [
    (os.getenv("SESSION5") or "").strip(),
    (os.getenv("SESSION6") or "").strip(),
]
SESSION_STRINGS = [s for s in SESSION_STRINGS if s]

clients = []
locks = []

for s in SESSION_STRINGS:
    clients.append(TelegramClient(StringSession(s), api_id, api_hash))
    locks.append(asyncio.Lock())

client_index = 0

# ========= GLOBAL =========
CLIENT_STATE = {}   # msg_id -> task info
CLICKED = {}        # msg_id -> set()

# ========= DB =========
def db():
    con = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = db()
    cur = con.cursor()

    cur.execute("""
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

    con.commit()
    con.close()

# ========= HELPERS =========
def get_client():
    global client_index
    i = client_index % len(clients)
    client_index += 1
    return i, clients[i]

# ========= PARSE =========
def parse_task(text):
    email = re.search(r'Email:\s*([^\n]+)', text)
    password = re.search(r'Password:\s*([^\n]+)', text)
    first = re.search(r'First name:\s*([^\n]+)', text)
    last = re.search(r'Last name:\s*([^\n]+)', text)
    recovery = re.search(r'Recovery email\s*([^\s\n]+)', text, re.I)

    return (
        first.group(1).strip() if first else "",
        last.group(1).strip() if last else "",
        email.group(1).strip() if email else "",
        password.group(1).strip() if password else "",
        recovery.group(1).strip() if recovery else "Not Provided"
    )

# ========= CLICK ENGINE (FIXED) =========
async def click(msg, msg_id, keyword):
    if not msg.buttons:
        return False

    CLICKED.setdefault(msg_id, set())

    for row in msg.buttons:
        for btn in row:
            t = (btn.text or "").lower()

            if keyword in t and keyword not in CLICKED[msg_id]:
                await msg.click(text=btn.text)
                CLICKED[msg_id].add(keyword)
                print(f"[CLICK] {keyword} | {msg_id}")
                return True

    return False

# ========= HANDLER =========
async def handler(event):
    msg = event.message
    if not msg:
        return

    msg_id = msg.id
    text = (msg.text or "").lower()

    task = CLIENT_STATE.get(msg_id)
    if not task:
        return

    try:
        # 🔥 ALWAYS REFRESH (IMPORTANT FIX)
        msg = await event.client.get_messages(SOURCE, ids=msg_id)

        # ========= SAVE =========
        if "recovery email" in text:
            first, last, email, password, recovery = parse_task(msg.text or "")

            if email:
                con = db()
                cur = con.cursor()

                cur.execute("""
                INSERT INTO registrations(
                    user_id, first_name, last_name, email, password,
                    recovery_email, task_id, msg_id, created_at, state
                )
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """, (
                    task["user_id"],
                    first, last, email, password,
                    recovery,
                    f"{task['user_id']}_{msg_id}",
                    msg_id,
                    int(time.time()),
                    "fetched"
                ))

                con.commit()
                con.close()

                print("[SAVE] ✅", email)

                CLIENT_STATE.pop(msg_id, None)
                CLICKED.pop(msg_id, None)
                return

        # ========= BUTTON FLOW (ORDER FIXED) =========
        await click(msg, msg_id, "done")
        await click(msg, msg_id, "complete")
        await click(msg, msg_id, "confirm")

    except Exception as e:
        print("[ERROR]", e)

# ========= FETCH =========
async def fetch_task(user_id):
    idx, client = get_client()

    async with locks[idx]:
        await client.send_message(SOURCE, "➕ Register a new Gmail")
        await asyncio.sleep(1)

        msg = (await client.get_messages(SOURCE, limit=1))[0]

        CLIENT_STATE[msg.id] = {
            "user_id": user_id,
            "client": idx,
            "created": time.time()
        }

        print("[TRACK]", msg.id)

# ========= LOOP =========
async def main():
    init_db()

    for i, c in enumerate(clients):
        await c.connect()
        if await c.is_user_authorized():
            c.add_event_handler(handler, events.NewMessage(from_users=SOURCE))
            c.add_event_handler(handler, events.MessageEdited(from_users=SOURCE))
            print(f"[READY] Client {i}")

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
