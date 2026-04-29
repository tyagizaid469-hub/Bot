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

# ========= STATE =========
CLIENT_STATE = {}
STEP_STATE = {}
CLICKED = {}

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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        job_type TEXT,
        payload TEXT,
        status TEXT,
        created_at INTEGER,
        updated_at INTEGER,
        error TEXT
    )
    """)

    con.commit()
    con.close()

# ========= CLIENT =========
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

# ========= SAFE CONNECT =========
async def safe_client(client):
    try:
        if not client.is_connected():
            await client.connect()
        return True
    except:
        return False

# ========= CLICK ENGINE =========
async def click(msg, msg_id, keyword):
    try:
        msg = await msg.client.get_messages(SOURCE, ids=msg_id)

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

    except Exception as e:
        print("[CLICK ERROR]", e)
        return False

# ========= HANDLER =========
async def handler(event):
    msg = event.message
    if not msg:
        return

    msg_id = msg.id
    task = CLIENT_STATE.get(msg_id)
    if not task:
        return

    try:
        msg = await event.client.get_messages(SOURCE, ids=msg_id)
        text = (msg.text or "").lower()

        STEP_STATE.setdefault(msg_id, {
            "done": False,
            "complete": False,
            "confirm": False
        })

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
                    "done"
                ))

                con.commit()
                con.close()

                print("[SAVE] ✅", email)

                CLIENT_STATE.pop(msg_id, None)
                STEP_STATE.pop(msg_id, None)
                CLICKED.pop(msg_id, None)
                return

        # ========= BUTTON FLOW =========
        if not STEP_STATE[msg_id]["done"]:
            if await click(msg, msg_id, "done"):
                STEP_STATE[msg_id]["done"] = True
                return

        if STEP_STATE[msg_id]["done"] and not STEP_STATE[msg_id]["complete"]:
            if await click(msg, msg_id, "complete"):
                STEP_STATE[msg_id]["complete"] = True
                return

        if STEP_STATE[msg_id]["complete"] and not STEP_STATE[msg_id]["confirm"]:
            if await click(msg, msg_id, "confirm"):
                STEP_STATE[msg_id]["confirm"] = True
                return

    except Exception as e:
        print("[ERROR]", msg_id, e)

# ========= FETCH =========
async def fetch_task(user_id):
    idx, client = get_client()
    lock = locks[idx]

    async with lock:
        try:
            if not await safe_client(client):
                print("[RECONNECT FAILED]")
                return

            await client.send_message(SOURCE, "➕ Register a new Gmail")
            await asyncio.sleep(2)

            msg = (await client.get_messages(SOURCE, limit=1))[0]

            CLIENT_STATE[msg.id] = {
                "user_id": user_id,
                "client": idx,
                "time": time.time()
            }

            print("[TRACK]", msg.id)

        except Exception as e:
            print("[FETCH ERROR]", e)

# ========= JOB LOOP =========
async def job_loop():
    while True:
        try:
            con = db()
            cur = con.cursor()

            cur.execute("SELECT * FROM jobs WHERE status='pending' LIMIT 1")
            job = cur.fetchone()

            if not job:
                con.close()
                await asyncio.sleep(1)
                continue

            job_id = job["id"]

            cur.execute("UPDATE jobs SET status='processing' WHERE id=?", (job_id,))
            con.commit()
            con.close()

            if job["job_type"] == "fetch":
                await fetch_task(job["user_id"])

            con = db()
            cur = con.cursor()
            cur.execute("UPDATE jobs SET status='done' WHERE id=?", (job_id,))
            con.commit()
            con.close()

        except Exception as e:
            print("[JOB ERROR]", e)
            await asyncio.sleep(2)

# ========= HEALTH CHECK =========
async def health_check():
    while True:
        for i, client in enumerate(clients):
            try:
                if not client.is_connected():
                    await client.connect()
                    print(f"[RECONNECT] Client {i}")
            except:
                print(f"[FAILED] Client {i}")

        await asyncio.sleep(10)

# ========= START =========
async def main():
    init_db()

    for i, c in enumerate(clients):
        await c.connect()
        print(f"[READY] Client {i}")

        c.add_event_handler(handler, events.NewMessage(from_users=SOURCE))
        c.add_event_handler(handler, events.MessageEdited(from_users=SOURCE))

    asyncio.create_task(job_loop())
    asyncio.create_task(health_check())

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
