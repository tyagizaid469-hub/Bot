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

if not SESSION_STRINGS:
    raise Exception("No sessions provided")

clients = []
locks = []

for s in SESSION_STRINGS:
    clients.append(TelegramClient(StringSession(s), api_id, api_hash))
    locks.append(asyncio.Lock())

client_index = 0

# ========= GLOBAL =========
TASKS = {}
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
        email TEXT UNIQUE,
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
        user_id INTEGER NOT NULL,
        job_type TEXT NOT NULL,
        payload TEXT DEFAULT '',
        status TEXT DEFAULT 'pending',
        created_at INTEGER,
        updated_at INTEGER,
        error TEXT DEFAULT ''
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

def get_buttons(msg):
    return [btn.text.lower() for row in (msg.buttons or []) for btn in row]

async def click_button(msg, keyword):
    for row in msg.buttons or []:
        for btn in row:
            if keyword in (btn.text or "").lower():
                await msg.click(text=btn.text)
                return True
    return False

# ========= PARSE =========
def parse_task(text):
    email    = re.search(r'Email:\s*([^\n]+)', text)
    password = re.search(r'Password:\s*([^\n]+)', text)
    first    = re.search(r'First name:\s*([^\n]+)', text)
    last     = re.search(r'Last name:\s*([^\n]+)', text)
    recovery = re.search(r'Recovery email\s*([^\s\n]+@gmail\.com)', text, re.I)

    return (
        first.group(1).strip()    if first    else "",
        last.group(1).strip()     if last     else "",
        email.group(1).strip()    if email    else "",
        password.group(1).strip() if password else "",
        recovery.group(1).strip() if recovery else "Not Provided"
    )

# ========= AUTO HANDLER =========
async def auto_handler(event):
    msg = event.message
    if not msg or not msg.text:
        return

    msg_id = msg.id
    text = msg.text.lower()

    if msg_id not in TASKS:
        return

    CLICKED.setdefault(msg_id, set())

    try:
        # ===== DEBUG (optional) =====
        if msg.buttons:
            btn_texts = [btn.text for row in msg.buttons for btn in row]
            print(f"[BTN][{msg_id}]", btn_texts)

        # ===== FINAL SAVE =====
        if "recovery email" in text:
            first, last, email, password, recovery = parse_task(msg.text)

            if email:
                con = db()
                cur = con.cursor()

                cur.execute("SELECT 1 FROM registrations WHERE email=?", (email,))
                if not cur.fetchone():
                    cur.execute("""
                    INSERT INTO registrations(
                        user_id, first_name, last_name, email, password,
                        recovery_email, task_id, msg_id, created_at, state
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """, (
                        TASKS[msg_id]["user_id"],
                        first, last, email, password,
                        recovery,
                        f"{TASKS[msg_id]['user_id']}_{msg_id}",
                        msg_id,
                        int(time.time()),
                        "fetched"
                    ))
                    con.commit()
                    print(f"[SAVE] ✅ {email}")

                con.close()

            TASKS.pop(msg_id, None)
            CLICKED.pop(msg_id, None)
            return

        # ===== STEP 3 =====
        if "confirm" in text and "confirm" not in CLICKED[msg_id]:
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if "confirm" in (btn.text or "").lower():
                            await msg.click(text=btn.text)
                            CLICKED[msg_id].add("confirm")
                            print(f"[STEP 3] confirm {msg_id}")
                            return

        # ===== STEP 2 =====
        if "complete" in text and "complete" not in CLICKED[msg_id]:
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if "complete" in (btn.text or "").lower():
                            await msg.click(text=btn.text)
                            CLICKED[msg_id].add("complete")
                            print(f"[STEP 2] complete {msg_id}")
                            return

        # ===== STEP 1 =====
        if "email:" in text and "done" not in CLICKED[msg_id]:
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if "done" in (btn.text or "").lower():
                            await msg.click(text=btn.text)
                            CLICKED[msg_id].add("done")
                            print(f"[STEP 1] done {msg_id}")
                            return

    except Exception as e:
        print(f"[ERROR][{msg_id}] {e}")
# ========= FETCH =========
async def fetch_task(user_id):
    idx, client = get_client()

    async with locks[idx]:
        print(f"[FETCH] {user_id}")
        await client.send_message(SOURCE, "➕ Register a new Gmail")
        await asyncio.sleep(1)

        msg = (await client.get_messages(SOURCE, limit=1))[0]

        TASKS[msg.id] = {
            "user_id": user_id,
            "client":  idx,
            "created": time.time()
        }

        print(f"[TRACK] {msg.id}")

# ========= JOB LOOP =========
async def job_loop():
    while True:
        con = db()
        cur = con.cursor()

        cur.execute("SELECT * FROM jobs WHERE status='pending' LIMIT 1")
        job = cur.fetchone()

        if not job:
            con.close()
            await asyncio.sleep(1)
            continue

        cur.execute("UPDATE jobs SET status='processing' WHERE id=?", (job["id"],))
        con.commit()
        con.close()

        try:
            if job["job_type"] == "fetch":
                await fetch_task(job["user_id"])

            con = db()
            cur = con.cursor()
            cur.execute("UPDATE jobs SET status='done' WHERE id=?", (job["id"],))
            con.commit()
            con.close()

        except Exception as e:
            con = db()
            cur = con.cursor()
            cur.execute(
                "UPDATE jobs SET status='failed', error=? WHERE id=?",
                (str(e), job["id"])
            )
            con.commit()
            con.close()
            print("[JOB ERROR]", e)

# ========= CLEANUP =========
async def cleanup():
    while True:
        now = time.time()
        for msg_id in list(TASKS.keys()):
            if now - TASKS[msg_id]["created"] > 90:
                TASKS.pop(msg_id, None)
                CLICKED.pop(msg_id, None)
        await asyncio.sleep(10)

# ========= START =========
async def main():
    init_db()

    for i, c in enumerate(clients):
        await c.connect()
        if await c.is_user_authorized():
            c.add_event_handler(auto_handler, events.NewMessage(from_users=SOURCE))
            c.add_event_handler(auto_handler, events.MessageEdited(from_users=SOURCE))
            print(f"[READY] Client {i}")

    asyncio.create_task(job_loop())
    asyncio.create_task(cleanup())

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
