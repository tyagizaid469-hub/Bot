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
TASKS = {}        # msg_id -> task
CLICKED = {}      # msg_id -> set()
# ========= DB =========
def db():
    con = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
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

# ========= PARSER =========
def parse(text):
    email = re.search(r'Email:\s*(.+)', text)
    password = re.search(r'Password:\s*(.+)', text)
    recovery = re.search(r'([\w\.-]+@gmail\.com)', text)

    return (
        email.group(1).strip() if email else "",
        password.group(1).strip() if password else "",
        recovery.group(1).strip() if recovery else ""
    )

# ========= AUTO HANDLER =========
async def auto_handler(event):
    msg = event.message
    if not msg:
        return

    msg_id = msg.id
    text = (msg.text or "").lower()

    if msg_id not in TASKS:
        return

    CLICKED.setdefault(msg_id, set())
    btns = get_buttons(msg)

    try:
        # ===== FINAL SAVE =====
        if re.search(r"recovery.*@gmail\.com", text):
            email, password, recovery = parse(msg.text or "")

            if email:
                con = db()
                cur = con.cursor()

                cur.execute("SELECT 1 FROM registrations WHERE email=?", (email,))
                if not cur.fetchone():
                    cur.execute("""
                    INSERT INTO registrations(email,password,recovery_email,msg_id,created_at,state)
                    VALUES(?,?,?,?,?,?)
                    """, (email, password, recovery, msg_id, int(time.time()), "fetched"))

                    con.commit()
                    print(f"[SAVE] ✅ {email}")

                con.close()

            TASKS.pop(msg_id, None)
            CLICKED.pop(msg_id, None)
            return

        # ===== SKIP 2FA =====
        if "enable 2fa" in btns:
            print("[SKIP] 2FA ignored")

        # ===== STEP 3 =====
        if "click again to confirm" in btns and "confirm" not in CLICKED[msg_id]:
            await click_button(msg, "confirm")
            CLICKED[msg_id].add("confirm")
            print("[STEP] confirm")
            return

        # ===== STEP 2 =====
        if "complete" in btns and "complete" not in CLICKED[msg_id]:
            await click_button(msg, "complete")
            CLICKED[msg_id].add("complete")
            print("[STEP] complete")
            return

        # ===== STEP 1 =====
        if "done" in btns and "done" not in CLICKED[msg_id]:
            if "confirm" not in btns:
                await click_button(msg, "done")
                CLICKED[msg_id].add("done")
                print("[STEP] done")
                return

    except Exception as e:
        print("[ERROR]", e)

# ========= FETCH =========
async def fetch_task(user_id):
    idx, client = get_client()

    async with locks[idx]:
        await client.send_message(SOURCE, "➕ Register a new Gmail")
        await asyncio.sleep(1)

        msg = (await client.get_messages(SOURCE, limit=1))[0]

        TASKS[msg.id] = {
            "user_id": user_id,
            "client": idx,
            "created": time.time()
        }

        print("[TRACK]", msg.id)

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

        cur.execute("UPDATE jobs SET status='processing' WHERE id=?", (job[0],))
        con.commit()
        con.close()

        try:
            if job[2] == "fetch":
                await fetch_task(job[1])

            con = db()
            cur = con.cursor()
            cur.execute("UPDATE jobs SET status='done' WHERE id=?", (job[0],))
            con.commit()
            con.close()

        except Exception as e:
            print("[JOB ERROR]", e)

# ========= CLEANUP =========
async def cleanup():
    while True:
        now = time.time()
        for msg_id in list(TASKS.keys()):
            if now - TASKS[msg_id]["created"] > 60:
                TASKS.pop(msg_id, None)
                CLICKED.pop(msg_id, None)
                print("[CLEAN] removed", msg_id)
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
