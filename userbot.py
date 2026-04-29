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
CLIENT_STATE = {}   # idx -> {user_id, msg_id}
CLICKED = set()

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

# ========= PARSE =========
def parse_task(text):
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@gmail\.com', text)

    main_email = emails[0] if len(emails) > 0 else ""
    recovery = emails[1] if len(emails) > 1 else "Not Provided"

    password = re.search(r'Password:\s*([^\n]+)', text)
    first = re.search(r'First name:\s*([^\n]+)', text)
    last = re.search(r'Last name:\s*([^\n]+)', text)

    return (
        first.group(1).strip() if first else "",
        last.group(1).strip() if last else "",
        main_email,
        password.group(1).strip() if password else "",
        recovery
    )

# ========= AUTO HANDLER =========
async def auto_handler(event):
    msg = event.message
    if not msg or not msg.text:
        return

    text = msg.text.lower()

    for idx, state in list(CLIENT_STATE.items()):
        msg_id = state["msg_id"]

        # IMPORTANT: message edit same id hota hai
        if msg.id != msg_id:
            continue

        try:
            # ===== FINAL STEP (SAVE FIRST) =====
            if "recovery email" in text:
                first, last, email, password, recovery = parse_task(msg.text)

                if recovery != "Not Provided":
                    con = db()
                    cur = con.cursor()

                    cur.execute("""
                    INSERT INTO registrations(
                        user_id, first_name, last_name, email, password,
                        recovery_email, task_id, msg_id, created_at, state
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """, (
                        state["user_id"],
                        first, last, email, password,
                        recovery,
                        f"{state['user_id']}_{msg.id}",
                        msg.id,
                        int(time.time()),
                        "fetched"
                    ))

                    con.commit()
                    con.close()

                    print("[SAVE] ✅", email, recovery)

                    del CLIENT_STATE[idx]
                    CLICKED.clear()
                    return

            # ===== BUTTON CLICK =====
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        t = (btn.text or "").lower()

                        # ❌ FINAL DONE SKIP
                        if "recovery email" in text and "done" in t:
                            continue

                        # STEP 1
                        if "done" in t and f"{msg_id}_done" not in CLICKED:
                            await msg.click(text=btn.text)
                            CLICKED.add(f"{msg_id}_done")
                            print("[STEP 1] Done")
                            return

                        # STEP 2
                        if "complete" in t and f"{msg_id}_complete" not in CLICKED:
                            await msg.click(text=btn.text)
                            CLICKED.add(f"{msg_id}_complete")
                            print("[STEP 2] Complete")
                            return

                        # STEP 3
                        if "confirm" in t and f"{msg_id}_confirm" not in CLICKED:
                            await msg.click(text=btn.text)
                            CLICKED.add(f"{msg_id}_confirm")
                            print("[STEP 3] Confirm")
                            return

        except Exception as e:
            print("[ERROR]", e)

# ========= FETCH =========
async def fetch_task(user_id):
    idx, client = get_client()

    async with locks[idx]:
        print("[FETCH]", user_id)

        await client.send_message(SOURCE, "➕ Register a new Gmail")
        await asyncio.sleep(1)

        msg = (await client.get_messages(SOURCE, limit=1))[0]

        CLIENT_STATE[idx] = {
            "user_id": user_id,
            "msg_id": msg.id
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
            print("[JOB ERROR]", e)

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
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
