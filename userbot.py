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
TASKS = {}   # msg_id -> {user_id, client_idx, stages_done:set()}

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
        msg_id INTEGER,
        created_at INTEGER,
        state TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS task_map(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        msg_id INTEGER,
        client_idx INTEGER,
        created_at INTEGER
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

# ========= AUTO HANDLER =========
async def auto_handler(event):
    msg = event.message
    if not msg:
        return

    msg_id = msg.id
    text = (msg.text or "").lower()

    if msg_id not in TASKS:
        return

    state = TASKS[msg_id]
    stages_done = state["stages"]

    # ================= STEP DETECTION =================

    # STEP 1 → DONE
    if "done" in text and "confirm" not in text and "step1" not in stages_done:
        await click_button(msg, "done")
        stages_done.add("step1")
        print("[AUTO] STEP1 ✅")
        return

    # STEP 2 → COMPLETE
    if "complete" in text and "step2" not in stages_done:
        await click_button(msg, "complete")
        stages_done.add("step2")
        print("[AUTO] STEP2 COMPLETE")
        return

    # STEP 3 → CONFIRM
    if "confirm" in text and "again" not in text and "step3" not in stages_done:
        await click_button(msg, "confirm")
        stages_done.add("step3")
        print("[AUTO] STEP3 CONFIRM")
        return

    # STOP AUTO HERE
    if "click again to confirm" in text:
        print("[AUTO] ⛔ WAIT FINAL")
        return

    # FINAL DATA
    if "email" in text and "password" in text:
        user_id = state["user_id"]

        first, last, email, password, recovery = parse_task(msg.text or "")

        con = db()
        cur = con.cursor()

        cur.execute("""
        INSERT INTO registrations(
            user_id, first_name, last_name, email, password,
            recovery_email, msg_id, created_at, state
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        """, (
            user_id, first, last, email, password,
            recovery, msg_id, int(time.time()), "fetched"
        ))

        con.commit()
        con.close()

        print("[FETCH] ✅ SAVED")
        return

# ========= BUTTON CLICK =========
async def click_button(msg, keyword):
    if not msg.buttons:
        return

    for row in msg.buttons:
        for btn in row:
            if keyword in (btn.text or "").lower():
                try:
                    await msg.click(text=btn.text)
                    return
                except Exception as e:
                    print("[CLICK ERROR]", e)

# ========= FETCH =========
async def fetch_task(user_id):
    idx, client = get_client()

    async with locks[idx]:
        await client.send_message(SOURCE, "➕ Register a new Gmail")
        await asyncio.sleep(2)

        msg = (await client.get_messages(SOURCE, limit=1))[0]

        TASKS[msg.id] = {
            "user_id": user_id,
            "client_idx": idx,
            "stages": set()
        }

        con = db()
        cur = con.cursor()
        cur.execute("""
        INSERT INTO task_map(user_id, msg_id, client_idx, created_at)
        VALUES(?,?,?,?)
        """, (user_id, msg.id, idx, int(time.time())))
        con.commit()
        con.close()

        print("[TRACK]", msg.id)

# ========= CONFIRM =========
async def confirm_task(user_id):
    con = db()
    cur = con.cursor()

    cur.execute("""
    SELECT msg_id, client_idx FROM task_map
    WHERE user_id=? ORDER BY id DESC LIMIT 1
    """, (user_id,))

    row = cur.fetchone()
    con.close()

    if not row:
        return

    msg_id = row["msg_id"]
    client = clients[row["client_idx"]]

    msg = await client.get_messages(SOURCE, ids=msg_id)

    if msg and msg.buttons:
        for row_btn in msg.buttons:
            for btn in row_btn:
                if "done" in (btn.text or "").lower():
                    await msg.click(text=btn.text)
                    print("[CONFIRM] FINAL DONE")
                    return

# ========= START =========
async def main():
    init_db()

    for i, c in enumerate(clients):
        await c.start()
        c.add_event_handler(auto_handler, events.NewMessage(from_users=SOURCE))
        c.add_event_handler(auto_handler, events.MessageEdited(from_users=SOURCE))
        print("[READY]", i)

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
