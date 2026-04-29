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
CLIENT_STATE = {}   # msg_id -> {user_id, client, created}
CLICKED = {}        # msg_id -> set() of clicked keywords

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
    if not clients:
        return None, None
    i = client_index % len(clients)
    client_index += 1
    return i, clients[i]

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
    if not msg:
        return

    msg_id = msg.id
    text   = (msg.text or "").lower()

    task = CLIENT_STATE.get(msg_id)
    if not task:
        return

    CLICKED.setdefault(msg_id, set())
    clicked = CLICKED[msg_id]

    try:
        # â”€â”€ FINAL SAVE: Recovery email aaya â†’ DB save, Done skip â”€â”€
        if "recovery email" in text:
            first, last, email, password, recovery = parse_task(msg.text or "")

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
                        task["user_id"],
                        first, last, email, password,
                        recovery,
                        f"{task['user_id']}_{msg_id}",
                        msg_id,
                        int(time.time()),
                        "fetched"
                    ))
                    con.commit()
                    print(f"[SAVE] âœ… {email} | recovery={recovery}")
                con.close()

            CLIENT_STATE.pop(msg_id, None)
            CLICKED.pop(msg_id, None)
            return

        # Buttons list (lowercase)
        btns = [btn.text.lower() for row in (msg.buttons or []) for btn in row]

        # â”€â”€ STEP 3: CLICK AGAIN TO CONFIRM â”€â”€
        if "click again to confirm" in btns and "confirm" not in clicked:
            for row in msg.buttons:
                for btn in row:
                    if "click again to confirm" in (btn.text or "").lower():
                        await msg.click(text=btn.text)
                        clicked.add("confirm")
                        print(f"[STEP 3] âš¡ CLICK AGAIN TO CONFIRM {msg_id}")
                        return

        # â”€â”€ STEP 2: Complete â”€â”€
        if "complete" in btns and "complete" not in clicked:
            for row in msg.buttons:
                for btn in row:
                    if "complete" in (btn.text or "").lower():
                        await msg.click(text=btn.text)
                        clicked.add("complete")
                        print(f"[STEP 2] âš¡ Complete {msg_id}")
                        return

        # â”€â”€ STEP 1: Done â€” sirf ek baar, confirm state nahi honi chahiye â”€â”€
        if "done" in btns and "done" not in clicked:
            if "click again to confirm" not in btns:
                for row in msg.buttons:
                    for btn in row:
                        if "done" in (btn.text or "").lower():
                            await msg.click(text=btn.text)
                            clicked.add("done")
                            print(f"[STEP 1] âš¡ Done {msg_id}")
                            return

    except Exception as e:
        print(f"[ERROR] {msg_id} {e}")

# ========= FETCH =========
async def fetch_task(user_id):
    idx, client = get_client()
    if client is None:
        return

    async with locks[idx]:
        print("[FETCH]", user_id)

        await client.send_message(SOURCE, "Register a new Gmail")

        # Source bot ka reply aane ka wait karo
        msg = None
        for _ in range(20):  # max 10 sec
            await asyncio.sleep(0.5)
            msgs = await client.get_messages(SOURCE, limit=1)
            if msgs and msgs[0].buttons:
                msg = msgs[0]
                break

        if not msg:
            print("[FETCH] âŒ Source bot ne reply nahi kiya")
            return

        # Track karo
        CLIENT_STATE[msg.id] = {
            "user_id": user_id,
            "client":  idx,
            "created": time.time()
        }
        CLICKED.setdefault(msg.id, set())
        msg_id = msg.id

        print("[TRACK]", msg_id)

        # ============================================================
        # FULL AUTO FLOW â€” saare buttons yahi click karo
        # auto_handler MessageEdited pe miss ho sakta hai isliye
        # yahan loop mein continuously message refresh karke click karo
        # ============================================================
        steps = [
            ("done",                   "STEP 1"),
            ("complete",               "STEP 2"),
            ("click again to confirm", "STEP 3"),
        ]

        for keyword, label in steps:
            clicked = False
            for _ in range(30):  # har step ke liye max 15 sec wait
                await asyncio.sleep(0.5)

                # Message refresh karo â€” buttons change ho jaate hain
                fresh = (await client.get_messages(SOURCE, ids=msg_id))
                if not fresh:
                    continue

                # Final message aa gaya â†’ loop band karo, auto_handler save karega
                if fresh.text and "recovery email" in fresh.text.lower():
                    print("[FETCH] âœ… Final message aa gaya â€” loop band")
                    return

                btns = [btn.text.lower() for row in (fresh.buttons or []) for btn in row]

                if keyword in btns and keyword not in CLICKED[msg_id]:
                    # confirm ke liye exact match chahiye
                    if keyword == "click again to confirm":
                        for row in fresh.buttons:
                            for btn in row:
                                if "click again to confirm" in (btn.text or "").lower():
                                    await fresh.click(text=btn.text)
                                    CLICKED[msg_id].add(keyword)
                                    print(f"[{label}] âš¡ {btn.text}")
                                    clicked = True
                                    break
                    else:
                        for row in fresh.buttons:
                            for btn in row:
                                if keyword in (btn.text or "").lower():
                                    await fresh.click(text=btn.text)
                                    CLICKED[msg_id].add(keyword)
                                    print(f"[{label}] âš¡ {btn.text}")
                                    clicked = True
                                    break

                if clicked:
                    await asyncio.sleep(1)  # next button aane ka wait
                    break

            if not clicked:
                print(f"[{label}] âš ï¸ Button nahi mila â€” skip")

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
