# -*- coding: utf-8 -*-
"""
db_pg.py — PostgreSQL drop-in replacement for SQLite db() in bot_app.py
========================================================================

SETUP:
  pip install psycopg2-binary

ENV VARIABLE (set on Railway / anywhere):
  DATABASE_URL=postgresql://user:password@host:5432/dbname

HOW TO USE in bot_app.py — sirf 2 lines change karo:
  1. Remove:  import sqlite3
     Add:     from db_pg import sqlite3, db, init_db, DB  # noqa (compatibility shim)

  2. Remove:  DB = (os.environ.get("DB_PATH") or "").strip()
              ... (all the DB path logic) ...
              def db(): ...
              def init_db(): ...
     (In bot_app.py un blocks ko delete karo ya comment kar do)

  Baaki POORI file as-is chalegi. ? placeholders, row_factory, lastrowid,
  fetchone(), fetchall() — sab kuch yahan handle ho jaata hai.

WHAT THIS FILE DOES:
  - psycopg2 ke upar ek thin wrapper banata hai
  - ? → %s placeholder conversion (sqlite3 style → psycopg2 style)
  - row_factory emulation (rows dict-like access karte hain, jaise sqlite3.Row)
  - lastrowid support via RETURNING id
  - INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
  - INSERT OR REPLACE → INSERT ... ON CONFLICT (...) DO UPDATE
  - AUTOINCREMENT → SERIAL
  - PRAGMA statements → silently ignored
  - sqlite_master → information_schema.tables
  - PRAGMA table_info → information_schema.columns
"""

import os
import re
import threading
import psycopg2
import psycopg2.extras
from psycopg2 import sql as pgsql

# ── Connection string ────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL env variable not set. "
        "Example: postgresql://user:pass@host:5432/dbname"
    )

# Keep a thread-local connection pool (one conn per thread, like sqlite3)
_local = threading.local()
DB_WRITE_LOCK = threading.RLock()  # same name as in bot_app.py

# Fake "DB" path string so any code that references DB doesn't crash
DB = DATABASE_URL


# ── Compatibility shim: fake sqlite3 module ──────────────────────────────────
class _FakeSqlite3:
    """Exposes just enough of the sqlite3 API that bot_app.py references."""

    class Row:
        pass  # never actually used; PgRow replaces it

    OperationalError = psycopg2.OperationalError
    IntegrityError = psycopg2.IntegrityError
    DatabaseError = psycopg2.DatabaseError

    @staticmethod
    def connect(path, **kwargs):
        """Redirect sqlite3.connect() calls (used in _queue_userbot_job) to PostgreSQL."""
        return _pg_connect()


sqlite3 = _FakeSqlite3()


# ── PostgreSQL connection ────────────────────────────────────────────────────
def _pg_connect():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return _PgConnectionWrapper(conn)


def db():
    """Drop-in for the SQLite db() function. Returns a PgConnectionWrapper."""
    if not getattr(_local, "conn", None) or _local.conn.closed:
        _local.conn = _pg_connect()
    return _local.conn


# ── SQL translation helpers ──────────────────────────────────────────────────
_PLACEHOLDER_RE = re.compile(r"\?")
_AUTOINCREMENT_RE = re.compile(r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b", re.IGNORECASE)
_INTEGER_PK_RE = re.compile(r"\bINTEGER\s+PRIMARY\s+KEY\b(?!\s+AUTOINCREMENT)", re.IGNORECASE)
_REAL_RE = re.compile(r"\bREAL\b", re.IGNORECASE)
_TEXT_DEFAULT_EMPTY_RE = re.compile(r"TEXT\s+DEFAULT\s+''", re.IGNORECASE)


def _translate_sql(sql: str) -> str | None:
    """Convert SQLite SQL dialect → PostgreSQL. Returns None to skip the query."""

    s = sql.strip()

    # ── Skip SQLite-only statements ──────────────────────────────────────────
    if re.match(r"PRAGMA\b", s, re.IGNORECASE):
        return None  # silently ignored

    # ── sqlite_master → information_schema ──────────────────────────────────
    if "sqlite_master" in s:
        # "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        # → "SELECT table_name FROM information_schema.tables WHERE table_name=%s"
        s = re.sub(
            r"SELECT\s+name\s+FROM\s+sqlite_master\s+WHERE\s+type\s*=\s*'table'\s+AND\s+name\s*=\s*\?",
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=%s",
            s, flags=re.IGNORECASE
        )
        s = _PLACEHOLDER_RE.sub("%s", s)
        return s

    # ── PRAGMA table_info(x) — used to check column existence ───────────────
    m = re.match(r"PRAGMA\s+table_info\((\w+)\)", s, re.IGNORECASE)
    if m:
        tbl = m.group(1)
        return (
            f"SELECT column_name AS name FROM information_schema.columns "
            f"WHERE table_schema='public' AND table_name='{tbl}'"
        )

    # ── INSERT OR IGNORE → ON CONFLICT DO NOTHING ────────────────────────────
    if re.match(r"INSERT\s+OR\s+IGNORE\b", s, re.IGNORECASE):
        s = re.sub(r"INSERT\s+OR\s+IGNORE\b", "INSERT", s, flags=re.IGNORECASE)
        s += " ON CONFLICT DO NOTHING"

    # ── INSERT OR REPLACE → ON CONFLICT DO UPDATE ───────────────────────────
    # This is table-specific; we use a generic upsert for known tables.
    elif re.match(r"INSERT\s+OR\s+REPLACE\b", s, re.IGNORECASE):
        s = _translate_insert_or_replace(s)

    # ── DDL translations ─────────────────────────────────────────────────────
    s = _AUTOINCREMENT_RE.sub("SERIAL PRIMARY KEY", s)
    s = _INTEGER_PK_RE.sub("BIGINT PRIMARY KEY", s)
    s = _REAL_RE.sub("DOUBLE PRECISION", s)

    # ── ? → %s ───────────────────────────────────────────────────────────────
    s = _PLACEHOLDER_RE.sub("%s", s)

    return s


def _translate_insert_or_replace(sql: str) -> str:
    """
    Convert INSERT OR REPLACE INTO tbl(cols) VALUES(...)
    → INSERT INTO tbl(cols) VALUES(...) ON CONFLICT(pk_col) DO UPDATE SET ...

    We detect the table name and build the conflict clause dynamically.
    For tables without a clear single PK, we fall back to DO NOTHING.
    """
    # Known PK columns per table (expand as needed)
    PK_MAP = {
        "autoreply": "id",
        "blocked_users": "user_id",
        "pending_referrals": "user_id",
        "users": "user_id",
        "device_logs": "user_id",
        "user_locations": "user_id",
        "device_fingerprints": "user_id",
        "task_rewards": "(user_id, milestone)",
        "rate": "(user_id, minute_key)",
        "payout_proofs": "payout_id",
        "admin_email_verify": "action_id",
        "form_table": "reg_id",
        "precredits": "action_id",
        "referral_bonuses": "(referrer_id, referred_user_id)",
    }

    s = re.sub(r"INSERT\s+OR\s+REPLACE\b", "INSERT", sql, flags=re.IGNORECASE)

    # Extract table name
    m = re.search(r"INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)", s, re.IGNORECASE)
    if not m:
        return s + " ON CONFLICT DO NOTHING"

    tbl = m.group(1).lower()
    cols_str = m.group(2)
    cols = [c.strip() for c in cols_str.split(",")]
    pk = PK_MAP.get(tbl)

    if not pk:
        return s + " ON CONFLICT DO NOTHING"

    # Build SET clause: all columns except PK column(s)
    pk_cols = {c.strip("() ") for c in pk.split(",")}
    set_parts = [f"{c}=EXCLUDED.{c}" for c in cols if c not in pk_cols]

    if not set_parts:
        return s + f" ON CONFLICT({pk}) DO NOTHING"

    return s + f" ON CONFLICT({pk}) DO UPDATE SET {', '.join(set_parts)}"


# ── Row wrapper (emulates sqlite3.Row) ───────────────────────────────────────
class PgRow:
    """Wraps a psycopg2 dict-cursor row so both row['col'] and row[0] work."""

    def __init__(self, mapping, keys):
        self._map = mapping      # OrderedDict from DictCursor
        self._keys = keys        # ordered list of column names
        self._vals = [mapping[k] for k in keys]

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._vals[key]
        return self._map[key]

    def __iter__(self):
        return iter(self._vals)

    def keys(self):
        return self._keys

    def get(self, key, default=None):
        return self._map.get(key, default)

    def __repr__(self):
        return f"PgRow({dict(self._map)})"


# ── Cursor wrapper ────────────────────────────────────────────────────────────
class PgCursorWrapper:
    def __init__(self, raw_cursor, conn_wrapper):
        self._cur = raw_cursor
        self._conn = conn_wrapper
        self.lastrowid = None
        self._rows = None
        self._col_names = []

    # ── execute ──────────────────────────────────────────────────────────────
    def execute(self, sql, params=None):
        translated = _translate_sql(sql)
        if translated is None:
            return  # silently skip (PRAGMA etc.)

        # Append RETURNING id for INSERT statements so lastrowid works
        needs_returning = (
            re.match(r"\s*INSERT\b", translated, re.IGNORECASE)
            and "RETURNING" not in translated.upper()
        )
        if needs_returning:
            translated = translated.rstrip("; ") + " RETURNING id"

        try:
            self._cur.execute(translated, params or ())
        except psycopg2.errors.UndefinedColumn:
            # Column already exists / migration already applied — ignore
            self._conn._raw.rollback()
            return
        except psycopg2.errors.DuplicateColumn:
            self._conn._raw.rollback()
            return
        except psycopg2.errors.UniqueViolation:
            self._conn._raw.rollback()
            return

        # Capture lastrowid from RETURNING id
        if needs_returning:
            try:
                row = self._cur.fetchone()
                if row:
                    # DictCursor returns OrderedDict
                    val = row.get("id") if hasattr(row, "get") else row[0]
                    self.lastrowid = val
            except Exception:
                pass
            # Re-fetch is done; mark internal result as empty
            self._rows = None
            self._col_names = []
        else:
            self._rows = None
            if self._cur.description:
                self._col_names = [d[0] for d in self._cur.description]

    def executemany(self, sql, seq):
        for params in seq:
            self.execute(sql, params)

    # ── fetch ─────────────────────────────────────────────────────────────────
    def _wrap_rows(self, raw_rows):
        if raw_rows is None:
            return None
        if not self._cur.description:
            return raw_rows
        keys = [d[0] for d in self._cur.description]
        return [PgRow(r, keys) for r in raw_rows]

    def fetchone(self):
        raw = self._cur.fetchone()
        if raw is None:
            return None
        keys = [d[0] for d in self._cur.description] if self._cur.description else []
        if isinstance(raw, dict):
            return PgRow(raw, keys)
        # fallback: plain tuple
        return raw

    def fetchall(self):
        rows = self._cur.fetchall()
        if not rows:
            return []
        if not self._cur.description:
            return rows
        keys = [d[0] for d in self._cur.description]
        return [PgRow(r, keys) for r in rows]

    def __iter__(self):
        for row in self.fetchall():
            yield row

    def close(self):
        try:
            self._cur.close()
        except Exception:
            pass

    # Needed by some code: len(cur.fetchall()) patterns already handled above
    @property
    def rowcount(self):
        return self._cur.rowcount


# ── Connection wrapper ────────────────────────────────────────────────────────
class _PgConnectionWrapper:
    def __init__(self, raw_conn):
        self._raw = raw_conn
        # Use DictCursor so rows are dict-like
        self._raw.cursor_factory = psycopg2.extras.RealDictCursor
        self.closed = False
        # Emulate sqlite3 row_factory attribute (no-op, we handle it via PgRow)
        self.row_factory = None

    def cursor(self):
        return PgCursorWrapper(self._raw.cursor(), self)

    def execute(self, sql, params=None):
        """Allow conn.execute() shorthand (used by _sqlite_checkpoint)."""
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self):
        try:
            self._raw.commit()
        except Exception:
            pass

    def rollback(self):
        try:
            self._raw.rollback()
        except Exception:
            pass

    def close(self):
        try:
            self._raw.close()
            self.closed = True
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()


# ── init_db() — same tables, PostgreSQL syntax ───────────────────────────────
def init_db():
    """
    Create all tables in PostgreSQL.
    Idempotent — safe to run on every startup.
    """
    con = db()
    cur = con.cursor()

    # ── users ────────────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id BIGINT PRIMARY KEY,
        username TEXT,
        lang TEXT DEFAULT 'hi',
        referrer_id BIGINT,
        main_balance DOUBLE PRECISION DEFAULT 0,
        hold_balance DOUBLE PRECISION DEFAULT 0,
        created_at BIGINT,
        referral_bonus_paid INTEGER DEFAULT 0,
        currency TEXT DEFAULT 'INR'
    )
    """)

    # ── blocked_users ────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS blocked_users(
        user_id BIGINT PRIMARY KEY,
        blocked_at BIGINT
    )
    """)

    # ── pending_referrals ────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pending_referrals(
        user_id BIGINT PRIMARY KEY,
        referrer_id BIGINT,
        created_at BIGINT
    )
    """)

    # ── referral_bonuses ─────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS referral_bonuses(
        id SERIAL PRIMARY KEY,
        referrer_id BIGINT,
        referred_user_id BIGINT,
        amount DOUBLE PRECISION,
        created_at BIGINT,
        UNIQUE(referrer_id, referred_user_id)
    )
    """)

    # ── task_rewards ─────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS task_rewards(
        user_id BIGINT,
        milestone INTEGER,
        amount DOUBLE PRECISION,
        paid_at BIGINT,
        PRIMARY KEY(user_id, milestone)
    )
    """)

    # ── rate ─────────────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS rate(
        user_id BIGINT,
        minute_key BIGINT,
        count INTEGER,
        PRIMARY KEY(user_id, minute_key)
    )
    """)

    # ── registrations ────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS registrations(
        id SERIAL PRIMARY KEY,
        user_id BIGINT,
        first_name TEXT,
        last_name TEXT,
        email TEXT UNIQUE,
        password TEXT,
        recovery_email TEXT,
        extra_data TEXT,
        created_at BIGINT,
        updated_at BIGINT,
        msg_id BIGINT,
        task_id TEXT,
        status TEXT DEFAULT 'created',
        state TEXT DEFAULT 'created'
    )
    """)

    # ── actions ──────────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS actions(
        action_id SERIAL PRIMARY KEY,
        user_id BIGINT,
        reg_id BIGINT,
        created_at BIGINT,
        updated_at BIGINT,
        expires_at BIGINT,
        state TEXT DEFAULT 'shown'
    )
    """)

    # ── hold_credits ─────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS hold_credits(
        id SERIAL PRIMARY KEY,
        user_id BIGINT,
        amount DOUBLE PRECISION,
        created_at BIGINT,
        matured_at BIGINT,
        moved INTEGER DEFAULT 0
    )
    """)

    # ── precredits ───────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS precredits(
        action_id BIGINT PRIMARY KEY,
        user_id BIGINT,
        hold_credit_id BIGINT,
        amount DOUBLE PRECISION,
        created_at BIGINT,
        reverted INTEGER DEFAULT 0
    )
    """)

    # ── payouts ──────────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS payouts(
        id SERIAL PRIMARY KEY,
        user_id BIGINT,
        amount BIGINT,
        upi_or_qr TEXT,
        created_at BIGINT,
        state TEXT DEFAULT 'pending',
        method TEXT DEFAULT 'upi',
        amount_usd DOUBLE PRECISION DEFAULT 0,
        meta TEXT DEFAULT '',
        reserved INTEGER DEFAULT 0,
        refunded INTEGER DEFAULT 0
    )
    """)

    # ── payout_proofs ────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS payout_proofs(
        payout_id BIGINT PRIMARY KEY,
        user_id BIGINT,
        amount BIGINT,
        upi_or_qr TEXT,
        utr TEXT,
        proof_file_id TEXT,
        created_at BIGINT
    )
    """)

    # ── form_table ───────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS form_table(
        id SERIAL PRIMARY KEY,
        reg_id BIGINT UNIQUE,
        user_id BIGINT,
        first_name TEXT,
        email TEXT,
        password TEXT,
        recovery_email TEXT,
        extra_data TEXT,
        msg_id BIGINT,
        created_at BIGINT
    )
    """)

    # ── autoreply ────────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS autoreply(
        id INTEGER PRIMARY KEY CHECK(id=1),
        enabled INTEGER DEFAULT 0,
        text TEXT DEFAULT ''
    )
    """)
    cur.execute("""
    INSERT INTO autoreply(id, enabled, text) VALUES(1,0,'')
    ON CONFLICT(id) DO NOTHING
    """)

    # ── device_logs ──────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS device_logs(
        user_id BIGINT PRIMARY KEY,
        username TEXT,
        lang TEXT,
        first_seen BIGINT,
        last_seen BIGINT,
        last_chat_type TEXT
    )
    """)

    # ── user_locations ───────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_locations(
        user_id BIGINT PRIMARY KEY,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        updated_at BIGINT
    )
    """)

    # ── device_fingerprints ──────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS device_fingerprints(
        user_id BIGINT PRIMARY KEY,
        fp_hash TEXT,
        ua TEXT,
        platform TEXT,
        tz TEXT,
        screen TEXT,
        hw INTEGER,
        mem DOUBLE PRECISION,
        touch INTEGER,
        android_version TEXT,
        device_model TEXT,
        device_name TEXT,
        updated_at BIGINT,
        ip_address TEXT,
        ua_snip TEXT,
        is_verified INTEGER,
        verified_at BIGINT
    )
    """)

    # ── ledger ───────────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ledger(
        id SERIAL PRIMARY KEY,
        user_id BIGINT,
        delta_main DOUBLE PRECISION DEFAULT 0,
        delta_hold DOUBLE PRECISION DEFAULT 0,
        reason TEXT,
        created_at BIGINT
    )
    """)

    # ── admin_email_verify ───────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS admin_email_verify(
        action_id BIGINT PRIMARY KEY,
        decided_by BIGINT,
        status TEXT,
        reason TEXT,
        decided_at BIGINT
    )
    """)

    # ── email_checks ─────────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS email_checks(
        id SERIAL PRIMARY KEY,
        user_id BIGINT,
        reg_id BIGINT,
        action_id BIGINT,
        email TEXT,
        ok INTEGER DEFAULT 0,
        created_at BIGINT
    )
    """)

    # ── jobs (userbot IPC queue) ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs(
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        job_type TEXT NOT NULL,
        payload TEXT DEFAULT '',
        status TEXT DEFAULT 'pending',
        created_at BIGINT NOT NULL,
        updated_at BIGINT,
        error TEXT DEFAULT ''
    )
    """)

    con.commit()
    print("[db_pg] PostgreSQL tables ready ✓")
