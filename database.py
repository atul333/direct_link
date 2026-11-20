"""
database.py — SQLite database layer for Channel Link Provider Bot
"""

import sqlite3
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Persistent Autocommit SQLite Connection Pool (Zero-Lock Contention) ──────
_WRITE_LOCK = threading.RLock()
_DB_CONNS: dict[str, "_SafeConnection"] = {}
_CENTRAL_INITIALISED: set[str] = set()


class _SafeConnection:
    """
    Thread-safe wrapper around a single autocommit (isolation_level=None) SQLite connection.
    - Eliminates cross-connection 'database is locked' errors inside the process
    - Never leaves an uncommitted transaction open
    - Retries automatically if an external process briefly locks the file
    - Ignores accidental .close() calls so the shared connection stays alive
    """

    def __init__(self, raw_conn: sqlite3.Connection) -> None:
        self._conn = raw_conn

    def execute(self, sql: str, parameters: tuple | list = ()) -> sqlite3.Cursor:
        for attempt in range(6):
            try:
                with _WRITE_LOCK:
                    return self._conn.execute(sql, parameters)
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and attempt < 5:
                    time.sleep(0.05 * (attempt + 1))
                    continue
                raise

    def cursor(self) -> "_SafeConnection":
        return self

    def commit(self) -> None:
        with _WRITE_LOCK:
            if self._conn.in_transaction:
                self._conn.commit()

    def close(self) -> None:
        # No-op: keep persistent connection alive
        pass

    @property
    def row_factory(self):
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, val) -> None:
        self._conn.row_factory = val


def _create_safe_conn(db_path: str) -> _SafeConnection:
    raw = sqlite3.connect(
        db_path,
        timeout=30.0,
        isolation_level=None,  # Autocommit mode: never holds open transactions
        check_same_thread=False,
    )
    raw.row_factory = sqlite3.Row
    raw.execute("PRAGMA journal_mode=WAL")
    raw.execute("PRAGMA synchronous=NORMAL")
    raw.execute("PRAGMA busy_timeout=30000")
    raw.execute("PRAGMA cache_size=-32000")  # 32MB page cache
    raw.execute("PRAGMA foreign_keys=ON")
    raw.execute("PRAGMA temp_store=MEMORY")
    return _SafeConnection(raw)


def get_connection(db_path: str) -> _SafeConnection:
    """Return a thread-safe persistent autocommit SQLite connection per db_path."""
    with _WRITE_LOCK:
        conn = _DB_CONNS.get(db_path)
        if conn is not None:
            try:
                conn.execute("SELECT 1")
                return conn
            except Exception:
                _DB_CONNS.pop(db_path, None)

        conn = _create_safe_conn(db_path)
        _DB_CONNS[db_path] = conn
        return conn


def init_db(db_path: str) -> None:
    """Create all tables if they do not already exist."""
    conn = get_connection(db_path)
    cur = conn.cursor()

    # ── Users ─────────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            first_name  TEXT,
            last_name   TEXT,
            joined_at   TEXT NOT NULL DEFAULT (datetime('now')),
            is_banned   INTEGER NOT NULL DEFAULT 0
        )
    """)

    # ── Channels ──────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            link         TEXT NOT NULL,
            price_inr    REAL NOT NULL,
            description  TEXT,
            is_active    INTEGER NOT NULL DEFAULT 1,
            created_at   TEXT NOT NULL DEFAULT (datetime('now')),
            created_by   INTEGER NOT NULL,
            chat_id      TEXT,
            media_counts TEXT
        )
    """)

    # Migrate existing channels table if chat_id or media_counts column is missing
    for ch_col in ("chat_id TEXT", "media_counts TEXT"):
        try:
            cur.execute(f"ALTER TABLE channels ADD COLUMN {ch_col}")
        except sqlite3.OperationalError:
            pass

    # ── Payments ──────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id             INTEGER NOT NULL,
            channel_id          INTEGER NOT NULL,
            razorpay_order_id   TEXT UNIQUE,
            razorpay_payment_id TEXT,
            amount_inr          REAL NOT NULL,
            status              TEXT NOT NULL DEFAULT 'pending',
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            paid_at             TEXT,
            qr_message_chat_id  INTEGER,
            qr_message_id       INTEGER,
            FOREIGN KEY (user_id)    REFERENCES users(user_id),
            FOREIGN KEY (channel_id) REFERENCES channels(id)
        )
    """)

    # Migrate: add QR message tracking columns if missing
    for col in ("qr_message_chat_id INTEGER", "qr_message_id INTEGER"):
        try:
            cur.execute(f"ALTER TABLE payments ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass

    # ── User channel access ────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_channel_access (
            user_id     INTEGER NOT NULL,
            channel_id  INTEGER NOT NULL,
            granted_at  TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (user_id, channel_id),
            FOREIGN KEY (user_id)    REFERENCES users(user_id),
            FOREIGN KEY (channel_id) REFERENCES channels(id)
        )
    """)

    # ── Broadcast log ─────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS broadcasts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id     INTEGER NOT NULL,
            channel_id   INTEGER,
            message_text TEXT NOT NULL,
            sent_count   INTEGER DEFAULT 0,
            fail_count   INTEGER DEFAULT 0,
            created_at   TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # ── Channel Broadcast Posts ───────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id        INTEGER NOT NULL,
            channel_id      INTEGER NOT NULL,
            title           TEXT NOT NULL,
            caption         TEXT,
            photo_file_id   TEXT,
            price_inr       REAL NOT NULL,
            target_channel  TEXT NOT NULL,
            channel_msg_id  INTEGER,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (channel_id) REFERENCES channels(id)
        )
    """)

    # Migrate: add cross_price_inr column to posts if missing
    try:
        cur.execute("ALTER TABLE posts ADD COLUMN cross_price_inr REAL")
    except sqlite3.OperationalError:
        pass

    # ── Bot Settings ──────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # Ensure all existing usernames in the database have exactly one @ prefix
    try:
        cur.row_factory = sqlite3.Row
        users = cur.execute("SELECT user_id, username FROM users WHERE username IS NOT NULL AND username != '' AND username NOT LIKE '@%'").fetchall()
        for u in users:
            clean_name = str(u["username"]).strip().lstrip("@")
            if clean_name:
                cur.execute("UPDATE users SET username = ? WHERE user_id = ?", (f"@{clean_name}", u["user_id"]))
    except Exception:
        pass

    # --- Performance indexes ---------------------------------------------------
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(razorpay_order_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_status_created ON payments(status, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_access_user_channel ON user_channel_access(user_id, channel_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_posts_channel ON posts(channel_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_channels_active ON channels(is_active)")

    conn.commit()
    logger.info("Database initialised at %s", db_path)


# ── User helpers ───────────────────────────────────────────────────────────────

def format_username(username: Optional[str]) -> str:
    """Ensure username is stored with exactly one @ prefix (e.g. @john_doe)."""
    if not username:
        return ""
    clean = str(username).strip().lstrip("@")
    if not clean:
        return ""
    return f"@{clean}"


def upsert_user(db_path: str, user_id: int, username: str,
                first_name: str, last_name: str) -> None:
    conn = get_connection(db_path)
    uname = format_username(username)
    with _WRITE_LOCK:
        conn.execute("""
            INSERT INTO users (user_id, username, first_name, last_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username   = excluded.username,
                first_name = excluded.first_name,
                last_name  = excluded.last_name
        """, (user_id, uname, first_name, last_name))
        conn.commit()


def upsert_central_user(shared_db_path: str, user_id: int, username: str, first_name: str) -> None:
    """Safely upsert user into central shared DB using per-thread persistent connection."""
    if not shared_db_path:
        return
    try:
        conn = get_connection(shared_db_path)
        if shared_db_path not in _CENTRAL_INITIALISED:
            with _WRITE_LOCK:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS shared_users (
                        user_id     INTEGER PRIMARY KEY,
                        username    TEXT,
                        first_name  TEXT,
                        updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                """)
                conn.commit()
            _CENTRAL_INITIALISED.add(shared_db_path)

        uname = format_username(username)
        with _WRITE_LOCK:
            conn.execute("""
                INSERT INTO shared_users (user_id, username, first_name, updated_at)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT(user_id) DO UPDATE SET
                    username   = excluded.username,
                    first_name = excluded.first_name,
                    updated_at = excluded.updated_at
            """, (user_id, uname, first_name))
            conn.commit()
    except Exception as e:
        logger.debug("Could not upsert central user: %s", e)


def get_all_users(db_path: str) -> list:
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT user_id FROM users WHERE is_banned = 0"
    ).fetchall()
    return [r["user_id"] for r in rows]


def get_user_count(db_path: str) -> int:
    conn = get_connection(db_path)
    count = conn.execute("SELECT COUNT(*) FROM users WHERE is_banned = 0").fetchone()[0]
    return count


def ban_user(db_path: str, user_id: int) -> None:
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (user_id,))
        conn.commit()


def unban_user(db_path: str, user_id: int) -> None:
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (user_id,))
        conn.commit()


def get_user(db_path: str, user_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT * FROM users WHERE user_id = ?", (user_id,)
    ).fetchone()
    return row


# ── Channel helpers ────────────────────────────────────────────────────────────

def add_channel(db_path: str, name: str, link: str,
                price_inr: float, description: str, admin_id: int,
                chat_id: Optional[str] = None,
                media_counts: Optional[str] = None) -> int:
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        cur = conn.execute("""
            INSERT INTO channels (name, link, price_inr, description, created_by, chat_id, media_counts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (name, link, price_inr, description, admin_id, chat_id, media_counts))
        channel_id = cur.lastrowid
        conn.commit()
    return channel_id


def update_channel(db_path: str, channel_id: int, **kwargs) -> bool:
    allowed = {"name", "link", "price_inr", "description", "is_active", "chat_id", "media_counts"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return False
    setters = ", ".join(f"{k} = ?" for k in updates)
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute(
            f"UPDATE channels SET {setters} WHERE id = ?",
            (*updates.values(), channel_id)
        )
        conn.commit()
    return True


def get_channel_by_chat_id(db_path: str, chat_id: str | int) -> Optional[sqlite3.Row]:
    conn = get_connection(db_path)
    cid_str = str(chat_id).strip()
    # Normalize with or without -100 prefix for channel IDs
    variations = [cid_str]
    if cid_str.startswith("-100"):
        variations.append(cid_str[4:])
    elif cid_str.startswith("-"):
        variations.append(cid_str[1:])
    else:
        variations.append(f"-100{cid_str}")

    placeholders = ",".join("?" for _ in variations)
    row = conn.execute(
        f"SELECT * FROM channels WHERE chat_id IN ({placeholders}) AND is_active = 1 LIMIT 1",
        variations,
    ).fetchone()
    return row


def get_channel(db_path: str, channel_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT * FROM channels WHERE id = ?", (channel_id,)
    ).fetchone()
    return row


def get_channel_by_link(db_path: str, link: str) -> Optional[sqlite3.Row]:
    """Find an existing channel by its invite/public link (active or inactive)."""
    if not link or not link.strip():
        return None
    clean = link.strip()
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT * FROM channels WHERE link = ? ORDER BY is_active DESC, id ASC LIMIT 1",
        (clean,),
    ).fetchone()
    return row


def get_active_channels(db_path: str) -> list:
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT * FROM channels WHERE is_active = 1 ORDER BY id"
    ).fetchall()
    return rows


def get_all_channels(db_path: str) -> list:
    conn = get_connection(db_path)
    rows = conn.execute("SELECT * FROM channels ORDER BY id").fetchall()
    return rows


def delete_channel(db_path: str, channel_id: int) -> None:
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute("UPDATE channels SET is_active = 0 WHERE id = ?", (channel_id,))
        conn.commit()


# ── Payment helpers ────────────────────────────────────────────────────────────

def create_payment(db_path: str, user_id: int, channel_id: int,
                   razorpay_order_id: str, amount_inr: float) -> int:
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        cur = conn.execute("""
            INSERT INTO payments (user_id, channel_id, razorpay_order_id, amount_inr)
            VALUES (?, ?, ?, ?)
        """, (user_id, channel_id, razorpay_order_id, amount_inr))
        payment_id = cur.lastrowid
        conn.commit()
    return payment_id


def store_qr_message_id(db_path: str, order_id: str, chat_id: int, message_id: int) -> None:
    """Persist the QR payment message location so it can be deleted after payment."""
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute("""
            UPDATE payments
            SET qr_message_chat_id = ?, qr_message_id = ?
            WHERE razorpay_order_id = ?
        """, (chat_id, message_id, order_id))
        conn.commit()


def get_qr_message_info(db_path: str, order_id: str) -> Optional[dict]:
    """Return {chat_id, message_id} for the QR message, or None."""
    conn = get_connection(db_path)
    row = conn.execute("""
        SELECT qr_message_chat_id, qr_message_id
        FROM payments
        WHERE razorpay_order_id = ?
          AND qr_message_id IS NOT NULL
        ORDER BY id DESC LIMIT 1
    """, (order_id,)).fetchone()
    if row and row["qr_message_id"]:
        return {"chat_id": row["qr_message_chat_id"], "message_id": row["qr_message_id"]}
    return None


def get_latest_qr_message_for_user(db_path: str, user_id: int) -> Optional[dict]:
    """Return {chat_id, message_id} for the latest QR message of a user."""
    conn = get_connection(db_path)
    row = conn.execute("""
        SELECT qr_message_chat_id, qr_message_id
        FROM payments
        WHERE user_id = ?
          AND qr_message_id IS NOT NULL
        ORDER BY id DESC LIMIT 1
    """, (user_id,)).fetchone()
    if row and row["qr_message_id"]:
        return {"chat_id": row["qr_message_chat_id"], "message_id": row["qr_message_id"]}
    return None


def get_payment_by_order(db_path: str, order_id: str) -> Optional[sqlite3.Row]:
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT * FROM payments WHERE razorpay_order_id = ?", (order_id,)
    ).fetchone()
    return row


def find_payment(db_path: str, identifier: str) -> Optional[sqlite3.Row]:
    """Search for payment record by order_id, qr_id, or payment_id (supports CLP5000- prefix)."""
    clean_id = identifier.replace("CLP5000-", "").strip()
    raw_id = identifier.strip()
    conn = get_connection(db_path)
    row = conn.execute("""
        SELECT * FROM payments 
        WHERE razorpay_order_id = ? 
           OR razorpay_payment_id = ?
           OR razorpay_order_id = ?
           OR razorpay_payment_id = ?
        ORDER BY created_at DESC LIMIT 1
    """, (clean_id, clean_id, raw_id, raw_id)).fetchone()
    return row


def get_pending_payment(db_path: str, user_id: int, channel_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection(db_path)
    row = conn.execute("""
        SELECT * FROM payments
        WHERE user_id = ? AND channel_id = ? AND status = 'pending'
        ORDER BY created_at DESC LIMIT 1
    """, (user_id, channel_id)).fetchone()
    return row


def get_all_pending_payments(
    db_path: str,
    hours: Optional[int] = None,
    minutes: Optional[int] = None,
    min_age_minutes: int = 0,
    limit: int = 30,
) -> list:
    """Fetch recent pending payments to poll Razorpay without scanning hundreds of stale rows."""
    conn = get_connection(db_path)
    if minutes is not None:
        max_age_expr = f"-{int(minutes)} minutes"
    else:
        h = int(hours) if hours is not None else 1
        max_age_expr = f"-{h} hours"

    if min_age_minutes > 0:
        min_age_expr = f"-{int(min_age_minutes)} minutes"
        rows = conn.execute("""
            SELECT * FROM payments
            WHERE status = 'pending'
              AND created_at >= datetime('now', ?)
              AND created_at < datetime('now', ?)
            ORDER BY created_at DESC
            LIMIT ?
        """, (max_age_expr, min_age_expr, int(limit))).fetchall()
    else:
        rows = conn.execute("""
            SELECT * FROM payments
            WHERE status = 'pending'
              AND created_at >= datetime('now', ?)
            ORDER BY created_at DESC
            LIMIT ?
        """, (max_age_expr, int(limit))).fetchall()
    return rows


def expire_old_pending_payments(db_path: str, hours: int = 2) -> int:
    """Mark abandoned pending QR orders older than `hours` as expired so poller stays fast."""
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        cur = conn.execute("""
            UPDATE payments
            SET status = 'expired'
            WHERE status = 'pending'
              AND created_at < datetime('now', ?)
        """, (f"-{int(hours)} hours",))
        count = cur.rowcount
        if count > 0:
            conn.commit()
    return count


def confirm_payment(db_path: str, order_id: str, razorpay_payment_id: str) -> Optional[int]:
    """Mark payment as paid. Returns channel_id on success."""
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT * FROM payments WHERE razorpay_order_id = ?", (order_id,)
    ).fetchone()
    if not row:
        return None
    with _WRITE_LOCK:
        conn.execute("""
            UPDATE payments
            SET status = 'paid', razorpay_payment_id = ?, paid_at = datetime('now')
            WHERE razorpay_order_id = ?
        """, (razorpay_payment_id, order_id))
        # Grant access
        conn.execute("""
            INSERT OR IGNORE INTO user_channel_access (user_id, channel_id)
            VALUES (?, ?)
        """, (row["user_id"], row["channel_id"]))
        conn.commit()
    return row["channel_id"]


def grant_free_access(db_path: str, user_id: int, channel_id: int) -> None:
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute("""
            INSERT OR IGNORE INTO user_channel_access (user_id, channel_id)
            VALUES (?, ?)
        """, (user_id, channel_id))
        conn.commit()


def has_access(db_path: str, user_id: int, channel_id: int) -> bool:
    conn = get_connection(db_path)
    row = conn.execute("""
        SELECT 1 FROM user_channel_access
        WHERE user_id = ?
          AND (
              channel_id = ?
              OR channel_id IN (
                  SELECT c2.id FROM channels c1
                  JOIN channels c2 ON c1.link = c2.link
                  WHERE c1.id = ? AND c1.link IS NOT NULL AND c1.link != ''
              )
          )
        LIMIT 1
    """, (user_id, channel_id, channel_id)).fetchone()
    return row is not None


def get_user_purchases(db_path: str, user_id: int) -> list:
    conn = get_connection(db_path)
    rows = conn.execute("""
        SELECT c.id, c.name, c.link, c.price_inr, uca.granted_at
        FROM user_channel_access uca
        JOIN channels c ON c.id = uca.channel_id
        WHERE uca.user_id = ?
        ORDER BY uca.granted_at DESC
    """, (user_id,)).fetchall()
    return rows


# ── Statistics helpers ─────────────────────────────────────────────────────────

def get_stats(db_path: str) -> dict:
    conn = get_connection(db_path)
    total_users   = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    active_users  = conn.execute("SELECT COUNT(*) FROM users WHERE is_banned=0").fetchone()[0]
    total_channels= conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
    active_ch     = conn.execute("SELECT COUNT(*) FROM channels WHERE is_active=1").fetchone()[0]
    total_paid    = conn.execute("SELECT COUNT(*) FROM payments WHERE status='paid'").fetchone()[0]
    total_revenue = conn.execute(
        "SELECT COALESCE(SUM(amount_inr),0) FROM payments WHERE status='paid'"
    ).fetchone()[0]
    pending       = conn.execute("SELECT COUNT(*) FROM payments WHERE status='pending'").fetchone()[0]

    # Revenue per channel
    ch_revenue = conn.execute("""
        SELECT c.name, COUNT(p.id) as sales, COALESCE(SUM(p.amount_inr),0) as revenue
        FROM channels c
        LEFT JOIN payments p ON p.channel_id = c.id AND p.status='paid'
        GROUP BY c.id
        ORDER BY revenue DESC
    """).fetchall()

    return {
        "total_users": total_users,
        "active_users": active_users,
        "total_channels": total_channels,
        "active_channels": active_ch,
        "total_paid_txns": total_paid,
        "total_revenue": total_revenue,
        "pending_payments": pending,
        "channel_breakdown": [dict(r) for r in ch_revenue],
    }


def log_broadcast(db_path: str, admin_id: int, channel_id: Optional[int],
                  message_text: str, sent: int, failed: int) -> None:
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute("""
            INSERT INTO broadcasts (admin_id, channel_id, message_text, sent_count, fail_count)
            VALUES (?, ?, ?, ?, ?)
        """, (admin_id, channel_id, message_text, sent, failed))
        conn.commit()


def get_payments_for_channel(db_path: str, channel_id: int) -> list:
    conn = get_connection(db_path)
    rows = conn.execute("""
        SELECT p.*, u.username, u.first_name
        FROM payments p
        JOIN users u ON u.user_id = p.user_id
        WHERE p.channel_id = ? AND p.status = 'paid'
        ORDER BY p.paid_at DESC
    """, (channel_id,)).fetchall()
    return rows


# ── Broadcast Posts Helpers ───────────────────────────────────────────────────

def create_post(db_path: str, admin_id: int, channel_id: int, title: str,
                caption: str, photo_file_id: Optional[str], price_inr: float,
                target_channel: str, channel_msg_id: Optional[int] = None,
                cross_price_inr: Optional[float] = None) -> int:
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        cur = conn.execute("""
            INSERT INTO posts (admin_id, channel_id, title, caption, photo_file_id, price_inr, target_channel, channel_msg_id, cross_price_inr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (admin_id, channel_id, title, caption, photo_file_id, price_inr, target_channel, channel_msg_id, cross_price_inr))
        post_id = cur.lastrowid
        conn.commit()
    return post_id


def update_post_msg_id(db_path: str, post_id: int, channel_msg_id: Optional[int]) -> None:
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute("UPDATE posts SET channel_msg_id = ? WHERE id = ?", (channel_msg_id, post_id))
        conn.commit()


def clear_post_msg_id(db_path: str, post_id: int) -> None:
    """Clear channel_msg_id when a post is removed from main channel, preserving photo_file_id & pricing for reactivation."""
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute("UPDATE posts SET channel_msg_id = NULL WHERE id = ?", (post_id,))
        conn.commit()


def get_latest_post_for_channel(
    db_path: str,
    channel_id: int,
    channel_link: str = "",
    channel_name: str = "",
) -> Optional[sqlite3.Row]:
    """Return the most recent post record for a channel (preferring exact channel_id and full AgAC photo_file_id)."""
    def _pick_best(rows: list) -> Optional[sqlite3.Row]:
        if not rows:
            return None
        with_photo = [
            p for p in rows
            if p["photo_file_id"] and str(p["photo_file_id"]).startswith("AgAC")
        ]
        if with_photo:
            return sorted(with_photo, key=lambda r: r["id"], reverse=True)[0]
        with_media = [p for p in rows if p["photo_file_id"]]
        if with_media:
            return sorted(with_media, key=lambda r: r["id"], reverse=True)[0]
        return sorted(rows, key=lambda r: r["id"], reverse=True)[0]

    exact_posts = get_posts_for_channel_exact(db_path, channel_id, channel_link)
    best_exact = _pick_best(exact_posts)
    if best_exact and best_exact["photo_file_id"]:
        return best_exact

    fallback_posts = get_posts_for_channel(db_path, channel_id, channel_link, channel_name)
    best_fallback = _pick_best(fallback_posts)
    return best_fallback or best_exact


def recover_deleted_posts_from_sqlite_bytes(db_path: str) -> int:
    """
    Carve and restore deleted `posts` rows (including photo_file_id, caption, price_inr,
    cross_price_inr, target_channel) from unallocated SQLite page/WAL bytes for any channel
    whose post row was previously hard-deleted.
    """
    import os
    import re
    import struct

    def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
        v = 0
        for i in range(9):
            b = buf[pos + i]
            if i == 8:
                v = (v << 8) | b
                return v, pos + 9
            v = (v << 7) | (b & 0x7F)
            if not (b & 0x80):
                return v, pos + i + 1
        return v, pos

    def _st_len(st: int) -> int:
        if st in (0, 8, 9):
            return 0
        if st in (1, 2, 3, 4):
            return st
        if st == 5:
            return 6
        if st in (6, 7):
            return 8
        if st >= 12:
            return (st - 12) // 2 if st % 2 == 0 else (st - 13) // 2
        return 0

    def _decode_num(st: int, b: bytes) -> Optional[float]:
        if st == 0:
            return None
        if st == 8:
            return 0.0
        if st == 9:
            return 1.0
        if st in (1, 2, 3, 4, 5, 6):
            return float(int.from_bytes(b, "big", signed=True))
        if st == 7 and len(b) == 8:
            return struct.unpack(">d", b)[0]
        return None

    raw_blobs = []
    for candidate in (db_path + "-wal", db_path, db_path + "-journal"):
        if os.path.exists(candidate):
            try:
                with open(candidate, "rb") as f:
                    raw_blobs.append(f.read())
            except Exception:
                pass
    if not raw_blobs:
        return 0

    raw = b"\n".join(raw_blobs)
    conn = get_connection(db_path)
    channels = get_all_channels(db_path)
    default_target = get_setting(db_path, "main_channel_id") or ""
    recovered = 0

    fallback_pat = re.compile(
        rb"^(?P<caption>.{0,300}?)(?P<file_id>(?:AgAC|BAAC|CgAC|BQAC|DQAC)[A-Za-z0-9_-]{60,120}?)(?=-100\d{7,15}|@[A-Za-z0-9_]{4,35}|[\x00-\x1f])",
        re.DOTALL,
    )

    for ch in channels:
        cid = ch["id"]
        existing = conn.execute(
            "SELECT id, photo_file_id FROM posts WHERE channel_id = ? ORDER BY id DESC LIMIT 1",
            (cid,),
        ).fetchone()
        existing_fid = (existing["photo_file_id"] or "").strip() if existing else ""
        if existing and len(existing_fid) >= 70 and "-100" not in existing_fid:
            continue

        title_str = (ch["name"] or "").strip()
        if cid == 1:
            cid_b = b""
        elif 2 <= cid <= 127:
            cid_b = bytes([cid])
        else:
            cid_b = cid.to_bytes(2, "big", signed=True)

        title_alts = [rb"VIP Channel #\d+"]
        if title_str and not re.match(r"^VIP Channel #\d+$", title_str, re.IGNORECASE):
            title_alts.insert(0, re.escape(title_str.encode("utf-8")))
        needle_re = re.compile(
            re.escape(cid_b) + rb"(?P<rec_title>" + b"|".join(title_alts) + rb")"
        )

        best_match = None
        for m_needle in needle_re.finditer(raw):
            idx = m_needle.start()
            title_b = m_needle.group("rec_title")
            needle_len = len(cid_b) + len(title_b)
            after_title_idx = idx + needle_len

            # Strategy 1: Exact SQLite record header varint decoding
            parsed = None
            for hstart in range(max(0, idx - 26), max(0, idx - 7)):
                try:
                    pos = hstart
                    sts = []
                    for _ in range(9):  # Col 2..10
                        st, pos = _read_varint(raw, pos)
                        sts.append(st)
                    if sts[1] == len(title_b) * 2 + 13 and sts[7] == 51:
                        body_pos = idx + needle_len
                        vals = []
                        col_sts = sts[2:9] if (idx - pos) in (4, 6, 8) else sts[2:8]
                        for st in col_sts:
                            ln = _st_len(st)
                            vals.append((st, raw[body_pos : body_pos + ln]))
                            body_pos += ln
                        caption_val = vals[0][1].decode("utf-8", "ignore")
                        file_id_val = vals[1][1].decode("ascii", "ignore") if vals[1][0] >= 13 else None
                        price_val = _decode_num(*vals[2])
                        target_val = vals[3][1].decode("utf-8", "ignore")
                        created_val = vals[5][1].decode("ascii", "ignore")
                        cross_val = _decode_num(*vals[6]) if len(vals) > 6 else None
                        if created_val.startswith("20") and (file_id_val or target_val):
                            parsed = {
                                "caption": caption_val,
                                "photo_file_id": file_id_val,
                                "price_inr": price_val if (price_val is not None and price_val >= 0) else ch["price_inr"],
                                "target_channel": target_val or default_target,
                                "created_at": created_val,
                                "cross_price_inr": cross_val,
                            }
                            break
                except Exception:
                    pass

            # Strategy 2: Regex fallback for file_id right after title
            if not parsed:
                tail = raw[after_title_idx : after_title_idx + 400]
                if b"https://t.me/" not in tail[:60]:
                    m = fallback_pat.match(tail)
                    if m:
                        fid = m.group("file_id").decode("ascii", "ignore")
                        cap = m.group("caption").decode("utf-8", "ignore")
                        t_match = re.search(rb"(-100\d{7,15}|@[A-Za-z0-9_]{4,35})", tail[m.end("file_id") : m.end("file_id") + 60])
                        tgt = t_match.group(1).decode("ascii", "ignore") if t_match else default_target
                        parsed = {
                            "caption": cap,
                            "photo_file_id": fid,
                            "price_inr": ch["price_inr"],
                            "target_channel": tgt,
                            "created_at": ch["created_at"],
                            "cross_price_inr": None,
                        }

            if parsed and parsed.get("photo_file_id"):
                best_match = parsed

        if best_match:
            with _WRITE_LOCK:
                if existing:
                    conn.execute(
                        """
                        UPDATE posts
                        SET photo_file_id = ?,
                            caption = COALESCE(NULLIF(caption, ''), ?),
                            cross_price_inr = COALESCE(cross_price_inr, ?)
                        WHERE id = ?
                        """,
                        (
                            best_match["photo_file_id"],
                            best_match["caption"],
                            best_match["cross_price_inr"],
                            existing["id"],
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO posts (
                            admin_id, channel_id, title, caption, photo_file_id,
                            price_inr, target_channel, channel_msg_id, created_at, cross_price_inr
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                        """,
                        (
                            ch["created_by"] or 0,
                            cid,
                            title_str,
                            best_match["caption"],
                            best_match["photo_file_id"],
                            best_match["price_inr"] if best_match["price_inr"] else ch["price_inr"],
                            best_match["target_channel"] or default_target,
                            best_match["created_at"],
                            best_match["cross_price_inr"],
                        ),
                    )
                conn.commit()
            recovered += 1
            logger.info(
                "Recovered deleted post media for channel #%d (%s): file_id=%s...",
                cid, title_str, best_match["photo_file_id"][:20],
            )

    return recovered


def get_post(db_path: str, post_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection(db_path)
    row = conn.execute("""
        SELECT p.*, c.name as channel_name, c.link as channel_link, c.chat_id as channel_chat_id
        FROM posts p
        JOIN channels c ON c.id = p.channel_id
        WHERE p.id = ?
    """, (post_id,)).fetchone()
    return row


def get_recent_posts(db_path: str, limit: int = 10) -> list:
    conn = get_connection(db_path)
    rows = conn.execute("""
        SELECT p.*, c.name as channel_name
        FROM posts p
        JOIN channels c ON c.id = p.channel_id
        ORDER BY p.id DESC LIMIT ?
    """, (limit,)).fetchall()
    return rows


def get_posts_for_channel_exact(
    db_path: str, channel_id: int, channel_link: str = ""
) -> list:
    """Return promotional posts belonging strictly to channel_id (or duplicate channel rows with exact same link)."""
    conn = get_connection(db_path)
    if channel_link and channel_link.strip():
        rows = conn.execute(
            "SELECT * FROM posts WHERE channel_id = ? OR channel_id IN (SELECT id FROM channels WHERE link = ?) ORDER BY id DESC",
            (channel_id, channel_link.strip()),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM posts WHERE channel_id = ? ORDER BY id DESC",
            (channel_id,),
        ).fetchall()
    return rows


def get_posts_for_channel(db_path: str, channel_id: int, channel_link: str = "", channel_name: str = "") -> list:
    """Return all promotional posts created for a specific channel, matching by ID, shared link, link hash, or title."""
    conn = get_connection(db_path)
    query = "SELECT * FROM posts WHERE channel_id = ?"
    params: list = [channel_id]
    if channel_link:
        clean = channel_link.strip()
        query += " OR channel_id IN (SELECT id FROM channels WHERE link = ?)"
        params.append(clean)
        link_hash = clean.rstrip("/").split("+")[-1].split("/")[-1].strip()
        if link_hash and len(link_hash) >= 5:
            query += " OR caption LIKE ? OR title LIKE ?"
            params.extend([f"%{link_hash}%", f"%{link_hash}%"])
    if channel_name:
        query += " OR title = ?"
        params.append(channel_name)
    rows = conn.execute(query, tuple(params)).fetchall()
    return rows


def delete_post(db_path: str, post_id: int) -> None:
    """Delete a single post from the database."""
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute("DELETE FROM posts WHERE id = ?", (post_id,))
        conn.commit()


def delete_posts_for_channel(db_path: str, channel_id: int) -> int:
    """Delete promotional posts for a specific channel from the DB without touching channel or purchases."""
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        cur = conn.execute("DELETE FROM posts WHERE channel_id = ?", (channel_id,))
        count = cur.rowcount
        conn.commit()
    return count


def deactivate_channel(db_path: str, channel_id: int, channel_link: str = "") -> None:
    """Soft-deactivate a channel (sets is_active = 0) without purging records or user access."""
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        if channel_link and channel_link.strip():
            conn.execute(
                "UPDATE channels SET is_active = 0 WHERE id = ? OR link = ?",
                (channel_id, channel_link.strip()),
            )
        else:
            conn.execute("UPDATE channels SET is_active = 0 WHERE id = ?", (channel_id,))
        conn.commit()


def get_post_by_msg_or_id(db_path: str, id_val: int) -> list:
    """Find posts matching id_val as either post.id or channel_msg_id."""
    conn = get_connection(db_path)
    rows = conn.execute("""
        SELECT * FROM posts WHERE id = ? OR channel_msg_id = ?
    """, (id_val, id_val)).fetchall()
    return rows


def purge_channel_and_posts(db_path: str, channel_id: int) -> None:
    """Completely delete a channel and all its promotional posts from the database."""
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        try:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute("DELETE FROM posts WHERE channel_id = ?", (channel_id,))
            conn.execute("DELETE FROM user_channel_access WHERE channel_id = ?", (channel_id,))
            conn.execute("DELETE FROM channels WHERE id = ?", (channel_id,))
            conn.commit()
        finally:
            conn.execute("PRAGMA foreign_keys = ON")

# ── Settings Helpers ──────────────────────────────────────────────────────────

def get_setting(db_path: str, key: str, default: Optional[str] = None) -> Optional[str]:
    conn = get_connection(db_path)
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(db_path: str, key: str, value: str) -> None:
    conn = get_connection(db_path)
    with _WRITE_LOCK:
        conn.execute("""
            INSERT INTO settings (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, value))
        conn.commit()
