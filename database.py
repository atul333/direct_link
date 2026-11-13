"""
database.py — SQLite database layer for Channel Link Provider Bot
"""

import sqlite3
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Persistent singleton connection (one per process, thread-safe via lock) ──
_DB_CONN: sqlite3.Connection | None = None
_DB_LOCK = threading.Lock()

def get_connection(db_path: str) -> sqlite3.Connection:
    """Return the shared persistent SQLite connection (creates once, reuses always).
    Thread-safe: all callers acquire _DB_LOCK before executing SQL.
    Eliminates the overhead of opening+PRAGMA-setting a new connection per call.
    """
    global _DB_CONN
    if _DB_CONN is None:
        _DB_CONN = sqlite3.connect(db_path, check_same_thread=False)
        _DB_CONN.row_factory = sqlite3.Row
        _DB_CONN.execute("PRAGMA journal_mode=WAL")
        _DB_CONN.execute("PRAGMA synchronous=NORMAL")
        _DB_CONN.execute("PRAGMA cache_size=-32000")  # 32MB page cache
        _DB_CONN.execute("PRAGMA foreign_keys=ON")
        _DB_CONN.execute("PRAGMA temp_store=MEMORY")  # Use memory for temp tables
        logger.info("Persistent SQLite connection created: %s", db_path)
    return _DB_CONN


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
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            link        TEXT NOT NULL,
            price_inr   REAL NOT NULL,
            description TEXT,
            is_active   INTEGER NOT NULL DEFAULT 1,
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            created_by  INTEGER NOT NULL,
            chat_id     TEXT
        )
    """)

    # Migrate existing channels table if chat_id column is missing
    try:
        cur.execute("ALTER TABLE channels ADD COLUMN chat_id TEXT")
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
        users = cur.execute("SELECT user_id, username FROM users WHERE username IS NOT NULL AND username != ''").fetchall()
        for u in users:
            clean_name = str(u["username"]).strip().lstrip("@")
            if clean_name:
                cur.execute("UPDATE users SET username = ? WHERE user_id = ?", (f"@{clean_name}", u["user_id"]))
    except Exception:
        pass

    # --- Performance indexes ---------------------------------------------------
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(razorpay_order_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id, status)")
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
    """Safely upsert user into central shared DB."""
    if not shared_db_path:
        return
    try:
        conn = sqlite3.connect(shared_db_path, timeout=5)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS shared_users (
                user_id     INTEGER PRIMARY KEY,
                username    TEXT,
                first_name  TEXT,
                updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        uname = format_username(username)
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
    conn.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (user_id,))
    conn.commit()


def unban_user(db_path: str, user_id: int) -> None:
    conn = get_connection(db_path)
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
                chat_id: Optional[str] = None) -> int:
    conn = get_connection(db_path)
    cur = conn.execute("""
        INSERT INTO channels (name, link, price_inr, description, created_by, chat_id)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (name, link, price_inr, description, admin_id, chat_id))
    channel_id = cur.lastrowid
    conn.commit()
    return channel_id


def update_channel(db_path: str, channel_id: int, **kwargs) -> bool:
    allowed = {"name", "link", "price_inr", "description", "is_active", "chat_id"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return False
    setters = ", ".join(f"{k} = ?" for k in updates)
    conn = get_connection(db_path)
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
    conn.execute("UPDATE channels SET is_active = 0 WHERE id = ?", (channel_id,))
    conn.commit()


# ── Payment helpers ────────────────────────────────────────────────────────────

def create_payment(db_path: str, user_id: int, channel_id: int,
                   razorpay_order_id: str, amount_inr: float) -> int:
    conn = get_connection(db_path)
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


def get_all_pending_payments(db_path: str, hours: int = 24) -> list:
    """Fetch recent pending payments to poll Razorpay."""
    conn = get_connection(db_path)
    rows = conn.execute(f"""
        SELECT * FROM payments
        WHERE status = 'pending' AND created_at >= datetime('now', '-{int(hours)} hours')
        ORDER BY created_at DESC
    """).fetchall()
    return rows



def confirm_payment(db_path: str, order_id: str, razorpay_payment_id: str) -> Optional[int]:
    """Mark payment as paid. Returns channel_id on success."""
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT * FROM payments WHERE razorpay_order_id = ?", (order_id,)
    ).fetchone()
    if not row:
            return None
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
    conn.execute("""
        INSERT OR IGNORE INTO user_channel_access (user_id, channel_id)
        VALUES (?, ?)
    """, (user_id, channel_id))
    conn.commit()


def has_access(db_path: str, user_id: int, channel_id: int) -> bool:
    conn = get_connection(db_path)
    row = conn.execute("""
        SELECT 1 FROM user_channel_access
        WHERE user_id = ? AND channel_id = ?
    """, (user_id, channel_id)).fetchone()
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
    cur = conn.execute("""
        INSERT INTO posts (admin_id, channel_id, title, caption, photo_file_id, price_inr, target_channel, channel_msg_id, cross_price_inr)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (admin_id, channel_id, title, caption, photo_file_id, price_inr, target_channel, channel_msg_id, cross_price_inr))
    post_id = cur.lastrowid
    conn.commit()
    return post_id


def update_post_msg_id(db_path: str, post_id: int, channel_msg_id: int) -> None:
    conn = get_connection(db_path)
    conn.execute("UPDATE posts SET channel_msg_id = ? WHERE id = ?", (channel_msg_id, post_id))
    conn.commit()


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


def get_posts_for_channel(db_path: str, channel_id: int, channel_link: str = "", channel_name: str = "") -> list:
    """Return all promotional posts created for a specific channel, matching by ID, link hash, or title."""
    conn = get_connection(db_path)
    query = "SELECT * FROM posts WHERE channel_id = ?"
    params = [channel_id]
    if channel_link:
        clean = channel_link.strip().rstrip("/")
        link_hash = clean.split("+")[-1].split("/")[-1].strip()
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
    conn.execute("DELETE FROM posts WHERE id = ?", (post_id,))
    conn.commit()


def delete_posts_for_channel(db_path: str, channel_id: int) -> int:
    """Delete promotional posts for a specific channel from the DB without touching channel or purchases."""
    conn = get_connection(db_path)
    cur = conn.execute("DELETE FROM posts WHERE channel_id = ?", (channel_id,))
    count = cur.rowcount
    conn.commit()
    return count


def deactivate_channel(db_path: str, channel_id: int) -> None:
    """Soft-deactivate a channel (sets is_active = 0) without purging records or user access."""
    conn = get_connection(db_path)
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
    conn.execute("""
        INSERT INTO settings (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (key, value))
    conn.commit()
