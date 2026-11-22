"""
bot.py — Channel Link Provider Bot (main entry point)

Admin commands:
  /post           — Broadcast image, text & buy button to Main Channel
  /posts          — List published broadcast posts
  /setmainchannel — Set default channel for broadcasts
  /addchannel     — Add a new channel product (with join-request anti-leak)
  /editchannel    — Edit channel name / link / price / description / chat_id
  /delchannel     — Deactivate a channel
  /channels       — List all channels (with IDs and chat_ids)
  /getid          — Get Chat ID of current chat or forwarded channel post
  /grantaccess    — Grant free access to a user
  /broadcast      — Broadcast a message to all bot users
  /stats          — Full statistics dashboard
  /users          — List recent users
  /payments       — List paid payments for a channel
  /ban            — Ban a user
  /unban          — Unban a user

User commands:
  /start          — Welcome + register (supports deep-linking buy_{post_id})
  /menu           — Browse available channels
  /mypurchases    — View purchased channels with anti-leak protected access
  /help           — Instructions & FAQ guide
  /verify         — Manually verify a payment by Transaction ID
"""

import asyncio
import base64
import html
import io
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import unicodedata
from datetime import datetime
from typing import Optional

from PIL import Image, ImageOps

import httpx
import razorpay
from aiohttp import web
from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, ChatJoinRequest, Bot, CallbackQuery, Message,
)
from telegram.error import BadRequest, Forbidden, TimedOut, NetworkError, RetryAfter
from telegram.constants import ParseMode, ChatMemberStatus
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler,
    ChatJoinRequestHandler, ChatMemberHandler, TypeHandler,
)

import database as db
import payments as pay
from broadcast import broadcast_message

# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
BOT_TOKEN          = os.getenv("BOT_TOKEN", "")
BOT_ID             = int(BOT_TOKEN.split(":")[0]) if ":" in BOT_TOKEN else 0
BOT_USERNAME       = os.getenv("BOT_USERNAME", "DirectChannelLink_A1_bot")
ADMIN_IDS          = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
RZP_KEY_ID         = os.getenv("RAZORPAY_KEY_ID", "")
RZP_KEY_SECRET     = os.getenv("RAZORPAY_KEY_SECRET", "")
RZP_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")
WEBHOOK_PORT       = int(os.getenv("WEBHOOK_PORT", "5000"))
DB_PATH            = os.getenv("DATABASE_PATH", "bot_data.db")
BROADCAST_DELAY    = float(os.getenv("BROADCAST_DELAY", "0.05"))
MAIN_CHANNEL_ID    = os.getenv("MAIN_CHANNEL_ID", "")
MANDATORY_CHANNELS_RAW = os.getenv("MANDATORY_CHANNELS", "-1003833649347,-1003962377737,-1004296919279")
MANDATORY_CHANNELS = [
    int(x.strip()) for x in MANDATORY_CHANNELS_RAW.split(",") if x.strip()
]
ADMIN_CONTACT      = os.getenv("ADMIN_CONTACT", "@videos69_Admin_Bot")

SHARED_DB_PATH = os.getenv("SHARED_PREMIUM_DB", "/home/ubuntu/premium_shared.db")
_known_users_cache: dict[int, str] = {}

# --- Active channel TTL cache (60 s) -----------------------------------------
_active_channels_cache: list = []
_active_channels_cache_time: float = 0.0
_CHANNEL_CACHE_TTL: float = 60.0

def get_active_channels_cached() -> list:
    """Return active deduplicated channels from memory; refresh every 60 s."""
    global _active_channels_cache, _active_channels_cache_time
    if not _active_channels_cache or (time.time() - _active_channels_cache_time > _CHANNEL_CACHE_TTL):
        rows = db.get_active_channels(DB_PATH)
        seen_links: set[str] = set()
        deduped: list = []
        for r in sorted(rows, key=lambda x: x["id"], reverse=True):
            lnk = (r["link"] or "").strip().rstrip("/").lower()
            if not lnk or lnk in seen_links:
                continue
            seen_links.add(lnk)
            deduped.append(r)
        deduped.sort(key=lambda x: x["id"])
        _active_channels_cache = deduped
        _active_channels_cache_time = time.time()
    return _active_channels_cache

def invalidate_channel_cache() -> None:
    """Force cache refresh on next access (call after add/edit/delete channel)."""
    global _active_channels_cache_time, _active_channels_cache
    _active_channels_cache_time = 0.0
    _active_channels_cache = []

# ── Async DB helpers (run blocking DB calls off the event loop) ──────────────
async def db_get_channel(channel_id: int):
    """Non-blocking: fetch channel from DB in thread pool."""
    return await asyncio.to_thread(db.get_channel, DB_PATH, channel_id)

async def db_has_access(user_id: int, channel_id: int) -> bool:
    """Non-blocking: check user access in thread pool."""
    return await asyncio.to_thread(db.has_access, DB_PATH, user_id, channel_id)

async def db_get_post(post_id: int):
    """Non-blocking: fetch post in thread pool."""
    return await asyncio.to_thread(db.get_post, DB_PATH, post_id)

async def db_get_user_purchases(user_id: int) -> list:
    """Non-blocking: fetch user purchases in thread pool."""
    return await asyncio.to_thread(db.get_user_purchases, DB_PATH, user_id)

async def db_get_payment_by_order(order_id: str):
    """Non-blocking: fetch payment by order in thread pool."""
    return await asyncio.to_thread(db.get_payment_by_order, DB_PATH, order_id)


# --- Mandatory-channel membership TTL cache ----------------------------------
# {user_id: (timestamp, [unjoined_list])}
_mandatory_cache: dict[int, tuple] = {}
_MANDATORY_CACHE_TTL: float = 900.0          # 15 min — verified users (all joined)
_MANDATORY_CACHE_TTL_PARTIAL: float = 20.0   # 20 sec — unverified users (avoids API storm on rapid taps)
_mandatory_inflight: dict[int, asyncio.Lock] = {}

# Shared persistent AsyncClient for non-Telegram HTTP calls (e.g. OCR fallback)
_ASYNC_HTTP_CLIENT = httpx.AsyncClient(
    timeout=httpx.Timeout(12.0, connect=5.0),
    follow_redirects=True,
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    headers={
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    },
)

_TG_WEB_USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
)
_TG_WEB_UA_COUNTER = 0
_LAST_CONFIRMED_LIVE_INVITE: Optional[str] = None


async def _tg_web_get(url: str) -> httpx.Response:
    """
    Fetch a t.me web page using a fresh cookie-less HTTP client so Telegram's
    `stel_ssid` session cookie is NEVER persisted or reused across link checks.
    (Reusing one `stel_ssid` cookie across dozens of t.me/+ checks causes Telegram
    to soft-throttle that session into generic 200 OK 'Join group chat on Telegram' pages.)
    """
    global _TG_WEB_UA_COUNTER
    ua = _TG_WEB_USER_AGENTS[_TG_WEB_UA_COUNTER % len(_TG_WEB_USER_AGENTS)]
    _TG_WEB_UA_COUNTER += 1
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(12.0, connect=5.0),
        follow_redirects=True,
        headers=headers,
    ) as client:
        return await client.get(url)

# ── Resilient Telegram API wrappers (suppress expired queries, unmodified edits & timeouts) ──
_orig_callback_query_answer = CallbackQuery.answer
_orig_callback_query_edit = CallbackQuery.edit_message_text
_orig_message_edit_text = Message.edit_text

async def _resilient_callback_query_answer(self, *args, **kwargs):
    try:
        return await _orig_callback_query_answer(self, *args, **kwargs)
    except BadRequest as exc:
        msg = str(exc).lower()
        if "query is too old" in msg or "query id is invalid" in msg:
            logger.debug("Silently ignored expired/invalid callback query: %s", exc)
            return None
        raise
    except (TimedOut, NetworkError) as exc:
        logger.debug("Silently ignored timeout on callback query answer: %s", exc)
        return None

async def _resilient_callback_query_edit(self, *args, **kwargs):
    try:
        return await _orig_callback_query_edit(self, *args, **kwargs)
    except BadRequest as exc:
        msg = str(exc).lower()
        if "message is not modified" in msg or "message to edit not found" in msg:
            logger.debug("Silently ignored benign BadRequest in query edit: %s", exc)
            return None
        raise

async def _resilient_message_edit_text(self, *args, **kwargs):
    try:
        return await _orig_message_edit_text(self, *args, **kwargs)
    except BadRequest as exc:
        msg = str(exc).lower()
        if "message is not modified" in msg or "message to edit not found" in msg:
            logger.debug("Silently ignored benign BadRequest in msg edit: %s", exc)
            return self
        raise

CallbackQuery.answer = _resilient_callback_query_answer
CallbackQuery.edit_message_text = _resilient_callback_query_edit
Message.edit_text = _resilient_message_edit_text


async def track_user_action_fast(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Zero-load user tracker: updates database only when user is new or username changed."""
    user = update.effective_user
    if not user or user.is_bot:
        return
    uid = user.id
    raw_uname = user.username
    uname = f"@{raw_uname.lstrip('@')}" if raw_uname else ""
    if uid in _known_users_cache and _known_users_cache[uid] == uname:
        return
    _known_users_cache[uid] = uname
    fname = user.first_name or ""
    lname = user.last_name or ""
    try:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, db.upsert_user, DB_PATH, uid, uname, fname, lname)
        if SHARED_DB_PATH and os.path.isfile(SHARED_DB_PATH):
            loop.run_in_executor(None, db.upsert_central_user, SHARED_DB_PATH, uid, uname, fname)
    except Exception:
        pass


# ── Conversation states ────────────────────────────────────────────────────────
(
    AC_NAME, AC_LINK, AC_PRICE, AC_DESC, AC_CHAT_ID,   # add channel
    EC_FIELD, EC_VALUE,                                 # edit channel
    BC_MSG,                                             # broadcast
    VERIFY_ID,                                          # manual verify
    POST_PHOTO, POST_TEXT, POST_CHANNEL, POST_PRICE, POST_CROSS_PRICE, POST_TARGET, POST_PREVIEW, # channel broadcast wizard
    CHECKLINKS_WAIT,                                    # interactive /checklinks link validator
    NEWLINKS_WAIT,                                      # interactive /newlinks active-link comparator
) = range(18)

# ── Helpers ────────────────────────────────────────────────────────────────────

def get_admin_ids() -> list[int]:
    """Read admin IDs dynamically from environment and defaults."""
    raw = os.getenv("ADMIN_IDS", "")
    ids = []
    for x in raw.split(","):
        x = x.strip()
        if x.isdigit():
            ids.append(int(x))
    for aid in ADMIN_IDS:
        if aid not in ids:
            ids.append(aid)
    return ids


def is_admin(user_id: int) -> bool:
    try:
        return int(user_id) in get_admin_ids()
    except Exception:
        return False



def rzp_client() -> razorpay.Client:
    return pay.get_razorpay_client(RZP_KEY_ID, RZP_KEY_SECRET)


_ADULT_MENU_EMOJIS = [
    "🔞", "🍑", "🥵", "💦", "🍒", "💋", "👅", "🍆",
    "🔥", "😈", "👙", "👠", "💃", "🌶️", "🫦", "🍓",
    "🍌", "🥂", "🦊", "😏",
]

# Strip decorative emojis from channel title while preserving squared Latin letters (e.g. 🅳🅴🆂🅸)
_STRIP_TITLE_EMOJIS_RE = re.compile(
    r"[\U0001F190-\U0010FFFF\U0001F000-\U0001F16F\u2600-\u27BF\uFE0F\u200D]+"
)


def _format_media_badge(media_counts: str) -> str:
    """Extract and format ONLY the video count (no images/photos count and no extra emojis)."""
    raw = (media_counts or "").strip()
    if not raw:
        return ""
    m_v = re.search(
        r"(\d[\d,\s.]*[kKmM]?)\s*(?:videos?|v[iI1l!|]?[dD][eEcC0oO][oO0]?s?|vids?)\b"
        r"|\b(?:videos?|vids?)\s*[:\-]?\s*(\d[\d,\s.]*[kKmM]?)",
        raw,
        re.IGNORECASE,
    )
    v_num = re.sub(r"[\s,]+", "", (m_v.group(1) or m_v.group(2) or "")) if m_v else ""
    if v_num:
        return f"{v_num} Videos"
    return ""


def _visual_em_width(text: str) -> float:
    """Estimate visual proportional-font width (in em units relative to \u2800 Braille blank = 1.0)."""
    total = 0.0
    for ch in text:
        code = ord(ch)
        if ch in ("\u200e", "\u200f", "\u2066", "\u2069", "\ufe0f", "\u200d"):
            continue
        if ch in "1ilI.,:!|' ":
            total += 0.45
        elif ch in "frtJ()-":
            total += 0.60
        elif ch in "MWm@":
            total += 1.25
        elif "A" <= ch <= "Z":
            total += 1.05
        elif code > 0x2E80:
            total += 1.75
        else:
            total += 0.85
    return total


MENU_PAGE_SIZE = 20
ADMIN_PAGE_SIZE = 20
PURCHASES_PAGE_SIZE = 20
TELEGRAM_MAX_REPLY_MARKUP_BYTES = 9200  # Telegram hard limit is 10,240 bytes


def _markup_json_size(markup: InlineKeyboardMarkup) -> int:
    """Measure serialized JSON size using ASCII escaping (worst-case PTB wire size)."""
    return len(json.dumps(markup.to_dict(), ensure_ascii=True).encode("utf-8"))


def build_menu_text(
    total_channels: int, page: int = 0, page_size: int = MENU_PAGE_SIZE
) -> str:
    """Build the /menu header text including current page range when paginated."""
    if total_channels <= 0:
        return "😔 No channels available right now."
    total_pages = max(1, (total_channels + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start_num = page * page_size + 1
    end_num = min(total_channels, (page + 1) * page_size)
    page_line = (
        f"📄 <i>Showing {start_num}–{end_num} of {total_channels} channels (Page {page + 1}/{total_pages})</i>\n"
        if total_pages > 1
        else f"📊 <i>Total Active Channels: {total_channels}</i>\n"
    )
    return (
        "<blockquote><b>🔞 EXCLUSIVE VIP CHANNELS DIRECTORY 🔥</b></blockquote>\n"
        "<blockquote>✨ Select any VIP channel below to view full details, perks & unlock instant access:\n\n"
        "💰 <i>Tap the sort buttons below to sort channels by price (Low ➔ High or High ➔ Low)</i>\n"
        "⚡ <i>Instant automated UPI payment • Protected 1-click delivery</i>\n"
        f"{page_line}"
        "──────────────────────────\n"
        f"🏆 Admin - {ADMIN_CONTACT}</blockquote>"
    )


def format_channel_menu_label(c, index: int = 1, target_em: float = 50.0) -> str:
    """
    Format /menu button label with:
    - 1, 2, 3... sequential nomenclature
    - Exactly 1 distinct adult emoji per row
    - Price first, then Channel Name, then Videos count (no photos/images count)
    - Right-padded with non-collapsing Braille blanks (\u2800) + \u200e so Telegram pushes
      the text all the way to the LEFT side of the button instead of centering in the middle.
    """
    raw_name = unicodedata.normalize("NFKC", (row_get(c, "name") or "").strip())
    clean_name = _STRIP_TITLE_EMOJIS_RE.sub("", raw_name)
    clean_name = re.sub(r"\s+", " ", clean_name).strip(" •-—:|")
    if not clean_name or re.match(r"^(?:VIP\s*)?Channel\s*#?\d+$", clean_name, re.IGNORECASE):
        clean_name = "VIP Exclusive"
    if len(clean_name) > 36:
        clean_name = clean_name[:35].rstrip() + "…"

    lead_emoji = _ADULT_MENU_EMOJIS[(max(1, index) - 1) % len(_ADULT_MENU_EMOJIS)]
    price_val = float(row_get(c, "price_inr", 0) or 0)
    price_tag = "FREE" if price_val <= 0 else f"₹{price_val:.0f}"

    videos_badge = _format_media_badge(row_get(c, "media_counts") or "")
    safe_name = f"\u2066{clean_name}\u2069\u200e"

    if videos_badge:
        core_label = f"\u200e{index}. {lead_emoji} {price_tag} • {safe_name} • {videos_badge}"
    else:
        core_label = f"\u200e{index}. {lead_emoji} {price_tag} • {safe_name}"

    if target_em > 0:
        cur_em = _visual_em_width(core_label)
        pad_count = max(0, int(round(target_em - cur_em)))
        if pad_count > 0:
            return core_label + ("\u2800" * pad_count) + "\u200e"
    return core_label + "\u200e"


def channel_list_keyboard(
    channels: list,
    prefix: str = "buy",
    sort_by: str = "default",
    page: int = 0,
    page_size: int = MENU_PAGE_SIZE,
) -> InlineKeyboardMarkup:
    sorted_channels = list(channels)
    if sort_by == "price_asc":
        sorted_channels.sort(key=lambda c: (float(row_get(c, "price_inr", 0) or 0), c["id"]))
    elif sort_by == "price_desc":
        sorted_channels.sort(key=lambda c: (-float(row_get(c, "price_inr", 0) or 0), c["id"]))

    total = len(sorted_channels)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    page_channels = sorted_channels[start : start + page_size]

    def _build_markup(target_em: float, items: list) -> InlineKeyboardMarkup:
        buttons = []
        if prefix == "view" and total > 1:
            asc_txt = "✅ 💰 Price: Low ➔ High" if sort_by == "price_asc" else "💰 Price: Low ➔ High"
            desc_txt = "✅ 💎 Price: High ➔ Low" if sort_by == "price_desc" else "💎 Price: High ➔ Low"
            buttons.append([
                InlineKeyboardButton(asc_txt, callback_data="menu_sort:price_asc"),
                InlineKeyboardButton(desc_txt, callback_data="menu_sort:price_desc"),
            ])

        for idx, c in enumerate(items, start + 1):
            buttons.append([InlineKeyboardButton(
                format_channel_menu_label(c, index=idx, target_em=target_em),
                callback_data=f"{prefix}:{c['id']}"
            )])

        if prefix == "view" and total_pages > 1:
            nav_row = []
            if page > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"menu_page:{page - 1}"))
            nav_row.append(InlineKeyboardButton(f"📄 Page {page + 1}/{total_pages}", callback_data="menu_noop"))
            if page < total_pages - 1:
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"menu_page:{page + 1}"))
            buttons.append(nav_row)

        return InlineKeyboardMarkup(buttons)

    # Keep full left-alignment padding (50.0 em) whenever it fits under Telegram's 10 KB limit;
    # gracefully reduce padding only if a page has unusually long multi-byte titles.
    for target_em in (50.0, 38.0, 26.0, 0.0):
        markup = _build_markup(target_em, page_channels)
        if _markup_json_size(markup) <= TELEGRAM_MAX_REPLY_MARKUP_BYTES:
            return markup

    # Absolute last-resort safety cap so Telegram never rejects with "Reply markup is too long"
    trimmed = list(page_channels)
    while len(trimmed) > 1:
        trimmed.pop()
        markup = _build_markup(0.0, trimmed)
        if _markup_json_size(markup) <= TELEGRAM_MAX_REPLY_MARKUP_BYTES:
            return markup
    return _build_markup(0.0, trimmed)


def admin_channel_keyboard(
    channels: list,
    prefix: str,
    page: int = 0,
    page_size: int = ADMIN_PAGE_SIZE,
) -> InlineKeyboardMarkup:
    total = len(channels)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    page_channels = channels[start : start + page_size]

    buttons = [
        [InlineKeyboardButton(
            f"[#{c['id']}] {(c['name'] or '')[:30]} ({'✅' if c['is_active'] else '❌'})",
            callback_data=f"{prefix}:{c['id']}"
        )]
        for c in page_channels
    ]
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"adm_ch_page:{prefix}:{page - 1}"))
        nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="menu_noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"adm_ch_page:{prefix}:{page + 1}"))
        buttons.append(nav_row)
    return InlineKeyboardMarkup(buttons)


def build_mypurchases_keyboard(
    purchases: list,
    page: int = 0,
    page_size: int = PURCHASES_PAGE_SIZE,
) -> InlineKeyboardMarkup:
    total = len(purchases)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    page_items = purchases[start : start + page_size]

    buttons = [
        [InlineKeyboardButton(f"✨ {(p['name'] or '')[:36]} 🔓", callback_data=f"getlink:{p['id']}")]
        for p in page_items
    ]
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"mypurch_page:{page - 1}"))
        nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="menu_noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"mypurch_page:{page + 1}"))
        buttons.append(nav_row)
    buttons.append([InlineKeyboardButton("🛍️ Browse More Channels", callback_data="back_menu")])
    return InlineKeyboardMarkup(buttons)


def build_post_channel_keyboard(
    channels: list,
    is_free: bool = False,
    page: int = 0,
    page_size: int = ADMIN_PAGE_SIZE,
) -> InlineKeyboardMarkup:
    total = len(channels)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    page_channels = channels[start : start + page_size]

    buttons = [[InlineKeyboardButton("➕ Paste New Invite Link", callback_data="post_ch:new")]]
    for c in page_channels:
        p_label = "FREE 🎁" if (is_free or float(c["price_inr"] or 0) <= 0) else f"₹{c['price_inr']:.0f}"
        buttons.append([InlineKeyboardButton(
            f"📌 [#{c['id']}] {(c['name'] or '')[:26]} ({p_label})",
            callback_data=f"post_ch:{c['id']}"
        )])
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"post_ch_page:{page - 1}"))
        nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="menu_noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"post_ch_page:{page + 1}"))
        buttons.append(nav_row)
    return InlineKeyboardMarkup(buttons)


# ──────────────────────────────────────────────────────────────────────────────
# MANDATORY CHANNELS (FORCE SUBSCRIBE)
# ──────────────────────────────────────────────────────────────────────────────

CHANNEL_INFO_CACHE: dict[int, dict] = {}
_UNREACHABLE_MANDATORY_UNTIL: dict[int, float] = {}


async def get_channel_info(bot: Bot, chat_id: int) -> dict:
    """Fetch title and invite link for a channel, with smart caching."""
    now = time.time()
    if chat_id in CHANNEL_INFO_CACHE:
        cached = CHANNEL_INFO_CACHE[chat_id]
        ttl = 3600.0 if cached.get("invite_link") else 600.0
        if now - cached["updated_at"] < ttl:
            return cached

    title = f"Channel {chat_id}"
    invite_link = None

    try:
        chat = await bot.get_chat(chat_id)
        if chat.title:
            title = chat.title

        if chat.invite_link:
            invite_link = chat.invite_link
        elif chat.username:
            invite_link = f"https://t.me/{chat.username}"
        else:
            try:
                invite = await bot.create_chat_invite_link(chat_id=chat_id, name="Mandatory Join")
                invite_link = invite.invite_link
            except Exception as e:
                logger.debug("Could not create invite link for %s: %s", chat_id, e)
    except Exception as exc:
        logger.warning("Could not get chat details for %s: %s (ensure bot is added as admin)", chat_id, exc)
        _UNREACHABLE_MANDATORY_UNTIL[chat_id] = now + 600.0

    info = {
        "chat_id": chat_id,
        "title": title,
        "invite_link": invite_link,
        "updated_at": now,
    }
    CHANNEL_INFO_CACHE[chat_id] = info
    return info


async def is_user_in_channel(bot: Bot, user_id: int, chat_id: int) -> bool:
    """Check if user is a member/admin in the specified channel."""
    now = time.time()
    if _UNREACHABLE_MANDATORY_UNTIL.get(chat_id, 0.0) > now:
        return True

    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        return member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
            ChatMemberStatus.RESTRICTED,
        )
    except Exception as exc:
        err_msg = str(exc).lower()
        if "user not found" in err_msg or "member not found" in err_msg:
            return False
        if "chat not found" in err_msg or "not a member" in err_msg or "bot was kicked" in err_msg or "not enough rights" in err_msg:
            # Bot is not an admin in this channel yet — suppress for 10 min so we don't spam API
            logger.warning("Bot is not an admin in mandatory channel %s: %s", chat_id, exc)
            _UNREACHABLE_MANDATORY_UNTIL[chat_id] = now + 600.0
            return True
        return False


async def get_unjoined_mandatory_channels(bot: Bot, user_id: int, bypass_cache: bool = False) -> list[dict]:
    """Return list of mandatory channels the user has NOT joined yet. Admins bypass.
    Checks all mandatory channels concurrently (asyncio.gather) and caches both
    verified users (15 min) and unverified users (20 s) to eliminate getChatMember storms.
    """
    if is_admin(user_id) or not MANDATORY_CHANNELS:
        return []

    now = time.time()
    if not bypass_cache:
        cached = _mandatory_cache.get(user_id)
        if cached:
            ts, unjoined_cached = cached
            ttl = _MANDATORY_CACHE_TTL if not unjoined_cached else _MANDATORY_CACHE_TTL_PARTIAL
            if (now - ts) < ttl:
                return unjoined_cached

    lock = _mandatory_inflight.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _mandatory_inflight[user_id] = lock

    async with lock:
        now = time.time()
        if not bypass_cache:
            cached = _mandatory_cache.get(user_id)
            if cached:
                ts, unjoined_cached = cached
                ttl = _MANDATORY_CACHE_TTL if not unjoined_cached else _MANDATORY_CACHE_TTL_PARTIAL
                if (now - ts) < ttl:
                    return unjoined_cached

        results = await asyncio.gather(
            *(is_user_in_channel(bot, user_id, cid) for cid in MANDATORY_CHANNELS),
            return_exceptions=True,
        )

        unjoined_cids = [
            cid for cid, joined in zip(MANDATORY_CHANNELS, results)
            if not (isinstance(joined, bool) and joined)
        ]

        if unjoined_cids:
            infos = await asyncio.gather(
                *(get_channel_info(bot, cid) for cid in unjoined_cids),
                return_exceptions=True,
            )
            unjoined = [info for info in infos if isinstance(info, dict)]
        else:
            unjoined = []

        _mandatory_cache[user_id] = (time.time(), unjoined)
        return unjoined


def format_user_display(username: Optional[str], fallback: str = "—") -> str:
    """Format a username for display with exactly one @ prefix, or return fallback."""
    if not username:
        return fallback
    clean = str(username).strip().lstrip("@")
    return f"@{clean}" if clean else fallback


def row_get(row, key: str, default=None):
    """Safely get a value from sqlite3.Row, dict, or object without raising AttributeError."""
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        if key in row.keys():
            val = row[key]
            return val if val is not None else default
    except Exception:
        pass
    return default


async def reply_or_send_text(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    protect_content: bool = False,
    parse_mode: str = ParseMode.HTML,
):
    """Safe text sender — returns the sent Message object."""
    chat_id = update.effective_chat.id if update.effective_chat else (update.effective_user.id if update.effective_user else None)
    if update.message:
        try:
            return await update.message.reply_text(
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                protect_content=protect_content,
            )
        except Exception:
            pass
    if chat_id:
        return await ctx.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            protect_content=protect_content,
        )
    return None


async def reply_or_send_photo(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    photo,
    caption: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    protect_content: bool = False,
    parse_mode: str = ParseMode.HTML,
):
    """Safe photo sender — returns the sent Message object."""
    chat_id = update.effective_chat.id if update.effective_chat else (update.effective_user.id if update.effective_user else None)
    if update.message:
        try:
            return await update.message.reply_photo(
                photo=photo,
                caption=caption,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                protect_content=protect_content,
            )
        except Exception:
            pass
    if chat_id:
        return await ctx.bot.send_photo(
            chat_id=chat_id,
            photo=photo,
            caption=caption,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            protect_content=protect_content,
        )
    return None



async def send_mandatory_channels_prompt(
    target,
    ctx: ContextTypes.DEFAULT_TYPE,
    unjoined_channels: list[dict],
    resume_action: str = "menu"
) -> None:
    """Prompt user to join mandatory channels before proceeding."""
    keyboard_buttons = []
    text_lines = []

    for i, ch in enumerate(unjoined_channels, 1):
        title = ch.get("title") or f"Official Channel #{i}"
        link = ch.get("invite_link")
        text_lines.append(f"{i}️⃣ <b>{title}</b>")
        if link:
            keyboard_buttons.append([InlineKeyboardButton(f"📢 Join {title}", url=link)])

    keyboard_buttons.append([
        InlineKeyboardButton("✅ Verify & Continue", callback_data=f"check_sub:{resume_action}")
    ])

    text = (
        "<blockquote><b>🛡️ COMMUNITY ACCESS VERIFICATION 🛡️</b></blockquote>\n"
        "<blockquote>👋 Welcome! Before accessing our exclusive VIP channels, please join our official sponsor & update channels:\n\n"
        + "\n".join(text_lines) + "\n"
        "──────────────────────────\n"
        "📌 <b>QUICK INSTRUCTIONS:</b>\n"
        "1️⃣ Tap each channel button above to join\n"
        "2️⃣ Return here and tap <b>'✅ Verify & Continue 🚀'</b> to unlock instant access!</blockquote>"
    )

    markup = InlineKeyboardMarkup(keyboard_buttons)

    # 1. Direct CallbackQuery
    if hasattr(target, "edit_message_text"):
        try:
            await target.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
            return
        except Exception:
            pass

    # 2. Update object with callback_query
    if hasattr(target, "callback_query") and target.callback_query:
        try:
            await target.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
            return
        except Exception:
            pass

    # 3. Message object
    if hasattr(target, "reply_text"):
        try:
            await target.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
            return
        except Exception:
            pass

    # 4. Fallback using effective chat
    chat_id = None
    if hasattr(target, "effective_chat") and target.effective_chat:
        chat_id = target.effective_chat.id
    elif hasattr(target, "from_user") and target.from_user:
        chat_id = target.from_user.id
    elif hasattr(target, "chat_id") and target.chat_id:
        chat_id = target.chat_id

    if chat_id:
        await ctx.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def cb_check_subscription(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback when user taps 'Verify & Continue'."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    resume_action = data.split(":", 1)[1] if ":" in data else "menu"

    unjoined = await get_unjoined_mandatory_channels(ctx.bot, user_id, bypass_cache=True)
    if unjoined:
        await query.answer("❌ You haven't joined all mandatory channels yet! Please join all of them first.", show_alert=True)
        await send_mandatory_channels_prompt(query, ctx, unjoined, resume_action)
        return

    await query.answer("🎉 Verified! Access unlocked.", show_alert=True)
    try:
        await query.message.delete()
    except Exception:
        pass

    if resume_action.startswith("buy_") or resume_action.startswith("post_"):
        try:
            post_id = int(resume_action.split("_")[1])
            return await handle_start_buy_post(update, ctx, post_id)
        except Exception as exc:
            logger.error("Error resuming post %s: %s", resume_action, exc)
    elif resume_action.startswith("c_"):
        try:
            channel_id = int(resume_action.split("_")[1])
            return await handle_start_buy_channel(update, ctx, channel_id)
        except Exception as exc:
            logger.error("Error resuming channel %s: %s", resume_action, exc)
    elif resume_action == "mypurchases":
        return await cmd_mypurchases(update, ctx)
    elif resume_action == "start":
        return await cmd_start(update, ctx)

    return await cmd_menu(update, ctx)


async def cmd_mandatory(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: view status of mandatory channels."""
    if not is_admin(update.effective_user.id):
        return

    lines = ["🔒 <b>Configured Mandatory Channels:</b>\n"]
    for i, cid in enumerate(MANDATORY_CHANNELS, 1):
        info = await get_channel_info(ctx.bot, cid)
        title = info.get("title") or f"Channel {cid}"
        link = info.get("invite_link") or "None"
        try:
            member = await ctx.bot.get_chat_member(chat_id=cid, user_id=ctx.bot.id)
            is_adm = member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
            status_emoji = "✅ Bot is Admin" if is_adm else "⚠️ Bot is Member (not admin)"
        except Exception as exc:
            status_emoji = f"❌ Error: {exc}"

        lines.append(
            f"{i}️⃣ <b>{title}</b>\n"
            f"   • ID: <code>{cid}</code>\n"
            f"   • Status: {status_emoji}\n"
            f"   • Link: {link}\n"
        )

    lines.append(
        "💡 <i>Tip: For any channel showing 'Chat not found', please make sure this bot is added "
        "as an Administrator with 'Post Messages' and 'Invite Users via Link' permissions.</i>"
    )

    await reply_or_send_text(update, ctx, "\n".join(lines), parse_mode=ParseMode.HTML)


# ──────────────────────────────────────────────────────────────────────────────
# COMMON
# ──────────────────────────────────────────────────────────────────────────────

async def send_protected_channel_link(bot, user_id: int, channel: db.sqlite3.Row) -> None:
    """
    Delivers the private channel access link to the buyer with anti-leak protection:
    1. Generates a Join-Request invite link (creates_join_request=True) if chat_id is available
    2. Sends message with protect_content=True (disables forwarding, copying text, screenshots)
    3. Unauthorized users attempting to use or share the link are declined by handle_chat_join_request
    """
    chat_id = row_get(channel, "chat_id")
    join_url = channel["link"]
    is_join_req = False

    if chat_id:
        try:
            try:
                target_chat_id = int(chat_id)
            except (ValueError, TypeError):
                target_chat_id = chat_id
            invite = await bot.create_chat_invite_link(
                chat_id=target_chat_id,
                name=f"Buyer_{user_id}_{int(time.time())}",
                creates_join_request=True,
                expire_date=int(time.time()) + 900,
            )
            join_url = invite.invite_link
            is_join_req = True
            logger.info("Generated join-request invite link for user %d in chat %s", user_id, chat_id)
        except Exception as exc:
            logger.warning("Could not create join-request link for chat %s: %s (falling back to static link)", chat_id, exc)

    action_text = (
        "👇 <b>Tap the button below to join — your account is pre-approved!</b>\n"
        "✨ <i>You will be admitted immediately upon clicking!</i>"
        if is_join_req else
        "👇 <b>Tap the button below to join your VIP channel immediately:</b>"
    )

    text = (
        f"<blockquote><b>🎉 PAYMENT CONFIRMED • ACCESS GRANTED! 🎉</b></blockquote>\n"
        f"<blockquote><b>🏆 VIP Channel:</b> <b>{channel['name']}</b>\n"
        f"<b>👤 Subscriber ID:</b> <code>{user_id}</code>\n"
        f"<b>⚡ Status:</b> <b>Active & Verified ✅</b></blockquote>\n"
        f"<blockquote><b>📌 IMPORTANT INSTRUCTIONS:</b>\n"
        f"1️⃣ <b>Click '🚀 Join VIP Channel Now' below immediately</b>\n"
        f"2️⃣ Do not forward or share this link with anyone\n"
        f"3️⃣ Access is securely locked to your Telegram account</blockquote>\n"
        f"<blockquote>⚠️ <b>ONE-TIME LINK — NO BACKUP AVAILABLE</b>\n"
        f"──────────────────────────\n"
        f"🏆 Admin - {ADMIN_CONTACT}</blockquote>\n\n"
        f"{action_text}"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Join VIP Channel Now 🔓", url=join_url)]
    ])

    await bot.send_message(
        chat_id=user_id,
        text=text,
        parse_mode=ParseMode.HTML,
        protect_content=True,
        reply_markup=keyboard,
    )


_PAYMENT_LOCKS: dict[str, asyncio.Lock] = {}
_PAID_ORDER_CACHE: set[str] = set()
_ACTIVE_QR_WATCHERS: dict[str, float] = {}


async def _delete_qr_message_safe(bot, chat_id: int, message_id: int, user_id: int) -> None:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info("Deleted QR payment message for user %d (msg_id=%s)", user_id, message_id)
    except Exception as exc:
        logger.debug("Could not delete QR message for user %d: %s", user_id, exc)


async def _notify_admins_of_sale(
    bot,
    user_id: int,
    channel_id: int,
    channel_name: str,
    amount: float,
    order_id: str,
    payment_id: str,
    method: str | None,
) -> None:
    """Background task to notify admins after the buyer has already received their channel link."""
    try:
        user = await asyncio.to_thread(db.get_user, DB_PATH, user_id)
        uname = format_user_display(user["username"], fallback="None") if user else "None"
        full_name = f"{user['first_name']} {user['last_name']}".strip() if user else "Telegram User"
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        isolated_tx_id = f"CLP5000-{payment_id}" if payment_id and not payment_id.startswith("CLP") else (payment_id or "N/A")
        isolated_ref_id = f"CLP5000-{order_id}" if order_id and not order_id.startswith("CLP") else order_id

        safe_full_name = html.escape(full_name or "Telegram User")
        safe_channel_name = html.escape(str(channel_name or f"Channel #{channel_id}"))
        admin_text = (
            "<blockquote><b>💸 New Sale! Payment Auto-Approved</b></blockquote>\n"
            "<blockquote>"
            f"👤 <b>User ID:</b> <code>{user_id}</code>\n"
            f"👤 <b>Username:</b> {html.escape(uname)}\n"
            f"👤 <b>Name:</b> {safe_full_name}\n"
            f"🏷️ <b>Channel:</b> <b>{safe_channel_name}</b> (<code>#{channel_id}</code>)\n"
            f"🆔 <b>Channel ID:</b> <code>#{channel_id}</code>\n"
            f"💰 <b>Amount Paid:</b> <b>₹{amount:.0f}</b>\n"
            f"🔒 <b>Isolated Port:</b> <code>{WEBHOOK_PORT}</code>\n"
            f"🧾 <b>Payment Ref:</b> <code>{html.escape(str(isolated_ref_id))}</code>\n"
            f"💳 <b>Transaction ID:</b> <code>{html.escape(str(isolated_tx_id))}</code>\n"
            f"📲 <b>Method:</b> {html.escape(str(method or 'UPI / Auto'))}\n"
            f"⏱️ <b>Time:</b> {now_str}</blockquote>\n\n"
            f"✅ <i>Access link delivered with anti-leak protection! (Port {WEBHOOK_PORT} only)</i>"
        )
        for aid in get_admin_ids():
            try:
                await bot.send_message(chat_id=aid, text=admin_text, parse_mode=ParseMode.HTML)
                logger.info("Admin %d notified of payment %s", aid, payment_id)
            except Exception as exc:
                logger.error("Failed to notify admin %d: %s", aid, exc)
    except Exception as exc:
        logger.error("Error in _notify_admins_of_sale: %s", exc)


async def process_successful_payment(
    bot,
    order_id: str,
    payment_id: str,
    amount_inr: float | None = None,
    method: str | None = None,
) -> bool:
    """
    Auto-approve payment with zero-latency link delivery:
    1. Per-order lock prevents duplicate delivery if webhook + fast-watcher fire simultaneously
    2. Mark paid & grant access in DB
    3. Deliver protected channel link AND delete QR message concurrently
    4. Notify all admins in a background task so buyer never waits
    """
    lock = _PAYMENT_LOCKS.get(order_id)
    if lock is None:
        lock = asyncio.Lock()
        _PAYMENT_LOCKS[order_id] = lock

    async with lock:
        _ACTIVE_QR_WATCHERS.pop(order_id, None)
        if order_id in _PAID_ORDER_CACHE:
            return True

        payment = await asyncio.to_thread(db.get_payment_by_order, DB_PATH, order_id)
        if not payment:
            logger.warning("No payment record found for order %s", order_id)
            return False

        already_paid = (payment["status"] == "paid")
        user_id = payment["user_id"]
        channel_id = payment["channel_id"]
        amount = amount_inr if amount_inr is not None else payment["amount_inr"]

        if not already_paid:
            await asyncio.to_thread(db.confirm_payment, DB_PATH, order_id, payment_id)
            QR_CACHE.pop((user_id, channel_id), None)
            logger.info(
                "Payment confirmed in DB: order=%s, payment=%s, user=%d, channel=%d",
                order_id, payment_id, user_id, channel_id,
            )
        else:
            _PAID_ORDER_CACHE.add(order_id)
            logger.info("Payment %s already marked as paid.", order_id)
            return True

        _PAID_ORDER_CACHE.add(order_id)
        channel = await db_get_channel(channel_id)

        # Look up QR message to delete (in-memory cache first, DB fallback)
        qr_msg_info = QR_MESSAGE_CACHE.pop(order_id, None) or QR_MESSAGE_CACHE.pop(user_id, None)
        if not qr_msg_info:
            qr_msg_info = (
                await asyncio.to_thread(db.get_qr_message_info, DB_PATH, order_id)
                or await asyncio.to_thread(db.get_latest_qr_message_for_user, DB_PATH, user_id)
            )

        # Deliver channel link to buyer AND delete QR message concurrently for instant delivery!
        tasks = []
        if channel:
            tasks.append(send_protected_channel_link(bot, user_id, channel))
        if qr_msg_info:
            tasks.append(
                _delete_qr_message_safe(bot, qr_msg_info["chat_id"], qr_msg_info["message_id"], user_id)
            )
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Notify admins asynchronously in background so buyer link delivery is never delayed
        ch_name = channel["name"] if channel else f"Channel #{channel_id}"
        asyncio.create_task(
            _notify_admins_of_sale(
                bot, user_id, channel_id, ch_name, amount, order_id, payment_id, method
            )
        )
        return True



async def handle_razorpay_webhook(request: web.Request) -> web.Response:
    """Process incoming Razorpay webhook events with strict bot and port isolation."""
    try:
        body = await request.read()
        sig = request.headers.get("X-Razorpay-Signature", "")

        if RZP_WEBHOOK_SECRET:
            if not pay.verify_webhook_signature(body, sig, RZP_WEBHOOK_SECRET):
                logger.warning("Invalid Razorpay webhook signature from %s", request.remote)
                return web.Response(status=400, text="Invalid signature")

        data = json.loads(body.decode("utf-8"))
        event = data.get("event", "")
        logger.info("Razorpay webhook event received on port %d: %s", WEBHOOK_PORT, event)

        if event in ("order.paid", "payment.captured", "qr_code.credited"):
            payload = data.get("payload", {})
            payment_entity = payload.get("payment", {}).get("entity", {})
            order_entity = payload.get("order", {}).get("entity", {})
            qr_entity = payload.get("qr_code", {}).get("entity", {})

            # Consolidate notes from qr_code, order, and payment entities
            notes = {}
            for ent in (qr_entity, order_entity, payment_entity):
                if ent and isinstance(ent.get("notes"), dict):
                    notes.update(ent["notes"])

            # 1. Port isolation check — ignore events intended for another bot's port
            req_port = str(notes.get("port") or notes.get("webhook_port") or "").strip()
            if req_port and req_port != str(WEBHOOK_PORT):
                logger.info(
                    "Webhook port mismatch: event port is %s, this bot listens on %s. Ignored.",
                    req_port, WEBHOOK_PORT,
                )
                return web.json_response({
                    "status": "ignored",
                    "reason": "different_port",
                    "received_port": req_port,
                    "bot_port": WEBHOOK_PORT,
                })

            # 2. Bot ID / Bot Number isolation check
            req_bot_id = str(notes.get("bot_id") or notes.get("bot_number") or "").strip()
            if req_bot_id and str(BOT_ID) and req_bot_id != str(BOT_ID):
                logger.info(
                    "Webhook bot_id mismatch: event bot_id is %s, this bot is %s. Ignored.",
                    req_bot_id, BOT_ID,
                )
                return web.json_response({
                    "status": "ignored",
                    "reason": "different_bot_id",
                    "received_bot_id": req_bot_id,
                    "this_bot_id": str(BOT_ID),
                })

            # 3. Bot username isolation check
            bot_tag = str(notes.get("bot") or notes.get("bot_username") or "").strip()
            if bot_tag and BOT_USERNAME and bot_tag.lower() != BOT_USERNAME.lower():
                logger.info(
                    "Webhook bot_username mismatch: event bot is '%s', this bot is '%s'. Ignored.",
                    bot_tag, BOT_USERNAME,
                )
                return web.json_response({
                    "status": "ignored",
                    "reason": "different_bot",
                    "received_bot": bot_tag,
                    "this_bot": BOT_USERNAME,
                })

            # 4. Reference ID extraction
            ref_id = (
                payment_entity.get("order_id") or
                qr_entity.get("id") or
                order_entity.get("id") or
                notes.get("qr_id") or
                notes.get("order_id")
            )
            if not ref_id:
                logger.info("Webhook event has no reference ID. Ignored.")
                return web.json_response({"status": "ignored", "reason": "no_ref_id"})

            # 5. Database record verification — ensures QR was generated in THIS bot's database
            payment = await asyncio.to_thread(db.find_payment, DB_PATH, ref_id)
            if not payment:
                logger.info(
                    "Webhook ref %s does not exist in this bot's database (%s). Ignored.",
                    ref_id, DB_PATH,
                )
                return web.json_response({
                    "status": "ignored",
                    "reason": "not_in_db",
                    "ref_id": ref_id,
                })

            payment_id = payment_entity.get("id") or ""
            amount_paise = payment_entity.get("amount") or order_entity.get("amount") or qr_entity.get("payment_amount") or 0
            amount_inr = (amount_paise / 100.0) if amount_paise else None
            method = payment_entity.get("method", "UPI QR Webhook")

            logger.info("Processing verified payment for this bot: ref_id=%s, payment_id=%s, amount=₹%s", ref_id, payment_id, amount_inr)
            bot = request.app["bot"]
            asyncio.create_task(
                process_successful_payment(bot, payment["razorpay_order_id"], payment_id, amount_inr, method)
            )

        return web.json_response({"status": "ok", "bot_id": str(BOT_ID), "port": WEBHOOK_PORT})
    except Exception as exc:
        logger.error("Razorpay webhook error: %s", exc)
        return web.Response(status=500, text=str(exc))


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "online",
        "service": "Razorpay Auto-Approval Webhook",
        "bot_id": BOT_ID,
        "bot_number": str(BOT_ID),
        "bot_username": BOT_USERNAME,
        "port": WEBHOOK_PORT,
    })


async def start_webhook_server(bot) -> web.AppRunner:
    """Start local aiohttp webhook server."""
    web_app = web.Application()
    web_app["bot"] = bot
    web_app.router.add_post("/razorpay-webhook", handle_razorpay_webhook)
    web_app.router.add_post("/razorpay_webhook", handle_razorpay_webhook)
    web_app.router.add_post("/webhook/razorpay", handle_razorpay_webhook)
    web_app.router.add_post("/webhook", handle_razorpay_webhook)
    web_app.router.add_get("/razorpay-webhook", handle_health)
    web_app.router.add_get("/razorpay_webhook", handle_health)
    web_app.router.add_get("/", handle_health)
    web_app.router.add_get("/health", handle_health)

    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WEBHOOK_PORT)
    await site.start()
    logger.info("Razorpay Webhook server running on http://0.0.0.0:%d/razorpay-webhook", WEBHOOK_PORT)
    return runner


async def _watch_single_qr_fast(bot, qr_id: str, amount_inr: float) -> None:
    """
    Dedicated per-QR fast watcher: polls Razorpay every 1.5s for the first 3 minutes
    after a QR code is sent to a user so link delivery happens within ~1 second of payment.
    """
    client = rzp_client()
    try:
        while qr_id in _ACTIVE_QR_WATCHERS and qr_id not in _PAID_ORDER_CACHE:
            refreshed_at = _ACTIVE_QR_WATCHERS.get(qr_id, 0.0)
            age = time.time() - refreshed_at
            if age > 600:
                break
            interval = 1.5 if age < 180 else 3.0
            await asyncio.sleep(interval)

            if qr_id in _PAID_ORDER_CACHE or qr_id not in _ACTIVE_QR_WATCHERS:
                break

            try:
                payments_list = await asyncio.to_thread(pay.fetch_qr_payments, client, qr_id)
                if payments_list:
                    for item in payments_list:
                        if item.get("status") in ("captured", "authorized"):
                            payment_id = item.get("id")
                            method = item.get("method", "UPI QR")
                            logger.info("Fast QR watcher detected paid QR code in %.1fs: %s", age, qr_id)
                            await process_successful_payment(
                                bot, qr_id, payment_id, amount_inr, method
                            )
                            return
            except Exception as exc:
                logger.debug("Fast watcher error for %s: %s", qr_id, exc)
    finally:
        _ACTIVE_QR_WATCHERS.pop(qr_id, None)


def start_qr_fast_watcher(bot, qr_id: str, amount_inr: float) -> None:
    """Start or refresh the 1.5s high-speed watcher for an active QR code."""
    if qr_id in _PAID_ORDER_CACHE:
        return
    already_watching = qr_id in _ACTIVE_QR_WATCHERS
    _ACTIVE_QR_WATCHERS[qr_id] = time.time()
    if not already_watching:
        asyncio.create_task(_watch_single_qr_fast(bot, qr_id, amount_inr))


async def _check_single_pending_payment(bot, client, p, sem: asyncio.Semaphore) -> None:
    """Non-blocking worker for checking a single pending payment off the event loop."""
    ref_id = p["razorpay_order_id"]
    if ref_id in _PAID_ORDER_CACHE or ref_id in _ACTIVE_QR_WATCHERS:
        return
    async with sem:
        try:
            if ref_id.startswith("qr_"):
                payments_list = await asyncio.to_thread(pay.fetch_qr_payments, client, ref_id)
                if payments_list:
                    for item in payments_list:
                        if item.get("status") in ("captured", "authorized"):
                            payment_id = item.get("id")
                            method = item.get("method", "UPI QR")
                            logger.info("Auto-poller detected paid QR code: %s", ref_id)
                            await process_successful_payment(
                                bot,
                                ref_id,
                                payment_id,
                                p["amount_inr"],
                                method,
                            )
                            break
            else:
                order = await asyncio.to_thread(pay.fetch_order, client, ref_id)
                if order.get("status") == "paid" or order.get("amount_paid", 0) >= order.get("amount", 1):
                    payments_list = await asyncio.to_thread(pay.fetch_order_payments, client, ref_id)
                    payment_id = ""
                    method = "Razorpay Poller"
                    if payments_list:
                        for item in payments_list:
                            if item.get("status") in ("captured", "authorized"):
                                payment_id = item.get("id")
                                method = item.get("method", "UPI/Card")
                                break
                        if not payment_id and payments_list:
                            payment_id = payments_list[0].get("id", "")

                    logger.info("Auto-poller detected paid order: %s", ref_id)
                    await process_successful_payment(
                        bot,
                        ref_id,
                        payment_id or f"auto_{ref_id}",
                        p["amount_inr"],
                        method,
                    )
        except Exception as err:
            logger.debug("Poller error checking %s: %s", ref_id, err)


async def poll_pending_payments(bot) -> None:
    """Non-blocking tiered background task checking pending Razorpay QR codes & orders."""
    logger.info("Payment auto-poller worker started (non-blocking tiered mode).")
    sem = asyncio.Semaphore(10)
    cycle = 0
    while True:
        try:
            await asyncio.sleep(3)
            cycle += 1

            # Tier 1: Active recent QRs (last 15 minutes) — checked every 3s
            pending_orders = await asyncio.to_thread(
                db.get_all_pending_payments, DB_PATH, minutes=15, limit=50
            )

            # Tier 2: Older QRs (15 to 90 minutes) — checked once every ~30s + expire >2h stale rows
            if cycle % 10 == 0:
                older_orders = await asyncio.to_thread(
                    db.get_all_pending_payments, DB_PATH, minutes=90, min_age_minutes=15, limit=30
                )
                if older_orders:
                    pending_orders = list(pending_orders) + list(older_orders)
                await asyncio.to_thread(db.expire_old_pending_payments, DB_PATH, 2)

            if not pending_orders:
                continue

            client = rzp_client()
            await asyncio.gather(
                *(_check_single_pending_payment(bot, client, p, sem) for p in pending_orders),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Error in poll_pending_payments: %s", exc)

# ── Channel Link Periodic Health Monitor & Safe Deactivation ─────────────────

# In-memory consecutive failure counter: {channel_id: fail_count}
_channel_consecutive_failures: dict[int, int] = {}
# Positive cache of recently verified working channel links: {channel_id: verified_at_ts}
_LINK_ALIVE_CACHE: dict[int, float] = {}


# In-memory cache of OCR results per Telegram photo file_id: {file_id: (channel_name, media_counts)}
_IMAGE_OCR_CACHE: dict[str, tuple[str, str]] = {}
_OCR_NOISE_WORDS = {
    "media", "links", "files", "voice", "gifs", "music", "members",
    "subscribers", "rec", "4g", "5g", "lte", "volte", "wifi",
}


def parse_channel_header_ocr_text(ocr_text: str) -> tuple[str, str]:
    """
    Parse OCR text from a Telegram channel screenshot header.
    Returns (channel_name, media_counts_str) e.g. ('福利精选中心', '11642 photos, 28905 videos')
    or ('White couple', '730 videos').
    """
    if not ocr_text:
        return "", ""

    clean_lines: list[str] = []
    for raw_line in ocr_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("[Note:") or "ocr.space" in line.lower():
            continue
        # Strip leading back-arrow symbols from mobile header
        line = re.sub(r"^[←⬅<«\-—•*+~|\\/:\s]+", "", line).strip()
        if not line:
            continue
        clean_lines.append(line)

    media_line_idx: Optional[int] = None
    photos_str = ""
    videos_str = ""
    inline_title_prefix = ""

    for idx, line in enumerate(clean_lines):
        m_p = re.search(
            r"(\d[\d,\s.]*[kKmM]?)\s*(?:photos?|p[hH]?[oO0][tT][oO0]s?|pics?)\b"
            r"|\b(?:photos?|pics?)\s*[:\-]?\s*(\d[\d,\s.]*[kKmM]?)",
            line,
            re.IGNORECASE,
        )
        m_v = re.search(
            r"(\d[\d,\s.]*[kKmM]?)\s*(?:videos?|v[iI1l!|]?[dD][eEcC0oO][oO0]?s?|vids?)\b"
            r"|\b(?:videos?|vids?)\s*[:\-]?\s*(\d[\d,\s.]*[kKmM]?)",
            line,
            re.IGNORECASE,
        )
        if m_p or m_v:
            media_line_idx = idx
            if m_p:
                raw_p = m_p.group(1) or m_p.group(2) or ""
                photos_str = re.sub(r"[\s,]+", "", raw_p)
            if m_v:
                raw_v = m_v.group(1) or m_v.group(2) or ""
                videos_str = re.sub(r"[\s,]+", "", raw_v)
            first_pos = min(
                [m.start() for m in (m_p, m_v) if m is not None],
                default=0,
            )
            if first_pos > 1:
                cand_prefix = line[:first_pos].strip(" ,•-—:|")
                if len(cand_prefix) >= 2 and not cand_prefix.isdigit():
                    inline_title_prefix = cand_prefix
            break

    parts: list[str] = []
    if photos_str:
        parts.append(f"{photos_str} photos")
    if videos_str:
        parts.append(f"{videos_str} videos")
    media_counts = ", ".join(parts)

    def _is_valid_title_line(candidate: str) -> bool:
        c = candidate.strip()
        if len(c) < 2:
            return False
        if c.lower() in _OCR_NOISE_WORDS:
            return False
        # Skip pure clock times or battery percentages (e.g. "20:10", "98%")
        if re.match(r"^\d{1,2}:\d{2}(?:\s*[APap][Mm])?$", c):
            return False
        if re.match(r"^\d{1,3}%$", c):
            return False
        if re.match(r"^[\d\s,.:;+\-*/=%]+$", c):
            return False
        return True

    extracted_name = ""
    if media_line_idx is not None and media_line_idx > 0:
        for i in range(media_line_idx - 1, -1, -1):
            if _is_valid_title_line(clean_lines[i]):
                extracted_name = clean_lines[i]
                break

    if not extracted_name and inline_title_prefix and _is_valid_title_line(inline_title_prefix):
        extracted_name = inline_title_prefix

    if not extracted_name and media_line_idx is None:
        for line in clean_lines[:3]:
            if _is_valid_title_line(line):
                extracted_name = line
                break

    return extracted_name[:80].strip(), media_counts


_TESSERACT_CHECKED = False


def _ensure_local_tesseract_sync() -> Optional[str]:
    """Return path to local tesseract binary; auto-install on Ubuntu Linux if missing."""
    global _TESSERACT_CHECKED
    path = shutil.which("tesseract")
    if path:
        return path
    for win_cand in (
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    ):
        if os.path.isfile(win_cand):
            return win_cand

    if not _TESSERACT_CHECKED and sys.platform.startswith("linux"):
        _TESSERACT_CHECKED = True
        try:
            subprocess.run(
                ["sudo", "-n", "apt-get", "update", "-qq"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
            )
            subprocess.run(
                ["sudo", "-n", "apt-get", "install", "-y", "-qq", "tesseract-ocr"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=60,
            )
        except Exception:
            pass
        return shutil.which("tesseract")

    _TESSERACT_CHECKED = True
    return None


def _run_local_tesseract_sync(raw_bytes: bytes) -> str:
    """Run local tesseract CLI on enhanced dark/light full image variants (offline, zero rate limits)."""
    tess_bin = _ensure_local_tesseract_sync()
    if not tess_bin:
        return ""
    try:
        im = Image.open(io.BytesIO(raw_bytes)).convert("L")
        w, h = im.size
        if max(w, h) > 1200:
            scale = 1200.0 / max(w, h)
            im_up = im.resize((max(1, int(w * scale)), max(1, int(h * scale))), Image.LANCZOS)
        elif max(w, h) < 700:
            im_up = im.resize((w * 2, h * 2), Image.LANCZOS)
        else:
            im_up = im
        # Invert dark-mode screenshot (white/grey text on dark background -> dark text on light background)
        im_inv = ImageOps.autocontrast(ImageOps.invert(im_up))

        collected: list[str] = []
        for variant in (im_inv, ImageOps.autocontrast(im_up)):
            buf = io.BytesIO()
            variant.save(buf, format="PNG")
            proc = subprocess.run(
                [tess_bin, "stdin", "stdout", "--psm", "6"],
                input=buf.getvalue(),
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=12,
            )
            txt = proc.stdout.decode("utf-8", errors="ignore").strip()
            if txt:
                collected.append(txt)
                _, media = parse_channel_header_ocr_text(txt)
                if media:
                    break
        return "\n".join(collected)
    except Exception:
        return ""


async def _ocr_image_bytes(img_bytes: bytes) -> str:
    """Extract text from image bytes using local Tesseract first, then OCR.space HTTP API fallback."""
    local_text = await asyncio.to_thread(_run_local_tesseract_sync, img_bytes)
    if local_text:
        _, local_media = parse_channel_header_ocr_text(local_text)
        if local_media:
            return local_text

    b64 = "data:image/jpeg;base64," + base64.b64encode(img_bytes).decode("ascii")
    api_key = os.getenv("OCR_SPACE_API_KEY", "helloworld").strip() or "helloworld"
    for engine in ("2", "1"):
        try:
            resp = await _ASYNC_HTTP_CLIENT.post(
                "https://api.ocr.space/parse/image",
                data={
                    "apikey": api_key,
                    "base64Image": b64,
                    "OCREngine": engine,
                },
                timeout=18.0,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            if data.get("IsErroredOnProcessing"):
                continue
            results = data.get("ParsedResults") or []
            if results and isinstance(results, list):
                remote_txt = str(results[0].get("ParsedText") or "").strip()
                if remote_txt:
                    return f"{remote_txt}\n{local_text}".strip() if local_text else remote_txt
        except Exception:
            continue
    return local_text


def _prepare_image_crops_for_ocr(raw_bytes: bytes) -> list[bytes]:
    """
    Prepare full image JPEG payload for OCR (keeping the entire screenshot so photo/video
    counts in the middle of Channel Info screens are never cropped out).
    """
    im = Image.open(io.BytesIO(raw_bytes)).convert("RGB")
    w, h = im.size

    if max(w, h) > 1400:
        scale = 1400.0 / max(w, h)
        im = im.resize((max(1, int(w * scale)), max(1, int(h * scale))), Image.LANCZOS)

    buf_full = io.BytesIO()
    im.save(buf_full, format="JPEG", quality=88)
    return [buf_full.getvalue()]


async def extract_channel_metadata_from_file_id(bot, photo_file_id: str) -> tuple[str, str]:
    """
    Download Telegram photo by file_id and extract (channel_name, media_counts) via OCR.
    Returns ('', '') if photo cannot be processed.
    """
    if not bot or not photo_file_id:
        return "", ""
    clean_fid = str(photo_file_id).split("-100")[0].strip()
    if not clean_fid.startswith("AgAC"):
        return "", ""

    if clean_fid in _IMAGE_OCR_CACHE:
        return _IMAGE_OCR_CACHE[clean_fid]

    try:
        tg_file = await bot.get_file(clean_fid)
        raw_bytearray = await tg_file.download_as_bytearray()
        raw_bytes = bytes(raw_bytearray)
        payloads = await asyncio.to_thread(_prepare_image_crops_for_ocr, raw_bytes)

        best_name = ""
        best_media = ""
        for payload in payloads:
            ocr_text = await _ocr_image_bytes(payload)
            name, media = parse_channel_header_ocr_text(ocr_text)
            if name and not best_name:
                best_name = name
            if media and not best_media:
                best_media = media
            if best_name and best_media:
                break

        if best_name or best_media:
            _IMAGE_OCR_CACHE[clean_fid] = (best_name, best_media)
            logger.info(
                "[IMAGE OCR] Extracted from %s -> name=%r, media=%r",
                clean_fid[:24], best_name, best_media,
            )
        return best_name, best_media
    except Exception as exc:
        logger.warning("[IMAGE OCR] Could not extract metadata from %s: %s", clean_fid[:24], exc)
        return "", ""


def extract_clean_title_from_alive_detail(detail: str) -> str:
    """Extract clean channel title from check_channel_link_alive detail string."""
    if not detail:
        return ""
    cleaned = re.sub(
        r"\s*\((?:\d[\d\s,]*\s*(?:subscribers?|members?|online)[^)]*|18\+\s*Sensitive\s*Content|Active)\)\s*$",
        "",
        detail,
        flags=re.I,
    ).strip()
    if cleaned.lower() in (
        "active", "active (invite link valid)", "active (telegram invite page)",
        "join group chat on telegram", "telegram", "18+", "sensitive content",
    ) or cleaned.lower().startswith("active ("):
        return ""
    return cleaned


_GENERIC_TG_PAGE_TITLES = {
    "not available",
    "unavailable",
    "join group chat on telegram",
    "telegram",
    "channel not found",
    "group not found",
}

_ADULT_RESTRICTION_REASONS = {"porno", "porn", "sensitive", "adult", "nsfw", "18+", "age"}

_MTPROTO_CLIENT = None
_MTPROTO_INIT_ATTEMPTED = False


def _normalize_tg_html_text(raw_html: str) -> str:
    """Unescape HTML entities, normalize curly apostrophes and collapse whitespace to lowercase."""
    if not raw_html:
        return ""
    unescaped = html.unescape(raw_html)
    norm = re.sub(r"[\u2018\u2019`´]", "'", unescaped)
    return re.sub(r"\s+", " ", norm).lower()


def _is_telegram_adult_sensitive_notice(raw_html: str) -> bool:
    """
    Detect Telegram's 18+ / sensitive / adult content warning or filter notice
    (e.g. '18+ This chat may contain sensitive content suitable only for adults.
    Do you still want to view it? Always show 18+ media / View Anyway', or
    web-preview 'used to spread pornographic content').
    Channels showing these notices ARE ACTIVE/LIVE — never expired or ToS-banned.
    """
    norm = _normalize_tg_html_text(raw_html)
    if not norm:
        return False

    direct_phrases = (
        "may contain sensitive content",
        "sensitive content suitable only for adults",
        "suitable only for adults",
        "always show 18+ media",
        "show 18+ media",
        "contains sensitive content",
        "marked as sensitive",
        "spread pornographic content",
        "pornographic content",
        "contains pornographic",
        "contains adult content",
        "adult-only content",
        "adults-only content",
        "nsfw content",
    )
    if any(p in norm for p in direct_phrases):
        return True

    # Combined signals from Telegram's 18+ / sensitive modal or preview gates
    if "18+" in norm and any(k in norm for k in ("sensitive", "adults", "view anyway", "media", "age-restricted", "age restricted")):
        return True
    if "sensitive content" in norm and any(k in norm for k in ("this chat", "this channel", "this group", "adults", "view it", "view anyway", "can't be displayed", "cannot be displayed")):
        return True
    if ("adult content" in norm or "age-restricted" in norm or "age restricted" in norm) and any(
        k in norm for k in ("this chat", "this channel", "this group", "can't be displayed", "cannot be displayed", "18+", "view anyway")
    ):
        return True

    return False


def _detect_telegram_ban_reason(raw_html: str, *, allow_web_porn_notice: bool = True) -> str:
    """
    Detect Telegram Terms of Service bans, copyright/law blocks, and unavailable notices
    after unescaping HTML entities (e.g. &#39;, &rsquo;) and normalizing apostrophes.
    Explicitly treats 18+ / sensitive / pornographic content warnings as ACTIVE (returns '').
    Returns a human-readable dead reason string, or '' if no ban notice is detected.
    """
    norm = _normalize_tg_html_text(raw_html)
    if not norm:
        return ""

    # 1. Hard Terms of Service / permanent bans (always dead on any surface)
    if (
        "violated telegram's terms of service" in norm
        or "violated telegrams terms of service" in norm
        or "violated telegram" in norm
        or (
            "terms of service" in norm
            and any(k in norm for k in ("can't be displayed", "cannot be displayed", "not available", "violated", "blocked"))
        )
    ):
        return "Not available: Violated Telegram's Terms of Service"

    if "copyright infringement" in norm or "violated copyright" in norm:
        return "Not available: Blocked for copyright infringement"

    if "because it was blocked" in norm or "channel has been blocked" in norm or "group has been blocked" in norm:
        return "Channel is blocked/banned by Telegram"

    if "channel with username" in norm and "not found" in norm:
        return "Channel not found"

    # 2. Live 18+ / sensitive / adult content gates are NEVER considered expired or banned
    if allow_web_porn_notice and _is_telegram_adult_sensitive_notice(raw_html):
        return ""

    if "violated local laws" in norm or "unavailable in your country" in norm or "restricted in your region" in norm:
        return "Not available: Blocked due to local laws"

    if any(
        phrase in norm
        for phrase in (
            "channel is inaccessible",
            "group is inaccessible",
            "chat is inaccessible",
            "channel is unavailable",
            "chat is unavailable",
            "this channel is not available",
            "this chat is not available",
            "this group is not available",
        )
    ):
        return "Channel is not available / inaccessible"

    # 3. Generic channel/group "can't be displayed" notices (only reached if NOT an 18+/sensitive notice above)
    if any(
        phrase in norm
        for phrase in (
            "this channel can't be displayed",
            "this channel cannot be displayed",
            "this group can't be displayed",
            "this group cannot be displayed",
            "this chat can't be displayed",
            "this chat cannot be displayed",
        )
    ):
        return "Not available: Channel can't be displayed (Banned/Restricted)"

    return ""


async def _check_link_via_mtproto_optional(
    clean_link: str, is_invite: bool, public_slug: str
) -> Optional[tuple[bool, str]]:
    """
    Optional deep MTProto check via Telethon (active only if TG_API_ID, TG_API_HASH,
    and TG_STRING_SESSION are set in .env). Detects private-invite restriction_reason
    (e.g. 'This channel can't be displayed because it violated Telegram's Terms of Service.').
    Explicitly treats 18+ / sensitive / pornographic restrictions as ACTIVE.
    Returns (is_alive, reason) if MTProto answered definitively, or None to use HTTP check.
    """
    global _MTPROTO_CLIENT, _MTPROTO_INIT_ATTEMPTED
    api_id_raw = os.getenv("TG_API_ID", "").strip()
    api_hash = os.getenv("TG_API_HASH", "").strip()
    str_session = os.getenv("TG_STRING_SESSION", "").strip()
    if not (api_id_raw.isdigit() and api_hash and str_session):
        return None

    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from telethon.tl.functions.messages import CheckChatInviteRequest
        from telethon.tl.functions.contacts import ResolveUsernameRequest
        from telethon.errors import (
            InviteHashExpiredError,
            InviteHashInvalidError,
            ChannelPrivateError,
            ChannelInvalidError,
            UsernameInvalidError,
            UsernameNotOccupiedError,
        )
    except Exception:
        return None

    try:
        if _MTPROTO_CLIENT is None and not _MTPROTO_INIT_ATTEMPTED:
            _MTPROTO_INIT_ATTEMPTED = True
            client = TelegramClient(StringSession(str_session), int(api_id_raw), api_hash)
            await client.connect()
            if await client.is_user_authorized():
                _MTPROTO_CLIENT = client
            else:
                await client.disconnect()
                return None

        if _MTPROTO_CLIENT is None:
            return None
        if not _MTPROTO_CLIENT.is_connected():
            await _MTPROTO_CLIENT.connect()

        def _check_restrictions(obj) -> tuple[Optional[str], bool]:
            """Returns (hard_ban_msg_or_None, is_adult_sensitive_flag)."""
            is_adult_flagged = False
            for target in (obj, getattr(obj, "chat", None)):
                if target is None:
                    continue
                reasons = getattr(target, "restriction_reason", None) or []
                for rr in reasons:
                    r_platform = str(getattr(rr, "platform", "") or "").lower()
                    r_reason = str(getattr(rr, "reason", "") or "").lower()
                    r_text = str(getattr(rr, "text", "") or "").strip()
                    ban_msg = _detect_telegram_ban_reason(r_text, allow_web_porn_notice=True)
                    if ban_msg:
                        return ban_msg, False
                    if r_reason in _ADULT_RESTRICTION_REASONS or _is_telegram_adult_sensitive_notice(r_text):
                        is_adult_flagged = True
                        continue
                    if r_platform == "all" and r_reason in ("terms", "tos", "copyright", "law", "illegal", "spam", "violence"):
                        return f"Not available: {r_text or f'Restricted ({r_reason})'}", False
            return None, is_adult_flagged

        if is_invite:
            if "/+" in clean_link:
                inv_hash = clean_link.split("/+", 1)[1].split("?", 1)[0].strip("/")
            else:
                inv_hash = re.split(r"joinchat/", clean_link, maxsplit=1, flags=re.IGNORECASE)[1].split("?", 1)[0].strip("/")
            try:
                res = await _MTPROTO_CLIENT(CheckChatInviteRequest(inv_hash))
            except (InviteHashExpiredError, InviteHashInvalidError, ChannelPrivateError, ChannelInvalidError):
                return False, "Invite link is expired, banned, or invalid"
            ban_msg, is_adult_flagged = _check_restrictions(res)
            if ban_msg:
                return False, ban_msg
            title = getattr(res, "title", None) or getattr(getattr(res, "chat", None), "title", None) or "Active"
            count = getattr(res, "participants_count", None)
            if count:
                return True, f"{title} ({count} members)"
            if is_adult_flagged:
                return True, f"{title} (18+ Sensitive Content)"
            return True, str(title)
        elif public_slug:
            try:
                res = await _MTPROTO_CLIENT(ResolveUsernameRequest(public_slug))
            except (UsernameInvalidError, UsernameNotOccupiedError, ChannelPrivateError, ChannelInvalidError):
                return False, "Channel not found or banned"
            adult_title = ""
            for ch_obj in getattr(res, "chats", None) or []:
                ban_msg, is_adult_flagged = _check_restrictions(ch_obj)
                if ban_msg:
                    return False, ban_msg
                if is_adult_flagged:
                    adult_title = getattr(ch_obj, "title", None) or f"@{public_slug}"
            if adult_title:
                return True, f"{adult_title} (18+ Sensitive Content)"
            return None
    except Exception as exc:
        logger.debug("[MTPROTO LINK CHECK] Skipped for %s: %s", clean_link, exc)
    return None


async def check_channel_link_alive(link: str, bot=None, chat_id=None) -> tuple[bool, str]:
    """
    Check if a Telegram channel link is active and valid.
    Detects expired/revoked links, deleted channels, and Terms of Service bans
    ('Not available — This channel can't be displayed because it violated Telegram's Terms of Service.').
    Explicitly treats 18+ / sensitive content warning gates ('This chat may contain sensitive content
    suitable only for adults', 'Always show 18+ media', web NSFW/pornographic preview notices) as ACTIVE.
    Returns (is_alive: bool, detail_or_reason: str).
    Never deletes any channel messages or deactivates database records.
    """
    # 1. Normalize the link (preserving exact case of invite hashes)
    clean_link = (link or "").strip()
    if not clean_link and not (bot and chat_id):
        return False, "Empty channel link"

    if clean_link:
        if clean_link.startswith("@"):
            clean_link = f"https://t.me/{clean_link.lstrip('@')}"
        elif not clean_link.startswith(("http://", "https://")):
            if clean_link.lower().startswith(("t.me/", "telegram.me/")):
                clean_link = f"https://t.me/{clean_link.split('/', 1)[1]}"
            else:
                clean_link = f"https://t.me/{clean_link.lstrip('@')}"
        else:
            clean_link = re.sub(
                r"^https?://(?:www\.)?(?:t\.me|telegram\.me)/",
                "https://t.me/",
                clean_link,
                flags=re.IGNORECASE,
            )
        # Normalize preview URLs like https://t.me/s/username -> https://t.me/username
        clean_link = re.sub(r"^https://t\.me/s/([A-Za-z0-9_]{4,})(?:[/?].*)?$", r"https://t.me/\1", clean_link, flags=re.IGNORECASE)

    is_invite = "/+" in clean_link or "joinchat/" in clean_link.lower()
    public_slug = ""
    if not is_invite and "t.me/" in clean_link:
        tail = clean_link.split("t.me/", 1)[1].split("?", 1)[0].strip("/")
        first_seg = tail.split("/", 1)[0].strip()
        if first_seg and not first_seg.startswith("+") and first_seg.lower() not in ("c", "s", "addlist", "joinchat", "proxy", "socks"):
            public_slug = first_seg.lstrip("@")
            clean_link = f"https://t.me/{public_slug}"

    # 2. Bot API check (do NOT return True early when clean_link is available, because Telegram
    #    Bot API getChat still returns ok=true for channels banned for Terms of Service!)
    api_title = ""
    if bot and chat_id:
        try:
            try:
                target_chat_id = int(chat_id)
            except (ValueError, TypeError):
                target_chat_id = chat_id
            chat = await bot.get_chat(target_chat_id)
            api_title = chat.title or ""
            if not clean_link:
                return True, api_title or "Active"
        except Exception as exc:
            err_msg = str(exc).lower()
            if (
                not _is_telegram_adult_sensitive_notice(err_msg)
                and any(k in err_msg for k in ("chat not found", "deactivated", "channel_banned", "chat_restricted", "terms of service", "violated"))
            ):
                return False, f"Telegram API confirmed banned/unavailable: {exc}"

    if bot and public_slug:
        try:
            chat = await bot.get_chat(f"@{public_slug}")
            api_title = api_title or (chat.title or "")
        except Exception as exc:
            err_msg = str(exc).lower()
            if (
                not _is_telegram_adult_sensitive_notice(err_msg)
                and any(k in err_msg for k in ("deactivated", "channel_banned", "chat_restricted", "terms of service", "violated"))
            ):
                return False, f"Channel banned/deactivated ({exc})"

    # 3. Optional MTProto check (if user session is configured in .env)
    mtproto_res = await _check_link_via_mtproto_optional(clean_link, is_invite, public_slug)
    if mtproto_res is not None:
        return mtproto_res

    # 4. For public channels, check Telegram's ?embed=1 widget endpoint FIRST (with retry) because it explicitly
    #    distinguishes:
    #      - Terms of Service / copyright / law bans -> non-empty ban text in tgme_widget_message_error (DEAD)
    #      - Nonexistent / deleted username          -> "Channel with username @... not found" (DEAD)
    #      - Live 18+ / sensitive-content channels   -> 200 OK with NO ban reason (e.g. empty error div on @oiMALIBOGonly,
    #                                                   pornographic/sensitive notice, "Post not found", or live data-peer) (ACTIVE)
    embed_ban_reason = ""
    embed_confirmed_adult = False
    embed_confirmed_live_peer = False
    embed_empty_sensitive_gate = False
    embed_post_not_found = False
    embed_checked_clean = False
    embed_transient_error = False
    embed_status_code = None
    embed_err_snippet = ""
    if public_slug:
        for emb_attempt in range(2):
            try:
                emb_resp = await _tg_web_get(f"https://t.me/{public_slug}/1?embed=1")
                embed_status_code = emb_resp.status_code
                if emb_resp.status_code in (429, 500, 502, 503, 504):
                    if emb_attempt == 0:
                        await asyncio.sleep(1.2)
                        continue
                    embed_transient_error = True
                    break
                if emb_resp.status_code == 200:
                    emb_text = emb_resp.text
                    m_emb_err = re.search(
                        r'class=["\'][^"\']*\btgme_widget_message_error\b[^"\']*["\'][^>]*>(.*?)</div>',
                        emb_text,
                        re.S,
                    )
                    emb_err_txt = (
                        html.unescape(re.sub(r"<[^>]+>", "", m_emb_err.group(1)).strip())
                        if m_emb_err is not None
                        else None
                    )
                    embed_err_snippet = (emb_err_txt or "")[:140]

                    embed_ban_reason = _detect_telegram_ban_reason(emb_text, allow_web_porn_notice=True)
                    if embed_ban_reason:
                        logger.info(
                            "[LINK CHECK] DEAD %s via embed -> %s (emb_err=%r)",
                            clean_link, embed_ban_reason, embed_err_snippet,
                        )
                        return False, embed_ban_reason

                    embed_checked_clean = True
                    if _is_telegram_adult_sensitive_notice(emb_text):
                        embed_confirmed_adult = True
                    elif m_emb_err is not None:
                        if emb_err_txt == "":
                            # Telegram suppresses embed text with an empty error div on live 18+/sensitive channels
                            embed_empty_sensitive_gate = True
                        elif emb_err_txt.lower() in ("post not found", "message not found"):
                            # Channel exists (not ToS-banned, not "channel not found"), only post #1 was deleted
                            embed_post_not_found = True
                    elif (
                        re.search(r'data-peer=["\']c\d+', emb_text)
                        or "please open telegram to view this post" in emb_text.lower()
                    ):
                        embed_confirmed_live_peer = True
                break
            except Exception as exc:
                if emb_attempt == 0:
                    await asyncio.sleep(1.0)
                    continue
                embed_transient_error = True
                logger.debug("[LINK CHECK] Embed check transient failure for @%s: %s", public_slug, exc)

    # 5. Check the web preview of the link on t.me (cookie-less to prevent stel_ssid soft-throttling)
    global _LAST_CONFIRMED_LIVE_INVITE

    def _extract_tme_title_and_extra(page_html: str) -> tuple[str, str]:
        m_t = re.search(
            r'class=["\'][^"\']*\btgme_page_title\b[^"\']*["\'][^>]*>\s*(?:<span[^>]*>)?(.*?)(?:</span>)?\s*</div>',
            page_html,
            re.S,
        )
        t_val = html.unescape(re.sub(r"<[^>]+>", "", m_t.group(1)).strip()) if m_t else ""
        if not t_val:
            m_og = re.search(
                r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
                page_html,
                re.I,
            )
            if m_og:
                t_val = html.unescape(m_og.group(1).strip())
        m_e = re.search(
            r'class=["\'][^"\']*\btgme_page_extra\b[^"\']*["\'][^>]*>(.*?)</div>',
            page_html,
            re.S,
        )
        e_val = html.unescape(re.sub(r"<[^>]+>", "", m_e.group(1)).strip()) if m_e else ""
        return t_val, e_val

    for attempt in range(2):
        try:
            resp = await _tg_web_get(clean_link)
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt == 0:
                    await asyncio.sleep(1.5)
                    continue
                return False, f"Telegram rate limit / server busy (HTTP {resp.status_code})"

            if resp.status_code == 404:
                return False, "Link not found (HTTP 404)"

            if resp.status_code != 200:
                return False, f"Unexpected HTTP status {resp.status_code}"

            # If Telegram redirected an invalid username link away from t.me to telegram.org homepage
            final_host = (resp.url.host or "").lower()
            if final_host and "telegram.org" in final_host and "t.me" not in final_host:
                return False, "Invalid or deleted channel (redirected to telegram.org)"

            text = resp.text

            # Check for Terms of Service ban, copyright/law blocks, and unavailable notices (entity-decoded).
            # 18+ / sensitive / pornographic content notices are explicitly allowed and treated as ACTIVE.
            ban_reason = _detect_telegram_ban_reason(text, allow_web_porn_notice=True)
            if ban_reason:
                logger.info("[LINK CHECK] DEAD %s via main page -> %s", clean_link, ban_reason)
                return False, ban_reason

            is_adult_page = _is_telegram_adult_sensitive_notice(text)

            # Extract channel title (tgme_page_title or og:title) and subscriber/member count from t.me preview
            title_str, extra_str = _extract_tme_title_and_extra(text)

            # Reject error / placeholder titles such as "Not available" or "Join group chat on Telegram"
            if title_str and (
                title_str.lower() in _GENERIC_TG_PAGE_TITLES
                or title_str.lower().startswith("telegram: contact @")
            ):
                if (
                    not (is_adult_page or embed_confirmed_adult or embed_empty_sensitive_gate)
                    and ("not available" in title_str.lower() or "unavailable" in title_str.lower())
                ):
                    return False, "Not available (Channel banned or restricted)"
                title_str = ""

            if title_str or extra_str:
                if is_invite:
                    _LAST_CONFIRMED_LIVE_INVITE = clean_link
                display_title = title_str or api_title
                if display_title and extra_str:
                    return True, f"{display_title} ({extra_str})"
                if display_title:
                    if is_adult_page or embed_confirmed_adult or embed_empty_sensitive_gate:
                        return True, f"{display_title} (18+ Sensitive Content)"
                    return True, display_title
                return True, f"Active ({extra_str})"

            # Check if t.me/<slug> confirms an existing public channel stub ("View in Telegram" / "Preview channel")
            text_low = text.lower()
            main_is_channel_stub = bool(public_slug) and (
                'class="tgme_page_context_link"' in text
                or "preview channel" in text_low
                or "view in telegram" in text_low
                or "you can view posts" in text_low
            )

            # Live 18+ / sensitive content channel where Telegram hides public title/subscriber preview
            # (e.g. https://t.me/oiMALIBOGonly -> "18+ sensitive content" modal in app, clean/empty embed on web)
            if (
                is_adult_page
                or embed_confirmed_adult
                or (embed_empty_sensitive_gate and main_is_channel_stub)
                or (embed_checked_clean and main_is_channel_stub)
            ):
                display = api_title or (f"@{public_slug}" if public_slug else "Active")
                return True, f"{display} (18+ Sensitive Content)"

            # Public channel confirmed live by embed widget peer data or "Post not found" on an existing channel stub
            if embed_confirmed_live_peer or (embed_post_not_found and main_is_channel_stub):
                display = api_title or (f"@{public_slug}" if public_slug else "Active")
                return True, f"{display} (Active)"

            # If embed check suffered a transient network/rate-limit error on a real channel stub,
            # trust Bot API confirmation if available, otherwise report transient error (never falsely mark banned!)
            if embed_transient_error and main_is_channel_stub:
                if api_title:
                    return True, f"{api_title} (18+ Sensitive Content)"
                return False, "Telegram rate limit / connection error (embed check busy)"

            # For private invite links returning the generic placeholder page ("Join group chat on Telegram"):
            # Retry once after brief pause on a fresh cookie-less connection, then verify against a known-live
            # invite canary to distinguish real expiry from Telegram IP soft-throttling!
            if is_invite:
                if attempt == 0:
                    await asyncio.sleep(1.5)
                    continue

                canary_url = _LAST_CONFIRMED_LIVE_INVITE
                if canary_url and canary_url != clean_link:
                    try:
                        c_resp = await _tg_web_get(canary_url)
                        if c_resp.status_code == 200:
                            c_title, c_extra = _extract_tme_title_and_extra(c_resp.text)
                            if c_title and c_title.lower() in _GENERIC_TG_PAGE_TITLES:
                                c_title = ""
                            if not c_title and not c_extra:
                                logger.warning(
                                    "[LINK CHECK] Soft-throttle detected on t.me (live canary %s also returned generic page) while checking %s",
                                    canary_url, clean_link,
                                )
                                return False, "Telegram rate limit (t.me temporarily throttling previews — retry in 1–2 min)"
                    except Exception:
                        pass

                logger.info(
                    "[LINK CHECK] DEAD %s -> no preview metadata (is_invite=True, api_title=%r)",
                    clean_link, api_title,
                )
                return False, "Invite link is expired, banned, or invalid"

            logger.info(
                "[LINK CHECK] DEAD %s -> no preview metadata (is_invite=False, api_title=%r, emb_status=%s, emb_err=%r, stub=%s)",
                clean_link, api_title, embed_status_code, embed_err_snippet, main_is_channel_stub,
            )
            return False, embed_ban_reason or "Channel not available, banned, or not found"

        except Exception as exc:
            if attempt == 0:
                await asyncio.sleep(1.0)
                continue
            logger.warning("[LINK CHECK] Network exception for %s: %s", clean_link, exc)
            return False, f"Connection error ({exc})"

    return False, "Could not verify link"


async def cleanup_dead_channel(bot, channel: dict, reason: str) -> dict:
    """
    Directly delete published promotional post(s) for an expired/banned channel from
    the Main Channel (preserving post metadata & photo_file_id in DB for reactivation)
    and deactivate the channel so it is removed from /menu.
    """
    cid = channel["id"]
    ch_name = (channel.get("name") or f"Channel #{cid}").strip()
    link = (channel.get("link") or "").strip()

    default_main = await asyncio.to_thread(db.get_setting, DB_PATH, "main_channel_id") or MAIN_CHANNEL_ID
    posts = await asyncio.to_thread(db.get_posts_for_channel_exact, DB_PATH, cid, link)
    msg_posts = [p for p in posts if p["channel_msg_id"]]

    deleted_msg_ids: list[int] = []
    already_gone_ids: list[int] = []
    delete_errors: list[str] = []
    attempted_targets: set[tuple[str, int]] = set()

    for p in msg_posts:
        msg_id = int(p["channel_msg_id"])
        target = (p["target_channel"] or "").strip() or default_main
        if not target:
            delete_errors.append(f"Msg #{msg_id}: no Main Channel configured")
            continue

        try:
            target_chat_id = int(target)
        except (ValueError, TypeError):
            target_chat_id = target

        dedup_key = (str(target_chat_id), msg_id)
        if dedup_key in attempted_targets:
            await asyncio.to_thread(db.clear_post_msg_id, DB_PATH, p["id"])
            continue
        attempted_targets.add(dedup_key)

        try:
            await bot.delete_message(chat_id=target_chat_id, message_id=msg_id)
            deleted_msg_ids.append(msg_id)
            await asyncio.to_thread(db.clear_post_msg_id, DB_PATH, p["id"])
            logger.info(
                "[AUTO-CLEANUP] Deleted Main Channel post #%d (chat=%s) for dead channel #%d (%s).",
                msg_id, target_chat_id, cid, ch_name,
            )
        except BadRequest as exc:
            err_low = str(exc).lower()
            if (
                "message to delete not found" in err_low
                or "message_id_invalid" in err_low
                or "message identifier is not specified" in err_low
            ):
                already_gone_ids.append(msg_id)
                await asyncio.to_thread(db.clear_post_msg_id, DB_PATH, p["id"])
                logger.info(
                    "[AUTO-CLEANUP] Post #%d for channel #%d already absent from Telegram; cleared msg_id in DB.",
                    msg_id, cid,
                )
            else:
                delete_errors.append(f"Msg #{msg_id}: {exc}")
                logger.warning(
                    "[AUTO-CLEANUP] Could not delete post #%d (chat=%s) for channel #%d: %s",
                    msg_id, target_chat_id, cid, exc,
                )
        except Exception as exc:
            delete_errors.append(f"Msg #{msg_id}: {exc}")
            logger.warning(
                "[AUTO-CLEANUP] Error deleting post #%d (chat=%s) for channel #%d: %s",
                msg_id, target_chat_id, cid, exc,
            )

    # Deactivate channel in DB (preserves channel record, payments, user access & post photo_file_id)
    await asyncio.to_thread(db.deactivate_channel, DB_PATH, cid, link)
    invalidate_channel_cache()
    _MONITOR_NOTIFIED_DEAD.pop(cid, None)
    logger.warning(
        "[AUTO-CLEANUP] Deactivated dead channel #%d (%s) — reason: %s | deleted_posts=%s",
        cid, ch_name, reason, deleted_msg_ids,
    )

    return {
        "deleted_count": len(deleted_msg_ids),
        "deleted_msg_ids": deleted_msg_ids,
        "already_gone_ids": already_gone_ids,
        "had_posts": len(msg_posts),
        "delete_errors": delete_errors,
    }


async def republish_channel_post(
    bot,
    channel_id: int,
    custom_photo_file_id: Optional[str] = None,
    custom_media_type: Optional[str] = None,
    allow_text_only: bool = False,
) -> tuple[bool, str, Optional[int]]:
    """
    Reactivates a channel (keeping the same channel_id so all previous buyers retain access
    and never see a QR code again) and re-publishes its promotional post (with its original
    deleted photo/video, price, and buy button) to the Main Channel.

    Returns (success: bool, status_or_error: str, message_id: Optional[int]).
    If no saved photo/video is found and allow_text_only is False, returns (False, "NEED_MEDIA", None).
    """
    channel = await asyncio.to_thread(db.get_channel, DB_PATH, channel_id)
    if not channel:
        return False, "Channel not found in database.", None

    # 1. Reactivate channel in DB so existing buyers and new buyers can access it
    await asyncio.to_thread(db.update_channel, DB_PATH, channel_id, is_active=1)
    invalidate_channel_cache()
    _MONITOR_NOTIFIED_DEAD.pop(channel_id, None)

    # 2. Fetch latest post record for this channel (or recover from SQLite raw bytes if previously deleted)
    post = await asyncio.to_thread(
        db.get_latest_post_for_channel, DB_PATH, channel_id, row_get(channel, "link", ""), channel["name"]
    )
    if not post or not post["photo_file_id"]:
        await asyncio.to_thread(db.recover_deleted_posts_from_sqlite_bytes, DB_PATH)
        post = await asyncio.to_thread(
            db.get_latest_post_for_channel, DB_PATH, channel_id, row_get(channel, "link", ""), channel["name"]
        )

    photo_file_id = custom_photo_file_id or (post["photo_file_id"] if post else None)
    if not photo_file_id and not allow_text_only:
        return False, "NEED_MEDIA", None

    default_main = await asyncio.to_thread(db.get_setting, DB_PATH, "main_channel_id") or MAIN_CHANNEL_ID
    target = (post["target_channel"] if post and post["target_channel"] else None) or default_main
    if not target:
        return False, "No Main Channel configured. Use /setmainchannel first.", None

    price = (post["price_inr"] if post and post["price_inr"] is not None else channel["price_inr"]) or 0.0
    if price <= 0 and channel["price_inr"] > 0:
        price = channel["price_inr"]
    cross_price = row_get(post, "cross_price_inr") if post else None
    custom_text = (post["caption"] if post and post["caption"] else "") or ""
    is_free = price <= 0
    ch_name = channel["name"]

    # 3. Ensure a post entry exists in DB linked to this exact channel_id
    if post:
        post_id = post["id"]
        if custom_photo_file_id:
            conn = db.get_connection(DB_PATH)
            conn.execute("UPDATE posts SET photo_file_id = ? WHERE id = ?", (custom_photo_file_id, post_id))
            conn.commit()
    else:
        admin_id = row_get(channel, "created_by") or (ADMIN_IDS[0] if ADMIN_IDS else 0)
        post_id = await asyncio.to_thread(
            db.create_post,
            DB_PATH,
            admin_id,
            channel_id,
            ch_name,
            custom_text,
            photo_file_id,
            price,
            target,
            None,
            cross_price,
        )

    # 4. Build caption & inline keyboard pointing to buy_{post_id} (linked to same channel_id!)
    bot_username = BOT_USERNAME
    if is_free:
        header_block = (
            f"<blockquote><b>🎁 EXCLUSIVE FREE VIP CHANNEL ACCESS 🎁</b></blockquote>\n"
            + (f"<blockquote>{custom_text}</blockquote>\n" if custom_text else "")
        )
        price_line = "💰 <b>Access Fee:</b> <b>FREE (₹0) 🎁</b>\n"
        payment_mode_line = "⚡ <b>Instant Access:</b> Zero Payment • 100% Free Join"
        action_line = "👇 <b>Tap the button below to join for free:</b>"
        channel_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Join Free Channel Now 🔓", url=channel["link"])]
        ])
    else:
        header_block = (
            f"<blockquote><b>🔥 EXCLUSIVE VIP CHANNEL ACCESS 🔥</b></blockquote>\n"
            + (f"<blockquote>{custom_text}</blockquote>\n" if custom_text else "")
        )
        if cross_price and cross_price > 0:
            price_line = f"💰 <b>Access Fee:</b> <b>₹{price:.0f}</b> <s>₹{cross_price:.0f}</s> <i>(Limited Time Offer)</i>\n"
        else:
            price_line = f"💰 <b>Access Fee:</b> <b>₹{price:.0f}</b> <i>(Limited Time Offer)</i>\n"
        payment_mode_line = "📲 <b>Payment Mode:</b> Any UPI App (PhonePe, GPay, Paytm, BHIM)"
        action_line = "👇 <b>Tap the button below to unlock instant VIP access:</b>"
        channel_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"⚡ Unlock Instant Access — ₹{price:.0f} 🔓",
                url=f"https://t.me/{bot_username}?start=buy_{post_id}",
            )]
        ])

    final_caption = (
        f"{header_block}"
        f"<blockquote><b>🌟 Channel:</b> <b>{html.escape(ch_name)}</b> (<code>#{channel_id}</code>)\n"
        f"──────────────────────────\n"
        f"{price_line}"
        f"{payment_mode_line}</blockquote>\n"
        f"<blockquote>⚠️ <b>ONE-TIME LINK — NO BACKUP AVAILABLE</b></blockquote>\n\n"
        f"{action_line}"
    )
    if photo_file_id and len(final_caption) > 1024:
        final_caption = final_caption[:1020] + "..."

    # 5. Send to Main Channel (auto-detecting Photo / Video / Animation file_id)
    try:
        try:
            target_chat_id = int(target)
        except (ValueError, TypeError):
            target_chat_id = target

        msg = None
        if photo_file_id:
            if custom_media_type == "video" or photo_file_id.startswith(("BAAC", "BQAC", "DQAC")):
                send_order = ("video", "photo", "animation")
            elif custom_media_type == "animation" or photo_file_id.startswith("CgAC"):
                send_order = ("animation", "video", "photo")
            else:
                send_order = ("photo", "video", "animation")

            last_err = None
            for mtype in send_order:
                try:
                    if mtype == "video":
                        msg = await bot.send_video(
                            chat_id=target_chat_id,
                            video=photo_file_id,
                            caption=final_caption,
                            parse_mode=ParseMode.HTML,
                            reply_markup=channel_keyboard,
                        )
                    elif mtype == "animation":
                        msg = await bot.send_animation(
                            chat_id=target_chat_id,
                            animation=photo_file_id,
                            caption=final_caption,
                            parse_mode=ParseMode.HTML,
                            reply_markup=channel_keyboard,
                        )
                    else:
                        msg = await bot.send_photo(
                            chat_id=target_chat_id,
                            photo=photo_file_id,
                            caption=final_caption,
                            parse_mode=ParseMode.HTML,
                            reply_markup=channel_keyboard,
                        )
                    break
                except Exception as media_exc:
                    last_err = media_exc
                    continue

            if msg is None:
                logger.warning("Could not send recovered media (%s), falling back to text: %s", photo_file_id[:20], last_err)
                msg = await bot.send_message(
                    chat_id=target_chat_id,
                    text=final_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=channel_keyboard,
                )
        else:
            msg = await bot.send_message(
                chat_id=target_chat_id,
                text=final_caption,
                parse_mode=ParseMode.HTML,
                reply_markup=channel_keyboard,
            )

        await asyncio.to_thread(db.update_post_msg_id, DB_PATH, post_id, msg.message_id)
        return True, str(target), msg.message_id
    except Exception as exc:
        logger.error("Failed to republish post for channel #%d to %s: %s", channel_id, target, exc)
        return False, str(exc), None


# Track already-alerted dead channels: {channel_id: (link, last_alert_ts)}
_MONITOR_NOTIFIED_DEAD: dict[int, tuple[str, float]] = {}


async def _get_admin_mention_html(bot, admin_id: int) -> str:
    """Build a clickable HTML mention (and @username if available) to ping the admin."""
    uname = ""
    display_name = "Admin"
    try:
        user_row = await asyncio.to_thread(db.get_user, DB_PATH, admin_id)
        if user_row:
            uname = (row_get(user_row, "username") or "").lstrip("@").strip()
            fn = (row_get(user_row, "first_name") or "").strip()
            if fn:
                display_name = fn
    except Exception:
        pass

    if not uname and bot:
        try:
            chat = await bot.get_chat(admin_id)
            if getattr(chat, "username", None):
                uname = str(chat.username).lstrip("@").strip()
            if getattr(chat, "first_name", None):
                display_name = str(chat.first_name).strip()
        except Exception:
            pass

    safe_name = html.escape(display_name or "Admin")
    mention_link = f'<a href="tg://user?id={admin_id}">{safe_name}</a>'
    if uname:
        return f"@{html.escape(uname)} ({mention_link})"
    return mention_link


async def _notify_admins_of_dead_channels(
    bot, dead_items: list[tuple[dict, str, list[int]]]
) -> None:
    """
    Send mention alert(s) to all admins for expired/banned channels WITHOUT auto-deactivating
    or auto-deleting posts. Gives admin one-click buttons to Open, Re-check, or Deactivate
    the link & Delete its Main Channel post(s).
    Batches up to 5 channels per message so every channel gets buttons safely under Telegram limits.
    """
    if not dead_items:
        return

    admin_ids = get_admin_ids()
    if not admin_ids:
        logger.warning("[10M LINK MONITOR] No ADMIN_IDS configured — cannot send expired-channel alert.")
        return

    batch_size = 5
    total_count = len(dead_items)
    total_batches = (total_count + batch_size - 1) // batch_size

    for batch_idx in range(total_batches):
        batch = dead_items[batch_idx * batch_size : (batch_idx + 1) * batch_size]
        start_num = batch_idx * batch_size + 1

        item_blocks: list[str] = []
        buttons: list[list[InlineKeyboardButton]] = []

        for offset, (ch, reason, post_msg_ids) in enumerate(batch):
            idx = start_num + offset
            cid = ch["id"]
            ch_name = html.escape((row_get(ch, "name") or f"Channel #{cid}").strip())
            short_name = (row_get(ch, "name") or f"#{cid}").strip()[:20]
            safe_ch_name = f"\u2066{ch_name}\u2069\u200e"
            safe_short_name = f"\u2066{short_name}\u2069\u200e"
            link = (row_get(ch, "link") or "").strip()
            price_val = float(row_get(ch, "price_inr", 0) or 0)
            price_str = "FREE" if price_val <= 0 else f"₹{price_val:.0f}"

            if post_msg_ids:
                msg_list = ", ".join(f"#{m}" for m in post_msg_ids[:5])
                post_status = f"📢 Main Channel Post: Msg {msg_list}"
            else:
                post_status = "ℹ️ No published Main Channel post in DB"

            item_blocks.append(
                f"\u200e<b>{idx}. {safe_ch_name}</b> (ID: <code>#{cid}</code> • {price_str})\n"
                f"🔗 <code>{html.escape(link)}</code>\n"
                f"⚠️ <b>Status:</b> <i>{html.escape(reason)}</i>\n"
                f"📌 🟢 Active in /menu • {post_status}"
            )

            open_url = link if link.startswith(("http://", "https://")) else f"https://t.me/{link.lstrip('@')}"
            row_btns = []
            if open_url.startswith(("http://", "https://")):
                row_btns.append(InlineKeyboardButton(f"🔗 Open #{cid}", url=open_url))
            row_btns.append(InlineKeyboardButton(f"🔄 Re-check #{cid}", callback_data=f"recheck_ch:{cid}"))
            buttons.append(row_btns)
            buttons.append([
                InlineKeyboardButton(
                    f"\u200e🗑️ Deactivate & Delete Post #{cid} ({safe_short_name})",
                    callback_data=f"deactivate_del_ch:{cid}",
                )
            ])

        body_details = "\n\n".join(item_blocks)
        reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
        batch_suffix = f" ({batch_idx + 1}/{total_batches})" if total_batches > 1 else ""

        for aid in admin_ids:
            try:
                mention = await _get_admin_mention_html(bot, aid)
                alert_text = (
                    f"🚨 <b>Channel Link Alert — Action Required{batch_suffix}</b> 🚨\n"
                    f"👤 Attention {mention}\n\n"
                    f"The 10-minute link checker detected <b>{total_count}</b> expired or banned channel link(s):\n\n"
                    f"{body_details}\n\n"
                    f"🛡️ <i>No links were auto-deactivated and no channel posts were deleted automatically. "
                    f"Validate below and tap <b>🗑️ Deactivate &amp; Delete Post</b> when ready.</i>"
                )
                if len(alert_text) > 4096:
                    alert_text = alert_text[:4000] + "\n\n<i>...run /channels to view full list.</i>"
                await bot.send_message(
                    chat_id=aid,
                    text=alert_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                    disable_notification=False,
                )
                logger.info(
                    "Sent expired-channel alert%s (%d channel(s)) to admin %d.",
                    batch_suffix, len(batch), aid,
                )
            except Exception as exc:
                logger.warning("Failed to send dead-channel alert to admin %d: %s", aid, exc)


async def cb_deactivate_and_delete_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Admin callback from monitor alert: deactivate the channel link (remove from /menu)
    AND delete its published post(s) from the Main Channel while preserving post media/price
    & previous buyers so admin can still Reactivate & Repost later if needed.
    """
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("⛔ Admin only.", show_alert=True)
        return

    try:
        cid = int(query.data.split(":", 1)[1])
    except (IndexError, ValueError):
        await query.answer("❌ Invalid channel ID.", show_alert=True)
        return

    ch = await asyncio.to_thread(db.get_channel, DB_PATH, cid)
    if not ch:
        await query.answer("❌ Channel not found in database.", show_alert=True)
        return

    await query.answer("⏳ Deactivating link & deleting channel post...")

    existing_rows = (
        query.message.reply_markup.inline_keyboard
        if (query.message and query.message.reply_markup)
        else []
    )
    action_targets = {
        btn.callback_data
        for row in existing_rows
        for btn in row
        if btn.callback_data and btn.callback_data.startswith("deactivate_del_ch:")
    }
    is_multi_alert = len(action_targets) > 1

    ch_dict = dict(ch)
    ch_safe_name = html.escape((ch_dict.get("name") or f"Channel #{cid}").strip())
    cleanup_info = await cleanup_dead_channel(
        ctx.bot, ch_dict, "Admin deactivated & deleted post from monitor alert"
    )

    del_ids = cleanup_info.get("deleted_msg_ids") or []
    gone_ids = cleanup_info.get("already_gone_ids") or []
    del_errs = cleanup_info.get("delete_errors") or []
    if del_ids:
        msg_list = ", ".join(f"#{m}" for m in del_ids[:5])
        post_line = f"🗑️ <b>Main Channel Post:</b> Deleted (Msg {msg_list})"
    elif gone_ids:
        msg_list = ", ".join(f"#{m}" for m in gone_ids[:5])
        post_line = f"🗑️ <b>Main Channel Post:</b> Already removed from Telegram (Msg {msg_list}; DB cleared)"
    elif del_errs:
        post_line = f"⚠️ <b>Main Channel Post Delete Error:</b> <code>{html.escape(del_errs[0][:140])}</code>"
    else:
        post_line = "ℹ️ <b>Main Channel Post:</b> No published post found in DB"

    result_text = (
        f"✅ <b>Channel Deactivated &amp; Post Removed!</b>\n\n"
        f"• <b>Channel:</b> <b>{ch_safe_name}</b> (ID: <code>#{cid}</code>)\n"
        f"• <b>/menu Status:</b> 🔴 Deactivated (removed from /menu)\n"
        f"• {post_line}\n\n"
        f"💡 <i>Original post media, pricing &amp; previous buyers are preserved in DB. "
        f"You can re-check or reactivate &amp; repost anytime below:</i>"
    )
    result_markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"🔄 Re-check #{cid}", callback_data=f"recheck_ch:{cid}"),
            InlineKeyboardButton(f"🟢 Reactivate & Repost #{cid}", callback_data=f"reactivate_ch:{cid}"),
        ]
    ])

    if is_multi_alert and query.message:
        new_rows = []
        for row in existing_rows:
            new_row = []
            for btn in row:
                if btn.callback_data == f"deactivate_del_ch:{cid}":
                    new_row.append(InlineKeyboardButton(
                        f"✅ Deactivated & Deleted #{cid}",
                        callback_data="menu_noop",
                    ))
                else:
                    new_row.append(btn)
            new_rows.append(new_row)
        try:
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_rows))
        except Exception:
            pass
        await ctx.bot.send_message(
            chat_id=query.message.chat_id,
            reply_to_message_id=query.message.message_id,
            text=result_text,
            parse_mode=ParseMode.HTML,
            reply_markup=result_markup,
        )
    else:
        await query.edit_message_text(
            result_text,
            parse_mode=ParseMode.HTML,
            reply_markup=result_markup,
        )


async def cb_recheck_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin callback: re-validate a specific channel link on demand from a monitor alert."""
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("⛔ Admin only.", show_alert=True)
        return

    try:
        cid = int(query.data.split(":", 1)[1])
    except (IndexError, ValueError):
        await query.answer("❌ Invalid channel ID.", show_alert=True)
        return

    ch = await asyncio.to_thread(db.get_channel, DB_PATH, cid)
    if not ch:
        await query.answer("❌ Channel not found in database.", show_alert=True)
        return

    link = (row_get(ch, "link", "") or "").strip()
    ch_name = (row_get(ch, "name", "") or f"Channel #{cid}").strip()
    is_alive, detail = await check_channel_link_alive(
        link, bot=ctx.bot, chat_id=row_get(ch, "chat_id")
    )
    if is_alive:
        _MONITOR_NOTIFIED_DEAD.pop(cid, None)
        await query.answer(f"✅ Channel #{cid} is WORKING!\n{detail[:140]}", show_alert=True)
        if not ch["is_active"] and query.message:
            try:
                await ctx.bot.send_message(
                    chat_id=query.message.chat_id,
                    reply_to_message_id=query.message.message_id,
                    text=(
                        f"✅ <b>Re-check Passed — Link is Working!</b>\n\n"
                        f"• <b>Channel:</b> <b>{html.escape(ch_name)}</b> (ID: <code>#{cid}</code>)\n"
                        f"• <b>Link:</b> <code>{html.escape(link)}</code>\n"
                        f"• <b>Live Status:</b> <i>{html.escape(detail)}</i>\n\n"
                        f"Tap below to reactivate this channel and resend its post to the Main Channel:"
                    ),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(
                            f"🟢 Reactivate & Repost #{cid} Now",
                            callback_data=f"reactivate_ch:{cid}",
                        )],
                        [InlineKeyboardButton(
                            f"🔄 Re-check Again #{cid}",
                            callback_data=f"recheck_ch:{cid}",
                        )],
                    ]),
                )
            except Exception as exc:
                logger.debug("Could not send recheck-passed reply for channel #%d: %s", cid, exc)
    else:
        await query.answer(f"❌ Channel #{cid} is STILL DEAD:\n{detail[:140]}", show_alert=True)


async def run_periodic_channel_link_check(bot) -> list[tuple[dict, str, list[int]]]:
    """
    Single pass of the periodic 10-minute channel link check using the exact /checklinks logic.
    Checks ONLY currently ACTIVE channels (is_active = 1) — never scans already-deactivated
    channels. Double-verifies any dead/banned links and sends a mention alert to admins
    WITHOUT auto-deactivating any link or deleting any channel posts.
    Admin can then tap 'Deactivate & Delete Post' directly from the alert.
    """
    raw_channels = await asyncio.to_thread(db.get_active_channels, DB_PATH)
    if not raw_channels:
        return []

    # Deduplicate active channels by canonical link (preferring newest ID)
    by_key: dict[str, dict] = {}
    ordered_channels: list[dict] = []
    for ch in sorted(raw_channels, key=lambda r: int(r["id"]), reverse=True):
        if not row_get(ch, "is_active", 0):
            continue
        link = (row_get(ch, "link") or "").strip()
        if not link:
            continue
        key = _canonical_telegram_link_key(link) or link
        if key in by_key:
            continue
        ch_d = dict(ch)
        by_key[key] = ch_d
        ordered_channels.append(ch_d)
    ordered_channels.sort(key=lambda r: int(r["id"]))

    remind_after = int(os.getenv("CHANNEL_ALERT_REMIND_SECONDS", "3600"))
    step_delay = float(os.getenv("CHANNEL_MONITOR_STEP_DELAY_SECONDS", "2.0"))
    transient_keywords = ("rate limit", "connection error", "busy", "timeout", "timed out", "could not verify", "throttling")
    dead_confirmed: list[tuple[dict, str]] = []
    alive_count = 0
    transient_skips = 0

    for idx, ch in enumerate(ordered_channels):
        if idx > 0:
            await asyncio.sleep(step_delay)
        cid = ch["id"]
        link = (ch.get("link") or "").strip()
        chat_id = ch.get("chat_id")

        is_alive, detail = await check_channel_link_alive(link, bot=bot, chat_id=chat_id)
        if is_alive:
            alive_count += 1
            _MONITOR_NOTIFIED_DEAD.pop(cid, None)
            continue

        low_detail = (detail or "").lower()
        if any(w in low_detail for w in transient_keywords):
            transient_skips += 1
            # Back off politely when Telegram soft-throttles t.me previews
            await asyncio.sleep(15.0)
            continue

        # Double-verify after short pause to rule out temporary hiccups
        await asyncio.sleep(2.0)
        is_alive2, detail2 = await check_channel_link_alive(link, bot=bot, chat_id=chat_id)
        if is_alive2:
            alive_count += 1
            _MONITOR_NOTIFIED_DEAD.pop(cid, None)
            continue

        low_detail2 = (detail2 or "").lower()
        if any(w in low_detail2 for w in transient_keywords):
            transient_skips += 1
            await asyncio.sleep(15.0)
            continue

        dead_confirmed.append((ch, detail2 or detail))

    # Circuit breaker: if almost all checked channels fail simultaneously in one pass,
    # treat as a server/Telegram network outage rather than sending a false alarm.
    checked_count = len(ordered_channels) - transient_skips
    mass_failure = (
        (checked_count >= 4 and len(dead_confirmed) == checked_count)
        or (checked_count >= 10 and len(dead_confirmed) >= max(8, int(checked_count * 0.7)))
    )
    if mass_failure:
        logger.warning(
            "[10M LINK MONITOR] %d/%d channels failed simultaneously — skipping alert (suspected t.me IP throttle or network outage).",
            len(dead_confirmed), checked_count,
        )
        return []

    now = time.time()
    to_notify: list[tuple[dict, str, list[int]]] = []
    for ch_dict, reason in dead_confirmed:
        cid = ch_dict["id"]
        # Re-check DB right before alerting in case admin deactivated it while the scan was running
        fresh_ch = await asyncio.to_thread(db.get_channel, DB_PATH, cid)
        if not fresh_ch or not row_get(fresh_ch, "is_active", 0):
            _MONITOR_NOTIFIED_DEAD.pop(cid, None)
            continue
        link = (ch_dict.get("link") or "").strip()
        prev = _MONITOR_NOTIFIED_DEAD.get(cid)
        if prev is None or prev[0] != link or (now - prev[1]) >= remind_after:
            posts = await asyncio.to_thread(db.get_posts_for_channel_exact, DB_PATH, cid, link)
            post_msg_ids = sorted({int(p["channel_msg_id"]) for p in posts if p["channel_msg_id"]})
            to_notify.append((ch_dict, reason, post_msg_ids))
            _MONITOR_NOTIFIED_DEAD[cid] = (link, now)

    logger.info(
        "[10M LINK MONITOR] Pass summary: total=%d | alive=%d | dead=%d | rate_limited=%d | newly_alerted=%d",
        len(ordered_channels), alive_count, len(dead_confirmed), transient_skips, len(to_notify),
    )

    if to_notify:
        await _notify_admins_of_dead_channels(bot, to_notify)

    return to_notify


async def channel_link_monitor_worker(bot) -> None:
    """
    Background job running every 10 minutes (600s) using the exact same /checklinks validation logic.
    Runs an initial pass shortly after startup (20s) and every 10 minutes thereafter.
    Never auto-deactivates links or auto-deletes posts — sends an admin mention alert with
    Open, Re-check, and 'Deactivate & Delete Post' action buttons.
    """
    startup_delay = int(os.getenv("CHANNEL_MONITOR_STARTUP_DELAY_SECONDS", "20"))
    check_interval = int(os.getenv("CHANNEL_MONITOR_INTERVAL_SECONDS", "600"))
    logger.info(
        "10-minute channel link monitor job started (initial_delay=%ds, interval=%ds, alert-only — admin controls deactivation & post deletion).",
        startup_delay, check_interval,
    )
    first_run = True
    while True:
        try:
            await asyncio.sleep(startup_delay if first_run else check_interval)
            first_run = False
            alerted = await run_periodic_channel_link_check(bot)
            if alerted:
                logger.warning(
                    "[10M LINK MONITOR] Alerted admins about %d expired/banned channel(s) (no auto-deactivation or auto-delete).",
                    len(alerted),
                )
            else:
                logger.info("[10M LINK MONITOR] Periodic check complete — no new expired/banned channels.")
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Error in channel_link_monitor_worker: %s", exc)
            await asyncio.sleep(60)
async def cmd_delpost(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: /delpost <message_id_or_link> to delete a specific post from the channel."""
    if not is_admin(update.effective_user.id):
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text(
            "<b>Usage:</b> <code>/delpost &lt;message_id_or_link&gt; [channel_id]</code>\n\n"
            "Deletes a promotional post from the channel.\n"
            "• <b>Examples:</b>\n"
            "  <code>/delpost 1234</code>\n"
            "  <code>/delpost https://t.me/c/1234567890/456</code>\n"
            "  <code>/delpost https://t.me/mychannel/456</code>\n\n"
            "💡 <i>Tip: You can also simply forward any channel post to this bot in private chat to delete it with one tap!</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    raw_input = args[0].strip()
    target = args[1].strip() if len(args) > 1 else None
    msg_id = None

    # Parse link formats: https://t.me/c/1234567890/456 or https://t.me/mychannel/456
    if "t.me/" in raw_input:
        clean = raw_input.rstrip("/")
        parts = clean.split("/")
        if len(parts) >= 2 and parts[-1].isdigit():
            msg_id = int(parts[-1])
            channel_part = parts[-2]
            if channel_part.startswith("c"):
                # Private channel link t.me/c/1234567890/456 -> -1001234567890
                cid_num = channel_part[1:]
                if cid_num.isdigit():
                    target = f"-100{cid_num}"
            else:
                target = f"@{channel_part.lstrip('@')}"
    else:
        try:
            msg_id = int(raw_input)
        except ValueError:
            await update.message.reply_text("⚠️ Message ID must be a number or a valid Telegram post link.")
            return

    if not target:
        target = db.get_setting(DB_PATH, "main_channel_id") or MAIN_CHANNEL_ID
    if not target:
        await update.message.reply_text("❌ No target channel configured. Provide channel ID/username as second argument.")
        return

    try:
        try:
            target_chat_id = int(target)
        except (ValueError, TypeError):
            target_chat_id = target
        await ctx.bot.delete_message(chat_id=target_chat_id, message_id=msg_id)
        # Also clean up from DB if found
        conn = db.get_connection(DB_PATH)
        conn.execute("DELETE FROM posts WHERE channel_msg_id = ?", (msg_id,))
        conn.commit()
        await update.message.reply_text(
            f"✅ <b>Successfully Deleted Post!</b>\n\n"
            f"• <b>Message ID:</b> <code>{msg_id}</code>\n"
            f"• <b>Channel:</b> <code>{target}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:
        await update.message.reply_text(
            f"❌ <b>Could not delete message:</b> {exc}\n\n"
            f"• Target: <code>{target}</code>\n"
            f"• Message ID: <code>{msg_id}</code>\n"
            f"⚠️ <i>Check if the bot is Administrator in that channel with 'Delete Messages' permission!</i>",
            parse_mode=ParseMode.HTML,
        )


_TG_LINK_EXTRACT_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/(?:\+[A-Za-z0-9_-]+|joinchat/[A-Za-z0-9_-]+|s/[A-Za-z0-9_]{4,}|[A-Za-z0-9_]{4,})(?:\?[A-Za-z0-9_=&-]+)?"
    r"|(?<!\S)@[A-Za-z][A-Za-z0-9_]{3,}",
    re.IGNORECASE,
)


def _canonical_telegram_link_key(link: str) -> str:
    """
    Build a canonical comparison key for a Telegram channel link so invite links
    (t.me/+HASH and t.me/joinchat/HASH) and public links (@user, t.me/user, t.me/s/user) match reliably.
    """
    raw = (link or "").strip().rstrip(".,;:!)>]}")
    if not raw:
        return ""
    if raw.startswith("@"):
        return f"user:{raw.lstrip('@').lower()}"

    cleaned = re.sub(
        r"^(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/",
        "",
        raw,
        flags=re.IGNORECASE,
    ).split("?", 1)[0].strip("/")

    if cleaned.startswith("+"):
        return f"invite:{cleaned[1:]}"
    if cleaned.lower().startswith("joinchat/"):
        return f"invite:{cleaned.split('/', 1)[1].strip('/')}"
    if cleaned.lower().startswith("s/"):
        cleaned = cleaned.split("/", 1)[1].strip("/")
    return f"user:{cleaned.split('/', 1)[0].lower()}"


def extract_telegram_links(raw_text: str, entities=None) -> list[str]:
    """Extract and normalize unique Telegram channel links from text and message entities."""
    candidates: list[str] = []
    if raw_text:
        for match in _TG_LINK_EXTRACT_RE.finditer(raw_text):
            candidates.append(match.group(0).strip().rstrip(".,;:!)>]}"))

    if entities:
        for ent in entities:
            ent_url = getattr(ent, "url", None)
            if ent_url:
                for match in _TG_LINK_EXTRACT_RE.finditer(ent_url):
                    candidates.append(match.group(0).strip().rstrip(".,;:!)>]}"))

    normalized_links: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        if not item:
            continue
        if item.startswith("@"):
            slug = item.lstrip("@")
            if slug.lower() in (
                "checklinks", "monitor", "newlinks", "comparelinks",
                "unadded", "missinglinks", "restlinks", "cancel",
            ):
                continue
            norm = f"https://t.me/{slug}"
        elif item.lower().startswith(("http://", "https://")):
            norm = re.sub(
                r"^https?://(?:www\.)?(?:t\.me|telegram\.me)/",
                "https://t.me/",
                item,
                flags=re.IGNORECASE,
            )
        else:
            path_part = item.split("/", 1)[1] if "/" in item else item
            norm = f"https://t.me/{path_part}"

        canon_key = _canonical_telegram_link_key(norm) or norm
        if canon_key not in seen:
            seen.add(canon_key)
            normalized_links.append(norm)

    return normalized_links


async def _run_checklinks_validation(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, links: list[str]
) -> int:
    """
    Validate provided Telegram channel links without deleting any channel messages.
    Separates results into:
    - New working links (not present in active bot channels yet) + tap-to-copy block
    - Already present channels (extracted separately with existing channel details & live status)
    - Expired / banned / invalid links
    """
    msg = await update.effective_message.reply_text(
        f"🔍 <b>Validating {len(links)} channel link(s)...</b>\nPlease wait.",
        parse_mode=ParseMode.HTML,
    )

    active_channels = await asyncio.to_thread(db.get_active_channels, DB_PATH)
    active_by_key: dict[str, dict] = {}
    for ch in active_channels:
        ch_link = (row_get(ch, "link") or "").strip()
        key = _canonical_telegram_link_key(ch_link)
        if key:
            active_by_key[key] = dict(ch)

    new_working_links: list[tuple[str, str]] = []
    already_present_links: list[tuple[str, dict, bool, str]] = []
    dead_links: list[tuple[str, str]] = []

    for idx, link in enumerate(links):
        if idx > 0:
            await asyncio.sleep(1.0)
        canon_key = _canonical_telegram_link_key(link)
        existing_ch = active_by_key.get(canon_key) if canon_key else None
        is_alive, detail = await check_channel_link_alive(
            link,
            bot=ctx.bot,
            chat_id=existing_ch.get("chat_id") if existing_ch else None,
        )
        if existing_ch:
            already_present_links.append((link, existing_ch, is_alive, detail))
        elif is_alive:
            new_working_links.append((link, detail))
        else:
            dead_links.append((link, detail))

    lines: list[str] = [
        "📊 <b>Channel Links Validation Report</b>",
        (
            f"Total Checked: <b>{len(links)}</b> • "
            f"✅ New Working: <b>{len(new_working_links)}</b> • "
            f"📌 Already Present: <b>{len(already_present_links)}</b> • "
            f"❌ Not Working: <b>{len(dead_links)}</b>\n"
        ),
    ]

    if new_working_links:
        lines.append(f"✅ <b>New Working Links ({len(new_working_links)}):</b>")
        for i, (w_link, w_detail) in enumerate(new_working_links, 1):
            safe_detail = html.escape(w_detail) if w_detail else "Active"
            lines.append(f"{i}. <code>{html.escape(w_link)}</code> — <b>{safe_detail}</b>")

        only_urls = "\n".join(html.escape(w_link) for w_link, _ in new_working_links)
        lines.append(f"\n📋 <b>New Working Links Only (Tap to Copy):</b>\n<pre>{only_urls}</pre>")
    elif already_present_links and not dead_links:
        lines.append("ℹ️ <b>All provided links are already present in the bot.</b>")
    elif not already_present_links:
        lines.append("⚠️ <b>No working links found among the provided links.</b>")
    else:
        lines.append("ℹ️ <b>No new working links found (valid links are already present in the bot).</b>")

    if already_present_links:
        lines.append(f"\n📌 <b>Channel Already Present in Bot ({len(already_present_links)}):</b>")
        for i, (a_link, ch_info, is_alive, detail) in enumerate(already_present_links, 1):
            ch_name = html.escape(str(ch_info.get("name") or f"Channel #{ch_info.get('id')}"))
            ch_id = ch_info.get("id")
            price_val = float(ch_info.get("price_inr") or 0)
            price_str = "FREE" if price_val <= 0 else f"₹{price_val:.0f}"
            live_tag = "✅ Working" if is_alive else f"❌ {html.escape(detail)}"
            lines.append(
                f"{i}. <code>{html.escape(a_link)}</code> — "
                f"<b>Channel already present</b> (<b>{ch_name}</b> • ID #{ch_id} • {price_str} • {live_tag})"
            )

    if dead_links:
        lines.append(f"\n❌ <b>Expired / Not Working Links ({len(dead_links)}):</b>")
        for i, (d_link, d_reason) in enumerate(dead_links, 1):
            lines.append(f"{i}. <code>{html.escape(d_link)}</code> — <i>{html.escape(d_reason)}</i>")

    full_text = "\n".join(lines)
    if len(full_text) <= 4096:
        await msg.edit_text(full_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    else:
        await msg.edit_text(
            full_text[:4000] + "\n\n<i>(Continued below...)</i>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        remaining = full_text[4000:]
        while remaining:
            chunk = remaining[:4000]
            remaining = remaining[4000:]
            await update.effective_message.reply_text(
                chunk, parse_mode=ParseMode.HTML, disable_web_page_preview=True
            )

    return ConversationHandler.END


async def cmd_checklinks(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Admin command: prompt admin to send channel links, then validate and extract new working links."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return ConversationHandler.END

    raw_text = (update.effective_message.text or "") if update.effective_message else ""
    body_after_cmd = re.sub(r"^/(?:checklinks|monitor)(?:@\S+)?\s*", "", raw_text, count=1, flags=re.IGNORECASE)
    entities = update.effective_message.entities if update.effective_message else None
    provided_links = extract_telegram_links(body_after_cmd, entities=entities)

    if provided_links:
        return await _run_checklinks_validation(update, ctx, provided_links)

    await update.effective_message.reply_text(
        "🔍 <b>Check Channel Links</b>\n\n"
        "Please send the channel link(s) you want to validate (one per line, or separated by spaces).\n\n"
        "✅ I will validate all links, separate any <b>already present channels</b>, and give you the <b>new working links</b>.\n"
        "🛡️ <i>No messages or posts will be deleted from any channel.</i>\n\n"
        "Type /cancel anytime to abort.",
        parse_mode=ParseMode.HTML,
    )
    return CHECKLINKS_WAIT


async def checklinks_receive_links(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive channel links sent by admin after /checklinks and validate them."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return ConversationHandler.END

    msg = update.effective_message
    raw_text = (msg.text or msg.caption or "") if msg else ""
    entities = (msg.entities or msg.caption_entities) if msg else None
    links = extract_telegram_links(raw_text, entities=entities)

    if not links:
        await msg.reply_text(
            "⚠️ <b>No valid Telegram channel links found in your message.</b>\n\n"
            "Please send one or more channel links (e.g. <code>https://t.me/+xxxx</code> or <code>https://t.me/channelname</code>), or type /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return CHECKLINKS_WAIT

    return await _run_checklinks_validation(update, ctx, links)


async def _run_newlinks_comparison(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, links: list[str]
) -> int:
    """Compare provided links with active channels in DB and return the links not added in bot yet."""
    active_channels = await asyncio.to_thread(db.get_active_channels, DB_PATH)

    active_by_key: dict[str, dict] = {}
    for ch in active_channels:
        ch_link = (row_get(ch, "link") or "").strip()
        key = _canonical_telegram_link_key(ch_link)
        if key:
            active_by_key[key] = dict(ch)

    not_added_links: list[str] = []
    already_added_links: list[tuple[str, dict]] = []

    for link in links:
        key = _canonical_telegram_link_key(link)
        matched_ch = active_by_key.get(key) if key else None
        if matched_ch:
            already_added_links.append((link, matched_ch))
        else:
            not_added_links.append(link)

    lines: list[str] = [
        "🆕 <b>Unadded Channel Links Report</b>",
        (
            f"Provided: <b>{len(links)}</b> • "
            f"🆕 Not Added Yet: <b>{len(not_added_links)}</b> • "
            f"✅ Already Active in Bot: <b>{len(already_added_links)}</b>\n"
        ),
    ]

    if not_added_links:
        lines.append(f"🆕 <b>Links NOT Added in Bot Yet ({len(not_added_links)}):</b>")
        for i, n_link in enumerate(not_added_links, 1):
            lines.append(f"{i}. <code>{html.escape(n_link)}</code>")

        copy_block = "\n".join(html.escape(n_link) for n_link in not_added_links)
        lines.append(f"\n📋 <b>Not Added Links Only (Tap to Copy):</b>\n<pre>{copy_block}</pre>")
    else:
        lines.append("✅ <b>All provided links are already active in the bot!</b>")

    if already_added_links:
        lines.append(f"\n✅ <b>Already Active in Bot ({len(already_added_links)}):</b>")
        for i, (a_link, ch_info) in enumerate(already_added_links, 1):
            ch_name = html.escape(str(ch_info.get("name") or f"Channel #{ch_info.get('id')}"))
            ch_id = ch_info.get("id")
            lines.append(f"{i}. <code>{html.escape(a_link)}</code> — <b>{ch_name}</b> (ID #{ch_id})")

    full_text = "\n".join(lines)
    msg = update.effective_message
    if len(full_text) <= 4096:
        await msg.reply_text(full_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    else:
        remaining = full_text
        while remaining:
            chunk = remaining[:4000]
            remaining = remaining[4000:]
            await msg.reply_text(chunk, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    return ConversationHandler.END


async def cmd_newlinks(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Admin command: ask admin to send channel links, compare with active bot links, and return unadded links."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return ConversationHandler.END

    raw_text = (update.effective_message.text or "") if update.effective_message else ""
    body_after_cmd = re.sub(
        r"^/(?:newlinks|comparelinks|unadded|missinglinks|restlinks)(?:@\S+)?\s*",
        "",
        raw_text,
        count=1,
        flags=re.IGNORECASE,
    )
    entities = update.effective_message.entities if update.effective_message else None
    provided_links = extract_telegram_links(body_after_cmd, entities=entities)

    if provided_links:
        return await _run_newlinks_comparison(update, ctx, provided_links)

    await update.effective_message.reply_text(
        "🆕 <b>Find Unadded Channel Links</b>\n\n"
        "Please send your list of channel links (one per line, or separated by spaces).\n\n"
        "🔍 I will compare them with all <b>active links in the bot</b> and send you the remaining links that are <b>not added in the bot yet</b>.\n\n"
        "Type /cancel anytime to abort.",
        parse_mode=ParseMode.HTML,
    )
    return NEWLINKS_WAIT


async def newlinks_receive_links(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive channel links sent by admin after /newlinks and return those not active in the bot yet."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return ConversationHandler.END

    msg = update.effective_message
    raw_text = (msg.text or msg.caption or "") if msg else ""
    entities = (msg.entities or msg.caption_entities) if msg else None
    links = extract_telegram_links(raw_text, entities=entities)

    if not links:
        await msg.reply_text(
            "⚠️ <b>No valid Telegram channel links found in your message.</b>\n\n"
            "Please send one or more channel links (e.g. <code>https://t.me/+xxxx</code> or <code>https://t.me/channelname</code>), or type /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return NEWLINKS_WAIT

    return await _run_newlinks_comparison(update, ctx, links)


async def cmd_setchatid(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Quick admin command to attach a Chat ID to a channel for anti-leak protection."""
    if not is_admin(update.effective_user.id):
        return
    args = ctx.args or []
    if len(args) < 2:
        channels = get_active_channels_cached()
        ch_list = "\n".join(
            f"• <code>/setchatid {c['id']} &lt;chat_id&gt;</code> — <b>{c['name']}</b> (Chat ID: {row_get(c, 'chat_id') or '❌ Not Set'})"
            for c in channels
        )
        await update.message.reply_text(
            "🛡️ <b>Set Channel Chat ID (Anti-Leak Protection)</b>\n\n"
            "Attaching a Chat ID enables:\n"
            "1️⃣ <b>Blocked Channel Previews:</b> Users cannot view channel messages before joining\n"
            "2️⃣ <b>Join-Request Firewall:</b> Links are non-transferable (forwarded/copied links cannot be used by others)\n"
            "3️⃣ <b>15-Minute Link Expiration:</b> Links self-destruct shortly after generation\n\n"
            f"<b>Usage:</b>\n{ch_list}\n\n"
            "💡 <i>Tip: Forward any message from your private VIP channel to this bot in private chat to automatically detect its Chat ID!</i>",
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        channel_id = int(args[0])
        target_chat_id = args[1].strip()
    except ValueError:
        await update.message.reply_text("⚠️ Invalid channel ID. Use numbers only.")
        return

    ch = db.get_channel(DB_PATH, channel_id)
    if not ch:
        await update.message.reply_text("❌ Channel not found.")
        return

    db.update_channel(DB_PATH, channel_id, chat_id=target_chat_id)
    await update.message.reply_text(
        f"✅ <b>Anti-Leak Protection Enabled for {ch['name']}!</b>\n\n"
        f"• <b>Chat ID:</b> <code>{target_chat_id}</code>\n"
        f"• <b>Join Requests:</b> Active ✅ (previews blocked)\n"
        f"• <b>Anti-Copy Firewall:</b> Active ✅ (unauthorized users rejected)\n\n"
        f"⚠️ <i>Make sure this bot is added as an <b>Administrator</b> in that channel with 'Invite Users via Link' and 'Manage Join Requests' permissions!</i>",
        parse_mode=ParseMode.HTML,
    )


async def handle_admin_forward(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Detect forwarded messages from channels to delete post or link Chat ID."""
    msg = update.effective_message
    user = update.effective_user
    if not user or not is_admin(user.id) or not msg:
        return
    forward_chat = msg.forward_from_chat
    if forward_chat and forward_chat.type in ("channel", "supergroup"):
        chat_id = forward_chat.id
        title = forward_chat.title or "Channel"
        fwd_msg_id = msg.forward_from_message_id

        buttons = []
        if fwd_msg_id:
            buttons.append([
                InlineKeyboardButton(f"🗑️ Delete this Post #{fwd_msg_id} from Channel Now", callback_data=f"del_fwd_post:{chat_id}:{fwd_msg_id}")
            ])

        channels = get_active_channels_cached()
        # Prioritize channels without chat_id set (newest first), capped at 25 buttons
        # so the inline keyboard never exceeds Telegram's 10 KB reply_markup limit.
        ordered_channels = sorted(
            channels,
            key=lambda c: (bool(row_get(c, "chat_id")), -int(c["id"])),
        )
        for c in ordered_channels[:25]:
            buttons.append([
                InlineKeyboardButton(
                    f"🛡️ Link Chat ID to {(c['name'] or '')[:26]} (ID #{c['id']})",
                    callback_data=f"link_cid:{c['id']}:{chat_id}",
                )
            ])

        info_lines = [
            f"📢 <b>Forwarded Channel Post Detected!</b>\n",
            f"• <b>Channel:</b> <b>{title}</b>",
            f"• <b>Chat ID:</b> <code>{chat_id}</code>",
        ]
        if fwd_msg_id:
            info_lines.append(f"• <b>Message ID:</b> <code>{fwd_msg_id}</code>")
        if len(ordered_channels) > 25:
            info_lines.append(
                f"\nℹ️ <i>Showing top 25 channels. To link any other channel ID directly, use:</i>\n"
                f"<code>/setchatid &lt;channel_id&gt; {chat_id}</code>"
            )

        info_lines.append("\n<b>Choose an action:</b>")
        await msg.reply_text(
            "\n".join(info_lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
        )


async def cb_del_fwd_post(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """One-click delete post from channel callback."""
    query = update.callback_query
    await query.answer()
    if not is_admin(update.effective_user.id):
        return
    parts = query.data.split(":")
    if len(parts) < 3:
        return
    chat_id = parts[1]
    msg_id = int(parts[2])

    try:
        try:
            target_chat_id = int(chat_id)
        except (ValueError, TypeError):
            target_chat_id = chat_id
        await ctx.bot.delete_message(chat_id=target_chat_id, message_id=msg_id)
        # Also clean up from DB if found
        conn = db.get_connection(DB_PATH)
        conn.execute("DELETE FROM posts WHERE channel_msg_id = ?", (msg_id,))
        conn.commit()
        await query.edit_message_text(
            f"✅ <b>Successfully deleted post #{msg_id} from channel!</b>\n\n"
            f"• Channel ID: <code>{chat_id}</code>\n"
            f"• Message ID: <code>{msg_id}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:
        await query.edit_message_text(
            f"❌ <b>Could not delete message #{msg_id}:</b> {exc}\n\n"
            f"⚠️ <i>Make sure this bot is promoted to <b>Administrator</b> in that channel with 'Delete Messages' permission!</i>",
            parse_mode=ParseMode.HTML,
        )


async def cb_link_chatid(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle clicking inline button to attach Chat ID."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    channel_id = int(parts[1])
    chat_id = parts[2]
    ch = db.get_channel(DB_PATH, channel_id)
    if not ch:
        await query.edit_message_text("❌ Channel not found.")
        return
    db.update_channel(DB_PATH, channel_id, chat_id=chat_id)
    await query.edit_message_text(
        f"✅ <b>Anti-Leak Protection Enabled for {ch['name']}!</b>\n\n"
        f"• <b>Chat ID:</b> <code>{chat_id}</code>\n"
        f"• <b>Join Requests:</b> Active ✅ (previews blocked)\n"
        f"• <b>Anti-Copy Firewall:</b> Active ✅ (unauthorized users rejected)\n\n"
        f"<i>Please verify the bot is an Administrator in this channel!</i>",
        parse_mode=ParseMode.HTML,
    )


async def handle_my_chat_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Auto-detect when bot is added to a channel as admin and save chat_id."""
    chat = update.effective_chat
    my_member = update.my_chat_member
    if not chat or not my_member:
        return
    if chat.type in ("channel", "supergroup"):
        new_status = my_member.new_chat_member.status
        if new_status in (ChatMemberStatus.ADMINISTRATOR, "administrator"):
            logger.info("Bot added as admin to channel: %s (ID: %d)", chat.title, chat.id)
            channels = db.get_all_channels(DB_PATH)
            for ch in channels:
                if (ch["name"] or "").strip().lower() == (chat.title or "").strip().lower() or not row_get(ch, "chat_id"):
                    db.update_channel(DB_PATH, ch["id"], chat_id=str(chat.id))
                    logger.info("Auto-assigned chat_id %d to channel %s (#%d)", chat.id, ch["name"], ch["id"])
                    break


async def cb_check_payment(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle user clicking 'Check Payment Status'."""
    query = update.callback_query
    await query.answer()
    ref_id = query.data.split(":")[1]

    payment = await asyncio.to_thread(db.get_payment_by_order, DB_PATH, ref_id)
    if not payment:
        await query.answer("❌ Payment reference not found.", show_alert=True)
        return

    if payment["status"] == "paid":
        channel = await db_get_channel(payment["channel_id"])
        await query.answer("✅ Payment verified! Delivering your link...", show_alert=True)
        if channel:
            await send_protected_channel_link(ctx.bot, query.from_user.id, channel)
        return

    # Check live with Razorpay API in a worker thread so event loop never blocks
    try:
        client = rzp_client()
        if ref_id.startswith("qr_"):
            payments_list = await asyncio.to_thread(pay.fetch_qr_payments, client, ref_id)
            if payments_list:
                for item in payments_list:
                    if item.get("status") in ("captured", "authorized"):
                        payment_id = item.get("id")
                        method = item.get("method", "UPI QR")
                        await process_successful_payment(
                            ctx.bot,
                            ref_id,
                            payment_id,
                            payment["amount_inr"],
                            method,
                        )
                        await query.answer("🎉 Payment verified! Access unlocked.", show_alert=True)
                        return
            await query.answer("⏳ Payment not detected yet. Please scan and pay with any UPI app.", show_alert=True)
        else:
            order = await asyncio.to_thread(pay.fetch_order, client, ref_id)
            if order.get("status") == "paid" or order.get("amount_paid", 0) >= order.get("amount", 1):
                payments_list = await asyncio.to_thread(pay.fetch_order_payments, client, ref_id)
                payment_id = ""
                method = "Instant Check"
                if payments_list:
                    for item in payments_list:
                        if item.get("status") in ("captured", "authorized"):
                            payment_id = item.get("id")
                            method = item.get("method", "UPI/Card")
                            break
                await process_successful_payment(
                    ctx.bot,
                    ref_id,
                    payment_id or f"check_{ref_id}",
                    payment["amount_inr"],
                    method,
                )
                await query.answer("🎉 Payment verified! Access unlocked.", show_alert=True)
            else:
                await query.answer("⏳ Payment not detected yet. Please complete payment first.", show_alert=True)
    except Exception as exc:
        logger.error("Error in cb_check_payment for %s: %s", ref_id, exc)
        await query.answer("⚠️ Verifying with payment network... Link will be delivered automatically upon completion.", show_alert=True)



# In-memory QR cache: (user_id, channel_id) -> {"qr_id": ..., "qr_bytes": ..., "vpa": ..., "created_at": ...}
QR_CACHE: dict[tuple[int, int], dict] = {}
_QR_INFLIGHT_LOCKS: dict[tuple[int, int], asyncio.Lock] = {}

# Tracks the QR payment photo message per user so it can be deleted after payment
# user_id -> {"chat_id": int, "message_id": int}
QR_MESSAGE_CACHE: dict[int, dict] = {}


def _blocking_get_or_create_qr(
    user_id: int, channel_id: int, price: float, channel_name: str, post_id: int | None = None
) -> tuple[str, bytes, str]:
    """
    Blocking helper (runs in a thread via asyncio.to_thread).
    Returns (qr_id, qr_img_bytes, vpa).
    Caches dynamic QRs for 10 minutes so repeated clicks reuse the valid dynamic QR.
    """
    key = (user_id, channel_id)
    now = time.time()
    if key in QR_CACHE:
        cached = QR_CACHE[key]
        if now - cached["created_at"] < 600 and cached.get("price") == price:
            return cached["qr_id"], cached["qr_bytes"], cached["vpa"]

    client = rzp_client()
    notes = {
        "user_id": str(user_id),
        "channel_id": str(channel_id),
        "bot": BOT_USERNAME,
        "bot_username": BOT_USERNAME,
        "bot_id": str(BOT_ID),
        "bot_number": str(BOT_ID),
        "port": str(WEBHOOK_PORT),
        "webhook_port": str(WEBHOOK_PORT),
    }
    if post_id:
        notes["post_id"] = str(post_id)

    qr = pay.create_upi_qr(
        client,
        amount_inr=price,
        name=channel_name,
        notes=notes,
    )
    qr_id = qr["id"]
    image_url = qr.get("image_url") or f"https://api.razorpay.com/v1/l/qrcode/{qr_id}"

    try:
        qr_img_bytes = pay.download_and_crop_razorpay_qr(image_url, qr_id=qr_id)
    except Exception as exc:
        logger.error("Failed to download/crop Razorpay QR from %s: %s", image_url, exc)
        try:
            qr_img_bytes = pay.generate_clean_qr_image(f"upi://pay?am={price:.2f}&cu=INR")
        except Exception:
            qr_img_bytes = b""

    vpa = ""

    # Record in database for background polling & verification
    db.create_payment(DB_PATH, user_id, channel_id, qr_id, price)

    QR_CACHE[key] = {
        "qr_id": qr_id,
        "qr_bytes": qr_img_bytes,
        "vpa": vpa,
        "price": price,
        "created_at": now,
    }
    return qr_id, qr_img_bytes, vpa


async def get_or_create_qr_code(
    user_id: int, channel_id: int, price: float, channel_name: str, post_id: int | None = None
) -> tuple[str, bytes, str]:
    """Async wrapper — deduplicates concurrent requests per (user_id, channel_id) and runs in a thread pool."""
    key = (user_id, channel_id)
    now = time.time()
    cached = QR_CACHE.get(key)
    if cached and (now - cached["created_at"] < 600) and cached.get("price") == price:
        return cached["qr_id"], cached["qr_bytes"], cached["vpa"]

    lock = _QR_INFLIGHT_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _QR_INFLIGHT_LOCKS[key] = lock

    async with lock:
        now = time.time()
        cached = QR_CACHE.get(key)
        if cached and (now - cached["created_at"] < 600) and cached.get("price") == price:
            return cached["qr_id"], cached["qr_bytes"], cached["vpa"]
        return await asyncio.to_thread(
            _blocking_get_or_create_qr, user_id, channel_id, price, channel_name, post_id
        )


def build_qr_payment_caption(
    channel_name: str,
    price: float,
    qr_id: str,
    cross_price: float | None = None,
) -> str:
    """Short, clear QR payment caption so the user understands at a glance."""
    if cross_price and cross_price > price:
        price_str = f"<s>₹{cross_price:.0f}</s> <b>₹{price:.0f}</b>"
    else:
        price_str = f"<b>₹{price:.0f}</b>"

    return (
        f"<blockquote><b>👑 {channel_name}</b>\n"
        f"💵 <b>Pay Amount:</b> {price_str}\n"
        f"🧾 <b>Ref ID:</b> <code>{qr_id}</code></blockquote>\n"
        f"<blockquote>📲 <b>Scan QR with any UPI App</b> <i>(GPay / PhonePe / Paytm)</i>\n"
        f"⚡ <b>Channel link is sent here automatically in 3–5 sec after payment!</b>\n"
        f"⚠️ <i>One-time link • Admin: {ADMIN_CONTACT}</i></blockquote>"
    )


async def _background_send_qr(
    bot: Bot,
    user_id: int,
    chat_id: int,
    channel_id: int,
    channel_name: str,
    price: float,
    post_id: int | None = None,
    cross_price: float | None = None,
    delete_msg_id: int | None = None,
) -> None:
    """Background task: generates QR code off the hot path and sends ONLY the QR photo with a short message."""
    try:
        qr_id, qr_img_bytes, _ = await get_or_create_qr_code(
            user_id, channel_id, price, channel_name, post_id=post_id
        )
    except Exception as exc:
        logger.error("Background QR creation failed for user %d channel %d: %s", user_id, channel_id, exc)
        try:
            await bot.send_message(
                chat_id=chat_id,
                text="❌ Payment QR generation encountered a temporary delay. Please tap the buy button again.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return

    caption_text = build_qr_payment_caption(channel_name, price, qr_id, cross_price=cross_price)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("« Browse All Channels", callback_data="back_menu")],
    ])

    # Start 1.5s high-speed watcher for this QR immediately so payment is detected in ~1 second
    start_qr_fast_watcher(bot, qr_id, price)

    bio = io.BytesIO(qr_img_bytes)
    bio.name = "qr.png"
    try:
        qr_msg = await bot.send_photo(
            chat_id=chat_id,
            photo=bio,
            caption=caption_text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        if qr_msg:
            QR_MESSAGE_CACHE[user_id] = {
                "chat_id": qr_msg.chat_id,
                "message_id": qr_msg.message_id,
            }
            QR_MESSAGE_CACHE[qr_id] = {
                "chat_id": qr_msg.chat_id,
                "message_id": qr_msg.message_id,
            }
            asyncio.create_task(
                asyncio.to_thread(
                    db.store_qr_message_id, DB_PATH, qr_id, qr_msg.chat_id, qr_msg.message_id
                )
            )
        if delete_msg_id:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=delete_msg_id)
            except Exception:
                pass
    except Exception as exc:
        logger.error("Failed to send QR photo to user %d: %s", user_id, exc)


async def handle_start_buy_post(update: Update, ctx: ContextTypes.DEFAULT_TYPE, post_id: int) -> None:
    """Handle user opening bot via deep-link buy button from a channel post."""
    user = update.effective_user
    post = await db_get_post(post_id)
    if not post:
        await reply_or_send_text(update, ctx, "❌ This post or offer is no longer available. Use /menu to see all channels.")
        return

    channel_id = post["channel_id"]
    channel, already = await asyncio.gather(
        db_get_channel(channel_id),
        db_has_access(user.id, channel_id),
    )
    if not channel or not channel["is_active"]:
        await reply_or_send_text(update, ctx, "❌ This channel is currently unavailable.")
        return

    price = post["price_inr"]
    cross_price = row_get(post, "cross_price_inr")

    if already:
        await send_protected_channel_link(ctx.bot, user.id, channel)
        return

    # Check for free channel access — zero payment, direct link
    if price <= 0:
        await asyncio.to_thread(db.grant_free_access, DB_PATH, user.id, channel_id)
        await send_protected_channel_link(ctx.bot, user.id, channel)
        return

    # Send ONLY the QR code with short message (no extra message before it)
    chat_id = update.effective_chat.id if update.effective_chat else user.id
    asyncio.create_task(
        _background_send_qr(
            bot=ctx.bot,
            user_id=user.id,
            chat_id=chat_id,
            channel_id=channel_id,
            channel_name=channel["name"],
            price=price,
            post_id=post_id,
            cross_price=cross_price,
        )
    )


async def handle_start_buy_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE, channel_id: int) -> None:
    """Handle direct channel purchase deep link."""
    user = update.effective_user
    channel, already = await asyncio.gather(
        db_get_channel(channel_id),
        db_has_access(user.id, channel_id),
    )
    if not channel or not row_get(channel, "is_active", 1):
        await reply_or_send_text(update, ctx, "❌ This channel is currently unavailable.")
        return

    price = channel["price_inr"]

    if already:
        await send_protected_channel_link(ctx.bot, user.id, channel)
        return

    # Check for free channel access — zero payment, direct link
    if price <= 0:
        await asyncio.to_thread(db.grant_free_access, DB_PATH, user.id, channel_id)
        await send_protected_channel_link(ctx.bot, user.id, channel)
        return

    # Send ONLY the QR code with short message (no extra message before it)
    chat_id = update.effective_chat.id if update.effective_chat else user.id
    asyncio.create_task(
        _background_send_qr(
            bot=ctx.bot,
            user_id=user.id,
            chat_id=chat_id,
            channel_id=channel_id,
            channel_name=channel["name"],
            price=price,
        )
    )


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    # Mandatory channel check (upsert_user handled by track_user_action_fast)
    unjoined = await get_unjoined_mandatory_channels(ctx.bot, user.id)
    if unjoined:
        arg_val = ctx.args[0].strip() if ctx.args else "start"
        await send_mandatory_channels_prompt(update, ctx, unjoined, arg_val)
        return

    # Check for deep-linking arguments: /start buy_{post_id} or /start c_{channel_id}
    if ctx.args:
        arg = ctx.args[0].strip()
        if arg.startswith("buy_") or arg.startswith("post_") or arg.startswith("free_"):
            try:
                post_id = int(arg.split("_")[1])
                return await handle_start_buy_post(update, ctx, post_id)
            except (IndexError, ValueError):
                pass
        elif arg.startswith("c_"):
            try:
                channel_id = int(arg.split("_")[1])
                return await handle_start_buy_channel(update, ctx, channel_id)
            except (IndexError, ValueError):
                pass

    welcome = (
        f"<blockquote><b>🌟 Welcome, {user.first_name}! 🌟</b></blockquote>\n"
        f"<blockquote><b>💎 VIP CHANNEL ACCESS PORTAL 💎</b>\n"
        f"🚀 <i>Get instant, lifetime, protected access to top exclusive VIP Telegram channels.</i></blockquote>\n"
        f"<blockquote><b>📖 HOW IT WORKS (3 SIMPLE STEPS):</b>\n"
        f"1️⃣ <b>Browse Channels:</b> Type /menu to view all available VIP channels & pricing.\n"
        f"2️⃣ <b>Fast UPI Payment:</b> Scan the generated QR code with any UPI app (GPay/PhonePe/Paytm).\n"
        f"3️⃣ <b>Instant Delivery:</b> System automatically detects payment within seconds and sends your private invite link!</blockquote>\n"
        f"<blockquote><b>📌 QUICK SHORTCUTS:</b>\n"
        f"🛍️ /menu — Browse all channels & pricing\n"
        f"🛒 /mypurchases — Access your bought channels\n"
        f"ℹ️ /help — How to buy & FAQ guide</blockquote>\n"
        f"<blockquote>⚠️ <b>ONE-TIME LINK — NO BACKUP AVAILABLE</b>\n"
        f"──────────────────────────\n"
        f"🏆 Admin - {ADMIN_CONTACT}</blockquote>"
    )
    welcome_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛍️ Browse VIP Channels", callback_data="back_menu")],
        [InlineKeyboardButton("📖 User Guide & FAQ", callback_data="guide_help")],
    ])
    await reply_or_send_text(update, ctx, welcome, reply_markup=welcome_kb)


async def cmd_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    unjoined = await get_unjoined_mandatory_channels(ctx.bot, user.id)
    if unjoined:
        await send_mandatory_channels_prompt(update, ctx, unjoined, "menu")
        return

    channels = get_active_channels_cached()
    if not channels:
        await reply_or_send_text(update, ctx, "😔 No channels available right now. Check back soon!")
        return

    sort_by = ctx.user_data.get("menu_sort", "default") if ctx.user_data is not None else "default"
    if ctx.user_data is not None:
        ctx.user_data["menu_page"] = 0
    text = build_menu_text(len(channels), page=0)
    await reply_or_send_text(
        update, ctx,
        text,
        reply_markup=channel_list_keyboard(channels, "view", sort_by=sort_by, page=0),
    )


async def cmd_mypurchases(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user:
        uname = f"@{user.username.lstrip('@')}" if user.username else ""
    unjoined = await get_unjoined_mandatory_channels(ctx.bot, user.id)
    if unjoined:
        await send_mandatory_channels_prompt(update, ctx, unjoined, "mypurchases")
        return

    purchases = await db_get_user_purchases(user.id)
    if not purchases:
        await reply_or_send_text(
            update, ctx,
            "🛒 <b>No Active VIP Purchases Found</b>\n\n"
            "You haven't unlocked any VIP channels yet.\n"
            "👉 Tap /menu or browse below to explore our exclusive VIP channels!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🛍️ Browse VIP Channels", callback_data="back_menu")]
            ])
        )
        return

    text = (
        "<blockquote><b>👑 YOUR PURCHASED VIP CHANNELS 👑</b></blockquote>\n"
        "<blockquote>✨ Tap on any channel below to retrieve your private, protected access link:\n\n"
        "🔒 <i>Links are bound to your account and protected against sharing.</i></blockquote>"
    )
    await reply_or_send_text(
        update, ctx,
        text,
        reply_markup=build_mypurchases_keyboard(purchases, page=0),
        protect_content=True,
    )


async def cb_mypurchases_page(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Paginate /mypurchases list cleanly when a user has many purchases."""
    query = update.callback_query
    try:
        page = max(0, int(query.data.split(":", 1)[1]))
    except (IndexError, ValueError):
        page = 0
    purchases = await db_get_user_purchases(query.from_user.id)
    if not purchases:
        await query.answer("No purchases found.")
        return
    total_pages = max(1, (len(purchases) + PURCHASES_PAGE_SIZE - 1) // PURCHASES_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    await query.answer(f"📄 Page {page + 1}/{total_pages}")
    try:
        await query.edit_message_reply_markup(
            reply_markup=build_mypurchases_keyboard(purchases, page=page)
        )
    except Exception:
        pass


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Detailed user and admin instruction guide with advanced formatting and emojis."""
    uid = update.effective_user.id
    admin_status = is_admin(uid)

    guide = (
        "<blockquote><b>📖 BOT INSTRUCTIONS & USER GUIDE 📖</b></blockquote>\n"
        "<blockquote>Welcome to the automated <b>Channel Link Provider</b>! Here is your complete guide:\n\n"
        "🛍️ <b>HOW TO PURCHASE VIP ACCESS:</b>\n"
        "1️⃣ <b>Browse:</b> Open /menu or tap the buy button on any channel post.\n"
        "2️⃣ <b>QR Code:</b> The bot generates an instant, cropped UPI QR code for the exact price.\n"
        "3️⃣ <b>Scan with Any UPI App:</b> Open PhonePe, Google Pay, Paytm, BHIM, or CRED.\n"
        "4️⃣ <b>Instant Auto-Delivery:</b> Payment is auto-detected within 3–5 seconds and your private invite link is delivered instantly!\n"
        "5️⃣ <b>Join Promptly:</b> Tap '🚀 Join VIP Channel Now' to enter.</blockquote>\n"
        "<blockquote>🔒 <b>ACCESS & SECURITY:</b>\n"
        "• <b>One-Time Access:</b> Invite links are personal and bound to your Telegram account.\n"
        "• <b>Anti-Leak Protection:</b> Forwarded links cannot be used by others.\n"
        "• <b>Active Purchases:</b> Use /mypurchases anytime to retrieve your active purchased links.</blockquote>\n"
        "<blockquote>📌 <b>AVAILABLE USER COMMANDS:</b>\n"
        "• /start — Welcome portal & quick actions\n"
        "• /menu — Browse VIP channels & pricing\n"
        "• /mypurchases — View your active channel links\n"
        "• /help — Show this instructions guide\n"
        "──────────────────────────\n"
        f"🏆 Admin - {ADMIN_CONTACT}</blockquote>"
    )

    if admin_status:
        admin_section = (
            "\n<blockquote>👑 <b>ADMIN COMMANDS & SHORTCUTS:</b>\n"
            "• /post — Create & broadcast promotional post to main channel\n"
            "• /posts — View recent broadcast posts\n"
            "• /setmainchannel — Set default channel for posts\n"
            "• /addchannel — Add new VIP channel product\n"
            "• /editchannel — Edit channel details/price/link\n"
            "• /delchannel — Deactivate a channel\n"
            "• /channels — List all channels\n"
            "• /checklinks — Validate channel links & get working links\n"
            "• /newlinks — Compare links with active channels & get unadded links\n"
            "• /stats — Full sales & revenue dashboard\n"
            "• /users — View recent registered users\n"
            "• /payments — View channel payments\n"
            "• /check &lt;id&gt; — Inspect payment or QR ID live\n"
            "• /grantaccess — Grant free access to user\n"
            "• /broadcast — Send message to all bot users\n"
            "• /ban / /unban — Manage user blacklist</blockquote>"
        )
        guide += admin_section

    help_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛍️ Browse VIP Channels", callback_data="back_menu")],
    ])
    await reply_or_send_text(update, ctx, guide, reply_markup=help_kb)


async def cb_guide_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback handler when user taps Guide & FAQ button."""
    query = update.callback_query
    await query.answer()
    await cmd_help(update, ctx)


# ──────────────────────────────────────────────────────────────────────────────
# CHANNEL VIEW / BUY (inline buttons)
# ──────────────────────────────────────────────────────────────────────────────

async def cb_view_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    channel_id = int(query.data.split(":")[1])
    # Run channel fetch and mandatory check concurrently for speed
    channel, unjoined = await asyncio.gather(
        db_get_channel(channel_id),
        get_unjoined_mandatory_channels(ctx.bot, query.from_user.id),
    )
    if not channel or not row_get(channel, "is_active", 1):
        await query.edit_message_text("❌ Channel not found or no longer available.")
        return

    user_id = query.from_user.id
    if unjoined:
        await send_mandatory_channels_prompt(query, ctx, unjoined, f"c_{channel_id}")
        return

    already = await db_has_access(user_id, channel_id)
    if already:
        await send_protected_channel_link(ctx.bot, user_id, channel)
        return

    desc = channel["description"] or "🌟 Exclusive VIP Access • Daily Premium Content, Media & Updates 🚀"
    if channel["price_inr"] <= 0:
        status_line = "🎁 <b>Access Fee:</b> <b>100% Free Channel (₹0)</b>"
        action_btn = InlineKeyboardButton("🎁 Claim Free Instant Access 🚀", callback_data=f"buy:{channel_id}")
    else:
        status_line = f"💰 <b>Access Fee:</b> <b>₹{channel['price_inr']:.0f}</b> <i>(One-time payment)</i>"
        action_btn = InlineKeyboardButton(f"💳 Pay ₹{channel['price_inr']:.0f} via UPI QR 🚀", callback_data=f"buy:{channel_id}")

    text = (
        f"<blockquote><b>👑 {channel['name']}</b></blockquote>\n"
        f"<blockquote>📖 <b>Description & Perks:</b>\n"
        f"<i>{desc}</i>\n\n"
        f"💎 <b>Key Highlights:</b>\n"
        f"• 🚀 <i>Instant 1-Click Access</i>\n"
        f"• 🔒 <i>Private, Verified & Safe Content</i>\n"
        f"• ⚡ <i>Zero Waiting Time • Auto-Processed</i>\n"
        f"──────────────────────────\n"
        f"{status_line}</blockquote>\n"
        f"<blockquote>⚠️ <b>ONE-TIME LINK — NO BACKUP AVAILABLE</b>\n"
        f"──────────────────────────\n"
        f"🏆 Admin - {ADMIN_CONTACT}</blockquote>"
    )

    keyboard = InlineKeyboardMarkup([
        [action_btn],
        [InlineKeyboardButton("« Back to Channels", callback_data="back_menu")],
    ])

    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def cb_get_link(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    channel_id = int(query.data.split(":")[1])
    user_id = query.from_user.id

    if not await db_has_access(user_id, channel_id):
        await query.answer("❌ You don't have access. Please purchase first.", show_alert=True)
        return

    channel = await db_get_channel(channel_id)
    await send_protected_channel_link(ctx.bot, user_id, channel)


async def cb_buy_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    channel_id = int(query.data.split(":")[1])
    user_id = query.from_user.id
    unjoined, channel, already = await asyncio.gather(
        get_unjoined_mandatory_channels(ctx.bot, user_id),
        db_get_channel(channel_id),
        db_has_access(user_id, channel_id),
    )
    if unjoined:
        await send_mandatory_channels_prompt(query, ctx, unjoined, f"c_{channel_id}")
        return

    if not channel or not row_get(channel, "is_active", 1):
        await query.edit_message_text("❌ This channel is no longer available.")
        return

    if already:
        await send_protected_channel_link(ctx.bot, user_id, channel)
        return

    price = channel["price_inr"]
    if price <= 0:
        await asyncio.to_thread(db.grant_free_access, DB_PATH, user_id, channel_id)
        await query.answer("🎉 Free access granted!", show_alert=True)
        try:
            await query.edit_message_text(
                f"🎉 <b>Instant Free Access Granted to {channel['name']}!</b>\n\n"
                f"Your private access link has been delivered to your chat below. 🚀",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        await send_protected_channel_link(ctx.bot, user_id, channel)
        return

    # Send ONLY the QR code with short message and delete the menu message once QR arrives
    chat_id = query.message.chat_id if query.message else user_id
    delete_msg_id = query.message.message_id if query.message else None
    asyncio.create_task(
        _background_send_qr(
            bot=ctx.bot,
            user_id=user_id,
            chat_id=chat_id,
            channel_id=channel_id,
            channel_name=channel["name"],
            price=price,
            delete_msg_id=delete_msg_id,
        )
    )


async def cb_verify_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt user to enter their Razorpay Payment ID."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    channel_id = int(parts[1])
    order_id = parts[2]

    ctx.user_data["verify_channel_id"] = channel_id
    ctx.user_data["verify_order_id"] = order_id

    await query.message.reply_text(
        "🔍 <b>Manual Payment Verification</b>\n\n"
        "Please send your <b>Payment Transaction ID</b>.\n"
        "You can find it in the UPI payment confirmation SMS or app history.\n"
        "It looks like: <code>pay_XXXXXXXXXXXXXXXX</code>\n\n"
        "Type /cancel to abort.",
        parse_mode=ParseMode.HTML,
    )
    return VERIFY_ID


async def handle_verify_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive payment ID, verify and grant access."""
    payment_id = update.message.text.strip()
    user_id = update.effective_user.id
    channel_id = ctx.user_data.get("verify_channel_id")
    order_id = ctx.user_data.get("verify_order_id")

    if not channel_id or not order_id:
        await update.message.reply_text("❌ Session expired. Please start from /menu.")
        return ConversationHandler.END

    if not payment_id.startswith("pay_"):
        await update.message.reply_text(
            "⚠️ That doesn't look like a valid Transaction ID.\n"
            "It should start with <code>pay_</code>. Please try again.",
            parse_mode=ParseMode.HTML,
        )
        return VERIFY_ID

    await update.message.reply_text("⏳ Verifying your payment…")

    try:
        client = rzp_client()
        payment_data = await asyncio.to_thread(pay.fetch_payment, client, payment_id)
        rzp_order_id = payment_data.get("order_id", "")

        if rzp_order_id != order_id:
            await update.message.reply_text(
                "❌ Payment ID doesn't match this order. "
                "Please use the correct Payment ID."
            )
            return VERIFY_ID

        status = payment_data.get("status", "")
        if status not in ("captured", "authorized"):
            await update.message.reply_text(
                f"⚠️ Payment status is <b>{status}</b>. "
                "Only captured/authorized payments are accepted.",
                parse_mode=ParseMode.HTML,
            )
            return VERIFY_ID

        # Grant access
        await asyncio.to_thread(db.confirm_payment, DB_PATH, order_id, payment_id)
        channel = await db_get_channel(channel_id)

        await send_protected_channel_link(ctx.bot, user_id, channel)
        ctx.user_data.clear()
        return ConversationHandler.END

    except Exception as exc:
        logger.error("Verification error: %s", exc)
        await update.message.reply_text(
            "❌ Could not verify payment. Please try again or contact support."
        )
        return VERIFY_ID


async def cb_back_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    unjoined = await get_unjoined_mandatory_channels(ctx.bot, query.from_user.id)
    if unjoined:
        return await send_mandatory_channels_prompt(query, ctx, unjoined, "menu")

    channels = get_active_channels_cached()
    sort_by = ctx.user_data.get("menu_sort", "default") if ctx.user_data is not None else "default"
    page = ctx.user_data.get("menu_page", 0) if ctx.user_data is not None else 0
    if channels:
        total_pages = max(1, (len(channels) + MENU_PAGE_SIZE - 1) // MENU_PAGE_SIZE)
        page = max(0, min(int(page), total_pages - 1))
        if ctx.user_data is not None:
            ctx.user_data["menu_page"] = page
        text = build_menu_text(len(channels), page=page)
        markup = channel_list_keyboard(channels, "view", sort_by=sort_by, page=page)
    else:
        text = "😔 No channels available right now."
        markup = None

    # Only edit in-place if the source message actually has text (avoids wasted
    # 400 BadRequest "There is no text in the message to edit" on QR/photo messages).
    if query.message and query.message.text:
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
            return
        except Exception:
            pass

    try:
        if query.message:
            await query.message.delete()
    except Exception:
        pass
    await reply_or_send_text(update, ctx, text, reply_markup=markup)


async def cb_menu_sort(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle sorting the /menu channel list by price (Low -> High or High -> Low)."""
    query = update.callback_query
    sort_mode = query.data.split(":", 1)[1] if ":" in query.data else "default"
    current_sort = ctx.user_data.get("menu_sort", "default") if ctx.user_data is not None else "default"
    if sort_mode == current_sort:
        sort_mode = "default"
    if ctx.user_data is not None:
        ctx.user_data["menu_sort"] = sort_mode
        ctx.user_data["menu_page"] = 0

    if sort_mode == "price_asc":
        await query.answer("💰 Sorted by Price: Low ➔ High")
    elif sort_mode == "price_desc":
        await query.answer("💎 Sorted by Price: High ➔ Low")
    else:
        await query.answer("📋 Restored Default Order")

    channels = get_active_channels_cached()
    if not channels:
        return

    text = build_menu_text(len(channels), page=0)
    markup = channel_list_keyboard(channels, "view", sort_by=sort_mode, page=0)
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    except Exception:
        try:
            await query.edit_message_reply_markup(reply_markup=markup)
        except Exception:
            pass


async def cb_menu_page(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle Prev/Next pagination on the /menu channel directory."""
    query = update.callback_query
    try:
        page = max(0, int(query.data.split(":", 1)[1]))
    except (IndexError, ValueError):
        page = 0

    channels = get_active_channels_cached()
    if not channels:
        await query.answer("No channels available.")
        return

    total_pages = max(1, (len(channels) + MENU_PAGE_SIZE - 1) // MENU_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    sort_by = ctx.user_data.get("menu_sort", "default") if ctx.user_data is not None else "default"
    if ctx.user_data is not None:
        ctx.user_data["menu_page"] = page

    await query.answer(f"📄 Page {page + 1}/{total_pages}")
    text = build_menu_text(len(channels), page=page)
    markup = channel_list_keyboard(channels, "view", sort_by=sort_by, page=page)
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    except Exception:
        try:
            await query.edit_message_reply_markup(reply_markup=markup)
        except Exception:
            pass


async def cb_menu_noop(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Acknowledge non-interactive page indicator button presses."""
    query = update.callback_query
    if query:
        await query.answer()


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data.clear()
    await update.message.reply_text("❌ Operation cancelled.")
    return ConversationHandler.END


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — ADD CHANNEL (conversation)
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_addchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await update.message.reply_text(
        "➕ <b>Add New Channel</b>\n\n"
        "Step 1/3 — Enter the channel <b>name</b>:\n\n"
        "✨ <i>Description and emojis will be generated automatically!</i>",
        parse_mode=ParseMode.HTML,
    )
    return AC_NAME


async def ac_get_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data["ac_name"] = update.message.text.strip()
    await update.message.reply_text("Step 2/3 — Enter the channel <b>invite link</b>:", parse_mode=ParseMode.HTML)
    return AC_LINK


async def ac_get_link(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    link = update.message.text.strip()
    link_low = link.lower()
    if not (
        link_low.startswith(("http://", "https://", "t.me/", "www.t.me/", "telegram.me/"))
        or (link.startswith("@") and len(link) >= 5)
    ):
        await update.message.reply_text("⚠️ Please enter a valid URL starting with http/https, t.me/, or @username.")
        return AC_LINK
    if link.startswith("@"):
        link = f"https://t.me/{link.lstrip('@')}"
    elif not link_low.startswith(("http://", "https://")):
        link = f"https://{link}"

    is_alive, reason = await check_channel_link_alive(link, bot=ctx.bot)
    if not is_alive:
        await update.message.reply_text(
            f"❌ <b>This channel link appears to be expired or the channel is banned!</b>\n\n"
            f"⚠️ Reason: {reason}\n\n"
            f"Please enter a valid, active channel invite link:",
            parse_mode=ParseMode.HTML,
        )
        return AC_LINK

    ctx.user_data["ac_link"] = link
    await update.message.reply_text("Step 3/3 — Enter the <b>price in ₹ (INR)</b>:", parse_mode=ParseMode.HTML)
    return AC_PRICE


async def ac_get_price(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        price = float(update.message.text.strip())
        if price <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("⚠️ Please enter a valid positive number for the price.")
        return AC_PRICE
    ctx.user_data["ac_price"] = price

    # Auto-generate description with emojis from channel name — no description asked to admin!
    name = ctx.user_data.get("ac_name", "this channel")
    auto_desc = (
        f"🌟 Exclusive VIP Access to <b>{name}</b>!\n"
        f"📲 Premium content • Daily updates • Members-only posts\n"
        f"🔒 Private • Verified • Instant auto-approved access"
    )
    ctx.user_data["ac_desc"] = auto_desc

    await update.message.reply_text(
        f"✅ Description auto-generated with emojis:\n<i>{auto_desc}</i>\n\n"
        "🛡️ <b>Anti-Leak Protection (Optional):</b>\n"
        "Enter the <b>Channel Chat ID</b> (e.g. <code>-1001234567890</code>):\n\n"
        "<i>Adding this lets the bot issue Join-Request verification links so only paying users get in! "
        "Type <code>skip</code> to use the standard invite link directly.</i>\n\n"
        "💡 <i>Tip: Forward any message from the private channel here, or use /getid.</i>",
        parse_mode=ParseMode.HTML,
    )
    return AC_CHAT_ID


async def ac_get_desc(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    # This step is now skipped — kept for backwards compatibility with ConversationHandler
    text = update.message.text.strip()
    ctx.user_data["ac_desc"] = "" if text.lower() == "skip" else text
    await update.message.reply_text(
        "Step 5/5 — Enter the <b>Channel Chat ID</b> (e.g. <code>-1001234567890</code>) for Anti-Leak Protection:\n\n"
        "🛡️ <i>Type <code>skip</code> if you want to use the standard invite link directly.</i>",
        parse_mode=ParseMode.HTML,
    )
    return AC_CHAT_ID


async def ac_get_chat_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    chat_id = None if text.lower() in ("skip", "none", "no") else text
    admin_id = update.effective_user.id

    channel_id = db.add_channel(
        DB_PATH,
        ctx.user_data["ac_name"],
        ctx.user_data["ac_link"],
        ctx.user_data["ac_price"],
        ctx.user_data["ac_desc"],
        admin_id,
        chat_id=chat_id,
    )
    invalidate_channel_cache()
    ctx.user_data.clear()

    protection_status = "<b>Enabled ✅ (Join-Request Firewall Active)</b>" if chat_id else "<i>Disabled (Standard link)</i>"
    await update.message.reply_text(
        f"✅ <b>Channel added!</b> (ID: <code>{channel_id}</code>)\n\n"
        f"🛡️ Anti-Leak Protection: {protection_status}\n\n"
        "📢 Ready to broadcast? Use /post to publish an announcement to your Main Channel with a buy button!",
        parse_mode=ParseMode.HTML,
    )
    return ConversationHandler.END


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — EDIT CHANNEL (conversation)
# ──────────────────────────────────────────────────────────────────────────────

def _build_edit_channel_card(channel) -> tuple[str, InlineKeyboardMarkup]:
    """Build the channel edit card text (with Name + #ID tag) and action keyboard."""
    channel_id = channel["id"]
    safe_name = html.escape((channel["name"] or f"Channel #{channel_id}").strip())
    safe_link = html.escape((channel["link"] or "").strip())
    price_val = float(row_get(channel, "price_inr", 0) or 0)
    price_str = "FREE" if price_val <= 0 else f"₹{price_val:.0f}"
    buttons = [
        [InlineKeyboardButton("📛 Name",        callback_data="edit_field:name")],
        [InlineKeyboardButton("🔗 Link",        callback_data="edit_field:link")],
        [InlineKeyboardButton("💰 Price (₹)",   callback_data="edit_field:price_inr")],
        [InlineKeyboardButton("📝 Description", callback_data="edit_field:description")],
        [InlineKeyboardButton("🛡️ Chat ID",     callback_data="edit_field:chat_id")],
    ]
    if not channel["is_active"]:
        buttons.append([
            InlineKeyboardButton("🟢 Activate & Repost to Main Channel", callback_data="edit_field:toggle_active")
        ])
    else:
        buttons.append([
            InlineKeyboardButton("📢 Repost to Main Channel (Keep Buyers)", callback_data="edit_field:repost_main")
        ])
        buttons.append([
            InlineKeyboardButton("🔴 Deactivate", callback_data="edit_field:toggle_active")
        ])

    status_str = "✅ Active" if channel["is_active"] else "❌ Inactive"
    text = (
        f"✏️ <b>Editing Channel:</b> <b>{safe_name}</b> (<code>#{channel_id}</code>)\n"
        f"• <b>Channel ID:</b> <code>#{channel_id}</code> • <b>Price:</b> {price_str} • {status_str}\n"
        f"• <b>Link:</b> <code>{safe_link}</code>\n\n"
        f"What do you want to change?"
    )
    return text, InlineKeyboardMarkup(buttons)


async def cmd_editchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    # Support direct jump: /editchannel 67 or /editchannel #67
    if ctx.args:
        raw_id = ctx.args[0].strip().lstrip("#")
        if raw_id.isdigit():
            channel_id = int(raw_id)
            channel = db.get_channel(DB_PATH, channel_id)
            if not channel:
                await update.message.reply_text(
                    f"❌ Channel <code>#{channel_id}</code> not found.",
                    parse_mode=ParseMode.HTML,
                )
                return ConversationHandler.END
            ctx.user_data["edit_channel_id"] = channel_id
            text, keyboard = _build_edit_channel_card(channel)
            await update.message.reply_text(
                text, parse_mode=ParseMode.HTML, reply_markup=keyboard, disable_web_page_preview=True
            )
            return EC_FIELD

    channels = db.get_all_channels(DB_PATH)
    if not channels:
        await update.message.reply_text("No channels to edit.")
        return ConversationHandler.END
    await update.message.reply_text(
        f"✏️ <b>Edit Channel</b> ({len(channels)} total)\n\n"
        f"Select a channel below (or send <code>/editchannel #ID</code> to jump directly):",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_channel_keyboard(channels, "edit_select", page=0),
    )
    return EC_FIELD


async def cb_edit_select(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    channel_id = int(query.data.split(":")[1])
    ctx.user_data["edit_channel_id"] = channel_id
    channel = db.get_channel(DB_PATH, channel_id)
    if not channel:
        await query.edit_message_text("❌ Channel not found.")
        return ConversationHandler.END

    text, keyboard = _build_edit_channel_card(channel)
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )
    return EC_FIELD


async def cb_edit_field(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    field = query.data.split(":")[1]
    ctx.user_data["edit_field"] = field
    channel_id = ctx.user_data["edit_channel_id"]
    channel = db.get_channel(DB_PATH, channel_id)
    safe_ch_name = html.escape((channel["name"] if channel else f"Channel #{channel_id}") or f"Channel #{channel_id}")

    if field in ("toggle_active", "repost_main"):
        if not channel:
            await query.edit_message_text("❌ Channel not found.")
            return ConversationHandler.END

        if field == "toggle_active" and channel["is_active"]:
            db.update_channel(DB_PATH, channel_id, is_active=0)
            invalidate_channel_cache()
            await query.edit_message_text(
                f"🔴 Channel <b>{safe_ch_name}</b> (<code>#{channel_id}</code>) deactivated ❌",
                parse_mode=ParseMode.HTML,
            )
            return ConversationHandler.END

        # Reactivating (or reposting to Main Channel): activate & republish post with original image!
        await query.edit_message_text(
            f"⏳ Reactivating <b>{safe_ch_name}</b> (<code>#{channel_id}</code>) and sending post to Main Channel...",
            parse_mode=ParseMode.HTML,
        )
        ok, info, msg_id = await republish_channel_post(ctx.bot, channel_id)
        if ok:
            await query.edit_message_text(
                f"✅ <b>{safe_ch_name}</b> (<code>#{channel_id}</code>) Activated &amp; Posted to Main Channel!\n\n"
                f"• <b>Channel:</b> <b>{safe_ch_name}</b> (<code>#{channel_id}</code>)\n"
                f"• <b>Main Channel:</b> <code>{html.escape(str(info))}</code> (Message ID: <code>{msg_id}</code>)\n"
                f"• <b>Previous Buyers:</b> Kept! (They directly receive the channel link — no QR code)\n"
                f"• <b>New Users:</b> Shown QR code to purchase",
                parse_mode=ParseMode.HTML,
            )
            ctx.user_data.clear()
            return ConversationHandler.END

        if info == "NEED_MEDIA":
            ctx.user_data["edit_field"] = "repost_media"
            await query.edit_message_text(
                f"🟢 <b>{safe_ch_name}</b> (<code>#{channel_id}</code>) is now Active!\n\n"
                f"📸🎬 <b>Send the Photo or Video</b> to publish this channel's post to the Main Channel "
                f"(or type <code>skip</code> for a text-only post):\n\n"
                f"✨ <i>Uses the exact same Channel ID (<code>#{channel_id}</code>), so all previous buyers "
                f"automatically get the direct channel link without seeing a QR code!</i>",
                parse_mode=ParseMode.HTML,
            )
            return EC_VALUE

        await query.edit_message_text(
            f"🟢 <b>{safe_ch_name}</b> (<code>#{channel_id}</code>) is now Active ✅, but could not auto-post to Main Channel:\n"
            f"<code>{html.escape(str(info))}</code>",
            parse_mode=ParseMode.HTML,
        )
        ctx.user_data.clear()
        return ConversationHandler.END

    labels = {
        "name": "new name",
        "link": "new invite link (will also reactivate & repost if inactive)",
        "price_inr": "new price in ₹",
        "description": "new description",
        "chat_id": "Channel Chat ID (e.g. -1001234567890 or type 'none' to remove)",
    }
    await query.message.reply_text(
        f"✏️ Editing <b>{safe_ch_name}</b> (<code>#{channel_id}</code>)\n"
        f"Enter the {labels.get(field, field)}:",
        parse_mode=ParseMode.HTML,
    )
    return EC_VALUE


async def ec_get_value(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    field = ctx.user_data.get("edit_field")
    channel_id = ctx.user_data.get("edit_channel_id")
    msg = update.message

    if field == "repost_media":
        photo_file_id = None
        media_type = None
        if msg.photo:
            photo_file_id = msg.photo[-1].file_id
            media_type = "photo"
        elif msg.video:
            photo_file_id = msg.video.file_id
            media_type = "video"
        elif msg.animation:
            photo_file_id = msg.animation.file_id
            media_type = "animation"
        elif msg.document and msg.document.mime_type and msg.document.mime_type.startswith("video/"):
            photo_file_id = msg.document.file_id
            media_type = "video"
        elif msg.text and msg.text.strip().lower() in ("skip", "/skip"):
            photo_file_id = None
        else:
            await msg.reply_text("⚠️ Please send a Photo, Video, or type <code>skip</code>.", parse_mode=ParseMode.HTML)
            return EC_VALUE

        ok, info, msg_id = await republish_channel_post(
            ctx.bot,
            channel_id,
            custom_photo_file_id=photo_file_id,
            custom_media_type=media_type,
            allow_text_only=True,
        )
        ch_after = db.get_channel(DB_PATH, channel_id)
        safe_ch_name = html.escape((ch_after["name"] if ch_after else f"Channel #{channel_id}") or f"Channel #{channel_id}")
        ctx.user_data.clear()
        if ok:
            await msg.reply_text(
                f"🎉 <b>Post Published to Main Channel!</b>\n\n"
                f"• <b>Channel:</b> <b>{safe_ch_name}</b> (<code>#{channel_id}</code>)\n"
                f"• <b>Target Channel:</b> <code>{html.escape(str(info))}</code> (Message ID: <code>{msg_id}</code>)\n"
                f"• <b>Previous Buyers:</b> Kept! (Direct link sent — no QR code)\n"
                f"• <b>New Users:</b> Shown QR code to purchase",
                parse_mode=ParseMode.HTML,
            )
        else:
            await msg.reply_text(f"❌ Could not publish post: <code>{html.escape(str(info))}</code>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    if not msg.text:
        await msg.reply_text("⚠️ Please enter text.")
        return EC_VALUE

    value = msg.text.strip()

    if field == "price_inr":
        try:
            value = float(value)
        except ValueError:
            await update.message.reply_text("⚠️ Enter a valid number.")
            return EC_VALUE
    elif field == "chat_id":
        if value.lower() in ("none", "clear", "skip"):
            value = None

    was_channel = db.get_channel(DB_PATH, channel_id)
    was_inactive = was_channel and not was_channel["is_active"]

    if field == "link":
        db.update_channel(DB_PATH, channel_id, link=value, is_active=1)
        invalidate_channel_cache()
        updated_ch = db.get_channel(DB_PATH, channel_id) or was_channel
        safe_ch_name = html.escape((updated_ch["name"] if updated_ch else f"Channel #{channel_id}") or f"Channel #{channel_id}")
        if was_inactive:
            ok, info, msg_id = await republish_channel_post(ctx.bot, channel_id)
            if ok:
                ctx.user_data.clear()
                await update.message.reply_text(
                    f"✅ <b>Link updated, channel reactivated, and post re-published to Main Channel!</b>\n\n"
                    f"• <b>Channel:</b> <b>{safe_ch_name}</b> (<code>#{channel_id}</code>)\n"
                    f"• <b>Target Channel:</b> <code>{html.escape(str(info))}</code> (Message ID: <code>{msg_id}</code>)\n"
                    f"• <b>Previous Buyers:</b> Kept! (No QR code shown to existing buyers)",
                    parse_mode=ParseMode.HTML,
                )
                return ConversationHandler.END
    else:
        db.update_channel(DB_PATH, channel_id, **{field: value})
        invalidate_channel_cache()

    updated_ch = db.get_channel(DB_PATH, channel_id) or was_channel
    safe_ch_name = html.escape((updated_ch["name"] if updated_ch else f"Channel #{channel_id}") or f"Channel #{channel_id}")
    ctx.user_data.clear()
    await update.message.reply_text(
        f"✅ <b>Channel updated successfully!</b>\n\n"
        f"🏷️ <b>Channel:</b> <b>{safe_ch_name}</b> (<code>#{channel_id}</code>)",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📢 Repost #{channel_id} to Main Channel Now", callback_data=f"reactivate_ch:{channel_id}")]
        ]),
    )
    return ConversationHandler.END


async def cmd_reactivate(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command /reactivate to one-click reactivate & repost deleted channels to Main Channel."""
    if not is_admin(update.effective_user.id):
        return

    await asyncio.to_thread(db.recover_deleted_posts_from_sqlite_bytes, DB_PATH)
    channels = await asyncio.to_thread(db.get_all_channels, DB_PATH)
    if not channels:
        await update.message.reply_text("ℹ️ No channels found in database.")
        return

    conn = db.get_connection(DB_PATH)
    post_rows = conn.execute(
        "SELECT channel_id, MAX(id) as pid, MAX(channel_msg_id) as msg_id, MAX(photo_file_id) as fid FROM posts GROUP BY channel_id"
    ).fetchall()
    post_map = {r["channel_id"]: r for r in post_rows}

    needs_repost = []
    other_channels = []
    for c in channels:
        cid = c["id"]
        pr = post_map.get(cid)
        is_act = bool(c["is_active"])
        has_live_msg = bool(pr and pr["msg_id"])
        has_img = bool(pr and pr["fid"])
        if not is_act or not has_live_msg:
            needs_repost.append((c, is_act, has_img))
        else:
            other_channels.append((c, is_act, has_img))

    buttons = []
    if needs_repost:
        buttons.append([
            InlineKeyboardButton(
                f"🚀 Restore & Repost All ({len(needs_repost)}) Missing Posts Now",
                callback_data="reactivate_ch:all",
            )
        ])
        for c, is_act, has_img in needs_repost[:25]:
            img_tag = "🖼️" if has_img else "📝"
            st_tag = "🔴" if not is_act else "⚠️"
            buttons.append([
                InlineKeyboardButton(
                    f"{st_tag} {img_tag} Reactivate & Repost: {c['name']} (₹{c['price_inr']:.0f})",
                    callback_data=f"reactivate_ch:{c['id']}",
                )
            ])

    for c, is_act, has_img in other_channels[-10:]:
        img_tag = "🖼️" if has_img else "📝"
        buttons.append([
            InlineKeyboardButton(
                f"✅ {img_tag} Repost Again: {c['name']} (₹{c['price_inr']:.0f})",
                callback_data=f"reactivate_ch:{c['id']}",
            )
        ])

    await update.message.reply_text(
        "🔄 <b>Reactivate & Repost Channels to Main Channel</b>\n\n"
        "• Tap any channel below to <b>reactivate it and re-send its post (with original image & price)</b> to the Main Channel.\n"
        "• <b>Previous Buyers Protected:</b> Users who already bought that channel will <b>directly receive the channel link</b> (no QR code).\n"
        "• <b>New Users:</b> Will receive a QR code to purchase.\n\n"
        f"📊 <b>Channels missing a Main Channel post:</b> <code>{len(needs_repost)}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
    )


async def cb_reactivate_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle one-click Reactivate & Repost button from /reactivate, /editchannel, or admin alerts."""
    query = update.callback_query
    await query.answer()
    if not is_admin(update.effective_user.id):
        return

    action_prefix, arg = query.data.split(":", 1)
    allow_text_only = (action_prefix == "reactivate_text")
    if arg == "all":
        await asyncio.to_thread(db.recover_deleted_posts_from_sqlite_bytes, DB_PATH)
        channels = await asyncio.to_thread(db.get_all_channels, DB_PATH)
        conn = db.get_connection(DB_PATH)
        post_rows = conn.execute(
            "SELECT channel_id, MAX(channel_msg_id) as msg_id, MAX(photo_file_id) as fid FROM posts GROUP BY channel_id"
        ).fetchall()
        post_map = {r["channel_id"]: r for r in post_rows}

        targets = [
            c for c in channels
            if (not c["is_active"]) or (c["id"] in post_map and not post_map[c["id"]]["msg_id"])
        ]
        if not targets:
            await query.edit_message_text("✅ All channels with saved posts are already active and posted!")
            return

        await query.edit_message_text(
            f"⏳ <b>Restoring & re-publishing {len(targets)} channel(s) to Main Channel...</b>",
            parse_mode=ParseMode.HTML,
        )
        restored = []
        need_media = []
        for c in targets:
            ok, info, msg_id = await republish_channel_post(ctx.bot, c["id"])
            if ok:
                restored.append(f"✅ <b>{html.escape(c['name'] or '')}</b> (Msg #{msg_id})")
                await asyncio.sleep(1.0)
            elif info == "NEED_MEDIA":
                need_media.append(f"📸 <b>{html.escape(c['name'] or '')}</b> (use /editchannel → select {html.escape(c['name'] or '')} → Repost to attach image)")
            else:
                need_media.append(f"⚠️ <b>{html.escape(c['name'] or '')}</b>: {html.escape(str(info))}")

        summary = [f"🎉 <b>Repost Complete! ({len(restored)}/{len(targets)} published)</b>\n"]
        if restored:
            summary.append("\n".join(restored))
        if need_media:
            summary.append("\n<b>Needs Image / Attention:</b>\n" + "\n".join(need_media))
        summary.append(
            "\n\n✅ <i>All previous buyers retain their access and will directly get the link without seeing a QR code!</i>"
        )
        await query.edit_message_text("\n".join(summary), parse_mode=ParseMode.HTML)
        return

    channel_id = int(arg)
    channel = await db_get_channel(channel_id)
    if not channel:
        await query.answer("❌ Channel not found.", show_alert=True)
        return

    # Check if this button came from a multi-channel alert so we don't wipe buttons for other channels
    existing_rows = (
        query.message.reply_markup.inline_keyboard
        if (query.message and query.message.reply_markup)
        else []
    )
    reactivate_targets = {
        btn.callback_data
        for row in existing_rows
        for btn in row
        if btn.callback_data and btn.callback_data.startswith("reactivate_ch:") and btn.callback_data != "reactivate_ch:all"
    }
    is_multi_alert = len(reactivate_targets) > 1

    ch_safe_name = html.escape(channel["name"] or f"Channel #{channel_id}")
    if not is_multi_alert:
        await query.edit_message_text(
            f"⏳ Reactivating <b>{ch_safe_name}</b> and sending post to Main Channel...",
            parse_mode=ParseMode.HTML,
        )

    ok, info, msg_id = await republish_channel_post(
        ctx.bot, channel_id, allow_text_only=allow_text_only
    )
    if ok:
        result_text = (
            f"✅ <b>{ch_safe_name} Activated &amp; Re-Posted to Main Channel!</b>\n\n"
            f"• <b>Channel ID:</b> <code>#{channel_id}</code>\n"
            f"• <b>Main Channel:</b> <code>{html.escape(str(info))}</code> (Message ID: <code>{msg_id}</code>)\n"
            f"• <b>/menu Status:</b> 🟢 Active\n"
            f"• <b>Previous Buyers:</b> Protected! (They directly get the channel link — no QR code)\n"
            f"• <b>New Users:</b> Shown QR code to purchase"
        )
        result_markup = None
    elif info == "NEED_MEDIA":
        result_text = (
            f"🟢 <b>{ch_safe_name} (ID #{channel_id}) is now Active in /menu!</b>\n\n"
            f"ℹ️ No saved image/video was found for its old post. You can either:\n"
            f"• Tap <b>📝 Repost Text-Only Post Now</b> below to resend immediately without an image, OR\n"
            f"• Run <code>/editchannel</code> → select <b>{ch_safe_name}</b> → tap <b>📢 Repost to Main Channel</b> to attach a fresh image."
        )
        result_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"📝 Repost Text-Only Post Now (#{channel_id})",
                callback_data=f"reactivate_text:{channel_id}",
            )]
        ])
    else:
        result_text = (
            f"⚠️ Channel <b>{ch_safe_name}</b> (<code>#{channel_id}</code>) activated, "
            f"but could not post to Main Channel: <code>{html.escape(str(info))}</code>"
        )
        result_markup = None

    if is_multi_alert and query.message:
        # Update only this channel's button in the multi-channel alert & reply with full confirmation
        if ok:
            new_rows = []
            for row in existing_rows:
                new_row = []
                for btn in row:
                    if btn.callback_data == f"reactivate_ch:{channel_id}":
                        new_row.append(InlineKeyboardButton(
                            f"✅ Reactivated & Reposted #{channel_id} (Msg #{msg_id})",
                            callback_data="menu_noop",
                        ))
                    else:
                        new_row.append(btn)
                new_rows.append(new_row)
            try:
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_rows))
            except Exception:
                pass
        await ctx.bot.send_message(
            chat_id=query.message.chat_id,
            reply_to_message_id=query.message.message_id,
            text=result_text,
            parse_mode=ParseMode.HTML,
            reply_markup=result_markup,
        )
    else:
        await query.edit_message_text(
            result_text,
            parse_mode=ParseMode.HTML,
            reply_markup=result_markup,
        )


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — DELETE CHANNEL
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_delchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    channels = db.get_all_channels(DB_PATH)
    if not channels:
        await update.message.reply_text("No channels found.")
        return
    await update.message.reply_text(
        f"🗑️ Select a channel to deactivate ({len(channels)} total):",
        reply_markup=admin_channel_keyboard(channels, "del_channel", page=0),
    )


async def cb_admin_channel_page(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Paginate /editchannel and /delchannel inline channel pickers safely."""
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("⛔ Admin only.", show_alert=True)
        return None
    parts = query.data.split(":")
    prefix = parts[1] if len(parts) > 1 else "del_channel"
    try:
        page = max(0, int(parts[2])) if len(parts) > 2 else 0
    except ValueError:
        page = 0

    channels = db.get_all_channels(DB_PATH)
    if not channels:
        await query.answer("No channels found.")
        return EC_FIELD if prefix == "edit_select" else None

    total_pages = max(1, (len(channels) + ADMIN_PAGE_SIZE - 1) // ADMIN_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    await query.answer(f"📄 Page {page + 1}/{total_pages}")
    try:
        await query.edit_message_reply_markup(
            reply_markup=admin_channel_keyboard(channels, prefix, page=page)
        )
    except Exception:
        pass
    return EC_FIELD if prefix == "edit_select" else None


async def cb_del_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("⛔ Admin only.", show_alert=True)
        return
    await query.answer()
    channel_id = int(query.data.split(":")[1])
    db.delete_channel(DB_PATH, channel_id)
    invalidate_channel_cache()
    _MONITOR_NOTIFIED_DEAD.pop(channel_id, None)
    await query.edit_message_text(f"✅ Channel #{channel_id} deactivated (removed from /menu; no posts deleted).")


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — CHANNELS LIST
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_channels(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    channels = db.get_all_channels(DB_PATH)
    if not channels:
        await update.message.reply_text("No channels in database.")
        return

    chunks: list[str] = []
    current = f"📋 <b>All Channels ({len(channels)} total)</b>\n"
    for c in channels:
        status = "✅ Active" if c["is_active"] else "❌ Inactive"
        has_cid = "chat_id" in c.keys() and c["chat_id"]
        cid_info = f"<code>{c['chat_id']}</code> (Join-Request Protected 🛡️)" if has_cid else "Not set (Static link)"
        entry = (
            f"\n<b>[{c['id']}] {html.escape(c['name'] or '')}</b> — {status}\n"
            f"   💰 ₹{c['price_inr']:.0f} | 📅 {c['created_at'][:10]}\n"
            f"   🛡️ Chat ID: {cid_info}\n"
            f"   🔗 {html.escape(c['link'] or '')}\n"
        )
        if len(current) + len(entry) > 3800:
            chunks.append(current)
            current = entry.lstrip("\n")
        else:
            current += entry
    if current:
        chunks.append(current)

    for chunk in chunks:
        await update.message.reply_text(
            chunk, parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — GRANT FREE ACCESS
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_grantaccess(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = ctx.args  # /grantaccess <user_id> <channel_id>
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /grantaccess <user_id> <channel_id>\n"
            "Use /channels to get channel IDs."
        )
        return
    try:
        user_id = int(args[0])
        channel_id = int(args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid user_id or channel_id.")
        return

    db.grant_free_access(DB_PATH, user_id, channel_id)
    channel = db.get_channel(DB_PATH, channel_id)
    await update.message.reply_text(
        f"✅ Granted free access to user <code>{user_id}</code> "
        f"for <b>{channel['name'] if channel else channel_id}</b>.",
        parse_mode=ParseMode.HTML,
    )


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — BROADCAST (conversation)
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    user_count = db.get_user_count(DB_PATH)
    await update.message.reply_text(
        f"📢 <b>Broadcast Message</b>\n\n"
        f"Will be sent to <b>{user_count}</b> users.\n\n"
        "Type your message (supports HTML formatting).\n"
        "Type /cancel to abort.",
        parse_mode=ParseMode.HTML,
    )
    return BC_MSG


async def bc_send(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    msg_text = update.message.text_html or update.message.text
    admin_id = update.effective_user.id
    user_ids = db.get_all_users(DB_PATH)

    await update.message.reply_text(
        f"⏳ Broadcasting to {len(user_ids)} users…\n"
        "This may take a while. You'll get a report when done."
    )

    sent, failed = await broadcast_message(
        update.get_bot(),
        user_ids,
        msg_text,
        delay=BROADCAST_DELAY,
    )
    db.log_broadcast(DB_PATH, admin_id, None, msg_text, sent, failed)

    await update.message.reply_text(
        f"📊 <b>Broadcast Complete</b>\n\n"
        f"✅ Sent: <b>{sent}</b>\n"
        f"❌ Failed: <b>{failed}</b>",
        parse_mode=ParseMode.HTML,
    )
    return ConversationHandler.END


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — STATS
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    stats = await asyncio.to_thread(db.get_stats, DB_PATH)

    summary_text = (
        "<blockquote><b>📊 Bot Statistics</b></blockquote>\n"
        "<blockquote>"
        f"👥 Total Users:      <b>{stats['total_users']}</b>\n"
        f"✅ Active Users:     <b>{stats['active_users']}</b>\n"
        f"📌 Total Channels:   <b>{stats['total_channels']}</b>\n"
        f"🟢 Active Channels:  <b>{stats['active_channels']}</b></blockquote>\n"
        "<blockquote>"
        f"💰 Total Revenue:    <b>₹{stats['total_revenue']:.2f}</b>\n"
        f"✅ Paid Txns:        <b>{stats['total_paid_txns']}</b>\n"
        f"⏳ Pending Payments: <b>{stats['pending_payments']}</b></blockquote>"
    )

    breakdown = stats.get("channel_breakdown") or []
    paid_channels = [ch for ch in breakdown if (ch.get("sales") or 0) > 0 or (ch.get("revenue") or 0) > 0]
    zero_sales_count = len(breakdown) - len(paid_channels)

    if not paid_channels:
        footer = (
            f"\n<blockquote><b>Per Channel Revenue:</b>\n"
            f"  None yet ({len(breakdown)} channel(s) with 0 sales)</blockquote>"
        )
        await update.message.reply_text(summary_text + footer, parse_mode=ParseMode.HTML)
        return

    entry_lines: list[str] = []
    for ch in paid_channels:
        cid = ch.get("id")
        id_tag = f"<code>#{cid}</code> " if cid is not None else ""
        dot = "🟢" if ch.get("is_active", 1) else "🔴"
        safe_name = html.escape(str(ch.get("name") or f"Channel #{cid}"))
        entry_lines.append(
            f"\u200e{dot} {id_tag}<b>\u2066{safe_name}\u2069\u200e</b> — "
            f"{int(ch['sales'])} sales | <b>₹{float(ch['revenue']):.0f}</b>"
        )
    if zero_sales_count > 0:
        entry_lines.append(f"\n<i>ℹ️ +{zero_sales_count} other channel(s) with 0 sales</i>")

    # Pack safely under Telegram's 4096-character limit (chunking into multiple messages if needed)
    header_prefix = f"{summary_text}\n<blockquote><b>Per Channel Revenue ({len(paid_channels)} with sales):</b>\n"
    chunks: list[str] = []
    cur = header_prefix
    for line in entry_lines:
        addition = ("\n" if not cur.endswith("\n") else "") + line
        if len(cur) + len(addition) + len("</blockquote>") > 3800:
            chunks.append(cur + "</blockquote>")
            cur = "<blockquote><b>Per Channel Revenue (cont.):</b>\n" + line
        else:
            cur += addition
    chunks.append(cur + "</blockquote>")

    for chunk in chunks:
        await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — USERS
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    conn = db.get_connection(DB_PATH)
    rows = conn.execute(
        "SELECT * FROM users ORDER BY joined_at DESC LIMIT 20"
    ).fetchall()

    if not rows:
        await update.message.reply_text("No users yet.")
        return

    lines = ["👥 <b>Recent 20 Users</b>\n"]
    for r in rows:
        name = r["first_name"] or ""
        uname = format_user_display(r["username"], fallback="—")
        banned = " 🚫" if r["is_banned"] else ""
        lines.append(
            f"• <code>{r['user_id']}</code> {name} ({uname}){banned} — {r['joined_at'][:10]}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — PAYMENTS
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_payments(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = ctx.args  # /payments <channel_id>
    if not args:
        await update.message.reply_text("Usage: /payments <channel_id>\nUse /channels to get channel IDs.")
        return
    try:
        channel_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid channel_id.")
        return

    rows = db.get_payments_for_channel(DB_PATH, channel_id)
    channel = db.get_channel(DB_PATH, channel_id)
    if not rows:
        await update.message.reply_text(f"No paid transactions for channel {channel_id}.")
        return

    lines = [f"💳 <b>Payments for {channel['name'] if channel else channel_id}</b>\n"]
    for r in rows:
        uname = format_user_display(r["username"], fallback=r["first_name"] or "—")
        lines.append(
            f"• <code>{r['user_id']}</code> ({uname})\n"
            f"  ₹{r['amount_inr']:.0f} | {r['paid_at'][:10] if r['paid_at'] else 'N/A'}\n"
            f"  <code>{r['razorpay_payment_id']}</code>"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_check_payment(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: diagnose and check status of any payment ID (qr_..., pay_..., order_...)."""
    if not is_admin(update.effective_user.id):
        return

    args = ctx.args
    if not args:
        await reply_or_send_text(
            update, ctx,
            "🔍 <b>Payment Diagnosis / Lookup</b>\n\n"
            "Usage: <code>/check &lt;Payment_Ref_or_ID&gt;</code>\n\n"
            "Examples:\n"
            "• <code>/check qr_Ttct07Uuxyl8HS</code>\n"
            "• <code>/check pay_XXXXXXXXXXXXXX</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    ref_id = args[0].strip()
    payment = await asyncio.to_thread(db.find_payment, DB_PATH, ref_id)

    live_status = "Unknown"
    live_amount = None
    live_pay_id = None
    live_method = None
    paid_timestamp = None

    client = rzp_client()
    try:
        if ref_id.startswith("qr_"):
            qr_payments = await asyncio.to_thread(pay.fetch_qr_payments, client, ref_id)
            if qr_payments:
                for item in qr_payments:
                    live_status = item.get("status", "unknown")
                    live_amount = (item.get("amount") or 0) / 100.0
                    live_pay_id = item.get("id")
                    live_method = item.get("method")
                    created_at = item.get("created_at")
                    if created_at:
                        paid_timestamp = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
                    if live_status in ("captured", "authorized"):
                        break
            else:
                live_status = "Unpaid (No payment detected on gateway)"
        elif ref_id.startswith("pay_"):
            p_data = await asyncio.to_thread(pay.fetch_payment, client, ref_id)
            live_status = p_data.get("status", "unknown")
            live_amount = (p_data.get("amount") or 0) / 100.0
            live_pay_id = p_data.get("id")
            live_method = p_data.get("method")
            created_at = p_data.get("created_at")
            if created_at:
                paid_timestamp = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
        else:
            o_data = await asyncio.to_thread(pay.fetch_order, client, ref_id)
            live_status = o_data.get("status", "unknown")
            live_amount = (o_data.get("amount_paid") or o_data.get("amount") or 0) / 100.0
            o_payments = await asyncio.to_thread(pay.fetch_order_payments, client, ref_id)
            if o_payments:
                live_pay_id = o_payments[0].get("id")
                live_method = o_payments[0].get("method")
    except Exception as exc:
        logger.warning("Error fetching live payment for %s: %s", ref_id, exc)
        live_status = f"Gateway error: {exc}"

    user_str = "Not linked"
    channel_str = "Unknown"
    db_status = "Not in database"
    price_str = f"₹{live_amount:.0f}" if live_amount else "N/A"

    if payment:
        uid = payment["user_id"]
        user = db.get_user(DB_PATH, uid)
        uname = format_user_display(user['username'], fallback="None") if user else "None"
        name = f"{user['first_name']} {user['last_name']}".strip() if user else ""
        user_str = f"<code>{uid}</code> ({uname} - {name})"
        channel = db.get_channel(DB_PATH, payment["channel_id"])
        channel_str = channel["name"] if channel else f"Channel #{payment['channel_id']}"
        db_status = payment["status"].upper()
        price_str = f"₹{payment['amount_inr']:.0f}"

        # If live gateway shows captured but DB was pending, auto-process it!
        if payment["status"] != "paid" and live_status in ("captured", "authorized"):
            await process_successful_payment(
                ctx.bot,
                payment["razorpay_order_id"],
                live_pay_id or f"auto_{ref_id}",
                payment["amount_inr"],
                live_method or "UPI QR",
            )
            db_status = "PAID (Auto-Approved & Delivered!)"

    status_icon = "✅" if (db_status.startswith("PAID") or live_status in ("captured", "authorized", "paid")) else "⏳"

    lines = [
        f"{status_icon} <b>Payment Diagnosis Report</b>\n",
        f"🧾 <b>Payment Ref ID:</b> <code>{ref_id}</code>",
        f"👤 <b>User:</b> {user_str}",
        f"🏆 <b>Channel:</b> {channel_str}",
        f"💰 <b>Amount:</b> {price_str}",
        f"💾 <b>Database Status:</b> <code>{db_status}</code>",
        f"🌐 <b>Live Gateway Status:</b> <b>{live_status}</b>",
    ]
    if live_pay_id:
        lines.append(f"💳 <b>Transaction ID:</b> <code>{live_pay_id}</code>")
    if live_method:
        lines.append(f"📲 <b>Method:</b> {live_method}")
    if paid_timestamp:
        lines.append(f"⏱️ <b>Paid At:</b> {paid_timestamp}")

    buttons = []
    if payment and payment["status"] != "paid":
        buttons.append([
            InlineKeyboardButton("🚀 Force Approve & Deliver Link", callback_data=f"adm_force_pay:{payment['razorpay_order_id']}")
        ])

    markup = InlineKeyboardMarkup(buttons) if buttons else None
    await reply_or_send_text(update, ctx, "\n".join(lines), reply_markup=markup, parse_mode=ParseMode.HTML)


async def cb_adm_force_pay(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback when admin clicks 'Force Approve & Deliver Link'."""
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return

    order_id = query.data.split(":", 1)[1]
    payment = db.get_payment_by_order(DB_PATH, order_id)
    if not payment:
        await query.answer("❌ Payment record not found.", show_alert=True)
        return

    await process_successful_payment(
        ctx.bot,
        order_id,
        f"admin_manual_{int(time.time())}",
        payment["amount_inr"],
        "Admin Manual Approval",
    )
    await query.answer("🎉 Payment approved and link delivered!", show_alert=True)
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    await reply_or_send_text(update, ctx, f"✅ <b>Order {order_id} has been manually approved and delivered to User {payment['user_id']}.</b>")


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — BAN / UNBAN
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_ban(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /ban <user_id>")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user_id.")
        return
    db.ban_user(DB_PATH, uid)
    await update.message.reply_text(f"🚫 User <code>{uid}</code> banned.", parse_mode=ParseMode.HTML)


async def cmd_unban(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user_id.")
        return
    db.unban_user(DB_PATH, uid)
    await update.message.reply_text(f"✅ User <code>{uid}</code> unbanned.", parse_mode=ParseMode.HTML)


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — CHANNEL BROADCAST POST WIZARD (/post)
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_post(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Start Channel Broadcast post creation wizard."""
    user = update.effective_user
    if not user:
        return ConversationHandler.END

    if update.effective_chat and update.effective_chat.type != "private":
        try:
            await update.message.reply_text(
                f"⚠️ Please message @{BOT_USERNAME} in a <b>private direct chat</b> to create and publish posts.",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
        return ConversationHandler.END

    if not is_admin(user.id):
        admin_list = get_admin_ids()
        await update.message.reply_text(
            f"❌ <b>Access Denied</b>\n\n"
            f"Your Telegram User ID is: <code>{user.id}</code>\n"
            f"Configured Admin IDs: <code>{admin_list}</code>\n\n"
            f"If you are the administrator, please ensure <code>ADMIN_IDS={user.id}</code> is set in <code>.env</code>.",
            parse_mode=ParseMode.HTML
        )
        return ConversationHandler.END

    ctx.user_data.clear()
    ctx.user_data["is_free"] = False
    await update.message.reply_text(
        "📢 <b>Create Channel Broadcast Post</b>\n\n"
        "This wizard publishes an announcement to your <b>Main Channel</b> with an inline 'Get Instant Access' button.\n"
        "Subscribers tap the button, pay via UPI QR, and get instant access!\n\n"
        "📸🎬 <b>Step 1/4:</b> Send a <b>Photo or Video</b> for the post.\n"
        "(Or type <code>skip</code> if you want a text-only announcement.)\n\n"
        "✨ <i>Post description and emojis will be generated automatically — no description needed!</i>\n"
        "Type /cancel anytime to abort.",
        parse_mode=ParseMode.HTML,
    )
    return POST_PHOTO


async def cmd_postfree(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Entry point for posting a FREE channel announcement (zero fee, direct link)."""
    user = update.effective_user
    if not user:
        return ConversationHandler.END

    if not is_admin(user.id):
        admin_list = get_admin_ids()
        await update.message.reply_text(
            f"❌ <b>Access Denied</b>\n\n"
            f"Your Telegram User ID is: <code>{user.id}</code>\n"
            f"Configured Admin IDs: <code>{admin_list}</code>\n\n"
            f"If you are the administrator, please ensure <code>ADMIN_IDS={user.id}</code> is set in <code>.env</code>.",
            parse_mode=ParseMode.HTML
        )
        return ConversationHandler.END

    ctx.user_data.clear()
    ctx.user_data["is_free"] = True
    ctx.user_data["post_price"] = 0.0
    ctx.user_data["post_cross_price"] = None
    ctx.user_data["free_link_mode"] = "direct"

    await update.message.reply_text(
        "🎁 <b>Create Free Channel Broadcast Post</b>\n\n"
        "This wizard publishes a <b>FREE Channel Announcement</b> to your <b>Main Channel</b>.\n"
        "Subscribers can join immediately with <b>zero payment</b>!\n\n"
        "📸🎬 <b>Step 1/3:</b> Send a <b>Photo or Video</b> for the post.\n"
        "(Or type <code>skip</code> if you want a text-only announcement.)\n\n"
        "✨ <i>Post caption and formatting will be generated automatically — no price or description needed!</i>\n"
        "Type /cancel anytime to abort.",
        parse_mode=ParseMode.HTML,
    )
    return POST_PHOTO


async def post_get_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive photo, video, animation, or handle skip for broadcast post, then immediately prompt for channel."""
    msg = update.message
    ctx.user_data.pop("post_ocr_task", None)
    if msg.photo:
        photo_fid = msg.photo[-1].file_id
        ctx.user_data["post_photo"] = photo_fid
        ctx.user_data["post_media_type"] = "photo"
        ctx.user_data["post_ocr_task"] = asyncio.create_task(
            extract_channel_metadata_from_file_id(ctx.bot, photo_fid)
        )
        if msg.caption:
            ctx.user_data["post_text"] = msg.caption_html or msg.caption
    elif msg.video:
        ctx.user_data["post_photo"] = msg.video.file_id
        ctx.user_data["post_media_type"] = "video"
        if msg.caption:
            ctx.user_data["post_text"] = msg.caption_html or msg.caption
    elif msg.animation:
        ctx.user_data["post_photo"] = msg.animation.file_id
        ctx.user_data["post_media_type"] = "animation"
        if msg.caption:
            ctx.user_data["post_text"] = msg.caption_html or msg.caption
    elif msg.document and msg.document.mime_type and msg.document.mime_type.startswith("video/"):
        ctx.user_data["post_photo"] = msg.document.file_id
        ctx.user_data["post_media_type"] = "video"
        if msg.caption:
            ctx.user_data["post_text"] = msg.caption_html or msg.caption
    elif msg.text and msg.text.strip().lower() in ("skip", "/skip"):
        ctx.user_data["post_photo"] = None
        ctx.user_data["post_media_type"] = None
    elif msg.text:
        ctx.user_data["post_photo"] = None
        ctx.user_data["post_media_type"] = None
        ctx.user_data["post_text"] = msg.text_html or msg.text
    else:
        await msg.reply_text("⚠️ Please send a photo, video, or type <code>skip</code>.", parse_mode=ParseMode.HTML)
        return POST_PHOTO

    channels = get_active_channels_cached()
    is_free = ctx.user_data.get("is_free", False)
    step_label = "Step 2/3: Free Channel Link" if is_free else "Step 2/4: Private Channel Link"
    if channels:
        await update.message.reply_text(
            f"🔗 <b>{step_label}</b>\n\n"
            f"Select an existing channel below, OR directly <b>send/paste a channel invite link</b> (e.g. <code>https://t.me/+...</code>):\n\n"
            f"✨ <i>Description & emojis are generated automatically!</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=build_post_channel_keyboard(channels, is_free=is_free, page=0),
        )
    else:
        await update.message.reply_text(
            f"🔗 <b>{step_label}</b>\n\n"
            f"Please paste or send the <b>channel invite link</b> (e.g. <code>https://t.me/+abcXYZ123</code>) you are sharing:\n\n"
            f"✨ <i>Description & emojis are generated automatically!</i>",
            parse_mode=ParseMode.HTML,
        )
    return POST_CHANNEL


async def post_get_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Fallback handler if text is provided."""
    text = update.message.text_html or update.message.text
    if text and text.strip().lower() not in ("skip", "/skip"):
        ctx.user_data["post_text"] = text

    channels = get_active_channels_cached()
    is_free = ctx.user_data.get("is_free", False)
    step_label = "Step 2/3: Free Channel Link" if is_free else "Step 2/4: Private Channel Link"
    if channels:
        await update.message.reply_text(
            f"🔗 <b>{step_label}</b>\n\n"
            f"Select an existing channel below, OR directly <b>send/paste a channel invite link</b> (e.g. <code>https://t.me/+...</code>):",
            parse_mode=ParseMode.HTML,
            reply_markup=build_post_channel_keyboard(channels, is_free=is_free, page=0),
        )
    else:
        await update.message.reply_text(
            f"🔗 <b>{step_label}</b>\n\n"
            f"Please paste or send the <b>channel invite link</b> (e.g. <code>https://t.me/+abcXYZ123</code>) you are sharing:",
            parse_mode=ParseMode.HTML,
        )
    return POST_CHANNEL


async def cb_post_channel_page(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Paginate existing channel picker inside /post Step 2."""
    query = update.callback_query
    try:
        page = max(0, int(query.data.split(":", 1)[1]))
    except (IndexError, ValueError):
        page = 0
    channels = get_active_channels_cached()
    if not channels:
        await query.answer("No channels available.")
        return POST_CHANNEL
    total_pages = max(1, (len(channels) + ADMIN_PAGE_SIZE - 1) // ADMIN_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    is_free = ctx.user_data.get("is_free", False)
    await query.answer(f"📄 Page {page + 1}/{total_pages}")
    try:
        await query.edit_message_reply_markup(
            reply_markup=build_post_channel_keyboard(channels, is_free=is_free, page=page)
        )
    except Exception:
        pass
    return POST_CHANNEL


async def cb_post_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive channel selection via inline button."""
    query = update.callback_query
    await query.answer()
    data = query.data.split(":")[1]

    if data == "new":
        await query.edit_message_text(
            "🔗 <b>Enter Private Channel Link:</b>\n\n"
            "Please send the private channel invite link (e.g. <code>https://t.me/+abcXYZ123</code>):",
            parse_mode=ParseMode.HTML
        )
        return POST_CHANNEL

    channel_id = int(data)
    channel = db.get_channel(DB_PATH, channel_id)
    if not channel:
        await query.edit_message_text("❌ Channel not found. Cancelled.")
        return ConversationHandler.END

    # If a screenshot photo was uploaded in Step 1, auto-update channel name & photo/video counts
    ocr_task = ctx.user_data.pop("post_ocr_task", None)
    if ocr_task:
        try:
            ocr_name, ocr_media = await asyncio.wait_for(ocr_task, timeout=12.0)
            updates = {}
            if ocr_name:
                updates["name"] = ocr_name
            if ocr_media:
                updates["media_counts"] = ocr_media
            if updates:
                db.update_channel(DB_PATH, channel_id, **updates)
                invalidate_channel_cache()
                channel = db.get_channel(DB_PATH, channel_id) or channel
        except Exception as exc:
            logger.debug("OCR task in cb_post_channel skipped: %s", exc)

    ctx.user_data["post_channel_id"] = channel_id
    ctx.user_data["channel_default_price"] = channel["price_inr"]

    if ctx.user_data.get("is_free"):
        ctx.user_data["post_price"] = 0.0
        ctx.user_data["post_cross_price"] = None
        try:
            await query.edit_message_text(
                f"Selected Channel: <b>{channel['name']}</b> (FREE 🎁)\n\n"
                f"Advancing to target main channel selection...",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
        return await prompt_post_target(update, ctx)

    await query.edit_message_text(
        f"Selected Channel: <b>{channel['name']}</b>\n\n"
        f"💰 <b>Step 3/4: Post Pricing</b>\n\n"
        f"Channel default: <b>₹{channel['price_inr']:.0f}</b>\n"
        f"Enter the <b>Selling Price</b> and optional <b>Cross-mark Price</b>:\n"
        f"• Example: <code>1 2</code> (Sells for ₹1, crossed-out ₹2)\n"
        f"• Or single price: <code>{channel['price_inr']:.0f}</code>\n"
        f"• Type <code>skip</code> to use default price:",
        parse_mode=ParseMode.HTML,
    )
    return POST_PRICE


async def post_get_channel_link(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive channel invite link entered as text message."""
    link = update.message.text.strip()
    link_low = link.lower()
    if not (
        link_low.startswith(("http://", "https://", "t.me/", "www.t.me/", "telegram.me/"))
        or (link.startswith("@") and len(link) >= 5)
    ):
        await update.message.reply_text("⚠️ Please enter a valid invite link starting with http/https, t.me/, or @username.")
        return POST_CHANNEL
    if link.startswith("@"):
        link = f"https://t.me/{link.lstrip('@')}"
    elif not link_low.startswith(("http://", "https://")):
        link = f"https://{link}"

    is_alive, reason = await check_channel_link_alive(link, bot=ctx.bot)
    if not is_alive:
        await update.message.reply_text(
            f"❌ <b>This channel link appears to be expired or the channel is banned!</b>\n\n"
            f"⚠️ Reason: {reason}\n\n"
            f"Please enter a valid, active channel invite link:",
            parse_mode=ParseMode.HTML,
        )
        return POST_CHANNEL

    # Extract channel name & photo/video counts from uploaded screenshot image (if any) + channel link
    ocr_name, ocr_media = "", ""
    ocr_task = ctx.user_data.pop("post_ocr_task", None)
    if ocr_task:
        try:
            ocr_name, ocr_media = await asyncio.wait_for(ocr_task, timeout=12.0)
        except Exception as exc:
            logger.debug("OCR task in post_get_channel_link skipped: %s", exc)
    elif ctx.user_data.get("post_photo"):
        ocr_name, ocr_media = await extract_channel_metadata_from_file_id(
            ctx.bot, ctx.user_data["post_photo"]
        )

    if not ocr_media and ctx.user_data.get("post_text"):
        _, cap_media = parse_channel_header_ocr_text(ctx.user_data["post_text"])
        if cap_media:
            ocr_media = cap_media

    link_title = extract_clean_title_from_alive_detail(reason)
    detected_name = ocr_name or link_title

    existing_ch = db.get_channel_by_link(DB_PATH, link)
    if existing_ch:
        channel_id = existing_ch["id"]
        channel_name = existing_ch["name"]
        updates: dict = {"is_active": 1}
        if detected_name and (
            ocr_name or re.match(r"^VIP Channel #\d+$", channel_name or "", re.IGNORECASE)
        ):
            channel_name = detected_name
            updates["name"] = channel_name
        if ocr_media:
            updates["media_counts"] = ocr_media
        db.update_channel(DB_PATH, channel_id, **updates)
        invalidate_channel_cache()
        ctx.user_data["post_channel_id"] = channel_id
        ctx.user_data["channel_default_price"] = existing_ch["price_inr"] or 0.0
    else:
        admin_id = update.effective_user.id
        count = len(db.get_all_channels(DB_PATH)) + 1
        channel_name = detected_name or f"VIP Channel #{count}"

        # Automatically save channel to database with extracted name and photo/video counts
        channel_id = db.add_channel(
            DB_PATH,
            name=channel_name,
            link=link,
            price_inr=0.0,
            description="🌟 Exclusive VIP Access • Daily Premium Content & Updates 🚀",
            admin_id=admin_id,
            media_counts=ocr_media or None,
        )
        invalidate_channel_cache()

        ctx.user_data["post_channel_id"] = channel_id
        ctx.user_data["channel_default_price"] = 0.0

    if ctx.user_data.get("is_free"):
        ctx.user_data["post_price"] = 0.0
        ctx.user_data["post_cross_price"] = None
        await update.message.reply_text(
            f"✅ Free channel saved as <b>{channel_name}</b>!\n\n"
            f"Advancing to target main channel selection...",
            parse_mode=ParseMode.HTML,
        )
        return await prompt_post_target(update, ctx)

    await update.message.reply_text(
        f"✅ Private link saved as <b>{channel_name}</b>!\n\n"
        f"💰 <b>Step 3/4: Post Pricing</b>\n\n"
        f"Enter the <b>Selling Price</b> and optional <b>Cross-mark Price</b>:\n"
        f"• Example: <code>1 2</code> (Sells for ₹1, crossed-out ₹2)\n"
        f"• Or enter a single price: <code>499</code>",
        parse_mode=ParseMode.HTML,
    )
    return POST_PRICE


async def prompt_post_target(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Prompt user for target channel after price and cross-price are set."""
    default_target = db.get_setting(DB_PATH, "main_channel_id") or MAIN_CHANNEL_ID
    ctx.user_data["default_target"] = default_target
    is_free = ctx.user_data.get("is_free", False)
    step_num = "3/3" if is_free else "4/4"

    if default_target:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📢 Post to Default Channel ({default_target})", callback_data="target_default")]
        ])
        await reply_or_send_text(
            update, ctx,
            f"📢 <b>Step {step_num}: Target Main Channel</b>\n\n"
            f"Configured default channel: <code>{default_target}</code>\n\n"
            f"Tap the button below to publish to your default channel, or type another channel username/ID:",
            reply_markup=keyboard,
        )
    else:
        await reply_or_send_text(
            update, ctx,
            f"📢 <b>Step {step_num}: Target Main Channel</b>\n\n"
            "Enter the channel username or numeric ID where this post will be sent (e.g. <code>@MyMainCommunity</code> or <code>-1004296919279</code>):\n\n"
            "⚠️ <i>Make sure this bot is added as an Administrator in that channel with 'Post Messages' permission!</i>",
        )
    return POST_TARGET


async def post_get_price(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive price for post, supporting both single price or 'selling_price cross_price'."""
    text = update.message.text.strip().lower()
    default_price = ctx.user_data.get("channel_default_price", 0.0)

    import re
    numbers = re.findall(r"\d+(?:\.\d+)?", text)

    if text in ("0", "free", "0 0") or (numbers and float(numbers[0]) == 0):
        ctx.user_data["is_free"] = True
        ctx.user_data["post_price"] = 0.0
        ctx.user_data["post_cross_price"] = None
        ctx.user_data["free_link_mode"] = "direct"
        channel_id = ctx.user_data.get("post_channel_id")
        if channel_id:
            db.update_channel(DB_PATH, channel_id, price_inr=0.0)
        await update.message.reply_text("🎁 <b>Free Post Selected!</b> Zero fee will be charged.", parse_mode=ParseMode.HTML)
        return await prompt_post_target(update, ctx)

    if text in ("skip", "/skip") and default_price > 0:
        price = default_price
        ctx.user_data["post_price"] = price
        ctx.user_data["post_cross_price"] = None
        return await prompt_post_target(update, ctx)

    if len(numbers) >= 2:
        # Both selling price and cross price entered together (e.g. "1 2" or "499 999")
        try:
            price = float(numbers[0])
            cross_price = float(numbers[1])
            if price <= 0 or cross_price <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("⚠️ Please enter valid positive numbers for price.")
            return POST_PRICE

        ctx.user_data["post_price"] = price
        ctx.user_data["post_cross_price"] = cross_price

        channel_id = ctx.user_data.get("post_channel_id")
        if channel_id:
            db.update_channel(DB_PATH, channel_id, price_inr=price)

        return await prompt_post_target(update, ctx)

    elif len(numbers) == 1:
        try:
            price = float(numbers[0])
            if price <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("⚠️ Please enter a valid positive number for price.")
            return POST_PRICE

        ctx.user_data["post_price"] = price
        channel_id = ctx.user_data.get("post_channel_id")
        if channel_id:
            db.update_channel(DB_PATH, channel_id, price_inr=price)

        await update.message.reply_text(
            f"💰 Selling Price set to: <b>₹{price:.0f}</b>\n\n"
            "✂️ <b>Cross-mark (Strikethrough / Original) Price:</b>\n"
            "Enter the original price to display with a cross-mark (e.g. <code>2</code> or <code>999</code>):\n\n"
            "<i>Send <code>skip</code> or <code>/skip</code> if you don't want any crossed-out price.</i>",
            parse_mode=ParseMode.HTML,
        )
        return POST_CROSS_PRICE

    else:
        await update.message.reply_text(
            "⚠️ Please enter a valid price (e.g. <code>1 2</code> or <code>499</code>).",
            parse_mode=ParseMode.HTML,
        )
        return POST_PRICE


async def post_get_cross_price(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive cross-mark price or skip."""
    text = update.message.text.strip().lower()
    if text in ("skip", "/skip", "none", "no"):
        ctx.user_data["post_cross_price"] = None
    else:
        try:
            clean = text.replace("₹", "").replace(",", "").strip()
            cross_price = float(clean)
            if cross_price <= 0:
                raise ValueError
            ctx.user_data["post_cross_price"] = cross_price
        except ValueError:
            await update.message.reply_text("⚠️ Enter a valid number for cross-mark price (e.g. <code>2</code>), or send /skip:", parse_mode=ParseMode.HTML)
            return POST_CROSS_PRICE

    return await prompt_post_target(update, ctx)


async def cb_post_target_default(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle clicking default target channel button."""
    query = update.callback_query
    await query.answer()
    default_target = ctx.user_data.get("default_target")
    if not default_target:
        await query.edit_message_text("❌ No default channel found. Please type the channel username or ID.")
        return POST_TARGET

    ctx.user_data["post_target"] = default_target
    try:
        await query.edit_message_text(f"📢 Selected Target Channel: <b>{default_target}</b>", parse_mode=ParseMode.HTML)
    except Exception:
        pass
    return await render_post_preview(query.message, ctx)


async def post_get_target(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive target channel via text, then show preview."""
    text = update.message.text.strip()
    default_target = ctx.user_data.get("default_target")

    if text.lower() == "use" and default_target:
        target = default_target
    else:
        target = text

    ctx.user_data["post_target"] = target
    return await render_post_preview(update.message, ctx)


async def render_post_preview(message, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Helper to render preview and confirmation buttons."""
    target = ctx.user_data["post_target"]
    channel = db.get_channel(DB_PATH, ctx.user_data["post_channel_id"])
    price = ctx.user_data["post_price"]
    photo_file_id = ctx.user_data.get("post_photo")
    media_type = ctx.user_data.get("post_media_type", "photo")
    bot_username = BOT_USERNAME

    is_free = ctx.user_data.get("is_free", False) or (price is not None and price <= 0)
    link_mode = ctx.user_data.get("free_link_mode", "direct")

    ch_id = channel["id"] if channel else ctx.user_data.get("post_channel_id", "")
    ch_name = channel["name"] if channel else "VIP Channel"
    safe_ch_name = html.escape(str(ch_name))
    ch_id_tag = f" (<code>#{ch_id}</code>)" if ch_id else ""
    custom_text = ctx.user_data.get("post_text")

    if is_free:
        if custom_text:
            header_block = (
                f"<blockquote><b>🎁 EXCLUSIVE FREE VIP CHANNEL ACCESS 🎁</b></blockquote>\n"
                f"<blockquote>{custom_text}</blockquote>\n"
            )
        else:
            header_block = f"<blockquote><b>🎁 EXCLUSIVE FREE VIP CHANNEL ACCESS 🎁</b></blockquote>\n"
        price_line = "💰 <b>Access Fee:</b> <b>FREE (₹0) 🎁</b>\n"
        payment_mode_line = "⚡ <b>Instant Access:</b> Zero Payment • 100% Free Join"
        action_line = "👇 <b>Tap the button below to join for free:</b>"

        btn_url = channel["link"] if link_mode == "direct" else f"https://t.me/{bot_username}?start=free_{channel['id']}"
        btn_label = "🚀 Join Free Channel Now 🔓" if link_mode == "direct" else "🎁 Claim Free Instant Access 🔓"
        preview_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(btn_label, url=btn_url)]
        ])
    else:
        if custom_text:
            header_block = (
                f"<blockquote><b>🔥 EXCLUSIVE VIP CHANNEL ACCESS 🔥</b></blockquote>\n"
                f"<blockquote>{custom_text}</blockquote>\n"
            )
        else:
            header_block = f"<blockquote><b>🔥 EXCLUSIVE VIP CHANNEL ACCESS 🔥</b></blockquote>\n"

        cross_price = ctx.user_data.get("post_cross_price")
        if cross_price and cross_price > 0:
            price_line = f"💰 <b>Access Fee:</b> <b>₹{price:.0f}</b> <s>₹{cross_price:.0f}</s> <i>(Limited Time Offer)</i>\n"
        else:
            price_line = f"💰 <b>Access Fee:</b> <b>₹{price:.0f}</b> <i>(Limited Time Offer)</i>\n"
        payment_mode_line = "📲 <b>Payment Mode:</b> Any UPI App (PhonePe, GPay, Paytm, BHIM)"
        action_line = "👇 <b>Tap the button below to unlock instant VIP access:</b>"

        preview_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"⚡ Unlock Instant Access — ₹{price:.0f} 🔓", url=f"https://t.me/{bot_username}?start=preview")]
        ])

    preview_caption = (
        f"{header_block}"
        f"<blockquote><b>🌟 Channel:</b> <b>{safe_ch_name}</b>{ch_id_tag}\n"
        f"──────────────────────────\n"
        f"{price_line}"
        f"{payment_mode_line}</blockquote>\n"
        f"<blockquote>⚠️ <b>ONE-TIME LINK — NO BACKUP AVAILABLE</b></blockquote>\n\n"
        f"{action_line}"
    )
    if photo_file_id and len(preview_caption) > 1024:
        preview_caption = preview_caption[:1020] + "..."

    ctx.user_data["final_caption"] = preview_caption

    if is_free:
        mode_label = "Direct Channel Link" if link_mode == "direct" else "Bot Protected Link"
        admin_controls = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Publish Free Post to Main Channel", callback_data="post_publish")],
            [InlineKeyboardButton(f"🔘 Button Link: {mode_label} (Tap to Switch)", callback_data="post_toggle_mode")],
            [InlineKeyboardButton("❌ Cancel", callback_data="post_abort")],
        ])
    else:
        admin_controls = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Publish to Main Channel", callback_data="post_publish")],
            [InlineKeyboardButton("❌ Cancel", callback_data="post_abort")],
        ])

    chat_id = message.chat_id
    await ctx.bot.send_message(
        chat_id=chat_id,
        text="📋 <b>POST PREVIEW</b> — How it will appear in your channel:",
        parse_mode=ParseMode.HTML,
    )

    if photo_file_id:
        try:
            if media_type == "video":
                await ctx.bot.send_video(
                    chat_id=chat_id,
                    video=photo_file_id,
                    caption=preview_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=preview_kb,
                )
            elif media_type == "animation":
                await ctx.bot.send_animation(
                    chat_id=chat_id,
                    animation=photo_file_id,
                    caption=preview_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=preview_kb,
                )
            else:
                await ctx.bot.send_photo(
                    chat_id=chat_id,
                    photo=photo_file_id,
                    caption=preview_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=preview_kb,
                )
        except Exception as exc:
            logger.warning("Could not send preview media, falling back to text: %s", exc)
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=preview_caption,
                parse_mode=ParseMode.HTML,
                reply_markup=preview_kb,
            )
    else:
        await ctx.bot.send_message(
            chat_id=chat_id,
            text=preview_caption,
            parse_mode=ParseMode.HTML,
            reply_markup=preview_kb,
        )

    await ctx.bot.send_message(
        chat_id=chat_id,
        text=(
            f"🏷️ <b>Channel:</b> <b>{safe_ch_name}</b>{ch_id_tag}\n"
            f"📢 <b>Target:</b> <b>{html.escape(str(target))}</b>\n\n"
            f"Tap Publish when ready:"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=admin_controls,
    )
    return POST_PREVIEW


async def cb_post_toggle_mode(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Toggle between Direct Channel Link and Bot Deep-Link for free posts."""
    query = update.callback_query
    await query.answer()
    current = ctx.user_data.get("free_link_mode", "direct")
    ctx.user_data["free_link_mode"] = "bot" if current == "direct" else "direct"
    return await render_post_preview(query.message, ctx)


async def cb_post_publish(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Publish broadcast post to target channel."""
    query = update.callback_query
    await query.answer()

    target = ctx.user_data.get("post_target")
    channel_id = ctx.user_data.get("post_channel_id")
    price = ctx.user_data.get("post_price", 0.0)
    photo_file_id = ctx.user_data.get("post_photo")
    media_type = ctx.user_data.get("post_media_type", "photo")
    final_caption = ctx.user_data.get("final_caption")
    cross_price = ctx.user_data.get("post_cross_price")
    is_free = ctx.user_data.get("is_free", False) or price <= 0
    link_mode = ctx.user_data.get("free_link_mode", "direct")
    admin_id = update.effective_user.id
    channel = db.get_channel(DB_PATH, channel_id)

    if not target or not channel_id:
        await query.edit_message_text("❌ Session expired. Please start /post again.")
        return ConversationHandler.END

    # 1. Create post entry in DB
    post_id = db.create_post(
        DB_PATH,
        admin_id=admin_id,
        channel_id=channel_id,
        title=channel["name"],
        caption=ctx.user_data.get("post_text", ""),
        photo_file_id=photo_file_id,
        price_inr=price,
        target_channel=target,
        cross_price_inr=cross_price,
    )

    bot_username = BOT_USERNAME
    if is_free:
        if link_mode == "direct":
            btn_text = "🚀 Join Free Channel Now 🔓"
            btn_url = channel["link"]
        else:
            btn_text = "🎁 Claim Free Instant Access 🔓"
            btn_url = f"https://t.me/{bot_username}?start=free_{post_id}"
        channel_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(btn_text, url=btn_url)]
        ])
    else:
        channel_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"⚡ Unlock Instant Access — ₹{price:.0f} 🔓",
                url=f"https://t.me/{bot_username}?start=buy_{post_id}"
            )]
        ])

    # 2. Send to Channel (Telegram native channel identity)
    try:
        try:
            target_chat_id = int(target)
        except (ValueError, TypeError):
            target_chat_id = target

        if photo_file_id:
            try:
                if media_type == "video":
                    msg = await ctx.bot.send_video(
                        chat_id=target_chat_id,
                        video=photo_file_id,
                        caption=final_caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=channel_keyboard,
                    )
                elif media_type == "animation":
                    msg = await ctx.bot.send_animation(
                        chat_id=target_chat_id,
                        animation=photo_file_id,
                        caption=final_caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=channel_keyboard,
                    )
                else:
                    msg = await ctx.bot.send_photo(
                        chat_id=target_chat_id,
                        photo=photo_file_id,
                        caption=final_caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=channel_keyboard,
                    )
            except Exception as photo_exc:
                logger.warning("Failed to send media to channel (%s), falling back to text: %s", photo_file_id, photo_exc)
                msg = await ctx.bot.send_message(
                    chat_id=target_chat_id,
                    text=final_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=channel_keyboard,
                )
        else:
            msg = await ctx.bot.send_message(
                chat_id=target_chat_id,
                text=final_caption,
                parse_mode=ParseMode.HTML,
                reply_markup=channel_keyboard,
            )

        db.update_post_msg_id(DB_PATH, post_id, msg.message_id)

        price_display = "FREE (₹0) 🎁" if is_free else f"₹{price:.0f}"
        button_link_info = f"Direct Channel Link ({channel['link']})" if (is_free and link_mode == "direct") else f"https://t.me/{bot_username}?start={'free' if is_free else 'buy'}_{post_id}"

        safe_pub_ch_name = html.escape(str(channel["name"] if channel else f"Channel #{channel_id}"))
        await query.edit_message_text(
            f"🎉 <b>Post Successfully Published!</b>\n\n"
            f"🏷️ <b>Channel:</b> <b>{safe_pub_ch_name}</b> (<code>#{channel_id}</code>)\n"
            f"📌 <b>Post ID:</b> <code>{post_id}</code>\n"
            f"📢 <b>Target Channel:</b> {html.escape(str(target))}\n"
            f"🔢 <b>Message ID:</b> <code>{msg.message_id}</code>\n"
            f"💰 <b>Price:</b> {price_display}\n"
            f"🔗 <b>Button Link:</b> <code>{html.escape(button_link_info)}</code>\n\n"
            f"✅ Subscribers can tap the button to join immediately!",
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:
        logger.error("Failed to publish to channel %s: %s", target, exc)
        await query.edit_message_text(
            f"❌ <b>Publish Failed!</b>\n\n"
            f"Telegram Error: <code>{exc}</code>\n\n"
            f"<b>Checklist to fix:</b>\n"
            f"1. Is the bot added to <b>{target}</b>?\n"
            f"2. Is the bot promoted to <b>Administrator</b> with 'Post Messages' permission?\n"
            f"3. Is the channel username or ID correct?",
            parse_mode=ParseMode.HTML,
        )

    ctx.user_data.clear()
    return ConversationHandler.END


async def cb_post_abort(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    ctx.user_data.clear()
    await query.edit_message_text("❌ Broadcast post publication cancelled.")
    return ConversationHandler.END


# ──────────────────────────────────────────────────────────────────────────────
# ADMIN — POSTS LIST & CHANNEL SETTINGS
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_posts(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """List recent broadcast posts."""
    if not is_admin(update.effective_user.id):
        return
    posts = db.get_recent_posts(DB_PATH, limit=10)
    if not posts:
        await update.message.reply_text("No broadcast posts created yet. Use /post to create one!")
        return

    lines = ["📢 <b>Recent Broadcast Posts</b>\n"]
    for p in posts:
        safe_p_name = html.escape(str(p["channel_name"] or f"Channel #{p['channel_id']}"))
        lines.append(
            f"<b>[Post #{p['id']}] {safe_p_name}</b> (<code>#{p['channel_id']}</code>)\n"
            f"  💰 Price: ₹{p['price_inr']:.0f} | 📢 Target: <code>{html.escape(str(p['target_channel'] or ''))}</code>\n"
            f"  📅 {p['created_at'][:16]} | 🔢 Msg ID: {p['channel_msg_id'] or 'N/A'}\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_clearposts(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Safeguard: inform admin that bulk post deletion has been permanently disabled."""
    if not is_admin(update.effective_user.id):
        return

    await update.message.reply_text(
        "🛡️ <b>Bulk Deletion Permanently Disabled</b>\n\n"
        "The <code>/clearposts</code> command has been removed to protect against accidental mass deletion of channel posts.\n\n"
        "<b>To delete a specific post safely:</b>\n"
        "• Use <code>/delpost &lt;message_id_or_link&gt;</code>\n"
        "• Or forward the channel post to this chat to delete it with one tap.",
        parse_mode=ParseMode.HTML,
    )


async def cmd_setmainchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Set default main channel for broadcasts."""
    if not is_admin(update.effective_user.id):
        return
    args = ctx.args
    if not args:
        curr = db.get_setting(DB_PATH, "main_channel_id") or MAIN_CHANNEL_ID or "Not set"
        await update.message.reply_text(
            f"Usage: /setmainchannel <@channel_username or channel_id>\n\n"
            f"Current default: <code>{curr}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    channel_target = args[0].strip()
    db.set_setting(DB_PATH, "main_channel_id", channel_target)
    await update.message.reply_text(
        f"✅ Default Main Channel set to: <code>{channel_target}</code>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_getid(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Get chat ID and information."""
    msg = update.effective_message
    chat = update.effective_chat

    if msg.forward_from_chat:
        fwd = msg.forward_from_chat
        await msg.reply_text(
            f"📋 <b>Forwarded Chat Info:</b>\n\n"
            f"Title: <b>{fwd.title}</b>\n"
            f"Type: <code>{fwd.type}</code>\n"
            f"Chat ID: <code>{fwd.id}</code>\n"
            f"Username: @{fwd.username or 'none'}",
            parse_mode=ParseMode.HTML,
        )
        return

    await msg.reply_text(
        f"📋 <b>Current Chat Info:</b>\n\n"
        f"Title/Name: <b>{chat.title or chat.first_name}</b>\n"
        f"Type: <code>{chat.type}</code>\n"
        f"Chat ID: <code>{chat.id}</code>\n"
        f"User ID: <code>{update.effective_user.id}</code>\n\n"
        f"💡 <i>Tip: Forward any message from your private channel here to quickly find its Channel Chat ID!</i>",
        parse_mode=ParseMode.HTML,
    )


# ──────────────────────────────────────────────────────────────────────────────
# JOIN REQUEST VERIFICATION ENGINE (ANTI-LEAK PROTECTION)
# ──────────────────────────────────────────────────────────────────────────────

async def handle_chat_join_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Automatically verifies and handles join requests for private channels.
    Approves if the requesting Telegram user has purchased access in database,
    otherwise declines and blocks unauthorized users.
    """
    join_req = update.chat_join_request
    user = join_req.from_user
    chat = join_req.chat

    channel = await asyncio.to_thread(db.get_channel_by_chat_id, DB_PATH, chat.id)
    if not channel:
        logger.warning("Join request for unmanaged chat %s (%d)", chat.title, chat.id)
        return

    if await db_has_access(user.id, channel["id"]):
        try:
            await join_req.approve()
            logger.info("Approved join request for user %d (%s) in chat %s (%d)", user.id, user.first_name, chat.title, chat.id)
            await ctx.bot.send_message(
                chat_id=user.id,
                text=f"✅ <b>Welcome to {chat.title}!</b>\nYour join request has been verified and approved.",
                parse_mode=ParseMode.HTML,
                protect_content=True,
            )
        except Exception as exc:
            logger.error("Failed to approve join request: %s", exc)
    else:
        try:
            await join_req.decline()
            logger.warning("Declined unauthorized join request for user %d in chat %s (%d)", user.id, chat.title, chat.id)
            await ctx.bot.send_message(
                chat_id=user.id,
                text=(
                    f"🚫 <b>Access Denied to {chat.title}</b>\n\n"
                    f"You have not purchased access to this channel.\n"
                    f"If you received this link from someone else, note that links are non-transferable.\n\n"
                    f"👉 To purchase valid access, visit @{BOT_USERNAME}"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            logger.warning("Failed to notify declined user %d: %s", user.id, exc)


# ──────────────────────────────────────────────────────────────────────────────
# MAIN — SETUP & RUN
# ──────────────────────────────────────────────────────────────────────────────

async def verify_and_prune_active_channels_fast(bot) -> list:
    """
    Fast startup pass: deduplicate duplicate active links and backfill missing generic channel titles.
    Does NOT auto-deactivate expired/dead links on startup so the 10-minute link monitor
    can alert admins first and let admins decide to deactivate & delete posts.
    """
    active_channels = await asyncio.to_thread(db.get_active_channels, DB_PATH)
    if not active_channels:
        return []

    seen_keys: dict[str, int] = {}
    unique_channels = []
    for ch in sorted(active_channels, key=lambda r: r["id"], reverse=True):
        cid = ch["id"]
        link = (row_get(ch, "link") or "").strip()
        key = _canonical_telegram_link_key(link)
        if not link or (key and key in seen_keys):
            await asyncio.to_thread(db.update_channel, DB_PATH, cid, is_active=0)
            logger.info("Deactivated duplicate/empty channel #%d (%s) from /menu.", cid, ch["name"])
            continue
        if key:
            seen_keys[key] = cid
        unique_channels.append(ch)

    unique_channels.sort(key=lambda r: r["id"])
    sem = asyncio.Semaphore(6)
    live_channels = []

    async def _check_one(ch):
        cid = ch["id"]
        cur_name = (row_get(ch, "name") or "").strip()
        link = (row_get(ch, "link") or "").strip()
        chat_id = row_get(ch, "chat_id")
        is_generic_name = bool(re.match(r"^VIP Channel #\d+$", cur_name, re.IGNORECASE))

        if is_generic_name and link:
            async with sem:
                is_alive, detail = await check_channel_link_alive(link, bot=bot, chat_id=chat_id)
            if is_alive:
                link_title = extract_clean_title_from_alive_detail(detail)
                if link_title:
                    await asyncio.to_thread(db.update_channel, DB_PATH, cid, name=link_title)
        return ch

    results = await asyncio.gather(*(_check_one(c) for c in unique_channels), return_exceptions=True)
    for res in results:
        if isinstance(res, (dict, object)) and res is not None and not isinstance(res, Exception):
            live_channels.append(res)

    invalidate_channel_cache()
    return live_channels


async def backfill_channels_metadata_worker(bot, live_active_channels: Optional[list] = None) -> None:
    """
    Background task: extract photo/video counts & header title via OCR for live active channels.
    """
    try:
        if live_active_channels is None:
            live_active_channels = await verify_and_prune_active_channels_fast(bot)

        for ch in live_active_channels:
            cid = ch["id"]
            fresh_ch = await asyncio.to_thread(db.get_channel, DB_PATH, cid) or ch
            if not row_get(fresh_ch, "is_active", 1):
                continue
            cur_name = (row_get(fresh_ch, "name") or "").strip()
            cur_media = (row_get(fresh_ch, "media_counts") or "").strip()
            link = (row_get(fresh_ch, "link") or "").strip()
            is_generic_name = bool(re.match(r"^VIP Channel #\d+$", cur_name, re.IGNORECASE))

            if not is_generic_name and cur_media:
                continue

            ocr_name, ocr_media = "", ""
            latest_post = await asyncio.to_thread(
                db.get_latest_post_for_channel, DB_PATH, cid, link, cur_name
            )
            if not (latest_post and latest_post["photo_file_id"]):
                latest_post = await asyncio.to_thread(
                    db.get_latest_post_for_channel, DB_PATH, cid, link, f"VIP Channel #{cid}"
                )

            if latest_post and latest_post["photo_file_id"]:
                fid = str(latest_post["photo_file_id"]).split("-100")[0].strip()
                if fid.startswith("AgAC"):
                    ocr_name, ocr_media = await extract_channel_metadata_from_file_id(bot, fid)
                    await asyncio.sleep(0.5)

            if not ocr_media and latest_post and latest_post["caption"]:
                _, cap_media = parse_channel_header_ocr_text(latest_post["caption"])
                if cap_media:
                    ocr_media = cap_media

            updates: dict = {}
            if is_generic_name and ocr_name:
                updates["name"] = ocr_name
            if not cur_media and ocr_media:
                updates["media_counts"] = ocr_media

            if updates:
                await asyncio.to_thread(db.update_channel, DB_PATH, cid, **updates)
                invalidate_channel_cache()
                logger.info("Backfilled active channel #%d metadata: %s", cid, updates)

        logger.info("Completed active channels verification & metadata backfill for /menu.")
    except asyncio.CancelledError:
        return
    except Exception as exc:
        logger.warning("Error in backfill_channels_metadata_worker: %s", exc)


async def post_init(app: Application) -> None:
    """Start Webhook server, background poller, and set bot commands."""
    global BOT_ID, BOT_USERNAME
    try:
        me = await app.bot.get_me()
        BOT_ID = me.id
        BOT_USERNAME = me.username or BOT_USERNAME
        logger.info("Bot authenticated: @%s (Bot ID: %s, Webhook Port: %d)", BOT_USERNAME, BOT_ID, WEBHOOK_PORT)
    except Exception as exc:
        logger.warning("Could not refresh bot profile in post_init: %s", exc)

    # 1. Start Webhook server for Razorpay
    try:
        runner = await start_webhook_server(app.bot)
        app.bot_data["webhook_runner"] = runner
    except Exception as exc:
        logger.error("Failed to start webhook server on port %d: %s", WEBHOOK_PORT, exc)

    # 2. Recover deleted post photo_file_ids & prune any expired/dead channels before serving /menu
    live_channels = None
    try:
        carved_count = await asyncio.to_thread(db.recover_deleted_posts_from_sqlite_bytes, DB_PATH)
        if carved_count:
            logger.info("Recovered %d deleted post record(s) (with photo/video file_ids) from SQLite storage.", carved_count)
        live_channels = await verify_and_prune_active_channels_fast(app.bot)
        logger.info("Verified %d active working channel(s) for /menu.", len(live_channels))
    except Exception as exc:
        logger.warning("Could not run active channel verification in post_init: %s", exc)

    # 3. Start background auto-poller, channel metadata backfill, and 10-minute link monitor job
    poller_task = asyncio.create_task(poll_pending_payments(app.bot))
    app.bot_data["poller_task"] = poller_task
    backfill_task = asyncio.create_task(backfill_channels_metadata_worker(app.bot, live_channels))
    app.bot_data["backfill_task"] = backfill_task
    monitor_task = asyncio.create_task(channel_link_monitor_worker(app.bot))
    app.bot_data["monitor_task"] = monitor_task

    # 4. Set bot commands
    user_cmds = [
        BotCommand("start", "Start the bot"),
        BotCommand("menu", "Browse available channels"),
        BotCommand("mypurchases", "View your purchased channel links"),
        BotCommand("help", "Instructions & FAQ guide"),
    ]
    admin_cmds = user_cmds + [
        BotCommand("post", "Create & broadcast paid post to Main Channel"),
        BotCommand("postfree", "Create & broadcast FREE channel post (no payment)"),
        BotCommand("reactivate", "Reactivate & repost deleted channels with original image"),
        BotCommand("posts", "List broadcast posts"),
        BotCommand("delpost", "Delete a specific post from channel"),
        BotCommand("setmainchannel", "Set default main channel"),
        BotCommand("addchannel", "Add a new channel product"),
        BotCommand("editchannel", "Edit a channel"),
        BotCommand("delchannel", "Deactivate a channel"),
        BotCommand("channels", "List all channels"),
        BotCommand("getid", "Get Chat ID of current or forwarded chat"),
        BotCommand("setchatid", "Set Channel Chat ID for Anti-Leak"),
        BotCommand("grantaccess", "Grant free access to a user"),
        BotCommand("broadcast", "Broadcast a message to all users"),
        BotCommand("stats", "View full statistics"),
        BotCommand("users", "List recent users"),
        BotCommand("payments", "List payments for a channel"),
        BotCommand("check", "Check payment status by ID"),
        BotCommand("mandatory", "View status of mandatory channels"),
        BotCommand("checklinks", "Validate channel links & get working links"),
        BotCommand("newlinks", "Compare links & get links not added in bot yet"),
        BotCommand("ban", "Ban a user"),
        BotCommand("unban", "Unban a user"),
    ]
    await app.bot.set_my_commands(user_cmds)
    for admin_id in ADMIN_IDS:
        try:
            from telegram import BotCommandScopeChat
            await app.bot.set_my_commands(
                admin_cmds, scope=BotCommandScopeChat(chat_id=admin_id)
            )
        except Exception:
            pass


async def post_shutdown(app: Application) -> None:
    """Cleanup webhook server and background poller on shutdown."""
    runner = app.bot_data.get("webhook_runner")
    if runner:
        await runner.cleanup()
        logger.info("Webhook server stopped.")
    poller_task = app.bot_data.get("poller_task")
    if poller_task:
        poller_task.cancel()
        logger.info("Background poller cancelled.")
    monitor_task = app.bot_data.get("monitor_task")
    if monitor_task:
        monitor_task.cancel()
        logger.info("Channel link monitor cancelled.")


async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log error and notify user/admin if possible."""
    err = context.error
    if isinstance(err, (BadRequest, Forbidden, TimedOut, NetworkError)):
        err_str = str(err).lower()
        if any(ign in err_str for ign in [
            "query is too old",
            "query id is invalid",
            "message is not modified",
            "message to delete not found",
            "message can't be deleted",
            "message to edit not found",
            "message can't be edited",
            "bot was blocked by the user",
            "user is deactivated",
            "chat not found",
            "timed out",
        ]):
            logger.debug("Harmless Telegram API condition ignored: %s", err)
            return

    logger.error("Exception while handling an update: %s", err, exc_info=err)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ An unexpected error occurred. Please try again or use /cancel to restart.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


def main() -> None:
    # Ensure an asyncio event loop exists in MainThread (required for Python 3.12/3.14+)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set in .env")
    if not ADMIN_IDS:
        raise ValueError("ADMIN_IDS is not set in .env")

    db.init_db(DB_PATH)
    logger.info("Starting bot… Admins: %s", ADMIN_IDS)

    # High-concurrency HTTPX connection pool for 5000+ user base
    bot_request = HTTPXRequest(
        connection_pool_size=64,
        connect_timeout=20.0,
        read_timeout=20.0,
        write_timeout=20.0,
        pool_timeout=30.0,
    )
    updates_request = HTTPXRequest(
        connection_pool_size=4,
        connect_timeout=20.0,
        read_timeout=35.0,
        write_timeout=20.0,
        pool_timeout=30.0,
    )

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(64)
        .request(bot_request)
        .get_updates_request(updates_request)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_error_handler(global_error_handler)

    # ── Cancel handler (global) ──────────────────────────────────────────────
    cancel_handler = CommandHandler("cancel", cmd_cancel)

    # ── Add Channel conversation ─────────────────────────────────────────────
    add_channel_conv = ConversationHandler(
        entry_points=[CommandHandler("addchannel", cmd_addchannel)],
        states={
            AC_NAME:    [MessageHandler(filters.TEXT & ~filters.COMMAND, ac_get_name)],
            AC_LINK:    [MessageHandler(filters.TEXT & ~filters.COMMAND, ac_get_link)],
            AC_PRICE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, ac_get_price)],
            AC_DESC:    [MessageHandler(filters.TEXT & ~filters.COMMAND, ac_get_desc)],
            AC_CHAT_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, ac_get_chat_id)],
        },
        fallbacks=[cancel_handler],
    )

    # ── Channel Broadcast Post wizard conversation ───────────────────────────
    post_conv = ConversationHandler(
        entry_points=[
            CommandHandler(["post", "broadcast_channel", "createpost"], cmd_post),
            CommandHandler(["postfree", "freepost", "broadcast_free"], cmd_postfree),
        ],
        states={
            POST_PHOTO: [
                MessageHandler(
                    filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.VIDEO,
                    post_get_photo,
                ),
                MessageHandler(filters.TEXT & ~filters.COMMAND, post_get_photo),
                CommandHandler("skip", post_get_photo),
            ],
            POST_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, post_get_text)],
            POST_CHANNEL: [
                CallbackQueryHandler(cb_post_channel, pattern=r"^post_ch:"),
                CallbackQueryHandler(cb_post_channel_page, pattern=r"^post_ch_page:"),
                CallbackQueryHandler(cb_menu_noop, pattern=r"^menu_noop$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, post_get_channel_link),
            ],
            POST_PRICE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, post_get_price),
                CommandHandler("skip", post_get_price),
            ],
            POST_CROSS_PRICE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, post_get_cross_price),
                CommandHandler("skip", post_get_cross_price),
            ],
            POST_TARGET: [
                CallbackQueryHandler(cb_post_target_default, pattern=r"^target_default$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, post_get_target),
            ],
            POST_PREVIEW: [
                CallbackQueryHandler(cb_post_publish, pattern=r"^post_publish$"),
                CallbackQueryHandler(cb_post_toggle_mode, pattern=r"^post_toggle_mode$"),
                CallbackQueryHandler(cb_post_abort, pattern=r"^post_abort$"),
            ],
        },
        fallbacks=[cancel_handler],
        per_message=False,
    )

    # ── Edit Channel conversation ────────────────────────────────────────────
    edit_channel_conv = ConversationHandler(
        entry_points=[CommandHandler("editchannel", cmd_editchannel)],
        states={
            EC_FIELD: [
                CallbackQueryHandler(cb_edit_select, pattern=r"^edit_select:"),
                CallbackQueryHandler(cb_edit_field, pattern=r"^edit_field:"),
                CallbackQueryHandler(cb_admin_channel_page, pattern=r"^adm_ch_page:edit_select:"),
                CallbackQueryHandler(cb_menu_noop, pattern=r"^menu_noop$"),
            ],
            EC_VALUE: [
                MessageHandler(
                    filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.VIDEO,
                    ec_get_value,
                ),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ec_get_value),
                CommandHandler("skip", ec_get_value),
            ],
        },
        fallbacks=[cancel_handler],
        per_message=False,
    )

    # ── Broadcast conversation ───────────────────────────────────────────────
    broadcast_conv = ConversationHandler(
        entry_points=[CommandHandler("broadcast", cmd_broadcast)],
        states={
            BC_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, bc_send)],
        },
        fallbacks=[cancel_handler],
    )

    # ── Payment verify conversation ──────────────────────────────────────────
    verify_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(cb_verify_prompt, pattern=r"^verify_prompt:"),
            CommandHandler("verify", lambda u, c: u.message.reply_text(
                "Please use the 💳 Buy button from /menu or a channel post to start a payment and verify it."
            )),
        ],
        states={
            VERIFY_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_verify_id)],
        },
        fallbacks=[cancel_handler],
        per_message=False,
    )

    # ── Check Links conversation ─────────────────────────────────────────────
    checklinks_conv = ConversationHandler(
        entry_points=[
            CommandHandler(["checklinks", "monitor"], cmd_checklinks),
        ],
        states={
            CHECKLINKS_WAIT: [
                MessageHandler(
                    (filters.TEXT | filters.CAPTION) & ~filters.COMMAND,
                    checklinks_receive_links,
                ),
            ],
        },
        fallbacks=[cancel_handler],
        allow_reentry=True,
    )

    # ── New / Unadded Links comparison conversation ──────────────────────────
    newlinks_conv = ConversationHandler(
        entry_points=[
            CommandHandler(
                ["newlinks", "comparelinks", "unadded", "missinglinks", "restlinks"],
                cmd_newlinks,
            ),
        ],
        states={
            NEWLINKS_WAIT: [
                MessageHandler(
                    (filters.TEXT | filters.CAPTION) & ~filters.COMMAND,
                    newlinks_receive_links,
                ),
            ],
        },
        fallbacks=[cancel_handler],
        allow_reentry=True,
    )

    # ── Register all handlers ────────────────────────────────────────────────
    app.add_handler(post_conv)
    app.add_handler(add_channel_conv)
    app.add_handler(edit_channel_conv)
    app.add_handler(broadcast_conv)
    app.add_handler(verify_conv)
    app.add_handler(checklinks_conv)
    app.add_handler(newlinks_conv)

    # Zero-load action tracker: records username in DB for ANY user interaction (group=-10 runs first)
    app.add_handler(TypeHandler(Update, track_user_action_fast), group=-10)

    # Join request firewall handler (anti-leak protection)
    app.add_handler(ChatJoinRequestHandler(handle_chat_join_request))
    app.add_handler(ChatMemberHandler(handle_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    # Basic user commands
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("menu",         cmd_menu))
    app.add_handler(CommandHandler("mypurchases",  cmd_mypurchases))
    app.add_handler(CommandHandler(["help", "instructions"], cmd_help))

    # Admin commands
    app.add_handler(CommandHandler("mandatory",      cmd_mandatory))
    app.add_handler(CommandHandler("posts",          cmd_posts))
    app.add_handler(CommandHandler(["clearposts", "delposts"], cmd_clearposts))
    app.add_handler(CommandHandler(["delpost", "deletepost"], cmd_delpost))
    app.add_handler(CommandHandler("setmainchannel", cmd_setmainchannel))
    app.add_handler(CommandHandler("getid",          cmd_getid))
    app.add_handler(CommandHandler("setchatid",      cmd_setchatid))
    app.add_handler(MessageHandler(filters.FORWARDED & filters.ChatType.PRIVATE, handle_admin_forward))
    app.add_handler(CommandHandler("channels",       cmd_channels))
    app.add_handler(CommandHandler(["reactivate", "restore", "repost"], cmd_reactivate))
    app.add_handler(CommandHandler("delchannel",     cmd_delchannel))
    app.add_handler(CommandHandler("grantaccess",    cmd_grantaccess))
    app.add_handler(CommandHandler("stats",          cmd_stats))
    app.add_handler(CommandHandler("users",          cmd_users))
    app.add_handler(CommandHandler("payments",       cmd_payments))
    app.add_handler(CommandHandler(["check", "checkpayment", "payment"], cmd_check_payment))
    app.add_handler(CommandHandler("ban",            cmd_ban))
    app.add_handler(CommandHandler("unban",          cmd_unban))

    # Inline button callbacks
    app.add_handler(CallbackQueryHandler(cb_check_subscription, pattern=r"^check_sub:"))
    app.add_handler(CallbackQueryHandler(cb_view_channel,  pattern=r"^view:"))
    app.add_handler(CallbackQueryHandler(cb_get_link,      pattern=r"^getlink:"))
    app.add_handler(CallbackQueryHandler(cb_buy_channel,   pattern=r"^buy:"))
    app.add_handler(CallbackQueryHandler(cb_check_payment, pattern=r"^check_?pay:"))
    app.add_handler(CallbackQueryHandler(cb_adm_force_pay, pattern=r"^adm_force_pay:"))
    app.add_handler(CallbackQueryHandler(cb_del_channel,   pattern=r"^del_channel:"))
    app.add_handler(CallbackQueryHandler(cb_deactivate_and_delete_channel, pattern=r"^deactivate_del_ch:"))
    app.add_handler(CallbackQueryHandler(cb_recheck_channel, pattern=r"^recheck_ch:"))
    app.add_handler(CallbackQueryHandler(cb_reactivate_channel, pattern=r"^reactivate_(?:ch|text):"))
    app.add_handler(CallbackQueryHandler(cb_del_fwd_post,  pattern=r"^del_fwd_post:"))
    app.add_handler(CallbackQueryHandler(cb_link_chatid,   pattern=r"^link_cid:"))
    app.add_handler(CallbackQueryHandler(cb_back_menu,     pattern=r"^back_menu$"))
    app.add_handler(CallbackQueryHandler(cb_menu_sort,     pattern=r"^menu_sort:"))
    app.add_handler(CallbackQueryHandler(cb_menu_page,     pattern=r"^menu_page:"))
    app.add_handler(CallbackQueryHandler(cb_menu_noop,     pattern=r"^menu_noop$"))
    app.add_handler(CallbackQueryHandler(cb_mypurchases_page, pattern=r"^mypurch_page:"))
    app.add_handler(CallbackQueryHandler(cb_admin_channel_page, pattern=r"^adm_ch_page:del_channel:"))
    app.add_handler(CallbackQueryHandler(cb_guide_help,    pattern=r"^guide_help$"))

    logger.info("Bot is running…")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        read_timeout=30,
        write_timeout=30,
        connect_timeout=20,
        pool_timeout=30,
    )


if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    main()

