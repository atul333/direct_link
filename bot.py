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
import io
import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

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
    """Return active channels from memory; refresh every 60 s."""
    global _active_channels_cache, _active_channels_cache_time
    if not _active_channels_cache or (time.time() - _active_channels_cache_time > _CHANNEL_CACHE_TTL):
        _active_channels_cache = db.get_active_channels(DB_PATH)
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

# Shared persistent AsyncClient for link health checks
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
) = range(16)

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


def channel_list_keyboard(channels: list, prefix: str = "buy") -> InlineKeyboardMarkup:
    buttons = []
    for c in channels:
        price_tag = "FREE 🎁" if c["price_inr"] <= 0 else f"₹{c['price_inr']:.0f} ⚡"
        buttons.append([InlineKeyboardButton(
            f"👑 {c['name']} • {price_tag}",
            callback_data=f"{prefix}:{c['id']}"
        )])
    return InlineKeyboardMarkup(buttons)


def admin_channel_keyboard(channels: list, prefix: str) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(
            f"[{c['id']}] {c['name']} ({'✅' if c['is_active'] else '❌'})",
            callback_data=f"{prefix}:{c['id']}"
        )]
        for c in channels
    ]
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


async def process_successful_payment(
    bot,
    order_id: str,
    payment_id: str,
    amount_inr: float | None = None,
    method: str | None = None,
) -> bool:
    """
    Auto-approve payment:
    1. Update database (mark paid, grant access)
    2. Delete the pending QR payment message from user's chat
    3. Deliver protected channel link to buyer (Access Granted message only)
    4. Notify all admins with full details
    """
    payment = db.get_payment_by_order(DB_PATH, order_id)
    if not payment:
        logger.warning("No payment record found for order %s", order_id)
        return False

    already_paid = (payment["status"] == "paid")
    user_id = payment["user_id"]
    channel_id = payment["channel_id"]
    channel = db.get_channel(DB_PATH, channel_id)
    amount = amount_inr if amount_inr is not None else payment["amount_inr"]

    if not already_paid:
        db.confirm_payment(DB_PATH, order_id, payment_id)
        QR_CACHE.pop((user_id, channel_id), None)
        logger.info("Payment confirmed in DB: order=%s, payment=%s, user=%d, channel=%d",
                    order_id, payment_id, user_id, channel_id)
    else:
        logger.info("Payment %s already marked as paid. Ensuring delivery...", order_id)

    # ── Delete the QR payment message from user's chat ────────────────────────
    # 1. Try in-memory cache first (check by order_id, then user_id)
    qr_msg_info = QR_MESSAGE_CACHE.pop(order_id, None) or QR_MESSAGE_CACHE.pop(user_id, None)
    # 2. Fallback: look up from database (survives bot restarts)
    if not qr_msg_info:
        qr_msg_info = db.get_qr_message_info(DB_PATH, order_id)
    if not qr_msg_info:
        qr_msg_info = db.get_latest_qr_message_for_user(DB_PATH, user_id)
    if qr_msg_info:
        try:
            await bot.delete_message(
                chat_id=qr_msg_info["chat_id"],
                message_id=qr_msg_info["message_id"],
            )
            logger.info("Deleted QR payment message for user %d (msg_id=%s)", user_id, qr_msg_info["message_id"])
        except Exception as exc:
            logger.debug("Could not delete QR message for user %d: %s", user_id, exc)

    user = db.get_user(DB_PATH, user_id)
    uname = format_user_display(user['username'], fallback="None") if user else "None"
    full_name = f"{user['first_name']} {user['last_name']}".strip() if user else "Telegram User"

    # ── Deliver Access Granted message + channel link to buyer ────────────────
    try:
        if channel:
            await send_protected_channel_link(bot, user_id, channel)
    except Exception as exc:
        logger.error("Failed to send channel link to user %d: %s", user_id, exc)

    # ── Notify admins (only on first approval) ────────────────────────────────
    if not already_paid:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        isolated_tx_id = f"CLP5000-{payment_id}" if payment_id and not payment_id.startswith("CLP") else (payment_id or "N/A")
        isolated_ref_id = f"CLP5000-{order_id}" if order_id and not order_id.startswith("CLP") else order_id

        admin_text = (
            "<blockquote><b>💸 New Sale! Payment Auto-Approved</b></blockquote>\n"
            "<blockquote>"
            f"👤 <b>User ID:</b> <code>{user_id}</code>\n"
            f"👤 <b>Username:</b> {uname}\n"
            f"👤 <b>Name:</b> {full_name}\n"
            f"🏷️ <b>Channel:</b> <b>{channel['name'] if channel else f'Channel #{channel_id}'}</b>\n"
            f"💰 <b>Amount Paid:</b> <b>₹{amount:.0f}</b>\n"
            f"🔒 <b>Isolated Port:</b> <code>{WEBHOOK_PORT}</code>\n"
            f"🧾 <b>Payment Ref:</b> <code>{isolated_ref_id}</code>\n"
            f"💳 <b>Transaction ID:</b> <code>{isolated_tx_id}</code>\n"
            f"📲 <b>Method:</b> {method or 'UPI / Auto'}\n"
            f"⏱️ <b>Time:</b> {now_str}</blockquote>\n\n"
            f"✅ <i>Access link delivered with anti-leak protection! (Port {WEBHOOK_PORT} only)</i>"
        )
        for aid in get_admin_ids():
            try:
                await bot.send_message(chat_id=aid, text=admin_text, parse_mode=ParseMode.HTML)
                logger.info("Admin %d notified of payment %s", aid, payment_id)
            except Exception as exc:
                logger.error("Failed to notify admin %d: %s", aid, exc)

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
            payment = db.find_payment(DB_PATH, ref_id)
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
            await process_successful_payment(bot, ref_id, payment_id, amount_inr, method)

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


async def _check_single_pending_payment(bot, client, p, sem: asyncio.Semaphore) -> None:
    """Non-blocking worker for checking a single pending payment off the event loop."""
    ref_id = p["razorpay_order_id"]
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
    sem = asyncio.Semaphore(4)
    cycle = 0
    while True:
        try:
            await asyncio.sleep(6)
            cycle += 1

            # Tier 1: Active recent QRs (last 15 minutes) — checked every 6s
            pending_orders = await asyncio.to_thread(
                db.get_all_pending_payments, DB_PATH, minutes=15, limit=25
            )

            # Tier 2: Older QRs (15 to 90 minutes) — checked once every ~60s + expire >2h stale rows
            if cycle % 10 == 0:
                older_orders = await asyncio.to_thread(
                    db.get_all_pending_payments, DB_PATH, minutes=90, min_age_minutes=15, limit=25
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


async def check_channel_link_alive(link: str, bot=None, chat_id=None) -> tuple[bool, str]:
    """
    Check if a Telegram channel link is active and valid.
    Returns (is_alive: bool, reason: str).
    Guaranteed NEVER to falsely mark channels dead due to rate limiting or anti-bot checks.
    """
    # 1. If chat_id is available and bot is passed, verify via authoritative Telegram Bot API
    if bot and chat_id:
        try:
            try:
                target_chat_id = int(chat_id)
            except (ValueError, TypeError):
                target_chat_id = chat_id
            chat = await bot.get_chat(target_chat_id)
            return True, f"Active (verified via Telegram API: {chat.title})"
        except Exception as exc:
            err_msg = str(exc).lower()
            if "chat not found" in err_msg or "deactivated" in err_msg:
                return False, f"Telegram API confirmed: {exc}"
            # For admin/permissions errors, do not treat channel as dead

    # 2. Check the web preview of the link
    clean_link = (link or "").strip()
    if not clean_link:
        return False, "Empty channel link"

    if not clean_link.startswith("http"):
        if clean_link.startswith("t.me"):
            clean_link = f"https://{clean_link}"
        else:
            clean_link = f"https://t.me/{clean_link.lstrip('@')}"

    try:
        resp = await _ASYNC_HTTP_CLIENT.get(clean_link)
        # Transient rate limits or server errors MUST NEVER mark channel dead!
        if resp.status_code in (429, 500, 502, 503, 504):
            logger.warning("[LINK CHECK] Transient HTTP %d for %s — skipping check", resp.status_code, clean_link)
            return True, f"Telegram rate limit/busy (HTTP {resp.status_code}) — treated as alive"

        if resp.status_code == 404:
            return False, "Link not found (HTTP 404)"

        if resp.status_code != 200:
            logger.warning("[LINK CHECK] Unexpected HTTP %d for %s — skipping check", resp.status_code, clean_link)
            return True, f"HTTP status {resp.status_code} (skipped)"

        text = resp.text

        # Anti-bot / Cloudflare / DDoS challenge protection:
        if any(pat in text for pat in ["Just a moment...", "cf-chl-", "challenge-running", "Enable JavaScript and cookies to continue", "Attention Required"]):
            logger.warning("[LINK CHECK] Anti-bot challenge page detected for %s — skipping", clean_link)
            return True, "Anti-bot challenge received — treated as alive"

        # Verify it's actually Telegram web:
        if "tgme_page" not in text and "tgme_head" not in text:
            logger.warning("[LINK CHECK] Non-Telegram response format for %s — skipping", clean_link)
            return True, "Non-standard page format — treated as alive"

        # Genuine Telegram dead/banned indicators:
        if "This channel can’t be displayed because it was blocked" in text:
            return False, "Channel is blocked by Telegram"

        if "Channel is inaccessible" in text:
            return False, "Channel is inaccessible"

        # For invite links (t.me/+... or joinchat):
        is_invite = "/+" in clean_link or "joinchat" in clean_link
        if is_invite:
            # If there's an action button with tg://join?invite= or Join Channel/Group, it's ACTIVE!
            if "tg://join?invite=" in text or "tgme_action_button" in text or "tgme_page_action" in text:
                return True, "Active (Invite link valid)"

            # If there's a title, subscriber count, or photo, it's ACTIVE!
            if "tgme_page_title" in text or "tgme_page_extra" in text or "tgme_page_photo" in text:
                return True, "Active"

            # If the page structure is present, it's a valid Telegram page
            if "tgme_page" in text:
                return True, "Active (Telegram invite page)"

            return True, "Active"
        else:
            # Public channel/username
            if "tgme_page_title" in text:
                return True, "Active"
            if "tgme_page_icon" in text and "tgme_page_extra" not in text and "tgme_action_button" not in text:
                return False, "Channel username not found"
            return True, "Active"

    except Exception as exc:
        logger.warning("[LINK CHECK] Network exception for %s: %s", clean_link, exc)
        return True, f"Network check skipped ({exc})"


async def cleanup_dead_channel(bot, channel: dict, reason: str) -> int:
    """
    Safely deactivate an expired/banned channel:
    1. Delete promotional posts for this channel from main channel(s)
    2. Soft-deactivate channel in database (is_active = 0) — NEVER purge user purchases or channel records!
    3. Remove promotional posts from DB
    4. Clear QR cache
    5. Notify admins with detailed breakdown
    Returns count of successfully deleted promotional posts.
    """
    channel_id = channel["id"]
    channel_name = channel["name"]
    channel_link = row_get(channel, "link", "")

    # 1. Find all promotional posts referencing this channel (by ID, link hash, or title)
    posts = db.get_posts_for_channel(DB_PATH, channel_id, channel_link, channel_name)
    deleted_posts_count = 0
    post_notes = []

    default_main = db.get_setting(DB_PATH, "main_channel_id") or MAIN_CHANNEL_ID

    # 2. Delete posts from main channels
    for p in posts:
        target = p["target_channel"] or default_main
        msg_id = p["channel_msg_id"]
        if target and msg_id:
            try:
                try:
                    target_chat_id = int(target)
                except (ValueError, TypeError):
                    target_chat_id = target
                await bot.delete_message(chat_id=target_chat_id, message_id=msg_id)
                logger.info(
                    "Deleted dead channel promotional post #%s (msg_id=%s) from %s",
                    p["id"], msg_id, target
                )
                deleted_posts_count += 1
                db.delete_post(DB_PATH, p["id"])
            except Exception as exc:
                err_str = str(exc)
                if "message to delete not found" in err_str.lower():
                    logger.info("Promotional post msg_id=%s was already deleted from %s", msg_id, target)
                    deleted_posts_count += 1
                    db.delete_post(DB_PATH, p["id"])
                else:
                    logger.warning(
                        "Could not delete message %s from %s for post %d: %s",
                        msg_id, target, p["id"], exc
                    )
                    post_notes.append(f"Msg #{msg_id} in {target}: {exc}")

    # 3. Soft-deactivate channel in database — PRESERVES all user access & sales records!
    db.deactivate_channel(DB_PATH, channel_id)
    logger.info("Soft-deactivated dead channel #%d (%s) in database (user access preserved).", channel_id, channel_name)

    # 4. Invalidate QR cache
    dead_keys = [k for k in list(QR_CACHE.keys()) if k[1] == channel_id]
    for k in dead_keys:
        QR_CACHE.pop(k, None)

    # 5. Build informative admin alert
    if deleted_posts_count > 0:
        cleanup_status = f"✅ Deleted <b>{deleted_posts_count}</b> post(s) from main channel"
    elif post_notes:
        cleanup_status = f"⚠️ Could not delete message from channel:\n" + "\n".join(f"• {n}" for n in post_notes)
    else:
        cleanup_status = (
            "ℹ️ <b>No promotional posts found in database for this channel.</b>\n"
            "💡 <i>To delete an existing post from the channel manually:</i>\n"
            "• Use <code>/delpost [message_id_or_link]</code>\n"
            "• Or simply <b>forward the channel post to this bot</b> in private chat to delete it with one tap!"
        )

    alert_msg = (
        f"🚨 <b>Channel Link Monitor Alert</b> 🚨\n\n"
        f"The following channel link expired or the channel was banned:\n\n"
        f"🏷️ <b>Channel:</b> {channel_name} (ID: #{channel_id})\n"
        f"🔗 <b>Link:</b> <code>{channel_link}</code>\n"
        f"⚠️ <b>Reason:</b> {reason}\n\n"
        f"🧹 <b>Automatic Cleanup Executed:</b>\n"
        f"{cleanup_status}\n"
        f"✅ Channel deactivated (is_active=0)\n"
        f"✅ Existing user purchases preserved in database\n"
        f"✅ Access to post links blocked (users cannot purchase dead link)\n\n"
        f"💡 <i>To update the link and reactivate: <code>/editchannel</code></i>\n"
        f"💡 <i>To delete any lingering post manually: <code>/delpost [message_id]</code></i>"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=alert_msg,
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            logger.warning("Failed to notify admin %d of channel cleanup: %s", admin_id, exc)

    return deleted_posts_count


async def channel_link_monitor_worker(bot) -> None:
    """
    Background worker periodically monitoring all active channels.

    Safety rules:
    1. CIRCUIT BREAKER: If ANY 2+ channels fail in the same cycle -> suspected network/rate-limit
       outage. All fail counters are FULLY RESET to 0 (not just decremented). No cleanup runs.
    2. FAIL THRESHOLD: A single channel must fail FAIL_THRESHOLD consecutive cycles to be deactivated.
    3. RECOVERY: A single successful check resets that channel's fail counter to 0.
    4. MIDNIGHT PROTECTION: Skips all checks between 00:30-01:30 local time (Telegram maintenance window).
    5. CHAT_ID PRIORITY: If a channel has a chat_id set, uses authoritative Telegram Bot API
       (never web scraping) - false positives are impossible via Bot API.
    """
    logger.info("Channel link monitor worker started.")
    check_interval = int(os.getenv("CHANNEL_MONITOR_INTERVAL_SECONDS", "300"))
    FAIL_THRESHOLD = int(os.getenv("CHANNEL_FAIL_THRESHOLD", "7"))  # ~35 min of persistent failures

    await asyncio.sleep(30)

    while True:
        try:
            # ---- MIDNIGHT PROTECTION -----------------------------------------
            # Telegram's t.me web pages are frequently rate-limited or return
            # challenge pages between midnight and 01:30. Skip checks in this window.
            now_dt = datetime.now()
            now_hour = now_dt.hour
            now_minute = now_dt.minute
            if (now_hour == 0 and now_minute >= 30) or now_hour == 1:
                logger.info("[LINK MONITOR] Skipping check during midnight maintenance window (00:30-01:59).")
                await asyncio.sleep(check_interval)
                continue

            channels = db.get_active_channels(DB_PATH)
            if not channels:
                await asyncio.sleep(check_interval)
                continue

            failed_this_cycle = []
            passed_this_cycle = []

            for ch in channels:
                cid = ch["id"]
                link = ch["link"]
                chat_id = row_get(ch, "chat_id")

                # Stagger requests to avoid rate limits
                await asyncio.sleep(3)

                is_alive, reason = await check_channel_link_alive(link, bot=bot, chat_id=chat_id)
                if not is_alive:
                    failed_this_cycle.append((ch, reason))
                    _channel_consecutive_failures[cid] = _channel_consecutive_failures.get(cid, 0) + 1
                    logger.warning(
                        "[LINK MONITOR] Channel #%d (%s) failed check (%d/%d): %s",
                        cid, ch["name"], _channel_consecutive_failures[cid], FAIL_THRESHOLD, reason
                    )
                else:
                    passed_this_cycle.append(ch)
                    if cid in _channel_consecutive_failures and _channel_consecutive_failures[cid] > 0:
                        logger.info("[LINK MONITOR] Channel #%d (%s) recovered -- resetting fail count.", cid, ch["name"])
                    _channel_consecutive_failures[cid] = 0

            # ---- CIRCUIT BREAKER ---------------------------------------------
            # If 2 or more channels failed in the SAME cycle, this is almost certainly
            # a network outage, Telegram rate limit, or IP block -- NOT all channels dying!
            if len(failed_this_cycle) >= 2:
                logger.error(
                    "[CIRCUIT BREAKER TRIGGERED] %d/%d channels failed in one cycle! "
                    "Suspected Telegram rate-limit or network outage. "
                    "FULLY RESETTING all fail counters -- no cleanup will run.",
                    len(failed_this_cycle), len(channels)
                )
                # Fully reset ALL fail counters -- a mass failure is never genuine
                for ch, _ in failed_this_cycle:
                    _channel_consecutive_failures[ch["id"]] = 0
                # Notify admin of the outage (but DO NOT deactivate anything)
                for admin_id in ADMIN_IDS:
                    try:
                        names = ", ".join(ch["name"] for ch, _ in failed_this_cycle)
                        await bot.send_message(
                            chat_id=admin_id,
                            text=(
                                "<b>Link Monitor: Network Outage Detected</b>\n\n"
                                f"<b>{len(failed_this_cycle)}</b> channels failed health check simultaneously "
                                "-- this is a Telegram rate-limit or IP block, NOT a real channel issue.\n\n"
                                f"<b>Channels affected:</b> {names}\n\n"
                                "<i>No channels were deactivated. All fail counters reset.</i>"
                            ),
                            parse_mode="HTML",
                        )
                    except Exception:
                        pass
            else:
                # Only 0 or 1 channel failed this cycle -- could be a genuine dead channel
                for ch in channels:
                    cid = ch["id"]
                    if _channel_consecutive_failures.get(cid, 0) >= FAIL_THRESHOLD:
                        logger.error(
                            "[LINK MONITOR] Channel #%d (%s) confirmed dead after %d consecutive cycles. "
                            "Initiating safe deactivation...",
                            cid, ch["name"], FAIL_THRESHOLD
                        )
                        _channel_consecutive_failures[cid] = 0
                        await cleanup_dead_channel(
                            bot, dict(ch),
                            f"Persistent failure across {FAIL_THRESHOLD} consecutive single-channel checks (~{FAIL_THRESHOLD * check_interval // 60} min)"
                        )

            await asyncio.sleep(check_interval)
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


async def cmd_checklinks(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to manually trigger health check of all channel links."""
    if not is_admin(update.effective_user.id):
        return

    channels = get_active_channels_cached()
    if not channels:
        await update.message.reply_text("ℹ️ No active channels found in database.")
        return

    msg = await update.message.reply_text("🔍 <b>Checking all channel links...</b>\nPlease wait.", parse_mode=ParseMode.HTML)

    report_lines = []
    cleaned_count = 0

    for ch in channels:
        ch_chat_id = row_get(ch, "chat_id")
        is_alive, reason = await check_channel_link_alive(ch["link"], bot=ctx.bot, chat_id=ch_chat_id)
        if is_alive:
            report_lines.append(f"✅ <b>{ch['name']}</b>: Active\n<code>{ch['link']}</code>")
        else:
            # Re-verify
            r1, _ = await check_channel_link_alive(ch["link"], bot=ctx.bot, chat_id=ch_chat_id)
            if not r1:
                deleted_posts = await cleanup_dead_channel(ctx.bot, dict(ch), reason)
                cleaned_count += 1
                report_lines.append(
                    f"❌ <b>{ch['name']}</b>: Expired/Banned\n"
                    f"🧹 Cleaned up {deleted_posts} post(s) & purged from DB"
                )

    summary_text = (
        f"📊 <b>Channel Link Health Check Report</b>\n\n"
        + "\n\n".join(report_lines)
        + f"\n\n🏁 <b>Summary:</b> {len(channels) - cleaned_count} active, {cleaned_count} removed."
    )
    await msg.edit_text(summary_text, parse_mode=ParseMode.HTML)


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
        for c in channels:
            buttons.append([
                InlineKeyboardButton(f"🛡️ Link Chat ID to {c['name']} (ID #{c['id']})", callback_data=f"link_cid:{c['id']}:{chat_id}")
            ])

        info_lines = [
            f"📢 <b>Forwarded Channel Post Detected!</b>\n",
            f"• <b>Channel:</b> <b>{title}</b>",
            f"• <b>Chat ID:</b> <code>{chat_id}</code>",
        ]
        if fwd_msg_id:
            info_lines.append(f"• <b>Message ID:</b> <code>{fwd_msg_id}</code>")

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


def build_instant_payment_info(
    channel_name: str,
    price: float,
    cross_price: float | None = None,
    description: str | None = None,
) -> str:
    """Instant first message showing channel details, price, and payment instructions before QR generation."""
    if cross_price and cross_price > price:
        price_line = f"<b>💰 Offer Price:</b> <s>₹{cross_price:.0f}</s>  <b>₹{price:.0f}</b> <i>(Special Offer 🔥)</i>"
    else:
        price_line = f"<b>💰 Access Price:</b> <b>₹{price:.0f}</b> <i>(One-Time Fee)</i>"

    desc_block = ""
    if description:
        desc_block = f"<b>📖 Details:</b> <i>{description}</i>\n"

    return (
        f"<blockquote><b>👑 {channel_name} — VIP ACCESS 👑</b></blockquote>\n"
        f"<blockquote><b>🏷️ Channel:</b> <b>{channel_name}</b>\n"
        f"{price_line}\n"
        f"{desc_block}"
        f"<b>⚡ Validity:</b> Lifetime Access • Instant Auto-Delivery\n"
        f"<b>💳 Payment Mode:</b> All UPI Apps (GPay • PhonePe • Paytm • BHIM)</blockquote>\n"
        f"<blockquote><b>📲 NEXT STEP:</b>\n"
        f"⏳ <b>Generating your unique ₹{price:.0f} UPI QR Code below...</b>\n"
        f"<i>Simply scan the QR code that appears next and your private invite link will be sent here automatically within 3–5 seconds! 🚀</i></blockquote>"
    )


def build_qr_payment_caption(channel_name: str, price: float, qr_id: str) -> str:
    """Rich, engaging payment QR caption with clear instructions and advanced emojis."""
    return (
        f"<blockquote><b>👑 SCAN & PAY FOR VIP ACCESS 👑</b></blockquote>\n"
        f"<blockquote><b>🏷️ Channel:</b> <b>{channel_name}</b>\n"
        f"<b>💵 Pay Amount:</b> <b>₹{price:.0f}</b> <i>(One-Time Fee)</i>\n"
        f"<b>🧾 Payment Ref ID:</b> <code>{qr_id}</code></blockquote>\n"
        f"<blockquote><b>📲 HOW TO PAY (FAST & EASY):</b>\n"
        f"1️⃣ <b>Scan the QR Code</b> above with any UPI app\n"
        f"   └ <i>GPay • PhonePe • Paytm • BHIM • CRED</i>\n"
        f"2️⃣ Complete the payment of <b>₹{price:.0f}</b>\n"
        f"3️⃣ <b>Wait 3–5 seconds</b> — payment is auto-detected!\n"
        f"   └ <i>Your VIP link will be delivered right here instantly! 🚀</i></blockquote>\n"
        f"<blockquote>⚠️ <b>ONE-TIME LINK — NO BACKUP AVAILABLE</b>\n"
        f"──────────────────────────\n"
        f"🏆 Admin - {ADMIN_CONTACT}\n"
        f"💬 <b>Need help?</b> Share your Ref ID (<code>{qr_id}</code>) with Admin.</blockquote>"
    )


async def _background_send_qr(
    bot: Bot,
    user_id: int,
    chat_id: int,
    channel_id: int,
    channel_name: str,
    price: float,
    post_id: int | None = None,
) -> None:
    """Background task: generates QR code off the hot path and sends the QR photo immediately after the price message."""
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

    caption_text = build_qr_payment_caption(channel_name, price, qr_id)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("« Browse All Channels", callback_data="back_menu")],
    ])

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
            await asyncio.to_thread(
                db.store_qr_message_id, DB_PATH, qr_id, qr_msg.chat_id, qr_msg.message_id
            )
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

    # STEP 1: Immediately send the price & channel details message first (< 0.1s response)
    info_text = build_instant_payment_info(
        channel_name=channel["name"],
        price=price,
        cross_price=cross_price,
        description=row_get(channel, "description"),
    )
    await reply_or_send_text(update, ctx, info_text)

    # STEP 2: Generate and send the QR code right after in a background task
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

    # STEP 1: Immediately send the price & channel details message first (< 0.1s response)
    info_text = build_instant_payment_info(
        channel_name=channel["name"],
        price=price,
        description=row_get(channel, "description"),
    )
    await reply_or_send_text(update, ctx, info_text)

    # STEP 2: Generate and send the QR code right after in a background task
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

    text = (
        "<blockquote><b>👑 EXCLUSIVE VIP CHANNELS DIRECTORY 👑</b></blockquote>\n"
        "<blockquote>✨ Select any VIP channel below to view full details, perks & unlock instant access:\n\n"
        "⚡ <i>Instant automated UPI payment • Protected 1-click delivery</i>\n"
        "──────────────────────────\n"
        f"🏆 Admin - {ADMIN_CONTACT}</blockquote>"
    )
    await reply_or_send_text(
        update, ctx,
        text,
        reply_markup=channel_list_keyboard(channels, "view"),
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
    buttons = [
        [InlineKeyboardButton(f"✨ {p['name']} 🔓", callback_data=f"getlink:{p['id']}")]
        for p in purchases
    ]
    buttons.append([InlineKeyboardButton("🛍️ Browse More Channels", callback_data="back_menu")])
    await reply_or_send_text(
        update, ctx,
        text,
        reply_markup=InlineKeyboardMarkup(buttons),
        protect_content=True,
    )


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
            "• /checklinks — Live scan all links & purge dead/banned channels\n"
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
    if not channel:
        await query.edit_message_text("❌ Channel not found.")
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

    # STEP 1: Immediately show price & channel payment details first
    info_text = build_instant_payment_info(
        channel_name=channel["name"],
        price=price,
        description=row_get(channel, "description"),
    )
    try:
        await query.edit_message_text(
            info_text,
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass

    # STEP 2: Generate and send QR code right after in a background task
    chat_id = query.message.chat_id if query.message else user_id
    asyncio.create_task(
        _background_send_qr(
            bot=ctx.bot,
            user_id=user_id,
            chat_id=chat_id,
            channel_id=channel_id,
            channel_name=channel["name"],
            price=price,
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
    text = "🛍️ <b>Available Channels</b>\n\nTap a channel to get details:" if channels else "😔 No channels available right now."
    markup = channel_list_keyboard(channels, "view") if channels else None

    try:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    except Exception:
        try:
            await query.message.delete()
        except Exception:
            pass
        await reply_or_send_text(update, ctx, text, reply_markup=markup)


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
    if not link.startswith("http") and not link.startswith("t.me"):
        await update.message.reply_text("⚠️ Please enter a valid URL starting with http/https or t.me.")
        return AC_LINK

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

async def cmd_editchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    channels = db.get_all_channels(DB_PATH)
    if not channels:
        await update.message.reply_text("No channels to edit.")
        return ConversationHandler.END
    await update.message.reply_text(
        "✏️ <b>Edit Channel</b>\n\nSelect a channel:",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_channel_keyboard(channels, "edit_select"),
    )
    return EC_FIELD


async def cb_edit_select(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    channel_id = int(query.data.split(":")[1])
    ctx.user_data["edit_channel_id"] = channel_id
    channel = db.get_channel(DB_PATH, channel_id)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📛 Name",        callback_data="edit_field:name")],
        [InlineKeyboardButton("🔗 Link",        callback_data="edit_field:link")],
        [InlineKeyboardButton("💰 Price (₹)",   callback_data="edit_field:price_inr")],
        [InlineKeyboardButton("📝 Description", callback_data="edit_field:description")],
        [InlineKeyboardButton("🛡️ Chat ID",     callback_data="edit_field:chat_id")],
        [InlineKeyboardButton(
            "🟢 Activate" if not channel["is_active"] else "🔴 Deactivate",
            callback_data="edit_field:toggle_active"
        )],
    ])
    await query.edit_message_text(
        f"Editing: <b>{channel['name']}</b>\nWhat do you want to change?",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )
    return EC_FIELD


async def cb_edit_field(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    field = query.data.split(":")[1]
    ctx.user_data["edit_field"] = field
    channel_id = ctx.user_data["edit_channel_id"]

    if field == "toggle_active":
        channel = db.get_channel(DB_PATH, channel_id)
        new_state = 0 if channel["is_active"] else 1
        db.update_channel(DB_PATH, channel_id, is_active=new_state)
        state_str = "activated ✅" if new_state else "deactivated ❌"
        await query.edit_message_text(f"Channel {state_str}.")
        return ConversationHandler.END

    labels = {
        "name": "new name",
        "link": "new invite link",
        "price_inr": "new price in ₹",
        "description": "new description",
        "chat_id": "Channel Chat ID (e.g. -1001234567890 or type 'none' to remove)",
    }
    await query.message.reply_text(f"Enter the {labels.get(field, field)}:")
    return EC_VALUE


async def ec_get_value(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    field = ctx.user_data.get("edit_field")
    channel_id = ctx.user_data.get("edit_channel_id")
    value = update.message.text.strip()

    if field == "price_inr":
        try:
            value = float(value)
        except ValueError:
            await update.message.reply_text("⚠️ Enter a valid number.")
            return EC_VALUE
    elif field == "chat_id":
        if value.lower() in ("none", "clear", "skip"):
            value = None

    db.update_channel(DB_PATH, channel_id, **{field: value})
    ctx.user_data.clear()
    await update.message.reply_text(f"✅ Channel updated successfully!")
    return ConversationHandler.END


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
        "🗑️ Select a channel to deactivate:",
        reply_markup=admin_channel_keyboard(channels, "del_channel"),
    )


async def cb_del_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    channel_id = int(query.data.split(":")[1])
    db.delete_channel(DB_PATH, channel_id)
    await query.edit_message_text("✅ Channel deactivated.")


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

    lines = ["📋 <b>All Channels</b>\n"]
    for c in channels:
        status = "✅ Active" if c["is_active"] else "❌ Inactive"
        has_cid = "chat_id" in c.keys() and c["chat_id"]
        cid_info = f"<code>{c['chat_id']}</code> (Join-Request Protected 🛡️)" if has_cid else "Not set (Static link)"
        lines.append(
            f"<b>[{c['id']}] {c['name']}</b> — {status}\n"
            f"   💰 ₹{c['price_inr']:.0f} | 📅 {c['created_at'][:10]}\n"
            f"   🛡️ Chat ID: {cid_info}\n"
            f"   🔗 {c['link']}\n"
        )
    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML,
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
    stats = db.get_stats(DB_PATH)

    ch_lines = ""
    for ch in stats["channel_breakdown"]:
        ch_lines += (
            f"\n  📌 <b>{ch['name']}</b> — "
            f"{ch['sales']} sales | ₹{ch['revenue']:.0f}"
        )

    text = (
        "<blockquote><b>📊 Bot Statistics</b></blockquote>\n"
        "<blockquote>"
        f"👥 Total Users:      <b>{stats['total_users']}</b>\n"
        f"✅ Active Users:     <b>{stats['active_users']}</b>\n"
        f"📌 Total Channels:   <b>{stats['total_channels']}</b>\n"
        f"🟢 Active Channels:  <b>{stats['active_channels']}</b></blockquote>\n"
        "<blockquote>"
        f"💰 Total Revenue:    <b>₹{stats['total_revenue']:.2f}</b>\n"
        f"✅ Paid Txns:        <b>{stats['total_paid_txns']}</b>\n"
        f"⏳ Pending Payments: <b>{stats['pending_payments']}</b></blockquote>\n"
        "<blockquote><b>Per Channel Revenue:</b>"
        f"{ch_lines or '  None yet'}</blockquote>"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


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
    if msg.photo:
        ctx.user_data["post_photo"] = msg.photo[-1].file_id
        ctx.user_data["post_media_type"] = "photo"
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
        buttons = []
        for c in channels:
            p_label = "FREE 🎁" if (is_free or c["price_inr"] <= 0) else f"₹{c['price_inr']:.0f}"
            buttons.append([InlineKeyboardButton(f"📌 {c['name']} ({p_label})", callback_data=f"post_ch:{c['id']}")])
        buttons.append([InlineKeyboardButton("➕ Paste New Invite Link", callback_data="post_ch:new")])
        await update.message.reply_text(
            f"🔗 <b>{step_label}</b>\n\n"
            f"Select an existing channel below, OR directly <b>send/paste a channel invite link</b> (e.g. <code>https://t.me/+...</code>):\n\n"
            f"✨ <i>Description & emojis are generated automatically!</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
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
        buttons = []
        for c in channels:
            p_label = "FREE 🎁" if (is_free or c["price_inr"] <= 0) else f"₹{c['price_inr']:.0f}"
            buttons.append([InlineKeyboardButton(f"📌 {c['name']} ({p_label})", callback_data=f"post_ch:{c['id']}")])
        buttons.append([InlineKeyboardButton("➕ Paste New Invite Link", callback_data="post_ch:new")])
        await update.message.reply_text(
            f"🔗 <b>{step_label}</b>\n\n"
            f"Select an existing channel below, OR directly <b>send/paste a channel invite link</b> (e.g. <code>https://t.me/+...</code>):",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    else:
        await update.message.reply_text(
            f"🔗 <b>{step_label}</b>\n\n"
            f"Please paste or send the <b>channel invite link</b> (e.g. <code>https://t.me/+abcXYZ123</code>) you are sharing:",
            parse_mode=ParseMode.HTML,
        )
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
    if not link.startswith("http") and not link.startswith("t.me"):
        await update.message.reply_text("⚠️ Please enter a valid invite link starting with http/https or t.me")
        return POST_CHANNEL

    is_alive, reason = await check_channel_link_alive(link, bot=ctx.bot)
    if not is_alive:
        await update.message.reply_text(
            f"❌ <b>This channel link appears to be expired or the channel is banned!</b>\n\n"
            f"⚠️ Reason: {reason}\n\n"
            f"Please enter a valid, active channel invite link:",
            parse_mode=ParseMode.HTML,
        )
        return POST_CHANNEL

    admin_id = update.effective_user.id
    count = len(db.get_all_channels(DB_PATH)) + 1
    channel_name = f"VIP Channel #{count}"

    # Automatically save channel to database with attractive emoji description
    channel_id = db.add_channel(
        DB_PATH,
        name=channel_name,
        link=link,
        price_inr=0.0,
        description="🌟 Exclusive VIP Access • Daily Premium Content & Updates 🚀",
        admin_id=admin_id,
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

    ch_name = channel["name"] if channel else "VIP Channel"
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
        f"<blockquote><b>🌟 Channel:</b> <b>{ch_name}</b>\n"
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
        text=f"📢 Target: <b>{target}</b>\n\nTap Publish when ready:",
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

        await query.edit_message_text(
            f"🎉 <b>Post Successfully Published!</b>\n\n"
            f"📌 <b>Post ID:</b> <code>{post_id}</code>\n"
            f"📢 <b>Target Channel:</b> {target}\n"
            f"🔢 <b>Message ID:</b> <code>{msg.message_id}</code>\n"
            f"💰 <b>Price:</b> {price_display}\n"
            f"🔗 <b>Button Link:</b> <code>{button_link_info}</code>\n\n"
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
        lines.append(
            f"<b>[Post #{p['id']}] {p['channel_name']}</b>\n"
            f"  💰 Price: ₹{p['price_inr']:.0f} | 📢 Target: <code>{p['target_channel']}</code>\n"
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

    # 2. Start background auto-poller worker
    poller_task = asyncio.create_task(poll_pending_payments(app.bot))
    app.bot_data["poller_task"] = poller_task

    # 3. Start background channel link monitor worker
    monitor_task = asyncio.create_task(channel_link_monitor_worker(app.bot))
    app.bot_data["monitor_task"] = monitor_task

    # 3. Set bot commands
    user_cmds = [
        BotCommand("start", "Start the bot"),
        BotCommand("menu", "Browse available channels"),
        BotCommand("mypurchases", "View your purchased channel links"),
        BotCommand("help", "Instructions & FAQ guide"),
    ]
    admin_cmds = user_cmds + [
        BotCommand("post", "Create & broadcast paid post to Main Channel"),
        BotCommand("postfree", "Create & broadcast FREE channel post (no payment)"),
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
        BotCommand("checklinks", "Scan channels & purge dead links"),
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
            ],
            EC_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ec_get_value)],
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

    # ── Register all handlers ────────────────────────────────────────────────
    app.add_handler(post_conv)
    app.add_handler(add_channel_conv)
    app.add_handler(edit_channel_conv)
    app.add_handler(broadcast_conv)
    app.add_handler(verify_conv)

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
    app.add_handler(CommandHandler("delchannel",     cmd_delchannel))
    app.add_handler(CommandHandler("grantaccess",    cmd_grantaccess))
    app.add_handler(CommandHandler("stats",          cmd_stats))
    app.add_handler(CommandHandler("users",          cmd_users))
    app.add_handler(CommandHandler("payments",       cmd_payments))
    app.add_handler(CommandHandler(["check", "checkpayment", "payment"], cmd_check_payment))
    app.add_handler(CommandHandler(["checklinks", "monitor"], cmd_checklinks))
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
    app.add_handler(CallbackQueryHandler(cb_del_fwd_post,  pattern=r"^del_fwd_post:"))
    app.add_handler(CallbackQueryHandler(cb_link_chatid,   pattern=r"^link_cid:"))
    app.add_handler(CallbackQueryHandler(cb_back_menu,     pattern=r"^back_menu$"))
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

