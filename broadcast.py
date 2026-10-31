"""
broadcast.py — Rate-limit-safe broadcast utility for Telegram bots.

Telegram rate limits:
  • 30 messages/second to different chats (global)
  • 20 messages/minute to the same group/channel
  • In practice: ~25-30 msg/sec to individual users is safe

We default to 0.05 s delay (~20 msg/s) with exponential back-off on 429s.
"""

import asyncio
import logging
from telegram import Bot
from telegram.error import RetryAfter, Forbidden, BadRequest

logger = logging.getLogger(__name__)


async def broadcast_message(
    bot: Bot,
    user_ids: list[int],
    text: str,
    parse_mode: str = "HTML",
    reply_markup=None,
    delay: float = 0.05,
) -> tuple[int, int]:
    """
    Send `text` to every user_id in user_ids.

    Returns (sent_count, failed_count).
    Handles:
      - RetryAfter (429): waits exactly as long as Telegram demands
      - Forbidden (bot was blocked): silently skips
      - BadRequest: logs and skips
    """
    sent = 0
    failed = 0
    total = len(user_ids)

    for i, uid in enumerate(user_ids, 1):
        try:
            await bot.send_message(
                chat_id=uid,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
            sent += 1
            logger.debug("Broadcast [%d/%d] → %d ✓", i, total, uid)
        except RetryAfter as exc:
            wait = exc.retry_after + 1
            logger.warning("Rate limited — sleeping %ss", wait)
            await asyncio.sleep(wait)
            # Retry once after the forced sleep
            try:
                await bot.send_message(
                    chat_id=uid,
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                )
                sent += 1
            except Exception as inner_exc:
                logger.error("Retry failed for %d: %s", uid, inner_exc)
                failed += 1
        except Forbidden:
            # User blocked the bot — skip silently
            failed += 1
        except BadRequest as exc:
            logger.warning("BadRequest for %d: %s", uid, exc)
            failed += 1
        except Exception as exc:
            logger.error("Unexpected error for %d: %s", uid, exc)
            failed += 1

        await asyncio.sleep(delay)

    logger.info("Broadcast complete — sent: %d, failed: %d", sent, failed)
    return sent, failed
