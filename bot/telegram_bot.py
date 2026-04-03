"""
Telegram Bot - Signal Delivery (v2.0)
Sends signals to Telegram with formatting
"""
import asyncio
import logging
import signal
from typing import Optional

import asyncpg
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode

from config import (
    TELEGRAM_BOT_TOKEN,
    REDIS_URL,
    POSTGRES_URL,
    CB_SIGNALS_PER_WINDOW,
    CB_WINDOW_SEC,
    CB_MIN_AVG_SCORE,
    CB_COOLDOWN_SEC,
)
from wallet_intelligence.engine import init_wallet_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("bot")


class TelegramBot:
    """
    Telegram bot for signal delivery.
    Features:
    - Formatted signal messages
    - Deduplication
    - Circuit breaker
    - Inline buttons for actions
    """
    
    def __init__(self):
        self.bot: Optional[Bot] = None
        self.dp: Optional[Dispatcher] = None
        self.router = Router()
        self.redis = None
        self.db = None
        
        # Circuit breaker state
        self._cb_enabled = True
        self._cb_cooldown_until = 0.0
        self._recent_signals: list[tuple[float, float]] = []  # (timestamp, score)
    
    async def setup(self):
        """Initialize bot, Redis, DB"""
        if not TELEGRAM_BOT_TOKEN:
            logger.error("TELEGRAM_BOT_TOKEN not set!")
            return False
        
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.dp = Dispatcher()
        self.dp.include_router(self.router)
        
        # Redis
        from redis.asyncio import Redis
        self.redis = Redis.from_url(REDIS_URL, decode_responses=True)
        
        # DB
        self.db = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=5)
        
        # Init wallet table
        await init_wallet_db(self.db)
        
        # Register handlers
        self._register_handlers()
        
        logger.info("Bot setup complete")
        return True
    
    def _register_handlers(self):
        """Register message handlers"""
        self.router.message.register(self.cmd_start, Command("start"))
        self.router.message.register(self.cmd_status, Command("status"))
        self.router.message.register(self.cmd_help, Command("help"))
        
        # Callback for inline buttons
        self.router.callback_query.register(self.handle_callback)
    
    @router.message(Command("start"))
    async def cmd_start(self, message: Message):
        """Handle /start"""
        await message.answer(
            "🐋 <b>Whale Signal Bot</b>\n\n"
            "Подписка на сигналы активирована.\n"
            "Сигналы будут приходить автоматически.",
            parse_mode=ParseMode.HTML
        )
    
    @router.message(Command("status"))
    async def cmd_status(self, message: Message):
        """Handle /status"""
        from time import time
        now = time()
        
        # Check circuit breaker
        cb_status = "🟢 Active" if self._cb_enabled else "🔴 Cooldown"
        
        # Recent signal count
        recent = [s for s in self._recent_signals if now - s[0] < CB_WINDOW_SEC]
        
        await message.answer(
            f"<b>System Status</b>\n\n"
            f"Circuit Breaker: {cb_status}\n"
            f"Signals (5min): {len(recent)}/{CB_SIGNALS_PER_WINDOW}\n"
            f"Redis: {'✅' if self.redis else '❌'}\n"
            f"DB: {'✅' if self.db else '❌'}",
            parse_mode=ParseMode.HTML
        )
    
    @router.message(Command("help"))
    async def cmd_help(self, message: Message):
        """Handle /help"""
        await message.answer(
            "<b>Commands:</b>\n\n"
            "/start - Start bot\n"
            "/status - System status\n"
            "/help - This help\n\n"
            "<b>Signal Buttons:</b>\n"
            "✅ Вход - Confirm entry\n"
            "❌ Пропустить - Skip signal",
            parse_mode=ParseMode.HTML
        )
    
    @router.callback_query(F.data.startswith("sig_"))
    async def handle_callback(self, callback):
        """Handle inline button callbacks"""
        action, signal_hash = callback.data.split("_", 1)
        
        if action == "accept":
            await callback.message.edit_text(
                callback.message.text + "\n\n✅ <b>Отмечен как ВХОД</b>",
                parse_mode=ParseMode.HTML
            )
        elif action == "reject":
            await callback.message.edit_text(
                callback.message.text + "\n\n❌ <b>Пропущен</b>",
                parse_mode=ParseMode.HTML
            )
        
        await callback.answer()
    
    def _check_circuit_breaker(self, score: float) -> bool:
        """
        Check circuit breaker.
        Returns True if signal should be sent, False if blocked.
        """
        from time import time
        now = time()
        
        # Check cooldown
        if now < self._cb_cooldown_until:
            return False
        
        # Update recent signals
        self._recent_signals = [(ts, s) for ts, s in self._recent_signals if now - ts < CB_WINDOW_SEC]
        self._recent_signals.append((now, score))
        
        # Check thresholds
        recent_count = len(self._recent_signals)
        if recent_count > CB_SIGNALS_PER_WINDOW:
            avg_score = sum(s for _, s in self._recent_signals) / recent_count
            if avg_score < CB_MIN_AVG_SCORE:
                self._cb_enabled = False
                self._cb_cooldown_until = now + CB_COOLDOWN_SEC
                logger.warning(f"Circuit breaker activated: {recent_count} signals, avg={avg_score:.3f}")
                return False
        
        return True
    
    async def send_signal(self, signal_data: dict):
        """Send signal to Telegram"""
        from time import time
        
        if not self.bot:
            logger.warning("Bot not initialized")
            return
        
        # Circuit breaker check
        if not self._check_circuit_breaker(signal_data.get("score", 0)):
            logger.debug("Signal blocked by circuit breaker")
            return
        
        # Deduplication
        signal_hash = signal_data.get("signal_hash", "")
        dedup_key = f"signal:sent:{signal_hash}"
        
        if await self.redis.exists(dedup_key):
            logger.debug(f"Duplicate signal: {signal_hash}")
            return
        
        # Format message
        token = signal_data.get("token", "")
        score = signal_data.get("score", 0)
        phase = signal_data.get("phase", "UNKNOWN")
        entry_lower = signal_data.get("entry_lower", 0)
        entry_upper = signal_data.get("entry_upper", 0)
        slippage = signal_data.get("slippage_estimate", 0)
        max_safe = signal_data.get("max_safe_size_usd", 0)
        flow_ratio = signal_data.get("flow_ratio", 0)
        momentum = signal_data.get("price_momentum", 0)
        pattern = signal_data.get("pattern_score", 0)
        timestamp = signal_data.get("timestamp", 0)
        
        # Emoji based on score
        if score >= 0.9:
            emoji = "🚨"
        elif score >= 0.85:
            emoji = "🔥"
        elif score >= 0.8:
            emoji = "🐋"
        else:
            emoji = "👀"
        
        message = (
            f"{emoji} <b>WHALE SIGNAL</b>\n\n"
            f"Token: <code>{token[:20]}...</code>\n"
            f"Score: <b>{score:.3f}</b>/1.0\n"
            f"Phase: <b>{phase}</b>\n\n"
            f"📊 Flow: <b>{flow_ratio:.2f}x</b>\n"
            f"💹 Momentum: <b>{momentum:.2%}</b>\n"
            f"📈 Entry Zone: <code>{entry_lower:.8f}</code> - <code>{entry_upper:.8f}</code>\n\n"
            f"💰 Slippage (est.): <b>{slippage:.2%}</b>\n"
            f"🛡 Max Safe Size: <b>${max_safe:.0f}</b>\n\n"
            f"Pattern: <b>{pattern:.2f}</b>\n"
            f"🕐 {timestamp}"
        )
        
        # Inline keyboard
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Вход", callback_data=f"sig_accept_{signal_hash}"),
                InlineKeyboardButton(text="❌ Пропустить", callback_data=f"sig_reject_{signal_hash}"),
            ]
        ])
        
        # Send to configured chat (would be from DB or config)
        # For now, just log
        logger.info(f"Would send signal: {token[:16]}")
        
        # Mark as sent (dedup)
        await self.redis.setex(dedup_key, 3600, "1")
    
    async def run(self):
        """Run bot and signal consumer loop"""
        if not await self.setup():
            logger.error("Bot setup failed")
            return
        
        # Start signal consumer
        consumer_task = asyncio.create_task(self._signal_consumer())
        
        try:
            await self.dp.start_polling(self.bot)
        finally:
            consumer_task.cancel()
            if self.bot:
                await self.bot.session.close()
    
    async def _signal_consumer(self):
        """Consume signals from Redis stream"""
        from redis.asyncio import Redis
        
        while True:
            try:
                # Read signals
                msgs = await self.redis.xread({"signals:generated": ">"}, count=1, block=1000)
                
                for stream, messages in msgs or []:
                    for msg_id, data in messages:
                        await self.send_signal(data)
                        await self.redis.xack("signals:generated", "bot", msg_id)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(1)


async def main():
    bot = TelegramBot()
    
    # Handle signals
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        pass  # Bot handles graceful shutdown
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
