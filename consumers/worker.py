"""
Consumer Worker (v2.0)
Main entry point for processing events from Redis Streams
"""
import asyncio
import json
import logging
import signal
import sys
from typing import Optional

import asyncpg
from redis.asyncio import Redis

from config import (
    REDIS_URL,
    POSTGRES_URL,
    STREAM_PARTITIONS,
    CONSUMER_GROUP,
    STREAM_SWAP,
    STREAM_ADD_LIQUIDITY,
    STREAM_CREATE_POOL,
    STREAM_DLQ,
)
from flow_engine import FlowEngine
from token_state import TokenStateEngine
from sequence_detector import SequenceDetector
from anti_manipulation import AntiManipulationEngine
from signal_engine import SignalEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("worker")


class Worker:
    """
    Main consumer worker that:
    1. Reads events from Redis Streams (swap, add_liquidity, create_pool)
    2. Processes through FlowEngine, TokenState, SequenceDetector
    3. Generates signals via SignalEngine
    4. Handles anti-manipulation checks
    """
    
    def __init__(self):
        self.redis: Optional[Redis] = None
        self.db: Optional[asyncpg.Pool] = None
        
        # Engines
        self.flow_engine = FlowEngine()
        self.token_state = TokenStateEngine(redis=None)  # Will be set later
        self.sequence_detector = SequenceDetector()
        self.anti_manip = AntiManipulationEngine()
        self.signal_engine = SignalEngine()
        
        self.running = False
        self._tasks: list[asyncio.Task] = []
    
    async def setup(self):
        """Initialize connections"""
        self.redis = Redis.from_url(REDIS_URL, decode_responses=True)
        
        # Create Postgres pool
        self.db = await asyncpg.create_pool(
            POSTGRES_URL,
            min_size=2,
            max_size=10,
        )
        
        # Set redis on token_state engine
        self.token_state = TokenStateEngine(self.redis)
        
        # Create consumer group if not exists
        for i in range(STREAM_PARTITIONS):
            try:
                await self.redis.xgroup_create(
                    f"{STREAM_SWAP}:{i}",
                    CONSUMER_GROUP,
                    id="0",
                    mkstream=True
                )
            except:
                pass  # Group exists
            
            try:
                await self.redis.xgroup_create(
                    f"{STREAM_ADD_LIQUIDITY}:{i}",
                    CONSUMER_GROUP,
                    id="0",
                    mkstream=True
                )
            except:
                pass
            
            try:
                await self.redis.xgroup_create(
                    f"{STREAM_CREATE_POOL}:{i}",
                    CONSUMER_GROUP,
                    id="0",
                    mkstream=True
                )
            except:
                pass
        
        logger.info("Worker setup complete")
    
    async def teardown(self):
        """Close connections"""
        self.running = False
        
        for task in self._tasks:
            task.cancel()
        
        if self.token_state:
            await self.token_state.stop()
        
        if self.redis:
            await self.redis.close()
        
        if self.db:
            await self.db.close()
        
        logger.info("Worker teardown complete")
    
    async def run(self):
        """Main worker loop"""
        await self.setup()
        await self.token_state.start()
        
        self.running = True
        
        # Create consumer tasks for each stream
        for i in range(STREAM_PARTITIONS):
            self._tasks.append(asyncio.create_task(self._consume_swap(i)))
            self._tasks.append(asyncio.create_task(self._consume_add_liquidity(i)))
            self._tasks.append(asyncio.create_task(self._consume_create_pool(i)))
        
        # Also start periodic signal check
        self._tasks.append(asyncio.create_task(self._signal_check_loop()))
        
        logger.info(f"Worker running with {len(self._tasks)} tasks")
        
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Worker cancelled")
        finally:
            await self.teardown()
    
    async def _consume_swap(self, partition: int):
        """Consume SWAP events"""
        stream = f"{STREAM_SWAP}:{partition}"
        consumer_name = f"swap-consumer-{partition}"
        
        while self.running:
            try:
                # Read new messages
                msgs = await self.redis.xreadgroup(
                    CONSUMER_GROUP,
                    consumer_name,
                    {stream: ">"},
                    count=10,
                    block=1000,
                )
                
                for stream_name, messages in msgs or []:
                    for msg_id, data in messages:
                        try:
                            await self._process_swap(data)
                            await self.redis.xack(stream_name, CONSUMER_GROUP, msg_id)
                        except Exception as e:
                            logger.error(f"Swap processing error: {e}")
                            # Send to DLQ
                            await self.redis.xadd(STREAM_DLQ, {**data, "error": str(e)})
                            
                        except Exception as e:
                            logger.error(f"Error in swap consumer: {e}")
                            await asyncio.sleep(1)
    
    async def _consume_add_liquidity(self, partition: int):
        """Consume ADD_LIQUIDITY events"""
        stream = f"{STREAM_ADD_LIQUIDITY}:{partition}"
        consumer_name = f"liq-consumer-{partition}"
        
        while self.running:
            try:
                msgs = await self.redis.xreadgroup(
                    CONSUMER_GROUP,
                    consumer_name,
                    {stream: ">"},
                    count=10,
                    block=1000,
                )
                
                for stream_name, messages in msgs or []:
                    for msg_id, data in messages:
                        try:
                            await self._process_add_liquidity(data)
                            await self.redis.xack(stream_name, CONSUMER_GROUP, msg_id)
                        except Exception as e:
                            logger.error(f"AddLiq processing error: {e}")
                            await self.redis.xadd(STREAM_DLQ, {**data, "error": str(e)})
                            
            except Exception as e:
                logger.error(f"Error in liq consumer: {e}")
                await asyncio.sleep(1)
    
    async def _consume_create_pool(self, partition: int):
        """Consume CREATE_POOL events"""
        stream = f"{STREAM_CREATE_POOL}:{partition}"
        consumer_name = f"pool-consumer-{partition}"
        
        while self.running:
            try:
                msgs = await self.redis.xreadgroup(
                    CONSUMER_GROUP,
                    consumer_name,
                    {stream: ">"},
                    count=10,
                    block=1000,
                )
                
                for stream_name, messages in msgs or []:
                    for msg_id, data in messages:
                        try:
                            await self._process_create_pool(data)
                            await self.redis.xack(stream_name, CONSUMER_GROUP, msg_id)
                        except Exception as e:
                            logger.error(f"CreatePool processing error: {e}")
                            await self.redis.xadd(STREAM_DLQ, {**data, "error": str(e)})
                            
            except Exception as e:
                logger.error(f"Error in pool consumer: {e}")
                await asyncio.sleep(1)
    
    async def _process_swap(self, data: dict):
        """Process SWAP event"""
        token_mint = data.get("token_mint", "")
        wallet = data.get("wallet", "")
        side = data.get("side", "BUY")
        amount_usd = float(data.get("amount_usd", 0))
        timestamp = float(data.get("timestamp", 0))
        
        # Update flow engine
        flow_metrics = self.flow_engine.process_swap(token_mint, side, amount_usd, timestamp)
        
        # Update token state
        await self.token_state.process_swap(token_mint, wallet, side, amount_usd, timestamp)
        
        # Update wallet stats in DB
        await self._update_wallet_stats(wallet, side, amount_usd)
    
    async def _process_add_liquidity(self, data: dict):
        """Process ADD_LIQUIDITY event"""
        token_mint = data.get("token_mint", "")
        wallet = data.get("wallet", "")
        liquidity_added = float(data.get("liquidity_added", 0))
        timestamp = float(data.get("timestamp", 0))
        
        # Update token state
        await self.token_state.process_add_liquidity(token_mint, wallet, liquidity_added, timestamp)
        
        # Update wallet stats
        await self._update_wallet_stats(wallet, "BUY", liquidity_added)
    
    async def _process_create_pool(self, data: dict):
        """Process CREATE_POOL event"""
        token_mint = data.get("token_mint", "")
        liquidity = float(data.get("liquidity", 0))
        initial_liquidity = float(data.get("initial_liquidity", 0))
        timestamp = float(data.get("timestamp", 0))
        
        # Update token state
        await self.token_state.process_create_pool(
            token_mint, liquidity, initial_liquidity, timestamp
        )
    
    async def _update_wallet_stats(self, wallet: str, side: str, amount_usd: float):
        """Update wallet statistics in Postgres"""
        if not self.db:
            return
        
        try:
            async with self.db.acquire() as conn:
                # Upsert wallet
                await conn.execute("""
                    INSERT INTO wallets (wallet, total_trades, total_volume_usd, last_updated)
                    VALUES ($1, 1, $2, NOW())
                    ON CONFLICT (wallet) DO UPDATE SET
                        total_trades = wallets.total_trades + 1,
                        total_volume_usd = wallets.total_volume_usd + $2,
                        last_updated = NOW()
                """, wallet, amount_usd)
                
                if side == "BUY":
                    await conn.execute("""
                        UPDATE wallets SET buy_trades = buy_trades + 1 WHERE wallet = $1
                    """, wallet)
                else:
                    await conn.execute("""
                        UPDATE wallets SET sell_trades = sell_trades + 1 WHERE wallet = $1
                    """, wallet)
                    
        except Exception as e:
            logger.error(f"Wallet stats update failed: {e}")
    
    async def _signal_check_loop(self):
        """Periodically check for signals on tracked tokens"""
        while self.running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds
                
                # Get all tracked tokens
                tokens = self.token_state.get_all_tokens()
                
                for token in tokens:
                    await self._check_and_generate_signal(token)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Signal check error: {e}")
    
    async def _check_and_generate_signal(self, token_mint: str):
        """Check conditions and generate signal if met"""
        state = self.token_state.get_state(token_mint)
        if not state or not state.has_pool:
            return
        
        # Get flow metrics
        flow_metrics = self.flow_engine.process_swap(token_mint, "BUY", 0)  # Just to get current state
        
        # Calculate flow strength
        flow_strength = self.flow_engine.calculate_flow_strength(
            flow_metrics, state.current_liquidity
        )
        
        # Get price momentum
        price_momentum = self.token_state.get_price_momentum(state)
        
        # Sequence detection
        events_list = list(state.events)
        pattern_result = self.sequence_detector.analyze(
            events_list, price_momentum, state.current_liquidity, state.initial_liquidity
        )
        
        # Phase detection
        token_age = time.time() - state.created_at
        activity_rate = len(events_list) / max(token_age / 3600, 1)
        phase = self.signal_engine.detect_phase(token_age, activity_rate)
        
        # Wallet quality (placeholder - would fetch from DB)
        wallet_quality = 0.5
        
        # Calculate score
        score = self.signal_engine.calculate_score(
            flow_strength=flow_strength,
            wallet_quality=wallet_quality,
            liquidity_score=self.token_state.get_liquidity_score(state),
            price_momentum=price_momentum,
            dev_wallet_ratio=0.0,  # Would calculate from wallet stats
        )
        
        # Get current price
        price_info = await self.token_state.redis.get(f"price:{token_mint}")
        price = json.loads(price_info).get("price", 0) if price_info else 0
        
        # Generate signal
        signal = self.signal_engine.generate_signal(
            token_mint=token_mint,
            score=score,
            phase=phase,
            price=price,
            flow_ratio=flow_metrics.flow_ratio,
            flow_strength=flow_strength,
            price_momentum=price_momentum,
            pattern_score=pattern_result.pattern_score,
            liquidity_usd=state.current_liquidity,
            anti_manipulation_passed=True,  # Would use anti_manip.check()
        )
        
        if signal:
            await self._publish_signal(signal)
    
    async def _publish_signal(self, signal):
        """Publish signal to Redis for delivery"""
        signal_data = {
            "token": signal.token,
            "score": signal.score,
            "phase": signal.phase,
            "entry_lower": signal.entry_zone[0],
            "entry_upper": signal.entry_zone[1],
            "slippage_estimate": signal.slippage_estimate,
            "max_safe_size_usd": signal.max_safe_size_usd,
            "flow_ratio": signal.flow_ratio,
            "price_momentum": signal.price_momentum,
            "pattern_score": signal.pattern_score,
            "timestamp": signal.timestamp,
            "signal_hash": signal.signal_hash,
        }
        
        await self.redis.xadd("signals:generated", signal_data)
        logger.info(f"Signal published: {signal.token[:16]}")


async def main():
    worker = Worker()
    
    # Handle signals
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        worker.running = False
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
