"""
Token State Engine (v2.0)
Maintains per-token state: volumes, prices, liquidity, events
"""
import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Literal

from redis.asyncio import Redis

from config import (
    REDIS_URL,
    STREAM_TOKEN_STATE,
    TOKEN_STATE_SNAPSHOT_SEC,
    PRICE_CACHE_TTL_SEC,
)

logger = logging.getLogger("token_state")


@dataclass
class TokenState:
    """Complete state for a single token"""
    token_mint: str
    created_at: float = field(default_factory=time.time)
    
    # Volume history (timestamp, buy_vol, sell_vol)
    buy_volume_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    sell_volume_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    # Price history (timestamp, price)
    price_history: deque = field(default_factory=lambda: deque(maxlen=20))
    
    # Liquidity history (timestamp, liquidity_usd)
    liquidity_history: deque = field(default_factory=lambda: deque(maxlen=100))
    
    # Events for sequence detection
    events: deque = field(default_factory=lambda: deque(maxlen=150))
    
    # Pool metrics
    initial_liquidity: float = 0.0
    max_liquidity: float = 0.0
    current_liquidity: float = 0.0
    
    # State flags
    has_pool: bool = False
    last_updated: float = field(default_factory=time.time)


class TokenStateEngine:
    """
    Manages token states, processes events, maintains rolling history.
    Publishes snapshots to Redis periodically.
    """
    
    def __init__(self, redis: Redis):
        self.redis = redis
        self.states: dict[str, TokenState] = {}
        self._snapshot_interval = TOKEN_STATE_SNAPSHOT_SEC
        self._running = False
        self._snapshot_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start periodic snapshots"""
        self._running = True
        self._snapshot_task = asyncio.create_task(self._snapshot_loop())
        logger.info("TokenStateEngine started")
    
    async def stop(self):
        """Stop snapshots"""
        self._running = False
        if self._snapshot_task:
            self._snapshot_task.cancel()
        logger.info("TokenStateEngine stopped")
    
    async def _snapshot_loop(self):
        """Periodic snapshot to Redis"""
        while self._running:
            try:
                await asyncio.sleep(self._snapshot_interval)
                await self._publish_snapshots()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Snapshot error: {e}")
    
    async def _publish_snapshots(self):
        """Publish current states to Redis"""
        for mint, state in self.states.items():
            try:
                snapshot = self._state_to_snapshot(state)
                key = f"{STREAM_TOKEN_STATE}:{mint}"
                await self.redis.set(key, json.dumps(snapshot))
            except Exception as e:
                logger.error(f"Failed to snapshot {mint[:16]}: {e}")
    
    def _state_to_snapshot(self, state: TokenState) -> dict:
        """Convert state to serializable dict"""
        return {
            "token_mint": state.token_mint,
            "created_at": state.created_at,
            "has_pool": state.has_pool,
            "initial_liquidity": state.initial_liquidity,
            "max_liquidity": state.max_liquidity,
            "current_liquidity": state.current_liquidity,
            "price_history_len": len(state.price_history),
            "events_len": len(state.events),
            "buy_vol_total": sum(v for _, v, _ in state.buy_volume_history),
            "sell_vol_total": sum(v for _, v, _ in state.sell_volume_history),
            "last_updated": state.last_updated,
        }
    
    def get_or_create_state(self, token_mint: str) -> TokenState:
        """Get existing or create new state"""
        if token_mint not in self.states:
            self.states[token_mint] = TokenState(token_mint=token_mint)
            logger.info(f"Created state for {token_mint[:16]}")
        return self.states[token_mint]
    
    async def process_swap(
        self,
        token_mint: str,
        wallet: str,
        side: str,
        amount_usd: float,
        timestamp: Optional[float] = None
    ):
        """Process SWAP event"""
        state = self.get_or_create_state(token_mint)
        now = timestamp or time.time()
        
        # Add volume history
        if side == "BUY":
            state.buy_volume_history.append((now, amount_usd, 0))
        else:
            state.sell_volume_history.append((now, 0, amount_usd))
        
        # Add event
        state.events.append({
            "type": "swap",
            "side": side,
            "wallet": wallet,
            "amount_usd": amount_usd,
            "timestamp": now,
        })
        
        # Update price from cache if available
        await self._update_price_from_cache(state)
        
        state.last_updated = now
    
    async def process_create_pool(
        self,
        token_mint: str,
        liquidity: float,
        initial_liquidity: float,
        timestamp: Optional[float] = None
    ):
        """Process CREATE_POOL event"""
        state = self.get_or_create_state(token_mint)
        now = timestamp or time.time()
        
        state.has_pool = True
        state.initial_liquidity = initial_liquidity
        state.max_liquidity = liquidity
        state.current_liquidity = liquidity
        
        # Add liquidity history
        state.liquidity_history.append((now, liquidity))
        
        # Add event
        state.events.append({
            "type": "create_pool",
            "liquidity": liquidity,
            "initial_liquidity": initial_liquidity,
            "timestamp": now,
        })
        
        state.last_updated = now
        logger.info(f"Pool created: {token_mint[:16]}, liq=${liquidity:.0f}")
    
    async def process_add_liquidity(
        self,
        token_mint: str,
        wallet: str,
        liquidity_added: float,
        timestamp: Optional[float] = None
    ):
        """Process ADD_LIQUIDITY event"""
        state = self.get_or_create_state(token_mint)
        now = timestamp or time.time()
        
        state.current_liquidity += liquidity_added
        state.max_liquidity = max(state.max_liquidity, state.current_liquidity)
        
        # Add liquidity history
        state.liquidity_history.append((now, state.current_liquidity))
        
        # Add event
        state.events.append({
            "type": "add_liquidity",
            "wallet": wallet,
            "liquidity_added": liquidity_added,
            "timestamp": now,
        })
        
        state.last_updated = now
    
    async def _update_price_from_cache(self, state: TokenState):
        """Update price from Redis cache"""
        try:
            cache_key = f"price:{state.token_mint}"
            data = await self.redis.get(cache_key)
            
            if data:
                price_info = json.loads(data)
                state.price_history.append((
                    price_info.get("timestamp", time.time()),
                    price_info.get("price", 0)
                ))
                
                # Update liquidity from price feed
                if "liquidity_usd" in price_info:
                    state.current_liquidity = price_info["liquidity_usd"]
        except Exception as e:
            logger.debug(f"Price cache miss for {state.token_mint[:16]}: {e}")
    
    def get_price_momentum(self, state: TokenState) -> float:
        """
        Calculate 5-minute price momentum.
        Returns: fractional change (e.g., 0.05 = 5% gain)
        """
        now = time.time()
        cutoff = now - 300  # 5 minutes ago
        
        prices = [(ts, price) for ts, price in state.price_history if ts >= cutoff]
        
        if len(prices) < 2:
            return 0.0
        
        oldest_price = prices[0][1]
        current_price = prices[-1][1]
        
        if oldest_price <= 0:
            return 0.0
        
        return (current_price - oldest_price) / oldest_price
    
    def get_liquidity_score(self, state: TokenState) -> float:
        """Normalize liquidity to 0..1"""
        # Typical range: 10k to 1M
        min_liq = 10000
        max_liq = 1000000
        
        score = (state.current_liquidity - min_liq) / (max_liq - min_liq)
        return max(0.0, min(1.0, score))
    
    def get_volume_5m(self, state: TokenState) -> float:
        """Get total volume in last 5 minutes"""
        now = time.time()
        cutoff = now - 300
        
        total_buy = sum(v for ts, v in state.buy_volume_history if ts >= cutoff)
        total_sell = sum(v for ts, v in state.sell_volume_history if ts >= cutoff)
        
        return total_buy + total_sell
    
    def get_state(self, token_mint: str) -> Optional[TokenState]:
        """Get token state if exists"""
        return self.states.get(token_mint)
    
    def get_all_tokens(self) -> list[str]:
        """List all tracked tokens"""
        return list(self.states.keys())


# =============================================================================
# STANDALONE TEST
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def test():
        redis = Redis.from_url(REDIS_URL, decode_responses=True)
        engine = TokenStateEngine(redis)
        
        # Test events
        await engine.process_create_pool("TestToken", 50000, 50000)
        
        for i in range(5):
            await engine.process_swap("TestToken", f"Wallet{i}", "BUY", 1000)
        
        state = engine.get_state("TestToken")
        print(f"State: has_pool={state.has_pool}, liq=${state.current_liquidity:.0f}")
        print(f"Buy history len: {len(state.buy_volume_history)}")
        print(f"Momentum: {engine.get_price_momentum(state):.3f}")
        
        await redis.close()
    
    asyncio.run(test())
