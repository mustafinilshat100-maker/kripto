"""
Price Engine - DexScreener WebSocket Feed (v2.0)
Gets real-time prices, liquidity, volume from DexScreener
"""
import asyncio
import json
import logging
import time
from typing import Optional

import websockets
from redis.asyncio import Redis

from config import (
    REDIS_URL,
    PRICE_CACHE_TTL_SEC,
    PRICE_SOURCE,
    PRICE_UPDATE_INTERVAL_SEC,
)

logger = logging.getLogger("price_engine")

DEXSCREENER_WS = "wss://api.dexscreener.com/socket"
FALLBACK_REST = "https://api.dexscreener.com/dex/tokens/"


class PriceEngine:
    """DexScreener WebSocket price feed"""
    
    def __init__(self, redis: Redis):
        self.redis = redis
        self.running = False
        self.subscribed_tokens: set[str] = set()
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._reconnect_delay = 1
        self._max_reconnect_delay = 60
        self._consecutive_failures = 0
    
    async def start(self):
        """Start price feed"""
        self.running = True
        logger.info("PriceEngine starting...")
        
        while self.running:
            try:
                await self._connect_ws()
            except Exception as e:
                logger.error(f"WS error: {e}")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)
    
    async def stop(self):
        """Stop price feed"""
        self.running = False
        if self.ws:
            await self.ws.close()
        logger.info("PriceEngine stopped")
    
    async def _connect_ws(self):
        """Connect to DexScreener WebSocket"""
        self._consecutive_failures += 1
        
        async with websockets.connect(DEXSCREENER_WS) as ws:
            self.ws = ws
            self._consecutive_failures = 0
            self._reconnect_delay = 1
            logger.info("Connected to DexScreener WS")
            
            # Send subscription for tracked tokens
            if self.subscribed_tokens:
                await self._send_subscriptions()
            
            # Listen for messages
            while self.running:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    await self._handle_message(json.loads(msg))
                except asyncio.TimeoutError:
                    # Ping to keep alive
                    await ws.ping()
    
    async def _send_subscriptions(self):
        """Subscribe to token updates"""
        for token in list(self.subscribed_tokens)[:50]:  # Limit 50 per msg
            subscribe_msg = {
                "method": "subscribe",
                "params": [f"token:{token}"]
            }
            await self.ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to {len(self.subscribed_tokens)} tokens")
    
    async def _handle_message(self, msg: dict):
        """Process incoming price update"""
        try:
            # DexScreener sends batch updates
            if msg.get("method") == "batchUpdate":
                for update in msg.get("params", []):
                    await self._process_update(update)
            elif msg.get("method") == "update":
                await self._process_update(msg.get("params", {}))
        except Exception as e:
            logger.error(f"Message handling error: {e}")
    
    async def _process_update(self, update: dict):
        """Process single token update"""
        try:
            data = update.get("data", update)
            token = data.get("baseToken", {}).get("address")
            if not token:
                return
            
            price_info = {
                "price": float(data.get("priceUsd", 0)),
                "liquidity_usd": float(data.get("liquidity", {}).get("usd", 0)),
                "volume_5m": float(data.get("volume", {}).get("m5", 0)),
                "timestamp": time.time(),
            }
            
            # Cache in Redis
            cache_key = f"price:{token}"
            await self.redis.setex(
                cache_key,
                PRICE_CACHE_TTL_SEC,
                json.dumps(price_info)
            )
            
            logger.debug(f"Price update: {token[:16]} @ ${price_info['price']:.6f}")
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid price data: {e}")
    
    async def subscribe_token(self, token_mint: str):
        """Subscribe to token price updates"""
        if token_mint not in self.subscribed_tokens:
            self.subscribed_tokens.add(token_mint)
            
            if self.ws and self.ws.open:
                subscribe_msg = {
                    "method": "subscribe",
                    "params": [f"token:{token_mint}"]
                }
                await self.ws.send(json.dumps(subscribe_msg))
            
            logger.info(f"Subscribed to {token_mint[:16]}")
    
    async def get_price(self, token_mint: str) -> Optional[dict]:
        """Get cached price for token"""
        cache_key = f"price:{token_mint}"
        data = await self.redis.get(cache_key)
        
        if data:
            return json.loads(data)
        
        # Fallback: REST API
        if PRICE_SOURCE == "dexscreener":
            return await self._fetch_price_rest(token_mint)
        
        return None
    
    async def _fetch_price_rest(self, token_mint: str) -> Optional[dict]:
        """Fallback REST API call"""
        try:
            import httpx
            url = f"{FALLBACK_REST}{token_mint}"
            
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, timeout=5)
                data = resp.json()
                
                pairs = data.get("pairs", [])
                if not pairs:
                    return None
                
                # Get most liquid pair
                best = max(pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0)))
                
                price_info = {
                    "price": float(best.get("priceUsd", 0)),
                    "liquidity_usd": float(best.get("liquidity", {}).get("usd", 0)),
                    "volume_5m": float(best.get("volume", {}).get("m5", 0)),
                    "timestamp": time.time(),
                }
                
                cache_key = f"price:{token_mint}"
                await self.redis.setex(cache_key, PRICE_CACHE_TTL_SEC, json.dumps(price_info))
                
                return price_info
                
        except Exception as e:
            logger.error(f"REST price fetch failed: {e}")
            return None


async def run_price_engine():
    """Standalone price engine runner"""
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    engine = PriceEngine(redis)
    
    try:
        await engine.start()
    except KeyboardInterrupt:
        await engine.stop()
    finally:
        await redis.close()


if __name__ == "__main__":
    asyncio.run(run_price_engine())
