"""
FastAPI Webhook Ingestion (v2.0)
POST /helius endpoint for Helius webhook events
"""
import asyncio
import hashlib
import logging
import time
from typing import Optional

import orjson
from fastapi import FastAPI, HTTPException, Request, Header
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from redis.exceptions import RedisError

from config import (
    REDIS_URL,
    POSTGRES_URL,
    QUOTE_MINTS,
    STREAM_SWAP,
    STREAM_ADD_LIQUIDITY,
    STREAM_CREATE_POOL,
    IDEMPOTENCY_TTL_SEC,
    HELIUS_WEBHOOK_SECRET,
    STREAM_PARTITIONS,
)
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("webhook")


# =============================================================================
# MODELS
# =============================================================================
class SwapEvent(BaseModel):
    signature: str
    from_token: str
    to_token: str
    amount_in: float
    amount_out: float
    wallet: str
    timestamp: int
    slot: Optional[int] = None


class CreatePoolEvent(BaseModel):
    signature: str
    mint: str
    liquidity: float
    initial_liquidity: float
    timestamp: int
    slot: Optional[int] = None


class AddLiquidityEvent(BaseModel):
    signature: str
    mint: str
    liquidity_added: float
    wallet: str
    timestamp: int
    slot: Optional[int] = None


class HeliusWebhook(BaseModel):
    events: list[dict] = Field(default_factory=list)


# =============================================================================
# HELPERS
# =============================================================================
def get_token_mint(from_token: str, to_token: str) -> str:
    """Determine main token (not SOL/USDC)"""
    for qt in QUOTE_MINTS:
        if from_token == qt:
            return to_token
        if to_token == qt:
            return from_token
    # Neither is quote - return from_token as fallback
    return from_token


def get_swap_side(from_token: str, to_token: str) -> str:
    """BUY if exchanging SOL/USDC for token, else SELL"""
    if from_token in QUOTE_MINTS:
        return "BUY"
    if to_token in QUOTE_MINTS:
        return "SELL"
    return "UNKNOWN"


def partition_key(token_mint: str) -> int:
    """Hash-based partitioning 0..STREAM_PARTITIONS-1"""
    h = int(hashlib.md5(token_mint.encode()).hexdigest(), 16)
    return h % STREAM_PARTITIONS


def calculate_amount_usd(event: dict) -> float:
    """Estimate USD value from swap event"""
    # In production, would use price oracle
    # For now, use amount_in if it's a known quote mint
    amount = float(event.get("amount_in", 0))
    return amount  # Would multiply by price for non-quote tokens


# =============================================================================
# APP
# =============================================================================
app = FastAPI(title="Solana Signal Webhook", version="2.0")
redis: Optional[Redis] = None


@app.on_event("startup")
async def startup():
    global redis
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("Connected to Redis")


@app.on_event("shutdown")
async def shutdown():
    if redis:
        await redis.close()
    logger.info("Redis connection closed")


# =============================================================================
# ENDPOINTS
# =============================================================================
@app.post("/helius")
async def helius_webhook(
    request: Request,
    webhook: HeliusWebhook,
    x_helius_signature: Optional[str] = Header(None),
):
    """
    Receive Helius webhook events:
    - SWAP: from_token, to_token, amount_in, amount_out, wallet
    - CREATE_POOL: mint, liquidity, initial_liquidity
    - ADD_LIQUIDITY: mint, liquidity_added, wallet
    """
    received_at = time.time()
    processed = 0
    dropped = 0

    for raw_event in webhook.events:
        try:
            event_type = raw_event.get("type", "").upper()
            
            if event_type == "SWAP":
                event = SwapEvent(**raw_event)
                
                # Idempotency check
                cache_key = f"idem:{event.signature}"
                if await redis.exists(cache_key):
                    dropped += 1
                    logger.debug(f"Duplicate swap: {event.signature}")
                    continue
                
                # Determine side and token
                side = get_swap_side(event.from_token, event.to_token)
                token_mint = get_token_mint(event.from_token, event.to_token)
                
                # Build stream event
                stream_event = {
                    "event_type": "swap",
                    "token_mint": token_mint,
                    "wallet": event.wallet,
                    "side": side,
                    "amount_usd": calculate_amount_usd(raw_event),
                    "signature": event.signature,
                    "timestamp": event.timestamp,
                    "received_at": received_at,
                }
                
                # Publish to stream
                partition = partition_key(token_mint)
                stream_name = f"{STREAM_SWAP}:{partition}"
                
                await redis.xadd(stream_name, stream_event)
                
                # Set idempotency key
                await redis.setex(cache_key, IDEMPOTENCY_TTL_SEC, "1")
                
                processed += 1
                logger.info(f"SWAP: {event.signature[:16]}... | {side} | {token_mint[:16]}")
                
            elif event_type == "CREATE_POOL":
                event = CreatePoolEvent(**raw_event)
                
                # Idempotency
                cache_key = f"idem:{event.signature}"
                if await redis.exists(cache_key):
                    dropped += 1
                    continue
                
                partition = partition_key(event.mint)
                stream_event = {
                    "event_type": "create_pool",
                    "token_mint": event.mint,
                    "liquidity": event.liquidity,
                    "initial_liquidity": event.initial_liquidity,
                    "signature": event.signature,
                    "timestamp": event.timestamp,
                    "received_at": received_at,
                }
                
                stream_name = f"{STREAM_CREATE_POOL}:{partition}"
                await redis.xadd(stream_name, stream_event)
                await redis.setex(cache_key, IDEMPOTENCY_TTL_SEC, "1")
                
                processed += 1
                logger.info(f"CREATE_POOL: {event.mint[:16]} | liq={event.liquidity}")
                
            elif event_type == "ADD_LIQUIDITY":
                event = AddLiquidityEvent(**raw_event)
                
                # Idempotency
                cache_key = f"idem:{event.signature}"
                if await redis.exists(cache_key):
                    dropped += 1
                    continue
                
                partition = partition_key(event.mint)
                stream_event = {
                    "event_type": "add_liquidity",
                    "token_mint": event.mint,
                    "wallet": event.wallet,
                    "liquidity_added": event.liquidity_added,
                    "signature": event.signature,
                    "timestamp": event.timestamp,
                    "received_at": received_at,
                }
                
                stream_name = f"{STREAM_ADD_LIQUIDITY}:{partition}"
                await redis.xadd(stream_name, stream_event)
                await redis.setex(cache_key, IDEMPOTENCY_TTL_SEC, "1")
                
                processed += 1
                logger.info(f"ADD_LIQUIDITY: {event.mint[:16]} | {event.wallet[:16]}")
                
            else:
                logger.warning(f"Unknown event type: {event_type}")
                
        except RedisError as e:
            logger.error(f"Redis error: {e}")
            raise HTTPException(status_code=500, detail="Stream write failed")
        except Exception as e:
            logger.error(f"Event processing error: {e}")
            dropped += 1

    latency_ms = (time.time() - received_at) * 1000
    logger.info(f"Webhook processed: {processed} ok, {dropped} dup/error, latency={latency_ms:.1f}ms")
    
    return {
        "processed": processed,
        "dropped": dropped,
        "latency_ms": round(latency_ms, 2),
    }


@app.get("/health")
async def health():
    """Health check"""
    try:
        await redis.ping()
        return {"status": "healthy", "redis": "ok"}
    except:
        raise HTTPException(status_code=503, detail="Redis unavailable")


@app.get("/streams")
async def list_streams():
    """Debug: list all streams"""
    try:
        streams = await redis.keys("events:*")
        return {"streams": streams[:50]}
    except RedisError as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
