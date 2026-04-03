"""
Production-Level Configuration (v2.0)
All thresholds, weights, and limits from .env
"""
import os
from typing import Final
from dotenv import load_dotenv

load_dotenv()


# =============================================================================
# FLOW ENGINE
# =============================================================================
FLOW_RATIO_BULLISH: Final[float] = float(os.getenv("FLOW_RATIO_BULLISH", "1.5"))
NET_PRESSURE_MIN: Final[float] = float(os.getenv("NET_PRESSURE_MIN", "0.02"))

# =============================================================================
# SCORING WEIGHTS
# =============================================================================
W1_FLOW: Final[float] = float(os.getenv("W1_FLOW", "0.35"))
W2_WALLET: Final[float] = float(os.getenv("W2_WALLET", "0.25"))
W3_LIQUIDITY: Final[float] = float(os.getenv("W3_LIQUIDITY", "0.15"))
W4_MOMENTUM: Final[float] = float(os.getenv("W4_MOMENTUM", "0.15"))
W5_DEV: Final[float] = float(os.getenv("W5_DEV", "0.10"))

# =============================================================================
# SIGNAL THRESHOLDS
# =============================================================================
SCORE_THRESHOLD: Final[float] = float(os.getenv("SCORE_THRESHOLD", "0.82"))
PATTERN_SCORE_MIN: Final[float] = float(os.getenv("PATTERN_SCORE_MIN", "0.70"))
SNIPER_RATIO_MAX: Final[float] = float(os.getenv("SNIPER_RATIO_MAX", "0.35"))
RUG_SCORE_MIN: Final[int] = int(os.getenv("RUG_SCORE_MIN", "75"))
MIN_LIQUIDITY_EXECUTION: Final[float] = float(os.getenv("MIN_LIQUIDITY_EXECUTION", "10000"))

# =============================================================================
# ANTI-MANIPULATION
# =============================================================================
SINGLE_WALLET_MAX_SHARE: Final[float] = float(os.getenv("SINGLE_WALLET_MAX_SHARE", "0.4"))
DEV_WALLET_MAX_SHARE: Final[float] = float(os.getenv("DEV_WALLET_MAX_SHARE", "0.25"))
NEW_WALLET_MAX_RATIO: Final[float] = float(os.getenv("NEW_WALLET_MAX_RATIO", "0.5"))

# =============================================================================
# PRICE SOURCE
# =============================================================================
PRICE_SOURCE: Final[str] = os.getenv("PRICE_SOURCE", "dexscreener")
PRICE_UPDATE_INTERVAL_SEC: Final[int] = int(os.getenv("PRICE_UPDATE_INTERVAL_SEC", "5"))

# =============================================================================
# SLIPPAGE MODEL
# =============================================================================
SLIPPAGE_COEFF_SMALL_POOL: Final[float] = float(os.getenv("SLIPPAGE_COEFF_SMALL_POOL", "0.8"))
SLIPPAGE_COEFF_LARGE_POOL: Final[float] = float(os.getenv("SLIPPAGE_COEFF_LARGE_POOL", "0.5"))
SMALL_POOL_THRESHOLD_USD: Final[float] = float(os.getenv("SMALL_POOL_THRESHOLD_USD", "50000"))

# =============================================================================
# REDIS
# =============================================================================
REDIS_HOST: Final[str] = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: Final[int] = int(os.getenv("REDIS_PORT", "6379"))
REDIS_URL: Final[str] = os.getenv("REDIS_URL") or os.getenv("REDIS_CONNECTION_STRING") or f"redis://{REDIS_HOST}:{REDIS_PORT}"

# =============================================================================
# POSTGRES
# =============================================================================
POSTGRES_HOST: Final[str] = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: Final[int] = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB: Final[str] = os.getenv("POSTGRES_DB", "signals")
POSTGRES_USER: Final[str] = os.getenv("POSTGRES_USER", "signals")
POSTGRES_PASSWORD: Final[str] = os.getenv("POSTGRES_PASSWORD", "signals_pass")
# Support Railway's DATABASE_URL format
POSTGRES_URL: Final[str] = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# =============================================================================
# TELEGRAM
# =============================================================================
TELEGRAM_BOT_TOKEN: Final[str] = os.getenv("TELEGRAM_BOT_TOKEN", "")

# =============================================================================
# HELIUS
# =============================================================================
HELIUS_WEBHOOK_SECRET: Final[str] = os.getenv("HELIUS_WEBHOOK_SECRET", "")

# =============================================================================
# STREAMS
# =============================================================================
STREAM_SWAP: Final[str] = "events:swap"
STREAM_ADD_LIQUIDITY: Final[str] = "events:add_liquidity"
STREAM_CREATE_POOL: Final[str] = "events:create_pool"
STREAM_DLQ: Final[str] = "events:dlq"
STREAM_TOKEN_STATE: Final[str] = "token_state"
STREAM_SIGNALS: Final[str] = "signals:generated"

STREAM_PARTITIONS: Final[int] = 8
CONSUMER_GROUP: Final[str] = "signal_workers"

# =============================================================================
# CACHE TTL
# =============================================================================
PRICE_CACHE_TTL_SEC: Final[int] = 10
WALLET_CACHE_TTL_SEC: Final[int] = 300  # 5 min
TOKEN_STATE_SNAPSHOT_SEC: Final[int] = 10

# =============================================================================
# IDEMPOTENCY
# =============================================================================
IDEMPOTENCY_TTL_SEC: Final[int] = 1800  # 30 min

# =============================================================================
# QUOTE MINTS (for determining base token)
# =============================================================================
QUOTE_MINTS: Final[list] = [
    "So11111111111111111111111111111111111111112",  # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDj1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
]

# =============================================================================
# CIRCUIT BREAKER
# =============================================================================
CB_SIGNALS_PER_WINDOW: Final[int] = 20
CB_WINDOW_SEC: Final[int] = 300  # 5 min
CB_MIN_AVG_SCORE: Final[float] = 0.7
CB_COOLDOWN_SEC: Final[int] = 300  # 5 min

# =============================================================================
# BACKTEST
# =============================================================================
BACKTEST_COMMISSION_ENTRY: Final[float] = 0.001  # 0.1%
BACKTEST_COMMISSION_EXIT: Final[float] = 0.001  # 0.1%
BACKTEST_SLIPPAGE_SIGMA: Final[float] = 0.5
BACKTEST_LOW_LIQ_MULTIPLIER: Final[float] = 1.5
BACKTEST_MAX_SLIPPAGE: Final[float] = 0.30

# =============================================================================
# ANTI-MANIPULATION ADAPTIVE
# =============================================================================
def get_single_wallet_max_share(liquidity_usd: float) -> float:
    """Adaptive threshold based on liquidity"""
    base = max(0.4, 0.6 * (1 - liquidity_usd / 100000))
    if liquidity_usd < 50000:
        return 0.25
    return base

# =============================================================================
# VALIDATION
# =============================================================================
def validate_weights() -> bool:
    """Ensure weights sum to 1.0"""
    total = W1_FLOW + W2_WALLET + W3_LIQUIDITY + W4_MOMENTUM + W5_DEV
    return abs(total - 1.0) < 0.001
