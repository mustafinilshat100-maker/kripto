"""
Wallet Intelligence Engine (v2.0)
Tracks wallet performance, calculates alpha scores, detects manipulation
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

import asyncpg
from redis.asyncio import Redis

from config import (
    REDIS_URL,
    WALLET_CACHE_TTL_SEC,
)

logger = logging.getLogger("wallet_intel")


@dataclass
class WalletStats:
    """Wallet performance metrics"""
    wallet: str
    total_trades: int = 0
    buy_trades: int = 0
    sell_trades: int = 0
    total_volume_usd: float = 0.0
    avg_trade_size_usd: float = 0.0
    realized_pnl: float = 0.0
    winrate: float = 0.0
    avg_entry_time_sec: float = 0.0
    total_capital_deployed: float = 0.0
    peak_capital: float = 0.0
    hold_time_median: float = 0.0
    alpha_score: float = 0.0
    last_updated: float = 0.0


class WalletIntelligenceEngine:
    """
    Manages wallet statistics and alpha scoring.
    
    Alpha score formula:
    - early_entry_score = 1 - percentile_rank(avg_entry_time_sec)
    - consistency_score = winrate * (trades / (trades + 10))
    - capital_efficiency = (pnl + 0.01) / (peak_capital + 0.01)
    - alpha_score = early_entry_score * consistency_score * capital_efficiency
    """
    
    def __init__(self, redis: Redis, db_pool: asyncpg.Pool):
        self.redis = redis
        self.db = db_pool
        self._cache: dict[str, WalletStats] = {}
        self._cache_ttl = WALLET_CACHE_TTL_SEC
    
    async def get_wallet_stats(self, wallet: str) -> Optional[WalletStats]:
        """Get wallet stats, from cache or DB"""
        # Check cache first
        if wallet in self._cache:
            stats = self._cache[wallet]
            if time.time() - stats.last_updated < self._cache_ttl:
                return stats
        
        # Fetch from DB
        stats = await self._fetch_from_db(wallet)
        
        if stats:
            self._cache[wallet] = stats
            # Cache in Redis too
            await self.redis.setex(
                f"wallet:{wallet}",
                self._cache_ttl,
                str(stats.alpha_score)
            )
        
        return stats
    
    async def _fetch_from_db(self, wallet: str) -> Optional[WalletStats]:
        """Fetch wallet stats from Postgres"""
        try:
            async with self.db.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT 
                        wallet,
                        total_trades,
                        buy_trades,
                        sell_trades,
                        total_volume_usd,
                        avg_trade_size_usd,
                        realized_pnl,
                        winrate,
                        avg_entry_time_sec,
                        total_capital_deployed,
                        peak_capital,
                        hold_time_median,
                        last_updated
                    FROM wallets
                    WHERE wallet = $1
                """, wallet)
                
                if not row:
                    return None
                
                stats = WalletStats(
                    wallet=row["wallet"],
                    total_trades=row["total_trades"],
                    buy_trades=row["buy_trades"],
                    sell_trades=row["sell_trades"],
                    total_volume_usd=row["total_volume_usd"],
                    avg_trade_size_usd=row["avg_trade_size_usd"],
                    realized_pnl=row["realized_pnl"],
                    winrate=row["winrate"] or 0.0,
                    avg_entry_time_sec=row["avg_entry_time_sec"] or 0.0,
                    total_capital_deployed=row["total_capital_deployed"] or 0.0,
                    peak_capital=row["peak_capital"] or 0.0,
                    hold_time_median=row["hold_time_median"] or 0.0,
                    last_updated=row["last_updated"].timestamp() if row["last_updated"] else 0,
                )
                
                # Calculate alpha score
                stats.alpha_score = self._calculate_alpha_score(stats)
                
                return stats
                
        except Exception as e:
            logger.error(f"Failed to fetch wallet {wallet}: {e}")
            return None
    
    def _calculate_alpha_score(self, stats: WalletStats) -> float:
        """Calculate alpha score (0..1)"""
        import math
        
        # Early entry score (lower entry time = higher score)
        # Assuming avg_entry_time of 0-300 seconds is good
        if stats.avg_entry_time_sec > 0:
            early_entry_score = max(0, 1 - (stats.avg_entry_time_sec / 300))
        else:
            early_entry_score = 0.5  # Neutral if no data
        
        # Consistency score
        consistency = stats.winrate * (stats.total_trades / (stats.total_trades + 10))
        
        # Capital efficiency
        if stats.peak_capital > 0:
            capital_eff = (stats.realized_pnl + 0.01) / (stats.peak_capital + 0.01)
        else:
            capital_eff = 0.5
        
        # Combined score
        alpha = early_entry_score * consistency * capital_eff
        
        return max(0.0, min(1.0, alpha))
    
    async def get_wallet_quality_score(self, wallet: str) -> float:
        """Get quick quality score for a wallet (0..1)"""
        stats = await self.get_wallet_stats(wallet)
        if not stats:
            return 0.3  # Default unknown wallet score
        
        return stats.alpha_score
    
    async def get_pool_wallet_quality(self, token_mint: str, wallet_volumes: dict[str, float]) -> tuple[float, float]:
        """
        Calculate pool-wide wallet quality and max wallet share.
        
        Returns: (avg_quality, max_wallet_share)
        """
        total_volume = sum(wallet_volumes.values())
        
        if total_volume == 0:
            return 0.5, 0.0
        
        weighted_quality = 0.0
        max_share = 0.0
        
        for wallet, volume in wallet_volumes.items():
            share = volume / total_volume
            max_share = max(max_share, share)
            
            quality = await self.get_wallet_quality_score(wallet)
            weighted_quality += quality * share
        
        return weighted_quality, max_share


async def init_wallet_db(db_pool: asyncpg.Pool):
    """Initialize wallet table"""
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                wallet VARCHAR(64) PRIMARY KEY,
                total_trades INT DEFAULT 0,
                buy_trades INT DEFAULT 0,
                sell_trades INT DEFAULT 0,
                total_volume_usd FLOAT DEFAULT 0,
                avg_trade_size_usd FLOAT DEFAULT 0,
                realized_pnl FLOAT DEFAULT 0,
                winrate FLOAT DEFAULT 0,
                avg_entry_time_sec FLOAT DEFAULT 0,
                total_capital_deployed FLOAT DEFAULT 0,
                peak_capital FLOAT DEFAULT 0,
                hold_time_median FLOAT DEFAULT 0,
                last_updated TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_wallets_volume ON wallets(total_volume_usd DESC)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_wallets_winrate ON wallets(winrate DESC)
        """)


if __name__ == "__main__":
    # Test
    print("WalletIntelligence module loaded")
