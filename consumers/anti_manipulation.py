"""
Anti-Manipulation Engine (v2.0)
Filters out suspicious pools before signal generation
"""
import logging
from dataclasses import dataclass
from typing import Optional

from config import (
    SINGLE_WALLET_MAX_SHARE,
    DEV_WALLET_MAX_SHARE,
    NEW_WALLET_MAX_RATIO,
    get_single_wallet_max_share,
)

logger = logging.getLogger("anti_manipulation")


@dataclass
class ManipulationCheck:
    """Result of manipulation check"""
    passed: bool
    reason: str  # "ok" or specific rejection reason
    severity: str  # "none", "warning", "critical"


class AntiManipulationEngine:
    """
    Checks for common manipulation patterns:
    - Single wallet dominance
    - Dev wallet control
    - Sybil / new wallet spam
    - Volume anomalies
    """
    
    def __init__(self):
        # Track recent volume anomalies per token
        self._anomaly_flags: dict[str, float] = {}  # token -> unlock_time
        self._anomaly_window = 600  # 10 minutes lockout
    
    def check(
        self,
        token_mint: str,
        wallet_volumes: dict[str, float],  # wallet -> volume_usd
        total_volume: float,
        dev_wallet: Optional[str],
        dev_volume: float,
        wallet_ages: dict[str, float],  # wallet -> age_hours
        new_wallet_volume_ratio: float,
        liquidity_usd: float,
        recent_wallet_volumes: dict[str, float],  # Volume in last 5 min per wallet
    ) -> ManipulationCheck:
        """
        Full anti-manipulation check.
        
        Returns ManipulationCheck with passed=True if all checks pass.
        """
        
        # Check if token is temporarily locked due to anomaly
        import time
        if token_mint in self._anomaly_flags:
            if time.time() < self._anomaly_flags[token_mint]:
                return ManipulationCheck(
                    passed=False,
                    reason="volume_anomaly_lockout",
                    severity="critical"
                )
            else:
                del self._anomaly_flags[token_mint]
        
        # 1. Single wallet dominance
        if total_volume > 0:
            max_wallet_share = max(wallet_volumes.values()) / total_volume
        else:
            max_wallet_share = 0
        
        # Adaptive threshold based on liquidity
        adaptive_single_max = get_single_wallet_max_share(liquidity_usd)
        
        if max_wallet_share > adaptive_single_max:
            logger.warning(f"Single wallet dominance: {max_wallet_share:.2%} > {adaptive_single_max:.2%}")
            return ManipulationCheck(
                passed=False,
                reason="single_wallet_dominance",
                severity="critical"
            )
        
        # 2. Dev wallet control
        if dev_wallet and total_volume > 0:
            dev_share = dev_volume / total_volume
            if dev_share > DEV_WALLET_MAX_SHARE:
                logger.warning(f"Dev wallet control: {dev_share:.2%} > {DEV_WALLET_MAX_SHARE:.2%}")
                return ManipulationCheck(
                    passed=False,
                    reason="dev_wallet_control",
                    severity="critical"
                )
        
        # 3. Sybil / new wallets
        if new_wallet_volume_ratio > NEW_WALLET_MAX_RATIO:
            logger.warning(f"Sybil detected: {new_wallet_volume_ratio:.2%} new wallet ratio")
            return ManipulationCheck(
                passed=False,
                reason="sybil_detected",
                severity="critical"
            )
        
        # 4. Volume anomaly in last 5 minutes
        if total_volume > 0:
            recent_total = sum(recent_wallet_volumes.values())
            if recent_total > 0:
                for wallet, vol in recent_wallet_volumes.items():
                    if recent_total > 0:
                        wallet_recent_share = vol / recent_total
                        if wallet_recent_share > 0.5:
                            # >50% from one wallet in 5 min - flag for lockout
                            import time
                            self._anomaly_flags[token_mint] = time.time() + self._anomaly_window
                            logger.warning(f"Volume anomaly: {wallet[:16]} contributed {wallet_recent_share:.2%} in 5 min")
                            return ManipulationCheck(
                                passed=False,
                                reason="volume_anomaly",
                                severity="critical"
                            )
        
        return ManipulationCheck(
            passed=True,
            reason="ok",
            severity="none"
        )
    
    def check_single_wallet(self, wallet_share: float, liquidity_usd: float) -> bool:
        """
        Quick single-wallet dominance check.
        Returns True if dominance detected.
        """
        threshold = get_single_wallet_max_share(liquidity_usd)
        return wallet_share > threshold
    
    def check_dev_wallet(self, dev_share: float) -> bool:
        """Quick dev wallet check. Returns True if suspicious."""
        return dev_share > DEV_WALLET_MAX_SHARE
    
    def check_sybil(self, new_wallet_ratio: float) -> bool:
        """Quick sybil check. Returns True if suspicious."""
        return new_wallet_ratio > NEW_WALLET_MAX_RATIO


# =============================================================================
# STANDALONE TEST
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    engine = AntiManipulationEngine()
    
    # Simulate a healthy pool
    result = engine.check(
        token_mint="HealthyToken",
        wallet_volumes={"W1": 5000, "W2": 3000, "W3": 2000},
        total_volume=10000,
        dev_wallet=None,
        dev_volume=0,
        wallet_ages={"W1": 48, "W2": 72, "W3": 24},
        new_wallet_volume_ratio=0.2,
        liquidity_usd=100000,
        recent_wallet_volumes={"W1": 1000, "W2": 800, "W3": 200},
    )
    print(f"Healthy pool: passed={result.passed}, reason={result.reason}")
    
    # Simulate single wallet dominance
    result = engine.check(
        token_mint="BadToken1",
        wallet_volumes={"W1": 8000, "W2": 1000, "W3": 1000},
        total_volume=10000,
        dev_wallet=None,
        dev_volume=0,
        wallet_ages={},
        new_wallet_volume_ratio=0.1,
        liquidity_usd=50000,
        recent_wallet_volumes={"W1": 5000, "W2": 500, "W3": 500},
    )
    print(f"Single wallet: passed={result.passed}, reason={result.reason}")
    
    # Simulate dev control
    result = engine.check(
        token_mint="BadToken2",
        wallet_volumes={"Dev": 4000, "W1": 3000, "W2": 3000},
        total_volume=10000,
        dev_wallet="Dev",
        dev_volume=4000,
        wallet_ages={},
        new_wallet_volume_ratio=0.1,
        liquidity_usd=50000,
        recent_wallet_volumes={"Dev": 2000, "W1": 1500, "W2": 1500},
    )
    print(f"Dev control: passed={result.passed}, reason={result.reason}")
