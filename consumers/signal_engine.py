"""
Signal Engine (v2.0)
Generates trading signals from flow, price, wallet, and pattern data
"""
import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Optional

from config import (
    W1_FLOW, W2_WALLET, W3_LIQUIDITY, W4_MOMENTUM, W5_DEV,
    SCORE_THRESHOLD, PATTERN_SCORE_MIN, SNIPER_RATIO_MAX,
    RUG_SCORE_MIN, MIN_LIQUIDITY_EXECUTION,
    SLIPPAGE_COEFF_SMALL_POOL, SLIPPAGE_COEFF_LARGE_POOL,
    SMALL_POOL_THRESHOLD_USD, BACKTEST_MAX_SLIPPAGE,
)

logger = logging.getLogger("signal_engine")


@dataclass
class Signal:
    """Trading signal output"""
    token: str
    score: float
    phase: str  # "LAUNCH", "ACCUMULATION", "DISTRIBUTION"
    entry_zone: tuple[float, float]  # [lower, upper]
    slippage_estimate: float
    max_safe_size_usd: float
    flow_ratio: float
    price_momentum: float
    pattern_score: float
    timestamp: float
    signal_hash: str


class SignalEngine:
    """
    Generates signals based on:
    - Flow strength (weighted W1)
    - Wallet quality (W2)
    - Liquidity score (W3)
    - Price momentum (W4)
    - Dev wallet ratio penalty (W5)
    
    Entry conditions:
    - pattern_score >= PATTERN_SCORE_MIN
    - phase in [LAUNCH, ACCUMULATION]
    - sniper_ratio < SNIPER_RATIO_MAX
    - rug_score >= RUG_SCORE_MIN
    - score >= SCORE_THRESHOLD
    - not anti_manipulation_triggered
    - liquidity_usd >= MIN_LIQUIDITY_EXECUTION
    """
    
    def __init__(self):
        # Phase detection thresholds (based on age and activity)
        self._launch_threshold = 1800  # < 30 min
        self._accumulation_threshold = 7200  # < 2 hours
    
    def calculate_score(
        self,
        flow_strength: float,
        wallet_quality: float,
        liquidity_score: float,
        price_momentum: float,
        dev_wallet_ratio: float,
    ) -> float:
        """Calculate weighted signal score"""
        # Normalize flow_strength from -1..1 to 0..1
        flow_norm = (flow_strength + 1) / 2
        
        score = (
            W1_FLOW * flow_norm +
            W2_WALLET * wallet_quality +
            W3_LIQUIDITY * liquidity_score +
            W4_MOMENTUM * max(0, price_momentum) +
            W5_DEV * (1 - dev_wallet_ratio)
        )
        
        return min(1.0, max(0.0, score))
    
    def detect_phase(self, token_age_sec: float, activity_rate: float) -> str:
        """
        Detect token phase based on age and activity.
        
        Returns: "LAUNCH", "ACCUMULATION", or "DISTRIBUTION"
        """
        if token_age_sec < self._launch_threshold:
            return "LAUNCH"
        elif token_age_sec < self._accumulation_threshold:
            return "ACCUMULATION"
        else:
            return "DISTRIBUTION"
    
    def estimate_slippage(
        self,
        desired_size_usd: float,
        liquidity_usd: float
    ) -> float:
        """
        Estimate realistic slippage.
        Uses different coefficients for small vs large pools.
        """
        if liquidity_usd < SMALL_POOL_THRESHOLD_USD:
            coeff = SLIPPAGE_COEFF_SMALL_POOL
        else:
            coeff = SLIPPAGE_COEFF_LARGE_POOL
        
        base_slip = (desired_size_usd / max(liquidity_usd, 1000)) * coeff
        return min(base_slip, BACKTEST_MAX_SLIPPAGE)
    
    def calculate_max_safe_size(self, liquidity_usd: float) -> float:
        """Calculate max safe position size (1% of liquidity)"""
        return liquidity_usd * 0.01
    
    def generate_signal(
        self,
        token_mint: str,
        score: float,
        phase: str,
        price: float,
        flow_ratio: float,
        flow_strength: float,
        price_momentum: float,
        pattern_score: float,
        liquidity_usd: float,
        anti_manipulation_passed: bool,
        sniper_ratio: float = 0.0,
        rug_score: float = 100.0,
        dev_wallet_ratio: float = 0.0,
        desired_size_usd: float = 1000.0,
    ) -> Optional[Signal]:
        """
        Generate signal if all entry conditions are met.
        
        Args:
            token_mint: Token address
            score: Weighted signal score
            phase: Token phase (LAUNCH/ACCUMULATION/DISTRIBUTION)
            price: Current price
            flow_ratio: Buy/sell ratio
            flow_strength: -1..1 bullish indicator
            price_momentum: 5-min price change
            pattern_score: Pattern detection score
            liquidity_usd: Current liquidity
            anti_manipulation_passed: True if AME passed
            sniper_ratio: Ratio of very early buys
            rug_score: Rug detection score (0-100)
            dev_wallet_ratio: Dev wallet volume / total
            desired_size_usd: Target position size
            
        Returns:
            Signal if all conditions pass, None otherwise
        """
        now = time.time()
        
        # Entry conditions
        conditions = {
            "pattern_score": pattern_score >= PATTERN_SCORE_MIN,
            "phase": phase in ["LAUNCH", "ACCUMULATION"],
            "sniper": sniper_ratio < SNIPER_RATIO_MAX,
            "rug": rug_score >= RUG_SCORE_MIN,
            "score": score >= SCORE_THRESHOLD,
            "anti_manip": anti_manipulation_passed,
            "liquidity": liquidity_usd >= MIN_LIQUIDITY_EXECUTION,
        }
        
        failed = [k for k, v in conditions.items() if not v]
        if failed:
            logger.debug(f"Signal blocked: {failed}")
            return None
        
        # Calculate entry zone (±0.5%)
        entry_lower = price * 0.995
        entry_upper = price * 1.005
        
        # Calculate slippage and safe size
        slippage = self.estimate_slippage(desired_size_usd, liquidity_usd)
        max_safe = self.calculate_max_safe_size(liquidity_usd)
        
        # Generate unique hash
        signal_str = f"{token_mint}:{round(score, 2)}:{int(now // 60)}"
        signal_hash = hashlib.md5(signal_str.encode()).hexdigest()[:16]
        
        signal = Signal(
            token=token_mint,
            score=score,
            phase=phase,
            entry_zone=(entry_lower, entry_upper),
            slippage_estimate=slippage,
            max_safe_size_usd=max_safe,
            flow_ratio=flow_ratio,
            price_momentum=price_momentum,
            pattern_score=pattern_score,
            timestamp=now,
            signal_hash=signal_hash,
        )
        
        logger.info(
            f"SIGNAL: {token_mint[:16]} | score={score:.3f} | "
            f"phase={phase} | flow={flow_ratio:.2f}x | "
            f"momentum={price_momentum:.2%} | pattern={pattern_score:.2f}"
        )
        
        return signal


# =============================================================================
# STANDALONE TEST
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    engine = SignalEngine()
    
    # Test scoring
    score = engine.calculate_score(
        flow_strength=0.5,
        wallet_quality=0.7,
        liquidity_score=0.6,
        price_momentum=0.05,
        dev_wallet_ratio=0.1,
    )
    print(f"Score: {score:.3f}")
    
    # Test phase detection
    print(f"New token: {engine.detect_phase(600, 10)}")  # 10 min old
    print(f"Accumulation: {engine.detect_phase(3600, 5)}")  # 1 hour
    print(f"Distribution: {engine.detect_phase(86400, 1)}")  # 1 day
    
    # Test slippage
    slip = engine.estimate_slippage(1000, 50000)
    print(f"Slip @ 50k liq: {slip:.3%}")
    
    slip = engine.estimate_slippage(1000, 100000)
    print(f"Slip @ 100k liq: {slip:.3%}")
    
    # Test signal generation
    signal = engine.generate_signal(
        token_mint="TestToken111111111111111111111111111111",
        score=0.85,
        phase="LAUNCH",
        price=0.001,
        flow_ratio=2.5,
        flow_strength=0.6,
        price_momentum=0.08,
        pattern_score=0.75,
        liquidity_usd=50000,
        anti_manipulation_passed=True,
    )
    
    if signal:
        print(f"Signal generated: {signal.token[:16]}")
        print(f"Entry zone: {signal.entry_zone}")
        print(f"Slippage: {signal.slippage_estimate:.3%}")
