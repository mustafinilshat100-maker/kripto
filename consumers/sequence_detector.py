"""
Sequence Detector - Pattern Score Calculation (v2.0)
Detects trading patterns from event sequences
"""
import logging
import time
from dataclasses import dataclass

from config import PATTERN_SCORE_MIN

logger = logging.getLogger("sequence_detector")


@dataclass
class PatternResult:
    """Pattern detection result"""
    pattern_score: float  # 0..1
    has_pool: bool
    buy_count: int
    liquidity_growth: float  # ratio
    sell_penalty: float  # 0..1
    breakout_detected: bool
    meets_threshold: bool


class SequenceDetector:
    """
    Analyzes event sequences to detect trading patterns.
    Pattern score 0..1 based on:
    - CREATE_POOL within 30 min
    - Buy count after pool
    - Liquidity growth
    - Absence of large sells
    - Price breakout
    """
    
    # Weights
    W_BUY_COUNT = 0.3
    W_LIQUIDITY = 0.3
    W_SELL_PENALTY = 0.2
    W_BREAKOUT = 0.2
    
    # Thresholds
    LOOKBACK_30MIN = 1800  # seconds
    MIN_BUYS_FOR_SCORE = 3
    BREAKOUT_THRESHOLD = 0.07  # 7% momentum
    
    def analyze(self, events: list, price_momentum: float, current_liquidity: float, initial_liquidity: float) -> PatternResult:
        """
        Analyze event sequence and return pattern score.
        
        Args:
            events: list of event dicts with type, side, timestamp, etc.
            price_momentum: fractional price change over 5 min
            current_liquidity: current liquidity USD
            initial_liquidity: initial liquidity at pool creation
        """
        now = time.time()
        cutoff_30min = now - self.LOOKBACK_30MIN
        
        # Filter recent events
        recent_events = [e for e in events if e.get("timestamp", 0) >= cutoff_30min]
        
        # 1. Check for CREATE_POOL in last 30 min
        pool_events = [e for e in recent_events if e.get("type") == "create_pool"]
        has_pool = len(pool_events) > 0
        
        if not has_pool:
            return PatternResult(
                pattern_score=0.0,
                has_pool=False,
                buy_count=0,
                liquidity_growth=0.0,
                sell_penalty=1.0,
                breakout_detected=False,
                meets_threshold=False
            )
        
        # 2. Count buys after pool creation
        pool_time = pool_events[0].get("timestamp", now)
        post_pool_events = [e for e in recent_events if e.get("timestamp", 0) >= pool_time]
        
        buys = [
            e for e in post_pool_events
            if e.get("type") in ("add_liquidity", "swap") and e.get("side") == "BUY"
        ]
        buy_count = len(buys)
        buy_count_score = min(buy_count / self.MIN_BUYS_FOR_SCORE, 1.0)
        
        # 3. Liquidity growth (capped at 3x)
        if initial_liquidity > 0:
            liquidity_growth = min(current_liquidity / initial_liquidity, 3.0) / 3.0
        else:
            liquidity_growth = 0.0
        
        # 4. Sell penalty (large sells in last 1 min)
        cutoff_1min = now - 60
        cutoff_5min = now - 300
        
        # Get buy volume in last 5 min
        recent_swaps = [e for e in recent_events if e.get("type") == "swap"]
        buy_vol_5m = sum(
            e.get("amount_usd", 0)
            for e in recent_swaps
            if e.get("timestamp", 0) >= cutoff_5min and e.get("side") == "BUY"
        )
        
        # Large sells in last 1 min (>20% of 5m buy volume)
        sell_vol_1m = sum(
            e.get("amount_usd", 0)
            for e in recent_swaps
            if e.get("timestamp", 0) >= cutoff_1min and e.get("side") == "SELL"
        )
        
        if buy_vol_5m > 0:
            large_sell_ratio = sell_vol_1m / buy_vol_5m
        else:
            large_sell_ratio = 0
        
        sell_penalty = max(0, 1 - large_sell_ratio * 2)
        
        # 5. Breakout detection
        breakout_detected = price_momentum > self.BREAKOUT_THRESHOLD
        if breakout_detected:
            breakout_score = 1.0
        else:
            breakout_score = max(0, price_momentum / self.BREAKOUT_THRESHOLD)
        
        # Calculate total pattern score
        pattern_score = (
            self.W_BUY_COUNT * buy_count_score +
            self.W_LIQUIDITY * liquidity_growth +
            self.W_SELL_PENALTY * sell_penalty +
            self.W_BREAKOUT * breakout_score
        )
        
        meets_threshold = pattern_score >= PATTERN_SCORE_MIN
        
        result = PatternResult(
            pattern_score=min(1.0, max(0.0, pattern_score)),
            has_pool=True,
            buy_count=buy_count,
            liquidity_growth=liquidity_growth * 3,  # Un-normalize for display
            sell_penalty=sell_penalty,
            breakout_detected=breakout_detected,
            meets_threshold=meets_threshold
        )
        
        logger.debug(
            f"Pattern: score={result.pattern_score:.2f}, "
            f"buys={result.buy_count}, growth={result.liquidity_growth:.2f}x, "
            f"breakout={result.breakout_detected}"
        )
        
        return result


# =============================================================================
# STANDALONE TEST
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    detector = SequenceDetector()
    now = time.time()
    
    # Simulate fresh token with buys
    events = [
        {"type": "create_pool", "timestamp": now - 1200, "liquidity": 50000},
        {"type": "swap", "side": "BUY", "amount_usd": 1000, "timestamp": now - 1000},
        {"type": "swap", "side": "BUY", "amount_usd": 1500, "timestamp": now - 800},
        {"type": "add_liquidity", "side": "BUY", "amount_usd": 5000, "timestamp": now - 600},
        {"type": "swap", "side": "BUY", "amount_usd": 2000, "timestamp": now - 400},
    ]
    
    result = detector.analyze(events, price_momentum=0.08, current_liquidity=75000, initial_liquidity=50000)
    print(f"Pattern score: {result.pattern_score:.3f}")
    print(f"Meets threshold: {result.meets_threshold}")
    print(f"Breakout: {result.breakout_detected}")
    print(f"Buy count: {result.buy_count}")
