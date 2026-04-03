"""
Flow Engine - Swap Volume Aggregation (v2.0)
Aggregates swap events over 1m, 5m, 15m windows
"""
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from config import (
    FLOW_RATIO_BULLISH,
    NET_PRESSURE_MIN,
)

logger = logging.getLogger("flow_engine")


@dataclass
class FlowWindow:
    """Rolling window of buy/sell volumes"""
    timestamp: float
    buy_usd: float
    sell_usd: float


@dataclass
class FlowMetrics:
    """Flow calculation results"""
    buy_volume_1m: float = 0.0
    sell_volume_1m: float = 0.0
    buy_volume_5m: float = 0.0
    sell_volume_5m: float = 0.0
    buy_volume_15m: float = 0.0
    sell_volume_15m: float = 0.0
    flow_ratio: float = 0.0
    net_pressure: float = 0.0
    flow_strength: float = 0.0  # -1 to 1, positive = bullish


class FlowEngine:
    """
    Aggregates swap volumes and calculates flow metrics.
    Uses rolling deques to maintain window state.
    """
    
    WINDOW_1M = 60      # 1 minute
    WINDOW_5M = 300     # 5 minutes
    WINDOW_15M = 900    # 15 minutes
    
    def __init__(self):
        self.windows_1m: dict[str, deque] = {}  # token -> deque of FlowWindow
        self.windows_5m: dict[str, deque] = {}
        self.windows_15m: dict[str, deque] = {}
        self._cleanup_interval = 300  # Cleanup old data every 5 min
        self._last_cleanup = time.time()
    
    def process_swap(
        self,
        token_mint: str,
        side: str,  # "BUY" or "SELL"
        amount_usd: float,
        timestamp: Optional[float] = None
    ) -> FlowMetrics:
        """
        Process a swap event and return updated flow metrics.
        Call this when a new SWAP event is received.
        """
        if timestamp is None:
            timestamp = time.time()
        
        now = timestamp
        is_buy = side == "BUY"
        
        # Ensure deques exist
        if token_mint not in self.windows_1m:
            self.windows_1m[token_mint] = deque()
            self.windows_5m[token_mint] = deque()
            self.windows_15m[token_mint] = deque()
        
        # Add to windows
        window = FlowWindow(timestamp=now, buy_usd=amount_usd if is_buy else 0, sell_usd=amount_usd if not is_buy else 0)
        
        self.windows_1m[token_mint].append(window)
        self.windows_5m[token_mint].append(window)
        self.windows_15m[token_mint].append(window)
        
        # Cleanup old entries
        self._cleanup_old_entries(token_mint, now)
        
        # Calculate metrics
        return self._calculate_metrics(token_mint)
    
    def _cleanup_old_entries(self, token_mint: str, now: float):
        """Remove entries outside window"""
        cutoff_1m = now - self.WINDOW_1M
        cutoff_5m = now - self.WINDOW_5M
        cutoff_15m = now - self.WINDOW_15M
        
        # 1m window
        dq = self.windows_1m[token_mint]
        while dq and dq[0].timestamp < cutoff_1m:
            dq.popleft()
        
        # 5m window
        dq = self.windows_5m[token_mint]
        while dq and dq[0].timestamp < cutoff_5m:
            dq.popleft()
        
        # 15m window
        dq = self.windows_15m[token_mint]
        while dq and dq[0].timestamp < cutoff_15m:
            dq.popleft()
    
    def _calculate_metrics(self, token_mint: str) -> FlowMetrics:
        """Calculate flow metrics from current window state"""
        metrics = FlowMetrics()
        
        # Aggregate 1m
        for w in self.windows_1m.get(token_mint, []):
            metrics.buy_volume_1m += w.buy_usd
            metrics.sell_volume_1m += w.sell_usd
        
        # Aggregate 5m
        for w in self.windows_5m.get(token_mint, []):
            metrics.buy_volume_5m += w.buy_usd
            metrics.sell_volume_5m += w.sell_usd
        
        # Aggregate 15m
        for w in self.windows_15m.get(token_mint, []):
            metrics.buy_volume_15m += w.buy_usd
            metrics.sell_volume_15m += w.sell_usd
        
        # Flow ratio
        if metrics.sell_volume_5m > 0:
            metrics.flow_ratio = metrics.buy_volume_5m / metrics.sell_volume_5m
        else:
            metrics.flow_ratio = metrics.buy_volume_5m if metrics.buy_volume_5m > 0 else 0
        
        return metrics
    
    def calculate_flow_strength(
        self,
        metrics: FlowMetrics,
        liquidity_usd: float
    ) -> float:
        """
        Calculate flow strength: -1 to 1
        Positive = bullish flow
        Uses tanh for smooth scaling
        """
        import math
        
        # Net pressure
        if liquidity_usd > 0:
            net_pressure = (metrics.buy_volume_5m - metrics.sell_volume_5m) / liquidity_usd
        else:
            net_pressure = 0
        
        # Flow ratio normalized around 1.0
        flow_ratio_norm = metrics.flow_ratio - 1.0
        
        # tanh compresses to -1..1 range smoothly
        flow_strength = math.tanh(flow_ratio_norm) * (1 + net_pressure)
        
        return max(-1.0, min(1.0, flow_strength))
    
    def get_flow_signal(self, metrics: FlowMetrics) -> str:
        """Get simple flow signal"""
        if metrics.flow_ratio >= FLOW_RATIO_BULLISH:
            return "BULLISH"
        elif metrics.flow_ratio <= 1 / FLOW_RATIO_BULLISH:
            return "BEARISH"
        else:
            return "NEUTRAL"
    
    def is_bullish_flow(self, metrics: FlowMetrics) -> bool:
        """Check if flow meets bullish threshold"""
        return metrics.flow_ratio >= FLOW_RATIO_BULLISH
    
    def is_net_pressure_positive(self, metrics: FlowMetrics, liquidity_usd: float) -> bool:
        """Check if net pressure meets minimum"""
        if liquidity_usd <= 0:
            return False
        
        net_pressure = (metrics.buy_volume_5m - metrics.sell_volume_5m) / liquidity_usd
        return net_pressure >= NET_PRESSURE_MIN


# =============================================================================
# STANDALONE TEST
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    engine = FlowEngine()
    
    # Simulate buys
    print("Testing FlowEngine...")
    
    for i in range(5):
        metrics = engine.process_swap("TestToken", "BUY", 1000)
        print(f"Swap {i}: flow_ratio={metrics.flow_ratio:.2f}")
    
    metrics = engine.process_swap("TestToken", "SELL", 2000)
    print(f"After sell: flow_ratio={metrics.flow_ratio:.2f}")
    
    # Calculate strength
    strength = engine.calculate_flow_strength(metrics, liquidity_usd=50000)
    print(f"Flow strength: {strength:.3f}")
    print(f"Signal: {engine.get_flow_signal(metrics)}")
