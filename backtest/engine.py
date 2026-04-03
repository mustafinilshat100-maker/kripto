"""
Backtest Engine (v2.0)
Realistic backtesting with dynamic slippage and commissions
"""
import json
import logging
import math
import random
from dataclasses import dataclass, field
from typing import Optional
from collections import deque

import numpy as np

from config import (
    BACKTEST_COMMISSION_ENTRY,
    BACKTEST_COMMISSION_EXIT,
    BACKTEST_SLIPPAGE_SIGMA,
    BACKTEST_LOW_LIQ_MULTIPLIER,
    BACKTEST_MAX_SLIPPAGE,
)

logger = logging.getLogger("backtest")


@dataclass
class Trade:
    """Single trade record"""
    token: str
    entry_price: float
    exit_price: float
    entry_time: float
    exit_time: float
    size_usd: float
    slippage_entry: float
    slippage_exit: float
    pnl_usd: float
    pnl_pct: float
    realized: bool


@dataclass
class BacktestResult:
    """Backtest results"""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    winrate: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    avg_slippage: float = 0.0
    avg_trade: float = 0.0
    trades: list[Trade] = field(default_factory=list)


class BacktestEngine:
    """
    Realistic backtest engine with:
    - Dynamic slippage model (lognormal)
    - Commission tracking (0.1% entry + 0.1% exit)
    - Performance metrics
    """
    
    def __init__(self, initial_capital: float = 10000):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.peak_capital = initial_capital
        self.trades: deque[Trade] = deque(maxlen=10000)
        self.equity_curve: deque[float] = deque(maxlen=10000)
        
        # Running stats
        self._gross_profit = 0.0
        self._gross_loss = 0.0
        self._daily_returns: list[float] = []
    
    def calculate_slippage(
        self,
        amount_usd: float,
        liquidity_usd: float,
        is_backtest: bool = True,
        volatility_factor: float = 1.0,
    ) -> float:
        """
        Calculate realistic slippage.
        
        For backtest: adds lognormal noise
        For live: deterministic
        
        Args:
            amount_usd: Position size
            liquidity_usd: Available liquidity
            is_backtest: Add noise for backtest
            volatility_factor: Multiplier for high volatility periods
        """
        base_slip = (amount_usd / max(liquidity_usd, 1000)) * 0.8
        
        if is_backtest:
            # Lognormal distribution with median = base_slip
            # sigma = 0.5 means typical range is 0.5x to 2x of base
            sigma = BACKTEST_SLIPPAGE_SIGMA * volatility_factor
            slip = np.random.lognormal(mean=np.log(base_slip + 0.0001), sigma=sigma)
        else:
            slip = base_slip
        
        # Low liquidity multiplier for memcoins
        if liquidity_usd < 20000:
            slip *= BACKTEST_LOW_LIQ_MULTIPLIER
        
        return min(slip, BACKTEST_MAX_SLIPPAGE)
    
    def execute_trade(
        self,
        token: str,
        entry_price: float,
        exit_price: float,
        size_usd: float,
        entry_time: float,
        exit_time: float,
        liquidity_usd: float,
        is_backtest: bool = True,
    ) -> Trade:
        """
        Execute a simulated trade with realistic costs.
        
        Returns Trade with all metrics including slippage and PnL.
        """
        # Calculate entry slippage
        slippage_entry = self.calculate_slippage(size_usd, liquidity_usd, is_backtest)
        
        # Effective entry price (accounts for slippage against us)
        effective_entry = entry_price * (1 + slippage_entry)
        
        # Calculate exit slippage
        slippage_exit = self.calculate_slippage(size_usd, liquidity_usd, is_backtest)
        effective_exit = exit_price * (1 - slippage_exit)
        
        # Commissions
        commission_entry = size_usd * BACKTEST_COMMISSION_ENTRY
        commission_exit = size_usd * BACKTEST_COMMISSION_EXIT
        
        # Net PnL
        gross_pnl = (effective_exit - effective_entry) / effective_entry * size_usd
        net_pnl = gross_pnl - commission_entry - commission_exit
        
        trade = Trade(
            token=token,
            entry_price=effective_entry,
            exit_price=effective_exit,
            entry_time=entry_time,
            exit_time=exit_time,
            size_usd=size_usd,
            slippage_entry=slippage_entry,
            slippage_exit=slippage_exit,
            pnl_usd=net_pnl,
            pnl_pct=(effective_exit - effective_entry) / effective_entry - BACKTEST_COMMISSION_ENTRY - BACKTEST_COMMISSION_EXIT,
        )
        
        return trade
    
    def run_backtest(self, signals: list[dict], prices: dict[str, list[dict]]) -> BacktestResult:
        """
        Run backtest on signals and price data.
        
        Args:
            signals: List of signal dicts with token, score, timestamp, entry_zone
            prices: Dict of token -> list of {timestamp, price, liquidity}
        
        Returns BacktestResult with all metrics.
        """
        result = BacktestResult()
        
        for signal in signals:
            token = signal.get("token")
            signal_time = signal.get("timestamp")
            entry_zone = signal.get("entry_zone", [0, float('inf')])
            size = signal.get("size_usd", 1000)  # Default $1000 position
            
            # Get price data
            token_prices = prices.get(token, [])
            
            # Find entry price (first price within entry zone)
            entry_price = None
            for p in token_prices:
                if p["timestamp"] >= signal_time:
                    if entry_zone[0] <= p["price"] <= entry_zone[1]:
                        entry_price = p["price"]
                        liquidity = p.get("liquidity_usd", 50000)
                        break
            
            if entry_price is None:
                logger.debug(f"No entry found for {token[:16]}")
                continue
            
            # Find exit price (after holding period or stop loss)
            # Simple: exit after 5 minutes or if price drops 5%
            exit_price = None
            exit_time = None
            holding_period = 300  # 5 minutes
            
            for p in token_prices:
                if p["timestamp"] > signal_time + holding_period:
                    exit_price = p["price"]
                    exit_time = p["timestamp"]
                    break
                
                # Stop loss at -5%
                if p["price"] < entry_price * 0.95:
                    exit_price = p["price"]
                    exit_time = p["timestamp"]
                    break
            
            if exit_price is None:
                continue
            
            # Execute trade
            trade = self.execute_trade(
                token=token,
                entry_price=entry_price,
                exit_price=exit_price,
                size_usd=size,
                entry_time=signal_time,
                exit_time=exit_time,
                liquidity_usd=liquidity,
                is_backtest=True,
            )
            
            self.trades.append(trade)
            result.trades.append(trade)
            
            # Update capital
            self.current_capital += trade.pnl_usd
            self.peak_capital = max(self.peak_capital, self.current_capital)
            self.equity_curve.append(self.current_capital)
        
        # Calculate final metrics
        return self._calculate_metrics(result)
    
    def _calculate_metrics(self, result: BacktestResult) -> BacktestResult:
        """Calculate all performance metrics"""
        n = len(result.trades)
        if n == 0:
            return result
        
        result.total_trades = n
        
        # Win/loss
        winning = [t for t in result.trades if t.pnl_usd > 0]
        losing = [t for t in result.trades if t.pnl_usd <= 0]
        
        result.winning_trades = len(winning)
        result.losing_trades = len(losing)
        result.winrate = len(winning) / n
        
        # Profit factor
        result.gross_profit = sum(t.pnl_usd for t in winning)
        result.gross_loss = abs(sum(t.pnl_usd for t in losing))
        
        if result.gross_loss > 0:
            result.profit_factor = result.gross_profit / result.gross_loss
        else:
            result.profit_factor = float('inf') if result.gross_profit > 0 else 0
        
        # Average trade
        pnls = [t.pnl_usd for t in result.trades]
        result.avg_trade = sum(pnls) / n
        
        # Average slippage
        slippages = [t.slippage_entry + t.slippage_exit for t in result.trades]
        result.avg_slippage = sum(slippages) / n
        
        # Sharpe ratio (simplified)
        if len(pnls) > 1:
            mean_return = sum(pnls) / len(pnls)
            std_return = (sum((p - mean_return) ** 2 for p in pnls) / len(pnls)) ** 0.5
            if std_return > 0:
                result.sharpe_ratio = mean_return / std_return * math.sqrt(252)  # Annualized
        
        # Max drawdown
        peak = self.initial_capital
        max_dd = 0
        
        for equity in self.equity_curve:
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak
            max_dd = max(max_dd, dd)
        
        result.max_drawdown = max_dd
        
        return result
    
    def print_summary(self, result: BacktestResult):
        """Print backtest summary"""
        print("\n" + "=" * 50)
        print("BACKTEST RESULTS")
        print("=" * 50)
        print(f"Total Trades:    {result.total_trades}")
        print(f"Win Rate:        {result.winrate:.1%}")
        print(f"Profit Factor:   {result.profit_factor:.2f}")
        print(f"Avg Trade:       ${result.avg_trade:.2f}")
        print(f"Sharpe Ratio:    {result.sharpe_ratio:.2f}")
        print(f"Max Drawdown:    {result.max_drawdown:.1%}")
        print(f"Avg Slippage:    {result.avg_slippage:.2%}")
        print("=" * 50)


# =============================================================================
# STANDALONE TEST
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    engine = BacktestEngine(initial_capital=10000)
    
    # Simulate 100 trades
    print("Testing BacktestEngine...")
    
    for i in range(100):
        entry = 0.001
        # Random exit (some wins, some losses)
        if random.random() > 0.45:  # 55% win rate
            exit_price = entry * (1 + random.uniform(0.01, 0.10))  # 1-10% gain
        else:
            exit_price = entry * (1 - random.uniform(0.01, 0.05))  # 1-5% loss
        
        trade = engine.execute_trade(
            token=f"Token{i}",
            entry_price=entry,
            exit_price=exit_price,
            size_usd=1000,
            entry_time=1000000 + i * 300,
            exit_time=1000000 + i * 300 + 300,
            liquidity_usd=50000,
            is_backtest=True,
        )
        
        engine.trades.append(trade)
        engine.current_capital += trade.pnl_usd
        engine.equity_curve.append(engine.current_capital)
    
    result = engine._calculate_metrics(BacktestResult(trades=list(engine.trades)))
    engine.print_summary(result)
