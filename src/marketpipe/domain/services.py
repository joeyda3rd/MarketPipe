"""Domain services for MarketPipe.

Domain services contain business logic that doesn't naturally belong
to any single entity or value object. They coordinate operations
across multiple domain objects or provide stateless business operations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import date, datetime
from decimal import Decimal

from .entities import OHLCVBar
from .value_objects import Symbol, Price, Volume, Timestamp, TimeRange
from .aggregates import SymbolBarsAggregate, DailySummary


class DomainService(ABC):
    """Base class for domain services.
    
    Domain services are stateless and contain business logic that
    doesn't naturally fit within entities or value objects.
    """
    pass


class OHLCVCalculationService(DomainService):
    """Service for performing OHLCV calculations and aggregations.
    
    This service provides business logic for calculating various
    financial metrics and aggregating OHLCV data across timeframes.
    """
    
    def aggregate_bars_to_timeframe(
        self,
        bars: List[OHLCVBar],
        timeframe_minutes: int
    ) -> List[OHLCVBar]:
        """Aggregate minute bars to larger timeframes (5min, 15min, etc.).
        
        Args:
            bars: List of minute-level OHLCV bars (must be sorted by timestamp)
            timeframe_minutes: Target timeframe in minutes (5, 15, 30, 60, etc.)
            
        Returns:
            List of aggregated bars for the specified timeframe
            
        Raises:
            ValueError: If timeframe is invalid or bars are not properly sorted
        """
        if timeframe_minutes <= 0:
            raise ValueError("Timeframe must be positive")
        
        if not bars:
            return []
        
        # Validate bars are sorted and from same symbol
        symbol = bars[0].symbol
        for i, bar in enumerate(bars):
            if bar.symbol != symbol:
                raise ValueError(f"All bars must be for same symbol. Found {bar.symbol}, expected {symbol}")
            if i > 0 and bar.timestamp.value <= bars[i-1].timestamp.value:
                raise ValueError("Bars must be sorted by timestamp")
        
        aggregated_bars = []
        current_group = []
        current_period_start = None
        
        for bar in bars:
            # Calculate the period start for this bar
            bar_timestamp = bar.timestamp.value
            period_start = self._calculate_period_start(bar_timestamp, timeframe_minutes)
            
            # If this is a new period, process the current group
            if current_period_start is not None and period_start != current_period_start:
                if current_group:
                    aggregated_bar = self._aggregate_bar_group(current_group, timeframe_minutes)
                    aggregated_bars.append(aggregated_bar)
                current_group = []
            
            current_period_start = period_start
            current_group.append(bar)
        
        # Process the final group
        if current_group:
            aggregated_bar = self._aggregate_bar_group(current_group, timeframe_minutes)
            aggregated_bars.append(aggregated_bar)
        
        return aggregated_bars
    
    def _calculate_period_start(self, timestamp: datetime, timeframe_minutes: int) -> datetime:
        """Calculate the start of the aggregation period for a timestamp."""
        # Round down to the nearest timeframe boundary
        minutes_since_midnight = timestamp.hour * 60 + timestamp.minute
        period_minutes = (minutes_since_midnight // timeframe_minutes) * timeframe_minutes
        
        return timestamp.replace(
            hour=period_minutes // 60,
            minute=period_minutes % 60,
            second=0,
            microsecond=0
        )
    
    def _aggregate_bar_group(self, bars: List[OHLCVBar], timeframe_minutes: int) -> OHLCVBar:
        """Aggregate a group of bars into a single bar."""
        if not bars:
            raise ValueError("Cannot aggregate empty group")
        
        # Use first bar's metadata
        first_bar = bars[0]
        last_bar = bars[-1]
        
        # Calculate aggregated values
        open_price = first_bar.open_price
        close_price = last_bar.close_price
        high_price = max(bar.high_price for bar in bars)
        low_price = min(bar.low_price for bar in bars)
        total_volume = sum(bar.volume for bar in bars)
        
        # Calculate total trade count if available
        trade_count = None
        if all(bar.trade_count is not None for bar in bars):
            trade_count = sum(bar.trade_count for bar in bars)
        
        # Calculate volume-weighted average price if available
        vwap = None
        if all(bar.vwap is not None for bar in bars):
            total_value = sum(bar.vwap * bar.volume.value for bar in bars)
            if total_volume.value > 0:
                vwap = Price(total_value.value / total_volume.value)
        
        # Use the period start timestamp
        period_start = self._calculate_period_start(first_bar.timestamp.value, timeframe_minutes)
        
        from .entities import EntityId
        return OHLCVBar(
            id=EntityId.generate(),
            symbol=first_bar.symbol,
            timestamp=Timestamp(period_start),
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=total_volume,
            trade_count=trade_count,
            vwap=vwap
        )
    
    def calculate_sma(self, bars: List[OHLCVBar], period: int, price_type: str = 'close') -> List[float]:
        """Calculate Simple Moving Average for a series of bars.
        
        Args:
            bars: List of OHLCV bars (must be sorted by timestamp)
            period: Number of periods for the moving average
            price_type: Which price to use ('open', 'high', 'low', 'close')
            
        Returns:
            List of SMA values (None for periods with insufficient data)
        """
        if period <= 0:
            raise ValueError("Period must be positive")
        
        if price_type not in ['open', 'high', 'low', 'close']:
            raise ValueError("price_type must be one of: open, high, low, close")
        
        sma_values = []
        prices = []
        
        for bar in bars:
            # Get the price based on type
            if price_type == 'open':
                price = bar.open_price.to_float()
            elif price_type == 'high':
                price = bar.high_price.to_float()
            elif price_type == 'low':
                price = bar.low_price.to_float()
            else:  # close
                price = bar.close_price.to_float()
            
            prices.append(price)
            
            # Calculate SMA if we have enough data
            if len(prices) >= period:
                sma = sum(prices[-period:]) / period
                sma_values.append(sma)
            else:
                sma_values.append(None)
        
        return sma_values
    
    def calculate_volatility(self, bars: List[OHLCVBar], period: int) -> List[Optional[float]]:
        """Calculate rolling volatility (standard deviation of returns).
        
        Args:
            bars: List of OHLCV bars (must be sorted by timestamp)
            period: Number of periods for volatility calculation
            
        Returns:
            List of volatility values (None for periods with insufficient data)
        """
        if period <= 1:
            raise ValueError("Period must be greater than 1")
        
        volatility_values = []
        returns = []
        
        prev_close = None
        for bar in bars:
            current_close = bar.close_price.to_float()
            
            if prev_close is not None:
                # Calculate return as ln(current/previous)
                import math
                return_value = math.log(current_close / prev_close)
                returns.append(return_value)
                
                # Calculate volatility if we have enough data
                if len(returns) >= period:
                    recent_returns = returns[-period:]
                    mean_return = sum(recent_returns) / len(recent_returns)
                    variance = sum((r - mean_return) ** 2 for r in recent_returns) / (len(recent_returns) - 1)
                    volatility = math.sqrt(variance)
                    volatility_values.append(volatility)
                else:
                    volatility_values.append(None)
            else:
                volatility_values.append(None)
            
            prev_close = current_close
        
        return volatility_values


class MarketDataValidationService(DomainService):
    """Service for validating market data business rules.
    
    This service contains business logic for validating OHLCV data
    beyond basic schema validation, including market-specific rules
    and cross-validation between related data points.
    """
    
    def validate_trading_hours(self, bar: OHLCVBar) -> List[str]:
        """Validate that bar timestamp is within reasonable trading hours.
        
        Args:
            bar: OHLCV bar to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Basic trading hours check (simplified)
        if not bar.is_during_market_hours():
            errors.append(f"Bar timestamp {bar.timestamp} is outside regular trading hours")
        
        return errors
    
    def validate_price_movements(self, current_bar: OHLCVBar, previous_bar: Optional[OHLCVBar]) -> List[str]:
        """Validate price movements for reasonableness.
        
        Args:
            current_bar: Current OHLCV bar
            previous_bar: Previous OHLCV bar (if available)
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        if previous_bar is None:
            return errors
        
        # Check for extreme price movements (>50% in one minute)
        prev_close = previous_bar.close_price.to_float()
        curr_open = current_bar.open_price.to_float()
        
        if prev_close > 0:
            price_change_pct = abs(curr_open - prev_close) / prev_close
            if price_change_pct > 0.5:  # 50% change
                errors.append(
                    f"Extreme price movement: {price_change_pct*100:.1f}% change "
                    f"from {prev_close} to {curr_open}"
                )
        
        # Check for zero volume with non-zero price movement
        if (current_bar.volume.value == 0 and 
            current_bar.open_price != current_bar.close_price):
            errors.append("Non-zero price movement with zero volume")
        
        return errors
    
    def validate_volume_patterns(self, bars: List[OHLCVBar]) -> List[str]:
        """Validate volume patterns across multiple bars.
        
        Args:
            bars: List of consecutive OHLCV bars
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        if len(bars) < 2:
            return errors
        
        # Check for sustained zero volume (suspicious)
        zero_volume_count = sum(1 for bar in bars[-10:] if bar.volume.value == 0)
        if zero_volume_count >= 5:
            errors.append(f"Suspicious: {zero_volume_count} bars with zero volume in recent history")
        
        # Check for extreme volume spikes
        volumes = [bar.volume.value for bar in bars[-20:] if bar.volume.value > 0]
        if len(volumes) >= 10:
            avg_volume = sum(volumes) / len(volumes)
            current_volume = bars[-1].volume.value
            
            if current_volume > avg_volume * 10:  # 10x average
                errors.append(f"Extreme volume spike: {current_volume} vs avg {avg_volume:.0f}")
        
        return errors


class TradingCalendarService(DomainService):
    """Service for trading calendar and market hours logic.
    
    This service provides business logic for determining market
    open/close times, trading days, and holiday schedules.
    """
    
    def is_trading_day(self, date: date) -> bool:
        """Check if a date is a trading day.
        
        Args:
            date: Date to check
            
        Returns:
            True if date is a trading day
            
        Note:
            This is a simplified implementation. Real implementation
            would consult a proper trading calendar with holidays.
        """
        # Simplified: weekdays only
        return date.weekday() < 5
    
    def get_trading_session_times(self, trading_date: date) -> Dict[str, datetime]:
        """Get trading session times for a specific date.
        
        Args:
            trading_date: The trading date
            
        Returns:
            Dictionary with session start/end times
        """
        # Simplified US market hours (doesn't handle DST properly)
        from datetime import time, timezone, timedelta
        
        # Eastern Time approximation (UTC-5, doesn't handle DST)
        et_offset = timedelta(hours=-5)
        et_tz = timezone(et_offset)
        
        return {
            'pre_market_open': datetime.combine(trading_date, time(4, 0), et_tz),
            'regular_open': datetime.combine(trading_date, time(9, 30), et_tz),
            'regular_close': datetime.combine(trading_date, time(16, 0), et_tz),
            'post_market_close': datetime.combine(trading_date, time(20, 0), et_tz),
        }
    
    def get_next_trading_day(self, current_date: date) -> date:
        """Get the next trading day after the given date.
        
        Args:
            current_date: Starting date
            
        Returns:
            Next trading day
        """
        from datetime import timedelta
        
        next_date = current_date + timedelta(days=1)
        while not self.is_trading_day(next_date):
            next_date += timedelta(days=1)
        
        return next_date
    
    def get_previous_trading_day(self, current_date: date) -> date:
        """Get the previous trading day before the given date.
        
        Args:
            current_date: Starting date
            
        Returns:
            Previous trading day
        """
        from datetime import timedelta
        
        prev_date = current_date - timedelta(days=1)
        while not self.is_trading_day(prev_date):
            prev_date -= timedelta(days=1)
        
        return prev_date