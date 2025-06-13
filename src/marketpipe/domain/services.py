# SPDX-License-Identifier: Apache-2.0
"""Domain services for MarketPipe.

Domain services contain business logic that doesn't naturally belong
to any single entity or value object. They coordinate operations
across multiple domain objects or provide stateless business operations.
"""

from __future__ import annotations

from abc import ABC
from typing import List, Optional, Dict, Iterable
from datetime import date, datetime
from decimal import Decimal

from .entities import OHLCVBar
from .value_objects import Price, Volume, Timestamp
from .aggregates import DailySummary


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

    def vwap(self, bars: Iterable[OHLCVBar]) -> Decimal:
        """Calculate Volume Weighted Average Price (VWAP) for a series of bars.

        Args:
            bars: Iterable of OHLCV bars

        Returns:
            VWAP as Decimal

        Raises:
            ValueError: If no bars provided or no volume data
        """
        bars_list = list(bars)
        if not bars_list:
            raise ValueError("Cannot calculate VWAP with no bars")

        total_value = Decimal("0")
        total_volume = Decimal("0")

        for bar in bars_list:
            if bar.volume.value <= 0:
                continue  # Skip bars with no volume

            # Use typical price (H+L+C)/3 if VWAP not available, otherwise use close
            if bar.vwap is not None:
                price = bar.vwap.value
            else:
                price = (
                    bar.high_price.value + bar.low_price.value + bar.close_price.value
                ) / Decimal("3")

            volume = Decimal(str(bar.volume.value))
            total_value += price * volume
            total_volume += volume

        if total_volume == 0:
            raise ValueError("Cannot calculate VWAP: no volume data")

        return total_value / total_volume

    def daily_summary(self, bars: Iterable[OHLCVBar]) -> DailySummary:
        """Calculate daily summary from intraday bars.

        Args:
            bars: Iterable of OHLCV bars for a single trading day

        Returns:
            DailySummary calculated from the bars

        Raises:
            ValueError: If no bars provided or bars span multiple days
        """
        bars_list = sorted(list(bars), key=lambda b: b.timestamp.value)
        if not bars_list:
            raise ValueError("Cannot calculate daily summary with no bars")

        # Validate all bars are from same symbol and trading date
        symbol = bars_list[0].symbol
        trading_date = bars_list[0].timestamp.trading_date()

        for bar in bars_list:
            if bar.symbol != symbol:
                raise ValueError(
                    f"All bars must be for same symbol. Found {bar.symbol}, expected {symbol}"
                )
            if bar.timestamp.trading_date() != trading_date:
                raise ValueError(
                    f"All bars must be from same trading date. Found {bar.timestamp.trading_date()}, expected {trading_date}"
                )

        # Calculate aggregated values
        first_bar = bars_list[0]
        last_bar = bars_list[-1]

        open_price = first_bar.open_price
        close_price = last_bar.close_price
        high_price = max(bar.high_price for bar in bars_list)
        low_price = min(bar.low_price for bar in bars_list)
        total_volume = Volume(sum(bar.volume.value for bar in bars_list))

        # Calculate VWAP
        vwap = None
        try:
            vwap_value = self.vwap(bars_list)
            vwap = Price(vwap_value)
        except ValueError:
            # No volume data for VWAP calculation
            pass

        return DailySummary(
            symbol=symbol,
            trading_date=trading_date,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=total_volume,
            vwap=vwap,
            bar_count=len(bars_list),
            first_bar_time=first_bar.timestamp,
            last_bar_time=last_bar.timestamp,
        )

    def resample(self, bars: Iterable[OHLCVBar], frame_seconds: int) -> List[OHLCVBar]:
        """Resample bars to a different timeframe.

        Args:
            bars: Iterable of OHLCV bars (must be sorted by timestamp)
            frame_seconds: Target timeframe in seconds (300 for 5min, 900 for 15min, etc.)

        Returns:
            List of resampled bars

        Raises:
            ValueError: If frame_seconds is invalid or bars are not sorted
        """
        if frame_seconds <= 0:
            raise ValueError("frame_seconds must be positive")

        bars_list = list(bars)
        if not bars_list:
            return []

        # Validate bars are sorted and from same symbol
        symbol = bars_list[0].symbol
        for i, bar in enumerate(bars_list):
            if bar.symbol != symbol:
                raise ValueError(
                    f"All bars must be for same symbol. Found {bar.symbol}, expected {symbol}"
                )
            if i > 0 and bar.timestamp.value <= bars_list[i - 1].timestamp.value:
                raise ValueError("Bars must be sorted by timestamp")

        resampled_bars = []
        current_group = []
        current_period_start = None

        for bar in bars_list:
            # Calculate the period start for this bar (align to timeframe boundaries)
            bar_timestamp = bar.timestamp.value
            seconds_since_midnight = (
                bar_timestamp.hour * 3600
                + bar_timestamp.minute * 60
                + bar_timestamp.second
            )
            period_seconds = (seconds_since_midnight // frame_seconds) * frame_seconds
            period_start = bar_timestamp.replace(
                hour=period_seconds // 3600,
                minute=(period_seconds % 3600) // 60,
                second=period_seconds % 60,
                microsecond=0,
            )

            # If this is a new period, process the current group
            if (
                current_period_start is not None
                and period_start != current_period_start
            ):
                if current_group:
                    resampled_bar = self._resample_bar_group(
                        current_group, period_start
                    )
                    resampled_bars.append(resampled_bar)
                current_group = []

            current_period_start = period_start
            current_group.append(bar)

        # Process the final group
        if current_group:
            period_start = current_period_start or current_group[0].timestamp.value
            resampled_bar = self._resample_bar_group(current_group, period_start)
            resampled_bars.append(resampled_bar)

        return resampled_bars

    def _resample_bar_group(
        self, bars: List[OHLCVBar], period_start: datetime
    ) -> OHLCVBar:
        """Resample a group of bars into a single bar.

        Args:
            bars: List of bars to resample
            period_start: Start timestamp for the resampled bar

        Returns:
            Resampled OHLCV bar
        """
        if not bars:
            raise ValueError("Cannot resample empty group")

        # Use first bar's metadata
        first_bar = bars[0]
        last_bar = bars[-1]

        # Calculate aggregated values
        open_price = first_bar.open_price
        close_price = last_bar.close_price
        high_price = max(bar.high_price for bar in bars)
        low_price = min(bar.low_price for bar in bars)
        total_volume = Volume(sum(bar.volume.value for bar in bars))

        # Calculate trade count if available
        trade_count = None
        if all(bar.trade_count is not None for bar in bars):
            trade_count = sum(bar.trade_count for bar in bars)

        # Calculate VWAP if available
        vwap = None
        try:
            vwap_value = self.vwap(bars)
            vwap = Price(vwap_value)
        except ValueError:
            # No volume data for VWAP calculation
            pass

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
            vwap=vwap,
        )

    def aggregate_bars_to_timeframe(
        self, bars: List[OHLCVBar], timeframe_minutes: int
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
        # Delegate to resample method with timeframe converted to seconds
        frame_seconds = timeframe_minutes * 60
        return self.resample(bars, frame_seconds)

    def calculate_sma(
        self, bars: List[OHLCVBar], period: int, price_type: str = "close"
    ) -> List[float]:
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

        if price_type not in ["open", "high", "low", "close"]:
            raise ValueError("price_type must be one of: open, high, low, close")

        sma_values = []
        prices = []

        for bar in bars:
            # Get the price based on type
            if price_type == "open":
                price = bar.open_price.to_float()
            elif price_type == "high":
                price = bar.high_price.to_float()
            elif price_type == "low":
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

    def calculate_volatility(
        self, bars: List[OHLCVBar], period: int
    ) -> List[Optional[float]]:
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
                    variance = sum((r - mean_return) ** 2 for r in recent_returns) / (
                        len(recent_returns) - 1
                    )
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

    def validate_bar(self, bar: OHLCVBar) -> List[str]:
        """Validate a single OHLCV bar against business rules.

        Args:
            bar: OHLCV bar to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        # Basic price validation
        if bar.open_price.value <= 0:
            errors.append(f"Open price must be positive, got {bar.open_price.value}")
        if bar.high_price.value <= 0:
            errors.append(f"High price must be positive, got {bar.high_price.value}")
        if bar.low_price.value <= 0:
            errors.append(f"Low price must be positive, got {bar.low_price.value}")
        if bar.close_price.value <= 0:
            errors.append(f"Close price must be positive, got {bar.close_price.value}")

        # Volume validation
        if bar.volume.value < 0:
            errors.append(f"Volume cannot be negative, got {bar.volume.value}")

        # OHLC consistency validation
        if not (
            bar.high_price >= bar.open_price
            and bar.high_price >= bar.close_price
            and bar.high_price >= bar.low_price
        ):
            errors.append(
                f"High price ({bar.high_price.value}) must be >= open ({bar.open_price.value}), close ({bar.close_price.value}), and low ({bar.low_price.value})"
            )

        if not (bar.low_price <= bar.open_price and bar.low_price <= bar.close_price):
            errors.append(
                f"Low price ({bar.low_price.value}) must be <= open ({bar.open_price.value}) and close ({bar.close_price.value})"
            )

        # Trading hours validation (optional, with default hours)
        trading_hour_errors = self._validate_trading_hours_window(bar)
        errors.extend(trading_hour_errors)

        return errors

    def validate_batch(self, bars: List[OHLCVBar]) -> List[str]:
        """Validate a batch of OHLCV bars for business rule compliance.

        Args:
            bars: List of OHLCV bars to validate

        Returns:
            List of aggregated validation error messages (empty if all valid)
        """
        all_errors = []

        if not bars:
            return all_errors

        # Validate individual bars
        for i, bar in enumerate(bars):
            bar_errors = self.validate_bar(bar)
            for error in bar_errors:
                all_errors.append(f"Bar {i} ({bar.timestamp}): {error}")

        # Validate timestamp monotonicity
        if len(bars) > 1:
            for i in range(1, len(bars)):
                if bars[i].timestamp.value <= bars[i - 1].timestamp.value:
                    all_errors.append(
                        f"Bar {i}: Timestamps must be monotonic. "
                        f"Bar {i-1} time {bars[i-1].timestamp.value} >= "
                        f"Bar {i} time {bars[i].timestamp.value}"
                    )

        # Validate price continuity (detect extreme gaps)
        for i in range(1, len(bars)):
            previous_bar = bars[i - 1]
            current_bar = bars[i]

            # Check for extreme price gaps
            price_movement_errors = self.validate_price_movements(
                current_bar, previous_bar
            )
            for error in price_movement_errors:
                all_errors.append(f"Bar {i}: {error}")

        # Validate volume patterns
        volume_errors = self.validate_volume_patterns(bars)
        for error in volume_errors:
            all_errors.append(f"Volume pattern: {error}")

        # Emit validation failed event if there are errors
        if all_errors:
            # Domain services should not emit events directly
            # Application layer should handle event emission based on validation results
            pass

        return all_errors

    def _validate_trading_hours_window(
        self,
        bar: OHLCVBar,
        start_hour: int = 9,
        start_minute: int = 30,
        end_hour: int = 16,
        end_minute: int = 0,
    ) -> List[str]:
        """Validate bar is within trading hours window.

        Args:
            bar: OHLCV bar to validate
            start_hour: Trading start hour (default 9 for 9:30 AM ET)
            start_minute: Trading start minute (default 30)
            end_hour: Trading end hour (default 16 for 4:00 PM ET)
            end_minute: Trading end minute (default 0)

        Returns:
            List of validation errors
        """
        errors = []

        # Convert to UTC and check if within trading hours
        # This is a simplified check - real implementation would need proper timezone handling
        bar_time = bar.timestamp.value
        bar_hour = bar_time.hour
        bar_minute = bar_time.minute

        # Convert times to minutes since midnight for easier comparison
        bar_minutes = bar_hour * 60 + bar_minute
        start_minutes = start_hour * 60 + start_minute
        end_minutes = end_hour * 60 + end_minute

        # Adjust for ET timezone (rough approximation - actual implementation would need proper timezone handling)
        # Assuming bar timestamp is in UTC, ET is typically UTC-5 or UTC-4
        et_offset = 5 * 60  # 5 hours in minutes
        bar_minutes_et = (bar_minutes - et_offset) % (24 * 60)

        if not (start_minutes <= bar_minutes_et <= end_minutes):
            errors.append(
                f"Bar timestamp appears to be outside regular trading hours "
                f"({start_hour:02d}:{start_minute:02d}-{end_hour:02d}:{end_minute:02d} ET)"
            )

        # Check if it's a weekend (simplified - real implementation would check trading calendar)
        if bar_time.weekday() >= 5:  # Saturday = 5, Sunday = 6
            errors.append(
                f"Bar timestamp is on weekend (trading day: {bar_time.strftime('%A')})"
            )

        return errors

    def validate_trading_hours(self, bar: OHLCVBar) -> List[str]:
        """Validate that bar timestamp is within reasonable trading hours.

        Args:
            bar: OHLCV bar to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        return self._validate_trading_hours_window(bar)

    def validate_price_movements(
        self, current_bar: OHLCVBar, previous_bar: Optional[OHLCVBar]
    ) -> List[str]:
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
        if (
            current_bar.volume.value == 0
            and current_bar.open_price != current_bar.close_price
        ):
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
            errors.append(
                f"Suspicious: {zero_volume_count} bars with zero volume in recent history"
            )

        # Check for extreme volume spikes
        volumes = [bar.volume.value for bar in bars[-20:] if bar.volume.value > 0]
        if len(volumes) >= 10:
            avg_volume = sum(volumes) / len(volumes)
            current_volume = bars[-1].volume.value

            if current_volume > avg_volume * 10:  # 10x average
                errors.append(
                    f"Extreme volume spike: {current_volume} vs avg {avg_volume:.0f}"
                )

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
            "pre_market_open": datetime.combine(trading_date, time(4, 0), et_tz),
            "regular_open": datetime.combine(trading_date, time(9, 30), et_tz),
            "regular_close": datetime.combine(trading_date, time(16, 0), et_tz),
            "post_market_close": datetime.combine(trading_date, time(20, 0), et_tz),
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
