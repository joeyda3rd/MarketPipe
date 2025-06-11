# SPDX-License-Identifier: Apache-2.0
"""Validation domain services."""

from __future__ import annotations

from marketpipe.domain.entities import OHLCVBar
from .value_objects import ValidationResult, BarError


class ValidationDomainService:
    """Domain service for validating OHLCV bars."""
    
    def validate_bars(self, symbol: str, bars: list[OHLCVBar]) -> ValidationResult:
        """Validate a collection of OHLCV bars for a symbol."""
        errors = []
        prev_ts = -1
        prev_bar = None
        
        for i, bar in enumerate(bars):
            # Check for monotonic timestamps
            if bar.timestamp_ns <= prev_ts:
                errors.append(BarError(bar.timestamp_ns, f"non-monotonic timestamp at index {i}"))
            
            # Check for positive prices
            if (bar.open_price.value <= 0 or bar.high_price.value <= 0 or 
                bar.low_price.value <= 0 or bar.close_price.value <= 0):
                errors.append(BarError(bar.timestamp_ns, f"non-positive price at index {i}"))
            
            # Check for non-negative volume
            if bar.volume.value < 0:
                errors.append(BarError(bar.timestamp_ns, f"negative volume at index {i}"))
            
            # OHLC consistency validation (high >= open,close,low; low <= open,close)
            if not self._validate_ohlc_consistency(bar):
                errors.append(BarError(bar.timestamp_ns, f"OHLC inconsistency at index {i}"))
            
            # Timestamp alignment (1-minute bars should align to minute boundaries)
            if not self._validate_timestamp_alignment(bar):
                errors.append(BarError(bar.timestamp_ns, f"timestamp not aligned to minute boundary at index {i}"))
            
            # Check for zero volume with non-zero price movement (individual bar check)
            if (bar.volume.value == 0 and 
                bar.open_price.value != bar.close_price.value):
                errors.append(BarError(
                    bar.timestamp_ns,
                    f"non-zero price movement with zero volume at index {i}"
                ))
            
            # Price movement validation (if we have previous bar)
            if prev_bar is not None:
                price_errors = self._validate_price_movements(bar, prev_bar, i)
                errors.extend(price_errors)
            
            # Volume pattern validation
            volume_errors = self._validate_volume_patterns(bar, i)
            errors.extend(volume_errors)
            
            prev_ts = bar.timestamp_ns
            prev_bar = bar
        
        return ValidationResult(symbol, len(bars), errors)
    
    def _validate_ohlc_consistency(self, bar: OHLCVBar) -> bool:
        """Validate OHLC price relationships."""
        return (bar.high_price.value >= bar.open_price.value and 
                bar.high_price.value >= bar.close_price.value and
                bar.high_price.value >= bar.low_price.value and
                bar.low_price.value <= bar.open_price.value and
                bar.low_price.value <= bar.close_price.value)
    
    def _validate_timestamp_alignment(self, bar: OHLCVBar) -> bool:
        """Validate that timestamp aligns to minute boundaries."""
        # 1-minute bars should align to minute boundaries (60 seconds in nanoseconds)
        return bar.timestamp_ns % 60_000_000_000 == 0
    
    def _validate_price_movements(self, current_bar: OHLCVBar, previous_bar: OHLCVBar, index: int) -> list[BarError]:
        """Validate price movements between consecutive bars."""
        errors = []
        
        # Check for extreme price movements (>50% in one minute)
        prev_close = previous_bar.close_price.value
        curr_open = current_bar.open_price.value
        
        if prev_close > 0:
            price_change_pct = abs(float(curr_open - prev_close)) / float(prev_close)
            if price_change_pct > 0.5:  # 50% change
                errors.append(BarError(
                    current_bar.timestamp_ns, 
                    f"extreme price movement at index {index}: {price_change_pct*100:.1f}% change"
                ))
        
        return errors
    
    def _validate_volume_patterns(self, bar: OHLCVBar, index: int) -> list[BarError]:
        """Validate volume patterns for individual bar."""
        errors = []
        
        # Check for unreasonably high volume (basic sanity check)
        if bar.volume.value > 1_000_000_000:  # 1 billion shares
            errors.append(BarError(
                bar.timestamp_ns,
                f"unreasonably high volume at index {index}: {bar.volume.value}"
            ))
        
        return errors
    
    def validate_trading_hours(self, bar: OHLCVBar) -> list[BarError]:
        """Validate that bar timestamp is within reasonable trading hours."""
        errors = []
        
        if not bar.is_during_market_hours():
            errors.append(BarError(
                bar.timestamp_ns,
                f"timestamp {bar.timestamp} is outside regular trading hours"
            ))
        
        return errors
    
    def validate_price_reasonableness(self, bar: OHLCVBar, symbol: str) -> list[BarError]:
        """Validate that prices are reasonable for the given symbol."""
        errors = []
        
        # Basic sanity checks for price ranges
        prices = [bar.open_price.value, bar.high_price.value, bar.low_price.value, bar.close_price.value]
        
        # Check for extremely high prices (>$100,000)
        if any(float(p) > 100_000 for p in prices):
            errors.append(BarError(
                bar.timestamp_ns,
                f"unreasonably high price for {symbol}: max={max(float(p) for p in prices)}"
            ))
        
        # Check for extremely low prices (<$0.01 for most stocks)
        if any(float(p) < 0.01 for p in prices):
            errors.append(BarError(
                bar.timestamp_ns,
                f"unreasonably low price for {symbol}: min={min(float(p) for p in prices)}"
            ))
        
        return errors 