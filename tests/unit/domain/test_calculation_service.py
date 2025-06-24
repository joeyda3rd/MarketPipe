# SPDX-License-Identifier: Apache-2.0
"""Unit tests for OHLCVCalculationService."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.marketpipe.domain.aggregates import DailySummary
from src.marketpipe.domain.entities import EntityId, OHLCVBar
from src.marketpipe.domain.services import OHLCVCalculationService
from src.marketpipe.domain.value_objects import Price, Symbol, Timestamp, Volume


@pytest.fixture
def service():
    """Test calculation service."""
    return OHLCVCalculationService()


@pytest.fixture
def symbol():
    """Test symbol."""
    return Symbol("AAPL")


@pytest.fixture
def sample_bars(symbol):
    """Sample OHLCV bars for testing."""
    bars = []
    base_time = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)

    # Create 5 minute bars with different prices and volumes
    for i in range(5):
        bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(base_time.replace(minute=30 + i)),
            open_price=Price(Decimal("100.00") + Decimal(str(i))),
            high_price=Price(Decimal("101.00") + Decimal(str(i))),
            low_price=Price(Decimal("99.00") + Decimal(str(i))),
            close_price=Price(Decimal("100.50") + Decimal(str(i))),
            volume=Volume(1000 * (i + 1)),  # Increasing volume
            trade_count=50 * (i + 1),
            vwap=Price(Decimal("100.25") + Decimal(str(i))),
        )
        bars.append(bar)

    return bars


class TestOHLCVCalculationService:
    """Test cases for OHLCVCalculationService."""

    def test_vwap_calculation_with_vwap_data(self, service, sample_bars):
        """Test VWAP calculation when bars have VWAP data."""
        # Expected VWAP calculation:
        # Bar 0: 100.25 * 1000 = 100,250
        # Bar 1: 101.25 * 2000 = 202,500
        # Bar 2: 102.25 * 3000 = 306,750
        # Bar 3: 103.25 * 4000 = 413,000
        # Bar 4: 104.25 * 5000 = 521,250
        # Total value: 1,543,750, Total volume: 15,000
        # VWAP: 1,543,750 / 15,000 = 102.916666...

        vwap = service.vwap(sample_bars)
        expected_vwap = Decimal("102.9166666666666666666666667")
        assert abs(vwap - expected_vwap) < Decimal("0.0001")

    def test_vwap_calculation_without_vwap_data(self, service, symbol):
        """Test VWAP calculation when bars don't have VWAP data (uses typical price)."""
        bars = [
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)),
                open_price=Price(Decimal("100.00")),
                high_price=Price(Decimal("102.00")),
                low_price=Price(Decimal("98.00")),
                close_price=Price(Decimal("101.00")),
                volume=Volume(1000),
                vwap=None,  # No VWAP data
            ),
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(datetime(2024, 1, 15, 9, 31, 0, tzinfo=timezone.utc)),
                open_price=Price(Decimal("101.00")),
                high_price=Price(Decimal("104.00")),
                low_price=Price(Decimal("100.00")),
                close_price=Price(Decimal("103.00")),
                volume=Volume(2000),
                vwap=None,  # No VWAP data
            ),
        ]

        vwap = service.vwap(bars)

        # Expected calculation using typical price (H+L+C)/3:
        # Bar 0: (102+98+101)/3 = 100.33, volume 1000 -> 100,333.33
        # Bar 1: (104+100+103)/3 = 102.33, volume 2000 -> 204,666.67
        # Total: 305,000, Volume: 3000, VWAP: 101.67
        expected_vwap = Decimal("101.6666666666666666666666667")
        assert abs(vwap - expected_vwap) < Decimal("0.0001")

    def test_vwap_with_zero_volume_bars(self, service, symbol):
        """Test VWAP calculation skips zero volume bars."""
        bars = [
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)),
                open_price=Price(Decimal("100.00")),
                high_price=Price(Decimal("100.00")),
                low_price=Price(Decimal("100.00")),
                close_price=Price(Decimal("100.00")),
                volume=Volume(0),  # Zero volume
                vwap=Price(Decimal("100.00")),
            ),
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(datetime(2024, 1, 15, 9, 31, 0, tzinfo=timezone.utc)),
                open_price=Price(Decimal("200.00")),
                high_price=Price(Decimal("200.00")),
                low_price=Price(Decimal("200.00")),
                close_price=Price(Decimal("200.00")),
                volume=Volume(1000),
                vwap=Price(Decimal("200.00")),
            ),
        ]

        vwap = service.vwap(bars)
        assert vwap == Decimal("200.00")  # Should only use the non-zero volume bar

    def test_vwap_with_empty_bars_raises_error(self, service):
        """Test VWAP calculation with empty bars raises error."""
        with pytest.raises(ValueError, match="Cannot calculate VWAP with no bars"):
            service.vwap([])

    def test_vwap_with_all_zero_volume_raises_error(self, service, symbol):
        """Test VWAP calculation with all zero volume bars raises error."""
        bars = [
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)),
                open_price=Price(Decimal("100.00")),
                high_price=Price(Decimal("100.00")),
                low_price=Price(Decimal("100.00")),
                close_price=Price(Decimal("100.00")),
                volume=Volume(0),
                vwap=Price(Decimal("100.00")),
            )
        ]

        with pytest.raises(ValueError, match="Cannot calculate VWAP: no volume data"):
            service.vwap(bars)

    def test_daily_summary_calculation(self, service, sample_bars):
        """Test daily summary calculation."""
        summary = service.daily_summary(sample_bars)

        assert isinstance(summary, DailySummary)
        assert summary.symbol == sample_bars[0].symbol
        assert summary.trading_date == sample_bars[0].timestamp.trading_date()
        assert summary.open_price == sample_bars[0].open_price  # First bar open
        assert summary.close_price == sample_bars[-1].close_price  # Last bar close
        assert summary.high_price == Price(Decimal("105.00"))  # Max high
        assert summary.low_price == Price(Decimal("99.00"))  # Min low
        assert summary.volume.value == 15000  # Sum of all volumes
        assert summary.bar_count == 5
        assert summary.vwap is not None

    def test_daily_summary_with_empty_bars_raises_error(self, service):
        """Test daily summary with empty bars raises error."""
        with pytest.raises(ValueError, match="Cannot calculate daily summary with no bars"):
            service.daily_summary([])

    def test_daily_summary_with_mixed_symbols_raises_error(self, service, sample_bars):
        """Test daily summary with mixed symbols raises error."""
        # Change symbol of last bar
        sample_bars[-1] = OHLCVBar(
            id=sample_bars[-1].id,
            symbol=Symbol("GOOGL"),  # Different symbol
            timestamp=sample_bars[-1].timestamp,
            open_price=sample_bars[-1].open_price,
            high_price=sample_bars[-1].high_price,
            low_price=sample_bars[-1].low_price,
            close_price=sample_bars[-1].close_price,
            volume=sample_bars[-1].volume,
        )

        with pytest.raises(ValueError, match="All bars must be for same symbol"):
            service.daily_summary(sample_bars)

    def test_daily_summary_with_mixed_dates_raises_error(self, service, sample_bars):
        """Test daily summary with mixed trading dates raises error."""
        # Change date of last bar
        sample_bars[-1] = OHLCVBar(
            id=sample_bars[-1].id,
            symbol=sample_bars[-1].symbol,
            timestamp=Timestamp(
                datetime(2024, 1, 16, 9, 34, 0, tzinfo=timezone.utc)
            ),  # Different date
            open_price=sample_bars[-1].open_price,
            high_price=sample_bars[-1].high_price,
            low_price=sample_bars[-1].low_price,
            close_price=sample_bars[-1].close_price,
            volume=sample_bars[-1].volume,
        )

        with pytest.raises(ValueError, match="All bars must be from same trading date"):
            service.daily_summary(sample_bars)

    def test_resample_to_5_minute_bars(self, service, symbol):
        """Test resampling 1-minute bars to 5-minute bars."""
        # Create 10 minute bars (2 complete 5-minute periods)
        bars = []
        base_time = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)

        for i in range(10):
            bar = OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(base_time.replace(minute=30 + i)),
                open_price=Price(Decimal("100.00") + Decimal(str(i))),
                high_price=Price(Decimal("101.00") + Decimal(str(i))),
                low_price=Price(Decimal("99.00") + Decimal(str(i))),
                close_price=Price(Decimal("100.50") + Decimal(str(i))),
                volume=Volume(1000),
            )
            bars.append(bar)

        # Resample to 5-minute bars (300 seconds)
        resampled = service.resample(bars, 300)

        assert len(resampled) == 2  # Should create 2 five-minute bars

        # First 5-minute bar (minutes 30-34)
        first_bar = resampled[0]
        assert first_bar.open_price == bars[0].open_price  # First bar's open
        assert first_bar.close_price == bars[4].close_price  # Fifth bar's close
        assert first_bar.high_price == Price(Decimal("105.00"))  # Max high from first 5 bars
        assert first_bar.low_price == Price(Decimal("99.00"))  # Min low from first 5 bars
        assert first_bar.volume.value == 5000  # Sum of first 5 volumes

        # Second 5-minute bar (minutes 35-39)
        second_bar = resampled[1]
        assert second_bar.open_price == bars[5].open_price  # Sixth bar's open
        assert second_bar.close_price == bars[9].close_price  # Tenth bar's close
        assert second_bar.volume.value == 5000  # Sum of last 5 volumes

    def test_resample_with_invalid_frame_seconds_raises_error(self, service, sample_bars):
        """Test resample with invalid frame_seconds raises error."""
        with pytest.raises(ValueError, match="frame_seconds must be positive"):
            service.resample(sample_bars, 0)

        with pytest.raises(ValueError, match="frame_seconds must be positive"):
            service.resample(sample_bars, -300)

    def test_resample_with_empty_bars_returns_empty(self, service):
        """Test resample with empty bars returns empty list."""
        result = service.resample([], 300)
        assert result == []

    def test_resample_with_unsorted_bars_raises_error(self, service, symbol):
        """Test resample with unsorted bars raises error."""
        bars = [
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(
                    datetime(2024, 1, 15, 9, 31, 0, tzinfo=timezone.utc)
                ),  # Later time
                open_price=Price(Decimal("100.00")),
                high_price=Price(Decimal("100.00")),
                low_price=Price(Decimal("100.00")),
                close_price=Price(Decimal("100.00")),
                volume=Volume(1000),
            ),
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(
                    datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
                ),  # Earlier time
                open_price=Price(Decimal("100.00")),
                high_price=Price(Decimal("100.00")),
                low_price=Price(Decimal("100.00")),
                close_price=Price(Decimal("100.00")),
                volume=Volume(1000),
            ),
        ]

        with pytest.raises(ValueError, match="Bars must be sorted by timestamp"):
            service.resample(bars, 300)

    def test_resample_with_mixed_symbols_raises_error(self, service, sample_bars):
        """Test resample with mixed symbols raises error."""
        # Change symbol of last bar
        sample_bars[-1] = OHLCVBar(
            id=sample_bars[-1].id,
            symbol=Symbol("GOOGL"),  # Different symbol
            timestamp=sample_bars[-1].timestamp,
            open_price=sample_bars[-1].open_price,
            high_price=sample_bars[-1].high_price,
            low_price=sample_bars[-1].low_price,
            close_price=sample_bars[-1].close_price,
            volume=sample_bars[-1].volume,
        )

        with pytest.raises(ValueError, match="All bars must be for same symbol"):
            service.resample(sample_bars, 300)

    def test_aggregate_bars_to_timeframe_delegates_to_resample(self, service, sample_bars):
        """Test that aggregate_bars_to_timeframe delegates to resample method."""
        # Test 5-minute aggregation (5 * 60 = 300 seconds)
        result = service.aggregate_bars_to_timeframe(sample_bars, 5)

        # Should produce same result as resample with 300 seconds
        expected = service.resample(sample_bars, 300)

        assert len(result) == len(expected)
        if result:  # If there are results
            assert result[0].timestamp == expected[0].timestamp
            assert result[0].volume == expected[0].volume

    def test_resample_vwap_calculation(self, service, symbol):
        """Test that resampling correctly calculates VWAP for grouped bars."""
        # Create bars with known VWAP values
        bars = [
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)),
                open_price=Price(Decimal("100.00")),
                high_price=Price(Decimal("100.00")),
                low_price=Price(Decimal("100.00")),
                close_price=Price(Decimal("100.00")),
                volume=Volume(1000),
                vwap=Price(Decimal("100.00")),
            ),
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(datetime(2024, 1, 15, 9, 31, 0, tzinfo=timezone.utc)),
                open_price=Price(Decimal("200.00")),
                high_price=Price(Decimal("200.00")),
                low_price=Price(Decimal("200.00")),
                close_price=Price(Decimal("200.00")),
                volume=Volume(2000),
                vwap=Price(Decimal("200.00")),
            ),
        ]

        # Resample to 5-minute bars
        resampled = service.resample(bars, 300)

        assert len(resampled) == 1

        # Check VWAP calculation: (100*1000 + 200*2000) / (1000+2000) = 166.67
        expected_vwap = Decimal("166.6666666666666666666666667")
        assert abs(resampled[0].vwap.value - expected_vwap) < Decimal("0.01")
