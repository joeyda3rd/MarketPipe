# SPDX-License-Identifier: Apache-2.0
"""Unit tests for SymbolBarsAggregate."""

from __future__ import annotations

import pytest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.marketpipe.domain.aggregates import SymbolBarsAggregate, DailySummary
from src.marketpipe.domain.entities import OHLCVBar, EntityId
from src.marketpipe.domain.value_objects import Symbol, Timestamp, Price, Volume
from src.marketpipe.domain.events import (
    BarCollectionStarted,
    BarCollectionCompleted,
    MarketDataReceived
)


@pytest.fixture
def symbol():
    """Test symbol."""
    return Symbol("AAPL")


@pytest.fixture
def trading_date():
    """Test trading date."""
    return date(2024, 1, 15)


@pytest.fixture
def aggregate(symbol, trading_date):
    """Test aggregate."""
    return SymbolBarsAggregate(symbol, trading_date)


@pytest.fixture
def sample_bar(symbol):
    """Sample OHLCV bar."""
    return OHLCVBar(
        id=EntityId.generate(),
        symbol=symbol,
        timestamp=Timestamp(datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)),
        open_price=Price(Decimal("100.00")),
        high_price=Price(Decimal("101.00")),
        low_price=Price(Decimal("99.50")),
        close_price=Price(Decimal("100.50")),
        volume=Volume(1000),
        trade_count=50,
        vwap=Price(Decimal("100.25"))
    )


@pytest.fixture
def sample_bar_2(symbol):
    """Second sample OHLCV bar."""
    return OHLCVBar(
        id=EntityId.generate(),
        symbol=symbol,
        timestamp=Timestamp(datetime(2024, 1, 15, 9, 31, 0, tzinfo=timezone.utc)),
        open_price=Price(Decimal("100.50")),
        high_price=Price(Decimal("102.00")),
        low_price=Price(Decimal("100.00")),
        close_price=Price(Decimal("101.50")),
        volume=Volume(1500),
        trade_count=75,
        vwap=Price(Decimal("101.00"))
    )


class TestSymbolBarsAggregate:
    """Test cases for SymbolBarsAggregate."""
    
    def test_initialization(self, symbol, trading_date):
        """Test aggregate initialization."""
        aggregate = SymbolBarsAggregate(symbol, trading_date)
        
        assert aggregate.symbol == symbol
        assert aggregate.trading_date == trading_date
        assert aggregate.bar_count == 0
        assert not aggregate.is_complete
        assert aggregate.version == 1
        assert len(aggregate.get_uncommitted_events()) == 0
    
    def test_add_bar_starts_collection_and_emits_events(self, aggregate, sample_bar):
        """Test that adding first bar starts collection and emits events."""
        # Initially no events
        assert len(aggregate.get_uncommitted_events()) == 0
        
        # Add first bar
        aggregate.add_bar(sample_bar)
        
        # Check state
        assert aggregate.bar_count == 1
        assert aggregate.get_bar(sample_bar.timestamp) == sample_bar
        assert aggregate.version == 3  # +1 for start_collection, +1 for add_bar
        
        # Check events
        events = aggregate.get_uncommitted_events()
        assert len(events) == 2
        
        # First event should be BarCollectionStarted
        assert isinstance(events[0], BarCollectionStarted)
        assert events[0].symbol == sample_bar.symbol
        assert events[0].trading_date == aggregate.trading_date
        
        # Second event should be MarketDataReceived
        assert isinstance(events[1], MarketDataReceived)
        assert events[1].symbol == sample_bar.symbol
        assert events[1].timestamp == sample_bar.timestamp
        assert events[1].record_count == 1
    
    def test_add_bar_maintains_running_totals(self, aggregate, sample_bar, sample_bar_2):
        """Test that adding bars maintains running totals."""
        # Add first bar
        aggregate.add_bar(sample_bar)
        assert aggregate._running_high == sample_bar.high_price
        assert aggregate._running_low == sample_bar.low_price
        assert aggregate._running_volume.value == sample_bar.volume.value
        
        # Add second bar with higher high and lower low
        aggregate.add_bar(sample_bar_2)
        assert aggregate._running_high == sample_bar_2.high_price  # Higher
        assert aggregate._running_low == sample_bar.low_price      # Lower from first bar
        assert aggregate._running_volume.value == sample_bar.volume.value + sample_bar_2.volume.value
    
    def test_add_bar_duplicate_timestamp_raises_error(self, aggregate, sample_bar):
        """Test that adding bar with duplicate timestamp raises error."""
        aggregate.add_bar(sample_bar)
        
        # Create another bar with same timestamp
        duplicate_bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=sample_bar.symbol,
            timestamp=sample_bar.timestamp,  # Same timestamp
            open_price=Price(Decimal("200.00")),
            high_price=Price(Decimal("201.00")),
            low_price=Price(Decimal("199.00")),
            close_price=Price(Decimal("200.50")),
            volume=Volume(500)
        )
        
        with pytest.raises(ValueError, match="Bar already exists"):
            aggregate.add_bar(duplicate_bar)
    
    def test_add_bar_wrong_symbol_raises_error(self, aggregate, sample_bar):
        """Test that adding bar with wrong symbol raises error."""
        wrong_symbol_bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=Symbol("GOOGL"),  # Wrong symbol
            timestamp=sample_bar.timestamp,
            open_price=sample_bar.open_price,
            high_price=sample_bar.high_price,
            low_price=sample_bar.low_price,
            close_price=sample_bar.close_price,
            volume=sample_bar.volume
        )
        
        with pytest.raises(ValueError, match="Bar symbol .* doesn't match aggregate symbol"):
            aggregate.add_bar(wrong_symbol_bar)
    
    def test_add_bar_wrong_date_raises_error(self, aggregate, sample_bar):
        """Test that adding bar with wrong date raises error."""
        wrong_date_bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=sample_bar.symbol,
            timestamp=Timestamp(datetime(2024, 1, 16, 9, 30, 0, tzinfo=timezone.utc)),  # Wrong date
            open_price=sample_bar.open_price,
            high_price=sample_bar.high_price,
            low_price=sample_bar.low_price,
            close_price=sample_bar.close_price,
            volume=sample_bar.volume
        )
        
        with pytest.raises(ValueError, match="Bar date .* doesn't match aggregate date"):
            aggregate.add_bar(wrong_date_bar)
    
    def test_add_bar_after_completion_raises_error(self, aggregate, sample_bar):
        """Test that adding bar after completion raises error."""
        aggregate.add_bar(sample_bar)
        aggregate.complete_collection()
        
        new_bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=sample_bar.symbol,
            timestamp=Timestamp(datetime(2024, 1, 15, 9, 31, 0, tzinfo=timezone.utc)),
            open_price=Price(Decimal("101.00")),
            high_price=Price(Decimal("102.00")),
            low_price=Price(Decimal("100.50")),
            close_price=Price(Decimal("101.50")),
            volume=Volume(500)
        )
        
        with pytest.raises(ValueError, match="Cannot add bars to completed collection"):
            aggregate.add_bar(new_bar)
    
    def test_close_day_completes_and_returns_summary(self, aggregate, sample_bar, sample_bar_2):
        """Test that close_day completes collection and returns daily summary."""
        # Add bars
        aggregate.add_bar(sample_bar)
        aggregate.add_bar(sample_bar_2)
        
        # Close day
        summary = aggregate.close_day()
        
        # Check completion
        assert aggregate.is_complete
        
        # Check summary
        assert isinstance(summary, DailySummary)
        assert summary.symbol == aggregate.symbol
        assert summary.trading_date == aggregate.trading_date
        assert summary.open_price == sample_bar.open_price
        assert summary.close_price == sample_bar_2.close_price
        assert summary.high_price == sample_bar_2.high_price  # Higher of the two
        assert summary.low_price == sample_bar.low_price      # Lower of the two
        assert summary.volume.value == sample_bar.volume.value + sample_bar_2.volume.value
        assert summary.bar_count == 2
        assert summary.vwap is not None  # Should calculate VWAP
        
        # Check events include BarCollectionCompleted
        events = aggregate.get_uncommitted_events()
        completed_events = [e for e in events if isinstance(e, BarCollectionCompleted)]
        assert len(completed_events) == 1
        assert completed_events[0].bar_count == 2
    
    def test_close_day_with_no_bars_raises_error(self, aggregate):
        """Test that close_day with no bars raises error."""
        with pytest.raises(ValueError, match="Cannot close day with no bars"):
            aggregate.close_day()
    
    def test_calculate_daily_summary_without_completion(self, aggregate, sample_bar, sample_bar_2):
        """Test that calculate_daily_summary works without completing collection."""
        # Add bars
        aggregate.add_bar(sample_bar)
        aggregate.add_bar(sample_bar_2)
        
        # Calculate summary without closing day
        summary = aggregate.calculate_daily_summary()
        
        # Check that collection is NOT completed
        assert not aggregate.is_complete
        
        # Check summary is correct
        assert isinstance(summary, DailySummary)
        assert summary.symbol == aggregate.symbol
        assert summary.trading_date == aggregate.trading_date
        assert summary.bar_count == 2
        assert summary.vwap is not None
    
    def test_vwap_calculation_with_mixed_volume(self, aggregate, symbol):
        """Test VWAP calculation with bars having different volumes."""
        # Create bars with different volumes and prices
        bar1 = OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)),
            open_price=Price(Decimal("100.00")),
            high_price=Price(Decimal("100.00")),
            low_price=Price(Decimal("100.00")),
            close_price=Price(Decimal("100.00")),
            volume=Volume(1000),  # 1000 shares at $100
        )
        
        bar2 = OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(datetime(2024, 1, 15, 9, 31, 0, tzinfo=timezone.utc)),
            open_price=Price(Decimal("200.00")),
            high_price=Price(Decimal("200.00")),
            low_price=Price(Decimal("200.00")),
            close_price=Price(Decimal("200.00")),
            volume=Volume(2000),  # 2000 shares at $200
        )
        
        aggregate.add_bar(bar1)
        aggregate.add_bar(bar2)
        
        summary = aggregate.calculate_daily_summary()
        
        # Expected VWAP = (100*1000 + 200*2000) / (1000+2000) = 500000/3000 = 166.67
        expected_vwap = Decimal("166.6666666666666666666666667")
        assert abs(summary.vwap.value - expected_vwap) < Decimal("0.01")
    
    def test_vwap_calculation_with_zero_volume_bars(self, aggregate, symbol):
        """Test VWAP calculation skips zero volume bars."""
        # Create bar with zero volume
        zero_volume_bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)),
            open_price=Price(Decimal("100.00")),
            high_price=Price(Decimal("100.00")),
            low_price=Price(Decimal("100.00")),
            close_price=Price(Decimal("100.00")),
            volume=Volume(0),  # Zero volume
        )
        
        # Create bar with volume
        volume_bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(datetime(2024, 1, 15, 9, 31, 0, tzinfo=timezone.utc)),
            open_price=Price(Decimal("200.00")),
            high_price=Price(Decimal("200.00")),
            low_price=Price(Decimal("200.00")),
            close_price=Price(Decimal("200.00")),
            volume=Volume(1000),
        )
        
        aggregate.add_bar(zero_volume_bar)
        aggregate.add_bar(volume_bar)
        
        summary = aggregate.calculate_daily_summary()
        
        # VWAP should be based only on the volume bar
        expected_vwap = Decimal("200.00")
        assert summary.vwap.value == expected_vwap
    
    def test_get_bars_in_range(self, aggregate, sample_bar, sample_bar_2):
        """Test getting bars within a time range."""
        from src.marketpipe.domain.value_objects import TimeRange
        
        aggregate.add_bar(sample_bar)
        aggregate.add_bar(sample_bar_2)
        
        # Create time range that includes only first bar
        time_range = TimeRange(
            start=sample_bar.timestamp,
            end=Timestamp(datetime(2024, 1, 15, 9, 30, 30, tzinfo=timezone.utc))
        )
        
        bars_in_range = aggregate.get_bars_in_range(time_range)
        assert len(bars_in_range) == 1
        assert bars_in_range[0] == sample_bar
    
    def test_has_gaps_detection(self, aggregate, symbol):
        """Test gap detection in trading data."""
        # Add only a few bars (much less than expected 390 minutes)
        for minute in range(3):
            bar = OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(datetime(2024, 1, 15, 9, 30 + minute, 0, tzinfo=timezone.utc)),
                open_price=Price(Decimal("100.00")),
                high_price=Price(Decimal("100.00")),
                low_price=Price(Decimal("100.00")),
                close_price=Price(Decimal("100.00")),
                volume=Volume(1000),
            )
            aggregate.add_bar(bar)
        
        assert aggregate.has_gaps()  # Should detect gaps with only 3 bars
    
    def test_event_commitment(self, aggregate, sample_bar):
        """Test event commitment mechanism."""
        aggregate.add_bar(sample_bar)
        
        # Should have uncommitted events
        events = aggregate.get_uncommitted_events()
        assert len(events) > 0
        
        # Mark events as committed
        aggregate.mark_events_committed()
        
        # Should have no uncommitted events
        events = aggregate.get_uncommitted_events()
        assert len(events) == 0
    
    @patch('src.marketpipe.events.EventBus.publish')
    def test_event_bus_integration(self, mock_publish, aggregate, sample_bar):
        """Test integration with EventBus for event emission."""
        # This test would verify that events are published to the EventBus
        # For now, we just test that events are created correctly
        aggregate.add_bar(sample_bar)
        aggregate.complete_collection()
        
        events = aggregate.get_uncommitted_events()
        assert len(events) >= 2  # At least BarCollectionStarted and MarketDataReceived
        
        # Verify event types
        event_types = [type(event).__name__ for event in events]
        assert "BarCollectionStarted" in event_types
        assert "MarketDataReceived" in event_types
        assert "BarCollectionCompleted" in event_types 