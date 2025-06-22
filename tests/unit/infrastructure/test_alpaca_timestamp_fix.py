"""Test Alpaca timestamp conversion fix."""

from __future__ import annotations

from datetime import datetime, timezone, date
from decimal import Decimal

from marketpipe.domain.value_objects import Symbol
from marketpipe.ingestion.infrastructure.adapters import AlpacaMarketDataAdapter


class TestAlpacaTimestampFix:
    """Test the fix for Alpaca timestamp conversion bug."""

    def test_alpaca_timestamp_conversion_from_nanoseconds(self):
        """Test that Alpaca timestamps are correctly converted from nanoseconds."""
        # Create adapter instance
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://data.alpaca.markets/v2",
            feed_type="iex",
        )

        # Mock bar data with timestamp in nanoseconds (as returned by AlpacaClient)
        # This represents 2024-05-02T14:30:00Z (1714660200 seconds * 1_000_000_000)
        mock_bar = {
            "timestamp": 1714660200000000000,  # 2024-05-02T14:30:00Z in nanoseconds
            "open": 500.0,
            "high": 505.0,
            "low": 495.0,
            "close": 502.0,
            "volume": 1000,
        }

        symbol = Symbol.from_string("AAPL")

        # Translate to domain model
        domain_bar = adapter._translate_alpaca_bar_to_domain(mock_bar, symbol)

        # Verify timestamp is correctly converted
        expected_datetime = datetime(2024, 5, 2, 14, 30, 0, tzinfo=timezone.utc)
        assert domain_bar.timestamp.value == expected_datetime

        # Verify other fields are preserved
        assert domain_bar.symbol == symbol
        assert domain_bar.open_price.value == Decimal("500.0")
        assert domain_bar.high_price.value == Decimal("505.0")
        assert domain_bar.low_price.value == Decimal("495.0")
        assert domain_bar.close_price.value == Decimal("502.0")
        assert domain_bar.volume.value == 1000

    def test_alpaca_timestamp_conversion_legacy_format(self):
        """Test timestamp conversion with legacy bar format using 't' field."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://data.alpaca.markets/v2",
            feed_type="iex",
        )

        # Mock bar with legacy format (using 't' instead of 'timestamp')
        # This represents 2024-01-15T09:30:00Z (1705311000 seconds * 1_000_000_000)
        mock_bar = {
            "t": 1705311000000000000,  # 2024-01-15T09:30:00Z in nanoseconds
            "o": 150.0,
            "h": 155.0,
            "l": 148.0,
            "c": 152.0,
            "v": 2000,
        }

        symbol = Symbol.from_string("GOOGL")

        # Translate to domain model
        domain_bar = adapter._translate_alpaca_bar_to_domain(mock_bar, symbol)

        # Verify timestamp is correctly converted
        expected_datetime = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
        assert domain_bar.timestamp.value == expected_datetime

        # Verify other fields are preserved
        assert domain_bar.symbol == symbol
        assert domain_bar.open_price.value == Decimal("150.0")
        assert domain_bar.high_price.value == Decimal("155.0")
        assert domain_bar.low_price.value == Decimal("148.0")
        assert domain_bar.close_price.value == Decimal("152.0")
        assert domain_bar.volume.value == 2000

    def test_alpaca_timestamp_no_bogus_offset(self):
        """Test that the bogus 9600 second offset is no longer applied."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://data.alpaca.markets/v2",
            feed_type="iex",
        )

        # Use a known timestamp: 2024-06-15T10:00:00Z
        # This is 1718445600 seconds since epoch
        timestamp_ns = 1718445600000000000  # nanoseconds

        mock_bar = {
            "timestamp": timestamp_ns,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 500,
        }

        symbol = Symbol.from_string("TSLA")

        # Translate to domain model
        domain_bar = adapter._translate_alpaca_bar_to_domain(mock_bar, symbol)

        # Verify timestamp is exactly what we expect (no offset)
        expected_datetime = datetime(2024, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
        assert domain_bar.timestamp.value == expected_datetime

        # Verify this is NOT the old behavior (which would subtract 9600 seconds)
        wrong_datetime = datetime(
            2024, 6, 15, 7, 20, 0, tzinfo=timezone.utc
        )  # 10:00 - 160 minutes
        assert domain_bar.timestamp.value != wrong_datetime

    def test_alpaca_timestamp_boundary_dates(self):
        """Test timestamp conversion for boundary dates that were problematic."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://data.alpaca.markets/v2",
            feed_type="iex",
        )

        # Test cases for dates that should work correctly now
        test_cases = [
            # (timestamp_ns, expected_date, expected_time, description)
            (1577836800000000000, date(2020, 1, 1), "00:00:00", "2020 New Year"),
            (1640995200000000000, date(2022, 1, 1), "00:00:00", "2022 New Year"),
            (1672531200000000000, date(2023, 1, 1), "00:00:00", "2023 New Year"),
            (1704067200000000000, date(2024, 1, 1), "00:00:00", "2024 New Year"),
            (1735689600000000000, date(2025, 1, 1), "00:00:00", "2025 New Year"),
        ]

        for timestamp_ns, expected_date, expected_time, description in test_cases:
            mock_bar = {
                "timestamp": timestamp_ns,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            }

            symbol = Symbol.from_string("SPY")
            domain_bar = adapter._translate_alpaca_bar_to_domain(mock_bar, symbol)

            # Verify the date component is correct
            actual_date = domain_bar.timestamp.value.date()
            assert actual_date == expected_date, (
                f"Failed for {description}: expected {expected_date}, got {actual_date}"
            )

            # Verify the time component is correct
            actual_time = domain_bar.timestamp.value.strftime("%H:%M:%S")
            assert actual_time == expected_time, (
                f"Failed for {description}: expected {expected_time}, got {actual_time}"
            )

    def test_alpaca_timestamp_trading_hours(self):
        """Test timestamp conversion for typical trading hours."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://data.alpaca.markets/v2",
            feed_type="iex",
        )

        # Test market open: 2024-03-15T13:30:00Z (9:30 AM ET)
        market_open_ns = 1710509400000000000

        mock_bar = {
            "timestamp": market_open_ns,
            "open": 420.0,
            "high": 425.0,
            "low": 418.0,
            "close": 422.0,
            "volume": 5000,
        }

        symbol = Symbol.from_string("SPY")
        domain_bar = adapter._translate_alpaca_bar_to_domain(mock_bar, symbol)

        # Verify this is market open time in UTC
        expected_datetime = datetime(2024, 3, 15, 13, 30, 0, tzinfo=timezone.utc)
        assert domain_bar.timestamp.value == expected_datetime

        # Verify date extraction works correctly
        assert domain_bar.timestamp.value.date() == date(2024, 3, 15)
