"""Test that boundary check works correctly after timestamp fix."""

from __future__ import annotations

from datetime import date, datetime, timezone

from marketpipe.domain.value_objects import Symbol
from marketpipe.ingestion.infrastructure.adapters import AlpacaMarketDataAdapter


class TestBoundaryCheckTimestampFix:
    """Test that boundary checks work correctly after fixing timestamp conversion."""

    def test_boundary_check_passes_with_correct_timestamps(self):
        """Test that boundary check passes when timestamps are correctly converted."""
        # Create adapter instance
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://data.alpaca.markets/v2",
            feed_type="iex",
        )

        # Mock bars with timestamps that match the requested date range
        # Requesting 2024-06-15 to 2024-06-16, should get data in that range
        mock_bars = [
            {
                "timestamp": 1718445600000000000,  # 2024-06-15T10:00:00Z
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
            {
                "timestamp": 1718449200000000000,  # 2024-06-15T11:00:00Z
                "open": 100.5,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1500,
            },
            {
                "timestamp": 1718532000000000000,  # 2024-06-16T10:00:00Z
                "open": 101.5,
                "high": 103.0,
                "low": 101.0,
                "close": 102.0,
                "volume": 1200,
            },
        ]

        symbol = Symbol.from_string("TSLA")

        # Translate all bars to domain models
        domain_bars = []
        for mock_bar in mock_bars:
            domain_bar = adapter._translate_alpaca_bar_to_domain(mock_bar, symbol)
            domain_bars.append(domain_bar)

        # Verify all timestamps are in the expected date range
        start_date = date(2024, 6, 15)
        end_date = date(2024, 6, 16)

        for domain_bar in domain_bars:
            bar_date = domain_bar.timestamp.value.date()
            assert (
                start_date <= bar_date <= end_date
            ), f"Bar date {bar_date} not in range {start_date} to {end_date}"

        # Verify specific timestamps are correct
        assert domain_bars[0].timestamp.value == datetime(
            2024, 6, 15, 10, 0, 0, tzinfo=timezone.utc
        )
        assert domain_bars[1].timestamp.value == datetime(
            2024, 6, 15, 11, 0, 0, tzinfo=timezone.utc
        )
        assert domain_bars[2].timestamp.value == datetime(
            2024, 6, 16, 10, 0, 0, tzinfo=timezone.utc
        )

    def test_boundary_check_detects_wrong_date_range(self):
        """Test that boundary check would detect when data is outside requested range."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://data.alpaca.markets/v2",
            feed_type="iex",
        )

        # Mock bars with timestamps OUTSIDE the requested range
        # Requesting 2024-06-15 to 2024-06-16, but getting 2020 data (the old bug)
        mock_bars_wrong_year = [
            {
                "timestamp": 1595673600000000000,  # 2020-07-25T10:00:00Z (wrong year!)
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            }
        ]

        symbol = Symbol.from_string("TSLA")

        # Translate bar to domain model
        domain_bar = adapter._translate_alpaca_bar_to_domain(mock_bars_wrong_year[0], symbol)

        # Verify the timestamp is in 2020 (this would trigger boundary check failure)
        assert domain_bar.timestamp.value.year == 2020
        assert domain_bar.timestamp.value.date() == date(2020, 7, 25)

        # This would fail boundary check for a 2024 request
        requested_start = date(2024, 6, 15)
        requested_end = date(2024, 6, 16)
        actual_date = domain_bar.timestamp.value.date()

        # Verify this would trigger boundary check failure
        assert not (requested_start <= actual_date <= requested_end)

    def test_timestamp_no_longer_stuck_in_2020(self):
        """Test that timestamps are no longer artificially stuck in 2020."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://data.alpaca.markets/v2",
            feed_type="iex",
        )

        # Test various years to ensure the fix works across different dates
        test_cases = [
            (1577836800000000000, 2020, "2020 New Year"),  # 2020-01-01T00:00:00Z
            (1640995200000000000, 2022, "2022 New Year"),  # 2022-01-01T00:00:00Z
            (1672531200000000000, 2023, "2023 New Year"),  # 2023-01-01T00:00:00Z
            (1704067200000000000, 2024, "2024 New Year"),  # 2024-01-01T00:00:00Z
            (1735689600000000000, 2025, "2025 New Year"),  # 2025-01-01T00:00:00Z
        ]

        symbol = Symbol.from_string("SPY")

        for timestamp_ns, expected_year, description in test_cases:
            mock_bar = {
                "timestamp": timestamp_ns,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            }

            domain_bar = adapter._translate_alpaca_bar_to_domain(mock_bar, symbol)
            actual_year = domain_bar.timestamp.value.year

            assert (
                actual_year == expected_year
            ), f"Failed for {description}: expected {expected_year}, got {actual_year}"

            # Verify this is NOT stuck in 2020 (unless that's the actual year)
            if expected_year != 2020:
                assert actual_year != 2020, f"Timestamp incorrectly stuck in 2020 for {description}"

    def test_market_hours_timestamps_correct(self):
        """Test that market hours timestamps are correctly handled."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://data.alpaca.markets/v2",
            feed_type="iex",
        )

        # Test typical market hours for 2024-03-15 (a Friday)
        # Market open: 9:30 AM ET = 13:30 UTC
        # Market close: 4:00 PM ET = 20:00 UTC
        market_hours_tests = [
            (1710509400000000000, "09:30", "Market open"),  # 2024-03-15T13:30:00Z
            (1710511200000000000, "10:00", "Morning trading"),  # 2024-03-15T14:00:00Z
            (1710525600000000000, "14:00", "Afternoon trading"),  # 2024-03-15T18:00:00Z
            (1710529200000000000, "15:00", "Near close"),  # 2024-03-15T19:00:00Z
        ]

        symbol = Symbol.from_string("SPY")

        for timestamp_ns, expected_time, description in market_hours_tests:
            mock_bar = {
                "timestamp": timestamp_ns,
                "open": 420.0,
                "high": 425.0,
                "low": 418.0,
                "close": 422.0,
                "volume": 5000,
            }

            domain_bar = adapter._translate_alpaca_bar_to_domain(mock_bar, symbol)

            # Verify date is correct
            assert domain_bar.timestamp.value.date() == date(2024, 3, 15)

            # Verify time is within reasonable market hours (not offset by bogus amount)
            hour_minute = domain_bar.timestamp.value.strftime("%H:%M")
            # The expected times are in ET converted to UTC, so check against UTC times
            if expected_time == "09:30":
                assert hour_minute == "13:30", f"Market open time incorrect for {description}"
            elif expected_time == "10:00":
                assert hour_minute == "14:00", f"Morning time incorrect for {description}"
