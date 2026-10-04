# SPDX-License-Identifier: Apache-2.0
"""Unit tests for Polygon adapter pagination functionality."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.infrastructure.polygon_adapter import PolygonMarketDataAdapter


class TestPolygonAdapterPagination:
    """Test Polygon adapter handles pagination correctly."""

    def test_adapter_can_be_constructed_without_an_event_loop(self, monkeypatch):
        original_lock = asyncio.Lock

        def require_running_loop():
            asyncio.get_running_loop()
            return original_lock()

        monkeypatch.setattr(asyncio, "Lock", require_running_loop)
        adapter = PolygonMarketDataAdapter(api_key="test_api_key")
        asyncio.run(adapter._apply_rate_limit())

        assert len(adapter._request_times) == 1

    @pytest.fixture
    def adapter(self):
        """Create a Polygon adapter for testing."""
        return PolygonMarketDataAdapter(
            api_key="test_api_key",
            rate_limit_per_minute=1000,  # High limit to avoid rate limiting in tests
        )

    @pytest.fixture
    def time_range(self):
        """Create a time range for testing."""
        start = Timestamp(datetime(2024, 12, 2, tzinfo=timezone.utc))
        end = Timestamp(datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc))
        return TimeRange(start, end)

    def _make_bar(self, timestamp_ms: int, open_price: float = 100.0) -> dict:
        """Create a mock bar result."""
        return {
            "t": timestamp_ms,
            "o": open_price,
            "h": open_price + 1,
            "l": open_price - 1,
            "c": open_price + 0.5,
            "v": 1000,
            "n": 10,
            "vw": open_price + 0.25,
        }

    @pytest.mark.asyncio
    async def test_adapter_follows_pagination_across_multiple_pages(self, adapter, time_range):
        """Test that adapter correctly follows next_url pagination across multiple pages."""
        # Create 3 pages of mock data
        page1_bars = [self._make_bar(1733155200000 + i * 60000) for i in range(1000)]  # Dec 2, 2024
        page2_bars = [self._make_bar(1733155200000 + 1000 * 60000 + i * 60000) for i in range(1000)]
        page3_bars = [self._make_bar(1733155200000 + 2000 * 60000 + i * 60000) for i in range(500)]

        pages = [
            {
                "status": "OK",
                "resultsCount": 1000,
                "results": page1_bars,
                "next_url": "https://api.polygon.io/v2/aggs?cursor=abc123",
            },
            {
                "status": "OK",
                "resultsCount": 1000,
                "results": page2_bars,
                "next_url": "https://api.polygon.io/v2/aggs?cursor=def456",
            },
            {
                "status": "OK",
                "resultsCount": 500,
                "results": page3_bars,
                # No next_url - last page
            },
        ]

        call_count = 0

        async def mock_make_request(url, params):
            nonlocal call_count
            result = pages[call_count]
            call_count += 1
            return result

        with patch.object(adapter, "_make_request", side_effect=mock_make_request):
            with patch.object(adapter, "_apply_rate_limit", new_callable=AsyncMock):
                bars = await adapter.fetch_bars_for_symbol(
                    symbol=Symbol.from_string("AAPL"),
                    time_range=time_range,
                    max_bars=1000,
                )

        # Should have made 3 API calls (one per page)
        assert call_count == 3

        # Should have fetched all bars from all 3 pages
        assert len(bars) == 2500

    @pytest.mark.asyncio
    async def test_adapter_stops_pagination_when_no_next_url(self, adapter, time_range):
        """Test that adapter stops paginating when there's no next_url."""
        page_bars = [self._make_bar(1733155200000 + i * 60000) for i in range(500)]

        pages = [
            {
                "status": "OK",
                "resultsCount": 500,
                "results": page_bars,
                # No next_url - only page
            }
        ]

        call_count = 0

        async def mock_make_request(url, params):
            nonlocal call_count
            result = pages[call_count]
            call_count += 1
            return result

        with patch.object(adapter, "_make_request", side_effect=mock_make_request):
            with patch.object(adapter, "_apply_rate_limit", new_callable=AsyncMock):
                bars = await adapter.fetch_bars_for_symbol(
                    symbol=Symbol.from_string("AAPL"),
                    time_range=time_range,
                    max_bars=1000,
                )

        # Should have made only 1 API call
        assert call_count == 1
        assert len(bars) == 500

    @pytest.mark.asyncio
    async def test_adapter_stops_pagination_when_reaching_end_date(self, adapter):
        """Test that adapter stops paginating when bars exceed end date."""
        # Create a narrow time range
        start = Timestamp(datetime(2024, 12, 2, 16, 0, 0, tzinfo=timezone.utc))
        end = Timestamp(datetime(2024, 12, 2, 16, 59, 59, tzinfo=timezone.utc))
        time_range = TimeRange(start, end)

        # First page has bars within range
        page1_bars = [
            self._make_bar(1733155200000 + i * 60000) for i in range(60)
        ]  # First 60 minutes
        # Second page has bars beyond range - should trigger early stop
        page2_bars = [self._make_bar(1733158800000 + i * 60000) for i in range(60)]  # After 1 hour

        pages = [
            {
                "status": "OK",
                "resultsCount": 60,
                "results": page1_bars,
                "next_url": "https://api.polygon.io/v2/aggs?cursor=abc123",
            },
            {
                "status": "OK",
                "resultsCount": 60,
                "results": page2_bars,
                "next_url": "https://api.polygon.io/v2/aggs?cursor=def456",
            },
        ]

        call_count = 0

        async def mock_make_request(url, params):
            nonlocal call_count
            result = pages[call_count]
            call_count += 1
            return result

        with patch.object(adapter, "_make_request", side_effect=mock_make_request):
            with patch.object(adapter, "_apply_rate_limit", new_callable=AsyncMock):
                bars = await adapter.fetch_bars_for_symbol(
                    symbol=Symbol.from_string("AAPL"),
                    time_range=time_range,
                    max_bars=1000,
                )

        # Should have made 2 API calls, stopped on second page due to end date
        assert call_count == 2

        # Should only have bars from the first page (60 bars within range)
        assert len(bars) == 60

    @pytest.mark.asyncio
    async def test_adapter_extracts_cursor_from_next_url(self, adapter, time_range):
        """Test that adapter correctly extracts cursor from next_url."""
        page1_bars = [self._make_bar(1733155200000 + i * 60000) for i in range(100)]
        page2_bars = [self._make_bar(1733155200000 + 100 * 60000 + i * 60000) for i in range(100)]

        pages = [
            {
                "status": "OK",
                "resultsCount": 100,
                "results": page1_bars,
                "next_url": "https://api.polygon.io/v2/aggs/ticker/AAPL?cursor=bXktY3Vyc29yLXZhbHVl&limit=1000",
            },
            {
                "status": "OK",
                "resultsCount": 100,
                "results": page2_bars,
            },
        ]

        call_count = 0
        captured_params = []

        async def mock_make_request(url, params):
            nonlocal call_count
            captured_params.append(params.copy())
            result = pages[call_count]
            call_count += 1
            return result

        with patch.object(adapter, "_make_request", side_effect=mock_make_request):
            with patch.object(adapter, "_apply_rate_limit", new_callable=AsyncMock):
                await adapter.fetch_bars_for_symbol(
                    symbol=Symbol.from_string("AAPL"),
                    time_range=time_range,
                    max_bars=1000,
                )

        # First call should not have cursor
        assert "cursor" not in captured_params[0]

        # Second call should have extracted cursor
        assert captured_params[1]["cursor"] == "bXktY3Vyc29yLXZhbHVl"

    @pytest.mark.asyncio
    async def test_adapter_handles_empty_results(self, adapter, time_range):
        """Test that adapter handles empty results correctly."""
        pages = [
            {
                "status": "OK",
                "resultsCount": 0,
                # No results key or empty results
            }
        ]

        call_count = 0

        async def mock_make_request(url, params):
            nonlocal call_count
            result = pages[call_count]
            call_count += 1
            return result

        with patch.object(adapter, "_make_request", side_effect=mock_make_request):
            with patch.object(adapter, "_apply_rate_limit", new_callable=AsyncMock):
                bars = await adapter.fetch_bars_for_symbol(
                    symbol=Symbol.from_string("AAPL"),
                    time_range=time_range,
                    max_bars=1000,
                )

        assert call_count == 1
        assert len(bars) == 0


class TestPolygonAdapterPaginationRegression:
    """Regression tests for the pagination bug fix."""

    @pytest.fixture
    def adapter(self):
        """Create a Polygon adapter for testing."""
        return PolygonMarketDataAdapter(
            api_key="test_api_key",
            rate_limit_per_minute=1000,
        )

    @pytest.fixture
    def time_range(self):
        """Create a time range for testing."""
        start = Timestamp(datetime(2024, 12, 2, tzinfo=timezone.utc))
        end = Timestamp(datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc))
        return TimeRange(start, end)

    def _make_bar(self, timestamp_ms: int) -> dict:
        """Create a mock bar result."""
        return {
            "t": timestamp_ms,
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000,
            "n": 10,
            "vw": 100.25,
        }

    @pytest.mark.asyncio
    async def test_pagination_bug_regression_fetches_all_pages(self, adapter, time_range):
        """
        Regression test for pagination bug.

        Previous bug: cursor was initialized to None, and the check
        `if cursor is None: break` happened BEFORE extracting next_url,
        causing pagination to stop after the first page.
        """
        # Simulate Polygon API returning 14,298 bars across 15 pages (1000 per page)
        total_bars = 14298
        bars_per_page = 1000
        num_pages = (total_bars + bars_per_page - 1) // bars_per_page

        pages = []
        for page_idx in range(num_pages):
            start_idx = page_idx * bars_per_page
            end_idx = min(start_idx + bars_per_page, total_bars)
            page_bars = [
                self._make_bar(1733155200000 + i * 60000) for i in range(start_idx, end_idx)
            ]

            page_data = {
                "status": "OK",
                "resultsCount": len(page_bars),
                "results": page_bars,
            }

            # Add next_url for all pages except the last
            if page_idx < num_pages - 1:
                page_data["next_url"] = f"https://api.polygon.io/v2/aggs?cursor=page{page_idx + 1}"

            pages.append(page_data)

        call_count = 0

        async def mock_make_request(url, params):
            nonlocal call_count
            result = pages[call_count]
            call_count += 1
            return result

        with patch.object(adapter, "_make_request", side_effect=mock_make_request):
            with patch.object(adapter, "_apply_rate_limit", new_callable=AsyncMock):
                bars = await adapter.fetch_bars_for_symbol(
                    symbol=Symbol.from_string("AAPL"),
                    time_range=time_range,
                    max_bars=1000,
                )

        # Should have fetched all pages
        assert call_count == num_pages, f"Expected {num_pages} API calls, got {call_count}"

        # Should have all bars
        assert len(bars) == total_bars, f"Expected {total_bars} bars, got {len(bars)}"
