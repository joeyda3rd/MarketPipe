# SPDX-License-Identifier: Apache-2.0
"""Tests for Polygon.io market data adapter."""

from __future__ import annotations

import asyncio
import datetime as dt
import pytest
import httpx
import respx
from unittest.mock import Mock, patch
from typing import Dict, Any

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.infrastructure.adapters import PolygonMarketDataAdapter, MarketDataProviderError
from marketpipe.ingestion.infrastructure.polygon_client import PolygonClient
from marketpipe.ingestion.infrastructure.models import PolygonClientConfig


class TestPolygonMarketDataAdapter:
    """Test suite for PolygonMarketDataAdapter."""

    @pytest.fixture
    def adapter(self):
        """Create a test adapter instance."""
        return PolygonMarketDataAdapter(
            api_key="test_api_key",
            base_url="https://api.polygon.io",
            rate_limit_per_min=50,
            max_results=1000,
        )

    @pytest.fixture
    def sample_polygon_response(self):
        """Sample Polygon API response."""
        return {
            "status": "OK",
            "ticker": "AAPL",
            "results": [
                {
                    "t": 1640995800000,  # 2022-01-01 09:30:00 UTC in milliseconds
                    "o": 150.0,
                    "h": 151.5,
                    "l": 149.5,
                    "c": 150.8,
                    "v": 12345,
                    "n": 100,  # trade count
                    "vw": 150.4,  # volume weighted average price
                }
            ],
            "next_url": None,
        }

    def test_adapter_initialization(self):
        """Test adapter initialization with default values."""
        adapter = PolygonMarketDataAdapter(api_key="test_key")
        
        assert adapter._api_key == "test_key"
        assert adapter._base_url == "https://api.polygon.io"
        assert adapter._rate_limit_per_min == 50
        assert adapter._max_results == 50_000
        assert adapter._polygon_client is not None

    def test_from_config_class_method(self):
        """Test creating adapter from configuration dictionary."""
        config = {
            "api_key": "test_key",
            "base_url": "https://custom.polygon.io",
            "rate_limit_per_min": 100,
            "max_results": 25_000,
        }
        
        adapter = PolygonMarketDataAdapter.from_config(config)
        
        assert adapter._api_key == "test_key"
        assert adapter._base_url == "https://custom.polygon.io"
        assert adapter._rate_limit_per_min == 100
        assert adapter._max_results == 25_000

    def test_from_config_with_defaults(self):
        """Test from_config with minimal configuration."""
        config = {"api_key": "test_key"}
        
        adapter = PolygonMarketDataAdapter.from_config(config)
        
        assert adapter._api_key == "test_key"
        assert adapter._base_url == "https://api.polygon.io"
        assert adapter._rate_limit_per_min == 50
        assert adapter._max_results == 50_000

    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_bars_for_symbol_success(self, adapter, sample_polygon_response):
        """Test successful bars fetching."""
        # Mock the Polygon API endpoint
        respx.get("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2022-01-01/2022-01-02").mock(
            return_value=httpx.Response(200, json=sample_polygon_response)
        )
        
        # Create test parameters
        symbol = Symbol.from_string("AAPL")
        start_time = dt.datetime(2022, 1, 1, 9, 30, tzinfo=dt.timezone.utc)
        end_time = dt.datetime(2022, 1, 2, 9, 30, tzinfo=dt.timezone.utc)
        time_range = TimeRange(Timestamp(start_time), Timestamp(end_time))
        
        # Fetch bars
        bars = await adapter.fetch_bars_for_symbol(symbol, time_range, max_bars=1000)
        
        # Verify results
        assert len(bars) == 1
        bar = bars[0]
        assert bar.symbol == symbol
        assert bar.open_price.value == 150.0
        assert bar.high_price.value == 151.5
        assert bar.low_price.value == 149.5
        assert bar.close_price.value == 150.8
        assert bar.volume.value == 12345

    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_bars_with_pagination(self, adapter):
        """Test fetching bars with pagination."""
        # First page response
        page1_response = {
            "status": "OK",
            "ticker": "AAPL",
            "results": [
                {
                    "t": 1640995800000,
                    "o": 150.0, "h": 151.0, "l": 149.0, "c": 150.5,
                    "v": 1000, "n": 50, "vw": 150.2,
                }
            ],
            "next_url": "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2022-01-01/2022-01-02?cursor=abc123",
        }
        
        # Second page response
        page2_response = {
            "status": "OK",
            "ticker": "AAPL",
            "results": [
                {
                    "t": 1640995860000,  # One minute later
                    "o": 150.5, "h": 151.5, "l": 150.0, "c": 151.0,
                    "v": 1500, "n": 75, "vw": 150.8,
                }
            ],
            "next_url": None,  # No more pages
        }
        
        # Mock both API calls
        respx.get("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2022-01-01/2022-01-02").mock(
            return_value=httpx.Response(200, json=page1_response)
        )
        respx.get("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2022-01-01/2022-01-02?cursor=abc123").mock(
            return_value=httpx.Response(200, json=page2_response)
        )
        
        # Create test parameters
        symbol = Symbol.from_string("AAPL")
        start_time = dt.datetime(2022, 1, 1, 9, 30, tzinfo=dt.timezone.utc)
        end_time = dt.datetime(2022, 1, 2, 9, 30, tzinfo=dt.timezone.utc)
        time_range = TimeRange(Timestamp(start_time), Timestamp(end_time))
        
        # Fetch bars
        bars = await adapter.fetch_bars_for_symbol(symbol, time_range, max_bars=1000)
        
        # Verify results from both pages
        assert len(bars) == 2
        assert bars[0].close_price.value == 150.5
        assert bars[1].close_price.value == 151.0

    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_bars_api_error(self, adapter):
        """Test handling of API errors."""
        # Mock API error response
        error_response = {
            "status": "ERROR",
            "error": "Invalid API key"
        }
        
        respx.get("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2022-01-01/2022-01-02").mock(
            return_value=httpx.Response(401, json=error_response)
        )
        
        # Create test parameters
        symbol = Symbol.from_string("AAPL")
        start_time = dt.datetime(2022, 1, 1, 9, 30, tzinfo=dt.timezone.utc)
        end_time = dt.datetime(2022, 1, 2, 9, 30, tzinfo=dt.timezone.utc)
        time_range = TimeRange(Timestamp(start_time), Timestamp(end_time))
        
        # Expect MarketDataProviderError
        with pytest.raises(MarketDataProviderError):
            await adapter.fetch_bars_for_symbol(symbol, time_range)

    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_bars_rate_limit_handling(self, adapter):
        """Test rate limit handling with Retry-After header."""
        # Mock rate limit response followed by success
        respx.get("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2022-01-01/2022-01-02").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "1"}),
                httpx.Response(200, json={
                    "status": "OK",
                    "ticker": "AAPL",
                    "results": [
                        {
                            "t": 1640995800000,
                            "o": 150.0, "h": 151.0, "l": 149.0, "c": 150.5,
                            "v": 1000, "n": 50, "vw": 150.2,
                        }
                    ],
                    "next_url": None,
                })
            ]
        )
        
        # Create test parameters
        symbol = Symbol.from_string("AAPL")
        start_time = dt.datetime(2022, 1, 1, 9, 30, tzinfo=dt.timezone.utc)
        end_time = dt.datetime(2022, 1, 2, 9, 30, tzinfo=dt.timezone.utc)
        time_range = TimeRange(Timestamp(start_time), Timestamp(end_time))
        
        # Mock sleep to avoid actual delays in tests
        with patch('asyncio.sleep'):
            bars = await adapter.fetch_bars_for_symbol(symbol, time_range)
        
        # Should succeed after retry
        assert len(bars) == 1
        assert bars[0].close_price.value == 150.5

    @pytest.mark.asyncio
    async def test_get_supported_symbols(self, adapter):
        """Test getting supported symbols."""
        symbols = await adapter.get_supported_symbols()
        
        assert len(symbols) > 0
        assert Symbol.from_string("AAPL") in symbols
        assert Symbol.from_string("GOOGL") in symbols
        assert Symbol.from_string("MSFT") in symbols

    @pytest.mark.asyncio
    @respx.mock
    async def test_is_available_success(self, adapter):
        """Test connection availability check - success case."""
        # Mock successful response
        respx.get(url__regex=r".*polygon\.io.*").mock(
            return_value=httpx.Response(200, json={
                "status": "OK",
                "ticker": "AAPL",
                "results": [],
                "next_url": None,
            })
        )
        
        is_available = await adapter.is_available()
        assert is_available is True

    @pytest.mark.asyncio
    @respx.mock
    async def test_is_available_failure(self, adapter):
        """Test connection availability check - failure case."""
        # Mock failed response
        respx.get(url__regex=r".*polygon\.io.*").mock(
            return_value=httpx.Response(401, json={"error": "Unauthorized"})
        )
        
        is_available = await adapter.is_available()
        assert is_available is False

    def test_get_provider_metadata(self, adapter):
        """Test provider metadata."""
        metadata = adapter.get_provider_metadata()
        
        assert metadata.provider_name == "polygon"
        assert metadata.supports_real_time is True
        assert metadata.supports_historical is True
        assert metadata.rate_limit_per_minute == 50
        assert metadata.minimum_time_resolution == "1m"
        assert metadata.maximum_history_days == 730

    def test_get_provider_info(self, adapter):
        """Test provider info (legacy method)."""
        info = adapter.get_provider_info()
        
        assert info["provider"] == "polygon"
        assert info["base_url"] == "https://api.polygon.io"
        assert info["rate_limit_per_min"] == 50
        assert info["max_results"] == 50_000
        assert info["supports_real_time"] is True
        assert info["supports_historical"] is True

    def test_translate_polygon_bar_to_domain(self, adapter):
        """Test translation of Polygon bar to domain model."""
        polygon_bar = {
            "timestamp": 1640995800000000000,  # nanoseconds
            "open": 150.0,
            "high": 151.5,
            "low": 149.5,
            "close": 150.8,
            "volume": 12345,
        }
        
        symbol = Symbol.from_string("AAPL")
        domain_bar = adapter._translate_polygon_bar_to_domain(polygon_bar, symbol)
        
        assert domain_bar.symbol == symbol
        assert domain_bar.open_price.value == 150.0
        assert domain_bar.high_price.value == 151.5
        assert domain_bar.low_price.value == 149.5
        assert domain_bar.close_price.value == 150.8
        assert domain_bar.volume.value == 12345

    def test_safe_decimal_conversion(self, adapter):
        """Test safe decimal conversion."""
        # Test various input types
        assert adapter._safe_decimal(150.0).to_eng_string() == "150.0"
        assert adapter._safe_decimal(150).to_eng_string() == "150"
        assert adapter._safe_decimal("150.5").to_eng_string() == "150.5"
        
        # Test invalid inputs
        with pytest.raises(Exception):  # Should raise DataTranslationError
            adapter._safe_decimal(None)
        
        with pytest.raises(Exception):  # Should raise DataTranslationError
            adapter._safe_decimal([1, 2, 3])

    @pytest.mark.asyncio
    async def test_legacy_fetch_bars_method(self, adapter):
        """Test legacy fetch_bars method for backward compatibility."""
        symbol = Symbol.from_string("AAPL")
        start_timestamp = 1640995800000000000  # nanoseconds
        end_timestamp = 1640999400000000000    # nanoseconds
        
        # Mock the modern method
        with patch.object(adapter, 'fetch_bars_for_symbol') as mock_fetch:
            mock_fetch.return_value = []
            
            result = await adapter.fetch_bars(symbol, start_timestamp, end_timestamp, batch_size=500)
            
            # Verify the modern method was called with correct parameters
            assert mock_fetch.called
            call_args = mock_fetch.call_args
            assert call_args[0][0] == symbol  # symbol argument
            assert call_args[0][2] == 500     # max_bars argument


class TestPolygonClient:
    """Test suite for PolygonClient infrastructure class."""

    @pytest.fixture
    def client_config(self):
        """Create test client configuration."""
        return PolygonClientConfig(
            api_key="test_api_key",
            base_url="https://api.polygon.io",
            rate_limit_per_min=50,
            max_results=1000,
        )

    @pytest.fixture
    def client(self, client_config):
        """Create test client instance."""
        return PolygonClient(
            config=client_config,
            auth=None,
            rate_limiter=None,
            state_backend=None,
        )

    def test_client_initialization(self, client):
        """Test client initialization."""
        assert client._current_symbol is None
        assert "PolygonClient initialized" in str(client.log.handlers)

    def test_endpoint_path(self, client):
        """Test endpoint path template."""
        path = client.endpoint_path()
        assert path == "/v2/aggs/ticker/{symbol}/range/1/minute/{from_date}/{to_date}"

    def test_build_request_params(self, client):
        """Test building request parameters."""
        params = client.build_request_params("AAPL", 1640995800000000000, 1640999400000000000)
        
        assert params["adjusted"] == "true"
        assert params["sort"] == "asc"
        assert params["limit"] == "1000"
        assert params["apikey"] == "test_api_key"
        assert "cursor" not in params
        
        # Test with cursor
        params_with_cursor = client.build_request_params(
            "AAPL", 1640995800000000000, 1640999400000000000, cursor="abc123"
        )
        assert params_with_cursor["cursor"] == "abc123"

    def test_build_url(self, client):
        """Test URL building."""
        # Test with nanosecond timestamps
        start_ns = 1640995800000000000  # 2022-01-01 09:30:00 UTC
        end_ns = 1640999400000000000    # 2022-01-01 10:30:00 UTC
        
        url = client.build_url("AAPL", start_ns, end_ns)
        
        expected = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2022-01-01/2022-01-01"
        assert url == expected

    def test_next_cursor_extraction(self, client):
        """Test cursor extraction from next_url."""
        # Test with cursor
        response_with_cursor = {
            "next_url": "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2022-01-01/2022-01-02?cursor=abc123&apikey=test"
        }
        cursor = client.next_cursor(response_with_cursor)
        assert cursor == "abc123"
        
        # Test without cursor
        response_without_cursor = {"next_url": None}
        cursor = client.next_cursor(response_without_cursor)
        assert cursor is None
        
        # Test with malformed URL
        response_malformed = {"next_url": "not-a-valid-url"}
        cursor = client.next_cursor(response_malformed)
        assert cursor is None

    def test_parse_response_success(self, client):
        """Test parsing successful response."""
        client._current_symbol = "AAPL"
        
        response = {
            "status": "OK",
            "ticker": "AAPL",
            "results": [
                {
                    "t": 1640995800000,  # milliseconds
                    "o": 150.0,
                    "h": 151.5,
                    "l": 149.5,
                    "c": 150.8,
                    "v": 12345,
                    "n": 100,
                    "vw": 150.4,
                }
            ],
        }
        
        rows = client.parse_response(response)
        
        assert len(rows) == 1
        row = rows[0]
        assert row["symbol"] == "AAPL"
        assert row["timestamp"] == 1640995800000000000  # converted to nanoseconds
        assert row["open"] == 150.0
        assert row["high"] == 151.5
        assert row["low"] == 149.5
        assert row["close"] == 150.8
        assert row["volume"] == 12345
        assert row["trade_count"] == 100
        assert row["vwap"] == 150.4
        assert row["source"] == "polygon"
        assert row["schema_version"] == 1

    def test_parse_response_error(self, client):
        """Test parsing error response."""
        error_response = {
            "status": "ERROR",
            "error": "Invalid API key"
        }
        
        with pytest.raises(RuntimeError, match="Polygon API error"):
            client.parse_response(error_response)

    def test_should_retry_logic(self, client):
        """Test retry logic for different status codes."""
        # Should retry on rate limits and server errors
        assert client.should_retry(429, {}) is True
        assert client.should_retry(500, {}) is True
        assert client.should_retry(502, {}) is True
        assert client.should_retry(503, {}) is True
        assert client.should_retry(504, {}) is True
        
        # Should retry on 403 with rate limit message
        assert client.should_retry(403, {"message": "rate limit exceeded"}) is True
        assert client.should_retry(403, {"error": "too many requests"}) is True
        
        # Should not retry on auth errors
        assert client.should_retry(403, {"error": "unauthorized"}) is False
        assert client.should_retry(401, {}) is False
        assert client.should_retry(404, {}) is False
        
        # Should not retry on explicit API errors
        assert client.should_retry(200, {"status": "ERROR"}) is False

    def test_backoff_calculation(self, client):
        """Test exponential backoff calculation."""
        # Test increasing backoff times
        backoff1 = client._backoff(1)
        backoff2 = client._backoff(2)
        backoff3 = client._backoff(3)
        
        assert 0 < backoff1 < backoff2 < backoff3
        assert backoff3 <= 32.2  # Should be capped at ~32 seconds + jitter


if __name__ == "__main__":
    pytest.main([__file__]) 