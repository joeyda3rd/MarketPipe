# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import datetime as dt
import json
import types
from typing import Any, Dict, List
from unittest.mock import Mock

import httpx
import pytest

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.infrastructure.adapters import FinnhubMarketDataAdapter
from marketpipe.ingestion.infrastructure.models import ClientConfig
from marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth


class TestFinnhubMarketDataAdapter:
    """Test suite for FinnhubMarketDataAdapter."""

    def test_finnhub_adapter_creation(self):
        """Test FinnhubMarketDataAdapter can be created."""
        adapter = FinnhubMarketDataAdapter(
            api_key="test_key",
            base_url="https://finnhub.io/api/v1",
            rate_limit_per_min=60,
        )
        
        assert adapter._api_key == "test_key"
        assert adapter._base_url == "https://finnhub.io/api/v1"
        assert adapter._client_config.rate_limit_per_min == 60

    def test_finnhub_adapter_from_config(self):
        """Test FinnhubMarketDataAdapter can be created from config."""
        config = {
            "api_key": "test_key",
            "base_url": "https://finnhub.io/api/v1",
            "rate_limit_per_min": 60,
        }
        
        adapter = FinnhubMarketDataAdapter.from_config(config)
        
        assert adapter._api_key == "test_key"
        assert adapter._base_url == "https://finnhub.io/api/v1"
        assert adapter._client_config.rate_limit_per_min == 60

    def test_finnhub_adapter_from_config_defaults(self):
        """Test FinnhubMarketDataAdapter uses defaults when not specified."""
        config = {
            "api_key": "test_key",
        }
        
        adapter = FinnhubMarketDataAdapter.from_config(config)
        
        assert adapter._api_key == "test_key"
        assert adapter._base_url == "https://finnhub.io/api/v1"
        assert adapter._client_config.rate_limit_per_min == 60

    def test_finnhub_adapter_provider_metadata(self):
        """Test FinnhubMarketDataAdapter returns correct provider metadata."""
        adapter = FinnhubMarketDataAdapter(
            api_key="test_key",
            rate_limit_per_min=100,
        )
        
        metadata = adapter.get_provider_metadata()
        
        assert metadata.provider_name == "finnhub"
        assert metadata.supports_real_time is True
        assert metadata.supports_historical is True
        assert metadata.rate_limit_per_minute == 100
        assert metadata.minimum_time_resolution == "1m"
        assert metadata.maximum_history_days == 365

    def test_finnhub_adapter_provider_info(self):
        """Test FinnhubMarketDataAdapter returns correct provider info."""
        adapter = FinnhubMarketDataAdapter(
            api_key="test_key",
            base_url="https://finnhub.io/api/v1",
            rate_limit_per_min=120,
        )
        
        info = adapter.get_provider_info()
        
        assert info["provider"] == "finnhub"
        assert info["base_url"] == "https://finnhub.io/api/v1"
        assert info["rate_limit_per_min"] == 120
        assert info["supports_real_time"] is True
        assert info["supports_historical"] is True

    @pytest.mark.asyncio
    async def test_finnhub_adapter_get_supported_symbols(self):
        """Test FinnhubMarketDataAdapter returns list of supported symbols."""
        adapter = FinnhubMarketDataAdapter(api_key="test_key")
        
        symbols = await adapter.get_supported_symbols()
        
        assert len(symbols) > 0
        assert Symbol.from_string("AAPL") in symbols
        assert Symbol.from_string("GOOGL") in symbols
        assert Symbol.from_string("MSFT") in symbols

    @pytest.mark.asyncio
    async def test_finnhub_adapter_fetch_bars_success(self, monkeypatch):
        """Test FinnhubMarketDataAdapter can fetch bars successfully."""
        # Mock Finnhub API response
        mock_response = {
            "c": [150.0, 151.0],  # close prices
            "h": [151.5, 152.0],  # high prices
            "l": [149.0, 150.0],  # low prices
            "o": [150.5, 150.2],  # open prices
            "s": "ok",
            "t": [1640995800, 1640995860],  # timestamps (Unix seconds)
            "v": [1000, 1500],  # volumes
        }

        async def mock_async_fetch_batch(symbol, start_ns, end_ns):
            # Convert the response to the format expected by the adapter
            rows = []
            for i in range(len(mock_response["t"])):
                rows.append({
                    "symbol": symbol,
                    "timestamp": mock_response["t"][i] * 1_000_000_000,  # Convert to nanoseconds
                    "open": mock_response["o"][i],
                    "high": mock_response["h"][i],
                    "low": mock_response["l"][i],
                    "close": mock_response["c"][i],
                    "volume": mock_response["v"][i],
                })
            return rows

        adapter = FinnhubMarketDataAdapter(api_key="test_key")
        adapter._finnhub_client.async_fetch_batch = mock_async_fetch_batch

        # Create test parameters
        symbol = Symbol.from_string("AAPL")
        start_time = dt.datetime(2022, 1, 1, 9, 30, tzinfo=dt.timezone.utc)
        end_time = dt.datetime(2022, 1, 1, 9, 32, tzinfo=dt.timezone.utc)
        time_range = TimeRange(Timestamp(start_time), Timestamp(end_time))

        # Fetch bars
        bars = await adapter.fetch_bars_for_symbol(symbol, time_range)

        # Verify results
        assert len(bars) == 2
        
        # Check first bar
        bar1 = bars[0]
        assert bar1.symbol == symbol
        assert bar1.open_price.value == 150.5
        assert bar1.high_price.value == 151.5
        assert bar1.low_price.value == 149.0
        assert bar1.close_price.value == 150.0
        assert bar1.volume.value == 1000

        # Check second bar
        bar2 = bars[1]
        assert bar2.symbol == symbol
        assert bar2.open_price.value == 150.2
        assert bar2.high_price.value == 152.0
        assert bar2.low_price.value == 150.0
        assert bar2.close_price.value == 151.0
        assert bar2.volume.value == 1500

    @pytest.mark.asyncio
    async def test_finnhub_adapter_fetch_bars_empty_response(self, monkeypatch):
        """Test FinnhubMarketDataAdapter handles empty response."""
        async def mock_async_fetch_batch(symbol, start_ns, end_ns):
            return []  # Empty response

        adapter = FinnhubMarketDataAdapter(api_key="test_key")
        adapter._finnhub_client.async_fetch_batch = mock_async_fetch_batch

        # Create test parameters
        symbol = Symbol.from_string("AAPL")
        start_time = dt.datetime(2022, 1, 1, 9, 30, tzinfo=dt.timezone.utc)
        end_time = dt.datetime(2022, 1, 1, 9, 32, tzinfo=dt.timezone.utc)
        time_range = TimeRange(Timestamp(start_time), Timestamp(end_time))

        # Fetch bars
        bars = await adapter.fetch_bars_for_symbol(symbol, time_range)

        # Verify results
        assert len(bars) == 0

    @pytest.mark.asyncio
    async def test_finnhub_adapter_fetch_bars_with_limit(self, monkeypatch):
        """Test FinnhubMarketDataAdapter respects max_bars limit."""
        # Mock response with 5 bars
        async def mock_async_fetch_batch(symbol, start_ns, end_ns):
            rows = []
            for i in range(5):
                rows.append({
                    "symbol": symbol,
                    "timestamp": (1640995800 + i * 60) * 1_000_000_000,
                    "open": 150.0 + i,
                    "high": 151.0 + i,
                    "low": 149.0 + i,
                    "close": 150.5 + i,
                    "volume": 1000 + i * 100,
                })
            return rows

        adapter = FinnhubMarketDataAdapter(api_key="test_key")
        adapter._finnhub_client.async_fetch_batch = mock_async_fetch_batch

        # Create test parameters
        symbol = Symbol.from_string("AAPL")
        start_time = dt.datetime(2022, 1, 1, 9, 30, tzinfo=dt.timezone.utc)
        end_time = dt.datetime(2022, 1, 1, 9, 35, tzinfo=dt.timezone.utc)
        time_range = TimeRange(Timestamp(start_time), Timestamp(end_time))

        # Fetch bars with limit of 3
        bars = await adapter.fetch_bars_for_symbol(symbol, time_range, max_bars=3)

        # Verify results
        assert len(bars) == 3

    def test_finnhub_adapter_safe_decimal_conversion(self):
        """Test _safe_decimal method handles various input types."""
        adapter = FinnhubMarketDataAdapter(api_key="test_key")

        # Test integer
        result = adapter._safe_decimal(150)
        assert str(result) == "150"

        # Test float
        result = adapter._safe_decimal(150.5)
        assert str(result) == "150.5"

        # Test string
        result = adapter._safe_decimal("150.25")
        assert str(result) == "150.25"

        # Test invalid input
        with pytest.raises(Exception):  # Should raise DataTranslationError
            adapter._safe_decimal(None)

    @pytest.mark.asyncio
    async def test_finnhub_adapter_legacy_fetch_bars(self, monkeypatch):
        """Test legacy fetch_bars method for backward compatibility."""
        async def mock_async_fetch_batch(symbol, start_ns, end_ns):
            return [{
                "symbol": symbol,
                "timestamp": 1640995800 * 1_000_000_000,
                "open": 150.0,
                "high": 151.0,
                "low": 149.0,
                "close": 150.5,
                "volume": 1000,
            }]

        adapter = FinnhubMarketDataAdapter(api_key="test_key")
        adapter._finnhub_client.async_fetch_batch = mock_async_fetch_batch

        # Use legacy method
        symbol = Symbol.from_string("AAPL")
        start_timestamp = 1640995800 * 1_000_000_000  # nanoseconds
        end_timestamp = 1640995860 * 1_000_000_000    # nanoseconds

        bars = await adapter.fetch_bars(symbol, start_timestamp, end_timestamp)

        # Verify results
        assert len(bars) == 1
        assert bars[0].symbol == symbol
        assert bars[0].open_price.value == 150.0
        assert bars[0].volume.value == 1000


class TestFinnhubClient:
    """Test suite for FinnhubClient (infrastructure layer)."""

    def test_finnhub_client_creation(self):
        """Test FinnhubClient can be created."""
        from marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
        
        config = ClientConfig(
            api_key="test_key",
            base_url="https://finnhub.io/api/v1",
            rate_limit_per_min=60,
        )
        auth = HeaderTokenAuth("test_key", "")
        
        client = FinnhubClient(config=config, auth=auth)
        
        assert client.config.api_key == "test_key"
        assert client.config.base_url == "https://finnhub.io/api/v1"
        assert client.config.rate_limit_per_min == 60

    def test_finnhub_client_build_request_params(self):
        """Test FinnhubClient builds correct request parameters."""
        from marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
        
        config = ClientConfig(api_key="test_key", base_url="https://finnhub.io/api/v1")
        auth = HeaderTokenAuth("test_key", "")
        client = FinnhubClient(config=config, auth=auth)

        # Test parameters
        symbol = "AAPL"
        start_ts = 1640995800 * 1_000_000_000  # nanoseconds
        end_ts = 1640995860 * 1_000_000_000    # nanoseconds

        params = client.build_request_params(symbol, start_ts, end_ts)

        assert params["symbol"] == "AAPL"
        assert params["resolution"] == "1"
        assert params["from"] == "1640995800"  # Unix seconds
        assert params["to"] == "1640995860"    # Unix seconds
        assert params["token"] == "test_key"

    def test_finnhub_client_next_cursor(self):
        """Test FinnhubClient handles pagination (should return None)."""
        from marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
        
        config = ClientConfig(api_key="test_key", base_url="https://finnhub.io/api/v1")
        auth = HeaderTokenAuth("test_key", "")
        client = FinnhubClient(config=config, auth=auth)

        # Finnhub doesn't use pagination for candle endpoint
        cursor = client.next_cursor({"s": "ok"})
        assert cursor is None

    def test_finnhub_client_parse_response_success(self):
        """Test FinnhubClient parses successful response correctly."""
        from marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
        
        config = ClientConfig(api_key="test_key", base_url="https://finnhub.io/api/v1")
        auth = HeaderTokenAuth("test_key", "")
        client = FinnhubClient(config=config, auth=auth)

        # Mock Finnhub response
        raw_response = {
            "c": [150.0, 151.0],  # close prices
            "h": [151.5, 152.0],  # high prices
            "l": [149.0, 150.0],  # low prices
            "o": [150.5, 150.2],  # open prices
            "s": "ok",
            "t": [1640995800, 1640995860],  # timestamps
            "v": [1000, 1500],  # volumes
        }

        rows = client.parse_response(raw_response)

        assert len(rows) == 2
        
        # Check first row
        row1 = rows[0]
        assert row1["timestamp"] == 1640995800 * 1_000_000_000  # nanoseconds
        assert row1["open"] == 150.5
        assert row1["high"] == 151.5
        assert row1["low"] == 149.0
        assert row1["close"] == 150.0
        assert row1["volume"] == 1000
        assert row1["source"] == "finnhub"
        assert row1["schema_version"] == 1

    def test_finnhub_client_parse_response_no_data(self):
        """Test FinnhubClient handles no data response."""
        from marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
        
        config = ClientConfig(api_key="test_key", base_url="https://finnhub.io/api/v1")
        auth = HeaderTokenAuth("test_key", "")
        client = FinnhubClient(config=config, auth=auth)

        # Mock no data response
        raw_response = {"s": "no_data"}

        rows = client.parse_response(raw_response)
        assert len(rows) == 0

    def test_finnhub_client_should_retry(self):
        """Test FinnhubClient retry logic."""
        from marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
        
        config = ClientConfig(api_key="test_key", base_url="https://finnhub.io/api/v1")
        auth = HeaderTokenAuth("test_key", "")
        client = FinnhubClient(config=config, auth=auth)

        # Should retry on 429, 500, 502, 503, 504
        assert client.should_retry(429, {}) is True
        assert client.should_retry(500, {}) is True
        assert client.should_retry(502, {}) is True
        assert client.should_retry(503, {}) is True
        assert client.should_retry(504, {}) is True

        # Should retry on 403 with rate limit message
        assert client.should_retry(403, {"error": "too many requests"}) is True

        # Should not retry on no_data response
        assert client.should_retry(200, {"s": "no_data"}) is False

        # Should not retry on 404
        assert client.should_retry(404, {}) is False


def test_finnhub_provider_registration():
    """Test that FinnhubMarketDataAdapter is registered with provider registry."""
    from marketpipe.ingestion.infrastructure.provider_registry import list_providers, get
    
    # Import the adapter to trigger registration
    from marketpipe.ingestion.infrastructure.adapters import FinnhubMarketDataAdapter
    
    # Check that finnhub is in the list of providers
    providers = list_providers()
    assert "finnhub" in providers
    
    # Check that we can get the provider class
    provider_cls = get("finnhub")
    assert provider_cls == FinnhubMarketDataAdapter


if __name__ == "__main__":
    pytest.main([__file__]) 