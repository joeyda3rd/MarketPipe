# SPDX-License-Identifier: Apache-2.0
"""Test Finnhub provider registration and basic functionality."""

from __future__ import annotations

import pytest
import types
import asyncio
from unittest.mock import patch

from src.marketpipe.ingestion.infrastructure.provider_registry import get_provider_registry
from src.marketpipe.ingestion.infrastructure.adapters import FinnhubMarketDataAdapter
from src.marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
from src.marketpipe.ingestion.infrastructure.models import ClientConfig
from src.marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
from src.marketpipe.ingestion.infrastructure.rate_limit import DualRateLimiter


def test_finnhub_provider_registered():
    """Test that FinnhubMarketDataAdapter is registered with provider registry."""
    registry = get_provider_registry()
    providers = registry.get_all_providers()
    
    assert "finnhub" in providers
    assert providers["finnhub"] == FinnhubMarketDataAdapter


def test_finnhub_adapter_created_from_config():
    """Test that FinnhubMarketDataAdapter can be created from configuration."""
    config = {
        "provider": "finnhub",
        "api_key": "test_api_key",
        "base_url": "https://finnhub.io/api/v1",
        "rate_limit_per_min": 60,
        "rate_limit_per_sec": 30,
    }
    
    adapter = FinnhubMarketDataAdapter.from_config(config)
    
    assert isinstance(adapter, FinnhubMarketDataAdapter)
    assert adapter._api_key == "test_api_key"
    assert adapter._base_url == "https://finnhub.io/api/v1"
    # Verify dual rate limiter is created
    assert isinstance(adapter._rate_limiter, DualRateLimiter)


def test_finnhub_client_functionality(monkeypatch):
    """Test FinnhubClient basic functionality with enhanced features."""
    
    # Mock response with proper Finnhub format including status
    mock_response = {
        "c": [100.5, 101.0],     # closes
        "h": [101.0, 101.5],     # highs  
        "l": [99.5, 100.0],      # lows
        "o": [100.0, 100.5],     # opens
        "t": [1640995800, 1640995860],  # timestamps (Unix seconds)
        "v": [1000, 1500],       # volumes
        "s": "ok"                # status - important for new logic
    }
    
    headers_seen = []
    
    def mock_get(url, params=None, headers=None, timeout=None):
        headers_seen.append(headers)
        return types.SimpleNamespace(
            status_code=200, 
            json=lambda: mock_response,
            text=str(mock_response),
            headers={}
        )
    
    monkeypatch.setattr("httpx.get", mock_get)
    
    config = ClientConfig(api_key="test_key", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test_key", "")
    client = FinnhubClient(config=config, auth=auth)
    
    rows = client.fetch_batch("AAPL", 0, 1000)
    
    # Verify header-based authentication is used
    assert len(headers_seen) > 0
    assert headers_seen[0]["X-Finnhub-Token"] == "test_key"
    
    # Verify response parsing with status handling
    assert len(rows) == 2
    assert all(row["source"] == "finnhub" for row in rows)
    assert all(row["schema_version"] == 1 for row in rows)
    assert rows[0]["symbol"] == "UNKNOWN"  # Finnhub doesn't echo symbol


def test_finnhub_client_parameter_building():
    """Test FinnhubClient parameter building with symbol handling."""
    config = ClientConfig(api_key="test", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test", "")
    client = FinnhubClient(config=config, auth=auth)
    
    # Test regular symbol
    params = client.build_request_params("AAPL", 1640995800_000_000_000, 1640999400_000_000_000)
    expected = {
        "symbol": "AAPL",
        "resolution": "1",
        "from": "1640995800",
        "to": "1640999400",
    }
    assert params == expected
    
    # Test symbol with dot notation (e.g., BRK.B)
    params_dot = client.build_request_params("BRK.B", 1640995800_000_000_000, 1640999400_000_000_000)
    expected_dot = {
        "symbol": "BRK.B",  # Should preserve dot
        "resolution": "1",
        "from": "1640995800", 
        "to": "1640999400",
    }
    assert params_dot == expected_dot


def test_finnhub_adapter_functionality(monkeypatch):
    """Test FinnhubMarketDataAdapter functionality with improvements."""
    
    # Mock successful response
    def mock_async_fetch_batch(symbol, start_ns, end_ns):
        return [
            {
                "symbol": symbol,
                "timestamp": 1640995800_000_000_000,
                "date": "2022-01-01",
                "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
                "volume": 1000, "schema_version": 1, "source": "finnhub",
            }
        ]
    
    adapter = FinnhubMarketDataAdapter(
        api_key="test_key",
        rate_limit_per_min=60,
        rate_limit_per_sec=30
    )
    
    # Mock the client method
    adapter._finnhub_client.async_fetch_batch = mock_async_fetch_batch
    
    # Test provider metadata includes dual rate limiting info
    metadata = adapter.get_provider_metadata()
    assert metadata.provider_name == "finnhub"
    assert metadata.rate_limit_per_minute == 60
    assert metadata.supports_real_time is True
    assert metadata.supports_historical is True


def test_finnhub_status_handling():
    """Test Finnhub-specific status field handling."""
    config = ClientConfig(api_key="test", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test", "")
    client = FinnhubClient(config=config, auth=auth)
    
    # Test "ok" status - should parse normally
    ok_response = {
        "c": [100.5], "h": [101.0], "l": [99.5], "o": [100.0], 
        "t": [1640995800], "v": [1000], "s": "ok"
    }
    rows = client.parse_response(ok_response)
    assert len(rows) == 1
    
    # Test "no_data" status - should return empty list silently
    no_data_response = {"s": "no_data"}
    rows = client.parse_response(no_data_response)
    assert len(rows) == 0
    
    # Test "error" status - should raise exception
    error_response = {"s": "error"}
    with pytest.raises(RuntimeError, match="Finnhub API error status: error"):
        client.parse_response(error_response)


def test_finnhub_retry_after_handling(monkeypatch):
    """Test enhanced Retry-After header handling for 429 responses."""
    call_count = 0
    
    def mock_get(url, params=None, headers=None, timeout=None):
        nonlocal call_count
        call_count += 1
        
        if call_count == 1:
            # First call returns 429 with Retry-After header
            return types.SimpleNamespace(
                status_code=429,
                json=lambda: {"error": "too many requests"},
                text="rate limit exceeded",
                headers={"Retry-After": "2.5"}  # Custom retry-after value
            )
        else:
            # Second call succeeds
            return types.SimpleNamespace(
                status_code=200,
                json=lambda: {
                    "c": [100.5], "h": [101.0], "l": [99.5], "o": [100.0],
                    "t": [1640995800], "v": [1000], "s": "ok"
                },
                text="success",
                headers={}
            )
    
    monkeypatch.setattr("httpx.get", mock_get)
    
    # Mock sleep to capture the retry delay
    sleep_calls = []
    monkeypatch.setattr("time.sleep", lambda s: sleep_calls.append(s))
    
    config = ClientConfig(api_key="test", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test", "")
    client = FinnhubClient(config=config, auth=auth)
    
    rows = client.fetch_batch("AAPL", 0, 1000)
    
    assert len(rows) == 1
    assert call_count == 2  # Initial call + 1 retry
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == 2.5  # Should respect Retry-After header value


def test_finnhub_date_chunking_logic():
    """Test that large date ranges are properly chunked into 30-day segments."""
    config = ClientConfig(api_key="test", base_url="https://finnhub.io/api/v1") 
    auth = HeaderTokenAuth("test", "")
    client = FinnhubClient(config=config, auth=auth)
    
    # Test date range > 30 days (should trigger chunking)
    # 90 days = ~3 chunks
    start_ts = 1640995800_000_000_000  # 2022-01-01
    end_ts = 1648771800_000_000_000    # 2022-04-01 (90 days later)
    
    # Calculate expected chunks
    from datetime import datetime, timezone
    start_dt = datetime.fromtimestamp(start_ts / 1_000_000_000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts / 1_000_000_000, tz=timezone.utc)
    total_days = (end_dt - start_dt).days
    
    # Should be chunked since > 30 days
    assert total_days > client._MAX_DAYS_PER_REQUEST
    
    # Mock paginate to track how many chunks are requested
    chunks_requested = []
    original_paginate = client.paginate
    
    def mock_paginate(symbol, start_ns, end_ns):
        chunks_requested.append((symbol, start_ns, end_ns))
        return []  # Return empty to avoid actual HTTP calls
    
    client.paginate = mock_paginate
    
    # This should trigger chunking
    client.fetch_batch("AAPL", start_ts, end_ts)
    
    # Verify multiple chunks were requested
    assert len(chunks_requested) > 1  # Should be split into multiple chunks
    assert all(chunk[0] == "AAPL" for chunk in chunks_requested)  # All for same symbol


if __name__ == "__main__":
    pytest.main([__file__]) 