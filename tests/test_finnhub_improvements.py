"""Tests for Finnhub API improvements without external dependencies."""

import types
import json
from datetime import datetime, timezone
from unittest.mock import patch, Mock

import pytest


def test_finnhub_date_range_handling():
    """Test that FinnhubClient handles date ranges correctly."""
    # Import only what we need to avoid metrics issues
    from src.marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
    from src.marketpipe.ingestion.infrastructure.models import ClientConfig
    from src.marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
    
    config = ClientConfig(api_key="test", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test", "")
    client = FinnhubClient(config=config, auth=auth, rate_limiter=None)
    
    # Test date range: 2025-06-16 to 2025-06-17 (1 day)
    start_ns = 1750032000000000000  # 2025-06-16T00:00:00Z
    end_ns = 1750118400000000000    # 2025-06-17T00:00:00Z
    
    # Test parameter building
    params = client.build_request_params("AAPL", start_ns, end_ns)
    
    # Verify correct Unix timestamp conversion
    expected_start_unix = 1750032000  # start_ns // 1_000_000_000
    expected_end_unix = 1750118400    # end_ns // 1_000_000_000
    
    assert params["symbol"] == "AAPL"
    assert params["resolution"] == "1"
    assert params["from"] == str(expected_start_unix)
    assert params["to"] == str(expected_end_unix)
    
    # Verify dates are correct
    start_dt = datetime.fromtimestamp(expected_start_unix, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(expected_end_unix, tz=timezone.utc)
    
    assert start_dt.date().isoformat() == "2025-06-16"
    assert end_dt.date().isoformat() == "2025-06-17"


def test_finnhub_response_parsing():
    """Test Finnhub response parsing with simulated API response."""
    from src.marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
    from src.marketpipe.ingestion.infrastructure.models import ClientConfig
    from src.marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
    
    config = ClientConfig(api_key="test", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test", "")
    client = FinnhubClient(config=config, auth=auth, rate_limiter=None)
    
    # Simulate Finnhub API response format
    mock_response = {
        "s": "ok",
        "c": [150.25, 151.00, 149.75],  # Close prices
        "h": [151.50, 152.00, 150.50],  # High prices
        "l": [149.00, 150.00, 148.75],  # Low prices
        "o": [150.00, 150.50, 149.50],  # Open prices
        "t": [1750032000, 1750032060, 1750032120],  # Unix timestamps
        "v": [1000, 1500, 1200]  # Volumes
    }
    
    # Parse response
    rows = client.parse_response(mock_response)
    
    # Verify parsing
    assert len(rows) == 3
    
    # Check first bar
    bar1 = rows[0]
    assert bar1["symbol"] == "UNKNOWN"  # Finnhub doesn't echo symbol
    assert bar1["open"] == 150.00
    assert bar1["high"] == 151.50
    assert bar1["low"] == 149.00
    assert bar1["close"] == 150.25
    assert bar1["volume"] == 1000
    assert bar1["timestamp"] == 1750032000000000000  # Converted to nanoseconds
    assert bar1["date"] == "2025-06-16"
    assert bar1["source"] == "finnhub"
    assert bar1["schema_version"] == 1


def test_finnhub_status_handling():
    """Test Finnhub status field handling."""
    from src.marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
    from src.marketpipe.ingestion.infrastructure.models import ClientConfig
    from src.marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
    
    config = ClientConfig(api_key="test", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test", "")
    client = FinnhubClient(config=config, auth=auth, rate_limiter=None)
    
    # Test "no_data" status - should return empty list
    no_data_response = {"s": "no_data"}
    rows = client.parse_response(no_data_response)
    assert rows == []
    
    # Test "error" status - should raise exception
    error_response = {"s": "error", "error": "Invalid symbol"}
    with pytest.raises(RuntimeError, match="Finnhub API error status: error"):
        client.parse_response(error_response)


def test_finnhub_chunking_logic():
    """Test that small date ranges don't get chunked."""
    from src.marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
    from src.marketpipe.ingestion.infrastructure.models import ClientConfig
    from src.marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
    
    config = ClientConfig(api_key="test", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test", "")
    client = FinnhubClient(config=config, auth=auth, rate_limiter=None)
    
    # Mock successful API response
    mock_response = {
        "s": "ok",
        "c": [150.25], "h": [151.50], "l": [149.00], "o": [150.00],
        "t": [1750032000], "v": [1000]
    }
    
    def mock_get(url, params=None, headers=None, timeout=None):
        return types.SimpleNamespace(
            status_code=200,
            json=lambda: mock_response,
            text=json.dumps(mock_response),
            headers={}
        )
    
    # Test 1-day range (should not chunk)
    start_ns = 1750032000000000000  # 2025-06-16T00:00:00Z
    end_ns = 1750118400000000000    # 2025-06-17T00:00:00Z
    
    with patch('httpx.get', mock_get):
        rows = client.fetch_batch("AAPL", start_ns, end_ns)
    
    # Should return data without chunking
    assert len(rows) == 1
    assert rows[0]["date"] == "2025-06-16"


def test_finnhub_auth_header():
    """Test that Finnhub uses header-based authentication."""
    from src.marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
    from src.marketpipe.ingestion.infrastructure.models import ClientConfig
    from src.marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
    
    config = ClientConfig(api_key="test-key-123", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test-key-123", "")
    client = FinnhubClient(config=config, auth=auth, rate_limiter=None)
    
    headers_captured = []
    
    def mock_get(url, params=None, headers=None, timeout=None):
        headers_captured.append(headers)
        return types.SimpleNamespace(
            status_code=200,
            json=lambda: {"s": "ok", "c": [], "h": [], "l": [], "o": [], "t": [], "v": []},
            text="{}",
            headers={}
        )
    
    start_ns = 1750032000000000000
    end_ns = 1750118400000000000
    
    with patch('httpx.get', mock_get):
        client.fetch_batch("AAPL", start_ns, end_ns)
    
    # Verify X-Finnhub-Token header is used
    assert len(headers_captured) > 0
    headers = headers_captured[0]
    assert "X-Finnhub-Token" in headers
    assert headers["X-Finnhub-Token"] == "test-key-123"


def test_finnhub_symbol_preservation():
    """Test that symbol is added to response if Finnhub omits it."""
    from src.marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
    from src.marketpipe.ingestion.infrastructure.models import ClientConfig
    from src.marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
    
    config = ClientConfig(api_key="test", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test", "")
    client = FinnhubClient(config=config, auth=auth, rate_limiter=None)
    
    # Response without symbol field (typical for Finnhub)
    response_without_symbol = {
        "s": "ok",
        "c": [150.25], "h": [151.50], "l": [149.00], "o": [150.00],
        "t": [1750032000], "v": [1000]
        # Note: no "symbol" field
    }
    
    rows = client.parse_response(response_without_symbol)
    
    # Should add "UNKNOWN" as default symbol
    assert len(rows) == 1
    assert rows[0]["symbol"] == "UNKNOWN"
    
    # Response with symbol field
    response_with_symbol = {
        "s": "ok",
        "symbol": "AAPL",
        "c": [150.25], "h": [151.50], "l": [149.00], "o": [150.00],
        "t": [1750032000], "v": [1000]
    }
    
    rows = client.parse_response(response_with_symbol)
    
    # Should use provided symbol
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"


def test_finnhub_retry_logic():
    """Test Finnhub-specific retry conditions."""
    from src.marketpipe.ingestion.infrastructure.finnhub_client import FinnhubClient
    from src.marketpipe.ingestion.infrastructure.models import ClientConfig
    from src.marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
    
    config = ClientConfig(api_key="test", base_url="https://finnhub.io/api/v1")
    auth = HeaderTokenAuth("test", "")
    client = FinnhubClient(config=config, auth=auth, rate_limiter=None)
    
    # Test standard retry conditions
    assert client.should_retry(429, {}) == True  # Rate limit
    assert client.should_retry(500, {}) == True  # Server error
    assert client.should_retry(503, {}) == True  # Service unavailable
    
    # Test non-retry conditions
    assert client.should_retry(200, {}) == False  # Success
    assert client.should_retry(404, {}) == False  # Not found
    assert client.should_retry(401, {}) == False  # Unauthorized
    
    # Test Finnhub-specific conditions
    assert client.should_retry(403, {"error": "rate limit exceeded"}) == True
    assert client.should_retry(403, {"error": "invalid api key"}) == False
    
    # Test Finnhub status responses
    assert client.should_retry(200, {"s": "error"}) == False  # Don't retry API errors


if __name__ == "__main__":
    # Run all tests
    test_finnhub_date_range_handling()
    test_finnhub_response_parsing()
    test_finnhub_status_handling()
    test_finnhub_chunking_logic()
    test_finnhub_auth_header()
    test_finnhub_symbol_preservation()
    test_finnhub_retry_logic()
    print("✅ All Finnhub improvement tests passed!") 