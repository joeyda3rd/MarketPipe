"""Unit tests for Alpaca client pagination and retry functionality."""

from __future__ import annotations

import types
import asyncio
from unittest.mock import MagicMock

import httpx
import pytest

from marketpipe.ingestion.infrastructure.alpaca_client import AlpacaClient
from marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
from marketpipe.ingestion.infrastructure.models import ClientConfig


class TestAlpacaClientPaginationHandling:
    """Test Alpaca client handles pagination correctly."""
    
    def test_client_handles_multiple_pages_for_symbol_data_retrieval(self, monkeypatch):
        """Test that client handles multiple pages when retrieving symbol data."""
        pages = [
            {
                "bars": [
                    {"S": "AAPL", "t": "2023-01-02T09:30:00", "o": 1, "h": 2, "l": 3, "c": 4, "v": 5}
                ],
                "next_page_token": "abc",
            },
            {
                "bars": [
                    {"S": "AAPL", "t": "2023-01-02T09:31:00", "o": 1, "h": 2, "l": 3, "c": 4, "v": 5}
                ]
            },
        ]

        headers_seen = []

        def mock_get(url, params=None, headers=None, timeout=None):
            headers_seen.append(headers)
            body = pages.pop(0)
            return types.SimpleNamespace(status_code=200, json=lambda: body, text=str(body))

        monkeypatch.setattr(httpx, "get", mock_get)

        cfg = ClientConfig(api_key="k", base_url="http://x")
        auth = HeaderTokenAuth("id", "sec")
        client = AlpacaClient(config=cfg, auth=auth)

        bars = client.fetch_batch("AAPL", 0, 1)

        assert len(bars) == 2
        assert headers_seen[0]["APCA-API-KEY-ID"] == "id"
        assert all(bar["schema_version"] == 1 for bar in bars)
    
    def test_client_processes_paginated_symbol_data_in_correct_order(self, monkeypatch):
        """Test that paginated symbol data is processed in correct chronological order."""
        pages = [
            {
                "bars": [
                    {"S": "AAPL", "t": "2023-01-02T09:30:00", "o": 100, "h": 102, "l": 99, "c": 101, "v": 1000}
                ],
                "next_page_token": "page2",
            },
            {
                "bars": [
                    {"S": "AAPL", "t": "2023-01-02T09:31:00", "o": 101, "h": 103, "l": 100, "c": 102, "v": 1500}
                ]
            },
        ]

        def mock_get(url, params=None, headers=None, timeout=None):
            body = pages.pop(0)
            return types.SimpleNamespace(status_code=200, json=lambda: body, text=str(body))

        monkeypatch.setattr(httpx, "get", mock_get)

        cfg = ClientConfig(api_key="test_key", base_url="http://test")
        auth = HeaderTokenAuth("test_id", "test_secret")
        client = AlpacaClient(config=cfg, auth=auth)

        bars = client.fetch_batch("AAPL", 0, 1)

        # Verify we got both pages of data
        assert len(bars) == 2
        
        # Verify the data is in expected order
        assert bars[0]["t"] == "2023-01-02T09:30:00"
        assert bars[1]["t"] == "2023-01-02T09:31:00"
        
        # Verify symbol and schema version are set correctly
        assert all(bar["symbol"] == "AAPL" for bar in bars)
        assert all(bar["schema_version"] == 1 for bar in bars)


class TestAlpacaClientAsyncOperations:
    """Test Alpaca client async operations work correctly."""
    
    def test_async_client_retrieves_symbol_data_correctly(self, monkeypatch):
        """Test that async client retrieves symbol data correctly."""
        pages = [
            {
                "bars": [{"S": "AAPL", "t": "2023-01-02T09:30:00", "o": 1, "h": 2, "l": 3, "c": 4, "v": 5}],
                "next_page_token": "abc",
            },
            {
                "bars": [{"S": "AAPL", "t": "2023-01-02T09:31:00", "o": 1, "h": 2, "l": 3, "c": 4, "v": 5}]
            },
        ]
        headers_seen = []

        class MockAsyncClient:
            def __init__(self, *args, **kwargs):
                pass
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc, tb):
                pass
            async def get(self, url, params=None, headers=None):
                headers_seen.append(headers)
                body = pages.pop(0)
                return types.SimpleNamespace(status_code=200, json=lambda: body)

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        cfg = ClientConfig(api_key="k", base_url="http://x")
        auth = HeaderTokenAuth("id", "sec")
        client = AlpacaClient(config=cfg, auth=auth)

        bars = asyncio.run(client.async_fetch_batch("AAPL", 0, 1))
        assert len(bars) == 2
        assert headers_seen[0]["APCA-API-KEY-ID"] == "id"


class TestAlpacaClientRateLimitHandling:
    """Test Alpaca client handles rate limits correctly."""
    
    def test_client_retries_after_rate_limit_response_from_alpaca(self, monkeypatch):
        """Test that client retries after receiving rate limit response from Alpaca."""
        calls = []

        def mock_get(url, params=None, headers=None, timeout=None):
            calls.append(1)
            if len(calls) == 1:
                return types.SimpleNamespace(
                    status_code=429,
                    json=lambda: {"message": "rate limit exceeded"},
                    text="rate limit",
                )
            body = {
                "bars": [
                    {
                        "S": "AAPL",
                        "t": "2023-01-02T09:30:00",
                        "o": 100,
                        "h": 102,
                        "l": 99,
                        "c": 101,
                        "v": 1000,
                    }
                ]
            }
            return types.SimpleNamespace(status_code=200, json=lambda: body, text=str(body))

        monkeypatch.setattr(httpx, "get", mock_get)

        sleeps = []
        monkeypatch.setattr("time.sleep", lambda s: sleeps.append(s))
        monkeypatch.setattr(
            "marketpipe.ingestion.infrastructure.alpaca_client.AlpacaClient._backoff",
            lambda self, attempt: 0.01,
        )

        cfg = ClientConfig(api_key="test_key", base_url="http://test")
        auth = HeaderTokenAuth("test_id", "test_secret")
        client = AlpacaClient(config=cfg, auth=auth)

        bars = client.fetch_batch("AAPL", 0, 1)

        assert len(bars) == 1
        assert len(calls) == 2  # First call failed, second succeeded
        assert len(sleeps) == 1  # One sleep between retries
    
    def test_client_applies_exponential_backoff_for_rate_limit_retries(self, monkeypatch):
        """Test that client applies exponential backoff for rate limit retries."""
        attempt_count = 0
        
        def mock_get(url, params=None, headers=None, timeout=None):
            nonlocal attempt_count
            attempt_count += 1
            
            if attempt_count <= 2:  # First two attempts fail
                return types.SimpleNamespace(
                    status_code=429,
                    json=lambda: {"message": "rate limit exceeded"},
                    text="rate limit",
                )
            
            # Third attempt succeeds
            body = {
                "bars": [
                    {"S": "AAPL", "t": "2023-01-02T09:30:00", "o": 100, "h": 101, "l": 99, "c": 100.5, "v": 1000}
                ]
            }
            return types.SimpleNamespace(status_code=200, json=lambda: body, text=str(body))

        monkeypatch.setattr(httpx, "get", mock_get)

        # Track sleep calls to verify backoff
        sleep_durations = []
        monkeypatch.setattr("time.sleep", lambda s: sleep_durations.append(s))
        
        # Use actual backoff calculation
        cfg = ClientConfig(api_key="test_key", base_url="http://test")
        auth = HeaderTokenAuth("test_id", "test_secret")
        client = AlpacaClient(config=cfg, auth=auth)

        bars = client.fetch_batch("AAPL", 0, 1)

        # Verify successful retrieval after retries
        assert len(bars) == 1
        assert attempt_count == 3
        
        # Verify backoff occurred (should have 2 sleeps for 2 retries)
        assert len(sleep_durations) == 2