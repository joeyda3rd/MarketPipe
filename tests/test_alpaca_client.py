# SPDX-License-Identifier: Apache-2.0
"""Legacy Alpaca client tests - maintaining backward compatibility."""

import types

import httpx
import asyncio

from marketpipe.ingestion.infrastructure.alpaca_client import AlpacaClient
from marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
from marketpipe.ingestion.infrastructure.models import ClientConfig


def test_legacy_alpaca_client_handles_symbol_data_pagination(monkeypatch):
    pages = [
        {
            "bars": [
                {
                    "S": "AAPL",
                    "t": "2023-01-02T09:30:00",
                    "o": 1,
                    "h": 2,
                    "l": 3,
                    "c": 4,
                    "v": 5,
                }
            ],
            "next_page_token": "abc",
        },
        {
            "bars": [
                {
                    "S": "AAPL",
                    "t": "2023-01-02T09:31:00",
                    "o": 1,
                    "h": 2,
                    "l": 3,
                    "c": 4,
                    "v": 5,
                }
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

    rows = client.fetch_batch("AAPL", 0, 1)

    assert len(rows) == 2
    assert headers_seen[0]["APCA-API-KEY-ID"] == "id"
    assert all(r["schema_version"] == 1 for r in rows)


def test_legacy_alpaca_client_supports_async_symbol_data_retrieval(monkeypatch):
    pages = [
        {
            "bars": [
                {
                    "S": "AAPL",
                    "t": "2023-01-02T09:30:00",
                    "o": 1,
                    "h": 2,
                    "l": 3,
                    "c": 4,
                    "v": 5,
                }
            ],
            "next_page_token": "abc",
        },
        {
            "bars": [
                {
                    "S": "AAPL",
                    "t": "2023-01-02T09:31:00",
                    "o": 1,
                    "h": 2,
                    "l": 3,
                    "c": 4,
                    "v": 5,
                }
            ]
        },
    ]
    headers_seen = []

    class DummyAsyncClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def get(self, url, params=None, headers=None):
            headers_seen.append(headers)
            body = pages.pop(0)
            return types.SimpleNamespace(status_code=200, json=lambda: body)

    monkeypatch.setattr(httpx, "AsyncClient", DummyAsyncClient)

    cfg = ClientConfig(api_key="k", base_url="http://x")
    auth = HeaderTokenAuth("id", "sec")
    client = AlpacaClient(config=cfg, auth=auth)

    # Use new event loop to avoid conflicts with pytest-asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        rows = loop.run_until_complete(client.async_fetch_batch("AAPL", 0, 1))
    finally:
        loop.close()
        
    assert len(rows) == 2
    assert headers_seen[0]["APCA-API-KEY-ID"] == "id"


def test_legacy_alpaca_client_retries_after_rate_limit_response(monkeypatch):
    """Test that legacy Alpaca client sleeps and retries after a 429 response."""
    calls = []

    def mock_get(url, params=None, headers=None, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            return types.SimpleNamespace(
                status_code=429,
                json=lambda: {"message": "too many"},
                text="rate limit",
            )
        body = {
            "bars": [
                {
                    "S": "AAPL",
                    "t": "2023-01-02T09:30:00",
                    "o": 1,
                    "h": 2,
                    "l": 3,
                    "c": 4,
                    "v": 5,
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

    cfg = ClientConfig(api_key="k", base_url="http://x")
    auth = HeaderTokenAuth("id", "sec")
    client = AlpacaClient(config=cfg, auth=auth)

    rows = client.fetch_batch("AAPL", 0, 1)

    assert len(rows) == 1
    assert len(calls) == 2
    assert len(sleeps) == 1


def test_alpaca_async(monkeypatch):
    """Test async client functionality."""
    pages = [
        {
            "bars": {"AAPL": [
                {"t": "2023-01-02T09:30:00Z", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100}
            ]},
            "next_page_token": "abc",
        },
        {
            "bars": {"AAPL": [
                {"t": "2023-01-02T09:31:00Z", "o": 1.5, "h": 2.1, "l": 1.0, "c": 2.0, "v": 150}
            ]}
        },
    ]
    headers_seen = []

    class DummyAsyncClient:
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

    monkeypatch.setattr(httpx, "AsyncClient", DummyAsyncClient)

    config = ClientConfig(api_key="test", base_url="https://api.test.com")
    auth = HeaderTokenAuth("id", "secret")
    client = AlpacaClient(config=config, auth=auth)

    # Use new event loop to avoid conflicts with pytest-asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        rows = loop.run_until_complete(client.async_fetch_batch("AAPL", 0, 1000))
    finally:
        loop.close()
        
    assert len(rows) == 2
    assert headers_seen[0]["APCA-API-KEY-ID"] == "id"
