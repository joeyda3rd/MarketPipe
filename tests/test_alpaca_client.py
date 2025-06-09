"""Legacy Alpaca client tests - maintaining backward compatibility."""

import types

import httpx
import pytest
import asyncio

from marketpipe.ingestion.infrastructure.alpaca_client import AlpacaClient
from marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
from marketpipe.ingestion.infrastructure.models import ClientConfig


def test_legacy_alpaca_client_handles_symbol_data_pagination(monkeypatch):
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

    rows = client.fetch_batch("AAPL", 0, 1)

    assert len(rows) == 2
    assert headers_seen[0]["APCA-API-KEY-ID"] == "id"
    assert all(r["schema_version"] == 1 for r in rows)

def test_legacy_alpaca_client_supports_async_symbol_data_retrieval(monkeypatch):
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

    rows = asyncio.run(client.async_fetch_batch("AAPL", 0, 1))
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
