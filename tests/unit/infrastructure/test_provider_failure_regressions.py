# SPDX-License-Identifier: Apache-2.0
"""Provider errors must not masquerade as successful, empty or partial downloads."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.infrastructure.alpaca_client import AlpacaClient
from marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
from marketpipe.ingestion.infrastructure.models import ClientConfig
from marketpipe.ingestion.infrastructure.polygon_adapter import PolygonMarketDataAdapter


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_alpaca_http_failure_is_reported_and_secrets_are_masked(asynchronous):
    response = httpx.Response(401, json={"message": "denied secret-value"})
    http_client = Mock()
    http_client.get.return_value = response
    async_client = Mock()
    async_client.get = AsyncMock(return_value=response)
    client = AlpacaClient(
        ClientConfig(api_key="key-value", base_url="https://example.invalid"),
        HeaderTokenAuth("key-value", "secret-value"),
        http_client=http_client,
        async_http_client=async_client,
    )
    with pytest.raises(RuntimeError, match="401") as error:
        if asynchronous:
            await client.async_fetch_batch("AAPL", 0, 1000)
        else:
            client.fetch_batch("AAPL", 0, 1000)
    assert "secret-value" not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_alpaca_retry_after_counts_toward_retry_limit(asynchronous):
    response = httpx.Response(429, json={"message": "rate limit"}, headers={"Retry-After": "1"})
    http_client = Mock()
    http_client.get.return_value = response
    async_client = Mock()
    async_client.get = AsyncMock(return_value=response)
    limiter = Mock()
    limiter.async_acquire = AsyncMock()
    limiter.notify_retry_after_async = AsyncMock()
    client = AlpacaClient(
        ClientConfig(api_key="key-value", base_url="https://example.invalid", max_retries=2),
        HeaderTokenAuth("key-value", "secret-value"),
        rate_limiter=limiter,
        http_client=http_client,
        async_http_client=async_client,
    )
    with pytest.raises(RuntimeError, match="retry limit"):
        if asynchronous:
            await client.async_fetch_batch("AAPL", 0, 1000)
        else:
            client.fetch_batch("AAPL", 0, 1000)
    request = async_client.get if asynchronous else http_client.get
    acquire = limiter.async_acquire if asynchronous else limiter.acquire
    assert request.call_count == 3
    assert acquire.call_count == 3


def polygon_page(next_url=None):
    page = {"results": [{"t": 1733097600000, "o": 100, "h": 101, "l": 99, "c": 100, "v": 1000}]}
    if next_url:
        page["next_url"] = next_url
    return page


def polygon_range():
    return TimeRange(
        Timestamp(datetime(2024, 12, 2, tzinfo=timezone.utc)),
        Timestamp(datetime(2024, 12, 3, tzinfo=timezone.utc)),
    )


@pytest.mark.asyncio
async def test_polygon_later_page_failure_does_not_return_partial_success():
    adapter = PolygonMarketDataAdapter("test-key")
    adapter._apply_rate_limit = AsyncMock()
    adapter._make_request = AsyncMock(
        side_effect=[polygon_page("https://example.invalid?cursor=page2"), OSError("unavailable")]
    )
    with pytest.raises(OSError, match="unavailable"):
        await adapter.fetch_bars_for_symbol(Symbol("AAPL"), polygon_range())


@pytest.mark.asyncio
async def test_polygon_decodes_cursor_once():
    adapter = PolygonMarketDataAdapter("test-key")
    adapter._apply_rate_limit = AsyncMock()
    adapter._make_request = AsyncMock(
        side_effect=[polygon_page("https://example.invalid?cursor=abc%2B%2F%3D"), polygon_page()]
    )
    await adapter.fetch_bars_for_symbol(Symbol("AAPL"), polygon_range())
    assert adapter._make_request.call_args_list[1].args[1]["cursor"] == "abc+/="


@pytest.mark.asyncio
async def test_polygon_repeated_cursor_fails_instead_of_looping():
    adapter = PolygonMarketDataAdapter("test-key")
    adapter._apply_rate_limit = AsyncMock()
    adapter._make_request = AsyncMock(return_value=polygon_page("https://example.invalid?cursor=x"))
    with pytest.raises(ValueError, match="repeated"):
        await adapter.fetch_bars_for_symbol(Symbol("AAPL"), polygon_range())
    assert adapter._make_request.call_count == 2


@pytest.mark.asyncio
async def test_polygon_debug_log_masks_api_key(caplog):
    adapter = PolygonMarketDataAdapter("sensitive-api-key")
    adapter._apply_rate_limit = AsyncMock()
    adapter._make_request = AsyncMock(return_value=polygon_page())
    with caplog.at_level("DEBUG"):
        await adapter.fetch_bars_for_symbol(Symbol("AAPL"), polygon_range())
    assert "sensitive-api-key" not in caplog.text


@pytest.mark.asyncio
async def test_polygon_records_time_after_rate_limit_sleep(monkeypatch):
    adapter = PolygonMarketDataAdapter("test-key", rate_limit_per_minute=1)
    clock = [0.0]
    monkeypatch.setattr(
        "marketpipe.ingestion.infrastructure.polygon_adapter.time.monotonic", lambda: clock[0]
    )

    async def sleep(seconds):
        clock[0] += seconds

    monkeypatch.setattr("marketpipe.ingestion.infrastructure.polygon_adapter.asyncio.sleep", sleep)
    for _ in range(3):
        await adapter._apply_rate_limit()
    assert clock[0] == 120.0
