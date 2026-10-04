"""Real HTTP requests exercise provider response contracts and bounded failures."""

from __future__ import annotations

import time
from datetime import datetime, timezone

import httpx
import pytest

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.infrastructure.alpaca_client import AlpacaClient
from marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
from marketpipe.ingestion.infrastructure.models import ClientConfig
from marketpipe.ingestion.infrastructure.polygon_adapter import PolygonMarketDataAdapter
from marketpipe.ingestion.infrastructure.rate_limit import RateLimiter

pytestmark = pytest.mark.e2e

BAR = {"t": "2024-01-15T14:30:00Z", "o": 100, "h": 101, "l": 99, "c": 100, "v": 10}
START_MS = 1705329000000
END_MS = 1705329120000


def alpaca(url, *, retries=0, timeout=1):
    return AlpacaClient(
        config=ClientConfig(
            api_key="contract-key", base_url=url, max_retries=retries, timeout=timeout
        ),
        auth=HeaderTokenAuth("contract-key", "contract-secret"),
        rate_limiter=RateLimiter(capacity=60000, refill_rate=1000),
    )


def test_alpaca_rejects_repeated_pagination_cursor(http_server):
    url, responses, requests = http_server
    responses.extend([(200, {}, {"bars": {"AAPL": [BAR]}, "next_page_token": "repeated"})] * 2)
    with pytest.raises(RuntimeError, match="pagination"):
        alpaca(url).fetch_batch("AAPL", START_MS, END_MS)
    assert len(requests) == 2


def test_alpaca_rejects_malformed_success_response(http_server):
    url, responses, _requests = http_server
    responses.append((200, {}, {"unexpected": "no bars envelope"}))
    with pytest.raises(ValueError, match="bars"):
        alpaca(url).fetch_batch("AAPL", START_MS, END_MS)


def test_alpaca_pagination_preserves_unique_ordered_bars(http_server):
    url, responses, requests = http_server
    second = dict(BAR, t="2024-01-15T14:31:00Z", v=20)
    responses.extend(
        [
            (200, {}, {"bars": {"AAPL": [BAR, second]}, "next_page_token": "second"}),
            (200, {}, {"bars": {"AAPL": [second]}, "next_page_token": None}),
        ]
    )
    result = alpaca(url).fetch_batch("AAPL", START_MS, END_MS)
    assert [row["volume"] for row in result] == [10, 20]
    assert requests[1][1]["page_token"] == ["second"]


@pytest.mark.parametrize("status", [401, 403])
def test_alpaca_authorization_failure_is_not_retried_or_leaked(http_server, caplog, status):
    url, responses, requests = http_server
    responses.append((status, {}, {"message": "contract-key contract-secret"}))
    with pytest.raises(RuntimeError) as error:
        alpaca(url, retries=1).fetch_batch("AAPL", START_MS, END_MS)
    assert len(requests) == 1
    for secret in ("contract-key", "contract-secret"):
        assert secret not in str(error.value) + caplog.text


@pytest.mark.parametrize("status", [429, 503])
def test_alpaca_transient_failure_recovers_with_bounded_retries(http_server, status):
    url, responses, requests = http_server
    responses.extend([(status, {"Retry-After": "0"}, {}), (200, {}, {"bars": {"AAPL": [BAR]}})])
    assert len(alpaca(url, retries=1).fetch_batch("AAPL", START_MS, END_MS)) == 1
    assert len(requests) == 2


def test_alpaca_read_timeout_propagates(http_server):
    url, responses, requests = http_server

    def slow_response(_parsed, _query):
        time.sleep(0.15)
        return 200, {}, {"bars": {"AAPL": [BAR]}}

    responses.append(slow_response)
    with pytest.raises(httpx.ReadTimeout):
        alpaca(url, timeout=0.02).fetch_batch("AAPL", START_MS, END_MS)
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_polygon_range_is_half_open_and_pages_are_deduplicated(http_server):
    url, responses, requests = http_server

    def bar(timestamp, volume):
        return {"t": timestamp, "o": 100, "h": 101, "l": 99, "c": 100, "v": volume}

    responses.extend(
        [
            (
                200,
                {},
                {"status": "OK", "results": [bar(START_MS, 10)], "next_url": url + "/?cursor=next"},
            ),
            (200, {}, {"status": "OK", "results": [bar(START_MS, 10), bar(END_MS, 20)]}),
        ]
    )
    adapter = PolygonMarketDataAdapter(
        "polygon-contract-key", base_url=url, rate_limit_per_minute=60000
    )
    time_range = TimeRange(
        Timestamp(datetime.fromtimestamp(START_MS / 1000, timezone.utc)),
        Timestamp(datetime.fromtimestamp(END_MS / 1000, timezone.utc)),
    )
    result = await adapter.fetch_bars_for_symbol(Symbol("AAPL"), time_range)
    assert [bar.volume.value for bar in result] == [10]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_polygon_rejects_malformed_results(http_server):
    url, responses, _requests = http_server
    responses.append((200, {}, {"status": "OK", "results": {"invalid": "shape"}}))
    adapter = PolygonMarketDataAdapter(
        "polygon-contract-key", base_url=url, rate_limit_per_minute=60000
    )
    time_range = TimeRange(
        Timestamp(datetime(2024, 1, 15, tzinfo=timezone.utc)),
        Timestamp(datetime(2024, 1, 16, tzinfo=timezone.utc)),
    )
    with pytest.raises(ValueError, match="results"):
        await adapter.fetch_bars_for_symbol(Symbol("AAPL"), time_range)


def test_alpaca_excludes_end_boundary_and_orders_overlapping_pages(http_server):
    url, responses, _requests = http_server
    responses.append(
        (
            200,
            {},
            {
                "bars": {
                    "AAPL": [
                        dict(BAR, t="2024-01-15T14:32:00Z", v=30),
                        dict(BAR, t="2024-01-15T14:31:00Z", v=20),
                        BAR,
                    ]
                }
            },
        )
    )
    assert [row["volume"] for row in alpaca(url).fetch_batch("AAPL", START_MS, END_MS)] == [10, 20]


@pytest.mark.asyncio
async def test_alpaca_async_rejects_repeated_cursor(http_server):
    url, responses, requests = http_server
    responses.extend([(200, {}, {"bars": {"AAPL": [BAR]}, "next_page_token": "repeat"})] * 2)
    with pytest.raises(RuntimeError, match="pagination"):
        await alpaca(url).async_fetch_batch("AAPL", START_MS, END_MS)
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_polygon_does_not_skip_malformed_bar_in_successful_response(http_server):
    url, responses, _requests = http_server
    responses.append((200, {}, {"status": "OK", "results": [{"t": START_MS}]}))
    adapter = PolygonMarketDataAdapter("contract-key", base_url=url, rate_limit_per_minute=60000)
    time_range = TimeRange(
        Timestamp(datetime(2024, 1, 15, tzinfo=timezone.utc)),
        Timestamp(datetime(2024, 1, 16, tzinfo=timezone.utc)),
    )
    with pytest.raises(ValueError, match="bar"):
        await adapter.fetch_bars_for_symbol(Symbol("AAPL"), time_range)


def test_alpaca_continues_after_empty_page_with_cursor(http_server):
    url, responses, requests = http_server
    responses.extend(
        [
            (200, {}, {"bars": {}, "next_page_token": "after-empty"}),
            (200, {}, {"bars": {"AAPL": [BAR]}}),
        ]
    )
    assert [row["volume"] for row in alpaca(url).fetch_batch("AAPL", START_MS, END_MS)] == [10]
    assert len(requests) == 2


def test_alpaca_closed_local_endpoint_propagates_connection_failure(http_server):
    import socket

    # A bound, non-listening socket prevents another process from taking the port.
    with socket.socket() as endpoint:
        endpoint.bind(("127.0.0.1", 0))
        url = f"http://127.0.0.1:{endpoint.getsockname()[1]}"
        with pytest.raises(httpx.ConnectError):
            alpaca(url, timeout=0.1).fetch_batch("AAPL", START_MS, END_MS)
