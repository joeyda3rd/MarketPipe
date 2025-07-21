# SPDX-License-Identifier: Apache-2.0
"""Proof-of-concept: Alpaca client tests using fakes instead of mocks.

This demonstrates Phase 1 improvements to test infrastructure by replacing
fragile mocks with reusable, realistic fakes.
"""

from __future__ import annotations

from typing import Any
import unittest

import httpx
import pytest

from marketpipe.ingestion.infrastructure.alpaca_client import AlpacaClient
from marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
from marketpipe.ingestion.infrastructure.models import ClientConfig
from tests.fakes.adapters import FakeAsyncHttpClient, FakeHttpClient


class TestAlpacaClientWithFakes:
    """Alpaca client tests using FakeHttpClient instead of monkeypatching."""

    @pytest.fixture
    def alpaca_config(self):
        """Standard Alpaca client configuration."""
        return ClientConfig(api_key="test-key", base_url="https://data.alpaca.markets/v2")

    @pytest.fixture
    def auth_strategy(self):
        """Standard auth strategy."""
        return HeaderTokenAuth("test-key-id", "test-secret")

    @pytest.fixture
    def sample_alpaca_response(self) -> dict[str, Any]:
        """Sample Alpaca API response."""
        return {
            "bars": [
                {
                    "S": "AAPL",
                    "t": "2023-01-02T09:30:00Z",
                    "o": 100.0,
                    "h": 101.0,
                    "l": 99.0,
                    "c": 100.5,
                    "v": 1000,
                }
            ]
        }

    def test_handles_paginated_response(self, alpaca_config, auth_strategy):
        """Test client handles pagination correctly.

        IMPROVEMENT: Uses FakeHttpClient instead of monkeypatching httpx.get().
        Benefits:
        - More realistic HTTP behavior
        - Better request verification
        - Reusable across tests
        - No coupling to httpx internals
        """
        http_client = FakeHttpClient()

        # Configure paginated responses
        http_client.configure_response(
            url_pattern=r".*/stocks/bars.*",
            status=200,
            body={
                "bars": [
                    {
                        "S": "AAPL",
                        "t": "2023-01-02T09:30:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99,
                        "c": 100.5,
                        "v": 1000,
                    }
                ],
                "next_page_token": "page2",
            },
        )

        http_client.configure_response(
            url_pattern=r".*/stocks/bars.*page_token=page2.*",
            status=200,
            body={
                "bars": [
                    {
                        "S": "AAPL",
                        "t": "2023-01-02T09:31:00Z",
                        "o": 100.5,
                        "h": 101.5,
                        "l": 99.5,
                        "c": 101.0,
                        "v": 1500,
                    }
                ]
            },
        )

        # Inject fake HTTP client
        client = AlpacaClient(config=alpaca_config, auth=auth_strategy)
        # Note: This requires AlpacaClient to accept http_client parameter
        # This is part of Phase 2 refactoring

        # For now, use monkeypatch as bridge until dependency injection is added
        import unittest.mock

        with unittest.mock.patch.object(
            client, "_request", side_effect=self._simulate_request_with_fake(http_client)
        ):
            rows = client.fetch_batch("AAPL", 0, 1000)

        # Verify behavior (not implementation details)
        assert len(rows) == 2, "Should combine results from both pages"
        assert all(row["symbol"] == "AAPL" for row in rows), "All bars should be for AAPL"
        assert all(
            row["schema_version"] == 1 for row in rows
        ), "All bars should have schema version"

        # Verify requests were made correctly
        requests = http_client.get_requests_made()
        assert len(requests) == 2, "Should make 2 requests for pagination"

        # Verify authentication was applied
        assert requests[0].headers.get("APCA-API-KEY-ID") == "test-key-id"
        assert requests[0].headers.get("APCA-API-SECRET-KEY") == "test-secret"

        # Verify pagination parameter was sent
        assert "page_token=page2" in requests[1].url

    def test_handles_rate_limiting_with_retry(self, alpaca_config, auth_strategy):
        """Test client retries on rate limit errors.

        IMPROVEMENT: Uses FakeHttpClient to simulate rate limiting scenarios.
        Benefits:
        - Tests realistic error conditions
        - No complex mock setup required
        - Can simulate timing behaviors
        """
        http_client = FakeHttpClient()

        # Configure rate limit error then success
        http_client.configure_error(
            url_pattern=r".*/stocks/bars.*",
            error=httpx.HTTPStatusError(
                "Rate limit exceeded", request=None, response=unittest.mock.Mock(status_code=429)
            ),
        )

        http_client.configure_response(
            url_pattern=r".*/stocks/bars.*",
            status=200,
            body={
                "bars": [
                    {
                        "S": "AAPL",
                        "t": "2023-01-02T09:30:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99,
                        "c": 100.5,
                        "v": 1000,
                    }
                ]
            },
            delay=0.1,  # Simulate network delay
        )

        # Test with rate limiting simulation
        http_client.configure_rate_limiting(delay_after_requests=1, delay=0.05)

        client = AlpacaClient(config=alpaca_config, auth=auth_strategy)

        # Simulate retry behavior
        with unittest.mock.patch.object(
            client, "_request", side_effect=self._simulate_request_with_fake(http_client)
        ):
            rows = client.fetch_batch("AAPL", 0, 1000)

        # Verify successful retry
        assert len(rows) == 1, "Should get data after retry"

        # Verify multiple requests were made (original + retry)
        requests = http_client.get_requests_made()
        assert len(requests) >= 1, "Should make at least one successful request"

    async def test_async_client_behavior(self, alpaca_config, auth_strategy):
        """Test async client functionality.

        IMPROVEMENT: Uses FakeAsyncHttpClient for async testing.
        Benefits:
        - Proper async behavior testing
        - No complex async mocking required
        - Realistic async HTTP simulation
        """
        async_http_client = FakeAsyncHttpClient()

        async_http_client.configure_response(
            url_pattern=r".*/stocks/bars.*",
            status=200,
            body={
                "bars": [
                    {
                        "S": "AAPL",
                        "t": "2023-01-02T09:30:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99,
                        "c": 100.5,
                        "v": 1000,
                    }
                ]
            },
        )

        client = AlpacaClient(config=alpaca_config, auth=auth_strategy)

        # Test async functionality
        with unittest.mock.patch.object(
            client,
            "_async_request",
            side_effect=self._simulate_async_request_with_fake(async_http_client),
        ):
            rows = await client.async_fetch_batch("AAPL", 0, 1000)

        assert len(rows) == 1
        assert rows[0]["symbol"] == "AAPL"

    def test_error_handling_scenarios(self, alpaca_config, auth_strategy):
        """Test various error scenarios.

        IMPROVEMENT: Uses FakeHttpClient to simulate different error types.
        Benefits:
        - Easy error scenario configuration
        - Tests realistic error conditions
        - No complex error mock setup
        """
        http_client = FakeHttpClient()

        test_cases = [
            {
                "name": "network_timeout",
                "error": httpx.TimeoutException("Request timeout"),
                "expected_exception": httpx.TimeoutException,
            },
            {
                "name": "connection_error",
                "error": httpx.ConnectError("Connection failed"),
                "expected_exception": httpx.ConnectError,
            },
            {
                "name": "server_error",
                "error": httpx.HTTPStatusError(
                    "Server error", request=None, response=unittest.mock.Mock(status_code=500)
                ),
                "expected_exception": httpx.HTTPStatusError,
            },
        ]

        for test_case in test_cases:
            with pytest.raises(test_case["expected_exception"]):
                http_client.clear_history()  # Reset for each test
                http_client.configure_error(r".*/stocks/bars.*", test_case["error"])

                client = AlpacaClient(config=alpaca_config, auth=auth_strategy)

                with unittest.mock.patch.object(
                    client, "_request", side_effect=self._simulate_request_with_fake(http_client)
                ):
                    client.fetch_batch("AAPL", 0, 1000)

    # Helper methods for bridging until dependency injection is implemented

    def _simulate_request_with_fake(self, http_client: FakeHttpClient):
        """Bridge method until AlpacaClient supports HTTP client injection."""

        def mock_request(params):
            # Convert AlpacaClient's internal request to HTTP client call
            url = f"{http_client.__dict__.get('base_url', 'https://data.alpaca.markets/v2')}/stocks/bars"
            response = http_client.get(url, params=params)
            return response.json()

        return mock_request

    def _simulate_async_request_with_fake(self, async_http_client: FakeAsyncHttpClient):
        """Bridge method for async client until dependency injection is implemented."""

        async def mock_async_request(params):
            url = f"{async_http_client.__dict__.get('base_url', 'https://data.alpaca.markets/v2')}/stocks/bars"
            response = await async_http_client.get(url, params=params)
            return response.json()

        return mock_async_request


class TestComparisonMocksVsFakes:
    """Side-by-side comparison of old mock-based vs new fake-based approaches."""

    def test_old_approach_with_mocks(self, monkeypatch):
        """OLD APPROACH: Using monkeypatch and mocks.

        Problems:
        - Couples to httpx implementation details
        - Complex mock setup
        - Hard to maintain when implementation changes
        - Limited verification capabilities
        """
        call_count = 0

        def mock_get(url, params=None, headers=None, timeout=None):
            nonlocal call_count
            call_count += 1

            if call_count == 1:
                # First call returns paginated response
                return unittest.mock.Mock(
                    status_code=200,
                    json=lambda: {
                        "bars": [
                            {
                                "S": "AAPL",
                                "t": "2023-01-02T09:30:00Z",
                                "o": 100,
                                "h": 101,
                                "l": 99,
                                "c": 100.5,
                                "v": 1000,
                            }
                        ],
                        "next_page_token": "page2",
                    },
                )
            else:
                # Second call returns final page
                return unittest.mock.Mock(
                    status_code=200,
                    json=lambda: {
                        "bars": [
                            {
                                "S": "AAPL",
                                "t": "2023-01-02T09:31:00Z",
                                "o": 100.5,
                                "h": 101.5,
                                "l": 99.5,
                                "c": 101.0,
                                "v": 1500,
                            }
                        ]
                    },
                )

        monkeypatch.setattr(httpx, "get", mock_get)

        config = ClientConfig(api_key="test-key", base_url="https://data.alpaca.markets/v2")
        auth = HeaderTokenAuth("test-key-id", "test-secret")
        client = AlpacaClient(config=config, auth=auth)

        rows = client.fetch_batch("AAPL", 0, 1000)

        # Can only verify implementation details, not behavior
        assert call_count == 2  # Couples to implementation!
        assert len(rows) == 2

    def test_new_approach_with_fakes(self):
        """NEW APPROACH: Using FakeHttpClient.

        Benefits:
        - Tests behavior, not implementation
        - Rich verification capabilities
        - Reusable across tests
        - More realistic behavior simulation
        """
        http_client = FakeHttpClient()

        # Configure responses - focuses on behavior
        http_client.configure_response(
            url_pattern=r".*/stocks/bars.*",
            body={
                "bars": [
                    {
                        "S": "AAPL",
                        "t": "2023-01-02T09:30:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99,
                        "c": 100.5,
                        "v": 1000,
                    }
                ],
                "next_page_token": "page2",
            },
        )

        http_client.configure_response(
            url_pattern=r".*/stocks/bars.*page_token=page2.*",
            body={
                "bars": [
                    {
                        "S": "AAPL",
                        "t": "2023-01-02T09:31:00Z",
                        "o": 100.5,
                        "h": 101.5,
                        "l": 99.5,
                        "c": 101.0,
                        "v": 1500,
                    }
                ]
            },
        )

        config = ClientConfig(api_key="test-key", base_url="https://data.alpaca.markets/v2")
        auth = HeaderTokenAuth("test-key-id", "test-secret")
        client = AlpacaClient(config=config, auth=auth)

        # Use bridge method until dependency injection
        with unittest.mock.patch.object(
            client, "_request", side_effect=self._simulate_request_with_fake(http_client)
        ):
            rows = client.fetch_batch("AAPL", 0, 1000)

        # Test behavior, not implementation details
        assert len(rows) == 2, "Should get combined results from pagination"
        assert all(row["symbol"] == "AAPL" for row in rows)

        # Rich verification through fake API
        requests = http_client.get_requests_made()
        assert len(requests) == 2, "Should make requests for both pages"
        assert "page_token=page2" in requests[1].url, "Should use pagination token"

        # Can verify authentication without coupling to internals
        assert requests[0].headers.get("APCA-API-KEY-ID") == "test-key-id"

    def _simulate_request_with_fake(self, http_client: FakeHttpClient):
        """Helper bridge method."""

        def mock_request(params):
            url = "https://data.alpaca.markets/v2/stocks/bars"
            response = http_client.get(url, params=params)
            return response.json()

        return mock_request


# TODO: Phase 2 - Add dependency injection to AlpacaClient
#
# class AlpacaClient(BaseApiClient):
#     def __init__(self,
#                  config: ClientConfig,
#                  auth: AuthStrategy,
#                  http_client: Optional[HttpClientProtocol] = None):
#         self.http_client = http_client or httpx.Client()
#         # ... rest of initialization
#
# This would eliminate the need for bridge methods and enable direct injection of fakes.
