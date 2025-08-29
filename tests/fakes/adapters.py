# SPDX-License-Identifier: Apache-2.0
"""Fake adapters for testing."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.market_data import IMarketDataProvider, ProviderMetadata
from marketpipe.domain.value_objects import Price, Symbol, TimeRange, Timestamp, Volume


@dataclass
class RequestCapture:
    """Captures details of an HTTP request for test verification."""

    url: str
    method: str = "GET"
    headers: dict[str, str] = None
    params: dict[str, Any] = None
    body: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.headers is None:
            self.headers = {}
        if self.params is None:
            self.params = {}
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class ResponseSpec:
    """Specification for a fake HTTP response."""

    url_pattern: str
    status: int = 200
    body: dict[str, Any] = None
    headers: dict[str, str] = None
    delay: Optional[float] = None
    error: Optional[Exception] = None

    def __post_init__(self):
        if self.body is None:
            self.body = {}
        if self.headers is None:
            self.headers = {"content-type": "application/json"}


class FakeResponse:
    """Fake HTTP response for testing."""

    def __init__(self, status_code: int, body: dict[str, Any], headers: dict[str, str] = None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def json(self) -> dict[str, Any]:
        """Return JSON body."""
        return self._body

    @property
    def text(self) -> str:
        """Return text representation of response."""
        return json.dumps(self._body)


class FakeHttpClient:
    """Configurable fake HTTP client for testing.

    Replaces monkeypatching httpx with a controllable fake that can:
    - Configure responses for URL patterns
    - Simulate errors, delays, and rate limiting
    - Capture request history for verification
    - Support both sync and async patterns
    """

    def __init__(self):
        self.response_specs: list[ResponseSpec] = []
        self.requests_made: list[RequestCapture] = []
        self.default_delay: Optional[float] = None
        self._rate_limit_delay: Optional[float] = None
        self._request_count = 0

    def configure_response(
        self,
        url_pattern: str,
        status: int = 200,
        body: dict[str, Any] = None,
        headers: dict[str, str] = None,
        delay: Optional[float] = None,
    ):
        """Configure expected response for URL pattern.

        Args:
            url_pattern: Regex pattern to match URLs
            status: HTTP status code
            body: Response body as dict
            headers: Response headers
            delay: Simulated delay in seconds
        """
        spec = ResponseSpec(
            url_pattern=url_pattern, status=status, body=body or {}, headers=headers, delay=delay
        )
        self.response_specs.append(spec)

    def configure_error(self, url_pattern: str, error: Exception):
        """Configure error response for URL pattern.

        Args:
            url_pattern: Regex pattern to match URLs
            error: Exception to raise
        """
        spec = ResponseSpec(url_pattern=url_pattern, error=error)
        self.response_specs.append(spec)

    def configure_rate_limiting(self, delay_after_requests: int, delay: float):
        """Simulate rate limiting after N requests.

        Args:
            delay_after_requests: Number of requests before rate limiting
            delay: Delay to introduce (seconds)
        """
        # Simple rate limiting simulation
        self._rate_limit_threshold = delay_after_requests
        self._rate_limit_delay = delay

    def get_requests_made(self) -> list[RequestCapture]:
        """Get history of requests made for test verification."""
        return self.requests_made.copy()

    def clear_history(self):
        """Clear request history and response specs."""
        self.requests_made.clear()
        self.response_specs.clear()
        self._request_count = 0

    def _find_matching_spec(self, url: str) -> Optional[ResponseSpec]:
        """Find response spec matching the URL."""
        for spec in self.response_specs:
            if re.search(spec.url_pattern, url):
                return spec
        return None

    def _simulate_delay(self, spec: Optional[ResponseSpec] = None):
        """Simulate network delay."""
        delay = None

        # Check for rate limiting
        if (
            self._rate_limit_delay
            and hasattr(self, "_rate_limit_threshold")
            and self._request_count >= self._rate_limit_threshold
        ):
            delay = self._rate_limit_delay
        elif spec and spec.delay:
            delay = spec.delay
        elif self.default_delay:
            delay = self.default_delay

        if delay:
            import time

            time.sleep(delay)

    async def _async_simulate_delay(self, spec: Optional[ResponseSpec] = None):
        """Simulate network delay for async calls."""
        delay = None

        # Check for rate limiting
        if (
            self._rate_limit_delay
            and hasattr(self, "_rate_limit_threshold")
            and self._request_count >= self._rate_limit_threshold
        ):
            delay = self._rate_limit_delay
        elif spec and spec.delay:
            delay = spec.delay
        elif self.default_delay:
            delay = self.default_delay

        if delay:
            await asyncio.sleep(delay)

    def get(
        self,
        url: str,
        params: dict[str, Any] = None,
        headers: dict[str, str] = None,
        timeout: float = None,
    ) -> FakeResponse:
        """Synchronous GET request."""
        self._request_count += 1

        # Capture request
        request = RequestCapture(url=url, method="GET", headers=headers or {}, params=params or {})
        self.requests_made.append(request)

        # Find matching response spec
        spec = self._find_matching_spec(url)

        # Simulate delay
        self._simulate_delay(spec)

        # Handle configured error
        if spec and spec.error:
            raise spec.error

        # Return configured response or default
        if spec:
            return FakeResponse(spec.status, spec.body, spec.headers)
        else:
            # Default response for unmatched URLs
            return FakeResponse(200, {"message": "default response"})

    async def aenter(self):
        """Async context manager entry."""
        return self

    async def aexit(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass

    # Async context manager protocol
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class FakeAsyncHttpClient(FakeHttpClient):
    """Async version of FakeHttpClient."""

    async def get(
        self,
        url: str,
        params: dict[str, Any] = None,
        headers: dict[str, str] = None,
        timeout: float = None,
    ) -> FakeResponse:
        """Asynchronous GET request."""
        self._request_count += 1

        # Capture request
        request = RequestCapture(url=url, method="GET", headers=headers or {}, params=params or {})
        self.requests_made.append(request)

        # Find matching response spec
        spec = self._find_matching_spec(url)

        # Simulate delay
        await self._async_simulate_delay(spec)

        # Handle configured error
        if spec and spec.error:
            raise spec.error

        # Return configured response or default
        if spec:
            return FakeResponse(spec.status, spec.body, spec.headers)
        else:
            return FakeResponse(200, {"message": "default response"})


class FakeMarketDataAdapter:
    """Enhanced fake market data adapter for testing various scenarios.

    Backwards-compatible with earlier tests by accepting optional parameters and
    exposing legacy helper methods used across integration suites.
    """

    def __init__(
        self,
        provider_name: str = "fake_provider",
        supported_symbols: list[str] = None,
        http_client: Any = None,  # accepted for compatibility; not used directly
        **_kwargs: Any,
    ):
        self.provider_name = provider_name
        self.supported_symbols = supported_symbols or ["AAPL", "GOOGL", "MSFT"]
        self.http_client = http_client
        self._symbol_data: dict[str, list[OHLCVBar]] = {}
        self._error_configs: dict[str, Exception] = {}
        self._rate_limit_config: Optional[dict[str, Any]] = None
        self._pagination_config: Optional[dict[str, Any]] = None
        self._request_history: list[dict[str, Any]] = []

    def configure_symbol_data(self, symbol: str, bars: list[OHLCVBar]):
        """Configure expected bar data for symbol."""
        self._symbol_data[symbol] = bars

    def configure_error(self, symbol: str, error: Exception):
        """Configure error responses for symbol."""
        self._error_configs[symbol] = error

    def configure_rate_limiting(self, delay: float, max_requests: int):
        """Simulate rate limiting behavior."""
        self._rate_limit_config = {
            "delay": delay,
            "max_requests": max_requests,
            "current_requests": 0,
        }

    def configure_pagination(self, page_size: int, total_pages: int):
        """Simulate paginated responses."""
        self._pagination_config = {"page_size": page_size, "total_pages": total_pages}

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> list[OHLCVBar]:
        """Fetch bars with configured behavior."""
        symbol_str = str(symbol)

        # Record request
        self._request_history.append(
            {
                "symbol": symbol_str,
                "time_range": time_range,
                "max_bars": max_bars,
                "timestamp": datetime.now(),
            }
        )

        # Check for configured error
        if symbol_str in self._error_configs:
            raise self._error_configs[symbol_str]

        # Simulate rate limiting
        if self._rate_limit_config:
            config = self._rate_limit_config
            config["current_requests"] += 1
            if config["current_requests"] > config["max_requests"]:
                await asyncio.sleep(config["delay"])

        # Return configured data or empty list
        return self._symbol_data.get(symbol_str, [])

    # ----- Backward-compatibility helpers used by other tests -----
    def set_bars_data(self, symbol: Symbol | str, bars: list[OHLCVBar]) -> None:
        """Alias for configure_symbol_data accepting Symbol or str."""
        sym = str(symbol)
        self.configure_symbol_data(sym, bars)

    def set_symbol_failure(self, symbol: Symbol | str, error: Exception | None = None) -> None:
        """Configure a symbol to raise an error when fetched."""
        sym = str(symbol)
        self.configure_error(sym, error or RuntimeError(f"Fetch failed for {sym}"))

    def get_fetch_calls(self) -> list[tuple[Symbol, TimeRange]]:
        """Return recorded fetch call tuples for verification."""
        calls: list[tuple[Symbol, TimeRange]] = []
        for rec in self._request_history:
            calls.append((Symbol(rec["symbol"]), rec["time_range"]))
        return calls

    def configure_streaming(self, **_kw: Any) -> None:  # no-op for compatibility
        return

    # Compatibility with application service that calls `fetch_bars` with timestamps
    async def fetch_bars(
        self,
        symbol: Symbol,
        start_timestamp: int,
        end_timestamp: int,
        batch_size: int = 1000,
    ) -> list[OHLCVBar]:
        """Fetch bars using nanosecond timestamps; returns configured data.

        For simplicity in tests, this returns the preconfigured bars for the symbol.
        """
        # Record request
        self._request_history.append(
            {
                "symbol": str(symbol),
                "time_range": TimeRange(
                    Timestamp.from_nanoseconds(start_timestamp),
                    Timestamp.from_nanoseconds(end_timestamp),
                ),
                "max_bars": batch_size,
                "timestamp": datetime.now(),
            }
        )

        # Simulate configured error for this symbol
        sym_str = str(symbol)
        if sym_str in self._error_configs:
            raise self._error_configs[sym_str]

        # Return configured bars (ignore range for test simplicity)
        return self._symbol_data.get(sym_str, [])

    async def get_supported_symbols(self) -> list[Symbol]:
        """Get supported symbols."""
        return [Symbol.from_string(s) for s in self.supported_symbols]

    async def is_available(self) -> bool:
        """Check if provider is available."""
        return True

    def get_provider_metadata(self) -> ProviderMetadata:
        """Get provider metadata."""
        return ProviderMetadata(
            provider_name=self.provider_name,
            supports_real_time=False,
            supports_historical=True,
            rate_limit_per_minute=None,
            minimum_time_resolution="1m",
            maximum_history_days=None,
        )

    def get_request_history(self) -> list[dict[str, Any]]:
        """Get request history for test verification."""
        return self._request_history.copy()

    def clear_history(self):
        """Clear request history."""
        self._request_history.clear()
        if self._rate_limit_config:
            self._rate_limit_config["current_requests"] = 0


# Keep existing FakeMarketDataProvider for backward compatibility
class FakeMarketDataProvider(IMarketDataProvider):
    """Simple fake provider for backward compatibility."""

    def __init__(self):
        self.call_count = 0

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> list[OHLCVBar]:
        """Return fake OHLCV bars for testing."""
        self.call_count += 1
        # Return empty list by default
        return []

    async def get_supported_symbols(self) -> list[Symbol]:
        return [Symbol.from_string("AAPL")]

    async def is_available(self) -> bool:
        return True

    def get_provider_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="fake",
            supports_real_time=False,
            supports_historical=True,
            rate_limit_per_minute=None,
            minimum_time_resolution="1m",
            maximum_history_days=None,
        )


def create_test_ohlcv_bars(
    symbol: Symbol, count: int = 10, start_time: datetime = None
) -> list[OHLCVBar]:
    """Create test OHLCV bars for testing purposes.

    Args:
        symbol: Symbol to create bars for
        count: Number of bars to create
        start_time: Starting timestamp (defaults to recent time)

    Returns:
        List of OHLCVBar entities with sequential timestamps
    """
    from datetime import timedelta, timezone

    if start_time is None:
        start_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    bars = []
    for i in range(count):
        timestamp = start_time + timedelta(minutes=i)

        bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(timestamp),
            open_price=Price.from_float(100.0 + i * 0.1),
            high_price=Price.from_float(101.0 + i * 0.1),
            low_price=Price.from_float(99.0 + i * 0.1),
            close_price=Price.from_float(100.5 + i * 0.1),
            volume=Volume(1000 + i * 10),
        )
        bars.append(bar)

    return bars
