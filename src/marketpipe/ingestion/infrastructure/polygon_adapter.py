# SPDX-License-Identifier: Apache-2.0
"""Polygon.io market data provider adapter."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import parse_qs, urlsplit

import httpx

from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.market_data import IMarketDataProvider, ProviderMetadata
from marketpipe.domain.value_objects import Price, Symbol, TimeRange, Timestamp, Volume
from marketpipe.security.mask import safe_for_log

from .provider_registry import provider


@provider("polygon")
class PolygonMarketDataAdapter(IMarketDataProvider):
    """
    Anti-corruption layer for Polygon.io API integration.

    Implements the complete Polygon.io REST API for historical OHLC data.
    Free tier: 5 requests per minute, paid tier: unlimited.

    API Documentation: https://polygon.io/docs/rest/stocks/aggregates/custom-bars
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.polygon.io",
        rate_limit_per_minute: int = 5,  # Free tier limit
        timeout: float = 30.0,
        max_retries: int = 3,
        logger: Optional[logging.Logger] = None,
    ):
        if rate_limit_per_minute <= 0:
            raise ValueError("rate_limit_per_minute must be positive")
        if timeout <= 0 or max_retries < 0:
            raise ValueError("timeout must be positive and max_retries non-negative")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.rate_limit_per_minute = rate_limit_per_minute
        self.timeout = timeout
        self.max_retries = max_retries
        self.log = logger or logging.getLogger(self.__class__.__name__)

        # Rate limiting state
        self._request_times: list[float] = []
        self._rate_limit_lock: Optional[asyncio.Lock] = None

        self.log.info(
            f"Polygon adapter initialized with {rate_limit_per_minute} requests/min limit"
        )

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
        timeframe: str = "1m",
    ) -> list[OHLCVBar]:
        """
        Fetch OHLCV bars from Polygon.io API.

        Args:
            symbol: Stock symbol (e.g., AAPL)
            time_range: Time range for data retrieval
            max_bars: Maximum bars per API request (Polygon's limit parameter)
            timeframe: Bar timeframe (e.g., "1m", "5m", "15m", "1h", "1d")

        Returns:
            List of OHLCV bars for the entire time range (paginated as needed)
        """
        self.log.info(
            f"Fetching {timeframe} bars for {symbol.value} from {time_range.start} to {time_range.end}"
        )

        # Parse timeframe
        multiplier, timespan = self._parse_timeframe(timeframe)

        # Format dates for API
        from_date = time_range.start.value.strftime("%Y-%m-%d")
        to_date = time_range.end.value.strftime("%Y-%m-%d")

        bars = []
        cursor = None
        page_count = 0

        # Convert time range to timestamps for validation
        start_ts = int(time_range.start.value.timestamp() * 1000)  # milliseconds
        end_ts = int(time_range.end.value.timestamp() * 1000)  # milliseconds

        seen_cursors = set()
        while True:
            page_count += 1
            self.log.info(f"📥 Fetching page {page_count} for {symbol.value}...")

            # Apply rate limiting
            await self._apply_rate_limit()

            # Build request URL
            url = f"{self.base_url}/v2/aggs/ticker/{symbol.value}/range/{multiplier}/{timespan}/{from_date}/{to_date}"

            # Build query parameters
            params = {
                "apikey": self.api_key,
                "adjusted": "true",  # Adjust for splits
                "sort": "asc",  # Oldest first
                "limit": min(max_bars, 50000),  # Respect max_bars parameter
            }

            if cursor:
                params["cursor"] = cursor

            self.log.debug(safe_for_log(f"Requesting: {url} with params: {params}", self.api_key))

            try:
                # Make HTTP request
                response_data = await self._make_request(url, params)

                # Parse response
                if "results" in response_data and not isinstance(response_data["results"], list):
                    raise ValueError("Polygon results must be a list")
                if "results" in response_data and response_data["results"]:
                    page_bars = self._parse_polygon_response(response_data, symbol)

                    # Filter bars to only include those within the requested time range
                    # Track if we've reached data beyond our end date
                    reached_end_of_range = False
                    filtered_bars = []
                    for bar in page_bars:
                        bar_ts = int(bar.timestamp.value.timestamp() * 1000)
                        if start_ts <= bar_ts < end_ts:
                            filtered_bars.append(bar)
                        elif bar_ts >= end_ts:
                            # Bar is after our end date - stop pagination after this page
                            self.log.info(
                                f"⏹️  Reached end of requested date range at bar {bar.timestamp.value}"
                            )
                            reached_end_of_range = True
                            break

                    bars.extend(filtered_bars)
                    self.log.info(
                        f"✅ Page {page_count}: Downloaded {len(page_bars)} bars, "
                        f"kept {len(filtered_bars)} within range (Total: {len(bars)} bars)"
                    )

                    # If we reached the end of the requested date range, stop paginating
                    if reached_end_of_range:
                        break
                else:
                    self.log.warning(f"No results in response for {symbol.value}")
                    break

                # Check for pagination - get next_url from response
                next_url = response_data.get("next_url")
                if not next_url:
                    break

                cursor_values = parse_qs(urlsplit(next_url).query).get("cursor")
                if not cursor_values or cursor_values[0] in seen_cursors:
                    raise ValueError("Polygon returned an invalid or repeated pagination cursor")
                cursor = cursor_values[0]
                seen_cursors.add(cursor)

            except Exception as e:
                self.log.error(
                    safe_for_log(
                        f"Failed to fetch page {page_count} for {symbol.value}: {e}", self.api_key
                    )
                )
                raise

        if bars:
            self.log.info(
                f"🎉 Successfully fetched {len(bars)} {timeframe} bars for {symbol.value} "
                f"across {page_count} API request(s)"
            )
        else:
            self.log.warning(
                f"⚠️  No bars returned for {symbol.value} in date range {from_date} to {to_date}"
            )

        return list({bar.timestamp.value: bar for bar in bars}.values())

    async def get_supported_symbols(self) -> list[Symbol]:
        """Get list of supported US stock symbols from Polygon.io."""
        try:
            await self._apply_rate_limit()

            url = f"{self.base_url}/v3/reference/tickers"
            params = {
                "apikey": self.api_key,
                "market": "stocks",
                "exchange": "XNAS,XNYS",  # NASDAQ and NYSE
                "active": "true",
                "limit": 1000,
            }

            response_data = await self._make_request(url, params)

            symbols = []
            for ticker_info in response_data.get("results", []):
                if "ticker" in ticker_info:
                    symbols.append(Symbol.from_string(ticker_info["ticker"]))

            self.log.info(f"Retrieved {len(symbols)} supported symbols from Polygon.io")
            return symbols

        except Exception as e:
            self.log.error(f"Failed to get supported symbols: {e}")
            # Return common symbols as fallback
            common_symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX"]
            return [Symbol.from_string(s) for s in common_symbols]

    async def is_available(self) -> bool:
        """Test if the provider is currently available."""
        try:
            # Test with a simple request
            url = f"{self.base_url}/v2/aggs/ticker/AAPL/range/1/day/2023-01-01/2023-01-01"
            params = {"apikey": self.api_key, "limit": 1}

            await self._apply_rate_limit()
            response_data = await self._make_request(url, params)

            if response_data.get("status") == "OK":
                self.log.info("Polygon.io connection validated successfully")
                return True
            else:
                self.log.error(f"Polygon.io validation failed: {response_data}")
                return False

        except Exception as e:
            self.log.error(f"Polygon.io connection validation failed: {e}")
            return False

    def get_provider_metadata(self) -> ProviderMetadata:
        """Get provider metadata."""
        return ProviderMetadata(
            provider_name="polygon",
            supports_real_time=True,
            supports_historical=True,
            rate_limit_per_minute=self.rate_limit_per_minute,
            minimum_time_resolution="1m",
            maximum_history_days=365 * 2,  # Polygon has good historical data
        )

    # Legacy method for backward compatibility
    async def fetch_bars(
        self,
        symbol: Symbol,
        start_timestamp: int,
        end_timestamp: int,
        batch_size: int = 1000,
        timeframe: str = "1m",
    ) -> list[OHLCVBar]:
        """
        Legacy method for backward compatibility with application service.

        Args:
            symbol: Stock symbol
            start_timestamp: Start timestamp in nanoseconds
            end_timestamp: End timestamp in nanoseconds
            batch_size: Maximum number of bars to fetch
            timeframe: Bar timeframe (e.g., "1m", "5m", "15m", "1h", "1d")

        Returns:
            List of OHLCV bars
        """
        # Convert nanosecond timestamps to TimeRange
        start_ts = Timestamp.from_nanoseconds(start_timestamp)
        end_ts = Timestamp.from_nanoseconds(end_timestamp)
        time_range = TimeRange(start_ts, end_ts)

        # Delegate to the interface method
        return await self.fetch_bars_for_symbol(symbol, time_range, batch_size, timeframe)

    async def _apply_rate_limit(self) -> None:
        """Apply rate limiting based on free tier limits."""
        # Python 3.9 binds locks to the current event loop during construction.
        if self._rate_limit_lock is None:
            self._rate_limit_lock = asyncio.Lock()
        async with self._rate_limit_lock:
            now = time.monotonic()

            # Remove requests older than 1 minute
            cutoff = now - 60.0
            self._request_times = [t for t in self._request_times if t > cutoff]

            # Check if we need to wait
            if len(self._request_times) >= self.rate_limit_per_minute:
                # Calculate wait time
                oldest_request = min(self._request_times)
                wait_time = 60.0 - (now - oldest_request)

                if wait_time > 0:
                    self.log.info(
                        f"⏳ Rate limit ({self.rate_limit_per_minute}/min) reached, "
                        f"waiting {wait_time:.1f}s..."
                    )
                    await asyncio.sleep(wait_time)

            # Record this request
            self._request_times.append(time.monotonic())

    async def _make_request(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        """Make HTTP request with retry logic."""
        headers = {
            "User-Agent": "MarketPipe/1.0 (Polygon.io Adapter)",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        params = {key: value for key, value in params.items() if key.lower() != "apikey"}

        for attempt in range(self.max_retries + 1):
            try:
                if attempt:
                    await self._apply_rate_limit()
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.get(url, params=params, headers=headers)

                    if response.status_code in {429, 500, 502, 503, 504}:
                        if attempt == self.max_retries:
                            raise RuntimeError(
                                safe_for_log(
                                    f"Polygon retry limit exceeded (HTTP {response.status_code})",
                                    self.api_key,
                                )
                            )
                        delay = min(2**attempt, 60)
                        if response.status_code == 429:
                            try:
                                delay = min(
                                    max(float(response.headers.get("Retry-After", delay)), 0), 60
                                )
                            except (TypeError, ValueError):
                                pass
                        self.log.warning(
                            "Polygon HTTP %s; retrying in %.1fs", response.status_code, delay
                        )
                        await asyncio.sleep(delay)
                        continue

                    # Handle authentication errors
                    if response.status_code == 401:
                        raise ValueError("Invalid Polygon.io API key")

                    # Handle forbidden
                    if response.status_code == 403:
                        raise ValueError("Polygon.io API access forbidden - check subscription")

                    # Handle other client errors
                    if response.status_code >= 400:
                        error_text = response.text
                        raise RuntimeError(
                            safe_for_log(
                                f"Polygon API error {response.status_code}: {error_text[:500]}",
                                self.api_key,
                            )
                        )

                    # Parse JSON response
                    from typing import cast

                    data = cast(dict[str, Any], response.json())

                    if not isinstance(data, dict):
                        raise ValueError("Polygon response must be an object")

                    # Check API status
                    if data.get("status") == "ERROR":
                        error_msg = data.get("error", "Unknown API error")
                        raise ValueError(
                            safe_for_log(f"Polygon API error: {error_msg}", self.api_key)
                        )

                    return data

            except httpx.TimeoutException:
                if attempt < self.max_retries:
                    wait_time = min(2**attempt, 60)  # Exponential backoff
                    self.log.warning(
                        f"Request timeout, retrying in {wait_time}s (attempt {attempt + 1})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise
            except httpx.RequestError as e:
                if attempt < self.max_retries:
                    wait_time = min(2**attempt, 60)
                    self.log.warning(
                        safe_for_log(
                            f"Request error: {e}, retrying in {wait_time}s (attempt {attempt + 1})",
                            self.api_key,
                        )
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise

        raise RuntimeError(f"Failed to complete request after {self.max_retries + 1} attempts")

    def _parse_timeframe(self, timeframe: str) -> tuple[int, str]:
        """Parse timeframe into multiplier and timespan for Polygon API."""
        timeframe_map = {
            "1m": (1, "minute"),
            "5m": (5, "minute"),
            "15m": (15, "minute"),
            "30m": (30, "minute"),
            "1h": (1, "hour"),
            "4h": (4, "hour"),
            "1d": (1, "day"),
        }

        if timeframe not in timeframe_map:
            raise ValueError(
                f"Unsupported timeframe: {timeframe}. Supported: {list(timeframe_map.keys())}"
            )

        return timeframe_map[timeframe]

    def _parse_polygon_response(
        self, response_data: dict[str, Any], symbol: Symbol
    ) -> list[OHLCVBar]:
        """Parse Polygon.io API response into OHLCVBar objects."""
        bars = []

        for result in response_data.get("results", []):
            try:
                # Extract data from Polygon format
                # Polygon response format:
                # {
                #   "c": close_price,
                #   "h": high_price,
                #   "l": low_price,
                #   "n": number_of_transactions,
                #   "o": open_price,
                #   "t": timestamp_ms,
                #   "v": volume,
                #   "vw": volume_weighted_average_price
                # }

                timestamp_ms = result["t"]
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

                bar = OHLCVBar(
                    id=EntityId.generate(),
                    symbol=symbol,
                    timestamp=Timestamp(timestamp_dt),
                    open_price=Price.from_float(float(result["o"])),
                    high_price=Price.from_float(float(result["h"])),
                    low_price=Price.from_float(float(result["l"])),
                    close_price=Price.from_float(float(result["c"])),
                    volume=Volume(int(result["v"])),
                    trade_count=result.get("n"),
                    vwap=Price.from_float(float(result["vw"])) if result.get("vw") else None,
                )

                bars.append(bar)

            except (KeyError, ValueError, TypeError):
                raise ValueError("Invalid Polygon bar response") from None

        return bars

    def __str__(self) -> str:
        return f"PolygonMarketDataAdapter(rate_limit={self.rate_limit_per_minute}/min)"
