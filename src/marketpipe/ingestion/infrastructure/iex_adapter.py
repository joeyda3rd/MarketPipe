# SPDX-License-Identifier: Apache-2.0
"""IEX Cloud market data adapter."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import httpx

from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.market_data import (
    IMarketDataProvider,
    InvalidSymbolError,
    MarketDataUnavailableError,
    ProviderMetadata,
)
from marketpipe.domain.value_objects import Price, Symbol, TimeRange, Timestamp, Volume

from .provider_registry import provider

logger = logging.getLogger(__name__)


@provider("iex")
class IEXMarketDataAdapter(IMarketDataProvider):
    """
    IEX Cloud market data provider adapter.

    Provides access to IEX Cloud API for fetching OHLCV bars.
    Supports both production and sandbox environments.
    """

    def __init__(
        self,
        api_token: str,
        is_sandbox: bool = False,
        base_url: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self._api_token = api_token
        self._is_sandbox = is_sandbox
        self._timeout = timeout

        if base_url:
            self._base_url = base_url
        else:
            self._base_url = (
                "https://sandbox.iexapis.com/stable"
                if is_sandbox
                else "https://cloud.iexapis.com/stable"
            )

        self._client: Optional[httpx.AsyncClient] = None
        logger.info(f"Initialized IEX adapter (sandbox={is_sandbox})")

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> IEXMarketDataAdapter:
        """
        Create adapter from configuration dictionary.

        Args:
            config: Configuration dictionary with keys:
                - api_token: IEX Cloud API token
                - is_sandbox: Whether to use sandbox (optional, default: False)
                - base_url: Override base URL (optional)
                - timeout: Request timeout (optional, default: 30.0)
        """
        return cls(
            api_token=config["api_token"],
            is_sandbox=config.get("is_sandbox", False),
            base_url=config.get("base_url"),
            timeout=config.get("timeout", 30.0),
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    async def _close_client(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> List[OHLCVBar]:
        """
        Fetch OHLCV bars from IEX Cloud.

        Note: IEX Cloud has limited historical data in sandbox mode.
        Production API offers more comprehensive data.
        """
        try:
            client = await self._get_client()

            # IEX Cloud uses different endpoints for different time ranges
            # For simplicity, we'll use the intraday endpoint for 1-minute bars
            url = f"{self._base_url}/stock/{symbol.value}/intraday-prices"

            params = {
                "token": self._api_token,
                "chartByDay": "true",  # Group by day
            }

            # Add date filtering if needed
            # Note: IEX Cloud has limitations on historical data in sandbox
            if not self._is_sandbox:
                start_date = time_range.start.value.date()
                params["exactDate"] = start_date.isoformat()

            logger.debug(f"Fetching IEX data for {symbol.value}: {url}")

            response = await client.get(url, params=params)
            response.raise_for_status()

            raw_data = response.json()

            # Handle empty response
            if not raw_data:
                logger.warning(f"No data returned from IEX for {symbol.value}")
                return []

            # Convert IEX format to domain objects
            domain_bars = []
            for raw_bar in raw_data[:max_bars]:  # Limit results
                try:
                    domain_bar = self._translate_iex_bar_to_domain(raw_bar, symbol)

                    # Filter by time range
                    if self._is_within_time_range(domain_bar.timestamp, time_range):
                        domain_bars.append(domain_bar)

                except Exception as e:
                    logger.warning(f"Failed to translate IEX bar for {symbol.value}: {e}")
                    continue

            logger.info(f"Fetched {len(domain_bars)} bars for {symbol.value} from IEX")
            return domain_bars

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise InvalidSymbolError(f"Symbol {symbol.value} not found on IEX") from e
            else:
                raise MarketDataUnavailableError(f"IEX API error: {e}") from e
        except Exception as e:
            raise MarketDataUnavailableError(f"Failed to fetch IEX data: {e}") from e

    def _translate_iex_bar_to_domain(self, iex_bar: Dict[str, Any], symbol: Symbol) -> OHLCVBar:
        """
        Translate IEX bar format to domain OHLCV bar.

        IEX format example:
        {
            "date": "2023-12-15",
            "minute": "09:30",
            "label": "09:30 AM",
            "open": 195.89,
            "close": 195.95,
            "high": 196.05,
            "low": 195.85,
            "average": 195.93,
            "volume": 125847,
            "notional": 24654321.45,
            "numberOfTrades": 542
        }
        """
        try:
            # Parse timestamp from date and minute
            date_str = iex_bar["date"]
            minute_str = iex_bar["minute"]

            # Combine date and minute into datetime
            datetime_str = f"{date_str} {minute_str}"
            dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
            dt = dt.replace(tzinfo=timezone.utc)

            # Extract OHLCV values
            open_price = Decimal(str(iex_bar["open"]))
            high_price = Decimal(str(iex_bar["high"]))
            low_price = Decimal(str(iex_bar["low"]))
            close_price = Decimal(str(iex_bar["close"]))
            volume_value = int(iex_bar["volume"])

            # Create domain objects
            return OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(dt),
                open_price=Price(open_price),
                high_price=Price(high_price),
                low_price=Price(low_price),
                close_price=Price(close_price),
                volume=Volume(volume_value),
            )

        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Failed to parse IEX bar data: {e}") from e

    def _is_within_time_range(self, timestamp: Timestamp, time_range: TimeRange) -> bool:
        """Check if timestamp is within the specified time range."""
        return time_range.start.value <= timestamp.value <= time_range.end.value

    async def get_supported_symbols(self) -> List[Symbol]:
        """
        Get list of symbols supported by IEX Cloud.

        Returns a basic list for demonstration.
        In production, you'd query IEX's reference data endpoint.
        """
        # Common US equity symbols supported by IEX
        common_symbols = [
            "AAPL",
            "GOOGL",
            "MSFT",
            "AMZN",
            "TSLA",
            "META",
            "NVDA",
            "NFLX",
            "CRM",
            "ORCL",
            "UBER",
            "LYFT",
            "SQ",
            "PYPL",
            "ROKU",
            "ZM",
            "SHOP",
            "SPOT",
            "SNAP",
            "TWTR",
        ]
        return [Symbol.from_string(s) for s in common_symbols]

    async def is_available(self) -> bool:
        """Test connection to IEX Cloud API."""
        try:
            client = await self._get_client()

            # Use a simple endpoint to test connectivity
            url = f"{self._base_url}/stock/AAPL/quote"
            params = {"token": self._api_token}

            response = await client.get(url, params=params)
            response.raise_for_status()

            logger.debug("IEX API connection test successful")
            return True

        except Exception as e:
            logger.warning(f"IEX API connection test failed: {e}")
            return False

    def get_provider_metadata(self) -> ProviderMetadata:
        """Get IEX Cloud provider metadata."""
        return ProviderMetadata(
            provider_name="iex",
            supports_real_time=not self._is_sandbox,  # Real-time only in production
            supports_historical=True,
            rate_limit_per_minute=(100 if self._is_sandbox else 500),  # Approximate limits
            minimum_time_resolution="1m",
            maximum_history_days=(30 if self._is_sandbox else 365),  # Sandbox has limited history
        )

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._close_client()


# Legacy compatibility functions for factory pattern
def create_iex_adapter(api_token: str, is_sandbox: bool = False) -> IEXMarketDataAdapter:
    """Create IEX adapter (legacy function for compatibility)."""
    return IEXMarketDataAdapter(api_token=api_token, is_sandbox=is_sandbox)
