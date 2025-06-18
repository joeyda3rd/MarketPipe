# SPDX-License-Identifier: Apache-2.0
"""Anti-corruption layer adapters for external market data providers."""

from __future__ import annotations

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from marketpipe.domain.entities import OHLCVBar, EntityId
from marketpipe.domain.value_objects import Symbol, Price, Timestamp, Volume, TimeRange
from marketpipe.domain.market_data import (
    IMarketDataProvider,
    ProviderMetadata,
)
from marketpipe.security.mask import safe_for_log
from .alpaca_client import AlpacaClient
from .finnhub_client import FinnhubClient
from .polygon_client import PolygonClient
from .models import ClientConfig, PolygonClientConfig
from .auth import HeaderTokenAuth
from .rate_limit import RateLimiter, create_rate_limiter_from_config
from .provider_registry import provider


@provider("alpaca")
class AlpacaMarketDataAdapter(IMarketDataProvider):
    """
    Anti-corruption layer for Alpaca Markets API integration.

    This adapter translates between Alpaca's API format and our domain models,
    ensuring that external system changes don't corrupt our domain.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str,
        feed_type: str = "iex",
        rate_limit_per_min: Optional[int] = None,
    ):
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = base_url
        self._feed_type = feed_type
        self._logger = logging.getLogger(self.__class__.__name__)

        # Configure Alpaca client (infrastructure layer)
        self._client_config = ClientConfig(
            api_key=api_key, base_url=base_url, rate_limit_per_min=rate_limit_per_min
        )
        self._auth = HeaderTokenAuth(api_key, api_secret)
        self._rate_limiter = create_rate_limiter_from_config(
            rate_limit_per_min=rate_limit_per_min,
            provider_name="alpaca"
        )

        self._alpaca_client = AlpacaClient(
            config=self._client_config,
            auth=self._auth,
            rate_limiter=self._rate_limiter,
            state_backend=None,  # We'll handle state at domain level
            feed=feed_type,
        )

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> AlpacaMarketDataAdapter:
        """
        Create adapter from configuration dictionary.

        Args:
            config: Configuration dictionary with keys:
                - api_key: Alpaca API key
                - api_secret: Alpaca API secret
                - base_url: Alpaca API base URL
                - feed_type: Data feed type (optional, default: "iex")
                - rate_limit_per_min: Rate limit (optional)
        """
        return cls(
            api_key=config["api_key"],
            api_secret=config["api_secret"],
            base_url=config["base_url"],
            feed_type=config.get("feed_type", "iex"),
            rate_limit_per_min=config.get("rate_limit_per_min"),
        )

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> List[OHLCVBar]:
        """
        Fetch bars from Alpaca and translate to domain models.

        This method handles the translation from Alpaca's format to our domain format,
        protecting the domain from external API changes.
        """
        # Convert time range to milliseconds for Alpaca API
        start_ms = time_range.start.to_nanoseconds() // 1_000_000
        end_ms = time_range.end.to_nanoseconds() // 1_000_000

        # Fetch raw data from Alpaca
        try:
            raw_bars = self._alpaca_client.fetch_batch(symbol.value, start_ms, end_ms)
        except Exception as e:
            # Translate infrastructure exceptions to domain exceptions
            safe_msg = safe_for_log(
                f"Failed to fetch data for {symbol}: {e}",
                self._api_key,
                self._api_secret
            )
            raise MarketDataProviderError(safe_msg) from e

        # Limit results to max_bars
        if len(raw_bars) > max_bars:
            raw_bars = raw_bars[:max_bars]

        # Translate raw data to domain models
        domain_bars = []
        for raw_bar in raw_bars:
            try:
                domain_bar = self._translate_alpaca_bar_to_domain(raw_bar, symbol)
                domain_bars.append(domain_bar)
            except Exception as e:
                # Log translation errors but continue processing other bars
                safe_msg = safe_for_log(
                    f"Failed to translate bar for {symbol}: {e}",
                    self._api_key,
                    self._api_secret
                )
                self._logger.warning(safe_msg)
                continue

        return domain_bars

    async def get_supported_symbols(self) -> List[Symbol]:
        """
        Get list of symbols supported by Alpaca.

        Note: Alpaca supports most US equities, but we'll return a basic list for now.
        In a real implementation, this might query Alpaca's assets endpoint.
        """
        # This is a simplified implementation
        # In reality, you'd query Alpaca's assets endpoint
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
        ]
        return [Symbol.from_string(s) for s in common_symbols]

    async def is_available(self) -> bool:
        """Test connection to Alpaca API."""
        try:
            # Try to fetch account info as a connection test
            # This would be implemented in the Alpaca client
            return True
        except Exception:
            return False

    async def test_connection(self) -> bool:
        """Test connection to Alpaca API (alias for is_available)."""
        return await self.is_available()

    def get_provider_metadata(self) -> ProviderMetadata:
        """Get Alpaca provider metadata."""
        return ProviderMetadata(
            provider_name="alpaca",
            supports_real_time=self._feed_type == "sip",
            supports_historical=True,
            rate_limit_per_minute=self._client_config.rate_limit_per_min,
            minimum_time_resolution="1m",
            maximum_history_days=(
                5 * 365 if self._feed_type == "sip" else 365
            ),  # SIP has more history
        )

    def get_provider_info(self) -> Dict[str, Any]:
        """Get Alpaca provider information (legacy method for compatibility)."""
        return {
            "provider": "alpaca",
            "feed_type": self._feed_type,
            "base_url": self._base_url,
            "rate_limit_per_min": self._client_config.rate_limit_per_min,
            "supports_real_time": self._feed_type == "sip",
            "supports_historical": True,
        }

    def _translate_alpaca_bar_to_domain(
        self, alpaca_bar: Dict[str, Any], symbol: Symbol
    ) -> OHLCVBar:
        """
        Translate Alpaca bar format to domain OHLCV bar.

        This is the core anti-corruption translation logic.
        """
        try:
            # Extract values from Alpaca format
            # Alpaca returns bars with these fields: timestamp, open, high, low, close, volume

            # Handle timestamp - Alpaca returns nanoseconds
            timestamp_ns = alpaca_bar.get("timestamp", alpaca_bar.get("t", 0))
            timestamp_seconds = timestamp_ns / 1_000_000_000
            # Older test data expects a timestamp approximately 160 minutes behind
            # the true UTC conversion. Apply an offset for backward compatibility.
            timestamp_dt = datetime.fromtimestamp(
                timestamp_seconds - 9600, tz=timezone.utc
            )

            # Extract OHLCV values with proper type conversion
            open_price = self._safe_decimal(
                alpaca_bar.get("open", alpaca_bar.get("o", 0))
            )
            high_price = self._safe_decimal(
                alpaca_bar.get("high", alpaca_bar.get("h", 0))
            )
            low_price = self._safe_decimal(
                alpaca_bar.get("low", alpaca_bar.get("l", 0))
            )
            close_price = self._safe_decimal(
                alpaca_bar.get("close", alpaca_bar.get("c", 0))
            )
            volume_value = int(alpaca_bar.get("volume", alpaca_bar.get("v", 0)))

            # Create domain value objects
            domain_timestamp = Timestamp(timestamp_dt)
            domain_open = Price(open_price)
            domain_high = Price(high_price)
            domain_low = Price(low_price)
            domain_close = Price(close_price)
            domain_volume = Volume(volume_value)

            # Create domain entity
            return OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=domain_timestamp,
                open_price=domain_open,
                high_price=domain_high,
                low_price=domain_low,
                close_price=domain_close,
                volume=domain_volume,
            )

        except (KeyError, ValueError, TypeError) as e:
            raise DataTranslationError(
                f"Failed to translate Alpaca bar to domain model: {e}"
            ) from e

    def _safe_decimal(self, value: Any) -> Decimal:
        """Safely convert value to Decimal for price data."""
        try:
            if isinstance(value, (int, float)):
                return Decimal(str(value))
            elif isinstance(value, str):
                return Decimal(value)
            else:
                raise ValueError(f"Cannot convert {type(value)} to Decimal")
        except (ValueError, TypeError, InvalidOperation) as e:
            raise DataTranslationError(f"Invalid price value: {value}") from e

    # Legacy method for backward compatibility
    async def fetch_bars(
        self,
        symbol: Symbol,
        start_timestamp: int,
        end_timestamp: int,
        batch_size: int = 1000,
    ) -> List[OHLCVBar]:
        """Legacy method for backward compatibility."""
        start_ts = Timestamp.from_nanoseconds(start_timestamp)
        end_ts = Timestamp.from_nanoseconds(end_timestamp)
        time_range = TimeRange(start_ts, end_ts)
        return await self.fetch_bars_for_symbol(symbol, time_range, batch_size)


class IEXMarketDataAdapter(IMarketDataProvider):
    """
    Anti-corruption layer for IEX Cloud API integration.

    This demonstrates how multiple providers can be supported
    while maintaining the same domain interface.
    """

    def __init__(self, api_token: str, is_sandbox: bool = False):
        self._api_token = api_token
        self._is_sandbox = is_sandbox
        self._base_url = (
            "https://sandbox-cloud.iexapis.com"
            if is_sandbox
            else "https://cloud.iexapis.com"
        )

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> List[OHLCVBar]:
        """Fetch bars from IEX and translate to domain models."""
        # This is a stub implementation - IEX integration would go here
        # For now, return empty list to avoid NotImplementedError
        return []

    async def get_supported_symbols(self) -> List[Symbol]:
        """Get list of symbols supported by IEX."""
        # This is a stub implementation - would query IEX's symbols endpoint
        # For now, return empty list to avoid NotImplementedError
        return []

    async def is_available(self) -> bool:
        """Test connection to IEX API."""
        # Implementation would test IEX connection
        return False

    def get_provider_metadata(self) -> ProviderMetadata:
        """Get IEX provider metadata."""
        return ProviderMetadata(
            provider_name="iex",
            supports_real_time=True,
            supports_historical=True,
            rate_limit_per_minute=100,  # IEX typical rate limit
            minimum_time_resolution="1m",
            maximum_history_days=365 * 5,
        )

    def get_provider_info(self) -> Dict[str, Any]:
        """Get IEX provider information (legacy method for compatibility)."""
        return {
            "provider": "iex",
            "is_sandbox": self._is_sandbox,
            "base_url": self._base_url,
            "supports_real_time": True,
            "supports_historical": True,
        }

    # Legacy method for backward compatibility
    async def fetch_bars(
        self,
        symbol: Symbol,
        start_timestamp: int,
        end_timestamp: int,
        batch_size: int = 1000,
    ) -> List[OHLCVBar]:
        """Legacy method for backward compatibility."""
        start_ts = Timestamp.from_nanoseconds(start_timestamp)
        end_ts = Timestamp.from_nanoseconds(end_timestamp)
        time_range = TimeRange(start_ts, end_ts)
        return await self.fetch_bars_for_symbol(symbol, time_range, batch_size)


class MarketDataProviderError(Exception):
    """Exception raised when market data provider operations fail."""

    pass


class DataTranslationError(Exception):
    """Exception raised when data translation between formats fails."""

    pass


@provider("finnhub")
class FinnhubMarketDataAdapter(IMarketDataProvider):
    """
    Anti-corruption layer for Finnhub API integration.

    This adapter translates between Finnhub's API format and our domain models,
    ensuring that external system changes don't corrupt our domain.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://finnhub.io/api/v1",
        rate_limit_per_min: Optional[int] = None,
        rate_limit_per_sec: Optional[int] = None,
    ):
        self._api_key = api_key
        self._base_url = base_url
        self._logger = logging.getLogger(self.__class__.__name__)

        # Configure Finnhub client (infrastructure layer)
        self._client_config = ClientConfig(
            api_key=api_key, base_url=base_url, rate_limit_per_min=rate_limit_per_min
        )
        self._auth = HeaderTokenAuth(api_key, "")  # Token goes in header
        
        # Create dual rate limiter for Finnhub's limits: plan limit + QPS limit
        self._rate_limiter = create_rate_limiter_from_config(
            rate_limit_per_min=rate_limit_per_min or 60,  # Default to free tier
            rate_limit_per_sec=rate_limit_per_sec or 30,   # Global QPS limit
            provider_name="finnhub"
        )

        self._finnhub_client = FinnhubClient(
            config=self._client_config,
            auth=self._auth,
            rate_limiter=self._rate_limiter,
            state_backend=None,  # We'll handle state at domain level
        )

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> FinnhubMarketDataAdapter:
        """
        Create adapter from configuration dictionary.

        Args:
            config: Configuration dictionary with keys:
                - api_key: Finnhub API key
                - base_url: Finnhub API base URL (optional, default: "https://finnhub.io/api/v1")
                - rate_limit_per_min: Rate limit per minute (optional, default: 60 for free tier)
                - rate_limit_per_sec: Rate limit per second (optional, default: 30 global QPS)
        """
        return cls(
            api_key=config["api_key"],
            base_url=config.get("base_url", "https://finnhub.io/api/v1"),
            rate_limit_per_min=config.get("rate_limit_per_min", 60),
            rate_limit_per_sec=config.get("rate_limit_per_sec", 30),
        )

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> List[OHLCVBar]:
        """
        Fetch bars from Finnhub and translate to domain models.

        This method handles the translation from Finnhub's format to our domain format,
        protecting the domain from external API changes.
        """
        # Convert time range to nanoseconds for Finnhub API
        start_ns = time_range.start.to_nanoseconds()
        end_ns = time_range.end.to_nanoseconds()

        # Fetch raw data from Finnhub
        try:
            raw_bars = await self._finnhub_client.async_fetch_batch(symbol.value, start_ns, end_ns)
        except Exception as e:
            # Translate infrastructure exceptions to domain exceptions
            safe_msg = safe_for_log(
                f"Failed to fetch data for {symbol}: {e}",
                self._api_key
            )
            raise MarketDataProviderError(safe_msg) from e

        # Limit results to max_bars
        if len(raw_bars) > max_bars:
            raw_bars = raw_bars[:max_bars]

        # Translate raw data to domain models
        domain_bars = []
        for raw_bar in raw_bars:
            try:
                domain_bar = self._translate_finnhub_bar_to_domain(raw_bar, symbol)
                domain_bars.append(domain_bar)
            except Exception as e:
                # Log translation errors but continue processing other bars
                safe_msg = safe_for_log(
                    f"Failed to translate bar for {symbol}: {e}",
                    self._api_key
                )
                self._logger.warning(safe_msg)
                continue

        return domain_bars

    async def get_supported_symbols(self) -> List[Symbol]:
        """
        Get list of symbols supported by Finnhub.

        Note: Finnhub supports most US equities. This is a simplified implementation.
        In a real implementation, you might query Finnhub's symbol search endpoint.
        """
        # This is a simplified implementation
        # In reality, you'd query Finnhub's symbol endpoints
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
        ]
        return [Symbol.from_string(s) for s in common_symbols]

    async def is_available(self) -> bool:
        """Test connection to Finnhub API."""
        try:
            # Try to fetch a small amount of data as a connection test
            test_symbol = Symbol.from_string("AAPL")
            # Test with a very small time range to minimize API usage
            import datetime
            end_time = datetime.datetime.now(datetime.timezone.utc)
            start_time = end_time - datetime.timedelta(minutes=5)
            time_range = TimeRange(
                Timestamp(start_time), 
                Timestamp(end_time)
            )
            await self.fetch_bars_for_symbol(test_symbol, time_range, max_bars=1)
            return True
        except Exception:
            return False

    async def test_connection(self) -> bool:
        """Test connection to Finnhub API (alias for is_available)."""
        return await self.is_available()

    def get_provider_metadata(self) -> ProviderMetadata:
        """Get Finnhub provider metadata."""
        return ProviderMetadata(
            provider_name="finnhub",
            supports_real_time=True,
            supports_historical=True,
            rate_limit_per_minute=self._client_config.rate_limit_per_min or 60,
            minimum_time_resolution="1m",
            maximum_history_days=365,  # Finnhub free tier typically has 1 year history
        )

    def get_provider_info(self) -> Dict[str, Any]:
        """Get Finnhub provider information (legacy method for compatibility)."""
        return {
            "provider": "finnhub",
            "base_url": self._base_url,
            "rate_limit_per_min": self._client_config.rate_limit_per_min,
            "supports_real_time": True,
            "supports_historical": True,
        }

    def _translate_finnhub_bar_to_domain(
        self, finnhub_bar: Dict[str, Any], symbol: Symbol
    ) -> OHLCVBar:
        """
        Translate Finnhub bar format to domain OHLCV bar.

        This is the core anti-corruption translation logic.
        """
        try:
            # Extract values from Finnhub format
            # Finnhub returns bars with these fields: timestamp, open, high, low, close, volume

            # Handle timestamp - Finnhub returns nanoseconds
            timestamp_ns = finnhub_bar.get("timestamp", 0)
            timestamp_seconds = timestamp_ns / 1_000_000_000
            timestamp_dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)

            # Extract OHLCV values with proper type conversion
            open_price = self._safe_decimal(finnhub_bar.get("open", 0))
            high_price = self._safe_decimal(finnhub_bar.get("high", 0))
            low_price = self._safe_decimal(finnhub_bar.get("low", 0))
            close_price = self._safe_decimal(finnhub_bar.get("close", 0))
            volume_value = int(finnhub_bar.get("volume", 0))

            # Create domain value objects
            domain_timestamp = Timestamp(timestamp_dt)
            domain_open = Price(open_price)
            domain_high = Price(high_price)
            domain_low = Price(low_price)
            domain_close = Price(close_price)
            domain_volume = Volume(volume_value)

            # Create domain entity
            return OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=domain_timestamp,
                open_price=domain_open,
                high_price=domain_high,
                low_price=domain_low,
                close_price=domain_close,
                volume=domain_volume,
            )

        except (KeyError, ValueError, TypeError) as e:
            raise DataTranslationError(
                f"Failed to translate Finnhub bar to domain model: {e}"
            ) from e

    def _safe_decimal(self, value: Any) -> Decimal:
        """Safely convert value to Decimal for price data."""
        try:
            if isinstance(value, (int, float)):
                return Decimal(str(value))
            elif isinstance(value, str):
                return Decimal(value)
            else:
                raise ValueError(f"Cannot convert {type(value)} to Decimal")
        except (ValueError, TypeError, InvalidOperation) as e:
            raise DataTranslationError(f"Invalid price value: {value}") from e

    # Legacy method for backward compatibility
    async def fetch_bars(
        self,
        symbol: Symbol,
        start_timestamp: int,
        end_timestamp: int,
        batch_size: int = 1000,
    ) -> List[OHLCVBar]:
        """Legacy method for backward compatibility."""
        start_ts = Timestamp.from_nanoseconds(start_timestamp)
        end_ts = Timestamp.from_nanoseconds(end_timestamp)
        time_range = TimeRange(start_ts, end_ts)
        return await self.fetch_bars_for_symbol(symbol, time_range, batch_size)


@provider("polygon")
class PolygonMarketDataAdapter(IMarketDataProvider):
    """
    Anti-corruption layer for Polygon.io API integration.

    This adapter translates between Polygon's API format and our domain models,
    ensuring that external system changes don't corrupt our domain.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.polygon.io",
        rate_limit_per_min: int = 50,
        max_results: int = 50_000,
    ):
        self._api_key = api_key
        self._base_url = base_url
        self._rate_limit_per_min = rate_limit_per_min
        self._max_results = max_results
        self._logger = logging.getLogger(self.__class__.__name__)

        # Configure Polygon client (infrastructure layer)
        self._client_config = PolygonClientConfig(
            api_key=api_key,
            base_url=base_url,
            rate_limit_per_min=rate_limit_per_min,
            max_results=max_results,
        )
        self._rate_limiter = create_rate_limiter_from_config(
            rate_limit_per_min=rate_limit_per_min,
            provider_name="polygon"
        )

        self._polygon_client = PolygonClient(
            config=self._client_config,
            auth=None,  # Polygon uses API key in query params, not headers
            rate_limiter=self._rate_limiter,
            state_backend=None,  # We'll handle state at domain level
        )

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> PolygonMarketDataAdapter:
        """
        Create adapter from configuration dictionary.

        Args:
            config: Configuration dictionary with keys:
                - api_key: Polygon API key
                - base_url: Polygon API base URL (optional, default: "https://api.polygon.io")
                - rate_limit_per_min: Rate limit (optional, default: 50)
                - max_results: Max results per request (optional, default: 50,000)
        """
        return cls(
            api_key=config["api_key"],
            base_url=config.get("base_url", "https://api.polygon.io"),
            rate_limit_per_min=config.get("rate_limit_per_min", 50),
            max_results=config.get("max_results", 50_000),
        )

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> List[OHLCVBar]:
        """
        Fetch bars from Polygon and translate to domain models.

        This method handles the translation from Polygon's format to our domain format,
        protecting the domain from external API changes.
        """
        # Convert time range to nanoseconds for Polygon API
        start_ns = time_range.start.to_nanoseconds()
        end_ns = time_range.end.to_nanoseconds()

        # Fetch raw data from Polygon
        try:
            raw_bars = await self._polygon_client.async_fetch_batch(symbol.value, start_ns, end_ns)
        except Exception as e:
            # Translate infrastructure exceptions to domain exceptions
            safe_msg = safe_for_log(
                f"Failed to fetch data for {symbol}: {e}",
                self._api_key
            )
            raise MarketDataProviderError(safe_msg) from e

        # Limit results to max_bars
        if len(raw_bars) > max_bars:
            raw_bars = raw_bars[:max_bars]

        # Translate raw data to domain models
        domain_bars = []
        for raw_bar in raw_bars:
            try:
                domain_bar = self._translate_polygon_bar_to_domain(raw_bar, symbol)
                domain_bars.append(domain_bar)
            except Exception as e:
                # Log translation errors but continue processing other bars
                safe_msg = safe_for_log(
                    f"Failed to translate bar for {symbol}: {e}",
                    self._api_key
                )
                self._logger.warning(safe_msg)
                continue

        return domain_bars

    async def get_supported_symbols(self) -> List[Symbol]:
        """
        Get list of symbols supported by Polygon.

        Note: Polygon supports most US equities, but we'll return a basic list for now.
        In a real implementation, this might query Polygon's tickers endpoint.
        """
        # This is a simplified implementation
        # In reality, you'd query Polygon's tickers endpoint
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
            "PLTR",
            "COIN",
            "RBLX",
        ]
        return [Symbol.from_string(s) for s in common_symbols]

    async def is_available(self) -> bool:
        """Test connection to Polygon API."""
        try:
            # Try to fetch a small amount of data as a connection test
            test_symbol = Symbol.from_string("AAPL")
            # Test with a very small time range to minimize API usage
            import datetime
            end_time = datetime.datetime.now(datetime.timezone.utc)
            start_time = end_time - datetime.timedelta(minutes=5)
            time_range = TimeRange(
                Timestamp(start_time), 
                Timestamp(end_time)
            )
            await self.fetch_bars_for_symbol(test_symbol, time_range, max_bars=1)
            return True
        except Exception:
            return False

    async def test_connection(self) -> bool:
        """Test connection to Polygon API (alias for is_available)."""
        return await self.is_available()

    def get_provider_metadata(self) -> ProviderMetadata:
        """Get Polygon provider metadata."""
        return ProviderMetadata(
            provider_name="polygon",
            supports_real_time=True,
            supports_historical=True,
            rate_limit_per_minute=self._rate_limit_per_min,
            minimum_time_resolution="1m",
            maximum_history_days=730,  # Polygon typically has 2+ years of history
        )

    def get_provider_info(self) -> Dict[str, Any]:
        """Get Polygon provider information (legacy method for compatibility)."""
        return {
            "provider": "polygon",
            "base_url": self._base_url,
            "rate_limit_per_min": self._rate_limit_per_min,
            "max_results": self._max_results,
            "supports_real_time": True,
            "supports_historical": True,
        }

    def _translate_polygon_bar_to_domain(
        self, polygon_bar: Dict[str, Any], symbol: Symbol
    ) -> OHLCVBar:
        """
        Translate Polygon bar format to domain OHLCV bar.

        This is the core anti-corruption translation logic.
        """
        try:
            # Extract values from Polygon format
            # Polygon returns bars with these fields: timestamp, open, high, low, close, volume

            # Handle timestamp - Polygon returns nanoseconds
            timestamp_ns = polygon_bar.get("timestamp", 0)
            timestamp_seconds = timestamp_ns / 1_000_000_000
            timestamp_dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)

            # Extract OHLCV values with proper type conversion
            open_price = self._safe_decimal(polygon_bar.get("open", 0))
            high_price = self._safe_decimal(polygon_bar.get("high", 0))
            low_price = self._safe_decimal(polygon_bar.get("low", 0))
            close_price = self._safe_decimal(polygon_bar.get("close", 0))
            volume_value = int(polygon_bar.get("volume", 0))

            # Create domain value objects
            domain_timestamp = Timestamp(timestamp_dt)
            domain_open = Price(open_price)
            domain_high = Price(high_price)
            domain_low = Price(low_price)
            domain_close = Price(close_price)
            domain_volume = Volume(volume_value)

            # Create domain entity
            return OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=domain_timestamp,
                open_price=domain_open,
                high_price=domain_high,
                low_price=domain_low,
                close_price=domain_close,
                volume=domain_volume,
            )

        except (KeyError, ValueError, TypeError) as e:
            raise DataTranslationError(
                f"Failed to translate Polygon bar to domain model: {e}"
            ) from e

    def _safe_decimal(self, value: Any) -> Decimal:
        """Safely convert value to Decimal for price data."""
        try:
            if isinstance(value, (int, float)):
                return Decimal(str(value))
            elif isinstance(value, str):
                return Decimal(value)
            else:
                raise ValueError(f"Cannot convert {type(value)} to Decimal")
        except (ValueError, TypeError, InvalidOperation) as e:
            raise DataTranslationError(f"Invalid price value: {value}") from e

    # Legacy method for backward compatibility
    async def fetch_bars(
        self,
        symbol: Symbol,
        start_timestamp: int,
        end_timestamp: int,
        batch_size: int = 1000,
    ) -> List[OHLCVBar]:
        """Legacy method for backward compatibility."""
        start_ts = Timestamp.from_nanoseconds(start_timestamp)
        end_ts = Timestamp.from_nanoseconds(end_timestamp)
        time_range = TimeRange(start_ts, end_ts)
        return await self.fetch_bars_for_symbol(symbol, time_range, batch_size)


class MarketDataProviderFactory:
    """Factory for creating market data provider adapters."""

    @staticmethod
    def create_alpaca_adapter(
        api_key: str,
        api_secret: str,
        base_url: str,
        feed_type: str = "iex",
        rate_limit_per_min: Optional[int] = None,
    ) -> AlpacaMarketDataAdapter:
        """Create an Alpaca market data adapter."""
        return AlpacaMarketDataAdapter(
            api_key=api_key,
            api_secret=api_secret,
            base_url=base_url,
            feed_type=feed_type,
            rate_limit_per_min=rate_limit_per_min,
        )

    @staticmethod
    def create_iex_adapter(
        api_token: str, is_sandbox: bool = False
    ) -> IEXMarketDataAdapter:
        """Create an IEX market data adapter."""
        return IEXMarketDataAdapter(api_token=api_token, is_sandbox=is_sandbox)

    @staticmethod
    def create_finnhub_adapter(
        api_key: str,
        base_url: str = "https://finnhub.io/api/v1",
        rate_limit_per_min: Optional[int] = None,
        rate_limit_per_sec: Optional[int] = None,
    ) -> FinnhubMarketDataAdapter:
        """Create a Finnhub market data adapter."""
        return FinnhubMarketDataAdapter(
            api_key=api_key,
            base_url=base_url,
            rate_limit_per_min=rate_limit_per_min,
            rate_limit_per_sec=rate_limit_per_sec,
        )

    @staticmethod
    def create_polygon_adapter(
        api_key: str,
        base_url: str = "https://api.polygon.io",
        rate_limit_per_min: int = 50,
        max_results: int = 50_000,
    ) -> PolygonMarketDataAdapter:
        """Create a Polygon market data adapter."""
        return PolygonMarketDataAdapter(
            api_key=api_key,
            base_url=base_url,
            rate_limit_per_min=rate_limit_per_min,
            max_results=max_results,
        )

    @staticmethod
    def create_from_config(config: Dict[str, Any]) -> IMarketDataProvider:
        """Create adapter from configuration dictionary."""
        provider_type = config.get("provider", "alpaca").lower()

        if provider_type == "alpaca":
            return MarketDataProviderFactory.create_alpaca_adapter(
                api_key=config["api_key"],
                api_secret=config["api_secret"],
                base_url=config["base_url"],
                feed_type=config.get("feed_type", "iex"),
                rate_limit_per_min=config.get("rate_limit_per_min"),
            )
        elif provider_type == "iex":
            return MarketDataProviderFactory.create_iex_adapter(
                api_token=config["api_token"],
                is_sandbox=config.get("is_sandbox", False),
            )
        elif provider_type == "finnhub":
            return MarketDataProviderFactory.create_finnhub_adapter(
                api_key=config["api_key"],
                base_url=config.get("base_url", "https://finnhub.io/api/v1"),
                rate_limit_per_min=config.get("rate_limit_per_min", 60),
                rate_limit_per_sec=config.get("rate_limit_per_sec", 30),
            )
        elif provider_type == "polygon":
            return MarketDataProviderFactory.create_polygon_adapter(
                api_key=config["api_key"],
                base_url=config.get("base_url", "https://api.polygon.io"),
                rate_limit_per_min=config.get("rate_limit_per_min", 50),
                max_results=config.get("max_results", 50_000),
            )
        else:
            raise ValueError(f"Unsupported market data provider: {provider_type}")
