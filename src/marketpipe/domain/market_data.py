# src/marketpipe/domain/market_data.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from .value_objects import Symbol, TimeRange
from .entities import OHLCVBar


# ---------- domain exceptions ----------
class MarketDataUnavailableError(Exception):
    """Raised when a provider cannot fulfil the request."""


class InvalidSymbolError(Exception):
    """Raised when a symbol is not supported by the provider."""


# ---------- metadata ----------
@dataclass(frozen=True)
class ProviderMetadata:
    provider_name: str
    supports_real_time: bool
    supports_historical: bool
    rate_limit_per_minute: Optional[int]
    minimum_time_resolution: str              # e.g. "1m"
    maximum_history_days: Optional[int]


# ---------- domain port ----------
class IMarketDataProvider(ABC):
    """Domain-level port for fetching OHLCV data."""

    @abstractmethod
    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> List[OHLCVBar]:
        """
        Fetch OHLCV bars for a symbol within a time range.
        
        Args:
            symbol: The financial symbol to fetch
            time_range: Time range to fetch data for
            max_bars: Maximum number of bars to fetch
            
        Returns:
            List of domain OHLCV bar entities
            
        Raises:
            MarketDataUnavailableError: When provider cannot fulfill request
            InvalidSymbolError: When symbol is not supported
        """
        ...

    @abstractmethod
    async def get_supported_symbols(self) -> List[Symbol]:
        """
        Get list of symbols supported by this provider.
        
        Returns:
            List of supported symbols
        """
        ...

    @abstractmethod
    async def is_available(self) -> bool:
        """
        Test if the provider is currently available.
        
        Returns:
            True if provider is available, False otherwise
        """
        ...

    @abstractmethod
    def get_provider_metadata(self) -> ProviderMetadata:
        """
        Get metadata about this provider's capabilities.
        
        Returns:
            Provider metadata including capabilities and limits
        """
        ... 