# SPDX-License-Identifier: Apache-2.0
"""Repository interfaces for MarketPipe domain.

Repositories provide a domain-focused interface for data access,
abstracting the underlying persistence mechanism. They are defined
in the domain layer as interfaces and implemented in infrastructure.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import date

# Import concrete implementations (only in type checking to keep interfaces clean)
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from .aggregates import DailySummary, SymbolBarsAggregate, UniverseAggregate
from .entities import OHLCVBar
from .value_objects import Symbol, TimeRange, Timestamp

if TYPE_CHECKING:
    pass


class ISymbolBarsRepository(ABC):
    """Repository interface for symbol bars aggregates.

    Provides methods for loading, saving, and querying symbol bar aggregates
    which represent all bars for a specific symbol on a trading date.
    """

    @abstractmethod
    async def get_by_symbol_and_date(
        self, symbol: Symbol, trading_date: date
    ) -> Optional[SymbolBarsAggregate]:
        """Load aggregate for symbol and trading date.

        Args:
            symbol: The financial instrument symbol
            trading_date: The trading date

        Returns:
            SymbolBarsAggregate if found, None otherwise
        """
        ...

    @abstractmethod
    async def save(self, aggregate: SymbolBarsAggregate) -> None:
        """Save aggregate and publish domain events.

        Args:
            aggregate: The symbol bars aggregate to save

        Raises:
            ConcurrencyError: If aggregate has been modified by another process
        """
        ...

    @abstractmethod
    async def find_symbols_with_data(self, start_date: date, end_date: date) -> List[Symbol]:
        """Find symbols that have data in the specified date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of symbols that have data in the date range
        """
        ...

    @abstractmethod
    async def get_completion_status(
        self, symbols: List[Symbol], trading_dates: List[date]
    ) -> Dict[str, Dict[str, bool]]:
        """Get completion status for symbol/date combinations.

        Args:
            symbols: List of symbols to check
            trading_dates: List of trading dates to check

        Returns:
            Nested dictionary: {symbol: {date: is_complete}}
        """
        ...

    @abstractmethod
    async def delete(self, symbol: Symbol, trading_date: date) -> bool:
        """Delete aggregate for symbol and trading date.

        Args:
            symbol: The financial instrument symbol
            trading_date: The trading date

        Returns:
            True if aggregate was deleted, False if not found
        """
        ...


class IOHLCVRepository(ABC):
    """Repository interface for individual OHLCV bars.

    Provides methods for querying individual bars across symbols and dates.
    This is separate from the aggregate repository to support different
    query patterns (streaming, filtering, etc.).
    """

    @abstractmethod
    async def get_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange
    ) -> AsyncIterator[OHLCVBar]:
        """Stream bars for symbol in time range.

        Args:
            symbol: The financial instrument symbol
            time_range: Time range to query

        Yields:
            OHLCVBar instances in chronological order
        """
        ...

    @abstractmethod
    async def get_bars_for_symbols(
        self, symbols: List[Symbol], time_range: TimeRange
    ) -> AsyncIterator[OHLCVBar]:
        """Stream bars for multiple symbols in time range.

        Args:
            symbols: List of financial instrument symbols
            time_range: Time range to query

        Yields:
            OHLCVBar instances sorted by timestamp then symbol
        """
        ...

    @abstractmethod
    async def save_bars(self, bars: List[OHLCVBar]) -> None:
        """Batch save multiple bars.

        Args:
            bars: List of OHLCV bars to save

        Raises:
            ValidationError: If any bars are invalid
            DuplicateKeyError: If bars with same symbol/timestamp already exist
        """
        ...

    @abstractmethod
    async def exists(self, symbol: Symbol, timestamp: Timestamp) -> bool:
        """Check if bar exists for symbol at timestamp.

        Args:
            symbol: The financial instrument symbol
            timestamp: The timestamp to check

        Returns:
            True if bar exists
        """
        ...

    @abstractmethod
    async def count_bars(self, symbol: Symbol, time_range: Optional[TimeRange] = None) -> int:
        """Count bars for symbol in optional time range.

        Args:
            symbol: The financial instrument symbol
            time_range: Optional time range filter

        Returns:
            Number of bars
        """
        ...

    @abstractmethod
    async def get_latest_timestamp(self, symbol: Symbol) -> Optional[Timestamp]:
        """Get the latest timestamp for a symbol.

        Args:
            symbol: The financial instrument symbol

        Returns:
            Latest timestamp if data exists, None otherwise
        """
        ...

    @abstractmethod
    async def delete_bars(self, symbol: Symbol, time_range: Optional[TimeRange] = None) -> int:
        """Delete bars for symbol in optional time range.

        Args:
            symbol: The financial instrument symbol
            time_range: Optional time range filter (all data if None)

        Returns:
            Number of bars deleted
        """
        ...


class IUniverseRepository(ABC):
    """Repository interface for universe aggregates.

    Manages the universe of symbols being tracked by the system.
    """

    @abstractmethod
    async def get_by_id(self, universe_id: str) -> Optional[UniverseAggregate]:
        """Load universe by ID.

        Args:
            universe_id: Unique identifier for the universe

        Returns:
            UniverseAggregate if found, None otherwise
        """
        ...

    @abstractmethod
    async def save(self, universe: UniverseAggregate) -> None:
        """Save universe aggregate.

        Args:
            universe: The universe aggregate to save
        """
        ...

    @abstractmethod
    async def get_default_universe(self) -> UniverseAggregate:
        """Get the default universe (create if doesn't exist).

        Returns:
            The default universe aggregate
        """
        ...

    @abstractmethod
    async def list_universes(self) -> List[str]:
        """List all universe IDs.

        Returns:
            List of universe identifiers
        """
        ...


class IDailySummaryRepository(ABC):
    """Repository interface for daily summary data.

    Provides access to aggregated daily OHLCV data calculated from minute bars.
    """

    @abstractmethod
    async def get_summary(self, symbol: Symbol, trading_date: date) -> Optional[DailySummary]:
        """Get daily summary for symbol and date.

        Args:
            symbol: The financial instrument symbol
            trading_date: The trading date

        Returns:
            DailySummary if available, None otherwise
        """
        ...

    @abstractmethod
    async def get_summaries(
        self, symbol: Symbol, start_date: date, end_date: date
    ) -> List[DailySummary]:
        """Get daily summaries for symbol in date range.

        Args:
            symbol: The financial instrument symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of daily summaries sorted by trading date
        """
        ...

    @abstractmethod
    async def save_summary(self, summary: DailySummary) -> None:
        """Save daily summary.

        Args:
            summary: The daily summary to save
        """
        ...

    @abstractmethod
    async def save_summaries(self, summaries: List[DailySummary]) -> None:
        """Batch save daily summaries.

        Args:
            summaries: List of daily summaries to save
        """
        ...

    @abstractmethod
    async def delete_summary(self, symbol: Symbol, trading_date: date) -> bool:
        """Delete daily summary.

        Args:
            symbol: The financial instrument symbol
            trading_date: The trading date

        Returns:
            True if summary was deleted, False if not found
        """
        ...


class ICheckpointRepository(ABC):
    """Repository interface for ingestion checkpoints.

    Manages progress checkpoints for resumable ingestion operations.
    """

    @abstractmethod
    async def save_checkpoint(self, symbol: Symbol, checkpoint_data: Dict[str, Any]) -> None:
        """Save ingestion checkpoint for symbol.

        Args:
            symbol: The financial instrument symbol
            checkpoint_data: Checkpoint state data
        """
        ...

    @abstractmethod
    async def get_checkpoint(self, symbol: Symbol) -> Optional[Dict[str, Any]]:
        """Get ingestion checkpoint for symbol.

        Args:
            symbol: The financial instrument symbol

        Returns:
            Checkpoint data if exists, None otherwise
        """
        ...

    @abstractmethod
    async def delete_checkpoint(self, symbol: Symbol) -> bool:
        """Delete checkpoint for symbol.

        Args:
            symbol: The financial instrument symbol

        Returns:
            True if checkpoint was deleted, False if not found
        """
        ...

    @abstractmethod
    async def list_checkpoints(self) -> List[Symbol]:
        """List all symbols with checkpoints.

        Returns:
            List of symbols that have checkpoints
        """
        ...


class IMarketDataProviderRepository(ABC):
    """Repository interface for market data provider configurations.

    Manages configuration and metadata for external market data providers.
    """

    @abstractmethod
    async def get_provider_config(self, provider_id: str) -> Optional[Dict[str, Any]]:
        """Get configuration for market data provider.

        Args:
            provider_id: Unique identifier for the provider

        Returns:
            Provider configuration if found, None otherwise
        """
        ...

    @abstractmethod
    async def save_provider_config(self, provider_id: str, config: Dict[str, Any]) -> None:
        """Save configuration for market data provider.

        Args:
            provider_id: Unique identifier for the provider
            config: Provider configuration data
        """
        ...

    @abstractmethod
    async def list_providers(self) -> List[str]:
        """List all configured provider IDs.

        Returns:
            List of provider identifiers
        """
        ...

    @abstractmethod
    async def is_provider_available(self, provider_id: str) -> bool:
        """Check if provider is available and properly configured.

        Args:
            provider_id: Unique identifier for the provider

        Returns:
            True if provider is available
        """
        ...


# Repository exceptions
class RepositoryError(Exception):
    """Base exception for repository operations."""

    ...


class ConcurrencyError(RepositoryError):
    """Raised when optimistic concurrency control fails."""

    ...


class DuplicateKeyError(RepositoryError):
    """Raised when attempting to save data with duplicate key."""

    ...


class ValidationError(RepositoryError):
    """Raised when data fails repository validation."""

    ...


class NotFoundError(RepositoryError):
    """Raised when requested data is not found."""

    ...
