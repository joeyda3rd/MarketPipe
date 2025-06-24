# SPDX-License-Identifier: Apache-2.0
"""Domain model package for MarketPipe.

This package contains the core domain models following Domain-Driven Design principles:
- Entities: Objects with identity and business behavior
- Value Objects: Immutable objects defined by their values
- Aggregates: Consistency boundaries with aggregate roots
- Domain Events: Communication between bounded contexts
- Domain Services: Business operations that don't belong to entities

The domain layer is the heart of the application and should remain
independent of infrastructure concerns.
"""

from .aggregates import SymbolBarsAggregate
from .entities import Entity, EntityId, OHLCVBar
from .events import (
    BarCollectionCompleted,
    DomainEvent,
    IngestionJobCompleted,
    ValidationFailed,
)
from .market_data import (
    IMarketDataProvider,
    InvalidSymbolError,
    MarketDataUnavailableError,
    ProviderMetadata,
)
from .services import DomainService
from .symbol import AssetClass, Status, SymbolRecord
from .value_objects import Price, Symbol, TimeRange, Timestamp, Volume

__all__ = [
    # Base classes
    "Entity",
    "EntityId",
    "DomainEvent",
    "DomainService",
    # Entities
    "OHLCVBar",
    # Value Objects
    "Symbol",
    "Price",
    "Timestamp",
    "Volume",
    "TimeRange",
    # Aggregates
    "SymbolBarsAggregate",
    # Events
    "BarCollectionCompleted",
    "ValidationFailed",
    "IngestionJobCompleted",
    # Market Data Port
    "IMarketDataProvider",
    "ProviderMetadata",
    "MarketDataUnavailableError",
    "InvalidSymbolError",
    # Symbol Master
    "SymbolRecord",
    "AssetClass",
    "Status",
]
