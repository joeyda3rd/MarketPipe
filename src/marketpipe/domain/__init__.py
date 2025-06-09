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

from .entities import Entity, EntityId, OHLCVBar
from .value_objects import Symbol, Price, Timestamp, Volume, TimeRange
from .aggregates import SymbolBarsAggregate
from .events import DomainEvent, BarCollectionCompleted, ValidationFailed, IngestionJobCompleted
from .services import DomainService
from .market_data import (
    IMarketDataProvider, 
    ProviderMetadata, 
    MarketDataUnavailableError, 
    InvalidSymbolError
)

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
]