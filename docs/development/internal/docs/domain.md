# Domain Module

## Purpose

The domain module implements MarketPipe's core business logic using Domain-Driven Design (DDD) patterns. It provides the foundational entities, value objects, aggregates, events, and services that represent financial market data concepts independent of infrastructure concerns.

## Key Public Interfaces

### Entities
```python
from marketpipe.domain import OHLCVBar, EntityId

# Create OHLCV bar entity with validation
bar = OHLCVBar(
    id=EntityId.generate(),
    symbol=Symbol("AAPL"),
    timestamp=Timestamp.now(),
    open_price=Price.from_float(150.0),
    high_price=Price.from_float(152.0),
    low_price=Price.from_float(149.0),
    close_price=Price.from_float(151.0),
    volume=Volume(1000000)
)

# Business methods
price_range = bar.calculate_price_range()
price_change = bar.calculate_price_change_percentage()
```

### Value Objects
```python
from marketpipe.domain import Symbol, Price, Timestamp, Volume, TimeRange

# Financial value objects with validation
symbol = Symbol("AAPL")  # Validates format
price = Price.from_float(123.45)  # 4 decimal precision
timestamp = Timestamp.from_iso("2024-01-01T09:30:00Z")
volume = Volume(1000000)

# Time range for queries
time_range = TimeRange.from_dates(
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 2)
)
```

### Aggregates
```python
from marketpipe.domain import SymbolBarsAggregate

# Create aggregate for consistency boundary
aggregate = SymbolBarsAggregate(Symbol("AAPL"), date(2024, 1, 1))

# Start collection and add bars
aggregate.start_collection()
aggregate.add_bar(bar)  # Enforces business rules
aggregate.complete_collection()

# Calculate daily summary
summary = aggregate.calculate_daily_summary()
events = aggregate.get_uncommitted_events()
```

### Domain Events
```python
from marketpipe.domain import (
    BarCollectionCompleted, ValidationFailed,
    IngestionJobCompleted, MarketDataReceived
)

# Events raised by domain operations
collection_event = BarCollectionCompleted(
    symbol=Symbol("AAPL"),
    trading_date=date(2024, 1, 1),
    bar_count=390
)

validation_event = ValidationFailed(
    symbol=Symbol("AAPL"),
    timestamp=Timestamp.now(),
    error_message="OHLC consistency violation"
)
```

### Domain Services
```python
from marketpipe.domain.services import (
    OHLCVCalculationService, MarketDataValidationService
)

calc_service = OHLCVCalculationService()
validation_service = MarketDataValidationService()

# Calculate VWAP and daily summary
vwap = calc_service.vwap(bars)
daily_summary = calc_service.daily_summary(bars)

# Validate bars with business rules
errors = validation_service.validate_bar(bar)
batch_errors = validation_service.validate_batch(bars)
```

### Market Data Provider Port
```python
from marketpipe.domain.market_data import IMarketDataProvider

# Domain-level interface for data providers
class MyProvider(IMarketDataProvider):
    async def fetch_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange, max_bars: int = 1000
    ) -> List[OHLCVBar]:
        # Implementation returns domain entities
        pass
```

## Brief Call Graph

```
CLI Commands
    ↓
Application Services
    ↓
Domain Aggregates ←→ Domain Events
    ↓
Domain Entities ←→ Domain Services
    ↓
Value Objects
```

### Key Patterns

1. **Entity Lifecycle**: `EntityId.generate()` → `Entity.__init__()` → business methods → events
2. **Aggregate Operations**: `start_collection()` → `add_bar()` → `complete_collection()` → `get_uncommitted_events()`
3. **Value Object Creation**: `Symbol.from_string()` → validation → immutable instance
4. **Event Publishing**: Domain operations → event creation → event bus → handlers

## Examples

### Basic OHLCV Bar Creation
```python
@Code(src/marketpipe/domain/entities.py:69-88)
# OHLCVBar entity with business validation
bar = OHLCVBar(
    id=EntityId.generate(),
    symbol=Symbol("AAPL"),
    timestamp=Timestamp.from_iso("2024-01-01T09:30:00Z"),
    open_price=Price.from_float(150.0),
    high_price=Price.from_float(152.0),
    low_price=Price.from_float(149.0),
    close_price=Price.from_float(151.0),
    volume=Volume(1000000)
)
```

### Aggregate with Business Rules
```python
@Code(src/marketpipe/domain/aggregates.py:24-85)
# Symbol bars aggregate enforcing consistency
aggregate = SymbolBarsAggregate(Symbol("AAPL"), date(2024, 1, 1))
aggregate.start_collection()

for bar in minute_bars:
    aggregate.add_bar(bar)  # Validates symbol, date, no duplicates

aggregate.complete_collection()
events = aggregate.get_uncommitted_events()  # Domain events
```

### Value Object Validation
```python
@Code(src/marketpipe/domain/value_objects.py:24-44)
# Symbol with format validation
symbol = Symbol("AAPL")  # Validates 1-10 uppercase letters

@Code(src/marketpipe/domain/value_objects.py:60-78)
# Price with decimal precision
price = Price.from_float(123.456789)  # Quantized to 4 decimals
```

### Domain Service Calculations
```python
@Code(src/marketpipe/domain/services.py:38-68)
# VWAP calculation service
calc_service = OHLCVCalculationService()
vwap_value = calc_service.vwap(bars)  # Volume-weighted average price

@Code(src/marketpipe/domain/services.py:70-118)
# Daily summary aggregation
daily_summary = calc_service.daily_summary(minute_bars)
```

### Event-Driven Architecture
```python
@Code(src/marketpipe/domain/events.py:100-125)
# Bar collection completed event
event = BarCollectionCompleted(
    symbol=Symbol("AAPL"),
    trading_date=date(2024, 1, 1),
    bar_count=390,
    has_gaps=False
)

@Code(src/marketpipe/domain/events.py:148-175)
# Validation failure event
validation_event = ValidationFailed(
    symbol=Symbol("AAPL"),
    timestamp=Timestamp.now(),
    error_message="High price below low price",
    rule_id="ohlc_consistency"
)
```

### Market Data Provider Interface
```python
@Code(src/marketpipe/domain/market_data.py:42-65)
# Domain-level provider interface
class AlpacaProvider(IMarketDataProvider):
    async def fetch_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange, max_bars: int = 1000
    ) -> List[OHLCVBar]:
        # Returns domain entities, not DTOs
        pass
```

## Architecture Benefits

- **Pure Domain Logic**: No infrastructure dependencies in domain layer
- **Business Rule Enforcement**: Entities and aggregates validate invariants
- **Event-Driven Communication**: Loose coupling between bounded contexts
- **Type Safety**: Value objects prevent primitive obsession
- **Testability**: Domain logic easily unit tested in isolation
