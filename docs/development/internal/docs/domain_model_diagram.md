# MarketPipe Domain Model Diagrams

This document contains UML and Mermaid diagrams illustrating the Domain-Driven Design architecture of MarketPipe after the DDD refactor.

## Bounded Context Overview

```mermaid
graph TB
    subgraph "Core Domain"
        DI[Data Ingestion Context]
    end

    subgraph "Supporting Domains"
        MDI[Market Data Integration Context]
        DV[Data Validation Context]
        DS[Data Storage Context]
        OM[Operations & Monitoring Context]
    end

    subgraph "Generic Subdomains"
        AA[Authentication & Authorization]
        RL[Rate Limiting]
        CM[Configuration Management]
        SM[State Management]
    end

    DI --> MDI
    DI --> DV
    DI --> DS
    DI --> OM

    MDI --> AA
    MDI --> RL

    DI --> SM
    DI --> CM

    classDef coreContext fill:#ffeb9c,stroke:#333,stroke-width:3px
    classDef supportingContext fill:#dae8fc,stroke:#333,stroke-width:2px
    classDef genericContext fill:#f8cecc,stroke:#333,stroke-width:1px

    class DI coreContext
    class MDI,DV,DS,OM supportingContext
    class AA,RL,CM,SM genericContext
```

## DDD Layer Architecture

```mermaid
graph TB
    subgraph "Domain Layer (src/marketpipe/domain/)"
        direction TB
        ENT[Entities:<br/>• OHLCVBar<br/>• EntityId]
        VO[Value Objects:<br/>• Symbol<br/>• Price<br/>• Timestamp<br/>• Volume<br/>• TimeRange]
        AGG[Aggregates:<br/>• SymbolBarsAggregate<br/>• UniverseAggregate]
        EVT[Domain Events:<br/>• IngestionJobCompleted<br/>• ValidationFailed<br/>• BarCollectionCompleted]
        SVC[Domain Services:<br/>• MarketDataValidationService]
        REPO[Repository Interfaces:<br/>• IOHLCVRepository<br/>• IUniverseRepository]
    end

    subgraph "Application Layer (src/marketpipe/*/application/)"
        direction TB
        ASVC[Application Services:<br/>• IngestionApplicationService<br/>• ValidationApplicationService<br/>• AggregationApplicationService]
        ORCH[Event Orchestration:<br/>• Event Bus Integration<br/>• Cross-Context Coordination]
    end

    subgraph "Infrastructure Layer (src/marketpipe/infrastructure/)"
        direction TB
        REPOS[Repository Implementations:<br/>• SQLiteOHLCVRepository<br/>• ParquetOHLCVRepository]
        EVENTS[Event Infrastructure:<br/>• InMemoryEventPublisher<br/>• Domain Event Handlers]
        MONITOR[Monitoring:<br/>• Prometheus Metrics<br/>• Event-based Metrics Collection]
        STORAGE[Storage:<br/>• Parquet Writers<br/>• DuckDB Integration<br/>• SQLite State Management]
    end

    subgraph "CLI Layer (src/marketpipe/cli/)"
        direction TB
        CLI[CLI Commands:<br/>• ingest-ohlcv<br/>• validate-ohlcv<br/>• aggregate-ohlcv<br/>• metrics<br/>• query]
    end

    %% Dependencies (following DDD rules)
    CLI --> ASVC
    ASVC --> ENT
    ASVC --> VO
    ASVC --> AGG
    ASVC --> EVT
    ASVC --> SVC
    ASVC --> REPO
    ASVC --> REPOS
    ASVC --> EVENTS
    REPOS --> ENT
    REPOS --> VO
    REPOS --> AGG
    EVENTS --> EVT
    MONITOR --> EVT

    classDef domain fill:#ffeb9c,stroke:#333,stroke-width:3px
    classDef application fill:#dae8fc,stroke:#333,stroke-width:2px
    classDef infrastructure fill:#f8cecc,stroke:#333,stroke-width:2px
    classDef cli fill:#e1d5e7,stroke:#333,stroke-width:2px

    class ENT,VO,AGG,EVT,SVC,REPO domain
    class ASVC,ORCH application
    class REPOS,EVENTS,MONITOR,STORAGE infrastructure
    class CLI cli
```

## Domain Model Class Diagram

```mermaid
classDiagram
    class Entity {
        <<abstract>>
        -EntityId id
        -int version
        +getId() EntityId
        +getVersion() int
        +equals(other) bool
        +hashCode() int
    }

    class EntityId {
        +UUID value
        +generate() EntityId
        +toString() str
    }

    class OHLCVBar {
        -Symbol symbol
        -Timestamp timestamp
        -Price openPrice
        -Price highPrice
        -Price lowPrice
        -Price closePrice
        -Volume volume
        -int tradeCount
        -Price vwap
        +calculatePriceRange() Price
        +calculatePriceChange() Price
        +isSameTradingDay(other) bool
        +isDuringMarketHours() bool
    }

    class Symbol {
        +str value
        +fromString(str) Symbol
        +toString() str
        +validate() void
    }

    class Price {
        +Decimal value
        +fromFloat(float) Price
        +zero() Price
        +add(Price) Price
        +subtract(Price) Price
        +multiply(number) Price
        +divide(number) Price
        +toFloat() float
    }

    class Timestamp {
        +datetime value
        +now() Timestamp
        +fromIso(str) Timestamp
        +fromNanoseconds(int) Timestamp
        +tradingDate() date
        +toNanoseconds() int
        +isMarketHours() bool
        +roundToMinute() Timestamp
    }

    class Volume {
        +int value
        +zero() Volume
        +add(Volume) Volume
        +subtract(Volume) Volume
        +multiply(number) Volume
    }

    class TimeRange {
        +Timestamp start
        +Timestamp end
        +fromDates(date, date) TimeRange
        +singleDay(date) TimeRange
        +contains(Timestamp) bool
        +overlaps(TimeRange) bool
        +durationSeconds() float
    }

    class SymbolBarsAggregate {
        -Symbol symbol
        -date tradingDate
        -dict bars
        -list events
        -int version
        -bool isComplete
        +startCollection() void
        +addBar(OHLCVBar) void
        +getBar(Timestamp) OHLCVBar
        +getAllBars() list
        +getBarsInRange(TimeRange) list
        +completeCollection() void
        +calculateDailySummary() DailySummary
        +getUncommittedEvents() list
        +markEventsCommitted() void
    }

    class UniverseAggregate {
        -str universeId
        -dict symbols
        -set activeSymbols
        -list events
        -int version
        +addSymbol(Symbol) void
        +removeSymbol(Symbol) void
        +activateSymbol(Symbol) void
        +deactivateSymbol(Symbol) void
        +getActiveSymbols() list
        +getAllSymbols() list
        +isSymbolActive(Symbol) bool
    }

    class DomainEvent {
        <<abstract>>
        +UUID eventId
        +datetime occurredAt
        +int version
        +getEventType() str
        +getAggregateId() str
    }

    class BarCollectionCompleted {
        +Symbol symbol
        +date tradingDate
        +int barCount
        +bool hasGaps
    }

    class IngestionJobCompleted {
        +str jobId
        +Symbol symbol
        +date tradingDate
        +int barsProcessed
        +bool success
        +str errorMessage
    }

    class ValidationFailed {
        +Symbol symbol
        +Timestamp timestamp
        +str errorMessage
        +str ruleId
        +str severity
    }

    class IEventPublisher {
        <<interface>>
        +publish(DomainEvent) void
        +subscribe(eventType, handler) void
    }

    class MarketDataValidationService {
        +validateBatch(bars, symbol) list
        +validateBar(bar) list
        +validateBusinessRules(bar) list
    }

    Entity <|-- OHLCVBar
    Entity o-- EntityId

    OHLCVBar o-- Symbol
    OHLCVBar o-- Timestamp
    OHLCVBar o-- Price
    OHLCVBar o-- Volume

    SymbolBarsAggregate o-- Symbol
    SymbolBarsAggregate o-- OHLCVBar
    SymbolBarsAggregate o-- DomainEvent

    UniverseAggregate o-- Symbol
    UniverseAggregate o-- DomainEvent

    DomainEvent <|-- BarCollectionCompleted
    DomainEvent <|-- IngestionJobCompleted
    DomainEvent <|-- ValidationFailed

    BarCollectionCompleted o-- Symbol
    IngestionJobCompleted o-- Symbol
    ValidationFailed o-- Symbol
    ValidationFailed o-- Timestamp

    MarketDataValidationService ..> OHLCVBar
    MarketDataValidationService ..> Symbol
```

## Event Flow Diagram

```mermaid
sequenceDiagram
    participant CLI as CLI Layer
    participant App as Application Service
    participant Dom as Domain Aggregate
    participant Evt as Domain Events
    participant Infra as Infrastructure

    CLI->>App: ingest command
    App->>Dom: create SymbolBarsAggregate
    App->>Dom: addBar(ohlcvBar)
    Dom->>Dom: validate business rules
    Dom->>Evt: emit BarCollectionCompleted
    App->>Infra: save aggregate
    App->>Infra: publish events
    Infra->>Infra: update metrics
    Infra-->>CLI: return success
```

## Repository Pattern

```mermaid
classDiagram
    class IOHLCVRepository {
        <<interface>>
        +save(bars) void
        +findBySymbolAndDateRange(symbol, start, end) list
        +exists(symbol, timestamp) bool
        +delete(symbol, timestamp) void
    }

    class SQLiteOHLCVRepository {
        +save(bars) void
        +findBySymbolAndDateRange(symbol, start, end) list
        +exists(symbol, timestamp) bool
        +delete(symbol, timestamp) void
    }

    class ParquetOHLCVRepository {
        +save(bars) void
        +findBySymbolAndDateRange(symbol, start, end) list
        +exists(symbol, timestamp) bool
        +delete(symbol, timestamp) void
    }

    IOHLCVRepository <|.. SQLiteOHLCVRepository
    IOHLCVRepository <|.. ParquetOHLCVRepository

    SQLiteOHLCVRepository ..> OHLCVBar
    ParquetOHLCVRepository ..> OHLCVBar
```

## Key DDD Principles Enforced

1. **Domain Purity**: Domain layer contains only business logic, no infrastructure dependencies
2. **Dependency Inversion**: Application layer depends on domain interfaces, infrastructure implements them
3. **Event-Driven Architecture**: Domain events enable loose coupling between bounded contexts
4. **Aggregate Boundaries**: Clear consistency boundaries around SymbolBarsAggregate and UniverseAggregate
5. **Repository Pattern**: Abstract data access behind domain interfaces
6. **Value Objects**: Immutable value objects for Symbol, Price, Timestamp, etc.
7. **Entity Identity**: Clear entity identity through EntityId value object
