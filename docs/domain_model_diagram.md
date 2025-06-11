# MarketPipe Domain Model Diagrams

This document contains UML and Mermaid diagrams illustrating the Domain-Driven Design architecture of MarketPipe.

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
        +toDict() dict
    }
    
    class Symbol {
        +str value
        +fromString(str) Symbol
        +toString() str
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
        +toDict() dict
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
    
    Entity <|-- OHLCVBar
    Entity o-- EntityId
    
    OHLCVBar o-- Symbol
    OHLCVBar o-- Timestamp
    OHLCVBar o-- Price
    OHLCVBar o-- Volume
    
    SymbolBarsAggregate o-- Symbol
    SymbolBarsAggregate o-- OHLCVBar
    SymbolBarsAggregate --> DomainEvent
    
    UniverseAggregate o-- Symbol
    UniverseAggregate --> DomainEvent
    
    DomainEvent <|-- BarCollectionCompleted
    DomainEvent <|-- IngestionJobCompleted
    DomainEvent <|-- ValidationFailed
    
    TimeRange o-- Timestamp
```

## Repository Interfaces

```mermaid
classDiagram
    class ISymbolBarsRepository {
        <<interface>>
        +getBySymbolAndDate(Symbol, date) SymbolBarsAggregate
        +save(SymbolBarsAggregate) void
        +findSymbolsWithData(date, date) list
        +getCompletionStatus(list, list) dict
        +delete(Symbol, date) bool
    }
    
    class IOHLCVRepository {
        <<interface>>
        +getBarsForSymbol(Symbol, TimeRange) AsyncIterator
        +getBarsForSymbols(list, TimeRange) AsyncIterator
        +saveBars(list) void
        +exists(Symbol, Timestamp) bool
        +countBars(Symbol, TimeRange) int
        +getLatestTimestamp(Symbol) Timestamp
        +deleteBars(Symbol, TimeRange) int
    }
    
    class IUniverseRepository {
        <<interface>>
        +getById(str) UniverseAggregate
        +save(UniverseAggregate) void
        +getDefaultUniverse() UniverseAggregate
        +listUniverses() list
    }
    
    class IDailySummaryRepository {
        <<interface>>
        +getSummary(Symbol, date) DailySummary
        +getSummaries(Symbol, date, date) list
        +saveSummary(DailySummary) void
        +saveSummaries(list) void
        +deleteSummary(Symbol, date) bool
    }
    
    class ICheckpointRepository {
        <<interface>>
        +saveCheckpoint(Symbol, dict) void
        +getCheckpoint(Symbol) dict
        +deleteCheckpoint(Symbol) bool
        +listCheckpoints() list
    }
    
    ISymbolBarsRepository ..> SymbolBarsAggregate
    IOHLCVRepository ..> OHLCVBar
    IUniverseRepository ..> UniverseAggregate
    IDailySummaryRepository ..> DailySummary
    ICheckpointRepository ..> Symbol
```

## Domain Services

```mermaid
classDiagram
    class DomainService {
        <<abstract>>
    }
    
    class OHLCVCalculationService {
        +aggregateBarsToTimeframe(list, int) list
        +calculateSMA(list, int, str) list
        +calculateVolatility(list, int) list
        -calculatePeriodStart(datetime, int) datetime
        -aggregateBarGroup(list, int) OHLCVBar
    }
    
    class MarketDataValidationService {
        +validateTradingHours(OHLCVBar) list
        +validatePriceMovements(OHLCVBar, OHLCVBar) list
        +validateVolumePatterns(list) list
    }
    
    class TradingCalendarService {
        +isTradingDay(date) bool
        +getTradingSessionTimes(date) dict
        +getNextTradingDay(date) date
        +getPreviousTradingDay(date) date
    }
    
    DomainService <|-- OHLCVCalculationService
    DomainService <|-- MarketDataValidationService
    DomainService <|-- TradingCalendarService
    
    OHLCVCalculationService ..> OHLCVBar
    MarketDataValidationService ..> OHLCVBar
    TradingCalendarService ..> Symbol
```

## Context Integration Flow

```mermaid
sequenceDiagram
    participant CLI as CLI Interface
    participant IC as Ingestion Coordinator
    participant MDI as Market Data Integration
    participant DV as Data Validation
    participant DS as Data Storage
    participant OM as Operations & Monitoring
    
    CLI->>IC: execute_ingestion_workflow(job)
    
    IC->>OM: record_job_started(job)
    IC->>MDI: fetch_market_data(symbol, time_range)
    MDI->>OM: record_request_metrics()
    MDI-->>IC: raw_bars[]
    
    IC->>DV: validate_bars(raw_bars)
    DV->>OM: record_validation_metrics()
    DV-->>IC: validation_result
    
    alt validation passed
        IC->>DS: store_bars(validated_bars)
        DS->>OM: record_storage_metrics()
        DS-->>IC: storage_result
        IC->>OM: record_job_completed(success)
    else validation failed
        IC->>OM: record_validation_error()
        IC->>OM: record_job_completed(failure)
    end
```

## Event Flow Diagram

```mermaid
graph LR
    subgraph "Data Ingestion Context"
        IJS[IngestionJobStarted]
        IJC[IngestionJobCompleted]
    end
    
    subgraph "Market Data Integration Context"
        MDR[MarketDataReceived]
        RLE[RateLimitExceeded]
    end
    
    subgraph "Data Validation Context"
        VF[ValidationFailed]
    end
    
    subgraph "Data Storage Context"
        DS[DataStored]
    end
    
    subgraph "Symbol Management"
        BCS[BarCollectionStarted]
        BCC[BarCollectionCompleted]
        SA[SymbolActivated]
        SD[SymbolDeactivated]
    end
    
    IJS --> MDR
    MDR --> VF
    MDR --> BCS
    BCS --> BCC
    BCC --> DS
    DS --> IJC
    
    VF --> IJC
    RLE --> IJC
    
    SA --> IJS
    SD --> IJC
    
    classDef eventClass fill:#e1d5e7,stroke:#9673a6,stroke-width:2px
    class IJS,IJC,MDR,RLE,VF,DS,BCS,BCC,SA,SD eventClass
```

## Aggregate Boundaries

```mermaid
graph TD
    subgraph "SymbolBarsAggregate Boundary"
        SBA[SymbolBarsAggregate Root]
        OB1[OHLCVBar Entity]
        OB2[OHLCVBar Entity]
        OB3[OHLCVBar Entity]
        DS1[DailySummary Value Object]
        
        SBA --> OB1
        SBA --> OB2
        SBA --> OB3
        SBA --> DS1
    end
    
    subgraph "UniverseAggregate Boundary"
        UA[UniverseAggregate Root]
        S1[Symbol Value Object]
        S2[Symbol Value Object]
        S3[Symbol Value Object]
        
        UA --> S1
        UA --> S2
        UA --> S3
    end
    
    subgraph "Value Objects (Shared Kernel)"
        SYM[Symbol]
        PRC[Price]
        TS[Timestamp]
        VOL[Volume]
        TR[TimeRange]
    end
    
    OB1 --> SYM
    OB1 --> PRC
    OB1 --> TS
    OB1 --> VOL
    
    S1 --> SYM
    TR --> TS
    
    classDef aggregateRoot fill:#ffeb9c,stroke:#d6b656,stroke-width:3px
    classDef entity fill:#dae8fc,stroke:#6c8ebf,stroke-width:2px
    classDef valueObject fill:#f8cecc,stroke:#b85450,stroke-width:1px
    
    class SBA,UA aggregateRoot
    class OB1,OB2,OB3 entity
    class DS1,S1,S2,S3,SYM,PRC,TS,VOL,TR valueObject
```

## Data Flow Architecture

```mermaid
flowchart TB
    subgraph "External Systems"
        A1[Alpaca API]
        P1[Polygon API]
        O1[Other Providers]
    end
    
    subgraph "Anti-Corruption Layer"
        ACL1[Alpaca Adapter]
        ACL2[Polygon Adapter]
        ACL3[Generic Adapter]
    end
    
    subgraph "Domain Model"
        DM[Canonical OHLCV Bars]
    end
    
    subgraph "Application Services"
        IC[Ingestion Coordinator]
        VS[Validation Service]
        SS[Storage Service]
    end
    
    subgraph "Infrastructure"
        PQ[Parquet Files]
        DB[DuckDB]
        SQ[SQLite State]
        PM[Prometheus Metrics]
    end
    
    A1 --> ACL1
    P1 --> ACL2
    O1 --> ACL3
    
    ACL1 --> DM
    ACL2 --> DM
    ACL3 --> DM
    
    DM --> IC
    IC --> VS
    VS --> SS
    
    SS --> PQ
    SS --> DB
    IC --> SQ
    IC --> PM
    
    classDef external fill:#f9f9f9,stroke:#333,stroke-width:1px
    classDef anticorruption fill:#fff2cc,stroke:#d6b656,stroke-width:2px
    classDef domain fill:#ffeb9c,stroke:#d6b656,stroke-width:3px
    classDef application fill:#dae8fc,stroke:#6c8ebf,stroke-width:2px
    classDef infrastructure fill:#f8cecc,stroke:#b85450,stroke-width:1px
    
    class A1,P1,O1 external
    class ACL1,ACL2,ACL3 anticorruption
    class DM domain
    class IC,VS,SS application
    class PQ,DB,SQ,PM infrastructure
```

These diagrams illustrate the key aspects of MarketPipe's Domain-Driven Design architecture:

1. **Bounded Context Overview**: Shows the relationship between core, supporting, and generic domains
2. **Domain Model Class Diagram**: Details the entities, value objects, and aggregates
3. **Repository Interfaces**: Shows the data access abstraction layer
4. **Domain Services**: Illustrates business logic services
5. **Context Integration Flow**: Sequence diagram showing cross-context communication
6. **Event Flow**: Shows how domain events flow between contexts
7. **Aggregate Boundaries**: Illustrates consistency boundaries and shared kernel
8. **Data Flow Architecture**: Shows the overall system architecture with anti-corruption layers