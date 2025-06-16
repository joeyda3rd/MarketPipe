# MarketPipe

[![Live Metrics](https://img.shields.io/badge/Live%20Metrics-online-brightgreen)](docs/pipeline.md#metrics)
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)

## Overview

**MarketPipe** is a modern, Python-native ETL framework purpose-built for financial market data workflows. Designed for speed, flexibility, and clarity, MarketPipe puts you in control—whether you're building your own trading stack, wrangling market data for analytics, or managing multiple data sources across asset classes.

With a foundation in Domain-Driven Design (DDD) and an intuitive CLI-first interface, MarketPipe gives you rock-solid ingestion, transformation, and validation—without the heavyweight baggage of legacy pipeline tools.

### Key Features

* **Reliable ingestion** from multiple providers (Alpaca, IEX, with more on the way)
* **Async, parallel processing**—process hundreds of symbols concurrently with checkpointing and resumption
* **Pluggable provider architecture**—easily swap or add new data sources
* **Modern storage**—fast Parquet files, DuckDB-powered views, and SQL/CLI querying out of the box
* **Event-driven validation**—built-in rules and CSV audit reports to catch bad data before it hits your models
* **Async metrics server**—non-blocking Prometheus metrics with event loop lag monitoring
* **Secure by design**—utilities to mask secrets in logs and support for safe connection handling
* **Thorough testing**—all major commands regression tested, with coverage and CI built-in

## Architecture Overview

MarketPipe follows Domain-Driven Design with clear separation between domain logic, application services, and infrastructure concerns:

```mermaid
flowchart TD
    %% ---------- DDD Layers ----------
    subgraph "Domain Layer"
        direction LR
        DOM["Domain Models:<br/>• Entities (OHLCVBar)<br/>• Value Objects (Symbol, Price)<br/>• Aggregates (SymbolBarsAggregate)<br/>• Domain Events<br/>• Domain Services"]:::domain
    end

    subgraph "Application Layer"
        direction LR
        APP["Application Services:<br/>• IngestionApplicationService<br/>• ValidationApplicationService<br/>• AggregationApplicationService<br/>• Event Orchestration"]:::application
    end

    subgraph "Infrastructure Layer"
        direction LR
        INFRA["Infrastructure:<br/>• Market Data Providers<br/>• Storage (Parquet/DuckDB)<br/>• Async Metrics Server<br/>• Event Publishers<br/>• Repositories"]:::infrastructure
    end

    %% ---------- CLI Interface ----------
    subgraph "CLI Interface"
        CLI["Typer CLI:<br/>• ingest-ohlcv<br/>• validate-ohlcv<br/>• aggregate-ohlcv<br/>• metrics (async/legacy)<br/>• query"]:::cli
    end

    %% ---------- External Systems ----------
    subgraph "External Systems"
        EXT["Market Data Providers:<br/>• Alpaca Markets<br/>• IEX Cloud<br/>• Fake Provider (testing)"]:::external
        METRICS["Monitoring:<br/>• Prometheus<br/>• Grafana<br/>• Event Loop Monitoring"]:::external
    end

    %% ---------- Flow arrows ----------
    CLI --> APP
    APP --> DOM
    APP --> INFRA
    INFRA --> EXT
    INFRA --> METRICS
    
    %% Event flow
    DOM -.-> APP
    APP -.-> INFRA

    %% ---------- Styles ----------
    classDef domain fill:#ffeb9c,stroke:#333,stroke-width:3px
    classDef application fill:#dae8fc,stroke:#333,stroke-width:2px
    classDef infrastructure fill:#f8cecc,stroke:#333,stroke-width:2px
    classDef cli fill:#e1d5e7,stroke:#333,stroke-width:2px
    classDef external fill:#d5e8d4,stroke:#333,stroke-width:1px
```

## Installation

```bash
pip install -e .
```

## Quick Start

### Basic Ingestion

```bash
# Using fake provider (generates synthetic data)
marketpipe ingest --provider fake --symbols TEST --start 2024-01-01 --end 2024-01-02 --batch-size 10

# Using Alpaca provider (requires ALPACA_KEY and ALPACA_SECRET env vars)
marketpipe ingest --provider alpaca --symbols AAPL,MSFT --start 2024-01-01 --end 2024-01-02 --batch-size 1000

# Using IEX provider (requires IEX_TOKEN env var)
marketpipe ingest --provider iex --symbols AAPL --start 2024-01-01 --end 2024-01-02 --batch-size 500
```

### Validation and Monitoring

```bash
# Validate ingested data
marketpipe validate --list

# Start async metrics server
marketpipe metrics --port 8000

# Query stored data
marketpipe query --symbol AAPL --start 2024-01-01
```

### Database Backends

MarketPipe supports multiple database backends:

```bash
# SQLite (default)
marketpipe ingest --provider fake --symbols TEST --start 2024-01-01 --end 2024-01-02

# PostgreSQL
export DATABASE_URL="postgresql://user:pass@localhost:5432/marketpipe"
marketpipe ingest --provider fake --symbols TEST --start 2024-01-01 --end 2024-01-02
```

## Module Index

| Module | Purpose | Documentation |
|--------|---------|---------------|
| **[core](docs/core.md)** | Application bootstrap, metrics collection, and monitoring infrastructure | [docs/core.md](docs/core.md) |
| **[cli](docs/cli.md)** | Command-line interface with Typer for all MarketPipe operations | [docs/cli.md](docs/cli.md) |
| **[domain](docs/domain.md)** | Core business logic using DDD patterns (entities, value objects, aggregates) | [docs/domain.md](docs/domain.md) |
| **[ingestion](docs/ingestion.md)** | Data collection orchestration with job management and connector architecture | [docs/ingestion.md](docs/ingestion.md) |
| **[validation](docs/validation.md)** | Data quality validation with event-driven processing and audit reporting | [docs/validation.md](docs/validation.md) |
| **[aggregation](docs/aggregation.md)** | Time-series aggregation using DuckDB for higher timeframes (5m, 1h, 1d) | [docs/aggregation.md](docs/aggregation.md) |
| **[infrastructure](docs/infrastructure.md)** | Storage engines, repositories, and event publishers for persistence layer | [docs/infrastructure.md](docs/infrastructure.md) |

## Architecture Benefits

The Domain-Driven Design architecture provides:

- **Clean Separation**: Domain logic isolated from infrastructure concerns
- **Testability**: Pure domain models with comprehensive test coverage
- **Maintainability**: Clear boundaries between layers prevent coupling
- **Extensibility**: New providers and features can be added without affecting core domain
- **Async Monitoring**: Non-blocking metrics server with event loop lag detection
- **Event-Driven**: Decoupled processing with domain events for validation and aggregation

## License

MarketPipe is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full license text.

The Apache 2.0 license permits commercial use, including the development of closed-source plugins, user interfaces, and hosted services based on this codebase.
