# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Installation
```bash
pip install -e .
```

### Running the CLI
```bash
# Main CLI entry point
marketpipe --help

# Ingest data using config
marketpipe ingest --config config/example_config.yaml

# Run specific modules
python -m marketpipe.ingestion --config config/example_config.yaml
```

### Testing
```bash
# Run all tests
python -m pytest tests/

# Run specific test
python -m pytest tests/test_cli.py -v

# Test CLI directly
python -m marketpipe.cli --help
```

## Architecture Overview

MarketPipe is a time-series ETL framework with a modular, threaded architecture:

### Core Pipeline Flow
1. **Ingestion**: `IngestionCoordinator` orchestrates threaded data collection
2. **Validation**: `SchemaValidator` ensures data integrity 
3. **Storage**: Partitioned Parquet files in `data/symbol=X/year=Y/month=M/day=D.parquet`
4. **Aggregation**: DuckDB-based time frame aggregation
5. **Metrics**: Prometheus metrics with Grafana dashboard support

### Key Architectural Patterns

#### Client Architecture
- `BaseApiClient` provides abstract vendor-agnostic interface
- `AlpacaClient` implements Alpaca Markets API specifics
- Authentication via `AuthStrategy` implementations (`HeaderTokenAuth`)
- Rate limiting through `RateLimiter` with configurable windows
- State persistence via `SQLiteState` for checkpoint/resume

#### Configuration System
- YAML-based configuration in `config/` directory
- Environment variable loading from `.env` files (credentials)
- ClientConfig dataclass for type-safe configuration

#### Threading Model
- `IngestionCoordinator` uses `ThreadPoolExecutor` for parallel symbol processing
- Worker threads handle individual symbol/date combinations
- Shared state coordination through SQLite backend

#### Metrics Integration
- Prometheus metrics exported on configurable port (default 8000)
- `REQUESTS`, `ERRORS`, `LATENCY`, `BACKLOG` counters/histograms
- Optional metrics server thread via `metrics_server.py`

### Module Structure
- `ingestion/`: Core ETL pipeline with connectors, validation, state management
- `connectors/`: API client implementations and auth strategies  
- `loader.py`: DuckDB/Parquet data loading interface
- `cli.py`: Typer-based command line interface
- `metrics.py`: Prometheus metrics definitions

### Data Storage
- Partitioned Parquet files with Hive-style partitioning
- Schema validation via JSON Schema in `schema/schema_v1.json`
- DuckDB integration for aggregation queries
- Configurable compression (snappy/zstd)

## Code Standards

### Core Conventions
- Always use `from __future__ import annotations` as first import
- Use snake_case for functions/variables, PascalCase for classes
- Include type hints for all function parameters and returns
- Follow vendor-specific naming: `{vendor}_client.py` (e.g., `alpaca_client.py`)

### Connector Architecture
All API connectors must:
- Inherit from `BaseApiClient`
- Implement required abstract methods: `build_request_params()`, `endpoint_path()`, `next_cursor()`, `parse_response()`, `should_retry()`
- Support both sync and async operations with clear naming (`fetch_batch()` / `async_fetch_batch()`)
- Include comprehensive metrics collection via `REQUESTS`, `ERRORS`, `LATENCY` labels
- Return data in canonical OHLCV schema format with `schema_version: 1`

### Testing Patterns
- Use descriptive test names: `test_alpaca_retry_on_429()`
- Mock HTTP responses with `monkeypatch.setattr(httpx, "get", mock_get)`
- Test both sync and async variants using `asyncio.run()`
- Validate schema compliance in all connector tests

### Configuration
- Use dataclasses for type-safe configuration with `__post_init__()` validation
- Load credentials from environment variables via `python-dotenv`
- Support CLI overrides for symbols, dates, and workers
- Validate date ranges and output paths early with user-friendly error messages

## Domain-Driven Design Architecture

### Bounded Contexts
MarketPipe is organized into distinct bounded contexts, each with clear responsibilities:

1. **Data Ingestion Context** (Core Domain)
   - Orchestrates collection of market data from external APIs
   - Entities: `IngestionJob`, `IngestionCheckpoint`
   - Services: `IngestionCoordinator`, `JobScheduler`

2. **Market Data Integration Context** (Supporting Domain)
   - Abstracts integration with market data providers
   - Entities: `MarketDataProvider`, `DataFeed`, `RawMarketData`
   - Services: `BaseApiClient`, `AlpacaClient`, `ResponseParser`

3. **Data Validation Context** (Supporting Domain)
   - Ensures data quality and business rule compliance
   - Entities: `ValidationRule`, `OHLCVBar`, `ValidationResult`
   - Services: `SchemaValidator`, `BusinessRuleValidator`

4. **Data Storage Context** (Supporting Domain)
   - Manages persistent storage of time-series data
   - Entities: `DataPartition`, `StorageMetadata`
   - Services: `ParquetWriter`, `PartitionManager`

### Ubiquitous Language
Use consistent domain terminology across all code and documentation:

- **Symbol**: Stock ticker identifier (e.g., AAPL) - not "ticker" or "security"
- **OHLCV Bar**: Open, High, Low, Close, Volume data - not "candle" or "quote"
- **Trading Date**: Calendar date in market timezone - not "business date"
- **Ingestion**: Process of collecting data - not "import" or "fetch"
- **Market Data Provider**: External data source - not "vendor" or "feed"
- **Validation**: Checking against business rules - not "verification"

### Domain Model Patterns
- Entities have identity and business behavior (e.g., `OHLCVBar`, `IngestionJob`)
- Value Objects are immutable and defined by their values (e.g., `Symbol`, `Price`, `Timestamp`)
- Aggregates enforce consistency boundaries (e.g., `SymbolBarsAggregate`)
- Repositories provide domain-focused data access interfaces
- Domain Events communicate between bounded contexts

### DDD Implementation Rules
- Keep domain models free of infrastructure concerns
- Use anti-corruption layers when integrating with external systems
- Communicate between contexts via well-defined interfaces
- Apply ubiquitous language consistently in code, tests, and documentation
- Organize code by bounded context, not technical layers