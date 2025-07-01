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

# OHLCV ingestion (current main command)
marketpipe ingest-ohlcv --symbols AAPL,MSFT --start 2024-01-01 --end 2024-01-02 --provider fake --feed-type iex

# Using configuration files
marketpipe ingest-ohlcv --config config/example_config.yaml

# Metrics server with beautiful dashboard
marketpipe metrics --port 8000
# ^ Starts Prometheus server on :8000 AND human-friendly dashboard on :8001

# Other key commands
marketpipe health-check --verbose
marketpipe query --symbol AAPL --start 2024-01-01
marketpipe validate --list
```

### Testing
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=marketpipe --cov-report=html

# Run specific test file
pytest tests/test_cli.py -v

# Run single test function
pytest tests/unit/cli/test_ingest_cli.py::test_ingest_help_no_validation -v

# Run integration tests only
pytest tests/integration/ -m "not auth_required and not slow"

# Test CLI commands
python -m marketpipe.cli --help
marketpipe health-check --verbose
```

### Code Quality
```bash
# Format code
black src/ tests/

# Lint code
ruff check src/ tests/

# Type checking
mypy src/marketpipe/

# Pre-commit hooks (auto-installed)
pre-commit run --all-files

# All dependency groups available
pip install -e '.[dev]'          # Complete development environment
pip install -e '.[test]'         # Testing only
pip install -e '.[quality]'      # Code quality tools
pip install -e '.[postgres]'     # PostgreSQL support
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
- Beautiful human-friendly dashboard on port+1 (e.g., 8001)
- `REQUESTS`, `ERRORS`, `LATENCY`, `BACKLOG` counters/histograms
- AsyncMetricsServer with event loop lag monitoring
- CLI command: `marketpipe metrics --port 8000` starts both servers

### Module Structure
- `ingestion/`: Core ETL pipeline with connectors, validation, state management
- `cli/`: Typer-based command line interface with sub-commands
- `domain/`: DDD domain models (entities, value objects, aggregates, events)
- `infrastructure/`: Storage engines, repositories, event publishers
- `validation/`: Data quality validation with event-driven processing
- `aggregation/`: DuckDB-based time frame aggregation
- `metrics_server.py`: Async Prometheus server with dashboard
- `loader.py`: DuckDB/Parquet data loading interface

## API Reference available for fast accurate ledger
 - run workbook/tools/generate_api_reference.py before reviewing to update
 - provides every definition name, its parameters, and its location in 2 JSON files 
 - dev/reference/api_reference.json for codebase references
 - dev/reference/test_api_reference.json for test references

### Data Storage
- Partitioned Parquet files with Hive-style partitioning: `data/frame=1m/symbol=AAPL/date=2024-01-01/`
- Schema validation via JSON Schema in `schema/schema_v1.json`
- DuckDB integration for aggregation queries and analytics
- Multiple database backends: SQLite (default) and PostgreSQL
- Configurable compression (snappy/zstd)
- ParquetStorageEngine for high-performance writing

## Code Standards

### Core Conventions
- Always use `from __future__ import annotations` as first import
- Use snake_case for functions/variables, PascalCase for classes
- Include type hints for all function parameters and returns
- Follow vendor-specific naming: `{vendor}_client.py` (e.g., `alpaca_client.py`)

### Provider Architecture
All market data providers must:
- Implement the adapter pattern via `MarketDataAdapter` interface
- Support provider registration via entry points in `pyproject.toml`
- Include comprehensive metrics collection via `REQUESTS`, `ERRORS`, `LATENCY` labels
- Return data in canonical OHLCV schema format with `schema_version: 1`
- Handle rate limiting and retries appropriately
- Support both sync and async operations where applicable

Available providers: `alpaca`, `iex`, `fake` (for testing)

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
- Configuration versioning system with backward compatibility
- YAML-based configuration files with schema validation

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

## CLI Architecture

### Command Structure
MarketPipe uses a hierarchical CLI with Typer:
- Main app: `marketpipe` with global commands
- OHLCV sub-app: `marketpipe ohlcv <command>` for pipeline operations
- Backward compatibility: deprecated aliases with warnings

### Key CLI Commands
- `marketpipe ingest-ohlcv`: Primary ingestion command with provider/symbol/date options
- `marketpipe metrics`: Dual-server setup (Prometheus + dashboard)
- `marketpipe health-check`: System validation and diagnostics
- `marketpipe query`: Data querying with SQL-like interface
- `marketpipe validate`: Data quality validation and reporting

### CLI Validation
All CLI inputs are validated upfront with user-friendly error messages:
- Date range validation (ISO format, logical ordering, not in future)
- Symbol validation (regex pattern, count limits)
- Output directory validation (permissions, parent existence)
- Provider/feed-type compatibility checking

## Metrics System

### Dual-Server Architecture
The metrics system provides two interfaces:
1. **Prometheus Server** (port 8000): Raw metrics in exposition format for monitoring systems
2. **Dashboard Server** (port 8001): Beautiful human-friendly web interface

### Dashboard Features
- Visual metric cards categorized by type (MarketPipe, Python Runtime, System)
- Auto-refresh every 30 seconds
- Color-coded metric types (counter, gauge, histogram)
- Error handling when metrics server is unreachable
- Professional CSS styling with responsive design

### Usage
```bash
marketpipe metrics --port 8000
# Starts both servers:
# - http://localhost:8000/metrics (Prometheus data)
# - http://localhost:8001 (Beautiful dashboard)
```

## Chat Date Context Rule

> **Rule**: At the beginning of every chat session, the assistant **MUST** obtain the current date from the operating system (e.g. via `date` shell command) and keep that value in its working context for the remainder of the session. If, at any point, the conversation context is reset or significantly condensed, the assistant **MUST** refresh the stored date by executing the same command again before proceeding.

This guarantees that time-sensitive reasoning remains accurate even when the context window is rebuilt or truncated.
