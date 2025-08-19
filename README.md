# MarketPipe

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **⚠️ Alpha Software**: MarketPipe is currently in alpha. APIs may change and features are being actively developed. Use with caution in production environments.

**Modern, Python-native ETL framework for financial market data workflows with Domain-Driven Design**

MarketPipe transforms how you collect, validate, and store financial market data. Built with modern Python patterns, it provides a robust, scalable foundation for financial data workflows with enterprise-grade observability and monitoring.

## Features

### **Modern Architecture**
- **Domain-Driven Design** with enforced boundaries and clean separation of concerns
- **Plugin-based providers** for easy integration with any market data source
- **Async/sync dual APIs** for maximum flexibility
- **Type-safe configuration** with comprehensive validation

### **Data Providers**
- **Alpaca Markets** - Real-time and historical market data
- **IEX Cloud** - Professional-grade financial data
- **Fake Provider** - Generate realistic test data for development
- **Extensible Plugin System** - Add your own providers easily

### **ETL Pipeline**
- **Parallel ingestion** across multiple symbols and timeframes
- **Schema validation** with business rule enforcement
- **Incremental loading** with checkpoint/resume capability
- **Data quality monitoring** with comprehensive error reporting
- **Partitioned storage** in Parquet format for optimal performance

### **Observability**
- **Prometheus metrics** with multiprocess support
- **Grafana dashboards** for real-time monitoring
- **Structured logging** with configurable levels
- **Performance tracking** and error alerting

### **Deployment Ready**
- **Docker Compose** stack with monitoring included
- **Database migrations** with Alembic
- **Health checks** and readiness probes
- **Production-ready configuration**

## Roadmap

MarketPipe's development focuses exclusively on enhancing ETL capabilities within our Domain-Driven Design architecture for financial market data with enterprise-grade reliability.

### Near-term (Next Releases)

#### **Universe Builder/Manager**
- CLI commands for managing symbol universes (domain models exist)
- Import/export universe definitions
- Universe validation and health checks
- Programmatic universe management API

#### **Enhanced Programmatic API**
- Expanded Python API beyond basic `load_ohlcv()`
- Better integration with Jupyter notebooks and research environments
- Direct access to ingestion and validation services
- Rich data access patterns for analysis workflows

#### **Enhanced Data Loader**
- Advanced filtering and aggregation capabilities
- Multiple export formats (CSV, JSON, Arrow, etc.)
- Custom query interface with complex predicates
- Streaming data access for large datasets

#### **Additional Data Providers**
**Immediate targets (8 providers):** FRED (Economic Data), Binance (Crypto), Finnhub, Polygon, Tiingo, Twelve Data, plus enhanced IEX and Alpaca integrations

**Extended ecosystem:** Alpha Vantage, CME DataMine, Coinbase, Quandl, Refinitiv, Yahoo Finance, Kraken, Tradier, and more

**Community contributions:** See our [Contributing Guide](#contributing) for adding new data provider integrations

### Medium-term (Later Releases)

#### **Schema Registry**
- Handle schema evolution from data providers
- Version management for data structures
- Backward compatibility tools
- Automated migration assistance

#### **Incremental ETL Engine**
- Enhanced change detection and delta processing
- Efficient backfill strategies
- Checkpoint recovery improvements
- Conflict resolution for overlapping data

#### **Advanced Transform Engine**
- Data normalization and standardization pipelines
- Custom transformation rules and business logic
- Cross-provider data harmonization
- Real-time data transformation capabilities

#### **ETL Observability Suite**
- Data lineage tracking through the pipeline
- Enhanced pipeline health monitoring
- Data quality trend analysis
- Performance optimization insights

#### **Load Optimization**
- Advanced partitioning strategies
- Compression and storage format options
- Multi-destination loading (multiple databases, cloud storage)
- Parallel loading with automatic scaling

#### **Data Catalog**
- Metadata management for all ETL assets
- Data discovery and documentation
- Usage analytics and lineage visualization
- Data governance and quality tracking

## Quick Start

### Installation (End Users)

```bash
# Recommended: in a fresh virtualenv (or use pipx)
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
python -m pip install --upgrade pip
pip install marketpipe

# Optional extras (e.g., PostgreSQL support)
# pip install "marketpipe[postgres]"

# Verify install
marketpipe --help
```

### Basic Usage

```bash
# Generate test data (no API keys needed)
marketpipe ingest --provider fake --symbols AAPL GOOGL --start 2025-01-01 --end 2025-01-02

# Query the data
marketpipe query --symbol AAPL --start 2024-01-01

# Start monitoring dashboard
marketpipe metrics --port 8000
```

### With Real Data

```bash
# Set up your environment
export ALPACA_KEY="your_api_key"
export ALPACA_SECRET="your_secret"

# Ingest real market data
marketpipe ingest --provider alpaca --symbols AAPL TSLA --start 2025-01-01 --end 2025-01-02

# Validate data quality
marketpipe validate --symbol AAPL --start 2025-01-01

# Aggregate to different timeframes
marketpipe aggregate --symbol AAPL --timeframe 5m --start 2025-01-01
```

## Documentation

### Core Commands

| Command | Description | Example |
|---------|-------------|---------|
| `ingest` | Collect data from providers | `marketpipe ingest --provider alpaca --symbols AAPL` |
| `query` | Query stored data | `marketpipe query --symbol AAPL --start 2024-01-01` |
| `validate` | Check data quality | `marketpipe validate --symbol AAPL` |
| `aggregate` | Create higher timeframes | `marketpipe aggregate --timeframe 5m` |
| `metrics` | Start monitoring server | `marketpipe metrics --port 8000` |

### Data Providers

#### Alpaca Markets
```bash
export ALPACA_KEY="your_api_key"
export ALPACA_SECRET="your_secret"
marketpipe ingest --provider alpaca --symbols AAPL --feed iex
```

#### IEX Cloud
```bash
export IEX_TOKEN="your_token"
marketpipe ingest --provider iex --symbols AAPL
```

#### Fake Provider (Development)
```bash
# No credentials needed - generates realistic test data
marketpipe ingest --provider fake --symbols AAPL GOOGL --start 2025-01-01
```

## Development

### Quick Setup (From Source)

```bash
git clone https://github.com/joeyda3rd/marketpipe.git
cd marketpipe

# Create a virtual environment (recommended)
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
python -m pip install --upgrade pip

# Install with dev tools for contributors
pip install -e .[dev]

# One-command dev setup
scripts/setup.sh

# Install pre-commit hooks (recommended)
pre-commit install

# Run a quick demo
scripts/demo.sh
```

### Development Commands

```bash
make fmt         # Format code (Black)
make lint        # Lint (Ruff)
make test        # Run tests (pytest)
make check       # Format + lint + mypy + arch check
```

### Testing

```bash
# Fast tests for development feedback (~3s)
scripts/test-fast.sh

# Pre-commit tests (ultra-fast, ~2s)
scripts/pre-commit-tests

# Full test suite with coverage
scripts/test-full.sh

# Simulate CI environment locally
scripts/test-ci.sh

# Make targets
make test         # fast default
make test-all     # full suite
```

### Architecture Validation

MarketPipe enforces Domain-Driven Design boundaries (see `[tool.importlinter]` in `pyproject.toml`).

```bash
# Check architecture compliance
make arch-check
```

## Database Setup

MarketPipe uses Alembic for database schema management:

```bash
# Apply latest migrations
alembic upgrade head

# Create new migration
alembic revision --autogenerate -m "description"

# PostgreSQL setup (optional, SQLite by default)
./tools/database/setup_postgres.sh
```

## Monitoring & Observability

### Metrics Server

```bash
# Start Prometheus metrics endpoint
marketpipe metrics --port 8000

# Metrics available at http://localhost:8000/metrics
```

### Grafana Dashboards

Pre-built dashboards for monitoring ingestion performance, data quality, and system health.

```bash
# See monitoring/README.md for setup instructions
```

### Key Metrics

- **Request rates** and **latency** by provider
- **Data quality** scores and validation errors
- **Ingestion throughput** and **backlog** monitoring
- **System resources** and **error rates**

## Docker Deployment

Complete containerized stack with monitoring:

```bash
# Start MarketPipe + Prometheus + Grafana
docker compose up -d

# Access services:
# MarketPipe metrics: http://localhost:8000/metrics
# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000 (admin/admin)
```

For production deployment, see `docker/README.md`.

## Project Structure

```
src/marketpipe/           # Source code
├── domain/              # Core business logic
├── ingestion/           # ETL pipeline
├── infrastructure/      # External integrations
└── cli/                 # Command-line interface

tests/                   # Comprehensive test suite
examples/                # Usage examples and demos
scripts/                 # Development tools
alembic/                 # Database migrations
tools/                   # Database and development utilities
monitoring/              # Grafana dashboards
docker/                  # Containerized deployment
config/                  # Configuration templates
```

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `ALPACA_KEY` | Alpaca API key | For Alpaca provider |
| `ALPACA_SECRET` | Alpaca API secret | For Alpaca provider |
| `IEX_TOKEN` | IEX Cloud token | For IEX provider |
| `DATABASE_URL` | Database connection | Optional (SQLite default) |

### Configuration Files

MarketPipe supports YAML configuration files for complex setups:

```yaml
providers:
  alpaca:
    feed: "iex"  # or "sip" for premium data
    rate_limit: 200

database:
  url: "postgresql://user:pass@host/db"

monitoring:
  enabled: true
  port: 8000
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Run tests: `make test`
4. Check architecture: `import-linter --config setup.cfg`
5. Submit a pull request

## Alpha Release Notes

### Current Capabilities (v0.1.0-alpha.1)

**Working Features:**
- Multi-provider data ingestion (Alpaca, IEX, Fake)
- Parquet storage with partitioning
- DuckDB query engine
- CLI interface with all core commands
- Docker deployment stack
- Prometheus monitoring
- Data validation and quality checks
- Database migrations

 **Known Limitations:**
- API may change during alpha phase
- Limited error recovery in edge cases
- Documentation is still evolving
- Performance optimizations ongoing

 **Production Readiness:**
- Suitable for development and testing
- Use caution in production environments
- Monitor resource usage and error rates
- Backup data regularly during alpha phase

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/joeyda3rd/marketpipe/issues)
- **Discussions**: [GitHub Discussions](https://github.com/joeyda3rd/marketpipe/discussions)
- **Security**: See [SECURITY.md](SECURITY.md) for security policy

---
