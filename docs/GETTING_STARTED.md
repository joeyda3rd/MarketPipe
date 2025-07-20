# Getting Started with MarketPipe

> ⚠️ **Alpha Software Warning**
> MarketPipe is currently in alpha development. While feature-complete for basic ETL workflows, expect breaking changes, incomplete documentation, and potential stability issues. Not recommended for production use without thorough testing.

## Overview

MarketPipe is a modern, Python-native ETL framework specifically designed for financial market data workflows. It uses Domain-Driven Design principles to provide a robust, maintainable architecture for collecting, validating, transforming, and storing market data from various providers.

## Quick Start (5 minutes)

### 1. Installation

```bash
# Install MarketPipe
pip install marketpipe

# Verify installation
marketpipe --version
```

### 2. Set Up Your First Data Source

MarketPipe supports multiple market data providers. For this quick start, we'll use Alpaca Markets (free tier available):

```bash
# Create your first configuration
mkdir my-marketpipe && cd my-marketpipe
cp -r $(python -c "import marketpipe; print(marketpipe.__path__[0])")/config/examples/* .

# Create environment file for credentials
echo "ALPACA_KEY=your_api_key_here" > .env
echo "ALPACA_SECRET=your_secret_here" >> .env
```

### 3. Your First Data Ingestion

```bash
# Edit the basic config for your symbols
nano basic_config.yaml

# Run your first ingestion
marketpipe ingest --config basic_config.yaml --symbol AAPL --start 2024-01-02 --end 2024-01-02

# Check the results
ls -la data/
```

## Complete Setup Guide

### Prerequisites

- **Python 3.9+** (3.11+ recommended)
- **8GB+ RAM** (for processing large datasets)
- **API credentials** for at least one supported data provider

### Installation Options

#### Option 1: Basic Installation
```bash
pip install marketpipe
```

#### Option 2: Development Installation
```bash
# Clone the repository
git clone https://github.com/your-org/marketpipe.git
cd marketpipe

# One-command development setup
scripts/setup

# Or manual setup:
pip install -e ".[dev]"

# Install pre-commit hooks (recommended for contributors)
pip install pre-commit
pre-commit install

# Verify installation
scripts/health-check
scripts/test-fast
```

#### Option 3: Docker Installation
```bash
# Pull the pre-built image
docker pull marketpipe/marketpipe:latest

# Or build locally
docker build -t marketpipe .
```

### Supported Data Providers

MarketPipe supports multiple market data providers through a unified interface:

| Provider | Cost | Data Quality | Real-time | Historical | Setup Difficulty |
|----------|------|--------------|-----------|------------|------------------|
| **Alpaca Markets** | Free tier available | High | Yes | Yes | Easy |
| **IEX Cloud** | Freemium | High | Yes | Yes | Easy |
| **Fake Provider** | Free | Test data | No | Limited | None |

## Configuration Guide

### Basic Configuration Structure

MarketPipe uses YAML configuration files with the following structure:

```yaml
# Provider configuration
alpaca:
  key: # Loaded from ALPACA_KEY environment variable
  secret: # Loaded from ALPACA_SECRET environment variable
  base_url: https://data.alpaca.markets/v2
  rate_limit_per_min: 200
  feed: iex  # 'iex' for free tier, 'sip' for paid

# Pipeline configuration
symbols:
  - AAPL
  - GOOGL
  - MSFT

start: "2024-01-02"
end: "2024-01-03"
output_path: "./data"
compression: snappy
workers: 3

# Optional: Enable monitoring
metrics:
  enabled: true
  port: 8000
```

### Environment Variables

Create a `.env` file in your project directory:

```bash
# Alpaca Markets credentials
ALPACA_KEY=your_api_key_here
ALPACA_SECRET=your_secret_here

# Optional: Database settings
DATABASE_URL=sqlite:///marketpipe.db

# Optional: Monitoring
PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
```

### Advanced Configuration

For production use, consider these additional settings:

```yaml
# State management for resumable ingestion
state:
  backend: sqlite
  path: "./ingestion_state.db"

# Enhanced error handling
ingestion:
  max_retries: 5
  backoff_factor: 2.0
  timeout: 60

# Performance tuning
performance:
  workers: 5
  batch_size: 1000
  memory_limit: "4GB"
```

## Core Workflows

### 1. Data Ingestion

The primary workflow for collecting market data:

```bash
# Basic ingestion for single symbol
marketpipe ingest --config config.yaml --symbol AAPL --start 2024-01-01 --end 2024-01-31

# Multiple symbols with date range
marketpipe ingest --config config.yaml \
  --symbol AAPL --symbol GOOGL --symbol MSFT \
  --start 2024-01-01 --end 2024-01-31

# Batch ingestion from file
echo "AAPL\nGOOGL\nMSFT\nTSLA" > symbols.txt
marketpipe ingest --config config.yaml --symbols-file symbols.txt --start 2024-01-01 --end 2024-01-31

# Resume from checkpoint
marketpipe ingest --config config.yaml --resume

# Dry run to preview
marketpipe ingest --config config.yaml --symbol AAPL --start 2024-01-01 --end 2024-01-31 --dry-run
```

### 2. Data Validation

Ensure data quality with built-in validation:

```bash
# Validate ingested data
marketpipe validate --data-path ./data --symbol AAPL

# Validate with custom schema
marketpipe validate --data-path ./data --schema custom_schema.json

# Generate validation report
marketpipe validate --data-path ./data --output-report validation_report.json
```

### 3. Data Aggregation

Transform minute-level data to other timeframes:

```bash
# Aggregate to hourly bars
marketpipe aggregate --input ./data --output ./hourly --timeframe 1h

# Aggregate to daily with custom date range
marketpipe aggregate --input ./data --output ./daily \
  --timeframe 1d --start 2024-01-01 --end 2024-01-31

# Multiple timeframes
marketpipe aggregate --input ./data --output ./aggregated \
  --timeframe 5m --timeframe 15m --timeframe 1h --timeframe 1d
```

### 4. Data Querying

Query your data using SQL or Python:

```bash
# Interactive query mode
marketpipe query --data-path ./data

# Execute SQL file
marketpipe query --data-path ./data --sql-file queries/daily_summary.sql

# Export results
marketpipe query --data-path ./data \
  --sql "SELECT * FROM read_parquet('./data/symbol=AAPL/**/*.parquet')" \
  --output results.csv
```

### 5. Monitoring & Observability

Track your ETL pipeline performance:

```bash
# Start metrics server
marketpipe metrics --port 8000

# View metrics in browser
open http://localhost:8000/metrics

# Check system status
marketpipe admin status
```

## Data Storage & Access

### File Organization

MarketPipe uses Hive-style partitioning for efficient data storage:

```
data/
├── symbol=AAPL/
│   ├── date=2024-01-02/
│   │   └── AAPL_2024-01-02.parquet
│   ├── date=2024-01-03/
│   │   └── AAPL_2024-01-03.parquet
│   └── ...
├── symbol=GOOGL/
│   └── ...
└── symbol=MSFT/
    └── ...
```

### Accessing Your Data

#### Python API
```python
import marketpipe

# Load data for analysis
df = marketpipe.load_ohlcv("./data", symbol="AAPL", start="2024-01-01", end="2024-01-31")

# Query with SQL
result = marketpipe.query_sql(
    "./data",
    "SELECT symbol, AVG(close) as avg_close FROM read_parquet('./data/**/*.parquet') GROUP BY symbol"
)
```

#### Direct Parquet Access
```python
import pandas as pd
import duckdb

# Using pandas
df = pd.read_parquet("./data/symbol=AAPL/")

# Using DuckDB for SQL queries
conn = duckdb.connect()
result = conn.execute(
    "SELECT * FROM read_parquet('./data/**/*.parquet') WHERE symbol = 'AAPL'"
).fetchdf()
```

### Schema Reference

MarketPipe uses a standardized OHLCV schema:

```json
{
  "symbol": "AAPL",
  "timestamp": 1640995800000000000,
  "date": "2022-01-01",
  "open": 177.83,
  "high": 178.34,
  "low": 177.71,
  "close": 178.20,
  "volume": 74919600,
  "trade_count": 645046,
  "vwap": 178.089,
  "session": "regular",
  "currency": "USD",
  "status": "ok",
  "source": "alpaca",
  "frame": "1m",
  "schema_version": 1
}
```

## Troubleshooting

### Common Issues

#### Rate Limiting
```bash
# Error: Rate limit exceeded
# Solution: Adjust rate limits in config
alpaca:
  rate_limit_per_min: 100  # Reduce from default 200
```

#### Memory Issues
```bash
# Error: Out of memory during ingestion
# Solution: Reduce batch size and workers
workers: 1
batch_size: 500
```

#### Authentication Errors
```bash
# Error: Invalid API credentials
# Solution: Verify environment variables
echo $ALPACA_KEY
echo $ALPACA_SECRET

# Check .env file exists and has correct format
cat .env
```

#### Data Validation Failures
```bash
# Error: Schema validation failed
# Solution: Check data quality and schema version
marketpipe validate --data-path ./data --verbose
```

### Getting Help

1. **Check logs**: MarketPipe provides detailed logging
   ```bash
   marketpipe ingest --config config.yaml --verbose
   ```

2. **Validate configuration**:
   ```bash
   marketpipe admin validate-config --config config.yaml
   ```

3. **System diagnostics**:
   ```bash
   marketpipe admin diagnose
   ```

4. **Community support**:
   - [GitHub Discussions](https://github.com/your-org/marketpipe/discussions)
   - [GitHub Issues](https://github.com/your-org/marketpipe/issues)

## Next Steps

### For Development

#### Quick Development Setup
```bash
# One-command setup
scripts/setup

# Install pre-commit hooks (recommended)
pip install pre-commit
pre-commit install

# Verify setup
scripts/health-check
```

#### Development Workflow
```bash
# Fast development feedback loop
scripts/test-fast        # Quick tests (~3s) during development
scripts/format           # Format code before committing

# Git workflow with automatic quality checks
git add .
git commit -m "Your changes"  # Pre-commit hooks run automatically

# Before pushing - simulate CI locally
scripts/test-ci          # Full CI simulation
```

#### Testing Options
```bash
# Ultra-fast tests for pre-commit hooks
scripts/pre-commit-tests  # ~2 seconds

# Development feedback tests
scripts/test-fast         # ~3 seconds, verbose output

# Full test suite with coverage
scripts/test-full         # Complete suite

# Run specific test categories
pytest -m fast           # Only fast tests
pytest -m api_client     # Only API client tests
pytest -m config         # Only configuration tests
```

#### Additional Resources
1. **Pre-commit Framework**: See [pre-commit.md](pre-commit.md) for detailed setup
2. **Contributing Guide**: Follow [CONTRIBUTING.md](../CONTRIBUTING.md)
3. **Architecture**: Review [Architecture Guide](ARCHITECTURE.md)

### For Production
1. **Review security**: [SECURITY.md](../SECURITY.md)
2. **Set up monitoring**: Configure Prometheus + Grafana
3. **Database setup**: Use PostgreSQL for production state management
4. **Performance tuning**: Optimize for your data volume and infrastructure

### Advanced Features
1. **Custom providers**: Implement new data source adapters
2. **Custom validators**: Add business-specific validation rules
3. **Stream processing**: Real-time data ingestion patterns
4. **Multi-region deployment**: Distributed ingestion setup

## Alpha Release Limitations

Please be aware of current limitations:

- ⚠️ **Breaking Changes Expected**: API and configuration may change
- ⚠️ **Limited Provider Support**: Only Alpaca and IEX fully implemented
- ⚠️ **Basic Error Recovery**: Some failure modes require manual intervention
- ⚠️ **Performance**: Not optimized for very large datasets (>1TB)
- ⚠️ **Documentation**: Some advanced features lack comprehensive docs

For production use, thoroughly test with your specific requirements and data volumes.

---

**Need help?** Join our [community discussions](https://github.com/your-org/marketpipe/discussions) or [report issues](https://github.com/your-org/marketpipe/issues).
