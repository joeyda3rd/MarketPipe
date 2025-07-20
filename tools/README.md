# MarketPipe Tools

This directory contains essential development and database tools for MarketPipe.

## Database Tools (`database/`)

### `setup_postgres.sh`
PostgreSQL database setup and configuration script for development and testing.

**Usage:**
```bash
./tools/database/setup_postgres.sh
```

**What it does:**
- Sets up PostgreSQL instance for development
- Creates necessary databases and users
- Configures permissions and extensions
- Validates connection and schema

## Development Tools (`development/`)

### `run_full_pipeline.py`
Complete ETL pipeline runner for testing and development workflows.

**Usage:**
```bash
python tools/development/run_full_pipeline.py --config config/example_config.yaml
```

**Features:**
- End-to-end pipeline testing
- Configurable data sources and timeframes
- Performance monitoring and metrics
- Error handling and retry logic

### `validation_report.py`
Data quality validation and reporting tool.

**Usage:**
```bash
python tools/development/validation_report.py --symbol AAPL --date 2024-01-02
```

**Features:**
- OHLCV data validation
- Schema compliance checking
- Quality metrics reporting
- Export validation results

### `smoketest.sh`
Quick smoke test for basic system functionality.

**Usage:**
```bash
./tools/development/smoketest.sh
```

**Tests:**
- CLI commands work
- Database connectivity
- API client functionality
- Configuration loading
- Basic data processing

## Usage Guidelines

### For Development
1. Use `smoketest.sh` for quick validation after changes
2. Use `run_full_pipeline.py` for comprehensive testing
3. Use `validation_report.py` for data quality analysis

### For Database Setup
1. Run `setup_postgres.sh` for PostgreSQL development environment
2. Use with `alembic upgrade head` to apply migrations
3. Verify with smoke tests

### Integration with Main Workflow
These tools complement the main development workflow:

```bash
# Quick development cycle
make test-fast                          # Fast tests
./tools/development/smoketest.sh        # Smoke test
make test                               # Full test suite

# Database development
./tools/database/setup_postgres.sh      # Setup database
alembic upgrade head                     # Apply migrations
python tools/development/run_full_pipeline.py  # Test pipeline
```

## Dependencies

Most tools require MarketPipe to be installed in development mode:

```bash
pip install -e ".[dev]"
```

Some tools may require additional PostgreSQL setup or specific configuration files.
