# MarketPipe CLI Commands Reference

This document provides a comprehensive list of all MarketPipe CLI commands with examples and explanations.

## Quick Test Commands

Copy and run each command separately to test different functionality:

### Basic Help and Information

```bash
python -m marketpipe --help
```
*Show main help and available commands*

```bash
python -m marketpipe providers
```
*List all available data providers (alpaca, iex, fake)*

```bash
python -m marketpipe migrate
```
*Ensure database schema is up to date*

### Data Ingestion Commands

#### Using Fake Provider (No Credentials Required)

```bash
python -m marketpipe ingest-ohlcv --provider fake --symbols AAPL --start 2024-01-01 --end 2024-01-02 --batch-size 10
```
*Ingest synthetic data for AAPL for 2 days*

```bash
python -m marketpipe ingest-ohlcv --provider fake --symbols AAPL,MSFT,GOOGL --start 2024-01-01 --end 2024-01-03 --batch-size 50
```
*Ingest multiple symbols for 3 days*

```bash
python -m marketpipe ohlcv ingest --provider fake --symbols TSLA --start 2024-01-01 --end 2024-01-02 --workers 2
```
*Using the structured command format*

#### Using Real Providers (Requires Credentials)

```bash
export ALPACA_KEY="your_key_here"
export ALPACA_SECRET="your_secret_here"
python -m marketpipe ingest-ohlcv --provider alpaca --symbols AAPL --start 2024-01-01 --end 2024-01-02 --feed-type iex
```
*Ingest real data from Alpaca (free IEX feed)*

```bash
export IEX_TOKEN="your_token_here"
python -m marketpipe ingest-ohlcv --provider iex --symbols AAPL --start 2024-01-01 --end 2024-01-02 --batch-size 500
```
*Ingest data from IEX Cloud*

#### Advanced Ingestion Options

```bash
python -m marketpipe ingest-ohlcv --provider fake --symbols AAPL --start 2024-01-01 --end 2024-01-02 --output ./custom_data --workers 4
```
*Custom output directory and worker count*

```bash
python -m marketpipe ingest-ohlcv --config config/example_config.yaml
```
*Use configuration file for ingestion*

```bash
python -m marketpipe ingest-ohlcv --config config/example_config.yaml --symbols NVDA --provider fake
```
*Override config file settings with CLI flags*

### Data Validation Commands

```bash
python -m marketpipe validate-ohlcv --list
```
*List all available validation reports*

```bash
python -m marketpipe validate-ohlcv --job-id job_20241201_123456
```
*Re-run validation for a specific job*

```bash
python -m marketpipe validate-ohlcv --show data/validation_reports/job_20241201_123456_AAPL.csv
```
*Display contents of a specific validation report*

```bash
python -m marketpipe ohlcv validate --list
```
*Using structured command format*

### Data Aggregation Commands

```bash
python -m marketpipe aggregate-ohlcv job_20241201_123456
```
*Aggregate 1-minute data to 5m, 15m, 1h, 1d timeframes*

```bash
python -m marketpipe ohlcv aggregate job_20241201_123456
```
*Using structured command format*

### Data Query Commands

```bash
python -m marketpipe query "SELECT * FROM bars_5m WHERE symbol='AAPL' LIMIT 10"
```
*Query 5-minute bars for AAPL*

```bash
python -m marketpipe query "SELECT symbol, COUNT(*) FROM bars_1d GROUP BY symbol"
```
*Count daily bars by symbol*

```bash
python -m marketpipe query "SELECT symbol, AVG(close) as avg_close FROM bars_1h WHERE symbol IN ('AAPL', 'MSFT') GROUP BY symbol"
```
*Average closing prices for multiple symbols*

```bash
python -m marketpipe query "SELECT MAX(high), MIN(low) FROM bars_1d WHERE symbol='TSLA'" --csv
```
*Export query results as CSV*

```bash
python -m marketpipe query "SELECT * FROM bars_15m WHERE symbol='GOOGL' ORDER BY timestamp DESC LIMIT 20" --limit 50
```
*Custom row limit for table output*

### Metrics and Monitoring Commands

```bash
python -m marketpipe metrics --port 8000
```
*Start async Prometheus metrics server on port 8000*

```bash
python -m marketpipe metrics --port 8080 --legacy-metrics
```
*Start legacy blocking metrics server*

```bash
python -m marketpipe metrics --list
```
*List all available metrics*

```bash
python -m marketpipe metrics --metric ingestion_bars
```
*Show history for specific metric*

```bash
python -m marketpipe metrics --metric validation_errors --since "2024-01-01 10:00"
```
*Show metric history since specific timestamp*

```bash
python -m marketpipe metrics --avg 1h --plot
```
*Show hourly averages with ASCII sparkline plots*

```bash
python -m marketpipe metrics --metric aggregation_latency --avg 1d --plot
```
*Show daily averages for specific metric with plot*

### Backfill Commands

```bash
python -m marketpipe ohlcv backfill backfill --symbol AAPL --lookback 7 --provider fake
```
*Detect and fill gaps for AAPL in last 7 days*

```bash
python -m marketpipe ohlcv backfill backfill --symbol AAPL --symbol MSFT --from 2024-01-01 --provider fake
```
*Backfill multiple symbols from specific date*

```bash
python -m marketpipe ohlcv backfill backfill --config config/example_config.yaml --lookback 30
```
*Use config file for symbol universe and backfill 30 days*

```bash
python -m marketpipe ohlcv backfill backfill --symbol TSLA --lookback 14 --provider alpaca
```
*Backfill with real provider (requires credentials)*

### Data Pruning Commands

```bash
python -m marketpipe prune parquet 30d --dry-run
```
*Preview what parquet files would be deleted (older than 30 days)*

```bash
python -m marketpipe prune parquet 1y
```
*Delete parquet files older than 1 year*

```bash
python -m marketpipe prune parquet 6m --root ./custom_data
```
*Prune files in custom directory (older than 6 months)*

```bash
python -m marketpipe prune database 90d --dry-run
```
*Preview database record cleanup (older than 90 days)*

```bash
python -m marketpipe prune database 18m
```
*Delete database records older than 18 months*

### Deprecated Commands (Still Work)

```bash
python -m marketpipe ingest --provider fake --symbols AAPL --start 2024-01-01 --end 2024-01-02
```
*Deprecated: Use `ingest-ohlcv` instead*

```bash
python -m marketpipe validate --list
```
*Deprecated: Use `validate-ohlcv` instead*

```bash
python -m marketpipe aggregate job_20241201_123456
```
*Deprecated: Use `aggregate-ohlcv` instead*

## Command Structure Patterns

MarketPipe supports multiple command patterns:

### 1. Convenience Commands (Recommended)
```bash
python -m marketpipe ingest-ohlcv [options]
python -m marketpipe validate-ohlcv [options]
python -m marketpipe aggregate-ohlcv [options]
```

### 2. Structured Commands
```bash
python -m marketpipe ohlcv ingest [options]
python -m marketpipe ohlcv validate [options]
python -m marketpipe ohlcv aggregate [options]
python -m marketpipe ohlcv backfill backfill [options]
```

### 3. Utility Commands
```bash
python -m marketpipe query [sql]
python -m marketpipe metrics [options]
python -m marketpipe providers
python -m marketpipe migrate
python -m marketpipe prune [type] [age] [options]
```

## Common Option Patterns

### Date Formats
- `--start 2024-01-01` (YYYY-MM-DD)
- `--end 2024-12-31` (YYYY-MM-DD)

### Symbol Lists
- `--symbols AAPL` (single symbol)
- `--symbols AAPL,MSFT,GOOGL` (comma-separated)
- `--symbol AAPL --symbol MSFT` (repeatable flag)

### Providers
- `--provider fake` (synthetic data, no credentials)
- `--provider alpaca` (requires ALPACA_KEY, ALPACA_SECRET)
- `--provider iex` (requires IEX_TOKEN)

### Time Windows (for pruning/metrics)
- `30d` (30 days)
- `6m` (6 months)
- `2y` (2 years)
- `1h` (1 hour, for metrics)

## Environment Variables

```bash
export ALPACA_KEY="your_alpaca_api_key"
export ALPACA_SECRET="your_alpaca_secret_key"
export IEX_TOKEN="your_iex_cloud_token"
export DATABASE_URL="postgresql://user:pass@localhost:5432/marketpipe"  # Optional: Use PostgreSQL instead of SQLite
```

## Quick Testing Sequence

Run these commands in order to test the complete pipeline:

```bash
# 1. Check installation
python -m marketpipe --help

# 2. Ingest sample data
python -m marketpipe ingest-ohlcv --provider fake --symbols AAPL,MSFT --start 2024-01-01 --end 2024-01-03 --batch-size 50

# 3. Validate data
python -m marketpipe validate-ohlcv --list

# 4. Aggregate data (use actual job ID from step 2)
python -m marketpipe aggregate-ohlcv job_20241201_123456

# 5. Query aggregated data
python -m marketpipe query "SELECT * FROM bars_1d LIMIT 5"

# 6. Check metrics
python -m marketpipe metrics --list

# 7. Start metrics server (in separate terminal)
python -m marketpipe metrics --port 8000
```

## Comprehensive Pipeline Script

For a complete end-to-end demonstration, use the comprehensive pipeline script:

```bash
# Dry run (shows commands without executing)
python scripts/comprehensive_data_pipeline.py --dry-run

# Execute with fake provider
python scripts/comprehensive_data_pipeline.py --provider fake

# Execute with real provider (requires credentials)
python scripts/comprehensive_data_pipeline.py --provider alpaca
```

This script demonstrates:
- 3-month data ingestion in weekly chunks
- Data validation and quality checks
- Multi-timeframe aggregation
- Metrics monitoring
- Data querying and analysis
- Gap detection and backfilling
- Cleanup and maintenance operations 