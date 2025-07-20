# CLI Module

## Purpose

The CLI module provides MarketPipe's command-line interface built with Typer. It offers hierarchical commands for OHLCV data ingestion, validation, aggregation, backfill operations, data pruning, and system utilities.

## Key Public Interfaces

### Command Structure
```bash
# Main commands
marketpipe ingest-ohlcv --provider alpaca --symbols AAPL,MSFT --start 2024-01-01 --end 2024-01-02
marketpipe validate-ohlcv --list
marketpipe aggregate-ohlcv --timeframe 1h --symbols AAPL
marketpipe metrics --port 8000

# OHLCV sub-commands
marketpipe ohlcv ingest --provider alpaca --symbols AAPL --start 2024-01-01 --end 2024-01-02
marketpipe ohlcv validate --job-id abc123
marketpipe ohlcv aggregate --timeframe 1d
marketpipe ohlcv backfill detect --symbols AAPL,MSFT

# Data management
marketpipe prune parquet 30d --dry-run
marketpipe prune database 18m
marketpipe query "SELECT * FROM bars_1d WHERE symbol='AAPL' LIMIT 10"
```

### Configuration-based Execution
```bash
# Using YAML config files
marketpipe ingest-ohlcv --config config/example.yaml
marketpipe ingest-ohlcv --config config.yaml --symbols AAPL --batch-size 500  # CLI overrides
```

## Call Graph

```
CLI Command
    ↓
bootstrap() → Database migrations + Service registration
    ↓
Command Implementation
    ↓
Application Services (Ingestion/Validation/Aggregation)
    ↓
Domain Services → Infrastructure → External APIs/Storage
```

## Examples

### Ingestion Command Flow
```python
@Code:src/marketpipe/cli/ohlcv_ingest.py:42-89
```

The ingestion commands wire together DDD services with provider-specific adapters.

### Validation Report Management
```python
@Code:src/marketpipe/cli/ohlcv_validate.py:16-68
```

Validation commands provide CSV report generation and inspection capabilities.

### Metrics Server Management
```python
@Code:src/marketpipe/cli/utils.py:17-66
```

The metrics command supports both async and legacy server modes with history queries.

### Data Pruning Operations
```python
@Code:src/marketpipe/cli/prune.py:43-70
```

Prune commands handle retention policies with date parsing and dry-run support.

## Command Reference

### Core OHLCV Commands

| Command | Purpose | Key Options |
|---------|---------|-------------|
| `ingest-ohlcv` | Ingest market data | `--provider`, `--symbols`, `--start/end`, `--config` |
| `validate-ohlcv` | Validate data quality | `--job-id`, `--list`, `--show` |
| `aggregate-ohlcv` | Aggregate timeframes | `--timeframe`, `--symbols`, `--start/end` |

### OHLCV Sub-commands

| Command | Purpose | Key Options |
|---------|---------|-------------|
| `ohlcv ingest` | Same as `ingest-ohlcv` | Same options |
| `ohlcv validate` | Same as `validate-ohlcv` | Same options |
| `ohlcv aggregate` | Same as `aggregate-ohlcv` | Same options |
| `ohlcv backfill detect` | Find data gaps | `--symbols`, `--start/end` |
| `ohlcv backfill fill` | Fill data gaps | `--symbols`, `--gaps-file` |

### Utility Commands

| Command | Purpose | Key Options |
|---------|---------|-------------|
| `metrics` | Metrics server/history | `--port`, `--metric`, `--avg`, `--plot` |
| `providers` | List data providers | None |
| `migrate` | Apply DB migrations | `--path` |
| `query` | Ad-hoc SQL queries | `--csv`, `--limit` |

### Data Management

| Command | Purpose | Key Options |
|---------|---------|-------------|
| `prune parquet` | Delete old parquet files | `--older-than`, `--root`, `--dry-run` |
| `prune database` | Delete old database records | `--older-than`, `--dry-run` |

## Configuration Support

### YAML Configuration
```yaml
@Code:config/example_config.yaml:1-23
```

### CLI Override Behavior
1. **Config-only**: `--config file.yaml`
2. **Flags-only**: `--symbols AAPL --start 2024-01-01 --end 2024-01-02`
3. **Config + overrides**: `--config file.yaml --batch-size 500`

CLI flags override corresponding config file values when both are provided.

## Provider Registry

```python
@Code:src/marketpipe/cli/utils.py:229-249
```

The `providers` command lists all registered market data providers and their capabilities.

## Error Handling

- Bootstrap failures → Database migration errors
- Configuration errors → Invalid YAML or missing credentials  
- Validation errors → Typer parameter validation
- Application errors → Domain service failures with user-friendly messages

## Deprecation Strategy

The CLI includes deprecation warnings for command migration:
- `marketpipe ingest` → `marketpipe ingest-ohlcv`
- `marketpipe validate` → `marketpipe validate-ohlcv`
- `marketpipe aggregate` → `marketpipe aggregate-ohlcv`

## Environment Variables

- `ALPACA_KEY/ALPACA_SECRET` - Default provider credentials
- `MP_DB` - Core database path
- `METRICS_DB_PATH` - Metrics database path
- `DATABASE_URL` - Alternative database configuration 