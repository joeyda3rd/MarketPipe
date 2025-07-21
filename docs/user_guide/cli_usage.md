# CLI Usage Guide

This guide covers MarketPipe's command-line interface with practical examples and common workflows. The CLI is designed for both interactive use and automation.

## Basic Commands

### Help and Information

```bash
# Main help
marketpipe --help

# Command-specific help
marketpipe ingest --help
marketpipe query --help

# List available data providers
marketpipe providers

# Check system health
marketpipe health-check --verbose
```

### Version and Status

```bash
# Show version information
marketpipe --version

# Database migration status
marketpipe migrate --dry-run

# System health with detailed diagnostics
marketpipe health-check --dependencies
```

## Data Operations

### Ingestion

The `ingest` command fetches market data from providers:

```bash
# Basic ingestion
marketpipe ingest --symbol AAPL --start 2024-01-02 --end 2024-01-02

# Multiple symbols
marketpipe ingest --symbol AAPL --symbol GOOGL --symbol MSFT \
  --start 2024-01-02 --end 2024-01-05

# With configuration file
marketpipe ingest --config config.yaml

# Dry run (show what would be ingested)
marketpipe ingest --symbol AAPL --start 2024-01-02 --end 2024-01-02 --dry-run

# Parallel processing
marketpipe ingest --symbol AAPL --workers 5 --start 2024-01-02 --end 2024-01-02
```

### Data Validation

Ensure data quality with validation commands:

```bash
# Validate specific symbol
marketpipe validate --symbol AAPL

# Validate with custom rules
marketpipe validate --symbol AAPL --rules schema/custom_rules.json

# Full pipeline validation
marketpipe validate --comprehensive --output validation_report.json
```

### Data Querying

Query ingested data using DuckDB integration:

```bash
# Basic query
marketpipe query --symbol AAPL --limit 10

# Date range query
marketpipe query --symbol AAPL --start 2024-01-02 --end 2024-01-05

# Custom SQL query
marketpipe query --sql "SELECT symbol, timestamp, close FROM bars WHERE volume > 1000000"

# Export to CSV
marketpipe query --symbol AAPL --output data.csv --format csv
```

### Data Aggregation

Convert minute bars to larger timeframes:

```bash
# Aggregate to 5-minute bars
marketpipe aggregate --symbol AAPL --timeframe 5m --start 2024-01-02

# Multiple timeframes
marketpipe aggregate --symbol AAPL --timeframe 15m,1h,1d --start 2024-01-01 --end 2024-01-31

# Custom aggregation rules
marketpipe aggregate --config aggregation_config.yaml
```

## Job Management

MarketPipe tracks ingestion jobs for monitoring and recovery:

```bash
# List active jobs
marketpipe jobs list

# Job status and history
marketpipe jobs status --symbol AAPL

# Cancel running job
marketpipe jobs cancel --job-id abc123

# Retry failed job
marketpipe jobs retry --job-id xyz789

# Clear job history
marketpipe jobs clear --older-than 30d
```

## Backfill Operations

For historical data collection:

```bash
# Backfill single symbol
marketpipe backfill --symbol AAPL --start 2023-01-01 --end 2023-12-31

# Backfill multiple symbols with chunking
marketpipe backfill --symbol AAPL,GOOGL,MSFT \
  --start 2023-01-01 --end 2023-12-31 \
  --chunk-size 30d --workers 8

# Resume interrupted backfill
marketpipe backfill --resume --job-id backfill_2024_001
```

## Monitoring and Metrics

### Health Checks

```bash
# Basic health check
marketpipe health-check

# Detailed diagnostics
marketpipe health-check --verbose --dependencies

# Provider connectivity test
marketpipe health-check --provider alpaca

# Database connectivity
marketpipe health-check --database
```

### Metrics Server

```bash
# Start Prometheus metrics server
marketpipe metrics --port 8000

# Metrics with custom directory
marketpipe metrics --port 8000 --multiprocess-dir /tmp/marketpipe_metrics

# View metrics in browser: http://localhost:8000/metrics
```

## Data Management

### Symbol Management

```bash
# List available symbols from provider
marketpipe symbols list --provider alpaca

# Add symbols to tracking
marketpipe symbols add AAPL GOOGL MSFT

# Remove symbols
marketpipe symbols remove DEFUNCT_SYMBOL

# Symbol information
marketpipe symbols info AAPL
```

### Data Pruning

```bash
# Prune old data (older than 1 year)
marketpipe prune --older-than 365d --symbol AAPL

# Prune by size (keep last 1GB)
marketpipe prune --keep-size 1GB --symbol AAPL

# Safe pruning with confirmation
marketpipe prune --interactive --older-than 180d
```

## Configuration Management

### Configuration Files

```bash
# Generate example configuration
marketpipe config generate --provider alpaca --output config.yaml

# Validate configuration
marketpipe config validate config.yaml

# Show effective configuration (with env vars)
marketpipe config show --resolved
```

### Environment Setup

```bash
# Initialize new project directory
marketpipe init --provider alpaca --symbols AAPL,GOOGL,MSFT

# Create .env template
marketpipe env generate --provider alpaca
```

## Advanced Operations

### Database Operations

```bash
# Run database migrations
marketpipe migrate

# Create database backup
marketpipe backup create --output backup_2024_01_20.sql

# Restore from backup
marketpipe backup restore backup_2024_01_20.sql

# Optimize database performance
marketpipe optimize --vacuum --reindex
```

### Factory Reset

```bash
# Clear all data (with confirmation)
marketpipe reset --confirm

# Reset specific components
marketpipe reset --jobs-only
marketpipe reset --data-only --symbol AAPL
```

## Workflow Examples

### Daily Data Pipeline

```bash
#!/bin/bash
# daily_pipeline.sh

# Health check
marketpipe health-check --quiet || exit 1

# Ingest yesterday's data
marketpipe ingest \
  --symbol AAPL,GOOGL,MSFT,TSLA \
  --start $(date -d "yesterday" +%Y-%m-%d) \
  --end $(date -d "yesterday" +%Y-%m-%d) \
  --workers 4

# Validate data quality
marketpipe validate --comprehensive --quiet

# Generate daily aggregations
marketpipe aggregate \
  --timeframe 5m,15m,1h \
  --start $(date -d "yesterday" +%Y-%m-%d) \
  --symbols AAPL,GOOGL,MSFT,TSLA

echo "Daily pipeline completed successfully"
```

### Historical Backfill

```bash
#!/bin/bash
# historical_backfill.sh

SYMBOL="AAPL"
START_DATE="2023-01-01"
END_DATE="2023-12-31"

# Check available space
marketpipe health-check --disk-space || exit 1

# Run backfill with progress monitoring
marketpipe backfill \
  --symbol $SYMBOL \
  --start $START_DATE \
  --end $END_DATE \
  --chunk-size 7d \
  --workers 6 \
  --progress

# Validate backfilled data
marketpipe validate \
  --symbol $SYMBOL \
  --start $START_DATE \
  --end $END_DATE \
  --report validation_report_${SYMBOL}.json

echo "Backfill completed for $SYMBOL"
```

### Data Quality Monitoring

```bash
#!/bin/bash
# quality_check.sh

# Daily data quality report
marketpipe validate \
  --comprehensive \
  --start $(date -d "yesterday" +%Y-%m-%d) \
  --output daily_quality_$(date +%Y%m%d).json \
  --format json

# Check for anomalies
marketpipe query --sql "
  SELECT symbol, COUNT(*) as bar_count,
         MIN(volume) as min_volume,
         MAX(volume) as max_volume
  FROM bars
  WHERE date = '$(date -d "yesterday" +%Y-%m-%d)'
  GROUP BY symbol
  HAVING bar_count < 390  -- Expected bars per trading day
" --output anomalies.csv

# Alert if anomalies found
if [ -s anomalies.csv ]; then
  echo "Data quality issues detected. Check anomalies.csv"
  exit 1
fi
```

## CLI Options Reference

### Global Options

| Option | Description | Example |
|--------|-------------|---------|
| `--config` | Configuration file path | `--config config.yaml` |
| `--verbose` | Verbose output | `--verbose` |
| `--quiet` | Minimal output | `--quiet` |
| `--log-level` | Set logging level | `--log-level DEBUG` |
| `--help` | Show help | `--help` |

### Common Date Options

| Option | Description | Format |
|--------|-------------|--------|
| `--start` | Start date | `YYYY-MM-DD` |
| `--end` | End date | `YYYY-MM-DD` |
| `--date` | Single date | `YYYY-MM-DD` |

### Output Options

| Option | Description | Values |
|--------|-------------|---------|
| `--format` | Output format | `json`, `csv`, `yaml`, `table` |
| `--output` | Output file | `results.json` |
| `--limit` | Limit results | `--limit 100` |

## Tips and Best Practices

### Performance Optimization

- **Use appropriate worker counts**: Start with 2-4 workers, increase based on system capacity
- **Chunk large date ranges**: Use `--chunk-size` for backfills spanning months
- **Monitor system resources**: Use `marketpipe health-check` to check disk space and memory

### Error Handling

- **Always check return codes**: Commands exit with non-zero on failure
- **Use dry-run mode**: Test commands with `--dry-run` before running
- **Keep logs**: Use `--log-level DEBUG` for troubleshooting

### Automation

- **Health checks first**: Always run `marketpipe health-check` in automation
- **Use configuration files**: Prefer config files over command-line arguments for repeatability
- **Monitor job status**: Use `marketpipe jobs status` to track long-running operations

## Troubleshooting

For detailed troubleshooting information, see the [Troubleshooting Guide](troubleshooting.md).

### Common Issues

- **Command not found**: Ensure MarketPipe is properly installed and in PATH
- **Permission errors**: Check file permissions on data directory
- **Network timeouts**: Adjust `--timeout` settings for slow connections
- **Memory issues**: Reduce `--workers` count or `--chunk-size` for large operations

## Next Steps

- **Configuration**: Learn advanced configuration in [Configuration Guide](configuration.md)
- **Monitoring**: Set up comprehensive monitoring in [Monitoring Guide](monitoring.md)
- **API Integration**: For programmatic access, see the [Developer Guide](../developer_guide/architecture.md)

---

*Last updated: 2024-01-20*
