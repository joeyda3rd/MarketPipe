# Troubleshooting Guide

This guide helps you diagnose and resolve common MarketPipe issues. Follow the structured approach to quickly identify and fix problems.

## Quick Diagnostics

### 1. Run Health Check

Always start with the comprehensive health check:

```bash
# Quick health overview
marketpipe health-check

# Detailed diagnostics
marketpipe health-check --verbose --dependencies

# Component-specific checks
marketpipe health-check --database --providers --disk-space
```

### 2. Check System Status

```bash
# Verify installation
marketpipe --version
python -m marketpipe --help

# Check configuration
marketpipe config show --resolved

# Test provider connectivity
marketpipe providers test alpaca
```

### 3. Enable Debug Logging

```bash
# Enable debug logging for troubleshooting
export MARKETPIPE_LOG_LEVEL=DEBUG
marketpipe ingest --symbol AAPL --start 2024-01-02 --end 2024-01-02

# Or set in configuration
marketpipe --log-level DEBUG ingest --config config.yaml
```

## Installation Issues

### Command Not Found

**Problem**: `marketpipe: command not found` after installation

**Solutions**:

```bash
# Check if MarketPipe is installed
pip list | grep marketpipe

# If not installed, install it
pip install marketpipe

# If installed but not in PATH, add Python scripts directory
export PATH="$PATH:$(python -m site --user-base)/bin"

# Or run as Python module
python -m marketpipe --help

# Make PATH change permanent
echo 'export PATH="$PATH:$(python -m site --user-base)/bin"' >> ~/.bashrc
source ~/.bashrc
```

### Permission Errors

**Problem**: Permission denied when running MarketPipe

**Solutions**:

```bash
# Check data directory permissions
ls -la ./data/
chmod 755 ./data/

# Create directories with proper permissions
mkdir -p ./data ./logs
chmod 755 ./data ./logs

# Fix ownership if needed
sudo chown -R $(whoami):$(whoami) ./data ./logs
```

### Dependencies Issues

**Problem**: Import errors or missing dependencies

**Solutions**:

```bash
# Reinstall with all dependencies
pip install --force-reinstall marketpipe[dev]

# Install specific missing dependencies
pip install pandas duckdb pyarrow httpx

# Check Python version (requires 3.9+)
python --version

# Use virtual environment to avoid conflicts
python -m venv marketpipe-env
source marketpipe-env/bin/activate
pip install marketpipe
```

## Configuration Issues

### Credentials Not Working

**Problem**: Authentication errors (401, 403)

**Solutions**:

```bash
# Check environment variables are set
echo $ALPACA_KEY
echo $ALPACA_SECRET

# Verify .env file exists and is formatted correctly
cat .env
# Should contain:
# ALPACA_KEY=your_key_here
# ALPACA_SECRET=your_secret_here

# Test credentials directly
curl -H "APCA-API-KEY-ID: $ALPACA_KEY" \
     -H "APCA-API-SECRET-KEY: $ALPACA_SECRET" \
     https://paper-api.alpaca.markets/v2/account

# Check credential format (remove any quotes or spaces)
echo "$ALPACA_KEY" | wc -c  # Should be expected length
echo "$ALPACA_KEY" | grep -E '^[A-Z0-9]{20}$'  # Basic format check
```

### Configuration File Issues

**Problem**: Configuration file not found or invalid

**Solutions**:

```bash
# Check if config file exists
ls -la config.yaml

# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('config.yaml'))"

# Use absolute path if relative path fails
marketpipe --config /full/path/to/config.yaml ingest

# Generate a working configuration
marketpipe config generate --provider alpaca --output working_config.yaml

# Show effective configuration (with env vars resolved)
marketpipe config show --resolved
```

### Environment Variables Not Loading

**Problem**: Environment variables not being recognized

**Solutions**:

```bash
# Check if .env file is in the current directory
ls -la .env

# Verify .env file format (no spaces around =)
cat .env
# Good: ALPACA_KEY=value
# Bad:  ALPACA_KEY = value

# Manually export variables
export ALPACA_KEY=your_key_here
export ALPACA_SECRET=your_secret_here

# Check if variables are loaded in Python
python -c "import os; print('ALPACA_KEY' in os.environ)"
```

## Data Provider Issues

### Rate Limiting

**Problem**: Rate limit exceeded (429 errors)

**Solutions**:

```bash
# Reduce rate limit in configuration
# config.yaml:
providers:
  alpaca:
    rate_limit_per_min: 100  # Reduced from default

# Add delays between requests
marketpipe ingest --symbol AAPL --start 2024-01-02 --workers 1

# Use exponential backoff (automatic in MarketPipe)
# Check retry settings in logs
MARKETPIPE_LOG_LEVEL=DEBUG marketpipe ingest --symbol AAPL
```

### Network Connectivity

**Problem**: Network timeouts or connection errors

**Solutions**:

```bash
# Test basic connectivity
curl -I https://data.alpaca.markets/v2/stocks/bars

# Check DNS resolution
nslookup data.alpaca.markets

# Test with increased timeout
# config.yaml:
providers:
  alpaca:
    timeout: 60.0  # Increased from default 30.0
    max_retries: 5

# Check firewall/proxy settings
export https_proxy=http://your-proxy:port
marketpipe ingest --symbol AAPL
```

### Data Provider Errors

**Problem**: API returning errors or invalid data

**Solutions**:

```bash
# Check provider status
marketpipe providers status alpaca

# Test with different symbols
marketpipe ingest --symbol SPY --start 2024-01-02  # Try ETF
marketpipe ingest --symbol MSFT --start 2024-01-02  # Try different stock

# Check market hours (data may not be available outside trading hours)
marketpipe providers info alpaca

# Use different data feed
# config.yaml:
providers:
  alpaca:
    feed: "sip"  # Instead of "iex"
```

## Database Issues

### Database Connection Errors

**Problem**: Cannot connect to database

**Solutions**:

```bash
# Check database file exists (SQLite)
ls -la marketpipe.db

# Test database connection
marketpipe health-check --database

# Reset database if corrupted
marketpipe reset --database --confirm

# Check database URL format
export MARKETPIPE_DB_URL="sqlite:///./marketpipe.db"

# For PostgreSQL
export MARKETPIPE_DB_URL="postgresql://user:pass@localhost/marketpipe"
marketpipe migrate  # Run migrations
```

### Migration Issues

**Problem**: Database migration failures

**Solutions**:

```bash
# Check migration status
marketpipe migrate --dry-run

# Run migrations manually
marketpipe migrate --verbose

# Reset database if migrations are stuck
marketpipe reset --database --confirm
marketpipe migrate

# Check database permissions
ls -la marketpipe.db
chmod 664 marketpipe.db
```

### Database Lock Errors

**Problem**: Database locked or busy

**Solutions**:

```bash
# Check for running processes
ps aux | grep marketpipe

# Kill any stuck processes
pkill -f marketpipe

# Clear SQLite locks
fuser marketpipe.db  # Shows processes using the database
# If needed: fuser -k marketpipe.db

# Use WAL mode for SQLite (allows concurrent access)
# config.yaml:
database:
  url: "sqlite:///marketpipe.db?check_same_thread=False"
  connect_args:
    pragmas:
      journal_mode: WAL
```

## Performance Issues

### Slow Ingestion

**Problem**: Data ingestion is very slow

**Solutions**:

```bash
# Monitor performance metrics
marketpipe metrics --port 8000 &
# Open http://localhost:8000/metrics

# Increase worker count
marketpipe ingest --workers 8 --symbol AAPL

# Use faster compression
# config.yaml:
output:
  compression: "snappy"  # Faster than zstd

# Process symbols in parallel
marketpipe ingest \
  --symbol AAPL --symbol GOOGL --symbol MSFT \
  --workers 6

# Check system resources
marketpipe health-check --system --verbose
```

### High Memory Usage

**Problem**: MarketPipe consuming too much memory

**Solutions**:

```bash
# Monitor memory usage
marketpipe health-check --system

# Reduce batch size
# config.yaml:
performance:
  chunk_size: 500      # Reduced from default
  batch_size: 50       # Smaller batches

# Process fewer symbols at once
marketpipe ingest --symbol AAPL --workers 2

# Clear metrics cache
export PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
rm -rf $PROMETHEUS_MULTIPROC_DIR/*
```

### Disk Space Issues

**Problem**: Running out of disk space

**Solutions**:

```bash
# Check disk usage
marketpipe health-check --disk-space
df -h ./data/

# Prune old data
marketpipe prune --older-than 365d --confirm

# Use better compression
# config.yaml:
output:
  compression: "zstd"  # Better compression ratio

# Move data to different location
# config.yaml:
output:
  path: "/larger/disk/marketpipe/data"
```

## Data Quality Issues

### Validation Errors

**Problem**: Data failing validation

**Solutions**:

```bash
# Run comprehensive validation
marketpipe validate --symbol AAPL --comprehensive

# Check validation rules
marketpipe validate --show-rules

# Disable strict validation temporarily
marketpipe ingest --symbol AAPL --validation-strict=false

# Check specific validation errors
MARKETPIPE_LOG_LEVEL=DEBUG marketpipe validate --symbol AAPL
```

### Missing Data

**Problem**: Expected data is missing

**Solutions**:

```bash
# Check market hours and holidays
marketpipe query --sql "SELECT date, COUNT(*) as bars FROM bars WHERE symbol='AAPL' GROUP BY date ORDER BY date"

# Verify symbol exists
marketpipe symbols info AAPL

# Check provider data availability
marketpipe providers test alpaca --symbol AAPL --start 2024-01-02

# Look for gaps in data
marketpipe query --sql "
  SELECT symbol, date, COUNT(*) as bar_count
  FROM bars
  WHERE symbol = 'AAPL'
  GROUP BY symbol, date
  HAVING bar_count < 390  -- Expected bars per trading day
  ORDER BY date
"
```

### Data Inconsistencies

**Problem**: Data seems incorrect or inconsistent

**Solutions**:

```bash
# Check OHLC consistency
marketpipe validate --symbol AAPL --rules ohlc_consistency

# Compare with external source
marketpipe query --sql "SELECT * FROM bars WHERE symbol='AAPL' AND date='2024-01-02' LIMIT 10"

# Check for duplicates
marketpipe query --sql "
  SELECT symbol, timestamp, COUNT(*) as duplicate_count
  FROM bars
  WHERE symbol = 'AAPL'
  GROUP BY symbol, timestamp
  HAVING duplicate_count > 1
"

# Rebuild data from scratch
marketpipe reset --data --symbol AAPL --confirm
marketpipe ingest --symbol AAPL --start 2024-01-02 --end 2024-01-02
```

## Monitoring Issues

### Metrics Not Appearing

**Problem**: Prometheus metrics not available

**Solutions**:

```bash
# Check metrics server is running
curl http://localhost:8000/metrics

# Start metrics server explicitly
marketpipe metrics --port 8000

# Check multiprocess directory
export PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
ls -la $PROMETHEUS_MULTIPROC_DIR

# Clear and restart metrics
rm -rf $PROMETHEUS_MULTIPROC_DIR/*
marketpipe metrics restart --port 8000
```

### Log Files Not Created

**Problem**: Log files are missing or empty

**Solutions**:

```bash
# Check log directory permissions
ls -la logs/
mkdir -p logs
chmod 755 logs/

# Enable file logging in configuration
# config.yaml:
logging:
  file:
    enabled: true
    path: "logs/marketpipe.log"

# Check log level
export MARKETPIPE_LOG_LEVEL=INFO
marketpipe ingest --symbol AAPL

# Force log output
marketpipe --log-level DEBUG health-check
```

## CLI Command Issues

### Command Arguments Not Working

**Problem**: CLI arguments not being recognized

**Solutions**:

```bash
# Check command syntax
marketpipe ingest --help

# Use quotes for complex arguments
marketpipe ingest --symbol "AAPL" --start "2024-01-02"

# Use configuration file for complex setups
marketpipe ingest --config config.yaml

# Check for shell expansion issues
marketpipe ingest --symbol AAPL --start $(date -d "yesterday" +%Y-%m-%d)
```

### Output Issues

**Problem**: Command output is confusing or missing

**Solutions**:

```bash
# Use verbose mode for more information
marketpipe ingest --verbose --symbol AAPL

# Use quiet mode to reduce noise
marketpipe ingest --quiet --symbol AAPL

# Format output as JSON for parsing
marketpipe query --symbol AAPL --format json --limit 10

# Redirect output to file
marketpipe query --symbol AAPL > data.csv
```

## Advanced Debugging

### Enable Debug Logging

```bash
# Set debug level
export MARKETPIPE_LOG_LEVEL=DEBUG

# Enable SQL query logging
# config.yaml:
database:
  echo: true  # Log all SQL queries

# Enable HTTP request logging
# config.yaml:
logging:
  level: DEBUG
  loggers:
    httpx: DEBUG
    urllib3: DEBUG
```

### Trace Execution

```bash
# Use Python tracer
python -m trace --trace -m marketpipe ingest --symbol AAPL

# Profile performance
python -m cProfile -o profile.stats -m marketpipe ingest --symbol AAPL
python -c "import pstats; pstats.Stats('profile.stats').sort_stats('cumulative').print_stats(20)"

# Memory profiling
pip install memory-profiler
python -m memory_profiler -m marketpipe ingest --symbol AAPL
```

### Network Debugging

```bash
# Monitor network traffic
sudo tcpdump -i any host data.alpaca.markets

# Use verbose curl for API testing
curl -v -H "APCA-API-KEY-ID: $ALPACA_KEY" \
      -H "APCA-API-SECRET-KEY: $ALPACA_SECRET" \
      "https://data.alpaca.markets/v2/stocks/bars?symbols=AAPL&start=2024-01-02T00:00:00Z&end=2024-01-02T23:59:59Z&timeframe=1Min"

# Check SSL/TLS issues
openssl s_client -connect data.alpaca.markets:443
```

## Getting Help

### Collect Diagnostic Information

```bash
#!/bin/bash
# diagnostics.sh - Collect troubleshooting information

echo "=== MarketPipe Diagnostics ==="
echo "Date: $(date)"
echo

echo "=== System Information ==="
python --version
pip show marketpipe
uname -a
echo

echo "=== Configuration ==="
marketpipe config show --resolved
echo

echo "=== Health Check ==="
marketpipe health-check --verbose
echo

echo "=== Recent Logs (last 50 lines) ==="
tail -50 logs/marketpipe.log
echo

echo "=== Disk Space ==="
df -h
echo

echo "=== Environment Variables ==="
env | grep MARKETPIPE | sed 's/=.*/=***/'  # Hide sensitive values
env | grep ALPACA | sed 's/=.*/=***/'
echo

echo "=== Process Information ==="
ps aux | grep marketpipe
```

### Support Channels

- **GitHub Issues**: [Report bugs with diagnostic info](https://github.com/yourorg/marketpipe/issues)
- **GitHub Discussions**: [Ask questions and share solutions](https://github.com/yourorg/marketpipe/discussions)
- **Documentation**: [Complete documentation](../README.md)
- **Health Check**: Always run `marketpipe health-check --verbose` before reporting issues

### Reporting Issues

When reporting issues, include:

1. **MarketPipe version**: `marketpipe --version`
2. **Python version**: `python --version`
3. **Operating system**: `uname -a`
4. **Full error message**: Copy complete error output
5. **Configuration**: `marketpipe config show --resolved` (redact secrets)
6. **Health check**: `marketpipe health-check --verbose`
7. **Steps to reproduce**: Exact commands that trigger the issue

## Common Solutions Summary

| Issue | Quick Fix | Command |
|-------|-----------|---------|
| Command not found | Add to PATH | `export PATH="$PATH:$(python -m site --user-base)/bin"` |
| Permission denied | Fix permissions | `chmod 755 ./data ./logs` |
| 401/403 errors | Check credentials | `echo $ALPACA_KEY && curl -H "APCA-API-KEY-ID: $ALPACA_KEY" ...` |
| Rate limiting | Reduce rate | `marketpipe ingest --workers 1` |
| Slow performance | Increase workers | `marketpipe ingest --workers 8` |
| Memory issues | Reduce batch size | Set `chunk_size: 500` in config |
| Database locked | Kill processes | `pkill -f marketpipe` |
| Missing data | Check market hours | `marketpipe providers info alpaca` |
| Validation errors | Check rules | `marketpipe validate --show-rules` |
| No metrics | Start server | `marketpipe metrics --port 8000` |

---

*Last updated: 2024-01-20*
