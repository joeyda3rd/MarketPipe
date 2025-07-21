# Configuration Guide

This guide covers all aspects of MarketPipe configuration, from basic setup to advanced customization. MarketPipe uses YAML configuration files, environment variables, and command-line arguments with a clear precedence hierarchy.

## Configuration Hierarchy

Configuration is applied in this order (later sources override earlier ones):

1. **Default values** - Built-in defaults
2. **Configuration files** - YAML files specified with `--config`
3. **Environment variables** - `MARKETPIPE_*` and provider-specific variables
4. **Command-line arguments** - Direct CLI flags

## Basic Configuration

### Quick Setup

Generate a starter configuration:

```bash
# Create basic configuration
marketpipe config generate --provider alpaca --output config.yaml

# Create environment file
marketpipe env generate --provider alpaca
```

### Minimal Configuration

```yaml
# config.yaml - Basic setup
providers:
  alpaca:
    feed: "iex"  # Free tier
    rate_limit_per_min: 200

symbols:
  - AAPL
  - GOOGL
  - MSFT

start: "2024-01-02"
end: "2024-01-05"

output:
  path: "./data"
  compression: "snappy"
```

## Provider Configuration

### Alpaca Markets

```yaml
providers:
  alpaca:
    # Credentials (loaded from environment)
    key: ${ALPACA_KEY}
    secret: ${ALPACA_SECRET}

    # API settings
    base_url: "https://data.alpaca.markets/v2"
    paper_url: "https://paper-api.alpaca.markets"
    feed: "iex"  # "iex" (free) or "sip" (paid)

    # Rate limiting
    rate_limit_per_min: 200
    timeout: 30.0
    max_retries: 3

    # Data settings
    session: "regular"  # "regular", "extended", "all"
    adjustment: "raw"   # "raw", "split", "dividend", "all"
```

### IEX Cloud

```yaml
providers:
  iex:
    token: ${IEX_TOKEN}
    base_url: "https://cloud.iexapis.com/stable"
    sandbox: false
    rate_limit_per_min: 100
    timeout: 30.0
```

### Custom Provider

```yaml
providers:
  custom:
    name: "CustomProvider"
    base_url: ${CUSTOM_API_URL}
    api_key: ${CUSTOM_API_KEY}
    headers:
      User-Agent: "MarketPipe/1.0"
      Authorization: "Bearer ${CUSTOM_API_KEY}"
    rate_limit_per_min: 60
```

## Environment Variables

### Provider Credentials

```bash
# Alpaca Markets
ALPACA_KEY=AKFZ...              # Alpaca API key
ALPACA_SECRET=abc123...         # Alpaca secret key

# IEX Cloud
IEX_TOKEN=pk_live_...           # IEX Cloud token

# Polygon.io
POLYGON_API_KEY=xyz789...       # Polygon API key
```

### MarketPipe Settings

```bash
# Core settings
MARKETPIPE_LOG_LEVEL=INFO       # DEBUG, INFO, WARNING, ERROR
MARKETPIPE_OUTPUT_PATH=./data   # Default data directory
MARKETPIPE_CONFIG_PATH=./config.yaml  # Default config file

# Database settings
MARKETPIPE_DB_URL=sqlite:///marketpipe.db  # Database URL
MARKETPIPE_DB_POOL_SIZE=5       # Connection pool size

# Performance settings
MARKETPIPE_WORKERS=3            # Default worker count
MARKETPIPE_CHUNK_SIZE=1000      # Default chunk size
MARKETPIPE_BATCH_SIZE=100       # Batch processing size

# Monitoring
MARKETPIPE_METRICS_PORT=8000    # Metrics server port
MARKETPIPE_METRICS_ENABLED=true # Enable metrics collection
```

### Provider-Specific Settings

```bash
# Alpaca-specific
ALPACA_FEED=iex                 # Default feed ("iex" or "sip")
ALPACA_RATE_LIMIT=200           # Requests per minute
ALPACA_TIMEOUT=30.0             # Request timeout

# IEX-specific
IEX_SANDBOX=false               # Use sandbox environment
IEX_VERSION=stable              # API version
```

## Data Configuration

### Symbols and Time Ranges

```yaml
# Static symbol list
symbols:
  - AAPL
  - GOOGL
  - MSFT
  - TSLA

# Dynamic symbol list from file
symbols_file: "symbols.txt"

# Date ranges
start: "2024-01-01"
end: "2024-12-31"

# Or relative dates
start: "-30d"  # 30 days ago
end: "today"

# Multiple date ranges
date_ranges:
  - start: "2024-01-01"
    end: "2024-01-31"
  - start: "2024-06-01"
    end: "2024-06-30"
```

### Data Processing

```yaml
processing:
  # Validation settings
  validation:
    enabled: true
    strict: false  # Continue on validation errors
    rules: "schema/custom_rules.json"

  # Aggregation settings
  aggregation:
    timeframes: ["1m", "5m", "15m", "1h", "1d"]
    functions:
      open: "first"
      high: "max"
      low: "min"
      close: "last"
      volume: "sum"

  # Deduplication
  deduplication:
    enabled: true
    key_fields: ["symbol", "timestamp"]
    strategy: "keep_latest"
```

### Output Configuration

```yaml
output:
  # Storage location
  path: "./data"

  # Partitioning strategy
  partitioning:
    by: ["symbol", "date"]  # Hive-style partitioning
    format: "symbol={symbol}/date={date}"

  # File format settings
  format: "parquet"
  compression: "snappy"  # snappy, zstd, lz4, gzip

  # Parquet-specific settings
  parquet:
    row_group_size: 50000
    use_dictionary: true
    write_statistics: true

  # File naming
  naming:
    pattern: "{symbol}_{date}_{timestamp}.parquet"
    timestamp_format: "%Y%m%d_%H%M%S"
```

## Performance Configuration

### Parallelism

```yaml
performance:
  # Worker settings
  workers: 4
  max_workers: 8

  # Memory management
  chunk_size: 1000     # Records per chunk
  batch_size: 100      # Database batch size

  # Connection settings
  connection_pool_size: 10
  max_connections: 20

  # Caching
  cache:
    enabled: true
    size: "1GB"
    ttl: 3600  # seconds
```

### Resource Limits

```yaml
limits:
  # Memory limits
  max_memory_mb: 2048
  max_row_group_size: 100000

  # Disk limits
  max_disk_usage_gb: 50
  temp_dir: "/tmp/marketpipe"

  # Network limits
  max_concurrent_requests: 10
  request_timeout: 30.0

  # Rate limiting
  global_rate_limit: 1000  # requests per minute
  backoff_factor: 2.0
  max_backoff: 60.0
```

## Monitoring and Observability

### Logging Configuration

```yaml
logging:
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL

  # File logging
  file:
    enabled: true
    path: "logs/marketpipe.log"
    max_size: "100MB"
    backup_count: 5
    rotation: "midnight"

  # Console logging
  console:
    enabled: true
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    colored: true

  # Structured logging
  structured:
    enabled: false
    format: "json"
```

### Metrics Configuration

```yaml
metrics:
  enabled: true
  port: 8000
  path: "/metrics"

  # Prometheus settings
  prometheus:
    multiprocess_mode: true
    multiprocess_dir: "/tmp/prometheus_multiproc"
    registry_file: "registry.db"

  # Custom metrics
  custom_metrics:
    - name: "data_quality_score"
      type: "gauge"
      labels: ["symbol", "provider"]
    - name: "ingestion_lag"
      type: "histogram"
      labels: ["symbol"]
```

## Advanced Configuration

### Database Configuration

```yaml
database:
  # Connection settings
  url: "postgresql://user:pass@localhost/marketpipe"
  pool_size: 10
  max_overflow: 20
  pool_timeout: 30

  # Migration settings
  auto_migrate: true
  migration_timeout: 300

  # Performance settings
  echo: false  # Log all SQL queries
  query_timeout: 60

  # Connection pool settings
  pool_pre_ping: true
  pool_recycle: 3600
```

### Security Configuration

```yaml
security:
  # API key encryption
  encryption:
    enabled: true
    key: ${ENCRYPTION_KEY}
    algorithm: "AES-256-GCM"

  # SSL/TLS settings
  tls:
    verify_ssl: true
    ca_bundle: "/etc/ssl/certs/ca-certificates.crt"
    client_cert: "/path/to/client.pem"
    client_key: "/path/to/client.key"

  # Access control
  access_control:
    allowed_ips: ["192.168.1.0/24"]
    rate_limit_by_ip: 100
    require_api_key: true
```

### Plugin Configuration

```yaml
plugins:
  # Data validators
  validators:
    - name: "ohlc_consistency"
      enabled: true
      config:
        tolerance: 0.001
    - name: "volume_anomaly"
      enabled: true
      config:
        multiplier: 10.0

  # Data transforms
  transforms:
    - name: "split_adjustment"
      enabled: true
      config:
        source: "yahoo"
    - name: "currency_conversion"
      enabled: false
      config:
        base_currency: "USD"

  # Notification plugins
  notifications:
    - name: "slack"
      enabled: true
      config:
        webhook_url: ${SLACK_WEBHOOK}
        channel: "#alerts"
```

## Configuration Validation

### Validate Configuration

```bash
# Validate configuration file
marketpipe config validate config.yaml

# Show resolved configuration (with env vars)
marketpipe config show --resolved

# Test configuration with providers
marketpipe health-check --config config.yaml
```

### Schema Validation

```yaml
# config_schema.yaml - Configuration schema
schema_version: 1
required_fields:
  - providers
  - symbols
  - start
  - end

field_types:
  symbols: list
  start: date
  end: date
  workers: integer

validation_rules:
  - field: "workers"
    min: 1
    max: 16
  - field: "symbols"
    min_length: 1
    max_length: 100
```

## Configuration Examples

### Production Configuration

```yaml
# production.yaml
providers:
  alpaca:
    feed: "sip"  # Paid feed for production
    rate_limit_per_min: 200
    timeout: 30.0
    max_retries: 5

symbols_file: "production_symbols.txt"

start: "-1d"
end: "today"

output:
  path: "/data/marketpipe"
  compression: "zstd"  # Better compression for storage

performance:
  workers: 8
  chunk_size: 5000
  batch_size: 500

logging:
  level: "WARNING"
  file:
    enabled: true
    path: "/var/log/marketpipe/marketpipe.log"

metrics:
  enabled: true
  port: 8000

database:
  url: "postgresql://marketpipe:${DB_PASSWORD}@db-server:5432/marketpipe_prod"
  pool_size: 20
```

### Development Configuration

```yaml
# development.yaml
providers:
  alpaca:
    feed: "iex"  # Free feed for development
    rate_limit_per_min: 100

  fake:
    enabled: true  # Use fake data for testing
    symbols: 50
    days: 30

symbols:
  - AAPL
  - GOOGL

start: "-7d"
end: "today"

output:
  path: "./dev_data"
  compression: "snappy"

performance:
  workers: 2
  chunk_size: 100

logging:
  level: "DEBUG"
  console:
    enabled: true
    colored: true

metrics:
  enabled: true
  port: 8001
```

### Testing Configuration

```yaml
# test.yaml
providers:
  fake:
    enabled: true
    symbols: 10
    days: 7
    volatility: 0.02

symbols:
  - TEST_SYMBOL_1
  - TEST_SYMBOL_2

start: "2024-01-01"
end: "2024-01-07"

output:
  path: "/tmp/test_data"
  compression: "snappy"

performance:
  workers: 1
  chunk_size: 50

logging:
  level: "DEBUG"
  console:
    enabled: true

validation:
  strict: true
  comprehensive: true
```

## Configuration Best Practices

### Security

- **Never commit credentials** to version control
- **Use environment variables** for all sensitive data
- **Encrypt configuration files** containing secrets
- **Rotate API keys** regularly
- **Use least privilege** for database connections

### Performance

- **Tune worker count** based on system capacity and provider limits
- **Use appropriate compression** for your storage requirements
- **Monitor resource usage** and adjust limits accordingly
- **Cache frequently accessed configuration**
- **Use connection pooling** for database connections

### Maintainability

- **Use descriptive configuration names**
- **Document custom settings** with comments
- **Validate configuration** before deployment
- **Keep environment-specific configs** separate
- **Use configuration templates** for consistency

### Monitoring

- **Enable comprehensive logging** in production
- **Set up metrics collection** for operational insight
- **Configure alerts** for configuration drift
- **Monitor configuration changes** via version control
- **Test configuration changes** in staging first

## Troubleshooting Configuration

### Common Issues

**Configuration not found:**
```bash
# Check config search paths
marketpipe config paths

# Specify config explicitly
marketpipe --config /path/to/config.yaml ingest
```

**Environment variables not loading:**
```bash
# Verify .env file location and content
cat .env

# Check variable expansion
marketpipe config show --resolved
```

**Provider authentication fails:**
```bash
# Test credentials
marketpipe health-check --provider alpaca --verbose

# Check credential format
echo $ALPACA_KEY | wc -c  # Should be expected length
```

### Debug Mode

Enable debug logging for configuration troubleshooting:

```bash
# Debug configuration loading
MARKETPIPE_LOG_LEVEL=DEBUG marketpipe --config config.yaml health-check

# Debug provider configuration
MARKETPIPE_LOG_LEVEL=DEBUG marketpipe providers test alpaca
```

## Next Steps

- **Monitoring Setup**: Configure observability in [Monitoring Guide](monitoring.md)
- **CLI Usage**: Learn command patterns in [CLI Usage Guide](cli_usage.md)
- **Troubleshooting**: Resolve issues with [Troubleshooting Guide](troubleshooting.md)

---

*Last updated: 2024-01-20*
