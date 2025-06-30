# MarketPipe Environment Variables Reference

This document provides comprehensive documentation for all environment variables used in MarketPipe.

## Quick Setup

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Fill in your API credentials and configuration values
3. Never commit your `.env` file to version control

---

## Data Provider Authentication

### Immediate Target Providers

These providers are prioritized for current implementation.

#### Alpaca Markets
- **`ALPACA_KEY`** - Your Alpaca API key ID (required)
- **`ALPACA_SECRET`** - Your Alpaca API secret key (required)
- **Purpose**: US equity market data via IEX feed (free tier) or SIP feed (paid)  
- **Get credentials**: https://alpaca.markets/
- **Usage**: Paper trading account provides free market data access

#### IEX Cloud
- **`IEX_TOKEN`** (Legacy) - IEX Cloud API token
- **`MP_IEX_PUB_TOKEN`** - IEX Cloud publishable token (standardized naming)
- **`MP_IEX_SECRET_TOKEN`** - IEX Cloud secret token (standardized naming)
- **Purpose**: Real-time US equity data and deep book
- **Get credentials**: https://iexcloud.io/console/tokens
- **Usage**: Provides real-time quotes, trades, and market data

#### Cryptocurrency Providers

##### Binance
- **`MP_BINANCE_API_KEY`** - Binance API key
- **`MP_BINANCE_API_SECRET`** - Binance API secret  
- **Purpose**: Spot crypto market data from world's largest exchange
- **Get credentials**: https://www.binance.com/en/my/settings/api-management
- **Usage**: Public market data (no auth needed), private endpoints require key/secret

#### Financial Data Providers

##### Finnhub
- **`MP_FINNHUB_API_KEY`** - Finnhub API key
- **Purpose**: Equities, fundamentals, crypto data with generous free tier
- **Get credentials**: https://finnhub.io/dashboard
- **Usage**: 1-minute bars, company news, fundamentals, alternative data

##### Polygon
- **`MP_POLYGON_API_KEY`** - Polygon.io API key
- **Purpose**: Tick-level US equities and options data with fast WebSockets
- **Get credentials**: https://polygon.io/dashboard/api-keys  
- **Usage**: High-frequency tick data, aggregates, options chains

##### Tiingo
- **`MP_TIINGO_API_KEY`** - Tiingo API key
- **Purpose**: High-quality US equities data and financial news
- **Get credentials**: https://api.tiingo.com/account/token
- **Usage**: End-of-day prices, intraday data, fundamentals, news feeds

##### Twelve Data
- **`MP_TWELVEDATA_API_KEY`** - Twelve Data API key
- **Purpose**: Global equities and forex data with 1-minute granularity
- **Get credentials**: https://twelvedata.com/account/api
- **Usage**: International market coverage, generous free tier limits

#### Economic Data

##### FRED (Federal Reserve Economic Data)
- **No credentials required**
- **Purpose**: US economic indicators for risk modeling and analysis
- **Usage**: GDP, inflation, employment, interest rates, and other macro indicators

---

### Backlog Providers

These providers are planned for future implementation.

#### Alpha Vantage
- **`MP_ALPHAVANTAGE_API_KEY`** - Alpha Vantage API key
- **Purpose**: Free tier with broad asset class coverage globally
- **Get credentials**: https://www.alphavantage.co/support/#api-key

#### CME DataMine
- **`MP_CME_DATAMINE_USERNAME`** - CME DataMine username
- **`MP_CME_DATAMINE_PASSWORD`** - CME DataMine password
- **Purpose**: Chicago Mercantile Exchange derivatives data
- **Get credentials**: https://datamine.cmegroup.com/

#### Coinbase
- **`MP_COINBASE_API_KEY`** - Coinbase Pro API key
- **`MP_COINBASE_API_SECRET`** - Coinbase Pro API secret
- **Purpose**: Major US cryptocurrency exchange data
- **Get credentials**: https://pro.coinbase.com/profile/api

#### EODHD
- **`MP_EODHD_API_KEY`** - EODHD API key
- **Purpose**: End-of-day historical data for global markets
- **Get credentials**: https://eodhistoricaldata.com/

#### Exegy
- **`MP_EXEGY_API_KEY`** - Exegy API key
- **Purpose**: Real-time market data feed for professional use
- **Get credentials**: Contact Exegy directly

#### Intrinio
- **`MP_INTRINIO_API_KEY`** - Intrinio API key
- **Purpose**: Professional financial data platform
- **Get credentials**: https://intrinio.com/

#### Kraken
- **`MP_KRAKEN_API_KEY`** - Kraken API key
- **`MP_KRAKEN_API_SECRET`** - Kraken API secret
- **Purpose**: European cryptocurrency exchange data
- **Get credentials**: https://www.kraken.com/features/api

#### MarketStack
- **`MP_MARKETSTACK_API_KEY`** - MarketStack API key
- **Purpose**: Real-time and historical stock market data
- **Get credentials**: https://marketstack.com/

#### Quandl
- **`MP_QUANDL_API_KEY`** - Quandl API key
- **Purpose**: Financial and economic datasets
- **Get credentials**: https://www.quandl.com/tools/api

#### Refinitiv
- **`MP_REFINITIV_APP_KEY`** - Refinitiv application key
- **Purpose**: Professional market data (formerly Thomson Reuters)
- **Get credentials**: Contact Refinitiv directly

#### Tradier
- **`MP_TRADIER_API_KEY`** - Tradier API key
- **`MP_TRADIER_API_SECRET`** - Tradier API secret
- **Purpose**: Brokerage API with comprehensive market data
- **Get credentials**: https://developer.tradier.com/

#### Yahoo Finance
- **No credentials required** for basic usage
- **Purpose**: Free market data with rate limiting considerations
- **Usage**: Use with caution in production due to unofficial API status

---

## System Configuration

### Database Configuration

#### Primary Database
- **`DATABASE_URL`** - Main application database connection string
- **Default**: `sqlite:///data/db/marketpipe.db`
- **Purpose**: Stores application metadata, configurations, and state
- **Format**: Standard database URLs (sqlite://, postgresql://, mysql://)

#### Data Warehouse
- **`MP_DB`** - DuckDB warehouse database path
- **Default**: `data/db/warehouse.duckdb`
- **Purpose**: OLAP queries and analytics on time-series data
- **Usage**: DuckDB provides fast analytical queries on Parquet files

#### Metrics Database
- **`METRICS_DB_PATH`** - SQLite database for metrics storage
- **Default**: `data/db/metrics.db`
- **Purpose**: Local metrics persistence and historical tracking
- **Usage**: Complements Prometheus for local development metrics

### Data Storage

#### Data Directories
- **`MP_DATA_DIR`** - Root directory for all MarketPipe data files
- **Default**: `./data`
- **Purpose**: Base path for Parquet files, state, and temporary data
- **Structure**: `{MP_DATA_DIR}/parquet/symbol={SYMBOL}/date={DATE}/`

- **`DATA_DIR`** - Specific data warehouse directory for symbols
- **Default**: `./data/warehouse/symbols_master`
- **Purpose**: Storage location for processed symbol master data
- **Usage**: Used by normalizer for symbol data warehouse output

### Monitoring and Observability

#### Prometheus Metrics
- **`PROMETHEUS_PORT`** - Port for Prometheus metrics HTTP server
- **Default**: `8000`
- **Purpose**: Expose metrics for scraping by Prometheus
- **Endpoint**: `http://localhost:{PROMETHEUS_PORT}/metrics`

- **`PROMETHEUS_MULTIPROC_DIR`** - Directory for multiprocess Prometheus metrics
- **Purpose**: Shared directory for metrics collection across multiple processes
- **Usage**: Required when running MarketPipe with multiple worker processes
- **Example**: `/tmp/prometheus_multiproc`

#### Metrics Configuration
- **`METRICS_ENABLED`** - Enable/disable metrics collection
- **Default**: `true`
- **Purpose**: Global toggle for metrics collection
- **Values**: "true", "false", "1", "0"

- **`METRICS_PORT`** - HTTP server port for metrics endpoint
- **Default**: `8000`
- **Purpose**: Alternative port configuration for metrics server
- **Usage**: Override `PROMETHEUS_PORT` in some contexts

- **`METRICS_MAX_CONNECTIONS`** - Maximum concurrent connections to metrics server
- **Default**: `100`
- **Purpose**: Limit concurrent connections to prevent resource exhaustion
- **Usage**: Tune based on monitoring infrastructure capacity

- **`METRICS_MAX_HEADER_SIZE`** - Maximum HTTP header size for metrics requests
- **Default**: `16384` (16KB)
- **Purpose**: Prevent large header attacks on metrics endpoint
- **Usage**: Increase if legitimate requests have large headers

#### Metrics Control
- **`MP_DISABLE_SQLITE_METRICS`** - Disable SQLite-specific metrics
- **Purpose**: Skip SQLite metrics collection (useful when using PostgreSQL)
- **Values**: Set to any non-empty value to disable
- **Usage**: Reduces overhead when SQLite metrics aren't needed

### Security and Logging

#### Logging Configuration  
- **`LOG_LEVEL`** - Application logging level
- **Default**: `INFO`
- **Purpose**: Control verbosity of application logs
- **Values**: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

#### Security Settings
- **`MASK_SECRETS`** - Enable/disable secret masking in logs
- **Default**: `true`
- **Purpose**: Prevent API keys and secrets from appearing in logs
- **Values**: "true", "false", "1", "0"

### Development and Testing

#### General Testing
- **`TESTING_MODE`** - Enable testing mode with relaxed validation
- **Default**: `false`
- **Purpose**: Disable certain production checks during testing
- **Values**: "true", "false", "1", "0"

- **`TEST_API_KEY`** - Generic API key for testing authentication flows
- **Purpose**: Test credential masking and authentication without real API keys
- **Usage**: Used in security tests to verify secret handling

#### PostgreSQL Testing
- **`TEST_POSTGRES`** - Enable PostgreSQL-specific tests
- **Purpose**: Run tests that require PostgreSQL database
- **Values**: Set to any non-empty value to enable
- **Usage**: Skips PostgreSQL tests when not set (e.g., CI without PostgreSQL)

- **`POSTGRES_TEST_DSN`** - PostgreSQL connection string for tests  
- **Default**: `postgresql://marketpipe:password@localhost:5433/marketpipe_test`
- **Purpose**: Test database connection for PostgreSQL integration tests
- **Format**: Standard PostgreSQL connection string

- **`POSTGRES_TEST_URL`** - Alternative PostgreSQL connection for tests
- **Default**: `postgresql://marketpipe:password@localhost:5433/marketpipe`
- **Purpose**: Secondary test database connection
- **Usage**: Used for different test scenarios or database migration tests

---

## Environment Variable Naming Conventions

### Provider Credentials
- **Pattern**: `MP_{PROVIDER}_{CREDENTIAL_TYPE}`
- **Examples**: `MP_POLYGON_API_KEY`, `MP_IEX_SECRET_TOKEN`
- **Rules**:
  - All provider variables start with `MP_` prefix
  - Provider name in uppercase
  - Credential type clearly specified
  - Underscores separate words

### Legacy Variables
Some providers have legacy variable names (without `MP_` prefix):
- `ALPACA_KEY` / `ALPACA_SECRET` - Maintained for backward compatibility
- `IEX_TOKEN` - Legacy form of IEX authentication

### System Variables
- **Pattern**: Descriptive names without provider prefix
- **Examples**: `DATABASE_URL`, `LOG_LEVEL`, `METRICS_ENABLED`
- **Rules**:
  - Clear, descriptive names
  - Consistent with common environment variable conventions
  - Grouped by functional area

---

## Security Best Practices

### Secrets Management
1. **Never commit secrets**: Keep your `.env` file out of version control
2. **Use environment-specific files**: `.env.development`, `.env.production`
3. **Rotate credentials regularly**: Update API keys periodically
4. **Use least privilege**: Request minimal permissions from API providers
5. **Monitor usage**: Track API key usage for suspicious activity

### Production Deployment
1. **Use secret management systems**: AWS Secrets Manager, Kubernetes secrets, etc.
2. **Enable secret masking**: Set `MASK_SECRETS=true` in production
3. **Secure metrics endpoints**: Restrict access to metrics endpoints
4. **Use encrypted connections**: HTTPS for all external API calls
5. **Audit access**: Log and monitor environment variable access

### Development Security
1. **Separate credentials**: Use different API keys for development and production
2. **Test with fake providers**: Use `fake` provider for development when possible
3. **Review logs**: Ensure no secrets leak into development logs
4. **Share safely**: Use secure channels for sharing development credentials

---

## Troubleshooting

### Common Issues

#### Missing Environment Variables
```bash
# Check which variables are set
env | grep -E "(MP_|ALPACA_|IEX_|POSTGRES_|METRICS_)" | sort

# Verify .env file loading
marketpipe config --show-env
```

#### Database Connection Issues
```bash
# Test database connectivity
marketpipe db --test-connection

# Check database URL format
echo $DATABASE_URL
```

#### API Authentication Issues
```bash
# Test provider authentication
marketpipe test-auth --provider alpaca
marketpipe test-auth --provider iex
```

#### Metrics Collection Issues
```bash
# Check metrics endpoint
curl http://localhost:${PROMETHEUS_PORT}/metrics

# Verify multiprocess metrics directory
ls -la $PROMETHEUS_MULTIPROC_DIR
```

### Getting Help
1. Check the logs: `tail -f logs/marketpipe.log`
2. Verify configuration: `marketpipe config --validate`
3. Test connectivity: `marketpipe test --provider <provider>`
4. Review documentation: `docs/` directory
5. Open an issue: GitHub issues for bugs and questions

---

## Configuration Examples

### Minimal Development Setup
```bash
# .env.development
LOG_LEVEL=DEBUG
TESTING_MODE=true
METRICS_ENABLED=false
MP_DATA_DIR=./dev-data
```

### Production Setup
```bash
# .env.production  
LOG_LEVEL=INFO
MASK_SECRETS=true
METRICS_ENABLED=true
PROMETHEUS_PORT=8000
DATABASE_URL=postgresql://user:pass@db:5432/marketpipe
MP_DATA_DIR=/data/marketpipe
```

### Testing Setup
```bash
# .env.test
TESTING_MODE=true
TEST_POSTGRES=1
POSTGRES_TEST_DSN=postgresql://test:test@localhost:5433/test_db
LOG_LEVEL=WARNING
METRICS_ENABLED=false
```

This comprehensive guide covers all environment variables used in MarketPipe. Keep this documentation updated as new variables are added to the codebase. 