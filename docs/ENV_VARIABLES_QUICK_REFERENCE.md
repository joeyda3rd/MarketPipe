# Environment Variables Quick Reference

This is a quick reference table for all MarketPipe environment variables. For detailed documentation, see [ENVIRONMENT_VARIABLES.md](ENVIRONMENT_VARIABLES.md).

## Data Provider Credentials

| Variable | Provider | Type | Required | Description |
|----------|----------|------|----------|-------------|
| `ALPACA_KEY` | Alpaca | API Key | Yes | Alpaca API key ID |
| `ALPACA_SECRET` | Alpaca | Secret | Yes | Alpaca API secret key |
| `IEX_TOKEN` | IEX Cloud | Token | Yes | IEX Cloud API token (legacy) |
| `MP_IEX_PUB_TOKEN` | IEX Cloud | Token | Yes | IEX Cloud publishable token |
| `MP_IEX_SECRET_TOKEN` | IEX Cloud | Secret | Yes | IEX Cloud secret token |
| `MP_BINANCE_API_KEY` | Binance | API Key | Optional | Binance API key |
| `MP_BINANCE_API_SECRET` | Binance | Secret | Optional | Binance API secret |
| `MP_FINNHUB_API_KEY` | Finnhub | API Key | Optional | Finnhub API key |
| `MP_POLYGON_API_KEY` | Polygon | API Key | Optional | Polygon.io API key |
| `MP_TIINGO_API_KEY` | Tiingo | API Key | Optional | Tiingo API key |
| `MP_TWELVEDATA_API_KEY` | Twelve Data | API Key | Optional | Twelve Data API key |
| `MP_ALPHAVANTAGE_API_KEY` | Alpha Vantage | API Key | Optional | Alpha Vantage API key |
| `MP_CME_DATAMINE_USERNAME` | CME DataMine | Username | Optional | CME DataMine username |
| `MP_CME_DATAMINE_PASSWORD` | CME DataMine | Password | Optional | CME DataMine password |
| `MP_COINBASE_API_KEY` | Coinbase | API Key | Optional | Coinbase Pro API key |
| `MP_COINBASE_API_SECRET` | Coinbase | Secret | Optional | Coinbase Pro API secret |
| `MP_EODHD_API_KEY` | EODHD | API Key | Optional | EODHD API key |
| `MP_EXEGY_API_KEY` | Exegy | API Key | Optional | Exegy API key |
| `MP_INTRINIO_API_KEY` | Intrinio | API Key | Optional | Intrinio API key |
| `MP_KRAKEN_API_KEY` | Kraken | API Key | Optional | Kraken API key |
| `MP_KRAKEN_API_SECRET` | Kraken | Secret | Optional | Kraken API secret |
| `MP_MARKETSTACK_API_KEY` | MarketStack | API Key | Optional | MarketStack API key |
| `MP_QUANDL_API_KEY` | Quandl | API Key | Optional | Quandl API key |
| `MP_REFINITIV_APP_KEY` | Refinitiv | App Key | Optional | Refinitiv application key |
| `MP_TRADIER_API_KEY` | Tradier | API Key | Optional | Tradier API key |
| `MP_TRADIER_API_SECRET` | Tradier | Secret | Optional | Tradier API secret |

## System Configuration

| Variable | Category | Type | Default | Description |
|----------|----------|------|---------|-------------|
| `DATABASE_URL` | Database | URL | `sqlite:///data/db/marketpipe.db` | Main database connection |
| `MP_DB` | Database | Path | `data/db/warehouse.duckdb` | DuckDB warehouse path |
| `METRICS_DB_PATH` | Database | Path | `data/db/metrics.db` | Metrics database path |
| `MP_DATA_DIR` | Storage | Path | `./data` | Root data directory |
| `DATA_DIR` | Storage | Path | `./data/warehouse/symbols_master` | Symbol warehouse directory |
| `PROMETHEUS_PORT` | Metrics | Port | `8000` | Prometheus metrics port |
| `PROMETHEUS_MULTIPROC_DIR` | Metrics | Path | - | Multiprocess metrics directory |
| `METRICS_ENABLED` | Metrics | Boolean | `true` | Enable metrics collection |
| `METRICS_PORT` | Metrics | Port | `8000` | Metrics server port |
| `METRICS_MAX_CONNECTIONS` | Metrics | Integer | `100` | Max concurrent connections |
| `METRICS_MAX_HEADER_SIZE` | Metrics | Integer | `16384` | Max HTTP header size |
| `MP_DISABLE_SQLITE_METRICS` | Metrics | Flag | - | Disable SQLite metrics |
| `LOG_LEVEL` | Logging | Level | `INFO` | Application log level |
| `MASK_SECRETS` | Security | Boolean | `true` | Mask secrets in logs |
| `TESTING_MODE` | Testing | Boolean | `false` | Enable testing mode |
| `TEST_API_KEY` | Testing | String | - | Generic test API key |
| `TEST_POSTGRES` | Testing | Flag | - | Enable PostgreSQL tests |
| `POSTGRES_TEST_DSN` | Testing | URL | `postgresql://marketpipe:password@localhost:5433/marketpipe_test` | Test DB connection |
| `POSTGRES_TEST_URL` | Testing | URL | `postgresql://marketpipe:password@localhost:5433/marketpipe` | Alternative test DB |

## Quick Setup Commands

```bash
# Copy template
cp .env.example .env

# Set up minimal development environment
echo "LOG_LEVEL=DEBUG" >> .env
echo "TESTING_MODE=true" >> .env
echo "METRICS_ENABLED=false" >> .env

# Add your provider credentials
echo "ALPACA_KEY=your_key_here" >> .env
echo "ALPACA_SECRET=your_secret_here" >> .env

# Verify configuration
marketpipe config --validate
```

## Environment Checks

```bash
# Check all MarketPipe environment variables
env | grep -E "(MP_|ALPACA_|IEX_|POSTGRES_|METRICS_)" | sort

# Check specific provider credentials
env | grep -E "^(ALPACA_|MP_POLYGON_|MP_FINNHUB_)"

# Verify database connections
echo $DATABASE_URL
echo $MP_DB
```

For complete details on each variable, usage examples, and troubleshooting, see the full [Environment Variables Documentation](ENVIRONMENT_VARIABLES.md). 