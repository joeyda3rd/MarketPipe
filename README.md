# MarketPipe

Financial market data ETL framework.

## What it does

Collects OHLCV data from market data providers (Alpaca, IEX) and stores it in Parquet files with DuckDB views for querying.

## Quick start

```bash
pip install -e .
marketpipe ingest --provider fake --symbols AAPL --start 2024-01-01 --end 2024-01-02
marketpipe query --symbol AAPL
```

## Providers

- `fake` - generates test data, no API keys needed
- `alpaca` - requires `ALPACA_KEY` and `ALPACA_SECRET` 
- `iex` - requires `IEX_TOKEN`

## Development

```bash
git clone <repo>
cd marketpipe
scripts/setup    # One-command setup
scripts/demo     # Quick demo
```

See `scripts/` for more development tools.

## Database Setup

MarketPipe uses Alembic for database schema management:

```bash
# Apply latest migrations
alembic upgrade head

# Create new migration
alembic revision --autogenerate -m "description"

# PostgreSQL setup (optional)
./tools/database/setup_postgres.sh
```

## Structure

- `src/marketpipe/` - source code
- `tests/` - tests
- `examples/` - usage examples
- `scripts/` - development tools
- `alembic/` - database migrations
- `tools/` - development and database tools
- `monitoring/` - Grafana dashboards and observability
- `docker/` - containerized deployment stack
- `config/` - configuration templates
- `setup.cfg` - architecture linting rules

## Monitoring

Enable metrics for production monitoring:

```bash
# Start metrics server
marketpipe metrics --port 8000

# Import Grafana dashboard
# See monitoring/README.md for setup instructions
```

## Docker Deployment

Complete containerized stack with monitoring:

```bash
# Start MarketPipe + Prometheus + Grafana
docker compose up -d

# Access services
# MarketPipe metrics: http://localhost:8000/metrics
# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000 (admin/admin)
```

See `docker/README.md` for full deployment documentation.

## Testing

```bash
# Fast development loop
make test

# Full test suite  
make test-all

# Smart test runner (runs relevant tests)
make test-smart
```

## Architecture

Domain-driven design with import boundaries enforced by import-linter:

```bash
# Check architecture compliance
import-linter --config setup.cfg
```

Configuration in `setup.cfg` ensures domain layer stays pure.

## License

Apache 2.0
