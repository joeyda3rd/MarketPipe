# Getting Started with MarketPipe

This guide gets you up and running with MarketPipe in under 5 minutes. Follow along to install the software, configure your first data source, and run your first market data ingestion.

## Prerequisites

- **Python 3.9+** - Check with `python --version`
- **Internet connection** - For downloading packages and market data
- **Market data provider account** - Free tiers available (Alpaca Markets recommended for beginners)

## Installation

### Quick Install

For most users, pip installation is the fastest way to get started:

```bash
# Install MarketPipe
pip install marketpipe

# Verify installation
marketpipe --version
marketpipe --help
```

### Development Install

If you plan to contribute or modify MarketPipe:

```bash
# Clone repository
git clone https://github.com/yourorg/marketpipe.git
cd marketpipe

# Install in development mode
pip install -e '.[dev]'

# Set up pre-commit hooks
pre-commit install

# Run health check
scripts/health-check
```

## First Run

### 1. Set Up Credentials

MarketPipe needs API credentials to fetch market data. We'll use Alpaca Markets (free tier available):

1. **Create Alpaca account**: Visit [alpaca.markets](https://alpaca.markets) and sign up
2. **Generate API keys**: Go to your dashboard → API Keys → Generate
3. **Create environment file**:

```bash
# Create .env file with your credentials
echo "ALPACA_KEY=your_api_key_here" > .env
echo "ALPACA_SECRET=your_secret_here" >> .env
```

!!! warning "Keep credentials secure"
    Never commit `.env` files to version control. Add `.env` to your `.gitignore`.

### 2. Run Your First Ingestion

```bash
# Ingest one day of Apple stock data
marketpipe ingest --symbol AAPL --start 2024-01-02 --end 2024-01-02

# Check what was created
ls -la data/
marketpipe query --symbol AAPL --limit 5
```

### 3. Explore Your Data

```bash
# View ingested data
marketpipe query --symbol AAPL --limit 10

# Check data quality
marketpipe validate --symbol AAPL

# View system metrics
marketpipe metrics --port 8000  # Open http://localhost:8000/metrics
```

## Configuration

### Basic Configuration

Create a configuration file for repeated use:

```yaml
# config.yaml
symbols:
  - AAPL
  - GOOGL
  - MSFT

start: "2024-01-02"
end: "2024-01-05"

providers:
  alpaca:
    # Credentials loaded from .env automatically
    feed: "iex"  # free tier
    rate_limit_per_min: 200

output:
  path: "./data"
  compression: "snappy"

workers: 3
```

Use the configuration file:

```bash
marketpipe ingest --config config.yaml
```

### Environment Variables

Key environment variables MarketPipe recognizes:

| Variable | Description | Example |
|----------|-------------|---------|
| `ALPACA_KEY` | Alpaca API key | `AKFZ...` |
| `ALPACA_SECRET` | Alpaca API secret | `abc123...` |
| `MARKETPIPE_LOG_LEVEL` | Logging level | `INFO`, `DEBUG` |
| `MARKETPIPE_OUTPUT_PATH` | Default output directory | `./data` |

See the [Configuration Guide](user_guide/configuration.md) for complete reference.

## Common Workflows

### Daily Data Ingestion

```bash
# Set up daily ingestion for multiple symbols
marketpipe ingest \
  --symbol AAPL --symbol GOOGL --symbol MSFT \
  --start 2024-01-02 \
  --end 2024-01-02 \
  --workers 3
```

### Historical Backfill

```bash
# Backfill a week of data
marketpipe backfill \
  --symbol AAPL \
  --start 2024-01-01 \
  --end 2024-01-07 \
  --workers 5
```

### Data Aggregation

```bash
# Aggregate minute bars to 5-minute bars
marketpipe aggregate \
  --symbol AAPL \
  --timeframe 5m \
  --start 2024-01-02
```

### Monitoring and Health Checks

```bash
# Check system health
marketpipe health-check

# View ingestion status
marketpipe jobs status

# Start metrics server for Prometheus
marketpipe metrics --port 8000
```

## FAQ

### Installation Issues

**Q: "pip install marketpipe" fails with compilation errors**

A: Install Python dev headers and build tools:
```bash
# Ubuntu/Debian
sudo apt-get install python3-dev build-essential

# macOS (with Homebrew)
brew install python3

# Try installing again
pip install --no-cache-dir marketpipe
```

**Q: Getting "Command 'marketpipe' not found" after installation**

A: The Python scripts directory might not be in your PATH:
```bash
# Check if pip installs to user directory
python -m site --user-base

# Add to PATH in ~/.bashrc or ~/.zshrc
export PATH="$PATH:$(python -m site --user-base)/bin"

# Or run via Python module
python -m marketpipe --help
```

### Data Provider Issues

**Q: Getting "401 Unauthorized" errors**

A: Check your API credentials:
```bash
# Verify environment variables are loaded
marketpipe providers list
marketpipe health-check --verbose

# Test credentials directly
curl -H "APCA-API-KEY-ID: $ALPACA_KEY" \
     -H "APCA-API-SECRET-KEY: $ALPACA_SECRET" \
     https://paper-api.alpaca.markets/v2/account
```

**Q: "Rate limit exceeded" errors**

A: Reduce request rate in configuration:
```yaml
providers:
  alpaca:
    rate_limit_per_min: 100  # Reduced from default 200
    timeout: 60  # Increased timeout
```

### Performance Issues

**Q: Ingestion is very slow**

A: Try these optimizations:
```bash
# Increase parallel workers
marketpipe ingest --workers 8 --symbol AAPL

# Use compression for faster I/O
marketpipe ingest --compression zstd --symbol AAPL

# Process multiple symbols in parallel
marketpipe ingest --symbol AAPL --symbol GOOGL --workers 4
```

### Data Issues

**Q: Missing data for certain time periods**

A: Check market hours and holidays:
```bash
# Validate data completeness
marketpipe validate --symbol AAPL --start 2024-01-02 --end 2024-01-02

# Query with verbose logging
MARKETPIPE_LOG_LEVEL=DEBUG marketpipe ingest --symbol AAPL --start 2024-01-02 --end 2024-01-02
```

## Next Steps

Now that you have MarketPipe running:

1. **Learn more commands**: See [CLI Usage Guide](user_guide/cli_usage.md)
2. **Advanced configuration**: Read [Configuration Guide](user_guide/configuration.md)
3. **Set up monitoring**: Follow [Monitoring Guide](user_guide/monitoring.md)
4. **Troubleshooting**: Check [Troubleshooting Guide](user_guide/troubleshooting.md)
5. **Contributing**: Read [Developer Guide](developer_guide/contributing.md)

## Support

- **Issues**: [GitHub Issues](https://github.com/yourorg/marketpipe/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourorg/marketpipe/discussions)
- **Documentation**: [Full Documentation](README.md)

---

*Last updated: 2024-01-20*
