# TSLA One Year Data Pipeline

This script downloads exactly 1 year of TSLA 1-minute OHLCV data from Alpaca Markets, ending on the last full trading day before today.

## Prerequisites

### 1. Alpaca Markets Account
- Sign up for a free account at [Alpaca Markets](https://alpaca.markets/)
- Get your API key and secret from the dashboard
- Free tier includes IEX data feed (sufficient for this pipeline)

### 2. Environment Variables
Set your Alpaca credentials as environment variables:

```bash
# Option 1: Export directly
export ALPACA_KEY="your_api_key_here"
export ALPACA_SECRET="your_api_secret_here"

# Option 2: Create .env file in project root
echo "ALPACA_KEY=your_api_key_here" > .env
echo "ALPACA_SECRET=your_api_secret_here" >> .env
```

### 3. Python Dependencies
Ensure all dependencies are installed:

```bash
pip install -e .
```

## Usage

### Run the Complete Pipeline

```bash
# From the project root directory
python scripts/tsla_one_year_pipeline.py
```

The script will automatically:

1. **Calculate Date Range**: Determines exactly 1 year ending on the last trading day
2. **Data Ingestion**: Downloads ~250 trading days of 1-minute TSLA bars from Alpaca
3. **Data Validation**: Validates data quality and generates reports
4. **Data Aggregation**: Creates 5m, 15m, 1h, and 1d timeframes
5. **Summary Report**: Generates a comprehensive summary

### Expected Runtime
- **15-30 minutes** depending on your internet connection
- **~100MB-500MB** of data will be downloaded
- Progress is shown in real-time with timestamped logs

## Output Structure

After completion, you'll have:

```
data/
├── raw/                          # 1-minute OHLCV data
│   └── symbol=TSLA/
│       └── date=YYYY-MM-DD/
│           └── *.parquet
├── aggregated/                   # Multi-timeframe data  
│   ├── 5m/
│   ├── 15m/
│   ├── 1h/
│   └── 1d/
├── validation_reports/           # Data quality reports
│   └── *.csv
└── tsla_one_year_summary.json   # Pipeline summary
```

## Data Analysis

### Quick Query Examples

```bash
# Total bars downloaded
python -m marketpipe query "SELECT COUNT(*) FROM 'data/raw/**/*.parquet'"

# Date range verification  
python -m marketpipe query "SELECT MIN(timestamp), MAX(timestamp) FROM 'data/raw/**/*.parquet'"

# Daily volume summary
python -m marketpipe query "SELECT date, AVG(volume) as avg_volume FROM bars_1d"
```

### Using the Data for Signals

The data is now ready for:
- Technical analysis
- Machine learning model training
- Backtesting trading strategies
- Signal generation algorithms

### Python Data Access

```python
import duckdb
import pandas as pd

# Connect to the data
conn = duckdb.connect()

# Query 1-minute data
df = conn.execute("""
    SELECT * FROM 'data/raw/**/*.parquet' 
    WHERE symbol = 'TSLA' 
    ORDER BY timestamp
""").df()

# Query daily aggregated data  
daily = conn.execute("SELECT * FROM bars_1d WHERE symbol = 'TSLA'").df()
```

## Troubleshooting

### Common Issues

1. **Credentials Error**
   ```
   ❌ Alpaca credentials not found in environment variables
   ```
   **Solution**: Set ALPACA_KEY and ALPACA_SECRET environment variables

2. **Rate Limiting**
   ```
   ⚠️ Rate limit exceeded, sleeping...
   ```
   **Solution**: This is normal, the script automatically handles rate limits

3. **Network Timeout**
   ```
   ❌ TSLA data ingestion timed out after 1800s
   ```
   **Solution**: Check internet connection and retry

4. **Disk Space**
   ```
   ❌ No space left on device
   ```
   **Solution**: Ensure at least 1GB free disk space

### Support

- Check logs for detailed error messages
- Review validation reports for data quality issues
- Use `--help` flag with individual commands for more options

## Advanced Usage

### Custom Date Ranges

If you need different date ranges, modify the script or use direct CLI commands:

```bash
# Custom date range example
python -m marketpipe ohlcv ingest \
    --symbols TSLA \
    --start 2023-01-01 \
    --end 2023-12-31 \
    --provider alpaca \
    --feed-type iex
```

### Metrics Monitoring

Start the metrics server to monitor pipeline progress:

```bash
# In another terminal
python -m marketpipe metrics --port 8000

# View metrics at http://localhost:8000/metrics
```

## Data Quality

The pipeline includes comprehensive validation:
- OHLC price consistency checks
- Volume validation
- Timestamp alignment verification
- Missing data detection
- Statistical outlier detection

Check `data/validation_reports/` for detailed quality reports. 