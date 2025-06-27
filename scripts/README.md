# MarketPipe Pipeline Scripts

This directory contains scripts for running complete MarketPipe pipelines.

## 🚀 Full Pipeline Script

### `run_full_pipeline.py`

A comprehensive script that runs the entire MarketPipe pipeline (ingest → validate → aggregate) for the top 10 US equities using live market data.

#### Features

- **Top 10 Equities**: AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, BRK.B, LLY, V
- **Full Year of Data**: Automatically calculates 1 year of data ending 2 days ago
- **Live Data Provider**: Uses Alpaca Markets API for real market data
- **Complete Pipeline**: Runs ingestion, validation, and aggregation phases
- **Robust Error Handling**: Comprehensive error checking and recovery
- **Dry Run Support**: Test configuration without executing

#### Prerequisites

Before running the script, ensure you have:

1. **MarketPipe Installed and Configured**
   ```bash
   pip install -e .
   ```

2. **Alpaca API Credentials**
   Set environment variables:
   ```bash
   export ALPACA_KEY="your_alpaca_key"
   export ALPACA_SECRET="your_alpaca_secret"
   ```

3. **Sufficient Disk Space**
   - Estimated data size: ~100 MB for 1 year of OHLCV data
   - Files stored in `data/` directory with partitioned Parquet format

#### Usage

```bash
# Test the pipeline configuration (recommended first step)
python scripts/run_full_pipeline.py --dry-run

# Execute the full pipeline with live data
python scripts/run_full_pipeline.py --execute

# Show help and examples
python scripts/run_full_pipeline.py --help
```

#### What the Script Does

1. **Configuration Generation**
   - Creates optimized YAML configuration for top 10 equities
   - Calculates date range: 1 year ending 2 days ago
   - Sets conservative batch sizes and worker counts

2. **Prerequisites Validation**
   - Checks MarketPipe CLI accessibility
   - Validates Alpaca API credentials
   - Verifies data directory write permissions
   - Estimates data requirements

3. **Health Check**
   - Runs `marketpipe health-check` to validate installation
   - Reports any configuration issues

4. **Phase 1: Data Ingestion**
   - Ingests OHLCV data from Alpaca Markets
   - Uses IEX feed type (free tier)
   - Processes 10 symbols over 1 year period
   - Extracts job ID for subsequent phases

5. **Phase 2: Data Validation**
   - Validates data quality and schema compliance
   - Generates CSV validation reports
   - Checks for missing data or anomalies

6. **Phase 3: Data Aggregation**
   - Aggregates data to multiple timeframes:
     - 1min, 5min, 15min, 1h, 1d
   - Creates DuckDB views for querying
   - Enables analytical queries

7. **Execution Summary**
   - Reports success/failure of each phase
   - Provides next steps and data locations
   - Cleans up temporary configuration files

#### Output Structure

After successful execution, data will be organized as:

```
data/
├── raw/
│   └── frame=1m/
│       └── symbol=AAPL/
│           └── date=2024-06-23/
│               └── [uuid].parquet
├── aggregated/
│   └── [timeframe data]
├── validation_reports/
│   └── [job_id]_[symbol].csv
└── db/
    ├── core.db
    ├── ingestion_jobs.db
    └── metrics.db
```

#### Querying Data

After pipeline completion, query aggregated data:

```bash
# Show recent data
python -m marketpipe query "SELECT * FROM aggregated_ohlcv ORDER BY timestamp DESC LIMIT 10"

# Get daily data for specific symbol
python -m marketpipe query "SELECT * FROM aggregated_ohlcv WHERE symbol='AAPL' AND timeframe='1d' ORDER BY timestamp DESC LIMIT 30"
```

#### Troubleshooting

**Common Issues:**

1. **Missing Credentials**
   ```
   Error: Missing environment variable: ALPACA_KEY
   ```
   Solution: Set Alpaca API credentials in environment variables

2. **Data Directory Permissions**
   ```
   Error: Data directory not writable
   ```
   Solution: Ensure write permissions to MarketPipe directory

3. **Provider Not Available**
   ```
   Error: Alpaca provider not available
   ```
   Solution: Check MarketPipe installation and provider configuration

4. **Job ID Extraction Failed**
   ```
   Warning: Could not extract job ID from ingestion output
   ```
   Solution: Script will attempt to use latest job ID from database

**Getting Help:**

- Run health check: `python -m marketpipe health-check --verbose`
- Check provider status: `python -m marketpipe providers`
- View logs for detailed error information

#### Performance Considerations

- **Rate Limits**: Script uses conservative batch sizes to respect Alpaca rate limits
- **Processing Time**: Expect 30-60 minutes for full year of data (10 symbols)
- **Resource Usage**: Uses 3 worker threads by default for moderate CPU usage
- **Storage**: Parquet files provide efficient compression and query performance

#### Configuration Customization

To modify the script for different requirements:

1. **Change Symbols**: Edit `TOP_10_EQUITIES` list in the script
2. **Adjust Date Range**: Modify date calculation logic
3. **Different Provider**: Change provider configuration (requires credentials)
4. **Performance Tuning**: Adjust workers, batch_size, timeout values

The script is designed to be robust and production-ready for regular pipeline execution.