# MarketPipe Data Directory

This directory contains all MarketPipe data files organized by type and purpose.

## Directory Structure

```
data/
├── db/                     # SQLite database files
│   ├── core.db            # Main domain database (symbol bars, OHLCV data)
│   ├── ingestion_jobs.db  # Ingestion job tracking and coordination
│   └── ingestion_checkpoints.db  # Checkpoint data for resumable ingestion
├── metrics/               # Metrics and monitoring data
│   ├── metrics.db         # SQLite metrics storage
│   └── multiprocess/      # Prometheus multiprocess metrics files
├── raw/                   # Raw market data files (Parquet)
├── aggregated/           # Aggregated data files (daily, weekly, etc.)
├── validation_reports/   # Data validation reports
└── agg/                  # Legacy aggregation directory
```

## Database Files

### Core Databases
- **core.db**: Main SQLite database containing domain entities, symbol bars, and OHLCV data
- **ingestion_jobs.db**: Tracks ingestion job status, scheduling, and coordination
- **metrics.db**: Stores application metrics and performance data

### Metrics
- **metrics/multiprocess/**: Directory for Prometheus multiprocess metrics files
  - Environment variable `PROMETHEUS_MULTIPROC_DIR` points here by default
  - Files are automatically cleaned up when metrics server stops

## Environment Variables

Set these environment variables to customize database locations:

```bash
# SQLite database paths
export MP_DB="data/db/core.db"                    # Core database
export METRICS_DB_PATH="data/metrics.db"          # Metrics database

# Prometheus multiprocess directory
export PROMETHEUS_MULTIPROC_DIR="data/metrics/multiprocess"
```

## Migration from Previous Versions

If you have database files in the root directory from previous versions:

1. Run the cleanup script: `python scripts/cleanup_db_files.py`
2. This will automatically:
   - Move `ingestion_jobs.db` to `data/db/ingestion_jobs.db`
   - Clean up Prometheus multiprocess `.db` files from root
   - Set up proper directory structure

## Backup Recommendations

Regular backups of the `data/db/` directory are recommended for production use:

```bash
# Create backup
tar -czf marketpipe_backup_$(date +%Y%m%d_%H%M%S).tar.gz data/db/

# Restore from backup
tar -xzf marketpipe_backup_YYYYMMDD_HHMMSS.tar.gz
``` 