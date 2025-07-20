# Data Retention (Prune) Commands

The MarketPipe prune commands provide data retention utilities for managing storage space and maintaining optimal performance by removing old data according to configurable retention policies.

## Overview

The prune functionality consists of two main commands:

- `mp prune parquet` - Delete old parquet files based on date patterns in file paths
- `mp prune database` - Delete old job records from the ingestion database (SQLite or PostgreSQL)

Both commands support dry-run mode for safe preview of operations and emit Prometheus metrics and domain events for monitoring and integration.

## Business Rules

### Age Expression Parsing

Age expressions support the following formats:
- `30d` or `30` - 30 days (days is the default unit)
- `18m` - 18 months (approximated as 30 days per month)
- `5y` - 5 years (approximated as 365 days per year)

### Parquet File Pruning

- **Target**: Parquet files in a directory structure
- **Criteria**: Files whose date (extracted from path) is older than the cutoff
- **Date Extraction**: Supports multiple path patterns:
  - `symbol=AAPL/2024-01-15.parquet` (ISO date in filename)
  - `symbol=AAPL/date=2024-01-15/file.parquet` (date= prefix)
  - `symbol=AAPL/2024/01/15.parquet` (year/month/day structure)
- **Action**: Deletes entire partition files
- **Metrics**: Records `DATA_PRUNED_BYTES_TOTAL{type="parquet"}`

### SQLite Database Pruning

- **Target**: `ingestion_jobs` table in SQLite database
- **Criteria**: Records where `day < cutoff_date`
- **Action**: DELETE + VACUUM to reclaim space
- **Backend Detection**: Only runs if SQLite repository is active
- **Metrics**: Records `DATA_PRUNED_ROWS_TOTAL{type="sqlite"}`

## Command Usage

### Parquet File Pruning

```bash
# Delete parquet files older than 5 years
mp prune parquet 5y

# Preview 30-day cleanup without making changes
mp prune parquet 30d --dry-run

# Use custom root directory
mp prune parquet 18m --root ./data/custom

# Full example with all options
mp prune parquet 2y --root /path/to/parquet --dry-run
```

**Options:**
- `--root PATH` - Root directory containing parquet files (default: `data/parquet`)
- `--dry-run, -n` - Show what would be deleted without making changes

### Database Pruning (SQLite & PostgreSQL)

```bash
# Delete job records older than 18 months (auto-detects backend)
mp prune database 18m

# Preview 90-day cleanup
mp prune database 90d --dry-run

# Delete records older than 1 year
mp prune database 1y

# Legacy command (deprecated but still works)
mp prune sqlite 18m  # Shows deprecation warning
```

**Options:**
- `--dry-run, -n` - Show what would be deleted without making changes

**Backend Support:**
- **SQLite**: Default backend, uses local database file
- **PostgreSQL**: Set `DATABASE_URL=postgresql://...` to use PostgreSQL
- **Auto-Detection**: Command automatically detects which backend is active

## Implementation Details

### Architecture

The prune commands follow MarketPipe's architectural patterns:

1. **CLI Layer** (`src/marketpipe/cli/prune.py`)
   - Typer-based command interface
   - Input validation and user feedback
   - Async coordination using `asyncio.run()`

2. **Repository Layer**
   - SQLite repository methods: `count_old_jobs()`, `delete_old_jobs()`
   - File system operations for parquet files

3. **Metrics Integration** (`src/marketpipe/metrics.py`)
   - Prometheus counters for bytes and rows pruned
   - Legacy metrics recording for backward compatibility

4. **Domain Events** (`src/marketpipe/domain/events.py`)
   - `DataPruned` event with data_type, amount, and cutoff fields
   - Event emission for monitoring and integration

### Error Handling

- **Graceful Degradation**: Commands continue processing even if individual files/records fail
- **User Feedback**: Clear error messages with context
- **Exit Codes**: Proper exit codes for scripting integration
- **Logging**: Detailed logging for troubleshooting

### Safety Features

- **Dry-Run Mode**: Preview operations without making changes
- **Backend Detection**: SQLite command only runs with SQLite backend
- **Validation**: Age expression validation with helpful error messages
- **Confirmation**: Clear output showing what was processed

## Metrics and Monitoring

### Prometheus Metrics

```
# Bytes of data pruned by type
DATA_PRUNED_BYTES_TOTAL{type="parquet"}

# Rows of data pruned by type
DATA_PRUNED_ROWS_TOTAL{type="sqlite"}
```

### Domain Events

```python
@dataclass(frozen=True)
class DataPruned(DomainEvent):
    data_type: str      # "parquet", "sqlite", etc.
    amount: int         # bytes for files, rows for records
    cutoff: date        # cutoff date used for pruning
```

## Examples

### Automated Cleanup Script

```bash
#!/bin/bash
# Daily cleanup script

# Clean old parquet files (keep 2 years)
mp prune parquet 2y

# Clean old job records (keep 18 months)
mp prune sqlite 18m

# Check metrics
curl -s http://localhost:8000/metrics | grep data_pruned
```

### Dry-Run Validation

```bash
# Preview what would be cleaned
mp prune parquet 1y --dry-run
mp prune sqlite 6m --dry-run

# If satisfied, run actual cleanup
mp prune parquet 1y
mp prune sqlite 6m
```

### Custom Retention Policies

```bash
# Conservative: Keep 5 years of parquet, 2 years of jobs
mp prune parquet 5y
mp prune sqlite 2y

# Aggressive: Keep 6 months of parquet, 3 months of jobs
mp prune parquet 6m
mp prune sqlite 3m

# Balanced: Keep 1 year of parquet, 6 months of jobs
mp prune parquet 1y
mp prune sqlite 6m
```

## Integration

### Cron Jobs

```cron
# Run daily at 2 AM
0 2 * * * /path/to/mp prune parquet 2y && /path/to/mp prune sqlite 18m

# Run weekly dry-run for monitoring
0 1 * * 0 /path/to/mp prune parquet 1y --dry-run | logger -t marketpipe-prune
```

### Monitoring Alerts

```yaml
# Prometheus alert rules
groups:
- name: marketpipe.prune
  rules:
  - alert: LargePruneOperation
    expr: increase(DATA_PRUNED_BYTES_TOTAL[1h]) > 10e9  # 10GB
    for: 0m
    labels:
      severity: warning
    annotations:
      summary: "Large amount of data pruned"
      description: "{{ $value }} bytes pruned in the last hour"
```

## Testing

The prune commands include comprehensive tests covering:

- Age expression parsing
- Parquet file detection and deletion
- SQLite record counting and deletion
- Dry-run mode functionality
- Metrics recording
- Domain event emission
- Error handling scenarios

Run tests with:
```bash
pytest tests/test_prune_commands.py -v
```

## Troubleshooting

### Common Issues

1. **"Directory does not exist"**
   - Check the `--root` path for parquet command
   - Ensure the directory exists and is readable

2. **"SQLite backend not active"**
   - The system is using PostgreSQL or another backend
   - SQLite pruning only works with SQLite repositories

3. **"No files found"**
   - Files may not match the expected date patterns
   - Try a longer retention period to test
   - Check file naming conventions

4. **Database migration errors**
   - Ensure database is properly initialized
   - Run `mp migrate` if needed

### Debug Mode

Enable debug logging for detailed operation information:

```bash
export MP_LOG_LEVEL=DEBUG
mp prune parquet 1y --dry-run
```

## Security Considerations

- **File Permissions**: Ensure proper read/write permissions for target directories
- **Database Access**: SQLite file must be writable by the process
- **Backup Strategy**: Always have backups before running prune operations
- **Access Control**: Restrict access to prune commands in production environments

## Performance

- **Parquet Pruning**: Performance depends on file system and number of files
- **SQLite Pruning**: VACUUM operation can be slow on large databases
- **Batch Processing**: Commands process files/records in batches for memory efficiency
- **Monitoring**: Use metrics to track performance and adjust retention policies
