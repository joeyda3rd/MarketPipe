# Aggregation Module

## Purpose

The aggregation module provides time-series aggregation of 1-minute OHLCV bars to higher timeframes (5m, 15m, 1h, 1d). It uses DuckDB for high-performance SQL-based aggregation, creates dynamic views for fast querying, and integrates with the event-driven architecture for automatic processing.

## Key Public Interfaces

### Application Services
```python
from marketpipe.aggregation import AggregationRunnerService

# Build and register aggregation service
service = AggregationRunnerService.build_default()
AggregationRunnerService.register()  # Auto-subscribes to ingestion events

# Manual aggregation
service.run_manual_aggregation(job_id="AAPL_2024-01-01")
```

### Domain Services
```python
from marketpipe.aggregation.domain.services import AggregationDomainService
from marketpipe.aggregation.domain.value_objects import FrameSpec

# Create domain service
domain = AggregationDomainService()

# Generate aggregation SQL
frame = FrameSpec("5m", 300)  # 5-minute bars
sql = domain.duckdb_sql(frame, src_table="bars")
```

### DuckDB Views
```python
from marketpipe.aggregation import duckdb_views

# Ensure views are available
duckdb_views.ensure_views()

# Query aggregated data
df = duckdb_views.query("""
    SELECT symbol, ts_ns, open, high, low, close, volume
    FROM bars_5m
    WHERE symbol = 'AAPL'
    ORDER BY ts_ns
""")

# Get data availability summary
summary = duckdb_views.get_available_data()
```

### Frame Specifications
```python
from marketpipe.aggregation.domain.value_objects import FrameSpec, DEFAULT_SPECS

# Standard timeframes
specs = DEFAULT_SPECS  # [5m, 15m, 1h, 1d]

# Custom timeframe
custom_frame = FrameSpec("30m", 1800)  # 30-minute bars
```

## Brief Call Graph

```
Ingestion Completion Event
    ↓
AggregationRunnerService.handle_ingestion_completed()
    ↓
AggregationDomainService.duckdb_sql() (for each frame)
    ↓
DuckDBAggregationEngine.aggregate_job()
    ↓
DuckDB SQL Execution
    ↓
ParquetStorageEngine.write() (aggregated data)
    ↓
duckdb_views.refresh_views()
    ↓
AggregationCompleted Event
```

### Aggregation Flow

1. **Event Trigger**: `IngestionJobCompleted` event → aggregation service
2. **SQL Generation**: Domain service generates DuckDB SQL for each timeframe
3. **Data Loading**: Load 1-minute bars from Parquet storage
4. **Aggregation**: Execute SQL aggregation using DuckDB engine
5. **Storage**: Write aggregated data to partitioned Parquet files
6. **View Refresh**: Update DuckDB views for new data
7. **Event Publishing**: Publish `AggregationCompleted` event

## Examples

### Event-Driven Aggregation
```python
@Code(src/marketpipe/aggregation/application/services.py:45-75)
# Handle ingestion completion automatically
def handle_ingestion_completed(self, event: IngestionJobCompleted) -> None:
    # Extract provider/feed for metrics
    provider, feed = self._extract_provider_feed_info(event)

    # Generate SQL for each timeframe
    sql_pairs = [
        (spec, self._domain.duckdb_sql(spec)) for spec in DEFAULT_SPECS
    ]

    # Run aggregation
    result = self._engine.aggregate_job(event.job_id, sql_pairs)

    # Refresh views and publish success event
    refresh_views()
    success_event = AggregationCompleted(event.job_id, len(DEFAULT_SPECS))
    get_event_bus().publish(success_event)
```

### SQL Generation
```python
@Code(src/marketpipe/aggregation/domain/services.py:10-27)
# Generate DuckDB SQL for timeframe aggregation
@staticmethod
def duckdb_sql(frame: FrameSpec, src_table: str = "bars") -> str:
    window_ns = frame.seconds * 1_000_000_000
    return f"""
    SELECT
        symbol,
        floor(ts_ns/{window_ns}) * {window_ns} AS ts_ns,
        first(open ORDER BY ts_ns)  AS open,
        max(high)    AS high,
        min(low)     AS low,
        last(close ORDER BY ts_ns)  AS close,
        sum(volume)  AS volume
    FROM {src_table}
    GROUP BY symbol, floor(ts_ns/{window_ns})
    ORDER BY symbol, ts_ns
    """
```

### DuckDB Engine Processing
```python
@Code(src/marketpipe/aggregation/infrastructure/duckdb_engine.py:30-70)
# Process aggregation for all symbols in a job
def aggregate_job(self, job_id: str, frame_sql_pairs: List[Tuple[FrameSpec, str]]) -> None:
    # Load raw data for all symbols
    symbol_dataframes = self._raw_storage.load_job_bars(job_id)

    # Create DuckDB connection
    con = duckdb.connect(":memory:")

    # Process each symbol
    for symbol, df in symbol_dataframes.items():
        # Ensure required columns
        if "ts_ns" not in df.columns and "timestamp_ns" in df.columns:
            df["ts_ns"] = df["timestamp_ns"]
        if "symbol" not in df.columns:
            df["symbol"] = symbol

        # Register DataFrame in DuckDB
        con.register("bars", pa.Table.from_pandas(df))

        # Execute aggregation for each timeframe
        for spec, sql in frame_sql_pairs:
            result_df = con.execute(sql).fetch_df()
            self._write_aggregated_data(result_df, symbol, spec, job_id)
```

### Dynamic View Management
```python
@Code(src/marketpipe/aggregation/infrastructure/duckdb_views.py:35-55)
# Create dynamic views for timeframe partitions
def _attach_partition(frame: str) -> None:
    path = AGG_ROOT / f"frame={frame}"

    if not path.exists():
        # Create empty view to avoid SQL errors
        _get_connection().execute(
            f"CREATE OR REPLACE VIEW bars_{frame} AS "
            f"SELECT NULL::VARCHAR as symbol, NULL::BIGINT as ts_ns, "
            f"NULL::DOUBLE as open, NULL::DOUBLE as high, NULL::DOUBLE as low, "
            f"NULL::DOUBLE as close, NULL::BIGINT as volume, NULL::VARCHAR as date "
            f"WHERE 1=0"
        )
        return

    # Create view using Hive partitioning
    view_sql = (
        f"CREATE OR REPLACE VIEW bars_{frame} AS "
        f"SELECT * FROM parquet_scan('{path}/**/*.parquet', hive_partitioning=1)"
    )
    _get_connection().execute(view_sql)
```

### Query Interface
```python
@Code(src/marketpipe/aggregation/infrastructure/duckdb_views.py:85-105)
# Execute SQL queries against aggregated views
def query(sql: str) -> pd.DataFrame:
    if not sql or not sql.strip():
        raise ValueError("SQL query cannot be empty")

    # Ensure views are available
    ensure_views()

    try:
        result_df = _get_connection().execute(sql).fetch_df()
        return result_df
    except Exception as e:
        raise RuntimeError(f"Failed to execute query: {e}") from e

# Example usage
df = duckdb_views.query("""
    SELECT symbol, ts_ns, close, volume
    FROM bars_1h
    WHERE symbol IN ('AAPL', 'MSFT')
    AND ts_ns >= 1640995200000000000
    ORDER BY symbol, ts_ns
""")
```

### Data Availability Summary
```python
@Code(src/marketpipe/aggregation/infrastructure/duckdb_views.py:110-135)
# Get summary of available aggregated data
def get_available_data() -> pd.DataFrame:
    summary_sql = """
    WITH frame_data AS (
        SELECT '5m' as frame, symbol, date, COUNT(*) as row_count FROM bars_5m GROUP BY symbol, date
        UNION ALL
        SELECT '15m' as frame, symbol, date, COUNT(*) as row_count FROM bars_15m GROUP BY symbol, date
        UNION ALL
        SELECT '1h' as frame, symbol, date, COUNT(*) as row_count FROM bars_1h GROUP BY symbol, date
        UNION ALL
        SELECT '1d' as frame, symbol, date, COUNT(*) as row_count FROM bars_1d GROUP BY symbol, date
    )
    SELECT
        frame, symbol,
        COUNT(DISTINCT date) as date_count,
        SUM(row_count) as total_rows
    FROM frame_data
    WHERE symbol IS NOT NULL
    GROUP BY frame, symbol
    ORDER BY frame, symbol
    """
    return _get_connection().execute(summary_sql).fetch_df()
```

### Manual Aggregation
```python
@Code(src/marketpipe/aggregation/application/services.py:95-115)
# Run aggregation manually for specific job
def run_manual_aggregation(self, job_id: str) -> None:
    # Record manual aggregation metrics
    record_metric("aggregation_manual_runs", 1, provider="manual", feed="manual")

    # Create fake event and process it
    event = IngestionJobCompleted(
        job_id=job_id,
        symbol=Symbol("MANUAL"),
        trading_date=date.today(),
        bars_processed=0,
        success=True,
    )
    self.handle_ingestion_completed(event)
```

### Service Registration
```python
@Code(src/marketpipe/aggregation/application/services.py:130-140)
# Auto-register aggregation service for events
@classmethod
def register(cls) -> None:
    service = cls.build_default()
    get_event_bus().subscribe(IngestionJobCompleted, service.handle_ingestion_completed)

# Usage in bootstrap
AggregationRunnerService.register()  # Automatic event subscription
```

### View Validation
```python
@Code(src/marketpipe/aggregation/infrastructure/duckdb_views.py:155-175)
# Validate view accessibility
def validate_views() -> dict[str, bool]:
    frames = ["5m", "15m", "1h", "1d"]
    status = {}

    ensure_views()

    for frame in frames:
        view_name = f"bars_{frame}"
        try:
            _get_connection().execute(f"SELECT COUNT(*) FROM {view_name} LIMIT 1").fetch_df()
            status[view_name] = True
        except Exception as e:
            status[view_name] = False

    return status
```

## Timeframe Specifications

### Default Timeframes
- **5m**: 5-minute bars (300 seconds)
- **15m**: 15-minute bars (900 seconds)
- **1h**: 1-hour bars (3600 seconds)
- **1d**: 1-day bars (86400 seconds)

### Aggregation Rules
- **Open**: First open price in the time window
- **High**: Maximum high price in the time window
- **Low**: Minimum low price in the time window
- **Close**: Last close price in the time window
- **Volume**: Sum of all volume in the time window

### Time Window Calculation
```python
# Window alignment using nanosecond timestamps
window_ns = frame.seconds * 1_000_000_000
aligned_ts = floor(ts_ns / window_ns) * window_ns
```

## Storage Structure

### Partitioned Parquet Layout
```
data/agg/
├── frame=5m/
│   ├── symbol=AAPL/
│   │   ├── date=2024-01-01/
│   │   │   └── job_id=AAPL_2024-01-01.parquet
│   │   └── date=2024-01-02/
│   └── symbol=MSFT/
├── frame=15m/
├── frame=1h/
└── frame=1d/
```

### View Mapping
- `bars_5m` → `data/agg/frame=5m/**/*.parquet`
- `bars_15m` → `data/agg/frame=15m/**/*.parquet`
- `bars_1h` → `data/agg/frame=1h/**/*.parquet`
- `bars_1d` → `data/agg/frame=1d/**/*.parquet`

## Architecture Benefits

- **High Performance**: DuckDB columnar processing for fast aggregation
- **Event-Driven**: Automatic aggregation on ingestion completion
- **Dynamic Views**: Real-time access to aggregated data without manual refresh
- **Partitioned Storage**: Efficient storage and querying by timeframe/symbol/date
- **SQL Interface**: Standard SQL queries against aggregated data
- **Extensible**: Easy to add new timeframes and aggregation rules
- **Memory Efficient**: Streaming processing with optimized DuckDB settings
