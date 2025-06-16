# Infrastructure Module

## Purpose

The infrastructure module provides concrete implementations of domain interfaces including storage engines, repositories, and event publishers. It handles persistence, external integrations, and technical concerns while maintaining clean separation from domain logic.

## Key Public Interfaces

### Storage Engine
```python
from marketpipe.infrastructure.storage import ParquetStorageEngine

# Initialize storage engine
engine = ParquetStorageEngine(root="data/raw", compression="zstd")

# Write partitioned data
file_path = engine.write(
    df,
    frame="1m",
    symbol="AAPL", 
    trading_day=date(2024, 1, 1),
    job_id="AAPL_2024-01-01",
    overwrite=False
)

# Load data by job
symbol_dataframes = engine.load_job_bars("AAPL_2024-01-01")

# Load symbol data across dates
df = engine.load_symbol_data("AAPL", "1m", start_date=date(2024, 1, 1))
```

### Domain Repositories
```python
from marketpipe.infrastructure.repositories.sqlite_domain import (
    SqliteSymbolBarsRepository,
    SqliteOHLCVRepository,
    SqliteCheckpointRepository
)

# Symbol bars aggregate repository
bars_repo = SqliteSymbolBarsRepository("data/db/core.db")
aggregate = await bars_repo.get_by_symbol_and_date(Symbol("AAPL"), date(2024, 1, 1))
await bars_repo.save(aggregate)

# Individual OHLCV bars repository
ohlcv_repo = SqliteOHLCVRepository("data/db/core.db")
async for bar in ohlcv_repo.get_bars_for_symbol(Symbol("AAPL"), time_range):
    process_bar(bar)

# Checkpoint repository
checkpoint_repo = SqliteCheckpointRepository("data/db/core.db")
await checkpoint_repo.save_checkpoint(Symbol("AAPL"), {"last_ts": 1640995800})
```

### Event Publishers
```python
from marketpipe.infrastructure.events import InMemoryEventPublisher

# Create event publisher
publisher = InMemoryEventPublisher()

# Register event handlers
publisher.register_handler("ingestion_completed", handle_ingestion)

# Publish events
await publisher.publish(ingestion_event)
await publisher.publish_many([event1, event2, event3])
```

## Brief Call Graph

```
Application Layer
    ↓
Domain Repositories (Interfaces)
    ↓
Infrastructure Repositories (SQLite/PostgreSQL)
    ↓
Database/Storage Layer

Storage Operations
    ↓
ParquetStorageEngine
    ↓
Partitioned Parquet Files

Event Flow
    ↓
InMemoryEventPublisher
    ↓
Registered Event Handlers
```

### Infrastructure Flow

1. **Storage Operations**: ParquetStorageEngine handles partitioned file I/O
2. **Repository Pattern**: SQLite repositories implement domain interfaces
3. **Event Publishing**: InMemoryEventPublisher distributes domain events
4. **Concurrency Safety**: File locking and optimistic concurrency control
5. **Data Integrity**: Schema validation and transaction management

## Examples

### Partitioned Storage Operations
```python
@Code(src/marketpipe/infrastructure/storage/parquet_engine.py:50-95)
# Write DataFrame with partitioned structure and concurrency safety
def write(self, df: pd.DataFrame, *, frame: str, symbol: str, trading_day: date, job_id: str, overwrite: bool = False) -> Path:
    # Validate DataFrame
    if df.empty:
        raise ValueError("Cannot write empty DataFrame")
    
    required_cols = {"ts_ns", "open", "high", "low", "close", "volume"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"DataFrame missing required columns: {missing}")
    
    # Create partition structure: frame=1m/symbol=AAPL/date=2024-01-01/
    partition_path = (
        self._root / f"frame={frame}" / f"symbol={symbol}" / f"date={trading_day.isoformat()}"
    )
    partition_path.mkdir(parents=True, exist_ok=True)
    
    file_path = partition_path / f"{job_id}.parquet"
    
    # Use file locking for concurrency safety
    lock_path = str(file_path) + ".lock"
    with fasteners.InterProcessLock(lock_path):
        if file_path.exists() and not overwrite:
            raise FileExistsError(f"File already exists: {file_path}")
        
        # Write with compression and optimized settings
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, file_path, compression=self._compression, row_group_size=10000, use_dictionary=False)
```

### Job-Based Data Loading
```python
@Code(src/marketpipe/infrastructure/storage/parquet_engine.py:307-340)
# Load all symbol data for a specific ingestion job
def load_job_bars(self, job_id: str) -> Dict[str, pd.DataFrame]:
    symbol_dataframes = {}
    
    # Search across all frame/symbol partitions for job files
    for frame_dir in self._root.glob("frame=*"):
        for symbol_dir in frame_dir.glob("symbol=*"):
            symbol = symbol_dir.name.split("=")[1]
            
            # Find job files across all dates for this symbol
            job_files = list(symbol_dir.glob(f"**/{job_id}.parquet"))
            
            if job_files:
                # Load and combine all files for this symbol
                dfs = []
                for file_path in job_files:
                    try:
                        df = pd.read_parquet(file_path)
                        dfs.append(df)
                    except Exception as e:
                        self.log.warning(f"Failed to load {file_path}: {e}")
                
                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)
                    if "ts_ns" in combined_df.columns:
                        combined_df = combined_df.sort_values("ts_ns")
                    symbol_dataframes[symbol] = combined_df
    
    return symbol_dataframes
```

### Symbol Bars Aggregate Repository
```python
@Code(src/marketpipe/infrastructure/repositories/sqlite_domain.py:40-75)
# Load symbol bars aggregate with version control
async def get_by_symbol_and_date(self, symbol: Symbol, trading_date: date) -> Optional[SymbolBarsAggregate]:
    try:
        async with self._conn() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT symbol, trading_date, version, is_complete, 
                       collection_started, bar_count, created_at, updated_at
                FROM symbol_bars_aggregates
                WHERE symbol = ? AND trading_date = ?
            """,
                (symbol.value, trading_date.isoformat()),
            )
            
            row = await cursor.fetchone()
            if row is None:
                return None
            
            # Reconstruct aggregate from stored data
            aggregate = SymbolBarsAggregate(symbol, trading_date)
            aggregate._version = row["version"]
            aggregate._is_complete = bool(row["is_complete"])
            aggregate._collection_started = bool(row["collection_started"])
            aggregate._bar_count = row["bar_count"]
            
            return aggregate
    except Exception as e:
        raise RepositoryError(f"Failed to get symbol bars aggregate: {e}") from e
```

### Optimistic Concurrency Control
```python
@Code(src/marketpipe/infrastructure/repositories/sqlite_domain.py:77-110)
# Save aggregate with concurrency conflict detection
async def save(self, aggregate: SymbolBarsAggregate) -> None:
    try:
        async with self._conn() as db:
            # Check for concurrency conflicts
            cursor = await db.execute(
                """
                SELECT version FROM symbol_bars_aggregates
                WHERE symbol = ? AND trading_date = ?
            """,
                (aggregate.symbol.value, aggregate.trading_date.isoformat()),
            )
            
            existing_row = await cursor.fetchone()
            if existing_row and existing_row[0] != aggregate.version - 1:
                raise ConcurrencyError(
                    f"Aggregate has been modified by another process. "
                    f"Expected version {aggregate.version - 1}, found {existing_row[0]}"
                )
            
            # Insert or update with new version
            await db.execute(
                """
                INSERT OR REPLACE INTO symbol_bars_aggregates 
                (symbol, trading_date, version, is_complete, collection_started, bar_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
                (
                    aggregate.symbol.value,
                    aggregate.trading_date.isoformat(),
                    aggregate.version,
                    aggregate.is_complete,
                    aggregate._collection_started,
                    aggregate.bar_count,
                ),
            )
            await db.commit()
```

### Event Publishing
```python
@Code(src/marketpipe/infrastructure/events/publishers.py:25-45)
# Publish domain events with handler execution
async def publish(self, event: DomainEvent) -> None:
    self._events.append(event)
    
    # Call registered handlers
    event_type = event.event_type
    if event_type in self._handlers:
        for handler in self._handlers[event_type]:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                # Log error but don't let handler failures break the publisher
                print(f"Event handler error for {event_type}: {e}")

def register_handler(self, event_type: str, handler) -> None:
    if event_type not in self._handlers:
        self._handlers[event_type] = []
    self._handlers[event_type].append(handler)
```

## Storage Architecture

### Partitioned Layout
```
data/raw/
├── frame=1m/
│   ├── symbol=AAPL/
│   │   ├── date=2024-01-01/
│   │   │   ├── AAPL_2024-01-01.parquet
│   │   │   └── AAPL_2024-01-01_backfill.parquet
│   │   └── date=2024-01-02/
│   └── symbol=MSFT/
└── frame=5m/
```

### Database Schema
```sql
-- Symbol bars aggregates
CREATE TABLE symbol_bars_aggregates (
    symbol TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    is_complete BOOLEAN NOT NULL DEFAULT FALSE,
    collection_started BOOLEAN NOT NULL DEFAULT FALSE,
    bar_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trading_date)
);

-- Individual OHLCV bars
CREATE TABLE ohlcv_bars (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL,
    volume INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp_ns)
);

-- Ingestion checkpoints
CREATE TABLE checkpoints (
    symbol TEXT PRIMARY KEY,
    checkpoint_data TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Architecture Benefits

- **Separation of Concerns**: Infrastructure isolated from domain logic
- **Pluggable Persistence**: Repository pattern enables different storage backends
- **Concurrency Safety**: File locking and optimistic concurrency control
- **Event-Driven**: Decoupled event publishing and handling
- **Partitioned Storage**: Efficient organization and querying of time-series data
- **Async Operations**: Non-blocking database operations with aiosqlite
- **Data Integrity**: Transaction management and constraint enforcement
- **Scalable Design**: Streaming operations and batch processing support 