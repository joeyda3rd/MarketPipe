# Ingestion Module

## Purpose

The ingestion module orchestrates the collection of market data from external providers using Domain-Driven Design patterns. It provides job management, connector architecture, data validation, and storage coordination with support for multiple market data providers (Alpaca, IEX, Fake).

## Key Public Interfaces

### Domain Layer
```python
from marketpipe.ingestion import (
    IngestionJob, IngestionJobId, ProcessingState,
    IngestionConfiguration, IngestionPartition
)

# Create ingestion job
job_id = IngestionJobId("AAPL_2024-01-01")
config = IngestionConfiguration(
    provider="alpaca",
    batch_size=1000,
    max_retries=3
)
job = IngestionJob(job_id, config, [Symbol("AAPL")], time_range)

# Job lifecycle
job.start()
job.mark_symbol_processed(Symbol("AAPL"), 390, partition)
job.complete()
```

### Application Services
```python
from marketpipe.ingestion import (
    IngestionCoordinatorService, IngestionJobService,
    CreateIngestionJobCommand, StartJobCommand
)

# Create and start jobs
command = CreateIngestionJobCommand(
    symbols=[Symbol("AAPL"), Symbol("MSFT")],
    time_range=TimeRange.single_day(date(2024, 1, 1)),
    configuration=config
)

job_id = await job_service.create_job(command)
await job_service.start_job(StartJobCommand(job_id))

# Execute complete workflow
result = await coordinator.execute_job(job_id)
```

### Market Data Connectors
```python
from marketpipe.ingestion.connectors import (
    AlpacaClient, BaseApiClient, ClientConfig,
    HeaderTokenAuth, RateLimiter
)

# Configure client
config = ClientConfig(
    api_key="your_key",
    base_url="https://data.alpaca.markets/v2",
    timeout=30.0
)
auth = HeaderTokenAuth("APCA-API-KEY-ID", "APCA-API-SECRET-KEY")
rate_limiter = RateLimiter(200, 60.0)

client = AlpacaClient(config, auth, rate_limiter)

# Fetch data (sync and async)
bars = client.fetch_batch("AAPL", start_ts, end_ts)
bars = await client.async_fetch_batch("AAPL", start_ts, end_ts)
```

### Provider Registry
```python
from marketpipe.ingestion.infrastructure import (
    provider, register, get, list_providers,
    build_provider, get_available_providers
)

# Register providers
@provider("alpaca")
class AlpacaProvider:
    pass

# Get providers
alpaca_provider = get("alpaca")
all_providers = list_providers()
available = get_available_providers()
```

## Brief Call Graph

```
CLI Commands
    ↓
IngestionCoordinatorService
    ↓
IngestionJobService ←→ IngestionJob (Domain)
    ↓
Market Data Adapters ←→ API Clients
    ↓
Data Storage ←→ Validation
    ↓
Event Publishing
```

### Processing Flow

1. **Job Creation**: `CreateIngestionJobCommand` → `IngestionJob` entity → repository
2. **Job Execution**: Coordinator → symbol processing → data fetching → validation → storage
3. **Event Publishing**: Domain events → event bus → metrics/monitoring
4. **State Management**: Checkpoints → resume capability → progress tracking

## Examples

### Basic Job Creation and Execution
```python
@Code(src/marketpipe/ingestion/domain/entities.py:140-170)
# Create ingestion job with validation
job = IngestionJob(
    job_id=IngestionJobId("AAPL_2024-01-01"),
    configuration=config,
    symbols=[Symbol("AAPL")],
    time_range=TimeRange.single_day(date(2024, 1, 1))
)

@Code(src/marketpipe/ingestion/domain/entities.py:269-287)
# Job lifecycle management
job.start()  # Validates state transition
job.mark_symbol_processed(symbol, bars_count, partition)
job.complete()  # Emits domain events
```

### Application Service Coordination
```python
@Code(src/marketpipe/ingestion/application/services.py:60-85)
# Create job through application service
command = CreateIngestionJobCommand(
    symbols=[Symbol("AAPL")],
    time_range=time_range,
    configuration=config
)
job_id = await job_service.create_job(command)

@Code(src/marketpipe/ingestion/application/services.py:292-320)
# Execute complete ingestion workflow
result = await coordinator.execute_job(job_id)
```

### Alpaca Client Usage
```python
@Code(src/marketpipe/ingestion/connectors/alpaca_client.py:25-45)
# Build Alpaca-specific request parameters
params = client.build_request_params(
    symbol="AAPL",
    start_ts=start_timestamp,
    end_ts=end_timestamp,
    cursor=pagination_token
)

@Code(src/marketpipe/ingestion/connectors/alpaca_client.py:110-135)
# Parse Alpaca response to canonical format
rows = client.parse_response(raw_json)
# Returns standardized OHLCV dictionaries
```

### Provider Registration
```python
@Code(src/marketpipe/ingestion/infrastructure/provider_registry.py:15-25)
# Register market data provider
@provider("alpaca")
class AlpacaProvider:
    def __init__(self, config):
        self.client = AlpacaClient(config)

# Get registered provider
provider_instance = get("alpaca")
```

### Anti-Corruption Layer
```python
@Code(src/marketpipe/ingestion/infrastructure/adapters.py:20-45)
# Adapter translates between domain and infrastructure
class AlpacaMarketDataAdapter(IMarketDataProvider):
    async def fetch_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange, max_bars: int = 1000
    ) -> List[OHLCVBar]:
        # Converts external API data to domain entities
        raw_data = await self._client.fetch_batch(...)
        return [self._to_domain_entity(bar) for bar in raw_data]
```

### Rate Limiting and Retry Logic
```python
@Code(src/marketpipe/ingestion/infrastructure/rate_limit.py:15-35)
# Rate limiter with async support
rate_limiter = RateLimiter(requests_per_window=200, window_seconds=60.0)
await rate_limiter.async_acquire()  # Blocks if limit exceeded

@Code(src/marketpipe/ingestion/connectors/alpaca_client.py:135-149)
# Exponential backoff retry logic
def should_retry(self, status: int, body: Dict[str, Any]) -> bool:
    return status in {429, 500, 502, 503, 504}

sleep_time = self._backoff(attempt)  # Exponential + jitter
```

### Event-Driven Processing
```python
@Code(src/marketpipe/ingestion/domain/events.py:15-35)
# Domain events for ingestion lifecycle
event = IngestionJobStarted(
    job_id=job.job_id,
    symbol=symbol,
    trading_date=trading_date
)

@Code(src/marketpipe/ingestion/domain/events.py:45-65)
# Batch processing events
batch_event = IngestionBatchProcessed(
    job_id=job.job_id,
    symbol=symbol,
    bars_count=len(bars),
    partition=partition
)
```

### Storage Integration
```python
@Code(src/marketpipe/ingestion/infrastructure/parquet_storage.py:20-40)
# Parquet storage with partitioning
storage = ParquetDataStorage(output_path="./data")
partition = await storage.store_bars(
    bars=domain_bars,
    symbol=symbol,
    trading_date=trading_date
)
```

## Architecture Benefits

- **Clean Separation**: Domain logic isolated from external APIs
- **Provider Agnostic**: Easy to add new market data sources
- **Fault Tolerance**: Retry logic, checkpointing, and resume capability
- **Event-Driven**: Loose coupling through domain events
- **Type Safety**: Strong typing throughout the pipeline
- **Testability**: Comprehensive mocking and testing support
