# Core Module

## Purpose

The core module provides MarketPipe's initialization, metrics collection, and monitoring infrastructure. It handles application bootstrap with database migrations, service registration, and provides both async and legacy metrics servers for Prometheus monitoring.

## Key Public Interfaces

### Bootstrap
```python
from marketpipe.bootstrap import bootstrap, get_event_bus

# Initialize MarketPipe (idempotent)
bootstrap()

# Get global event bus
event_bus = get_event_bus()
```

### Metrics Collection
```python
from marketpipe.metrics import (
    REQUESTS, ERRORS, LATENCY,
    record_metric, SqliteMetricsRepository
)

# Record metrics with labels
REQUESTS.labels(source="cli", provider="alpaca", feed="iex").inc()
ERRORS.labels(source="cli", provider="alpaca", feed="iex", code="429").inc()

# Store metrics history
repo = SqliteMetricsRepository()
await repo.record("custom_metric", 123.45, provider="alpaca", feed="iex")
```

### Metrics Server
```python
from marketpipe.metrics_server import run, AsyncMetricsServer

# Legacy blocking server
run(port=8000, legacy=True)

# Async non-blocking server
server = AsyncMetricsServer(port=8000)
await server.start()
```

## Call Graph

```
CLI Commands
    ↓
bootstrap()
    ↓
apply_pending_alembic() → Database migrations
    ↓
ValidationRunnerService.register()
AggregationRunnerService.register()
    ↓
Event handlers registration

Metrics Flow:
record_metric() → SqliteMetricsRepository → Database storage
    ↓
AsyncMetricsServer → HTTP /metrics endpoint → Prometheus
```

## Examples

### Application Initialization
```python
@Code:src/marketpipe/bootstrap.py:78-108
```

The bootstrap process applies Alembic migrations and registers core services:

### Metrics Recording
```python
@Code:src/marketpipe/metrics.py:274-289
```

Metrics are recorded with full context labels for provider and feed tracking.

### Async Metrics Server
```python
@Code:src/marketpipe/metrics_server.py:42-69
```

The async server provides non-blocking metrics endpoints with connection limits and event loop lag monitoring.

## Key Components

| Component | Purpose | Location |
|-----------|---------|----------|
| `bootstrap()` | App initialization | `src/marketpipe/bootstrap.py` |
| `REQUESTS/ERRORS/LATENCY` | Core metrics | `src/marketpipe/metrics.py` |
| `AsyncMetricsServer` | Async HTTP server | `src/marketpipe/metrics_server.py` |
| `SqliteMetricsRepository` | Metrics storage | `src/marketpipe/metrics.py` |
| `get_event_bus()` | Global event bus | `src/marketpipe/bootstrap.py` |

## Database Schema

The core module creates these tables via Alembic migrations:
- `metrics` - Historical metrics data
- `alembic_version` - Migration tracking

## Environment Variables

- `MP_DB` - Core database path (default: `data/db/core.db`)
- `METRICS_DB_PATH` - Metrics database path
- `PROMETHEUS_MULTIPROC_DIR` - Multiprocess metrics directory
- `METRICS_MAX_CONNECTIONS` - Server connection limit (default: 100) 