# AGENTS.md

AI agent guidance for MarketPipe - A Domain-Driven Financial ETL Pipeline

## Architecture Overview

MarketPipe is a **Domain-Driven Design (DDD)** financial ETL pipeline organized into bounded contexts:

### Bounded Contexts
- **Data Ingestion** (Core Domain): Orchestrates market data collection 
- **Market Data Integration** (Supporting): External API abstractions
- **Data Validation** (Supporting): Quality and business rule enforcement
- **Data Storage** (Supporting): Partitioned time-series persistence

### Core Pipeline Flow
1. **Ingestion**: `IngestionCoordinator` orchestrates threaded collection
2. **Validation**: `SchemaValidator` enforces business rules
3. **Storage**: Partitioned Parquet files (`data/symbol=X/date=Y.parquet`)
4. **Metrics**: Prometheus metrics with Grafana dashboards

## Ubiquitous Language

Use these exact terms consistently across all code:

**Financial Domain:**
- `Symbol` (not ticker/security): Stock identifier (AAPL, GOOGL)
- `OHLCVBar` (not candle/quote): Open, High, Low, Close, Volume data
- `Trading Date` (not business date): Market timezone date
- `Market Data Provider` (not vendor/source): External data sources

**Processing Domain:**
- `Ingestion` (not import/fetch): Data collection process
- `Validation` (not verification): Business rule checking
- `Partition` (not shard): Data organization unit
- `Checkpoint` (not bookmark): Resumable progress marker

**Time Domain:**
- `Timestamp`: Specific UTC moment 
- `TimeRange`: Start/end period
- `TimeFrame`: Aggregation period (1m, 5m, 1h, 1d)
- `TradingSession`: Regular, pre-market, post-market

## Code Standards

### Modern Python Patterns
```python
from __future__ import annotations  # Always first import

from typing import Dict, List, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod

@dataclass
class ClientConfig:
    """Type-safe configuration with validation."""
    api_key: str
    base_url: str
    timeout: float = 30.0
    
    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("api_key is required")

class BaseApiClient(ABC):
    """Abstract vendor-agnostic client."""
    
    @abstractmethod
    def parse_response(self, raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform vendor data to canonical OHLCV schema."""
```

### DDD Entity Patterns
```python
@dataclass(frozen=True)
class EntityId:
    value: UUID
    
    @classmethod
    def generate(cls) -> EntityId:
        return cls(uuid4())

class OHLCVBar(Entity):
    """Domain entity with business behavior."""
    
    def __init__(self, id: EntityId, symbol: Symbol, timestamp: Timestamp, ...):
        super().__init__(id)
        self._symbol = symbol
        self._validate_ohlc_consistency()
    
    def _validate_ohlc_consistency(self) -> None:
        """Business rule validation."""
        if not (self._high >= self._open and self._high >= self._close):
            raise ValueError("OHLC prices are inconsistent")

@dataclass(frozen=True)
class Symbol:
    """Value object for stock symbols."""
    value: str
    
    def __post_init__(self):
        if not self.value.isalpha() or len(self.value) > 10:
            raise ValueError(f"Invalid symbol: {self.value}")
```

### Connector Architecture
All API connectors must:

1. **Inherit from `BaseApiClient`**
2. **Implement required methods:**
   - `build_request_params()`: Vendor-specific query parameters
   - `parse_response()`: Transform to canonical OHLCV schema
   - `should_retry()`: Vendor-specific retry logic
   - `endpoint_path()`: API endpoint path
   - `next_cursor()`: Extract pagination token

3. **Return canonical schema:**
```python
def parse_response(self, raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform vendor response to canonical format."""
    return [{
        "symbol": bar["symbol"],
        "timestamp": parse_timestamp(bar["timestamp"]),
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": float(bar["close"]),
        "volume": int(bar["volume"]),
        "schema_version": 1,
        "source": "alpaca",
        "frame": "1m"
    } for bar in raw_json.get("bars", [])]
```

4. **Support async/sync dual patterns:**
```python
def fetch_batch(self, symbol: str, start_ts: int, end_ts: int) -> List[Dict[str, Any]]:
    """Synchronous batch fetch."""
    
async def async_fetch_batch(self, symbol: str, start_ts: int, end_ts: int) -> List[Dict[str, Any]]:
    """Asynchronous batch fetch."""
```

### Validation Patterns
```python
class SchemaValidator:
    """Multi-level OHLCV validation."""
    
    def validate_batch(self, rows: List[Dict[str, Any]], symbol: str) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Validate with comprehensive error reporting."""
        valid_rows, errors = [], []
        
        for i, row in enumerate(rows):
            # Level 1: Schema validation
            schema_errors = self._validate_schema(row)
            # Level 2: Business rules
            business_errors = self._validate_business_rules(row)
            # Level 3: Data quality
            quality_errors = self._validate_data_quality(row, symbol)
            
            all_errors = schema_errors + business_errors + quality_errors
            if all_errors:
                errors.extend([f"Row {i}: {err}" for err in all_errors])
            else:
                valid_rows.append(row)
        
        return valid_rows, errors
```

### Error Handling
```python
# Specific exceptions with domain context
class InvalidOHLCVDataError(ValueError):
    """OHLCV data violates business rules."""

class MarketDataProviderError(Exception):
    """Market data provider communication error."""

# Comprehensive error handling
try:
    response_json = response.json()
except (json.JSONDecodeError, ValueError) as e:
    self.log.warning(f"Failed to parse JSON: {e}. Status: {response.status_code}")
    if self.should_retry(response.status_code, {}):
        continue
    else:
        raise RuntimeError(f"Failed to parse API response: {response.text}")
```

### Configuration Management
```python
# YAML configuration with environment variables
@dataclass
class PipelineConfig:
    symbols: List[str]
    start: str
    end: str
    output_path: str
    compression: str = "snappy"
    workers: int = 3

def load_config(config_path: str) -> PipelineConfig:
    """Load config with environment variable support."""
    load_dotenv()  # Load .env file
    
    # Get credentials from environment
    api_key = os.getenv("ALPACA_KEY") or config_dict.get("key", "")
    if not api_key:
        raise ValueError("ALPACA_KEY not found in environment or config")
```

### Testing Patterns
```python
def test_alpaca_pagination(monkeypatch):
    """Test pagination with descriptive name."""
    pages = [{"bars": {"AAPL": [mock_bar_1]}, "next_page_token": "abc"}]
    
    def mock_get(url, params=None, headers=None, timeout=None):
        return MockResponse(status_code=200, json_data=pages.pop(0))
    
    monkeypatch.setattr(httpx, "get", mock_get)
    
    config = ClientConfig(api_key="test", base_url="https://api.test.com")
    client = AlpacaClient(config=config, auth=HeaderTokenAuth("key", "secret"))
    
    rows = client.fetch_batch("AAPL", 0, 1000)
    assert len(rows) == 1
    assert rows[0]["schema_version"] == 1
```

### Metrics Integration
```python
from prometheus_client import Counter, Histogram

REQUESTS = Counter('marketpipe_requests_total', 'API requests', ['vendor'])
ERRORS = Counter('marketpipe_errors_total', 'API errors', ['vendor', 'status_code'])
LATENCY = Histogram('marketpipe_request_duration_seconds', 'Request latency', ['vendor'])

# In client methods
def _request(self, params: Mapping[str, str]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    try:
        response = httpx.get(url, params=params)
        REQUESTS.labels(vendor="alpaca").inc()
        return response.json()
    except Exception as e:
        ERRORS.labels(vendor="alpaca", status_code="exception").inc()
        raise
    finally:
        duration = time.perf_counter() - start_time
        LATENCY.labels(vendor="alpaca").observe(duration)
```

## Development Commands

```bash
# Installation
pip install -e .

# CLI usage
marketpipe ingest --config config/example_config.yaml
marketpipe metrics --port 8000

# Testing
python -m pytest tests/ -v
python -m pytest tests/test_alpaca_client.py::test_pagination

# Module execution
python -m marketpipe.ingestion config/example_config.yaml
```

## Key Anti-Patterns to Avoid

❌ **Domain Model Violations:**
- Using technical terms in domain layer (`DatabaseRecord` → use domain entities)
- Generic naming (`Data`, `Item` → use specific domain concepts)
- Cross-context dependencies (direct imports between bounded contexts)

❌ **Code Style Issues:**
- Missing `from __future__ import annotations`
- Broad exception handling (`except Exception`)
- Missing type hints
- Inconsistent terminology (ticker vs symbol)

❌ **Architecture Violations:**
- Skipping abstract method implementation in connectors
- Missing schema validation
- Direct database access from domain layer
- Tight coupling between bounded contexts

## File Organization

```
src/marketpipe/
├── domain/                    # Shared domain models
│   ├── entities.py           # Base Entity, EntityId
│   ├── value_objects.py      # Symbol, Price, Timestamp, Volume
│   └── events.py             # Domain events
├── ingestion/                # Data Ingestion Context
│   ├── domain/               # Ingestion entities/aggregates
│   ├── services/             # Application services
│   └── infrastructure/       # External integrations
├── integration/              # Market Data Integration Context
│   └── connectors/           # API client implementations
├── validation/               # Data Quality Context
├── storage/                  # Data Storage Context
└── metrics/                  # Monitoring Context
```

Focus on **domain-driven design**, **ubiquitous language**, and **bounded context separation** when making any changes to the codebase. 