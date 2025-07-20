# Test Fakes Documentation

This directory contains sophisticated fake implementations for testing MarketPipe components without mocking. These fakes provide realistic behavior while remaining controllable and deterministic for testing.

## Philosophy

Instead of mocking collaborators with `Mock` objects, these fakes implement the actual interfaces and protocols while providing test-specific configuration and verification capabilities. This approach:

- **Reduces test brittleness**: Tests don't break when implementation details change
- **Improves test realism**: Fakes behave more like real components  
- **Enhances maintainability**: Shared fakes can be improved to benefit all tests
- **Enables better verification**: Fakes can provide rich verification APIs

## Available Fakes

### FakeHttpClient (`adapters.py`)

Replaces monkeypatching `httpx` with a configurable fake HTTP client.

**Features:**
- Configure responses for URL patterns
- Simulate errors, delays, and rate limiting  
- Capture request history for verification
- Support both sync and async operations

**Example Usage:**
```python
from tests.fakes.adapters import FakeHttpClient

def test_api_client_handles_errors():
    http_client = FakeHttpClient()
    
    # Configure error response
    http_client.configure_error(
        url_pattern=r".*/stocks/bars",
        error=httpx.TimeoutException("Request timeout")
    )
    
    client = AlpacaClient(config=config, http_client=http_client)
    
    with pytest.raises(httpx.TimeoutException):
        client.fetch_bars("AAPL", time_range)
    
    # Verify request was made
    requests = http_client.get_requests_made()
    assert len(requests) == 1
    assert "stocks/bars" in requests[0].url
```

**Benefits:**
- Eliminates monkeypatching of `httpx.get()` 
- Provides realistic HTTP behavior simulation
- Enables verification without coupling to internals
- Supports complex scenarios (rate limiting, pagination)

### FakeDatabase (`database.py`)

Provides real SQLite database with isolation for testing database-dependent code.

**Features:**
- Real SQLite database with migrations applied
- Automatic cleanup and isolation between tests
- Support for test data seeding
- Environment variable management

**Example Usage:**
```python
from tests.fakes.database import FakeDatabase, DatabaseEnvironment

async def test_bootstrap_with_real_database():
    db = FakeDatabase()
    await db.setup_schema()
    
    with DatabaseEnvironment(db):
        # Environment variables set for services
        result = bootstrap()
        assert result.success
        
        # Can verify real database state
        # No mocking of database operations needed
```

**Benefits:**
- Tests real database behavior (transactions, constraints, migrations)
- Catches actual database issues in tests
- Eliminates complex database operation mocking
- Supports both file-based and in-memory databases

### FakeMarketDataAdapter (`adapters.py`)

Enhanced fake market data provider supporting various test scenarios.

**Features:**
- Configure symbol data responses
- Simulate provider errors and rate limiting
- Track request history for verification  
- Support pagination and edge cases

**Example Usage:**
```python
from tests.fakes.adapters import FakeMarketDataAdapter

def test_ingestion_handles_provider_errors():
    provider = FakeMarketDataAdapter("test_provider")
    
    # Configure different responses per symbol
    provider.configure_symbol_data("AAPL", [sample_bars])
    provider.configure_error("GOOGL", ConnectionError("Network error"))
    
    result = ingestion_service.ingest_symbols(["AAPL", "GOOGL"])
    
    assert result.successful_symbols == ["AAPL"]
    assert result.failed_symbols == ["GOOGL"]
    
    # Verify request patterns
    history = provider.get_request_history()
    assert len(history) == 2
```

**Benefits:**
- Tests real market data scenarios without external dependencies
- Supports error simulation and edge case testing
- Eliminates complex provider mocking
- Enables testing of provider-specific behavior

### FakeMetricsCollector (`metrics.py`)

In-memory metrics collection for testing metrics behavior without Prometheus.

**Features:**
- Record counter, histogram, and gauge operations
- Query metrics for test verification
- Built-in assertion helpers
- Integration with `prometheus_client` patching

**Example Usage:**
```python
from tests.fakes.metrics import FakeMetricsCollector, patch_prometheus_metrics

def test_service_emits_metrics():
    metrics = FakeMetricsCollector()
    
    with patch_prometheus_metrics(metrics):
        # Service uses prometheus_client normally
        service = IngestionService()
        service.process_symbols(["AAPL"])
        
        # Verify metrics were emitted
        metrics.assert_counter_incremented("symbols_processed", {"status": "success"})
        metrics.assert_histogram_observed("processing_duration")
        
        assert metrics.get_counter_value("symbols_processed", {"status": "success"}) == 1
```

**Benefits:**
- Eliminates prometheus_client mocking
- Provides rich verification capabilities
- Supports realistic metrics testing
- Enables testing of metrics-driven functionality

## Migration Guide

### Converting from Mocks to Fakes

**Before (using mocks):**
```python
def test_client_retries_on_error(monkeypatch):
    call_count = 0
    
    def mock_get(url, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise httpx.TimeoutException("Timeout")
        return MockResponse(200, {"data": []})
    
    monkeypatch.setattr(httpx, "get", mock_get)
    
    client = AlpacaClient(config)
    result = client.fetch_bars("AAPL", time_range)
    
    assert call_count == 2  # Implementation detail!
```

**After (using fakes):**
```python  
def test_client_retries_on_error():
    http_client = FakeHttpClient()
    
    # Configure timeout then success
    http_client.configure_error(r".*/bars", httpx.TimeoutException("Timeout"))
    http_client.configure_response(r".*/bars", 200, {"data": []})
    
    client = AlpacaClient(config, http_client=http_client)
    result = client.fetch_bars("AAPL", time_range)
    
    # Test behavior, not implementation
    assert result == []
    requests = http_client.get_requests_made()
    assert len(requests) == 2  # Verify retry happened
```

**Key Differences:**
- Fake configures expected behavior, not implementation details
- Test verifies business behavior, not internal mechanics
- Fake can be reused across multiple tests
- Test doesn't break when retry logic changes

### Converting Bootstrap Tests

**Before (mocking everything):**
```python
@patch('marketpipe.bootstrap.apply_pending_alembic')
@patch('marketpipe.bootstrap.ValidationRunnerService.register')  
@patch('marketpipe.bootstrap.AggregationRunnerService.register')
def test_bootstrap_idempotent(mock_agg, mock_val, mock_alembic):
    mock_alembic.return_value = None
    mock_val.return_value = None
    mock_agg.return_value = None
    
    bootstrap()
    bootstrap()  # Second call
    
    # Test mocks, not behavior
    assert mock_alembic.call_count == 2
```

**After (using fake database):**
```python
async def test_bootstrap_idempotent():
    db = FakeDatabase()
    await db.setup_schema()
    
    with DatabaseEnvironment(db):
        result1 = bootstrap()
        result2 = bootstrap()
        
        # Test actual behavior
        assert result1.success
        assert result2.success
        assert result2.was_already_configured
```

## Best Practices

### When to Use Fakes vs Mocks

**Use Fakes When:**
- Testing integration between components
- Collaborator has stable, well-defined interface
- You want to test realistic behavior
- Multiple tests need similar collaborator behavior
- Collaborator behavior is complex (HTTP, database, etc.)

**Use Mocks When:**  
- Testing a single unit in isolation
- Collaborator interface is simple or unstable
- You need to verify specific method calls
- Testing error conditions that are hard to simulate

### Fake Design Guidelines

1. **Implement Real Interfaces**: Fakes should implement the same interfaces as real components
2. **Provide Configuration APIs**: Allow tests to configure fake behavior
3. **Include Verification APIs**: Provide ways to verify interactions
4. **Support Realistic Scenarios**: Handle edge cases like errors, timeouts
5. **Maintain Simplicity**: Don't replicate all complexity of real component

### Common Patterns

#### Test Fixture Setup
```python
@pytest.fixture
def configured_environment():
    """Complete test environment with all fakes."""
    database = FakeDatabase()
    http_client = FakeHttpClient()
    metrics = FakeMetricsCollector()
    provider = FakeMarketDataAdapter()
    
    return TestEnvironment(database, http_client, metrics, provider)
```

#### Service Factory Pattern
```python
class TestServiceFactory:
    def __init__(self, test_environment: TestEnvironment):
        self.test_env = test_environment
        
    def create_ingestion_service(self) -> IngestionService:
        return IngestionService(
            provider=self.test_env.provider,
            database=self.test_env.database,
            metrics=self.test_env.metrics
        )
```

#### Verification Pattern
```python
def test_end_to_end_workflow(configured_environment):
    # Arrange
    env = configured_environment
    env.provider.configure_symbol_data("AAPL", [sample_bars])
    
    service = TestServiceFactory(env).create_ingestion_service()
    
    # Act
    result = service.process_symbols(["AAPL"])
    
    # Assert - verify behavior, not implementation
    assert result.success
    
    # Verify interactions through fake APIs
    assert env.metrics.get_counter_value("symbols_processed") == 1
    assert len(env.provider.get_request_history()) == 1
    # Could verify database state if needed
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure fake modules are importable from test files
2. **Database Schema Issues**: Make sure migrations are applied in `FakeDatabase.setup_schema()`
3. **HTTP Client Injection**: Services need to accept HTTP client via constructor
4. **Metrics Patching**: Use `patch_prometheus_metrics` context manager correctly

### Debugging Tips

1. **Enable Request Logging**: Use fake's request history to debug HTTP interactions
2. **Check Metric Observations**: Use `get_all_observations()` to see what metrics were recorded
3. **Database Inspection**: Connect to fake database file to inspect state
4. **Pattern Matching**: Verify URL patterns in `FakeHttpClient` are correct

## Future Enhancements

Planned improvements to the fake implementations:

1. **Enhanced HTTP Server**: Real HTTP server for more realistic integration tests  
2. **Database Seeding**: Pre-built datasets for common test scenarios
3. **Provider Sandboxes**: Integration with provider sandbox/test APIs
4. **Performance Monitoring**: Built-in performance benchmarking capabilities
5. **Configuration Validation**: Validate fake configurations match real component behavior

## Contributing

When adding new fakes:

1. Follow the established patterns in existing fakes
2. Provide comprehensive documentation and examples
3. Include verification APIs for test assertions
4. Support realistic behavior simulation
5. Add pytest fixtures for easy use
6. Include example usage in docstrings 