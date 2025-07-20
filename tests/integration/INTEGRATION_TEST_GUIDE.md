# Integration Test Strategy Guide

## Overview

This guide explains when to use integration tests vs unit tests in MarketPipe, based on lessons learned from the test infrastructure refactoring.

## Test Type Decision Matrix

### Use Integration Tests When:

#### ✅ **Multi-Component Interactions**
```python
# GOOD: Tests cross-component behavior
def test_full_ingestion_pipeline_with_validation():
    """Integration test: provider → coordinator → storage → validation"""
    provider = FakeMarketDataAdapter() 
    coordinator = IngestionCoordinatorService(provider, database, metrics)
    result = coordinator.ingest_symbol_data(symbol, time_range)
    # Tests real component interactions
```

#### ✅ **External System Integration**
```python 
# GOOD: Tests real database operations
def test_bootstrap_with_real_migrations():
    orchestrator = BootstrapOrchestrator(
        migration_service=AlembicMigrationService(),  # Real migrations!
        service_registry=FakeServiceRegistry(),
        environment_provider=env_provider
    )
    # Tests actual database migration behavior
```

#### ✅ **Complex State Management** 
```python
# GOOD: Tests stateful behavior across operations
def test_bootstrap_idempotent_with_real_database():
    result1 = orchestrator.bootstrap()  # First bootstrap
    result2 = orchestrator.bootstrap()  # Second bootstrap
    assert result2.was_already_bootstrapped  # Real state persistence
```

#### ✅ **Error Propagation Across Boundaries**
```python
# GOOD: Tests how errors flow through system
def test_pipeline_with_partial_api_failures():
    provider.configure_error(Symbol("BADSTOCK"), ValueError("Unknown symbol"))
    # Tests realistic failure scenarios and recovery
```

#### ✅ **Performance and Resource Usage**
```python
# GOOD: Tests real resource consumption
def test_pipeline_memory_usage_with_large_datasets():
    # Process 10,000 bars and measure actual memory usage
    # Tests real performance characteristics
```

### Use Unit Tests When:

#### ✅ **Single Component Logic**
```python
# GOOD: Tests single domain object behavior  
def test_ohlcv_bar_validates_price_consistency():
    # Tests a single entity's validation rules
    with pytest.raises(ValueError, match="OHLC prices are inconsistent"):
        OHLCVBar(...)  # Invalid price combination
```

#### ✅ **Pure Functions**
```python
# GOOD: Tests stateless transformation
def test_timestamp_conversion_to_nanoseconds():
    timestamp = Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc))
    assert timestamp.to_nanoseconds() == 1705312200000000000
```

#### ✅ **Algorithm Implementation**
```python  
# GOOD: Tests specific algorithm logic
def test_exponential_backoff_calculation():
    backoff = AlpacaClient._backoff(attempt=3)
    assert 3.0 <= backoff <= 5.0  # Tests backoff algorithm
```

#### ✅ **Validation Rules**
```python
# GOOD: Tests individual business rules
def test_symbol_validation_rejects_invalid_formats():
    with pytest.raises(ValueError):
        Symbol("123INVALID")  # Tests validation logic
```

## Integration Test Patterns

### Pattern 1: Real Database + Fake Services

```python
def test_with_real_database():
    """Use real database for persistence, fake services for control."""
    orchestrator = BootstrapOrchestrator(
        migration_service=AlembicMigrationService(),  # REAL
        service_registry=FakeServiceRegistry(),       # FAKE - controllable
        environment_provider=FakeEnvironmentProvider()  # FAKE - configurable
    )
    # Tests real database behavior with controlled service interactions
```

**Benefits:**
- Tests actual database migrations and schema
- Catches real SQL and constraint issues
- Controllable service behavior for specific scenarios

### Pattern 2: Real HTTP + Controlled Responses

```python 
def test_with_real_http_server():
    """Use real HTTP server for network behavior, control responses."""
    mock_server = MockHTTPServer()  # Real HTTP server
    mock_server.configure_response("AAPL", bars_data)  # Controlled responses
    
    client = AlpacaClient(config=ClientConfig(base_url=mock_server.get_url()))
    # Tests real HTTP client behavior with controlled server responses
```

**Benefits:**
- Tests actual HTTP client behavior (retries, timeouts, headers)
- Tests realistic network conditions 
- Controlled, repeatable responses

### Pattern 3: Dependency Injection with Mixed Real/Fake

```python
def test_mixed_real_fake_components():
    """Mix real and fake components as needed."""
    coordinator = IngestionCoordinatorService(
        market_data_provider=FakeMarketDataAdapter(),  # FAKE - controlled data
        database=FakeDatabase(),                       # FAKE but realistic
        metrics_collector=RealMetricsCollector(),      # REAL - actual metrics
        http_client=RealHttpClient()                   # REAL - actual HTTP
    )
    # Tests real metrics and HTTP with controlled data and storage
```

**Benefits:**
- Test only the components you need to be real
- Controlled testing environment
- Realistic behavior where it matters

## Anti-Patterns to Avoid

### ❌ Integration Tests with Excessive Mocking

```python
# BAD: Integration test that mocks everything
@patch('marketpipe.storage.write')
@patch('marketpipe.validation.validate') 
@patch('marketpipe.provider.fetch')
def test_pipeline_all_mocked():
    # This is not an integration test - it's a complex unit test!
```

**Problem:** If you're mocking all the integrations, it's not testing integration.

### ❌ Unit Tests with Real External Dependencies

```python
# BAD: Unit test that hits real database
def test_symbol_entity():
    # Insert into actual database
    db.execute("INSERT INTO symbols...")
    symbol = Symbol.load_from_db(db, "AAPL")  # Real DB access
```

**Problem:** Unit tests should be isolated and fast.

### ❌ Testing Implementation Details in Integration Tests

```python
# BAD: Integration test checking internal method calls
def test_pipeline_calls_private_methods():
    coordinator.process_data(...)
    assert coordinator._internal_method.called  # Testing implementation
```

**Problem:** Integration tests should test behavior, not implementation.

## Test Performance Guidelines

### Integration Test Performance

**Target:** Integration tests should complete in < 5 seconds each

**Strategies:**
- Use in-memory databases (SQLite `:memory:`)
- Use small, focused test datasets
- Use fake HTTP clients instead of real network calls when possible
- Limit concurrent operations in tests

### Unit Test Performance  

**Target:** Unit tests should complete in < 100ms each

**Strategies:**
- No I/O operations (database, file, network)
- No external dependencies
- Minimal object creation
- Focus on single component

## Test Data Management

### For Integration Tests

```python
@pytest.fixture
def integration_environment():
    """Complete test environment with realistic data."""
    db = FakeDatabase()
    http_client = FakeHttpClient()
    
    # Seed with realistic test data
    test_bars = create_test_ohlcv_bars(
        symbol="AAPL",
        count=100,
        start_time=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
    )
    
    return IntegrationTestEnvironment(db, http_client, test_bars)
```

### For Unit Tests

```python
def test_price_calculation():
    """Unit test with minimal test data."""
    bar = OHLCVBar(
        symbol=Symbol("AAPL"),
        open_price=Price.from_float(100.0),
        # ... minimal required data
    )
    # Test specific calculation with minimal setup
```

## Error Testing Strategies

### Integration Error Testing

```python  
def test_pipeline_handles_database_failure():
    """Test error propagation across components."""
    db = FakeDatabase()
    db.configure_failure_after_operations(5)  # Fail after 5 operations
    
    coordinator = IngestionCoordinatorService(provider, db, metrics)
    
    # Should handle database failure gracefully
    with pytest.raises(DatabaseError):
        coordinator.process_large_dataset(...)
    
    # Verify partial progress was made
    assert metrics.get_counter_value("bars_processed") == 5
```

### Unit Error Testing

```python
def test_price_validation_error():
    """Test specific validation error.""" 
    with pytest.raises(ValueError, match="Price cannot be negative"):
        Price.from_float(-10.0)
```

## Debugging Integration vs Unit Tests

### Integration Test Debugging

**When integration tests fail:**
1. Check test environment setup (database, HTTP server)
2. Verify test data configuration 
3. Check component interaction logs
4. Ensure proper cleanup between tests

**Debugging tools:**
- Enable logging for all components
- Use database inspection tools
- Check HTTP request/response logs
- Verify metrics collection

### Unit Test Debugging

**When unit tests fail:**
1. Check input data validity
2. Verify expected vs actual output
3. Check business logic implementation
4. Ensure pure function behavior

**Debugging tools:**
- Print/log input and output values
- Use debugger for step-through
- Verify test assertions
- Check edge case handling

## Migration Strategy

### Converting Mock-Heavy Tests to Integration

1. **Identify integration points** - what components interact?
2. **Choose real vs fake components** - what needs to be real for confidence?  
3. **Set up test environment** - database, HTTP server, etc.
4. **Replace mocks with fakes** - more realistic test doubles
5. **Verify same coverage** - ensure new tests cover same scenarios
6. **Add new scenarios** - integration tests can test things mocks couldn't

### Example Migration

```python
# BEFORE: Mock-heavy test
@patch('httpx.get')
@patch('database.save')
@patch('validator.validate')
def test_ingestion_old(mock_val, mock_save, mock_get):
    mock_get.return_value = Mock(status_code=200, json=lambda: {...})
    mock_val.return_value = ValidationResult(valid=True)
    
    result = ingest_data("AAPL")
    
    mock_get.assert_called_once()
    mock_save.assert_called_once()
    # Can only test mock interactions, not real behavior

# AFTER: Integration test
def test_ingestion_integration():
    """Integration test with realistic components."""
    http_client = FakeHttpClient()
    http_client.configure_response(r".*", body={...})  # Realistic response
    
    database = FakeDatabase()  # Real SQLite operations
    validator = RealValidator()  # Real validation logic
    
    coordinator = IngestionCoordinator(http_client, database, validator)
    result = coordinator.ingest_data("AAPL")
    
    # Can test real behavior
    assert result.bars_processed == 10
    stored_bars = database.query_bars("AAPL")  # Real database query
    assert len(stored_bars) == 10
    # Tests actual business behavior!
```

## Summary

**Integration Tests:** Use for testing component interactions, external system behavior, and end-to-end scenarios. Focus on business behavior and realistic data flows.

**Unit Tests:** Use for testing individual component logic, pure functions, and isolated business rules. Focus on fast, isolated verification.

**Key Principle:** Choose the test type that gives you the most confidence in the behavior you're trying to verify, while keeping tests maintainable and fast.

The MarketPipe test infrastructure refactoring shows that **well-designed integration tests using dependency injection and realistic fakes can provide much higher confidence than complex mock-based tests**, while still being maintainable and reasonably fast. 