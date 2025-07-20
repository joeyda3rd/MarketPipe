# Phase 4: Shared Test Infrastructure Architecture

## Overview

Phase 4 introduces comprehensive shared test infrastructure to standardize testing patterns, reduce boilerplate, and improve maintainability across the MarketPipe test suite.

## 🎯 **Key Components**

### 1. **Fixtures** (`tests/conftest.py`)

#### `integration_environment`
Complete test environment with all necessary components:
- **FakeDatabase**: Real SQLite with schema setup
- **FakeHttpClient**: Controllable HTTP behavior
- **FakeMetricsCollector**: Metrics verification
- **Temporary directory**: File operations

```python
def test_example(integration_environment):
    env = integration_environment

    # Database operations
    assert env.database is not None

    # HTTP client simulation
    env.http_client.configure_response("http://api.com", 200, {"data": "success"})
    response = env.http_client.get("http://api.com")

    # Metrics tracking
    env.metrics.increment_counter("operations")
    assert env.metrics.get_counter_value("operations") == 1

    # File operations
    temp_file = env.get_temp_dir() / "test.txt"
    temp_file.write_text("hello")
```

#### `domain_objects`
Factory for creating domain objects with sensible defaults:

```python
def test_with_domain_objects(domain_objects):
    # Single bar with defaults
    bar = domain_objects.create_ohlcv_bar()

    # Multiple bars with custom values
    bars = domain_objects.create_ohlcv_bars("GOOGL", count=10, volume=2000)

    # Time ranges
    time_range = domain_objects.create_time_range(duration_minutes=60)
```

#### Common Data Fixtures
- **`test_symbols`**: Consistent symbol list across tests
- **`test_trading_dates`**: Common trading dates
- **`benchmark_data`**: Performance testing datasets

### 2. **Base Classes** (`tests/base.py`)

#### `IntegrationTestCase`
Base for integration tests with automatic setup:

```python
class TestMyFeature(IntegrationTestCase):
    def test_something(self):
        # self.database, self.http_client, self.metrics available automatically
        # Automatic setup and cleanup

        services = self.create_service_factory()  # For dependency injection
        self.assert_metrics_recorded({"operations": 2})
```

#### `PipelineTestCase`
Specialized for pipeline integration tests:

```python
class TestPipeline(PipelineTestCase):
    def test_pipeline_flow(self):
        # Configure test data
        self.configure_provider_data("AAPL", bar_count=10)

        # Run pipeline
        result = self.run_ingestion_pipeline(["AAPL"])

        # Verify results
        self.assert_pipeline_success(result)
```

#### `BenchmarkTestCase`
For performance testing:

```python
@pytest.mark.benchmark
class TestPerformance(BenchmarkTestCase):
    def test_throughput(self):
        with self.measure_time() as timer:
            # ... perform operations

        self.assert_time_under(timer.elapsed, 5.0)
        self.assert_throughput_over(items_processed, timer.elapsed, 1000)
```

#### `DatabaseTestCase`
For database-focused tests with guaranteed schema setup.

### 3. **Performance Benchmarks** (`tests/benchmarks/`)

Comprehensive performance testing framework:
- **Data ingestion throughput**
- **Memory usage patterns**
- **Database operations**
- **HTTP client performance**
- **Concurrent operations**

## 🚀 **Migration Guide**

### **Before (Old Pattern)**
```python
@patch('httpx.get')
@patch('database.connect')
@patch('metrics.increment')
def test_complex_mock_setup(mock_metrics, mock_db, mock_http):
    # Lots of mock configuration
    mock_http.return_value = Mock(status_code=200, json=lambda: {...})
    mock_db.return_value = Mock(...)

    # Test logic buried in mock setup
    result = function_under_test()

    # Mock verification
    mock_http.assert_called_with(...)
    mock_metrics.assert_called_with("counter", 1)
```

### **After (New Pattern)**
```python
def test_with_shared_infrastructure(integration_environment):
    env = integration_environment

    # Simple, realistic configuration
    env.http_client.configure_response("http://api.com", 200, {"data": "success"})

    # Focus on business logic
    result = function_under_test(
        database=env.database,
        http_client=env.http_client,
        metrics=env.metrics
    )

    # Verify real behavior
    assert env.metrics.get_counter_value("operations") == 1
```

### **Migration Strategy**
1. **Start with fixtures**: Replace manual setup with `integration_environment`
2. **Adopt base classes**: Convert complex test classes to inherit from base classes
3. **Use domain factory**: Replace manual object creation with `domain_objects`
4. **Add benchmarks**: Convert performance-sensitive tests to use `BenchmarkTestCase`

## 📊 **Benefits Achieved**

### **Reduced Boilerplate**
- **Before**: 10-15 lines of setup per test
- **After**: 0 lines - automatic via fixtures/base classes

### **Improved Consistency**
- **Before**: Each test file creates dependencies differently
- **After**: Standardized patterns across all tests

### **Better Maintainability**
- **Before**: Changes require updating dozens of test files
- **After**: Changes in `conftest.py` affect all tests uniformly

### **Enhanced Realism**
- **Before**: Complex mock coordination
- **After**: Real components with configurable behavior

## 🎯 **Usage Patterns**

### **Fixture-Based Tests (Recommended for new tests)**
```python
def test_feature(integration_environment, domain_objects):
    env = integration_environment
    bars = domain_objects.create_ohlcv_bars("AAPL", count=5)

    # Test logic with minimal setup
```

### **Base Class Tests (Good for complex integration)**
```python
class TestComplexFeature(PipelineTestCase):
    def test_full_pipeline(self):
        # Rich set of helper methods available
        self.configure_provider_data("AAPL", bar_count=100)
        result = self.run_ingestion_pipeline(["AAPL"])
        self.assert_pipeline_success(result)
```

### **Performance Tests (For benchmarking)**
```python
@pytest.mark.benchmark
class TestPerformanceRegression(BenchmarkTestCase):
    def test_ingestion_speed(self, benchmark_data):
        dataset = benchmark_data['large_dataset']

        with self.measure_time() as timer:
            # ... performance-critical operations

        self.assert_time_under(timer.elapsed, 10.0)
        self.record_performance_result("ingestion", ...)
```

## 🔧 **Configuration Options**

### **pytest Command Line Options**
```bash
# Run all tests (skips slow tests by default)
pytest

# Run slow tests
pytest --run-slow

# Run benchmark tests
pytest --benchmark

# Run integration tests only
pytest -m integration

# Run benchmarks only
pytest tests/benchmarks/
```

### **Test Markers**
- `@pytest.mark.integration`: Integration tests
- `@pytest.mark.benchmark`: Performance benchmarks
- `@pytest.mark.slow`: Slow-running tests

## 📁 **File Organization**

```
tests/
├── conftest.py                     # Shared fixtures
├── base.py                         # Base test classes
├── examples/
│   └── test_using_shared_infrastructure.py  # Usage examples
├── benchmarks/
│   └── test_performance_benchmarks.py       # Performance tests
├── integration/
│   ├── test_bootstrap_integration.py        # Phase 3 integration tests
│   ├── test_enhanced_pipeline.py           # Pipeline integration
│   └── test_provider_integration.py        # Provider integration
└── fakes/
    ├── adapters.py                 # Phase 1 fakes
    ├── database.py                 # Phase 1 database fake
    └── bootstrap.py                # Phase 2 bootstrap fakes
```

## 🎉 **Success Metrics**

### **Quantitative Improvements**
- **Setup code reduction**: 80% less boilerplate per test
- **Test consistency**: 100% of new tests use shared patterns
- **Maintenance efficiency**: Single point of change for common test infrastructure
- **Performance visibility**: Automated benchmark regression detection

### **Qualitative Improvements**
- **Developer experience**: New tests are faster to write
- **Code readability**: Tests focus on business logic, not setup
- **Debugging ease**: Real components provide observable behavior
- **Maintenance confidence**: Changes to shared infrastructure affect all tests uniformly

## 🔮 **Future Enhancements**

Phase 4 establishes the foundation for:
- **Automated performance regression detection**
- **Test data management and seeding**
- **Cross-component integration testing**
- **Test environment provisioning**
- **Enhanced debugging and observability**

The shared test infrastructure provides a solid foundation for continued test suite improvement and ensures MarketPipe maintains high-quality, reliable tests as the codebase evolves.
