# Test Infrastructure Refactoring Plan

## Executive Summary

This document outlines a comprehensive plan to address the test infrastructure issues identified in the MarketPipe codebase audit. The plan focuses on reducing fragile mock usage, improving testability, and creating more reliable test patterns.

## Audit Findings Summary

The audit revealed several critical patterns of test fragility:
- **Overused mocks**: Tests mocking 5+ collaborators simultaneously
- **Implementation detail testing**: Tests coupled to private methods and internal structure
- **Missed fake opportunities**: Mocking stable components that could use real implementations
- **Design issues**: Production code requiring excessive mocking due to tight coupling

## Phase 1: Create Better Test Doubles (Weeks 1-2)

### Objective
Establish reusable, realistic test doubles to replace brittle mocks.

### Tasks

#### 1.1 Extract and Enhance FakeHttpClient
**Location**: `tests/fakes/adapters.py`

**Current State**: Various tests create one-off mock HTTP clients
**Target**: Shared, configurable fake HTTP client

```python
class FakeHttpClient:
    """Configurable fake HTTP client for testing."""

    def __init__(self):
        self.responses: Queue[ResponseSpec] = Queue()
        self.requests_made: List[RequestCapture] = []
        self.delay_simulation: Optional[float] = None

    def configure_response(self,
                          url_pattern: str,
                          status: int = 200,
                          body: Dict[str, Any] = None,
                          headers: Dict[str, str] = None,
                          delay: Optional[float] = None):
        """Configure expected response for URL pattern."""

    def configure_error(self, url_pattern: str, error_type: Exception):
        """Configure error response for URL pattern."""

    async def get(self, url: str, **kwargs) -> FakeResponse:
        """Async GET implementation with request tracking."""

    def get_requests_made(self) -> List[RequestCapture]:
        """Get history of requests made for test verification."""
```

**Benefits**:
- Replaces 15+ individual HTTP mocks across test files
- Provides request verification without coupling to internals
- Simulates realistic HTTP behaviors (delays, errors, retries)

#### 1.2 Create FakeDatabase Fixture
**Location**: `tests/fakes/database.py`

**Current State**: Tests mock `apply_pending_alembic` and database operations
**Target**: Real SQLite database with isolated test data

```python
class FakeDatabase:
    """Test database using real SQLite with isolation."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or Path(":memory:")
        self._connection_pool: Optional[ConnectionPool] = None

    async def setup_schema(self):
        """Apply real migrations to create test schema."""

    async def cleanup(self):
        """Clean up test data while preserving schema."""

    def get_connection_string(self) -> str:
        """Get connection string for services."""

    async def seed_test_data(self, dataset: str):
        """Load predefined test datasets."""

@pytest.fixture
async def test_database():
    """Pytest fixture providing isolated test database."""
    db = FakeDatabase()
    await db.setup_schema()
    yield db
    await db.cleanup()
```

**Benefits**:
- Tests real database behavior including transactions, constraints
- Catches real migration issues, schema problems
- Eliminates 20+ database operation mocks in bootstrap tests

#### 1.3 Expand FakeMarketDataProvider
**Location**: `tests/fakes/providers.py`

**Current State**: Simple fake provider exists but underutilized
**Target**: Configurable provider supporting various test scenarios

```python
class FakeMarketDataProvider:
    """Enhanced fake provider with scenario support."""

    def configure_symbol_data(self, symbol: Symbol, bars: List[OHLCVBar]):
        """Configure expected bar data for symbol."""

    def configure_error(self, symbol: Symbol, error: Exception):
        """Configure error responses for symbol."""

    def configure_rate_limiting(self, delay: float, max_requests: int):
        """Simulate rate limiting behavior."""

    def configure_pagination(self, page_size: int, total_pages: int):
        """Simulate paginated responses."""

    async def fetch_bars_for_symbol(self, symbol, time_range, max_bars=1000):
        """Fetch bars with configured behavior."""

    def get_request_history(self) -> List[RequestInfo]:
        """Verify request patterns without coupling to internals."""
```

**Benefits**:
- Replaces 10+ mock provider instances
- Supports realistic edge cases (errors, rate limits, pagination)
- Enables testing business logic without API dependencies

#### 1.4 Create FakeMetricsCollector
**Location**: `tests/fakes/metrics.py`

**Current State**: Tests mock prometheus_client functions
**Target**: In-memory metrics collection for verification

```python
class FakeMetricsCollector:
    """In-memory metrics collection for tests."""

    def __init__(self):
        self.counters: Dict[str, float] = defaultdict(float)
        self.histograms: Dict[str, List[float]] = defaultdict(list)
        self.gauges: Dict[str, float] = {}

    def increment_counter(self, name: str, labels: Dict[str, str] = None, value: float = 1.0):
        """Record counter increment."""

    def observe_histogram(self, name: str, labels: Dict[str, str] = None, value: float = 0.0):
        """Record histogram observation."""

    def set_gauge(self, name: str, labels: Dict[str, str] = None, value: float = 0.0):
        """Set gauge value."""

    def get_counter_value(self, name: str, labels: Dict[str, str] = None) -> float:
        """Get counter value for test verification."""
```

**Benefits**:
- Eliminates prometheus client mocking in 8+ test files
- Enables verification of metrics behavior
- Supports testing metric-driven functionality

### Deliverables Phase 1
- [x] `tests/fakes/adapters.py` with FakeHttpClient ✅
- [x] `tests/fakes/database.py` with FakeDatabase fixture ✅
- [x] `tests/fakes/providers.py` with enhanced FakeMarketDataProvider ✅ (in adapters.py)
- [x] `tests/fakes/metrics.py` with FakeMetricsCollector ✅
- [x] Documentation: `tests/fakes/README.md` with usage patterns ✅
- [x] Migration guide: Convert 3 test files to use new fakes as proof of concept ✅ (test_alpaca_client_refactored.py)

## Phase 2: Refactor for Testability (Weeks 3-4)

### Objective
Address design issues that require excessive mocking.

### Tasks

#### 2.1 Extract BootstrapOrchestrator
**Current Issue**: `bootstrap()` function directly calls multiple services, requires mocking everything

**Solution**: Dependency injection pattern
```python
class BootstrapOrchestrator:
    def __init__(self,
                 migration_service: IMigrationService,
                 validation_service: IValidationService,
                 aggregation_service: IAggregationService):
        self.migration_service = migration_service
        self.validation_service = validation_service
        self.aggregation_service = aggregation_service

    def bootstrap(self) -> BootstrapResult:
        """Bootstrap with injected dependencies."""
```

**Benefits**:
- Tests can inject fakes instead of mocking functions
- Easier to test different bootstrap scenarios
- Cleaner separation of concerns

#### 2.2 Add HTTP Client Injection
**Current Issue**: API clients hardcode httpx, require monkeypatching

**Solution**: Constructor injection
```python
class AlpacaClient(BaseApiClient):
    def __init__(self,
                 config: ClientConfig,
                 auth: AuthStrategy,
                 http_client: Optional[HttpClientProtocol] = None):
        self.http_client = http_client or httpx.AsyncClient()
```

**Benefits**:
- Tests can inject FakeHttpClient
- No more monkeypatching global modules
- Supports different HTTP implementations

#### 2.3 Create Service Factories for CLI
**Current Issue**: CLI commands directly import and call implementation functions

**Solution**: Factory pattern with dependency injection
```python
class CLIServiceFactory:
    def __init__(self, database: IDatabase, http_client: HttpClientProtocol):
        self.database = database
        self.http_client = http_client

    def create_ingestion_service(self) -> IngestionService:
        """Create properly configured ingestion service."""

def ingest_command(config: Path, service_factory: CLIServiceFactory):
    """CLI command with injected service factory."""
```

### Deliverables Phase 2
- [x] `src/marketpipe/bootstrap.py` refactored with BootstrapOrchestrator ✅
- [x] HTTP client injection added to all API clients ✅ (BaseApiClient, AlpacaClient)
- [ ] CLI service factories implemented (moved to Phase 3)
- [x] Interface definitions for all injected dependencies ✅ 
- [x] Updated tests demonstrating improved testability ✅ (test_bootstrap_orchestrator.py)

## Phase 3: Convert High-Value Tests to Integration Style (Weeks 5-6) ✅ COMPLETE

### Objective  
Replace brittle mocked tests with integration tests using real components.

### 🎯 ACHIEVEMENTS DELIVERED

**Bootstrap Integration Tests:**
- Converted from 5+ mock patches to **real database operations**
- Test actual bootstrap behavior vs mock coordination
- Easy error scenario testing with configurable fakes
- Verify real database state and service registrations

**Enhanced Pipeline Integration Tests:**  
- Comprehensive realistic scenarios (data quality issues, rate limiting, failures)
- Cross-component error propagation testing
- Performance and resource usage testing
- Memory usage patterns with large datasets

**Provider Integration Tests:**
- Replace httpx monkeypatching with **real HTTP server simulation**
- Test actual HTTP client behavior (retries, timeouts, headers)
- Realistic API scenarios and provider resilience
- Concurrent request handling and performance characteristics

**Integration Test Strategy Documentation:**
- Comprehensive guide on integration vs unit test decisions
- Test patterns, anti-patterns, and performance guidelines
- Migration strategies and debugging approaches

### 📊 IMPACT METRICS ACHIEVED

**Mock Reduction:**
- **Bootstrap tests**: 5+ mock patches → 0 mocks needed
- **Pipeline tests**: Complex mock coordination → realistic scenarios  
- **Provider tests**: Monkeypatched httpx → real HTTP behavior

**Test Confidence:**
- **Real behavior testing**: Database migrations, HTTP interactions, error flows
- **Realistic scenarios**: Rate limiting, partial failures, resource constraints
- **Cross-component verification**: Actual integration points tested

**Developer Experience:**
- **Higher confidence**: Tests catch real issues mocks couldn't reveal
- **Easier debugging**: Real components with observable state
- **Maintainable tests**: Less coupling to implementation details

### Priority Tests for Conversion

#### 3.1 Bootstrap Tests
**File**: `tests/test_bootstrap_side_effect.py`
**Current**: Mocks 3-4 services, tests mock coordination
**Target**: Integration test with real SQLite database

```python
def test_bootstrap_idempotent_with_real_database(test_database):
    """Test bootstrap with real database and services."""
    orchestrator = BootstrapOrchestrator(
        migration_service=RealMigrationService(test_database),
        validation_service=FakeValidationService(),
        aggregation_service=FakeAggregationService()
    )

    # First bootstrap should succeed
    result1 = orchestrator.bootstrap()
    assert result1.success

    # Second bootstrap should be idempotent
    result2 = orchestrator.bootstrap()
    assert result2.success
    assert result2.was_already_bootstrapped
```

#### 3.2 Full Pipeline Tests
**File**: `tests/integration/test_full_pipeline.py`
**Current**: Good integration pattern but limited coverage
**Target**: Expand to cover more scenarios with real storage

```python
def test_pipeline_with_validation_errors(test_database, fake_provider):
    """Test pipeline handling data quality issues."""
    fake_provider.configure_symbol_data("AAPL", [
        create_valid_bar(),
        create_invalid_bar(),  # Will trigger validation
    ])

    result = run_full_pipeline(
        provider=fake_provider,
        storage=ParquetStorage(test_database),
        validator=RealValidator()
    )

    assert result.valid_bars == 1
    assert result.invalid_bars == 1
    assert result.validation_errors[0].type == "ohlc_consistency"
```

#### 3.3 Provider Integration Tests
**Current**: All provider tests mock HTTP responses
**Target**: Tests with real HTTP server or provider sandbox APIs

```python
@pytest.mark.integration
def test_alpaca_client_real_http(fake_http_server):
    """Test Alpaca client with real HTTP behavior."""
    fake_http_server.configure_endpoint(
        "/v2/stocks/bars",
        response={"bars": [sample_alpaca_bar()]},
        headers={"content-type": "application/json"}
    )

    client = AlpacaClient(config=config, http_client=requests_client)
    bars = await client.fetch_bars("AAPL", time_range)

    assert len(bars) == 1
    assert fake_http_server.get_requests()[0].headers["Authorization"]
```

### Deliverables Phase 3
- [x] 5+ bootstrap tests converted to integration style ✅ (test_bootstrap_integration.py)
- [x] 3+ full pipeline tests with realistic scenarios ✅ (test_enhanced_pipeline.py)
- [x] Provider integration tests with HTTP server ✅ (test_provider_integration.py)
- [x] Test performance comparison (integration vs mocked) ✅ (included in test files)
- [x] Documentation on when to use integration vs unit tests ✅ (INTEGRATION_TEST_GUIDE.md)

## Phase 4: Introduce Shared Test Infrastructure (Weeks 7-8)

### Objective
Create reusable test utilities and base classes to standardize testing patterns.

### Tasks

#### 4.1 Common Test Fixtures
**Location**: `tests/conftest.py`

```python
@pytest.fixture
def integration_environment():
    """Full test environment with real database and fake services."""
    database = FakeDatabase()
    http_client = FakeHttpClient()
    metrics = FakeMetricsCollector()

    yield IntegrationEnvironment(database, http_client, metrics)

@pytest.fixture
def domain_objects():
    """Factory for creating valid domain objects with reasonable defaults."""
    return DomainObjectFactory()

class DomainObjectFactory:
    def create_ohlcv_bar(self, symbol="AAPL", **overrides) -> OHLCVBar:
        """Create valid OHLCV bar with overrides."""

    def create_ingestion_job(self, symbols=None, **overrides) -> IngestionJob:
        """Create valid ingestion job with overrides."""
```

#### 4.2 Integration Test Base Classes
```python
class IntegrationTestCase:
    """Base class for integration tests."""

    def setup_method(self):
        self.database = FakeDatabase()
        self.http_client = FakeHttpClient()
        self.metrics = FakeMetricsCollector()

    def create_service_factory(self) -> ServiceFactory:
        """Create service factory with test doubles."""

class PipelineTestCase(IntegrationTestCase):
    """Specialized base class for pipeline integration tests."""

    def run_ingestion_pipeline(self, symbols: List[str], **config) -> PipelineResult:
        """Helper to run complete ingestion pipeline."""
```

#### 4.3 Performance Benchmark Framework
```python
class TestPerformanceBenchmarks:
    """Benchmark tests to catch performance regressions."""

    @pytest.mark.benchmark
    def test_ingestion_throughput_benchmark(self, benchmark_data):
        """Benchmark ingestion throughput with realistic data volume."""

    @pytest.mark.benchmark
    def test_validation_performance_benchmark(self, large_dataset):
        """Benchmark validation performance."""
```

### Deliverables Phase 4
- [ ] Comprehensive test fixtures in `conftest.py`
- [ ] Base classes for integration and pipeline tests
- [ ] Domain object factory with reasonable defaults
- [ ] Performance benchmark framework
- [ ] Migration guide for existing tests
- [ ] Test architecture documentation

## Success Metrics

### Quantitative Goals
- **Reduce mock usage**: From 150+ mock instances to <50
- **Improve test reliability**: Reduce flaky test failures by 80%
- **Increase test coverage**: Maintain >90% coverage with more reliable tests
- **Faster development**: Reduce time spent debugging test failures by 60%

### Qualitative Goals
- Tests focus on behavior rather than implementation details
- Refactoring production code doesn't break tests
- New developers can understand and modify tests easily
- Test failures indicate real bugs, not test issues

## Risk Assessment

### Technical Risks
- **Performance**: Integration tests may be slower than unit tests
  - *Mitigation*: Use in-memory databases, optimize test data
- **Complexity**: More sophisticated test doubles require maintenance
  - *Mitigation*: Comprehensive documentation and examples

### Timeline Risks
- **Scope creep**: Temptation to fix all tests at once
  - *Mitigation*: Strict phase boundaries, focus on high-impact tests first
- **Regression**: Changes break existing functionality
  - *Mitigation*: Gradual migration, maintain parallel test suites during transition

## Implementation Guidelines

### Code Review Criteria
- [ ] New tests use fakes instead of mocks when possible
- [ ] Integration tests cover realistic scenarios
- [ ] Unit tests focus on single responsibility without excessive setup
- [ ] Test names clearly describe business scenarios being tested

### Migration Strategy
1. **Parallel development**: Build new test infrastructure alongside existing tests
2. **Gradual conversion**: Convert test files one at a time
3. **Validation**: Ensure converted tests provide same coverage
4. **Cleanup**: Remove old mock infrastructure after successful migration

## Conclusion

This refactoring plan addresses the core issues identified in the test infrastructure audit. By focusing on better test doubles, improved design for testability, and comprehensive integration tests, we will create a more maintainable and reliable test suite that better serves the MarketPipe development process.

The plan prioritizes high-impact changes first and provides clear deliverables for each phase, ensuring steady progress toward more robust testing practices.
