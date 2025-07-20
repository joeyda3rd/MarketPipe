# Parallel Testing with Async Code

## Overview

MarketPipe uses pytest-xdist for parallel test execution to speed up feedback loops. However, async code presents unique challenges when running tests in parallel. This document explains how we handle these challenges.

## How pytest-xdist Works

### Process Isolation
- Each worker process gets its own Python interpreter
- Workers run independently and can't share state
- Each worker has its own event loop for async tests
- Results are collected and merged by the main process

### Current Configuration
```bash
# From Makefile - parallel execution
pytest -n auto    # Auto-detect CPU cores
pytest -n 4       # Explicit worker count
```

## Async Code Challenges in Parallel Testing

### 1. Event Loop Conflicts

**Problem:**
```python
# This can fail in parallel execution
async def test_concurrent_requests():
    loop = asyncio.get_event_loop()  # May conflict between workers
    tasks = [client.async_fetch_batch("AAPL", 0, 1000) for _ in range(5)]
    results = await asyncio.gather(*tasks)
```

**Solution - pytest-asyncio handles this:**
```python
# pytest-asyncio creates isolated event loops per worker
@pytest.mark.asyncio
async def test_concurrent_requests():
    # Each worker gets its own event loop automatically
    tasks = [client.async_fetch_batch("AAPL", 0, 1000) for _ in range(5)]
    results = await asyncio.gather(*tasks)
```

### 2. Shared Resources

**Problem:**
```python
# Multiple workers hitting same database/files simultaneously
async def test_database_operations():
    await db.create_table("test_data")  # Race condition!
    await db.insert_data(test_records)
    result = await db.query("SELECT * FROM test_data")
```

**Solution - Isolation strategies:**
```python
# Strategy 1: Unique names per worker
@pytest.mark.asyncio
async def test_database_operations():
    worker_id = os.getenv('PYTEST_XDIST_WORKER', 'main')
    table_name = f"test_data_{worker_id}_{uuid.uuid4().hex[:8]}"

    await db.create_table(table_name)
    await db.insert_data(table_name, test_records)
    result = await db.query(f"SELECT * FROM {table_name}")

# Strategy 2: Mark as integration (sequential)
@pytest.mark.integration
async def test_database_operations():
    # Integration tests run sequentially to avoid conflicts
    await db.create_table("test_data")
    await db.insert_data(test_records)
```

### 3. Rate Limiting

**Problem:**
```python
# Multiple workers sharing rate limiter state
async def test_rate_limited_client():
    # Workers may interfere with each other's rate limits
    for i in range(10):
        await client.async_fetch_batch("AAPL", i*1000, (i+1)*1000)
```

**Solution - Isolated rate limiters:**
```python
@pytest.fixture
def isolated_client():
    """Create client with worker-specific rate limiter."""
    worker_id = os.getenv('PYTEST_XDIST_WORKER', 'main')
    config = ClientConfig(api_key=f"test_key_{worker_id}", base_url="https://test.api")

    # Each worker gets its own rate limiter
    rate_limiter = RateLimiter(requests_per_window=100, window_seconds=60)
    return AlpacaClient(config=config, auth=test_auth, rate_limiter=rate_limiter)

@pytest.mark.asyncio
async def test_rate_limited_client(isolated_client):
    # No interference between workers
    for i in range(10):
        await isolated_client.async_fetch_batch("AAPL", i*1000, (i+1)*1000)
```

## MarketPipe's Parallel Testing Strategy

### 1. Test Classification

```python
# Fast unit tests - parallel execution
@pytest.mark.unit
async def test_parse_response():
    # Pure functions, no I/O, no shared state
    client = AlpacaClient(config, auth)
    result = client.parse_response(mock_json)
    assert len(result) == 2

# Integration tests - sequential execution
@pytest.mark.integration
async def test_full_ingestion_workflow():
    # Involves files, databases, external APIs
    coordinator = IngestionCoordinator(client, validator, writer)
    results = await coordinator.ingest_batch(["AAPL"], date_ranges)
```

### 2. Makefile Commands

```makefile
# Parallel execution for fast feedback
test:
	pytest -n auto --lf --ff -m "not slow and not integration" -q

# Sequential execution for integration tests
test-integration:
	pytest tests/integration/ -v  # No -n auto

# Smart testing with appropriate parallelization
test-smart:
	python3 scripts/smart_test_runner.py --parallel-safe
```

### 3. Worker-Safe Fixtures

```python
@pytest.fixture
def temp_data_dir():
    """Create worker-specific temporary directory."""
    worker_id = os.getenv('PYTEST_XDIST_WORKER', 'main')
    temp_dir = Path(f"/tmp/marketpipe_test_{worker_id}_{uuid.uuid4().hex[:8]}")
    temp_dir.mkdir(parents=True, exist_ok=True)

    yield temp_dir

    # Cleanup
    if temp_dir.exists():
        shutil.rmtree(temp_dir)

@pytest.fixture
def mock_http_client():
    """Create isolated HTTP client mock for each worker."""
    # Each worker gets its own mock state
    with patch('httpx.AsyncClient') as mock_client:
        # Configure mock for this worker
        yield mock_client
```

### 4. Async-Safe Mocking

```python
class MockAsyncClient:
    """Async client mock that works with pytest-xdist."""

    def __init__(self):
        self.worker_id = os.getenv('PYTEST_XDIST_WORKER', 'main')
        self.call_count = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def get(self, url, **kwargs):
        self.call_count += 1
        # Return worker-specific responses if needed
        response_data = {
            "worker": self.worker_id,
            "call": self.call_count,
            "data": mock_market_data
        }
        return MockResponse(200, response_data)

# Usage in tests
@pytest.mark.asyncio
async def test_async_client_with_mock(monkeypatch):
    monkeypatch.setattr('httpx.AsyncClient', MockAsyncClient)

    client = AlpacaClient(config, auth)
    result = await client.async_fetch_batch("AAPL", 0, 1000)
    assert len(result) > 0
```

## Best Practices

### 1. Design for Isolation

```python
# ✅ Good - No shared state
@pytest.mark.asyncio
async def test_data_parsing():
    raw_data = {"bars": {"AAPL": [test_bar_data]}}
    client = AlpacaClient(config, auth)
    result = client.parse_response(raw_data)
    assert result[0]["symbol"] == "AAPL"

# ❌ Avoid - Shared file system state
@pytest.mark.asyncio
async def test_file_operations():
    with open("test_data.json", "w") as f:  # Race condition!
        json.dump(test_data, f)

    result = await process_file("test_data.json")
    assert result is not None
```

### 2. Use Appropriate Test Markers

```python
# Fast, isolated async tests - run in parallel
@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_request_retry():
    # Mock everything, no real I/O
    pass

# Slow or stateful tests - run sequentially
@pytest.mark.integration
@pytest.mark.asyncio
async def test_end_to_end_ingestion():
    # Real database, files, etc.
    pass

# Flaky tests that need special handling
@pytest.mark.flaky
@pytest.mark.asyncio
async def test_external_api_integration():
    # Real external API calls
    pass
```

### 3. Monitor Test Performance

```bash
# Check test timing to identify bottlenecks
make test-timing

# Run specific problematic tests in isolation
pytest tests/test_problematic.py -v --tb=short

# Debug worker conflicts
pytest -n 2 -v --tb=short tests/test_async.py
```

## Debugging Parallel Async Issues

### 1. Identify Worker Conflicts

```python
# Add worker identification to tests
def test_worker_identification():
    worker_id = os.getenv('PYTEST_XDIST_WORKER', 'main')
    print(f"Running in worker: {worker_id}")

    # Use worker_id in test logic to identify conflicts
    assert worker_id is not None
```

### 2. Isolate Problematic Tests

```bash
# Run single worker to eliminate parallel issues
pytest -n 1 tests/test_problematic.py

# Run without pytest-xdist entirely
pytest tests/test_problematic.py

# Compare timing: parallel vs sequential
time pytest -n auto tests/unit/
time pytest tests/unit/
```

### 3. Mock Async Resources Properly

```python
# ✅ Proper async mocking
class AsyncMockClient:
    async def request(self, *args, **kwargs):
        await asyncio.sleep(0.01)  # Simulate async operation
        return {"status": "success"}

# ❌ Sync mock for async code
class SyncMockClient:
    def request(self, *args, **kwargs):  # Missing async!
        return {"status": "success"}
```

## Current MarketPipe Implementation

### Test Categories and Execution
- **Unit tests**: Parallel execution with `-n auto`
- **Integration tests**: Sequential execution for safety
- **Smart tests**: Adaptive - parallel for unit, sequential for integration

### Key Files
- `pytest.ini`: Marker configuration
- `pyproject.toml`: pytest-xdist dependency
- `Makefile`: Test execution commands with appropriate parallelization
- `tests/conftest.py`: Worker-safe fixtures

### Performance Impact
- **Parallel unit tests**: ~3-4x speedup on typical development machines
- **Sequential integration**: No speedup but maintains reliability
- **Smart selection**: Best of both - fast feedback with safety
