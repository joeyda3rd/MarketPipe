# Testing Guide

This guide covers MarketPipe's testing strategy, best practices, and how to write effective tests. Our testing approach ensures reliability, maintainability, and confidence in the codebase.

## Testing Philosophy

MarketPipe follows a comprehensive testing strategy based on these principles:

1. **Test Pyramid**: More unit tests, fewer integration tests, minimal end-to-end tests
2. **Test-Driven Development**: Write tests before implementation when possible
3. **Fast Feedback**: Tests should run quickly and provide immediate feedback
4. **Reliable Tests**: Tests should be deterministic and not flaky
5. **Readable Tests**: Tests serve as documentation for expected behavior

## Test Organization

### Directory Structure

```
tests/
├── unit/                       # Fast, isolated unit tests
│   ├── domain/                # Domain model tests
│   ├── application/           # Application service tests
│   ├── infrastructure/        # Infrastructure tests (with mocks)
│   └── interface/             # CLI/API interface tests
├── integration/               # Slower integration tests
│   ├── providers/             # Provider integration tests
│   ├── storage/               # Storage integration tests
│   └── end_to_end/           # Complete workflow tests
├── fixtures/                  # Test data and fixtures
│   ├── ohlcv_data.json       # Sample market data
│   ├── configurations.yaml   # Test configurations
│   └── responses/            # Mock API responses
└── conftest.py               # Shared pytest fixtures
```

### Test Categories

#### Unit Tests (Fast)
- Test individual components in isolation
- Use mocks for external dependencies
- Run in < 1 second total
- Should comprise 70% of total tests

#### Integration Tests (Medium)
- Test component interactions
- Use real implementations where safe
- Run in < 30 seconds total
- Should comprise 25% of total tests

#### End-to-End Tests (Slow)
- Test complete user workflows
- Use real external services (carefully)
- Run in < 5 minutes total
- Should comprise 5% of total tests

## Test Framework Setup

### Prerequisites

```bash
# Install test dependencies
pip install -e '.[dev]'

# Verify pytest installation
pytest --version
```

### Running Tests

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=src/marketpipe --cov-report=html --cov-report=term

# Run specific test categories
pytest tests/unit/                    # Unit tests only
pytest tests/integration/             # Integration tests only
pytest -m "not slow"                  # Exclude slow tests

# Run tests matching pattern
pytest -k "test_alpaca"              # Tests containing "alpaca"
pytest tests/unit/test_domain.py     # Specific test file

# Run with verbose output
pytest -v

# Run with debugging
pytest --pdb                         # Drop to debugger on failure
pytest --lf                         # Run last failed tests only
```

### Test Configuration

```ini
# pytest.ini
[tool:pytest]
minversion = 6.0
addopts =
    --strict-markers
    --strict-config
    --cov=src/marketpipe
    --cov-report=term-missing
    --cov-report=html:htmlcov
    --cov-fail-under=85
testpaths = tests
markers =
    unit: Unit tests (fast)
    integration: Integration tests (medium)
    slow: Slow tests (use sparingly)
    external: Tests requiring external services
python_files = test_*.py
python_classes = Test*
python_functions = test_*
```

## Writing Unit Tests

### Domain Model Testing

Test business logic and domain rules:

```python
# tests/unit/domain/test_entities.py
import pytest
from decimal import Decimal
from marketpipe.domain.entities import OHLCVBar
from marketpipe.domain.value_objects import Symbol, Price, Volume, Timestamp
from marketpipe.domain.exceptions import DomainError

class TestOHLCVBar:
    """Test suite for OHLCV bar entity."""

    def test_create_valid_bar(self):
        """Should create valid OHLCV bar with consistent prices."""
        # Arrange
        symbol = Symbol("AAPL")
        timestamp = Timestamp.from_string("2024-01-02T09:30:00Z")
        open_price = Price.from_float(150.0)
        high_price = Price.from_float(152.0)
        low_price = Price.from_float(149.0)
        close_price = Price.from_float(151.0)
        volume = Volume(100000)

        # Act
        bar = OHLCVBar(
            symbol=symbol,
            timestamp=timestamp,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=volume
        )

        # Assert
        assert bar.symbol == symbol
        assert bar.high_price >= bar.open_price
        assert bar.high_price >= bar.close_price
        assert bar.low_price <= bar.open_price
        assert bar.low_price <= bar.close_price

    def test_invalid_ohlc_prices_raises_error(self):
        """Should raise error when OHLC prices are inconsistent."""
        # Arrange
        symbol = Symbol("AAPL")
        timestamp = Timestamp.from_string("2024-01-02T09:30:00Z")
        volume = Volume(100000)

        # Act & Assert - High price lower than open
        with pytest.raises(DomainError, match="OHLC prices are inconsistent"):
            OHLCVBar(
                symbol=symbol,
                timestamp=timestamp,
                open_price=Price.from_float(150.0),
                high_price=Price.from_float(149.0),  # Invalid: high < open
                low_price=Price.from_float(148.0),
                close_price=Price.from_float(149.5),
                volume=volume
            )

    def test_calculate_true_range(self):
        """Should calculate True Range correctly."""
        # Arrange
        symbol = Symbol("AAPL")
        timestamp1 = Timestamp.from_string("2024-01-02T09:30:00Z")
        timestamp2 = Timestamp.from_string("2024-01-02T09:31:00Z")
        volume = Volume(100000)

        bar1 = OHLCVBar(
            symbol=symbol, timestamp=timestamp1, volume=volume,
            open_price=Price.from_float(150.0),
            high_price=Price.from_float(152.0),
            low_price=Price.from_float(149.0),
            close_price=Price.from_float(151.0),
        )

        bar2 = OHLCVBar(
            symbol=symbol, timestamp=timestamp2, volume=volume,
            open_price=Price.from_float(150.5),
            high_price=Price.from_float(153.0),
            low_price=Price.from_float(148.0),
            close_price=Price.from_float(152.5),
        )

        # Act
        true_range = bar2.calculate_true_range(bar1)

        # Assert
        # TR = max(H-L, |H-PC|, |L-PC|)
        # = max(153-148, |153-151|, |148-151|) = max(5, 2, 3) = 5
        assert true_range.value == Decimal('5.00')
```

### Value Object Testing

```python
# tests/unit/domain/test_value_objects.py
import pytest
from decimal import Decimal
from marketpipe.domain.value_objects import Symbol, Price, Volume

class TestSymbol:
    """Test suite for Symbol value object."""

    def test_valid_symbol_creation(self):
        """Should create valid symbol."""
        symbol = Symbol("AAPL")
        assert symbol.value == "AAPL"

    @pytest.mark.parametrize("invalid_symbol", [
        "",          # Empty
        "123",       # Numeric
        "AAPL123",   # Alphanumeric
        "A" * 15,    # Too long
    ])
    def test_invalid_symbol_raises_error(self, invalid_symbol):
        """Should raise error for invalid symbols."""
        with pytest.raises(ValueError):
            Symbol(invalid_symbol)

class TestPrice:
    """Test suite for Price value object."""

    def test_price_precision(self):
        """Should maintain 4 decimal places precision."""
        price = Price.from_float(123.456789)
        assert price.value == Decimal('123.4568')

    def test_negative_price_raises_error(self):
        """Should raise error for negative prices."""
        with pytest.raises(ValueError, match="Price cannot be negative"):
            Price(Decimal('-10.00'))

    def test_price_arithmetic(self):
        """Should support price arithmetic operations."""
        price1 = Price.from_float(100.0)
        price2 = Price.from_float(50.0)

        assert price1 + price2 == Price.from_float(150.0)
        assert price1 - price2 == Price.from_float(50.0)
```

### Application Service Testing

Test use case orchestration with mocked dependencies:

```python
# tests/unit/application/test_ingestion_service.py
import pytest
from unittest.mock import Mock, patch
from marketpipe.application.ingestion_service import IngestionService, IngestionResult
from marketpipe.domain.value_objects import Symbol, DateRange

class TestIngestionService:
    """Test suite for ingestion service."""

    def setup_method(self):
        """Set up test dependencies."""
        self.mock_provider = Mock()
        self.mock_repository = Mock()
        self.mock_event_publisher = Mock()

        self.service = IngestionService(
            provider=self.mock_provider,
            repository=self.mock_repository,
            event_publisher=self.mock_event_publisher
        )

    def test_successful_ingestion(self):
        """Should successfully ingest data and publish events."""
        # Arrange
        symbol = "AAPL"
        start_date = "2024-01-02"
        end_date = "2024-01-02"

        mock_raw_data = [
            {
                "symbol": "AAPL",
                "timestamp": "2024-01-02T09:30:00Z",
                "open": 150.0,
                "high": 152.0,
                "low": 149.0,
                "close": 151.0,
                "volume": 100000
            }
        ]

        self.mock_provider.fetch_ohlcv_data.return_value = mock_raw_data

        # Act
        result = self.service.ingest_symbol_data(symbol, start_date, end_date)

        # Assert
        assert result.success
        assert result.records_processed == 1

        # Verify interactions
        self.mock_provider.fetch_ohlcv_data.assert_called_once()
        self.mock_repository.save_bars.assert_called_once()
        self.mock_event_publisher.publish.assert_called_once()

    def test_provider_error_handling(self):
        """Should handle provider errors gracefully."""
        # Arrange
        self.mock_provider.fetch_ohlcv_data.side_effect = Exception("API Error")

        # Act
        result = self.service.ingest_symbol_data("AAPL", "2024-01-02", "2024-01-02")

        # Assert
        assert not result.success
        assert result.records_processed == 0
        assert "API Error" in result.error_message

        # Verify no side effects
        self.mock_repository.save_bars.assert_not_called()
        self.mock_event_publisher.publish.assert_not_called()
```

## Integration Testing

### Provider Integration Tests

Test real provider integrations with controlled conditions:

```python
# tests/integration/providers/test_alpaca_integration.py
import pytest
from unittest.mock import patch
from marketpipe.infrastructure.providers.alpaca_provider import AlpacaProvider
from marketpipe.infrastructure.config import AlpacaConfig

class TestAlpacaIntegration:
    """Integration tests for Alpaca provider."""

    @pytest.fixture
    def alpaca_config(self):
        """Alpaca configuration for testing."""
        return AlpacaConfig(
            api_key="test_key",
            secret="test_secret",
            base_url="https://api.test.com",
            timeout=30.0
        )

    @patch('httpx.get')
    def test_fetch_data_integration(self, mock_get, alpaca_config):
        """Should integrate with HTTP client correctly."""
        # Arrange
        mock_response_data = {
            "bars": {
                "AAPL": [
                    {
                        "t": "2024-01-02T09:30:00Z",
                        "o": 150.0,
                        "h": 152.0,
                        "l": 149.0,
                        "c": 151.0,
                        "v": 100000
                    }
                ]
            }
        }

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data

        provider = AlpacaProvider(alpaca_config)

        # Act
        result = provider.fetch_ohlcv_data(
            Symbol("AAPL"),
            DateRange.from_strings("2024-01-02", "2024-01-02")
        )

        # Assert
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"
        assert result[0]["open"] == 150.0

        # Verify HTTP call
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert "stocks/bars" in args[0]
        assert "APCA-API-KEY-ID" in kwargs["headers"]
```

### Database Integration Tests

Test database operations with real database:

```python
# tests/integration/storage/test_database_integration.py
import pytest
from sqlalchemy import create_engine
from marketpipe.infrastructure.database import Database, create_tables
from marketpipe.infrastructure.repositories.sqlalchemy_repository import SqlAlchemyOHLCVRepository

@pytest.fixture
def test_database():
    """Create test database."""
    engine = create_engine("sqlite:///:memory:")
    create_tables(engine)

    database = Database(engine)
    yield database

    database.close()

class TestDatabaseIntegration:
    """Integration tests for database operations."""

    def test_save_and_retrieve_bars(self, test_database):
        """Should save and retrieve OHLCV bars from database."""
        # Arrange
        repository = SqlAlchemyOHLCVRepository(test_database)

        bars = [
            create_test_ohlcv_bar("AAPL", "2024-01-02T09:30:00Z"),
            create_test_ohlcv_bar("AAPL", "2024-01-02T09:31:00Z"),
        ]

        # Act
        repository.save_bars(bars)
        retrieved_bars = repository.find_by_symbol_and_range(
            Symbol("AAPL"),
            DateRange.from_strings("2024-01-02", "2024-01-02")
        )

        # Assert
        assert len(retrieved_bars) == 2
        assert retrieved_bars[0].symbol.value == "AAPL"
```

## Test Fixtures and Utilities

### Shared Fixtures

```python
# tests/conftest.py
import pytest
from datetime import datetime, timezone
from marketpipe.domain.entities import OHLCVBar
from marketpipe.domain.value_objects import Symbol, Price, Volume, Timestamp

@pytest.fixture
def sample_symbol():
    """Sample symbol for testing."""
    return Symbol("AAPL")

@pytest.fixture
def sample_ohlcv_data():
    """Sample OHLCV data for testing."""
    return [
        {
            "symbol": "AAPL",
            "timestamp": "2024-01-02T09:30:00Z",
            "open": 150.0,
            "high": 152.0,
            "low": 149.0,
            "close": 151.0,
            "volume": 100000
        },
        {
            "symbol": "AAPL",
            "timestamp": "2024-01-02T09:31:00Z",
            "open": 151.0,
            "high": 153.0,
            "low": 150.0,
            "close": 152.0,
            "volume": 150000
        }
    ]

@pytest.fixture
def sample_ohlcv_bars():
    """Sample OHLCV bars as domain entities."""
    return [
        create_test_ohlcv_bar("AAPL", "2024-01-02T09:30:00Z"),
        create_test_ohlcv_bar("AAPL", "2024-01-02T09:31:00Z"),
    ]

def create_test_ohlcv_bar(symbol: str, timestamp: str) -> OHLCVBar:
    """Create test OHLCV bar with valid data."""
    return OHLCVBar(
        symbol=Symbol(symbol),
        timestamp=Timestamp.from_string(timestamp),
        open_price=Price.from_float(150.0),
        high_price=Price.from_float(152.0),
        low_price=Price.from_float(149.0),
        close_price=Price.from_float(151.0),
        volume=Volume(100000)
    )
```

### Mock Helpers

```python
# tests/helpers/mock_helpers.py
from unittest.mock import Mock, MagicMock
from typing import List, Dict, Any

class MockProviderBuilder:
    """Builder for creating mock providers."""

    def __init__(self):
        self.mock_provider = Mock()
        self._responses = []

    def with_response(self, data: List[Dict[str, Any]]) -> 'MockProviderBuilder':
        """Add response data."""
        self._responses.append(data)
        return self

    def with_error(self, error: Exception) -> 'MockProviderBuilder':
        """Add error response."""
        self._responses.append(error)
        return self

    def build(self) -> Mock:
        """Build mock provider."""
        if len(self._responses) == 1:
            response = self._responses[0]
            if isinstance(response, Exception):
                self.mock_provider.fetch_ohlcv_data.side_effect = response
            else:
                self.mock_provider.fetch_ohlcv_data.return_value = response
        else:
            self.mock_provider.fetch_ohlcv_data.side_effect = self._responses

        return self.mock_provider

# Usage
mock_provider = (MockProviderBuilder()
    .with_response([{"symbol": "AAPL", "open": 150.0}])
    .build())
```

## Test Data Management

### Test Data Files

```python
# tests/fixtures/data_loader.py
import json
from pathlib import Path
from typing import Dict, Any, List

class TestDataLoader:
    """Loader for test data files."""

    def __init__(self):
        self.fixtures_path = Path(__file__).parent

    def load_ohlcv_data(self, filename: str) -> List[Dict[str, Any]]:
        """Load OHLCV test data from JSON file."""
        file_path = self.fixtures_path / "ohlcv_data" / filename
        with open(file_path, 'r') as f:
            return json.load(f)

    def load_provider_response(self, provider: str, filename: str) -> Dict[str, Any]:
        """Load provider response data."""
        file_path = self.fixtures_path / "responses" / provider / filename
        with open(file_path, 'r') as f:
            return json.load(f)

# tests/fixtures/ohlcv_data/aapl_sample.json
[
  {
    "symbol": "AAPL",
    "timestamp": "2024-01-02T09:30:00Z",
    "open": 150.0,
    "high": 152.0,
    "low": 149.0,
    "close": 151.0,
    "volume": 100000
  }
]
```

## Performance Testing

### Benchmark Tests

```python
# tests/performance/test_benchmarks.py
import pytest
import time
from marketpipe.infrastructure.storage.parquet_storage import ParquetStorage

class TestPerformanceBenchmarks:
    """Performance benchmark tests."""

    @pytest.mark.slow
    def test_large_data_ingestion_performance(self, tmp_path):
        """Should handle large data ingestion efficiently."""
        # Arrange
        storage = ParquetStorage(tmp_path)
        large_dataset = create_large_test_dataset(10000)  # 10k records

        # Act
        start_time = time.time()
        storage.save_bars(large_dataset)
        end_time = time.time()

        # Assert
        duration = end_time - start_time
        records_per_second = len(large_dataset) / duration

        # Should process at least 1000 records per second
        assert records_per_second > 1000

        # Memory usage should be reasonable
        # (Add memory profiling if needed)

def create_large_test_dataset(size: int) -> List[OHLCVBar]:
    """Create large test dataset for performance testing."""
    import random
    from datetime import datetime, timedelta

    bars = []
    base_time = datetime(2024, 1, 2, 9, 30)

    for i in range(size):
        timestamp = base_time + timedelta(minutes=i)
        bars.append(create_test_ohlcv_bar(
            "AAPL",
            timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        ))

    return bars
```

## Testing Best Practices

### 1. Test Organization

```python
class TestOHLCVBar:
    """Organize tests by class being tested."""

    def setup_method(self):
        """Set up common test data."""
        self.symbol = Symbol("AAPL")
        self.timestamp = Timestamp.from_string("2024-01-02T09:30:00Z")

    def test_valid_creation(self):
        """Test names should describe expected behavior."""
        # Use AAA pattern: Arrange, Act, Assert
        pass

    def test_invalid_input_raises_error(self):
        """Test error conditions explicitly."""
        pass
```

### 2. Mock Usage Guidelines

```python
# Good: Mock external dependencies
@patch('httpx.get')
def test_api_call(self, mock_get):
    mock_get.return_value.json.return_value = {"data": "test"}
    # Test implementation

# Good: Use dependency injection for easier testing
def test_service_with_mock_dependency():
    mock_repo = Mock()
    service = MyService(repository=mock_repo)
    # Test service behavior

# Avoid: Mocking everything (makes tests brittle)
# Avoid: Mocking internal methods of class under test
```

### 3. Test Data Best Practices

```python
# Good: Use factories for consistent test data
def create_test_bar(**overrides):
    defaults = {
        "symbol": "AAPL",
        "open": 150.0,
        "high": 152.0,
        "low": 149.0,
        "close": 151.0,
        "volume": 100000
    }
    defaults.update(overrides)
    return OHLCVBar(**defaults)

# Good: Use parameterized tests for multiple scenarios
@pytest.mark.parametrize("symbol,expected", [
    ("AAPL", True),
    ("GOOGL", True),
    ("", False),
    ("123", False),
])
def test_symbol_validation(symbol, expected):
    if expected:
        assert Symbol(symbol).value == symbol
    else:
        with pytest.raises(ValueError):
            Symbol(symbol)
```

### 4. Async Testing

```python
# Test async code properly
@pytest.mark.asyncio
async def test_async_provider():
    """Test async provider methods."""
    provider = AlpacaProvider(config)
    result = await provider.async_fetch_data("AAPL")
    assert len(result) > 0
```

## Continuous Integration

### GitHub Actions Configuration

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.9, 3.10, 3.11, 3.12]

    steps:
    - uses: actions/checkout@v3

    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v3
      with:
        python-version: ${{ matrix.python-version }}

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e '.[dev]'

    - name: Lint with ruff
      run: ruff check src/ tests/

    - name: Type check with mypy
      run: mypy src/

    - name: Test with pytest
      run: |
        pytest --cov=src/marketpipe --cov-report=xml --cov-report=term

    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
        fail_ci_if_error: true
```

### Test Scripts

```bash
#!/bin/bash
# scripts/test

set -e

echo "Running MarketPipe test suite..."

# Fast tests for development
echo "1. Running unit tests..."
pytest tests/unit/ -v --tb=short

# Integration tests
echo "2. Running integration tests..."
pytest tests/integration/ -v --tb=short

# Code quality checks
echo "3. Running code quality checks..."
ruff check src/ tests/
black --check src/ tests/
mypy src/

# Coverage check
echo "4. Running coverage analysis..."
pytest --cov=src/marketpipe --cov-report=term --cov-fail-under=85

echo "All tests passed! ✅"
```

## Debugging Tests

### Debug Failing Tests

```bash
# Run with debugging
pytest --pdb                         # Drop to debugger on failure
pytest --pdb-trace                   # Start debugger immediately

# Run specific failing test
pytest tests/unit/test_domain.py::TestOHLCVBar::test_creation -v

# Show local variables on failure
pytest --tb=long

# Show print statements
pytest -s
```

### Test Isolation Issues

```python
# Ensure test isolation
def test_isolation():
    """Each test should be independent."""
    # Bad: Relying on global state
    global_var = "modified"

    # Good: Reset state in teardown
    def teardown():
        global_var = "original"
```

## Next Steps

To improve your testing skills with MarketPipe:

1. **Start with unit tests**: Write tests for new functionality
2. **Study existing tests**: Learn patterns from existing test suite
3. **Practice TDD**: Write tests before implementation
4. **Measure coverage**: Aim for >85% coverage on new code
5. **Review test PRs**: Learn from code review feedback

---

*Last updated: 2024-01-20*
