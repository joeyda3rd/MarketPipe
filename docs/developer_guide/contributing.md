# Contributing to MarketPipe

Thank you for your interest in contributing to MarketPipe! This guide provides everything you need to know about contributing to the project, from setting up your development environment to submitting pull requests.

## Quick Start

### 1. Development Setup

```bash
# Clone and setup in one go
git clone https://github.com/yourorg/marketpipe.git
cd marketpipe
scripts/setup    # One-command setup with dependencies and environment
```

### 2. Verify Your Setup

```bash
# Run health check
scripts/health-check

# Run quick tests
scripts/test-fast

# Start development server
marketpipe metrics --port 8000
```

### 3. Make Your First Contribution

```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Make your changes
# ... edit files ...

# Run tests
scripts/test

# Commit with conventional format
git commit -m "feat: add new data provider for XYZ"

# Push and create PR
git push origin feature/your-feature-name
```

## Development Environment

### Prerequisites

- **Python 3.9+** with pip and venv
- **Git** for version control
- **Docker** (optional, for integration tests)
- **PostgreSQL** (optional, for database tests)

### Installation Options

#### Option 1: Automated Setup (Recommended)

```bash
# Clone repository
git clone https://github.com/yourorg/marketpipe.git
cd marketpipe

# Run setup script
scripts/setup

# This script:
# - Creates virtual environment
# - Installs dependencies
# - Sets up pre-commit hooks
# - Configures development environment
# - Runs initial health check
```

#### Option 2: Manual Setup

```bash
# Clone repository
git clone https://github.com/yourorg/marketpipe.git
cd marketpipe

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e '.[dev]'

# Install pre-commit hooks
pre-commit install

# Set up environment variables
cp .env.example .env
# Edit .env with your API credentials

# Verify setup
marketpipe health-check --verbose
```

### Development Dependencies

The development environment includes:

- **Testing**: pytest, pytest-asyncio, pytest-cov, pytest-mock
- **Code Quality**: black, ruff, mypy, pre-commit
- **Documentation**: sphinx, mkdocs (optional)
- **Development Tools**: ipython, jupyter (optional)

## Project Structure

### Key Directories

```
MarketPipe/
├── src/marketpipe/              # Main package code
│   ├── __init__.py             # Package initialization
│   ├── cli.py                  # Command-line interface
│   ├── domain/                 # Domain models and services
│   ├── ingestion/              # Data ingestion pipeline
│   ├── providers/              # Market data provider integrations
│   ├── storage/                # Data storage and retrieval
│   └── monitoring/             # Metrics and observability
├── tests/                      # Test suite
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   └── fixtures/               # Test data and fixtures
├── docs/                       # Documentation
├── scripts/                    # Development and deployment scripts
├── config/                     # Configuration examples
└── monitoring/                 # Monitoring and alerting configs
```

### Code Organization Patterns

MarketPipe follows Domain-Driven Design (DDD) principles:

- **Domain Layer**: Core business logic (`src/marketpipe/domain/`)
- **Application Layer**: Use cases and workflows (`src/marketpipe/ingestion/`)
- **Infrastructure Layer**: External integrations (`src/marketpipe/providers/`)
- **Interface Layer**: CLI and APIs (`src/marketpipe/cli.py`)

## Development Workflow

### 1. Issue-Driven Development

- **Find an issue**: Browse [GitHub issues](https://github.com/yourorg/marketpipe/issues) or create a new one
- **Discuss approach**: Comment on the issue to discuss your approach
- **Get assignment**: Ask to be assigned to the issue before starting work

### 2. Branch Strategy

We use GitHub Flow with descriptive branch names:

```bash
# Feature branches
git checkout -b feature/add-polygon-provider
git checkout -b feature/improve-error-handling

# Bug fix branches
git checkout -b fix/memory-leak-in-ingestion
git checkout -b fix/timezone-handling-bug

# Documentation branches
git checkout -b docs/update-contributing-guide
git checkout -b docs/add-api-examples
```

### 3. Commit Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```bash
# Types: feat, fix, docs, style, refactor, test, chore
git commit -m "feat: add support for Polygon.io data provider"
git commit -m "fix: resolve timezone handling in OHLCV parsing"
git commit -m "docs: update CLI usage examples"
git commit -m "test: add unit tests for rate limiting"
```

### 4. Pull Request Process

1. **Create PR**: Use the PR template and provide clear description
2. **Code Review**: Address feedback and iterate on your changes
3. **Tests Pass**: Ensure all CI checks are green
4. **Maintainer Review**: Wait for maintainer approval
5. **Merge**: Maintainer will merge once approved

## Code Quality Standards

### Pre-commit Hooks

Pre-commit hooks run automatically and enforce:

- **Code formatting**: Black for Python code formatting
- **Linting**: Ruff for fast Python linting
- **Type checking**: mypy for static type checking
- **Import sorting**: isort for consistent imports
- **Spell checking**: codespell for documentation
- **YAML validation**: yamllint for configuration files

```bash
# Install hooks
pre-commit install

# Run manually
pre-commit run --all-files

# Skip hooks (emergencies only)
git commit -m "fix: urgent hotfix" --no-verify
```

### Code Style Guidelines

#### Python Code Style

- **PEP 8 compliance** with Black formatting
- **Type hints required** for all public APIs
- **Docstrings required** for all public functions and classes
- **Modern Python patterns** (3.9+ features encouraged)

```python
from __future__ import annotations

from typing import Any, Dict, List, Optional
from dataclasses import dataclass

@dataclass
class ProviderConfig:
    """Configuration for market data provider."""
    api_key: str
    base_url: str
    timeout: float = 30.0

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.api_key:
            raise ValueError("api_key is required")

def fetch_data(
    symbol: str,
    start_date: str,
    end_date: str,
    provider_config: ProviderConfig,
) -> List[Dict[str, Any]]:
    """Fetch market data for symbol in date range.

    Args:
        symbol: Stock symbol (e.g., 'AAPL')
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        provider_config: Provider configuration

    Returns:
        List of OHLCV records as dictionaries

    Raises:
        ValueError: If date format is invalid
        ProviderError: If API request fails
    """
    # Implementation here
    pass
```

#### Documentation Style

- **Markdown format** for all documentation
- **Code examples** that can be executed
- **Clear headings** with proper hierarchy
- **Links** that work within the documentation

### Testing Standards

#### Test Organization

```bash
tests/
├── unit/                       # Fast, isolated unit tests
│   ├── test_domain.py         # Domain model tests
│   ├── test_providers.py      # Provider unit tests
│   └── test_cli.py            # CLI unit tests
├── integration/               # Slower integration tests
│   ├── test_ingestion_flow.py # End-to-end ingestion
│   └── test_database.py       # Database integration
└── fixtures/                  # Test data
    ├── sample_ohlcv.json      # Sample market data
    └── test_configs.yaml      # Test configurations
```

#### Writing Tests

```python
import pytest
from unittest.mock import Mock, patch
from marketpipe.providers.alpaca import AlpacaProvider

class TestAlpacaProvider:
    """Test suite for Alpaca provider."""

    def test_fetch_data_success(self):
        """Should successfully fetch and parse market data."""
        # Arrange
        provider = AlpacaProvider(config=test_config)

        # Act
        result = provider.fetch_data("AAPL", "2024-01-02", "2024-01-02")

        # Assert
        assert len(result) > 0
        assert result[0]["symbol"] == "AAPL"
        assert "timestamp" in result[0]

    @patch('httpx.get')
    def test_fetch_data_with_api_error(self, mock_get):
        """Should handle API errors gracefully."""
        # Arrange
        mock_get.return_value.status_code = 500
        provider = AlpacaProvider(config=test_config)

        # Act & Assert
        with pytest.raises(ProviderError):
            provider.fetch_data("AAPL", "2024-01-02", "2024-01-02")

    @pytest.mark.asyncio
    async def test_async_fetch_data(self):
        """Should support async data fetching."""
        provider = AlpacaProvider(config=test_config)
        result = await provider.async_fetch_data("AAPL", "2024-01-02", "2024-01-02")
        assert len(result) > 0
```

#### Running Tests

```bash
# Run all tests
scripts/test

# Run specific test categories
scripts/test-unit       # Unit tests only
scripts/test-integration # Integration tests only
scripts/test-fast       # Quick tests for development

# Run with coverage
scripts/test-coverage

# Run specific test file
pytest tests/unit/test_providers.py -v

# Run specific test method
pytest tests/unit/test_providers.py::TestAlpacaProvider::test_fetch_data_success -v
```

## Contributing Areas

### 1. Data Provider Integrations

Add support for new market data providers:

- **Implement BaseProvider interface**
- **Add authentication handling**
- **Implement rate limiting**
- **Add comprehensive tests**
- **Update documentation**

Example providers to add:
- Yahoo Finance
- Alpha Vantage
- Twelve Data
- Quandl

### 2. Data Processing Features

Enhance data processing capabilities:

- **New aggregation functions**
- **Data quality validations**
- **Performance optimizations**
- **Schema evolution support**

### 3. Monitoring and Observability

Improve monitoring capabilities:

- **Custom metrics**
- **Alert rules**
- **Dashboard improvements**
- **Log aggregation**

### 4. Documentation

Help improve documentation:

- **User guides and tutorials**
- **API documentation**
- **Architecture guides**
- **Troubleshooting guides**

### 5. Testing and Quality

Enhance testing coverage:

- **Unit test coverage**
- **Integration test scenarios**
- **Performance benchmarks**
- **Load testing**

## Architecture Guidelines

### Domain-Driven Design

MarketPipe follows DDD principles:

```python
# Domain Layer - Pure business logic
class Symbol(ValueObject):
    """Stock symbol value object."""
    value: str

class OHLCVBar(Entity):
    """OHLCV bar entity with business rules."""
    symbol: Symbol
    timestamp: Timestamp
    # ... other fields

# Application Layer - Use cases
class IngestionUseCase:
    """Orchestrates data ingestion workflow."""

    def ingest_symbol_data(
        self,
        symbol: Symbol,
        date_range: DateRange
    ) -> IngestionResult:
        # Orchestrate domain services
        pass

# Infrastructure Layer - External integrations
class AlpacaProvider(MarketDataProvider):
    """Alpaca Markets integration."""

    def fetch_ohlcv_data(self, symbol: Symbol) -> List[OHLCVBar]:
        # External API integration
        pass
```

### Async/Sync Patterns

Provide both sync and async APIs:

```python
class DataProvider:
    """Provider with dual sync/async API."""

    def fetch_data(self, symbol: str) -> List[Dict[str, Any]]:
        """Synchronous data fetching."""
        return self._fetch_sync(symbol)

    async def async_fetch_data(self, symbol: str) -> List[Dict[str, Any]]:
        """Asynchronous data fetching."""
        return await self._fetch_async(symbol)
```

### Error Handling Patterns

Use specific exceptions with proper context:

```python
class MarketPipeError(Exception):
    """Base exception for MarketPipe."""
    pass

class ProviderError(MarketPipeError):
    """Error communicating with data provider."""

    def __init__(self, provider: str, message: str, status_code: Optional[int] = None):
        self.provider = provider
        self.status_code = status_code
        super().__init__(f"Provider {provider} error: {message}")

class ValidationError(MarketPipeError):
    """Data validation error."""

    def __init__(self, field: str, value: Any, rule: str):
        self.field = field
        self.value = value
        self.rule = rule
        super().__init__(f"Validation failed for {field}={value}: {rule}")
```

## Release Process

### Versioning

We use [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

### Release Checklist

1. **Update CHANGELOG.md** with changes
2. **Bump version** in `pyproject.toml`
3. **Run full test suite**
4. **Update documentation**
5. **Create release PR**
6. **Tag release** after merge
7. **Publish to PyPI** (automated)

## Getting Help

### Community Support

- **GitHub Discussions**: [Ask questions and share ideas](https://github.com/yourorg/marketpipe/discussions)
- **GitHub Issues**: [Report bugs or request features](https://github.com/yourorg/marketpipe/issues)
- **Discord/Slack**: [Real-time community chat](link-to-chat)

### Maintainer Contact

For sensitive issues or security concerns:
- **Email**: maintainers@marketpipe.dev
- **Security**: security@marketpipe.dev

### Development Resources

- **Architecture Guide**: [System design and patterns](architecture.md)
- **Testing Guide**: [Testing strategy and best practices](testing.md)
- **Release Process**: [How we ship releases](release_process.md)

## Recognition

Contributors are recognized in:

- **CONTRIBUTORS.md**: All contributors listed
- **Release notes**: Major contributions highlighted
- **GitHub**: Contributor graphs and statistics

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please read our [Code of Conduct](../CODE_OF_CONDUCT.md) for community guidelines.

## License

By contributing to MarketPipe, you agree that your contributions will be licensed under the Apache 2.0 License.

---

*Last updated: 2024-01-20*
