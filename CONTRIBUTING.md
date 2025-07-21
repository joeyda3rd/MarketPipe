# Contributing to MarketPipe

Thank you for your interest in contributing to MarketPipe! This document provides guidelines for contributors.

## Development Setup

### Quick Setup
```bash
# Clone and setup in one go
git clone https://github.com/yourorg/marketpipe.git
cd marketpipe
scripts/setup    # One-command setup with dependencies and environment
```

### Manual Setup
1. **Clone the repository**
   ```bash
   git clone https://github.com/yourorg/marketpipe.git
   cd marketpipe
   ```

2. **Install in development mode**
   ```bash
   pip install -e '.[dev]'
   ```

3. **Install pre-commit hooks (recommended)**
   ```bash
   pip install pre-commit
   pre-commit install
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your API credentials
   ```

5. **Verify setup**
   ```bash
   scripts/health-check
   scripts/test-fast
   ```

## Code Quality

MarketPipe maintains high code quality standards through automated checks:

### Pre-commit Hooks (Recommended)
The easiest way to maintain code quality is through pre-commit hooks:

```bash
# Install pre-commit hooks
pre-commit install

# Now all checks run automatically on git commit
git add .
git commit -m "Your changes"  # Automatic formatting, linting, and tests
```

### Manual Quality Checks
If you prefer to run checks manually:

```bash
# Code formatting
scripts/format

# All quality checks
black src/ tests/
ruff check src/ tests/
mypy src/marketpipe/

# Testing
scripts/test-fast        # Quick feedback during development
scripts/test-full        # Complete suite before submitting PR
```

### Quality Standards
- **Formatting**: Black (line length 100)
- **Import sorting**: isort (compatible with Black)
- **Linting**: Ruff (replaces flake8, pylint)
- **Type checking**: MyPy for static analysis
- **Security**: Bandit for vulnerability scanning
- **Testing**: Comprehensive test suite with markers
- **Documentation**: Docstrings for public APIs

### Test Organization
Tests are organized with pytest markers:

```bash
# Run different test categories
pytest -m fast           # Ultra-fast tests (~2s)
pytest -m unit            # Unit tests
pytest -m integration     # Integration tests
pytest -m api_client      # API client tests
pytest -m config          # Configuration tests
```

## General Contribution Workflow

### Making Changes

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Development cycle**
   ```bash
   # Make your changes
   # ... edit files ...

   # Quick feedback during development
   scripts/test-fast

   # Format and check code quality
   scripts/format

   # Commit (pre-commit hooks run automatically)
   git add .
   git commit -m "feat: your descriptive commit message"
   ```

3. **Before submitting**
   ```bash
   # Run full CI simulation locally
   scripts/test-ci

   # Or run components individually
   scripts/test-full        # Complete test suite
   pre-commit run --all-files  # All quality checks
   ```

### Testing Your Changes

MarketPipe has multiple levels of testing:

```bash
# During development - quick feedback
scripts/test-fast           # ~3 seconds

# Pre-commit validation - ultra-fast
scripts/pre-commit-tests    # ~2 seconds (runs automatically)

# Before PR - comprehensive
scripts/test-full           # Full suite with coverage
scripts/test-ci            # Simulate CI environment

# Specific test categories
pytest -m fast             # Only fast tests
pytest -m api_client       # API client tests
pytest -m integration      # Integration tests
```

### Code Style

All code style is enforced automatically through pre-commit hooks:
- **Black** for formatting (line length 100)
- **isort** for import sorting
- **Ruff** for linting
- **MyPy** for type checking
- **Bandit** for security scanning

### Commit Messages

Use conventional commit format:
```
feat: add new provider integration
fix: resolve rate limiting issue
docs: update API documentation
test: add integration tests for feature X
```

## Adding a Provider Adapter

MarketPipe uses a standardized approach for integrating new market data providers. Follow these steps to add support for a new provider:

### 1. Provider Key and Environment Variables

Choose a lowercase provider key (e.g., `newprovider`) and define environment variables following the naming convention:

**Pattern**: `MP_{PROVIDERKEY_UPPER}_{CREDNAME_UPPER}`

**Examples**:
- `MP_NEWPROVIDER_API_KEY`
- `MP_NEWPROVIDER_API_SECRET`
- `MP_NEWPROVIDER_USERNAME`

### 2. Update Environment Variable Documentation

1. **Add to canonical mapping** in `docs/provider_env_map.yaml`:
   ```yaml
   newprovider:
     key: "newprovider"
     description: "Brief description of the provider"
     core_data: "Types of data available"
     auth_scheme: "Authentication method"
     env_vars:
       - "MP_NEWPROVIDER_API_KEY"
       - "MP_NEWPROVIDER_API_SECRET"
   ```

2. **Update `.env.example`** with alphabetically sorted variables:
   ```bash
   # New Provider - Description
   # Get credentials from: https://provider.com/api-keys
   MP_NEWPROVIDER_API_KEY=
   MP_NEWPROVIDER_API_SECRET=
   ```

### 3. Create Settings Class

Add a settings class in `src/marketpipe/settings/providers.py`:

```python
class NewProviderSettings(BaseSettings):
    """New Provider API settings.

    Brief description of the provider and what it offers.

    Environment Variables:
        MP_NEWPROVIDER_API_KEY: API key from provider dashboard
        MP_NEWPROVIDER_API_SECRET: Secret key from provider dashboard
    """

    api_key: str = Field(..., env="MP_NEWPROVIDER_API_KEY", description="New Provider API key")
    api_secret: str = Field(..., env="MP_NEWPROVIDER_API_SECRET", description="New Provider secret key")
    base_url: str = Field(
        default="https://api.newprovider.com",
        description="New Provider API base URL"
    )

    class Config:
        env_prefix = ""
        case_sensitive = True
```

Don't forget to:
- Add the class to `PROVIDER_SETTINGS` registry
- Add to `__all__` exports

### 4. Create Provider Adapter

Create the adapter file `src/marketpipe/ingestion/infrastructure/newprovider_adapter.py`:

```python
# SPDX-License-Identifier: Apache-2.0
"""New Provider market data adapter."""

from __future__ import annotations

import logging
from typing import Any

from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.market_data import (
    IMarketDataProvider,
    ProviderMetadata,
)
from marketpipe.domain.value_objects import Symbol, TimeRange
from marketpipe.settings.providers import NewProviderSettings

from .provider_registry import provider

logger = logging.getLogger(__name__)


@provider("newprovider")
class NewProviderMarketDataAdapter(IMarketDataProvider):
    """
    Market data adapter for New Provider API.

    Provides access to New Provider's market data through their REST API.
    """

    def __init__(self, api_key: str, api_secret: str, **kwargs):
        self._api_key = api_key
        self._api_secret = api_secret
        self._logger = logging.getLogger(self.__class__.__name__)

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "NewProviderMarketDataAdapter":
        """Create adapter from configuration dictionary."""
        settings = NewProviderSettings()
        return cls(
            api_key=settings.api_key,
            api_secret=settings.api_secret,
        )

    def get_metadata(self) -> ProviderMetadata:
        """Return provider metadata."""
        return ProviderMetadata(
            name="newprovider",
            display_name="New Provider",
            description="Market data from New Provider",
            supported_timeframes=["1m", "5m", "1h", "1d"],
            rate_limits={"requests_per_minute": 60},
        )

    async def fetch_ohlcv_bars(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        timeframe: str = "1m",
    ) -> list[OHLCVBar]:
        """Fetch OHLCV bars for a symbol."""
        # Implementation here
        raise NotImplementedError("Implement OHLCV fetching logic")
```

### 5. Registry Entry

The `@provider("newprovider")` decorator automatically registers your adapter. Ensure the adapter is imported in `src/marketpipe/ingestion/infrastructure/__init__.py`:

```python
from .newprovider_adapter import NewProviderMarketDataAdapter

__all__ = [
    # ... existing exports
    "NewProviderMarketDataAdapter",
]
```

### 6. Write Tests

Create comprehensive tests in `tests/unit/infrastructure/test_newprovider_adapter.py`:

```python
import pytest
from marketpipe.ingestion.infrastructure import NewProviderMarketDataAdapter


class TestNewProviderAdapter:
    """Test suite for New Provider adapter."""

    def test_adapter_creation(self):
        """Test adapter can be created with valid credentials."""
        adapter = NewProviderMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret"
        )
        assert adapter is not None

    def test_metadata(self):
        """Test provider metadata is correct."""
        adapter = NewProviderMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret"
        )
        metadata = adapter.get_metadata()
        assert metadata.name == "newprovider"
        assert "New Provider" in metadata.display_name
```

### 7. Contract Tests

Ensure your adapter passes the standard contract tests by adding it to `tests/integration/test_provider_contracts.py`.

### 8. Validation Script

The CI system runs `scripts/check_env_placeholders.py` to ensure all environment variables defined in settings classes exist in `.env.example`. This runs automatically but you can test locally:

```bash
python scripts/check_env_placeholders.py
```

### 9. Pull Request Checklist

When submitting your provider adapter, include this table in your PR description:

| Component | Status | Notes |
|-----------|--------|-------|
| Provider key chosen | ✅ | `newprovider` |
| Environment variables defined | ✅ | `MP_NEWPROVIDER_API_KEY`, `MP_NEWPROVIDER_API_SECRET` |
| Settings class created | ✅ | `NewProviderSettings` in `providers.py` |
| Adapter implementation | ✅ | `NewProviderMarketDataAdapter` |
| Registry entry | ✅ | `@provider("newprovider")` decorator |
| Unit tests | ✅ | Basic adapter functionality |
| Contract tests | ✅ | Standard provider interface |
| Documentation updated | ✅ | `.env.example`, `provider_env_map.yaml` |
| CI validation passes | ✅ | `check_env_placeholders.py` |

### 10. Testing Your Provider

Test your provider integration:

```bash
# Set environment variables
export MP_NEWPROVIDER_API_KEY="your_key"
export MP_NEWPROVIDER_API_SECRET="your_secret"

# Test ingestion
marketpipe ingest --provider newprovider --symbols AAPL --start 2024-01-01 --end 2024-01-02

# Verify provider is available
python -c "from marketpipe.ingestion.infrastructure import list_providers; print(list_providers())"
```

## Submitting Changes

### Pre-submission Checklist

Before submitting your pull request:

```bash
# 1. Ensure all tests pass
scripts/test-ci              # Full CI simulation

# 2. Verify code quality
pre-commit run --all-files   # All quality checks

# 3. Check test coverage (if applicable)
scripts/test-full            # Includes coverage report
```

### Pull Request Process

1. **Push your feature branch**
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Open a pull request** with:
   - Clear description of changes
   - Link to related issues
   - Screenshots/examples if applicable
   - Provider checklist table (if adding a provider)

3. **Respond to feedback** and make requested changes

4. **Final checks**: Ensure CI passes and conflicts are resolved

### Quality Gates

Your PR must pass:
- ✅ **Pre-commit hooks** (formatting, linting, security)
- ✅ **Fast tests** (core functionality)
- ✅ **Full test suite** (comprehensive coverage)
- ✅ **Type checking** (MyPy validation)
- ✅ **Code review** (maintainer approval)

## Code Review Process

All contributions go through code review:

- **Automated checks**: Pre-commit hooks, CI tests, coverage analysis
- **Manual review**: Code quality, architecture compliance, documentation
- **Provider-specific**: API integration testing, rate limit compliance

## Questions?

- **Issues**: Use GitHub Issues for bug reports and feature requests
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Security**: Email security@yourorg.com for security-related issues

Thank you for contributing to MarketPipe!
