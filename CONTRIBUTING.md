# Contributing to MarketPipe

Thank you for your interest in contributing to MarketPipe! This document provides guidelines for contributors.

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourorg/marketpipe.git
   cd marketpipe
   ```

2. **Install in development mode**
   ```bash
   pip install -e '.[dev]'
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your API credentials
   ```

4. **Run tests to verify setup**
   ```bash
   pytest
   ```

## Code Quality

MarketPipe maintains high code quality standards:

- **Formatting**: Use `black` for code formatting
- **Linting**: Use `ruff` for linting
- **Type checking**: Use `mypy` for static type analysis
- **Testing**: Write tests for all new functionality

Run quality checks:
```bash
black src/ tests/
ruff check src/ tests/
mypy src/marketpipe/
pytest
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

1. **Create a feature branch**
   ```bash
   git checkout -b feature/add-newprovider
   ```

2. **Make your changes** following the guidelines above

3. **Run all quality checks**
   ```bash
   pre-commit run --all-files
   pytest
   ```

4. **Commit with descriptive messages**
   ```bash
   git commit -m "feat: add NewProvider market data adapter

   - Implement NewProviderMarketDataAdapter with OHLCV support
   - Add standardized environment variable configuration
   - Include comprehensive unit and contract tests
   - Update documentation and .env.example"
   ```

5. **Submit pull request** with the provider checklist table

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