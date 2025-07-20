# SPDX-License-Identifier: Apache-2.0
"""Tests for provider loader system."""

import pytest
from marketpipe.domain.market_data import IMarketDataProvider, ProviderMetadata
from marketpipe.ingestion.infrastructure.provider_loader import (
    build_provider,
    get_available_providers,
    validate_provider_config,
)
from marketpipe.ingestion.infrastructure.provider_registry import (
    clear_registry,
    register,
)


class MockProvider(IMarketDataProvider):
    """Mock provider for testing."""

    def __init__(self, **kwargs):
        self.config = kwargs

    @classmethod
    def from_config(cls, config):
        return cls(**config)

    async def fetch_bars_for_symbol(self, symbol, time_range, max_bars=1000):
        return []

    async def get_supported_symbols(self):
        return []

    async def is_available(self):
        return True

    def get_provider_metadata(self):
        return ProviderMetadata(
            provider_name="mock",
            supports_real_time=False,
            supports_historical=True,
            rate_limit_per_minute=None,
            minimum_time_resolution="1m",
            maximum_history_days=None,
        )


class MockProviderNoFromConfig(IMarketDataProvider):
    """Mock provider without from_config method."""

    def __init__(self, param1=None, param2=None):
        self.param1 = param1
        self.param2 = param2

    async def fetch_bars_for_symbol(self, symbol, time_range, max_bars=1000):
        return []

    async def get_supported_symbols(self):
        return []

    async def is_available(self):
        return True

    def get_provider_metadata(self):
        return ProviderMetadata(
            provider_name="mock_no_config",
            supports_real_time=False,
            supports_historical=True,
            rate_limit_per_minute=None,
            minimum_time_resolution="1m",
            maximum_history_days=None,
        )


class TestProviderLoader:
    """Test provider loader functionality."""

    def setup_method(self):
        """Setup for each test."""
        clear_registry()
        register("mock", MockProvider)
        register("mock_no_config", MockProviderNoFromConfig)

    def teardown_method(self):
        """Cleanup after each test."""
        clear_registry()

    def test_build_provider_with_from_config(self):
        """Test building provider using from_config method."""
        config = {"provider": "mock", "param1": "value1", "param2": "value2"}

        provider = build_provider(config)

        assert isinstance(provider, MockProvider)
        assert provider.config["param1"] == "value1"
        assert provider.config["param2"] == "value2"

    def test_build_provider_without_from_config(self):
        """Test building provider without from_config method."""
        config = {"provider": "mock_no_config", "param1": "value1", "param2": "value2"}

        provider = build_provider(config)

        assert isinstance(provider, MockProviderNoFromConfig)
        assert provider.param1 == "value1"
        assert provider.param2 == "value2"

    def test_build_provider_missing_provider_name(self):
        """Test building provider without provider name."""
        config = {"param1": "value1"}

        with pytest.raises(ValueError) as exc_info:
            build_provider(config)

        assert "Provider name is required" in str(exc_info.value)

    def test_build_provider_unknown_provider(self):
        """Test building unknown provider."""
        config = {"provider": "unknown", "param1": "value1"}

        with pytest.raises(KeyError) as exc_info:
            build_provider(config)

        assert "Provider 'unknown' not found" in str(exc_info.value)
        assert "Available providers:" in str(exc_info.value)

    def test_build_provider_invalid_config(self):
        """Test building provider with invalid configuration."""

        # Mock provider that raises TypeError on instantiation
        class BadProvider(IMarketDataProvider):
            def __init__(self, required_param):
                self.required_param = required_param

            async def fetch_bars_for_symbol(self, symbol, time_range, max_bars=1000):
                return []

            async def get_supported_symbols(self):
                return []

            async def is_available(self):
                return True

            def get_provider_metadata(self):
                return ProviderMetadata(
                    provider_name="bad",
                    supports_real_time=False,
                    supports_historical=True,
                    rate_limit_per_minute=None,
                    minimum_time_resolution="1m",
                    maximum_history_days=None,
                )

        register("bad", BadProvider)

        config = {
            "provider": "bad",
            # Missing required_param
        }

        with pytest.raises(ValueError) as exc_info:
            build_provider(config)

        assert "Failed to create bad provider" in str(exc_info.value)

    def test_get_available_providers(self):
        """Test getting available providers."""
        providers = get_available_providers()
        assert "mock" in providers
        assert "mock_no_config" in providers


class TestProviderConfigValidation:
    """Test provider configuration validation."""

    def setup_method(self):
        """Setup for each test."""
        clear_registry()
        register("test_provider", MockProvider)

    def teardown_method(self):
        """Cleanup after each test."""
        clear_registry()

    def test_validate_valid_config(self):
        """Test validating valid configuration."""
        config = {"provider": "test_provider", "param1": "value1"}

        result = validate_provider_config(config)
        assert result is True

    def test_validate_config_not_dict(self):
        """Test validating non-dictionary configuration."""
        with pytest.raises(ValueError) as exc_info:
            validate_provider_config("not a dict")

        assert "must be a dictionary" in str(exc_info.value)

    def test_validate_config_missing_provider(self):
        """Test validating configuration without provider name."""
        config = {"param1": "value1"}

        with pytest.raises(ValueError) as exc_info:
            validate_provider_config(config)

        assert "Provider name is required" in str(exc_info.value)

    def test_validate_config_provider_not_string(self):
        """Test validating configuration with non-string provider name."""
        config = {"provider": 123, "param1": "value1"}

        with pytest.raises(ValueError) as exc_info:
            validate_provider_config(config)

        assert "Provider name must be a string" in str(exc_info.value)

    def test_validate_config_unknown_provider(self):
        """Test validating configuration with unknown provider."""
        config = {"provider": "unknown_provider", "param1": "value1"}

        with pytest.raises(ValueError) as exc_info:
            validate_provider_config(config)

        assert "Unknown provider 'unknown_provider'" in str(exc_info.value)
        assert "Available:" in str(exc_info.value)


class TestRealProviders:
    """Test loading real providers."""

    def setup_method(self):
        """Setup for each test - register built-in providers."""
        clear_registry()

        # Manually register built-in providers for testing
        from marketpipe.ingestion.infrastructure.adapters import AlpacaMarketDataAdapter
        from marketpipe.ingestion.infrastructure.fake_adapter import (
            FakeMarketDataAdapter,
        )
        from marketpipe.ingestion.infrastructure.iex_adapter import IEXMarketDataAdapter

        register("fake", FakeMarketDataAdapter)
        register("alpaca", AlpacaMarketDataAdapter)
        register("iex", IEXMarketDataAdapter)

    def teardown_method(self):
        """Cleanup after each test."""
        clear_registry()

    def test_build_fake_provider(self):
        """Test building fake provider."""
        config = {"provider": "fake", "base_price": 150.0, "volatility": 0.05}

        provider = build_provider(config)

        # Should be the actual FakeMarketDataAdapter
        from marketpipe.ingestion.infrastructure.fake_adapter import (
            FakeMarketDataAdapter,
        )

        assert isinstance(provider, FakeMarketDataAdapter)

    def test_build_alpaca_provider(self):
        """Test building Alpaca provider."""
        config = {
            "provider": "alpaca",
            "api_key": "test_key",
            "api_secret": "test_secret",
            "base_url": "https://test.alpaca.markets",
        }

        provider = build_provider(config)

        # Should be the actual AlpacaMarketDataAdapter
        from marketpipe.ingestion.infrastructure.adapters import AlpacaMarketDataAdapter

        assert isinstance(provider, AlpacaMarketDataAdapter)

    def test_build_iex_provider(self):
        """Test building IEX provider."""
        config = {"provider": "iex", "api_token": "test_token", "is_sandbox": True}

        provider = build_provider(config)

        # Should be the actual IEXMarketDataAdapter
        from marketpipe.ingestion.infrastructure.iex_adapter import IEXMarketDataAdapter

        assert isinstance(provider, IEXMarketDataAdapter)
