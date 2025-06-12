# SPDX-License-Identifier: Apache-2.0
"""Tests for provider registry system."""

import pytest
from typing import List
from unittest.mock import Mock, patch

from marketpipe.domain.entities import OHLCVBar
from marketpipe.domain.value_objects import Symbol, TimeRange
from marketpipe.domain.market_data import IMarketDataProvider, ProviderMetadata
from marketpipe.ingestion.infrastructure.provider_registry import (
    register,
    get,
    list_providers,
    clear_registry,
    provider,
    is_registered,
)


class MockProvider(IMarketDataProvider):
    """Mock provider for testing."""

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> List[OHLCVBar]:
        return []

    async def get_supported_symbols(self) -> List[Symbol]:
        return [Symbol.from_string("TEST")]

    async def is_available(self) -> bool:
        return True

    def get_provider_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="mock",
            supports_real_time=False,
            supports_historical=True,
            rate_limit_per_minute=None,
            minimum_time_resolution="1m",
            maximum_history_days=None,
        )


class TestProviderRegistry:
    """Test provider registry functionality."""

    def setup_method(self):
        """Clear registry before each test."""
        clear_registry()
        # Prevent auto-registration during tests
        import marketpipe.ingestion.infrastructure.provider_registry as registry
        registry._AUTO_REGISTERED = True

    def teardown_method(self):
        """Clear registry after each test."""
        clear_registry()

    def test_register_and_get_provider(self):
        """Test registering and retrieving providers."""
        # Register a provider
        register("test", MockProvider)

        # Check it's registered
        assert is_registered("test")
        assert "test" in list_providers()

        # Get the provider class
        provider_cls = get("test")
        assert provider_cls == MockProvider

    def test_get_nonexistent_provider(self):
        """Test getting a provider that doesn't exist."""
        with pytest.raises(KeyError) as exc_info:
            get("nonexistent")

        assert "Provider 'nonexistent' not found" in str(exc_info.value)
        # Just check that the error message contains available providers info
        assert "Available providers:" in str(exc_info.value)

    def test_list_providers_empty(self):
        """Test listing providers when none are registered."""
        providers = list_providers()
        assert providers == []

    def test_list_providers_multiple(self):
        """Test listing multiple providers."""
        register("provider1", MockProvider)
        register("provider2", MockProvider)

        providers = list_providers()
        assert set(providers) == {"provider1", "provider2"}

    def test_provider_decorator(self):
        """Test the provider decorator."""

        @provider("decorated")
        class DecoratedProvider(MockProvider):
            pass

        # Check it was automatically registered
        assert is_registered("decorated")
        assert get("decorated") == DecoratedProvider

    def test_register_invalid_provider(self):
        """Test registering a class that doesn't implement IMarketDataProvider."""

        class InvalidProvider:
            pass

        with pytest.raises(ValueError) as exc_info:
            register("invalid", InvalidProvider)

        assert "must implement IMarketDataProvider" in str(exc_info.value)

    def test_clear_registry(self):
        """Test clearing the registry."""
        register("test1", MockProvider)
        register("test2", MockProvider)

        assert len(list_providers()) == 2

        clear_registry()

        assert list_providers() == []
        assert not is_registered("test1")
        assert not is_registered("test2")

    @patch("marketpipe.ingestion.infrastructure.provider_registry.entry_points")
    def test_auto_register_from_entry_points(self, mock_entry_points):
        """Test auto-registration from entry points."""
        # Mock entry points
        mock_ep = Mock()
        mock_ep.name = "auto_provider"
        mock_ep.load.return_value = MockProvider

        mock_entry_points.return_value = {"marketpipe.providers": [mock_ep]}

        # Clear registry to trigger auto-registration
        clear_registry()

        # This should trigger auto-registration
        providers = list_providers()

        assert "auto_provider" in providers
        assert get("auto_provider") == MockProvider

    @patch("marketpipe.ingestion.infrastructure.provider_registry.entry_points")
    def test_auto_register_handles_load_error(self, mock_entry_points):
        """Test auto-registration handles errors gracefully."""
        # Mock entry points with failing load
        mock_ep = Mock()
        mock_ep.name = "failing_provider"
        mock_ep.load.side_effect = ImportError("Failed to import")

        mock_entry_points.return_value = {"marketpipe.providers": [mock_ep]}

        # Clear registry to trigger auto-registration
        clear_registry()

        # This should not raise an exception
        providers = list_providers()

        # Provider should not be registered
        assert "failing_provider" not in providers

    @patch("marketpipe.ingestion.infrastructure.provider_registry.entry_points")
    def test_auto_register_handles_entry_points_error(self, mock_entry_points):
        """Test auto-registration handles entry points errors gracefully."""
        # Mock entry_points to raise an exception
        mock_entry_points.side_effect = Exception("Entry points failed")

        # Clear registry to trigger auto-registration
        clear_registry()

        # This should not raise an exception
        providers = list_providers()

        # Should return empty list
        assert providers == []


class TestBuiltinProviders:
    """Test that built-in providers are registered."""

    def setup_method(self):
        """Setup for each test - register built-in providers."""
        clear_registry()
        
        # Manually register built-in providers for testing
        from marketpipe.ingestion.infrastructure.fake_adapter import FakeMarketDataAdapter
        from marketpipe.ingestion.infrastructure.adapters import AlpacaMarketDataAdapter
        from marketpipe.ingestion.infrastructure.iex_adapter import IEXMarketDataAdapter
        
        register("fake", FakeMarketDataAdapter)
        register("alpaca", AlpacaMarketDataAdapter)
        register("iex", IEXMarketDataAdapter)

    def teardown_method(self):
        """Cleanup after each test."""
        clear_registry()

    def test_alpaca_provider_registered(self):
        """Test that Alpaca provider is registered."""
        providers = list_providers()
        assert "alpaca" in providers

    def test_iex_provider_registered(self):
        """Test that IEX provider is registered."""
        providers = list_providers()
        assert "iex" in providers

    def test_fake_provider_registered(self):
        """Test that fake provider is registered."""
        providers = list_providers()
        assert "fake" in providers

    def test_all_builtin_providers_implement_interface(self):
        """Test that all built-in providers implement IMarketDataProvider."""
        for provider_name in ["alpaca", "iex", "fake"]:
            provider_cls = get(provider_name)
            assert issubclass(provider_cls, IMarketDataProvider)

            # Test that they have the expected methods
            assert hasattr(provider_cls, "fetch_bars_for_symbol")
            assert hasattr(provider_cls, "get_supported_symbols")
            assert hasattr(provider_cls, "is_available")
            assert hasattr(provider_cls, "get_provider_metadata")


class TestProviderInstantiation:
    """Test provider instantiation patterns."""

    def test_alpaca_provider_from_config(self):
        """Test Alpaca provider creation from config."""
        from marketpipe.ingestion.infrastructure.adapters import AlpacaMarketDataAdapter

        config = {
            "api_key": "test_key",
            "api_secret": "test_secret",
            "base_url": "https://test.alpaca.markets",
            "feed_type": "iex",
            "rate_limit_per_min": 100,
        }

        provider = AlpacaMarketDataAdapter.from_config(config)
        assert provider._api_key == "test_key"
        assert provider._api_secret == "test_secret"
        assert provider._base_url == "https://test.alpaca.markets"
        assert provider._feed_type == "iex"

    def test_iex_provider_from_config(self):
        """Test IEX provider creation from config."""
        from marketpipe.ingestion.infrastructure.iex_adapter import IEXMarketDataAdapter

        config = {"api_token": "test_token", "is_sandbox": True, "timeout": 60.0}

        provider = IEXMarketDataAdapter.from_config(config)
        assert provider._api_token == "test_token"
        assert provider._is_sandbox == True
        assert provider._timeout == 60.0

    def test_fake_provider_from_config(self):
        """Test fake provider creation from config."""
        from marketpipe.ingestion.infrastructure.fake_adapter import (
            FakeMarketDataAdapter,
        )

        config = {
            "base_price": 150.0,
            "volatility": 0.05,
            "fail_probability": 0.1,
            "supported_symbols": ["TEST1", "TEST2"],
        }

        provider = FakeMarketDataAdapter.from_config(config)
        assert provider._base_price == 150.0
        assert provider._volatility == 0.05
        assert provider._fail_probability == 0.1
        assert provider._supported_symbols == ["TEST1", "TEST2"]
