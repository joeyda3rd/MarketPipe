from __future__ import annotations

import datetime
from unittest.mock import patch

import pytest

from marketpipe.domain import SymbolRecord
from marketpipe.ingestion.symbol_providers import SymbolProviderBase, get, list_providers, register
from marketpipe.ingestion.symbol_providers.dummy import DummyProvider


class TestProviderRegistry:
    """Test the provider registry functionality."""

    def test_registry_lists_dummy(self):
        """Test that dummy provider is registered and listed."""
        providers = list_providers()
        assert "dummy" in providers

    def test_get_provider_returns_instance(self):
        """Test getting a provider instance by name."""
        provider = get("dummy")
        assert isinstance(provider, DummyProvider)
        assert isinstance(provider, SymbolProviderBase)

    def test_get_provider_with_config(self):
        """Test provider instantiation with configuration."""
        as_of_date = datetime.date(2024, 1, 15)
        provider = get("dummy", as_of=as_of_date, token="test-token")

        assert provider.as_of == as_of_date
        assert provider.cfg["token"] == "test-token"

    def test_get_unknown_provider_raises(self):
        """Test that unknown provider name raises ValueError."""
        with pytest.raises(ValueError, match="Unknown symbol provider 'nonexistent'"):
            get("nonexistent")

    def test_duplicate_registration_raises(self):
        """Test that duplicate registration raises ValueError."""

        # Define a test provider class that we'll try to register twice
        class TestProvider(SymbolProviderBase):
            async def _fetch_raw(self):
                return []

            def _map_to_records(self, payload):
                return []

        # First registration should work
        @register("test_duplicate")
        class FirstProvider(SymbolProviderBase):
            async def _fetch_raw(self):
                return []

            def _map_to_records(self, payload):
                return []

        # Second registration with same name should raise
        with pytest.raises(ValueError, match="Provider name 'test_duplicate' already registered"):

            @register("test_duplicate")
            class SecondProvider(SymbolProviderBase):
                async def _fetch_raw(self):
                    return []

                def _map_to_records(self, payload):
                    return []

    def test_register_non_provider_raises(self):
        """Test that registering non-SymbolProviderBase class raises TypeError."""

        class NotAProvider:
            pass

        with pytest.raises(TypeError, match="Class must inherit SymbolProviderBase"):
            register("invalid")(NotAProvider)

    def test_register_injects_name(self):
        """Test that registration injects the name attribute."""

        @register("test_name_injection")
        class NamedProvider(SymbolProviderBase):
            async def _fetch_raw(self):
                return []

            def _map_to_records(self, payload):
                return []

        assert NamedProvider.name == "test_name_injection"


class TestSymbolProviderBase:
    """Test the abstract base class functionality."""

    @pytest.mark.asyncio
    async def test_fetch_symbols_returns_symbolrecord(self):
        """Test fetch_symbols returns SymbolRecord instances."""
        provider = get("dummy")
        records = await provider.fetch_symbols()

        assert len(records) == 1
        assert isinstance(records[0], SymbolRecord)
        assert records[0].ticker == "TEST"
        assert records[0].exchange_mic == "XNAS"
        assert records[0].asset_class.value == "EQUITY"
        assert records[0].currency == "USD"
        assert records[0].status.value == "ACTIVE"

    def test_sync_wrapper(self):
        """Test sync wrapper returns same result as async method."""
        provider = get("dummy")

        # Get results from sync wrapper
        sync_records = provider.fetch_symbols_sync()

        # Compare with async results
        import asyncio

        async_records = asyncio.run(provider.fetch_symbols())

        assert len(sync_records) == len(async_records)
        assert sync_records[0].ticker == async_records[0].ticker
        assert sync_records[0].exchange_mic == async_records[0].exchange_mic

    def test_init_with_default_date(self):
        """Test provider initialization with default as_of date."""
        provider = get("dummy")
        assert provider.as_of == datetime.date.today()

    def test_init_with_custom_date(self):
        """Test provider initialization with custom as_of date."""
        custom_date = datetime.date(2024, 1, 15)
        provider = get("dummy", as_of=custom_date)
        assert provider.as_of == custom_date

    def test_init_with_provider_config(self):
        """Test provider initialization with configuration."""
        provider = get("dummy", token="test-token", base_url="https://example.com")
        assert provider.cfg["token"] == "test-token"
        assert provider.cfg["base_url"] == "https://example.com"

    def test_abstract_methods_enforcement(self):
        """Test that abstract methods must be implemented."""

        # Class with missing _fetch_raw should fail
        class IncompleteProvider1(SymbolProviderBase):
            def _map_to_records(self, payload):
                return []

        with pytest.raises(TypeError):
            IncompleteProvider1()

        # Class with missing _map_to_records should fail
        class IncompleteProvider2(SymbolProviderBase):
            async def _fetch_raw(self):
                return []

        with pytest.raises(TypeError):
            IncompleteProvider2()

    @pytest.mark.asyncio
    async def test_fetch_symbols_workflow(self):
        """Test the complete fetch_symbols workflow."""
        provider = get("dummy")

        # Patch the internal methods to verify they're called
        with patch.object(provider, "_fetch_raw") as mock_fetch:
            with patch.object(provider, "_map_to_records") as mock_map:
                # Setup return values
                mock_fetch.return_value = [{"test": "data"}]
                mock_map.return_value = [
                    SymbolRecord(
                        ticker="TEST",
                        exchange_mic="XNAS",
                        asset_class="EQUITY",
                        currency="USD",
                        status="ACTIVE",
                        as_of=datetime.date.today(),
                    )
                ]

                # Call fetch_symbols
                records = await provider.fetch_symbols()

                # Verify methods were called
                mock_fetch.assert_called_once()
                mock_map.assert_called_once_with([{"test": "data"}])

                # Verify result
                assert len(records) == 1
                assert isinstance(records[0], SymbolRecord)


class TestDummyProvider:
    """Test the dummy provider implementation."""

    @pytest.mark.asyncio
    async def test_dummy_fetch_raw_returns_static_data(self):
        """Test that dummy provider returns static data."""
        provider = get("dummy")
        raw_data = await provider._fetch_raw()

        assert len(raw_data) == 1
        assert raw_data[0]["ticker"] == "TEST"
        assert raw_data[0]["exchange_mic"] == "XNAS"
        assert raw_data[0]["asset_class"] == "EQUITY"
        assert raw_data[0]["currency"] == "USD"
        assert raw_data[0]["status"] == "ACTIVE"

    def test_dummy_map_to_records(self):
        """Test that dummy provider maps data to SymbolRecord."""
        provider = get("dummy")

        test_data = [
            {
                "ticker": "TEST2",
                "exchange_mic": "XNAS",
                "asset_class": "EQUITY",
                "currency": "USD",
                "status": "ACTIVE",
                "as_of": "2024-01-01",
            }
        ]

        records = provider._map_to_records(test_data)

        assert len(records) == 1
        assert isinstance(records[0], SymbolRecord)
        assert records[0].ticker == "TEST2"

    @pytest.mark.asyncio
    async def test_dummy_as_of_date_in_response(self):
        """Test that dummy provider includes as_of date in response."""
        custom_date = datetime.date(2024, 2, 14)
        provider = get("dummy", as_of=custom_date)

        raw_data = await provider._fetch_raw()
        assert raw_data[0]["as_of"] == str(custom_date)

    def test_dummy_provider_name(self):
        """Test that dummy provider has correct name."""
        assert DummyProvider.name == "dummy"

    @pytest.mark.asyncio
    async def test_dummy_full_workflow(self):
        """Test complete dummy provider workflow."""
        provider = get("dummy")
        records = await provider.fetch_symbols()

        assert len(records) == 1
        record = records[0]

        # Verify all required fields are present and valid
        assert record.ticker == "TEST"
        assert record.exchange_mic == "XNAS"
        assert record.asset_class.value == "EQUITY"
        assert record.currency == "USD"
        assert record.status.value == "ACTIVE"
        assert record.as_of == provider.as_of
