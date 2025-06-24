"""Unit tests for DummyProvider.

Tests the dummy symbol provider adapter including:
- Basic functionality and data validation
- Both sync and async execution paths
- Configuration handling
- Integration with SymbolRecord validation

Uses no network access and provides static test data.
Achieves 100% branch coverage for the dummy provider.
"""

from __future__ import annotations

import datetime

import pytest

from marketpipe.domain import SymbolRecord, AssetClass, Status
from marketpipe.ingestion.symbol_providers import get
from marketpipe.ingestion.symbol_providers.dummy import DummyProvider


class TestDummyProviderRegistration:
    """Test DummyProvider registration and instantiation."""

    def test_dummy_provider_registered(self):
        """Test that dummy provider is registered and available."""
        from marketpipe.ingestion.symbol_providers import list_providers

        providers = list_providers()
        assert "dummy" in providers

    def test_get_dummy_provider_returns_correct_instance(self):
        """Test getting dummy provider instance."""
        provider = get("dummy")

        assert isinstance(provider, DummyProvider)
        assert provider.name == "dummy"

    def test_dummy_provider_with_configuration(self):
        """Test dummy provider with configuration parameters."""
        provider = DummyProvider(some_config="test_value")
        assert provider.cfg["some_config"] == "test_value"


class TestDummyProviderAsync:
    """Test DummyProvider async functionality."""

    @pytest.mark.asyncio
    async def test_async_fetch_symbols(self):
        """Test async fetch_symbols method returns valid data."""
        provider = DummyProvider(as_of=datetime.date(2024, 1, 15))

        records = await provider.fetch_symbols()

        # Verify we get exactly one record
        assert len(records) == 1

        # Verify the record structure
        record = records[0]
        assert isinstance(record, SymbolRecord)
        assert record.ticker == "TEST"
        assert record.exchange_mic == "XNAS"
        assert record.asset_class == AssetClass.EQUITY
        assert record.currency == "USD"
        assert record.status == Status.ACTIVE
        assert record.as_of == datetime.date(2024, 1, 15)

    @pytest.mark.asyncio
    async def test_async_fetch_raw(self):
        """Test async _fetch_raw method returns expected structure."""
        provider = DummyProvider(as_of=datetime.date(2024, 1, 15))

        raw_data = await provider._fetch_raw()

        # Verify raw data structure
        assert isinstance(raw_data, list)
        assert len(raw_data) == 1

        raw_record = raw_data[0]
        assert raw_record["ticker"] == "TEST"
        assert raw_record["exchange_mic"] == "XNAS"
        assert raw_record["asset_class"] == "EQUITY"
        assert raw_record["currency"] == "USD"
        assert raw_record["status"] == "ACTIVE"
        assert raw_record["as_of"] == "2024-01-15"

    @pytest.mark.asyncio
    async def test_map_to_records(self):
        """Test _map_to_records method properly converts data."""
        provider = DummyProvider(as_of=datetime.date(2024, 1, 15))

        # Create test payload
        test_payload = [
            {
                "ticker": "TEST",
                "exchange_mic": "XNAS",
                "asset_class": "EQUITY",
                "currency": "USD",
                "status": "ACTIVE",
                "as_of": "2024-01-15",
            }
        ]

        records = provider._map_to_records(test_payload)

        assert len(records) == 1
        record = records[0]
        assert record.ticker == "TEST"
        assert record.exchange_mic == "XNAS"


class TestDummyProviderSync:
    """Test DummyProvider sync functionality."""

    def test_sync_fetch_symbols(self):
        """Test sync fetch_symbols_sync method."""
        provider = DummyProvider(as_of=datetime.date(2024, 1, 15))

        records = provider.fetch_symbols_sync()

        # Verify we get exactly one record
        assert len(records) == 1

        # Verify the record structure
        record = records[0]
        assert isinstance(record, SymbolRecord)
        assert record.ticker == "TEST"
        assert record.exchange_mic == "XNAS"
        assert record.asset_class == AssetClass.EQUITY


class TestDummyProviderConfiguration:
    """Test DummyProvider configuration handling."""

    def test_provider_name_attribute(self):
        """Test that provider has correct name attribute."""
        provider = DummyProvider()
        assert provider.name == "dummy"

    def test_as_of_date_default(self):
        """Test default as_of date behavior."""
        provider = DummyProvider()
        assert provider.as_of == datetime.date.today()

    def test_as_of_date_custom(self):
        """Test custom as_of date."""
        custom_date = datetime.date(2024, 1, 15)
        provider = DummyProvider(as_of=custom_date)
        assert provider.as_of == custom_date

    def test_configuration_preserved(self):
        """Test that provider configuration is preserved."""
        provider = DummyProvider(
            as_of=datetime.date(2024, 1, 15), test_param="test_value", numeric_param=42
        )

        assert provider.cfg["test_param"] == "test_value"
        assert provider.cfg["numeric_param"] == 42
        assert provider.as_of == datetime.date(2024, 1, 15)


class TestDummyProviderErrorHandling:
    """Test DummyProvider error handling and edge cases."""

    def test_map_to_records_with_empty_payload(self):
        """Test _map_to_records handles empty payload gracefully."""
        provider = DummyProvider()

        records = provider._map_to_records([])

        assert records == []

    def test_map_to_records_with_invalid_data(self):
        """Test _map_to_records handles invalid data gracefully."""
        provider = DummyProvider()

        # Test with invalid data that would fail SymbolRecord validation
        invalid_payload = [
            {
                "ticker": "",  # Invalid empty ticker
                "exchange_mic": "INVALID_LONG_MIC",  # Invalid MIC
                "asset_class": "INVALID_CLASS",  # Invalid asset class
                "currency": "INVALID_CURRENCY_CODE",  # Invalid currency
                "status": "INVALID_STATUS",  # Invalid status
                "as_of": "invalid-date",  # Invalid date
            }
        ]

        # Should handle validation errors gracefully via safe_create
        records = provider._map_to_records(invalid_payload)

        # safe_create should filter out invalid records
        assert len(records) == 0

    def test_map_to_records_with_partial_valid_data(self):
        """Test _map_to_records handles mix of valid and invalid data."""
        provider = DummyProvider()

        # Mix of valid and invalid records
        mixed_payload = [
            {
                "ticker": "VALID",
                "exchange_mic": "XNAS",
                "asset_class": "EQUITY",
                "currency": "USD",
                "status": "ACTIVE",
                "as_of": "2024-01-15",
            },
            {
                "ticker": "",  # Invalid
                "exchange_mic": "INVALID",
                "asset_class": "INVALID",
            },
        ]

        records = provider._map_to_records(mixed_payload)

        # Should only get the valid record
        assert len(records) == 1
        assert records[0].ticker == "VALID"


class TestDummyProviderIntegration:
    """Integration tests for DummyProvider with other components."""

    @pytest.mark.asyncio
    async def test_end_to_end_async_workflow(self):
        """Test complete async workflow from creation to symbol records."""
        # Create provider
        provider = get("dummy", as_of=datetime.date(2024, 1, 15))

        # Fetch symbols
        records = await provider.fetch_symbols()

        # Verify complete workflow
        assert len(records) == 1
        record = records[0]
        assert record.ticker == "TEST"
        assert record.as_of == datetime.date(2024, 1, 15)

        # Verify all required fields are populated
        assert record.exchange_mic is not None
        assert record.asset_class is not None
        assert record.currency is not None
        assert record.status is not None

    def test_end_to_end_sync_workflow(self):
        """Test complete sync workflow from creation to symbol records."""
        # Create provider
        provider = get("dummy", as_of=datetime.date(2024, 1, 15))

        # Fetch symbols synchronously
        records = provider.fetch_symbols_sync()

        # Verify complete workflow
        assert len(records) == 1
        record = records[0]
        assert record.ticker == "TEST"
        assert record.as_of == datetime.date(2024, 1, 15)

    def test_provider_consistency_across_calls(self):
        """Test that provider returns consistent results across multiple calls."""
        provider = DummyProvider(as_of=datetime.date(2024, 1, 15))

        # Make multiple calls
        records1 = provider.fetch_symbols_sync()
        records2 = provider.fetch_symbols_sync()

        # Should be identical
        assert len(records1) == len(records2) == 1
        assert records1[0].ticker == records2[0].ticker == "TEST"
        assert records1[0].as_of == records2[0].as_of
