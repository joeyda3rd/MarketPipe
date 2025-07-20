"""Unit tests for PolygonSymbolProvider.

Tests the Polygon.io symbol provider adapter including:
- HTTP mocking for API calls without network access
- Pagination handling across multiple pages
- Data mapping and validation
- Error handling and retry scenarios
- Configuration management

Uses monkeypatch pattern following project conventions for HTTP mocking.
All tests run in isolation without requiring environment variables or network access.
"""

from __future__ import annotations

import datetime
import types
from typing import Any

import httpx
import pytest
from marketpipe.domain import AssetClass, Status, SymbolRecord
from marketpipe.ingestion.symbol_providers import get
from marketpipe.ingestion.symbol_providers.polygon import PolygonSymbolProvider


class TestPolygonProviderRegistration:
    """Test Polygon provider registration and instantiation."""

    def test_polygon_provider_registered(self):
        """Test that polygon provider is registered and available."""
        from marketpipe.ingestion.symbol_providers import list_providers

        providers = list_providers()
        assert "polygon" in providers

    def test_get_polygon_provider_returns_correct_instance(self):
        """Test getting polygon provider instance."""
        provider = get("polygon", token="test-token")

        assert isinstance(provider, PolygonSymbolProvider)
        assert provider.name == "polygon"
        assert provider.cfg["token"] == "test-token"

    def test_polygon_provider_requires_token_config(self):
        """Test that polygon provider validates token configuration."""
        provider = PolygonSymbolProvider(token="test-key")
        assert provider.cfg["token"] == "test-key"


class TestPolygonProviderSinglePage:
    """Test Polygon provider with single page responses."""

    @pytest.mark.asyncio
    async def test_single_page_happy_path(self, monkeypatch):
        """Test single page response with two records."""
        # Mock API response
        mock_response_data = {
            "results": [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                    "currency_name": "USD",
                    "list_date": "1980-12-12",
                    "locale": "us",
                    "figi": "BBG000B9XRY4",
                    "cusip": "037833100",
                    "share_class_shares_outstanding": 15550061000,
                    "sector": "Technology",
                    "industry": "Consumer Electronics",
                },
                {
                    "ticker": "GOOGL",
                    "name": "Alphabet Inc.",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                    "currency_name": "USD",
                    "list_date": "2004-08-19",
                    "locale": "us",
                    "figi": "BBG009S39JX6",
                    "cusip": "02079K305",
                    "share_class_shares_outstanding": 5553000000,
                    "sector": "Technology",
                    "industry": "Internet Content & Information",
                },
            ],
            "count": 2,
            "status": "OK",
        }

        class MockAsyncClient:
            def __init__(self, timeout=None):
                self.timeout = timeout

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str, params: dict[str, Any] = None):
                # Verify expected URL and parameters
                assert url == "https://api.polygon.io/v3/reference/tickers"
                assert params["market"] == "stocks"
                assert params["apiKey"] == "test-token"
                assert params["limit"] == 1000

                return types.SimpleNamespace(
                    status_code=200, json=lambda: mock_response_data, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        # Test provider
        provider = PolygonSymbolProvider(as_of=datetime.date(2024, 1, 15), token="test-token")

        records = await provider.fetch_symbols()

        # Verify results
        assert len(records) == 2

        # Verify first record (AAPL)
        aapl = records[0]
        assert isinstance(aapl, SymbolRecord)
        assert aapl.ticker == "AAPL"
        assert aapl.exchange_mic == "XNAS"
        assert aapl.asset_class == AssetClass.EQUITY
        assert aapl.currency == "USD"
        assert aapl.status == Status.ACTIVE
        assert aapl.company_name == "Apple Inc."
        assert aapl.first_trade_date == datetime.date(1980, 12, 12)
        assert aapl.figi == "BBG000B9XRY4"
        assert aapl.cusip == "037833100"
        assert aapl.country == "US"
        assert aapl.sector == "Technology"
        assert aapl.industry == "Consumer Electronics"
        assert aapl.shares_outstanding == 15550061000
        assert aapl.as_of == datetime.date(2024, 1, 15)
        assert aapl.meta is not None

        # Verify second record (GOOGL)
        googl = records[1]
        assert googl.ticker == "GOOGL"
        assert googl.company_name == "Alphabet Inc."
        assert googl.first_trade_date == datetime.date(2004, 8, 19)

    @pytest.mark.asyncio
    async def test_empty_response_returns_empty_list(self, monkeypatch):
        """Test handling of empty API response."""
        mock_response_data = {"results": [], "count": 0, "status": "OK"}

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str, params: dict[str, Any] = None):
                return types.SimpleNamespace(
                    status_code=200, json=lambda: mock_response_data, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = PolygonSymbolProvider(token="test-token")
        records = await provider.fetch_symbols()

        assert len(records) == 0


class TestPolygonProviderPagination:
    """Test Polygon provider pagination handling."""

    @pytest.mark.asyncio
    async def test_pagination_two_pages(self, monkeypatch):
        """Test pagination handling across two pages."""
        # Mock first page response
        page_1_data = {
            "results": [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                    "currency_name": "USD",
                    "locale": "us",
                }
            ],
            "next_url": "https://api.polygon.io/v3/reference/tickers?cursor=YWZ0ZXI9MjAyMy0wMi0wMVQwMCUzQTAwJTNBMDAuMDAwWiZsaW1pdD0yJnNvcnQ9dGlja2Vy",
        }

        # Mock second page response
        page_2_data = {
            "results": [
                {
                    "ticker": "GOOGL",
                    "name": "Alphabet Inc.",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                    "currency_name": "USD",
                    "locale": "us",
                }
            ],
            "count": 1,
            "status": "OK",
        }

        call_count = 0

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str, params: dict[str, Any] = None):
                nonlocal call_count
                call_count += 1

                if call_count == 1:
                    # First call - verify initial parameters
                    assert url == "https://api.polygon.io/v3/reference/tickers"
                    assert params["market"] == "stocks"
                    assert params["limit"] == 1000
                    assert params["apiKey"] == "test-token"
                    return types.SimpleNamespace(
                        status_code=200, json=lambda: page_1_data, raise_for_status=lambda: None
                    )
                elif call_count == 2:
                    # Second call - verify next_url and only apiKey param
                    assert "cursor" in url
                    assert params == {"apiKey": "test-token"}
                    return types.SimpleNamespace(
                        status_code=200, json=lambda: page_2_data, raise_for_status=lambda: None
                    )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = PolygonSymbolProvider(token="test-token")
        records = await provider.fetch_symbols()

        # Verify we got records from both pages
        assert len(records) == 2
        assert call_count == 2

        # Verify records from both pages
        tickers = [r.ticker for r in records]
        assert "AAPL" in tickers
        assert "GOOGL" in tickers

    @pytest.mark.asyncio
    async def test_pagination_stops_when_no_next_url(self, monkeypatch):
        """Test pagination stops when next_url is not present."""
        mock_response_data = {
            "results": [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                    "currency_name": "USD",
                    "locale": "us",
                }
            ],
            "count": 1,
            "status": "OK",
            # No next_url field - should stop pagination
        }

        call_count = 0

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str, params: dict[str, Any] = None):
                nonlocal call_count
                call_count += 1
                return types.SimpleNamespace(
                    status_code=200, json=lambda: mock_response_data, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = PolygonSymbolProvider(token="test-token")
        records = await provider.fetch_symbols()

        # Should only make one call since no next_url
        assert call_count == 1
        assert len(records) == 1


class TestPolygonProviderMapping:
    """Test data mapping and field transformations."""

    @pytest.mark.asyncio
    async def test_status_and_asset_class_mapping(self, monkeypatch):
        """Test mapping of active/inactive status and asset types."""
        mock_response_data = {
            "results": [
                {
                    "ticker": "VTI",
                    "name": "Vanguard Total Stock Market ETF",
                    "primary_exchange": "ARCX",
                    "type": "ETF",
                    "active": True,
                    "currency_name": "USD",
                    "locale": "us",
                },
                {
                    "ticker": "REALTY",
                    "name": "REIT Company",
                    "primary_exchange": "XNYS",
                    "type": "REIT",
                    "active": False,  # Delisted
                    "currency_name": "USD",
                    "locale": "us",
                },
                {
                    "ticker": "ADRCORP",
                    "name": "ADR Corporation",
                    "primary_exchange": "XNAS",
                    "type": "ADRC",
                    "active": True,
                    "currency_name": "USD",
                    "locale": "us",
                },
            ]
        }

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str, params: dict[str, Any] = None):
                return types.SimpleNamespace(
                    status_code=200, json=lambda: mock_response_data, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = PolygonSymbolProvider(token="test-token")
        records = await provider.fetch_symbols()

        assert len(records) == 3

        # Test ETF mapping
        vti = next(r for r in records if r.ticker == "VTI")
        assert vti.asset_class == AssetClass.ETF
        assert vti.status == Status.ACTIVE
        assert vti.exchange_mic == "ARCX"

        # Test REIT mapping and delisted status
        reit = next(r for r in records if r.ticker == "REALTY")
        assert reit.asset_class == AssetClass.REIT
        assert reit.status == Status.DELISTED

        # Test ADR mapping
        adr = next(r for r in records if r.ticker == "ADRCORP")
        assert adr.asset_class == AssetClass.ADR
        assert adr.status == Status.ACTIVE

    @pytest.mark.asyncio
    async def test_exchange_mic_mapping(self, monkeypatch):
        """Test mapping of exchange codes to MIC identifiers."""
        mock_response_data = {
            "results": [
                {
                    "ticker": "TEST1",
                    "primary_exchange": "XNYS",  # Known exchange
                    "type": "CS",
                    "active": True,
                    "currency_name": "USD",
                },
                {
                    "ticker": "TEST2",
                    "primary_exchange": "UNKNOWN_EXCHANGE",  # Unknown exchange
                    "type": "CS",
                    "active": True,
                    "currency_name": "USD",
                },
            ]
        }

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str, params: dict[str, Any] = None):
                return types.SimpleNamespace(
                    status_code=200, json=lambda: mock_response_data, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = PolygonSymbolProvider(token="test-token")
        records = await provider.fetch_symbols()

        # Known exchange mapped correctly
        test1 = next(r for r in records if r.ticker == "TEST1")
        assert test1.exchange_mic == "XNYS"

        # Unknown exchange truncated to 4 chars uppercase
        test2 = next(r for r in records if r.ticker == "TEST2")
        assert test2.exchange_mic == "UNKN"

    @pytest.mark.asyncio
    async def test_optional_fields_handling(self, monkeypatch):
        """Test handling of missing or null optional fields."""
        mock_response_data = {
            "results": [
                {
                    "ticker": "MINIMAL",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                    "currency_name": "USD",
                    # Missing: name, list_date, figi, cusip, etc.
                }
            ]
        }

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str, params: dict[str, Any] = None):
                return types.SimpleNamespace(
                    status_code=200, json=lambda: mock_response_data, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = PolygonSymbolProvider(token="test-token")
        records = await provider.fetch_symbols()

        assert len(records) == 1
        record = records[0]

        # Required fields should be set
        assert record.ticker == "MINIMAL"
        assert record.exchange_mic == "XNAS"
        assert record.currency == "USD"
        assert record.status == Status.ACTIVE

        # Optional fields should be None
        assert record.company_name is None
        assert record.first_trade_date is None
        assert record.figi is None
        assert record.cusip is None
        assert record.country is None
        assert record.sector is None
        assert record.industry is None


class TestPolygonProviderErrorHandling:
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_http_error_raises(self, monkeypatch):
        """Test that HTTP errors are properly raised."""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str, params: dict[str, Any] = None):
                def raise_for_status():
                    raise httpx.HTTPStatusError(
                        "429 Too Many Requests",
                        request=None,
                        response=types.SimpleNamespace(status_code=429),
                    )

                return types.SimpleNamespace(status_code=429, raise_for_status=raise_for_status)

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = PolygonSymbolProvider(token="test-token")

        with pytest.raises(httpx.HTTPStatusError):
            await provider.fetch_symbols()

    @pytest.mark.asyncio
    async def test_missing_token_raises_key_error(self):
        """Test that missing API token raises KeyError."""
        provider = PolygonSymbolProvider()  # No token provided

        with pytest.raises(KeyError, match="Polygon API token not provided"):
            await provider.fetch_symbols()

    @pytest.mark.asyncio
    async def test_invalid_json_response(self, monkeypatch):
        """Test handling of invalid JSON response."""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str, params: dict[str, Any] = None):
                def bad_json():
                    raise ValueError("Invalid JSON")

                return types.SimpleNamespace(
                    status_code=200, json=bad_json, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = PolygonSymbolProvider(token="test-token")

        with pytest.raises(ValueError, match="Invalid JSON"):
            await provider.fetch_symbols()

    @pytest.mark.asyncio
    async def test_timeout_configuration(self, monkeypatch):
        """Test that AsyncClient is configured with proper timeout."""
        client_configs = []

        class MockAsyncClient:
            def __init__(self, timeout=None):
                client_configs.append(timeout)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str, params: dict[str, Any] = None):
                return types.SimpleNamespace(
                    status_code=200, json=lambda: {"results": []}, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = PolygonSymbolProvider(token="test-token")
        await provider.fetch_symbols()

        # Verify timeout was set to 30 seconds
        assert len(client_configs) == 1
        assert client_configs[0] == 30


class TestPolygonProviderConfiguration:
    """Test provider configuration and initialization."""

    def test_provider_name_attribute(self):
        """Test that provider has correct name attribute."""
        assert PolygonSymbolProvider.name == "polygon"

    def test_as_of_date_default(self):
        """Test default as_of date is today."""
        provider = PolygonSymbolProvider(token="test")
        assert provider.as_of == datetime.date.today()

    def test_as_of_date_custom(self):
        """Test custom as_of date is preserved."""
        custom_date = datetime.date(2024, 1, 15)
        provider = PolygonSymbolProvider(as_of=custom_date, token="test")
        assert provider.as_of == custom_date

    def test_configuration_preserved(self):
        """Test that all configuration is preserved."""
        config = {"token": "test-token", "extra_param": "extra_value"}
        provider = PolygonSymbolProvider(**config)
        assert provider.cfg["token"] == "test-token"
        assert provider.cfg["extra_param"] == "extra_value"
