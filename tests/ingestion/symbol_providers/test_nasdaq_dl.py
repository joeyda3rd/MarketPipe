"""Unit tests for NasdaqDailyListProvider.

Tests the Nasdaq Daily List symbol provider adapter including:
- HTTP mocking for text file downloads without network access
- Pipe-delimited text parsing and validation
- Data mapping and field validation
- Footer date parsing and as_of override handling
- Business rule filters (test issues, ETF inclusion)
- Error handling and malformed data scenarios

Uses respx for HTTP mocking following project conventions.
All tests run in isolation without requiring network access.
"""

from __future__ import annotations

import datetime
import types

import httpx
import pytest

from marketpipe.domain import AssetClass, Status, SymbolRecord
from marketpipe.ingestion.symbol_providers import get, list_providers
from marketpipe.ingestion.symbol_providers.nasdaq_dl import NasdaqDailyListProvider


class TestNasdaqDailyListProviderRegistration:
    """Test Nasdaq Daily List provider registration and instantiation."""

    def test_nasdaq_dl_provider_registered(self):
        """Test that nasdaq_dl provider is registered and available."""
        providers = list_providers()
        assert "nasdaq_dl" in providers

    def test_get_nasdaq_dl_provider_returns_correct_instance(self):
        """Test getting nasdaq_dl provider instance."""
        provider = get("nasdaq_dl")

        assert isinstance(provider, NasdaqDailyListProvider)
        assert provider.name == "nasdaq_dl"

    def test_nasdaq_dl_provider_configuration(self):
        """Test nasdaq_dl provider accepts configuration parameters."""
        provider = NasdaqDailyListProvider(
            as_of=datetime.date(2024, 1, 15), include_etfs=False, skip_test_issues=True
        )

        assert provider.as_of == datetime.date(2024, 1, 15)
        assert provider.cfg["include_etfs"] is False
        assert provider.cfg["skip_test_issues"] is True


class TestNasdaqDailyListProviderParsing:
    """Test Nasdaq Daily List text file parsing."""

    @pytest.mark.asyncio
    async def test_fetch_symbols_happy_path(self, monkeypatch):
        """Test successful parsing of Nasdaq Daily List with mixed securities."""
        # Mock file content with header, two data rows, and footer
        mock_file_content = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc.|Q|N|N|100|N|N
SPY|SPDR S&P 500 ETF Trust|P|N|N|100|Y|N
GOOGL|Alphabet Inc. Class A|Q|N|N|100|N|N
File Creation Time: 20250619
"""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                self.timeout = timeout

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str):
                # Verify expected URL
                assert url == "https://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"

                return types.SimpleNamespace(
                    status_code=200, text=mock_file_content, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        # Test provider without as_of (should parse from footer)
        provider = NasdaqDailyListProvider()
        records = await provider.fetch_symbols()

        # Verify results
        assert len(records) == 3

        # Verify AAPL record
        aapl = next(r for r in records if r.ticker == "AAPL")
        assert isinstance(aapl, SymbolRecord)
        assert aapl.ticker == "AAPL"
        assert aapl.exchange_mic == "XNAS"  # Q -> XNAS
        assert aapl.asset_class == AssetClass.EQUITY
        assert aapl.currency == "USD"
        assert aapl.status == Status.ACTIVE
        assert aapl.company_name == "Apple Inc."
        assert aapl.as_of == datetime.date.today()  # Defaults to today when no as_of provided
        assert aapl.meta["market_category"] == "Q"
        assert aapl.meta["etf_flag"] == "N"
        assert aapl.meta["source"] == "nasdaq_daily_list"

        # Verify SPY ETF record
        spy = next(r for r in records if r.ticker == "SPY")
        assert spy.asset_class == AssetClass.ETF
        assert spy.exchange_mic == "ARCX"  # P -> ARCX
        assert spy.meta["etf_flag"] == "Y"

        # Verify GOOGL record
        googl = next(r for r in records if r.ticker == "GOOGL")
        assert googl.company_name == "Alphabet Inc. Class A"
        assert googl.exchange_mic == "XNAS"  # Q -> XNAS

    @pytest.mark.asyncio
    async def test_test_issue_skipped(self, monkeypatch):
        """Test that test issues are skipped when skip_test_issues=True."""
        mock_file_content = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc.|Q|N|N|100|N|N
TEST|Test Security|Q|Y|N|100|N|N
GOOGL|Alphabet Inc.|Q|N|N|100|N|N
File Creation Time: 20250619
"""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str):
                return types.SimpleNamespace(
                    status_code=200, text=mock_file_content, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        # Test with default settings (skip_test_issues=True)
        provider = NasdaqDailyListProvider()
        records = await provider.fetch_symbols()

        # Verify TEST symbol was skipped
        assert len(records) == 2
        tickers = [r.ticker for r in records]
        assert "AAPL" in tickers
        assert "GOOGL" in tickers
        assert "TEST" not in tickers

    @pytest.mark.asyncio
    async def test_custom_as_of_overrides_footer(self, monkeypatch):
        """Test that custom as_of date overrides footer date."""
        mock_file_content = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc.|Q|N|N|100|N|N
File Creation Time: 20250619
"""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str):
                return types.SimpleNamespace(
                    status_code=200, text=mock_file_content, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        # Test with custom as_of date
        custom_date = datetime.date(2025, 1, 1)
        provider = NasdaqDailyListProvider(as_of=custom_date)
        records = await provider.fetch_symbols()

        # Verify custom date was used
        assert len(records) == 1
        assert records[0].as_of == custom_date

    @pytest.mark.asyncio
    async def test_blank_market_category_defaults_to_xnas(self, monkeypatch):
        """Test that blank market category defaults to XNAS."""
        mock_file_content = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
BLANK|Test Company||N|N|100|N|N
File Creation Time: 20250619
"""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str):
                return types.SimpleNamespace(
                    status_code=200, text=mock_file_content, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = NasdaqDailyListProvider()
        records = await provider.fetch_symbols()

        assert len(records) == 1
        assert records[0].exchange_mic == "XNAS"  # Should default to XNAS

    @pytest.mark.asyncio
    async def test_footer_with_extra_spaces(self, monkeypatch):
        """Test footer parsing with extra spaces after colon."""
        mock_file_content = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc.|Q|N|N|100|N|N
File Creation Time:  20250619
"""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str):
                return types.SimpleNamespace(
                    status_code=200, text=mock_file_content, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = NasdaqDailyListProvider()
        records = await provider.fetch_symbols()

        assert len(records) == 1
        assert records[0].as_of == datetime.date.today()  # Defaults to today when no as_of provided

    @pytest.mark.asyncio
    async def test_empty_ticker_filtered_out(self, monkeypatch):
        """Test that rows with empty tickers are filtered out."""
        mock_file_content = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc.|Q|N|N|100|N|N
|Empty Ticker Company|Q|N|N|100|N|N
GOOGL|Alphabet Inc.|Q|N|N|100|N|N
File Creation Time: 20250619
"""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str):
                return types.SimpleNamespace(
                    status_code=200, text=mock_file_content, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = NasdaqDailyListProvider()
        records = await provider.fetch_symbols()

        # Should have 2 records, empty ticker should be filtered out
        assert len(records) == 2
        tickers = [r.ticker for r in records]
        assert "AAPL" in tickers
        assert "GOOGL" in tickers

    @pytest.mark.asyncio
    async def test_etf_exclusion_configuration(self, monkeypatch):
        """Test excluding ETFs with include_etfs=False."""
        mock_file_content = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc.|Q|N|N|100|N|N
SPY|SPDR S&P 500 ETF|P|N|N|100|Y|N
File Creation Time: 20250619
"""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str):
                return types.SimpleNamespace(
                    status_code=200, text=mock_file_content, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = NasdaqDailyListProvider(include_etfs=False)
        records = await provider.fetch_symbols()

        # Should only have AAPL, SPY should be filtered out
        assert len(records) == 1
        assert records[0].ticker == "AAPL"
        assert records[0].asset_class == AssetClass.EQUITY


class TestNasdaqDailyListProviderErrorHandling:
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

            async def get(self, url: str):
                def raise_for_status():
                    raise httpx.HTTPStatusError(
                        "404 Not Found",
                        request=None,
                        response=types.SimpleNamespace(status_code=404),
                    )

                return types.SimpleNamespace(status_code=404, raise_for_status=raise_for_status)

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = NasdaqDailyListProvider()

        with pytest.raises(httpx.HTTPStatusError):
            await provider.fetch_symbols()

    @pytest.mark.asyncio
    async def test_empty_file_returns_empty_list(self, monkeypatch):
        """Test handling of empty file content."""
        mock_file_content = ""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str):
                return types.SimpleNamespace(
                    status_code=200, text=mock_file_content, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = NasdaqDailyListProvider()
        records = await provider.fetch_symbols()

        assert len(records) == 0

    @pytest.mark.asyncio
    async def test_malformed_rows_are_skipped(self, monkeypatch):
        """Test that malformed rows are skipped gracefully."""
        mock_file_content = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc.|Q|N|N|100|N|N
MALFORMED|Too Few Columns|Q
|Empty Symbol||Q|N|N|100|N|N
GOOGL|Alphabet Inc.|Q|N|N|100|N|N
File Creation Time: 20250619
"""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str):
                return types.SimpleNamespace(
                    status_code=200, text=mock_file_content, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = NasdaqDailyListProvider()
        records = await provider.fetch_symbols()

        # Should only get valid records
        assert len(records) == 2
        tickers = [r.ticker for r in records]
        assert "AAPL" in tickers
        assert "GOOGL" in tickers

    @pytest.mark.asyncio
    async def test_footer_parsing_fallback(self, monkeypatch):
        """Test fallback to today's date when footer parsing fails."""
        mock_file_content = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc.|Q|N|N|100|N|N
Invalid Footer Line
"""

        class MockAsyncClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def get(self, url: str):
                return types.SimpleNamespace(
                    status_code=200, text=mock_file_content, raise_for_status=lambda: None
                )

        monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

        provider = NasdaqDailyListProvider()
        records = await provider.fetch_symbols()

        # Should fallback to today's date
        assert len(records) == 1
        assert records[0].as_of == datetime.date.today()


class TestNasdaqDailyListProviderConfiguration:
    """Test provider configuration and attributes."""

    def test_provider_name_attribute(self):
        """Test that provider has correct name attribute."""
        assert NasdaqDailyListProvider.name == "nasdaq_dl"

    def test_default_configuration(self):
        """Test default configuration values."""
        provider = NasdaqDailyListProvider()

        # Default configuration
        assert provider.cfg.get("include_etfs", True) is True
        assert provider.cfg.get("skip_test_issues", True) is True

    def test_as_of_date_handling(self):
        """Test as_of date assignment."""
        # Default to today
        provider = NasdaqDailyListProvider()
        assert provider.as_of == datetime.date.today()

        # Custom date
        custom_date = datetime.date(2024, 1, 15)
        provider = NasdaqDailyListProvider(as_of=custom_date)
        assert provider.as_of == custom_date

    def test_configuration_preservation(self):
        """Test that all configuration is preserved in cfg dict."""
        config = {"include_etfs": False, "skip_test_issues": False, "custom_param": "test_value"}

        provider = NasdaqDailyListProvider(**config)

        assert provider.cfg["include_etfs"] is False
        assert provider.cfg["skip_test_issues"] is False
        assert provider.cfg["custom_param"] == "test_value"
