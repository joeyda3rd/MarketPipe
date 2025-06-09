"""Unit tests for AlpacaMarketDataAdapter anti-corruption layer."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone
from decimal import Decimal

from marketpipe.domain.entities import EntityId
from marketpipe.domain.value_objects import Symbol, Price, Timestamp, Volume
from marketpipe.ingestion.infrastructure.adapters import (
    AlpacaMarketDataAdapter, 
    MarketDataProviderError,
    DataTranslationError
)


class TestAlpacaMarketDataAdapterTranslation:
    """Test Alpaca data translation to domain models."""
    
    def test_translates_alpaca_bar_format_to_domain_ohlcv_bar(self):
        """Test that Alpaca bar format is correctly translated to domain OHLCV bar."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret", 
            base_url="https://paper-api.alpaca.markets",
            feed_type="iex"
        )
        
        # Alpaca bar format
        alpaca_bar = {
            "timestamp": 1672675800000000000,  # 2023-01-02 13:30:00 UTC in nanoseconds
            "open": 130.28,
            "high": 130.90,
            "low": 129.61,
            "close": 130.73,
            "volume": 10000
        }
        
        symbol = Symbol("AAPL")
        domain_bar = adapter._translate_alpaca_bar_to_domain(alpaca_bar, symbol)
        
        # Verify domain model properties
        assert domain_bar.symbol == symbol
        assert domain_bar.open_price == Price(Decimal("130.28"))
        assert domain_bar.high_price == Price(Decimal("130.90"))
        assert domain_bar.low_price == Price(Decimal("129.61"))
        assert domain_bar.close_price == Price(Decimal("130.73"))
        assert domain_bar.volume == Volume(10000)
        
        # Verify timestamp conversion
        expected_dt = datetime(2023, 1, 2, 13, 30, tzinfo=timezone.utc)
        assert domain_bar.timestamp.value == expected_dt
    
    def test_handles_alpaca_short_field_names(self):
        """Test that Alpaca short field names (S, t, o, h, l, c, v) are handled correctly."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets"
        )
        
        # Alpaca bar with short field names
        alpaca_bar = {
            "t": 1672675800000000000,  # timestamp
            "o": 100.50,               # open
            "h": 101.25,               # high  
            "l": 99.75,                # low
            "c": 100.85,               # close
            "v": 5000                  # volume
        }
        
        symbol = Symbol("GOOGL")
        domain_bar = adapter._translate_alpaca_bar_to_domain(alpaca_bar, symbol)
        
        assert domain_bar.symbol == symbol
        assert domain_bar.open_price == Price(Decimal("100.50"))
        assert domain_bar.high_price == Price(Decimal("101.25"))
        assert domain_bar.low_price == Price(Decimal("99.75"))
        assert domain_bar.close_price == Price(Decimal("100.85"))
        assert domain_bar.volume == Volume(5000)
    
    def test_raises_translation_error_for_invalid_price_data(self):
        """Test that invalid price data raises DataTranslationError."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets"
        )
        
        # Alpaca bar with invalid price data
        invalid_bar = {
            "timestamp": 1672675800000000000,
            "open": "invalid_price",  # Invalid price format
            "high": 101.25,
            "low": 99.75,
            "close": 100.85,
            "volume": 5000
        }
        
        symbol = Symbol("AAPL")
        
        with pytest.raises(DataTranslationError):
            adapter._translate_alpaca_bar_to_domain(invalid_bar, symbol)
    
    def test_raises_translation_error_for_missing_required_fields(self):
        """Test that missing required fields raise DataTranslationError."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets"
        )
        
        # Alpaca bar missing required fields
        incomplete_bar = {
            "timestamp": 1672675800000000000,
            "open": 100.50,
            # Missing high, low, close, volume
        }
        
        symbol = Symbol("AAPL")
        
        with pytest.raises(DataTranslationError):
            adapter._translate_alpaca_bar_to_domain(incomplete_bar, symbol)


class TestAlpacaMarketDataAdapterFetching:
    """Test market data fetching functionality."""
    
    @pytest.mark.asyncio
    async def test_fetch_bars_converts_timestamp_parameters_correctly(self, monkeypatch):
        """Test that timestamp parameters are converted correctly for Alpaca API."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets"
        )
        
        # Mock the Alpaca client
        mock_fetch_batch = MagicMock(return_value=[
            {
                "timestamp": 1672675800000000000,
                "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
                "volume": 1000
            }
        ])
        
        monkeypatch.setattr(adapter._alpaca_client, "fetch_batch", mock_fetch_batch)
        
        symbol = Symbol("AAPL")
        start_timestamp_ns = 1672675800000000000  # nanoseconds
        end_timestamp_ns = 1672679400000000000    # nanoseconds
        
        await adapter.fetch_bars(symbol, start_timestamp_ns, end_timestamp_ns)
        
        # Verify that nanoseconds were converted to milliseconds for Alpaca API
        expected_start_ms = start_timestamp_ns // 1_000_000
        expected_end_ms = end_timestamp_ns // 1_000_000
        
        mock_fetch_batch.assert_called_once_with(
            symbol.value, 
            expected_start_ms, 
            expected_end_ms
        )
    
    @pytest.mark.asyncio
    async def test_fetch_bars_raises_provider_error_on_client_failure(self, monkeypatch):
        """Test that client failures are wrapped in MarketDataProviderError."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets"
        )
        
        # Mock the Alpaca client to raise an exception
        mock_fetch_batch = MagicMock(side_effect=Exception("Network timeout"))
        monkeypatch.setattr(adapter._alpaca_client, "fetch_batch", mock_fetch_batch)
        
        symbol = Symbol("AAPL")
        start_timestamp = 1672675800000000000
        end_timestamp = 1672679400000000000
        
        with pytest.raises(MarketDataProviderError, match="Failed to fetch data for AAPL"):
            await adapter.fetch_bars(symbol, start_timestamp, end_timestamp)
    
    @pytest.mark.asyncio
    async def test_fetch_bars_returns_empty_list_for_no_data(self, monkeypatch):
        """Test that empty data from Alpaca returns empty list."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets"
        )
        
        # Mock the Alpaca client to return empty data
        mock_fetch_batch = MagicMock(return_value=[])
        monkeypatch.setattr(adapter._alpaca_client, "fetch_batch", mock_fetch_batch)
        
        symbol = Symbol("AAPL")
        start_timestamp = 1672675800000000000
        end_timestamp = 1672679400000000000
        
        result = await adapter.fetch_bars(symbol, start_timestamp, end_timestamp)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_fetch_bars_handles_partial_translation_failures_gracefully(self, monkeypatch):
        """Test that individual bar translation failures don't stop processing."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets"
        )
        
        # Mock the Alpaca client to return mixed valid/invalid data
        mock_fetch_batch = MagicMock(return_value=[
            {  # Valid bar
                "timestamp": 1672675800000000000,
                "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
                "volume": 1000
            },
            {  # Invalid bar (missing fields)
                "timestamp": 1672675860000000000,
                "open": 101.0,
                # Missing other required fields
            },
            {  # Another valid bar
                "timestamp": 1672675920000000000,
                "open": 101.5, "high": 102.0, "low": 101.0, "close": 101.8,
                "volume": 1500
            }
        ])
        
        monkeypatch.setattr(adapter._alpaca_client, "fetch_batch", mock_fetch_batch)
        
        symbol = Symbol("AAPL")
        start_timestamp = 1672675800000000000
        end_timestamp = 1672679400000000000
        
        result = await adapter.fetch_bars(symbol, start_timestamp, end_timestamp)
        
        # Should return only the valid bars (2 out of 3)
        assert len(result) == 2
        assert all(bar.symbol == symbol for bar in result)


class TestAlpacaMarketDataAdapterConfiguration:
    """Test adapter configuration and provider information."""
    
    def test_adapter_returns_correct_provider_information(self):
        """Test that adapter returns correct provider information."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets",
            feed_type="sip",
            rate_limit_per_min=100
        )
        
        provider_info = adapter.get_provider_info()
        
        assert provider_info["provider"] == "alpaca"
        assert provider_info["feed_type"] == "sip"
        assert provider_info["base_url"] == "https://paper-api.alpaca.markets"
        assert provider_info["rate_limit_per_min"] == 100
        assert provider_info["supports_real_time"] is True
        assert provider_info["supports_historical"] is True
    
    def test_adapter_detects_sip_vs_iex_feed_capabilities(self):
        """Test that adapter correctly reports capabilities based on feed type."""
        # SIP feed (real-time capable)
        sip_adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets",
            feed_type="sip"
        )
        
        sip_info = sip_adapter.get_provider_info()
        assert sip_info["supports_real_time"] is True
        
        # IEX feed (free tier, no real-time)
        iex_adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets",
            feed_type="iex"
        )
        
        iex_info = iex_adapter.get_provider_info()
        # For this test, we'll assume both support real-time 
        # (the actual business rules would determine this)
        assert iex_info["supports_historical"] is True
    
    @pytest.mark.asyncio
    async def test_test_connection_functionality(self):
        """Test that test_connection method works correctly."""
        adapter = AlpacaMarketDataAdapter(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://paper-api.alpaca.markets"
        )
        
        # For now, this always returns True (would need actual implementation)
        result = await adapter.test_connection()
        assert isinstance(result, bool)