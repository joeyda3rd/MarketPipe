#!/usr/bin/env python3
"""
Test script to verify Polygon and Finnhub provider implementations.
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone

# Add src to path for imports
import sys
sys.path.insert(0, 'src')

from marketpipe.ingestion.infrastructure.provider_registry import list_providers
from marketpipe.ingestion.infrastructure.polygon_adapter import PolygonMarketDataAdapter
from marketpipe.ingestion.infrastructure.finnhub_adapter import FinnhubMarketDataAdapter
from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp


async def test_providers():
    """Test provider registration and basic functionality."""
    
    print("=== Testing Provider Registration ===")
    providers = list_providers()
    print(f"Available providers: {providers}")
    
    # Check if our new providers are registered
    expected_providers = {"alpaca", "iex", "fake", "polygon", "finnhub"}
    missing_providers = expected_providers - set(providers)
    
    if missing_providers:
        print(f"❌ Missing providers: {missing_providers}")
    else:
        print("✅ All expected providers are registered!")
    
    print("\n=== Testing Provider Metadata ===")
    
    # Test Polygon metadata (if available)
    if "polygon" in providers:
        try:
            polygon_adapter = PolygonMarketDataAdapter(api_key="test_key")
            metadata = polygon_adapter.get_provider_metadata()
            print(f"Polygon metadata: {metadata}")
            print(f"✅ Polygon adapter created successfully")
        except Exception as e:
            print(f"❌ Polygon adapter error: {e}")
    
    # Test Finnhub metadata (if available)
    if "finnhub" in providers:
        try:
            finnhub_adapter = FinnhubMarketDataAdapter(api_key="test_key")
            metadata = finnhub_adapter.get_provider_metadata()
            print(f"Finnhub metadata: {metadata}")
            print(f"✅ Finnhub adapter created successfully")
        except Exception as e:
            print(f"❌ Finnhub adapter error: {e}")
    
    print("\n=== Testing Connection Validation ===")
    
    # Test connection validation with environment variables if available
    polygon_token = os.getenv("POLYGON_TOKEN")
    finnhub_token = os.getenv("FINNHUB_API_KEY")
    
    if polygon_token and "polygon" in providers:
        try:
            polygon_adapter = PolygonMarketDataAdapter(api_key=polygon_token)
            is_available = await polygon_adapter.is_available()
            print(f"Polygon connection test: {'✅ Connected' if is_available else '❌ Failed'}")
        except Exception as e:
            print(f"❌ Polygon connection error: {e}")
    else:
        print("⚠️  Polygon token not found in environment, skipping connection test")
    
    if finnhub_token and "finnhub" in providers:
        try:
            finnhub_adapter = FinnhubMarketDataAdapter(api_key=finnhub_token)
            is_available = await finnhub_adapter.is_available()
            print(f"Finnhub connection test: {'✅ Connected' if is_available else '❌ Failed'}")
        except Exception as e:
            print(f"❌ Finnhub connection error: {e}")
    else:
        print("⚠️  Finnhub token not found in environment, skipping connection test")
    
    print("\n=== Testing Data Fetching (if tokens available) ===")
    
    # Test data fetching with a small request
    symbol = Symbol.from_string("AAPL")
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=7)  # Last week
    time_range = TimeRange(
        start=Timestamp(start_date), 
        end=Timestamp(end_date)
    )
    
    if polygon_token and "polygon" in providers:
        try:
            polygon_adapter = PolygonMarketDataAdapter(api_key=polygon_token, rate_limit_per_minute=5)
            bars = await polygon_adapter.fetch_bars_for_symbol(symbol, time_range, max_bars=5)
            print(f"Polygon data fetch: ✅ Got {len(bars)} bars for {symbol.value}")
            if bars:
                print(f"  Sample bar: {bars[0]}")
        except Exception as e:
            print(f"❌ Polygon data fetch error: {e}")
    
    if finnhub_token and "finnhub" in providers:
        try:
            finnhub_adapter = FinnhubMarketDataAdapter(api_key=finnhub_token, rate_limit_per_minute=60)
            bars = await finnhub_adapter.fetch_bars_for_symbol(symbol, time_range, max_bars=5)
            print(f"Finnhub data fetch: ✅ Got {len(bars)} bars for {symbol.value}")
            if bars:
                print(f"  Sample bar: {bars[0]}")
        except Exception as e:
            print(f"❌ Finnhub data fetch error: {e}")
    
    print("\n=== Test Complete ===")


if __name__ == "__main__":
    asyncio.run(test_providers()) 