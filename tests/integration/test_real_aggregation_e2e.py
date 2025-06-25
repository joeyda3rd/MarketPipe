# SPDX-License-Identifier: Apache-2.0
"""End-to-end integration tests for real aggregation pipeline without mocking.

This test validates the complete aggregation flow using real DuckDB engine,
real ParquetStorageEngine, and actual data processing - no mocking of core services.
"""

from __future__ import annotations

import pandas as pd
import pytest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from marketpipe.aggregation.application.services import AggregationRunnerService
from marketpipe.aggregation.domain.services import AggregationDomainService
from marketpipe.aggregation.domain.value_objects import DEFAULT_SPECS, FrameSpec
from marketpipe.aggregation.infrastructure.duckdb_engine import DuckDBAggregationEngine
from marketpipe.domain.events import IngestionJobCompleted
from marketpipe.domain.value_objects import Symbol
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine


def generate_realistic_minute_bars(
    symbol: str, trading_day: date, count: int = 390, base_price: float = 150.0
) -> pd.DataFrame:
    """Generate realistic 1-minute OHLCV bars for a full trading day.
    
    Args:
        symbol: Stock symbol (e.g., "AAPL")
        trading_day: Trading date
        count: Number of minute bars (390 = full trading day)
        base_price: Starting price for realistic movements
        
    Returns:
        DataFrame with realistic OHLCV data
    """
    # Start at market open (9:30 AM ET)
    market_open = datetime.combine(trading_day, datetime.min.time()) + timedelta(hours=13, minutes=30)
    market_open = market_open.replace(tzinfo=timezone.utc)
    
    bars = []
    current_price = base_price
    
    for i in range(count):
        # Market realistic price movement (random walk with slight upward bias)
        import random
        price_change = random.gauss(0.001, 0.002)  # Small random walk
        current_price *= (1 + price_change)
        
        # Generate OHLC around current price
        open_price = current_price
        close_price = current_price * (1 + random.gauss(0, 0.001))  # Small close movement
        
        # High/Low should bracket open/close appropriately
        high_price = max(open_price, close_price) * (1 + abs(random.gauss(0, 0.0005)))
        low_price = min(open_price, close_price) * (1 - abs(random.gauss(0, 0.0005)))
        
        # Volume with realistic patterns (higher at open/close)
        volume_base = 1000
        if i < 30 or i > 360:  # Higher volume at open/close
            volume_base *= 2
        volume = int(volume_base * (1 + random.gauss(0, 0.3)))
        volume = max(volume, 100)  # Minimum volume
        
        timestamp = market_open + timedelta(minutes=i)
        timestamp_ns = int(timestamp.timestamp() * 1_000_000_000)
        
        bars.append({
            "ts_ns": timestamp_ns,
            "symbol": symbol,
            "open": round(open_price, 2),
            "high": round(high_price, 2), 
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": volume,
            "trade_count": random.randint(50, 200),
            "vwap": round((high_price + low_price + close_price) / 3, 2),
        })
        
        current_price = close_price  # Price continues from close
    
    return pd.DataFrame(bars)


@pytest.mark.integration
class TestRealAggregationEndToEnd:
    """Test complete aggregation pipeline using real services."""
    
    def test_complete_aggregation_pipeline_without_mocking(self, tmp_path, monkeypatch):
        """Test end-to-end aggregation using real DuckDB engine and storage."""
        # Setup directory structure
        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)
        
        # Create realistic test data for multiple symbols and days
        symbols = ["AAPL", "GOOGL", "MSFT"]
        date_range = [
            date(2024, 1, 15),  # Monday
            date(2024, 1, 16),  # Tuesday  
            date(2024, 1, 17),  # Wednesday
        ]
        job_id = "real-agg-test-job"
        
        # Use real ParquetStorageEngine - no mocking
        raw_engine = ParquetStorageEngine(raw_dir)
        
        # Populate with realistic 1-minute bars
        total_bars = 0
        for symbol in symbols:
            for trading_day in date_range:
                bars_1m = generate_realistic_minute_bars(symbol, trading_day, count=390)
                raw_engine.write(
                    df=bars_1m,
                    frame="1m", 
                    symbol=symbol,
                    trading_day=trading_day,
                    job_id=job_id,
                    overwrite=True
                )
                total_bars += len(bars_1m)
        
        print(f"✓ Created {total_bars} raw 1-minute bars across {len(symbols)} symbols and {len(date_range)} days")
        
        # Verify raw data was written correctly
        job_data = raw_engine.load_job_bars(job_id)
        assert len(job_data) == len(symbols), f"Expected {len(symbols)} symbols, got {len(job_data)}"
        
        for symbol in symbols:
            assert symbol in job_data, f"Symbol {symbol} not found in job data"
            symbol_df = job_data[symbol]
            expected_bars = 390 * len(date_range)  # 390 bars per day
            assert len(symbol_df) == expected_bars, f"Expected {expected_bars} bars for {symbol}, got {len(symbol_df)}"
        
        # Initialize real DuckDB aggregation engine
        duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
        domain_service = AggregationDomainService()
        
        # Create real aggregation service (no mocking)
        aggregation_service = AggregationRunnerService(
            engine=duckdb_engine,
            domain=domain_service
        )
        
        # Create realistic ingestion completed event
        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol(symbols[0]),  # Primary symbol
            trading_date=date_range[0],  # Primary date
            bars_processed=total_bars,
            success=True
        )
        
        # Execute real aggregation (this is the core test - no mocking!)
        print("🔄 Running real aggregation pipeline...")
        aggregation_service.handle_ingestion_completed(event)
        print("✅ Aggregation pipeline completed")
        
        # Verify aggregated files exist for all timeframes
        agg_engine = ParquetStorageEngine(agg_dir)
        
        for symbol in symbols:
            for trading_day in date_range:
                for spec in DEFAULT_SPECS:
                    # Check that aggregated data exists
                    symbol_agg_data = agg_engine.load_symbol_data(symbol=symbol, frame=spec.name)
                    assert not symbol_agg_data.empty, f"No {spec.name} data found for {symbol} on {trading_day}"
                    
                    print(f"✓ Found {len(symbol_agg_data)} {spec.name} bars for {symbol}")
        
        # Validate aggregated data quality with mathematical verification
        print("🔍 Validating aggregation mathematics...")
        
        # Test specific aggregation: AAPL on first day, 5-minute bars
        aapl_1m = job_data["AAPL"]
        aapl_1m_day1 = aapl_1m[
            aapl_1m["ts_ns"] >= int(datetime.combine(date_range[0], datetime.min.time()).replace(tzinfo=timezone.utc).timestamp() * 1e9)
        ].head(390)  # First day only
        
        aapl_5m = agg_engine.load_symbol_data(symbol="AAPL", frame="5m")
        aapl_5m_day1 = aapl_5m[
            aapl_5m["ts_ns"] >= int(datetime.combine(date_range[0], datetime.min.time()).replace(tzinfo=timezone.utc).timestamp() * 1e9)
        ].head(78)  # 390 minutes / 5 = 78 bars
        
        assert len(aapl_5m_day1) == 78, f"Expected 78 5-minute bars, got {len(aapl_5m_day1)}"
        
        # Verify first 5-minute bar aggregation
        # Get first 5 1-minute bars (should aggregate to first 5-minute bar)
        first_5_mins = aapl_1m_day1.head(5)
        first_5m_bar = aapl_5m_day1.iloc[0]
        
        # Mathematical verification of OHLCV aggregation
        expected_open = first_5_mins.iloc[0]["open"]
        expected_close = first_5_mins.iloc[4]["close"] 
        expected_high = first_5_mins["high"].max()
        expected_low = first_5_mins["low"].min()
        expected_volume = first_5_mins["volume"].sum()
        
        # Allow small floating point differences
        assert abs(first_5m_bar["open"] - expected_open) < 0.01, f"Open mismatch: {first_5m_bar['open']} vs {expected_open}"
        assert abs(first_5m_bar["close"] - expected_close) < 0.01, f"Close mismatch: {first_5m_bar['close']} vs {expected_close}"
        assert abs(first_5m_bar["high"] - expected_high) < 0.01, f"High mismatch: {first_5m_bar['high']} vs {expected_high}"
        assert abs(first_5m_bar["low"] - expected_low) < 0.01, f"Low mismatch: {first_5m_bar['low']} vs {expected_low}"
        assert first_5m_bar["volume"] == expected_volume, f"Volume mismatch: {first_5m_bar['volume']} vs {expected_volume}"
        
        print("✅ OHLCV aggregation mathematics verified")
        
        # Verify timestamp alignment for all timeframes
        print("🔍 Validating timestamp alignment...")
        for spec in DEFAULT_SPECS:
            symbol_data = agg_engine.load_symbol_data(symbol="AAPL", frame=spec.name)
            if not symbol_data.empty:
                # Check that timestamps are aligned to frame boundaries
                first_ts = symbol_data.iloc[0]["ts_ns"]
                first_datetime = pd.Timestamp(first_ts, unit="ns")
                
                if spec.name == "5m":
                    # 5-minute bars should align to 5-minute boundaries
                    assert first_datetime.minute % 5 == 0, f"5m timestamp not aligned: {first_datetime}"
                elif spec.name == "15m":
                    # 15-minute bars should align to 15-minute boundaries  
                    assert first_datetime.minute % 15 == 0, f"15m timestamp not aligned: {first_datetime}"
                elif spec.name == "1h":
                    # 1-hour bars should align to hour boundaries
                    assert first_datetime.minute == 0, f"1h timestamp not aligned: {first_datetime}"
                elif spec.name == "1d":
                    # 1-day bars should align to day boundaries (market open)
                    assert first_datetime.hour == 13 and first_datetime.minute == 30, f"1d timestamp not aligned: {first_datetime}"
                
                print(f"✓ {spec.name} timestamps properly aligned")
        
        print("✅ All timestamp alignments verified")
        
        # Test aggregation of aggregated data (15m from 5m, etc.)
        print("🔍 Testing multi-level aggregation...")
        
        # Verify that 15-minute bars are consistent with 5-minute aggregation
        aapl_15m = agg_engine.load_symbol_data(symbol="AAPL", frame="15m")
        if not aapl_15m.empty:
            # First 15-minute bar should aggregate first 3 5-minute bars
            first_3_5m = aapl_5m_day1.head(3)
            first_15m_bar = aapl_15m.iloc[0]
            
            # Volume should be sum of 3 5-minute bars
            expected_15m_volume = first_3_5m["volume"].sum()
            assert first_15m_bar["volume"] == expected_15m_volume, \
                f"15m volume aggregation error: {first_15m_bar['volume']} vs {expected_15m_volume}"
            
            print("✅ Multi-level aggregation verified")
        
        print("🎉 Real aggregation E2E test completed successfully!")
        print(f"✅ Processed {total_bars} bars across {len(symbols)} symbols")
        print(f"✅ Generated all {len(DEFAULT_SPECS)} timeframes: {[spec.name for spec in DEFAULT_SPECS]}")
        print(f"✅ Mathematical aggregation accuracy verified")
        print(f"✅ Timestamp alignment verified for all timeframes")
    
    def test_aggregation_error_handling_and_recovery(self, tmp_path):
        """Test aggregation pipeline error handling with partial data."""
        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)
        
        job_id = "error-test-job"
        
        # Create mixed data: good data for AAPL, no data for GOOGL
        raw_engine = ParquetStorageEngine(raw_dir)
        
        # Only create data for AAPL
        aapl_bars = generate_realistic_minute_bars("AAPL", date(2024, 1, 15), count=100)
        raw_engine.write(
            df=aapl_bars,
            frame="1m",
            symbol="AAPL", 
            trading_day=date(2024, 1, 15),
            job_id=job_id
        )
        
        # Initialize aggregation components
        duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
        domain_service = AggregationDomainService()
        aggregation_service = AggregationRunnerService(engine=duckdb_engine, domain=domain_service)
        
        # Test that aggregation succeeds even with partial data
        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol("AAPL"),
            trading_date=date(2024, 1, 15),
            bars_processed=100,
            success=True
        )
        
        # Should not raise exception even with only partial symbol data
        aggregation_service.handle_ingestion_completed(event)
        
        # Verify AAPL data was aggregated
        agg_engine = ParquetStorageEngine(agg_dir)
        aapl_5m = agg_engine.load_symbol_data(symbol="AAPL", frame="5m")
        assert not aapl_5m.empty, "AAPL 5m aggregation should succeed"
        
        print("✅ Error handling test passed: partial data processed successfully")
    
    def test_empty_job_aggregation(self, tmp_path):
        """Test aggregation behavior with completely empty job."""
        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)
        
        # Initialize aggregation components
        duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
        domain_service = AggregationDomainService()
        aggregation_service = AggregationRunnerService(engine=duckdb_engine, domain=domain_service)
        
        # Test aggregation with non-existent job
        event = IngestionJobCompleted(
            job_id="nonexistent-job",
            symbol=Symbol("AAPL"),
            trading_date=date(2024, 1, 15),
            bars_processed=0,
            success=True
        )
        
        # Should not raise exception for empty job
        aggregation_service.handle_ingestion_completed(event)
        
        print("✅ Empty job handling test passed")