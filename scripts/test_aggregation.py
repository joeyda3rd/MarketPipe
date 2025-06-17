#!/usr/bin/env python3
"""Test script to manually create and test DuckDB aggregation views."""

import duckdb

def test_aggregation():
    """Test manual creation of aggregated views."""
    conn = duckdb.connect()
    
    print("🔍 Testing DuckDB aggregation views...")
    
    # First, check what data we have
    try:
        result = conn.execute("SELECT symbol, COUNT(*) as count FROM read_parquet('data/raw/**/*.parquet') WHERE symbol IS NOT NULL GROUP BY symbol").fetchall()
        print(f"📊 Raw data available: {result}")
    except Exception as e:
        print(f"❌ Failed to read raw data: {e}")
        return
    
    # Test 5-minute aggregation
    print("\n🔧 Creating 5-minute bars view...")
    try:
        conn.execute("""
        CREATE OR REPLACE VIEW bars_5m AS
        SELECT 
            symbol,
            time_bucket(INTERVAL '5 minutes', ts_ns::timestamp) as ts_ns,
            first(open ORDER BY ts_ns) as open,
            max(high) as high,
            min(low) as low,
            last(close ORDER BY ts_ns) as close,
            sum(volume) as volume
        FROM read_parquet('data/raw/**/*.parquet') 
        WHERE symbol IS NOT NULL
        GROUP BY symbol, time_bucket(INTERVAL '5 minutes', ts_ns::timestamp)
        ORDER BY symbol, ts_ns
        """)
        print("✅ 5-minute view created successfully")
        
        # Test the view
        result = conn.execute("SELECT symbol, COUNT(*) as count FROM bars_5m GROUP BY symbol LIMIT 5").fetchall()
        print(f"📊 5-minute aggregated bars: {result}")
        
    except Exception as e:
        print(f"❌ 5-minute view failed: {e}")
    
    # Test daily aggregation
    print("\n🔧 Creating daily bars view...")
    try:
        conn.execute("""
        CREATE OR REPLACE VIEW bars_1d AS
        SELECT 
            symbol,
            date_trunc('day', ts_ns::timestamp) as ts_ns,
            first(open ORDER BY ts_ns) as open,
            max(high) as high,
            min(low) as low,
            last(close ORDER BY ts_ns) as close,
            sum(volume) as volume
        FROM read_parquet('data/raw/**/*.parquet') 
        WHERE symbol IS NOT NULL
        GROUP BY symbol, date_trunc('day', ts_ns::timestamp)
        ORDER BY symbol, ts_ns
        """)
        print("✅ Daily view created successfully")
        
        # Test the view
        result = conn.execute("SELECT symbol, COUNT(*) as count FROM bars_1d GROUP BY symbol LIMIT 5").fetchall()
        print(f"📊 Daily aggregated bars: {result}")
        
    except Exception as e:
        print(f"❌ Daily view failed: {e}")
    
    print("\n🎉 Aggregation test completed!")

if __name__ == "__main__":
    test_aggregation() 