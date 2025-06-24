#!/usr/bin/env python3
"""Fix script for DuckDB aggregation timestamp conversion issues."""

import duckdb


def fix_aggregation():
    """Fix timestamp conversion and create working aggregated views."""
    conn = duckdb.connect()

    print("🔧 Fixing DuckDB aggregation with proper timestamp conversion...")

    # First, check the schema
    try:
        result = conn.execute("DESCRIBE SELECT * FROM read_parquet('data/raw/**/*.parquet') LIMIT 1").fetchall()
        print(f"📋 Schema: {result}")
    except Exception as e:
        print(f"❌ Failed to describe schema: {e}")
        return

    # Test timestamp conversion approaches
    print("\n🔧 Testing timestamp conversion...")
    try:
        # Method 1: Convert nanoseconds to timestamp
        result = conn.execute("""
        SELECT 
            symbol,
            ts_ns,
            to_timestamp(ts_ns / 1000000000) as ts_converted,
            open, high, low, close, volume
        FROM read_parquet('data/raw/**/*.parquet') 
        WHERE symbol = 'AAPL' 
        LIMIT 3
        """).fetchall()
        print(f"✅ Timestamp conversion test: {result}")
    except Exception as e:
        print(f"❌ Timestamp conversion failed: {e}")
        return

    # Create 5-minute aggregation with proper timestamp conversion
    print("\n🔧 Creating 5-minute bars view with fixed timestamps...")
    try:
        conn.execute("""
        CREATE OR REPLACE VIEW bars_5m AS
        SELECT 
            symbol,
            time_bucket(INTERVAL '5 minutes', to_timestamp(ts_ns / 1000000000)) as ts_bucket,
            ts_ns / 1000000000 as ts_seconds,
            first(open ORDER BY ts_ns) as open,
            max(high) as high,
            min(low) as low,
            last(close ORDER BY ts_ns) as close,
            sum(volume) as volume,
            count(*) as bar_count
        FROM read_parquet('data/raw/**/*.parquet') 
        WHERE symbol IS NOT NULL
        GROUP BY symbol, time_bucket(INTERVAL '5 minutes', to_timestamp(ts_ns / 1000000000))
        ORDER BY symbol, ts_bucket
        """)
        print("✅ 5-minute view created successfully")

        # Test the view
        result = conn.execute("SELECT symbol, COUNT(*) as count FROM bars_5m GROUP BY symbol LIMIT 5").fetchall()
        print(f"📊 5-minute aggregated bars: {result}")

        # Show sample data
        sample = conn.execute("SELECT * FROM bars_5m WHERE symbol = 'AAPL' LIMIT 3").fetchall()
        print(f"📄 Sample 5-minute data: {sample}")

    except Exception as e:
        print(f"❌ 5-minute view failed: {e}")

    # Create daily aggregation
    print("\n🔧 Creating daily bars view...")
    try:
        conn.execute("""
        CREATE OR REPLACE VIEW bars_1d AS
        SELECT 
            symbol,
            date_trunc('day', to_timestamp(ts_ns / 1000000000)) as date_bucket,
            first(open ORDER BY ts_ns) as open,
            max(high) as high,
            min(low) as low,
            last(close ORDER BY ts_ns) as close,
            sum(volume) as volume,
            count(*) as bar_count
        FROM read_parquet('data/raw/**/*.parquet') 
        WHERE symbol IS NOT NULL
        GROUP BY symbol, date_trunc('day', to_timestamp(ts_ns / 1000000000))
        ORDER BY symbol, date_bucket
        """)
        print("✅ Daily view created successfully")

        # Test the view
        result = conn.execute("SELECT symbol, COUNT(*) as count FROM bars_1d GROUP BY symbol LIMIT 5").fetchall()
        print(f"📊 Daily aggregated bars: {result}")

        # Show sample data
        sample = conn.execute("SELECT * FROM bars_1d WHERE symbol = 'AAPL' LIMIT 3").fetchall()
        print(f"📄 Sample daily data: {sample}")

    except Exception as e:
        print(f"❌ Daily view failed: {e}")

    # Test queries that should now work
    print("\n🧪 Testing queries that were failing...")

    test_queries = [
        ("SELECT symbol, COUNT(*) as bar_count FROM bars_5m WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 10",
         "5-minute bar counts"),
        ("SELECT symbol, COUNT(*) as bar_count FROM bars_1d WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 10",
         "Daily bar counts"),
        ("SELECT symbol, AVG(close) as avg_close FROM bars_1d WHERE symbol IN ('AAPL', 'MSFT') GROUP BY symbol",
         "Average closing prices"),
    ]

    for query, description in test_queries:
        try:
            result = conn.execute(query).fetchall()
            print(f"✅ {description}: {result}")
        except Exception as e:
            print(f"❌ {description} failed: {e}")

    print("\n🎉 Aggregation fix completed!")

if __name__ == "__main__":
    fix_aggregation()
