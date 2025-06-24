#!/usr/bin/env python3
"""
Debug script to check aggregation pipeline
"""

import subprocess
from pathlib import Path


def run_command(cmd, description):
    """Run a command and return success/failure."""
    print(f"\n🔧 {description}")
    print(f"💻 {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print("✅ Success")
        if result.stdout.strip():
            print("📄 Output:")
            for line in result.stdout.strip().split('\n'):
                print(f"  {line}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed (exit code {e.returncode})")
        if e.stdout:
            print("📄 Stdout:")
            for line in e.stdout.strip().split('\n'):
                print(f"  {line}")
        if e.stderr:
            print("🚨 Stderr:")
            for line in e.stderr.strip().split('\n'):
                print(f"  {line}")
        return False

def main():
    """Debug the aggregation pipeline."""
    print("🔍 Debugging MarketPipe Aggregation Pipeline")
    print("=" * 50)

    # Check raw data structure
    raw_path = Path("data/raw")
    if raw_path.exists():
        parquet_files = list(raw_path.rglob("*.parquet"))
        print(f"📁 Found {len(parquet_files)} raw parquet files:")
        for f in parquet_files[:5]:  # Show first 5
            print(f"  {f}")
        if len(parquet_files) > 5:
            print(f"  ... and {len(parquet_files) - 5} more")
    else:
        print("❌ No raw data directory found")
        return

    # Check aggregated data structure
    agg_path = Path("data/agg")
    if agg_path.exists():
        agg_files = list(agg_path.rglob("*.parquet"))
        print(f"📁 Found {len(agg_files)} aggregated parquet files:")
        for f in agg_files[:5]:
            print(f"  {f}")
    else:
        print("❌ No aggregated data directory found")

    # Try to manually trigger aggregation
    print("\n🔄 Manually triggering aggregation...")

    # First, let's see if we can query raw data directly
    print("\n1️⃣ Testing direct raw data query...")
    run_command([
        "python", "-c",
        "import duckdb; con = duckdb.connect(':memory:'); "
        "result = con.execute(\"SELECT COUNT(*) as total FROM parquet_scan('data/raw/**/*.parquet', hive_partitioning=1)\").fetchone(); "
        "print(f'Raw data rows: {result[0]}')"
    ], "Count rows in raw data")

    # Try to test the aggregation engine directly
    print("\n2️⃣ Testing DuckDB aggregation...")
    run_command([
        "python", "-c",
        "from marketpipe.aggregation.infrastructure.duckdb_engine import DuckDBAggregationEngine; "
        "engine = DuckDBAggregationEngine('data/raw', 'data/agg'); "
        "result = engine.aggregate_raw_data('5m'); "
        "print(f'Aggregation result: {result}')"
    ], "Test DuckDB aggregation engine")

    # Check if views can be created
    print("\n3️⃣ Testing view creation...")
    run_command([
        "python", "-c",
        "from marketpipe.aggregation.infrastructure.duckdb_views import ensure_views, get_available_data; "
        "ensure_views(); "
        "data = get_available_data(); "
        "print(f'Available data: {len(data)} records')"
    ], "Test view creation and data availability")

    # Try a simple query
    print("\n4️⃣ Testing simple query...")
    run_command([
        "python", "-m", "marketpipe", "query",
        "SELECT 'test' as message, COUNT(*) as count FROM bars_5m"
    ], "Test simple query")

if __name__ == "__main__":
    main()
