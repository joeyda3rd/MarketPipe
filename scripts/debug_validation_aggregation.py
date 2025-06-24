#!/usr/bin/env python3
"""
Debug Validation and Aggregation Data Access Issues

This script investigates why validation and aggregation can't find ingested data
even though the data exists and can be queried directly.
"""

import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path


def log_and_print(message: str):
    """Print and log message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def run_command(cmd: list, description: str) -> tuple[bool, str, str]:
    """Run command and return success, stdout, stderr."""
    log_and_print(f"🔧 {description}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def debug_job_database():
    """Debug the SQLite job database to see what's stored."""
    log_and_print("\n🔍 Debugging Job Database")
    log_and_print("-" * 50)

    db_path = "ingestion_jobs.db"
    if not Path(db_path).exists():
        log_and_print(f"❌ Database not found: {db_path}")
        return False

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        log_and_print(f"📊 Tables in database: {[t[0] for t in tables]}")

        # Check ingestion_jobs table
        if any('ingestion_job' in str(t).lower() for t in tables):
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%job%'")
            job_tables = cursor.fetchall()

            for table in job_tables:
                table_name = table[0]
                log_and_print(f"\n📋 Table: {table_name}")

                # Get schema
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                log_and_print(f"  📄 Columns: {[col[1] for col in columns]}")

                # Get data
                cursor.execute(f"SELECT * FROM {table_name} ORDER BY created_at DESC LIMIT 5")
                rows = cursor.fetchall()
                log_and_print(f"  📊 Recent rows: {len(rows)}")

                for i, row in enumerate(rows):
                    log_and_print(f"    Row {i+1}: {row}")

        conn.close()
        return True

    except Exception as e:
        log_and_print(f"❌ Database error: {e}")
        return False

def debug_validation_data_discovery():
    """Debug how validation tries to find data."""
    log_and_print("\n🔍 Debugging Validation Data Discovery")
    log_and_print("-" * 50)

    # Check what validation is looking for
    log_and_print("📋 Testing validation list command with verbose output:")

    success, stdout, stderr = run_command(
        ["python", "-m", "marketpipe", "validate-ohlcv", "--list"],
        "Validation list (verbose)"
    )

    log_and_print(f"📄 Stdout: {stdout}")
    log_and_print(f"🚨 Stderr: {stderr}")

    # Try to understand the job lookup
    log_and_print("\n📋 Checking if validation can find our specific job:")

    success, stdout, stderr = run_command(
        ["python", "-m", "marketpipe", "validate-ohlcv", "--job-id", "AAPL_2025-05-18", "--verbose"],
        "Validation with verbose flag"
    )

    log_and_print(f"📄 Stdout: {stdout}")
    log_and_print(f"🚨 Stderr: {stderr}")

    return success

def debug_aggregation_data_discovery():
    """Debug how aggregation tries to find data."""
    log_and_print("\n🔍 Debugging Aggregation Data Discovery")
    log_and_print("-" * 50)

    # Test aggregation with verbose output
    success, stdout, stderr = run_command(
        ["python", "-m", "marketpipe", "aggregate-ohlcv", "AAPL_2025-05-18", "--verbose"],
        "Aggregation with verbose flag"
    )

    log_and_print(f"📄 Stdout: {stdout}")
    log_and_print(f"🚨 Stderr: {stderr}")

    return success

def debug_data_file_structure():
    """Debug the actual data file structure and paths."""
    log_and_print("\n🔍 Debugging Data File Structure")
    log_and_print("-" * 50)

    data_path = Path("data")

    # Walk through all data files
    import os
    for root, dirs, files in os.walk(data_path):
        root_path = Path(root)
        level = len(root_path.parts) - len(data_path.parts)
        indent = "  " * level
        log_and_print(f"{indent}📁 {root_path.name}/")

        subindent = "  " * (level + 1)
        for file in files:
            file_path = root_path / file
            size = file_path.stat().st_size
            log_and_print(f"{subindent}📄 {file} ({size:,} bytes)")

    # Check specific patterns that validation/aggregation might be looking for
    patterns_to_check = [
        "data/raw/**/*.parquet",
        "data/raw/frame=1m/**/*.parquet",
        "data/raw/frame=1m/symbol=AAPL/**/*.parquet",
    ]

    for pattern in patterns_to_check:
        files = list(Path(".").glob(pattern))
        log_and_print(f"📊 Pattern '{pattern}': {len(files)} files")
        for f in files[:3]:  # Show first 3
            log_and_print(f"  📄 {f}")

def debug_job_data_connection():
    """Debug the connection between job records and data files."""
    log_and_print("\n🔍 Debugging Job-Data Connection")
    log_and_print("-" * 50)

    # Check if there's a way to map job ID to data files
    job_id = "AAPL_2025-05-18"

    # Look for files that might be related to this job
    data_files = list(Path("data/raw").rglob("*.parquet"))
    log_and_print(f"📊 Total raw data files: {len(data_files)}")

    # Check file timestamps vs job creation
    if data_files:
        sample_file = data_files[0]
        file_mtime = datetime.fromtimestamp(sample_file.stat().st_mtime)
        log_and_print(f"📅 Sample file modified: {file_mtime}")

        # Check if file path contains job-related info
        log_and_print(f"📄 Sample file path: {sample_file}")

        # Check if AAPL data exists
        aapl_files = [f for f in data_files if "AAPL" in str(f)]
        log_and_print(f"📊 AAPL-related files: {len(aapl_files)}")

        for f in aapl_files:
            log_and_print(f"  📄 {f}")

def debug_cli_module_imports():
    """Debug if CLI modules can import the data access components."""
    log_and_print("\n🔍 Debugging CLI Module Imports")
    log_and_print("-" * 50)

    try:
        # Test if we can import the validation components
        log_and_print("📦 Testing validation imports...")

        # This will show us if there are import errors
        success, stdout, stderr = run_command(
            ["python", "-c", "from marketpipe.cli.validate_ohlcv import *; print('Validation imports OK')"],
            "Test validation imports"
        )

        if success:
            log_and_print("✅ Validation imports successful")
        else:
            log_and_print(f"❌ Validation import error: {stderr}")

        # Test aggregation imports
        log_and_print("📦 Testing aggregation imports...")

        success, stdout, stderr = run_command(
            ["python", "-c", "from marketpipe.cli.aggregate_ohlcv import *; print('Aggregation imports OK')"],
            "Test aggregation imports"
        )

        if success:
            log_and_print("✅ Aggregation imports successful")
        else:
            log_and_print(f"❌ Aggregation import error: {stderr}")

    except Exception as e:
        log_and_print(f"❌ Import test error: {e}")

def main():
    """Run all debugging checks."""
    log_and_print("🔍 MarketPipe Validation & Aggregation Debug")
    log_and_print("=" * 60)

    # Run all debug functions
    debug_functions = [
        debug_job_database,
        debug_data_file_structure,
        debug_job_data_connection,
        debug_cli_module_imports,
        debug_validation_data_discovery,
        debug_aggregation_data_discovery,
    ]

    results = {}
    for debug_func in debug_functions:
        try:
            log_and_print(f"\n{'='*60}")
            results[debug_func.__name__] = debug_func()
        except Exception as e:
            log_and_print(f"❌ {debug_func.__name__} crashed: {e}")
            results[debug_func.__name__] = False

    # Summary
    log_and_print("\n📊 Debug Summary")
    log_and_print("=" * 60)

    for func_name, success in results.items():
        status = "✅" if success else "❌"
        log_and_print(f"{status} {func_name}: {'SUCCESS' if success else 'FAILED'}")

    log_and_print("\n💡 Next steps based on findings will be determined...")

if __name__ == "__main__":
    main()
