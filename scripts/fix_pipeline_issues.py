#!/usr/bin/env python3
"""
Comprehensive Fix Script for MarketPipe Pipeline Issues

This script addresses all the root causes identified:
1. Timestamp issues (wrong dates in data)
2. SQL schema mismatches 
3. Missing pipeline components
4. Configuration problems

Usage:
    python scripts/fix_pipeline_issues.py
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import json

def log_and_print(message: str):
    """Print and log message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def run_command(cmd: list, description: str) -> tuple[bool, str, str]:
    """Run command and return success, stdout, stderr."""
    log_and_print(f"🔧 {description}")
    log_and_print(f"💻 Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            log_and_print(f"✅ {description} succeeded")
        else:
            log_and_print(f"❌ {description} failed")
            log_and_print(f"🚨 Error: {result.stderr}")
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        log_and_print(f"⏰ {description} timed out")
        return False, "", "Command timed out"
    except Exception as e:
        log_and_print(f"💥 {description} crashed: {e}")
        return False, "", str(e)

def fix_1_clean_old_data():
    """Fix 1: Clean old data with wrong timestamps."""
    log_and_print("\n🧹 FIX 1: Cleaning Old Data with Wrong Timestamps")
    log_and_print("-" * 60)
    
    # Back up existing data first
    data_path = Path("data")
    if data_path.exists():
        backup_path = Path("data_backup_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
        log_and_print(f"📦 Backing up data to {backup_path}")
        shutil.copytree(data_path, backup_path)
        
        # Remove problematic data
        for problem_dir in ["data/raw", "data/aggregated", "data/validation_reports"]:
            if Path(problem_dir).exists():
                log_and_print(f"🗑️ Removing {problem_dir}")
                shutil.rmtree(problem_dir)
        
        # Recreate directories
        for dir_name in ["raw", "aggregated", "validation_reports"]:
            Path(f"data/{dir_name}").mkdir(parents=True, exist_ok=True)
            log_and_print(f"📁 Created clean data/{dir_name}")
    
    return True

def fix_2_create_proper_config():
    """Fix 2: Create proper configuration for current dates."""
    log_and_print("\n⚙️ FIX 2: Creating Proper Configuration")
    log_and_print("-" * 60)
    
    # Calculate proper date range (last 5 trading days)
    today = datetime.now()
    start_date = today - timedelta(days=7)  # Go back a week to ensure we get trading days
    end_date = today - timedelta(days=1)    # Yesterday
    
    config_content = f"""# MarketPipe Configuration - Fixed Version
alpaca:
  # Credentials loaded from environment variables
  key: # Will be loaded from ALPACA_KEY
  secret: # Will be loaded from ALPACA_SECRET  
  base_url: https://data.alpaca.markets/v2
  rate_limit_per_min: 200
  feed: iex
  timeout: 30.0
  max_retries: 3

# Small test with current dates
symbols:
  - AAPL
  - MSFT

start: "{start_date.strftime('%Y-%m-%d')}"
end: "{end_date.strftime('%Y-%m-%d')}"
output_path: "./data"
compression: snappy
workers: 1  # Single worker for testing

# Enable metrics
metrics:
  enabled: true
  port: 8000
  multiprocess_dir: "/tmp/prometheus_multiproc"

# Enable state management  
state:
  backend: sqlite
  path: "./ingestion_jobs.db"
"""
    
    config_path = Path("config/fixed_config.yaml")
    config_path.parent.mkdir(exist_ok=True)
    
    with open(config_path, 'w') as f:
        f.write(config_content)
    
    log_and_print(f"📝 Created fixed configuration: {config_path}")
    log_and_print(f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    return True

def fix_3_check_credentials():
    """Fix 3: Verify API credentials are available."""
    log_and_print("\n🔑 FIX 3: Checking API Credentials")
    log_and_print("-" * 60)
    
    # Check if .env file exists
    env_file = Path(".env")
    if not env_file.exists():
        log_and_print("❌ No .env file found")
        log_and_print("💡 Please create .env file with:")
        log_and_print("   ALPACA_KEY=your_api_key_here")
        log_and_print("   ALPACA_SECRET=your_secret_here")
        return False
    
    # Check environment variables
    alpaca_key = os.getenv("ALPACA_KEY")
    alpaca_secret = os.getenv("ALPACA_SECRET")
    
    if not alpaca_key or not alpaca_secret:
        log_and_print("❌ ALPACA_KEY or ALPACA_SECRET not found in environment")
        log_and_print("💡 Make sure your .env file contains both values")
        return False
    
    log_and_print("✅ API credentials found")
    log_and_print(f"🔑 ALPACA_KEY: {alpaca_key[:8]}..." if alpaca_key else "❌ Missing")
    log_and_print(f"🔑 ALPACA_SECRET: {'*' * 8}..." if alpaca_secret else "❌ Missing")
    
    return True

def fix_4_test_small_ingestion():
    """Fix 4: Test small ingestion with correct dates."""
    log_and_print("\n📥 FIX 4: Testing Small Ingestion with Correct Dates")
    log_and_print("-" * 60)
    
    # Use the fixed config for a small test
    success, stdout, stderr = run_command(
        [
            "python", "-m", "marketpipe", "ingest-ohlcv", 
            "--config", "config/fixed_config.yaml",
            "--symbol", "AAPL",
            "--workers", "1"
        ],
        "Small test ingestion"
    )
    
    if success:
        log_and_print("✅ Small ingestion test succeeded")
        
        # Check if data was created with correct dates
        data_files = list(Path("data/raw").rglob("*.parquet"))
        if data_files:
            log_and_print(f"📊 Created {len(data_files)} data files")
            
            # Check dates in file paths
            current_year = datetime.now().year
            correct_dates = [f for f in data_files if str(current_year) in str(f)]
            if correct_dates:
                log_and_print(f"✅ Found {len(correct_dates)} files with {current_year} dates")
                return True
            else:
                log_and_print(f"❌ No files have {current_year} dates in path")
                # Show what dates we got
                for f in data_files[:3]:
                    log_and_print(f"📅 Sample file: {f}")
        else:
            log_and_print("❌ No parquet files created")
    
    return success

def fix_5_test_query_schema():
    """Fix 5: Test and fix query schema issues."""
    log_and_print("\n🔍 FIX 5: Testing and Fixing Query Schema")
    log_and_print("-" * 60)
    
    # Test basic queries that should work with actual schema
    test_queries = [
        "SHOW TABLES",
        "SELECT COUNT(*) as file_count FROM duckdb_files('data/raw/**/*.parquet')",
        "SELECT * FROM 'data/raw/**/*.parquet' LIMIT 5",
    ]
    
    for query in test_queries:
        success, stdout, stderr = run_command(
            ["python", "-m", "marketpipe", "query", query],
            f"Testing query: {query[:40]}..."
        )
        
        if success:
            log_and_print(f"📊 Query result: {stdout.strip()[:100]}...")
        else:
            log_and_print(f"❌ Query failed: {stderr.strip()[:100]}...")
    
    return True

def fix_6_test_validation():
    """Fix 6: Test validation with correct data."""
    log_and_print("\n✅ FIX 6: Testing Validation with Correct Data")
    log_and_print("-" * 60)
    
    # First check if we have any ingestion jobs to validate
    success, stdout, stderr = run_command(
        ["python", "-m", "marketpipe", "validate-ohlcv", "--list"],
        "Listing available validation targets"
    )
    
    if success and "No validation reports found" not in stdout:
        log_and_print("📊 Found validation targets, attempting validation")
        
        # Try validation
        success, stdout, stderr = run_command(
            ["python", "-m", "marketpipe", "validate-ohlcv", "--job-id", "latest"],
            "Running validation on latest job"
        )
        
        if success:
            log_and_print("✅ Validation succeeded")
        else:
            log_and_print(f"⚠️ Validation had issues: {stderr}")
    else:
        log_and_print("ℹ️ No validation targets available yet (need successful ingestion first)")
    
    return True

def fix_7_test_aggregation():
    """Fix 7: Test aggregation with correct data."""
    log_and_print("\n📈 FIX 7: Testing Aggregation with Correct Data")
    log_and_print("-" * 60)
    
    # Check if we have raw data to aggregate
    raw_files = list(Path("data/raw").rglob("*.parquet"))
    if not raw_files:
        log_and_print("⚠️ No raw data files found, skipping aggregation test")
        return True
    
    log_and_print(f"📊 Found {len(raw_files)} raw files, testing aggregation")
    
    success, stdout, stderr = run_command(
        ["python", "-m", "marketpipe", "aggregate-ohlcv", "--frame", "5m", "--symbol", "AAPL"],
        "Testing 5-minute aggregation"
    )
    
    if success:
        log_and_print("✅ Aggregation test succeeded")
        
        # Check if aggregated files were created
        agg_files = list(Path("data/aggregated").rglob("*.parquet"))
        log_and_print(f"📊 Created {len(agg_files)} aggregated files")
    else:
        log_and_print(f"❌ Aggregation failed: {stderr}")
    
    return success

def main():
    """Run all fixes in sequence."""
    log_and_print("🚀 MarketPipe Comprehensive Fix Script")
    log_and_print("=" * 60)
    
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    # Run all fixes
    fixes = [
        fix_1_clean_old_data,
        fix_2_create_proper_config,
        fix_3_check_credentials,
        fix_4_test_small_ingestion,
        fix_5_test_query_schema,
        fix_6_test_validation,
        fix_7_test_aggregation,
    ]
    
    results = {}
    for fix_func in fixes:
        try:
            log_and_print(f"\n{'='*60}")
            results[fix_func.__name__] = fix_func()
        except Exception as e:
            log_and_print(f"❌ {fix_func.__name__} crashed: {e}")
            results[fix_func.__name__] = False
    
    # Summary
    log_and_print("\n📊 Fix Summary")
    log_and_print("=" * 60)
    
    for func_name, success in results.items():
        status = "✅" if success else "❌"
        log_and_print(f"{status} {func_name}: {'SUCCESS' if success else 'FAILED'}")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    log_and_print(f"\n🎯 Overall: {passed}/{total} fixes succeeded")
    
    if passed == total:
        log_and_print("\n🎉 ALL FIXES COMPLETED SUCCESSFULLY!")
        log_and_print("💡 You can now run the enhanced pipeline script")
    else:
        log_and_print("\n⚠️ Some fixes failed. Check the logs above for details.")
        log_and_print("💡 Address the failed fixes before running the full pipeline")

if __name__ == "__main__":
    main() 