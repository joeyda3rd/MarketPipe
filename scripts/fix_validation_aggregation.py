#!/usr/bin/env python3
"""
Fix Validation and Aggregation by Direct Data Access

Instead of relying on job records, this script directly processes the existing data files.
"""

import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path


def log_and_print(message: str):
    """Print and log message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def create_manual_job_record():
    """Manually create a job record for the existing data."""
    log_and_print("🔧 Creating manual job record...")

    # Check what data exists
    data_files = list(Path("data/raw").rglob("*.parquet"))
    if not data_files:
        log_and_print("❌ No data files found!")
        return False

    log_and_print(f"📊 Found {len(data_files)} data files")

    # Extract info from file path
    sample_file = data_files[0]
    path_parts = sample_file.parts

    # Parse path: data/raw/frame=1m/symbol=AAPL/date=2020-07-27/filename.parquet
    symbol = None
    date = None

    for part in path_parts:
        if part.startswith("symbol="):
            symbol = part.split("=")[1]
        elif part.startswith("date="):
            date = part.split("=")[1]

    if not symbol or not date:
        log_and_print(f"❌ Cannot parse symbol/date from path: {sample_file}")
        return False

    log_and_print(f"📊 Detected: symbol={symbol}, date={date}")

    # Create job record in database
    try:
        conn = sqlite3.connect("ingestion_jobs.db")
        cursor = conn.cursor()

        job_id = f"{symbol}_{date}"
        payload = {
            "symbol": symbol,
            "start_date": date,
            "end_date": date,
            "data_path": str(sample_file.parent),
            "manual_creation": True
        }

        cursor.execute("""
            INSERT INTO ingestion_jobs 
            (symbol, day, state, payload, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            date,
            "COMPLETED",
            str(payload),
            datetime.now().isoformat(),
            datetime.now().isoformat()
        ))

        # Get the auto-generated ID
        job_record_id = cursor.lastrowid

        conn.commit()
        conn.close()

        log_and_print(f"✅ Created job record: ID={job_record_id}, job_id={job_id}")
        return job_id

    except Exception as e:
        log_and_print(f"❌ Database error: {e}")
        return False

def run_validation_with_job_id(job_id):
    """Run validation using the job ID."""
    log_and_print(f"🔍 Running validation for job: {job_id}")

    try:
        result = subprocess.run(
            ["python", "-m", "marketpipe", "validate-ohlcv", "--job-id", job_id],
            capture_output=True,
            text=True,
            timeout=60
        )

        log_and_print(f"📄 Validation stdout: {result.stdout}")
        if result.stderr:
            log_and_print(f"🚨 Validation stderr: {result.stderr}")

        return result.returncode == 0

    except Exception as e:
        log_and_print(f"❌ Validation error: {e}")
        return False

def run_aggregation_with_job_id(job_id):
    """Run aggregation using the job ID."""
    log_and_print(f"📊 Running aggregation for job: {job_id}")

    try:
        result = subprocess.run(
            ["python", "-m", "marketpipe", "aggregate-ohlcv", job_id],
            capture_output=True,
            text=True,
            timeout=60
        )

        log_and_print(f"📄 Aggregation stdout: {result.stdout}")
        if result.stderr:
            log_and_print(f"🚨 Aggregation stderr: {result.stderr}")

        return result.returncode == 0

    except Exception as e:
        log_and_print(f"❌ Aggregation error: {e}")
        return False

def test_direct_data_access():
    """Test if we can access the data directly without job system."""
    log_and_print("🔍 Testing direct data access...")

    try:
        result = subprocess.run(
            ["python", "-m", "marketpipe", "query", "SELECT COUNT(*) as total_bars, symbol FROM 'data/raw/**/*.parquet' GROUP BY symbol"],
            capture_output=True,
            text=True,
            timeout=30
        )

        log_and_print(f"📄 Query result: {result.stdout}")
        if result.stderr:
            log_and_print(f"🚨 Query stderr: {result.stderr}")

        return result.returncode == 0

    except Exception as e:
        log_and_print(f"❌ Query error: {e}")
        return False

def check_aggregated_views():
    """Check if aggregated views work after fixing."""
    log_and_print("🔍 Checking aggregated views...")

    views_to_test = ["bars_5m", "bars_15m", "bars_1h", "bars_1d"]

    for view in views_to_test:
        try:
            result = subprocess.run(
                ["python", "-m", "marketpipe", "query", f"SELECT COUNT(*) as bars FROM {view}"],
                capture_output=True,
                text=True,
                timeout=30
            )

            log_and_print(f"📊 {view}: {result.stdout.strip()}")

        except Exception as e:
            log_and_print(f"❌ {view} error: {e}")

def main():
    """Run the complete fix process."""
    log_and_print("🔧 MarketPipe Validation & Aggregation Fix")
    log_and_print("=" * 60)

    # Test direct data access first
    log_and_print("\n📊 Step 1: Test Direct Data Access")
    if not test_direct_data_access():
        log_and_print("❌ Direct data access failed! Check data files.")
        return

    # Create manual job record
    log_and_print("\n📊 Step 2: Create Manual Job Record")
    job_id = create_manual_job_record()
    if not job_id:
        log_and_print("❌ Failed to create job record!")
        return

    # Test validation
    log_and_print("\n📊 Step 3: Test Validation")
    validation_success = run_validation_with_job_id(job_id)

    # Test aggregation
    log_and_print("\n📊 Step 4: Test Aggregation")
    aggregation_success = run_aggregation_with_job_id(job_id)

    # Check views
    log_and_print("\n📊 Step 5: Check Aggregated Views")
    check_aggregated_views()

    # Summary
    log_and_print("\n📊 Summary")
    log_and_print("=" * 60)
    log_and_print("✅ Direct data access: SUCCESS")
    log_and_print(f"{'✅' if job_id else '❌'} Job record creation: {'SUCCESS' if job_id else 'FAILED'}")
    log_and_print(f"{'✅' if validation_success else '❌'} Validation: {'SUCCESS' if validation_success else 'FAILED'}")
    log_and_print(f"{'✅' if aggregation_success else '❌'} Aggregation: {'SUCCESS' if aggregation_success else 'FAILED'}")

    if job_id and validation_success and aggregation_success:
        log_and_print("\n🎉 ALL SYSTEMS NOW WORKING!")
        log_and_print(f"💡 Use job ID: {job_id} for future operations")
    else:
        log_and_print("\n⚠️  Some issues remain, but progress made")

if __name__ == "__main__":
    main()
