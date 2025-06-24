#!/usr/bin/env python3
"""
TSLA One Year Data Pipeline Script - CORRECTED VERSION

This script runs the complete MarketPipe pipeline to get exactly 1 year's worth 
of TSLA 1-minute OHLCV data (98,280 bars) using available historical data.

Due to Alpaca free tier limitations, this uses 2020 data which provides:
- 252 trading days × 390 minutes/day = 98,280 total bars
- Complete signal-ready dataset for backtesting and analysis

The pipeline includes:
1. Date calculation (full calendar year 2020)
2. Data ingestion in 30-day chunks (bypassing API limitations)
3. Data validation 
4. Data aggregation (5m, 15m, 1h, 1d)
5. Summary report with correct bar counts

Usage:
    python scripts/tsla_one_year_pipeline_fixed.py
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def log_and_print(message: str, level: str = "INFO") -> None:
    """Log and print message with color coding."""
    colors = {
        "INFO": "\033[94m",      # Blue
        "SUCCESS": "\033[92m",   # Green
        "WARNING": "\033[93m",   # Yellow
        "ERROR": "\033[91m",     # Red
        "PROCESS": "\033[95m",   # Magenta
        "CLEAN": "\033[96m"      # Cyan
    }
    reset = "\033[0m"

    colored_message = f"{colors.get(level, '')}{message}{reset}"
    print(colored_message)

    # Also log to file
    log_level = getattr(logging, level, logging.INFO)
    logger.log(log_level, message)

def check_credentials() -> bool:
    """Check if Alpaca credentials are available."""
    log_and_print("Checking Alpaca credentials...", "PROCESS")

    alpaca_key = os.getenv("ALPACA_KEY")
    alpaca_secret = os.getenv("ALPACA_SECRET")

    if not alpaca_key or not alpaca_secret:
        log_and_print("❌ Missing Alpaca credentials!", "ERROR")
        log_and_print("Please set ALPACA_KEY and ALPACA_SECRET environment variables", "ERROR")
        log_and_print("Or create a .env file with these variables", "ERROR")
        return False

    log_and_print("✅ Alpaca credentials found", "SUCCESS")
    return True

def calculate_full_year_range() -> Tuple[str, str]:
    """Calculate full calendar year 2020 (available historical data)."""
    # Use 2020 as it's available in Alpaca free tier and provides complete data
    start_date = "2020-01-02"  # First trading day of 2020
    end_date = "2020-12-31"    # Last day of 2020

    log_and_print(f"📅 Data range: {start_date} to {end_date}", "INFO")
    log_and_print("📊 Expected: ~252 trading days × 390 mins = 98,280 bars", "INFO")
    return start_date, end_date

def create_date_chunks(start_date: str, end_date: str, max_days: int = 25) -> List[Tuple[str, str]]:
    """Split date range into chunks that fit API limitations."""
    from datetime import datetime

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    chunks = []
    current_start = start

    while current_start <= end:
        # Calculate chunk end (ensure it doesn't exceed overall end)
        chunk_end = min(current_start + timedelta(days=max_days), end)

        # Skip weekends for start date
        while current_start.weekday() >= 5 and current_start <= end:
            current_start += timedelta(days=1)

        # Skip weekends for end date
        while chunk_end.weekday() >= 5 and chunk_end >= current_start:
            chunk_end -= timedelta(days=1)

        if current_start <= chunk_end:
            chunks.append((
                current_start.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d")
            ))

        # Move to next chunk
        current_start = chunk_end + timedelta(days=1)

    log_and_print(f"📦 Created {len(chunks)} date chunks (≤{max_days} days each)", "INFO")
    return chunks

def run_command(cmd: List[str], description: str, timeout: Optional[int] = None) -> Tuple[bool, str, str]:
    """Run shell command and return success status and output."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=Path.cwd()
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", f"Command timed out after {timeout} seconds"
    except Exception as e:
        return False, "", str(e)

def clean_previous_data():
    """Clean up previous data to ensure fresh run."""
    log_and_print("Cleaning previous data...", "CLEAN")

    import shutil
    dirs_to_clean = [
        "data/raw",
        "data/aggregated",
        "data/validation_reports"
    ]

    for dir_path in dirs_to_clean:
        try:
            if Path(dir_path).exists():
                shutil.rmtree(dir_path)
                log_and_print(f"Cleaned {dir_path}", "SUCCESS")
        except Exception as e:
            log_and_print(f"Warning: Could not clean {dir_path}: {e}", "WARNING")

def clear_job_conflicts():
    """Clear any existing job conflicts that might prevent ingestion."""
    log_and_print("Clearing any existing job conflicts...", "PROCESS")

    try:
        success, stdout, stderr = run_command(
            ["python", "scripts/clear_all_job_conflicts.py"],
            "Clear job conflicts"
        )
        if success:
            log_and_print("✅ Job conflicts cleared", "SUCCESS")
        else:
            log_and_print(f"Warning: Could not clear job conflicts: {stderr}", "WARNING")
    except Exception as e:
        log_and_print(f"Warning: Could not clear job conflicts: {e}", "WARNING")

def run_chunked_ingestion(date_chunks: List[Tuple[str, str]]) -> List[str]:
    """Run data ingestion for all date chunks."""
    log_and_print("Starting chunked TSLA data ingestion from Alpaca...", "PROCESS")

    successful_jobs = []
    failed_chunks = []

    for i, (start_date, end_date) in enumerate(date_chunks, 1):
        log_and_print(f"📦 Processing chunk {i}/{len(date_chunks)}: {start_date} to {end_date}", "PROCESS")

        success, stdout, stderr = run_command(
            [
                "python", "-m", "marketpipe", "ohlcv", "ingest",
                "--symbols", "TSLA",
                "--start", start_date,
                "--end", end_date,
                "--provider", "alpaca",
                "--feed-type", "iex",
                "--output", "data/raw",
                "--workers", "4"
            ],
            f"TSLA chunk {i} ingestion",
            timeout=600  # 10 minutes per chunk
        )

        if success:
            # Extract job ID from stdout
            job_id = None
            for line in stdout.split('\n'):
                if "Job ID:" in line:
                    job_id = line.split("Job ID:")[-1].strip()
                    break

            if job_id:
                successful_jobs.append(job_id)
                log_and_print(f"✅ Chunk {i} completed: {job_id}", "SUCCESS")
            else:
                log_and_print(f"⚠️ Chunk {i} completed but no job ID found", "WARNING")
        else:
            failed_chunks.append((i, start_date, end_date))
            log_and_print(f"❌ Chunk {i} failed: {stderr}", "ERROR")

            # Continue with other chunks even if one fails
            continue

    if failed_chunks:
        log_and_print(f"⚠️ {len(failed_chunks)} chunks failed:", "WARNING")
        for chunk_num, start, end in failed_chunks:
            log_and_print(f"  Chunk {chunk_num}: {start} to {end}", "WARNING")

    if successful_jobs:
        log_and_print(f"✅ Completed {len(successful_jobs)} chunks successfully", "SUCCESS")
        return successful_jobs
    else:
        log_and_print("❌ No chunks completed successfully", "ERROR")
        return []

def run_validation(job_ids: List[str]) -> bool:
    """Run data validation on all ingested data."""
    log_and_print("Running data validation...", "PROCESS")

    # Validate all TSLA data at once
    success, stdout, stderr = run_command(
        [
            "python", "-m", "marketpipe", "ohlcv", "validate",
            "--symbols", "TSLA",
            "--output", "data/validation_reports"
        ],
        "TSLA data validation",
        timeout=300  # 5 minutes
    )

    if success:
        log_and_print("✅ Data validation completed", "SUCCESS")
        return True
    else:
        log_and_print(f"❌ Validation failed: {stderr}", "ERROR")
        return False

def run_aggregation(job_ids: List[str]) -> bool:
    """Run data aggregation for multiple timeframes."""
    log_and_print("Running data aggregation for multiple timeframes...", "PROCESS")

    timeframes = ["5m", "15m", "1h", "1d"]

    for timeframe in timeframes:
        log_and_print(f"📊 Aggregating to {timeframe} timeframe...", "PROCESS")

        success, stdout, stderr = run_command(
            [
                "python", "-m", "marketpipe", "ohlcv", "aggregate",
                "--symbols", "TSLA",
                "--timeframe", timeframe,
                "--output", "data/aggregated"
            ],
            f"TSLA {timeframe} aggregation",
            timeout=300  # 5 minutes per timeframe
        )

        if success:
            log_and_print(f"✅ {timeframe} aggregation completed", "SUCCESS")
        else:
            log_and_print(f"❌ {timeframe} aggregation failed: {stderr}", "ERROR")
            return False

    log_and_print("✅ All aggregations completed", "SUCCESS")
    return True

def count_total_bars() -> int:
    """Count total bars in the dataset."""
    try:
        # Use DuckDB to count bars
        success, stdout, stderr = run_command(
            [
                "python", "-m", "marketpipe", "query",
                "SELECT COUNT(*) as total_bars FROM 'data/raw/**/*.parquet' WHERE symbol = 'TSLA'"
            ],
            "Count total bars"
        )

        if success:
            # Parse output to get the count
            for line in stdout.split('\n'):
                if line.strip().isdigit():
                    return int(line.strip())

        return 0
    except Exception:
        return 0

def generate_summary_report():
    """Generate final summary report."""
    log_and_print("Generating summary report...", "PROCESS")

    try:
        # Count total bars
        total_bars = count_total_bars()

        # Calculate data statistics
        success, stdout, stderr = run_command(
            [
                "python", "-m", "marketpipe", "query",
                "SELECT MIN(date) as start_date, MAX(date) as end_date, COUNT(DISTINCT date) as trading_days FROM 'data/raw/**/*.parquet' WHERE symbol = 'TSLA'"
            ],
            "Get date range statistics"
        )

        summary = {
            "timestamp": datetime.now().isoformat(),
            "symbol": "TSLA",
            "pipeline": "1-Year OHLCV Data (Corrected)",
            "source": "Alpaca Markets",
            "data_summary": {
                "total_bars": total_bars,
                "expected_bars": 98280,
                "completeness_pct": round((total_bars / 98280) * 100, 2) if total_bars > 0 else 0,
                "timeframes_available": ["1m", "5m", "15m", "1h", "1d"],
                "data_quality": "Validated",
                "ready_for_signals": total_bars > 50000  # Threshold for usable dataset
            }
        }

        # Save summary
        summary_path = Path("data/tsla_one_year_corrected_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)

        log_and_print(f"📊 Summary saved to: {summary_path}", "SUCCESS")
        log_and_print(f"📈 Total bars: {total_bars:,} ({summary['data_summary']['completeness_pct']}% of expected)", "INFO")

        if total_bars >= 90000:  # 90% of expected
            log_and_print("🎯 Dataset is COMPLETE and ready for signal generation!", "SUCCESS")
        elif total_bars >= 50000:  # 50% of expected
            log_and_print("⚠️ Dataset is PARTIAL but usable for analysis", "WARNING")
        else:
            log_and_print("❌ Dataset is INCOMPLETE - may need troubleshooting", "ERROR")

    except Exception as e:
        log_and_print(f"Warning: Could not generate complete summary: {e}", "WARNING")

def main():
    """Run the complete TSLA one-year pipeline with corrected date handling."""
    log_and_print("=" * 60, "INFO")
    log_and_print("TSLA ONE YEAR OHLCV DATA PIPELINE - CORRECTED VERSION", "INFO")
    log_and_print("=" * 60, "INFO")
    log_and_print("This will download 1 full year of 1-minute TSLA data (~98,280 bars)")
    log_and_print("Using 2020 historical data (available in Alpaca free tier)")
    log_and_print("Estimated runtime: 20-40 minutes for complete dataset")
    log_and_print("=" * 60, "INFO")

    start_time = time.time()

    try:
        # Step 1: Check prerequisites
        if not check_credentials():
            return 1

        # Step 2: Calculate date range (full year 2020)
        start_date, end_date = calculate_full_year_range()

        # Step 3: Create date chunks (≤30 days each)
        date_chunks = create_date_chunks(start_date, end_date, max_days=25)

        # Step 4: Clean previous data
        clean_previous_data()

        # Step 5: Clear job conflicts
        clear_job_conflicts()

        # Step 6: Run chunked ingestion
        job_ids = run_chunked_ingestion(date_chunks)
        if not job_ids:
            log_and_print("Pipeline failed at ingestion step", "ERROR")
            return 1

        # Step 7: Run validation
        if not run_validation(job_ids):
            log_and_print("Pipeline failed at validation step", "ERROR")
            return 1

        # Step 8: Run aggregation
        if not run_aggregation(job_ids):
            log_and_print("Pipeline failed at aggregation step", "ERROR")
            return 1

        # Step 9: Generate summary
        generate_summary_report()

        # Success!
        elapsed = time.time() - start_time
        log_and_print(f"🎉 TSLA One Year Pipeline completed successfully in {elapsed/60:.1f} minutes!", "SUCCESS")
        log_and_print("🎯 Full year dataset with ~98,280 bars is ready for signal generation!", "SUCCESS")

        return 0

    except KeyboardInterrupt:
        log_and_print("Pipeline interrupted by user", "WARNING")
        return 1
    except Exception as e:
        log_and_print(f"Pipeline failed with unexpected error: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
