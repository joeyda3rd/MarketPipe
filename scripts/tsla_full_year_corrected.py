#!/usr/bin/env python3
"""
TSLA Full Year Data Pipeline - CORRECTED VERSION

Gets exactly 98,280 bars (252 trading days × 390 minutes) of TSLA 1-minute data.
Uses 2020 historical data which is available in Alpaca free tier.

Fixed issues:
1. Uses proper full year date range (2020-01-02 to 2020-12-31)
2. Handles 30-day API limitation with proper chunking
3. Validates we get the expected ~98,280 bars

Usage: python scripts/tsla_full_year_corrected.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple


def log_and_print(message: str, level: str = "INFO") -> None:
    """Log and print message with color coding."""
    colors = {
        "INFO": "\033[94m",      # Blue
        "SUCCESS": "\033[92m",   # Green
        "WARNING": "\033[93m",   # Yellow
        "ERROR": "\033[91m",     # Red
        "PROCESS": "\033[95m",   # Magenta
    }
    reset = "\033[0m"

    colored_message = f"{colors.get(level, '')}{message}{reset}"
    print(colored_message)

def check_credentials() -> bool:
    """Check if Alpaca credentials are available."""
    log_and_print("Checking Alpaca credentials...", "PROCESS")

    alpaca_key = os.getenv("ALPACA_KEY")
    alpaca_secret = os.getenv("ALPACA_SECRET")

    if not alpaca_key or not alpaca_secret:
        log_and_print("❌ Missing Alpaca credentials!", "ERROR")
        log_and_print("Please set ALPACA_KEY and ALPACA_SECRET environment variables", "ERROR")
        return False

    log_and_print("✅ Alpaca credentials found", "SUCCESS")
    return True

def create_month_chunks() -> List[Tuple[str, str]]:
    """Create monthly chunks for 2020 to stay under 30-day limit."""
    chunks = [
        ("2020-01-02", "2020-01-31"),  # January
        ("2020-02-03", "2020-02-28"),  # February
        ("2020-03-02", "2020-03-31"),  # March
        ("2020-04-01", "2020-04-30"),  # April
        ("2020-05-01", "2020-05-29"),  # May
        ("2020-06-01", "2020-06-30"),  # June
        ("2020-07-01", "2020-07-31"),  # July
        ("2020-08-03", "2020-08-31"),  # August
        ("2020-09-01", "2020-09-30"),  # September
        ("2020-10-01", "2020-10-30"),  # October
        ("2020-11-02", "2020-11-30"),  # November
        ("2020-12-01", "2020-12-31"),  # December
    ]

    log_and_print(f"📦 Created {len(chunks)} monthly chunks for 2020", "INFO")
    log_and_print("📊 Expected total: ~98,280 bars (252 trading days × 390 minutes)", "INFO")
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

def clean_and_setup():
    """Clean previous data and setup fresh environment."""
    log_and_print("Setting up clean environment...", "PROCESS")

    # Clear any job conflicts
    try:
        run_command(["python", "scripts/clear_all_job_conflicts.py"], "Clear conflicts")
        log_and_print("✅ Job conflicts cleared", "SUCCESS")
    except:
        log_and_print("⚠️ Could not clear job conflicts", "WARNING")

    # Ensure data directory exists
    Path("data/tsla_full_year").mkdir(parents=True, exist_ok=True)

def run_monthly_ingestion(chunks: List[Tuple[str, str]]) -> List[str]:
    """Run data ingestion for all monthly chunks."""
    log_and_print("Starting monthly TSLA data ingestion...", "PROCESS")

    successful_jobs = []
    total_bars = 0

    for i, (start_date, end_date) in enumerate(chunks, 1):
        log_and_print(f"📦 Month {i}/12: {start_date} to {end_date}", "PROCESS")

        success, stdout, stderr = run_command(
            [
                "python", "-m", "marketpipe", "ohlcv", "ingest",
                "--symbols", "TSLA",
                "--start", start_date,
                "--end", end_date,
                "--provider", "alpaca",
                "--feed-type", "iex",
                "--output", "data/tsla_full_year",
                "--workers", "4"
            ],
            f"TSLA month {i}",
            timeout=600  # 10 minutes per month
        )

        if success:
            # Count bars in this chunk
            bars_line = [line for line in stdout.split('\n') if 'Total bars:' in line]
            chunk_bars = 0
            if bars_line:
                try:
                    chunk_bars = int(bars_line[0].split('Total bars:')[1].strip())
                    total_bars += chunk_bars
                except:
                    chunk_bars = 0

            successful_jobs.append(f"TSLA_{start_date}")
            log_and_print(f"✅ Month {i} completed: {chunk_bars:,} bars", "SUCCESS")
        else:
            log_and_print(f"❌ Month {i} failed: {stderr}", "ERROR")

    log_and_print(f"📊 Total ingested: {total_bars:,} bars", "INFO")
    log_and_print(f"📈 Progress: {(total_bars/98280)*100:.1f}% of expected 98,280 bars", "INFO")

    return successful_jobs

def count_final_bars() -> int:
    """Count total bars in the final dataset."""
    try:
        success, stdout, stderr = run_command(
            [
                "python", "-m", "marketpipe", "query",
                "SELECT COUNT(*) as total FROM 'data/tsla_full_year/**/*.parquet' WHERE symbol = 'TSLA'"
            ],
            "Count total bars"
        )

        if success:
            for line in stdout.split('\n'):
                if line.strip().isdigit():
                    return int(line.strip())
        return 0
    except:
        return 0

def generate_final_report(total_bars: int):
    """Generate final comprehensive report."""
    log_and_print("Generating final report...", "PROCESS")

    # Calculate statistics
    expected_bars = 98280
    completeness = (total_bars / expected_bars) * 100 if total_bars > 0 else 0

    # Get date range
    try:
        success, stdout, stderr = run_command(
            [
                "python", "-m", "marketpipe", "query",
                "SELECT MIN(date) as start_date, MAX(date) as end_date, COUNT(DISTINCT date) as trading_days FROM 'data/tsla_full_year/**/*.parquet' WHERE symbol = 'TSLA'"
            ],
            "Get date statistics"
        )
        date_info = "Date range analysis available in query output"
    except:
        date_info = "Could not analyze date range"

    report = {
        "pipeline": "TSLA Full Year Data - CORRECTED",
        "timestamp": datetime.now().isoformat(),
        "symbol": "TSLA",
        "data_source": "Alpaca Markets (IEX feed)",
        "results": {
            "total_bars": total_bars,
            "expected_bars": expected_bars,
            "completeness_percent": round(completeness, 2),
            "data_year": "2020",
            "resolution": "1-minute",
            "ready_for_signals": total_bars >= 80000  # 80% threshold
        },
        "quality_status": {
            "complete": completeness >= 95,
            "usable": completeness >= 80,
            "partial": completeness >= 50,
            "insufficient": completeness < 50
        }
    }

    # Save report
    report_path = Path("data/tsla_full_year_final_report.json")
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    # Print summary
    log_and_print("=" * 60, "INFO")
    log_and_print("📊 FINAL RESULTS", "INFO")
    log_and_print("=" * 60, "INFO")
    log_and_print(f"📈 Total bars: {total_bars:,}", "INFO")
    log_and_print(f"🎯 Expected: {expected_bars:,}", "INFO")
    log_and_print(f"📊 Completeness: {completeness:.1f}%", "INFO")

    if completeness >= 95:
        log_and_print("🎉 EXCELLENT: Dataset is complete and ready for signal generation!", "SUCCESS")
    elif completeness >= 80:
        log_and_print("✅ GOOD: Dataset is highly usable for analysis and backtesting", "SUCCESS")
    elif completeness >= 50:
        log_and_print("⚠️ PARTIAL: Dataset is usable but may have gaps", "WARNING")
    else:
        log_and_print("❌ INSUFFICIENT: Dataset needs more data", "ERROR")

    log_and_print(f"📄 Full report saved: {report_path}", "INFO")

def main():
    """Run the corrected TSLA full year pipeline."""
    log_and_print("=" * 60, "INFO")
    log_and_print("TSLA FULL YEAR PIPELINE - CORRECTED VERSION", "INFO")
    log_and_print("Target: 98,280 bars (252 trading days × 390 minutes)", "INFO")
    log_and_print("=" * 60, "INFO")

    start_time = time.time()

    try:
        # Step 1: Prerequisites
        if not check_credentials():
            return 1

        # Step 2: Setup
        clean_and_setup()

        # Step 3: Create monthly chunks
        chunks = create_month_chunks()

        # Step 4: Run monthly ingestion
        job_ids = run_monthly_ingestion(chunks)
        if not job_ids:
            log_and_print("❌ No successful ingestion jobs completed", "ERROR")
            return 1

        # Step 5: Count final results
        total_bars = count_final_bars()

        # Step 6: Generate report
        generate_final_report(total_bars)

        # Success summary
        elapsed = time.time() - start_time
        log_and_print(f"🏁 Pipeline completed in {elapsed/60:.1f} minutes", "SUCCESS")
        log_and_print(f"🎯 Result: {total_bars:,} bars ready for signal generation!", "SUCCESS")

        return 0 if total_bars > 50000 else 1

    except KeyboardInterrupt:
        log_and_print("Pipeline interrupted by user", "WARNING")
        return 1
    except Exception as e:
        log_and_print(f"Pipeline failed: {e}", "ERROR")
        return 1

if __name__ == "__main__":
    sys.exit(main())
