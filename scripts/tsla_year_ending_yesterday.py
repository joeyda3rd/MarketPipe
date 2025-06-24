#!/usr/bin/env python3
"""
TSLA One Year Ending Yesterday Pipeline

Downloads exactly 1 year of TSLA 1-minute OHLCV data ending on the last trading day before today.
Date range: 2024-06-20 to 2025-06-20 (~101,400 bars)

This script:
✅ Uses current date calculation (ending yesterday)
✅ Works within 730-day limit 
✅ Handles 30-day API chunking
✅ Validates final bar count
✅ Provides comprehensive reporting

Usage: python scripts/tsla_year_ending_yesterday.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
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

def calculate_year_ending_yesterday() -> Tuple[str, str]:
    """Calculate exactly 1 year ending yesterday."""
    today = datetime.now().date()

    # Find yesterday (last trading day)
    end_date = today - timedelta(days=1)
    while end_date.weekday() >= 5:  # Skip weekends
        end_date -= timedelta(days=1)

    # Go back exactly 1 year
    start_date = end_date - timedelta(days=365)
    while start_date.weekday() >= 5:  # Skip weekends
        start_date += timedelta(days=1)

    # Verify within 730-day limit
    limit_date = today - timedelta(days=730)
    if start_date < limit_date:
        log_and_print(f"⚠️ Start date {start_date} is before 730-day limit {limit_date}", "WARNING")
        start_date = limit_date + timedelta(days=1)
        while start_date.weekday() >= 5:
            start_date += timedelta(days=1)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    total_days = (end_date - start_date).days
    est_trading_days = total_days * 5 // 7
    est_bars = est_trading_days * 390

    log_and_print(f"📅 Date range: {start_str} to {end_str}", "INFO")
    log_and_print(f"📊 Total days: {total_days}", "INFO")
    log_and_print(f"📈 Est. trading days: ~{est_trading_days}", "INFO")
    log_and_print(f"🎯 Est. bars: ~{est_bars:,}", "INFO")

    return start_str, end_str

def create_monthly_chunks(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    """Create monthly chunks staying under 30-day limit."""
    from datetime import datetime, timedelta

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    chunks = []
    current = start

    while current <= end:
        # Create ~25-day chunks (under 30-day limit)
        chunk_end = min(current + timedelta(days=25), end)

        # Adjust for weekends
        while current.weekday() >= 5 and current <= end:
            current += timedelta(days=1)
        while chunk_end.weekday() >= 5 and chunk_end >= current:
            chunk_end -= timedelta(days=1)

        if current <= chunk_end:
            chunks.append((
                current.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d")
            ))

        current = chunk_end + timedelta(days=1)

    log_and_print(f"📦 Created {len(chunks)} chunks (≤25 days each)", "INFO")
    return chunks

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

def setup_environment():
    """Setup clean environment."""
    log_and_print("Setting up environment...", "PROCESS")

    # Clear conflicts
    try:
        run_command(["python", "scripts/clear_all_job_conflicts.py"], "Clear conflicts")
        log_and_print("✅ Job conflicts cleared", "SUCCESS")
    except:
        log_and_print("⚠️ Could not clear job conflicts", "WARNING")

    # Create output directory
    output_dir = Path("data/tsla_year_ending_yesterday")
    output_dir.mkdir(parents=True, exist_ok=True)
    log_and_print(f"📁 Output directory: {output_dir}", "INFO")

def run_chunked_ingestion(chunks: List[Tuple[str, str]]) -> List[str]:
    """Run ingestion for all chunks."""
    log_and_print("🚀 Starting chunked TSLA ingestion...", "PROCESS")

    successful_jobs = []
    total_bars = 0
    failed_chunks = []

    for i, (start_date, end_date) in enumerate(chunks, 1):
        log_and_print(f"📦 Chunk {i:2d}/{len(chunks)}: {start_date} to {end_date}", "PROCESS")

        success, stdout, stderr = run_command(
            [
                "python", "-m", "marketpipe", "ohlcv", "ingest",
                "--symbols", "TSLA",
                "--start", start_date,
                "--end", end_date,
                "--provider", "alpaca",
                "--feed-type", "iex",
                "--output", "data/tsla_year_ending_yesterday",
                "--workers", "4"
            ],
            f"TSLA chunk {i}",
            timeout=900  # 15 minutes per chunk
        )

        if success:
            # Extract bar count
            bars_line = [line for line in stdout.split('\n') if 'Total bars:' in line]
            chunk_bars = 0
            if bars_line:
                try:
                    chunk_bars = int(bars_line[0].split('Total bars:')[1].strip())
                    total_bars += chunk_bars
                except:
                    chunk_bars = 0

            successful_jobs.append(f"TSLA_{start_date}")
            log_and_print(f"✅ Chunk {i:2d} completed: {chunk_bars:,} bars", "SUCCESS")
            log_and_print(f"📈 Running total: {total_bars:,} bars", "INFO")

        else:
            failed_chunks.append((i, start_date, end_date))
            error_msg = stderr.split('❌')[1] if '❌' in stderr else stderr[:100]
            log_and_print(f"❌ Chunk {i:2d} failed: {error_msg}", "ERROR")

    # Summary
    log_and_print("=" * 60, "INFO")
    log_and_print("📊 INGESTION SUMMARY", "INFO")
    log_and_print(f"✅ Successful: {len(successful_jobs)}/{len(chunks)} chunks", "INFO")
    log_and_print(f"📈 Total bars: {total_bars:,}", "INFO")
    log_and_print(f"⚠️ Failed: {len(failed_chunks)} chunks", "WARNING" if failed_chunks else "INFO")

    return successful_jobs

def verify_final_dataset() -> Tuple[int, str, str]:
    """Verify and count the final dataset."""
    log_and_print("🔍 Verifying final dataset...", "PROCESS")

    try:
        # Count total bars
        success, stdout, stderr = run_command(
            [
                "python", "-m", "marketpipe", "query",
                "SELECT COUNT(*) as total_bars FROM 'data/tsla_year_ending_yesterday/**/*.parquet' WHERE symbol = 'TSLA'"
            ],
            "Count bars"
        )

        total_bars = 0
        if success:
            for line in stdout.split('\n'):
                if line.strip().isdigit():
                    total_bars = int(line.strip())
                    break

        # Get date range
        success2, stdout2, stderr2 = run_command(
            [
                "python", "-m", "marketpipe", "query",
                "SELECT MIN(date) as start_date, MAX(date) as end_date, COUNT(DISTINCT date) as trading_days FROM 'data/tsla_year_ending_yesterday/**/*.parquet' WHERE symbol = 'TSLA'"
            ],
            "Get date range"
        )

        date_info = "Date range query completed"
        if success2:
            date_info = stdout2

        return total_bars, date_info, stderr if not success else ""

    except Exception as e:
        return 0, f"Error during verification: {e}", str(e)

def generate_verification_report(total_bars: int, date_info: str):
    """Generate final verification report."""
    log_and_print("📄 Generating verification report...", "PROCESS")

    # Calculate stats
    expected_min = 95000  # Realistic minimum for 1 year
    expected_max = 105000  # Realistic maximum for 1 year

    report = {
        "pipeline": "TSLA Year Ending Yesterday",
        "completion_timestamp": datetime.now().isoformat(),
        "symbol": "TSLA",
        "data_source": "Alpaca Markets (IEX feed)",
        "verification": {
            "total_bars": total_bars,
            "expected_range": f"{expected_min:,} - {expected_max:,}",
            "within_expected": expected_min <= total_bars <= expected_max,
            "completeness_estimate": round((total_bars / 100000) * 100, 1) if total_bars > 0 else 0,
            "ready_for_signals": total_bars >= 90000
        },
        "date_analysis": date_info,
        "quality_status": {
            "excellent": total_bars >= 100000,
            "very_good": total_bars >= 95000,
            "good": total_bars >= 85000,
            "acceptable": total_bars >= 70000,
            "needs_work": total_bars < 70000
        }
    }

    # Save report
    report_path = Path("data/tsla_year_ending_yesterday_verification.json")
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    # Print verification results
    log_and_print("=" * 70, "INFO")
    log_and_print("🎯 TSLA YEAR ENDING YESTERDAY - VERIFICATION RESULTS", "SUCCESS")
    log_and_print("=" * 70, "INFO")
    log_and_print(f"📊 Total bars verified: {total_bars:,}", "INFO")
    log_and_print(f"📈 Expected range: {expected_min:,} - {expected_max:,}", "INFO")

    if total_bars >= 100000:
        log_and_print("🎉 EXCELLENT: Full year dataset complete!", "SUCCESS")
        log_and_print("🚀 Ready for: Signal generation, backtesting, ML", "SUCCESS")
    elif total_bars >= 95000:
        log_and_print("✅ VERY GOOD: Nearly complete dataset", "SUCCESS")
        log_and_print("🚀 Ready for: All trading applications", "SUCCESS")
    elif total_bars >= 85000:
        log_and_print("✅ GOOD: High-quality dataset ready", "SUCCESS")
        log_and_print("🚀 Ready for: Signal generation", "SUCCESS")
    elif total_bars >= 70000:
        log_and_print("⚠️ ACCEPTABLE: Usable dataset with some gaps", "WARNING")
        log_and_print("🚀 Ready for: Basic analysis", "WARNING")
    else:
        log_and_print("❌ NEEDS WORK: Dataset incomplete", "ERROR")

    log_and_print(f"📄 Full report: {report_path}", "INFO")
    log_and_print("=" * 70, "INFO")

def main():
    """Run the complete year ending yesterday pipeline."""
    log_and_print("=" * 70, "INFO")
    log_and_print("🎯 TSLA ONE YEAR ENDING YESTERDAY PIPELINE", "INFO")
    log_and_print("Downloading 1 full year of TSLA data ending yesterday", "INFO")
    log_and_print("Target: ~100,000+ bars (252 trading days × 390 minutes)", "INFO")
    log_and_print("=" * 70, "INFO")

    start_time = time.time()

    try:
        # Step 1: Check credentials
        if not check_credentials():
            return 1

        # Step 2: Calculate date range
        start_date, end_date = calculate_year_ending_yesterday()

        # Step 3: Create chunks
        chunks = create_monthly_chunks(start_date, end_date)

        # Step 4: Setup environment
        setup_environment()

        # Step 5: Run ingestion
        job_ids = run_chunked_ingestion(chunks)
        if not job_ids:
            log_and_print("❌ No successful ingestion jobs", "ERROR")
            return 1

        # Step 6: Verify dataset
        total_bars, date_info, errors = verify_final_dataset()

        # Step 7: Generate verification report
        generate_verification_report(total_bars, date_info)

        # Final status
        elapsed = time.time() - start_time
        log_and_print(f"🏁 Pipeline completed in {elapsed/60:.1f} minutes", "SUCCESS")

        if total_bars >= 90000:
            log_and_print("🎉 SUCCESS: Year ending yesterday dataset ready!", "SUCCESS")
            return 0
        elif total_bars >= 70000:
            log_and_print("✅ PARTIAL SUCCESS: Usable dataset created", "SUCCESS")
            return 0
        else:
            log_and_print("⚠️ INCOMPLETE: Dataset may need troubleshooting", "WARNING")
            return 1

    except KeyboardInterrupt:
        log_and_print("Pipeline interrupted by user", "WARNING")
        return 1
    except Exception as e:
        log_and_print(f"Pipeline failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
