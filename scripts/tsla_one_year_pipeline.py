#!/usr/bin/env python3
"""
TSLA One Year Data Pipeline Script

This script runs the complete MarketPipe pipeline to get exactly 1 year's worth 
of TSLA 1-minute OHLCV data from Alpaca, ending on the last full trading day before today.

The pipeline includes:
1. Date calculation (1 year ending last trading day)
2. Data ingestion from Alpaca
3. Data validation
4. Data aggregation (5m, 15m, 1h, 1d)
5. Summary report

Usage:
    python scripts/tsla_one_year_pipeline.py
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

import pandas as pd

# Add src to path so we can import MarketPipe modules
script_dir = Path(__file__).parent
src_dir = script_dir.parent / "src"
sys.path.insert(0, str(src_dir))

def log_and_print(message: str, level: str = "INFO"):
    """Print and log message with timestamp and level."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    emoji_map = {
        "INFO": "ℹ️",
        "SUCCESS": "✅",
        "ERROR": "❌",
        "WARNING": "⚠️",
        "PROCESS": "🔧",
        "DATA": "📊",
        "CONFIG": "⚙️",
        "CLEAN": "🧹"
    }
    emoji = emoji_map.get(level, "📝")
    print(f"[{timestamp}] {emoji} {message}")

def calculate_trading_date_range() -> Tuple[str, str]:
    """Calculate 1 year date range ending on last full trading day before today."""
    log_and_print("Calculating trading date range...", "PROCESS")

    today = datetime.now().date()

    # Find last full trading day (weekday before today)
    end_date = today - timedelta(days=1)  # Yesterday

    # If yesterday was weekend, go back to Friday
    while end_date.weekday() >= 5:  # Saturday=5, Sunday=6
        end_date -= timedelta(days=1)

    # Go back exactly 1 year (252 trading days ≈ 365 calendar days)
    # Use 365 days to be safe, will cover more than 1 year of trading days
    start_date = end_date - timedelta(days=365)

    # Make sure start is a weekday too
    while start_date.weekday() >= 5:
        start_date -= timedelta(days=1)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    log_and_print("Date range calculated:", "SUCCESS")
    log_and_print(f"  Start: {start_str} ({start_date.strftime('%A')})")
    log_and_print(f"  End: {end_str} ({end_date.strftime('%A')})")
    log_and_print(f"  Total days: {(end_date - start_date).days}")
    log_and_print(f"  Est. trading days: ~{(end_date - start_date).days * 5 // 7}")

    return start_str, end_str

def create_temp_config(start_date: str, end_date: str) -> Path:
    """Create temporary configuration file for TSLA ingestion."""
    log_and_print("Creating temporary configuration...", "CONFIG")

    config_content = f"""# Temporary config for TSLA 1-year pipeline
alpaca:
  # Credentials loaded from environment variables
  key: # Will be loaded from ALPACA_KEY in .env file
  secret: # Will be loaded from ALPACA_SECRET in .env file
  base_url: https://data.alpaca.markets/v2
  rate_limit_per_min: 200
  feed: iex  # Use "iex" for free tier, "sip" for paid subscription

symbols:
  - TSLA

start: "{start_date}"
end: "{end_date}"
output_path: "./data"
compression: snappy
workers: 4

metrics:
  enabled: true
  port: 8000
"""

    # Create temporary config file
    temp_config = Path("config/tsla_one_year_temp.yaml")
    temp_config.parent.mkdir(exist_ok=True)

    with open(temp_config, 'w') as f:
        f.write(config_content)

    log_and_print(f"Temporary config created: {temp_config}", "SUCCESS")
    return temp_config

def run_command(cmd: list, description: str, timeout: int = 300) -> Tuple[bool, str, str]:
    """Run command and return success, stdout, stderr."""
    log_and_print(f"{description}...", "PROCESS")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=script_dir.parent  # Run from project root
        )

        success = result.returncode == 0
        if success:
            log_and_print(f"{description} completed successfully", "SUCCESS")
        else:
            log_and_print(f"{description} failed with code {result.returncode}", "ERROR")
            if result.stderr:
                log_and_print(f"Error output: {result.stderr[:500]}...")

        return success, result.stdout, result.stderr

    except subprocess.TimeoutExpired:
        log_and_print(f"{description} timed out after {timeout}s", "ERROR")
        return False, "", "Command timed out"
    except Exception as e:
        log_and_print(f"{description} failed with exception: {e}", "ERROR")
        return False, "", str(e)

def check_credentials():
    """Check if Alpaca credentials are available."""
    log_and_print("Checking Alpaca credentials...", "PROCESS")

    alpaca_key = os.getenv("ALPACA_KEY")
    alpaca_secret = os.getenv("ALPACA_SECRET")

    if not alpaca_key or not alpaca_secret:
        log_and_print("Alpaca credentials not found in environment variables", "ERROR")
        log_and_print("Please set ALPACA_KEY and ALPACA_SECRET environment variables", "ERROR")
        log_and_print("You can also create a .env file with these variables", "INFO")
        return False

    log_and_print("Alpaca credentials found", "SUCCESS")
    return True

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
    log_and_print("Clearing any existing job conflicts...", "CLEAN")

    try:
        # Run the clear conflicts script
        result = subprocess.run(
            ["python", "scripts/clear_all_job_conflicts.py"],
            capture_output=True,
            text=True,
            cwd=script_dir.parent
        )

        if result.returncode == 0:
            log_and_print("Job conflicts cleared successfully", "SUCCESS")
        else:
            log_and_print("Warning: Could not clear job conflicts", "WARNING")

    except Exception as e:
        log_and_print(f"Warning: Error clearing job conflicts: {e}", "WARNING")

def create_date_chunks(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    """Break date range into 30-day chunks due to MarketPipe limitation."""
    from datetime import datetime, timedelta

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    chunks = []
    current = start

    while current < end:
        chunk_end = min(current + timedelta(days=30), end)
        chunks.append((
            current.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d")
        ))
        current = chunk_end + timedelta(days=1)

    return chunks

def run_ingestion(start_date: str, end_date: str) -> Optional[str]:
    """Run data ingestion in 30-day chunks and return final job ID."""
    log_and_print("Starting TSLA data ingestion from Alpaca...", "PROCESS")

    # Break into 30-day chunks due to MarketPipe limitation
    date_chunks = create_date_chunks(start_date, end_date)
    log_and_print(f"Splitting into {len(date_chunks)} chunks of ~30 days each", "INFO")

    job_ids = []
    total_bars = 0

    for i, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
        log_and_print(f"Processing chunk {i}/{len(date_chunks)}: {chunk_start} to {chunk_end}", "PROCESS")

        success, stdout, stderr = run_command(
            [
                "python", "-m", "marketpipe", "ohlcv", "ingest",
                "--symbols", "TSLA",
                "--start", chunk_start,
                "--end", chunk_end,
                "--provider", "alpaca",
                "--feed-type", "iex",
                "--output", "data/raw",
                "--workers", "4"
            ],
            f"TSLA ingestion chunk {i}/{len(date_chunks)}",
            timeout=600  # 10 minutes per chunk
        )

        if not success:
            # Check for specific error types
            if "Job scheduling conflicts" in stderr:
                log_and_print("Job scheduling conflict detected - this should have been cleared", "ERROR")
                log_and_print("Try running: python scripts/clear_all_job_conflicts.py", "INFO")
            elif "credentials" in stderr.lower() or "authentication" in stderr.lower():
                log_and_print("Authentication issue - check your Alpaca credentials", "ERROR")
                log_and_print("Ensure ALPACA_KEY and ALPACA_SECRET are set correctly", "INFO")
            elif "cannot span more than 30 days" in stderr:
                log_and_print(f"Date range too large for chunk: {chunk_start} to {chunk_end}", "ERROR")
                log_and_print("This chunk will be skipped", "WARNING")
                continue
            else:
                log_and_print(f"Chunk {i} failed with error: {stderr[:500]}...", "ERROR")
                log_and_print("Continuing with remaining chunks...", "WARNING")
                continue

        # Extract job ID and bar count from output
        chunk_job_id = None
        chunk_bars = 0

        for line in stdout.split('\n'):
            if "Job ID:" in line:
                chunk_job_id = line.split("Job ID:")[-1].strip()
            elif "Total bars:" in line:
                try:
                    chunk_bars = int(line.split("Total bars:")[-1].strip())
                    total_bars += chunk_bars
                except:
                    pass

        if chunk_job_id:
            job_ids.append(chunk_job_id)
            log_and_print(f"Chunk {i} completed - Job ID: {chunk_job_id}, Bars: {chunk_bars}", "SUCCESS")
        else:
            log_and_print(f"Chunk {i} completed but could not determine Job ID", "WARNING")

    if job_ids:
        final_job_id = job_ids[-1]  # Use the last job ID
        log_and_print("All ingestion completed!", "SUCCESS")
        log_and_print(f"Total jobs: {len(job_ids)}, Total bars: {total_bars}", "SUCCESS")
        log_and_print(f"Final Job ID: {final_job_id}", "SUCCESS")
        return final_job_id
    else:
        log_and_print("No successful ingestion jobs completed", "ERROR")
        return None

def run_validation(job_id: str) -> bool:
    """Run data validation."""
    log_and_print("Running data validation...", "PROCESS")

    success, stdout, stderr = run_command(
        ["python", "-m", "marketpipe", "validate-ohlcv", "--job-id", job_id],
        "Data validation"
    )

    if success:
        # Check validation reports
        reports_dir = Path("data/validation_reports")
        if reports_dir.exists():
            reports = list(reports_dir.glob("*.csv"))
            log_and_print(f"Validation completed - {len(reports)} reports generated", "SUCCESS")

            # Check for any errors in reports
            total_errors = 0
            for report in reports:
                try:
                    df = pd.read_csv(report)
                    total_errors += len(df)
                except Exception:
                    pass

            if total_errors > 0:
                log_and_print(f"Validation found {total_errors} data quality issues", "WARNING")
            else:
                log_and_print("Validation passed - no data quality issues found", "SUCCESS")
        else:
            log_and_print("Validation completed but no reports found", "WARNING")

    return success

def run_aggregation(job_id: str) -> bool:
    """Run data aggregation."""
    log_and_print("Running data aggregation (1m -> 5m, 15m, 1h, 1d)...", "PROCESS")

    success, stdout, stderr = run_command(
        ["python", "-m", "marketpipe", "aggregate-ohlcv", job_id],
        "Data aggregation"
    )

    if success:
        # Check aggregated data
        agg_dir = Path("data/aggregated")
        if agg_dir.exists():
            agg_files = list(agg_dir.rglob("*.parquet"))
            log_and_print(f"Aggregation completed - {len(agg_files)} aggregated files created", "SUCCESS")

            # Show breakdown by timeframe
            timeframes = {}
            for agg_file in agg_files:
                # Extract timeframe from path (e.g., 5m, 15m, 1h, 1d)
                for tf in ["5m", "15m", "1h", "1d"]:
                    if tf in str(agg_file):
                        timeframes[tf] = timeframes.get(tf, 0) + 1
                        break

            for tf, count in timeframes.items():
                log_and_print(f"  {tf}: {count} files")
        else:
            log_and_print("Aggregation completed but no aggregated files found", "WARNING")

    return success

def generate_summary_report():
    """Generate summary report of the pipeline results."""
    log_and_print("Generating summary report...", "DATA")

    summary = {
        "timestamp": datetime.now().isoformat(),
        "symbol": "TSLA",
        "pipeline": "1-Year OHLCV Data",
        "source": "Alpaca Markets",
        "data_summary": {}
    }

    try:
        # Query raw data summary
        success, stdout, stderr = run_command(
            ["python", "-m", "marketpipe", "query",
             "SELECT COUNT(*) as total_bars, MIN(timestamp) as first_bar, MAX(timestamp) as last_bar FROM 'data/raw/**/*.parquet' WHERE symbol = 'TSLA'"],
            "Raw data summary query"
        )

        if success and stdout.strip():
            lines = stdout.strip().split('\n')
            if len(lines) > 1:  # Skip header
                data_line = lines[-1]
                parts = data_line.split()
                if len(parts) >= 3:
                    summary["data_summary"]["raw_bars"] = parts[0]
                    summary["data_summary"]["first_timestamp"] = parts[1] if len(parts) > 1 else "unknown"
                    summary["data_summary"]["last_timestamp"] = parts[2] if len(parts) > 2 else "unknown"

    except Exception as e:
        log_and_print(f"Could not query raw data summary: {e}", "WARNING")

    # Check aggregated data
    agg_dir = Path("data/aggregated")
    if agg_dir.exists():
        summary["data_summary"]["aggregated_files"] = len(list(agg_dir.rglob("*.parquet")))

    # Check validation reports
    reports_dir = Path("data/validation_reports")
    if reports_dir.exists():
        summary["data_summary"]["validation_reports"] = len(list(reports_dir.glob("*.csv")))

    # Save summary to file
    summary_file = Path("data/tsla_one_year_summary.json")
    summary_file.parent.mkdir(exist_ok=True)

    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)

    log_and_print(f"Summary report saved to: {summary_file}", "SUCCESS")

    # Print summary
    log_and_print("=" * 60, "DATA")
    log_and_print("TSLA ONE YEAR PIPELINE SUMMARY", "DATA")
    log_and_print("=" * 60, "DATA")
    log_and_print(f"Symbol: {summary['symbol']}")
    log_and_print(f"Source: {summary['source']}")
    log_and_print(f"Completed: {summary['timestamp']}")

    if summary["data_summary"]:
        log_and_print("Data Summary:")
        for key, value in summary["data_summary"].items():
            log_and_print(f"  {key.replace('_', ' ').title()}: {value}")

    log_and_print("=" * 60, "DATA")

def cleanup_temp_files(temp_config: Path):
    """Clean up temporary configuration file."""
    try:
        if temp_config.exists():
            temp_config.unlink()
            log_and_print("Temporary configuration cleaned up", "CLEAN")
    except Exception as e:
        log_and_print(f"Could not clean up temp config: {e}", "WARNING")

def main():
    """Run the complete TSLA one-year pipeline."""
    log_and_print("=" * 60, "INFO")
    log_and_print("TSLA ONE YEAR OHLCV DATA PIPELINE", "INFO")
    log_and_print("=" * 60, "INFO")
    log_and_print("This will download ~250 trading days of 1-minute TSLA data from Alpaca")
    log_and_print("Estimated runtime: 15-30 minutes depending on connection speed")
    log_and_print("=" * 60, "INFO")

    start_time = time.time()

    try:
        # Step 1: Check prerequisites
        if not check_credentials():
            return 1

        # Step 2: Calculate date range
        start_date, end_date = calculate_trading_date_range()

        # Step 3: Clean previous data
        clean_previous_data()

        # Step 4: Clear job conflicts
        clear_job_conflicts()

        # Step 5: Run ingestion using direct CLI parameters
        job_id = run_ingestion(start_date, end_date)
        if not job_id:
            log_and_print("Pipeline failed at ingestion step", "ERROR")
            return 1

        # Step 6: Run validation
        if not run_validation(job_id):
            log_and_print("Pipeline failed at validation step", "ERROR")
            return 1

        # Step 7: Run aggregation
        if not run_aggregation(job_id):
            log_and_print("Pipeline failed at aggregation step", "ERROR")
            return 1

        # Step 8: Generate summary
        generate_summary_report()

        # Success!
        elapsed = time.time() - start_time
        log_and_print(f"🎉 TSLA One Year Pipeline completed successfully in {elapsed/60:.1f} minutes!", "SUCCESS")
        log_and_print("Data is ready for analysis and trading signal generation", "SUCCESS")

        return 0

    except KeyboardInterrupt:
        log_and_print("Pipeline interrupted by user", "WARNING")
        return 1
    except Exception as e:
        log_and_print(f"Pipeline failed with unexpected error: {e}", "ERROR")
        return 1

if __name__ == "__main__":
    sys.exit(main())
