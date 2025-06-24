#!/usr/bin/env python3
"""
TSLA Full Year Pipeline Script

This script runs a complete data pipeline for TSLA:
1. Ingest 1-minute OHLCV data for the previous year (ending day before today)
2. Handle 30-day ingestion limits by chunking the date range
3. Verify and validate data quality
4. Generate comprehensive summary reports

Usage: python scripts/tsla_full_year_pipeline.py
"""

import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path


def log(message):
    """Log with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def run_command(cmd, description, check_exit_code=True):
    """Run a command and return result."""
    log(f"Starting: {description}")
    log(f"Command: {cmd}")

    start_time = time.time()
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    duration = time.time() - start_time

    if result.returncode == 0:
        log(f"✅ SUCCESS ({duration:.1f}s): {description}")
        if result.stdout.strip():
            print(f"Output: {result.stdout.strip()}")
    else:
        log(f"❌ FAILED ({duration:.1f}s): {description}")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if check_exit_code:
            sys.exit(1)

    return result

def calculate_date_range():
    """Calculate start and end dates for previous year ending day before today."""
    today = date.today()
    end_date = today - timedelta(days=1)  # Day before today
    start_date = date(end_date.year - 1, 1, 1)  # January 1st of previous year

    log(f"Date range calculated: {start_date} to {end_date}")
    return start_date, end_date

def chunk_date_range(start_date, end_date, chunk_days=25):
    """Break date range into chunks to handle 30-day ingestion limits."""
    chunks = []
    current_date = start_date

    while current_date <= end_date:
        chunk_end = min(current_date + timedelta(days=chunk_days), end_date)
        chunks.append((current_date, chunk_end))
        current_date = chunk_end + timedelta(days=1)

    log(f"Created {len(chunks)} date chunks of ~{chunk_days} days each")
    return chunks

def check_prerequisites():
    """Check that MarketPipe is properly installed."""
    log("Checking prerequisites...")

    # Check if marketpipe command is available
    result = run_command("marketpipe --help", "Check MarketPipe installation", check_exit_code=False)
    if result.returncode != 0:
        log("❌ MarketPipe CLI not found. Please install MarketPipe first.")
        sys.exit(1)

    # Check if we're in the right directory
    if not Path("src/marketpipe").exists():
        log("❌ Not in MarketPipe root directory. Please run from MarketPipe root.")
        sys.exit(1)

    log("✅ Prerequisites check passed")

def ingest_chunk(chunk_start, chunk_end, output_dir, provider="alpaca", attempt=1):
    """Ingest data for a single date chunk with retry logic."""
    log(f"Ingesting chunk: {chunk_start} to {chunk_end} (attempt {attempt})")

    ingest_cmd = f"""marketpipe ohlcv ingest --symbols TSLA --start {chunk_start} --end {chunk_end} --provider {provider} --output {output_dir}"""

    result = run_command(ingest_cmd, f"Ingest TSLA {chunk_start} to {chunk_end}", check_exit_code=False)

    if result.returncode != 0:
        if "verification failed" in result.stderr.lower():
            log("📊 Verification detected data boundary issues")

            # Try alternative providers
            if provider == "alpaca" and "polygon" in result.stderr.lower():
                log("💡 Trying Polygon as suggested alternative...")
                return ingest_chunk(chunk_start, chunk_end, output_dir, "polygon", attempt)
            elif provider == "alpaca" and "finnhub" in result.stderr.lower():
                log("💡 Trying Finnhub as suggested alternative...")
                return ingest_chunk(chunk_start, chunk_end, output_dir, "finnhub", attempt)

        if attempt < 3:
            log(f"⚠️ Chunk ingestion failed, retrying with attempt {attempt + 1}")
            time.sleep(5)  # Wait before retry
            return ingest_chunk(chunk_start, chunk_end, output_dir, provider, attempt + 1)
        else:
            log(f"❌ Chunk ingestion failed after {attempt} attempts")
            return False

    return True

def main():
    """Main pipeline execution."""
    log("=== TSLA Full Year Pipeline Starting ===")

    # Step 0: Prerequisites
    check_prerequisites()

    # Step 1: Calculate date range
    start_date, end_date = calculate_date_range()

    # Step 2: Set up output directory
    output_dir = "data/tsla_full_year"
    log(f"Using output directory: {output_dir}")

    # Step 3: Create date chunks to handle 30-day limit
    log("=== STEP 3: PREPARING DATE CHUNKS ===")
    date_chunks = chunk_date_range(start_date, end_date, chunk_days=25)

    # Step 4: Ingest data in chunks
    log("=== STEP 4: CHUNKED INGESTION ===")
    successful_chunks = 0
    failed_chunks = []

    for i, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
        log(f"Processing chunk {i}/{len(date_chunks)}: {chunk_start} to {chunk_end}")

        if ingest_chunk(chunk_start, chunk_end, output_dir):
            successful_chunks += 1
            log(f"✅ Chunk {i} completed successfully")
        else:
            failed_chunks.append((chunk_start, chunk_end))
            log(f"❌ Chunk {i} failed")

    log(f"Ingestion summary: {successful_chunks}/{len(date_chunks)} chunks successful")
    if failed_chunks:
        log(f"Failed chunks: {failed_chunks}")

    # Step 5: Check if we have data before proceeding
    tsla_dir = Path(output_dir) / "symbol=TSLA"
    if not tsla_dir.exists() or not list(tsla_dir.rglob("*.parquet")):
        log("❌ No TSLA data found, cannot continue with pipeline")
        return 1

    # Step 6: Comprehensive data verification
    log("=== STEP 6: DATA VERIFICATION AND ANALYSIS ===")

    verify_cmd = f"""python -c "
import duckdb
import sys
from pathlib import Path
from datetime import datetime, date

try:
    conn = duckdb.connect()
    
    # Basic statistics
    query = '''
    SELECT 
        COUNT(*) as total_bars,
        MIN(DATE(ts_ns::TIMESTAMP)) as first_date,
        MAX(DATE(ts_ns::TIMESTAMP)) as last_date,
        COUNT(DISTINCT DATE(ts_ns::TIMESTAMP)) as trading_days,
        SUM(volume) as total_volume,
        AVG(close) as avg_price,
        MIN(low) as min_price,
        MAX(high) as max_price
    FROM read_parquet('{output_dir}/symbol=TSLA/**/*.parquet')
    '''
    
    result = conn.execute(query).fetchone()
    
    print('\\n=== TSLA DATA SUMMARY ===')
    print(f'Total 1-minute bars: {{result[0]:,}}')
    print(f'Date range: {{result[1]}} to {{result[2]}}')
    print(f'Trading days: {{result[3]}}')
    print(f'Total volume: {{result[4]:,}}')
    print(f'Average price: ${{result[5]:.2f}}')
    print(f'Price range: ${{result[6]:.2f}} - ${{result[7]:.2f}}')
    
    # Expected trading days calculation (rough estimate)
    from datetime import date as dt_date
    first_date = datetime.strptime(str(result[1]), '%Y-%m-%d').date()
    last_date = datetime.strptime(str(result[2]), '%Y-%m-%d').date()
    total_days = (last_date - first_date).days + 1
    expected_trading_days = int(total_days * 5/7 * 0.95)  # Rough estimate accounting for holidays
    
    print(f'Expected trading days (estimate): {{expected_trading_days}}')
    if result[3] < expected_trading_days * 0.8:
        print('⚠️  Significantly fewer trading days than expected - check for data gaps')
    else:
        print('✅ Trading days count looks reasonable')
    
    # Check for data gaps
    gap_query = '''
    WITH daily_counts AS (
        SELECT DATE(ts_ns::TIMESTAMP) as date, COUNT(*) as bars
        FROM read_parquet('{output_dir}/symbol=TSLA/**/*.parquet')
        GROUP BY DATE(ts_ns::TIMESTAMP)
    )
    SELECT date, bars FROM daily_counts WHERE bars < 300 ORDER BY date LIMIT 10
    '''
    
    gaps = conn.execute(gap_query).fetchall()
    if gaps:
        print('\\n=== POTENTIAL DATA GAPS ===')
        print('Days with fewer than 300 bars (may indicate gaps):')
        for date, bars in gaps:
            print(f'  {{date}}: {{bars}} bars')
    else:
        print('\\n✅ No significant data gaps detected')
    
    # Check recent data availability
    recent_query = '''
    SELECT MAX(DATE(ts_ns::TIMESTAMP)) as last_date,
           CURRENT_DATE - MAX(DATE(ts_ns::TIMESTAMP)) as days_behind
    FROM read_parquet('{output_dir}/symbol=TSLA/**/*.parquet')
    '''
    
    recent_result = conn.execute(recent_query).fetchone()
    print(f'\\n=== DATA FRESHNESS ===')
    print(f'Last available date: {{recent_result[0]}}')
    print(f'Days behind current: {{recent_result[1]}}')
    
    if recent_result[1] > 5:
        print('⚠️  Data is more than 5 days old - consider using a different provider')
    else:
        print('✅ Data is reasonably fresh')
    
    # Monthly breakdown
    monthly_query = '''
    SELECT 
        strftime('%Y-%m', ts_ns::TIMESTAMP) as month,
        COUNT(*) as bars,
        COUNT(DISTINCT DATE(ts_ns::TIMESTAMP)) as trading_days,
        SUM(volume) as volume,
        AVG(close) as avg_price
    FROM read_parquet('{output_dir}/symbol=TSLA/**/*.parquet')
    GROUP BY strftime('%Y-%m', ts_ns::TIMESTAMP)
    ORDER BY month
    '''
    
    print('\\n=== MONTHLY BREAKDOWN ===')
    monthly_results = conn.execute(monthly_query).fetchall()
    for row in monthly_results:
        print(f'{{row[0]}}: {{row[1]:,}} bars, {{row[2]}} days, ${{row[4]:.2f}} avg, {{row[3]:,}} vol')
        
except Exception as e:
    print(f'Error generating summary: {{e}}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"
"""

    run_command(verify_cmd, "Comprehensive data verification", check_exit_code=False)

    # Step 7: File structure verification
    log("=== STEP 7: FILE STRUCTURE VERIFICATION ===")

    if Path(output_dir).exists():
        parquet_files = list(Path(output_dir).rglob("*.parquet"))
        log(f"Found {len(parquet_files)} Parquet files")

        # Show sample files
        for i, file in enumerate(parquet_files[:5]):
            log(f"  {i+1}. {file.relative_to(Path(output_dir))}")

        if len(parquet_files) > 5:
            log(f"  ... and {len(parquet_files) - 5} more files")

        # Check file sizes
        total_size = sum(f.stat().st_size for f in parquet_files)
        log(f"Total data size: {total_size / (1024*1024):.1f} MB")
    else:
        log("❌ Output directory not found!")

    # Step 8: Sample data inspection
    log("=== STEP 8: SAMPLE DATA INSPECTION ===")

    sample_cmd = f"""python -c "
import duckdb
import sys

try:
    conn = duckdb.connect()
    
    # Sample data from first and last days
    sample_query = '''
    SELECT DATE(ts_ns::TIMESTAMP) as date, 
           COUNT(*) as bars_count,
           MIN(open) as day_open,
           MAX(high) as day_high,
           MIN(low) as day_low,
           LAST(close ORDER BY ts_ns) as day_close,
           SUM(volume) as day_volume
    FROM read_parquet('{output_dir}/symbol=TSLA/**/*.parquet')
    GROUP BY DATE(ts_ns::TIMESTAMP)
    ORDER BY date
    LIMIT 5
    '''
    
    print('\\n=== FIRST 5 TRADING DAYS ===')
    results = conn.execute(sample_query).fetchall()
    for row in results:
        print(f'{{row[0]}}: {{row[1]}} bars, O={{row[2]:.2f}} H={{row[3]:.2f}} L={{row[4]:.2f}} C={{row[5]:.2f}} V={{row[6]:,}}')
    
    # Last 5 days
    last_query = '''
    SELECT DATE(ts_ns::TIMESTAMP) as date, 
           COUNT(*) as bars_count,
           MIN(open) as day_open,
           MAX(high) as day_high,
           MIN(low) as day_low,
           LAST(close ORDER BY ts_ns) as day_close,
           SUM(volume) as day_volume
    FROM read_parquet('{output_dir}/symbol=TSLA/**/*.parquet')
    GROUP BY DATE(ts_ns::TIMESTAMP)
    ORDER BY date DESC
    LIMIT 5
    '''
    
    print('\\n=== LAST 5 TRADING DAYS ===')
    results = conn.execute(last_query).fetchall()
    for row in results:
        print(f'{{row[0]}}: {{row[1]}} bars, O={{row[2]:.2f}} H={{row[3]:.2f}} L={{row[4]:.2f}} C={{row[5]:.2f}} V={{row[6]:,}}')
    
    # Check for any obvious data quality issues
    quality_query = '''
    SELECT 
        COUNT(*) as total_bars,
        COUNT(*) FILTER (WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0) as negative_prices,
        COUNT(*) FILTER (WHERE high < low) as invalid_high_low,
        COUNT(*) FILTER (WHERE high < open OR high < close) as invalid_high,
        COUNT(*) FILTER (WHERE low > open OR low > close) as invalid_low,
        COUNT(*) FILTER (WHERE volume < 0) as negative_volume
    FROM read_parquet('{output_dir}/symbol=TSLA/**/*.parquet')
    '''
    
    print('\\n=== DATA QUALITY CHECKS ===')
    quality_result = conn.execute(quality_query).fetchone()
    print(f'Total bars: {{quality_result[0]:,}}')
    print(f'Negative prices: {{quality_result[1]}}')
    print(f'Invalid high/low: {{quality_result[2]}}')
    print(f'Invalid high: {{quality_result[3]}}')
    print(f'Invalid low: {{quality_result[4]}}')
    print(f'Negative volume: {{quality_result[5]}}')
    
    if sum(quality_result[1:]) == 0:
        print('✅ All data quality checks passed')
    else:
        print('⚠️  Some data quality issues detected')
        
except Exception as e:
    print(f'Error inspecting sample data: {{e}}')
    import traceback
    traceback.print_exc()
"
"""

    run_command(sample_cmd, "Sample data inspection", check_exit_code=False)

    # Final steps
    log("=== PIPELINE COMPLETE ===")
    log(f"TSLA data for {start_date} to {end_date} has been processed")
    log(f"Data location: {output_dir}")
    log(f"Successful chunks: {successful_chunks}/{len(date_chunks)}")

    if failed_chunks:
        log(f"⚠️ Failed chunks: {len(failed_chunks)}")
        for chunk_start, chunk_end in failed_chunks:
            log(f"  - {chunk_start} to {chunk_end}")

    log("Next steps:")
    log("1. Review the data summary and quality checks above")
    log("2. Check for any error messages or warnings")
    log("3. Consider re-running failed chunks if any")
    log("4. Use 'marketpipe query' for custom analysis")
    log("5. Run aggregation pipeline if you need multi-timeframe data")

    return 0 if len(failed_chunks) == 0 else 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        log("\n❌ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        log(f"❌ Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
