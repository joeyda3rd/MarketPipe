#!/usr/bin/env python3
"""
Create CSV validation reports for OHLCV data.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
from pathlib import Path
import csv

def create_csv_validation_report(parquet_path, output_dir="data/validation_reports"):
    """Generate a CSV validation report for OHLCV data."""
    
    # Load the data
    df = pd.read_parquet(parquet_path)
    
    # Extract metadata
    symbol = df["symbol"].iloc[0]
    start_time = datetime.fromtimestamp(df["ts_ns"].min() / 1e9)
    end_time = datetime.fromtimestamp(df["ts_ns"].max() / 1e9)
    date_str = start_time.strftime("%Y-%m-%d")
    
    # Run validation checks
    missing = df.isnull().sum()
    
    # OHLC consistency checks
    ohlc_valid = ((df['high'] >= df['open']) & 
                  (df['high'] >= df['close']) & 
                  (df['high'] >= df['low']) & 
                  (df['low'] <= df['open']) & 
                  (df['low'] <= df['close']))
    
    valid_ohlc = ohlc_valid.sum()
    invalid_ohlc = len(df) - valid_ohlc
    
    # Volume checks
    zero_volume = (df['volume'] == 0).sum()
    negative_volume = (df['volume'] < 0).sum()
    
    # Time series checks
    df_sorted = df.sort_values('ts_ns')
    time_gaps = df_sorted['ts_ns'].diff().iloc[1:]
    expected_gap = 60 * 1e9  # 60 seconds in nanoseconds
    regular_gaps = (time_gaps == expected_gap).sum()
    irregular_gaps = len(time_gaps) - regular_gaps
    
    # Create validation report data
    validation_results = []
    
    # Data overview
    validation_results.append({
        'check_category': 'Data Overview',
        'check_name': 'Total Records',
        'check_type': 'INFO',
        'expected_value': 'N/A',
        'actual_value': len(df),
        'passed': True,
        'error_count': 0,
        'details': f'Symbol: {symbol}, Date: {date_str}'
    })
    
    # Missing value checks
    for col in df.columns:
        validation_results.append({
            'check_category': 'Missing Values',
            'check_name': f'{col}_not_null',
            'check_type': 'REQUIRED',
            'expected_value': 0,
            'actual_value': missing[col],
            'passed': missing[col] == 0,
            'error_count': missing[col],
            'details': f'Missing values in {col} column'
        })
    
    # OHLC consistency
    validation_results.append({
        'check_category': 'OHLC Consistency',
        'check_name': 'ohlc_relationships_valid',
        'check_type': 'BUSINESS_RULE',
        'expected_value': len(df),
        'actual_value': valid_ohlc,
        'passed': invalid_ohlc == 0,
        'error_count': invalid_ohlc,
        'details': 'High >= Open,Close,Low and Low <= Open,Close'
    })
    
    # Volume checks
    validation_results.append({
        'check_category': 'Volume Validation',
        'check_name': 'volume_non_negative',
        'check_type': 'BUSINESS_RULE',
        'expected_value': 0,
        'actual_value': negative_volume,
        'passed': negative_volume == 0,
        'error_count': negative_volume,
        'details': 'Volume must be >= 0'
    })
    
    validation_results.append({
        'check_category': 'Volume Validation',
        'check_name': 'volume_zero_count',
        'check_type': 'DATA_QUALITY',
        'expected_value': 'Low',
        'actual_value': zero_volume,
        'passed': zero_volume < len(df) * 0.1,  # Less than 10% zero volume
        'error_count': zero_volume,
        'details': f'{zero_volume/len(df)*100:.1f}% zero volume bars'
    })
    
    # Time series checks
    validation_results.append({
        'check_category': 'Time Series',
        'check_name': 'regular_intervals',
        'check_type': 'DATA_QUALITY',
        'expected_value': len(time_gaps),
        'actual_value': regular_gaps,
        'passed': irregular_gaps < len(time_gaps) * 0.05,  # Less than 5% irregular
        'error_count': irregular_gaps,
        'details': f'{irregular_gaps/len(time_gaps)*100:.1f}% irregular intervals'
    })
    
    # Price validation
    price_cols = ['open', 'high', 'low', 'close']
    for col in price_cols:
        positive_prices = (df[col] > 0).sum()
        negative_prices = len(df) - positive_prices
        validation_results.append({
            'check_category': 'Price Validation',
            'check_name': f'{col}_price_positive',
            'check_type': 'BUSINESS_RULE',
            'expected_value': len(df),
            'actual_value': positive_prices,
            'passed': negative_prices == 0,
            'error_count': negative_prices,
            'details': f'All {col} prices must be positive'
        })
    
    # Create CSV report
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    report_filename = f"validation_report_{symbol}_{date_str}_{datetime.now().strftime('%H%M%S')}.csv"
    report_path = Path(output_dir) / report_filename
    
    # Write CSV
    with open(report_path, 'w', newline='') as csvfile:
        fieldnames = ['check_category', 'check_name', 'check_type', 'expected_value', 
                     'actual_value', 'passed', 'error_count', 'details']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for result in validation_results:
            writer.writerow(result)
    
    # Print summary
    total_checks = len(validation_results)
    passed_checks = sum(1 for r in validation_results if r['passed'])
    failed_checks = total_checks - passed_checks
    total_errors = sum(r['error_count'] for r in validation_results)
    
    print(f"📄 Validation Report Created: {report_path}")
    print(f"✅ Passed: {passed_checks}/{total_checks} checks")
    if failed_checks > 0:
        print(f"❌ Failed: {failed_checks}/{total_checks} checks")
        print(f"⚠️  Total errors: {total_errors}")
    print(f"📊 Data: {len(df)} records for {symbol} on {date_str}")
    
    return str(report_path)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python create_csv_validation_report.py <parquet_file>")
        sys.exit(1)
    
    parquet_file = sys.argv[1]
    
    if not Path(parquet_file).exists():
        print(f"❌ Error: File not found: {parquet_file}")
        sys.exit(1)
    
    try:
        report_path = create_csv_validation_report(parquet_file)
        print(f"\n💡 View report with: cat {report_path}")
    except Exception as e:
        print(f"❌ Error generating validation report: {e}")
        sys.exit(1) 