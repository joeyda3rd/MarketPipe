#!/usr/bin/env python3
"""
Manual validation report generator for OHLCV data.
"""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


def generate_validation_report(parquet_path):
    """Generate a comprehensive validation report for OHLCV data."""

    # Load the data
    df = pd.read_parquet(parquet_path)

    print('=== VALIDATION REPORT FOR AAPL 2020-07-30 ===\n')

    # Basic data quality checks
    print('📊 Data Overview:')
    print(f'  Total Records: {len(df)}')
    print('  Date: 2020-07-30')
    print(f'  Symbol: {df["symbol"].iloc[0]}')
    print(f'  Time Range: {datetime.fromtimestamp(df["ts_ns"].min() / 1e9)} to {datetime.fromtimestamp(df["ts_ns"].max() / 1e9)}')
    print()

    # Check for missing values
    print('🔍 Missing Value Analysis:')
    missing = df.isnull().sum()
    for col in missing.index:
        if missing[col] > 0:
            print(f'  ❌ {col}: {missing[col]} missing values')
        else:
            print(f'  ✅ {col}: No missing values')
    print()

    # OHLC consistency checks
    print('💰 OHLC Price Consistency:')
    ohlc_valid = ((df['high'] >= df['open']) &
                  (df['high'] >= df['close']) &
                  (df['high'] >= df['low']) &
                  (df['low'] <= df['open']) &
                  (df['low'] <= df['close']))

    valid_ohlc = ohlc_valid.sum()
    invalid_ohlc = len(df) - valid_ohlc
    print(f'  ✅ Valid OHLC relationships: {valid_ohlc} ({valid_ohlc/len(df)*100:.1f}%)')
    if invalid_ohlc > 0:
        print(f'  ❌ Invalid OHLC relationships: {invalid_ohlc} ({invalid_ohlc/len(df)*100:.1f}%)')

    # Volume checks
    print('\n📈 Volume Analysis:')
    zero_volume = (df['volume'] == 0).sum()
    negative_volume = (df['volume'] < 0).sum()
    print(f'  Total volume: {df["volume"].sum():,}')
    print(f'  Average volume per bar: {df["volume"].mean():.0f}')
    print(f'  Zero volume bars: {zero_volume} ({zero_volume/len(df)*100:.1f}%)')
    if negative_volume > 0:
        print(f'  ❌ Negative volume bars: {negative_volume}')
    else:
        print('  ✅ No negative volume found')

    # Price range analysis
    print('\n💵 Price Analysis:')
    print(f'  Price range: ${df["low"].min():.2f} - ${df["high"].max():.2f}')
    print(f'  Opening price: ${df["open"].iloc[0]:.2f}')
    print(f'  Closing price: ${df["close"].iloc[-1]:.2f}')
    print(f'  Price change: ${df["close"].iloc[-1] - df["open"].iloc[0]:.2f}')

    # Time series checks
    print('\n⏰ Time Series Analysis:')
    df_sorted = df.sort_values('ts_ns')
    time_gaps = df_sorted['ts_ns'].diff().iloc[1:]
    expected_gap = 60 * 1e9  # 60 seconds in nanoseconds
    regular_gaps = (time_gaps == expected_gap).sum()
    irregular_gaps = len(time_gaps) - regular_gaps
    print(f'  ✅ Regular 1-minute intervals: {regular_gaps} ({regular_gaps/len(time_gaps)*100:.1f}%)')
    if irregular_gaps > 0:
        print(f'  ⚠️  Irregular intervals: {irregular_gaps} ({irregular_gaps/len(time_gaps)*100:.1f}%)')

    # Data distribution analysis
    print('\n📊 Statistical Analysis:')
    print(f'  Mean price: ${df[["open", "high", "low", "close"]].mean().mean():.2f}')
    print(f'  Price volatility (std): ${df[["open", "high", "low", "close"]].std().mean():.2f}')
    print(f'  Max single-bar price move: ${(df["high"] - df["low"]).max():.2f}')

    # Summary
    print('\n=== VALIDATION SUMMARY ===')
    total_issues = invalid_ohlc + negative_volume + irregular_gaps
    if total_issues == 0:
        print('✅ All validation checks PASSED - Data quality is excellent!')
        print('✅ No missing values detected')
        print('✅ All OHLC relationships are consistent')
        print('✅ All volume values are valid')
        print('✅ Time series is properly formatted')
    else:
        print(f'⚠️  Found {total_issues} potential data quality issues')
        if invalid_ohlc > 0:
            print(f'   - {invalid_ohlc} OHLC consistency violations')
        if negative_volume > 0:
            print(f'   - {negative_volume} negative volume entries')
        if irregular_gaps > 0:
            print(f'   - {irregular_gaps} time gaps that are not 1-minute intervals')

    print('\n💡 Recommendation:')
    if total_issues == 0:
        print('   Data is ready for production use and aggregation.')
    else:
        print('   Review and clean data before using in production.')

    return {
        'total_records': len(df),
        'total_issues': total_issues,
        'ohlc_issues': invalid_ohlc,
        'volume_issues': negative_volume,
        'time_issues': irregular_gaps,
        'validation_passed': total_issues == 0
    }

if __name__ == '__main__':
    parquet_file = 'data/raw/frame=1m/symbol=AAPL/date=2020-07-30/b6132fb0.parquet'
    if len(sys.argv) > 1:
        parquet_file = sys.argv[1]

    if not Path(parquet_file).exists():
        print(f"❌ Error: File not found: {parquet_file}")
        sys.exit(1)

    try:
        results = generate_validation_report(parquet_file)
        sys.exit(0 if results['validation_passed'] else 1)
    except Exception as e:
        print(f"❌ Error generating validation report: {e}")
        sys.exit(1)
