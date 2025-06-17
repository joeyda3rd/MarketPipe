#!/usr/bin/env python3
"""
Quick script to inspect the actual data structure in parquet files
"""
import pyarrow.parquet as pq
from pathlib import Path
import pandas as pd

def main():
    # Check a sample parquet file to see the actual data structure
    parquet_files = list(Path('data/raw').rglob('*.parquet'))
    if not parquet_files:
        print("❌ No parquet files found")
        return
    
    print(f"📊 Found {len(parquet_files)} parquet files")
    
    # Key discovery: All data is in date=1970-01-01 and date=2020-07-27!
    print("\n🚨 TIMESTAMP ISSUE DETECTED:")
    print("📅 Files are partitioned as date=1970-01-01 and date=2020-07-27")
    print("📅 This should be 2025 dates for recent ingestion!")
    
    # Examine first few files individually using ParquetFile
    for i, file in enumerate(parquet_files[:3]):
        print(f'\n📄 File {i+1}: {file}')
        
        try:
            # Use ParquetFile to avoid schema merge issues
            parquet_file = pq.ParquetFile(str(file))
            schema = parquet_file.schema
            print(f'📊 Schema: {schema}')
            
            # Read table
            table = parquet_file.read()
            df = table.to_pandas()
            
            print(f'📊 Shape: {df.shape}')
            print(f'📊 Columns: {df.columns.tolist()}')
            
            if len(df) > 0:
                print(f'📊 Sample data:')
                # Show first row but limit width
                sample = df.head(1)
                for col in sample.columns:
                    val = sample[col].iloc[0]
                    print(f'  {col}: {val}')
                    
                if 'date' in df.columns:
                    unique_dates = df["date"].unique()
                    print(f'📊 Unique dates in file: {unique_dates}')
                    
                if 'ts_ns' in df.columns:
                    df['datetime'] = pd.to_datetime(df['ts_ns'], unit='ns')
                    ts_sample = df["datetime"].iloc[0]
                    print(f'📊 Timestamp sample: {ts_sample}')
                    ts_min = df["datetime"].min()
                    ts_max = df["datetime"].max()
                    print(f'📊 Timestamp range: {ts_min} to {ts_max}')
                    
        except Exception as e:
            print(f"❌ Error reading file: {e}")
            
    # Directory structure shows the issue clearly
    print(f'\n📊 Directory structure analysis:')
    raw_path = Path('data/raw')
    date_dirs = {}
    for path in raw_path.rglob('date=*'):
        if path.is_dir():
            date_part = path.name
            if date_part not in date_dirs:
                date_dirs[date_part] = 0
            date_dirs[date_part] += len(list(path.glob('*.parquet')))
    
    print("📅 Date partition summary:")
    for date_dir, file_count in date_dirs.items():
        print(f"  {date_dir}: {file_count} files")
    
    print(f'\n🔍 ROOT CAUSE ANALYSIS:')
    print(f'❌ Data ingestion created wrong dates (1970-01-01, 2020-07-27)')
    print(f'✅ Should be 2025 dates based on ingestion date range')
    print(f'💡 This suggests timestamp conversion issue in ingestion process')

if __name__ == "__main__":
    main() 