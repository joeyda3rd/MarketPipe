#!/usr/bin/env python3
"""
Populate aggregation directories by converting 1-minute data to multiple timeframes.

This script reads 1-minute data from data/raw/frame=1m/ and creates aggregated
timeframes in data/agg/frame={5m,15m,1h,1d}/ with proper partitioning.
"""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_aggregated_data():
    """Create aggregated data files in proper directory structure."""
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    # Check if we have raw data
    raw_path = Path("data/raw/frame=1m")
    if not raw_path.exists():
        logger.error(f"Raw data path does not exist: {raw_path}")
        return
    
    logger.info(f"Found raw data in {raw_path}")
    
    # Create aggregation root
    agg_root = Path("data/agg")
    agg_root.mkdir(exist_ok=True)
    
    # Define timeframe aggregations
    timeframes = {
        "5m": "5 minutes",
        "15m": "15 minutes", 
        "1h": "1 hour",
        "1d": "1 day"
    }
    
    for frame, interval in timeframes.items():
        logger.info(f"\n🔧 Creating {frame} aggregation...")
        
        # Create frame directory
        frame_dir = agg_root / f"frame={frame}"
        frame_dir.mkdir(exist_ok=True)
        
        try:
            # Query to aggregate data
            if frame == "1d":
                # For daily, use date truncation
                agg_sql = f"""
                SELECT 
                    symbol,
                    date_trunc('day', to_timestamp(ts_ns / 1000000000)) as ts_ns,
                    strftime(date_trunc('day', to_timestamp(ts_ns / 1000000000)), '%Y-%m-%d') as date,
                    first(open ORDER BY ts_ns) as open,
                    max(high) as high,
                    min(low) as low,
                    last(close ORDER BY ts_ns) as close,
                    sum(volume) as volume,
                    '{frame}' as frame
                FROM read_parquet('{raw_path}/**/*.parquet') 
                WHERE symbol IS NOT NULL
                GROUP BY symbol, date_trunc('day', to_timestamp(ts_ns / 1000000000))
                ORDER BY symbol, ts_ns
                """
            else:
                # For intraday, use time_bucket
                agg_sql = f"""
                SELECT 
                    symbol,
                    epoch(time_bucket(INTERVAL '{interval}', to_timestamp(ts_ns / 1000000000))) * 1000000000 as ts_ns,
                    strftime(date_trunc('day', to_timestamp(ts_ns / 1000000000)), '%Y-%m-%d') as date,
                    first(open ORDER BY ts_ns) as open,
                    max(high) as high,
                    min(low) as low,
                    last(close ORDER BY ts_ns) as close,
                    sum(volume) as volume,
                    '{frame}' as frame
                FROM read_parquet('{raw_path}/**/*.parquet') 
                WHERE symbol IS NOT NULL
                GROUP BY symbol, time_bucket(INTERVAL '{interval}', to_timestamp(ts_ns / 1000000000))
                ORDER BY symbol, ts_ns
                """
            
            # Execute query and get results
            result = conn.execute(agg_sql).fetchall()
            columns = [desc[0] for desc in conn.description]
            
            logger.info(f"   Got {len(result)} aggregated bars for {frame}")
            
            if not result:
                logger.warning(f"   No data to aggregate for {frame}")
                continue
            
            # Group by symbol and date for partitioning
            data_by_partition = {}
            for row in result:
                row_dict = dict(zip(columns, row))
                symbol = row_dict['symbol']
                date = row_dict['date']
                partition_key = (symbol, date)
                
                if partition_key not in data_by_partition:
                    data_by_partition[partition_key] = []
                data_by_partition[partition_key].append(row_dict)
            
            # Write partitioned files
            for (symbol, date), rows in data_by_partition.items():
                # Create partition directory
                partition_dir = frame_dir / f"symbol={symbol}" / f"date={date}"
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                # Convert to Arrow table
                table = pa.Table.from_pylist(rows)
                
                # Write parquet file
                output_file = partition_dir / f"{symbol}_{date}_{frame}.parquet"
                pq.write_table(table, output_file, compression='snappy')
                
                logger.info(f"   ✅ Wrote {len(rows)} bars to {output_file}")
            
            logger.info(f"✅ Completed {frame} aggregation")
            
        except Exception as e:
            logger.error(f"❌ Failed to create {frame} aggregation: {e}")
    
    logger.info("\n🎉 Aggregation population completed!")

def test_views():
    """Test that the DuckDB views now work."""
    from src.marketpipe.aggregation.infrastructure.duckdb_views import ensure_views, query
    
    logger.info("\n🧪 Testing DuckDB views...")
    
    # Ensure views are created
    ensure_views()
    
    # Test queries
    test_queries = [
        ("SELECT symbol, COUNT(*) as count FROM bars_5m WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 5", "5-minute bars"),
        ("SELECT symbol, COUNT(*) as count FROM bars_1d WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 5", "Daily bars"),
        ("SELECT symbol, AVG(close) as avg_close FROM bars_1d WHERE symbol IN ('AAPL', 'MSFT') GROUP BY symbol", "Average prices"),
    ]
    
    for sql, description in test_queries:
        try:
            result = query(sql)
            logger.info(f"✅ {description}: {len(result)} rows")
            logger.info(f"   Data: {result.to_dict('records') if len(result) > 0 else 'No data'}")
        except Exception as e:
            logger.error(f"❌ {description} failed: {e}")

if __name__ == "__main__":
    logger.info("🚀 Starting aggregation population...")
    create_aggregated_data()
    test_views() 