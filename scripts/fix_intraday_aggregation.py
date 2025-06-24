#!/usr/bin/env python3
"""Fix intraday aggregation by correcting GROUP BY clause issues."""

import logging
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_intraday_aggregations():
    """Fix the 5m, 15m, and 1h aggregations with correct SQL."""

    conn = duckdb.connect()
    raw_path = Path("data/raw/frame=1m")
    agg_root = Path("data/agg")

    # Only process intraday timeframes that failed
    timeframes = {
        "5m": "5 minutes",
        "15m": "15 minutes",
        "1h": "1 hour"
    }

    for frame, interval in timeframes.items():
        logger.info(f"\n🔧 Fixing {frame} aggregation...")

        frame_dir = agg_root / f"frame={frame}"
        frame_dir.mkdir(exist_ok=True)

        try:
            # Fixed SQL with proper GROUP BY and ANY_VALUE for non-aggregated columns
            agg_sql = f"""
            SELECT 
                symbol,
                epoch(time_bucket(INTERVAL '{interval}', to_timestamp(ts_ns / 1000000000))) * 1000000000 as ts_ns,
                ANY_VALUE(strftime(date_trunc('day', to_timestamp(ts_ns / 1000000000)), '%Y-%m-%d')) as date,
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
                partition_dir = frame_dir / f"symbol={symbol}" / f"date={date}"
                partition_dir.mkdir(parents=True, exist_ok=True)

                table = pa.Table.from_pylist(rows)
                output_file = partition_dir / f"{symbol}_{date}_{frame}.parquet"
                pq.write_table(table, output_file, compression='snappy')

                logger.info(f"   ✅ Wrote {len(rows)} bars to {output_file}")

            logger.info(f"✅ Completed {frame} aggregation")

        except Exception as e:
            logger.error(f"❌ Failed to create {frame} aggregation: {e}")

    logger.info("\n🎉 Intraday aggregation fix completed!")

def test_all_views():
    """Test all timeframe views."""
    logger.info("\n🧪 Testing all aggregated views...")

    test_queries = [
        ("SELECT symbol, COUNT(*) as count FROM bars_5m WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 5", "5-minute bars"),
        ("SELECT symbol, COUNT(*) as count FROM bars_15m WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 5", "15-minute bars"),
        ("SELECT symbol, COUNT(*) as count FROM bars_1h WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 5", "1-hour bars"),
        ("SELECT symbol, COUNT(*) as count FROM bars_1d WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 5", "Daily bars (already working)"),
    ]

    for sql, description in test_queries:
        try:
            import subprocess
            result = subprocess.run([
                "python", "-m", "marketpipe", "query", sql
            ], capture_output=True, text=True, check=True)

            if "Query returned no results" in result.stdout:
                logger.warning(f"⚠️  {description}: No results (view might be empty)")
            else:
                logger.info(f"✅ {description}: Working!")
                # Show first few lines of output
                lines = result.stdout.strip().split('\n')
                for line in lines[2:7]:  # Skip header lines, show data
                    if line.strip():
                        logger.info(f"   {line}")
        except Exception as e:
            logger.error(f"❌ {description} failed: {e}")

if __name__ == "__main__":
    logger.info("🚀 Fixing intraday aggregations...")
    fix_intraday_aggregations()
    test_all_views()
