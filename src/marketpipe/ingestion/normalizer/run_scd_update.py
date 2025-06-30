#!/usr/bin/env python3
"""
CLI wrapper for running SCD-2 updates on symbols_master dataset.

This script takes in-memory diff tables from Story B2 and writes them to a
partitioned Parquet dataset representing the full history of symbols_master
as a Slowly Changing Dimension type 2 table.

Usage:
    python run_scd_update.py --data-dir ./data/symbols_master --dry-run
    python run_scd_update.py --data-dir ./data/symbols_master --verbose
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb

try:
    from marketpipe.ingestion.normalizer.scd_writer import (
        attach_symbols_master,
        run_scd_update,
    )
except ImportError:
    # Fallback for development/testing
    import sys

    sys.path.insert(0, str(Path(__file__).parent))
    from scd_writer import attach_symbols_master, run_scd_update


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the CLI."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def validate_inputs(db_path: str, data_dir: str) -> None:
    """Validate CLI inputs."""
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    # Create data directory if it doesn't exist
    Path(data_dir).mkdir(parents=True, exist_ok=True)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Run SCD-2 update for symbols_master dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run update with default output directory
  python run_scd_update.py /tmp/symbols.db

  # Run update with custom output directory
  python run_scd_update.py /tmp/symbols.db --out /data/warehouse/symbols_master

  # Dry run to see what would be changed
  python run_scd_update.py /tmp/symbols.db --dry-run

  # Verbose logging
  python run_scd_update.py /tmp/symbols.db --verbose
        """,
    )

    parser.add_argument("db_path", help="Path to DuckDB database file containing diff tables")

    parser.add_argument(
        "--out",
        default=None,
        help="Output directory for Parquet dataset (default: $DATA_DIR or ./data/warehouse/symbols_master)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without actually writing files",
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Determine output directory
    if args.out:
        data_dir = args.out
    else:
        data_dir = os.getenv("DATA_DIR", "./data/warehouse/symbols_master")

    logger.info(f"Starting SCD-2 update: db={args.db_path}, out={data_dir}, dry_run={args.dry_run}")

    try:
        # Validate inputs
        validate_inputs(args.db_path, data_dir)

        # Connect to database
        logger.info(f"Connecting to database: {args.db_path}")
        db = duckdb.connect(args.db_path)

        try:
            # Attach symbols_master dataset
            logger.info(f"Attaching symbols_master dataset from {data_dir}")
            attach_symbols_master(db, data_dir)

            # Check required tables exist
            required_tables = ["symbols_snapshot", "diff_insert", "diff_update", "diff_unchanged"]
            for table in required_tables:
                try:
                    count = db.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    logger.debug(f"Table {table}: {count} rows")
                except Exception as e:
                    logger.error(f"Required table {table} not found: {e}")
                    sys.exit(1)

            # Run SCD-2 update
            logger.info("Running SCD-2 update...")
            stats = run_scd_update(db, data_dir, dry_run=args.dry_run)

            # Report results
            logger.info("SCD-2 update completed successfully!")
            logger.info(f"Results: {stats}")

            if args.dry_run:
                print("\n=== DRY RUN SUMMARY ===")
            else:
                print("\n=== UPDATE SUMMARY ===")

            print(f"Rows inserted: {stats['rows_inserted']}")
            print(f"Rows updated: {stats['rows_updated']}")
            print(f"Rows closed: {stats['rows_closed']}")
            print(f"Files written: {stats['files_written']}")

            if stats["rows_inserted"] + stats["rows_updated"] == 0:
                print("No changes detected - dataset is up to date")

        finally:
            db.close()

    except Exception as e:
        logger.error(f"SCD-2 update failed: {e}")
        sys.exit(1)


def cli() -> None:
    """Entry point for console script."""
    main()


if __name__ == "__main__":
    main()
