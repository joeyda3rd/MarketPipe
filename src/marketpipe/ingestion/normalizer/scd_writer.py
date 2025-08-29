from __future__ import annotations

import datetime as dt
import logging
import uuid
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet

from marketpipe.metrics import SYMBOLS_ROWS

logger = logging.getLogger(__name__)


def run_scd_update(
    db: duckdb.DuckDBPyConnection, data_dir: str, dry_run: bool = False
) -> dict[str, int]:
    """
    Apply SCD-2 updates to symbols_master Parquet dataset.

    Args:
        db: DuckDB connection with diff tables and symbols_master attached
        data_dir: Base directory for Parquet dataset
        dry_run: If True, only return statistics without writing

    Returns:
        Dictionary with update statistics
    """
    stats = {"rows_inserted": 0, "rows_updated": 0, "rows_closed": 0, "files_written": 0}

    # Get snapshot date from symbols_snapshot table
    try:
        result = db.sql("SELECT max(as_of) FROM symbols_snapshot").fetchone()
        if not result or result[0] is None:
            raise ValueError("No snapshot date found in symbols_snapshot table")
        snapshot_date = result[0]

        if not isinstance(snapshot_date, dt.date):
            # Handle string dates
            if isinstance(snapshot_date, str):
                snapshot_date = dt.datetime.fromisoformat(snapshot_date).date()
            else:
                raise ValueError(f"Invalid snapshot_date type: {type(snapshot_date)}")

    except Exception as e:
        raise ValueError(f"Failed to determine snapshot_date: {e}") from e

    logger.info(f"Processing SCD-2 update for snapshot_date: {snapshot_date}")

    # Check if diff tables exist and get counts
    try:
        tup = db.sql("SELECT COUNT(*) FROM diff_insert").fetchone()
        insert_count = int(tup[0]) if tup and tup[0] is not None else 0
        tup = db.sql("SELECT COUNT(*) FROM diff_update").fetchone()
        update_count = int(tup[0]) if tup and tup[0] is not None else 0
        tup = db.sql("SELECT COUNT(*) FROM diff_unchanged").fetchone()
        unchanged_count = int(tup[0]) if tup and tup[0] is not None else 0

        logger.info(
            f"Diff summary - Insert: {insert_count}, Update: {update_count}, Unchanged: {unchanged_count}"
        )

        if insert_count == 0 and update_count == 0:
            logger.info("No changes to process")
            return stats

    except Exception as e:
        raise RuntimeError(f"Failed to read diff tables: {e}") from e

    # Step 1: Assign IDs to insert rows
    if insert_count > 0:
        try:
            # Get current max ID from symbols_master
            max_id_result = db.sql("SELECT COALESCE(MAX(id), 0) FROM symbols_master").fetchone()
            max_id = max_id_result[0] if max_id_result else 0

            logger.info(f"Current max ID in symbols_master: {max_id}")

            # Assign sequential IDs starting from max_id + 1
            db.execute(
                """
                CREATE OR REPLACE TABLE diff_insert_with_ids AS
                SELECT
                    ROW_NUMBER() OVER (ORDER BY natural_key) + $1 AS id,
                    *
                FROM diff_insert
            """,
                [max_id],
            )

            # Replace the original diff_insert table
            db.execute("DROP TABLE IF EXISTS diff_insert")
            db.execute("CREATE TABLE diff_insert AS SELECT * FROM diff_insert_with_ids")
            db.execute("DROP TABLE diff_insert_with_ids")

            stats["rows_inserted"] = insert_count
            logger.info(f"Assigned IDs {max_id + 1} to {max_id + insert_count} for new inserts")

        except Exception as e:
            raise RuntimeError(f"Failed to assign IDs to insert rows: {e}") from e

        # Step 2: Prepare all data (existing + new) for single write operation
    close_date = snapshot_date - dt.timedelta(days=1) if update_count > 0 else None
    all_data_to_write = []

    # Get existing data (with updates closed if needed)
    if update_count > 0:
        try:
            if not dry_run:
                # Read all existing data and close rows that are being updated
                existing_data = db.execute(
                    """
                    SELECT *,
                           CASE
                               WHEN id IN (SELECT id FROM diff_update) AND valid_to IS NULL
                               THEN $1::DATE
                               ELSE valid_to
                           END as updated_valid_to
                    FROM symbols_master
                """,
                    [close_date],
                ).arrow()

                # Count rows that were closed
                tup = db.execute(
                    """
                    SELECT COUNT(*)
                    FROM symbols_master
                    WHERE id IN (SELECT id FROM diff_update)
                      AND valid_to IS NULL
                """
                ).fetchone()
                closed_count = int(tup[0]) if tup and tup[0] is not None else 0

                stats["rows_closed"] = closed_count
                logger.info(f"Will close {closed_count} existing rows with valid_to = {close_date}")

                if len(existing_data) > 0:
                    # Rename the updated column back to valid_to
                    existing_df = existing_data.to_pandas()
                    existing_df["valid_to"] = existing_df["updated_valid_to"]
                    existing_df = existing_df.drop(columns=["updated_valid_to"])

                    # Ensure consistent column ordering for schema compatibility
                    expected_columns = [
                        "id",
                        "natural_key",
                        "symbol",
                        "company_name",
                        "exchange",
                        "asset_type",
                        "status",
                        "market_cap",
                        "sector",
                        "industry",
                        "country",
                        "currency",
                        "valid_from",
                        "valid_to",
                        "created_at",
                        "as_of",
                    ]
                    # Keep only columns that exist in the dataframe
                    available_columns = [
                        col for col in expected_columns if col in existing_df.columns
                    ]
                    existing_df = existing_df[available_columns]

                    all_data_to_write.append(existing_df)
            else:
                # Dry run - just count what would be closed
                tup = db.execute(
                    """
                    SELECT COUNT(*)
                    FROM symbols_master
                    WHERE id IN (SELECT id FROM diff_update)
                      AND valid_to IS NULL
                """
                ).fetchone()
                closed_count = int(tup[0]) if tup and tup[0] is not None else 0
                stats["rows_closed"] = closed_count
                logger.info(f"[DRY RUN] Would close {closed_count} existing rows")

        except Exception as e:
            raise RuntimeError(f"Failed to process existing rows: {e}") from e
    else:
        # No updates, just read existing data as-is
        try:
            existing_data = db.execute("SELECT * FROM symbols_master").arrow()
            if len(existing_data) > 0:
                existing_df = existing_data.to_pandas()
                # Ensure consistent column ordering
                expected_columns = [
                    "id",
                    "natural_key",
                    "symbol",
                    "company_name",
                    "exchange",
                    "asset_type",
                    "status",
                    "market_cap",
                    "sector",
                    "industry",
                    "country",
                    "currency",
                    "valid_from",
                    "valid_to",
                    "created_at",
                    "as_of",
                ]
                available_columns = [col for col in expected_columns if col in existing_df.columns]
                existing_df = existing_df[available_columns]
                all_data_to_write.append(existing_df)
        except Exception as e:
            logger.info(f"No existing data to preserve: {e}")

    # Step 3: Build new rows to add
    try:
        if insert_count == 0 and update_count == 0:
            # No changes, but we might still need to rewrite existing data if in update mode
            if len(all_data_to_write) > 0 and not dry_run:
                logger.info("No new rows, rewriting existing data due to updates")
            else:
                logger.info("No new rows to write")
                return stats

        # Get the new rows as pandas DataFrame with consistent column ordering
        if insert_count > 0 or update_count > 0:
            # Get new rows with consistent column ordering
            column_select = """
                id, natural_key, symbol, company_name, exchange, asset_type, status,
                market_cap, sector, industry, country, currency,
                $1 AS valid_from, NULL::DATE AS valid_to, CURRENT_TIMESTAMP AS created_at, as_of
            """

            if insert_count > 0 and update_count > 0:
                # Both inserts and updates
                new_rows_table = db.execute(
                    f"""
                    SELECT {column_select}
                    FROM diff_insert
                    UNION ALL
                    SELECT {column_select.replace('$1', '$2')}
                    FROM diff_update
                """,
                    [snapshot_date, snapshot_date],
                ).arrow()
            elif insert_count > 0:
                # Only inserts
                new_rows_table = db.execute(
                    f"""
                    SELECT {column_select}
                    FROM diff_insert
                """,
                    [snapshot_date],
                ).arrow()
            elif update_count > 0:
                # Only updates
                new_rows_table = db.execute(
                    f"""
                    SELECT {column_select}
                    FROM diff_update
                """,
                    [snapshot_date],
                ).arrow()

            # Convert to pandas DataFrame
            new_rows_df = new_rows_table.to_pandas()
            all_data_to_write.append(new_rows_df)

            total_new_rows = len(new_rows_df)
            logger.info(f"Generated {total_new_rows} new rows for Parquet write")
        else:
            total_new_rows = 0

        if dry_run:
            total_rows = sum(len(df) for df in all_data_to_write)
            logger.info(
                f"[DRY RUN] Would write {total_rows} total rows ({total_new_rows} new) to {data_dir}"
            )
            return stats

    except Exception as e:
        raise RuntimeError(f"Failed to build new rows table: {e}") from e

    # Step 4: Write combined dataset (existing + new) to partitioned Parquet
    if len(all_data_to_write) > 0:
        try:
            # Ensure output directory exists
            Path(data_dir).mkdir(parents=True, exist_ok=True)

            # Combine all data (filter out empty DataFrames to avoid FutureWarning)
            non_empty_dfs = [df for df in all_data_to_write if not df.empty]
            if non_empty_dfs:
                combined_df = pd.concat(non_empty_dfs, ignore_index=True)
            else:
                # If no data to write, create empty DataFrame with expected schema
                combined_df = pd.DataFrame(
                    columns=[
                        "id",
                        "natural_key",
                        "symbol",
                        "company_name",
                        "exchange",
                        "asset_type",
                        "status",
                        "market_cap",
                        "sector",
                        "industry",
                        "country",
                        "currency",
                        "valid_from",
                        "valid_to",
                        "created_at",
                        "as_of",
                    ]
                )
            total_rows = len(combined_df)

            # Ensure consistent data types for date columns
            if "valid_from" in combined_df.columns:
                combined_df["valid_from"] = pd.to_datetime(combined_df["valid_from"]).dt.date
            if "valid_to" in combined_df.columns:
                # Handle NULL values properly
                combined_df["valid_to"] = pd.to_datetime(
                    combined_df["valid_to"], errors="coerce"
                ).dt.date
            if "as_of" in combined_df.columns:
                combined_df["as_of"] = pd.to_datetime(combined_df["as_of"]).dt.date

            # Convert to Arrow table and add partitioning columns
            combined_table = pa.Table.from_pandas(combined_df, preserve_index=False)
            year_month_table = _add_partition_columns(combined_table)

            # Write complete dataset (replaces all existing data)
            ds.write_dataset(
                year_month_table,
                base_dir=data_dir,
                format="parquet",
                partitioning=["year", "month"],
                partitioning_flavor="hive",
                basename_template=f"part-{uuid.uuid4().hex[:8]}-{{i}}.parquet",
                existing_data_behavior="delete_matching",
            )

            stats["files_written"] = _count_new_files(data_dir)
            stats["rows_updated"] = update_count

            logger.info(
                f"Successfully wrote {total_rows} total rows ({total_new_rows} new) to partitioned dataset at {data_dir}"
            )

        except Exception as e:
            raise RuntimeError(f"Failed to write Parquet dataset: {e}") from e

    # Step 5: Optional - Update table statistics
    try:
        if hasattr(db, "execute"):
            # Refresh symbols_master table stats if it's attached
            db.execute("ANALYZE symbols_master")
            logger.debug("Updated table statistics")
    except Exception as e:
        logger.warning(f"Failed to update table statistics: {e}")

    # Record metrics for SCD operations
    if stats["rows_inserted"] > 0:
        SYMBOLS_ROWS.labels(action="insert").inc(stats["rows_inserted"])
    if stats["rows_updated"] > 0:
        SYMBOLS_ROWS.labels(action="update").inc(stats["rows_updated"])

    logger.info(f"SCD-2 update completed: {stats}")
    return stats


def _add_partition_columns(table: pa.Table) -> pa.Table:
    """Add year and month columns for partitioning based on valid_from."""
    # Convert to pandas to easily extract year/month
    df = table.to_pandas()

    # Check for column name collisions with partition columns
    if "year" in df.columns or "month" in df.columns:
        raise ValueError(
            "Data contains 'year' or 'month' columns that would conflict with partitioning"
        )

    # Ensure valid_from is datetime type
    if not pd.api.types.is_datetime64_any_dtype(df["valid_from"]):
        df["valid_from"] = pd.to_datetime(df["valid_from"])

    df["year"] = df["valid_from"].dt.year
    df["month"] = df["valid_from"].dt.month

    # Convert back to Arrow table
    return pa.Table.from_pandas(df, preserve_index=False)


def _count_new_files(data_dir: str) -> int:
    """Count Parquet files in the dataset directory."""
    try:
        parquet_files = list(Path(data_dir).rglob("*.parquet"))
        return len(parquet_files)
    except Exception:
        return 0


def attach_symbols_master(db: duckdb.DuckDBPyConnection, data_dir: str) -> None:
    """
    Attach symbols_master Parquet dataset to DuckDB connection.

    Args:
        db: DuckDB connection
        data_dir: Base directory containing the Parquet dataset
    """
    parquet_path = Path(data_dir)

    if not parquet_path.exists():
        logger.info(f"Creating new symbols_master dataset at {data_dir}")
        parquet_path.mkdir(parents=True, exist_ok=True)

        # Create empty table with proper schema
        db.execute(
            """
            CREATE OR REPLACE TABLE symbols_master (
                id INTEGER PRIMARY KEY,
                natural_key VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                company_name VARCHAR,
                exchange VARCHAR,
                asset_type VARCHAR,
                status VARCHAR,
                market_cap BIGINT,
                sector VARCHAR,
                industry VARCHAR,
                country VARCHAR,
                currency VARCHAR,
                valid_from DATE NOT NULL,
                valid_to DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                as_of DATE NOT NULL
            )
        """
        )
        return

    # Check if any parquet files exist
    parquet_files = list(parquet_path.rglob("*.parquet"))

    if parquet_files:
        # Attach existing dataset - drop existing table/view first
        try:
            # Safely drop both table and view (one might not exist)
            try:
                db.execute("DROP TABLE IF EXISTS symbols_master")
            except Exception:
                pass
            try:
                db.execute("DROP VIEW IF EXISTS symbols_master")
            except Exception:
                pass

            db.execute(
                f"""
                CREATE OR REPLACE VIEW symbols_master AS
                SELECT * FROM read_parquet('{parquet_path}/**/*.parquet')
            """
            )
            logger.info(f"Attached symbols_master from {data_dir} ({len(parquet_files)} files)")
        except Exception as e:
            logger.warning(f"Failed to attach symbols_master: {e}")
            # Create empty table as fallback
            _create_empty_symbols_master_table(db)
    else:
        # No parquet files exist, create empty table
        logger.info(f"No parquet files found in {data_dir}, creating empty symbols_master table")
        _create_empty_symbols_master_table(db)


def _create_empty_symbols_master_table(db: duckdb.DuckDBPyConnection) -> None:
    """Create empty symbols_master table with proper schema."""
    db.execute(
        """
        CREATE OR REPLACE TABLE symbols_master (
            id INTEGER PRIMARY KEY,
            natural_key VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            company_name VARCHAR,
            exchange VARCHAR,
            asset_type VARCHAR,
            status VARCHAR,
            market_cap BIGINT,
            sector VARCHAR,
            industry VARCHAR,
            country VARCHAR,
            currency VARCHAR,
            valid_from DATE NOT NULL,
            valid_to DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            as_of DATE NOT NULL
        )
    """
    )
