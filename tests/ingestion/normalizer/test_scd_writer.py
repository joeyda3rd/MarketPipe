from __future__ import annotations

import datetime as dt
import tempfile
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest

from marketpipe.ingestion.normalizer.scd_writer import (
    _add_partition_columns,
    _count_new_files,
    attach_symbols_master,
    run_scd_update,
)


def _drop_symbols_master_safe(db: duckdb.DuckDBPyConnection):
    """Safely drop symbols_master whether it's a table or view."""
    try:
        db.execute("DROP TABLE IF EXISTS symbols_master")
    except:
        pass
    try:
        db.execute("DROP VIEW IF EXISTS symbols_master")
    except:
        pass


@pytest.fixture
def temp_data_dir():
    """Create temporary directory for test Parquet files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def sample_snapshot_data():
    """Sample symbols snapshot data for testing."""
    return [
        {
            "natural_key": "AAPL-NASDAQ",
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "exchange": "NASDAQ",
            "asset_type": "stock",
            "status": "active",
            "market_cap": 3000000000000,
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "country": "US",
            "currency": "USD",
            "as_of": dt.date(2024, 1, 15),
        },
        {
            "natural_key": "GOOGL-NASDAQ",
            "symbol": "GOOGL",
            "company_name": "Alphabet Inc.",
            "exchange": "NASDAQ",
            "asset_type": "stock",
            "status": "active",
            "market_cap": 2000000000000,
            "sector": "Technology",
            "industry": "Internet Software & Services",
            "country": "US",
            "currency": "USD",
            "as_of": dt.date(2024, 1, 15),
        },
    ]


@pytest.fixture
def empty_db():
    """Create empty in-memory DuckDB for testing."""
    db = duckdb.connect(":memory:")
    yield db
    db.close()


class TestSCDWriter:
    """Test suite for SCD-2 writer functionality."""

    def test_first_load_creates_initial_records(
        self, empty_db, temp_data_dir, sample_snapshot_data
    ):
        """Test initial load creates new records with proper SCD-2 fields."""
        # Setup: Create snapshot and diff tables
        snapshot_df = pd.DataFrame(sample_snapshot_data)
        empty_db.execute("CREATE TABLE symbols_snapshot AS SELECT * FROM snapshot_df")

        # Create diff_insert table (all rows are new in first load)
        empty_db.execute(
            """
            CREATE TABLE diff_insert AS
            SELECT * FROM symbols_snapshot
        """
        )

        # Create empty diff tables
        empty_db.execute("CREATE TABLE diff_update AS SELECT * FROM symbols_snapshot WHERE 1=0")
        empty_db.execute("CREATE TABLE diff_unchanged AS SELECT * FROM symbols_snapshot WHERE 1=0")

        # Attach empty symbols_master
        attach_symbols_master(empty_db, temp_data_dir)

        # Run SCD update
        stats = run_scd_update(empty_db, temp_data_dir)

        # Verify results
        assert stats["rows_inserted"] == 2
        assert stats["rows_updated"] == 0
        assert stats["rows_closed"] == 0
        assert stats["files_written"] > 0

        # Verify files were created
        parquet_files = list(Path(temp_data_dir).rglob("*.parquet"))
        assert len(parquet_files) > 0

        # Verify data structure
        _drop_symbols_master_safe(empty_db)
        empty_db.execute(
            f"""
            CREATE OR REPLACE VIEW symbols_master AS
            SELECT * FROM read_parquet('{temp_data_dir}/**/*.parquet')
        """
        )

        result = empty_db.sql("SELECT * FROM symbols_master ORDER BY id").df()
        assert len(result) == 2
        assert result["id"].tolist() == [1, 2]
        assert all(pd.isna(result["valid_to"]))  # All should have NULL valid_to
        assert all(result["valid_from"].dt.date == dt.date(2024, 1, 15))

    def test_second_load_handles_updates(self, empty_db, temp_data_dir, sample_snapshot_data):
        """Test second load properly handles updates with SCD-2 semantics."""
        # First load - setup initial data
        snapshot_df = pd.DataFrame(sample_snapshot_data)
        empty_db.execute("CREATE TABLE symbols_snapshot AS SELECT * FROM snapshot_df")

        empty_db.execute("CREATE TABLE diff_insert AS SELECT * FROM symbols_snapshot")
        empty_db.execute("CREATE TABLE diff_update AS SELECT * FROM symbols_snapshot WHERE 1=0")
        empty_db.execute("CREATE TABLE diff_unchanged AS SELECT * FROM symbols_snapshot WHERE 1=0")

        attach_symbols_master(empty_db, temp_data_dir)
        run_scd_update(empty_db, temp_data_dir)

        # Second load - create update scenario
        updated_data = sample_snapshot_data.copy()
        updated_data[0]["company_name"] = "Apple Inc. (Updated)"  # Change AAPL company name
        updated_data[0]["as_of"] = dt.date(2024, 1, 16)  # New snapshot date
        updated_data[1]["as_of"] = dt.date(2024, 1, 16)  # New snapshot date

        # Clear and recreate snapshot table
        empty_db.execute("DROP TABLE symbols_snapshot")
        snapshot_df = pd.DataFrame(updated_data)
        empty_db.execute("CREATE TABLE symbols_snapshot AS SELECT * FROM snapshot_df")

        # Re-attach symbols_master with existing data
        attach_symbols_master(empty_db, temp_data_dir)

        # Create diff tables for second load
        empty_db.execute("DROP TABLE IF EXISTS diff_insert")
        empty_db.execute("DROP TABLE IF EXISTS diff_update")
        empty_db.execute("DROP TABLE IF EXISTS diff_unchanged")

        # Simulate diff results (AAPL updated, GOOGL unchanged)
        empty_db.execute(
            """
            CREATE TABLE diff_insert AS
            SELECT * FROM symbols_snapshot WHERE 1=0
        """
        )

        empty_db.execute(
            """
            CREATE TABLE diff_update AS
            SELECT 1 as id, * FROM symbols_snapshot WHERE symbol = 'AAPL'
        """
        )

        empty_db.execute(
            """
            CREATE TABLE diff_unchanged AS
            SELECT 2 as id, * FROM symbols_snapshot WHERE symbol = 'GOOGL'
        """
        )

        # Run second SCD update
        stats = run_scd_update(empty_db, temp_data_dir)

        # Verify results
        assert stats["rows_inserted"] == 0
        assert stats["rows_updated"] == 1
        assert stats["rows_closed"] == 1

        # Verify data state
        _drop_symbols_master_safe(empty_db)
        empty_db.execute(
            f"""
            CREATE OR REPLACE VIEW symbols_master AS
            SELECT * FROM read_parquet('{temp_data_dir}/**/*.parquet')
        """
        )

        result = empty_db.sql("SELECT * FROM symbols_master ORDER BY id, valid_from").df()

        # Should have 3 rows total: 2 original + 1 new version of AAPL
        assert len(result) == 3

        # Check AAPL has two versions
        aapl_rows = result[result["symbol"] == "AAPL"]
        assert len(aapl_rows) == 2

        # Old AAPL row should be closed
        old_aapl = aapl_rows[aapl_rows["valid_from"].dt.date == dt.date(2024, 1, 15)]
        assert len(old_aapl) == 1
        assert old_aapl["valid_to"].iloc[0].date() == dt.date(
            2024, 1, 15
        )  # closed on snapshot_date - 1
        assert old_aapl["company_name"].iloc[0] == "Apple Inc."

        # New AAPL row should be open
        new_aapl = aapl_rows[aapl_rows["valid_from"].dt.date == dt.date(2024, 1, 16)]
        assert len(new_aapl) == 1
        assert pd.isna(new_aapl["valid_to"].iloc[0])
        assert new_aapl["company_name"].iloc[0] == "Apple Inc. (Updated)"

        # GOOGL should remain unchanged (only one row)
        googl_rows = result[result["symbol"] == "GOOGL"]
        assert len(googl_rows) == 1
        assert pd.isna(googl_rows["valid_to"].iloc[0])

    def test_id_sequence_continuity(self, empty_db, temp_data_dir):
        """Test that IDs continue sequence correctly across loads."""
        # First load
        initial_data = [
            {
                "natural_key": "A-NASDAQ",
                "symbol": "A",
                "company_name": "Company A",
                "exchange": "NASDAQ",
                "asset_type": "stock",
                "status": "active",
                "market_cap": 1000000,
                "sector": "Tech",
                "industry": "Software",
                "country": "US",
                "currency": "USD",
                "as_of": dt.date(2024, 1, 15),
            },
            {
                "natural_key": "B-NASDAQ",
                "symbol": "B",
                "company_name": "Company B",
                "exchange": "NASDAQ",
                "asset_type": "stock",
                "status": "active",
                "market_cap": 2000000,
                "sector": "Finance",
                "industry": "Banking",
                "country": "US",
                "currency": "USD",
                "as_of": dt.date(2024, 1, 15),
            },
        ]

        snapshot_df = pd.DataFrame(initial_data)
        empty_db.execute("CREATE TABLE symbols_snapshot AS SELECT * FROM snapshot_df")
        empty_db.execute("CREATE TABLE diff_insert AS SELECT * FROM symbols_snapshot")
        empty_db.execute("CREATE TABLE diff_update AS SELECT * FROM symbols_snapshot WHERE 1=0")
        empty_db.execute("CREATE TABLE diff_unchanged AS SELECT * FROM symbols_snapshot WHERE 1=0")

        attach_symbols_master(empty_db, temp_data_dir)
        run_scd_update(empty_db, temp_data_dir)

        # Second load with new symbols
        new_data = initial_data + [
            {
                "natural_key": "C-NASDAQ",
                "symbol": "C",
                "company_name": "Company C",
                "exchange": "NASDAQ",
                "asset_type": "stock",
                "status": "active",
                "market_cap": 3000000,
                "sector": "Healthcare",
                "industry": "Biotech",
                "country": "US",
                "currency": "USD",
                "as_of": dt.date(2024, 1, 16),
            },
            {
                "natural_key": "D-NASDAQ",
                "symbol": "D",
                "company_name": "Company D",
                "exchange": "NASDAQ",
                "asset_type": "stock",
                "status": "active",
                "market_cap": 4000000,
                "sector": "Energy",
                "industry": "Oil & Gas",
                "country": "US",
                "currency": "USD",
                "as_of": dt.date(2024, 1, 16),
            },
        ]

        # Update snapshot
        empty_db.execute("DROP TABLE symbols_snapshot")
        snapshot_df = pd.DataFrame(new_data)
        empty_db.execute("CREATE TABLE symbols_snapshot AS SELECT * FROM snapshot_df")

        # Re-attach and setup diff tables
        attach_symbols_master(empty_db, temp_data_dir)
        empty_db.execute("DROP TABLE IF EXISTS diff_insert")
        empty_db.execute("DROP TABLE IF EXISTS diff_update")
        empty_db.execute("DROP TABLE IF EXISTS diff_unchanged")

        # Only C and D are new inserts
        empty_db.execute(
            """
            CREATE TABLE diff_insert AS
            SELECT * FROM symbols_snapshot WHERE symbol IN ('C', 'D')
        """
        )
        empty_db.execute("CREATE TABLE diff_update AS SELECT * FROM symbols_snapshot WHERE 1=0")
        empty_db.execute(
            """
            CREATE TABLE diff_unchanged AS
            SELECT 1 as id, * FROM symbols_snapshot WHERE symbol = 'A'
            UNION ALL
            SELECT 2 as id, * FROM symbols_snapshot WHERE symbol = 'B'
        """
        )

        stats = run_scd_update(empty_db, temp_data_dir)
        assert stats["rows_inserted"] == 2

        # Verify ID sequence
        _drop_symbols_master_safe(empty_db)
        empty_db.execute(
            f"""
            CREATE OR REPLACE VIEW symbols_master AS
            SELECT * FROM read_parquet('{temp_data_dir}/**/*.parquet')
        """
        )

        result = empty_db.sql("SELECT * FROM symbols_master ORDER BY id").df()
        ids = result["id"].tolist()
        assert ids == [1, 2, 3, 4]  # Sequential IDs

        # Verify C and D got IDs 3 and 4
        new_symbols = result[result["symbol"].isin(["C", "D"])]
        assert set(new_symbols["id"]) == {3, 4}

    def test_dry_run_mode(self, empty_db, temp_data_dir, sample_snapshot_data):
        """Test dry run mode doesn't write files but returns statistics."""
        # Setup
        snapshot_df = pd.DataFrame(sample_snapshot_data)
        empty_db.execute("CREATE TABLE symbols_snapshot AS SELECT * FROM snapshot_df")
        empty_db.execute("CREATE TABLE diff_insert AS SELECT * FROM symbols_snapshot")
        empty_db.execute("CREATE TABLE diff_update AS SELECT * FROM symbols_snapshot WHERE 1=0")
        empty_db.execute("CREATE TABLE diff_unchanged AS SELECT * FROM symbols_snapshot WHERE 1=0")

        attach_symbols_master(empty_db, temp_data_dir)

        # Run in dry-run mode
        stats = run_scd_update(empty_db, temp_data_dir, dry_run=True)

        # Verify statistics are returned
        assert stats["rows_inserted"] == 2
        assert stats["rows_updated"] == 0
        assert stats["rows_closed"] == 0

        # Verify no files were written
        parquet_files = list(Path(temp_data_dir).rglob("*.parquet"))
        assert len(parquet_files) == 0

    def test_no_changes_scenario(self, empty_db, temp_data_dir):
        """Test scenario where diff tables are empty (no changes)."""
        # Setup with empty diff tables
        empty_db.execute(
            """
            CREATE TABLE symbols_snapshot (
                natural_key VARCHAR, symbol VARCHAR, company_name VARCHAR,
                exchange VARCHAR, asset_type VARCHAR, status VARCHAR,
                market_cap BIGINT, sector VARCHAR, industry VARCHAR,
                country VARCHAR, currency VARCHAR, as_of DATE
            )
        """
        )
        empty_db.execute(
            "INSERT INTO symbols_snapshot VALUES ('test', 'TEST', 'Test', 'NASDAQ', 'stock', 'active', 1000000, 'Tech', 'Software', 'US', 'USD', '2024-01-15')"
        )

        empty_db.execute("CREATE TABLE diff_insert AS SELECT * FROM symbols_snapshot WHERE 1=0")
        empty_db.execute("CREATE TABLE diff_update AS SELECT * FROM symbols_snapshot WHERE 1=0")
        empty_db.execute("CREATE TABLE diff_unchanged AS SELECT * FROM symbols_snapshot WHERE 1=0")

        attach_symbols_master(empty_db, temp_data_dir)

        # Run update
        stats = run_scd_update(empty_db, temp_data_dir)

        # Verify no changes
        assert stats["rows_inserted"] == 0
        assert stats["rows_updated"] == 0
        assert stats["rows_closed"] == 0
        assert stats["files_written"] == 0

    def test_missing_snapshot_table_error(self, empty_db, temp_data_dir):
        """Test error handling when symbols_snapshot table is missing."""
        attach_symbols_master(empty_db, temp_data_dir)

        with pytest.raises(ValueError, match="Failed to determine snapshot_date"):
            run_scd_update(empty_db, temp_data_dir)

    def test_missing_diff_tables_error(self, empty_db, temp_data_dir):
        """Test error handling when diff tables are missing."""
        empty_db.execute("CREATE TABLE symbols_snapshot (as_of DATE)")
        empty_db.execute("INSERT INTO symbols_snapshot VALUES ('2024-01-15')")

        attach_symbols_master(empty_db, temp_data_dir)

        with pytest.raises(RuntimeError, match="Failed to read diff tables"):
            run_scd_update(empty_db, temp_data_dir)

    def test_partition_columns_helper(self):
        """Test the partition columns helper function."""
        import pyarrow as pa

        # Create test table with valid_from dates
        data = {
            "id": [1, 2, 3],
            "symbol": ["A", "B", "C"],
            "valid_from": [dt.date(2024, 1, 15), dt.date(2024, 2, 20), dt.date(2023, 12, 25)],
        }

        table = pa.Table.from_pydict(data)
        result_table = _add_partition_columns(table)

        # Verify year and month columns were added
        assert "year" in result_table.column_names
        assert "month" in result_table.column_names

        # Check values
        result_df = result_table.to_pandas()
        assert result_df["year"].tolist() == [2024, 2024, 2023]
        assert result_df["month"].tolist() == [1, 2, 12]

    def test_count_files_helper(self, temp_data_dir):
        """Test the file counting helper function."""
        # Initially empty
        assert _count_new_files(temp_data_dir) == 0

        # Create some test files
        (Path(temp_data_dir) / "year=2024" / "month=01").mkdir(parents=True)
        (Path(temp_data_dir) / "year=2024" / "month=01" / "test1.parquet").touch()
        (Path(temp_data_dir) / "year=2024" / "month=02").mkdir(parents=True)
        (Path(temp_data_dir) / "year=2024" / "month=02" / "test2.parquet").touch()

        assert _count_new_files(temp_data_dir) == 2

    def test_idempotent_operation(self, empty_db, temp_data_dir, sample_snapshot_data):
        """Test that running updater twice with no changes is idempotent."""
        # First run
        snapshot_df = pd.DataFrame(sample_snapshot_data)
        empty_db.execute("CREATE TABLE symbols_snapshot AS SELECT * FROM snapshot_df")
        empty_db.execute("CREATE TABLE diff_insert AS SELECT * FROM symbols_snapshot")
        empty_db.execute("CREATE TABLE diff_update AS SELECT * FROM symbols_snapshot WHERE 1=0")
        empty_db.execute("CREATE TABLE diff_unchanged AS SELECT * FROM symbols_snapshot WHERE 1=0")

        attach_symbols_master(empty_db, temp_data_dir)
        stats1 = run_scd_update(empty_db, temp_data_dir)

        initial_file_count = _count_new_files(temp_data_dir)

        # Second run with no changes
        empty_db.execute("DROP TABLE IF EXISTS diff_insert")
        empty_db.execute("DROP TABLE IF EXISTS diff_update")
        empty_db.execute("DROP TABLE IF EXISTS diff_unchanged")

        empty_db.execute("CREATE TABLE diff_insert AS SELECT * FROM symbols_snapshot WHERE 1=0")
        empty_db.execute("CREATE TABLE diff_update AS SELECT * FROM symbols_snapshot WHERE 1=0")
        empty_db.execute("CREATE TABLE diff_unchanged AS SELECT * FROM symbols_snapshot")

        attach_symbols_master(empty_db, temp_data_dir)
        stats2 = run_scd_update(empty_db, temp_data_dir)

        # Verify no new changes
        assert stats2["rows_inserted"] == 0
        assert stats2["rows_updated"] == 0
        assert stats2["rows_closed"] == 0

        # File count should not change (no new files written)
        final_file_count = _count_new_files(temp_data_dir)
        assert final_file_count == initial_file_count

    def test_attach_symbols_master_new_dataset(self, empty_db, temp_data_dir):
        """Test attaching symbols_master when dataset doesn't exist yet."""
        # Should create empty table
        attach_symbols_master(empty_db, temp_data_dir)

        # Verify table exists with proper schema
        result = empty_db.sql("DESCRIBE symbols_master").df()
        expected_columns = {
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
        }
        assert set(result["column_name"]) == expected_columns

    def test_attach_symbols_master_existing_dataset(self, empty_db, temp_data_dir):
        """Test attaching symbols_master when dataset already exists."""
        # Create some test parquet files first
        test_data = pd.DataFrame(
            {
                "id": [1, 2],
                "natural_key": ["A-NASDAQ", "B-NASDAQ"],
                "symbol": ["A", "B"],
                "company_name": ["Company A", "Company B"],
                "exchange": ["NASDAQ", "NASDAQ"],
                "asset_type": ["stock", "stock"],
                "status": ["active", "active"],
                "market_cap": [1000000, 2000000],
                "sector": ["Tech", "Finance"],
                "industry": ["Software", "Banking"],
                "country": ["US", "US"],
                "currency": ["USD", "USD"],
                "valid_from": [dt.date(2024, 1, 15), dt.date(2024, 1, 15)],
                "valid_to": [None, None],
                "created_at": [dt.datetime.now(), dt.datetime.now()],
                "as_of": [dt.date(2024, 1, 15), dt.date(2024, 1, 15)],
            }
        )

        # Create directory structure and write test data
        year_month_dir = Path(temp_data_dir) / "year=2024" / "month=01"
        year_month_dir.mkdir(parents=True)
        test_data.to_parquet(year_month_dir / "test.parquet", index=False)

        # Attach should create view from existing data
        attach_symbols_master(empty_db, temp_data_dir)

        # Verify data is accessible
        result = empty_db.sql("SELECT COUNT(*) FROM symbols_master").fetchone()[0]
        assert result == 2

    def test_rollback_on_parquet_write_failure(self, empty_db, temp_data_dir, sample_snapshot_data):
        """Test that database changes are not committed if Parquet write fails."""
        # Setup test data similar to second_load test
        snapshot_df = pd.DataFrame(sample_snapshot_data)
        empty_db.execute("CREATE TABLE symbols_snapshot AS SELECT * FROM snapshot_df")

        # Create diff tables with some changes
        empty_db.execute(
            """
            CREATE TABLE diff_insert AS
            SELECT 'MSFT-NASDAQ' as natural_key, 'MSFT' as symbol, 'Microsoft Corp' as company_name,
                   'NASDAQ' as exchange, 'stock' as asset_type, 'active' as status,
                   3000000000000 as market_cap, 'Technology' as sector, 'Software' as industry,
                   'US' as country, 'USD' as currency, DATE '2024-01-15' as as_of
        """
        )

        empty_db.execute(
            """
            CREATE TABLE diff_update AS
            SELECT 1 as id, 'AAPL-NASDAQ' as natural_key, 'AAPL' as symbol, 'Apple Inc. (Updated)' as company_name,
                   'NASDAQ' as exchange, 'stock' as asset_type, 'active' as status,
                   3000000000000 as market_cap, 'Technology' as sector, 'Consumer Electronics' as industry,
                   'US' as country, 'USD' as currency, DATE '2024-01-15' as as_of
        """
        )

        empty_db.execute("CREATE TABLE diff_unchanged AS SELECT * FROM symbols_snapshot WHERE 1=0")

        # Attach symbols_master with initial data
        attach_symbols_master(empty_db, temp_data_dir)

        # Add some initial data to symbols_master to test rollback
        empty_db.execute(
            """
            INSERT INTO symbols_master 
            (id, natural_key, symbol, company_name, exchange, asset_type, status, market_cap, 
             sector, industry, country, currency, valid_from, valid_to, created_at, as_of)
            VALUES 
            (1, 'AAPL-NASDAQ', 'AAPL', 'Apple Inc.', 'NASDAQ', 'stock', 'active', 3000000000000,
             'Technology', 'Consumer Electronics', 'US', 'USD', '2024-01-15', NULL, CURRENT_TIMESTAMP, '2024-01-15'),
            (2, 'GOOGL-NASDAQ', 'GOOGL', 'Alphabet Inc.', 'NASDAQ', 'stock', 'active', 2000000000000,
             'Technology', 'Internet Software & Services', 'US', 'USD', '2024-01-15', NULL, CURRENT_TIMESTAMP, '2024-01-15')
        """
        )

        # Mock the write operation to fail
        with patch(
            "pyarrow.dataset.write_dataset", side_effect=Exception("Simulated write failure")
        ):
            # Attempt SCD update - should fail
            with pytest.raises(RuntimeError, match="Failed to write Parquet dataset"):
                run_scd_update(empty_db, temp_data_dir)

        # Verify that database state was not modified (diff tables should still exist and unchanged)
        insert_count = empty_db.sql("SELECT COUNT(*) FROM diff_insert").fetchone()[0]
        update_count = empty_db.sql("SELECT COUNT(*) FROM diff_update").fetchone()[0]

        # Should still have the original diff data
        assert insert_count == 1  # MSFT insert
        assert update_count == 1  # AAPL update

        # symbols_master should be unchanged (still has original data)
        master_count = empty_db.sql("SELECT COUNT(*) FROM symbols_master").fetchone()[0]
        assert master_count == 2  # Original AAPL and GOOGL

    def test_partition_column_collision_detection(self):
        """Test that partition column collisions are detected."""
        import pyarrow as pa

        # Create a table with 'year' column that would collide
        df = pd.DataFrame(
            {
                "valid_from": ["2024-01-15"],
                "year": [2024],  # This should cause collision
                "symbol": ["TEST"],
            }
        )
        table = pa.Table.from_pandas(df, preserve_index=False)

        with pytest.raises(
            ValueError,
            match="Data contains 'year' or 'month' columns that would conflict with partitioning",
        ):
            _add_partition_columns(table)
