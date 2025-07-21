"""Unit tests for symbol views.

Tests the DuckDB SQL views (v_symbol_history and v_symbol_latest) that provide
read-only access to the SCD-2 symbols_master dataset.
"""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from marketpipe.ingestion.normalizer.refresh_views import refresh


class TestSymbolViews:
    """Test suite for symbol view functionality."""

    @pytest.fixture
    def fixture_master(self) -> list[dict]:
        """Mock symbols_master table with SCD-2 structure.

        Returns:
            List of symbol records representing SCD-2 scenarios:
            - ID 1: One active row (no history)
            - ID 2: One closed row and one active row (with history)

        Expected view results:
            - v_symbol_latest: 2 rows (one per ID, only active)
            - v_symbol_history: 3 rows (all rows including history)
        """
        return [
            # ID 1: Single active record (no history)
            {
                "id": 1,
                "ticker": "AAPL",
                "name": "Apple Inc.",
                "exchange_mic": "XNAS",
                "figi": "BBG000B9XRY4",
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "320193",
                "lei": None,
                "sic": "3571",
                "country": "US",
                "industry": "Technology",
                "sector": "Consumer Electronics",
                "market_cap": 3000000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-15T10:00:00Z",
                "type": "CS",
                "first_trade_date": date(1980, 12, 12),
                "provider": "alpaca",
                "as_of": date(2024, 1, 15),
                "natural_key": "BBG000B9XRY4",
                "valid_from": date(2024, 1, 15),
                "valid_to": None,  # Active record
            },
            # ID 2: Historical record (closed)
            {
                "id": 2,
                "ticker": "GOOGL",
                "name": "Alphabet Inc. (Old Name)",
                "exchange_mic": "XNAS",
                "figi": "BBG009S39JX6",
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "1652044",
                "lei": None,
                "sic": "7370",
                "country": "US",
                "industry": "Technology",
                "sector": "Internet Services",
                "market_cap": 1700000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-10T14:00:00Z",
                "type": "CS",
                "first_trade_date": date(2004, 8, 19),
                "provider": "polygon",
                "as_of": date(2024, 1, 10),
                "natural_key": "BBG009S39JX6",
                "valid_from": date(2024, 1, 10),
                "valid_to": date(2024, 1, 15),  # Closed on 2024-01-15
            },
            # ID 2: Current active record (supersedes above)
            {
                "id": 2,
                "ticker": "GOOGL",
                "name": "Alphabet Inc.",
                "exchange_mic": "XNAS",
                "figi": "BBG009S39JX6",
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "1652044",
                "lei": None,
                "sic": "7370",
                "country": "US",
                "industry": "Technology",
                "sector": "Internet Services",
                "market_cap": 1800000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-15T14:00:00Z",
                "type": "CS",
                "first_trade_date": date(2004, 8, 19),
                "provider": "alpaca",
                "as_of": date(2024, 1, 15),
                "natural_key": "BBG009S39JX6",
                "valid_from": date(2024, 1, 15),
                "valid_to": None,  # Active record
            },
        ]

    def _setup_symbols_master_table(
        self, conn: duckdb.DuckDBPyConnection, rows: list[dict]
    ) -> None:
        """Create and populate symbols_master table with fixture data."""
        import pandas as pd

        # Drop existing table if it exists
        conn.execute("DROP TABLE IF EXISTS symbols_master")

        # Convert to DataFrame and register as table
        df = pd.DataFrame(rows)
        conn.register("symbols_master", df)

    def test_latest_view_rowcount(self, fixture_master: list[dict]) -> None:
        """Test that v_symbol_latest returns exactly one row per ID."""
        conn = duckdb.connect(":memory:")

        # Setup data and create views
        self._setup_symbols_master_table(conn, fixture_master)
        refresh(":memory:", connection=conn)

        # Test: Should return 2 rows (one per unique ID)
        result = conn.execute("SELECT COUNT(*) FROM v_symbol_latest").fetchone()
        assert result[0] == 2, "v_symbol_latest should return exactly 2 rows"

        conn.close()

    def test_latest_view_uniqueness(self, fixture_master: list[dict]) -> None:
        """Test that v_symbol_latest has exactly one row per ID."""
        conn = duckdb.connect(":memory:")

        # Setup data and create views
        self._setup_symbols_master_table(conn, fixture_master)
        refresh(":memory:", connection=conn)

        # Test: Each ID should appear exactly once
        result = conn.execute(
            """
            SELECT id, COUNT(*) as cnt
            FROM v_symbol_latest
            GROUP BY id
            ORDER BY id
        """
        ).fetchall()

        assert len(result) == 2, "Should have entries for 2 distinct IDs"
        assert all(row[1] == 1 for row in result), "Each ID should appear exactly once"
        assert [row[0] for row in result] == [1, 2], "Should have IDs 1 and 2"

        conn.close()

    def test_history_contains_all(self, fixture_master: list[dict]) -> None:
        """Test that v_symbol_history contains all rows from symbols_master."""
        conn = duckdb.connect(":memory:")

        # Setup data and create views
        self._setup_symbols_master_table(conn, fixture_master)
        refresh(":memory:", connection=conn)

        # Test: Should return all 3 rows from fixture
        history_count = conn.execute("SELECT COUNT(*) FROM v_symbol_history").fetchone()[0]
        master_count = conn.execute("SELECT COUNT(*) FROM symbols_master").fetchone()[0]

        assert history_count == 3, "v_symbol_history should return all 3 fixture rows"
        assert (
            history_count == master_count
        ), "v_symbol_history should mirror symbols_master row count"

        conn.close()

    def test_valid_to_null_only_in_latest(self, fixture_master: list[dict]) -> None:
        """Test that v_symbol_latest contains only rows with valid_to IS NULL."""
        conn = duckdb.connect(":memory:")

        # Setup data and create views
        self._setup_symbols_master_table(conn, fixture_master)
        refresh(":memory:", connection=conn)

        # Test: All rows in latest view should have valid_to IS NULL
        result = conn.execute(
            """
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN valid_to IS NULL THEN 1 END) as null_count
            FROM v_symbol_latest
        """
        ).fetchone()

        total_rows, null_count = result
        assert total_rows == null_count, "All rows in v_symbol_latest should have valid_to IS NULL"
        assert total_rows == 2, "Should have exactly 2 active rows"

        conn.close()

    def test_latest_view_returns_most_recent_valid_from(self, fixture_master: list[dict]) -> None:
        """Test that v_symbol_latest returns the row with most recent valid_from for each ID."""
        conn = duckdb.connect(":memory:")

        # Setup data and create views
        self._setup_symbols_master_table(conn, fixture_master)
        refresh(":memory:", connection=conn)

        # Test: For ID 2, should return the row with valid_from = 2024-01-15 (not 2024-01-10)
        result = conn.execute(
            """
            SELECT id, valid_from, name
            FROM v_symbol_latest
            WHERE id = 2
        """
        ).fetchone()

        assert result is not None, "Should find row for ID 2"
        assert result[1] == date(2024, 1, 15), "Should return most recent valid_from date"
        assert result[2] == "Alphabet Inc.", "Should return current name, not old name"

        conn.close()

    def test_view_idempotence(self, fixture_master: list[dict]) -> None:
        """Test that running refresh multiple times produces identical results."""
        conn = duckdb.connect(":memory:")

        # Setup data and create views
        self._setup_symbols_master_table(conn, fixture_master)

        # Run refresh twice
        refresh(":memory:", connection=conn)
        first_latest_count = conn.execute("SELECT COUNT(*) FROM v_symbol_latest").fetchone()[0]
        first_history_count = conn.execute("SELECT COUNT(*) FROM v_symbol_history").fetchone()[0]

        refresh(":memory:", connection=conn)
        second_latest_count = conn.execute("SELECT COUNT(*) FROM v_symbol_latest").fetchone()[0]
        second_history_count = conn.execute("SELECT COUNT(*) FROM v_symbol_history").fetchone()[0]

        # Test: Results should be identical
        assert first_latest_count == second_latest_count, "v_symbol_latest count should be stable"
        assert (
            first_history_count == second_history_count
        ), "v_symbol_history count should be stable"

        conn.close()

    def test_downstream_join_sanity(self, fixture_master: list[dict]) -> None:
        """Test basic join between latest and history views for sanity."""
        conn = duckdb.connect(":memory:")

        # Setup data and create views
        self._setup_symbols_master_table(conn, fixture_master)
        refresh(":memory:", connection=conn)

        # Test: Join latest with history should return at least as many rows as latest
        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM v_symbol_latest l
            JOIN v_symbol_history h USING (id)
        """
        ).fetchone()

        latest_count = conn.execute("SELECT COUNT(*) FROM v_symbol_latest").fetchone()[0]

        assert result[0] >= latest_count, "Join should return at least as many rows as latest view"

        conn.close()

    def test_missing_symbols_master_table(self) -> None:
        """Test that refresh fails gracefully when symbols_master doesn't exist."""
        conn = duckdb.connect(":memory:")

        # Test: Should raise RuntimeError when table doesn't exist
        with pytest.raises(RuntimeError, match="symbols_master table not found"):
            refresh(":memory:", connection=conn)

        conn.close()

    def test_sql_file_path_resolution(self) -> None:
        """Test that the SQL file is found correctly relative to the Python module."""
        # This test verifies the Path(__file__).with_name() logic works
        from marketpipe.ingestion.normalizer.refresh_views import refresh

        # Create minimal test - if SQL file doesn't exist, should raise FileNotFoundError
        # If it does exist, should raise RuntimeError (missing symbols_master table)
        conn = duckdb.connect(":memory:")

        try:
            refresh(":memory:", connection=conn)
            raise AssertionError("Should have raised an exception")
        except FileNotFoundError:
            raise AssertionError("SQL file should exist") from None
        except RuntimeError as e:
            assert "symbols_master table not found" in str(
                e
            ), "Should fail on missing table, not missing file"

        conn.close()

    def test_performance_view_creation_speed(self, fixture_master: list[dict]) -> None:
        """Test that view creation completes quickly (metadata-only operation)."""
        import time

        conn = duckdb.connect(":memory:")
        self._setup_symbols_master_table(conn, fixture_master)

        # Test: View creation should be very fast (< 200ms as per requirements)
        start_time = time.perf_counter()
        refresh(":memory:", connection=conn)
        duration = time.perf_counter() - start_time

        assert duration < 0.2, f"View creation took {duration:.3f}s, should be < 0.2s"

        conn.close()
