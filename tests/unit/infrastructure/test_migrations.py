"""Unit tests for the SQLite migration system."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from marketpipe.migrations import apply_pending


def test_migrations_apply_once(tmp_path):
    """Test that migrations are applied once and are idempotent."""
    db = tmp_path / "core.db"

    # Apply migrations twice
    apply_pending(db)
    apply_pending(db)  # Should be idempotent

    # Check that both migrations were applied once
    with sqlite3.connect(db) as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM schema_version")
        version_count = cursor.fetchone()[0]
        assert version_count == 3

        # Check that the correct versions were applied
        cursor = conn.execute("SELECT version FROM schema_version ORDER BY version")
        versions = [row[0] for row in cursor.fetchall()]
        assert versions == ["001", "002", "003"]


def test_migrations_create_schema_version_table(tmp_path):
    """Test that schema_version table is created."""
    db = tmp_path / "core.db"

    apply_pending(db)

    with sqlite3.connect(db) as conn:
        # Check that schema_version table exists
        cursor = conn.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='schema_version'
        """
        )
        assert cursor.fetchone() is not None

        # Check table structure
        cursor = conn.execute("PRAGMA table_info(schema_version)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert "version" in columns
        assert "applied_ts" in columns
        assert columns["version"] == "TEXT"
        assert columns["applied_ts"] == "INTEGER"


def test_migrations_create_core_tables(tmp_path):
    """Test that core tables are created by migration 001."""
    db = tmp_path / "core.db"

    apply_pending(db)

    with sqlite3.connect(db) as conn:
        # Check that all expected tables exist
        cursor = conn.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """
        )
        tables = [row[0] for row in cursor.fetchall()]

        expected_tables = [
            "checkpoints",
            "metrics",
            "ohlcv_bars",
            "schema_version",
            "symbol_bars_aggregates",
        ]

        assert sorted(tables) == sorted(expected_tables)


def test_migrations_create_indexes(tmp_path):
    """Test that indexes are created by migrations."""
    db = tmp_path / "core.db"

    apply_pending(db)

    with sqlite3.connect(db) as conn:
        # Check that indexes exist
        cursor = conn.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='index' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """
        )
        indexes = [row[0] for row in cursor.fetchall()]

        # Should have metrics index from migration 002
        assert "idx_metrics_name_ts" in indexes
        assert "idx_symbol_bars_date" in indexes
        assert "idx_ohlcv_symbol_timestamp" in indexes


def test_migration_failure_rollback(tmp_path):
    """Test that migration failures are rolled back."""
    db = tmp_path / "core.db"

    # Create a broken migration file in a test-specific location
    test_migrations_dir = tmp_path / "test_migrations" / "versions"
    test_migrations_dir.mkdir(parents=True, exist_ok=True)
    bad_migration = test_migrations_dir / "999_broken.sql"

    try:
        # Write a broken migration
        bad_migration.write_text("INVALID SQL SYNTAX;")

        # Mock the migrations directory to point to our test location
        with patch("marketpipe.migrations.Path") as mock_path_class:
            # Create a mock Path instance that behaves like the original
            mock_path_instance = mock_path_class.return_value
            mock_path_instance.parent.mkdir.return_value = None

            # Mock the migrations module's Path(__file__).parent to return our test dir
            def path_side_effect(*args, **kwargs):
                if args and str(args[0]).endswith("__init__.py"):
                    # Return a path object that has our test migrations dir as parent/versions
                    mock_file_path = mock_path_class.return_value
                    mock_file_path.parent = tmp_path / "test_migrations"
                    return mock_file_path
                else:
                    # For the db_path, return the original Path behavior
                    return Path(*args, **kwargs)

            mock_path_class.side_effect = path_side_effect

            # This should fail
            with pytest.raises(RuntimeError, match="Migration 999 failed"):
                apply_pending(db)

        # Check that the broken migration was not recorded
        with sqlite3.connect(db) as conn:
            cursor = conn.execute(
                "SELECT version FROM schema_version WHERE version = '999'"
            )
            assert cursor.fetchone() is None
    finally:
        # Clean up
        if bad_migration.exists():
            bad_migration.unlink()


def test_empty_migrations_directory(tmp_path):
    """Test behavior when migrations directory is empty."""
    db = tmp_path / "core.db"

    # Create empty migrations directory
    test_migrations_dir = tmp_path / "empty_migrations" / "versions"
    test_migrations_dir.mkdir(parents=True)

    # Mock the glob method to return no files
    with patch("pathlib.Path.glob") as mock_glob:
        mock_glob.return_value = []  # Empty list simulates no migration files

        # Should not fail with empty directory
        apply_pending(db)

        # Schema version table should still be created
        with sqlite3.connect(db) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM schema_version")
            assert cursor.fetchone()[0] == 0


def test_partial_migrations_applied(tmp_path):
    """Test that only pending migrations are applied."""
    db = tmp_path / "core.db"

    # First apply migration 001 to create the base tables
    apply_pending(db)

    # Now manually remove migration 001 from the schema_version to simulate
    # that only 001 was applied previously
    with sqlite3.connect(db) as conn:
        # Clear the schema_version table and re-add only migration 001
        conn.execute("DELETE FROM schema_version")
        conn.execute(
            "INSERT INTO schema_version (version, applied_ts) VALUES (?, ?)",
            ("001", 1640995200),  # Mock timestamp
        )
        conn.commit()

    # Apply pending migrations (should only apply 002)
    apply_pending(db)

    # Check that both migrations are now recorded
    with sqlite3.connect(db) as conn:
        cursor = conn.execute("SELECT version FROM schema_version ORDER BY version")
        versions = [row[0] for row in cursor.fetchall()]
        assert versions == ["001", "002", "003"]


def test_migrations_with_nonexistent_database_dir(tmp_path):
    """Test that database directory is created if it doesn't exist."""
    db = tmp_path / "subdir" / "that" / "does" / "not" / "exist" / "core.db"

    # Should create directory structure
    apply_pending(db)

    assert db.exists()
    assert db.parent.exists()

    with sqlite3.connect(db) as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM schema_version")
        assert cursor.fetchone()[0] == 3


def test_concurrent_migration_application(tmp_path):
    """Test that migrations handle concurrent access safely."""
    import threading

    db = tmp_path / "core.db"
    errors = []

    def apply_migrations():
        try:
            apply_pending(db)
        except Exception as e:
            errors.append(e)

    # Start multiple threads trying to apply migrations
    threads = []
    for _ in range(3):
        thread = threading.Thread(target=apply_migrations)
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # At most one thread should have succeeded, others should handle gracefully
    assert len(errors) <= 2  # Some threads might fail due to db locks, that's ok

    # Check that migrations were applied correctly
    with sqlite3.connect(db) as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM schema_version")
        assert cursor.fetchone()[0] == 3
