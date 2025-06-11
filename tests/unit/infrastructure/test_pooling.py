"""Unit tests for the SQLite connection pooling system."""

from __future__ import annotations

import sqlite3
import threading
import time


from marketpipe.infrastructure.sqlite_pool import (
    connection,
    get_pool,
    close_all_pools,
    get_pool_stats,
    _init_conn,
)


def test_pool_reuse(tmp_path):
    """Test that connections are reused from the pool."""
    db = tmp_path / "test.db"

    # Clear any existing pools
    close_all_pools()

    conn1 = None
    conn2 = None

    # Get two connections sequentially
    with connection(db) as c1:
        conn1 = c1
        # Verify connection is configured correctly
        cursor = c1.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]
        assert journal_mode == "wal"

    with connection(db) as c2:
        conn2 = c2

    # Should reuse the same connection object
    assert conn1 is conn2

    # Pool should have one connection
    pool = get_pool(db)
    assert len(pool) == 1

    # Clean up
    close_all_pools()


def test_multiple_database_pools(tmp_path):
    """Test that different databases get separate pools."""
    db1 = tmp_path / "db1.db"
    db2 = tmp_path / "db2.db"

    # Clear any existing pools
    close_all_pools()

    with connection(db1) as c1:
        with connection(db2) as c2:
            # Should be different connections
            assert c1 is not c2

            # Should have separate pools
            pool1 = get_pool(db1)
            pool2 = get_pool(db2)
            assert pool1 is not pool2

            # Each pool should be empty while connections are in use
            assert len(pool1) == 0
            assert len(pool2) == 0

    # After context exits, pools should have connections
    assert len(get_pool(db1)) == 1
    assert len(get_pool(db2)) == 1

    # Clean up
    close_all_pools()


def test_connection_configuration(tmp_path):
    """Test that connections are properly configured with WAL mode and other settings."""
    db = tmp_path / "test.db"

    with connection(db) as conn:
        # Check journal mode
        cursor = conn.execute("PRAGMA journal_mode")
        assert cursor.fetchone()[0] == "wal"

        # Check busy timeout
        cursor = conn.execute("PRAGMA busy_timeout")
        timeout = cursor.fetchone()[0]
        assert timeout == 3000  # 3 seconds

        # Check synchronous mode
        cursor = conn.execute("PRAGMA synchronous")
        sync_mode = cursor.fetchone()[0]
        assert sync_mode == 1  # NORMAL

        # Check cache size
        cursor = conn.execute("PRAGMA cache_size")
        cache_size = cursor.fetchone()[0]
        assert cache_size == 10000

        # Check temp store
        cursor = conn.execute("PRAGMA temp_store")
        temp_store = cursor.fetchone()[0]
        assert temp_store == 2  # MEMORY


def test_concurrent_access(tmp_path):
    """Test that connection pool handles concurrent access correctly."""
    db = tmp_path / "test.db"
    close_all_pools()

    # Create test table
    with connection(db) as conn:
        conn.execute("CREATE TABLE test (id INTEGER, value TEXT)")
        conn.commit()

    results = []
    errors = []

    def worker(worker_id: int):
        try:
            with connection(db) as conn:
                # Insert data
                conn.execute(
                    "INSERT INTO test (id, value) VALUES (?, ?)",
                    (worker_id, f"worker_{worker_id}"),
                )
                conn.commit()

                # Read data
                cursor = conn.execute("SELECT COUNT(*) FROM test")
                count = cursor.fetchone()[0]
                results.append((worker_id, count))

        except Exception as e:
            errors.append((worker_id, str(e)))

    # Start multiple workers concurrently
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all to complete
    for thread in threads:
        thread.join()

    # Should have no errors
    assert len(errors) == 0, f"Errors occurred: {errors}"

    # Should have results from all workers
    assert len(results) == 5

    # Verify data was inserted correctly
    with connection(db) as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM test")
        assert cursor.fetchone()[0] == 5

    close_all_pools()


def test_pool_growth_under_load(tmp_path):
    """Test that pool creates new connections when all are in use."""
    db = tmp_path / "test.db"
    close_all_pools()

    # Create initial connection
    with connection(db) as conn:
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.commit()

    connections_in_use = []

    def hold_connection(duration: float):
        with connection(db) as conn:
            connections_in_use.append(conn)
            time.sleep(duration)
            connections_in_use.remove(conn)

    # Start multiple threads that hold connections
    threads = []
    for i in range(3):
        thread = threading.Thread(target=hold_connection, args=(0.5,))
        threads.append(thread)
        thread.start()

    # Give threads time to acquire connections
    time.sleep(0.1)

    # Should have multiple connections in use
    assert len(connections_in_use) > 1

    # Wait for threads to complete
    for thread in threads:
        thread.join()

    # Pool should have grown to accommodate concurrent access
    pool = get_pool(db)
    assert len(pool) >= 2

    close_all_pools()


def test_pool_stats(tmp_path):
    """Test pool statistics functionality."""
    db1 = tmp_path / "db1.db"
    db2 = tmp_path / "db2.db"
    close_all_pools()

    # Initially no pools
    stats = get_pool_stats()
    assert len(stats) == 0

    # Create connections to different databases
    with connection(db1) as c1:
        with connection(db2) as c2:
            pass  # Connections in use, pools should be empty

    # Check stats after connections returned to pools
    stats = get_pool_stats()
    assert len(stats) == 2
    assert all(count == 1 for count in stats.values())

    # Database paths should be in stats
    db_paths = set(stats.keys())
    assert str(db1) in db_paths
    assert str(db2) in db_paths

    close_all_pools()


def test_directory_creation(tmp_path):
    """Test that database directories are created automatically."""
    db = tmp_path / "deeply" / "nested" / "directory" / "test.db"

    # Directory doesn't exist initially
    assert not db.parent.exists()

    # Using connection should create the directory
    with connection(db) as conn:
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.commit()

    # Directory and database should now exist
    assert db.parent.exists()
    assert db.exists()


def test_close_all_pools(tmp_path):
    """Test that close_all_pools properly cleans up."""
    # Clear any existing pools first
    close_all_pools()

    db1 = tmp_path / "db1.db"
    db2 = tmp_path / "db2.db"

    # Create some connections
    with connection(db1) as c1:
        pass
    with connection(db2) as c2:
        pass

    # Should have pools
    stats = get_pool_stats()
    assert len(stats) == 2

    # Close pools
    close_all_pools()

    # Should be empty now
    stats = get_pool_stats()
    assert len(stats) == 0


def test_init_conn_configuration(tmp_path):
    """Test that _init_conn properly configures connections."""
    db = tmp_path / "test.db"

    conn = _init_conn(db)

    try:
        # Verify all the pragma settings
        cursor = conn.execute("PRAGMA journal_mode")
        assert cursor.fetchone()[0] == "wal"

        cursor = conn.execute("PRAGMA busy_timeout")
        assert cursor.fetchone()[0] == 3000

        cursor = conn.execute("PRAGMA synchronous")
        assert cursor.fetchone()[0] == 1  # NORMAL

        cursor = conn.execute("PRAGMA cache_size")
        assert cursor.fetchone()[0] == 10000

        cursor = conn.execute("PRAGMA temp_store")
        assert cursor.fetchone()[0] == 2  # MEMORY

        # Test that autocommit is enabled (isolation_level=None)
        conn.execute("CREATE TABLE test (id INTEGER)")
        # If autocommit is enabled, table should be created immediately
        cursor = conn.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='test'
        """
        )
        assert cursor.fetchone() is not None

    finally:
        conn.close()


def test_exception_handling_in_context(tmp_path):
    """Test that connections are properly returned to pool even when exceptions occur."""
    db = tmp_path / "test.db"
    close_all_pools()

    try:
        with connection(db) as conn:
            # This should cause an exception
            conn.execute("INVALID SQL SYNTAX")
    except sqlite3.OperationalError:
        pass  # Expected

    # Connection should still be returned to pool
    pool = get_pool(db)
    assert len(pool) == 1

    # Connection should still be usable
    with connection(db) as conn:
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.commit()

    close_all_pools()


def test_default_database_path():
    """Test that default database path works correctly."""
    # Test with default path
    with connection() as conn:
        # Should create default directory structure
        conn.execute("CREATE TABLE IF NOT EXISTS test_unique_12345 (id INTEGER)")
        conn.execute("INSERT INTO test_unique_12345 (id) VALUES (1)")

        # Verify it works
        cursor = conn.execute("SELECT COUNT(*) FROM test_unique_12345")
        assert cursor.fetchone()[0] == 1

    # Clean up
    with connection() as conn:
        conn.execute("DROP TABLE IF EXISTS test_unique_12345")
