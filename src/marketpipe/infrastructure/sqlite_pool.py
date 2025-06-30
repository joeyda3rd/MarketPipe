"""SQLite connection pool with WAL mode for improved concurrency.

Provides thread-safe connection pooling and automatic WAL mode setup
for better concurrent read/write performance across contexts.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

# Global state for connection pools
_lock = threading.Lock()
_pools: dict[str, list[sqlite3.Connection]] = {}

__all__ = ["connection", "get_pool", "close_all_pools", "get_pool_stats"]


def _init_conn(path: Path) -> sqlite3.Connection:
    """Initialize a new SQLite connection with optimal settings."""
    conn = sqlite3.connect(
        str(path), check_same_thread=False, isolation_level=None  # autocommit mode
    )

    # Enable WAL mode for better concurrency
    conn.execute("PRAGMA journal_mode=WAL;")

    # Set busy timeout to handle contention
    conn.execute("PRAGMA busy_timeout=3000;")  # 3 seconds

    # Optimize for performance
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA cache_size=10000;")
    conn.execute("PRAGMA temp_store=MEMORY;")

    logger.debug(f"Initialized SQLite connection for {path}")
    return conn


def get_pool(path: Path) -> list[sqlite3.Connection]:
    """Get or create connection pool for database path."""
    path_str = str(path)

    with _lock:
        if path_str not in _pools:
            # Create initial connection for the pool
            initial_conn = _init_conn(path)
            _pools[path_str] = [initial_conn]
            logger.info(f"Created new connection pool for {path}")

        return _pools[path_str]


@contextmanager
def connection(
    path: Path = Path("data/db/core.db"),
) -> Generator[sqlite3.Connection, None, None]:
    """Get a connection from the pool.

    Args:
        path: Path to SQLite database file

    Yields:
        sqlite3.Connection: Database connection with WAL mode enabled

    Example:
        with connection(Path("data/db/core.db")) as conn:
            cursor = conn.execute("SELECT * FROM table")
            rows = cursor.fetchall()
    """
    # Ensure database directory exists
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    pool = get_pool(path)

    # Get connection from pool or create new one
    with _lock:
        if pool:
            conn = pool.pop()
        else:
            conn = _init_conn(path)

    try:
        yield conn
    finally:
        # Return connection to pool
        with _lock:
            pool.append(conn)


def close_all_pools() -> None:
    """Close all connections in all pools. Used for testing/cleanup."""
    global _pools

    with _lock:
        for path_str, pool in _pools.items():
            for conn in pool:
                try:
                    conn.close()
                    logger.debug(f"Closed connection for {path_str}")
                except Exception as e:
                    logger.warning(f"Error closing connection for {path_str}: {e}")

        _pools.clear()
        logger.info("Closed all connection pools")


def get_pool_stats() -> dict[str, int]:
    """Get statistics about current connection pools."""
    with _lock:
        return {path: len(pool) for path, pool in _pools.items()}
