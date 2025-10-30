# SPDX-License-Identifier: Apache-2.0
"""Async SQLite mixin for non-blocking database operations.

Provides a base mixin class for repositories that need async SQLite access
without blocking the event loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import weakref
from collections.abc import AsyncIterator

import aiosqlite

# Per-event-loop locks to avoid "bound to different event loop" errors
_EVENT_LOOP_LOCKS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
    weakref.WeakKeyDictionary()
)


def _get_event_loop_lock() -> asyncio.Lock:
    """Get or create a lock for the current event loop."""
    try:
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    except RuntimeError:
        # Fallback: obtain (or create) a loop to use as a key
        loop = asyncio.get_event_loop()

    if loop not in _EVENT_LOOP_LOCKS:
        _EVENT_LOOP_LOCKS[loop] = asyncio.Lock()

    return _EVENT_LOOP_LOCKS[loop]


class SqliteAsyncMixin:
    """Mixin providing async SQLite connection management.

    Usage:
        class MyRepository(SqliteAsyncMixin):
            def __init__(self, db_path: str):
                self.db_path = db_path

            async def my_operation(self):
                async with self._conn() as db:
                    cursor = await db.execute("SELECT * FROM table")
                    rows = await cursor.fetchall()
                    return rows
    """

    db_path: str

    @contextlib.asynccontextmanager
    async def _conn(self) -> AsyncIterator[aiosqlite.Connection]:
        """Async context manager for SQLite connections.

        Provides a configured aiosqlite connection with:
        - WAL mode for better concurrent access
        - 30 second timeout for operations
        - Automatic connection cleanup

        Yields:
            aiosqlite.Connection: Configured database connection
        """
        db_lock = _get_event_loop_lock()

        async with db_lock:
            async with aiosqlite.connect(self.db_path, timeout=30) as db:
                # Enable WAL mode for better concurrency
                await db.execute("PRAGMA journal_mode=WAL;")

                # Set other performance optimizations
                await db.execute("PRAGMA synchronous=NORMAL;")
                await db.execute("PRAGMA cache_size=10000;")
                await db.execute("PRAGMA temp_store=MEMORY;")

                yield db
