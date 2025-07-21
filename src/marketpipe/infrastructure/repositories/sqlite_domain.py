# SPDX-License-Identifier: Apache-2.0
"""SQLite implementations of domain repositories.

Provides concrete implementations of domain repository interfaces using SQLite
with async aiosqlite for non-blocking operations.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import date
from pathlib import Path
from typing import Any, Optional

import aiosqlite

from marketpipe.domain.aggregates import SymbolBarsAggregate
from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.repositories import (
    ConcurrencyError,
    DuplicateKeyError,
    ICheckpointRepository,
    IOHLCVRepository,
    ISymbolBarsRepository,
    RepositoryError,
)
from marketpipe.domain.value_objects import Price, Symbol, TimeRange, Timestamp, Volume
from marketpipe.infrastructure.sqlite_async_mixin import SqliteAsyncMixin
from marketpipe.migrations import apply_pending


class SqliteSymbolBarsRepository(SqliteAsyncMixin, ISymbolBarsRepository):
    """SQLite implementation of ISymbolBarsRepository.

    Stores symbol bars aggregates with optimistic concurrency control.
    Uses aiosqlite for non-blocking async operations.
    """

    def __init__(self, db_path: str = "data/db/core.db"):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = str(self._db_path)  # Required by SqliteAsyncMixin
        # Apply migrations on first use
        apply_pending(self._db_path)

    async def get_by_symbol_and_date(
        self, symbol: Symbol, trading_date: date
    ) -> Optional[SymbolBarsAggregate]:
        """Load aggregate for symbol and trading date."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT symbol, trading_date, version, is_complete,
                           collection_started, bar_count, created_at, updated_at
                    FROM symbol_bars_aggregates
                    WHERE symbol = ? AND trading_date = ?
                """,
                    (symbol.value, trading_date.isoformat()),
                )

                row = await cursor.fetchone()
                if row is None:
                    return None

                # Create aggregate from stored data
                aggregate = SymbolBarsAggregate(symbol, trading_date)
                aggregate._version = row["version"]
                aggregate._is_complete = bool(row["is_complete"])
                aggregate._collection_started = bool(row["collection_started"])
                aggregate._bar_count = row["bar_count"]

                return aggregate
        except Exception as e:
            raise RepositoryError(f"Failed to get symbol bars aggregate: {e}") from e

    async def save(self, aggregate: SymbolBarsAggregate) -> None:
        """Save aggregate and publish domain events."""
        try:
            async with self._conn() as db:
                # Check for concurrency conflicts
                cursor = await db.execute(
                    """
                    SELECT version FROM symbol_bars_aggregates
                    WHERE symbol = ? AND trading_date = ?
                """,
                    (aggregate.symbol.value, aggregate.trading_date.isoformat()),
                )

                existing_row = await cursor.fetchone()
                if existing_row and existing_row[0] != aggregate.version - 1:
                    raise ConcurrencyError(
                        f"Aggregate has been modified by another process. "
                        f"Expected version {aggregate.version - 1}, found {existing_row[0]}"
                    )

                # Insert or update
                await db.execute(
                    """
                    INSERT OR REPLACE INTO symbol_bars_aggregates
                    (symbol, trading_date, version, is_complete, collection_started, bar_count, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                    (
                        aggregate.symbol.value,
                        aggregate.trading_date.isoformat(),
                        aggregate.version,
                        aggregate.is_complete,
                        aggregate._collection_started,
                        aggregate.bar_count,
                    ),
                )

                await db.commit()
        except ConcurrencyError:
            # Re-raise ConcurrencyError as-is
            raise
        except aiosqlite.IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                raise DuplicateKeyError(
                    f"Symbol bars aggregate already exists for {aggregate.symbol} on {aggregate.trading_date}"
                ) from e
            raise RepositoryError(f"Database integrity error: {e}") from e
        except Exception as e:
            raise RepositoryError(f"Failed to save symbol bars aggregate: {e}") from e

    async def find_symbols_with_data(self, start_date: date, end_date: date) -> list[Symbol]:
        """Find symbols that have data in the specified date range."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    """
                    SELECT DISTINCT symbol
                    FROM symbol_bars_aggregates
                    WHERE trading_date >= ? AND trading_date <= ?
                    ORDER BY symbol
                """,
                    (start_date.isoformat(), end_date.isoformat()),
                )

                rows = await cursor.fetchall()
                return [Symbol(row[0]) for row in rows]
        except Exception as e:
            raise RepositoryError(f"Failed to find symbols with data: {e}") from e

    async def get_completion_status(
        self, symbols: list[Symbol], trading_dates: list[date]
    ) -> dict[str, dict[str, bool]]:
        """Get completion status for symbol/date combinations."""
        try:
            result = {}
            async with self._conn() as db:
                symbol_placeholders = ",".join("?" * len(symbols))
                date_placeholders = ",".join("?" * len(trading_dates))

                cursor = await db.execute(
                    f"""
                    SELECT symbol, trading_date, is_complete
                    FROM symbol_bars_aggregates
                    WHERE symbol IN ({symbol_placeholders})
                    AND trading_date IN ({date_placeholders})
                """,
                    [s.value for s in symbols] + [d.isoformat() for d in trading_dates],
                )

                # Initialize result structure
                for symbol in symbols:
                    result[symbol.value] = {}
                    for trading_date in trading_dates:
                        result[symbol.value][trading_date.isoformat()] = False

                # Fill in actual completion status
                rows = await cursor.fetchall()
                for row in rows:
                    symbol_str, date_str, is_complete = row
                    result[symbol_str][date_str] = bool(is_complete)

            return result
        except Exception as e:
            raise RepositoryError(f"Failed to get completion status: {e}") from e

    async def delete(self, symbol: Symbol, trading_date: date) -> bool:
        """Delete aggregate for symbol and trading date."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    """
                    DELETE FROM symbol_bars_aggregates
                    WHERE symbol = ? AND trading_date = ?
                """,
                    (symbol.value, trading_date.isoformat()),
                )

                await db.commit()
                return cursor.rowcount > 0
        except Exception as e:
            raise RepositoryError(f"Failed to delete symbol bars aggregate: {e}") from e


class SqliteOHLCVRepository(SqliteAsyncMixin, IOHLCVRepository):
    """SQLite implementation of IOHLCVRepository.

    Stores individual OHLCV bars with efficient querying capabilities.
    Uses aiosqlite for non-blocking async operations.
    """

    def __init__(self, db_path: str = "data/db/core.db"):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = str(self._db_path)  # Required by SqliteAsyncMixin
        # Apply migrations on first use
        apply_pending(self._db_path)

    async def get_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange
    ) -> AsyncIterator[OHLCVBar]:
        """Stream bars for symbol in time range."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT id, symbol, timestamp_ns, open_price, high_price, low_price,
                           close_price, volume, trade_count, vwap
                    FROM ohlcv_bars
                    WHERE symbol = ? AND timestamp_ns >= ? AND timestamp_ns <= ?
                    ORDER BY timestamp_ns
                """,
                    (
                        symbol.value,
                        time_range.start.to_nanoseconds(),
                        time_range.end.to_nanoseconds(),
                    ),
                )

                async for row in cursor:
                    bar = self._row_to_ohlcv_bar(row)
                    yield bar
        except Exception as e:
            raise RepositoryError(f"Failed to get bars for symbol: {e}") from e

    async def get_bars_for_symbols(
        self, symbols: list[Symbol], time_range: TimeRange
    ) -> AsyncIterator[OHLCVBar]:
        """Stream bars for multiple symbols in time range."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                symbol_placeholders = ",".join("?" * len(symbols))

                cursor = await db.execute(
                    f"""
                    SELECT id, symbol, timestamp_ns, open_price, high_price, low_price,
                           close_price, volume, trade_count, vwap
                    FROM ohlcv_bars
                    WHERE symbol IN ({symbol_placeholders})
                    AND timestamp_ns >= ? AND timestamp_ns <= ?
                    ORDER BY timestamp_ns, symbol
                """,
                    [s.value for s in symbols]
                    + [time_range.start.to_nanoseconds(), time_range.end.to_nanoseconds()],
                )

                async for row in cursor:
                    bar = self._row_to_ohlcv_bar(row)
                    yield bar
        except Exception as e:
            raise RepositoryError(f"Failed to get bars for symbols: {e}") from e

    def _row_to_ohlcv_bar(self, row) -> OHLCVBar:
        """Convert database row to OHLCVBar entity."""
        return OHLCVBar(
            id=EntityId.from_string(row["id"]),
            symbol=Symbol(row["symbol"]),
            timestamp=Timestamp.from_nanoseconds(row["timestamp_ns"]),
            open_price=Price.from_float(row["open_price"]),
            high_price=Price.from_float(row["high_price"]),
            low_price=Price.from_float(row["low_price"]),
            close_price=Price.from_float(row["close_price"]),
            volume=Volume(row["volume"]),
            trade_count=row["trade_count"],
            vwap=Price.from_float(row["vwap"]) if row["vwap"] is not None else None,
        )

    async def save_bars(self, bars: list[OHLCVBar]) -> None:
        """Batch save multiple bars."""
        try:
            async with self._conn() as db:
                for bar in bars:
                    await db.execute(
                        """
                        INSERT INTO ohlcv_bars
                        (id, symbol, timestamp_ns, trading_date, open_price, high_price,
                         low_price, close_price, volume, trade_count, vwap)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            str(bar.id),
                            bar.symbol.value,
                            bar.timestamp_ns,
                            bar.timestamp.trading_date().isoformat(),
                            bar.open_price.to_float(),
                            bar.high_price.to_float(),
                            bar.low_price.to_float(),
                            bar.close_price.to_float(),
                            bar.volume.value,
                            bar.trade_count,
                            bar.vwap.to_float() if bar.vwap else None,
                        ),
                    )

                await db.commit()
        except aiosqlite.IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                raise DuplicateKeyError(
                    "One or more bars already exist for the given symbol/timestamp combinations"
                ) from e
            raise RepositoryError(f"Database integrity error: {e}") from e
        except Exception as e:
            raise RepositoryError(f"Failed to save bars: {e}") from e

    async def exists(self, symbol: Symbol, timestamp: Timestamp) -> bool:
        """Check if bar exists for symbol at timestamp."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    """
                    SELECT 1 FROM ohlcv_bars
                    WHERE symbol = ? AND timestamp_ns = ?
                """,
                    (symbol.value, timestamp.to_nanoseconds()),
                )

                row = await cursor.fetchone()
                return row is not None
        except Exception as e:
            raise RepositoryError(f"Failed to check if bar exists: {e}") from e

    async def count_bars(self, symbol: Symbol, time_range: Optional[TimeRange] = None) -> int:
        """Count bars for symbol in optional time range."""
        try:
            async with self._conn() as db:
                if time_range:
                    cursor = await db.execute(
                        """
                        SELECT COUNT(*) FROM ohlcv_bars
                        WHERE symbol = ? AND timestamp_ns >= ? AND timestamp_ns <= ?
                    """,
                        (
                            symbol.value,
                            time_range.start.to_nanoseconds(),
                            time_range.end.to_nanoseconds(),
                        ),
                    )
                else:
                    cursor = await db.execute(
                        """
                        SELECT COUNT(*) FROM ohlcv_bars
                        WHERE symbol = ?
                    """,
                        (symbol.value,),
                    )

                row = await cursor.fetchone()
                return row[0]
        except Exception as e:
            raise RepositoryError(f"Failed to count bars: {e}") from e

    async def get_latest_timestamp(self, symbol: Symbol) -> Optional[Timestamp]:
        """Get the latest timestamp for a symbol."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    """
                    SELECT MAX(timestamp_ns) FROM ohlcv_bars
                    WHERE symbol = ?
                """,
                    (symbol.value,),
                )

                row = await cursor.fetchone()
                result = row[0] if row else None
                return Timestamp.from_nanoseconds(result) if result is not None else None
        except Exception as e:
            raise RepositoryError(f"Failed to get latest timestamp: {e}") from e

    async def delete_bars(self, symbol: Symbol, time_range: Optional[TimeRange] = None) -> int:
        """Delete bars for symbol in optional time range."""
        try:
            async with self._conn() as db:
                if time_range:
                    cursor = await db.execute(
                        """
                        DELETE FROM ohlcv_bars
                        WHERE symbol = ? AND timestamp_ns >= ? AND timestamp_ns <= ?
                    """,
                        (
                            symbol.value,
                            time_range.start.to_nanoseconds(),
                            time_range.end.to_nanoseconds(),
                        ),
                    )
                else:
                    cursor = await db.execute(
                        """
                        DELETE FROM ohlcv_bars WHERE symbol = ?
                    """,
                        (symbol.value,),
                    )

                await db.commit()
                return cursor.rowcount
        except Exception as e:
            raise RepositoryError(f"Failed to delete bars: {e}") from e


class SqliteCheckpointRepository(SqliteAsyncMixin, ICheckpointRepository):
    """SQLite implementation of ICheckpointRepository.

    Stores ingestion checkpoints for resumable operations.
    Uses aiosqlite for non-blocking async operations.
    """

    def __init__(self, db_path: str = "data/db/core.db"):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = str(self._db_path)  # Required by SqliteAsyncMixin
        # Apply migrations on first use
        apply_pending(self._db_path)

    async def save_checkpoint(self, symbol: Symbol, checkpoint_data: dict[str, Any]) -> None:
        """Save checkpoint data for a symbol."""
        try:
            async with self._conn() as db:
                json_data = json.dumps(checkpoint_data)
                await db.execute(
                    """
                    INSERT OR REPLACE INTO checkpoints
                    (symbol, checkpoint_data, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """,
                    (symbol.value, json_data),
                )

                await db.commit()
        except Exception as e:
            raise RepositoryError(f"Failed to save checkpoint: {e}") from e

    async def get_checkpoint(self, symbol: Symbol) -> Optional[dict[str, Any]]:
        """Get checkpoint data for a symbol."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    """
                    SELECT checkpoint_data FROM checkpoints
                    WHERE symbol = ?
                """,
                    (symbol.value,),
                )

                row = await cursor.fetchone()
                if row:
                    return json.loads(row[0])
                return None
        except Exception as e:
            raise RepositoryError(f"Failed to get checkpoint: {e}") from e

    async def delete_checkpoint(self, symbol: Symbol) -> bool:
        """Delete checkpoint for a symbol."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    """
                    DELETE FROM checkpoints WHERE symbol = ?
                """,
                    (symbol.value,),
                )

                await db.commit()
                return cursor.rowcount > 0
        except Exception as e:
            raise RepositoryError(f"Failed to delete checkpoint: {e}") from e

    async def list_checkpoints(self) -> list[Symbol]:
        """List all symbols with checkpoints."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    """
                    SELECT symbol FROM checkpoints ORDER BY symbol
                """
                )

                rows = await cursor.fetchall()
                return [Symbol(row[0]) for row in rows]
        except Exception as e:
            raise RepositoryError(f"Failed to list checkpoints: {e}") from e


def _add_entityid_from_string():
    """Add from_string method to EntityId if not present."""
    if not hasattr(EntityId, "from_string"):

        @classmethod
        def from_string(cls, id_str: str) -> EntityId:
            from uuid import UUID

            return cls(UUID(id_str))

        EntityId.from_string = from_string


# Add the method on import
_add_entityid_from_string()
