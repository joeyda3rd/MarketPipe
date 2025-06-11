# SPDX-License-Identifier: Apache-2.0
"""SQLite implementations of domain repositories.

Provides concrete implementations of domain repository interfaces using SQLite
with both async (aiosqlite) and sync (sqlite3) support for flexibility.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import List, Optional, Dict, Any, AsyncIterator

try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

from marketpipe.domain.repositories import (
    ISymbolBarsRepository,
    IOHLCVRepository,
    ICheckpointRepository,
    RepositoryError,
    ConcurrencyError,
    DuplicateKeyError,
)
from marketpipe.domain.entities import OHLCVBar, EntityId
from marketpipe.domain.value_objects import Symbol, Timestamp, Price, Volume, TimeRange
from marketpipe.domain.aggregates import SymbolBarsAggregate
from marketpipe.infrastructure.sqlite_pool import connection
from marketpipe.migrations import apply_pending


class SqliteSymbolBarsRepository(ISymbolBarsRepository):
    """SQLite implementation of ISymbolBarsRepository.

    Stores symbol bars aggregates with optimistic concurrency control.
    Uses SQLite for persistence with async support when available.
    """

    def __init__(self, db_path: str = "data/db/core.db"):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        # Apply migrations on first use
        apply_pending(self._db_path)

    async def get_by_symbol_and_date(
        self, symbol: Symbol, trading_date: date
    ) -> Optional[SymbolBarsAggregate]:
        """Load aggregate for symbol and trading date."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_get_by_symbol_and_date(symbol, trading_date)
            else:
                return self._sync_get_by_symbol_and_date(symbol, trading_date)
        except Exception as e:
            raise RepositoryError(f"Failed to get symbol bars aggregate: {e}") from e

    def _sync_get_by_symbol_and_date(
        self, symbol: Symbol, trading_date: date
    ) -> Optional[SymbolBarsAggregate]:
        """Synchronous implementation."""
        with connection(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT symbol, trading_date, version, is_complete, collection_started, bar_count
                FROM symbol_bars_aggregates
                WHERE symbol = ? AND trading_date = ?
            """,
                (symbol.value, trading_date.isoformat()),
            )

            row = cursor.fetchone()
            if not row:
                return None

            # Create aggregate and set internal state
            aggregate = SymbolBarsAggregate(symbol, trading_date)
            aggregate._version = row["version"]
            aggregate._is_complete = bool(row["is_complete"])
            aggregate._collection_started = bool(row["collection_started"])

            return aggregate

    async def _async_get_by_symbol_and_date(
        self, symbol: Symbol, trading_date: date
    ) -> Optional[SymbolBarsAggregate]:
        """Asynchronous implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(
                """
                SELECT symbol, trading_date, version, is_complete, collection_started, bar_count
                FROM symbol_bars_aggregates
                WHERE symbol = ? AND trading_date = ?
            """,
                (symbol.value, trading_date.isoformat()),
            )

            row = await cursor.fetchone()
            if not row:
                return None

            # Create aggregate and set internal state
            aggregate = SymbolBarsAggregate(symbol, trading_date)
            aggregate._version = row["version"]
            aggregate._is_complete = bool(row["is_complete"])
            aggregate._collection_started = bool(row["collection_started"])

            return aggregate

    async def save(self, aggregate: SymbolBarsAggregate) -> None:
        """Save aggregate and publish domain events."""
        try:
            if AIOSQLITE_AVAILABLE:
                await self._async_save(aggregate)
            else:
                self._sync_save(aggregate)
        except ConcurrencyError:
            # Re-raise ConcurrencyError as-is
            raise
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                raise DuplicateKeyError(
                    f"Symbol bars aggregate already exists for {aggregate.symbol} on {aggregate.trading_date}"
                ) from e
            raise RepositoryError(f"Database integrity error: {e}") from e
        except Exception as e:
            raise RepositoryError(f"Failed to save symbol bars aggregate: {e}") from e

    def _sync_save(self, aggregate: SymbolBarsAggregate) -> None:
        """Synchronous save implementation."""
        with connection(self._db_path) as conn:
            # Check for concurrency conflicts
            cursor = conn.execute(
                """
                SELECT version FROM symbol_bars_aggregates
                WHERE symbol = ? AND trading_date = ?
            """,
                (aggregate.symbol.value, aggregate.trading_date.isoformat()),
            )

            existing_row = cursor.fetchone()
            if existing_row and existing_row[0] != aggregate.version - 1:
                raise ConcurrencyError(
                    f"Aggregate has been modified by another process. "
                    f"Expected version {aggregate.version - 1}, found {existing_row[0]}"
                )

            # Insert or update
            conn.execute(
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

            conn.commit()

    async def _async_save(self, aggregate: SymbolBarsAggregate) -> None:
        """Asynchronous save implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            # Check for concurrency conflicts
            cursor = await conn.execute(
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
            await conn.execute(
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

            await conn.commit()

    async def find_symbols_with_data(
        self, start_date: date, end_date: date
    ) -> List[Symbol]:
        """Find symbols that have data in the specified date range."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_find_symbols_with_data(start_date, end_date)
            else:
                return self._sync_find_symbols_with_data(start_date, end_date)
        except Exception as e:
            raise RepositoryError(f"Failed to find symbols with data: {e}") from e

    def _sync_find_symbols_with_data(
        self, start_date: date, end_date: date
    ) -> List[Symbol]:
        """Synchronous implementation."""
        with connection(self._db_path) as conn:
            cursor = conn.execute(
                """
                SELECT DISTINCT symbol
                FROM symbol_bars_aggregates
                WHERE trading_date >= ? AND trading_date <= ?
                ORDER BY symbol
            """,
                (start_date.isoformat(), end_date.isoformat()),
            )

            return [Symbol(row[0]) for row in cursor.fetchall()]

    async def _async_find_symbols_with_data(
        self, start_date: date, end_date: date
    ) -> List[Symbol]:
        """Asynchronous implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            cursor = await conn.execute(
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

    async def get_completion_status(
        self, symbols: List[Symbol], trading_dates: List[date]
    ) -> Dict[str, Dict[str, bool]]:
        """Get completion status for symbol/date combinations."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_get_completion_status(symbols, trading_dates)
            else:
                return self._sync_get_completion_status(symbols, trading_dates)
        except Exception as e:
            raise RepositoryError(f"Failed to get completion status: {e}") from e

    def _sync_get_completion_status(
        self, symbols: List[Symbol], trading_dates: List[date]
    ) -> Dict[str, Dict[str, bool]]:
        """Synchronous implementation."""
        result = {}

        with connection(self._db_path) as conn:
            symbol_placeholders = ",".join("?" * len(symbols))
            date_placeholders = ",".join("?" * len(trading_dates))

            cursor = conn.execute(
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
            for row in cursor.fetchall():
                symbol_str, date_str, is_complete = row
                result[symbol_str][date_str] = bool(is_complete)

        return result

    async def _async_get_completion_status(
        self, symbols: List[Symbol], trading_dates: List[date]
    ) -> Dict[str, Dict[str, bool]]:
        """Asynchronous implementation."""
        result = {}

        async with aiosqlite.connect(self._db_path) as conn:
            symbol_placeholders = ",".join("?" * len(symbols))
            date_placeholders = ",".join("?" * len(trading_dates))

            cursor = await conn.execute(
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

    async def delete(self, symbol: Symbol, trading_date: date) -> bool:
        """Delete aggregate for symbol and trading date."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_delete(symbol, trading_date)
            else:
                return self._sync_delete(symbol, trading_date)
        except Exception as e:
            raise RepositoryError(f"Failed to delete symbol bars aggregate: {e}") from e

    def _sync_delete(self, symbol: Symbol, trading_date: date) -> bool:
        """Synchronous delete implementation."""
        with connection(self._db_path) as conn:
            cursor = conn.execute(
                """
                DELETE FROM symbol_bars_aggregates
                WHERE symbol = ? AND trading_date = ?
            """,
                (symbol.value, trading_date.isoformat()),
            )

            conn.commit()
            return cursor.rowcount > 0

    async def _async_delete(self, symbol: Symbol, trading_date: date) -> bool:
        """Asynchronous delete implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            cursor = await conn.execute(
                """
                DELETE FROM symbol_bars_aggregates
                WHERE symbol = ? AND trading_date = ?
            """,
                (symbol.value, trading_date.isoformat()),
            )

            await conn.commit()
            return cursor.rowcount > 0


class SqliteOHLCVRepository(IOHLCVRepository):
    """SQLite implementation of IOHLCVRepository.

    Stores individual OHLCV bars with efficient querying capabilities.
    """

    def __init__(self, db_path: str = "data/db/core.db"):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        # Apply migrations on first use
        apply_pending(self._db_path)

    async def get_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange
    ) -> AsyncIterator[OHLCVBar]:
        """Stream bars for symbol in time range."""
        try:
            if AIOSQLITE_AVAILABLE:
                async for bar in self._async_get_bars_for_symbol(symbol, time_range):
                    yield bar
            else:
                for bar in self._sync_get_bars_for_symbol(symbol, time_range):
                    yield bar
        except Exception as e:
            raise RepositoryError(f"Failed to get bars for symbol: {e}") from e

    def _sync_get_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange
    ) -> List[OHLCVBar]:
        """Synchronous implementation."""
        bars = []
        with connection(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
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

            for row in cursor:
                bar = self._row_to_ohlcv_bar(row)
                bars.append(bar)

        return bars

    async def _async_get_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange
    ) -> AsyncIterator[OHLCVBar]:
        """Asynchronous implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(
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

    async def get_bars_for_symbols(
        self, symbols: List[Symbol], time_range: TimeRange
    ) -> AsyncIterator[OHLCVBar]:
        """Stream bars for multiple symbols in time range."""
        try:
            if AIOSQLITE_AVAILABLE:
                async for bar in self._async_get_bars_for_symbols(symbols, time_range):
                    yield bar
            else:
                for bar in self._sync_get_bars_for_symbols(symbols, time_range):
                    yield bar
        except Exception as e:
            raise RepositoryError(f"Failed to get bars for symbols: {e}") from e

    def _sync_get_bars_for_symbols(
        self, symbols: List[Symbol], time_range: TimeRange
    ) -> List[OHLCVBar]:
        """Synchronous implementation."""
        bars = []
        with connection(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            symbol_placeholders = ",".join("?" * len(symbols))

            cursor = conn.execute(
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

            for row in cursor:
                bar = self._row_to_ohlcv_bar(row)
                bars.append(bar)

        return bars

    async def _async_get_bars_for_symbols(
        self, symbols: List[Symbol], time_range: TimeRange
    ) -> AsyncIterator[OHLCVBar]:
        """Asynchronous implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            conn.row_factory = aiosqlite.Row
            symbol_placeholders = ",".join("?" * len(symbols))

            cursor = await conn.execute(
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

    async def save_bars(self, bars: List[OHLCVBar]) -> None:
        """Batch save multiple bars."""
        try:
            if AIOSQLITE_AVAILABLE:
                await self._async_save_bars(bars)
            else:
                self._sync_save_bars(bars)
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                raise DuplicateKeyError(
                    "One or more bars already exist for the given symbol/timestamp combinations"
                ) from e
            raise RepositoryError(f"Database integrity error: {e}") from e
        except Exception as e:
            raise RepositoryError(f"Failed to save bars: {e}") from e

    def _sync_save_bars(self, bars: List[OHLCVBar]) -> None:
        """Synchronous save implementation."""
        with connection(self._db_path) as conn:
            for bar in bars:
                conn.execute(
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

            conn.commit()

    async def _async_save_bars(self, bars: List[OHLCVBar]) -> None:
        """Asynchronous save implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            for bar in bars:
                await conn.execute(
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

            await conn.commit()

    async def exists(self, symbol: Symbol, timestamp: Timestamp) -> bool:
        """Check if bar exists for symbol at timestamp."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_exists(symbol, timestamp)
            else:
                return self._sync_exists(symbol, timestamp)
        except Exception as e:
            raise RepositoryError(f"Failed to check if bar exists: {e}") from e

    def _sync_exists(self, symbol: Symbol, timestamp: Timestamp) -> bool:
        """Synchronous exists check."""
        with connection(self._db_path) as conn:
            cursor = conn.execute(
                """
                SELECT 1 FROM ohlcv_bars
                WHERE symbol = ? AND timestamp_ns = ?
            """,
                (symbol.value, timestamp.to_nanoseconds()),
            )

            return cursor.fetchone() is not None

    async def _async_exists(self, symbol: Symbol, timestamp: Timestamp) -> bool:
        """Asynchronous exists check."""
        async with aiosqlite.connect(self._db_path) as conn:
            cursor = await conn.execute(
                """
                SELECT 1 FROM ohlcv_bars
                WHERE symbol = ? AND timestamp_ns = ?
            """,
                (symbol.value, timestamp.to_nanoseconds()),
            )

            row = await cursor.fetchone()
            return row is not None

    async def count_bars(
        self, symbol: Symbol, time_range: Optional[TimeRange] = None
    ) -> int:
        """Count bars for symbol in optional time range."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_count_bars(symbol, time_range)
            else:
                return self._sync_count_bars(symbol, time_range)
        except Exception as e:
            raise RepositoryError(f"Failed to count bars: {e}") from e

    def _sync_count_bars(
        self, symbol: Symbol, time_range: Optional[TimeRange] = None
    ) -> int:
        """Synchronous count implementation."""
        with connection(self._db_path) as conn:
            if time_range:
                cursor = conn.execute(
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
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) FROM ohlcv_bars
                    WHERE symbol = ?
                """,
                    (symbol.value,),
                )

            return cursor.fetchone()[0]

    async def _async_count_bars(
        self, symbol: Symbol, time_range: Optional[TimeRange] = None
    ) -> int:
        """Asynchronous count implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            if time_range:
                cursor = await conn.execute(
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
                cursor = await conn.execute(
                    """
                    SELECT COUNT(*) FROM ohlcv_bars
                    WHERE symbol = ?
                """,
                    (symbol.value,),
                )

            row = await cursor.fetchone()
            return row[0]

    async def get_latest_timestamp(self, symbol: Symbol) -> Optional[Timestamp]:
        """Get the latest timestamp for a symbol."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_get_latest_timestamp(symbol)
            else:
                return self._sync_get_latest_timestamp(symbol)
        except Exception as e:
            raise RepositoryError(f"Failed to get latest timestamp: {e}") from e

    def _sync_get_latest_timestamp(self, symbol: Symbol) -> Optional[Timestamp]:
        """Synchronous implementation."""
        with connection(self._db_path) as conn:
            cursor = conn.execute(
                """
                SELECT MAX(timestamp_ns) FROM ohlcv_bars
                WHERE symbol = ?
            """,
                (symbol.value,),
            )

            result = cursor.fetchone()[0]
            return Timestamp.from_nanoseconds(result) if result is not None else None

    async def _async_get_latest_timestamp(self, symbol: Symbol) -> Optional[Timestamp]:
        """Asynchronous implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            cursor = await conn.execute(
                """
                SELECT MAX(timestamp_ns) FROM ohlcv_bars
                WHERE symbol = ?
            """,
                (symbol.value,),
            )

            row = await cursor.fetchone()
            result = row[0] if row else None
            return Timestamp.from_nanoseconds(result) if result is not None else None

    async def delete_bars(
        self, symbol: Symbol, time_range: Optional[TimeRange] = None
    ) -> int:
        """Delete bars for symbol in optional time range."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_delete_bars(symbol, time_range)
            else:
                return self._sync_delete_bars(symbol, time_range)
        except Exception as e:
            raise RepositoryError(f"Failed to delete bars: {e}") from e

    def _sync_delete_bars(
        self, symbol: Symbol, time_range: Optional[TimeRange] = None
    ) -> int:
        """Synchronous delete implementation."""
        with connection(self._db_path) as conn:
            if time_range:
                cursor = conn.execute(
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
                cursor = conn.execute(
                    """
                    DELETE FROM ohlcv_bars WHERE symbol = ?
                """,
                    (symbol.value,),
                )

            conn.commit()
            return cursor.rowcount

    async def _async_delete_bars(
        self, symbol: Symbol, time_range: Optional[TimeRange] = None
    ) -> int:
        """Asynchronous delete implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            if time_range:
                cursor = await conn.execute(
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
                cursor = await conn.execute(
                    """
                    DELETE FROM ohlcv_bars WHERE symbol = ?
                """,
                    (symbol.value,),
                )

            await conn.commit()
            return cursor.rowcount


class SqliteCheckpointRepository(ICheckpointRepository):
    """SQLite implementation of ICheckpointRepository.

    Stores ingestion checkpoints for resumable operations.
    """

    def __init__(self, db_path: str = "data/db/core.db"):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        # Apply migrations on first use
        apply_pending(self._db_path)

    async def save_checkpoint(
        self, symbol: Symbol, checkpoint_data: Dict[str, Any]
    ) -> None:
        """Save checkpoint data for a symbol."""
        try:
            if AIOSQLITE_AVAILABLE:
                await self._async_save_checkpoint(symbol, checkpoint_data)
            else:
                self._sync_save_checkpoint(symbol, checkpoint_data)
        except Exception as e:
            raise RepositoryError(f"Failed to save checkpoint: {e}") from e

    def _sync_save_checkpoint(
        self, symbol: Symbol, checkpoint_data: Dict[str, Any]
    ) -> None:
        """Synchronous save implementation."""
        with connection(self._db_path) as conn:
            json_data = json.dumps(checkpoint_data)
            conn.execute(
                """
                INSERT OR REPLACE INTO checkpoints 
                (symbol, checkpoint_data, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
                (symbol.value, json_data),
            )

            conn.commit()

    async def _async_save_checkpoint(
        self, symbol: Symbol, checkpoint_data: Dict[str, Any]
    ) -> None:
        """Asynchronous save implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            json_data = json.dumps(checkpoint_data)
            await conn.execute(
                """
                INSERT OR REPLACE INTO checkpoints 
                (symbol, checkpoint_data, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
                (symbol.value, json_data),
            )

            await conn.commit()

    async def get_checkpoint(self, symbol: Symbol) -> Optional[Dict[str, Any]]:
        """Get checkpoint data for a symbol."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_get_checkpoint(symbol)
            else:
                return self._sync_get_checkpoint(symbol)
        except Exception as e:
            raise RepositoryError(f"Failed to get checkpoint: {e}") from e

    def _sync_get_checkpoint(self, symbol: Symbol) -> Optional[Dict[str, Any]]:
        """Synchronous get implementation."""
        with connection(self._db_path) as conn:
            cursor = conn.execute(
                """
                SELECT checkpoint_data FROM checkpoints
                WHERE symbol = ?
            """,
                (symbol.value,),
            )

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
            return None

    async def _async_get_checkpoint(self, symbol: Symbol) -> Optional[Dict[str, Any]]:
        """Asynchronous get implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            cursor = await conn.execute(
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

    async def delete_checkpoint(self, symbol: Symbol) -> bool:
        """Delete checkpoint for a symbol."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_delete_checkpoint(symbol)
            else:
                return self._sync_delete_checkpoint(symbol)
        except Exception as e:
            raise RepositoryError(f"Failed to delete checkpoint: {e}") from e

    def _sync_delete_checkpoint(self, symbol: Symbol) -> bool:
        """Synchronous delete implementation."""
        with connection(self._db_path) as conn:
            cursor = conn.execute(
                """
                DELETE FROM checkpoints WHERE symbol = ?
            """,
                (symbol.value,),
            )

            conn.commit()
            return cursor.rowcount > 0

    async def _async_delete_checkpoint(self, symbol: Symbol) -> bool:
        """Asynchronous delete implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            cursor = await conn.execute(
                """
                DELETE FROM checkpoints WHERE symbol = ?
            """,
                (symbol.value,),
            )

            await conn.commit()
            return cursor.rowcount > 0

    async def list_checkpoints(self) -> List[Symbol]:
        """List all symbols with checkpoints."""
        try:
            if AIOSQLITE_AVAILABLE:
                return await self._async_list_checkpoints()
            else:
                return self._sync_list_checkpoints()
        except Exception as e:
            raise RepositoryError(f"Failed to list checkpoints: {e}") from e

    def _sync_list_checkpoints(self) -> List[Symbol]:
        """Synchronous list implementation."""
        with connection(self._db_path) as conn:
            cursor = conn.execute(
                """
                SELECT symbol FROM checkpoints ORDER BY symbol
            """
            )

            return [Symbol(row[0]) for row in cursor.fetchall()]

    async def _async_list_checkpoints(self) -> List[Symbol]:
        """Asynchronous list implementation."""
        async with aiosqlite.connect(self._db_path) as conn:
            cursor = await conn.execute(
                """
                SELECT symbol FROM checkpoints ORDER BY symbol
            """
            )

            rows = await cursor.fetchall()
            return [Symbol(row[0]) for row in rows]


# Add missing methods to EntityId for string conversion
def _add_entityid_from_string():
    """Add from_string method to EntityId if it doesn't exist."""
    if not hasattr(EntityId, "from_string"):

        @classmethod
        def from_string(cls, id_str: str) -> EntityId:
            from uuid import UUID

            return cls(UUID(id_str))

        EntityId.from_string = from_string


# Initialize EntityId extensions
_add_entityid_from_string()
