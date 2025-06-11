# SPDX-License-Identifier: Apache-2.0
"""Unit tests for SQLite domain repositories.

Tests the concrete implementations of domain repository interfaces
using SQLite for persistence.
"""

from __future__ import annotations

import asyncio
import pytest
import tempfile
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

from marketpipe.infrastructure.repositories.sqlite_domain import (
    SqliteSymbolBarsRepository,
    SqliteOHLCVRepository,
    SqliteCheckpointRepository,
)
from marketpipe.domain.entities import OHLCVBar, EntityId
from marketpipe.domain.value_objects import Symbol, Timestamp, Price, Volume, TimeRange
from marketpipe.domain.aggregates import SymbolBarsAggregate
from marketpipe.domain.repositories import (
    RepositoryError,
    ConcurrencyError,
    DuplicateKeyError,
    ValidationError,
    NotFoundError,
)


@pytest.fixture
def temp_db_path(tmp_path):
    """Create a temporary database path for testing."""
    return str(tmp_path / "test.db")


@pytest.fixture
def sample_symbol():
    """Create a sample symbol for testing."""
    return Symbol("AAPL")


@pytest.fixture
def sample_trading_date():
    """Create a sample trading date for testing."""
    return date(2024, 1, 15)


@pytest.fixture
def sample_timestamp():
    """Create a sample timestamp for testing."""
    return Timestamp.from_iso("2024-01-15T09:30:00Z")


@pytest.fixture
def sample_ohlcv_bar(sample_symbol, sample_timestamp):
    """Create a sample OHLCV bar for testing."""
    return OHLCVBar(
        id=EntityId.generate(),
        symbol=sample_symbol,
        timestamp=sample_timestamp,
        open_price=Price.from_float(150.0),
        high_price=Price.from_float(152.0),
        low_price=Price.from_float(149.0),
        close_price=Price.from_float(151.0),
        volume=Volume(1000),
        trade_count=50,
        vwap=Price.from_float(150.5),
    )


@pytest.fixture
def sample_bars(sample_symbol):
    """Create a list of sample OHLCV bars for testing."""
    bars = []
    base_time = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
    
    for i in range(5):
        timestamp = Timestamp(base_time.replace(minute=30 + i))
        bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=sample_symbol,
            timestamp=timestamp,
            open_price=Price.from_float(150.0 + i),
            high_price=Price.from_float(152.0 + i),
            low_price=Price.from_float(149.0 + i),
            close_price=Price.from_float(151.0 + i),
            volume=Volume(1000 + i * 100),
            trade_count=50 + i * 5,
            vwap=Price.from_float(150.5 + i),
        )
        bars.append(bar)
    
    return bars


class TestSqliteSymbolBarsRepository:
    """Test cases for SqliteSymbolBarsRepository."""
    
    @pytest.mark.asyncio
    async def test_save_and_get_aggregate(self, temp_db_path, sample_symbol, sample_trading_date):
        """Test saving and retrieving a symbol bars aggregate."""
        repo = SqliteSymbolBarsRepository(temp_db_path)
        
        # Create aggregate
        aggregate = SymbolBarsAggregate(sample_symbol, sample_trading_date)
        aggregate.start_collection()
        aggregate.complete_collection()
        
        # Save aggregate
        await repo.save(aggregate)
        
        # Retrieve aggregate
        retrieved = await repo.get_by_symbol_and_date(sample_symbol, sample_trading_date)
        
        assert retrieved is not None
        assert retrieved.symbol == sample_symbol
        assert retrieved.trading_date == sample_trading_date
        assert retrieved.is_complete == True
        assert retrieved.version == aggregate.version
    
    @pytest.mark.asyncio
    async def test_get_nonexistent_aggregate(self, temp_db_path, sample_symbol, sample_trading_date):
        """Test retrieving a non-existent aggregate returns None."""
        repo = SqliteSymbolBarsRepository(temp_db_path)
        
        result = await repo.get_by_symbol_and_date(sample_symbol, sample_trading_date)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_save_duplicate_aggregate_raises_error(self, temp_db_path, sample_symbol, sample_trading_date):
        """Test that saving a duplicate aggregate raises ConcurrencyError."""
        repo = SqliteSymbolBarsRepository(temp_db_path)
        
        # Create and save first aggregate
        aggregate1 = SymbolBarsAggregate(sample_symbol, sample_trading_date)
        await repo.save(aggregate1)
        
        # Try to save another aggregate with same symbol/date
        # This should raise ConcurrencyError because both aggregates have version 1
        # but the database now has version 1, so the second save expects version 0
        aggregate2 = SymbolBarsAggregate(sample_symbol, sample_trading_date)
        
        with pytest.raises(ConcurrencyError):
            await repo.save(aggregate2)
    
    @pytest.mark.asyncio
    async def test_concurrency_error_on_version_mismatch(self, temp_db_path, sample_symbol, sample_trading_date):
        """Test that concurrent modifications raise ConcurrencyError."""
        repo = SqliteSymbolBarsRepository(temp_db_path)
        
        # Create and save aggregate
        aggregate = SymbolBarsAggregate(sample_symbol, sample_trading_date)
        await repo.save(aggregate)
        
        # Simulate concurrent modification by manually changing version
        aggregate._version = 999  # Simulate version mismatch
        
        with pytest.raises(ConcurrencyError):
            await repo.save(aggregate)
    
    @pytest.mark.asyncio
    async def test_find_symbols_with_data(self, temp_db_path):
        """Test finding symbols that have data in a date range."""
        repo = SqliteSymbolBarsRepository(temp_db_path)
        
        # Create aggregates for different symbols and dates
        symbols = [Symbol("AAPL"), Symbol("GOOGL"), Symbol("MSFT")]
        dates = [date(2024, 1, 15), date(2024, 1, 16), date(2024, 1, 17)]
        
        for symbol in symbols:
            for trading_date in dates:
                aggregate = SymbolBarsAggregate(symbol, trading_date)
                await repo.save(aggregate)
        
        # Find symbols in date range
        found_symbols = await repo.find_symbols_with_data(
            date(2024, 1, 15), 
            date(2024, 1, 16)
        )
        
        assert len(found_symbols) == 3
        assert all(symbol in found_symbols for symbol in symbols)
    
    @pytest.mark.asyncio
    async def test_get_completion_status(self, temp_db_path):
        """Test getting completion status for symbol/date combinations."""
        repo = SqliteSymbolBarsRepository(temp_db_path)
        
        # Create some completed and incomplete aggregates
        symbol1 = Symbol("AAPL")
        symbol2 = Symbol("GOOGL")
        date1 = date(2024, 1, 15)
        date2 = date(2024, 1, 16)
        
        # Complete aggregate
        complete_agg = SymbolBarsAggregate(symbol1, date1)
        complete_agg.start_collection()
        complete_agg.complete_collection()
        await repo.save(complete_agg)
        
        # Incomplete aggregate
        incomplete_agg = SymbolBarsAggregate(symbol1, date2)
        incomplete_agg.start_collection()
        await repo.save(incomplete_agg)
        
        # Get completion status
        status = await repo.get_completion_status(
            [symbol1, symbol2],
            [date1, date2]
        )
        
        assert status[symbol1.value][date1.isoformat()] == True
        assert status[symbol1.value][date2.isoformat()] == False
        assert status[symbol2.value][date1.isoformat()] == False
        assert status[symbol2.value][date2.isoformat()] == False
    
    @pytest.mark.asyncio
    async def test_delete_aggregate(self, temp_db_path, sample_symbol, sample_trading_date):
        """Test deleting an aggregate."""
        repo = SqliteSymbolBarsRepository(temp_db_path)
        
        # Create and save aggregate
        aggregate = SymbolBarsAggregate(sample_symbol, sample_trading_date)
        await repo.save(aggregate)
        
        # Verify it exists
        retrieved = await repo.get_by_symbol_and_date(sample_symbol, sample_trading_date)
        assert retrieved is not None
        
        # Delete it
        deleted = await repo.delete(sample_symbol, sample_trading_date)
        assert deleted == True
        
        # Verify it's gone
        retrieved = await repo.get_by_symbol_and_date(sample_symbol, sample_trading_date)
        assert retrieved is None
        
        # Try to delete again
        deleted = await repo.delete(sample_symbol, sample_trading_date)
        assert deleted == False


class TestSqliteOHLCVRepository:
    """Test cases for SqliteOHLCVRepository."""
    
    @pytest.mark.asyncio
    async def test_save_and_get_bars(self, temp_db_path, sample_bars):
        """Test saving and retrieving OHLCV bars."""
        repo = SqliteOHLCVRepository(temp_db_path)
        
        # Save bars
        await repo.save_bars(sample_bars)
        
        # Create time range covering all bars
        time_range = TimeRange(
            sample_bars[0].timestamp,
            sample_bars[-1].timestamp
        )
        
        # Retrieve bars
        retrieved_bars = []
        async for bar in repo.get_bars_for_symbol(sample_bars[0].symbol, time_range):
            retrieved_bars.append(bar)
        
        assert len(retrieved_bars) == len(sample_bars)
        
        # Verify bars are sorted by timestamp
        for i in range(1, len(retrieved_bars)):
            assert retrieved_bars[i-1].timestamp <= retrieved_bars[i].timestamp
    
    @pytest.mark.asyncio
    async def test_save_duplicate_bars_raises_error(self, temp_db_path, sample_ohlcv_bar):
        """Test that saving duplicate bars raises DuplicateKeyError."""
        repo = SqliteOHLCVRepository(temp_db_path)
        
        # Save bar
        await repo.save_bars([sample_ohlcv_bar])
        
        # Try to save same bar again
        with pytest.raises(DuplicateKeyError):
            await repo.save_bars([sample_ohlcv_bar])
    
    @pytest.mark.asyncio
    async def test_get_bars_for_multiple_symbols(self, temp_db_path):
        """Test retrieving bars for multiple symbols."""
        repo = SqliteOHLCVRepository(temp_db_path)
        
        # Create bars for different symbols
        symbols = [Symbol("AAPL"), Symbol("GOOGL")]
        bars = []
        base_time = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
        
        for i, symbol in enumerate(symbols):
            for j in range(3):
                timestamp = Timestamp(base_time.replace(minute=30 + j))
                bar = OHLCVBar(
                    id=EntityId.generate(),
                    symbol=symbol,
                    timestamp=timestamp,
                    open_price=Price.from_float(150.0 + i + j),
                    high_price=Price.from_float(152.0 + i + j),
                    low_price=Price.from_float(149.0 + i + j),
                    close_price=Price.from_float(151.0 + i + j),
                    volume=Volume(1000 + i * 100 + j * 10),
                )
                bars.append(bar)
        
        # Save all bars
        await repo.save_bars(bars)
        
        # Retrieve bars for multiple symbols
        time_range = TimeRange(
            Timestamp(base_time),
            Timestamp(base_time.replace(minute=35))
        )
        
        retrieved_bars = []
        async for bar in repo.get_bars_for_symbols(symbols, time_range):
            retrieved_bars.append(bar)
        
        assert len(retrieved_bars) == 6  # 3 bars per symbol
        
        # Verify bars are sorted by timestamp then symbol
        for i in range(1, len(retrieved_bars)):
            prev_bar = retrieved_bars[i-1]
            curr_bar = retrieved_bars[i]
            assert (prev_bar.timestamp < curr_bar.timestamp or 
                   (prev_bar.timestamp == curr_bar.timestamp and 
                    prev_bar.symbol.value <= curr_bar.symbol.value))
    
    @pytest.mark.asyncio
    async def test_exists_check(self, temp_db_path, sample_ohlcv_bar):
        """Test checking if a bar exists."""
        repo = SqliteOHLCVRepository(temp_db_path)
        
        # Check non-existent bar
        exists = await repo.exists(sample_ohlcv_bar.symbol, sample_ohlcv_bar.timestamp)
        assert exists == False
        
        # Save bar
        await repo.save_bars([sample_ohlcv_bar])
        
        # Check existing bar
        exists = await repo.exists(sample_ohlcv_bar.symbol, sample_ohlcv_bar.timestamp)
        assert exists == True
    
    @pytest.mark.asyncio
    async def test_count_bars(self, temp_db_path, sample_bars):
        """Test counting bars for a symbol."""
        repo = SqliteOHLCVRepository(temp_db_path)
        
        # Save bars
        await repo.save_bars(sample_bars)
        
        # Count all bars for symbol
        count = await repo.count_bars(sample_bars[0].symbol)
        assert count == len(sample_bars)
        
        # Count bars in time range
        time_range = TimeRange(
            sample_bars[0].timestamp,
            sample_bars[2].timestamp
        )
        count_in_range = await repo.count_bars(sample_bars[0].symbol, time_range)
        assert count_in_range == 3  # First 3 bars
    
    @pytest.mark.asyncio
    async def test_get_latest_timestamp(self, temp_db_path, sample_bars):
        """Test getting the latest timestamp for a symbol."""
        repo = SqliteOHLCVRepository(temp_db_path)

        # Check non-existent symbol
        latest = await repo.get_latest_timestamp(Symbol("NOEXIST"))  # Valid length (7 chars)
        assert latest is None

        # Save some bars
        await repo.save_bars(sample_bars)

        # Get latest timestamp
        latest = await repo.get_latest_timestamp(Symbol("AAPL"))
        assert latest is not None

        # Should be the timestamp of the last bar (sample_bars are sorted by time)
        expected_latest = max(bar.timestamp for bar in sample_bars)
        assert latest == expected_latest


class TestSqliteCheckpointRepository:
    """Test cases for SqliteCheckpointRepository."""
    
    @pytest.mark.asyncio
    async def test_save_and_get_checkpoint(self, temp_db_path, sample_symbol):
        """Test saving and retrieving checkpoints."""
        repo = SqliteCheckpointRepository(temp_db_path)
        
        # Save checkpoint
        checkpoint_data = {
            "last_timestamp": "2024-01-15T09:30:00Z",
            "processed_bars": 100,
            "status": "in_progress"
        }
        await repo.save_checkpoint(sample_symbol, checkpoint_data)
        
        # Retrieve checkpoint
        retrieved = await repo.get_checkpoint(sample_symbol)
        
        assert retrieved == checkpoint_data
    
    @pytest.mark.asyncio
    async def test_get_nonexistent_checkpoint(self, temp_db_path, sample_symbol):
        """Test retrieving a non-existent checkpoint returns None."""
        repo = SqliteCheckpointRepository(temp_db_path)
        
        result = await repo.get_checkpoint(sample_symbol)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_checkpoint(self, temp_db_path, sample_symbol):
        """Test updating an existing checkpoint."""
        repo = SqliteCheckpointRepository(temp_db_path)
        
        # Save initial checkpoint
        initial_data = {"status": "started", "progress": 0}
        await repo.save_checkpoint(sample_symbol, initial_data)
        
        # Update checkpoint
        updated_data = {"status": "in_progress", "progress": 50}
        await repo.save_checkpoint(sample_symbol, updated_data)
        
        # Retrieve updated checkpoint
        retrieved = await repo.get_checkpoint(sample_symbol)
        
        assert retrieved == updated_data
    
    @pytest.mark.asyncio
    async def test_delete_checkpoint(self, temp_db_path, sample_symbol):
        """Test deleting a checkpoint."""
        repo = SqliteCheckpointRepository(temp_db_path)
        
        # Save checkpoint
        checkpoint_data = {"status": "completed"}
        await repo.save_checkpoint(sample_symbol, checkpoint_data)
        
        # Verify it exists
        retrieved = await repo.get_checkpoint(sample_symbol)
        assert retrieved is not None
        
        # Delete it
        deleted = await repo.delete_checkpoint(sample_symbol)
        assert deleted == True
        
        # Verify it's gone
        retrieved = await repo.get_checkpoint(sample_symbol)
        assert retrieved is None
        
        # Try to delete again
        deleted = await repo.delete_checkpoint(sample_symbol)
        assert deleted == False
    
    @pytest.mark.asyncio
    async def test_list_checkpoints(self, temp_db_path):
        """Test listing all checkpoints."""
        repo = SqliteCheckpointRepository(temp_db_path)
        
        # Save checkpoints for multiple symbols
        symbols = [Symbol("AAPL"), Symbol("GOOGL"), Symbol("MSFT")]
        for symbol in symbols:
            await repo.save_checkpoint(symbol, {"status": "completed"})
        
        # List checkpoints
        checkpoint_symbols = await repo.list_checkpoints()
        
        assert len(checkpoint_symbols) == 3
        assert all(symbol in checkpoint_symbols for symbol in symbols)
        
        # Verify they're sorted
        symbol_values = [s.value for s in checkpoint_symbols]
        assert symbol_values == sorted(symbol_values)


class TestRepositoryErrorHandling:
    """Test error handling across all repositories."""
    
    @pytest.mark.asyncio
    async def test_invalid_database_path_raises_repository_error(self):
        """Test that invalid database paths raise RepositoryError."""
        # Try to create repository with read-only path (more predictable than permission error)
        import tempfile
        import os
        
        # Create a temporary directory and then make it read-only
        with tempfile.TemporaryDirectory() as temp_dir:
            readonly_dir = os.path.join(temp_dir, "readonly")
            os.makedirs(readonly_dir)
            os.chmod(readonly_dir, 0o444)  # Read-only
            
            invalid_path = os.path.join(readonly_dir, "subdir", "test.db")
            
            # This should fail when trying to create the parent directory
            with pytest.raises((RepositoryError, PermissionError)):
                repo = SqliteSymbolBarsRepository(invalid_path)
    
    @pytest.mark.asyncio
    async def test_repository_handles_invalid_data_gracefully(self, temp_db_path):
        """Test that repositories handle invalid data gracefully."""
        repo = SqliteOHLCVRepository(temp_db_path)
        
        # Try to create bar with invalid OHLC relationships
        with pytest.raises(ValueError):  # Should be caught by domain validation
            invalid_bar = OHLCVBar(
                id=EntityId.generate(),
                symbol=Symbol("AAPL"),
                timestamp=Timestamp.now(),
                open_price=Price.from_float(150.0),
                high_price=Price.from_float(140.0),  # High < Open (invalid)
                low_price=Price.from_float(149.0),
                close_price=Price.from_float(151.0),
                volume=Volume(1000),
            )


class TestRepositoryIdempotency:
    """Test idempotency and consistency across repositories."""
    
    @pytest.mark.asyncio
    async def test_multiple_saves_are_idempotent(self, temp_db_path, sample_symbol, sample_trading_date):
        """Test that saving the same aggregate multiple times is idempotent."""
        repo = SqliteSymbolBarsRepository(temp_db_path)
        
        # Create aggregate
        aggregate = SymbolBarsAggregate(sample_symbol, sample_trading_date)
        
        # Save multiple times (should update, not create duplicates)
        await repo.save(aggregate)
        
        # Modify aggregate and save again
        aggregate.start_collection()
        await repo.save(aggregate)
        
        aggregate.complete_collection()
        await repo.save(aggregate)
        
        # Verify only one aggregate exists
        retrieved = await repo.get_by_symbol_and_date(sample_symbol, sample_trading_date)
        assert retrieved is not None
        assert retrieved.is_complete == True
    
    @pytest.mark.asyncio
    async def test_concurrent_access_safety(self, temp_db_path, sample_symbol):
        """Test that concurrent access to repositories is safe."""
        repo = SqliteCheckpointRepository(temp_db_path)

        # Define async function for concurrent checkpoint updates
        async def update_checkpoint(symbol_suffix: str, value: int):
            symbol = Symbol(f"TST{symbol_suffix}")  # Use valid symbol format (letters only)
            data = {"value": value, "timestamp": datetime.now().isoformat()}
            await repo.save_checkpoint(symbol, data)
            return await repo.get_checkpoint(symbol)

        # Run multiple concurrent updates
        tasks = [update_checkpoint(chr(65 + i), i) for i in range(10)]  # A, B, C, etc.
        results = await asyncio.gather(*tasks)

        # Verify all updates succeeded
        assert len(results) == 10
        for i, result in enumerate(results):
            assert result is not None
            assert result["value"] == i


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"]) 