# SPDX-License-Identifier: Apache-2.0
"""Test gap detector service functionality."""

from __future__ import annotations

import datetime as dt
import tempfile
from pathlib import Path

import pytest

from marketpipe.ingestion.services.gap_detector import GapDetectorService


class TestGapDetectorService:
    """Test the GapDetectorService class."""

    def test_init(self):
        """Test service initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir))
            assert service._root == Path(temp_dir)
            assert service._timeframe == "1m"  # default

    def test_init_with_timeframe(self):
        """Test service initialization with custom timeframe."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir), timeframe="5m")
            assert service._root == Path(temp_dir)
            assert service._timeframe == "5m"

    def test_find_missing_days_no_symbol_directory(self):
        """Test finding missing days when symbol directory doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir))

            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 3)

            missing = service.find_missing_days("AAPL", start, end)

            # All days should be missing since no directory exists
            expected = [dt.date(2023, 1, 1), dt.date(2023, 1, 2), dt.date(2023, 1, 3)]
            assert missing == expected

    def test_find_missing_days_empty_symbol_directory(self):
        """Test finding missing days when symbol directory exists but is empty."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir))

            # Create empty symbol directory
            symbol_dir = Path(temp_dir) / "symbol=AAPL"
            symbol_dir.mkdir()

            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 3)

            missing = service.find_missing_days("AAPL", start, end)

            # All days should be missing since directory is empty
            expected = [dt.date(2023, 1, 1), dt.date(2023, 1, 2), dt.date(2023, 1, 3)]
            assert missing == expected

    def test_find_missing_days_with_existing_files(self):
        """Test finding missing days when some files exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir))

            # Create directory structure with some files
            symbol_dir = Path(temp_dir) / "symbol=AAPL"
            year_dir = symbol_dir / "year=2023"
            month_dir = year_dir / "month=01"
            month_dir.mkdir(parents=True)

            # Create files for some days
            (month_dir / "day=01.parquet").touch()
            (month_dir / "day=03.parquet").touch()

            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 5)

            missing = service.find_missing_days("AAPL", start, end)

            # Days 2, 4, 5 should be missing
            expected = [dt.date(2023, 1, 2), dt.date(2023, 1, 4), dt.date(2023, 1, 5)]
            assert missing == expected

    def test_find_missing_days_no_missing_files(self):
        """Test finding missing days when all files exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir))

            # Create directory structure with all files
            symbol_dir = Path(temp_dir) / "symbol=AAPL"
            year_dir = symbol_dir / "year=2023"
            month_dir = year_dir / "month=01"
            month_dir.mkdir(parents=True)

            # Create files for all days
            for day in range(1, 4):
                (month_dir / f"day={day:02d}.parquet").touch()

            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 3)

            missing = service.find_missing_days("AAPL", start, end)

            # No days should be missing
            assert missing == []

    def test_find_missing_days_multiple_years(self):
        """Test finding missing days across multiple years."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir))

            # Create directory structure for 2022
            symbol_dir = Path(temp_dir) / "symbol=AAPL"
            year_2022_dir = symbol_dir / "year=2022"
            month_2022_dir = year_2022_dir / "month=12"
            month_2022_dir.mkdir(parents=True)
            (month_2022_dir / "day=31.parquet").touch()

            # Create directory structure for 2023
            year_2023_dir = symbol_dir / "year=2023"
            month_2023_dir = year_2023_dir / "month=01"
            month_2023_dir.mkdir(parents=True)
            (month_2023_dir / "day=02.parquet").touch()

            start = dt.date(2022, 12, 31)
            end = dt.date(2023, 1, 2)

            missing = service.find_missing_days("AAPL", start, end)

            # Only Jan 1, 2023 should be missing
            expected = [dt.date(2023, 1, 1)]
            assert missing == expected

    def test_find_missing_days_malformed_directories(self):
        """Test handling of malformed directory names."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir))

            symbol_dir = Path(temp_dir) / "symbol=AAPL"
            symbol_dir.mkdir()

            # Create malformed directories
            (symbol_dir / "invalid_year").mkdir()
            (symbol_dir / "year=invalid").mkdir()
            (symbol_dir / "year=2023").mkdir()
            (symbol_dir / "year=2023" / "invalid_month").mkdir()
            (symbol_dir / "year=2023" / "month=invalid").mkdir()

            valid_month = symbol_dir / "year=2023" / "month=01"
            valid_month.mkdir()

            # Create valid and invalid files
            (valid_month / "day=01.parquet").touch()
            (valid_month / "invalid_day.parquet").touch()
            (valid_month / "day=invalid.parquet").touch()

            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 3)

            missing = service.find_missing_days("AAPL", start, end)

            # Days 2 and 3 should be missing (day 1 exists)
            expected = [dt.date(2023, 1, 2), dt.date(2023, 1, 3)]
            assert missing == expected

    def test_find_missing_days_case_insensitive_symbol(self):
        """Test that symbol lookup is case insensitive."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir))

            # Create directory with uppercase symbol
            symbol_dir = Path(temp_dir) / "symbol=AAPL"
            year_dir = symbol_dir / "year=2023"
            month_dir = year_dir / "month=01"
            month_dir.mkdir(parents=True)
            (month_dir / "day=01.parquet").touch()

            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 2)

            # Search with lowercase symbol
            missing = service.find_missing_days("aapl", start, end)

            # Only day 2 should be missing
            expected = [dt.date(2023, 1, 2)]
            assert missing == expected

    @pytest.mark.asyncio
    async def test_find_missing_days_async(self):
        """Test async version of find_missing_days."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir))

            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 3)

            missing = await service.find_missing_days_async("AAPL", start, end)

            # All days should be missing since no directory exists
            expected = [dt.date(2023, 1, 1), dt.date(2023, 1, 2), dt.date(2023, 1, 3)]
            assert missing == expected

    def test_existing_days_boundary_filtering(self):
        """Test that _existing_days properly filters by date boundaries."""
        with tempfile.TemporaryDirectory() as temp_dir:
            service = GapDetectorService(Path(temp_dir))

            # Create files spanning multiple months
            symbol_dir = Path(temp_dir) / "symbol=AAPL"

            # December 2022
            dec_dir = symbol_dir / "year=2022" / "month=12"
            dec_dir.mkdir(parents=True)
            (dec_dir / "day=30.parquet").touch()
            (dec_dir / "day=31.parquet").touch()

            # January 2023
            jan_dir = symbol_dir / "year=2023" / "month=01"
            jan_dir.mkdir(parents=True)
            (jan_dir / "day=01.parquet").touch()
            (jan_dir / "day=15.parquet").touch()
            (jan_dir / "day=31.parquet").touch()

            # February 2023
            feb_dir = symbol_dir / "year=2023" / "month=02"
            feb_dir.mkdir(parents=True)
            (feb_dir / "day=01.parquet").touch()

            # Test with narrow range that excludes some files
            start = dt.date(2023, 1, 10)
            end = dt.date(2023, 1, 20)

            missing = service.find_missing_days("AAPL", start, end)

            # Should only find missing days in the specified range
            # Day 15 exists, so we expect all days except day 15 to be missing
            all_days_in_range = [
                start + dt.timedelta(days=i) for i in range((end - start).days + 1)
            ]
            expected_missing = [d for d in all_days_in_range if d != dt.date(2023, 1, 15)]

            assert missing == expected_missing
