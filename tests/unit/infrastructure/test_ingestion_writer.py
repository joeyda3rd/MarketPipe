"""Test ingestion writer functionality."""

from __future__ import annotations

import os
import tempfile
from typing import Dict, List

import pytest
import pyarrow.parquet as pq

from marketpipe.ingestion.writer import write_parquet


class TestWriteParquet:
    """Test parquet writing functionality."""

    def test_write_parquet_basic_functionality(self):
        """Test basic parquet writing functionality."""
        rows = [
            {
                "symbol": "TEST",
                "timestamp": 1640995800000000000,  # 2022-01-01 09:30:00 UTC
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            result_path = write_parquet(rows, temp_dir)
            
            # Check that file was created
            assert os.path.exists(result_path)
            
            # Check path structure
            expected_path = os.path.join(
                temp_dir, "symbol=TEST", "year=2022", "month=01", "day=01.parquet"
            )
            assert result_path == expected_path
            
            # Verify file was created and has content
            assert os.path.getsize(result_path) > 0
            
            # Verify we can read metadata without schema conflicts
            metadata = pq.read_metadata(result_path)
            assert metadata.num_rows == 1

    def test_write_parquet_empty_rows_raises_error(self):
        """Test that empty rows list raises ValueError."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(ValueError, match="No rows supplied"):
                write_parquet([], temp_dir)

    def test_write_parquet_overwrite_behavior(self):
        """Test overwrite behavior."""
        rows = [
            {
                "symbol": "TEST",
                "timestamp": 1640995800000000000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            # Write first time
            path1 = write_parquet(rows, temp_dir)
            
            # Write again without overwrite - should return same path
            path2 = write_parquet(rows, temp_dir, overwrite=False)
            assert path1 == path2
            
            # Write with overwrite should work
            path3 = write_parquet(rows, temp_dir, overwrite=True)
            assert path1 == path3

    def test_write_parquet_compression_options(self):
        """Test different compression options."""
        rows = [
            {
                "symbol": "TEST",
                "timestamp": 1640995800000000000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            # Test with different compression
            result_path = write_parquet(rows, temp_dir, compression="gzip")
            assert os.path.exists(result_path)
            
            # Verify file was created and has content
            assert os.path.getsize(result_path) > 0
            
            # Verify we can read metadata
            metadata = pq.read_metadata(result_path)
            assert metadata.num_rows == 1

    def test_write_parquet_creates_directory_structure(self):
        """Test that directory structure is created properly."""
        rows = [
            {
                "symbol": "AAPL",
                "timestamp": 1672570200000000000,  # 2023-01-01 09:30:00 UTC
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            result_path = write_parquet(rows, temp_dir)
            
            # Check that directories were created
            expected_dirs = [
                os.path.join(temp_dir, "symbol=AAPL"),
                os.path.join(temp_dir, "symbol=AAPL", "year=2023"),
                os.path.join(temp_dir, "symbol=AAPL", "year=2023", "month=01"),
            ]
            
            for expected_dir in expected_dirs:
                assert os.path.exists(expected_dir)
                assert os.path.isdir(expected_dir) 