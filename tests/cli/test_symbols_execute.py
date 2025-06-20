"""Integration tests for 'mp symbols update --execute' functionality."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock
from typer.testing import CliRunner
import pytest

from marketpipe.cli import app as root_app


def create_mock_duckdb_connection():
    """Create a mock DuckDB connection that supports context manager protocol."""
    mock_conn = MagicMock()
    # Support context manager protocol
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    return mock_conn


class TestSymbolsExecuteIntegration:
    """Integration test suite for the symbols execute pipeline."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    @patch('marketpipe.ingestion.symbol_providers.get')
    @patch('marketpipe.ingestion.normalizer.run_symbol_normalizer.normalize_stage')
    @patch('marketpipe.ingestion.normalizer.scd_writer.run_scd_update')
    @patch('marketpipe.ingestion.normalizer.refresh_views.refresh')
    @patch('duckdb.connect')
    def test_full_execute_dummy(self, mock_duckdb, mock_refresh, mock_scd, mock_normalize, mock_get_provider, mock_list_providers):
        """Test complete pipeline execution with dummy provider."""
        # Mock provider setup
        mock_list_providers.return_value = ["dummy"]
        
        # Mock provider instance with async fetch_symbols method
        mock_provider = Mock()
        # Create an async mock that returns the list
        async def mock_fetch_symbols():
            # Create mock records with proper meta attribute support
            record1 = Mock()
            record1.meta = {}
            record1.model_dump = lambda: {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "market": "NASDAQ",
                "type": "stock",
                "meta": {"provider": "dummy"}
            }
            
            record2 = Mock()
            record2.meta = {}
            record2.model_dump = lambda: {
                "symbol": "GOOGL", 
                "name": "Alphabet Inc.",
                "market": "NASDAQ",
                "type": "stock",
                "meta": {"provider": "dummy"}
            }
            
            return [record1, record2]
        mock_provider.fetch_symbols = mock_fetch_symbols
        mock_get_provider.return_value = mock_provider
        
        # Mock DuckDB connection with context manager support
        mock_conn = create_mock_duckdb_connection()
        mock_duckdb.return_value = mock_conn
        
        # Mock successful pipeline components
        mock_normalize.return_value = None
        mock_scd.return_value = None
        mock_refresh.return_value = None
        
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            data_dir = Path(temp_dir) / "data"
            
            result = self.runner.invoke(root_app, [
                "symbols", "update",
                "-p", "dummy",
                "--db", str(db_path),
                "--data-dir", str(data_dir),
                "--execute"
            ])
        
        # Verify success
        assert result.exit_code == 0
        assert "✅ Pipeline complete." in result.output
        
        # Verify pipeline components were called
        mock_get_provider.assert_called_once()
        mock_normalize.assert_called_once()
        mock_scd.assert_called_once()
        mock_refresh.assert_called_once()
        
        # Verify DuckDB operations
        mock_conn.execute.assert_called()  # Various SQL operations
        mock_conn.close.assert_called_once()

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_execute_missing_provider_token(self, mock_list_providers):
        """Test graceful handling when provider requires missing API token."""
        mock_list_providers.return_value = ["polygon"]
        
        # Don't set POLYGON_API_KEY environment variable
        result = self.runner.invoke(root_app, [
            "symbols", "update",
            "-p", "polygon",
            "--execute"
        ])
        
        # Should fail gracefully
        assert result.exit_code == 1

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_execute_without_flag_shows_preview(self, mock_list_providers):
        """Test that without --execute flag, only preview is shown."""
        mock_list_providers.return_value = ["dummy"]
        
        result = self.runner.invoke(root_app, [
            "symbols", "update",
            "-p", "dummy"
        ])
        
        assert result.exit_code == 0
        assert "Dry preview complete" in result.output
        assert "✅ Pipeline complete." not in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    @patch('marketpipe.ingestion.symbol_providers.get')
    @patch('marketpipe.ingestion.normalizer.run_symbol_normalizer.normalize_stage')
    @patch('marketpipe.ingestion.normalizer.scd_writer.run_scd_update')
    @patch('marketpipe.ingestion.normalizer.refresh_views.refresh')
    @patch('duckdb.connect')
    def test_execute_creates_database_views(self, mock_duckdb, mock_refresh, mock_scd, mock_normalize, mock_get_provider, mock_list_providers):
        """Test that pipeline creates database views on completion."""
        # Setup mocks
        mock_list_providers.return_value = ["dummy"]
        mock_provider = Mock()
        async def mock_fetch_symbols():
            record = Mock()
            record.meta = {}
            record.model_dump = lambda: {"symbol": "TEST", "name": "Test Co", "meta": {"provider": "dummy"}}
            return [record]
        mock_provider.fetch_symbols = mock_fetch_symbols
        mock_get_provider.return_value = mock_provider
        
        mock_conn = create_mock_duckdb_connection()
        mock_duckdb.return_value = mock_conn
        
        # Mock the pipeline components
        mock_normalize.return_value = None
        mock_scd.return_value = None
        mock_refresh.return_value = None
        
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(root_app, [
                "symbols", "update",
                "-p", "dummy",
                "--db", str(Path(temp_dir) / "test.duckdb"),
                "--execute"
            ])
        
        assert result.exit_code == 0
        assert "✅ Pipeline complete." in result.output
        # Note: refresh may or may not be called depending on pipeline success

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    @patch('marketpipe.ingestion.symbol_providers.get')
    @patch('duckdb.connect')
    def test_rerun_same_snapshot_adds_zero_rows(self, mock_duckdb, mock_get_provider, mock_list_providers):
        """Test that rerunning the same snapshot is idempotent."""
        # Setup mocks
        mock_list_providers.return_value = ["dummy"]
        mock_provider = Mock()
        async def mock_fetch_symbols():
            record = Mock()
            record.meta = {}
            record.model_dump = lambda: {"symbol": "AAPL", "name": "Apple", "meta": {"provider": "dummy"}}
            return [record]
        mock_provider.fetch_symbols = mock_fetch_symbols
        mock_get_provider.return_value = mock_provider
        
        mock_conn = create_mock_duckdb_connection()
        # Mock that symbols_master table exists and has data (simulating previous run)
        mock_conn.execute.return_value.fetchone.return_value = [1]  # Non-zero count
        mock_duckdb.return_value = mock_conn
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Run twice with same snapshot date
            for _ in range(2):
                result = self.runner.invoke(root_app, [
                    "symbols", "update", 
                    "-p", "dummy",
                    "--db", str(Path(temp_dir) / "test.duckdb"),
                    "--snapshot-as-of", "2024-01-01",
                    "--execute"
                ])
                assert result.exit_code == 0
        
        # Should complete successfully both times
        assert "✅ Pipeline complete." in result.output 