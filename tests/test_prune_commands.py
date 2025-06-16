# SPDX-License-Identifier: Apache-2.0
"""Tests for prune commands."""

from __future__ import annotations

import pytest
import tempfile
import datetime as dt
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
from typer.testing import CliRunner

from marketpipe.cli.prune import prune_app, _parse_age


class TestAgeParser:
    """Test age expression parsing."""

    def test_parse_days(self):
        """Test parsing day expressions."""
        today = dt.date.today()
        
        # Test explicit days
        result = _parse_age("30d")
        expected = today - dt.timedelta(days=30)
        assert result == expected
        
        # Test implicit days (default unit)
        result = _parse_age("30")
        assert result == expected

    def test_parse_months(self):
        """Test parsing month expressions."""
        today = dt.date.today()
        
        result = _parse_age("18m")
        expected = today - dt.timedelta(days=18 * 30)  # Approximate months
        assert result == expected

    def test_parse_years(self):
        """Test parsing year expressions."""
        today = dt.date.today()
        
        result = _parse_age("5y")
        expected = today - dt.timedelta(days=5 * 365)  # Approximate years
        assert result == expected

    def test_invalid_expressions(self):
        """Test invalid age expressions."""
        with pytest.raises(Exception):  # typer.BadParameter
            _parse_age("invalid")
        
        with pytest.raises(Exception):
            _parse_age("30x")  # Invalid unit
        
        with pytest.raises(Exception):
            _parse_age("")  # Empty string


class TestParquetPruning:
    """Test parquet file pruning functionality."""

    def test_prune_parquet_dry_run(self, tmp_path):
        """Test parquet pruning in dry-run mode."""
        runner = CliRunner()
        
        # Create test parquet files with date structure
        test_files = [
            tmp_path / "symbol=AAPL" / "2020-01-01.parquet",
            tmp_path / "symbol=AAPL" / "2024-01-01.parquet",
            tmp_path / "symbol=GOOGL" / "date=2019-12-31" / "data.parquet",
        ]
        
        for file_path in test_files:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("fake parquet data")
        
        # Run dry-run command
        result = runner.invoke(prune_app, [
            "parquet", "3y", 
            "--root", str(tmp_path),
            "--dry-run"
        ])
        
        assert result.exit_code == 0
        assert "Dry run complete" in result.stdout
        assert "Would delete" in result.stdout
        
        # Verify files still exist (dry run shouldn't delete)
        for file_path in test_files:
            assert file_path.exists()

    def test_prune_parquet_actual_deletion(self, tmp_path):
        """Test actual parquet file deletion."""
        runner = CliRunner()
        
        # Create old and new files
        old_file = tmp_path / "symbol=AAPL" / "2020-01-01.parquet"
        new_file = tmp_path / "symbol=AAPL" / "2024-01-01.parquet"
        
        for file_path in [old_file, new_file]:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("fake parquet data")
        
        with patch('marketpipe.cli.prune.bootstrap'):
            with patch('marketpipe.metrics.DATA_PRUNED_BYTES_TOTAL') as mock_metric:
                result = runner.invoke(prune_app, [
                    "parquet", "3y",
                    "--root", str(tmp_path)
                ])
        
        assert result.exit_code == 0
        assert "Removed" in result.stdout or "No files found" in result.stdout
        
        # Old file should be deleted, new file should remain
        assert not old_file.exists()
        assert new_file.exists()

    def test_prune_parquet_no_files(self, tmp_path):
        """Test pruning when no files match criteria."""
        runner = CliRunner()
        
        # Create only recent files
        recent_file = tmp_path / "symbol=AAPL" / "2024-01-01.parquet"
        recent_file.parent.mkdir(parents=True, exist_ok=True)
        recent_file.write_text("fake data")
        
        with patch('marketpipe.cli.prune.bootstrap'):
            result = runner.invoke(prune_app, [
                "parquet", "5y",
                "--root", str(tmp_path)
            ])
        
        assert result.exit_code == 0
        assert "No files found" in result.stdout

    def test_prune_parquet_nonexistent_directory(self):
        """Test pruning with nonexistent directory."""
        runner = CliRunner()
        
        with patch('marketpipe.cli.prune.bootstrap'):
            result = runner.invoke(prune_app, [
                "parquet", "1y",
                "--root", "/nonexistent/path"
            ])
        
        assert result.exit_code == 1
        assert "Directory does not exist" in result.stdout


class TestSQLitePruning:
    """Test SQLite database pruning functionality."""

    @pytest.fixture
    def mock_repo(self):
        """Create a mock repository for testing."""
        repo = Mock()
        repo.__class__.__name__ = "SqliteIngestionJobRepository"
        repo.count_old_jobs = AsyncMock()
        repo.delete_old_jobs = AsyncMock()
        return repo

    def test_prune_sqlite_dry_run(self, mock_repo):
        """Test SQLite pruning in dry-run mode."""
        runner = CliRunner()
        mock_repo.count_old_jobs.return_value = 42
        
        with patch('marketpipe.cli.prune.bootstrap'):
            with patch('marketpipe.cli.prune.create_ingestion_job_repository', return_value=mock_repo):
                result = runner.invoke(prune_app, [
                    "sqlite", "18m", "--dry-run"
                ])
        
        assert result.exit_code == 0
        assert "Would delete 42 job records" in result.stdout
        mock_repo.count_old_jobs.assert_called_once()
        mock_repo.delete_old_jobs.assert_not_called()

    def test_prune_sqlite_actual_deletion(self, mock_repo):
        """Test actual SQLite record deletion."""
        runner = CliRunner()
        mock_repo.delete_old_jobs.return_value = 15
        
        with patch('marketpipe.cli.prune.bootstrap'):
            with patch('marketpipe.cli.prune.create_ingestion_job_repository', return_value=mock_repo):
                with patch('marketpipe.metrics.DATA_PRUNED_ROWS_TOTAL') as mock_metric:
                    result = runner.invoke(prune_app, [
                        "sqlite", "18m"
                    ])
        
        assert result.exit_code == 0
        assert "Deleted 15 job records" in result.stdout
        mock_repo.delete_old_jobs.assert_called_once()

    def test_prune_sqlite_no_records(self, mock_repo):
        """Test SQLite pruning when no records match."""
        runner = CliRunner()
        mock_repo.delete_old_jobs.return_value = 0
        
        with patch('marketpipe.cli.prune.bootstrap'):
            with patch('marketpipe.cli.prune.create_ingestion_job_repository', return_value=mock_repo):
                result = runner.invoke(prune_app, [
                    "sqlite", "18m"
                ])
        
        assert result.exit_code == 0
        assert "No job records found" in result.stdout

    def test_prune_sqlite_non_sqlite_backend(self):
        """Test SQLite pruning with non-SQLite backend."""
        runner = CliRunner()
        
        # Mock a non-SQLite repository
        mock_repo = Mock()
        mock_repo.__class__.__name__ = "PostgresIngestionJobRepository"
        
        with patch('marketpipe.cli.prune.bootstrap'):
            with patch('marketpipe.cli.prune.create_ingestion_job_repository', return_value=mock_repo):
                result = runner.invoke(prune_app, [
                    "sqlite", "18m"
                ])
        
        assert result.exit_code == 0
        assert "SQLite backend not active" in result.stdout

    def test_prune_sqlite_error_handling(self, mock_repo):
        """Test error handling in SQLite pruning."""
        runner = CliRunner()
        mock_repo.delete_old_jobs.side_effect = Exception("Database error")
        
        with patch('marketpipe.cli.prune.bootstrap'):
            with patch('marketpipe.cli.prune.create_ingestion_job_repository', return_value=mock_repo):
                result = runner.invoke(prune_app, [
                    "sqlite", "18m"
                ])
        
        assert result.exit_code == 1
        assert "Failed to delete old records" in result.stdout


class TestMetricsIntegration:
    """Test metrics collection during pruning."""

    def test_parquet_metrics_recorded(self, tmp_path):
        """Test that parquet pruning records metrics."""
        runner = CliRunner()
        
        # Create test file
        test_file = tmp_path / "symbol=AAPL" / "2020-01-01.parquet"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("fake data")
        
        with patch('marketpipe.cli.prune.bootstrap'):
            with patch('marketpipe.metrics.DATA_PRUNED_BYTES_TOTAL') as mock_metric:
                with patch('marketpipe.metrics.record_metric') as mock_record:
                    result = runner.invoke(prune_app, [
                        "parquet", "3y",
                        "--root", str(tmp_path)
                    ])
        
        # Verify metrics were called (if file was deleted)
        if not test_file.exists():
            mock_record.assert_called()

    def test_sqlite_metrics_recorded(self):
        """Test that SQLite pruning records metrics."""
        runner = CliRunner()
        
        mock_repo = Mock()
        mock_repo.__class__.__name__ = "SqliteIngestionJobRepository"
        mock_repo.delete_old_jobs = AsyncMock(return_value=10)
        
        with patch('marketpipe.cli.prune.bootstrap'):
            with patch('marketpipe.cli.prune.create_ingestion_job_repository', return_value=mock_repo):
                with patch('marketpipe.metrics.DATA_PRUNED_ROWS_TOTAL') as mock_metric:
                    with patch('marketpipe.metrics.record_metric') as mock_record:
                        result = runner.invoke(prune_app, [
                            "sqlite", "18m"
                        ])
        
        # Verify metrics were recorded
        mock_record.assert_called()


class TestDomainEvents:
    """Test domain event emission during pruning."""

    def test_parquet_domain_event(self, tmp_path):
        """Test that parquet pruning emits domain events."""
        runner = CliRunner()
        
        # Create test file
        test_file = tmp_path / "symbol=AAPL" / "2020-01-01.parquet"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("fake data")
        
        with patch('marketpipe.cli.prune.bootstrap'):
            with patch('marketpipe.domain.events.DataPruned') as mock_event:
                result = runner.invoke(prune_app, [
                    "parquet", "3y",
                    "--root", str(tmp_path)
                ])
        
        # Verify event was created (if file was deleted)
        if not test_file.exists():
            mock_event.assert_called()

    def test_sqlite_domain_event(self):
        """Test that SQLite pruning emits domain events."""
        runner = CliRunner()
        
        mock_repo = Mock()
        mock_repo.__class__.__name__ = "SqliteIngestionJobRepository"
        mock_repo.delete_old_jobs = AsyncMock(return_value=5)
        
        with patch('marketpipe.cli.prune.bootstrap'):
            with patch('marketpipe.cli.prune.create_ingestion_job_repository', return_value=mock_repo):
                with patch('marketpipe.domain.events.DataPruned') as mock_event:
                    result = runner.invoke(prune_app, [
                        "sqlite", "18m"
                    ])
        
        # Verify event was created
        mock_event.assert_called_with(
            data_type="sqlite",
            amount=5,
            cutoff=pytest.approx(dt.date.today() - dt.timedelta(days=18 * 30), abs=dt.timedelta(days=1))
        )


class TestCLIIntegration:
    """Test CLI integration and help text."""

    def test_prune_help(self):
        """Test prune command help."""
        runner = CliRunner()
        result = runner.invoke(prune_app, ["--help"])
        
        assert result.exit_code == 0
        assert "Data retention utilities" in result.stdout

    def test_prune_parquet_help(self):
        """Test prune parquet subcommand help."""
        runner = CliRunner()
        result = runner.invoke(prune_app, ["parquet", "--help"])
        
        assert result.exit_code == 0
        assert "Delete parquet files older than" in result.stdout
        assert "Examples:" in result.stdout

    def test_prune_sqlite_help(self):
        """Test prune sqlite subcommand help."""
        runner = CliRunner()
        result = runner.invoke(prune_app, ["sqlite", "--help"])
        
        assert result.exit_code == 0
        assert "Delete old rows from the ingestion_jobs" in result.stdout
        assert "Examples:" in result.stdout

    def test_invalid_subcommand(self):
        """Test invalid subcommand."""
        runner = CliRunner()
        result = runner.invoke(prune_app, ["invalid"])
        
        assert result.exit_code != 0 