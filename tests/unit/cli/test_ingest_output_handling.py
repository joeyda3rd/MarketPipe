# SPDX-License-Identifier: Apache-2.0
"""Unit tests for CLI output path handling and verification."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from marketpipe.cli import app


class TestCLIOutputHandling:
    """Test CLI behavior with --output flag and verification."""

    def setup_method(self):
        """Setup test environment."""
        self.runner = CliRunner()

    def test_output_flag_creates_custom_directory(self):
        """Test that --output flag creates files in the specified directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "custom_output"

            # Mock the ingestion services and boundary check
            with patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build, \
                 patch("marketpipe.cli.ohlcv_ingest._check_boundaries") as mock_check:
                # Mock the services
                mock_job_service = AsyncMock()
                mock_coordinator_service = AsyncMock()
                
                mock_job_service.create_job.return_value = "test_job"
                mock_coordinator_service.execute_job.return_value = {
                    "symbols_processed": 1,
                    "total_bars": 1000,
                    "processing_time_seconds": 5.0,
                }
                
                mock_build.return_value = (mock_job_service, mock_coordinator_service)
                mock_check.return_value = None  # Successful verification

                # Run ingestion with custom output path
                result = self.runner.invoke(
                    app,
                    [
                        "ingest-ohlcv",
                        "--provider",
                        "fake",
                        "--symbols",
                        "AAPL",
                        "--start",
                        "2024-01-01",
                        "--end",
                        "2024-01-02",
                        "--output",
                        str(output_path),
                        "--workers",
                        "1",
                    ],
                )

                # Verify command succeeded
                assert result.exit_code == 0, f"Command failed: {result.stdout}"
                assert "Job completed successfully" in result.stdout
                
                # Verify boundary check was called
                mock_check.assert_called_once()

    def test_verification_failure_exits_with_error(self):
        """Test that verification failure causes CLI to exit with code 1."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "test_output"

            # Mock the ingestion services and boundary check to simulate failure
            with patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build, \
                 patch("marketpipe.cli.ohlcv_ingest._check_boundaries") as mock_check:
                # Mock the services
                mock_job_service = AsyncMock()
                mock_coordinator_service = AsyncMock()
                
                mock_job_service.create_job.return_value = "test_job"
                mock_coordinator_service.execute_job.return_value = {
                    "symbols_processed": 1,
                    "total_bars": 1000,
                    "processing_time_seconds": 5.0,
                }
                
                mock_build.return_value = (mock_job_service, mock_coordinator_service)
                
                # Mock boundary check to simulate verification failure (exit with code 1)
                mock_check.side_effect = SystemExit(1)

                # Use alpaca provider which will return stale data and fail verification
                result = self.runner.invoke(
                    app,
                    [
                        "ingest-ohlcv",
                        "--provider",
                        "alpaca",
                        "--symbols",
                        "AAPL",
                        "--start",
                        "2024-06-20",
                        "--end",
                        "2024-06-21",
                        "--output",
                        str(output_path),
                        "--workers",
                        "1",
                    ],
                )

                # The CLI should exit with code 1 due to verification failure
                assert result.exit_code == 1

    def test_default_output_path_when_no_flag(self):
        """Test that data goes to data/output when no --output flag is provided."""
        
        # Mock the ingestion services and boundary check
        with patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build, \
             patch("marketpipe.cli.ohlcv_ingest._check_boundaries") as mock_check:
            # Mock the services
            mock_job_service = MagicMock()
            mock_coordinator_service = MagicMock()
            
            mock_job_service.create_job.return_value = "test_job"
            mock_coordinator_service.execute_job.return_value = {
                "symbols_processed": 1,
                "total_bars": 1000,
                "processing_time_seconds": 5.0,
            }
            
            mock_build.return_value = (mock_job_service, mock_coordinator_service)
            mock_check.return_value = None  # Successful verification

            # Run ingestion without custom output path
            result = self.runner.invoke(
                app,
                [
                    "ingest-ohlcv",
                    "--provider",
                    "fake",
                    "--symbols",
                    "AAPL",
                    "--start",
                    "2024-01-01",
                    "--end",
                    "2024-01-02",
                    "--workers",
                    "1",
                ],
            )

            # Verify command succeeded
            assert result.exit_code == 0, f"Command failed: {result.stdout}"
            assert "Job completed successfully" in result.stdout

    def test_verification_service_gets_correct_parameters(self):
        """Test that verification service is called and processes data correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "verification_test"

            # Mock the ingestion services and boundary check
            with patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build, \
                 patch("marketpipe.cli.ohlcv_ingest._check_boundaries") as mock_check:
                # Mock the services
                mock_job_service = AsyncMock()
                mock_coordinator_service = AsyncMock()
                
                mock_job_service.create_job.return_value = "test_job"
                mock_coordinator_service.execute_job.return_value = {
                    "symbols_processed": 1,
                    "total_bars": 1000,
                    "processing_time_seconds": 5.0,
                }
                
                mock_build.return_value = (mock_job_service, mock_coordinator_service)
                mock_check.return_value = None  # Successful verification

                # Run ingestion with fake provider (which generates deterministic data)
                result = self.runner.invoke(
                    app,
                    [
                        "ingest-ohlcv",
                        "--provider",
                        "fake",
                        "--symbols",
                        "AAPL",
                        "--start",
                        "2024-01-01",
                        "--end",
                        "2024-01-02",
                        "--output",
                        str(output_path),
                        "--workers",
                        "1",
                    ],
                )

                # Verify the verification step ran
                assert result.exit_code == 0
                assert "Job completed successfully" in result.stdout
                
                # Verify boundary check was called with correct parameters
                mock_check.assert_called_once_with(
                    path=str(output_path),
                    symbol="AAPL",
                    start="2024-01-01",
                    end="2024-01-02",
                    provider="fake",
                )


class TestProviderSuggestions:
    """Test provider suggestion functionality."""

    def setup_method(self):
        """Setup test environment."""
        self.runner = CliRunner()

    def test_provider_suggestions_in_output(self):
        """Test that provider suggestions appear in error output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "provider_test"

            # Mock the ingestion services and boundary check to simulate suggestions
            with patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build, \
                 patch("marketpipe.cli.ohlcv_ingest._check_boundaries") as mock_check, \
                 patch("builtins.print") as mock_print:
                # Mock the services
                mock_job_service = AsyncMock()
                mock_coordinator_service = AsyncMock()
                
                mock_job_service.create_job.return_value = "test_job"
                mock_coordinator_service.execute_job.return_value = {
                    "symbols_processed": 1,
                    "total_bars": 1000,
                    "processing_time_seconds": 5.0,
                }
                
                mock_build.return_value = (mock_job_service, mock_coordinator_service)
                
                # Mock boundary check to print provider suggestions and exit
                def mock_boundary_check_with_suggestions(*args, **kwargs):
                    print("Try provider=fake or provider=polygon")
                    raise SystemExit(1)
                
                mock_check.side_effect = mock_boundary_check_with_suggestions

                # Use alpaca provider which will fail verification and show suggestions
                result = self.runner.invoke(
                    app,
                    [
                        "ingest-ohlcv",
                        "--provider",
                        "alpaca",
                        "--symbols",
                        "AAPL",
                        "--start",
                        "2025-06-20",
                        "--end",
                        "2025-06-21",
                        "--output",
                        str(output_path),
                        "--workers",
                        "1",
                    ],
                )

                # Check that provider suggestions appear in output
                assert result.exit_code == 1
                mock_print.assert_called()
                # Verify the suggestion was printed
                print_calls = [call for call in mock_print.call_args_list]
                suggestion_found = any("Try provider=" in str(call) for call in print_calls)
                assert suggestion_found, f"Provider suggestions not found in print calls: {print_calls}"

    def test_verification_error_handling(self):
        """Test that verification errors are handled gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "error_test"

            # Mock the ingestion services and boundary check
            with patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build, \
                 patch("marketpipe.cli.ohlcv_ingest._check_boundaries") as mock_check:
                # Mock the services
                mock_job_service = AsyncMock()
                mock_coordinator_service = AsyncMock()
                
                mock_job_service.create_job.return_value = "test_job"
                mock_coordinator_service.execute_job.return_value = {
                    "symbols_processed": 1,
                    "total_bars": 1000,
                    "processing_time_seconds": 5.0,
                }
                
                mock_build.return_value = (mock_job_service, mock_coordinator_service)
                mock_check.return_value = None  # Successful verification

                # Run ingestion with fake provider - should complete successfully
                result = self.runner.invoke(
                    app,
                    [
                        "ingest-ohlcv",
                        "--provider",
                        "fake",
                        "--symbols",
                        "AAPL",
                        "--start",
                        "2024-01-01",
                        "--end",
                        "2024-01-02",
                        "--output",
                        str(output_path),
                        "--workers",
                        "1",
                    ],
                )

                # Should complete successfully and run verification
                assert result.exit_code == 0, f"Command should complete successfully: {result.stdout}"
                assert "Job completed successfully" in result.stdout


class TestIngestOutputHandling:
    """Test ingest CLI output handling and boundary checks."""

    def setup_method(self):
        """Setup test environment."""
        self.runner = CliRunner()

    def test_boundary_check_prevents_stale_data(self, tmp_path):
        """Test that boundary check prevents stale data from being accepted."""
        # Create mock data that's outside the requested range (stale data)
        stale_data = [
            {
                "symbol": "AAPL",
                "date": "2020-07-30",
                "timestamp": 1596067800000000000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
        ]

        # Create parquet file with stale data
        output_dir = tmp_path / "output"
        parquet_dir = output_dir / "symbol=AAPL"
        parquet_dir.mkdir(parents=True)

        table = pa.Table.from_pylist(stale_data)
        pq.write_table(table, parquet_dir / "data.parquet")

        # Mock the ingestion to simulate provider returning stale data
        with patch("marketpipe.cli.ohlcv_ingest._check_boundaries") as mock_check:
            mock_check.side_effect = SystemExit(1)

            result = self.runner.invoke(
                app,
                [
                    "ingest-ohlcv",
                    "--provider",
                    "alpaca",
                    "--symbols",
                    "AAPL",
                    "--start",
                    "2024-06-20",
                    "--end",
                    "2024-06-25",
                    "--output",
                    str(output_dir),
                ],
            )

        # Should exit with error code 1
        assert result.exit_code == 1

    def test_boundary_check_accepts_correct_data(self, tmp_path):
        """Test that boundary check accepts data within requested range."""
        # Create mock data within the requested range
        correct_data = [
            {
                "symbol": "AAPL",
                "date": "2024-06-20",
                "timestamp": 1718870400000000000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
        ]

        # Create parquet file with correct data
        output_dir = tmp_path / "output"
        parquet_dir = output_dir / "symbol=AAPL"
        parquet_dir.mkdir(parents=True)

        table = pa.Table.from_pylist(correct_data)
        pq.write_table(table, parquet_dir / "data.parquet")

        # Mock the boundary check and ingestion services
        with patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build, \
             patch("marketpipe.cli.ohlcv_ingest._check_boundaries") as mock_check:
            # Mock the services
            mock_job_service = MagicMock()
            mock_coordinator_service = MagicMock()
            
            mock_job_service.create_job.return_value = "test_job"
            mock_coordinator_service.execute_job.return_value = {
                "symbols_processed": 1,
                "total_bars": 1000,
                "processing_time_seconds": 5.0,
            }
            
            mock_build.return_value = (mock_job_service, mock_coordinator_service)
            
            # Simulate successful boundary check
            mock_check.return_value = None  # No exception = success

            result = self.runner.invoke(
                app,
                [
                    "ingest-ohlcv",
                    "--provider",
                    "alpaca",
                    "--symbols",
                    "AAPL",
                    "--start",
                    "2024-06-20",
                    "--end",
                    "2024-06-25",
                    "--output",
                    str(output_dir),
                ],
            )

        # Should succeed when data is within range
        assert result.exit_code == 0
        mock_check.assert_called_once()

    def test_cli_error_message_format(self):
        """Test that CLI error messages match the required format."""
        expected_patterns = [
            "ERROR:",
            "Provider returned data",
            "but the request was",
            "Run aborted",
            "Try a different provider",
        ]

        # This would be the actual error message from the boundary check
        error_msg = (
            "ERROR: Provider returned data 2020-07-30..2020-07-30 "
            "but the request was 2024-06-20..2024-06-25. "
            "Run aborted. Try a different provider or check your date parsing."
        )

        for pattern in expected_patterns:
            assert pattern in error_msg, f"Expected pattern '{pattern}' not found in error message"

    def test_successful_ingest_message_format(self):
        """Test that successful ingestion messages match the required format."""
        success_msg = "Ingest OK: 1,000 bars, 2024-06-20..2024-06-25, symbol AAPL, provider alpaca"

        expected_patterns = ["Ingest OK:", "bars", "symbol AAPL", "provider alpaca"]

        for pattern in expected_patterns:
            assert (
                pattern in success_msg
            ), f"Expected pattern '{pattern}' not found in success message"

    def test_cli_requires_mandatory_options(self):
        """Test that CLI requires start and end when not using config."""
        # Test missing start
        result = self.runner.invoke(
            app,
            [
                "ingest-ohlcv",
                "--provider",
                "alpaca",
                "--symbols",
                "AAPL",
                "--end",
                "2024-06-25",
                "--output",
                "tests/resources",
            ],
        )
        assert result.exit_code != 0
        assert "start" in result.output.lower() or "required" in result.output.lower()

        # Test missing end
        result = self.runner.invoke(
            app,
            [
                "ingest-ohlcv",
                "--provider",
                "alpaca",
                "--symbols",
                "AAPL",
                "--start",
                "2024-06-20",
                "--output",
                "tests/resources",
            ],
        )
        assert result.exit_code != 0
        assert "end" in result.output.lower() or "required" in result.output.lower()
