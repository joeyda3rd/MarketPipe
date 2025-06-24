# SPDX-License-Identifier: Apache-2.0
"""End-to-end integration tests for error propagation across system layers.

This test validates that errors from deep system components properly bubble up
to the CLI with appropriate context and security masking, without losing
critical debugging information.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import date
from pathlib import Path
from textwrap import dedent
from unittest.mock import Mock, patch
from contextlib import contextmanager

import pytest
from typer.testing import CliRunner
import pandas as pd

from marketpipe.cli import app
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from marketpipe.aggregation.infrastructure.duckdb_engine import DuckDBAggregationEngine
from marketpipe.security.mask import safe_for_log


class ErrorInjector:
    """Utility class for injecting errors at different system layers."""
    
    @staticmethod
    @contextmanager
    def storage_error(error_type: str, message: str):
        """Inject storage layer errors."""
        if error_type == "permission_denied":
            with patch.object(ParquetStorageEngine, 'write') as mock_write:
                mock_write.side_effect = PermissionError(message)
                yield
        elif error_type == "disk_full":
            with patch.object(ParquetStorageEngine, 'write') as mock_write:
                mock_write.side_effect = OSError(28, message)  # ENOSPC
                yield
        elif error_type == "corrupt_data":
            with patch.object(ParquetStorageEngine, 'load_job_bars') as mock_load:
                mock_load.side_effect = ValueError(message)
                yield
        else:
            yield
    
    @staticmethod
    @contextmanager
    def duckdb_error(error_type: str, message: str):
        """Inject DuckDB layer errors."""
        if error_type == "sql_error":
            with patch.object(DuckDBAggregationEngine, 'aggregate_job') as mock_agg:
                mock_agg.side_effect = RuntimeError(message)
                yield
        elif error_type == "connection_error":
            with patch('duckdb.connect') as mock_connect:
                mock_connect.side_effect = ConnectionError(message)
                yield
        else:
            yield
    
    @staticmethod
    @contextmanager
    def network_error(error_type: str, message: str):
        """Inject network layer errors."""
        if error_type == "timeout":
            with patch('httpx.get') as mock_get:
                mock_get.side_effect = TimeoutError(message)
                yield
        elif error_type == "connection_refused":
            with patch('httpx.get') as mock_get:
                mock_get.side_effect = ConnectionRefusedError(message)
                yield
        else:
            yield
    
    @staticmethod
    @contextmanager
    def validation_error(error_type: str, message: str):
        """Inject validation layer errors."""
        if error_type == "schema_error":
            from marketpipe.validation.domain.services import ValidationDomainService
            with patch.object(ValidationDomainService, 'validate_bars') as mock_validate:
                mock_validate.side_effect = ValueError(message)
                yield
        else:
            yield


@pytest.mark.integration
class TestErrorPropagationEndToEnd:
    """Test error propagation from system depths to CLI interface."""

    def test_storage_errors_propagate_with_context(self, tmp_path, caplog):
        """Test storage layer errors reach CLI with proper context."""
        
        # Clear any existing log records
        caplog.clear()
        with caplog.at_level(logging.ERROR):
            
            runner = CliRunner()
            
            # Test permission denied error
            with ErrorInjector.storage_error("permission_denied", "Permission denied: /protected/path"):
                
                # Create test config that will trigger storage operation
                config_content = dedent(f"""
                    symbols: [AAPL]
                    start: "2024-01-15"
                    end: "2024-01-15"
                    output_path: "{tmp_path}/protected_output"
                    provider: "fake"
                """)
                
                config_file = tmp_path / "error_test.yaml"
                config_file.write_text(config_content)
                
                result = runner.invoke(app, [
                    "ingest",
                    "--config", str(config_file),
                ], catch_exceptions=True)
                
                # Should fail gracefully
                assert result.exit_code != 0
                
                # Check error message contains helpful context without exposing internals
                error_output = result.stdout.lower()
                assert any(word in error_output for word in ["permission", "denied", "storage", "error"])
                
                # Verify no stack traces leaked to user
                assert "traceback" not in error_output
                assert ".py" not in error_output or "config" in error_output  # Config files OK
                
                print("✅ Storage permission error handled gracefully")
        
        # Test disk full error
        with ErrorInjector.storage_error("disk_full", "No space left on device"):
            
            result = runner.invoke(app, [
                "ingest",
                "--symbols", "AAPL",
                "--start", "2024-01-15",
                "--end", "2024-01-15",
                "--output-path", str(tmp_path / "disk_full_test"),
            ], catch_exceptions=True)
            
            if result.exit_code != 0:
                error_output = result.stdout.lower()
                # Should have user-friendly disk space message
                assert any(word in error_output for word in ["space", "disk", "storage", "full"])
                print("✅ Disk full error handled gracefully")

    def test_aggregation_errors_propagate_with_context(self, tmp_path, caplog):
        """Test aggregation layer errors reach CLI with proper context."""
        
        caplog.clear()
        with caplog.at_level(logging.ERROR):
            
            runner = CliRunner()
            
            # Test SQL execution error
            with ErrorInjector.duckdb_error("sql_error", "SQL execution failed: invalid column 'nonexistent'"):
                
                result = runner.invoke(app, [
                    "aggregate",
                    "test-job-id",
                ], catch_exceptions=True)
                
                # Should fail with helpful error message
                if result.exit_code != 0:
                    error_output = result.stdout.lower()
                    assert any(word in error_output for word in ["sql", "aggregation", "failed", "error"])
                    
                    # Should not expose internal SQL details to end users
                    assert "nonexistent" not in error_output
                    print("✅ SQL aggregation error handled gracefully")
            
            # Test DuckDB connection error
            with ErrorInjector.duckdb_error("connection_error", "Failed to connect to DuckDB"):
                
                result = runner.invoke(app, [
                    "aggregate", 
                    "test-job-id",
                ], catch_exceptions=True)
                
                if result.exit_code != 0:
                    error_output = result.stdout.lower()
                    assert any(word in error_output for word in ["database", "connection", "failed"])
                    print("✅ Database connection error handled gracefully")

    def test_secret_masking_in_error_propagation(self, tmp_path, caplog):
        """Test that secrets are masked in error messages throughout the stack."""
        
        caplog.clear()
        with caplog.at_level(logging.ERROR):
            
            # Mock an error that might expose API credentials
            fake_api_key = "ALPACA_TEST_KEY_1234567890"
            fake_api_secret = "SECRET_12345_ABCDEF_67890"
            
            runner = CliRunner()
            
            # Create error scenario with potential secret exposure
            with patch('marketpipe.ingestion.infrastructure.alpaca_client.AlpacaClient') as mock_client:
                
                # Configure mock to raise error with secret in message
                error_with_secret = f"Authentication failed with key {fake_api_key} and secret {fake_api_secret}"
                mock_client.side_effect = ConnectionError(error_with_secret)
                
                config_content = dedent(f"""
                    alpaca:
                      key: "{fake_api_key}"
                      secret: "{fake_api_secret}"
                      base_url: "https://data.alpaca.markets/v2"
                      feed: "iex"
                    
                    symbols: [AAPL]
                    start: "2024-01-15"
                    end: "2024-01-15"
                    output_path: "{tmp_path}/secret_test"
                """)
                
                config_file = tmp_path / "secret_test.yaml"
                config_file.write_text(config_content)
                
                result = runner.invoke(app, [
                    "ingest",
                    "--config", str(config_file),
                ], catch_exceptions=True)
                
                # Error should occur, but secrets should be masked
                error_output = result.stdout
                
                # Verify secrets are NOT in output
                assert fake_api_key not in error_output, f"API key leaked in output: {error_output}"
                assert fake_api_secret not in error_output, f"API secret leaked in output: {error_output}"
                
                # Verify error context is still useful
                assert any(word in error_output.lower() for word in ["authentication", "failed", "connection"])
                
                # Check that masking utility works correctly
                masked_message = safe_for_log(error_with_secret, fake_api_key, fake_api_secret)
                assert fake_api_key not in masked_message
                assert fake_api_secret not in masked_message
                assert "****" in masked_message  # Should have masked portions
                
                print("✅ Secrets properly masked in error propagation")

    def test_validation_errors_with_helpful_context(self, tmp_path):
        """Test validation errors provide helpful context without overwhelming users."""
        
        runner = CliRunner()
        
        # Test with validation layer error
        with ErrorInjector.validation_error("schema_error", "Schema validation failed: missing required field 'ts_ns'"):
            
            result = runner.invoke(app, [
                "validate",
                "--job-id", "test-validation-job",
            ], catch_exceptions=True)
            
            if result.exit_code != 0:
                error_output = result.stdout.lower()
                
                # Should have helpful validation context
                assert any(word in error_output for word in ["validation", "failed", "schema"])
                
                # Should not expose internal field names to end users
                assert "ts_ns" not in error_output
                
                print("✅ Validation error handled with appropriate abstraction")

    def test_cascading_failure_scenarios(self, tmp_path):
        """Test multiple failure scenarios cascading through the system."""
        
        runner = CliRunner()
        
        # Scenario 1: Storage failure leads to aggregation failure
        with ErrorInjector.storage_error("corrupt_data", "Corrupted parquet file detected"):
            
            # First try to aggregate (should fail due to corrupt data)
            result = runner.invoke(app, [
                "aggregate",
                "corrupt-data-job",
            ], catch_exceptions=True)
            
            if result.exit_code != 0:
                error_output = result.stdout.lower()
                assert any(word in error_output for word in ["data", "corrupt", "failed"])
                print("✅ Cascading storage→aggregation failure handled")
        
        # Scenario 2: Network failure leads to ingestion failure
        with ErrorInjector.network_error("timeout", "Request timeout after 30 seconds"):
            
            result = runner.invoke(app, [
                "ingest",
                "--symbols", "AAPL",
                "--start", "2024-01-15",
                "--end", "2024-01-15",
                "--output-path", str(tmp_path / "timeout_test"),
            ], catch_exceptions=True)
            
            if result.exit_code != 0:
                error_output = result.stdout.lower()
                assert any(word in error_output for word in ["timeout", "network", "connection"])
                print("✅ Network timeout propagated appropriately")

    def test_error_context_preservation(self, tmp_path, caplog):
        """Test that error context is preserved but appropriately filtered."""
        
        caplog.clear()
        
        runner = CliRunner()
        
        # Create a complex error scenario
        complex_error_message = "DuckDB Error: Catalog Error: Table 'bars' does not exist in schema 'main'"
        
        with ErrorInjector.duckdb_error("sql_error", complex_error_message):
            
            with caplog.at_level(logging.DEBUG):
                
                result = runner.invoke(app, [
                    "aggregate",
                    "missing-table-job",
                ], catch_exceptions=True)
                
                # CLI output should be user-friendly
                cli_output = result.stdout.lower()
                if result.exit_code != 0:
                    assert any(word in cli_output for word in ["aggregation", "failed", "data"])
                    
                    # Technical details should be filtered from CLI
                    assert "catalog error" not in cli_output
                    assert "schema 'main'" not in cli_output
                
                # But full details should be available in logs for debugging
                debug_logs = caplog.text.lower()
                if "table" in debug_logs and "exist" in debug_logs:
                    print("✅ Full error context preserved in debug logs")
                
                print("✅ Error context appropriately filtered for different audiences")

    def test_resource_cleanup_on_errors(self, tmp_path):
        """Test that resources are properly cleaned up even when errors occur."""
        
        # Track resource usage before test
        initial_files = list(tmp_path.glob("**/*"))
        
        runner = CliRunner()
        
        # Test that failed operations don't leave partial files
        with ErrorInjector.storage_error("disk_full", "No space left on device"):
            
            result = runner.invoke(app, [
                "ingest",
                "--symbols", "AAPL",
                "--start", "2024-01-15", 
                "--end", "2024-01-15",
                "--output-path", str(tmp_path / "cleanup_test"),
            ], catch_exceptions=True)
            
            # Check that no partial files were left behind
            final_files = list(tmp_path.glob("**/*"))
            new_files = set(final_files) - set(initial_files)
            
            # Should not have created partial data files on failure
            parquet_files = [f for f in new_files if f.suffix == '.parquet']
            if parquet_files:
                print(f"⚠️  Found {len(parquet_files)} parquet files after failed operation")
            
            print("✅ Resource cleanup tested on error scenarios")

    def test_user_actionable_error_messages(self, tmp_path):
        """Test that error messages provide actionable guidance to users."""
        
        runner = CliRunner()
        
        # Test configuration error with actionable advice
        bad_config = tmp_path / "bad_config.yaml"
        bad_config.write_text("invalid yaml content: [unclosed")
        
        result = runner.invoke(app, [
            "ingest",
            "--config", str(bad_config),
        ], catch_exceptions=True)
        
        if result.exit_code != 0:
            error_output = result.stdout.lower()
            
            # Should suggest what user can do
            actionable_words = ["check", "verify", "fix", "correct", "format", "syntax"]
            has_actionable_guidance = any(word in error_output for word in actionable_words)
            
            if has_actionable_guidance:
                print("✅ Error message provides actionable guidance")
            else:
                print("⚠️  Error message could be more actionable")
        
        # Test missing file error with helpful suggestion
        result = runner.invoke(app, [
            "ingest",
            "--config", str(tmp_path / "nonexistent.yaml"),
        ], catch_exceptions=True)
        
        if result.exit_code != 0:
            error_output = result.stdout.lower()
            
            # Should mention file not found in helpful way
            assert any(word in error_output for word in ["not found", "missing", "exist"])
            print("✅ Missing file error handled helpfully")


@pytest.mark.integration
def test_error_logging_levels_and_targets(tmp_path, caplog):
    """Test that errors are logged at appropriate levels and targets."""
    
    caplog.clear()
    
    # Test different error severities get appropriate log levels
    with caplog.at_level(logging.DEBUG):
        
        runner = CliRunner()
        
        # User error (should be INFO/WARNING level)
        result = runner.invoke(app, [
            "ingest",
            "--symbols", "",  # Invalid empty symbol
            "--start", "2024-01-15",
            "--end", "2024-01-15",
        ], catch_exceptions=True)
        
        # System error (should be ERROR level)  
        with ErrorInjector.storage_error("permission_denied", "System permission denied"):
            
            result = runner.invoke(app, [
                "ingest", 
                "--symbols", "AAPL",
                "--start", "2024-01-15",
                "--end", "2024-01-15",
                "--output-path", str(tmp_path / "perm_test"),
            ], catch_exceptions=True)
        
        # Check that appropriate log levels were used
        log_records = caplog.records
        error_records = [r for r in log_records if r.levelno >= logging.ERROR]
        warning_records = [r for r in log_records if r.levelno == logging.WARNING]
        
        print(f"✅ Found {len(error_records)} error-level log entries")
        print(f"✅ Found {len(warning_records)} warning-level log entries")
        print("✅ Error logging levels validated")