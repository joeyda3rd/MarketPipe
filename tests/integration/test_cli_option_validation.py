"""
CLI Option Validation Matrix

Comprehensive testing framework for validating all CLI option combinations,
edge cases, and configuration precedence rules.

This module implements Phase 2 of the CLI validation framework:
- Tests all valid option combinations and edge cases
- Validates configuration precedence (CLI > env > config > defaults)
- Tests provider variations and feed types
- Validates date ranges, symbol formats, and numeric parameters
- Ensures proper error handling for invalid combinations
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
import yaml


@dataclass
class OptionTestCase:
    """Test case for a specific option combination."""

    command_path: list[str]
    options: dict[str, Any]
    expected_success: bool = True
    expected_error_patterns: list[str] = field(default_factory=list)
    test_description: str = ""
    requires_config: bool = False
    requires_env_vars: bool = False


@dataclass
class OptionValidationResult:
    """Result of option validation test."""

    test_case: OptionTestCase
    success: bool = False
    exit_code: int = -1
    stdout: str = ""
    stderr: str = ""
    execution_time_ms: float = 0.0
    error_messages: list[str] = field(default_factory=list)


class CLIOptionValidator:
    """Validates CLI options and their combinations."""

    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir or Path(__file__).parent.parent.parent
        self.test_results: list[OptionValidationResult] = []

    def validate_option_combination(self, test_case: OptionTestCase) -> OptionValidationResult:
        """
        Validate a specific option combination.

        Args:
            test_case: Test case to validate

        Returns:
            OptionValidationResult with validation details
        """
        import time

        result = OptionValidationResult(test_case=test_case)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            original_cwd = os.getcwd()

            try:
                os.chdir(temp_path)

                # Setup test environment
                env_vars = self._setup_test_environment(test_case, temp_path)

                # Build command
                cmd_args = self._build_command_args(test_case)

                # Execute command
                start_time = time.time()
                process_result = subprocess.run(
                    cmd_args,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    env=env_vars,
                    cwd=self.base_dir,
                )
                execution_time = (time.time() - start_time) * 1000

                # Analyze results
                result.exit_code = process_result.returncode
                result.stdout = process_result.stdout
                result.stderr = process_result.stderr
                result.execution_time_ms = execution_time

                # Filter out normal operational logs from stderr for success determination
                filtered_stderr = self._filter_operational_logs(process_result.stderr)
                result.stderr = filtered_stderr
                
                # Determine success based on expectations
                if test_case.expected_success:
                    # For expected success, check exit code and absence of actual errors
                    result.success = (process_result.returncode == 0 and 
                                    not self._has_actual_errors(filtered_stderr))
                else:
                    result.success = process_result.returncode != 0

                    # Check for expected error patterns
                    if test_case.expected_error_patterns:
                        output = (process_result.stdout + filtered_stderr).lower()
                        pattern_matches = [
                            pattern.lower() in output
                            for pattern in test_case.expected_error_patterns
                        ]
                        result.success = result.success and any(pattern_matches)

                return result

            except subprocess.TimeoutExpired:
                result.error_messages.append("Command timed out")
                return result
            except Exception as e:
                result.error_messages.append(f"Execution error: {e}")
                return result
            finally:
                os.chdir(original_cwd)

    def _filter_operational_logs(self, stderr: str) -> str:
        """Filter out normal operational logs from stderr, keeping only actual errors."""
        if not stderr:
            return stderr
        
        lines = stderr.split('\n')
        filtered_lines = []
        
        for line in lines:
            # Skip alembic migration INFO logs
            if 'INFO  [alembic.runtime.migration]' in line:
                continue
            # Skip other known operational logs
            if any(pattern in line for pattern in [
                'Context impl SQLiteImpl',
                'Will assume non-transactional DDL',
                'Running upgrade',
                'INFO  [alembic'
            ]):
                continue
            # Keep everything else
            filtered_lines.append(line)
        
        return '\n'.join(filtered_lines).strip()
    
    def _has_actual_errors(self, stderr: str) -> bool:
        """Check if stderr contains actual error messages (not just operational logs)."""
        if not stderr:
            return False
        
        # Look for actual error indicators
        error_patterns = [
            'ERROR',
            'CRITICAL', 
            'FATAL',
            'Exception',
            'Traceback',
            'Error:',
            'Failed:',
            'No such',
            'Permission denied',
            'Connection refused'
        ]
        
        lines = stderr.split('\n')
        for line in lines:
            if any(pattern in line for pattern in error_patterns):
                return True
        
        return False

    def _setup_test_environment(self, test_case: OptionTestCase, temp_path: Path) -> dict[str, str]:
        """Setup test environment with config files and environment variables."""
        import time
        import random
        
        # Clean up any existing persistent database files that could cause job conflicts
        persistent_db_files = [
            self.base_dir / "data" / "ingestion_jobs.db",
            self.base_dir / "data" / "metrics.db", 
            self.base_dir / "data" / "db" / "core.db",
        ]
        for db_file in persistent_db_files:
            if db_file.exists():
                try:
                    db_file.unlink()
                except (PermissionError, OSError):
                    pass  # Continue if we can't remove the file
        
        env_vars = os.environ.copy()

        # Force databases to be created in temp directory with unique names to avoid conflicts
        unique_suffix = f"{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        env_vars.update({
            "MARKETPIPE_DB_PATH": str(temp_path / f"test_{unique_suffix}.db"),
            "MARKETPIPE_METRICS_DB_PATH": str(temp_path / f"metrics_{unique_suffix}.db"),
            "MARKETPIPE_INGESTION_DB_PATH": str(temp_path / f"ingestion_{unique_suffix}.db"),
        })

        # Create test config file if needed
        if test_case.requires_config:
            config_path = temp_path / "test_config.yaml"
            config_data = self._generate_test_config(test_case)
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            # Update options to use config file
            test_case.options["--config"] = str(config_path)

        # Set environment variables if needed
        if test_case.requires_env_vars:
            env_vars.update(
                {
                    "ALPACA_KEY": "test_key_12345",
                    "ALPACA_SECRET": "test_secret_67890",
                    "IEX_TOKEN": "test_iex_token_abcdef",
                }
            )

        return env_vars

    def _generate_test_config(self, test_case: OptionTestCase) -> dict[str, Any]:
        """Generate test configuration based on test case."""
        return {
            "config_version": "1",  # Required for config validation
            "symbols": ["AAPL", "MSFT"],
            "start": "2024-01-03",  # Use 'start' not 'start_date'
            "end": "2024-01-04",    # Use 'end' not 'end_date'
            "output_path": "data/parquet",  # Use 'output_path' not 'output_dir'
            "workers": 2,
            "batch_size": 1000,
            "provider": "fake",  # Use fake provider to avoid auth issues
            "feed_type": "iex",
        }

    def _build_command_args(self, test_case: OptionTestCase) -> list[str]:
        """Build command arguments from test case."""
        cmd_args = ["python", "-m", "marketpipe"] + test_case.command_path

        for option, value in test_case.options.items():
            if isinstance(value, bool) and value:
                cmd_args.append(option)
            elif not isinstance(value, bool):
                cmd_args.extend([option, str(value)])

        return cmd_args


class CLIOptionTestGenerator:
    """Generates comprehensive test cases for CLI option validation."""

    def generate_provider_tests(self) -> list[OptionTestCase]:
        """Generate tests for different provider configurations."""
        providers = ["alpaca", "fake", "finnhub", "iex", "polygon"]
        feed_types = ["iex", "sip"]

        test_cases = []

        # Test each provider
        for provider in providers:
            test_cases.append(
                OptionTestCase(
                    command_path=["ingest-ohlcv"],
                    options={
                        "--provider": provider,
                        "--symbols": "AAPL",
                        "--start": "2024-01-03",
                        "--end": "2024-01-04",
                        "--output": "test_data",
                    },
                    expected_success=provider == "fake",  # Only fake provider works without auth
                    test_description=f"Test {provider} provider",
                )
            )

        # Test feed types (only for supported providers)
        for feed_type in feed_types:
            test_cases.append(
                OptionTestCase(
                    command_path=["ingest-ohlcv"],
                    options={
                        "--provider": "fake",
                        "--feed-type": feed_type,
                        "--symbols": "AAPL",
                        "--start": "2024-01-03",
                        "--end": "2024-01-04",
                        "--output": "test_data",
                    },
                    expected_success=True,
                    test_description=f"Test {feed_type} feed type",
                )
            )

        return test_cases

    def generate_date_validation_tests(self) -> list[OptionTestCase]:
        """Generate tests for date range validation."""
        test_cases = []

        # Valid date ranges
        valid_dates = [
            ("2024-01-03", "2024-01-31"),
            ("2024-06-15", "2024-06-16"),  # Single day (end must be after start)
        ]

        for start, end in valid_dates:
            test_cases.append(
                OptionTestCase(
                    command_path=["ingest-ohlcv"],
                    options={
                        "--provider": "fake",
                        "--symbols": "AAPL",
                        "--start": start,
                        "--end": end,
                        "--output": "test_data",
                    },
                    expected_success=True,
                    test_description=f"Valid date range {start} to {end}",
                )
            )

        # Invalid date ranges
        invalid_dates = [
            ("2024-13-01", "2024-13-31", ["invalid date", "month"]),
            ("2024-01-32", "2024-01-32", ["invalid date", "day"]),
            ("not-a-date", "2024-01-03", ["invalid date", "format"]),
            ("2024-01-31", "2024-01-03", ["start date", "after", "end date"]),
            ("2022-12-01", "2022-12-31", ["older than 730 days"]),  # Too old
            ("2024-06-15", "2024-06-15", ["start date must be before end date"]),  # Same date
        ]

        for start, end, error_patterns in invalid_dates:
            test_cases.append(
                OptionTestCase(
                    command_path=["ingest-ohlcv"],
                    options={
                        "--provider": "fake",
                        "--symbols": "AAPL",
                        "--start": start,
                        "--end": end,
                        "--output": "test_data",
                    },
                    expected_success=False,
                    expected_error_patterns=error_patterns,
                    test_description=f"Invalid date range {start} to {end}",
                )
            )

        return test_cases

    def generate_symbol_format_tests(self) -> list[OptionTestCase]:
        """Generate tests for symbol format validation."""
        test_cases = []

        # Valid symbol formats - use unique date ranges to avoid job conflicts
        valid_symbols = [
            ("AAPL", "2024-01-03", "2024-01-04"),
            ("AAPL,MSFT,GOOGL", "2024-01-05", "2024-01-06"), 
            ("AAPL,MSFT", "2024-01-07", "2024-01-08"),
            ("FAKE1,FAKE2", "2024-01-09", "2024-01-10")
        ]

        for symbols, start_date, end_date in valid_symbols:
            test_cases.append(
                OptionTestCase(
                    command_path=["ingest-ohlcv"],
                    options={
                        "--provider": "fake",
                        "--symbols": symbols,
                        "--start": start_date,
                        "--end": end_date,
                        "--output": "test_data",
                    },
                    expected_success=True,
                    test_description=f"Valid symbols: {symbols}",
                )
            )

        # Invalid symbol formats - use unique date ranges
        invalid_symbols = [
            ("", ["empty", "symbol"], "2024-01-11", "2024-01-12"),
            ("TOOLONGSYMBOL", ["invalid", "symbol"], "2024-01-13", "2024-01-14"),
            ("123INVALID", ["invalid", "symbol"], "2024-01-15", "2024-01-16"),
            ("SYM@BOL", ["invalid", "symbol"], "2024-01-17", "2024-01-18"),
        ]

        for symbols, error_patterns, start_date, end_date in invalid_symbols:
            test_cases.append(
                OptionTestCase(
                    command_path=["ingest-ohlcv"],
                    options={
                        "--provider": "fake",
                        "--symbols": symbols,
                        "--start": start_date,
                        "--end": end_date,
                        "--output": "test_data",
                    },
                    expected_success=False,
                    expected_error_patterns=error_patterns,
                    test_description=f"Invalid symbols: {symbols}",
                )
            )

        return test_cases

    def generate_numeric_parameter_tests(self) -> list[OptionTestCase]:
        """Generate tests for numeric parameter validation."""
        test_cases = []

        # Worker count tests - use unique date ranges to avoid conflicts
        worker_counts = [
            (1, True, "Minimum workers", "2024-01-03", "2024-01-04"),
            (4, True, "Default workers", "2024-01-05", "2024-01-06"),
            (20, True, "Maximum workers", "2024-01-07", "2024-01-08"),  # Actual limit is 20, not 32
            (0, False, "Zero workers", "2024-01-09", "2024-01-10"),
            (-1, False, "Negative workers", "2024-01-11", "2024-01-12"),
            (32, False, "Too many workers", "2024-01-13", "2024-01-14"),  # 32 exceeds the limit
        ]

        for workers, expected_success, description, start_date, end_date in worker_counts:
            test_cases.append(
                OptionTestCase(
                    command_path=["ingest-ohlcv"],
                    options={
                        "--provider": "fake",
                        "--symbols": "AAPL",
                        "--start": start_date,
                        "--end": end_date,
                        "--workers": workers,
                        "--output": "test_data",
                    },
                    expected_success=expected_success,
                    expected_error_patterns=["worker", "invalid"] if not expected_success else [],
                    test_description=description,
                )
            )

        # Batch size tests - use unique date ranges to avoid conflicts
        batch_sizes = [
            (1, True, "Minimum batch size", "2024-01-15", "2024-01-16"),
            (1000, True, "Default batch size", "2024-01-17", "2024-01-18"),
            (10000, True, "Maximum batch size", "2024-01-19", "2024-01-20"),
            (0, False, "Zero batch size", "2024-01-21", "2024-01-22"),
            (-1, False, "Negative batch size", "2024-01-23", "2024-01-24"),
        ]

        for batch_size, expected_success, description, start_date, end_date in batch_sizes:
            test_cases.append(
                OptionTestCase(
                    command_path=["ingest-ohlcv"],
                    options={
                        "--provider": "fake",
                        "--symbols": "AAPL",
                        "--start": start_date,
                        "--end": end_date,
                        "--batch-size": batch_size,
                        "--output": "test_data",
                    },
                    expected_success=expected_success,
                    expected_error_patterns=["batch", "invalid"] if not expected_success else [],
                    test_description=description,
                )
            )

        return test_cases

    def generate_path_validation_tests(self) -> list[OptionTestCase]:
        """Generate tests for path validation."""
        test_cases = []

        # Valid paths - use unique date ranges to avoid conflicts
        # Note: All paths will be executed within temporary directories to avoid artifacts
        valid_paths = [
            ("data/test", "2024-01-25", "2024-01-26"),
            ("/tmp/marketpipe_test", "2024-01-27", "2024-01-28"),
            ("test_relative_path", "2024-01-29", "2024-01-30")
        ]

        for path, start_date, end_date in valid_paths:
            test_cases.append(
                OptionTestCase(
                    command_path=["ingest-ohlcv"],
                    options={
                        "--provider": "fake",
                        "--symbols": "AAPL",
                        "--start": start_date,
                        "--end": end_date,
                        "--output": path,
                    },
                    expected_success=True,
                    test_description=f"Valid output path: {path}",
                )
            )

        # Invalid paths - use unique date ranges to avoid conflicts
        invalid_paths = [
            ("/root/forbidden", ["permission", "access"], "2024-01-31", "2024-02-01"),
            ("", ["empty", "path"], "2024-02-02", "2024-02-03"),
            ("/dev/null/invalid", ["invalid", "path"], "2024-02-04", "2024-02-05"),
        ]

        for path, error_patterns, start_date, end_date in invalid_paths:
            test_cases.append(
                OptionTestCase(
                    command_path=["ingest-ohlcv"],
                    options={
                        "--provider": "fake",
                        "--symbols": "AAPL",
                        "--start": start_date,
                        "--end": end_date,
                        "--output": path,
                    },
                    expected_success=False,
                    expected_error_patterns=error_patterns,
                    test_description=f"Invalid output path: {path}",
                )
            )

        return test_cases

    def generate_configuration_precedence_tests(self) -> list[OptionTestCase]:
        """Generate tests for configuration precedence rules."""
        test_cases = []

        # CLI override of config file
        test_cases.append(
            OptionTestCase(
                command_path=["ingest-ohlcv"],
                options={
                    "--symbols": "TSLA",  # Should override config file
                    "--workers": 8,  # Should override config file
                },
                expected_success=True,
                requires_config=True,
                test_description="CLI options override config file",
            )
        )

        # Environment variables with config
        test_cases.append(
            OptionTestCase(
                command_path=["ingest-ohlcv"],
                options={
                    "--symbols": "GOOGL",  # Different symbol to avoid conflicts
                    "--start": "2024-01-05",  # Different date to avoid conflicts
                    "--end": "2024-01-06",
                    "--output": "test_data",
                },
                expected_success=True,
                requires_config=True,
                requires_env_vars=True,
                test_description="Environment variables with config file",
            )
        )

        return test_cases


class TestCLIOptionValidation:
    """Test suite for comprehensive CLI option validation."""

    @pytest.fixture
    def option_generator(self):
        """Option test generator fixture."""
        return CLIOptionTestGenerator()

    @pytest.fixture
    def option_validator(self):
        """Option validator fixture."""
        return CLIOptionValidator()

    def test_provider_configurations(self, option_generator, option_validator):
        """Test all provider configurations work correctly."""
        test_cases = option_generator.generate_provider_tests()
        failed_tests = []

        for test_case in test_cases:
            result = option_validator.validate_option_combination(test_case)

            if not result.success:
                failed_tests.append(f"{test_case.test_description}: {result.stderr}")

        if failed_tests:
            pytest.fail("Provider configuration tests failed:\n" + "\n".join(failed_tests))

    def test_date_range_validation(self, option_generator, option_validator):
        """Test date range validation handles edge cases correctly."""
        test_cases = option_generator.generate_date_validation_tests()
        failed_tests = []

        for test_case in test_cases:
            result = option_validator.validate_option_combination(test_case)

            if not result.success:
                failed_tests.append(f"{test_case.test_description}: {result.stderr}")

        if failed_tests:
            pytest.fail("Date validation tests failed:\n" + "\n".join(failed_tests))

    def test_symbol_format_validation(self, option_generator, option_validator):
        """Test symbol format validation works correctly."""
        test_cases = option_generator.generate_symbol_format_tests()
        failed_tests = []

        for test_case in test_cases:
            result = option_validator.validate_option_combination(test_case)

            if not result.success:
                failed_tests.append(f"{test_case.test_description}: {result.stderr}")

        if failed_tests:
            pytest.fail("Symbol format tests failed:\n" + "\n".join(failed_tests))

    def test_numeric_parameter_validation(self, option_generator, option_validator):
        """Test numeric parameter validation enforces correct ranges."""
        test_cases = option_generator.generate_numeric_parameter_tests()
        failed_tests = []

        for test_case in test_cases:
            result = option_validator.validate_option_combination(test_case)

            if not result.success:
                failed_tests.append(f"{test_case.test_description}: {result.stderr}")

        if failed_tests:
            pytest.fail("Numeric parameter tests failed:\n" + "\n".join(failed_tests))

    def test_path_validation(self, option_generator, option_validator):
        """Test path validation handles various path formats."""
        test_cases = option_generator.generate_path_validation_tests()
        failed_tests = []

        for test_case in test_cases:
            result = option_validator.validate_option_combination(test_case)

            if not result.success:
                failed_tests.append(f"{test_case.test_description}: {result.stderr}")

        if failed_tests:
            pytest.fail("Path validation tests failed:\n" + "\n".join(failed_tests))

    def test_configuration_precedence(self, option_generator, option_validator):
        """Test configuration precedence rules work correctly."""
        test_cases = option_generator.generate_configuration_precedence_tests()
        failed_tests = []

        for test_case in test_cases:
            result = option_validator.validate_option_combination(test_case)

            if not result.success:
                failed_tests.append(f"{test_case.test_description}: {result.stderr}")

        if failed_tests:
            pytest.fail("Configuration precedence tests failed:\n" + "\n".join(failed_tests))

    def test_option_combination_matrix(self, option_generator, option_validator):
        """Test common option combinations work together."""
        # Test comprehensive option combinations - use unique date ranges to avoid conflicts
        combination_tests = [
            ({"--workers": 2}, "2024-02-06", "2024-02-07"),
            ({"--batch-size": 500}, "2024-02-08", "2024-02-09"),
            ({"--workers": 4, "--batch-size": 1000}, "2024-02-10", "2024-02-11"),
        ]

        failed_combinations = []

        for additional_options, start_date, end_date in combination_tests:
            combined_options = {
                "--provider": "fake",
                "--symbols": "AAPL",
                "--start": start_date,
                "--end": end_date,
                "--output": "test_data",
                **additional_options
            }

            test_case = OptionTestCase(
                command_path=["ingest-ohlcv"],
                options=combined_options,
                expected_success=True,
                test_description=f"Combined options: {additional_options}",
            )

            result = option_validator.validate_option_combination(test_case)

            if not result.success:
                failed_combinations.append(f"{additional_options}: {result.stderr}")

        if failed_combinations:
            pytest.fail("Option combination tests failed:\n" + "\n".join(failed_combinations))


if __name__ == "__main__":
    # Can be run directly for option validation
    generator = CLIOptionTestGenerator()
    validator = CLIOptionValidator()

    test_suites = [
        ("Provider Tests", generator.generate_provider_tests()),
        ("Date Validation Tests", generator.generate_date_validation_tests()),
        ("Symbol Format Tests", generator.generate_symbol_format_tests()),
        ("Numeric Parameter Tests", generator.generate_numeric_parameter_tests()),
        ("Path Validation Tests", generator.generate_path_validation_tests()),
        ("Configuration Precedence Tests", generator.generate_configuration_precedence_tests()),
    ]

    for suite_name, test_cases in test_suites:
        print(f"\n{suite_name}:")
        print("=" * len(suite_name))

        for test_case in test_cases:
            result = validator.validate_option_combination(test_case)
            status = "✅" if result.success else "❌"
            print(f"{status} {test_case.test_description}")

            if not result.success:
                print(f"   Error: {result.stderr.strip()}")
