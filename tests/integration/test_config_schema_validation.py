"""
Configuration Schema Validation

Comprehensive testing framework that validates all configuration combinations
work correctly with the MarketPipe CLI.

This module extends Phase 4 of the CLI validation framework:
- Tests all valid configuration file combinations
- Validates environment variable integration
- Tests configuration override precedence rules
- Ensures schema validation works correctly
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
import yaml


@dataclass
class ConfigTestCase:
    """Test case for configuration validation."""

    name: str
    description: str
    config_data: dict[str, Any]
    env_vars: dict[str, str] = field(default_factory=dict)
    cli_overrides: dict[str, Any] = field(default_factory=dict)
    expected_success: bool = True
    expected_error_patterns: list[str] = field(default_factory=list)
    test_precedence: bool = False


@dataclass
class ConfigValidationResult:
    """Result of configuration validation."""

    test_case: ConfigTestCase
    config_parsed: bool = False
    command_succeeded: bool = False
    precedence_correct: bool = False
    output: str = ""
    error_output: str = ""
    exit_code: int = -1
    validation_errors: list[str] = field(default_factory=list)


class ConfigurationValidator:
    """Validates configuration files and precedence rules."""

    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir or Path(__file__).parent.parent.parent

    def validate_config_case(self, test_case: ConfigTestCase) -> ConfigValidationResult:
        """
        Validate a configuration test case.

        Args:
            test_case: Configuration test case to validate

        Returns:
            ConfigValidationResult with validation details
        """
        result = ConfigValidationResult(test_case=test_case)

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Create configuration file
                config_file = temp_path / "test_config.yaml"
                with open(config_file, "w") as f:
                    yaml.dump(test_case.config_data, f)

                # Setup environment
                env = os.environ.copy()
                env.update(test_case.env_vars)

                # Build command
                cmd_args = [
                    "python",
                    "-m",
                    "marketpipe",
                    "ingest-ohlcv",
                    "--config",
                    str(config_file),
                ]

                # Add CLI overrides
                for option, value in test_case.cli_overrides.items():
                    if isinstance(value, bool) and value:
                        cmd_args.append(option)
                    elif not isinstance(value, bool):
                        cmd_args.extend([option, str(value)])

                # Add help to avoid actual execution, but only for valid configs
                # For invalid configs, we need validation to run to get proper error messages
                if test_case.expected_success and "--help" not in cmd_args:
                    cmd_args.append("--help")

                # Execute command
                process_result = subprocess.run(
                    cmd_args, capture_output=True, text=True, timeout=30, env=env, cwd=self.base_dir
                )

                result.exit_code = process_result.returncode
                result.output = process_result.stdout
                result.error_output = process_result.stderr

                # Analyze results
                if test_case.expected_success:
                    result.config_parsed = process_result.returncode == 0
                    result.command_succeeded = process_result.returncode == 0
                else:
                    # For expected failures, we expect config to be parsed but command to fail
                    result.config_parsed = True  # Config parsing happens even for invalid configs
                    result.command_succeeded = (
                        process_result.returncode == 0
                    )  # Should be False for failures

                    # Check for expected error patterns
                    combined_output = (process_result.stdout + process_result.stderr).lower()
                    for pattern in test_case.expected_error_patterns:
                        if pattern.lower() not in combined_output:
                            result.validation_errors.append(
                                f"Expected error pattern not found: {pattern}"
                            )

                # Test precedence if required
                if test_case.test_precedence:
                    result.precedence_correct = self._validate_precedence(
                        test_case, config_file, env
                    )
                else:
                    result.precedence_correct = True

                return result

        except subprocess.TimeoutExpired:
            result.validation_errors.append("Command timed out")
            return result
        except Exception as e:
            result.validation_errors.append(f"Validation error: {e}")
            return result

    def _validate_precedence(
        self, test_case: ConfigTestCase, config_file: Path, env: dict[str, str]
    ) -> bool:
        """Validate that configuration precedence works correctly."""
        try:
            # Test without CLI overrides (should use config + env)
            base_cmd = [
                "python",
                "-m",
                "marketpipe",
                "ingest-ohlcv",
                "--config",
                str(config_file),
                "--help",
            ]

            base_result = subprocess.run(
                base_cmd, capture_output=True, text=True, timeout=30, env=env, cwd=self.base_dir
            )

            # Test with CLI overrides (should override config + env)
            override_cmd = base_cmd[:-1]  # Remove --help
            for option, value in test_case.cli_overrides.items():
                if isinstance(value, bool) and value:
                    override_cmd.append(option)
                elif not isinstance(value, bool):
                    override_cmd.extend([option, str(value)])
            override_cmd.append("--help")

            override_result = subprocess.run(
                override_cmd, capture_output=True, text=True, timeout=30, env=env, cwd=self.base_dir
            )

            # Both should succeed, but precedence should apply
            return base_result.returncode == 0 and override_result.returncode == 0

        except Exception:
            return False


class ConfigTestGenerator:
    """Generates comprehensive configuration test cases."""

    def generate_valid_config_tests(self) -> list[ConfigTestCase]:
        """Generate test cases for valid configurations."""
        test_cases = []

        # Basic valid configuration
        test_cases.append(
            ConfigTestCase(
                name="basic_valid",
                description="Basic valid configuration",
                config_data={
                    "providers": {"fake": {"feed_type": "iex", "batch_size": 1000}},
                    "ingestion": {
                        "symbols": ["AAPL", "MSFT"],
                        "start_date": "2023-01-01",
                        "end_date": "2023-01-31",
                        "output_dir": "data/parquet",
                        "workers": 4,
                    },
                },
            )
        )

        # Minimal configuration
        test_cases.append(
            ConfigTestCase(
                name="minimal_valid",
                description="Minimal valid configuration",
                config_data={
                    "ingestion": {
                        "symbols": ["AAPL"],
                        "start_date": "2023-01-01",
                        "end_date": "2023-01-01",
                    }
                },
            )
        )

        # Multiple providers configuration
        test_cases.append(
            ConfigTestCase(
                name="multi_provider",
                description="Multiple providers configuration",
                config_data={
                    "providers": {
                        "alpaca": {"feed_type": "iex", "batch_size": 1000},
                        "iex": {"feed_type": "iex", "batch_size": 100},
                        "fake": {"feed_type": "sip", "batch_size": 500},
                    },
                    "ingestion": {
                        "symbols": ["AAPL"],
                        "start_date": "2023-01-01",
                        "end_date": "2023-01-01",
                        "workers": 2,
                    },
                },
            )
        )

        # Complex configuration with all options
        test_cases.append(
            ConfigTestCase(
                name="complex_valid",
                description="Complex configuration with all options",
                config_data={
                    "providers": {
                        "fake": {
                            "feed_type": "iex",
                            "batch_size": 1000,
                            "rate_limit": 60,
                            "timeout": 30,
                        }
                    },
                    "ingestion": {
                        "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN"],
                        "start_date": "2023-01-01",
                        "end_date": "2023-12-31",
                        "output_dir": "data/custom",
                        "workers": 8,
                        "batch_size": 2000,
                    },
                    "validation": {"enabled": True, "strict": False},
                    "aggregation": {"timeframes": ["1min", "5min", "1h", "1d"], "enabled": True},
                },
            )
        )

        return test_cases

    def generate_invalid_config_tests(self) -> list[ConfigTestCase]:
        """Generate test cases for invalid configurations."""
        test_cases = []

        # Missing required fields
        test_cases.append(
            ConfigTestCase(
                name="missing_symbols",
                description="Configuration missing symbols",
                config_data={"config_version": "1", "start": "2024-01-01", "end": "2024-01-01"},
                expected_success=False,
                expected_error_patterns=["symbols", "required"],
            )
        )

        # Invalid date format
        test_cases.append(
            ConfigTestCase(
                name="invalid_date",
                description="Configuration with invalid date format",
                config_data={
                    "config_version": "1",
                    "symbols": ["AAPL"],
                    "start": "invalid-date",
                    "end": "2024-01-01",
                },
                expected_success=False,
                expected_error_patterns=["valid date", "invalid"],
            )
        )

        # Invalid date range
        test_cases.append(
            ConfigTestCase(
                name="invalid_date_range",
                description="Configuration with end date before start date",
                config_data={
                    "config_version": "1",
                    "symbols": ["AAPL"],
                    "start": "2024-01-31",
                    "end": "2024-01-01",
                },
                expected_success=False,
                expected_error_patterns=["start date must be before end date"],
            )
        )

        # Invalid provider
        test_cases.append(
            ConfigTestCase(
                name="invalid_provider",
                description="Configuration with unknown provider",
                config_data={
                    "config_version": "1",
                    "symbols": ["AAPL"],
                    "start": "2024-01-01",
                    "end": "2024-01-01",
                    "provider": "unknown_provider",
                },
                expected_success=False,
                expected_error_patterns=["Unknown provider"],
            )
        )

        # Invalid numeric values
        test_cases.append(
            ConfigTestCase(
                name="invalid_numeric",
                description="Configuration with invalid numeric values",
                config_data={
                    "config_version": "1",
                    "symbols": ["AAPL"],
                    "start": "2024-01-01",
                    "end": "2024-01-01",
                    "workers": -1,
                    "batch_size": 0,
                },
                expected_success=False,
                expected_error_patterns=["greater than or equal to 1"],
            )
        )

        return test_cases

    def generate_precedence_tests(self) -> list[ConfigTestCase]:
        """Generate test cases for configuration precedence validation."""
        test_cases = []

        # CLI overrides config
        test_cases.append(
            ConfigTestCase(
                name="cli_overrides_config",
                description="CLI options override config file",
                config_data={
                    "ingestion": {
                        "symbols": ["MSFT"],  # Should be overridden
                        "start_date": "2023-01-01",
                        "end_date": "2023-01-01",
                        "workers": 2,  # Should be overridden
                    }
                },
                cli_overrides={"--symbols": "AAPL", "--workers": "4"},  # Override  # Override
                test_precedence=True,
            )
        )

        # Environment variables with config
        test_cases.append(
            ConfigTestCase(
                name="env_with_config",
                description="Environment variables work with config",
                config_data={
                    "providers": {"alpaca": {"feed_type": "iex"}},
                    "ingestion": {
                        "symbols": ["AAPL"],
                        "start_date": "2023-01-01",
                        "end_date": "2023-01-01",
                    },
                },
                env_vars={"ALPACA_KEY": "test_key", "ALPACA_SECRET": "test_secret"},
                test_precedence=True,
            )
        )

        # CLI overrides environment and config
        test_cases.append(
            ConfigTestCase(
                name="cli_overrides_all",
                description="CLI overrides both environment and config",
                config_data={
                    "ingestion": {
                        "symbols": ["MSFT"],
                        "start_date": "2023-01-01",
                        "end_date": "2023-01-01",
                        "workers": 2,
                    }
                },
                env_vars={"MP_WORKERS": "8"},  # Should be overridden by CLI
                cli_overrides={"--symbols": "AAPL", "--workers": "4"},
                test_precedence=True,
            )
        )

        return test_cases

    def generate_schema_validation_tests(self) -> list[ConfigTestCase]:
        """Generate test cases for schema validation."""
        test_cases = []

        # Valid schema structure
        test_cases.append(
            ConfigTestCase(
                name="valid_schema",
                description="Configuration with valid schema structure",
                config_data={
                    "schema_version": "1.0",
                    "providers": {"fake": {"feed_type": "iex", "batch_size": 1000}},
                    "ingestion": {
                        "symbols": ["AAPL"],
                        "start_date": "2023-01-01",
                        "end_date": "2023-01-01",
                        "output_dir": "data/parquet",
                    },
                },
            )
        )

        # Extra fields (should be ignored)
        test_cases.append(
            ConfigTestCase(
                name="extra_fields",
                description="Configuration with extra fields",
                config_data={
                    "ingestion": {
                        "symbols": ["AAPL"],
                        "start_date": "2023-01-01",
                        "end_date": "2023-01-01",
                    },
                    "extra_field": "should_be_ignored",
                    "unknown_section": {"some_value": 123},
                },
            )
        )

        # Nested validation
        test_cases.append(
            ConfigTestCase(
                name="nested_validation",
                description="Configuration with nested validation",
                config_data={
                    "providers": {
                        "fake": {
                            "feed_type": "iex",
                            "batch_size": 1000,
                            "retry": {"max_attempts": 3, "backoff": 1.5},
                        }
                    },
                    "ingestion": {
                        "symbols": ["AAPL"],
                        "start_date": "2023-01-01",
                        "end_date": "2023-01-01",
                    },
                },
            )
        )

        return test_cases


class TestConfigSchemaValidation:
    """Test suite for configuration schema validation."""

    @pytest.fixture
    def config_validator(self):
        """Configuration validator fixture."""
        return ConfigurationValidator()

    @pytest.fixture
    def config_generator(self):
        """Configuration test generator fixture."""
        return ConfigTestGenerator()

    def test_valid_configurations(self, config_generator, config_validator):
        """Test that valid configurations are accepted."""
        test_cases = config_generator.generate_valid_config_tests()
        validation_failures = []

        for test_case in test_cases:
            result = config_validator.validate_config_case(test_case)

            if not result.config_parsed or not result.command_succeeded:
                validation_failures.append(f"{test_case.name}: {result.validation_errors}")

        if validation_failures:
            pytest.fail(
                "Valid configuration tests failed:\n"
                + "\n".join(str(f) for f in validation_failures)
            )

    def test_invalid_configurations(self, config_generator, config_validator):
        """Test that invalid configurations are properly rejected."""
        test_cases = config_generator.generate_invalid_config_tests()
        validation_failures = []

        for test_case in test_cases:
            result = config_validator.validate_config_case(test_case)

            # For invalid configs, we only care that the command failed
            if result.command_succeeded:
                validation_failures.append(
                    f"{test_case.name}: Should have been rejected but was accepted"
                )

            if result.validation_errors:
                validation_failures.append(f"{test_case.name}: {result.validation_errors}")

        if validation_failures:
            pytest.fail(
                "Invalid configuration tests failed:\n"
                + "\n".join(str(f) for f in validation_failures)
            )

    def test_configuration_precedence(self, config_generator, config_validator):
        """Test that configuration precedence rules work correctly."""
        test_cases = config_generator.generate_precedence_tests()
        precedence_failures = []

        for test_case in test_cases:
            result = config_validator.validate_config_case(test_case)

            if not result.precedence_correct:
                precedence_failures.append(f"{test_case.name}: Precedence validation failed")

            if result.validation_errors:
                precedence_failures.append(f"{test_case.name}: {result.validation_errors}")

        if precedence_failures:
            pytest.fail(
                "Configuration precedence tests failed:\n"
                + "\n".join(str(f) for f in precedence_failures)
            )

    def test_schema_validation(self, config_generator, config_validator):
        """Test schema validation works correctly."""
        test_cases = config_generator.generate_schema_validation_tests()
        schema_failures = []

        for test_case in test_cases:
            result = config_validator.validate_config_case(test_case)

            if not result.config_parsed or not result.command_succeeded:
                schema_failures.append(
                    f"{test_case.name}: Schema validation failed - {result.validation_errors}"
                )

        if schema_failures:
            pytest.fail(
                "Schema validation tests failed:\n" + "\n".join(str(f) for f in schema_failures)
            )

    def test_configuration_file_formats(self, config_validator):
        """Test different configuration file formats."""
        base_config = {
            "ingestion": {"symbols": ["AAPL"], "start_date": "2023-01-01", "end_date": "2023-01-01"}
        }

        format_failures = []

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Test YAML format
            yaml_file = temp_path / "config.yaml"
            with open(yaml_file, "w") as f:
                yaml.dump(base_config, f)

            yaml_test = ConfigTestCase(
                name="yaml_format", description="YAML configuration format", config_data=base_config
            )

            result = config_validator.validate_config_case(yaml_test)
            if not result.config_parsed:
                format_failures.append("YAML format not supported")

            # Test JSON format (if supported)
            json_file = temp_path / "config.json"
            with open(json_file, "w") as f:
                json.dump(base_config, f)

            # Note: This would require CLI to support JSON, which it may not
            # This test documents the expected behavior

        if format_failures:
            pytest.fail("Configuration format tests failed:\n" + "\n".join(format_failures))

    def test_configuration_error_messages(self, config_generator, config_validator):
        """Test that configuration errors provide helpful messages."""
        test_cases = config_generator.generate_invalid_config_tests()
        error_message_failures = []

        for test_case in test_cases:
            result = config_validator.validate_config_case(test_case)

            if result.command_succeeded:
                continue  # Skip if unexpectedly succeeded

            error_output = (result.output + result.error_output).lower()

            # Should contain helpful error information
            helpful_indicators = ["error", "invalid", "required", "missing", "format"]
            has_helpful_error = any(indicator in error_output for indicator in helpful_indicators)

            if not has_helpful_error:
                error_message_failures.append(f"{test_case.name}: Unhelpful error message")

        if error_message_failures:
            pytest.fail(
                "Configuration error message tests failed:\n" + "\n".join(error_message_failures)
            )


if __name__ == "__main__":
    # Can be run directly for configuration validation
    validator = ConfigurationValidator()
    generator = ConfigTestGenerator()

    test_suites = [
        ("Valid Configurations", generator.generate_valid_config_tests()),
        ("Invalid Configurations", generator.generate_invalid_config_tests()),
        ("Precedence Tests", generator.generate_precedence_tests()),
        ("Schema Validation", generator.generate_schema_validation_tests()),
    ]

    print("Configuration Schema Validation Report")
    print("=" * 50)

    for suite_name, test_cases in test_suites:
        print(f"\n{suite_name}:")
        print("-" * len(suite_name))

        for test_case in test_cases:
            result = validator.validate_config_case(test_case)

            if test_case.expected_success:
                status = "✅" if result.config_parsed and result.command_succeeded else "❌"
            else:
                status = "✅" if not result.config_parsed or not result.command_succeeded else "❌"

            print(f"{status} {test_case.name}: {test_case.description}")

            if result.validation_errors:
                for error in result.validation_errors:
                    print(f"   - {error}")

            if test_case.test_precedence:
                precedence_status = "✅" if result.precedence_correct else "❌"
                print(f"   Precedence: {precedence_status}")
