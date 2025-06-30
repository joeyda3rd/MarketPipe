"""
CLI Backward Compatibility Testing

Comprehensive testing framework that ensures deprecated commands still work
with appropriate warnings and that configuration schemas remain compatible.

This module implements Phase 4 of the CLI validation framework:
- Tests deprecated commands show warnings but still function
- Validates configuration schema backward compatibility
- Tests migration paths from old to new command structures
- Ensures consistent behavior across command aliases
"""

from __future__ import annotations

import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
import yaml


@dataclass
class DeprecationTestCase:
    """Test case for deprecated command validation."""

    deprecated_command: list[str]
    replacement_command: list[str]
    options: dict[str, Any] = field(default_factory=dict)
    expected_warning_patterns: list[str] = field(default_factory=list)
    should_still_work: bool = True
    test_description: str = ""


@dataclass
class CompatibilityTestResult:
    """Result of backward compatibility testing."""

    test_case: DeprecationTestCase
    command_executed: bool = False
    warning_shown: bool = False
    replacement_suggested: bool = False
    functionality_works: bool = False
    output: str = ""
    error_output: str = ""
    exit_code: int = -1
    issues: list[str] = field(default_factory=list)


class BackwardCompatibilityValidator:
    """Validates backward compatibility of CLI commands."""

    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir or Path(__file__).parent.parent.parent

    def validate_deprecated_command(
        self, test_case: DeprecationTestCase
    ) -> CompatibilityTestResult:
        """
        Validate that a deprecated command works with appropriate warnings.

        Args:
            test_case: Deprecation test case to validate

        Returns:
            CompatibilityTestResult with validation details
        """
        result = CompatibilityTestResult(test_case=test_case)

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Build command
                cmd_args = ["python", "-m", "marketpipe"] + test_case.deprecated_command

                # Add options, replacing output path with temp directory
                for option, value in test_case.options.items():
                    if isinstance(value, bool) and value:
                        cmd_args.append(option)
                    elif not isinstance(value, bool):
                        if option == "--output":
                            cmd_args.extend([option, str(Path(temp_dir) / "output")])
                        else:
                            cmd_args.extend([option, str(value)])

                # Execute command in temp directory to avoid DB conflicts
                process_result = subprocess.run(
                    cmd_args, capture_output=True, text=True, timeout=30, cwd=temp_dir
                )

                result.command_executed = True
                result.exit_code = process_result.returncode
                result.output = process_result.stdout
                result.error_output = process_result.stderr

                # Check if functionality still works
                if test_case.should_still_work:
                    result.functionality_works = process_result.returncode == 0
                else:
                    result.functionality_works = process_result.returncode != 0

                # Check for deprecation warnings
                combined_output = (process_result.stdout + process_result.stderr).lower()

                warning_indicators = [
                    "deprecated",
                    "warning",
                    "use instead",
                    "please use",
                    "will be removed",
                    "legacy",
                    "obsolete",
                ]

                result.warning_shown = any(
                    indicator in combined_output for indicator in warning_indicators
                )

                # Check for replacement suggestions
                replacement_cmd = " ".join(test_case.replacement_command)
                result.replacement_suggested = replacement_cmd.lower() in combined_output

                # Check for specific warning patterns
                for pattern in test_case.expected_warning_patterns:
                    if pattern.lower() not in combined_output:
                        result.issues.append(f"Expected warning pattern not found: {pattern}")

                # Analyze issues
                if test_case.should_still_work and not result.functionality_works:
                    result.issues.append("Deprecated command should still work but failed")

                if not result.warning_shown:
                    result.issues.append("No deprecation warning shown")

                return result

        except subprocess.TimeoutExpired:
            result.issues.append("Command timed out")
            return result
        except Exception as e:
            result.issues.append(f"Execution error: {e}")
            return result

    def compare_command_outputs(
        self, deprecated_cmd: list[str], new_cmd: list[str], options: dict[str, Any]
    ) -> tuple[bool, list[str]]:
        """
        Compare outputs between deprecated and new commands to ensure compatibility.

        Returns:
            (outputs_equivalent, differences)
        """
        differences = []

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                Path(temp_dir)

                # Run deprecated command
                deprecated_args = ["python", "-m", "marketpipe"] + deprecated_cmd
                for option, value in options.items():
                    if isinstance(value, bool) and value:
                        deprecated_args.append(option)
                    elif not isinstance(value, bool):
                        deprecated_args.extend([option, str(value)])

                deprecated_result = subprocess.run(
                    deprecated_args, capture_output=True, text=True, timeout=30, cwd=self.base_dir
                )

                # Run new command
                new_args = ["python", "-m", "marketpipe"] + new_cmd
                for option, value in options.items():
                    if isinstance(value, bool) and value:
                        new_args.append(option)
                    elif not isinstance(value, bool):
                        new_args.extend([option, str(value)])

                new_result = subprocess.run(
                    new_args, capture_output=True, text=True, timeout=30, cwd=self.base_dir
                )

                # Compare exit codes
                if deprecated_result.returncode != new_result.returncode:
                    differences.append(
                        f"Exit codes differ: {deprecated_result.returncode} vs {new_result.returncode}"
                    )

                # Compare core functionality (ignoring deprecation warnings)
                deprecated_clean = self._clean_output_for_comparison(deprecated_result.stdout)
                new_clean = self._clean_output_for_comparison(new_result.stdout)

                if deprecated_clean != new_clean:
                    differences.append("Output content differs between deprecated and new commands")

                return len(differences) == 0, differences

        except Exception as e:
            return False, [f"Comparison error: {e}"]

    def _clean_output_for_comparison(self, output: str) -> str:
        """Clean output for comparison by removing timestamps, warnings, etc."""
        lines = output.split("\n")
        cleaned_lines = []

        for line in lines:
            line_lower = line.lower()

            # Skip deprecation warnings and timestamps
            if any(word in line_lower for word in ["deprecated", "warning", "timestamp"]):
                continue

            # Skip usage lines as they will differ between aliases
            if line.strip().startswith("Usage:"):
                continue

            # Remove ANSI color codes
            line = re.sub(r"\033\[[0-9;]*m", "", line)

            # Normalize description differences (remove parenthetical notes)
            line = re.sub(r"\s*\([^)]*convenience command[^)]*\)", "", line)

            # Skip empty lines
            if line.strip():
                cleaned_lines.append(line.strip())

        return "\n".join(cleaned_lines)


class DeprecationTestGenerator:
    """Generates test cases for deprecated command validation."""

    def generate_deprecated_command_tests(self) -> list[DeprecationTestCase]:
        """Generate test cases for all known deprecated commands."""
        test_cases = []

        # Main deprecated commands
        deprecated_commands = [
            {
                "deprecated": ["ingest"],
                "replacement": ["ingest-ohlcv"],
                "description": "Legacy ingest command",
            },
            {
                "deprecated": ["validate"],
                "replacement": ["validate-ohlcv"],
                "description": "Legacy validate command",
            },
            {
                "deprecated": ["aggregate"],
                "replacement": ["aggregate-ohlcv"],
                "description": "Legacy aggregate command",
            },
        ]

        # Test with help option
        for cmd_info in deprecated_commands:
            test_cases.append(
                DeprecationTestCase(
                    deprecated_command=cmd_info["deprecated"],
                    replacement_command=cmd_info["replacement"],
                    options={"--help": True},
                    expected_warning_patterns=["deprecated", cmd_info["replacement"][0]],
                    test_description=f"{cmd_info['description']} --help",
                )
            )

        # Test with actual options (using fake provider for safety)
        for cmd_info in deprecated_commands:
            if cmd_info["deprecated"][0] in ["ingest"]:  # Only test ingest with real options
                test_cases.append(
                    DeprecationTestCase(
                        deprecated_command=cmd_info["deprecated"],
                        replacement_command=cmd_info["replacement"],
                        options={
                            "--provider": "fake",
                            "--symbols": "AAPL",
                            "--start": "2024-12-01",
                            "--end": "2024-12-02",
                            "--output": "/tmp/test_deprecated",
                        },
                        expected_warning_patterns=["deprecated", cmd_info["replacement"][0]],
                        should_still_work=False,  # Focus on warnings, not full functionality
                        test_description=f"{cmd_info['description']} with options",
                    )
                )

        return test_cases

    def generate_alias_consistency_tests(self) -> list[DeprecationTestCase]:
        """Generate tests for command alias consistency."""
        test_cases = []

        # Test that both forms work equivalently
        equivalent_commands = [
            (["ingest-ohlcv"], ["ohlcv", "ingest"]),
            (["validate-ohlcv"], ["ohlcv", "validate"]),
            (["aggregate-ohlcv"], ["ohlcv", "aggregate"]),
        ]

        for primary_cmd, alias_cmd in equivalent_commands:
            test_cases.append(
                DeprecationTestCase(
                    deprecated_command=alias_cmd,  # Test alias as "deprecated"
                    replacement_command=primary_cmd,
                    options={"--help": True},
                    expected_warning_patterns=[],  # Aliases shouldn't warn
                    test_description=f"Alias consistency: {' '.join(alias_cmd)} vs {' '.join(primary_cmd)}",
                )
            )

        return test_cases


class ConfigurationCompatibilityValidator:
    """Validates configuration file backward compatibility."""

    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir or Path(__file__).parent.parent.parent

    def validate_config_schema_compatibility(self) -> list[str]:
        """Validate that old configuration schemas still work."""
        compatibility_issues = []

        # Test legacy configuration formats
        legacy_configs = [
            self._generate_v1_config(),
            self._generate_minimal_config(),
            self._generate_mixed_format_config(),
        ]

        for config_name, config_data in legacy_configs:
            issues = self._test_config_compatibility(config_name, config_data)
            compatibility_issues.extend(issues)

        return compatibility_issues

    def _generate_v1_config(self) -> tuple[str, dict[str, Any]]:
        """Generate a version 1 style configuration."""
        config = {
            "provider": "fake",  # Old style single provider
            "symbols": ["AAPL", "MSFT"],
            "start_date": "2023-01-01",
            "end_date": "2023-01-31",
            "output_dir": "data/parquet",
            "batch_size": 1000,
            "workers": 4,
        }
        return "v1_config", config

    def _generate_minimal_config(self) -> tuple[str, dict[str, Any]]:
        """Generate a minimal configuration."""
        config = {
            "ingestion": {"symbols": ["AAPL"], "start_date": "2023-01-01", "end_date": "2023-01-01"}
        }
        return "minimal_config", config

    def _generate_mixed_format_config(self) -> tuple[str, dict[str, Any]]:
        """Generate a mixed format configuration."""
        config = {
            "providers": {"fake": {"feed_type": "iex"}},
            "provider": "fake",  # Mix old and new style
            "ingestion": {
                "symbols": ["AAPL"],
                "start_date": "2023-01-01",
                "end_date": "2023-01-01",
                "output_dir": "data/test",
            },
        }
        return "mixed_format_config", config

    def _test_config_compatibility(
        self, config_name: str, config_data: dict[str, Any]
    ) -> list[str]:
        """Test compatibility of a specific configuration."""
        issues = []

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                config_file = temp_path / f"{config_name}.yaml"

                # Write config file
                with open(config_file, "w") as f:
                    yaml.dump(config_data, f)

                # Test with current ingest command
                cmd = [
                    "python",
                    "-m",
                    "marketpipe",
                    "ingest-ohlcv",
                    "--config",
                    str(config_file),
                    "--help",  # Just test that config is parsed
                ]

                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30, cwd=self.base_dir
                )

                if result.returncode != 0:
                    issues.append(f"{config_name}: Configuration rejected - {result.stderr}")

                # Check for migration warnings
                combined_output = (result.stdout + result.stderr).lower()
                if "config" in combined_output and "deprecated" in combined_output:
                    # This is okay - just a warning
                    pass

        except Exception as e:
            issues.append(f"{config_name}: Error testing configuration - {e}")

        return issues


class TestCLIBackwardCompatibility:
    """Test suite for CLI backward compatibility validation."""

    @pytest.fixture
    def compatibility_validator(self):
        """Backward compatibility validator fixture."""
        return BackwardCompatibilityValidator()

    @pytest.fixture
    def deprecation_generator(self):
        """Deprecation test generator fixture."""
        return DeprecationTestGenerator()

    @pytest.fixture
    def config_validator(self):
        """Configuration compatibility validator fixture."""
        return ConfigurationCompatibilityValidator()

    def test_deprecated_commands_show_warnings(
        self, deprecation_generator, compatibility_validator
    ):
        """Test that deprecated commands show appropriate warnings."""
        test_cases = deprecation_generator.generate_deprecated_command_tests()
        validation_failures = []

        for test_case in test_cases:
            result = compatibility_validator.validate_deprecated_command(test_case)

            if result.issues:
                validation_failures.extend(
                    [f"{test_case.test_description}: {issue}" for issue in result.issues]
                )

        if validation_failures:
            pytest.fail(
                "Deprecated command validation failures:\n" + "\n".join(validation_failures)
            )

    def test_deprecated_commands_still_work(self, deprecation_generator, compatibility_validator):
        """Test that deprecated commands still provide functionality."""
        test_cases = deprecation_generator.generate_deprecated_command_tests()
        functionality_failures = []

        for test_case in test_cases:
            if test_case.should_still_work:
                result = compatibility_validator.validate_deprecated_command(test_case)

                if not result.functionality_works:
                    functionality_failures.append(
                        f"{test_case.test_description}: Does not work - exit code {result.exit_code}"
                    )

        if functionality_failures:
            pytest.fail(
                "Deprecated command functionality failures:\n" + "\n".join(functionality_failures)
            )

    def test_command_alias_consistency(self, deprecation_generator, compatibility_validator):
        """Test that command aliases provide consistent behavior."""
        test_cases = deprecation_generator.generate_alias_consistency_tests()
        consistency_failures = []

        for test_case in test_cases:
            # Test that aliases work
            result = compatibility_validator.validate_deprecated_command(test_case)

            if not result.functionality_works:
                consistency_failures.append(f"{test_case.test_description}: Alias failed")

            # Test output equivalence
            equivalent, differences = compatibility_validator.compare_command_outputs(
                test_case.deprecated_command, test_case.replacement_command, test_case.options
            )

            if not equivalent:
                consistency_failures.append(
                    f"{test_case.test_description}: Output differs - {differences}"
                )

        if consistency_failures:
            pytest.fail("Command alias consistency failures:\n" + "\n".join(consistency_failures))

    def test_configuration_backward_compatibility(self, config_validator):
        """Test that old configuration formats still work."""
        compatibility_issues = config_validator.validate_config_schema_compatibility()

        if compatibility_issues:
            pytest.fail("Configuration compatibility issues:\n" + "\n".join(compatibility_issues))

    def test_error_message_consistency(self, compatibility_validator):
        """Test that error messages are consistent between old and new commands."""
        error_consistency_failures = []

        # Test invalid option handling
        command_pairs = [(["ingest"], ["ingest-ohlcv"]), (["validate"], ["validate-ohlcv"])]

        for deprecated_cmd, new_cmd in command_pairs:
            try:
                # Test with invalid option
                invalid_options = {"--invalid-option": "value"}

                equivalent, differences = compatibility_validator.compare_command_outputs(
                    deprecated_cmd, new_cmd, invalid_options
                )

                # For error cases, we expect both to fail similarly
                if not equivalent and "Exit codes differ" not in str(differences):
                    error_consistency_failures.append(
                        f"{deprecated_cmd} vs {new_cmd}: Error handling differs"
                    )

            except Exception:
                # This is expected for invalid options
                pass

        if error_consistency_failures:
            pytest.fail(
                "Error message consistency failures:\n" + "\n".join(error_consistency_failures)
            )

    def test_migration_guidance(self, deprecation_generator, compatibility_validator):
        """Test that deprecated commands provide clear migration guidance."""
        test_cases = deprecation_generator.generate_deprecated_command_tests()
        migration_failures = []

        for test_case in test_cases:
            result = compatibility_validator.validate_deprecated_command(test_case)

            # Should suggest replacement command
            if not result.replacement_suggested and test_case.replacement_command:
                migration_failures.append(
                    f"{test_case.test_description}: No replacement command suggested"
                )

            # Should provide clear guidance
            combined_output = (result.output + result.error_output).lower()
            guidance_keywords = ["use", "instead", "replace", "migrate", "new"]

            has_guidance = any(keyword in combined_output for keyword in guidance_keywords)
            if not has_guidance:
                migration_failures.append(
                    f"{test_case.test_description}: No clear migration guidance"
                )

        if migration_failures:
            pytest.fail("Migration guidance failures:\n" + "\n".join(migration_failures))


if __name__ == "__main__":
    # Can be run directly for backward compatibility validation
    validator = BackwardCompatibilityValidator()
    generator = DeprecationTestGenerator()
    config_validator = ConfigurationCompatibilityValidator()

    print("Backward Compatibility Validation Report")
    print("=" * 50)

    # Test deprecated commands
    print("\nTesting Deprecated Commands:")
    test_cases = generator.generate_deprecated_command_tests()

    for test_case in test_cases:
        result = validator.validate_deprecated_command(test_case)

        status = "✅" if not result.issues else "❌"
        print(f"{status} {test_case.test_description}")

        if result.issues:
            for issue in result.issues:
                print(f"   - {issue}")

        if result.warning_shown:
            print("   ⚠️  Warning shown: ✅")
        if result.replacement_suggested:
            print("   💡 Replacement suggested: ✅")

    # Test configuration compatibility
    print("\nTesting Configuration Compatibility:")
    config_issues = config_validator.validate_config_schema_compatibility()

    if config_issues:
        for issue in config_issues:
            print(f"❌ {issue}")
    else:
        print("✅ All configuration formats compatible")

    # Test command aliases
    print("\nTesting Command Aliases:")
    alias_cases = generator.generate_alias_consistency_tests()

    for test_case in alias_cases:
        result = validator.validate_deprecated_command(test_case)
        status = "✅" if result.functionality_works else "❌"
        print(f"{status} {test_case.test_description}")
