"""
Enhanced CLI Command Testing Matrix

This module extends the existing CLI testing framework with:
- Edge case option combinations and parameter validation
- Error condition testing and boundary scenarios
- Command interaction and chaining validation
- Configuration precedence and environment variable testing
- Performance testing for command execution
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest


@dataclass
class EdgeCaseTest:
    """Definition for an edge case test scenario."""

    command_path: list[str]
    test_name: str
    options: dict[str, Any] = field(default_factory=dict)
    positional_args: list[str] = field(default_factory=list)  # For positional arguments
    env_vars: dict[str, str] = field(default_factory=dict)
    config_content: str | None = None
    expected_exit_code: int = 0
    expected_error_patterns: list[str] = field(default_factory=list)
    expected_success_patterns: list[str] = field(default_factory=list)
    timeout_seconds: int = 30
    category: str = "general"


@dataclass
class EdgeCaseResult:
    """Result of an edge case test execution."""

    test: EdgeCaseTest
    success: bool = False
    exit_code: int = -1
    stdout: str = ""
    stderr: str = ""
    execution_time_ms: float = 0.0
    error_messages: list[str] = field(default_factory=list)
    patterns_matched: list[str] = field(default_factory=list)


class EnhancedCLITester:
    """Enhanced CLI testing with edge cases and error scenarios."""

    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir or Path(__file__).parent.parent.parent
        self.test_results: list[EdgeCaseResult] = []

    def create_edge_case_tests(self) -> list[EdgeCaseTest]:
        """Create comprehensive edge case test scenarios."""
        tests = []

        # 1. Date Range Edge Cases
        tests.extend(self._create_date_edge_cases())

        # 2. Symbol Format Edge Cases
        tests.extend(self._create_symbol_edge_cases())

        # 3. Configuration Edge Cases
        tests.extend(self._create_config_edge_cases())

        # 4. Provider and Authentication Edge Cases
        tests.extend(self._create_provider_edge_cases())

        # 5. Numeric Parameter Edge Cases
        tests.extend(self._create_numeric_edge_cases())

        # 6. File System Edge Cases
        tests.extend(self._create_filesystem_edge_cases())

        # 7. Factory Reset Edge Cases
        tests.extend(self._create_factory_reset_edge_cases())

        return tests

    def _create_date_edge_cases(self) -> list[EdgeCaseTest]:
        """Create date-related edge case tests."""
        return [
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="valid_date_range",
                options={
                    "--start": "2024-01-07",
                    "--end": "2024-01-08",
                    "--provider": "fake",
                    "--symbols": "COST",
                },
                category="dates",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="invalid_date_format",
                options={
                    "--start": "invalid-date",
                    "--end": "2024-01-02",
                    "--provider": "fake",
                    "--symbols": "AAPL",
                },
                expected_exit_code=2,
                expected_error_patterns=["invalid", "date"],
                category="dates",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="end_before_start",
                options={
                    "--start": "2024-01-15",
                    "--end": "2024-01-01",
                    "--provider": "fake",
                    "--symbols": "AAPL",
                },
                expected_exit_code=2,
                expected_error_patterns=["end date", "start date"],
                category="dates",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="future_dates",
                options={
                    "--start": "2030-01-01",
                    "--end": "2030-01-02",
                    "--provider": "fake",
                    "--symbols": "AAPL",
                },
                expected_exit_code=2,
                expected_error_patterns=["future", "date"],
                category="dates",
            ),
        ]

    def _create_symbol_edge_cases(self) -> list[EdgeCaseTest]:
        """Create symbol format edge case tests."""
        return [
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="valid_single_symbol",
                options={
                    "--symbols": "MSFT",
                    "--provider": "fake",
                    "--start": "2024-01-03",
                    "--end": "2024-01-04",
                },
                category="symbols",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="valid_multiple_symbols",
                options={
                    "--symbols": "GOOGL,TSLA,AMZN",
                    "--provider": "fake",
                    "--start": "2024-01-05",
                    "--end": "2024-01-06",
                },
                category="symbols",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="empty_symbol",
                options={
                    "--symbols": "",
                    "--provider": "fake",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["empty", "symbol"],
                category="symbols",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="invalid_symbol_chars",
                options={
                    "--symbols": "AA$PL,M@SFT",
                    "--provider": "fake",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["invalid", "symbol"],
                category="symbols",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="too_many_symbols",
                options={
                    "--symbols": ",".join([f"SYM{i:03d}" for i in range(1000)]),
                    "--provider": "fake",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["too many", "limit"],
                category="symbols",
            ),
        ]

    def _create_config_edge_cases(self) -> list[EdgeCaseTest]:
        """Create configuration-related edge case tests."""
        return [
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="nonexistent_config",
                options={"--config": "/nonexistent/path/config.yaml"},
                expected_exit_code=2,
                expected_error_patterns=["not found", "config"],
                category="config",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="invalid_yaml_config",
                config_content="invalid: yaml: content: [unclosed",
                expected_exit_code=2,
                expected_error_patterns=["invalid", "yaml", "config"],
                category="config",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="empty_config",
                config_content="",
                expected_exit_code=2,
                expected_error_patterns=["empty", "config"],
                category="config",
            ),
        ]

    def _create_provider_edge_cases(self) -> list[EdgeCaseTest]:
        """Create provider and authentication edge cases."""
        return [
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="invalid_provider",
                options={
                    "--provider": "nonexistent_provider",
                    "--symbols": "AAPL",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["invalid", "provider"],
                category="providers",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="missing_feed_type",
                options={
                    "--provider": "alpaca",
                    "--symbols": "AAPL",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["feed", "type", "required"],
                category="providers",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="invalid_feed_type",
                options={
                    "--provider": "alpaca",
                    "--feed-type": "invalid_feed",
                    "--symbols": "AAPL",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["invalid", "feed"],
                category="providers",
            ),
        ]

    def _create_numeric_edge_cases(self) -> list[EdgeCaseTest]:
        """Create numeric parameter edge cases."""
        return [
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="zero_workers",
                options={
                    "--workers": "0",
                    "--provider": "fake",
                    "--symbols": "AAPL",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["workers", "positive"],
                category="numeric",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="negative_workers",
                options={
                    "--workers": "-5",
                    "--provider": "fake",
                    "--symbols": "AAPL",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["workers", "positive"],
                category="numeric",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="excessive_workers",
                options={
                    "--workers": "1000",
                    "--provider": "fake",
                    "--symbols": "AAPL",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["workers", "maximum"],
                category="numeric",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="zero_batch_size",
                options={
                    "--batch-size": "0",
                    "--provider": "fake",
                    "--symbols": "AAPL",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["batch", "positive"],
                category="numeric",
            ),
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="excessive_batch_size",
                options={
                    "--batch-size": "100000",
                    "--provider": "fake",
                    "--symbols": "AAPL",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["batch", "maximum"],
                category="numeric",
            ),
        ]

    def _create_filesystem_edge_cases(self) -> list[EdgeCaseTest]:
        """Create file system related edge cases."""
        return [
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="nonexistent_output_dir",
                options={
                    "--output": "/nonexistent/path/output",
                    "--provider": "fake",
                    "--symbols": "AAPL",
                    "--start": "2024-01-01",
                    "--end": "2024-01-02",
                },
                expected_exit_code=2,
                expected_error_patterns=["parent", "directory", "does not exist"],
                category="filesystem",
            ),
            EdgeCaseTest(
                command_path=["prune", "parquet"],
                test_name="invalid_age_format",
                positional_args=["30x"],  # positional argument
                expected_exit_code=2,
                expected_error_patterns=["age", "format", "invalid"],
                category="filesystem",
            ),
        ]

    def _create_factory_reset_edge_cases(self) -> list[EdgeCaseTest]:
        """Create factory reset specific edge cases."""
        return [
            EdgeCaseTest(
                command_path=["factory-reset"],
                test_name="missing_nuclear_confirmation",
                options={},
                expected_exit_code=1,
                expected_error_patterns=["confirm-nuclear", "safety"],
                category="factory_reset",
            ),
            EdgeCaseTest(
                command_path=["factory-reset"],
                test_name="dry_run_with_nuclear",
                options={"--dry-run": True, "--confirm-nuclear": True},
                expected_exit_code=0,
                expected_success_patterns=["dry run", "no files would be deleted"],
                category="factory_reset",
            ),
            EdgeCaseTest(
                command_path=["factory-reset"],
                test_name="nonexistent_base_dir",
                options={
                    "--base-dir": "/nonexistent/path",
                    "--dry-run": True,
                    "--confirm-nuclear": True,
                },
                expected_exit_code=0,
                expected_success_patterns=["no data files found", "clean"],
                category="factory_reset",
            ),
        ]

    def execute_test(self, test: EdgeCaseTest) -> EdgeCaseResult:
        """Execute a single edge case test."""
        result = EdgeCaseResult(test=test)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Set up environment variables
            env = os.environ.copy()
            env.update(test.env_vars)

            # Create config file if needed
            config_file = None
            if test.config_content is not None:
                config_file = temp_path / "test_config.yaml"
                config_file.write_text(test.config_content)
                test.options["--config"] = str(config_file)

            # Build command
            cmd = ["python", "-m", "marketpipe"] + test.command_path

            # test.options may be provided as a *set* of flag names in some cases – convert that
            # to a mapping of flag -> True so we can iterate with .items() safely.
            if isinstance(test.options, set):
                test.options = dict.fromkeys(test.options, True)

            # Add options
            for key, value in test.options.items():
                if isinstance(value, bool) and value:
                    cmd.append(key)
                elif not isinstance(value, bool):
                    cmd.extend([key, str(value)])

            # Add positional arguments
            cmd.extend(test.positional_args)

            # Execute command
            try:
                start_time = time.time()
                process_result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=test.timeout_seconds,
                    cwd=self.base_dir,
                    env=env,
                )
                result.execution_time_ms = (time.time() - start_time) * 1000
                result.exit_code = process_result.returncode
                result.stdout = process_result.stdout
                result.stderr = process_result.stderr

            except subprocess.TimeoutExpired:
                result.error_messages.append(f"Command timed out after {test.timeout_seconds}s")
                result.exit_code = -1
            except Exception as e:
                result.error_messages.append(f"Execution error: {e}")
                result.exit_code = -1

        # Validate results
        result.success = self._validate_test_result(test, result)

        return result

    def _validate_test_result(self, test: EdgeCaseTest, result: EdgeCaseResult) -> bool:
        """Validate if a test result matches expectations."""
        # Check exit code
        if result.exit_code != test.expected_exit_code:
            result.error_messages.append(
                f"Expected exit code {test.expected_exit_code}, got {result.exit_code}"
            )
            return False

        # Check error patterns
        combined_output = (result.stdout + result.stderr).lower()
        for pattern in test.expected_error_patterns:
            if pattern.lower() not in combined_output:
                result.error_messages.append(f"Expected error pattern '{pattern}' not found")
                return False
            else:
                result.patterns_matched.append(pattern)

        # Check success patterns
        for pattern in test.expected_success_patterns:
            if pattern.lower() not in combined_output:
                result.error_messages.append(f"Expected success pattern '{pattern}' not found")
                return False
            else:
                result.patterns_matched.append(pattern)

        return True

    def run_all_edge_case_tests(self) -> list[EdgeCaseResult]:
        """Run all edge case tests and return results."""
        tests = self.create_edge_case_tests()
        results = []

        for test in tests:
            result = self.execute_test(test)
            results.append(result)

        self.test_results = results
        return results

    def generate_edge_case_report(self, results: list[EdgeCaseResult]) -> str:
        """Generate comprehensive edge case testing report."""
        report = []
        report.append("# MarketPipe CLI Edge Case Testing Report")
        report.append("=" * 60)
        report.append("")

        # Summary statistics
        total_tests = len(results)
        passed_tests = sum(1 for r in results if r.success)
        failed_tests = total_tests - passed_tests

        report.append("## Summary")
        report.append(f"- Total Edge Case Tests: {total_tests}")
        report.append(f"- Passed: {passed_tests} ({passed_tests/total_tests*100:.1f}%)")
        report.append(f"- Failed: {failed_tests} ({failed_tests/total_tests*100:.1f}%)")
        report.append("")

        # Category breakdown
        categories = {}
        for result in results:
            cat = result.test.category
            if cat not in categories:
                categories[cat] = {"total": 0, "passed": 0}
            categories[cat]["total"] += 1
            if result.success:
                categories[cat]["passed"] += 1

        report.append("## Results by Category")
        for category, stats in sorted(categories.items()):
            total = stats["total"]
            passed = stats["passed"]
            report.append(f"- {category.title()}: {passed}/{total} ({passed/total*100:.1f}%)")
        report.append("")

        # Failed tests details
        failed_results = [r for r in results if not r.success]
        if failed_results:
            report.append("## Failed Tests")
            for result in failed_results:
                report.append(f"### ❌ {result.test.test_name}")
                report.append(f"Command: `marketpipe {' '.join(result.test.command_path)}`")
                report.append(f"Category: {result.test.category}")
                report.append(
                    f"Exit Code: {result.exit_code} (expected: {result.test.expected_exit_code})"
                )

                if result.error_messages:
                    report.append("Errors:")
                    for error in result.error_messages:
                        report.append(f"  - {error}")

                if result.stdout:
                    report.append("STDOUT:")
                    report.append(f"```\n{result.stdout[:500]}...\n```")

                if result.stderr:
                    report.append("STDERR:")
                    report.append(f"```\n{result.stderr[:500]}...\n```")

                report.append("")

        return "\n".join(report)


class TestEnhancedCLIMatrix:
    """Enhanced CLI matrix test suite."""

    @pytest.fixture
    def tester(self):
        """Enhanced CLI tester fixture."""
        return EnhancedCLITester()

    def test_all_edge_cases(self, tester):
        """Run comprehensive edge case testing."""
        results = tester.run_all_edge_case_tests()

        # Generate report
        report = tester.generate_edge_case_report(results)
        print("\n" + report)

        # Collect failures
        failed_tests = [r for r in results if not r.success]

        if failed_tests:
            failure_summary = []
            for failed in failed_tests:
                failure_summary.append(
                    f"  - {failed.test.test_name} ({failed.test.category}): {', '.join(failed.error_messages)}"
                )

            pytest.fail(
                f"Found {len(failed_tests)} edge case test failures:\n"
                + "\n".join(failure_summary)
                + f"\n\nFull report:\n{report}"
            )

    def test_performance_benchmarks(self, tester):
        """Test command performance benchmarks."""
        # Test fast commands (help, providers, etc.)
        fast_commands = [
            ["--help"],
            ["providers"],
            ["migrate", "--help"],
            ["health-check", "--help"],
        ]

        slow_commands = []
        MAX_FAST_TIME_MS = 2000  # 2 seconds for help commands

        for cmd_path in fast_commands:
            test = EdgeCaseTest(
                command_path=cmd_path,
                test_name=f"performance_{'_'.join(cmd_path)}",
                category="performance",
            )

            result = tester.execute_test(test)

            if result.execution_time_ms > MAX_FAST_TIME_MS:
                slow_commands.append(f"{' '.join(cmd_path)}: {result.execution_time_ms:.1f}ms")

        if slow_commands:
            pytest.fail(
                f"Commands exceeded {MAX_FAST_TIME_MS}ms performance threshold:\n"
                + "\n".join(f"  - {cmd}" for cmd in slow_commands)
            )

    def test_command_option_combinations(self, tester):
        """Test valid option combinations work correctly."""
        valid_combinations = [
            EdgeCaseTest(
                command_path=["ingest-ohlcv"],
                test_name="basic_fake_provider",
                options={
                    "--provider": "fake",
                    "--symbols": "TEST",
                    "--start": "2024-01-09",
                    "--end": "2024-01-10",
                },
                expected_exit_code=0,
                expected_success_patterns=[
                    "post-ingestion",
                    "verification",
                    "completed successfully",
                ],
                category="combinations",
            ),
            EdgeCaseTest(
                command_path=["query"],
                test_name="query_with_csv_output",
                options={"--csv": True},
                positional_args=["SELECT 1 as test"],
                expected_exit_code=0,
                expected_success_patterns=["test", "1"],
                category="combinations",
            ),
            EdgeCaseTest(
                command_path=["prune", "parquet"],
                test_name="prune_with_dry_run",
                options={"--dry-run": True, "--root": "/nonexistent/parquet/path"},
                positional_args=["30d"],
                expected_exit_code=1,
                expected_error_patterns=["directory", "does not exist"],
                category="combinations",
            ),
        ]

        failed_combinations = []

        for test in valid_combinations:
            result = tester.execute_test(test)
            if not result.success:
                failed_combinations.append(f"{test.test_name}: {', '.join(result.error_messages)}")

        if failed_combinations:
            pytest.fail(
                "Valid option combinations failed:\n"
                + "\n".join(f"  - {combo}" for combo in failed_combinations)
            )


if __name__ == "__main__":
    # Can be run directly for quick validation
    tester = EnhancedCLITester()

    print("Running enhanced CLI edge case tests...")
    results = tester.run_all_edge_case_tests()

    print(f"\nCompleted {len(results)} edge case tests")
    report = tester.generate_edge_case_report(results)
    print(report)

    failed_count = sum(1 for r in results if not r.success)
    if failed_count > 0:
        print(f"\n❌ {failed_count} tests failed")
        exit(1)
    else:
        print("\n✅ All edge case tests passed")
