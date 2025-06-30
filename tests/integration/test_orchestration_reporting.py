# SPDX-License-Identifier: Apache-2.0
"""E2E test orchestration and reporting framework.

This module provides orchestration capabilities for running comprehensive
end-to-end test suites and generating detailed reports on system behavior,
performance, and reliability across all MarketPipe components.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pytest


@dataclass
class E2ETestResult:
    """Individual test result data."""

    name: str
    status: str  # "PASS", "FAIL", "SKIP", "ERROR"
    duration_seconds: float
    error_message: str | None = None
    metadata: dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class E2ETestSuiteResult:
    """Test suite execution results."""

    suite_name: str
    start_time: datetime
    end_time: datetime
    total_tests: int
    passed: int
    failed: int
    skipped: int
    errors: int
    duration_seconds: float
    test_results: list[E2ETestResult]
    system_info: dict[str, Any]

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        return (self.passed / self.total_tests * 100) if self.total_tests > 0 else 0.0

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate as percentage."""
        return (
            ((self.failed + self.errors) / self.total_tests * 100) if self.total_tests > 0 else 0.0
        )


class E2ETestOrchestrator:
    """Orchestrates execution of comprehensive E2E test suites."""

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results: list[E2ETestSuiteResult] = []

    def run_test_suite(
        self,
        suite_name: str,
        test_functions: list[Callable],
        setup_func: Callable | None = None,
        teardown_func: Callable | None = None,
    ) -> E2ETestSuiteResult:
        """Run a complete test suite with setup/teardown."""

        print(f"🚀 Starting test suite: {suite_name}")
        start_time = datetime.now(timezone.utc)

        test_results = []
        passed = failed = skipped = errors = 0

        # Setup
        setup_error = None
        if setup_func:
            try:
                print(f"⚙️  Running setup for {suite_name}...")
                setup_func()
                print("✅ Setup completed")
            except Exception as e:
                setup_error = str(e)
                print(f"❌ Setup failed: {e}")

        # Run tests only if setup succeeded
        if not setup_error:
            for test_func in test_functions:
                result = self._run_single_test(test_func)
                test_results.append(result)

                if result.status == "PASS":
                    passed += 1
                elif result.status == "FAIL":
                    failed += 1
                elif result.status == "SKIP":
                    skipped += 1
                elif result.status == "ERROR":
                    errors += 1
        else:
            # Mark all tests as errors due to setup failure
            for test_func in test_functions:
                test_results.append(
                    E2ETestResult(
                        name=test_func.__name__,
                        status="ERROR",
                        duration_seconds=0.0,
                        error_message=f"Setup failed: {setup_error}",
                    )
                )
                errors += 1

        # Teardown
        if teardown_func and not setup_error:
            try:
                print(f"🧹 Running teardown for {suite_name}...")
                teardown_func()
                print("✅ Teardown completed")
            except Exception as e:
                print(f"⚠️  Teardown warning: {e}")

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        # Collect system information
        system_info = self._collect_system_info()

        suite_result = E2ETestSuiteResult(
            suite_name=suite_name,
            start_time=start_time,
            end_time=end_time,
            total_tests=len(test_functions),
            passed=passed,
            failed=failed,
            skipped=skipped,
            errors=errors,
            duration_seconds=duration,
            test_results=test_results,
            system_info=system_info,
        )

        self.results.append(suite_result)

        print(
            f"📊 Suite {suite_name} completed: {passed}P/{failed}F/{skipped}S/{errors}E in {duration:.1f}s"
        )
        return suite_result

    def _run_single_test(self, test_func: Callable) -> E2ETestResult:
        """Run a single test function and capture results."""

        test_name = test_func.__name__
        print(f"  🔬 Running {test_name}...")

        start_time = time.monotonic()

        try:
            # Attempt to run the test
            result = test_func()

            # Check if test was skipped
            if hasattr(result, "_skipped") or result == "SKIP":
                status = "SKIP"
                error_message = getattr(result, "_skip_reason", None)
            else:
                status = "PASS"
                error_message = None

        except pytest.skip.Exception as e:
            status = "SKIP"
            error_message = str(e)
        except AssertionError as e:
            status = "FAIL"
            error_message = str(e)
        except Exception as e:
            status = "ERROR"
            error_message = f"{type(e).__name__}: {str(e)}"

        end_time = time.monotonic()
        duration = end_time - start_time

        # Collect test metadata
        metadata = {
            "test_module": test_func.__module__ if hasattr(test_func, "__module__") else "unknown",
            "docstring": test_func.__doc__ or "",
        }

        result = E2ETestResult(
            name=test_name,
            status=status,
            duration_seconds=duration,
            error_message=error_message,
            metadata=metadata,
        )

        status_emoji = {"PASS": "✅", "FAIL": "❌", "SKIP": "⏭️", "ERROR": "💥"}

        print(f"    {status_emoji.get(status, '❓')} {test_name}: {status} ({duration:.2f}s)")

        return result

    def _collect_system_info(self) -> dict[str, Any]:
        """Collect system information for reporting."""

        import platform
        import sys

        try:
            import psutil

            memory_info = {
                "total_gb": psutil.virtual_memory().total / (1024**3),
                "available_gb": psutil.virtual_memory().available / (1024**3),
                "used_percent": psutil.virtual_memory().percent,
            }
        except ImportError:
            memory_info = {"error": "psutil not available"}

        return {
            "python_version": sys.version,
            "platform": platform.platform(),
            "processor": platform.processor(),
            "memory": memory_info,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def generate_reports(self) -> dict[str, Path]:
        """Generate comprehensive test reports."""

        report_files = {}

        # Generate JSON report
        json_report = self._generate_json_report()
        json_path = self.output_dir / "e2e_test_report.json"
        with open(json_path, "w") as f:
            json.dump(json_report, f, indent=2, default=str)
        report_files["json"] = json_path

        # Generate HTML report
        html_report = self._generate_html_report()
        html_path = self.output_dir / "e2e_test_report.html"
        with open(html_path, "w") as f:
            f.write(html_report)
        report_files["html"] = html_path

        # Generate summary report
        summary_report = self._generate_summary_report()
        summary_path = self.output_dir / "e2e_test_summary.txt"
        with open(summary_path, "w") as f:
            f.write(summary_report)
        report_files["summary"] = summary_path

        return report_files

    def _generate_json_report(self) -> dict[str, Any]:
        """Generate JSON format test report."""

        total_tests = sum(r.total_tests for r in self.results)
        total_passed = sum(r.passed for r in self.results)
        total_failed = sum(r.failed for r in self.results)
        total_skipped = sum(r.skipped for r in self.results)
        total_errors = sum(r.errors for r in self.results)
        total_duration = sum(r.duration_seconds for r in self.results)

        return {
            "summary": {
                "total_suites": len(self.results),
                "total_tests": total_tests,
                "passed": total_passed,
                "failed": total_failed,
                "skipped": total_skipped,
                "errors": total_errors,
                "success_rate": (total_passed / total_tests * 100) if total_tests > 0 else 0,
                "total_duration_seconds": total_duration,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
            "suites": [asdict(result) for result in self.results],
        }

    def _generate_html_report(self) -> str:
        """Generate HTML format test report."""

        total_tests = sum(r.total_tests for r in self.results)
        total_passed = sum(r.passed for r in self.results)
        total_failed = sum(r.failed for r in self.results)
        total_skipped = sum(r.skipped for r in self.results)
        total_errors = sum(r.errors for r in self.results)
        success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>MarketPipe E2E Test Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .summary {{ background: #f5f5f5; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
        .suite {{ margin-bottom: 20px; border: 1px solid #ddd; border-radius: 5px; }}
        .suite-header {{ background: #e9e9e9; padding: 10px; font-weight: bold; }}
        .test-result {{ padding: 5px 10px; border-bottom: 1px solid #eee; }}
        .pass {{ color: green; }}
        .fail {{ color: red; }}
        .skip {{ color: orange; }}
        .error {{ color: purple; }}
        .metrics {{ display: flex; gap: 20px; }}
        .metric {{ text-align: center; }}
        .metric-value {{ font-size: 24px; font-weight: bold; }}
        .progress-bar {{ width: 100%; height: 20px; background: #f0f0f0; border-radius: 10px; overflow: hidden; }}
        .progress-fill {{ height: 100%; background: linear-gradient(90deg, green {success_rate}%, red {success_rate}%); }}
    </style>
</head>
<body>
    <h1>🧪 MarketPipe End-to-End Test Report</h1>

    <div class="summary">
        <h2>📊 Test Summary</h2>
        <div class="metrics">
            <div class="metric">
                <div class="metric-value">{total_tests}</div>
                <div>Total Tests</div>
            </div>
            <div class="metric">
                <div class="metric-value pass">{total_passed}</div>
                <div>Passed</div>
            </div>
            <div class="metric">
                <div class="metric-value fail">{total_failed}</div>
                <div>Failed</div>
            </div>
            <div class="metric">
                <div class="metric-value skip">{total_skipped}</div>
                <div>Skipped</div>
            </div>
            <div class="metric">
                <div class="metric-value error">{total_errors}</div>
                <div>Errors</div>
            </div>
            <div class="metric">
                <div class="metric-value">{success_rate:.1f}%</div>
                <div>Success Rate</div>
            </div>
        </div>
        <div style="margin-top: 20px;">
            <div>Overall Progress:</div>
            <div class="progress-bar">
                <div class="progress-fill"></div>
            </div>
        </div>
    </div>

    <h2>📋 Test Suites</h2>
"""

        for suite in self.results:
            html += f"""
    <div class="suite">
        <div class="suite-header">
            🧪 {suite.suite_name}
            ({suite.passed}P/{suite.failed}F/{suite.skipped}S/{suite.errors}E - {suite.duration_seconds:.1f}s)
        </div>
"""

            for test in suite.test_results:
                status_class = test.status.lower()
                status_emoji = {"pass": "✅", "fail": "❌", "skip": "⏭️", "error": "💥"}.get(
                    status_class, "❓"
                )

                html += f"""
        <div class="test-result {status_class}">
            {status_emoji} {test.name} - {test.status} ({test.duration_seconds:.2f}s)
"""
                if test.error_message:
                    html += f"""
            <div style="margin-left: 20px; font-size: 12px; color: #666;">
                {test.error_message}
            </div>
"""
                html += "        </div>\n"

            html += "    </div>\n"

        html += f"""
    <div style="margin-top: 40px; text-align: center; color: #666;">
        Report generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
    </div>
</body>
</html>
"""

        return html

    def _generate_summary_report(self) -> str:
        """Generate text summary report."""

        total_tests = sum(r.total_tests for r in self.results)
        total_passed = sum(r.passed for r in self.results)
        total_failed = sum(r.failed for r in self.results)
        total_skipped = sum(r.skipped for r in self.results)
        total_errors = sum(r.errors for r in self.results)
        total_duration = sum(r.duration_seconds for r in self.results)
        success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0

        report = f"""
================================================================================
🧪 MARKETPIPE END-TO-END TEST REPORT
================================================================================

📊 SUMMARY
----------
Test Suites:     {len(self.results)}
Total Tests:     {total_tests}
Passed:          {total_passed} ({total_passed/total_tests*100:.1f}%)
Failed:          {total_failed} ({total_failed/total_tests*100:.1f}%)
Skipped:         {total_skipped} ({total_skipped/total_tests*100:.1f}%)
Errors:          {total_errors} ({total_errors/total_tests*100:.1f}%)
Success Rate:    {success_rate:.1f}%
Total Duration:  {total_duration:.1f} seconds

📋 SUITE BREAKDOWN
------------------
"""

        for suite in self.results:
            report += f"""
{suite.suite_name}:
  Tests: {suite.total_tests} | Passed: {suite.passed} | Failed: {suite.failed} | Skipped: {suite.skipped} | Errors: {suite.errors}
  Duration: {suite.duration_seconds:.1f}s | Success Rate: {suite.success_rate:.1f}%
"""

        # Add failed tests details
        failed_tests = []
        for suite in self.results:
            for test in suite.test_results:
                if test.status in ["FAIL", "ERROR"]:
                    failed_tests.append((suite.suite_name, test))

        if failed_tests:
            report += f"""
❌ FAILED/ERROR TESTS ({len(failed_tests)})
------------------------
"""
            for suite_name, test in failed_tests:
                report += f"""
{suite_name}::{test.name}
  Status: {test.status}
  Duration: {test.duration_seconds:.2f}s
  Error: {test.error_message or 'No error message'}
"""

        report += f"""
================================================================================
Report generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
================================================================================
"""

        return report


# Example test suite orchestration functions
def create_comprehensive_e2e_suite(tmp_path) -> list[Callable]:
    """Create comprehensive E2E test suite."""

    # Import test functions from other modules
    test_functions = []

    # Mock test functions for demonstration
    def test_real_aggregation_pipeline():
        """Test real aggregation without mocking."""
        # This would call actual test from test_real_aggregation_e2e.py
        time.sleep(0.1)  # Simulate test execution
        return "PASS"

    def test_error_propagation():
        """Test error propagation across layers."""
        time.sleep(0.05)
        return "PASS"

    def test_performance_benchmarks():
        """Test performance under load."""
        time.sleep(0.2)
        return "PASS"

    def test_boundary_conditions():
        """Test system boundary conditions."""
        time.sleep(0.1)
        if datetime.now().second % 2 == 0:  # Simulate occasional failure
            raise AssertionError("Boundary condition violated")
        return "PASS"

    def test_multi_provider_integration():
        """Test multi-provider scenarios."""
        time.sleep(0.15)
        return "PASS"

    def test_data_quality_validation():
        """Test comprehensive data quality checks."""
        time.sleep(0.08)
        return "PASS"

    def test_postgres_integration():
        """Test PostgreSQL integration."""
        # Simulate skipped test if PostgreSQL not available
        pytest.skip("PostgreSQL not available in test environment")

    test_functions.extend(
        [
            test_real_aggregation_pipeline,
            test_error_propagation,
            test_performance_benchmarks,
            test_boundary_conditions,
            test_multi_provider_integration,
            test_data_quality_validation,
            test_postgres_integration,
        ]
    )

    return test_functions


@pytest.mark.integration
@pytest.mark.orchestration
class TestE2EOrchestration:
    """Test the E2E test orchestration framework."""

    def test_orchestrator_basic_functionality(self, tmp_path):
        """Test basic orchestrator functionality."""

        orchestrator = E2ETestOrchestrator(tmp_path / "reports")

        # Create simple test suite
        def test_simple_pass():
            return "PASS"

        def test_simple_fail():
            raise AssertionError("Test designed to fail")

        def test_simple_skip():
            pytest.skip("Test designed to skip")

        test_functions = [test_simple_pass, test_simple_fail, test_simple_skip]

        # Run test suite
        result = orchestrator.run_test_suite("basic_test", test_functions)

        # Verify results
        assert result.total_tests == 3
        assert result.passed == 1
        assert result.failed == 1
        assert result.skipped == 1
        assert result.errors == 0

        print(
            f"✓ Basic orchestration test: {result.passed}P/{result.failed}F/{result.skipped}S/{result.errors}E"
        )

        # Generate reports
        report_files = orchestrator.generate_reports()

        assert "json" in report_files
        assert "html" in report_files
        assert "summary" in report_files

        for report_type, file_path in report_files.items():
            assert file_path.exists(), f"{report_type} report not generated"
            assert file_path.stat().st_size > 0, f"{report_type} report is empty"
            print(f"✓ Generated {report_type} report: {file_path}")

        print("✅ Orchestrator basic functionality test completed")

    def test_comprehensive_e2e_suite_execution(self, tmp_path):
        """Test execution of comprehensive E2E test suite."""

        orchestrator = E2ETestOrchestrator(tmp_path / "reports")

        # Create comprehensive test suite
        test_functions = create_comprehensive_e2e_suite(tmp_path)

        # Define setup and teardown
        def setup_comprehensive_tests():
            print("Setting up comprehensive E2E environment...")
            # Setup code would go here

        def teardown_comprehensive_tests():
            print("Cleaning up comprehensive E2E environment...")
            # Cleanup code would go here

        # Run comprehensive suite
        result = orchestrator.run_test_suite(
            "comprehensive_e2e",
            test_functions,
            setup_func=setup_comprehensive_tests,
            teardown_func=teardown_comprehensive_tests,
        )

        print("📊 Comprehensive E2E Results:")
        print(f"  Total Tests: {result.total_tests}")
        print(f"  Success Rate: {result.success_rate:.1f}%")
        print(f"  Duration: {result.duration_seconds:.1f}s")

        # Verify reasonable test execution
        assert result.total_tests > 5, "Should have multiple tests"
        assert result.passed > 0, "Should have some passing tests"
        assert result.duration_seconds > 0, "Should take some time to execute"

        # Generate comprehensive reports
        report_files = orchestrator.generate_reports()

        # Verify report content
        json_report_path = report_files["json"]
        with open(json_report_path) as f:
            json_data = json.load(f)

        assert "summary" in json_data
        assert "suites" in json_data
        assert json_data["summary"]["total_tests"] == result.total_tests

        print("✅ Comprehensive E2E suite execution test completed")

    def test_performance_reporting_integration(self, tmp_path):
        """Test integration with performance reporting."""

        orchestrator = E2ETestOrchestrator(tmp_path / "reports")

        # Create performance-focused test suite
        def test_fast_operation():
            time.sleep(0.01)  # 10ms
            return "PASS"

        def test_medium_operation():
            time.sleep(0.1)  # 100ms
            return "PASS"

        def test_slow_operation():
            time.sleep(0.5)  # 500ms
            return "PASS"

        performance_tests = [test_fast_operation, test_medium_operation, test_slow_operation]

        # Run performance suite
        result = orchestrator.run_test_suite("performance_tests", performance_tests)

        # Verify timing measurements
        assert result.duration_seconds >= 0.6, "Should capture execution time"

        # Check individual test timings
        fast_test = next(t for t in result.test_results if t.name == "test_fast_operation")
        slow_test = next(t for t in result.test_results if t.name == "test_slow_operation")

        assert (
            fast_test.duration_seconds < slow_test.duration_seconds
        ), "Should differentiate test speeds"

        print("⏱️  Performance Timing Results:")
        for test in result.test_results:
            print(f"  {test.name}: {test.duration_seconds:.3f}s")

        print("✅ Performance reporting integration test completed")


@pytest.mark.integration
@pytest.mark.orchestration
def test_full_e2e_orchestration_demo(tmp_path):
    """Demonstrate full E2E orchestration capabilities."""

    print("🎭 FULL E2E ORCHESTRATION DEMONSTRATION")
    print("=" * 60)

    orchestrator = E2ETestOrchestrator(tmp_path / "demo_reports")

    # Create multiple test suites representing different aspects
    test_suites = {
        "core_pipeline": [
            lambda: time.sleep(0.1) or "PASS",  # Simulate aggregation test
            lambda: time.sleep(0.05) or "PASS",  # Simulate storage test
            lambda: time.sleep(0.08) or "PASS",  # Simulate validation test
        ],
        "integration_tests": [
            lambda: time.sleep(0.2) or "PASS",  # Simulate multi-provider test
            lambda: pytest.skip("External service unavailable"),  # Simulate skip
            lambda: time.sleep(0.15) or "PASS",  # Simulate boundary test
        ],
        "performance_tests": [
            lambda: time.sleep(0.3) or "PASS",  # Simulate load test
            lambda: time.sleep(0.1)
            or (_ for _ in ()).throw(
                AssertionError("Performance threshold exceeded")
            ),  # Simulate failure
            lambda: time.sleep(0.05) or "PASS",  # Simulate memory test
        ],
    }

    # Execute all test suites
    all_results = []

    for suite_name, test_functions in test_suites.items():
        print(f"\n🔄 Executing {suite_name}...")

        result = orchestrator.run_test_suite(suite_name, test_functions)
        all_results.append(result)

        print(f"  Results: {result.passed}P/{result.failed}F/{result.skipped}S/{result.errors}E")

    # Generate comprehensive reports
    print("\n📊 Generating comprehensive reports...")
    report_files = orchestrator.generate_reports()

    for report_type, file_path in report_files.items():
        print(f"  {report_type.upper()}: {file_path}")

    # Display summary
    total_tests = sum(r.total_tests for r in all_results)
    total_passed = sum(r.passed for r in all_results)
    overall_success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0

    print("\n🎯 DEMONSTRATION SUMMARY:")
    print(f"  Test Suites: {len(all_results)}")
    print(f"  Total Tests: {total_tests}")
    print(f"  Overall Success Rate: {overall_success_rate:.1f}%")
    print(f"  Reports Generated: {len(report_files)}")

    # Verify demonstration worked
    assert len(all_results) == 3, "Should have executed 3 test suites"
    assert total_tests > 0, "Should have executed some tests"
    assert len(report_files) == 3, "Should have generated 3 report formats"

    # Show sample report content
    summary_path = report_files["summary"]
    print("\n📄 Sample Report Content (first 10 lines):")
    with open(summary_path) as f:
        lines = f.readlines()[:10]
        for line in lines:
            print(f"  {line.rstrip()}")

    print("\n✅ Full E2E orchestration demonstration completed successfully!")
    print("=" * 60)
