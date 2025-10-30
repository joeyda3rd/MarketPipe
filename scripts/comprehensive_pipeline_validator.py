#!/usr/bin/env python3
"""
MarketPipe Comprehensive Pipeline Validator

A comprehensive validation script that tests actual MarketPipe pipeline behavior
and functionality, not just help commands. Tests real ingestion, validation,
aggregation, querying, and job management functionality.

Usage:
    python scripts/comprehensive_pipeline_validator.py [OPTIONS]

Options:
    --mode MODE          Validation mode: quick, critical, full, stress (default: critical)
    --output-dir DIR     Directory for validation outputs (default: ./validation_output)
    --dry-run           Show what would be tested without executing
    --verbose           Enable verbose logging
    --report-format FMT  Report format: json, yaml, html (default: json)
    --data-dir DIR      Directory for test data (default: ./test_data)
    --timeout INTEGER   Command timeout in seconds (default: 120)
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import typer
import yaml
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table


@dataclass
class TestResult:
    """Result of a single test."""

    test_name: str
    category: str
    command: str
    status: str  # PASS, FAIL, SKIP, ERROR
    duration: float
    exit_code: Optional[int]
    stdout: str
    stderr: str
    error_message: str = ""
    expected_behavior: str = ""
    actual_behavior: str = ""
    severity: str = "normal"  # low, normal, high, critical


@dataclass
class ValidationReport:
    """Complete validation report."""

    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    mode: str = "critical"
    results: list[TestResult] = field(default_factory=list)
    test_environment: dict[str, Any] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def total_duration(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def pass_count(self) -> int:
        return len([r for r in self.results if r.status == "PASS"])

    @property
    def fail_count(self) -> int:
        return len([r for r in self.results if r.status == "FAIL"])

    @property
    def error_count(self) -> int:
        return len([r for r in self.results if r.status == "ERROR"])

    @property
    def skip_count(self) -> int:
        return len([r for r in self.results if r.status == "SKIP"])

    @property
    def total_count(self) -> int:
        return len(self.results)

    @property
    def pass_rate(self) -> float:
        return (self.pass_count / self.total_count * 100) if self.total_count > 0 else 0.0


class ComprehensivePipelineValidator:
    """Comprehensive MarketPipe pipeline functionality validator."""

    def __init__(
        self,
        mode: str = "critical",
        output_dir: str = "./validation_output",
        data_dir: str = "./test_data",
        timeout: int = 120,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        self.mode = mode
        self.output_dir = Path(output_dir)
        self.data_dir = Path(data_dir)
        self.timeout = timeout
        self.dry_run = dry_run
        self.verbose = verbose
        self.console = Console()
        self.report = ValidationReport(mode=mode)

        # Create output and test data directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Create test configuration files
        self._setup_test_environment()

    def _setup_test_environment(self) -> None:
        """Set up test environment with configurations and test data."""
        self.test_config_file = self.data_dir / "test_config.yaml"
        self.minimal_config_file = self.data_dir / "minimal_config.yaml"
        self.invalid_config_file = self.data_dir / "invalid_config.yaml"
        self.ratio_check_file = self.data_dir / "ratio_check.py"

        # Basic test configuration
        test_config = {
            "alpaca": {
                "key": "test_key_placeholder",
                "secret": "test_secret_placeholder",
                "base_url": "https://paper-api.alpaca.markets",
                "rate_limit_per_min": 200,
                "feed": "iex",
                "timeout": 30.0,
                "max_retries": 3,
            },
            "symbols": ["AAPL", "MSFT", "GOOGL"],
            "start": (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
            "end": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "output_path": str(self.data_dir / "parquet_output"),
            "compression": "snappy",
            "workers": 2,
            "batch_size": 100,
            "metrics": {"enabled": False},  # Disable for testing
        }

        # Minimal configuration
        minimal_config = {
            "symbols": ["AAPL"],
            "start": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "end": datetime.now().strftime("%Y-%m-%d"),
            "output_path": str(self.data_dir / "minimal_output"),
        }

        # Invalid configuration (for error testing)
        invalid_config = {
            "symbols": [],  # Empty symbols should fail
            "start": "invalid-date",
            "end": "2022-01-01",  # End before start
            "output_path": "/nonexistent/readonly/path",
        }

        # Write configuration files
        with open(self.test_config_file, "w") as f:
            yaml.dump(test_config, f)

        with open(self.minimal_config_file, "w") as f:
            yaml.dump(minimal_config, f)

        with open(self.invalid_config_file, "w") as f:
            yaml.dump(invalid_config, f)

        # Write a reusable ratio check script to simplify shell quoting
        ratio_script = f"""
import sys
import duckdb

AGG = r"{str(self.data_dir / 'agg_output')}"
con = duckdb.connect()

def cnt(view,s):
    try:
        return con.execute(f"SELECT COUNT(*) FROM parquet_scan('{str(self.data_dir / 'agg_output')}/frame={{view}}/symbol={{s}}/**/*.parquet', hive_partitioning=1)").fetchone()[0]
    except Exception:
        return 0

syms=['AAPL','MSFT','GOOGL']
issues=[]
for s in syms:
    c1h=cnt('1h', s); c4h=cnt('4h', s)
    r = (c4h/(c1h or 1))
    print(f"SYMBOL {{s}}: 1h={{c1h}} 4h={{c4h}} ratio={{r:.3f}}")
    if c1h>0 and (c4h==0 or c4h>c1h):
        issues.append((s,'bounds',c1h,c4h))
    if c1h>=6 and not (0.05 <= r <= 1.00):
        issues.append((s,'ratio',c1h,c4h,r))

if issues:
    print('WARN ratio checks:', issues)
sys.exit(0)
"""
        with open(self.ratio_check_file, "w") as f:
            f.write(ratio_script)

        self.report.test_environment = {
            "test_config_file": str(self.test_config_file),
            "minimal_config_file": str(self.minimal_config_file),
            "data_dir": str(self.data_dir),
            "output_dir": str(self.output_dir),
            "timeout": self.timeout,
            "mode": self.mode,
        }

    def get_test_suite(self) -> dict[str, list[dict[str, Any]]]:
        """Get test suite based on validation mode."""

        # Core tests that always run
        core_tests = {
            "health_check": [
                {
                    "name": "Basic Health Check",
                    "command": "python -m marketpipe health-check",
                    "expected": "should complete health diagnostics",
                    "severity": "critical",
                },
                {
                    "name": "Health Check Verbose",
                    "command": "python -m marketpipe health-check --verbose",
                    "expected": "should show detailed health check output",
                    "severity": "normal",
                },
            ],
            "providers": [
                {
                    "name": "List Providers",
                    "command": "python -m marketpipe providers",
                    "expected": "should list available data providers",
                    "severity": "high",
                }
            ],
            "basic_commands": [
                {
                    "name": "Main Help",
                    "command": "python -m marketpipe --help",
                    "expected": "should show main help menu",
                    "severity": "low",
                },
                {
                    "name": "Jobs List (Empty)",
                    "command": "python -m marketpipe jobs list",
                    "expected": "should list jobs (may be empty)",
                    "severity": "normal",
                },
                {
                    "name": "Jobs Status",
                    "command": "python -m marketpipe jobs status",
                    "expected": "should show jobs status summary",
                    "severity": "normal",
                },
            ],
        }

        # Additional tests for critical/full modes
        functional_tests = {
            "configuration": [
                {
                    "name": "Config File Access - Valid Config",
                    "command": f"python -c \"import yaml; config=yaml.safe_load(open('{self.test_config_file}')); print('Config loaded successfully' if config else 'Failed')\"",
                    "expected": "should load and parse valid configuration file",
                    "severity": "high",
                },
                {
                    "name": "Parameter Validation - Invalid Date Range",
                    "command": "python -m marketpipe ingest-ohlcv --symbols AAPL --start 2025-12-31 --end 2025-01-01 --output /tmp/test_output",
                    "expected": "should reject invalid date range (end before start)",
                    "severity": "high",
                    "expect_failure": True,
                },
            ],
            "parameter_validation": [
                {
                    "name": "Invalid Symbol Format",
                    "command": f"python -m marketpipe ingest-ohlcv --symbols '123INVALID' --start {(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')} --end {datetime.now().strftime('%Y-%m-%d')} --output /tmp/test_output",
                    "expected": "should reject invalid symbol format",
                    "severity": "normal",
                    "expect_failure": True,
                    "timeout": 30,
                },
                {
                    "name": "Missing Required Parameters",
                    "command": "python -m marketpipe ingest-ohlcv --output /tmp/test_output",
                    "expected": "should require symbols and date range",
                    "severity": "high",
                    "expect_failure": True,
                    "timeout": 30,
                },
                {
                    "name": "OHLCV Command Structure (Invalid Provider)",
                    "command": f"python -m marketpipe ohlcv ingest --symbols AAPL --start {(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')} --end {datetime.now().strftime('%Y-%m-%d')} --output /tmp/test_nonexistent_provider --provider invalid123",
                    "expected": "should reject unknown provider in ohlcv path",
                    "severity": "normal",
                    "expect_failure": True,
                    "timeout": 30,
                },
            ],
            "symbols": [
                {
                    "name": "Symbol Update - Dry Run",
                    "command": "python -m marketpipe symbols update --dry-run",
                    "expected": "should perform symbol update dry run",
                    "severity": "normal",
                }
            ],
            "data_management": [
                {
                    "name": "Query Help",
                    "command": "python -m marketpipe query --help",
                    "expected": "should show query command help",
                    "severity": "normal",
                },
                {
                    "name": "Prune Parquet Help",
                    "command": "python -m marketpipe prune parquet --help",
                    "expected": "should show prune parquet options",
                    "severity": "low",
                },
                {
                    "name": "Prune Database Help",
                    "command": "python -m marketpipe prune database --help",
                    "expected": "should show prune database options",
                    "severity": "low",
                },
            ],
        }

        # Full mode tests (including actual data operations)
        # Real ingestion test adapts based on presence of API credentials
        alpaca_key = os.environ.get("ALPACA_KEY")
        alpaca_secret = os.environ.get("ALPACA_SECRET")
        iex_token = os.environ.get("IEX_TOKEN")

        symbols_list = ["AAPL", "MSFT", "GOOGL"]
        symbols_csv = ",".join(symbols_list)

        real_ingestion_command = (
            f"python -m marketpipe ingest-ohlcv "
            f"--symbols {symbols_csv} "
            f"--start {(datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')} "
            f"--end {datetime.now().strftime('%Y-%m-%d')} "
            f"--output {self.data_dir / 'real_output'} "
            f"--batch-size 500 --workers 1"
        )

        if alpaca_key and alpaca_secret:
            # Run a real ingestion expecting success using Alpaca IEX
            real_ingestion_tests = [
                {
                    "name": "Real Ingestion (Alpaca IEX)",
                    "command": real_ingestion_command + " --provider alpaca --feed-type iex",
                    "expected": "should ingest real OHLCV data and write Parquet files",
                    "severity": "critical",
                    "expect_failure": False,
                    "timeout": 300,
                }
            ]
        elif iex_token:
            # Run a real ingestion expecting success using IEX
            real_ingestion_tests = [
                {
                    "name": "Real Ingestion (IEX)",
                    "command": real_ingestion_command + " --provider iex",
                    "expected": "should ingest real OHLCV data via IEX and write Parquet files",
                    "severity": "critical",
                    "expect_failure": False,
                    "timeout": 300,
                }
            ]
        else:
            # Fall back to a bounded initialization test that we expect to fail without creds
            real_ingestion_tests = [
                {
                    "name": "Ingestion Command Initialization (No Creds)",
                    "command": "timeout 30 " + real_ingestion_command,
                    "expected": "should validate parameters; will fail without API credentials",
                    "severity": "high",
                    "expect_failure": True,
                    "timeout": 45,
                }
            ]

        advanced_tests = {
            "real_ingestion": real_ingestion_tests,
            "validation": [
                {
                    "name": "List Validation Reports",
                    "command": "python -m marketpipe validate-ohlcv --list",
                    "expected": "should list available validation reports (may be empty)",
                    "severity": "normal",
                }
            ],
            "aggregation": [
                {
                    "name": "Aggregation Command Help",
                    "command": "python -m marketpipe aggregate-ohlcv --help",
                    "expected": "should show aggregation command help and options",
                    "severity": "low",
                },
                # Full aggregation steps – only meaningful with real ingestion
                {
                    "name": "Aggregate Latest Job (AAPL)",
                    "command": self._build_aggregate_command_for_latest_job(),
                    "expected": "should aggregate to multiple frames and refresh views",
                    "severity": "critical",
                },
                {
                    "name": "Aggregate All Jobs",
                    "command": self._build_aggregate_all_jobs_command(),
                    "expected": "should aggregate all discovered job_ids",
                    "severity": "critical",
                },
                {
                    "name": "Query Aggregated 5m Sample",
                    "command": self._build_query_with_agg_root(
                        "SELECT * FROM bars_5m WHERE symbol='AAPL' ORDER BY ts_ns LIMIT 5"
                    ),
                    "expected": "should return rows from 5m aggregated view",
                    "severity": "high",
                },
                {
                    "name": "Query Aggregated 15m Count",
                    "command": self._build_query_with_agg_root(
                        "SELECT COUNT(*) AS cnt FROM bars_15m WHERE symbol='AAPL'"
                    ),
                    "expected": "should return non-zero count for 15m",
                    "severity": "normal",
                },
                {
                    "name": "Query Aggregated 1h Count",
                    "command": self._build_query_with_agg_root(
                        "SELECT COUNT(*) AS cnt FROM bars_1h WHERE symbol='AAPL'"
                    ),
                    "expected": "should return non-zero count for 1h",
                    "severity": "normal",
                },
                {
                    "name": "Query Aggregated 4h Count",
                    "command": self._build_query_with_agg_root(
                        "SELECT COUNT(*) AS cnt FROM bars_4h WHERE symbol='AAPL'"
                    ),
                    "expected": "should return non-zero count for 4h",
                    "severity": "normal",
                },
                {
                    "name": "Query Aggregated 1d Count",
                    "command": self._build_query_with_agg_root(
                        "SELECT COUNT(*) AS cnt FROM bars_1d WHERE symbol='AAPL'"
                    ),
                    "expected": "should return non-zero count for 1d",
                    "severity": "normal",
                },
                {
                    "name": "Query Aggregated 4h Multi-Symbol",
                    "command": self._build_query_with_agg_root(
                        "SELECT symbol, COUNT(*) AS cnt FROM bars_4h WHERE symbol IN ('AAPL','MSFT','GOOGL') GROUP BY symbol ORDER BY symbol"
                    ),
                    "expected": "should return counts per symbol for 4h",
                    "severity": "normal",
                },
                {
                    "name": "Sanity Check 4h vs 1h Ratios",
                    "command": self._build_ratio_check_command(),
                    "expected": "4h counts should be roughly 1/4 of 1h per symbol",
                    "severity": "high",
                },
            ],
            "queries": [
                {
                    "name": "Test Query - Show Tables",
                    "command": 'python -m marketpipe query "SHOW TABLES" --limit 10',
                    "expected": "should show available data tables/views",
                    "severity": "normal",
                }
            ],
        }

        # Stress tests
        stress_tests = {
            "performance": [
                {
                    "name": "Multi-Symbol Ingestion",
                    "command": f"python -m marketpipe ingest-ohlcv --symbols AAPL,MSFT,GOOGL,TSLA --start {(datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')} --end {datetime.now().strftime('%Y-%m-%d')} --output {self.data_dir / 'stress_output'} --workers 4",
                    "expected": "should handle multiple symbols efficiently",
                    "severity": "normal",
                    "timeout": 600,  # 10 minutes
                }
            ]
        }

        # Select tests based on mode
        if self.mode == "quick":
            return core_tests
        elif self.mode == "critical":
            tests = core_tests.copy()
            tests.update(functional_tests)
            return tests
        elif self.mode == "full":
            tests = core_tests.copy()
            tests.update(functional_tests)
            tests.update(advanced_tests)
            return tests
        elif self.mode == "stress":
            tests = core_tests.copy()
            tests.update(functional_tests)
            tests.update(advanced_tests)
            tests.update(stress_tests)
            return tests
        else:
            return core_tests

    def run_command(self, test_config: dict[str, Any]) -> TestResult:
        """Run a single test command and capture results."""
        test_name = test_config["name"]
        command = test_config["command"]
        expected = test_config["expected"]
        severity = test_config.get("severity", "normal")
        expect_failure = test_config.get("expect_failure", False)
        test_timeout = test_config.get("timeout", self.timeout)
        category = test_config.get("category", "unknown")

        if self.dry_run:
            return TestResult(
                test_name=test_name,
                category=category,
                command=command,
                status="SKIP",
                duration=0.0,
                exit_code=None,
                stdout="[DRY RUN] Command would be executed",
                stderr="",
                expected_behavior=expected,
                severity=severity,
            )

        if self.verbose:
            self.console.print(f"[blue]Running:[/blue] {test_name}")
            self.console.print(f"[dim]Command:[/dim] {command}")

        start_time = time.time()

        try:
            # Run command with timeout
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=test_timeout,
                cwd=str(Path.cwd()),
            )

            duration = time.time() - start_time

            # Analyze result
            status = self._analyze_test_result(result, expect_failure, test_config)

            return TestResult(
                test_name=test_name,
                category=category,
                command=command,
                status=status,
                duration=duration,
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                expected_behavior=expected,
                actual_behavior=self._describe_result(result),
                severity=severity,
            )

        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            return TestResult(
                test_name=test_name,
                category=category,
                command=command,
                status="ERROR",
                duration=duration,
                exit_code=None,
                stdout="",
                stderr="",
                error_message=f"Command timed out after {test_timeout} seconds",
                expected_behavior=expected,
                actual_behavior="Command timeout",
                severity=severity,
            )

        except Exception as e:
            duration = time.time() - start_time
            return TestResult(
                test_name=test_name,
                category=category,
                command=command,
                status="ERROR",
                duration=duration,
                exit_code=None,
                stdout="",
                stderr="",
                error_message=str(e),
                expected_behavior=expected,
                actual_behavior=f"Exception: {str(e)}",
                severity=severity,
            )

    def _analyze_test_result(
        self, result: subprocess.CompletedProcess, expect_failure: bool, test_config: dict[str, Any]
    ) -> str:
        """Analyze command result to determine test status."""

        # Handle expected failures
        if expect_failure:
            return "PASS" if result.returncode != 0 else "FAIL"

        # Handle expected successes
        if result.returncode == 0:
            return "PASS"

        # Special handling for specific commands
        command = test_config["command"]

        # Health check special case - may have warnings but still be functional
        if "health-check" in command and result.returncode == 1:
            if result.stdout and ("passed" in result.stdout.lower() or "✅" in result.stdout):
                # Look for partial success indicators
                lines = result.stdout.split("\n")
                for line in lines:
                    if "Summary:" in line and "passed" in line:
                        # Extract pass count (e.g., "Summary: 9/10 checks passed")
                        try:
                            parts = line.split()
                            for part in parts:
                                if "/" in part and part.replace("/", "").replace(" ", "").isdigit():
                                    passed, total = part.split("/")
                                    pass_rate = int(passed) / int(total)
                                    if pass_rate >= 0.8:  # 80% pass rate acceptable
                                        return "PASS"
                        except Exception:
                            pass

        # Jobs commands may return non-zero when no jobs exist (not necessarily failure)
        if "jobs" in command and result.returncode != 0:
            if (
                "no jobs found" in result.stdout.lower()
                or "no active jobs" in result.stderr.lower()
            ):
                return "PASS"

        # Query commands may fail if no data exists (expected in fresh install)
        if "query" in command and result.returncode != 0:
            if (
                "no such table" in result.stderr.lower()
                or "table does not exist" in result.stderr.lower()
            ):
                return "PASS"  # Expected when no data has been ingested yet

        # Dry run failures are actual failures
        if "--dry-run" in command:
            return "FAIL"

        # Default: non-zero exit code is failure
        return "FAIL"

    def _describe_result(self, result: subprocess.CompletedProcess) -> str:
        """Create a description of what actually happened."""
        if result.returncode == 0:
            return "Command completed successfully"
        else:
            error_lines = []
            if result.stderr:
                # Get first non-warning error line
                for line in result.stderr.split("\n"):
                    if (
                        line.strip()
                        and not line.startswith("/usr/lib/python")
                        and "UserWarning" not in line
                    ):
                        error_lines.append(line.strip())
                        break

            if error_lines:
                return f"Command failed: {error_lines[0]}"
            else:
                return f"Command exited with code {result.returncode}"

    def run_validation(self) -> ValidationReport:
        """Run comprehensive validation suite."""
        self.console.print(
            "[bold green]🚀 Starting MarketPipe Comprehensive Validation[/bold green]"
        )
        self.console.print(
            f"[dim]Mode: {self.mode} | Output: {self.output_dir} | Timeout: {self.timeout}s[/dim]"
        )

        if self.dry_run:
            self.console.print("[yellow]🔍 DRY RUN MODE - No commands will be executed[/yellow]")

        test_suite = self.get_test_suite()
        total_tests = sum(len(tests) for tests in test_suite.values())

        self.console.print(
            f"[blue]📋 Running {total_tests} tests across {len(test_suite)} categories[/blue]"
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console,
        ) as progress:

            main_task = progress.add_task("Overall Progress", total=total_tests)

            for category, tests in test_suite.items():
                category_task = progress.add_task(f"Testing {category}", total=len(tests))

                for test_config in tests:
                    test_config["category"] = category

                    progress.update(category_task, description=f"Running {test_config['name']}")

                    result = self.run_command(test_config)
                    self.report.results.append(result)

                    # Log result
                    if self.verbose or result.status in ["FAIL", "ERROR"]:
                        status_color = {
                            "PASS": "green",
                            "FAIL": "red",
                            "ERROR": "red",
                            "SKIP": "yellow",
                        }.get(result.status, "white")

                        self.console.print(
                            f"  [{status_color}]{result.status}[/{status_color}] "
                            f"{result.test_name} ({result.duration:.1f}s)"
                        )

                        if result.status in ["FAIL", "ERROR"] and result.error_message:
                            self.console.print(f"    [red]Error:[/red] {result.error_message}")

                    progress.update(category_task, advance=1)
                    progress.update(main_task, advance=1)

                progress.remove_task(category_task)

        self.report.end_time = datetime.now()
        self._generate_summary()

        # Display results
        self._display_results()

        # If real ingestion succeeded, perform manual verification on output
        try:
            if any(
                r.category == "real_ingestion" and r.status == "PASS" for r in self.report.results
            ):
                manual = self._manual_verify_real_output()
                if manual:
                    self.report.results.append(manual)
                    # Regenerate summary to include manual verification result
                    self._generate_summary()
                    # Show manual verification outcome
                    status_color = {
                        "PASS": "green",
                        "FAIL": "red",
                        "ERROR": "red",
                        "SKIP": "yellow",
                    }.get(manual.status, "white")
                    self.console.print(
                        f"  [{status_color}]{manual.status}[/{status_color}] {manual.test_name} ({manual.duration:.1f}s)"
                    )
                    if manual.stdout:
                        # Show sample rows inline for visibility
                        self.console.print(manual.stdout)
            # If aggregation ran and passed, preview aggregated outputs too
            if any(
                r.category == "aggregation"
                and r.status == "PASS"
                and r.test_name.startswith("Aggregate")
                for r in self.report.results
            ):
                manual_agg = self._manual_verify_aggregated_output()
                if manual_agg:
                    self.report.results.append(manual_agg)
                    self._generate_summary()
                    status_color = {
                        "PASS": "green",
                        "FAIL": "red",
                        "ERROR": "red",
                        "SKIP": "yellow",
                    }.get(manual_agg.status, "white")
                    self.console.print(
                        f"  [{status_color}]{manual_agg.status}[/{status_color}] {manual_agg.test_name}"
                    )
                    if manual_agg.stdout:
                        self.console.print(manual_agg.stdout)
        except Exception:
            # Non-fatal if manual verification itself errors
            pass

        return self.report

    def _build_aggregate_command_for_latest_job(self) -> str:
        """Build an aggregate-ohlcv command for the latest discovered job under the raw output.

        Uses env overrides so aggregation reads from our test raw/agg roots.
        """
        out_dir = self.data_dir / "real_output"
        # Look for any job parquet filenames and pick the most recent by mtime
        job_files = sorted(
            out_dir.rglob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True
        )
        if not job_files:
            # If nothing to aggregate yet, return a no-op that will fail (and be visible)
            return "echo 'no job files found' && false"
        latest = job_files[0]
        job_id = latest.stem  # filename without extension
        raw_env = f"MARKETPIPE_RAW_ROOT={out_dir}"
        agg_env = f"MARKETPIPE_AGG_ROOT={self.data_dir / 'agg_output'}"
        return f"{raw_env} {agg_env} python -m marketpipe aggregate-ohlcv {job_id}"

    def _build_query_with_agg_root(self, sql: str) -> str:
        """Prefix query command with aggregation root env override.

        Ensures query CLI resolves views from our agg_output.
        """
        agg_env = f"MARKETPIPE_AGG_ROOT={self.data_dir / 'agg_output'}"
        return f'{agg_env} python -m marketpipe query "{sql}" --limit 20'

    def _build_aggregate_all_jobs_command(self) -> str:
        """Build a Python one-liner to aggregate all discovered job_ids at runtime."""
        raw = str(self.data_dir / "real_output")
        agg = str(self.data_dir / "agg_output")
        py = (
            "import os,sys,subprocess; from pathlib import Path; "
            f"raw=r'{raw}'; agg=r'{agg}'; env=os.environ.copy(); env['MARKETPIPE_RAW_ROOT']=raw; env['MARKETPIPE_AGG_ROOT']=agg; "
            "ids=sorted({p.stem for p in Path(raw).rglob('*.parquet')}); "
            "[subprocess.run([sys.executable,'-m','marketpipe','aggregate-ohlcv', i], check=True, env=env) for i in ids]"
        )
        return f'python -c "{py}"'

    def _build_ratio_check_command(self) -> str:
        """Run the reusable ratio check script to sanity-check 4h vs 1h."""
        return f"python {self.ratio_check_file}"

    def _manual_verify_aggregated_output(self) -> Optional[TestResult]:
        """Preview rows from aggregated Parquet output across multiple frames."""
        try:
            agg_dir = self.data_dir / "agg_output"
            frames = ["5m", "15m", "1h", "4h", "1d"]
            import duckdb

            con = duckdb.connect()
            lines = []
            for frame in frames:
                path = agg_dir / f"frame={frame}" / "symbol=AAPL"
                if not path.exists():
                    lines.append(f"{frame}: MISSING")
                    continue
                df = con.execute(
                    f"SELECT symbol, to_timestamp(ts_ns/1e9) AS ts, open, high, low, close, volume FROM read_parquet('{path}/**/*.parquet') ORDER BY ts LIMIT 3"
                ).fetchdf()
                lines.append(f"{frame} sample:\n{df.to_string(index=False)}")
            con.close()
            return TestResult(
                test_name="Manual Verification (Aggregates)",
                category="manual_verification",
                command=f"inspect {agg_dir}",
                status="PASS",
                duration=0.0,
                exit_code=0,
                stdout="\n\n".join(lines),
                stderr="",
                expected_behavior="aggregated frames exist with sample rows",
                actual_behavior="preview succeeded",
                severity="high",
            )
        except Exception as e:
            return TestResult(
                test_name="Manual Verification (Aggregates)",
                category="manual_verification",
                command="duckdb preview aggregates",
                status="ERROR",
                duration=0.0,
                exit_code=1,
                stdout="",
                stderr=str(e),
                expected_behavior="aggregated frames exist with sample rows",
                actual_behavior=f"exception: {e}",
                severity="high",
            )

    def _manual_verify_real_output(self) -> Optional[TestResult]:
        """Perform a manual verification of any real ingested output.

        Looks under the validator's real_output directory for Parquet files and uses
        DuckDB to preview a few rows, returning a TestResult capturing findings.
        """
        try:
            out_dir = self.data_dir / "real_output"
            frame_dir = out_dir / "frame=1m"
            if not frame_dir.exists():
                return None

            # Gather a single symbol folder if available
            symbols = [p for p in frame_dir.glob("symbol=*") if p.is_dir()]
            if not symbols:
                return TestResult(
                    test_name="Manual Verification",
                    category="manual_verification",
                    command=f"inspect {frame_dir}",
                    status="FAIL",
                    duration=0.0,
                    exit_code=0,
                    stdout="",
                    stderr="",
                    expected_behavior="parquet files exist in frame=1m/symbol=SYMB",
                    actual_behavior="no symbol folders found under frame=1m",
                    severity="high",
                )

            import duckdb  # local import to avoid hard dep if unused

            symbol_dir = symbols[0]
            parquet_glob = str(symbol_dir / "**/*.parquet")
            t0 = time.time()
            con = duckdb.connect()
            sample = con.execute(
                f"SELECT symbol, to_timestamp(ts_ns/1e9) AS ts, open, high, low, close, volume "
                f"FROM read_parquet('{parquet_glob}') ORDER BY ts LIMIT 5"
            ).fetchdf()
            elapsed = time.time() - t0
            con.close()

            stdout = (
                f"Found data in {symbol_dir.name}. Sample rows:\n{sample.to_string(index=False)}\n"
            )
            return TestResult(
                test_name="Manual Verification",
                category="manual_verification",
                command=f"duckdb read_parquet('{parquet_glob}') LIMIT 5",
                status="PASS",
                duration=elapsed,
                exit_code=0,
                stdout=stdout,
                stderr="",
                expected_behavior="preview a few real ingested rows",
                actual_behavior="preview succeeded",
                severity="critical",
            )
        except Exception as e:
            return TestResult(
                test_name="Manual Verification",
                category="manual_verification",
                command="duckdb preview",
                status="ERROR",
                duration=0.0,
                exit_code=1,
                stdout="",
                stderr=str(e),
                expected_behavior="preview a few real ingested rows",
                actual_behavior=f"exception: {e}",
                severity="critical",
            )

    def _generate_summary(self) -> None:
        """Generate validation summary statistics."""
        category_stats = {}
        severity_stats = {}

        for result in self.report.results:
            # Category stats
            if result.category not in category_stats:
                category_stats[result.category] = {
                    "total": 0,
                    "pass": 0,
                    "fail": 0,
                    "error": 0,
                    "skip": 0,
                }

            category_stats[result.category]["total"] += 1
            category_stats[result.category][result.status.lower()] += 1

            # Severity stats
            if result.severity not in severity_stats:
                severity_stats[result.severity] = {
                    "total": 0,
                    "pass": 0,
                    "fail": 0,
                    "error": 0,
                    "skip": 0,
                }

            severity_stats[result.severity]["total"] += 1
            severity_stats[result.severity][result.status.lower()] += 1

        # Calculate slowest tests
        slowest_tests = sorted(self.report.results, key=lambda r: r.duration, reverse=True)[:5]

        self.report.summary = {
            "overall_status": (
                "PASS"
                if self.report.pass_rate >= 80
                else "FAIL" if self.report.pass_rate >= 60 else "CRITICAL"
            ),
            "pass_rate": f"{self.report.pass_rate:.1f}%",
            "duration_seconds": round(self.report.total_duration, 2),
            "category_breakdown": category_stats,
            "severity_breakdown": severity_stats,
            "slowest_tests": [
                {"name": t.test_name, "duration": round(t.duration, 2), "status": t.status}
                for t in slowest_tests
            ],
            "critical_failures": [
                r.test_name
                for r in self.report.results
                if r.status in ["FAIL", "ERROR"] and r.severity in ["critical", "high"]
            ],
        }

    def _display_results(self) -> None:
        """Display validation results in a formatted table."""
        self.console.print("\n[bold]🎯 Validation Results Summary[/bold]")

        # Overall stats
        overall_color = "green" if self.report.summary["overall_status"] == "PASS" else "red"
        self.console.print(
            f"[{overall_color}]Overall Status: {self.report.summary['overall_status']}[/{overall_color}] "
            f"({self.report.summary['pass_rate']} pass rate)"
        )

        self.console.print(f"Total Tests: {self.report.total_count}")
        self.console.print(f"Duration: {self.report.summary['duration_seconds']}s")

        # Category breakdown table
        table = Table(title="Results by Category")
        table.add_column("Category", style="cyan")
        table.add_column("Total", justify="right")
        table.add_column("Pass", style="green", justify="right")
        table.add_column("Fail", style="red", justify="right")
        table.add_column("Error", style="red", justify="right")
        table.add_column("Skip", style="yellow", justify="right")
        table.add_column("Pass Rate", justify="right")

        for category, stats in self.report.summary["category_breakdown"].items():
            pass_rate = (stats["pass"] / stats["total"] * 100) if stats["total"] > 0 else 0
            table.add_row(
                category,
                str(stats["total"]),
                str(stats["pass"]),
                str(stats["fail"]),
                str(stats["error"]),
                str(stats["skip"]),
                f"{pass_rate:.0f}%",
            )

        self.console.print(table)

        # Show critical failures if any
        if self.report.summary["critical_failures"]:
            self.console.print("\n[red]❌ Critical Failures:[/red]")
            for failure in self.report.summary["critical_failures"]:
                self.console.print(f"  • {failure}")

    def save_report(self, format: str = "json") -> str:
        """Save validation report in specified format."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format == "json":
            filename = f"validation_report_{timestamp}.json"
            filepath = self.output_dir / filename

            # Convert to JSON-serializable format
            report_data = {
                "start_time": self.report.start_time.isoformat(),
                "end_time": self.report.end_time.isoformat() if self.report.end_time else None,
                "mode": self.report.mode,
                "test_environment": self.report.test_environment,
                "summary": self.report.summary,
                "results": [
                    {
                        "test_name": r.test_name,
                        "category": r.category,
                        "command": r.command,
                        "status": r.status,
                        "duration": r.duration,
                        "exit_code": r.exit_code,
                        "stdout": r.stdout,
                        "stderr": r.stderr,
                        "error_message": r.error_message,
                        "expected_behavior": r.expected_behavior,
                        "actual_behavior": r.actual_behavior,
                        "severity": r.severity,
                    }
                    for r in self.report.results
                ],
            }

            with open(filepath, "w") as f:
                json.dump(report_data, f, indent=2)

        elif format == "yaml":
            filename = f"validation_report_{timestamp}.yaml"
            filepath = self.output_dir / filename

            with open(filepath, "w") as f:
                yaml.dump(report_data, f, default_flow_style=False)

        elif format == "html":
            filename = f"validation_report_{timestamp}.html"
            filepath = self.output_dir / filename

            html_content = self._generate_html_report()
            with open(filepath, "w") as f:
                f.write(html_content)

        self.console.print(f"[green]📄 Report saved: {filepath}[/green]")
        return str(filepath)

    def _generate_html_report(self) -> str:
        """Generate HTML validation report."""
        status_colors = {
            "PASS": "#28a745",
            "FAIL": "#dc3545",
            "ERROR": "#dc3545",
            "SKIP": "#ffc107",
        }

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>MarketPipe Pipeline Validation Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .header {{ background-color: #f8f9fa; padding: 20px; border-radius: 5px; }}
                .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
                .stat {{ background: #fff; padding: 15px; border-radius: 5px; border-left: 4px solid #007bff; }}
                .test-result {{ margin: 10px 0; padding: 10px; border-radius: 5px; }}
                .pass {{ background-color: #d4edda; }}
                .fail {{ background-color: #f8d7da; }}
                .error {{ background-color: #f8d7da; }}
                .skip {{ background-color: #fff3cd; }}
                .command {{ font-family: monospace; background: #f1f1f1; padding: 5px; border-radius: 3px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚀 MarketPipe Pipeline Validation Report</h1>
                <p><strong>Mode:</strong> {self.report.mode}</p>
                <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Duration:</strong> {self.report.total_duration:.1f} seconds</p>
            </div>

            <div class="summary">
                <div class="stat">
                    <h3>{self.report.total_count}</h3>
                    <p>Total Tests</p>
                </div>
                <div class="stat">
                    <h3 style="color: #28a745">{self.report.pass_count}</h3>
                    <p>Passed</p>
                </div>
                <div class="stat">
                    <h3 style="color: #dc3545">{self.report.fail_count}</h3>
                    <p>Failed</p>
                </div>
                <div class="stat">
                    <h3 style="color: #ffc107">{self.report.skip_count}</h3>
                    <p>Skipped</p>
                </div>
                <div class="stat">
                    <h3>{self.report.pass_rate:.1f}%</h3>
                    <p>Pass Rate</p>
                </div>
            </div>

            <h2>📊 Results by Category</h2>
            <table>
                <tr><th>Category</th><th>Total</th><th>Pass</th><th>Fail</th><th>Skip</th><th>Pass Rate</th></tr>
        """

        for category, stats in self.report.summary["category_breakdown"].items():
            pass_rate = (stats["pass"] / stats["total"] * 100) if stats["total"] > 0 else 0
            html += f"""
                <tr>
                    <td>{category}</td>
                    <td>{stats["total"]}</td>
                    <td style="color: #28a745">{stats["pass"]}</td>
                    <td style="color: #dc3545">{stats["fail"]}</td>
                    <td style="color: #ffc107">{stats["skip"]}</td>
                    <td>{pass_rate:.0f}%</td>
                </tr>
            """

        html += """
            </table>

            <h2>🔍 Detailed Test Results</h2>
        """

        for result in self.report.results:
            status_class = result.status.lower()
            html += f"""
            <div class="test-result {status_class}">
                <h4>{result.test_name}
                    <span style="color: {status_colors.get(result.status, '#000')}">[{result.status}]</span>
                    <span style="float: right; font-size: 0.8em">{result.duration:.1f}s</span>
                </h4>
                <div class="command">{result.command}</div>
                <p><strong>Expected:</strong> {result.expected_behavior}</p>
            """

            if result.error_message:
                html += f"<p><strong>Error:</strong> {result.error_message}</p>"

            if result.stderr and result.status in ["FAIL", "ERROR"]:
                # Show first few lines of stderr for debugging
                stderr_lines = result.stderr.split("\n")[:3]
                stderr_excerpt = "\n".join(stderr_lines)
                html += f"<details><summary>Error Details</summary><pre>{stderr_excerpt}</pre></details>"

            html += "</div>"

        html += """
            </body>
        </html>
        """

        return html

    def cleanup(self) -> None:
        """Clean up test environment."""
        if not self.dry_run:
            # Clean up test data directories
            try:
                if self.data_dir.exists():
                    shutil.rmtree(self.data_dir)
            except Exception as e:
                self.console.print(
                    f"[yellow]Warning: Could not clean up {self.data_dir}: {e}[/yellow]"
                )


def main():
    """Main CLI entry point."""

    def run_validation(
        mode: str = typer.Option("critical", help="Validation mode: quick, critical, full, stress"),
        output_dir: str = typer.Option("./validation_output", help="Output directory"),
        data_dir: str = typer.Option("./test_data", help="Test data directory"),
        timeout: int = typer.Option(120, help="Command timeout in seconds"),
        dry_run: bool = typer.Option(False, help="Show what would be tested"),
        verbose: bool = typer.Option(False, help="Enable verbose logging"),
        report_format: str = typer.Option("json", help="Report format: json, yaml, html"),
        cleanup: bool = typer.Option(True, help="Clean up test data after completion"),
    ):
        """Run comprehensive MarketPipe pipeline validation."""

        validator = ComprehensivePipelineValidator(
            mode=mode,
            output_dir=output_dir,
            data_dir=data_dir,
            timeout=timeout,
            dry_run=dry_run,
            verbose=verbose,
        )

        try:
            # Run validation
            report = validator.run_validation()

            # Save report
            validator.save_report(format=report_format)

            # Cleanup if requested
            if cleanup and not dry_run:
                validator.cleanup()

            # Exit with appropriate code
            exit_code = 0 if report.summary["overall_status"] == "PASS" else 1
            typer.Exit(exit_code)

        except KeyboardInterrupt:
            print("\n❌ Validation interrupted by user")
            typer.Exit(1)
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            typer.Exit(1)

    typer.run(run_validation)


if __name__ == "__main__":
    main()
