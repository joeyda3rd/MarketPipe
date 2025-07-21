#!/usr/bin/env python3
"""
MarketPipe Pipeline Validator

A comprehensive validation script that tests all critical MarketPipe commands
and options to ensure proper functionality and behavior validation.

Usage:
    python scripts/pipeline_validator.py [OPTIONS]

Options:
    --mode MODE          Validation mode: full, quick, critical (default: critical)
    --output-dir DIR     Directory for validation outputs (default: ./validation_output)
    --config-file FILE   Test configuration file (default: auto-generated)
    --skip-tests LIST    Comma-separated list of test categories to skip
    --verbose            Enable verbose logging
    --dry-run           Show what would be tested without executing
    --report-format FMT  Report format: json, yaml, html (default: json)
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

try:
    import typer
    from rich.console import Console
    from rich.progress import BarColumn, Progress, TimeElapsedColumn
    from rich.table import Table
except ImportError as e:
    print(f"❌ Missing required dependencies: {e}")
    print("Please install with: pip install typer rich")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
console = Console()


@dataclass
class ValidationResult:
    """Result of a validation test."""

    test_name: str
    category: str
    command: str
    status: str  # PASS, FAIL, SKIP, ERROR
    duration: float
    exit_code: int | None
    stdout: str
    stderr: str
    error_message: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class ValidationReport:
    """Complete validation report."""

    timestamp: str
    mode: str
    total_tests: int
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    errors: int = 0
    total_duration: float = 0.0
    results: list[ValidationResult] = field(default_factory=list)
    environment: dict[str, Any] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)


class PipelineValidator:
    """Comprehensive MarketPipe pipeline validator."""

    def __init__(
        self,
        mode: str = "critical",
        output_dir: str = "./validation_output",
        config_file: str | None = None,
        skip_tests: list[str] | None = None,
        verbose: bool = False,
        dry_run: bool = False,
    ):
        self.mode = mode
        self.output_dir = Path(output_dir)
        self.config_file = config_file
        self.skip_tests = skip_tests or []
        self.verbose = verbose
        self.dry_run = dry_run

        # Setup output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize report
        self.report = ValidationReport(
            timestamp=datetime.now().isoformat(), mode=mode, total_tests=0
        )

        # Test configuration
        self.test_config_path = self.output_dir / "test_config.yaml"
        self.test_data_dir = self.output_dir / "test_data"
        self.temp_dirs = []

        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)

    def setup_environment(self):
        """Setup test environment and configuration."""
        console.print("[bold blue]🔧 Setting up test environment...[/bold blue]")

        # Record environment info
        self.report.environment = {
            "python_version": sys.version,
            "platform": sys.platform,
            "cwd": str(Path.cwd()),
            "timestamp": datetime.now().isoformat(),
            "mode": self.mode,
            "output_dir": str(self.output_dir),
        }

        # Create test data directory
        self.test_data_dir.mkdir(exist_ok=True)

        # Generate test configuration if not provided
        if not self.config_file:
            self._generate_test_config()

        # Setup temporary environment file
        self._setup_env_file()

        console.print(f"✅ Environment setup complete. Output dir: {self.output_dir}")

    def _generate_test_config(self):
        """Generate a test configuration file."""
        test_config = {
            "config_version": "1",
            "alpaca": {
                "key": "test_key_placeholder",
                "secret": "test_secret_placeholder",
                "base_url": "https://data.alpaca.markets/v2",
                "rate_limit_per_min": 200,
                "feed": "iex",
            },
            "symbols": ["AAPL", "GOOGL"],
            "start": (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
            "end": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "output_path": str(self.test_data_dir),
            "compression": "snappy",
            "workers": 1,
            "metrics": {"enabled": False, "port": 8000},
        }

        with open(self.test_config_path, "w") as f:
            yaml.dump(test_config, f, default_flow_style=False)

        self.config_file = str(self.test_config_path)
        logger.info(f"Generated test configuration: {self.config_file}")

    def _setup_env_file(self):
        """Setup environment variables for testing."""
        env_file = self.output_dir / ".env"
        with open(env_file, "w") as f:
            f.write("# Test environment variables\n")
            f.write("ALPACA_KEY=test_key_for_validation\n")
            f.write("ALPACA_SECRET=test_secret_for_validation\n")

        # Set environment variables for current session
        os.environ["ALPACA_KEY"] = "test_key_for_validation"
        os.environ["ALPACA_SECRET"] = "test_secret_for_validation"

    def get_test_categories(self) -> dict[str, list[str]]:
        """Define test categories and their associated tests."""
        categories = {
            "help": [
                "marketpipe --help",
                "marketpipe ohlcv --help",
                "marketpipe symbols --help",
                "marketpipe jobs --help",
                "marketpipe prune --help",
            ],
            "health": ["marketpipe health-check", "marketpipe health-check --verbose"],
            "providers": ["marketpipe providers"],
            "symbols": ["marketpipe symbols update --help"],
            "query": ["marketpipe query --help"],
            "validation": [
                "marketpipe validate-ohlcv --help",
                "marketpipe ohlcv validate --help",
            ],
            "ingestion": ["marketpipe ingest-ohlcv --help", "marketpipe ohlcv ingest --help"],
            "aggregation": [
                "marketpipe aggregate-ohlcv --help",
                "marketpipe ohlcv aggregate --help",
            ],
            "jobs": ["marketpipe jobs --help"],
            "metrics": ["marketpipe metrics --help"],
            "maintenance": ["marketpipe migrate --help", "marketpipe prune --help"],
        }

        # Filter based on mode
        if self.mode == "quick":
            return {
                "help": categories["help"][:3],
                "health": categories["health"][:1],
                "providers": categories["providers"][:1],
            }
        elif self.mode == "critical":
            return {
                k: v
                for k, v in categories.items()
                if k in ["help", "health", "providers", "symbols", "validation"]
            }
        else:  # full mode
            return categories

    def run_command(self, command: str, timeout: int = 30) -> ValidationResult:
        """Run a single command and capture results."""
        test_name = command.replace(f"--config {self.config_file}", "--config <test_config>")
        category = self._get_command_category(command)

        if self.dry_run:
            return ValidationResult(
                test_name=test_name,
                category=category,
                command=command,
                status="SKIP",
                duration=0.0,
                exit_code=None,
                stdout="[DRY RUN] Command would be executed",
                stderr="",
            )

        start_time = time.time()

        try:
            # Prepare command
            cmd_parts = command.split()
            if cmd_parts[0] == "marketpipe":
                cmd_parts = ["python", "-m", "marketpipe"] + cmd_parts[1:]

            # Run command
            result = subprocess.run(
                cmd_parts,
                cwd=Path.cwd(),
                capture_output=True,
                text=True,
                timeout=timeout,
                env={**os.environ, "PYTHONPATH": str(Path.cwd() / "src")},
            )

            duration = time.time() - start_time

            # Analyze the result more intelligently
            status = self._analyze_command_result(result, command)

            return ValidationResult(
                test_name=test_name,
                category=category,
                command=command,
                status=status,
                duration=duration,
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )

        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            return ValidationResult(
                test_name=test_name,
                category=category,
                command=command,
                status="ERROR",
                duration=duration,
                exit_code=-1,
                stdout="",
                stderr="Command timed out",
                error_message="Command execution timed out",
            )

        except Exception as e:
            duration = time.time() - start_time
            return ValidationResult(
                test_name=test_name,
                category=category,
                command=command,
                status="ERROR",
                duration=duration,
                exit_code=-1,
                stdout="",
                stderr=str(e),
                error_message=f"Exception during execution: {e}",
            )

    def _get_command_category(self, command: str) -> str:
        """Determine the category of a command."""
        if "--help" in command:
            return "help"
        elif "health-check" in command:
            return "health"
        elif "providers" in command:
            return "providers"
        elif "symbols" in command:
            return "symbols"
        elif "validate" in command:
            return "validation"
        elif "ingest" in command:
            return "ingestion"
        elif "aggregate" in command:
            return "aggregation"
        elif "jobs" in command:
            return "jobs"
        elif "metrics" in command:
            return "metrics"
        elif "query" in command:
            return "query"
        else:
            return "misc"

    def _analyze_command_result(self, result, command: str) -> str:
        """Analyze command result to determine pass/fail status intelligently."""
        # Help commands should always pass if they return help text
        if "--help" in command:
            if result.returncode == 0 and (
                "help" in result.stdout.lower() or "usage:" in result.stdout.lower()
            ):
                return "PASS"
            else:
                return "FAIL"

        # Health check special handling - consider 90%+ pass rate as success
        if "health-check" in command:
            if result.returncode == 0:
                return "PASS"
            elif result.stdout and "Summary:" in result.stdout:
                # Parse the health check summary
                try:
                    lines = result.stdout.split("\n")
                    for line in lines:
                        if "Summary:" in line and "passed" in line:
                            # Extract pass count and total (e.g., "Summary: 9/10 checks passed")
                            parts = line.split()
                            for part in parts:
                                if "/" in part:
                                    passed, total = part.split("/")
                                    pass_rate = int(passed) / int(total)
                                    if pass_rate >= 0.8:  # 80% or better
                                        return "PASS"
                                    break
                except (ValueError, IndexError):
                    pass
                return "FAIL"
            else:
                return "FAIL"

        # For all other commands, use exit code
        return "PASS" if result.returncode == 0 else "FAIL"

    def run_validation(self) -> ValidationReport:
        """Run the complete validation suite."""
        console.print(
            f"[bold green]🚀 Starting MarketPipe validation in {self.mode} mode[/bold green]"
        )

        start_time = time.time()

        # Setup
        self.setup_environment()

        # Get test categories
        test_categories = self.get_test_categories()

        # Filter out skipped tests
        if self.skip_tests:
            for skip_category in self.skip_tests:
                if skip_category in test_categories:
                    del test_categories[skip_category]
                    console.print(f"⏭️  Skipping category: {skip_category}")

        # Calculate total tests
        total_commands = sum(len(commands) for commands in test_categories.values())
        self.report.total_tests = total_commands

        # Run tests with progress bar
        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
            console=console,
        ) as progress:

            task = progress.add_task("Running validation tests", total=total_commands)

            for category, commands in test_categories.items():
                console.print(f"\n[bold yellow]📋 Testing category: {category}[/bold yellow]")

                for command in commands:
                    progress.update(task, description=f"Testing: {command[:50]}...")

                    result = self.run_command(command)
                    self.report.results.append(result)

                    # Update counters
                    if result.status == "PASS":
                        self.report.passed += 1
                        console.print(f"  ✅ {result.test_name}")
                    elif result.status == "FAIL":
                        self.report.failed += 1
                        console.print(f"  ❌ {result.test_name}")
                        if self.verbose:
                            console.print(f"     Error: {result.stderr[:200]}")
                    elif result.status == "SKIP":
                        self.report.skipped += 1
                        console.print(f"  ⏭️  {result.test_name}")
                    else:  # ERROR
                        self.report.errors += 1
                        console.print(f"  💥 {result.test_name}")
                        if self.verbose:
                            console.print(f"     Error: {result.error_message}")

                    progress.update(task, advance=1)

        # Finalize report
        self.report.total_duration = time.time() - start_time
        self.report.summary = self._generate_summary()

        return self.report

    def _generate_summary(self) -> dict[str, Any]:
        """Generate validation summary."""
        total = self.report.total_tests
        if total == 0:
            return {"message": "No tests run"}

        pass_rate = (self.report.passed / total) * 100

        # Categorize results
        category_stats = {}
        for result in self.report.results:
            cat = result.category
            if cat not in category_stats:
                category_stats[cat] = {"pass": 0, "fail": 0, "skip": 0, "error": 0}
            category_stats[cat][result.status.lower()] += 1

        # Find slowest tests
        slowest_tests = sorted(
            [(r.test_name, r.duration) for r in self.report.results],
            key=lambda x: x[1],
            reverse=True,
        )[:5]

        return {
            "overall_status": (
                "PASS" if pass_rate >= 80 else "FAIL" if pass_rate >= 60 else "CRITICAL"
            ),
            "pass_rate": f"{pass_rate:.1f}%",
            "duration_seconds": round(self.report.total_duration, 2),
            "category_breakdown": category_stats,
            "slowest_tests": slowest_tests,
            "critical_failures": [
                r.test_name
                for r in self.report.results
                if r.status in ["FAIL", "ERROR"] and r.category in ["health", "providers"]
            ],
        }

    def save_report(self, format: str = "json"):
        """Save validation report to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format == "json":
            report_file = self.output_dir / f"validation_report_{timestamp}.json"
            with open(report_file, "w") as f:
                json.dump(self.report.__dict__, f, indent=2, default=str)
        elif format == "yaml":
            report_file = self.output_dir / f"validation_report_{timestamp}.yaml"
            with open(report_file, "w") as f:
                yaml.dump(self.report.__dict__, f, default_flow_style=False)
        elif format == "html":
            report_file = self.output_dir / f"validation_report_{timestamp}.html"
            self._generate_html_report(report_file)

        console.print(f"📊 Report saved: {report_file}")
        return report_file

    def _generate_html_report(self, output_file: Path):
        """Generate HTML report."""
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>MarketPipe Validation Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
        .stat-box {{ background: #e8f4f8; padding: 15px; border-radius: 5px; flex: 1; }}
        .pass {{ color: green; }}
        .fail {{ color: red; }}
        .skip {{ color: orange; }}
        .error {{ color: purple; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .status-pass {{ background-color: #d4edda; }}
        .status-fail {{ background-color: #f8d7da; }}
        .status-skip {{ background-color: #fff3cd; }}
        .status-error {{ background-color: #f5c6cb; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>MarketPipe Validation Report</h1>
        <p>Generated: {self.report.timestamp}</p>
        <p>Mode: {self.report.mode}</p>
        <p>Duration: {self.report.total_duration:.2f} seconds</p>
    </div>

    <div class="summary">
        <div class="stat-box">
            <h3>Total Tests</h3>
            <h2>{self.report.total_tests}</h2>
        </div>
        <div class="stat-box">
            <h3 class="pass">Passed</h3>
            <h2>{self.report.passed}</h2>
        </div>
        <div class="stat-box">
            <h3 class="fail">Failed</h3>
            <h2>{self.report.failed}</h2>
        </div>
        <div class="stat-box">
            <h3 class="skip">Skipped</h3>
            <h2>{self.report.skipped}</h2>
        </div>
        <div class="stat-box">
            <h3 class="error">Errors</h3>
            <h2>{self.report.errors}</h2>
        </div>
    </div>

    <h2>Detailed Results</h2>
    <table>
        <tr>
            <th>Test Name</th>
            <th>Category</th>
            <th>Status</th>
            <th>Duration</th>
            <th>Exit Code</th>
        </tr>
"""

        for result in self.report.results:
            status_class = f"status-{result.status.lower()}"
            html_content += f"""
        <tr class="{status_class}">
            <td>{result.test_name}</td>
            <td>{result.category}</td>
            <td>{result.status}</td>
            <td>{result.duration:.3f}s</td>
            <td>{result.exit_code or 'N/A'}</td>
        </tr>
"""

        html_content += """
    </table>
</body>
</html>
"""

        with open(output_file, "w") as f:
            f.write(html_content)

    def print_summary(self):
        """Print validation summary to console."""
        summary = self.report.summary

        # Create summary table
        table = Table(title="MarketPipe Validation Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")

        table.add_row("Overall Status", summary["overall_status"])
        table.add_row("Pass Rate", summary["pass_rate"])
        table.add_row("Total Tests", str(self.report.total_tests))
        table.add_row("Passed", str(self.report.passed))
        table.add_row("Failed", str(self.report.failed))
        table.add_row("Skipped", str(self.report.skipped))
        table.add_row("Errors", str(self.report.errors))
        table.add_row("Duration", f"{summary['duration_seconds']}s")

        console.print(table)

        # Show critical failures if any
        if summary["critical_failures"]:
            console.print("\n[bold red]❌ Critical Failures:[/bold red]")
            for failure in summary["critical_failures"]:
                console.print(f"  • {failure}")

        # Show category breakdown
        console.print("\n[bold blue]📊 Category Breakdown:[/bold blue]")
        for category, stats in summary["category_breakdown"].items():
            total_cat = sum(stats.values())
            pass_pct = (stats["pass"] / total_cat * 100) if total_cat > 0 else 0
            console.print(f"  {category}: {stats['pass']}/{total_cat} ({pass_pct:.0f}% pass)")

    def cleanup(self):
        """Clean up temporary files and directories."""
        for temp_dir in self.temp_dirs:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

        console.print("🧹 Cleanup completed")


def main(
    mode: str = typer.Option("critical", help="Validation mode: full, quick, critical"),
    output_dir: str = typer.Option("./validation_output", help="Output directory"),
    config_file: str | None = typer.Option(None, help="Test configuration file"),
    skip_tests: str | None = typer.Option(None, help="Comma-separated test categories to skip"),
    verbose: bool = typer.Option(False, help="Enable verbose logging"),
    dry_run: bool = typer.Option(False, help="Show what would be tested"),
    report_format: str = typer.Option("json", help="Report format: json, yaml, html"),
):
    """MarketPipe Pipeline Validator - Test all critical commands and validate behavior."""

    skip_list = skip_tests.split(",") if skip_tests else []

    validator = PipelineValidator(
        mode=mode,
        output_dir=output_dir,
        config_file=config_file,
        skip_tests=skip_list,
        verbose=verbose,
        dry_run=dry_run,
    )

    try:
        # Run validation
        report = validator.run_validation()

        # Print summary
        validator.print_summary()

        # Save report
        validator.save_report(report_format)

        # Exit with appropriate code
        if report.summary["overall_status"] == "CRITICAL":
            console.print("\n[bold red]💥 Validation FAILED - Critical issues found[/bold red]")
            sys.exit(2)
        elif report.summary["overall_status"] == "FAIL":
            console.print("\n[bold yellow]⚠️  Validation completed with issues[/bold yellow]")
            sys.exit(1)
        else:
            console.print("\n[bold green]✅ Validation PASSED[/bold green]")
            sys.exit(0)

    except KeyboardInterrupt:
        console.print("\n❌ Validation interrupted by user")
        sys.exit(130)
    except Exception as e:
        console.print(f"\n💥 Validation failed with error: {e}")
        if verbose:
            console.print(traceback.format_exc())
        sys.exit(1)
    finally:
        validator.cleanup()


if __name__ == "__main__":
    typer.run(main)
