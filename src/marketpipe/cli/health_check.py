"""
CLI Health Check Command

Built-in validation that users can run to ensure their MarketPipe installation
and configuration is working correctly.

This module implements the health check component of Phase 5:
- Validates all providers are accessible
- Tests database connectivity
- Verifies configuration validity
- Checks data directory permissions
- Validates dependencies are installed
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import typer
import yaml

# Import MarketPipe components for testing
try:
    from marketpipe import __version__ as mp_version
    from marketpipe.ingestion.infrastructure.provider_loader import get_available_providers

    MARKETPIPE_AVAILABLE = True
except ImportError:
    MARKETPIPE_AVAILABLE = False


@dataclass
class HealthCheckResult:
    """Result of a health check test."""

    name: str
    description: str
    passed: bool = False
    warning: bool = False
    error_message: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    execution_time_ms: float = 0.0


class MarketPipeHealthChecker:
    """Comprehensive health check for MarketPipe installation."""

    def __init__(self):
        self.results: list[HealthCheckResult] = []

    def run_all_checks(
        self, config_path: Path | None = None, verbose: bool = False
    ) -> list[HealthCheckResult]:
        """
        Run all health checks.

        Args:
            config_path: Optional path to configuration file
            verbose: Show detailed output

        Returns:
            List of HealthCheckResult objects
        """
        self.results = []

        # Core system checks
        self.check_python_version()
        self.check_dependencies()
        self.check_marketpipe_installation()

        # Configuration checks
        self.check_configuration(config_path)

        # Provider checks
        self.check_provider_registry()
        self.check_fake_provider()

        # Database checks
        self.check_database_connectivity()

        # File system checks
        self.check_data_directory_permissions()

        # CLI checks
        self.check_cli_commands()

        # Integration checks
        self.check_end_to_end_pipeline()

        return self.results

    def check_python_version(self) -> HealthCheckResult:
        """Check Python version compatibility."""
        start_time = time.time()

        result = HealthCheckResult(
            name="python_version", description="Python version compatibility"
        )

        try:
            version = sys.version_info

            # MarketPipe requires Python 3.8+
            if version >= (3, 8):
                result.passed = True
                result.details = {
                    "version": f"{version.major}.{version.minor}.{version.micro}",
                    "compatible": True,
                }
            else:
                result.passed = False
                result.error_message = (
                    f"Python {version.major}.{version.minor} is not supported. Requires Python 3.8+"
                )
                result.details = {
                    "version": f"{version.major}.{version.minor}.{version.micro}",
                    "compatible": False,
                    "minimum_required": "3.8",
                }

        except Exception as e:
            result.passed = False
            result.error_message = f"Failed to check Python version: {e}"

        finally:
            result.execution_time_ms = (time.time() - start_time) * 1000
            self.results.append(result)

        return result

    def check_dependencies(self) -> HealthCheckResult:
        """Check required dependencies are installed."""
        start_time = time.time()

        result = HealthCheckResult(name="dependencies", description="Required dependencies")

        required_packages = [
            "typer",
            "pydantic",
            "httpx",
            "pandas",
            "pyarrow",
            "duckdb",
            "pyyaml",
            "python-dotenv",
            "prometheus-client",
        ]

        # Map package names to their import names
        import_mapping = {
            "pyyaml": "yaml",
            "python-dotenv": "dotenv",
            "prometheus-client": "prometheus_client",
        }

        missing_packages = []
        installed_packages = {}

        for package in required_packages:
            import_name = import_mapping.get(package, package.replace("-", "_"))
            try:
                module = importlib.import_module(import_name)
                version = getattr(module, "__version__", "unknown")
                installed_packages[package] = version
            except ImportError:
                missing_packages.append(package)

        if not missing_packages:
            result.passed = True
            result.details = {
                "installed_packages": installed_packages,
                "all_required_present": True,
            }
        else:
            result.passed = False
            result.error_message = f"Missing required packages: {', '.join(missing_packages)}"
            result.details = {
                "installed_packages": installed_packages,
                "missing_packages": missing_packages,
            }

        result.execution_time_ms = (time.time() - start_time) * 1000
        self.results.append(result)
        return result

    def check_marketpipe_installation(self) -> HealthCheckResult:
        """Check MarketPipe installation."""
        start_time = time.time()

        result = HealthCheckResult(
            name="marketpipe_installation", description="MarketPipe installation"
        )

        try:
            if MARKETPIPE_AVAILABLE:
                result.passed = True
                result.details = {"version": mp_version, "importable": True}
            else:
                result.passed = False
                result.error_message = "MarketPipe modules not importable"
                result.details = {"importable": False}

        except Exception as e:
            result.passed = False
            result.error_message = f"MarketPipe installation check failed: {e}"

        result.execution_time_ms = (time.time() - start_time) * 1000
        self.results.append(result)
        return result

    def check_configuration(self, config_path: Path | None) -> HealthCheckResult:
        """Check configuration file validity."""
        start_time = time.time()

        result = HealthCheckResult(name="configuration", description="Configuration file validity")

        try:
            if config_path and config_path.exists():
                # Test loading configuration
                with open(config_path) as f:
                    config_data = yaml.safe_load(f)

                result.passed = True
                result.details = {
                    "config_file": str(config_path),
                    "parseable": True,
                    "sections": list(config_data.keys()) if config_data else [],
                }
            elif config_path:
                result.passed = False
                result.error_message = f"Configuration file not found: {config_path}"
                result.details = {"config_file": str(config_path), "exists": False}
            else:
                result.passed = True
                result.warning = True
                result.error_message = "No configuration file specified"
                result.details = {"config_file": None, "using_defaults": True}

        except Exception as e:
            result.passed = False
            result.error_message = f"Configuration validation failed: {e}"

        result.execution_time_ms = (time.time() - start_time) * 1000
        self.results.append(result)
        return result

    def check_provider_registry(self) -> HealthCheckResult:
        """Check provider registry functionality."""
        start_time = time.time()

        result = HealthCheckResult(name="provider_registry", description="Provider registry")

        try:
            if MARKETPIPE_AVAILABLE:
                providers = get_available_providers()

                result.passed = len(providers) > 0
                result.details = {
                    "available_providers": providers,
                    "provider_count": len(providers),
                }

                if not result.passed:
                    result.error_message = "No providers found in registry"
            else:
                result.passed = False
                result.error_message = "MarketPipe not available for provider registry check"

        except Exception as e:
            result.passed = False
            result.error_message = f"Provider registry check failed: {e}"

        result.execution_time_ms = (time.time() - start_time) * 1000
        self.results.append(result)
        return result

    def check_fake_provider(self) -> HealthCheckResult:
        """Check fake provider functionality."""
        start_time = time.time()

        result = HealthCheckResult(name="fake_provider", description="Fake provider functionality")

        try:
            # Test fake provider via CLI
            cmd = [sys.executable, "-m", "marketpipe", "providers"]

            process_result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if process_result.returncode == 0:
                output = process_result.stdout.lower()
                if "fake" in output:
                    result.passed = True
                    result.details = {"fake_provider_available": True, "cli_accessible": True}
                else:
                    result.passed = False
                    result.error_message = "Fake provider not found in provider list"
            else:
                result.passed = False
                result.error_message = f"Provider list command failed: {process_result.stderr}"

        except Exception as e:
            result.passed = False
            result.error_message = f"Fake provider check failed: {e}"

        result.execution_time_ms = (time.time() - start_time) * 1000
        self.results.append(result)
        return result

    def check_database_connectivity(self) -> HealthCheckResult:
        """Check database connectivity."""
        start_time = time.time()

        result = HealthCheckResult(
            name="database_connectivity", description="Database connectivity"
        )

        try:
            import duckdb

            # Test DuckDB connection
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "test.db"

                conn = duckdb.connect(str(db_path))
                conn.execute("CREATE TABLE test (id INTEGER)")
                conn.execute("INSERT INTO test VALUES (1)")
                res = conn.execute("SELECT COUNT(*) FROM test").fetchone()
                conn.close()

                if res and res[0] == 1:
                    result.passed = True
                    result.details = {"duckdb_working": True, "test_database_path": str(db_path)}
                else:
                    result.passed = False
                    result.error_message = "DuckDB test query failed"

        except Exception as e:
            result.passed = False
            result.error_message = f"Database connectivity check failed: {e}"

        result.execution_time_ms = (time.time() - start_time) * 1000
        self.results.append(result)
        return result

    def check_data_directory_permissions(self) -> HealthCheckResult:
        """Check data directory permissions."""
        start_time = time.time()

        result = HealthCheckResult(
            name="data_directory_permissions", description="Data directory permissions"
        )

        try:
            # Test current directory permissions
            current_dir = Path.cwd()

            # Test write permissions
            test_file = current_dir / ".marketpipe_health_check_test"

            try:
                test_file.write_text("test")
                test_file.unlink()

                result.passed = True
                result.details = {"current_directory": str(current_dir), "writable": True}
            except Exception:
                result.passed = False
                result.error_message = f"No write permission in current directory: {current_dir}"
                result.details = {"current_directory": str(current_dir), "writable": False}

        except Exception as e:
            result.passed = False
            result.error_message = f"Directory permission check failed: {e}"

        result.execution_time_ms = (time.time() - start_time) * 1000
        self.results.append(result)
        return result

    def check_cli_commands(self) -> HealthCheckResult:
        """Check CLI commands are accessible."""
        start_time = time.time()

        result = HealthCheckResult(name="cli_commands", description="CLI commands accessibility")

        try:
            # Test main CLI help
            cmd = [sys.executable, "-m", "marketpipe", "--help"]

            process_result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if process_result.returncode == 0:
                result.passed = True
                result.details = {"cli_accessible": True, "help_working": True}
            else:
                result.passed = False
                result.error_message = f"CLI help command failed: {process_result.stderr}"

        except Exception as e:
            result.passed = False
            result.error_message = f"CLI command check failed: {e}"

        result.execution_time_ms = (time.time() - start_time) * 1000
        self.results.append(result)
        return result

    def check_end_to_end_pipeline(self) -> HealthCheckResult:
        """Check end-to-end pipeline functionality."""
        start_time = time.time()

        result = HealthCheckResult(
            name="end_to_end_pipeline", description="End-to-end pipeline test"
        )

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Test basic ingestion with fake provider
                cmd = [
                    sys.executable,
                    "-m",
                    "marketpipe",
                    "ingest-ohlcv",
                    "--provider",
                    "fake",
                    "--symbols",
                    "AAPL",
                    "--start",
                    "2023-01-01",
                    "--end",
                    "2023-01-01",
                    "--output",
                    str(temp_path / "data"),
                ]

                process_result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

                if process_result.returncode == 0:
                    # Check if data files were created
                    data_files = list((temp_path / "data").rglob("*.parquet"))

                    if data_files:
                        result.passed = True
                        result.details = {
                            "pipeline_working": True,
                            "files_created": len(data_files),
                            "data_directory": str(temp_path / "data"),
                        }
                    else:
                        result.passed = False
                        result.error_message = "Pipeline ran but no data files created"
                        result.details = {"pipeline_working": False, "files_created": 0}
                else:
                    result.passed = False
                    result.error_message = f"Pipeline test failed: {process_result.stderr}"

        except Exception as e:
            result.passed = False
            result.error_message = f"End-to-end pipeline check failed: {e}"

        result.execution_time_ms = (time.time() - start_time) * 1000
        self.results.append(result)
        return result

    def generate_report(self, verbose: bool = False) -> str:
        """Generate health check report."""
        lines = []
        lines.append("MarketPipe Health Check Report")
        lines.append("=" * 50)
        lines.append("")

        # Summary
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r.passed)
        warning_checks = sum(1 for r in self.results if r.warning)
        failed_checks = total_checks - passed_checks

        lines.append(f"Summary: {passed_checks}/{total_checks} checks passed")
        if warning_checks > 0:
            lines.append(f"Warnings: {warning_checks}")
        if failed_checks > 0:
            lines.append(f"Failures: {failed_checks}")
        lines.append("")

        # Individual results
        for result in self.results:
            status_icon = "✅" if result.passed else ("⚠️" if result.warning else "❌")
            lines.append(f"{status_icon} {result.description}")

            if not result.passed and result.error_message:
                lines.append(f"   Error: {result.error_message}")

            if verbose and result.details:
                lines.append(f"   Details: {result.details}")

            lines.append(f"   Time: {result.execution_time_ms:.1f}ms")
            lines.append("")

        # Recommendations
        if failed_checks > 0:
            lines.append("Recommendations:")
            lines.append("-" * 15)

            for result in self.results:
                if not result.passed:
                    lines.append(f"• {result.name}: {self._get_recommendation(result)}")

            lines.append("")

        return "\n".join(lines)

    def _get_recommendation(self, result: HealthCheckResult) -> str:
        """Get recommendation for failed health check."""
        recommendations = {
            "python_version": "Upgrade to Python 3.8 or higher",
            "dependencies": "Install missing packages with: pip install -r requirements.txt",
            "marketpipe_installation": "Reinstall MarketPipe with: pip install -e .",
            "configuration": "Check configuration file syntax and required fields",
            "provider_registry": "Verify MarketPipe installation and provider modules",
            "fake_provider": "Check that fake provider is properly installed",
            "database_connectivity": "Verify DuckDB installation",
            "data_directory_permissions": "Ensure write permissions in data directory",
            "cli_commands": "Check MarketPipe installation and PATH configuration",
            "end_to_end_pipeline": "Run individual components to isolate the issue",
        }

        return recommendations.get(result.name, "Check error message for details")


def health_check_command(
    config: Path | None = typer.Option(None, "--config", "-c", help="Configuration file path"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed output"),
    output_file: Path | None = typer.Option(None, "--output", "-o", help="Save report to file"),
) -> None:
    """
    Run comprehensive health check of MarketPipe installation and configuration.

    This command validates:
    - Python version compatibility
    - Required dependencies
    - MarketPipe installation
    - Configuration validity
    - Provider accessibility
    - Database connectivity
    - File system permissions
    - CLI command functionality
    - End-to-end pipeline
    """
    checker = MarketPipeHealthChecker()

    typer.echo("Running MarketPipe health check...")
    typer.echo("")

    # Run all health checks
    results = checker.run_all_checks(config, verbose)

    # Generate and display report
    report = checker.generate_report(verbose)
    typer.echo(report)

    # Save to file if requested
    if output_file:
        output_file.write_text(report)
        typer.echo(f"Report saved to: {output_file}")

    # Exit with appropriate code
    failed_checks = sum(1 for r in results if not r.passed and not r.warning)
    if failed_checks > 0:
        typer.echo(f"\nHealth check completed with {failed_checks} failures.", err=True)
        raise typer.Exit(1)
    else:
        typer.echo("\nHealth check completed successfully!")
        raise typer.Exit(0)


if __name__ == "__main__":
    typer.run(health_check_command)
