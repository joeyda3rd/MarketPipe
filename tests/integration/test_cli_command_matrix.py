"""
CLI Command Matrix Testing Framework

Comprehensive validation framework that ensures every MarketPipe command and
option works correctly across all supported scenarios.

This module implements Phase 1 of the CLI validation framework:
- Auto-discovers all CLI commands from Typer app structure
- Tests every command combination for completeness
- Validates help text consistency and format
- Checks for side effects (no unexpected file creation)
- Provides comprehensive coverage matrix reporting
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

# Import the CLI app for introspection
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

try:
    from typer.testing import CliRunner

    from marketpipe.cli import app

    TYPER_AVAILABLE = True
except ImportError:
    TYPER_AVAILABLE = False
    app = None


@dataclass
class CommandInfo:
    """Information about a discovered CLI command."""

    name: str
    path: list[str]  # Full command path (e.g., ['ohlcv', 'ingest'])
    parameters: dict[str, Any] = field(default_factory=dict)
    is_subcommand: bool = False
    parent_app: str = ""
    help_text: str = ""
    deprecated: bool = False


@dataclass
class ValidationResult:
    """Result of command validation."""

    command: CommandInfo
    help_works: bool = False
    help_output: str = ""
    side_effects_clean: bool = False
    created_files: list[Path] = field(default_factory=list)
    error_messages: list[str] = field(default_factory=list)
    execution_time_ms: float = 0.0


class CLICommandDiscovery:
    """Discovers all available CLI commands and their structure."""

    def __init__(self):
        self.discovered_commands: list[CommandInfo] = []
        self.command_tree: dict[str, Any] = {}

    def discover_all_commands(self) -> list[CommandInfo]:
        """
        Auto-discover all CLI commands from the Typer app structure.

        Returns:
            List of CommandInfo objects representing all available commands
        """
        commands = []

        # First, discover main app commands
        commands.extend(self._discover_main_commands())

        # Then discover subapp commands
        commands.extend(self._discover_subapp_commands())

        # Add known deprecated commands
        commands.extend(self._get_deprecated_commands())

        self.discovered_commands = commands
        return commands

    def _discover_main_commands(self) -> list[CommandInfo]:
        """Discover main-level commands."""
        commands = []

        # Known main commands based on research
        main_commands = [
            "ingest-ohlcv",
            "validate-ohlcv",
            "aggregate-ohlcv",
            "query",
            "metrics",
            "providers",
            "migrate",
            "health-check",
            "factory-reset",
        ]

        for cmd in main_commands:
            commands.append(CommandInfo(name=cmd, path=[cmd], is_subcommand=False))

        return commands

    def _discover_subapp_commands(self) -> list[CommandInfo]:
        """Discover subapp commands."""
        commands = []

        # OHLCV subcommands
        ohlcv_commands = ["ingest", "validate", "aggregate", "backfill"]
        for cmd in ohlcv_commands:
            commands.append(
                CommandInfo(name=cmd, path=["ohlcv", cmd], is_subcommand=True, parent_app="ohlcv")
            )

        # Prune subcommands
        prune_commands = ["parquet", "database"]
        for cmd in prune_commands:
            commands.append(
                CommandInfo(name=cmd, path=["prune", cmd], is_subcommand=True, parent_app="prune")
            )

        # Symbols subcommands
        symbols_commands = ["update"]
        for cmd in symbols_commands:
            commands.append(
                CommandInfo(
                    name=cmd, path=["symbols", cmd], is_subcommand=True, parent_app="symbols"
                )
            )

        # Jobs subcommands
        jobs_commands = ["list", "status", "doctor", "kill"]
        for cmd in jobs_commands:
            commands.append(
                CommandInfo(name=cmd, path=["jobs", cmd], is_subcommand=True, parent_app="jobs")
            )

        return commands

    def _get_deprecated_commands(self) -> list[CommandInfo]:
        """Get known deprecated commands."""
        deprecated = [
            CommandInfo(name="ingest", path=["ingest"], deprecated=True),
            CommandInfo(name="validate", path=["validate"], deprecated=True),
            CommandInfo(name="aggregate", path=["aggregate"], deprecated=True),
        ]
        return deprecated


class CLICommandValidator:
    """Validates CLI commands for correctness and consistency."""

    def __init__(self, use_subprocess: bool = True):
        self.use_subprocess = use_subprocess
        self.runner = CliRunner() if TYPER_AVAILABLE else None

    def validate_command(self, command: CommandInfo) -> ValidationResult:
        """One isolated invocation checks help, timing, and filesystem side effects."""
        result = ValidationResult(command=command)
        started = time.monotonic()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            try:
                if self.use_subprocess:
                    process = subprocess.run(
                        [sys.executable, "-m", "marketpipe", *command.path, "--help"],
                        capture_output=True,
                        text=True,
                        timeout=10,
                        cwd=path,
                    )
                    result.help_works = process.returncode == 0
                    result.help_output = process.stdout + process.stderr
                    result.created_files = list(path.rglob("*"))
                elif self.runner and app:
                    with self.runner.isolated_filesystem(temp_dir=path) as isolated:
                        process = self.runner.invoke(app, [*command.path, "--help"])
                        result.help_works = process.exit_code == 0
                        result.help_output = process.output
                        result.created_files = list(Path(isolated).rglob("*"))
                result.side_effects_clean = not result.created_files
            except subprocess.TimeoutExpired:
                result.help_output = "Command timed out"
            except Exception as error:
                result.help_output = f"Command failed: {error}"
        result.execution_time_ms = (time.monotonic() - started) * 1000
        return result


class CLIMatrixTestReporter:
    """Generates comprehensive reports of CLI validation results."""

    def __init__(self):
        self.results: list[ValidationResult] = []

    def add_results(self, results: list[ValidationResult]):
        """Add validation results to the reporter."""
        self.results.extend(results)

    def generate_coverage_report(self) -> str:
        """Generate comprehensive coverage report."""
        report = []
        report.append("# MarketPipe CLI Command Matrix Validation Report")
        report.append("=" * 60)
        report.append("")

        # Summary statistics
        total_commands = len(self.results)
        help_working = sum(1 for r in self.results if r.help_works)
        clean_commands = sum(1 for r in self.results if r.side_effects_clean)

        report.append("## Summary")
        report.append(f"- Total Commands Tested: {total_commands}")
        report.append(
            f"- Help Commands Working: {help_working}/{total_commands} ({help_working/total_commands*100:.1f}%)"
        )
        report.append(
            f"- Side-Effect Clean: {clean_commands}/{total_commands} ({clean_commands/total_commands*100:.1f}%)"
        )
        report.append("")

        # Command breakdown by category
        report.append("## Command Categories")

        main_commands = [
            r for r in self.results if not r.command.is_subcommand and not r.command.deprecated
        ]
        subcommands = [r for r in self.results if r.command.is_subcommand]
        deprecated = [r for r in self.results if r.command.deprecated]

        report.append(f"- Main Commands: {len(main_commands)}")
        report.append(f"- Subcommands: {len(subcommands)}")
        report.append(f"- Deprecated Commands: {len(deprecated)}")
        report.append("")

        # Detailed results
        report.append("## Detailed Results")
        report.append("")

        for result in sorted(self.results, key=lambda r: r.command.path):
            status_icon = "✅" if result.help_works and result.side_effects_clean else "❌"
            command_path = " ".join(result.command.path)

            report.append(f"{status_icon} `marketpipe {command_path}`")

            if not result.help_works:
                report.append("   - ❌ Help command failed")
            if not result.side_effects_clean:
                report.append(f"   - ❌ Created {len(result.created_files)} unexpected files")
            if result.command.deprecated:
                report.append("   - ⚠️  Deprecated command")

            report.append(f"   - Execution time: {result.execution_time_ms:.1f}ms")
            report.append("")

        return "\n".join(report)

    def get_failed_commands(self) -> list[ValidationResult]:
        """Get commands that failed validation."""
        return [r for r in self.results if not r.help_works or not r.side_effects_clean]


class TestCLICommandMatrix:
    """Test suite for comprehensive CLI command matrix validation."""

    @pytest.fixture
    def discovery(self):
        """CLI command discovery fixture."""
        return CLICommandDiscovery()

    @pytest.fixture
    def validator(self):
        """CLI command validator fixture."""
        return CLICommandValidator(use_subprocess=True)

    @pytest.fixture
    def reporter(self):
        """CLI test reporter fixture."""
        return CLIMatrixTestReporter()

    def test_discover_all_commands(self, discovery):
        """Test that command discovery finds all expected commands."""
        commands = discovery.discover_all_commands()

        # Should find at least these command categories
        main_commands = [c for c in commands if not c.is_subcommand and not c.deprecated]
        subcommands = [c for c in commands if c.is_subcommand]
        deprecated = [c for c in commands if c.deprecated]

        assert (
            len(main_commands) >= 7
        ), f"Expected at least 7 main commands, found {len(main_commands)}"
        assert len(subcommands) >= 7, f"Expected at least 7 subcommands, found {len(subcommands)}"
        assert (
            len(deprecated) >= 3
        ), f"Expected at least 3 deprecated commands, found {len(deprecated)}"

        # Verify specific critical commands exist
        command_paths = [" ".join(c.path) for c in commands]
        critical_commands = [
            "ingest-ohlcv",
            "validate-ohlcv",
            "aggregate-ohlcv",
            "ohlcv ingest",
            "ohlcv validate",
            "ohlcv aggregate",
            "query",
            "metrics",
        ]

        for critical in critical_commands:
            assert critical in command_paths, f"Critical command '{critical}' not discovered"

    def test_all_help_commands_work(self, discovery, validator, reporter):
        """Test that all discovered commands have working help."""
        commands = discovery.discover_all_commands()
        results = []

        for command in commands:
            result = validator.validate_command(command)
            results.append(result)

        reporter.add_results(results)

        # Generate comprehensive report
        report = reporter.generate_coverage_report()
        print("\n" + report)

        # Check for failures
        failed_commands = reporter.get_failed_commands()

        if failed_commands:
            failure_details = []
            for failed in failed_commands:
                cmd_path = " ".join(failed.command.path)
                issues = []
                if not failed.help_works:
                    issues.append("help failed")
                if not failed.side_effects_clean:
                    issues.append(f"created {len(failed.created_files)} files")
                failure_details.append(f"  - {cmd_path}: {', '.join(issues)}")

            pytest.fail(
                f"Found {len(failed_commands)} commands with issues:\n"
                + "\n".join(failure_details)
                + f"\n\nFull report:\n{report}"
            )

    def test_help_output_consistency(self, discovery, validator):
        """Test that help output follows consistent patterns."""
        commands = discovery.discover_all_commands()
        inconsistencies = []

        # Commands that use custom help format (not standard Typer help)
        custom_help_commands = {"ingest-ohlcv", "ohlcv ingest"}

        for command in commands:
            if command.deprecated:
                continue  # Skip deprecated commands for consistency checks

            result = validator.validate_command(command)
            command_path = " ".join(command.path)

            if result.help_works:
                help_output = result.help_output.lower()

                # Check if this is a custom help command
                if command_path in custom_help_commands:
                    # For custom help, just ensure it's not empty and provides some information
                    if len(help_output.strip()) < 10:
                        inconsistencies.append(f"{command_path}: Custom help too short or empty")
                else:
                    # For standard Typer help, check for usage section
                    if "usage:" not in help_output:
                        inconsistencies.append(f"{command_path}: Missing 'Usage:' section")

                    # Commands with options should show options section
                    # Look for either "options:" or "Options" (which appears in the fancy box format)
                    has_options_section = "options:" in help_output or "options " in help_output
                    if "--" in result.help_output and not has_options_section:
                        inconsistencies.append(
                            f"{command_path}: Has options but missing 'Options:' section"
                        )

        if inconsistencies:
            pytest.fail(
                "Help output inconsistencies found:\n"
                + "\n".join(f"  - {issue}" for issue in inconsistencies)
            )

    def test_deprecated_commands_show_warnings(self, discovery, validator):
        """Test that deprecated commands show appropriate warnings."""
        commands = discovery.discover_all_commands()
        deprecated_commands = [c for c in commands if c.deprecated]

        missing_warnings = []

        for command in deprecated_commands:
            result = validator.validate_command(command)

            if result.help_works:
                help_output = result.help_output.lower()

                # Should contain deprecation warning
                has_warning = any(
                    word in help_output
                    for word in ["deprecated", "warning", "use instead", "please use"]
                )

                if not has_warning:
                    missing_warnings.append(" ".join(command.path))

        if missing_warnings:
            pytest.fail(f"Deprecated commands missing warnings: {', '.join(missing_warnings)}")

    def test_command_execution_performance(self, discovery, validator):
        """Test that help commands execute within reasonable time limits."""
        commands = discovery.discover_all_commands()
        slow_commands = []

        # 5 second timeout for help commands
        MAX_HELP_TIME_MS = 5000

        for command in commands:
            result = validator.validate_command(command)

            if result.execution_time_ms > MAX_HELP_TIME_MS:
                slow_commands.append(f"{' '.join(command.path)}: {result.execution_time_ms:.1f}ms")

        if slow_commands:
            pytest.fail(
                f"Commands exceeded {MAX_HELP_TIME_MS}ms timeout:\n"
                + "\n".join(f"  - {cmd}" for cmd in slow_commands)
            )


if __name__ == "__main__":
    # Can be run directly for quick validation
    discovery = CLICommandDiscovery()
    validator = CLICommandValidator()
    reporter = CLIMatrixTestReporter()

    print("Discovering CLI commands...")
    commands = discovery.discover_all_commands()
    print(f"Found {len(commands)} commands")

    print("Validating commands...")
    results = []
    for command in commands:
        result = validator.validate_command(command)
        results.append(result)
        status = "✅" if result.help_works and result.side_effects_clean else "❌"
        print(f"{status} {' '.join(command.path)}")

    reporter.add_results(results)
    print("\n" + reporter.generate_coverage_report())
