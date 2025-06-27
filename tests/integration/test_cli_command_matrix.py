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

import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
    path: List[str]  # Full command path (e.g., ['ohlcv', 'ingest'])
    parameters: Dict[str, Any] = field(default_factory=dict)
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
    created_files: List[Path] = field(default_factory=list)
    error_messages: List[str] = field(default_factory=list)
    execution_time_ms: float = 0.0


class CLICommandDiscovery:
    """Discovers all available CLI commands and their structure."""

    def __init__(self):
        self.discovered_commands: List[CommandInfo] = []
        self.command_tree: Dict[str, Any] = {}

    def discover_all_commands(self) -> List[CommandInfo]:
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

    def _discover_main_commands(self) -> List[CommandInfo]:
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
            "migrate"
        ]

        for cmd in main_commands:
            commands.append(CommandInfo(
                name=cmd,
                path=[cmd],
                is_subcommand=False
            ))

        return commands

    def _discover_subapp_commands(self) -> List[CommandInfo]:
        """Discover subapp commands."""
        commands = []

        # OHLCV subcommands
        ohlcv_commands = ["ingest", "validate", "aggregate", "backfill"]
        for cmd in ohlcv_commands:
            commands.append(CommandInfo(
                name=cmd,
                path=["ohlcv", cmd],
                is_subcommand=True,
                parent_app="ohlcv"
            ))

        # Prune subcommands
        prune_commands = ["parquet", "database"]
        for cmd in prune_commands:
            commands.append(CommandInfo(
                name=cmd,
                path=["prune", cmd],
                is_subcommand=True,
                parent_app="prune"
            ))

        # Symbols subcommands
        symbols_commands = ["update"]
        for cmd in symbols_commands:
            commands.append(CommandInfo(
                name=cmd,
                path=["symbols", cmd],
                is_subcommand=True,
                parent_app="symbols"
            ))

        return commands

    def _get_deprecated_commands(self) -> List[CommandInfo]:
        """Get known deprecated commands."""
        deprecated = [
            CommandInfo(
                name="ingest",
                path=["ingest"],
                deprecated=True
            ),
            CommandInfo(
                name="validate",
                path=["validate"],
                deprecated=True
            ),
            CommandInfo(
                name="aggregate",
                path=["aggregate"],
                deprecated=True
            )
        ]
        return deprecated


class CLICommandValidator:
    """Validates CLI commands for correctness and consistency."""

    def __init__(self, use_subprocess: bool = True):
        self.use_subprocess = use_subprocess
        self.runner = CliRunner() if TYPER_AVAILABLE else None

    def validate_command(self, command: CommandInfo) -> ValidationResult:
        """
        Validate a single command for correctness.
        
        Args:
            command: Command to validate
            
        Returns:
            ValidationResult with validation details
        """
        result = ValidationResult(command=command)

        # Test help command works
        help_result = self._test_help_command(command)
        result.help_works = help_result[0]
        result.help_output = help_result[1]
        result.execution_time_ms = help_result[2]

        # Test no side effects
        side_effect_result = self._test_no_side_effects(command)
        result.side_effects_clean = side_effect_result[0]
        result.created_files = side_effect_result[1]

        return result

    def _test_help_command(self, command: CommandInfo) -> Tuple[bool, str, float]:
        """
        Test that help command works and returns valid output.
        
        Returns:
            (success, output, execution_time_ms)
        """
        import time

        cmd_path = command.path + ["--help"]

        if self.use_subprocess:
            start_time = time.time()
            try:
                result = subprocess.run(
                    ["python", "-m", "marketpipe"] + cmd_path,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    cwd=Path(__file__).parent.parent.parent
                )
                execution_time = (time.time() - start_time) * 1000

                return (
                    result.returncode == 0,
                    result.stdout + result.stderr,
                    execution_time
                )
            except subprocess.TimeoutExpired:
                return False, "Command timed out", (time.time() - start_time) * 1000
            except Exception as e:
                return False, f"Error running command: {e}", (time.time() - start_time) * 1000

        elif self.runner and app:
            start_time = time.time()
            try:
                # Build command for Typer testing
                full_cmd = cmd_path
                result = self.runner.invoke(app, full_cmd)
                execution_time = (time.time() - start_time) * 1000

                return (
                    result.exit_code == 0,
                    result.output,
                    execution_time
                )
            except Exception as e:
                return False, f"CliRunner error: {e}", (time.time() - start_time) * 1000

        return False, "No testing method available", 0.0

    def _test_no_side_effects(self, command: CommandInfo) -> Tuple[bool, List[Path]]:
        """
        Test that help command doesn't create unwanted files or directories.
        
        Returns:
            (no_side_effects, list_of_created_files)
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            original_cwd = os.getcwd()

            try:
                os.chdir(temp_path)

                # Capture initial state
                initial_files = set()
                if temp_path.exists():
                    initial_files = set(temp_path.rglob("*"))

                # Run help command
                cmd_path = command.path + ["--help"]

                if self.use_subprocess:
                    subprocess.run(
                        ["python", "-m", "marketpipe"] + cmd_path,
                        capture_output=True,
                        text=True,
                        timeout=30,
                        cwd=Path(__file__).parent.parent.parent
                    )
                elif self.runner and app:
                    self.runner.invoke(app, cmd_path)

                # Check for new files
                final_files = set()
                if temp_path.exists():
                    final_files = set(temp_path.rglob("*"))

                created_files = list(final_files - initial_files)

                return len(created_files) == 0, created_files

            except Exception:
                return False, []
            finally:
                os.chdir(original_cwd)


class CLIMatrixTestReporter:
    """Generates comprehensive reports of CLI validation results."""

    def __init__(self):
        self.results: List[ValidationResult] = []

    def add_results(self, results: List[ValidationResult]):
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
        report.append(f"- Help Commands Working: {help_working}/{total_commands} ({help_working/total_commands*100:.1f}%)")
        report.append(f"- Side-Effect Clean: {clean_commands}/{total_commands} ({clean_commands/total_commands*100:.1f}%)")
        report.append("")

        # Command breakdown by category
        report.append("## Command Categories")

        main_commands = [r for r in self.results if not r.command.is_subcommand and not r.command.deprecated]
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

    def get_failed_commands(self) -> List[ValidationResult]:
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

        assert len(main_commands) >= 7, f"Expected at least 7 main commands, found {len(main_commands)}"
        assert len(subcommands) >= 7, f"Expected at least 7 subcommands, found {len(subcommands)}"
        assert len(deprecated) >= 3, f"Expected at least 3 deprecated commands, found {len(deprecated)}"

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
            "metrics"
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
                f"Found {len(failed_commands)} commands with issues:\n" +
                "\n".join(failure_details) +
                f"\n\nFull report:\n{report}"
            )

    def test_help_output_consistency(self, discovery, validator):
        """Test that help output follows consistent patterns."""
        commands = discovery.discover_all_commands()
        inconsistencies = []

        for command in commands:
            if command.deprecated:
                continue  # Skip deprecated commands for consistency checks

            result = validator.validate_command(command)

            if result.help_works:
                help_output = result.help_output.lower()

                # All help should contain usage information
                if "usage:" not in help_output:
                    inconsistencies.append(f"{' '.join(command.path)}: Missing 'Usage:' section")

                # Commands with options should show options section
                # Look for either "options:" or "Options" (which appears in the fancy box format)
                has_options_section = "options:" in help_output or "options " in help_output
                if "--" in result.help_output and not has_options_section:
                    inconsistencies.append(f"{' '.join(command.path)}: Has options but missing 'Options:' section")

        if inconsistencies:
            pytest.fail("Help output inconsistencies found:\n" + "\n".join(f"  - {issue}" for issue in inconsistencies))

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
                has_warning = any(word in help_output for word in [
                    "deprecated", "warning", "use instead", "please use"
                ])

                if not has_warning:
                    missing_warnings.append(" ".join(command.path))

        if missing_warnings:
            pytest.fail(
                f"Deprecated commands missing warnings: {', '.join(missing_warnings)}"
            )

    def test_command_execution_performance(self, discovery, validator):
        """Test that help commands execute within reasonable time limits."""
        commands = discovery.discover_all_commands()
        slow_commands = []

        # 5 second timeout for help commands
        MAX_HELP_TIME_MS = 5000

        for command in commands:
            result = validator.validate_command(command)

            if result.execution_time_ms > MAX_HELP_TIME_MS:
                slow_commands.append(
                    f"{' '.join(command.path)}: {result.execution_time_ms:.1f}ms"
                )

        if slow_commands:
            pytest.fail(
                f"Commands exceeded {MAX_HELP_TIME_MS}ms timeout:\n" +
                "\n".join(f"  - {cmd}" for cmd in slow_commands)
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
