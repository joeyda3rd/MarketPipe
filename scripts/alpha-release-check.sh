#!/usr/bin/env python3
"""Alpha release quality gates for MarketPipe.

Validates that MarketPipe is ready for an alpha release by checking:
- Core functionality works with fake provider
- CLI commands are properly documented
- No hardcoded credentials or secrets
- Configuration integrity
- Basic performance thresholds
- Documentation is up to date
"""

import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Tuple


class AlphaReleaseChecker:
    """Comprehensive alpha release validation."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.passed_checks = 0
        self.failed_checks = 0
        self.warnings = []
        self.project_root = Path(__file__).parent.parent

    def log(self, message: str, level: str = "INFO"):
        """Log a message with appropriate formatting."""
        if self.verbose or level in ["ERROR", "WARNING"]:
            prefix = {
                "INFO": "ℹ️",
                "SUCCESS": "✅",
                "WARNING": "⚠️",
                "ERROR": "❌",
                "DEBUG": "🔍",
            }.get(level, "•")
            print(f"{prefix} {message}")

    def run_command(
        self, cmd: List[str], cwd: Path = None, timeout: int = 30
    ) -> Tuple[bool, str, str]:
        """Run a command and return (success, stdout, stderr)."""
        try:
            result = subprocess.run(
                cmd, cwd=cwd or self.project_root, capture_output=True, text=True, timeout=timeout
            )
            return result.returncode == 0, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return False, "", f"Command timed out after {timeout}s"
        except Exception as e:
            return False, "", str(e)

    def check_cli_help_commands(self) -> bool:
        """Verify all CLI commands have proper help text."""
        self.log("Checking CLI help commands...", "DEBUG")

        commands_to_test = [
            ["marketpipe", "--help"],
            ["marketpipe", "health-check", "--help"],
            ["marketpipe", "ingest-ohlcv", "--help"],
            ["marketpipe", "metrics", "--help"],
            ["marketpipe", "validate", "--help"],
        ]

        all_passed = True
        for cmd in commands_to_test:
            success, stdout, stderr = self.run_command(cmd)
            if not success:
                self.log(f"Command '{' '.join(cmd)}' failed: {stderr}", "ERROR")
                all_passed = False
            elif len(stdout.strip()) < 50:  # Help should be substantial
                self.log(f"Command '{' '.join(cmd)}' has insufficient help text", "WARNING")
                self.warnings.append(f"Short help text for {' '.join(cmd)}")
            else:
                self.log(f"✓ {' '.join(cmd)} has proper help", "DEBUG")

        return all_passed

    def check_fake_provider_functionality(self) -> bool:
        """Test that fake provider works for basic ingestion."""
        self.log("Testing fake provider functionality...", "DEBUG")

        with tempfile.TemporaryDirectory() as temp_dir:
            # Test basic fake provider ingestion
            cmd = [
                sys.executable,
                "-m",
                "marketpipe.cli",
                "ingest-ohlcv",
                "--provider",
                "fake",
                "--symbols",
                "AAPL",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-01",
                "--output",
                temp_dir,
                "--feed-type",
                "iex",
            ]

            success, stdout, stderr = self.run_command(cmd, timeout=60)

            if not success:
                self.log(f"Fake provider ingestion failed: {stderr}", "ERROR")
                return False

            # Check that output was created
            output_path = Path(temp_dir)
            parquet_files = list(output_path.glob("**/*.parquet"))

            if not parquet_files:
                self.log("No Parquet files created by fake provider", "ERROR")
                return False

            self.log(f"✓ Fake provider created {len(parquet_files)} files", "DEBUG")
            return True

    def check_health_command(self) -> bool:
        """Verify health check command works."""
        self.log("Testing health check command...", "DEBUG")

        cmd = [sys.executable, "-m", "marketpipe.cli", "health-check", "--verbose"]
        success, stdout, stderr = self.run_command(cmd)

        if not success:
            self.log(f"Health check failed: {stderr}", "ERROR")
            return False

        # Check for expected health check indicators
        expected_checks = ["Python version", "Dependencies", "Import test"]
        for check in expected_checks:
            if check.lower() not in stdout.lower():
                self.log(f"Health check missing '{check}' validation", "WARNING")
                self.warnings.append(f"Health check incomplete: missing {check}")

        self.log("✓ Health check command works", "DEBUG")
        return True

    def check_no_hardcoded_credentials(self) -> bool:
        """Scan for potential hardcoded credentials."""
        self.log("Scanning for hardcoded credentials...", "DEBUG")

        suspicious_patterns = [
            r"['\"][A-Za-z0-9]{32,}['\"]",  # Long alphanumeric strings
            r"sk-[A-Za-z0-9]{32,}",  # API key patterns
            r"['\"]AKIA[A-Z0-9]{16}['\"]",  # AWS access keys
        ]

        issues_found = []
        src_dir = self.project_root / "src"

        for py_file in src_dir.rglob("*.py"):
            if "test" in str(py_file).lower():
                continue  # Skip test files

            try:
                content = py_file.read_text()
                for line_num, line in enumerate(content.split("\n"), 1):
                    # Skip comments and docstrings
                    if line.strip().startswith("#"):
                        continue
                    if '"""' in line or "'''" in line:
                        continue

                    # Check for suspicious patterns
                    for pattern in suspicious_patterns:
                        import re

                        if re.search(pattern, line):
                            issues_found.append(f"{py_file.name}:{line_num}: {line.strip()}")
            except Exception:
                continue

        if issues_found:
            self.log("Potential hardcoded credentials found:", "ERROR")
            for issue in issues_found:
                self.log(f"  {issue}", "ERROR")
            return False

        self.log("✓ No hardcoded credentials detected", "DEBUG")
        return True

    def check_configuration_integrity(self) -> bool:
        """Verify configuration files use proper placeholders."""
        self.log("Checking configuration file integrity...", "DEBUG")

        env_example = self.project_root / ".env.example"
        if not env_example.exists():
            self.log(".env.example file missing", "WARNING")
            self.warnings.append("Missing .env.example file")
            return True  # Not critical for alpha

        try:
            content = env_example.read_text()
            lines = content.split("\n")

            suspicious_lines = []
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                # Check for lines that don't use placeholders
                if "=" in line:
                    key, value = line.split("=", 1)
                    value = value.strip()

                    # Skip empty values or clear placeholders
                    if not value or value.startswith("<") or value in ['""', "''"]:
                        continue

                    # Flag potentially real values
                    if len(value) > 10 and not any(
                        placeholder in value.lower()
                        for placeholder in ["your_", "example", "placeholder", "xxx"]
                    ):
                        suspicious_lines.append(f"Line {line_num}: {line}")

            if suspicious_lines:
                self.log("Potentially real values in .env.example:", "WARNING")
                for line in suspicious_lines:
                    self.log(f"  {line}", "WARNING")
                self.warnings.append("Suspicious values in .env.example")

        except Exception as e:
            self.log(f"Error checking .env.example: {e}", "WARNING")
            self.warnings.append("Could not validate .env.example")

        self.log("✓ Configuration integrity checked", "DEBUG")
        return True

    def check_version_consistency(self) -> bool:
        """Verify version consistency across files."""
        self.log("Checking version consistency...", "DEBUG")

        # Check pyproject.toml version
        pyproject_toml = self.project_root / "pyproject.toml"
        if not pyproject_toml.exists():
            self.log("pyproject.toml not found", "ERROR")
            return False

        try:
            import tomllib

            with open(pyproject_toml, "rb") as f:
                pyproject_data = tomllib.load(f)
            pyproject_version = pyproject_data["project"]["version"]
        except Exception as e:
            self.log(f"Error reading pyproject.toml version: {e}", "ERROR")
            return False

        # Check __init__.py version
        init_file = self.project_root / "src" / "marketpipe" / "__init__.py"
        if init_file.exists():
            try:
                content = init_file.read_text()
                import re

                version_match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
                if version_match:
                    init_version = version_match.group(1)
                    if pyproject_version != init_version:
                        self.log(
                            f"Version mismatch: pyproject.toml={pyproject_version}, __init__.py={init_version}",
                            "ERROR",
                        )
                        return False
                    else:
                        self.log(f"✓ Version consistent: {pyproject_version}", "DEBUG")
                else:
                    self.log("No __version__ found in __init__.py", "WARNING")
                    self.warnings.append("Missing __version__ in __init__.py")
            except Exception as e:
                self.log(f"Error checking __init__.py: {e}", "WARNING")
                self.warnings.append("Could not verify __init__.py version")

        return True

    def check_basic_performance(self) -> bool:
        """Basic performance smoke test."""
        self.log("Running basic performance check...", "DEBUG")

        # Simple performance test - fake provider should be fast
        start_time = time.perf_counter()

        with tempfile.TemporaryDirectory() as temp_dir:
            cmd = [
                sys.executable,
                "-m",
                "marketpipe.cli",
                "ingest-ohlcv",
                "--provider",
                "fake",
                "--symbols",
                "AAPL,GOOGL,MSFT",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-01",
                "--output",
                temp_dir,
                "--feed-type",
                "iex",
            ]

            success, stdout, stderr = self.run_command(cmd, timeout=60)
            duration = time.perf_counter() - start_time

            if not success:
                self.log(f"Performance test failed: {stderr}", "ERROR")
                return False

            # Alpha performance threshold: should complete in under 30 seconds
            if duration > 30:
                self.log(f"Performance concern: took {duration:.2f}s (>30s threshold)", "WARNING")
                self.warnings.append(f"Slow performance: {duration:.2f}s for basic ingestion")
            else:
                self.log(f"✓ Performance acceptable: {duration:.2f}s", "DEBUG")

        return True

    def check_test_suite_health(self) -> bool:
        """Verify test suite runs and passes."""
        self.log("Checking test suite health...", "DEBUG")

        # Run fast tests only (for speed)
        cmd = [sys.executable, "-m", "pytest", "-m", "fast", "--tb=no", "-q", "--maxfail=5"]
        success, stdout, stderr = self.run_command(cmd, timeout=120)

        if not success:
            self.log(f"Fast test suite failed: {stderr}", "ERROR")
            return False

        # Parse test results
        if "failed" in stdout.lower():
            self.log("Some fast tests are failing", "WARNING")
            self.warnings.append("Test failures in fast suite")
        else:
            self.log("✓ Fast test suite passes", "DEBUG")

        return True

    def run_all_checks(self) -> bool:
        """Run all alpha release checks."""
        print("🚀 Alpha Release Quality Gates")
        print("=" * 40)
        print()

        checks = [
            ("CLI Help Commands", self.check_cli_help_commands),
            ("Fake Provider Functionality", self.check_fake_provider_functionality),
            ("Health Check Command", self.check_health_command),
            ("No Hardcoded Credentials", self.check_no_hardcoded_credentials),
            ("Configuration Integrity", self.check_configuration_integrity),
            ("Version Consistency", self.check_version_consistency),
            ("Basic Performance", self.check_basic_performance),
            ("Test Suite Health", self.check_test_suite_health),
        ]

        for check_name, check_func in checks:
            print(f"🔍 Running: {check_name}")
            try:
                if check_func():
                    print(f"✅ PASS: {check_name}")
                    self.passed_checks += 1
                else:
                    print(f"❌ FAIL: {check_name}")
                    self.failed_checks += 1
            except Exception as e:
                print(f"❌ ERROR: {check_name} - {e}")
                self.failed_checks += 1
            print()

        # Summary
        print("📊 Summary")
        print("=" * 20)
        print(f"✅ Passed: {self.passed_checks}")
        print(f"❌ Failed: {self.failed_checks}")
        print(f"⚠️  Warnings: {len(self.warnings)}")

        if self.warnings:
            print("\n⚠️  Warnings:")
            for warning in self.warnings:
                print(f"  • {warning}")

        print()

        if self.failed_checks > 0:
            print("❌ Alpha release NOT READY - fix failing checks")
            return False
        elif len(self.warnings) > 3:
            print("⚠️  Alpha release has CONCERNS - review warnings")
            return False
        else:
            print("🎉 Alpha release READY!")
            return True


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Alpha release quality gates")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--check",
        choices=["all", "security", "functionality", "performance"],
        default="all",
        help="Run specific check category",
    )

    args = parser.parse_args()

    checker = AlphaReleaseChecker(verbose=args.verbose)

    if args.check == "all":
        success = checker.run_all_checks()
    elif args.check == "security":
        success = (
            checker.check_no_hardcoded_credentials() and checker.check_configuration_integrity()
        )
    elif args.check == "functionality":
        success = (
            checker.check_cli_help_commands()
            and checker.check_fake_provider_functionality()
            and checker.check_health_command()
        )
    elif args.check == "performance":
        success = checker.check_basic_performance()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
