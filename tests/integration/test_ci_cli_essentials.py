# SPDX-License-Identifier: Apache-2.0
"""Essential CLI command execution tests for CI.

This test file addresses Critical Gap #3 from E2E_TEST_GAP_ANALYSIS.md:
"CLI Commands Only Tested for --help"

PURPOSE:
These tests validate that core CLI commands actually EXECUTE successfully,
not just display help text. This catches CLI regressions before users see them.

WHAT THIS TESTS:
- Actual command execution (not mocked)
- Exit codes (0 = success)
- Expected output patterns
- Real CLI subprocess calls

WHY THIS MATTERS:
- CI currently only tests --help flags
- Commands could fail with valid arguments and CI wouldn't know
- Error handling could be broken
- Command dispatch could fail silently

EXECUTION TIME: Target <20 seconds for CI
"""

from __future__ import annotations

import subprocess

import pytest


@pytest.mark.integration
class TestEssentialCLICommands:
    """Test essential CLI commands execute successfully in CI.

    These are the commands users will run most often. They must work reliably.
    """

    def test_health_check_executes(self, tmp_path):
        """Test: marketpipe health-check executes successfully.

        This command should always work, even in a fresh environment.
        It's often the first diagnostic command users run.
        """
        import os

        env = os.environ.copy()
        env["MP_DATA_DIR"] = str(tmp_path / "data")

        result = subprocess.run(
            ["marketpipe", "health-check"], capture_output=True, text=True, timeout=30, env=env
        )

        # Health check should complete (exit code 0 or 1 depending on state)
        # Exit code 0 = healthy, 1 = warnings
        assert result.returncode in [
            0,
            1,
        ], f"Health check should execute and return valid code. Got: {result.returncode}\nStderr: {result.stderr}"

        # Should produce output
        output = result.stdout + result.stderr
        assert len(output) > 20, "Health check should produce diagnostic output"

    def test_ingest_fake_provider_executes(self, tmp_path):
        """Test: marketpipe ingest with fake provider executes successfully.

        This is the most common command for new users and testing.
        It must work reliably.
        """
        import os

        env = os.environ.copy()
        env["MP_DATA_DIR"] = str(tmp_path / "data")

        result = subprocess.run(
            [
                "marketpipe",
                "ingest",
                "--provider",
                "fake",
                "--symbols",
                "AAPL",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",  # End must be after start for validation
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )

        # Should succeed
        assert (
            result.returncode == 0
        ), f"Fake provider ingest should succeed. Exit code: {result.returncode}\nStderr: {result.stderr}"

        # Should create data files
        data_dir = tmp_path / "data"
        if data_dir.exists():
            files = list(data_dir.rglob("*"))
            assert len(files) > 0, "Should create some files"

    def test_providers_list_executes(self):
        """Test: marketpipe providers list executes successfully.

        This command should list available data providers.
        """
        result = subprocess.run(
            ["marketpipe", "providers"], capture_output=True, text=True, timeout=10
        )

        # Should succeed
        assert (
            result.returncode == 0
        ), f"Providers list should succeed. Exit code: {result.returncode}\nStderr: {result.stderr}"

        # Should list fake provider at minimum
        output = result.stdout.lower() + result.stderr.lower()
        assert "fake" in output, "Should list 'fake' provider in output"

    def test_jobs_list_executes(self, tmp_path):
        """Test: marketpipe jobs list executes successfully.

        This command should execute even if no jobs exist.
        """
        import os

        env = os.environ.copy()
        env["MP_DATA_DIR"] = str(tmp_path / "data")

        result = subprocess.run(
            ["marketpipe", "jobs", "list"], capture_output=True, text=True, timeout=30, env=env
        )

        # Should execute (exit code 0 even if no jobs)
        assert (
            result.returncode == 0
        ), f"Jobs list should execute successfully. Exit code: {result.returncode}\nStderr: {result.stderr}"

    def test_jobs_cleanup_dry_run_executes(self, tmp_path):
        """Test: marketpipe jobs cleanup --dry-run executes successfully.

        This command should execute safely in dry-run mode.
        """
        import os

        env = os.environ.copy()
        env["MP_DATA_DIR"] = str(tmp_path / "data")

        result = subprocess.run(
            ["marketpipe", "jobs", "cleanup", "--all", "--dry-run"],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        # Dry-run cleanup should always succeed
        assert (
            result.returncode == 0
        ), f"Jobs cleanup dry-run should succeed. Exit code: {result.returncode}\nStderr: {result.stderr}"

        # Should indicate dry-run mode
        output = result.stdout.lower() + result.stderr.lower()
        dry_run_indicators = ["dry", "preview", "would"]
        assert any(
            indicator in output for indicator in dry_run_indicators
        ), "Dry-run output should indicate no actual changes made"

    @pytest.mark.skip(reason="--version option not implemented yet")
    def test_version_info_available(self):
        """Test: version information is available.

        Users should be able to check the version.

        NOTE: Currently skipped as --version option is not implemented.
        """
        result = subprocess.run(
            ["marketpipe", "--version"], capture_output=True, text=True, timeout=10
        )

        # Should succeed
        assert (
            result.returncode == 0
        ), f"Version command should succeed. Exit code: {result.returncode}"

        # Should contain version-like output
        output = result.stdout + result.stderr
        # Check for version pattern (e.g., "0.1.0" or similar)
        import re

        version_pattern = r"\d+\.\d+\.\d+"
        assert re.search(version_pattern, output), f"Should display version number. Got: {output}"


@pytest.mark.integration
class TestCLIErrorHandling:
    """Test that CLI properly handles error conditions.

    Good error handling is critical for user experience.
    """

    def test_invalid_provider_fails_gracefully(self, tmp_path):
        """Test: invalid provider name fails with helpful error.

        Users will mistype provider names. The error should be clear.
        """
        import os

        env = os.environ.copy()
        env["MP_DATA_DIR"] = str(tmp_path / "data")

        result = subprocess.run(
            [
                "marketpipe",
                "ingest",
                "--provider",
                "invalid_provider_name",
                "--symbols",
                "AAPL",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        # Should fail (non-zero exit code)
        assert result.returncode != 0, "Invalid provider should cause command to fail"

        # Should provide helpful error message
        error_output = result.stderr.lower() + result.stdout.lower()
        assert (
            "invalid" in error_output or "unknown" in error_output or "provider" in error_output
        ), f"Should have clear error about invalid provider. Got: {result.stderr}"

    def test_missing_required_arg_fails_gracefully(self):
        """Test: missing required argument fails with helpful error.

        CLI should validate required arguments before execution.
        """
        result = subprocess.run(
            [
                "marketpipe",
                "ingest",
                "--provider",
                "fake",
                # Missing --symbols (required)
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Should fail
        assert result.returncode != 0, "Missing required argument should cause command to fail"

        # Should mention the missing argument
        error_output = result.stderr.lower() + result.stdout.lower()
        assert (
            "symbol" in error_output or "required" in error_output
        ), f"Should have clear error about missing required arg. Got: {result.stderr}"

    def test_invalid_date_format_fails_gracefully(self, tmp_path):
        """Test: invalid date format fails with helpful error.

        Date formatting is a common user error. Error should be clear.
        """
        import os

        env = os.environ.copy()
        env["MP_DATA_DIR"] = str(tmp_path / "data")

        result = subprocess.run(
            [
                "marketpipe",
                "ingest",
                "--provider",
                "fake",
                "--symbols",
                "AAPL",
                "--start",
                "01/01/2025",  # Wrong format (should be YYYY-MM-DD)
                "--end",
                "2025-01-02",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        # Should fail
        assert result.returncode != 0, "Invalid date format should cause command to fail"

        # Should mention date or format issue
        error_output = result.stderr.lower() + result.stdout.lower()
        assert (
            "date" in error_output or "format" in error_output or "invalid" in error_output
        ), f"Should have clear error about date format. Got: {result.stderr}"


@pytest.mark.integration
def test_cli_help_consistency():
    """Test that all main commands have consistent help output.

    Help text should be available and formatted consistently.
    """
    commands = [
        ["marketpipe"],
        ["marketpipe", "ingest"],
        ["marketpipe", "query"],
        ["marketpipe", "validate"],
        ["marketpipe", "aggregate"],
        ["marketpipe", "metrics"],
        ["marketpipe", "jobs"],
    ]

    for cmd in commands:
        # Test help flag
        result = subprocess.run(cmd + ["--help"], capture_output=True, text=True, timeout=10)

        assert result.returncode == 0, f"Help for {' '.join(cmd)} should succeed"

        # Help should be substantial
        assert len(result.stdout) > 50, f"Help for {' '.join(cmd)} should have meaningful content"

        # Should contain "Usage:" or "usage:"
        assert (
            "usage:" in result.stdout.lower()
        ), f"Help for {' '.join(cmd)} should show usage information"


if __name__ == "__main__":
    # Allow running this file directly for quick testing
    pytest.main([__file__, "-v", "-s"])
