#!/usr/bin/env python3
"""CI simulation runner for MarketPipe.

Runs the same test sequence as GitHub Actions CI locally.
Useful for debugging CI failures before pushing to GitHub.
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd, description, cwd):
    """Run a command and return success status."""
    print(f"\n🔄 {description}...")
    result = subprocess.run(cmd, cwd=cwd, shell=isinstance(cmd, str))

    if result.returncode == 0:
        print(f"✅ {description} - PASSED")
        return True
    else:
        print(f"❌ {description} - FAILED")
        return False


def main():
    """Run CI simulation."""

    # Get project root (parent of scripts directory)
    project_root = Path(__file__).parent.parent

    print("🚀 Running CI simulation locally...")
    print("This simulates the GitHub Actions workflow")

    success = True

    # 1. Code formatting check
    success &= run_command(
        ["black", "--check", "--diff", "src/", "tests/"],
        "Code formatting check (Black)",
        project_root
    )

    # 2. Linting
    success &= run_command(
        ["ruff", "check", "src/", "tests/"],
        "Linting (Ruff)",
        project_root
    )

    # 3. Type checking
    success &= run_command(
        ["mypy", "src/marketpipe/"],
        "Type checking (MyPy)",
        project_root
    )

    # 4. Fast tests (like pre-commit)
    success &= run_command(
        [sys.executable, "-m", "pytest", "-m", "fast", "-q"],
        "Fast tests",
        project_root
    )

    # 5. Core tests (non-integration)
    success &= run_command(
        [sys.executable, "-m", "pytest", "-m", "not integration", "--cov=marketpipe", "--timeout=60", "--maxfail=5"],
        "Core tests with coverage",
        project_root
    )

    # 6. Integration tests (lightweight)
    success &= run_command(
        [sys.executable, "-m", "pytest", "tests/integration/", "-m", "not auth_required and not slow", "--timeout=120", "--maxfail=3"],
        "Lightweight integration tests",
        project_root
    )

    # 7. CLI validation
    success &= run_command(
        f"{sys.executable} -m marketpipe --help > /dev/null",
        "CLI help validation",
        project_root
    )

    if success:
        print("\n🎉 CI simulation PASSED - Ready for push!")
        print("All checks that run in GitHub Actions have passed locally.")
        return 0
    else:
        print("\n💥 CI simulation FAILED")
        print("Fix the failing checks before pushing to GitHub.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
