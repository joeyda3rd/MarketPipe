#!/usr/bin/env python
"""Smart test runner for pre-commit hooks.

Runs only fast unit tests to provide quick feedback during development.
For comprehensive testing, use `make test` or `pytest` directly.
"""

import sys
import subprocess
from pathlib import Path


def main():
    """Run fast tests suitable for pre-commit validation."""
    
    # Change to project root
    project_root = Path(__file__).parent.parent.parent
    
    # Define fast test paths (core unit tests only)
    fast_test_paths = [
        "tests/test_base_client.py",           # Key API client tests
        "tests/unit/test_main.py",             # Main module tests
        "tests/unit/test_ddd_guard_rails.py",  # DDD validation tests
        "tests/unit/domain/",                  # Domain logic tests
        "tests/unit/config/",                  # Configuration tests
    ]
    
    try:
        # Run fast tests with optimized settings for pre-commit
        cmd = [
            sys.executable, "-m", "pytest",
            *fast_test_paths,
            "-x",                    # Stop on first failure
            "--tb=line",             # Very short traceback format
            "-q",                    # Quiet output
            "--disable-warnings",    # Suppress warnings for cleaner output
            "--maxfail=1",          # Stop after first failure for speed
            "--no-cov",             # Disable coverage for speed
            # Skip async tests to reduce overhead
            "-k", "not asyncio",    
        ]
        
        print("🧪 Running fast tests for pre-commit...")
        result = subprocess.run(cmd, cwd=project_root)
        
        if result.returncode == 0:
            print("✅ Fast tests passed!")
            return 0
        else:
            print("❌ Fast tests failed!")
            return result.returncode
            
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 