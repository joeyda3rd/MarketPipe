#!/usr/bin/env python3
"""
Personal smart test runner for fast development cycles.

Usage:
    python .workspace/test-runner/smart_test.py [--dry-run] [--all]
"""

import subprocess
import sys
import os
import time
from pathlib import Path
from typing import Set, List

class SmartTestRunner:
    """Fast test runner that detects relevant tests for changed files."""
    
    def __init__(self):
        self.repo_root = Path(__file__).parent.parent.parent
        os.chdir(self.repo_root)
        
        # File to test mappings for fast feedback
        self.mappings = {
            # Core modules
            "src/marketpipe/cli.py": ["tests/test_cli.py"],
            "src/marketpipe/ingestion/": ["tests/test_*ingestion*", "tests/test_coordinator*"],
            "src/marketpipe/ingestion/connectors/": ["tests/test_*client*", "tests/test_*connector*"],
            "src/marketpipe/aggregation.py": ["tests/test_aggregation*"],
            "src/marketpipe/validation.py": ["tests/test_validation*"],
            "src/marketpipe/loader.py": ["tests/test_loader*"],
            "src/marketpipe/metrics.py": ["tests/test_metrics*"],
            
            # Configuration
            "pyproject.toml": ["tests/"],  # Run all tests for config changes
            "pytest.ini": ["tests/"],
            "Makefile": [],  # Skip tests for Makefile changes
            
            # Critical files that need full test suite
            ".github/": ["tests/"],
            "schema/": ["tests/"],
        }
    
    def get_changed_files(self) -> Set[str]:
        """Get files changed since last commit."""
        try:
            result = subprocess.run(
                ["git", "diff", "--name-only", "HEAD~1"], 
                capture_output=True, text=True, check=True
            )
            files = set(result.stdout.strip().split('\n')) if result.stdout.strip() else set()
            
            # Also check unstaged changes
            result = subprocess.run(
                ["git", "diff", "--name-only"], 
                capture_output=True, text=True, check=True
            )
            if result.stdout.strip():
                files.update(result.stdout.strip().split('\n'))
            
            return files
        except subprocess.CalledProcessError:
            print("⚠️  Can't detect changed files, running all tests")
            return {"tests/"}
    
    def map_files_to_tests(self, changed_files: Set[str]) -> Set[str]:
        """Map changed files to relevant test files."""
        test_patterns = set()
        
        for file in changed_files:
            matched = False
            
            for pattern, tests in self.mappings.items():
                if file.startswith(pattern) or file.endswith(pattern):
                    test_patterns.update(tests)
                    matched = True
                    break
            
            if not matched:
                # Default: if we can't map it, run relevant tests based on path
                if file.startswith("src/marketpipe/"):
                    # Try to find corresponding test
                    module_name = Path(file).stem
                    test_patterns.add(f"tests/test_{module_name}*")
                else:
                    # Unknown file type, be safe and run more tests
                    test_patterns.add("tests/")
        
        return test_patterns
    
    def expand_test_patterns(self, patterns: Set[str]) -> List[str]:
        """Expand test patterns to actual test files."""
        test_files = []
        
        for pattern in patterns:
            if pattern == "tests/":
                # Full test suite
                return ["tests/"]
            elif "*" in pattern:
                # Glob pattern
                matches = list(Path(".").glob(pattern))
                test_files.extend(str(m) for m in matches if m.exists())
            else:
                # Specific file
                if Path(pattern).exists():
                    test_files.append(pattern)
        
        return test_files or ["tests/"]
    
    def run_tests(self, test_targets: List[str], dry_run: bool = False) -> bool:
        """Run tests with smart options."""
        if test_targets == ["tests/"]:
            cmd = ["pytest", "--tb=short", "-q"]
            scope = "all tests"
        else:
            cmd = ["pytest", "-x", "--ff", "--tb=short"] + test_targets
            scope = f"{len(test_targets)} test files"
        
        print(f"🧪 Running {scope}")
        if test_targets != ["tests/"]:
            print(f"   Targets: {', '.join(test_targets)}")
        
        if dry_run:
            print(f"   Command: {' '.join(cmd)}")
            return True
        
        start_time = time.time()
        try:
            result = subprocess.run(cmd, check=True)
            elapsed = time.time() - start_time
            print(f"✅ Tests passed in {elapsed:.1f}s")
            return True
        except subprocess.CalledProcessError:
            elapsed = time.time() - start_time
            print(f"❌ Tests failed in {elapsed:.1f}s")
            return False

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Smart test runner for fast development")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    args = parser.parse_args()
    
    runner = SmartTestRunner()
    
    if args.all:
        success = runner.run_tests(["tests/"], dry_run=args.dry_run)
    else:
        changed_files = runner.get_changed_files()
        if not changed_files:
            print("No changed files detected, running minimal test set")
            success = runner.run_tests(["tests/test_cli.py"], dry_run=args.dry_run)
        else:
            print(f"📁 Changed files: {', '.join(sorted(changed_files))}")
            
            test_patterns = runner.map_files_to_tests(changed_files)
            test_files = runner.expand_test_patterns(test_patterns)
            
            success = runner.run_tests(test_files, dry_run=args.dry_run)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 