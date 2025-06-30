#!/usr/bin/env python3
"""
Advanced Intelligent Test System for MarketPipe

This is the full-featured intelligent testing system with database tracking,
flaky test detection, performance profiling, and advanced analytics.

This version is kept in the workspace to avoid cluttering the main repository
while still providing advanced capabilities for power users.
"""

import argparse
import os
import subprocess
import sys
import sqlite3
import json
import time
import re
from pathlib import Path
from typing import List, Set, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta


@dataclass
class TestRunSummary:
    """Summary of test run results."""
    total_tests: int
    passed: int
    failed: int
    skipped: int
    duration_seconds: float
    test_files: List[str]
    failed_tests: List[str]
    mode: str
    timestamp: str
    git_branch: str
    git_commit: str


class SimpleTestRunner:
    """Simple test runner for intelligent system."""
    
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        
        # File-to-test mappings
        self.file_patterns = {
            # Core source files
            r'src/marketpipe/cli\.py': ['tests/test_cli.py'],
            r'src/marketpipe/main\.py': ['tests/test_cli.py'],
            r'src/marketpipe/metrics\.py': ['tests/test_metrics.py'],
            r'src/marketpipe/validation\.py': ['tests/test_validation.py'],
            r'src/marketpipe/aggregation\.py': ['tests/test_aggregation.py'],
            r'src/marketpipe/loader\.py': ['tests/test_loader.py'],
            
            # Ingestion module
            r'src/marketpipe/ingestion/coordinator\.py': ['tests/test_coordinator_flow.py'],
            r'src/marketpipe/ingestion/.*\.py': ['tests/test_coordinator_flow.py'],
            
            # Connectors
            r'src/marketpipe/ingestion/connectors/base_api_client\.py': ['tests/test_base_client.py'],
            r'src/marketpipe/ingestion/connectors/alpaca_client\.py': ['tests/test_alpaca_client.py'],
            r'src/marketpipe/ingestion/connectors/.*\.py': ['tests/test_base_client.py', 'tests/test_alpaca_client.py'],
            
            # Configuration
            r'config/.*\.yaml': ['tests/test_config.py'],
            r'pyproject\.toml': ['tests/'],  # Run all tests for build config changes
            r'pytest\.ini': ['tests/'],      # Run all tests for pytest config changes
            
            # Test files themselves
            r'tests/.*\.py': ['tests/'],     # Run all tests if test files change
        }
    
    def get_relevant_tests(self) -> List[str]:
        """Get list of test files relevant to current changes."""
        try:
            # Get changed files
            result = subprocess.run(
                ['git', 'diff', '--name-only', 'HEAD~1', 'HEAD'],
                capture_output=True, text=True, cwd=self.repo_root
            )
            
            if result.returncode != 0:
                # Fallback to staged changes
                result = subprocess.run(
                    ['git', 'diff', '--name-only', '--cached'],
                    capture_output=True, text=True, cwd=self.repo_root
                )
            
            if result.returncode != 0:
                # Fallback to working directory changes
                result = subprocess.run(
                    ['git', 'diff', '--name-only'],
                    capture_output=True, text=True, cwd=self.repo_root
                )
            
            changed_files = result.stdout.strip().split('\n') if result.stdout.strip() else []
            
            if not changed_files:
                # No changes detected, run fast subset
                return self._get_fast_test_subset()
            
            # Map changed files to test files
            relevant_tests = set()
            for changed_file in changed_files:
                if not changed_file:
                    continue
                    
                matched = False
                for pattern, test_files in self.file_patterns.items():
                    if re.match(pattern, changed_file):
                        for test_file in test_files:
                            if test_file == 'tests/':
                                # Run all tests
                                return ['tests/']
                            elif (self.repo_root / test_file).exists():
                                relevant_tests.add(test_file)
                        matched = True
                        break
                
                if not matched:
                    # Unmatched file - run fast subset as safety net
                    relevant_tests.update(self._get_fast_test_subset())
            
            return list(relevant_tests) if relevant_tests else self._get_fast_test_subset()
            
        except Exception as e:
            print(f"Warning: Could not detect changed files: {e}")
            return self._get_fast_test_subset()
    
    def _get_fast_test_subset(self) -> List[str]:
        """Get a fast subset of tests for default execution."""
        fast_tests = []
        test_files = [
            'tests/test_cli.py',
            'tests/test_base_client.py',
            'tests/test_alpaca_client.py',
            'tests/test_metrics.py',
        ]
        
        for test_file in test_files:
            if (self.repo_root / test_file).exists():
                fast_tests.append(test_file)
        
        return fast_tests if fast_tests else ['tests/']


class IntelligentTestDatabase:
    """SQLite database for tracking test performance and flaky tests."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS test_runs (
                    id INTEGER PRIMARY KEY,
                    timestamp TEXT,
                    git_branch TEXT,
                    git_commit TEXT,
                    mode TEXT,
                    total_tests INTEGER,
                    passed INTEGER,
                    failed INTEGER,
                    skipped INTEGER,
                    duration_seconds REAL,
                    test_files TEXT,  -- JSON array
                    failed_tests TEXT  -- JSON array
                );
                
                CREATE TABLE IF NOT EXISTS test_performance (
                    id INTEGER PRIMARY KEY,
                    test_name TEXT,
                    run_id INTEGER,
                    duration_seconds REAL,
                    status TEXT,  -- passed, failed, skipped
                    FOREIGN KEY (run_id) REFERENCES test_runs (id)
                );
                
                CREATE TABLE IF NOT EXISTS flaky_tests (
                    test_name TEXT PRIMARY KEY,
                    failure_count INTEGER,
                    total_runs INTEGER,
                    last_failure TEXT,
                    quarantined BOOLEAN DEFAULT FALSE
                );
                
                CREATE INDEX IF NOT EXISTS idx_test_name ON test_performance(test_name);
                CREATE INDEX IF NOT EXISTS idx_run_timestamp ON test_runs(timestamp);
            """)
    
    def record_test_run(self, summary: TestRunSummary) -> int:
        """Record a test run and return the run ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO test_runs 
                (timestamp, git_branch, git_commit, mode, total_tests, passed, failed, skipped, 
                 duration_seconds, test_files, failed_tests)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                summary.timestamp, summary.git_branch, summary.git_commit, summary.mode,
                summary.total_tests, summary.passed, summary.failed, summary.skipped,
                summary.duration_seconds, json.dumps(summary.test_files), 
                json.dumps(summary.failed_tests)
            ))
            return cursor.lastrowid
    
    def update_flaky_tests(self, failed_tests: List[str]):
        """Update flaky test tracking."""
        with sqlite3.connect(self.db_path) as conn:
            for test_name in failed_tests:
                # Check if test exists
                cursor = conn.execute("SELECT failure_count, total_runs FROM flaky_tests WHERE test_name = ?", (test_name,))
                result = cursor.fetchone()
                
                if result:
                    # Update existing record
                    failure_count, total_runs = result
                    conn.execute("""
                        UPDATE flaky_tests 
                        SET failure_count = ?, total_runs = ?, last_failure = ?
                        WHERE test_name = ?
                    """, (failure_count + 1, total_runs + 1, datetime.now().isoformat(), test_name))
                else:
                    # Insert new record
                    conn.execute("""
                        INSERT INTO flaky_tests (test_name, failure_count, total_runs, last_failure)
                        VALUES (?, 1, 1, ?)
                    """, (test_name, datetime.now().isoformat()))
    
    def get_flaky_tests(self, min_failure_rate: float = 0.3) -> List[Dict]:
        """Get tests that fail frequently."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT test_name, failure_count, total_runs, last_failure, quarantined
                FROM flaky_tests
                WHERE CAST(failure_count AS FLOAT) / total_runs >= ?
                ORDER BY CAST(failure_count AS FLOAT) / total_runs DESC
            """, (min_failure_rate,))
            
            return [
                {
                    'test_name': row[0],
                    'failure_count': row[1],
                    'total_runs': row[2],
                    'failure_rate': row[1] / row[2],
                    'last_failure': row[3],
                    'quarantined': bool(row[4])
                }
                for row in cursor.fetchall()
            ]


class IntelligentTestRunner:
    """Advanced test runner with intelligent features."""
    
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.workspace_dir = repo_root / ".workspace"
        self.artifacts_dir = self.workspace_dir / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        
        # Database for tracking
        self.db = IntelligentTestDatabase(self.artifacts_dir / "intelligent_test.db")
        
        # Base test runner
        self.test_runner = SimpleTestRunner(repo_root)
    
    def run_intelligent_tests(self, mode: str = "fast", dry_run: bool = False) -> TestRunSummary:
        """Run tests with intelligent selection and tracking."""
        print(f"🧠 Intelligent test system - {mode} mode")
        
        # Get test selection from base runner
        test_files = self.test_runner.get_relevant_tests()
        
        if dry_run:
            print(f"📋 Would run {len(test_files)} test files:")
            for test_file in test_files:
                print(f"  📄 {test_file}")
            return self._create_dry_run_summary(test_files, mode)
        
        # Build pytest command
        pytest_cmd = self._build_intelligent_pytest_cmd(test_files, mode)
        
        print(f"🚀 Running: {' '.join(pytest_cmd)}")
        start_time = time.time()
        
        # Run tests
        result = subprocess.run(pytest_cmd, capture_output=True, text=True, cwd=self.repo_root)
        duration = time.time() - start_time
        
        # Parse results
        summary = self._parse_test_results(result, test_files, mode, duration)
        
        # Record in database
        run_id = self.db.record_test_run(summary)
        
        # Update flaky test tracking
        if summary.failed_tests:
            self.db.update_flaky_tests(summary.failed_tests)
        
        # Display results
        self._display_results(summary)
        
        return summary
    
    def _build_intelligent_pytest_cmd(self, test_files: List[str], mode: str) -> List[str]:
        """Build pytest command with intelligent options."""
        cmd = ["python", "-m", "pytest"]
        
        if test_files:
            cmd.extend(test_files)
        
        if mode == "fast":
            cmd.extend(["-q", "--tb=short", "--maxfail=3", "-x"])
            # Add parallel execution if safe
            if self._can_run_parallel(test_files):
                cmd.extend(["-n", "auto"])
        elif mode == "thorough":
            cmd.extend(["-v", "--tb=long"])
        
        return cmd
    
    def _can_run_parallel(self, test_files: List[str]) -> bool:
        """Determine if tests can run in parallel safely."""
        # Simple heuristic: avoid parallel for integration tests
        return not any("integration" in tf for tf in test_files)
    
    def _parse_test_results(self, result: subprocess.CompletedProcess, test_files: List[str], 
                          mode: str, duration: float) -> TestRunSummary:
        """Parse pytest output into a structured summary."""
        # Extract git info
        try:
            git_branch = subprocess.run(["git", "branch", "--show-current"], 
                                       capture_output=True, text=True, cwd=self.repo_root).stdout.strip()
            git_commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], 
                                       capture_output=True, text=True, cwd=self.repo_root).stdout.strip()
        except:
            git_branch = "unknown"
            git_commit = "unknown"
        
        # Parse pytest output (simplified)
        output = result.stdout + result.stderr
        
        # Extract basic counts (this is a simplified parser)
        passed = len(re.findall(r'PASSED', output))
        failed = len(re.findall(r'FAILED', output))
        skipped = len(re.findall(r'SKIPPED', output))
        
        # Extract failed test names (simplified)
        failed_tests = re.findall(r'FAILED (.*?) -', output)
        
        return TestRunSummary(
            total_tests=passed + failed + skipped,
            passed=passed,
            failed=failed,
            skipped=skipped,
            duration_seconds=duration,
            test_files=test_files,
            failed_tests=failed_tests,
            mode=mode,
            timestamp=datetime.now().isoformat(),
            git_branch=git_branch,
            git_commit=git_commit
        )
    
    def _create_dry_run_summary(self, test_files: List[str], mode: str) -> TestRunSummary:
        """Create a summary for dry run."""
        return TestRunSummary(
            total_tests=0,
            passed=0,
            failed=0,
            skipped=0,
            duration_seconds=0.0,
            test_files=test_files,
            failed_tests=[],
            mode=f"{mode} (dry-run)",
            timestamp=datetime.now().isoformat(),
            git_branch="dry-run",
            git_commit="dry-run"
        )
    
    def _display_results(self, summary: TestRunSummary):
        """Display test results."""
        print(f"\n📊 Test Results ({summary.mode}):")
        print(f"  ✅ Passed: {summary.passed}")
        print(f"  ❌ Failed: {summary.failed}")
        print(f"  ⏭️  Skipped: {summary.skipped}")
        print(f"  ⏱️  Duration: {summary.duration_seconds:.2f}s")
        
        if summary.failed_tests:
            print(f"\n❌ Failed tests:")
            for test in summary.failed_tests:
                print(f"  📄 {test}")
    
    def get_flaky_tests(self) -> List[Dict]:
        """Get list of flaky tests."""
        return self.db.get_flaky_tests()
    
    def diagnose_environment(self):
        """Diagnose test environment."""
        print("🔍 Environment Diagnosis:")
        
        # Python version
        try:
            python_version = subprocess.run(["python", "--version"], 
                                           capture_output=True, text=True).stdout.strip()
            print(f"  🐍 {python_version}")
        except:
            print("  🐍 Python: Error getting version")
        
        # Virtual environment
        venv = os.environ.get('VIRTUAL_ENV', 'Not activated')
        if venv != 'Not activated':
            venv = Path(venv).name
        print(f"  📦 Virtual env: {venv}")
        
        # Git status
        try:
            git_status = subprocess.run(["git", "status", "--porcelain"], 
                                       capture_output=True, text=True, cwd=self.repo_root).stdout.strip()
            dirty_files = len(git_status.split('\n')) if git_status else 0
            print(f"  📝 Git status: {dirty_files} changed files")
        except:
            print("  📝 Git status: Error getting status")
        
        # Pytest availability
        try:
            import pytest
            print(f"  🧪 pytest: {pytest.__version__}")
        except ImportError:
            print("  🧪 pytest: ❌ Not available")
        
        # Test discovery
        tests_dir = self.repo_root / "tests"
        if tests_dir.exists():
            test_files = list(tests_dir.glob("test_*.py"))
            print(f"  📂 Test files found: {len(test_files)}")
        else:
            print("  📂 Tests directory: ❌ Not found")


def main():
    """Main entry point for intelligent test system."""
    parser = argparse.ArgumentParser(description="Intelligent Test System for MarketPipe")
    parser.add_argument("--mode", choices=["fast", "thorough"], default="fast",
                      help="Test execution mode")
    parser.add_argument("--dry-run", action="store_true",
                      help="Show what would be tested without running")
    parser.add_argument("--diagnose", action="store_true",
                      help="Diagnose test environment")
    parser.add_argument("--flaky-alert", action="store_true",
                      help="Show flaky test alerts")
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent.parent
    runner = IntelligentTestRunner(repo_root)
    
    if args.diagnose:
        runner.diagnose_environment()
        return
    
    if args.flaky_alert:
        flaky_tests = runner.get_flaky_tests()
        if flaky_tests:
            print("🚨 Flaky Test Alert:")
            for test in flaky_tests[:5]:  # Show top 5
                print(f"  📄 {test['test_name']} - {test['failure_rate']:.1%} failure rate")
        else:
            print("✅ No flaky tests detected")
        return
    
    # Run tests
    summary = runner.run_intelligent_tests(mode=args.mode, dry_run=args.dry_run)
    
    # Exit with appropriate code
    sys.exit(0 if summary.failed == 0 else 1)


if __name__ == "__main__":
    main() 