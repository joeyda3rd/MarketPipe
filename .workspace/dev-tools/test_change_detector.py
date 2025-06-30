#!/usr/bin/env python3
"""
Test Change Detector - MarketPipe

Analyzes git changes and automatically determines which tests should be run.
Prevents missed tests by mapping code changes to relevant test files.
"""

from __future__ import annotations

import argparse
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from enum import Enum


class TestType(Enum):
    """Types of tests that can be run."""
    UNIT = "unit"
    INTEGRATION = "integration" 
    SLOW = "slow"
    ALL = "all"


@dataclass
class TestSuggestion:
    """A suggested test to run based on code changes."""
    test_path: str
    reason: str
    priority: int  # 1 = high, 2 = medium, 3 = low
    test_type: TestType
    estimated_duration: str


@dataclass
class ChangeAnalysis:
    """Analysis of what changed and what tests to run."""
    changed_files: List[str]
    test_suggestions: List[TestSuggestion]
    coverage_impact: Dict[str, float]
    should_run_full_suite: bool


class TestChangeDetector:
    """Detects code changes and suggests appropriate tests."""
    
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.src_root = repo_root / "src" / "marketpipe"
        self.test_root = repo_root / "tests"
        self.log = logging.getLogger(__name__)
        
        # File pattern to test mapping
        self.test_mappings = {
            # CLI changes -> CLI tests
            r"src/marketpipe/cli\.py": [
                "tests/unit/cli/",
                "tests/integration/test_cli_*.py"
            ],
            
            # Domain changes -> Domain + integration tests
            r"src/marketpipe/domain/.*": [
                "tests/unit/domain/",
                "tests/integration/test_*_domain_*.py",
                "tests/unit/test_ddd_guard_rails.py"
            ],
            
            # Infrastructure changes -> Infrastructure + integration tests
            r"src/marketpipe/infrastructure/.*": [
                "tests/unit/infrastructure/",
                "tests/integration/test_*_infrastructure_*.py"
            ],
            
            # Ingestion changes -> Ingestion tests
            r"src/marketpipe/ingestion/.*": [
                "tests/unit/ingestion/",
                "tests/integration/test_*_ingestion_*.py",
                "tests/integration/test_coordinator_*.py"
            ],
            
            # Provider adapters -> Provider tests
            r"src/marketpipe/.*_adapter\.py": [
                "tests/unit/test_*_adapter.py",
                "tests/integration/test_*_provider_*.py"
            ],
            
            # Configuration changes -> Config tests
            r"src/marketpipe/config/.*": [
                "tests/unit/config/",
                "tests/integration/test_*_config_*.py"
            ],
            
            # Database changes -> Migration tests
            r"alembic/versions/.*|src/marketpipe/infrastructure/database/.*": [
                "tests/unit/database/",
                "tests/integration/test_migration_*.py",
                "tests/postgres/"
            ],
            
            # Metrics changes -> Metrics tests
            r"src/marketpipe/.*metrics.*": [
                "tests/unit/test_metrics.py",
                "tests/integration/test_metrics_*.py"
            ],
            
            # Test file changes -> Run the changed tests
            r"tests/.*": ["__changed_test_files__"],
            
            # Configuration files -> Full integration suite
            r"pytest\.ini|pyproject\.toml|setup\.cfg": ["__full_integration__"],
            
            # CI/CD changes -> Full test suite
            r"\.github/workflows/.*|Makefile": ["__full_suite__"]
        }
        
        # Critical files that always require full suite
        self.critical_files = {
            "src/marketpipe/__init__.py",
            "src/marketpipe/cli.py", 
            "setup.py",
            "pyproject.toml"
        }

    def analyze_changes(self, base_ref: str = "HEAD~1") -> ChangeAnalysis:
        """Analyze changes since base_ref and suggest tests to run."""
        try:
            changed_files = self._get_changed_files(base_ref)
            self.log.info(f"Found {len(changed_files)} changed files")
            
            test_suggestions = self._generate_test_suggestions(changed_files)
            coverage_impact = self._estimate_coverage_impact(changed_files)
            should_run_full = self._should_run_full_suite(changed_files)
            
            return ChangeAnalysis(
                changed_files=changed_files,
                test_suggestions=test_suggestions,
                coverage_impact=coverage_impact,
                should_run_full_suite=should_run_full
            )
            
        except Exception as e:
            self.log.error(f"Failed to analyze changes: {e}")
            # Fallback: suggest running full suite
            return ChangeAnalysis(
                changed_files=[],
                test_suggestions=[TestSuggestion(
                    test_path="tests/",
                    reason="Error analyzing changes - running full suite",
                    priority=1,
                    test_type=TestType.ALL,
                    estimated_duration="5-10 minutes"
                )],
                coverage_impact={},
                should_run_full_suite=True
            )

    def _get_changed_files(self, base_ref: str) -> List[str]:
        """Get list of files changed since base_ref."""
        try:
            # Get changed files from git
            result = subprocess.run(
                ["git", "diff", "--name-only", base_ref],
                capture_output=True,
                text=True,
                cwd=self.repo_root
            )
            
            if result.returncode != 0:
                self.log.warning(f"Git diff failed: {result.stderr}")
                return []
            
            changed_files = [f.strip() for f in result.stdout.split('\n') if f.strip()]
            
            # Also check staged files
            staged_result = subprocess.run(
                ["git", "diff", "--name-only", "--staged"],
                capture_output=True,
                text=True,
                cwd=self.repo_root
            )
            
            if staged_result.returncode == 0:
                staged_files = [f.strip() for f in staged_result.stdout.split('\n') if f.strip()]
                changed_files.extend(staged_files)
            
            # Remove duplicates and return
            return list(set(changed_files))
            
        except Exception as e:
            self.log.error(f"Failed to get changed files: {e}")
            return []

    def _generate_test_suggestions(self, changed_files: List[str]) -> List[TestSuggestion]:
        """Generate test suggestions based on changed files."""
        suggestions = []
        matched_patterns = set()
        
        for file_path in changed_files:
            for pattern, test_paths in self.test_mappings.items():
                if re.search(pattern, file_path):
                    matched_patterns.add(pattern)
                    
                    for test_path in test_paths:
                        if test_path == "__changed_test_files__":
                            # Run the changed test file itself
                            if file_path.startswith("tests/"):
                                suggestions.append(TestSuggestion(
                                    test_path=file_path,
                                    reason=f"Test file was changed: {file_path}",
                                    priority=1,
                                    test_type=self._classify_test_file(file_path),
                                    estimated_duration="30 seconds"
                                ))
                        elif test_path == "__full_integration__":
                            suggestions.append(TestSuggestion(
                                test_path="tests/integration/",
                                reason=f"Critical config changed: {file_path}",
                                priority=1,
                                test_type=TestType.INTEGRATION,
                                estimated_duration="2-3 minutes"
                            ))
                        elif test_path == "__full_suite__":
                            suggestions.append(TestSuggestion(
                                test_path="tests/",
                                reason=f"CI/build config changed: {file_path}",
                                priority=1,
                                test_type=TestType.ALL,
                                estimated_duration="5-10 minutes"
                            ))
                        else:
                            # Regular test path mapping
                            expanded_paths = self._expand_test_path(test_path)
                            for expanded_path in expanded_paths:
                                suggestions.append(TestSuggestion(
                                    test_path=expanded_path,
                                    reason=f"Related to changed file: {file_path}",
                                    priority=self._calculate_priority(file_path, expanded_path),
                                    test_type=self._classify_test_path(expanded_path),
                                    estimated_duration=self._estimate_duration(expanded_path)
                                ))
        
        # Add safety nets
        suggestions.extend(self._add_safety_net_tests(changed_files, matched_patterns))
        
        # Deduplicate and sort by priority
        return self._deduplicate_and_sort_suggestions(suggestions)

    def _expand_test_path(self, test_path: str) -> List[str]:
        """Expand test path patterns to actual test files."""
        expanded = []
        
        if test_path.endswith("/"):
            # Directory - find all test files
            test_dir = self.repo_root / test_path
            if test_dir.exists():
                for test_file in test_dir.rglob("test_*.py"):
                    expanded.append(str(test_file.relative_to(self.repo_root)))
        elif "*" in test_path:
            # Glob pattern
            for test_file in self.repo_root.glob(test_path):
                if test_file.is_file():
                    expanded.append(str(test_file.relative_to(self.repo_root)))
        else:
            # Specific file
            test_file = self.repo_root / test_path
            if test_file.exists():
                expanded.append(test_path)
        
        return expanded

    def _classify_test_file(self, test_path: str) -> TestType:
        """Classify a test file by its type."""
        if "integration" in test_path:
            return TestType.INTEGRATION
        elif "slow" in test_path or any(marker in test_path for marker in ["e2e", "performance"]):
            return TestType.SLOW
        else:
            return TestType.UNIT

    def _classify_test_path(self, test_path: str) -> TestType:
        """Classify a test path by its likely type."""
        if "integration" in test_path:
            return TestType.INTEGRATION
        elif "performance" in test_path or "load" in test_path:
            return TestType.SLOW
        elif test_path == "tests/":
            return TestType.ALL
        else:
            return TestType.UNIT

    def _calculate_priority(self, changed_file: str, test_path: str) -> int:
        """Calculate test priority (1=high, 2=medium, 3=low)."""
        # High priority for critical files
        if any(critical in changed_file for critical in self.critical_files):
            return 1
            
        # High priority for direct test file changes
        if changed_file.startswith("tests/") and changed_file == test_path:
            return 1
            
        # High priority for domain/infrastructure changes
        if any(component in changed_file for component in ["domain", "infrastructure", "cli"]):
            return 1
            
        # Medium priority for application layer
        if "application" in changed_file or "ingestion" in changed_file:
            return 2
            
        # Low priority for docs/config
        return 3

    def _estimate_duration(self, test_path: str) -> str:
        """Estimate test duration based on path."""
        if test_path == "tests/":
            return "5-10 minutes"
        elif "integration" in test_path:
            return "1-3 minutes"
        elif "performance" in test_path or "load" in test_path:
            return "5+ minutes"
        else:
            return "10-60 seconds"

    def _add_safety_net_tests(self, changed_files: List[str], matched_patterns: Set[str]) -> List[TestSuggestion]:
        """Add safety net tests for unmatched changes."""
        suggestions = []
        
        # Check for files that didn't match any pattern
        unmatched_code_files = [
            f for f in changed_files 
            if f.startswith("src/") and f.endswith(".py") 
            and not any(re.search(pattern, f) for pattern in matched_patterns)
        ]
        
        if unmatched_code_files:
            suggestions.append(TestSuggestion(
                test_path="tests/unit/",
                reason=f"Unmatched code changes: {', '.join(unmatched_code_files[:3])}{'...' if len(unmatched_code_files) > 3 else ''}",
                priority=2,
                test_type=TestType.UNIT,
                estimated_duration="1-2 minutes"
            ))
        
        # Always suggest running fast unit tests as a safety net
        if not any(s.test_type == TestType.UNIT for s in suggestions):
            suggestions.append(TestSuggestion(
                test_path="tests/unit/",
                reason="Safety net: ensure core functionality works",
                priority=3,
                test_type=TestType.UNIT,
                estimated_duration="30-60 seconds"
            ))
        
        return suggestions

    def _deduplicate_and_sort_suggestions(self, suggestions: List[TestSuggestion]) -> List[TestSuggestion]:
        """Remove duplicates and sort by priority."""
        # Deduplicate by test_path
        seen_paths = set()
        unique_suggestions = []
        
        for suggestion in suggestions:
            if suggestion.test_path not in seen_paths:
                seen_paths.add(suggestion.test_path)
                unique_suggestions.append(suggestion)
        
        # Sort by priority (1=high first), then by test type
        return sorted(unique_suggestions, key=lambda s: (s.priority, s.test_type.value))

    def _estimate_coverage_impact(self, changed_files: List[str]) -> Dict[str, float]:
        """Estimate coverage impact of changes."""
        # This is a simplified estimation
        # In a full implementation, you'd parse actual coverage data
        impact = {}
        
        for file_path in changed_files:
            if file_path.startswith("src/marketpipe/"):
                module_name = file_path.replace("src/", "").replace("/", ".").replace(".py", "")
                # Estimate impact based on file type
                if "domain" in file_path:
                    impact[module_name] = 0.8  # High impact
                elif "infrastructure" in file_path:
                    impact[module_name] = 0.6  # Medium impact
                else:
                    impact[module_name] = 0.4  # Lower impact
        
        return impact

    def _should_run_full_suite(self, changed_files: List[str]) -> bool:
        """Determine if full test suite should be run."""
        # Run full suite for critical file changes
        for file_path in changed_files:
            if any(critical in file_path for critical in self.critical_files):
                return True
            if file_path in ["pytest.ini", "pyproject.toml", ".github/workflows/ci.yml"]:
                return True
        
        # Run full suite if many files changed
        if len(changed_files) > 10:
            return True
            
        return False

    def generate_pytest_command(self, analysis: ChangeAnalysis, fast_mode: bool = True) -> str:
        """Generate optimal pytest command based on analysis."""
        if analysis.should_run_full_suite:
            return "pytest -q" if fast_mode else "pytest -v"
        
        if not analysis.test_suggestions:
            return "pytest -q -m 'unit' --lf --maxfail=3"
        
        # Build command from high-priority suggestions
        high_priority_tests = [s for s in analysis.test_suggestions if s.priority == 1]
        
        if not high_priority_tests:
            high_priority_tests = analysis.test_suggestions[:3]  # Take top 3
        
        test_paths = [s.test_path for s in high_priority_tests]
        
        # Optimize based on test types
        test_types = {s.test_type for s in high_priority_tests}
        
        cmd_parts = ["pytest", "-q"]
        
        if fast_mode:
            cmd_parts.extend(["--lf", "--maxfail=3"])
            
            if TestType.UNIT in test_types and TestType.INTEGRATION not in test_types:
                cmd_parts.extend(["-n", "auto"])  # Parallel for unit tests only
        
        # Add test paths
        cmd_parts.extend(test_paths)
        
        return " ".join(cmd_parts)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Detect code changes and suggest tests")
    parser.add_argument(
        "--base-ref", 
        default="HEAD~1", 
        help="Git reference to compare against (default: HEAD~1)"
    )
    parser.add_argument(
        "--format", 
        choices=["json", "text", "command"], 
        default="text",
        help="Output format"
    )
    parser.add_argument(
        "--fast", 
        action="store_true", 
        help="Generate fast test command"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Show what would be tested without running"
    )
    parser.add_argument(
        "--verbose", 
        action="store_true", 
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")
    
    # Find repo root
    repo_root = Path.cwd()
    while not (repo_root / ".git").exists() and repo_root != repo_root.parent:
        repo_root = repo_root.parent
    
    if not (repo_root / ".git").exists():
        print("❌ Not in a git repository", file=sys.stderr)
        sys.exit(1)
    
    # Analyze changes
    detector = TestChangeDetector(repo_root)
    analysis = detector.analyze_changes(args.base_ref)
    
    # Output results
    if args.format == "json":
        import json
        print(json.dumps({
            "changed_files": analysis.changed_files,
            "test_suggestions": [
                {
                    "test_path": s.test_path,
                    "reason": s.reason,
                    "priority": s.priority,
                    "test_type": s.test_type.value,
                    "estimated_duration": s.estimated_duration
                }
                for s in analysis.test_suggestions
            ],
            "should_run_full_suite": analysis.should_run_full_suite,
            "pytest_command": detector.generate_pytest_command(analysis, args.fast)
        }, indent=2))
    
    elif args.format == "command":
        print(detector.generate_pytest_command(analysis, args.fast))
    
    else:  # text format
        print("🔍 Code Change Analysis")
        print("=" * 50)
        
        if not analysis.changed_files:
            print("📝 No changes detected")
            sys.exit(0)
            
        print(f"📝 Changed files ({len(analysis.changed_files)}):")
        for file_path in analysis.changed_files[:10]:  # Show first 10
            print(f"  • {file_path}")
        if len(analysis.changed_files) > 10:
            print(f"  ... and {len(analysis.changed_files) - 10} more")
        
        print(f"\n🧪 Test suggestions ({len(analysis.test_suggestions)}):")
        for i, suggestion in enumerate(analysis.test_suggestions[:5]):  # Show top 5
            priority_emoji = "🔴" if suggestion.priority == 1 else "🟡" if suggestion.priority == 2 else "🟢"
            print(f"  {i+1}. {priority_emoji} {suggestion.test_path}")
            print(f"     Reason: {suggestion.reason}")
            print(f"     Duration: {suggestion.estimated_duration}")
            print()
        
        if analysis.should_run_full_suite:
            print("⚠️  Recommendation: Run FULL test suite (critical changes detected)")
        else:
            print("✅ Recommendation: Run targeted tests")
        
        print(f"\n🚀 Suggested command:")
        cmd = detector.generate_pytest_command(analysis, args.fast)
        print(f"   {cmd}")
        
        if args.dry_run:
            print("\n💡 This is a dry run. To execute the tests, run the command above.")
        else:
            print("\n💡 To run these tests: copy the command above or use --format=command")


if __name__ == "__main__":
    main() 