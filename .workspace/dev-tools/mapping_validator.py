#!/usr/bin/env python3
"""
Test Mapping Validator - MarketPipe

Validates and maintains test mappings to ensure comprehensive coverage.
Detects missing dependencies and suggests improvements to the smart test system.
"""

import ast
import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict
import json
import re


@dataclass
class Dependency:
    """Represents a code dependency."""
    from_file: str
    to_file: str
    import_type: str  # 'direct', 'indirect', 'test_import'
    line_number: int
    import_statement: str


@dataclass
class MappingGap:
    """Represents a gap in test mapping coverage."""
    changed_file: str
    affected_files: List[str]
    missing_tests: List[str]
    confidence: float  # 0.0 to 1.0
    reason: str


class DependencyAnalyzer:
    """Analyzes code dependencies using static analysis."""
    
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.src_root = repo_root / "src" / "marketpipe"
        self.test_root = repo_root / "tests"
        
    def find_test_files_for_source(self, source_file: str) -> List[str]:
        """Find test files that likely test a given source file."""
        tests = []
        
        if not source_file.startswith('src/marketpipe/'):
            return tests
        
        # Extract module path
        module_path = source_file.replace('src/marketpipe/', '').replace('.py', '')
        module_name = Path(module_path).name
        
        # Common test file patterns
        test_patterns = [
            f"tests/unit/test_{module_name}.py",
            f"tests/unit/{module_path}/test_{module_name}.py", 
            f"tests/unit/test_{module_path.replace('/', '_')}.py",
            f"tests/integration/test_{module_name}_*.py",
        ]
        
        for pattern in test_patterns:
            test_file = self.repo_root / pattern
            if test_file.exists():
                tests.append(str(test_file.relative_to(self.repo_root)))
        
        # Find tests by directory structure
        module_dir = Path(module_path).parent
        test_dir = self.test_root / "unit" / module_dir
        if test_dir.exists():
            for test_file in test_dir.glob("test_*.py"):
                tests.append(str(test_file.relative_to(self.repo_root)))
        
        return list(set(tests))
    
    def find_cross_module_dependencies(self, source_file: str) -> List[str]:
        """Find other modules that might be affected by changes to source_file."""
        affected = []
        
        if not source_file.startswith('src/marketpipe/'):
            return affected
        
        # Look for imports of this module in other files
        module_path = source_file.replace('src/marketpipe/', '').replace('.py', '').replace('/', '.')
        
        # Search for imports across the codebase
        try:
            # Use grep to find imports
            result = subprocess.run([
                'grep', '-r', '-l', f'from.*{module_path}', str(self.src_root)
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if line and Path(line).exists():
                        rel_path = str(Path(line).relative_to(self.repo_root))
                        if rel_path != source_file:
                            affected.append(rel_path)
            
            # Also check for direct imports
            result2 = subprocess.run([
                'grep', '-r', '-l', f'import.*{module_path}', str(self.src_root)
            ], capture_output=True, text=True)
            
            if result2.returncode == 0:
                for line in result2.stdout.strip().split('\n'):
                    if line and Path(line).exists():
                        rel_path = str(Path(line).relative_to(self.repo_root))
                        if rel_path != source_file and rel_path not in affected:
                            affected.append(rel_path)
                            
        except Exception as e:
            print(f"Warning: Could not analyze cross-module dependencies: {e}")
        
        return affected


class MappingValidator:
    """Validates test mappings against actual code structure."""
    
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.analyzer = DependencyAnalyzer(repo_root)
        self.current_mappings = self._load_current_mappings()
    
    def _load_current_mappings(self) -> Dict[str, List[str]]:
        """Load current test mappings from smart_test_runner.py."""
        # Simplified mapping extraction - in practice this could be more sophisticated
        return {
            r"src/marketpipe/cli\.py": ["tests/unit/cli/", "tests/integration/test_cli_*.py"],
            r"src/marketpipe/domain/": ["tests/unit/domain/", "tests/unit/test_ddd_guard_rails.py"],
            r"src/marketpipe/infrastructure/": ["tests/unit/infrastructure/", "tests/integration/"],
            r"src/marketpipe/ingestion/": ["tests/unit/ingestion/", "tests/integration/test_coordinator_*.py"],
            r"src/marketpipe/.*adapter\.py": ["tests/unit/test_*_adapter.py"],
            r"src/marketpipe/config/": ["tests/unit/config/"],
        }
    
    def validate_coverage(self, changed_files: List[str]) -> List[MappingGap]:
        """Validate that mappings provide adequate test coverage."""
        gaps = []
        
        for changed_file in changed_files:
            if not changed_file.startswith('src/marketpipe/') or not changed_file.endswith('.py'):
                continue
            
            # Find tests that should run for this file
            expected_tests = self.analyzer.find_test_files_for_source(changed_file)
            
            # Find tests that current mappings would run
            mapped_tests = self._get_mapped_tests(changed_file)
            
            # Find cross-module dependencies
            affected_modules = self.analyzer.find_cross_module_dependencies(changed_file)
            
            # Check for missing coverage
            missing_tests = set(expected_tests) - set(mapped_tests)
            
            if missing_tests or affected_modules:
                confidence = self._calculate_confidence(changed_file, expected_tests, mapped_tests, affected_modules)
                
                if confidence > 0.3:  # Only report significant gaps
                    gaps.append(MappingGap(
                        changed_file=changed_file,
                        affected_files=affected_modules,
                        missing_tests=list(missing_tests),
                        confidence=confidence,
                        reason=self._generate_reason(changed_file, missing_tests, affected_modules)
                    ))
        
        return gaps
    
    def _get_mapped_tests(self, changed_file: str) -> List[str]:
        """Get tests that current mappings would run for a changed file."""
        mapped_tests = []
        
        for pattern, test_paths in self.current_mappings.items():
            if re.search(pattern, changed_file):
                for test_path in test_paths:
                    expanded = self._expand_test_path(test_path)
                    mapped_tests.extend(expanded)
        
        return list(set(mapped_tests))
    
    def _expand_test_path(self, test_path: str) -> List[str]:
        """Expand test path patterns to actual files."""
        if test_path.endswith("/"):
            test_dir = self.repo_root / test_path
            if test_dir.exists():
                return [str(p.relative_to(self.repo_root)) for p in test_dir.rglob("test_*.py")]
        elif "*" in test_path:
            return [str(p.relative_to(self.repo_root)) for p in self.repo_root.glob(test_path)]
        else:
            test_file = self.repo_root / test_path
            if test_file.exists():
                return [test_path]
        return []
    
    def _calculate_confidence(self, changed_file: str, expected_tests: List[str], 
                            mapped_tests: List[str], affected_modules: List[str]) -> float:
        """Calculate confidence in gap detection."""
        confidence = 0.0
        
        # Base confidence if tests are missing
        if expected_tests and not mapped_tests:
            confidence = 0.8
        elif len(set(expected_tests) - set(mapped_tests)) > 0:
            missing_ratio = len(set(expected_tests) - set(mapped_tests)) / len(expected_tests)
            confidence = missing_ratio * 0.7
        
        # Increase confidence if cross-module dependencies exist
        if affected_modules:
            confidence += min(0.3, len(affected_modules) * 0.1)
        
        # Decrease confidence for certain file types that are less critical
        if any(part in changed_file for part in ['__init__.py', 'constants.py', 'exceptions.py']):
            confidence *= 0.5
        
        return min(1.0, confidence)
    
    def _generate_reason(self, changed_file: str, missing_tests: Set[str], affected_modules: List[str]) -> str:
        """Generate human-readable reason for the mapping gap."""
        reasons = []
        
        if missing_tests:
            reasons.append(f"Missing {len(missing_tests)} expected test files")
        
        if affected_modules:
            reasons.append(f"Affects {len(affected_modules)} other modules")
        
        return "; ".join(reasons) if reasons else "Potential coverage gap detected"


def main():
    parser = argparse.ArgumentParser(description="Validate test mappings")
    parser.add_argument("--changed-files", nargs="*", help="Files to analyze")
    parser.add_argument("--min-confidence", type=float, default=0.5, help="Minimum confidence threshold")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    
    args = parser.parse_args()
    
    # Find repo root
    repo_root = Path.cwd()
    while not (repo_root / ".git").exists() and repo_root != repo_root.parent:
        repo_root = repo_root.parent
    
    if not (repo_root / ".git").exists():
        print("❌ Not in a git repository")
        sys.exit(1)
    
    # Get changed files
    if args.changed_files:
        changed_files = args.changed_files
    else:
        try:
            result = subprocess.run(
                ["git", "diff", "--name-only", "HEAD~1"],
                capture_output=True, text=True, cwd=repo_root
            )
            changed_files = [f.strip() for f in result.stdout.split('\n') if f.strip()]
        except:
            changed_files = []
    
    if not changed_files:
        print("📝 No changed files to analyze")
        return
    
    # Validate mappings
    validator = MappingValidator(repo_root)
    gaps = validator.validate_coverage(changed_files)
    
    # Filter by confidence
    significant_gaps = [g for g in gaps if g.confidence >= args.min_confidence]
    
    if args.format == "json":
        output = {
            "changed_files": changed_files,
            "mapping_gaps": [
                {
                    "changed_file": gap.changed_file,
                    "affected_files": gap.affected_files,
                    "missing_tests": gap.missing_tests,
                    "confidence": gap.confidence,
                    "reason": gap.reason
                }
                for gap in significant_gaps
            ]
        }
        print(json.dumps(output, indent=2))
    else:
        print("🔍 Test Mapping Validation")
        print("=" * 40)
        print(f"Analyzed {len(changed_files)} changed files")
        
        if not significant_gaps:
            print("✅ No significant mapping gaps detected")
        else:
            print(f"⚠️  Found {len(significant_gaps)} potential gaps:")
            
            for gap in significant_gaps:
                print(f"\n📁 {gap.changed_file}")
                print(f"   Confidence: {gap.confidence:.2f}")
                print(f"   {gap.reason}")
                
                if gap.missing_tests:
                    print(f"   Missing tests:")
                    for test in gap.missing_tests[:3]:
                        print(f"     • {test}")
                    if len(gap.missing_tests) > 3:
                        print(f"     ... and {len(gap.missing_tests) - 3} more")
                
                if gap.affected_files:
                    print(f"   Affected modules:")
                    for module in gap.affected_files[:3]:
                        print(f"     • {module}")
                    if len(gap.affected_files) > 3:
                        print(f"     ... and {len(gap.affected_files) - 3} more")


if __name__ == "__main__":
    main() 