# SPDX-License-Identifier: Apache-2.0
#!/usr/bin/env python3
"""
DDD Rules Validation Script for MarketPipe

This script validates that the codebase follows Domain-Driven Design principles
and the rules defined in .cursor/rules/ddd/*.mdc files.

Usage:
    python scripts/ddd-validation/validate_ddd_rules.py
    python scripts/ddd-validation/validate_ddd_rules.py --fix
    python scripts/ddd-validation/validate_ddd_rules.py --report-only
"""

import ast
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
import argparse


@dataclass
class ValidationResult:
    """Result of a DDD validation check."""
    rule_id: str
    file_path: str
    line_number: int
    severity: str  # error, warning, info
    message: str
    suggestion: Optional[str] = None


@dataclass
class ValidationSummary:
    """Summary of all validation results."""
    total_files_checked: int
    total_violations: int
    errors: int
    warnings: int
    info: int
    results: List[ValidationResult]


class DDDValidator:
    """Validates Domain-Driven Design rules in MarketPipe codebase."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.src_path = project_root / "src" / "marketpipe"
        self.domain_path = self.src_path / "domain"
        
        # Ubiquitous Language terms
        self.approved_terms = {
            "symbol", "ohlcv", "bar", "timestamp", "trading_date", "ingestion",
            "market_data_provider", "validation", "partition", "checkpoint",
            "price", "volume", "timeframe", "aggregate", "entity", "value_object"
        }
        
        self.banned_terms = {
            "ticker", "security", "instrument", "candle", "quote", "business_date",
            "import", "fetch", "vendor", "feed", "verification", "record", "data"
        }
        
    def validate_all(self) -> ValidationSummary:
        """Run all DDD validation checks."""
        results = []
        files_checked = 0
        
        # Check all Python files in src/marketpipe
        for py_file in self.src_path.rglob("*.py"):
            if py_file.name.startswith("test_"):
                continue  # Skip test files for domain validation
            
            files_checked += 1
            results.extend(self._validate_file(py_file))
        
        # Categorize results
        errors = sum(1 for r in results if r.severity == "error")
        warnings = sum(1 for r in results if r.severity == "warning")
        info = sum(1 for r in results if r.severity == "info")
        
        return ValidationSummary(
            total_files_checked=files_checked,
            total_violations=len(results),
            errors=errors,
            warnings=warnings,
            info=info,
            results=results
        )
    
    def _validate_file(self, file_path: Path) -> List[ValidationResult]:
        """Validate a single Python file."""
        results = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse AST for structural validation
            try:
                tree = ast.parse(content)
                results.extend(self._validate_ast(file_path, tree, content))
            except SyntaxError:
                results.append(ValidationResult(
                    rule_id="syntax_error",
                    file_path=str(file_path),
                    line_number=1,
                    severity="error",
                    message="File has syntax errors"
                ))
            
            # Validate content-based rules
            results.extend(self._validate_content(file_path, content))
            
        except Exception as e:
            results.append(ValidationResult(
                rule_id="file_error",
                file_path=str(file_path),
                line_number=1,
                severity="error",
                message=f"Error reading file: {e}"
            ))
        
        return results
    
    def _validate_ast(self, file_path: Path, tree: ast.AST, content: str) -> List[ValidationResult]:
        """Validate using AST analysis."""
        results = []
        lines = content.split('\n')
        
        # Check imports at top
        results.extend(self._validate_imports(file_path, tree, lines))
        
        # Check class definitions
        results.extend(self._validate_classes(file_path, tree, lines))
        
        # Check function definitions
        results.extend(self._validate_functions(file_path, tree, lines))
        
        # Check domain model structure
        if self._is_domain_file(file_path):
            results.extend(self._validate_domain_structure(file_path, tree, lines))
        
        return results
    
    def _validate_content(self, file_path: Path, content: str) -> List[ValidationResult]:
        """Validate content-based rules."""
        results = []
        lines = content.split('\n')
        
        # Check ubiquitous language
        results.extend(self._validate_ubiquitous_language(file_path, lines))
        
        # Check naming conventions
        results.extend(self._validate_naming_conventions(file_path, lines))
        
        # Check docstring quality
        results.extend(self._validate_docstrings(file_path, lines))
        
        return results
    
    def _validate_imports(self, file_path: Path, tree: ast.AST, lines: List[str]) -> List[ValidationResult]:
        """Validate import organization and future annotations."""
        results = []
        
        # Check for future annotations
        has_future_annotations = False
        first_import_line = None
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module == "__future__" and any(alias.name == "annotations" for alias in node.names):
                    has_future_annotations = True
                if first_import_line is None:
                    first_import_line = node.lineno
        
        # Only check non-__init__ files
        if file_path.name != "__init__.py" and not has_future_annotations:
            results.append(ValidationResult(
                rule_id="missing_future_annotations",
                file_path=str(file_path),
                line_number=1,
                severity="error",
                message="Missing 'from __future__ import annotations'",
                suggestion="Add 'from __future__ import annotations' as first import"
            ))
        
        # Check domain layer doesn't import infrastructure
        if self._is_domain_file(file_path):
            results.extend(self._validate_domain_imports(file_path, tree))
        
        return results
    
    def _validate_classes(self, file_path: Path, tree: ast.AST, lines: List[str]) -> List[ValidationResult]:
        """Validate class definitions and patterns."""
        results = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Check naming convention
                if not self._is_pascal_case(node.name):
                    results.append(ValidationResult(
                        rule_id="class_naming",
                        file_path=str(file_path),
                        line_number=node.lineno,
                        severity="error",
                        message=f"Class '{node.name}' should use PascalCase",
                        suggestion=f"Rename to '{self._to_pascal_case(node.name)}'"
                    ))
                
                # Check domain model patterns
                if self._is_domain_file(file_path):
                    results.extend(self._validate_domain_class(file_path, node, lines))
        
        return results
    
    def _validate_functions(self, file_path: Path, tree: ast.AST, lines: List[str]) -> List[ValidationResult]:
        """Validate function definitions."""
        results = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Check naming convention
                if not self._is_snake_case(node.name) and not node.name.startswith("__"):
                    results.append(ValidationResult(
                        rule_id="function_naming",
                        file_path=str(file_path),
                        line_number=node.lineno,
                        severity="error",
                        message=f"Function '{node.name}' should use snake_case",
                        suggestion=f"Rename to '{self._to_snake_case(node.name)}'"
                    ))
                
                # Check type annotations
                results.extend(self._validate_function_annotations(file_path, node))
        
        return results
    
    def _validate_domain_structure(self, file_path: Path, tree: ast.AST, lines: List[str]) -> List[ValidationResult]:
        """Validate domain model structure."""
        results = []
        
        # Check that domain files don't have infrastructure dependencies
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module and ("infrastructure" in node.module or "adapters" in node.module):
                    results.append(ValidationResult(
                        rule_id="domain_infrastructure_dependency",
                        file_path=str(file_path),
                        line_number=node.lineno,
                        severity="error",
                        message=f"Domain layer should not import infrastructure: {node.module}",
                        suggestion="Move infrastructure dependencies to application or infrastructure layer"
                    ))
        
        return results
    
    def _validate_domain_class(self, file_path: Path, node: ast.ClassDef, lines: List[str]) -> List[ValidationResult]:
        """Validate domain model class patterns."""
        results = []
        
        # Check entity pattern
        if node.name.endswith("Entity") or self._inherits_from(node, "Entity"):
            results.extend(self._validate_entity_pattern(file_path, node))
        
        # Check value object pattern
        if "value_objects.py" in str(file_path):
            results.extend(self._validate_value_object_pattern(file_path, node))
        
        # Check aggregate pattern
        if node.name.endswith("Aggregate") or "aggregates.py" in str(file_path):
            results.extend(self._validate_aggregate_pattern(file_path, node))
        
        return results
    
    def _validate_entity_pattern(self, file_path: Path, node: ast.ClassDef) -> List[ValidationResult]:
        """Validate entity pattern compliance."""
        results = []
        
        # Check for ID property
        has_id_property = False
        for item in node.body:
            if isinstance(item, ast.FunctionDef) and item.name == "id":
                if len(item.decorator_list) > 0:
                    for decorator in item.decorator_list:
                        if isinstance(decorator, ast.Name) and decorator.id == "property":
                            has_id_property = True
        
        if not has_id_property:
            results.append(ValidationResult(
                rule_id="entity_missing_id",
                file_path=str(file_path),
                line_number=node.lineno,
                severity="error",
                message=f"Entity '{node.name}' should have an 'id' property",
                suggestion="Add @property id method returning EntityId"
            ))
        
        return results
    
    def _validate_value_object_pattern(self, file_path: Path, node: ast.ClassDef) -> List[ValidationResult]:
        """Validate value object pattern compliance."""
        results = []
        
        # Check for @dataclass(frozen=True)
        has_frozen_dataclass = False
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name):
                if decorator.func.id == "dataclass":
                    for keyword in decorator.keywords:
                        if keyword.arg == "frozen" and isinstance(keyword.value, ast.Constant):
                            if keyword.value.value is True:
                                has_frozen_dataclass = True
        
        if not has_frozen_dataclass:
            results.append(ValidationResult(
                rule_id="value_object_not_frozen",
                file_path=str(file_path),
                line_number=node.lineno,
                severity="warning",
                message=f"Value object '{node.name}' should be @dataclass(frozen=True)",
                suggestion="Add @dataclass(frozen=True) decorator"
            ))
        
        return results
    
    def _validate_aggregate_pattern(self, file_path: Path, node: ast.ClassDef) -> List[ValidationResult]:
        """Validate aggregate pattern compliance."""
        results = []
        
        # Check for domain events
        has_events_method = False
        for item in node.body:
            if isinstance(item, ast.FunctionDef) and "events" in item.name.lower():
                has_events_method = True
        
        if not has_events_method:
            results.append(ValidationResult(
                rule_id="aggregate_missing_events",
                file_path=str(file_path),
                line_number=node.lineno,
                severity="info",
                message=f"Aggregate '{node.name}' should manage domain events",
                suggestion="Add get_uncommitted_events() and mark_events_committed() methods"
            ))
        
        return results
    
    def _validate_ubiquitous_language(self, file_path: Path, lines: List[str]) -> List[ValidationResult]:
        """Validate use of ubiquitous language."""
        results = []
        
        for line_num, line in enumerate(lines, 1):
            # Check for banned terms in comments and strings
            for banned_term in self.banned_terms:
                pattern = r'\b' + banned_term + r'\b'
                if re.search(pattern, line, re.IGNORECASE):
                    # Skip if it's just a variable name or in a URL
                    if not re.search(r'(http|https|www\.|\.com|\.org)', line, re.IGNORECASE):
                        results.append(ValidationResult(
                            rule_id="banned_terminology",
                            file_path=str(file_path),
                            line_number=line_num,
                            severity="warning",
                            message=f"Avoid term '{banned_term}' - use domain language instead",
                            suggestion=self._suggest_alternative_term(banned_term)
                        ))
        
        return results
    
    def _validate_naming_conventions(self, file_path: Path, lines: List[str]) -> List[ValidationResult]:
        """Validate naming conventions."""
        results = []
        
        for line_num, line in enumerate(lines, 1):
            # Check for camelCase in variable names (should be snake_case)
            camel_case_pattern = r'\b[a-z]+[A-Z][a-zA-Z]*\s*='
            if re.search(camel_case_pattern, line):
                match = re.search(r'(\b[a-z]+[A-Z][a-zA-Z]*)\s*=', line)
                if match:
                    camel_name = match.group(1)
                    snake_name = self._to_snake_case(camel_name)
                    results.append(ValidationResult(
                        rule_id="variable_naming",
                        file_path=str(file_path),
                        line_number=line_num,
                        severity="warning",
                        message=f"Variable '{camel_name}' should use snake_case",
                        suggestion=f"Rename to '{snake_name}'"
                    ))
        
        return results
    
    def _validate_docstrings(self, file_path: Path, lines: List[str]) -> List[ValidationResult]:
        """Validate docstring quality and presence."""
        results = []
        
        # This is a simplified check - full implementation would parse AST
        in_class = False
        in_function = False
        class_line = 0
        function_line = 0
        
        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()
            
            if stripped.startswith('class '):
                in_class = True
                class_line = line_num
                # Check if next non-empty line is docstring
                
            elif stripped.startswith('def ') and not stripped.startswith('def _'):
                in_function = True
                function_line = line_num
                
            elif stripped.startswith('"""') or stripped.startswith("'''"):
                if in_class:
                    in_class = False
                elif in_function:
                    in_function = False
        
        return results
    
    def _validate_domain_imports(self, file_path: Path, tree: ast.AST) -> List[ValidationResult]:
        """Validate that domain layer doesn't import infrastructure."""
        results = []
        
        forbidden_imports = ["requests", "httpx", "sqlalchemy", "sqlite3", "psycopg2"]
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in forbidden_imports:
                        results.append(ValidationResult(
                            rule_id="domain_infrastructure_import",
                            file_path=str(file_path),
                            line_number=node.lineno,
                            severity="error",
                            message=f"Domain layer should not import infrastructure library: {alias.name}",
                            suggestion="Move infrastructure dependencies to application/infrastructure layer"
                        ))
            
            elif isinstance(node, ast.ImportFrom):
                if node.module in forbidden_imports:
                    results.append(ValidationResult(
                        rule_id="domain_infrastructure_import",
                        file_path=str(file_path),
                        line_number=node.lineno,
                        severity="error",
                        message=f"Domain layer should not import infrastructure library: {node.module}",
                        suggestion="Move infrastructure dependencies to application/infrastructure layer"
                    ))
        
        return results
    
    def _validate_function_annotations(self, file_path: Path, node: ast.FunctionDef) -> List[ValidationResult]:
        """Validate function has proper type annotations."""
        results = []
        
        # Skip private methods and special methods
        if node.name.startswith('_'):
            return results
        
        # Check return annotation
        if node.returns is None and node.name != "__init__":
            results.append(ValidationResult(
                rule_id="missing_return_annotation",
                file_path=str(file_path),
                line_number=node.lineno,
                severity="warning",
                message=f"Function '{node.name}' missing return type annotation",
                suggestion="Add -> ReturnType annotation"
            ))
        
        # Check parameter annotations
        missing_annotations = []
        for arg in node.args.args:
            if arg.annotation is None and arg.arg != "self":
                missing_annotations.append(arg.arg)
        
        if missing_annotations:
            results.append(ValidationResult(
                rule_id="missing_param_annotations",
                file_path=str(file_path),
                line_number=node.lineno,
                severity="warning",
                message=f"Function '{node.name}' missing parameter annotations: {', '.join(missing_annotations)}",
                suggestion="Add type annotations for all parameters"
            ))
        
        return results
    
    # Helper methods
    def _is_domain_file(self, file_path: Path) -> bool:
        """Check if file is in domain layer."""
        return "domain" in file_path.parts
    
    def _is_pascal_case(self, name: str) -> bool:
        """Check if name is PascalCase."""
        return re.match(r'^[A-Z][a-zA-Z0-9]*$', name) is not None
    
    def _is_snake_case(self, name: str) -> bool:
        """Check if name is snake_case."""
        return re.match(r'^[a-z_][a-z0-9_]*$', name) is not None
    
    def _to_pascal_case(self, name: str) -> str:
        """Convert name to PascalCase."""
        return ''.join(word.capitalize() for word in name.split('_'))
    
    def _to_snake_case(self, name: str) -> str:
        """Convert name to snake_case."""
        # Insert underscore before uppercase letters
        s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
        return s1.lower()
    
    def _inherits_from(self, node: ast.ClassDef, base_name: str) -> bool:
        """Check if class inherits from given base."""
        for base in node.bases:
            if isinstance(base, ast.Name) and base.id == base_name:
                return True
        return False
    
    def _suggest_alternative_term(self, banned_term: str) -> str:
        """Suggest alternative term for banned terminology."""
        alternatives = {
            "ticker": "symbol",
            "security": "symbol",
            "instrument": "symbol",
            "candle": "ohlcv_bar",
            "quote": "ohlcv_bar",
            "business_date": "trading_date",
            "import": "ingestion",
            "fetch": "ingestion",
            "vendor": "market_data_provider",
            "feed": "data_feed",
            "verification": "validation",
            "record": "entity or value_object",
            "data": "be more specific (ohlcv_bar, price_data, etc.)"
        }
        return alternatives.get(banned_term, "use domain-specific terminology")


def print_validation_report(summary: ValidationSummary, verbose: bool = False):
    """Print validation report to console."""
    print("\n" + "="*60)
    print("DDD VALIDATION REPORT")
    print("="*60)
    
    print(f"Files checked: {summary.total_files_checked}")
    print(f"Total violations: {summary.total_violations}")
    print(f"  Errors: {summary.errors}")
    print(f"  Warnings: {summary.warnings}")
    print(f"  Info: {summary.info}")
    
    if summary.total_violations == 0:
        print("\n✅ All DDD rules passed!")
        return
    
    # Group by severity
    errors = [r for r in summary.results if r.severity == "error"]
    warnings = [r for r in summary.results if r.severity == "warning"]
    info_items = [r for r in summary.results if r.severity == "info"]
    
    if errors:
        print(f"\n❌ ERRORS ({len(errors)}):")
        for result in errors[:10 if not verbose else None]:  # Limit output unless verbose
            print(f"  {result.file_path}:{result.line_number}")
            print(f"    {result.message}")
            if result.suggestion:
                print(f"    💡 {result.suggestion}")
        
        if not verbose and len(errors) > 10:
            print(f"    ... and {len(errors) - 10} more errors")
    
    if warnings:
        print(f"\n⚠️  WARNINGS ({len(warnings)}):")
        for result in warnings[:5 if not verbose else None]:
            print(f"  {result.file_path}:{result.line_number}")
            print(f"    {result.message}")
            if result.suggestion:
                print(f"    💡 {result.suggestion}")
        
        if not verbose and len(warnings) > 5:
            print(f"    ... and {len(warnings) - 5} more warnings")
    
    if info_items and verbose:
        print(f"\nℹ️  INFO ({len(info_items)}):")
        for result in info_items:
            print(f"  {result.file_path}:{result.line_number}")
            print(f"    {result.message}")
            if result.suggestion:
                print(f"    💡 {result.suggestion}")
    
    print("\n" + "="*60)
    
    if summary.errors > 0:
        print("❌ Fix errors before committing!")
        return False
    else:
        print("✅ No blocking errors found.")
        return True


def main():
    """Main entry point for DDD validation."""
    parser = argparse.ArgumentParser(description="Validate DDD rules in MarketPipe")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show all violations")
    parser.add_argument("--report-only", action="store_true", help="Only show report, don't exit with error code")
    parser.add_argument("--fix", action="store_true", help="Attempt to auto-fix simple violations")
    
    args = parser.parse_args()
    
    # Find project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    
    print(f"Validating DDD rules in: {project_root}")
    print(f"Domain model path: {project_root / 'src' / 'marketpipe' / 'domain'}")
    
    # Run validation
    validator = DDDValidator(project_root)
    summary = validator.validate_all()
    
    # Print report
    success = print_validation_report(summary, verbose=args.verbose)
    
    # Exit with appropriate code
    if not args.report_only and not success:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()