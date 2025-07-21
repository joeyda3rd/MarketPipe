#!/usr/bin/env python3
"""Phase 3 Release Optimization Validation Script

This script validates that all Phase 3 deliverables are properly implemented:
1. Release Documentation Created
2. Repository Size Optimized
3. Documentation Index Updated
4. Package Configuration Ready

Usage:
    python scripts/phase3_validation.py
"""

import os
import sys
from pathlib import Path


def check_file_exists(filepath: str, description: str) -> tuple[bool, str]:
    """Check if a file exists and return status."""
    if Path(filepath).exists():
        size = Path(filepath).stat().st_size
        return True, f"✅ {description} exists ({size:,} bytes)"
    else:
        return False, f"❌ {description} missing: {filepath}"


def check_file_content(
    filepath: str, required_content: list[str], description: str
) -> tuple[bool, str]:
    """Check if a file contains required content."""
    if not Path(filepath).exists():
        return False, f"❌ {description} missing: {filepath}"

    try:
        with open(filepath) as f:
            content = f.read()

        missing_content = [req for req in required_content if req not in content]
        if missing_content:
            return False, f"❌ {description} missing content: {', '.join(missing_content)}"
        else:
            return True, f"✅ {description} contains all required content"
    except Exception as e:
        return False, f"❌ Error reading {description}: {e}"


def validate_phase3_deliverables() -> dict[str, list[tuple[bool, str]]]:
    """Validate all Phase 3 deliverables."""
    results = {
        "Release Documentation": [],
        "Repository Optimization": [],
        "Documentation Index": [],
        "Package Configuration": [],
    }

    print("🚀 ===============================================")
    print("   PHASE 3 RELEASE OPTIMIZATION VALIDATION")
    print("===============================================")
    print()

    # 1. Release Documentation
    print("📚 Validating Release Documentation...")

    # RELEASE_NOTES.md
    status, msg = check_file_exists("docs/RELEASE_NOTES.md", "Release Notes")
    results["Release Documentation"].append((status, msg))

    if status:
        status, msg = check_file_content(
            "docs/RELEASE_NOTES.md",
            [
                "Version 1.0.0-alpha",
                "Comprehensive Pipeline Validator",
                "100% behavioral validation",
                "Professional repository structure",
            ],
            "Release Notes content",
        )
        results["Release Documentation"].append((status, msg))

    # INSTALLATION.md
    status, msg = check_file_exists("docs/INSTALLATION.md", "Installation Guide")
    results["Release Documentation"].append((status, msg))

    if status:
        status, msg = check_file_content(
            "docs/INSTALLATION.md",
            [
                "Quick Install",
                "Development Installation",
                "Installation Verification",
                "Troubleshooting",
            ],
            "Installation Guide content",
        )
        results["Release Documentation"].append((status, msg))

    # Documentation Index
    status, msg = check_file_exists("docs/README.md", "Documentation Index")
    results["Release Documentation"].append((status, msg))

    if status:
        status, msg = check_file_content(
            "docs/README.md",
            [
                "MarketPipe Documentation",
                "Quick Navigation",
                "Documentation Categories",
                "Use Case",
            ],
            "Documentation Index content",
        )
        results["Release Documentation"].append((status, msg))

    print()

    # 2. Repository Optimization
    print("🧹 Validating Repository Optimization...")

    # .gitignore updates
    status, msg = check_file_exists(".gitignore", "Git Ignore File")
    results["Repository Optimization"].append((status, msg))

    if status:
        status, msg = check_file_content(
            ".gitignore",
            ["Phase 3 Cleanup Additions", "validation_output/", "benchmark-*.json", "*.egg-info/"],
            "Git Ignore Phase 3 additions",
        )
        results["Repository Optimization"].append((status, msg))

    # Check for absence of temporary directories
    temp_dirs = ["validation_output", "test_data", ".mypy_cache", ".pytest_cache", ".ruff_cache"]
    for temp_dir in temp_dirs:
        if Path(temp_dir).exists():
            results["Repository Optimization"].append(
                (False, f"❌ Temporary directory still exists: {temp_dir}")
            )
        else:
            results["Repository Optimization"].append(
                (True, f"✅ Temporary directory properly excluded: {temp_dir}")
            )

    print()

    # 3. Documentation Structure
    print("📖 Validating Documentation Structure...")

    # Check professional documentation structure
    doc_structure = [
        "docs/README.md",
        "docs/RELEASE_NOTES.md",
        "docs/INSTALLATION.md",
        "docs/GETTING_STARTED.md",
        "docs/COMPREHENSIVE_PIPELINE_VALIDATOR.md",
        "docs/operations/",
        "docs/development/",
    ]

    for doc_item in doc_structure:
        if Path(doc_item).exists():
            results["Documentation Index"].append((True, f"✅ Documentation structure: {doc_item}"))
        else:
            results["Documentation Index"].append((False, f"❌ Missing documentation: {doc_item}"))

    print()

    # 4. Package Configuration
    print("📦 Validating Package Configuration...")

    # pyproject.toml
    status, msg = check_file_exists("pyproject.toml", "Package Configuration")
    results["Package Configuration"].append((status, msg))

    if status:
        try:
            with open("pyproject.toml") as f:
                content = f.read()

            required_sections = [
                "[project]",
                'name = "marketpipe"',
                "dependencies",
                "[build-system]",
            ]
            missing = [req for req in required_sections if req not in content]

            if missing:
                results["Package Configuration"].append(
                    (False, f"❌ pyproject.toml missing: {', '.join(missing)}")
                )
            else:
                results["Package Configuration"].append(
                    (True, "✅ pyproject.toml has required sections")
                )

        except Exception as e:
            results["Package Configuration"].append(
                (False, f"❌ Error reading pyproject.toml: {e}")
            )

    # README.md (root)
    status, msg = check_file_exists("README.md", "Root README")
    results["Package Configuration"].append((status, msg))

    print()

    return results


def print_validation_summary(results: dict[str, list[tuple[bool, str]]]) -> bool:
    """Print validation summary and return overall success status."""
    print("📊 PHASE 3 VALIDATION SUMMARY")
    print("=" * 50)

    total_tests = 0
    passed_tests = 0
    overall_success = True

    for category, test_results in results.items():
        print(f"\n🎯 {category}:")
        category_passed = 0
        category_total = len(test_results)

        for status, message in test_results:
            print(f"   {message}")
            if status:
                category_passed += 1
            else:
                overall_success = False

        total_tests += category_total
        passed_tests += category_passed

        pass_rate = (category_passed / category_total * 100) if category_total > 0 else 0
        print(
            f"   Category Status: {category_passed}/{category_total} ({pass_rate:.1f}% pass rate)"
        )

    print("\n" + "=" * 50)
    overall_pass_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
    print(f"🎯 OVERALL RESULTS: {passed_tests}/{total_tests} ({overall_pass_rate:.1f}% pass rate)")

    if overall_success:
        print("🎉 PHASE 3 VALIDATION: SUCCESS!")
        print("✅ All release optimization deliverables validated")
    else:
        print("⚠️ PHASE 3 VALIDATION: ISSUES FOUND")
        print("❌ Some deliverables need attention")

    return overall_success


def main():
    """Main validation function."""
    # Change to project root if needed
    if Path("src/marketpipe").exists():
        os.chdir(".")
    elif Path("../src/marketpipe").exists():
        os.chdir("..")
    else:
        print("❌ Could not find MarketPipe project root")
        sys.exit(1)

    print(f"📁 Validating from: {Path.cwd()}")
    print()

    # Run validation
    results = validate_phase3_deliverables()

    # Print summary
    success = print_validation_summary(results)

    print("\n🚀 Phase 3 Release Optimization Status:")
    if success:
        print("✅ Ready for production release!")
        print("✅ Professional documentation complete")
        print("✅ Repository optimized for public distribution")
        print("✅ All quality standards met")
    else:
        print("⚠️ Complete remaining tasks before release")

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
