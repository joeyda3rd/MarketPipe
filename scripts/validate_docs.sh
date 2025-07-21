#!/bin/bash
# Documentation Validation Script for MarketPipe
# This script validates the documentation for quality, consistency, and completeness

set -e

DOCS_DIR="docs"
MAIN_DOCS=$(find "$DOCS_DIR" -name "*.md" -not -path "*/archive/*")
EXIT_CODE=0

echo "🔍 MarketPipe Documentation Validation"
echo "======================================="
echo

# Function to report issues
report_issue() {
    echo "❌ $1"
    EXIT_CODE=1
}

report_success() {
    echo "✅ $1"
}

# Check 1: Required files exist
echo "📋 Checking required documentation files..."
required_files=(
    "docs/README.md"
    "docs/getting_started.md"
    "docs/mkdocs.yml"
    "docs/user_guide/cli_usage.md"
    "docs/user_guide/configuration.md"
    "docs/user_guide/monitoring.md"
    "docs/user_guide/troubleshooting.md"
    "docs/developer_guide/contributing.md"
    "docs/developer_guide/architecture.md"
    "docs/developer_guide/testing.md"
)

for file in "${required_files[@]}"; do
    if [[ -f "$file" ]]; then
        report_success "Required file exists: $file"
    else
        report_issue "Missing required file: $file"
    fi
done
echo

# Check 2: Markdown file headers
echo "📝 Checking markdown file headers..."
for file in $MAIN_DOCS; do
    if head -n 1 "$file" | grep -q "^# "; then
        report_success "Valid header in: $(basename "$file")"
    else
        report_issue "Missing or invalid H1 header in: $file"
    fi
done
echo

# Check 3: File naming conventions
echo "🏷️  Checking file naming conventions..."
for file in $MAIN_DOCS; do
    basename_file=$(basename "$file" .md)
    if [[ "$basename_file" =~ ^[a-z0-9_]+$ ]] || [[ "$basename_file" == "README" ]] || [[ "$basename_file" == "TRANSFORMATION_SUMMARY" ]]; then
        report_success "Valid filename: $(basename "$file")"
    else
        report_issue "Invalid filename (should be snake_case): $file"
    fi
done
echo

# Check 4: Code blocks have language hints
echo "💻 Checking code block formatting..."
for file in $MAIN_DOCS; do
    # Look for code blocks without language hints
    if grep -n '```$' "$file" > /dev/null 2>&1; then
        report_issue "Code blocks without language hints found in: $file"
        echo "   Lines: $(grep -n '```$' "$file" | cut -d: -f1 | tr '\n' ' ')"
    else
        report_success "All code blocks have language hints: $(basename "$file")"
    fi
done
echo

# Check 5: No TODO markers in main docs
echo "📝 Checking for TODO markers..."
for file in $MAIN_DOCS; do
    if grep -i "TODO\|FIXME\|XXX" "$file" > /dev/null 2>&1; then
        report_issue "TODO markers found in: $file"
        grep -n -i "TODO\|FIXME\|XXX" "$file" | head -3
    else
        report_success "No TODO markers: $(basename "$file")"
    fi
done
echo

# Check 6: Reasonable file sizes (not too short, not too long)
echo "📏 Checking file sizes..."
for file in $MAIN_DOCS; do
    lines=$(wc -l < "$file")
    if [[ $lines -lt 20 ]]; then
        report_issue "File too short ($lines lines): $file"
    elif [[ $lines -gt 2000 ]]; then
        report_issue "File too long ($lines lines, consider splitting): $file"
    else
        report_success "Reasonable length ($lines lines): $(basename "$file")"
    fi
done
echo

# Check 7: MkDocs configuration
echo "⚙️  Checking MkDocs configuration..."
if [[ -f "docs/mkdocs.yml" ]]; then
    if grep -q "site_name:" "docs/mkdocs.yml" && grep -q "nav:" "docs/mkdocs.yml"; then
        report_success "MkDocs configuration appears valid"
    else
        report_issue "MkDocs configuration missing required fields"
    fi

    # Check if all main docs are referenced in navigation
    nav_files=$(grep -A 50 "nav:" "docs/mkdocs.yml" | grep "\.md" | sed 's/.*: *//' | sed 's/#.*//')
    for file in $MAIN_DOCS; do
        relative_path=${file#docs/}
        if echo "$nav_files" | grep -q "$relative_path"; then
            report_success "File in navigation: $(basename "$file")"
        else
            # Skip TRANSFORMATION_SUMMARY as it's internal
            if [[ "$relative_path" != "TRANSFORMATION_SUMMARY.md" ]]; then
                report_issue "File not in navigation: $relative_path"
            fi
        fi
    done
else
    report_issue "MkDocs configuration file missing"
fi
echo

# Check 8: Archive directory properly organized
echo "🗂️  Checking archive organization..."
if [[ -d "docs/archive" ]]; then
    archived_count=$(find docs/archive -name "*.md" | wc -l)
    report_success "Archive directory exists with $archived_count archived files"
else
    report_issue "Archive directory missing"
fi
echo

# Summary
echo "📊 Validation Summary"
echo "==================="
total_files=$(echo "$MAIN_DOCS" | wc -w)
echo "Total documentation files checked: $total_files"

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "🎉 All validation checks passed!"
    echo
    echo "Documentation is ready for:"
    echo "  • markdownlint validation"
    echo "  • spell checking with codespell"
    echo "  • link validation"
    echo "  • MkDocs site generation"
else
    echo "⚠️  Some validation issues found. Please address them before proceeding."
fi

echo
echo "Next steps:"
echo "  1. Run: markdownlint docs/**/*.md"
echo "  2. Run: codespell docs/"
echo "  3. Run: mkdocs serve (to test site generation)"
echo "  4. Validate links with your preferred link checker"

exit $EXIT_CODE
