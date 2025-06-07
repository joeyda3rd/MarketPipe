#!/bin/bash
# Ubiquitous Language Compliance Check for MarketPipe

set -e

echo "🗣️ Checking ubiquitous language compliance..."

# Define banned terms and their suggested replacements
declare -A banned_terms=(
    ["ticker"]="symbol"
    ["security"]="symbol" 
    ["instrument"]="symbol"
    ["candle"]="ohlcv_bar"
    ["quote"]="ohlcv_bar"
    ["price_data"]="ohlcv_bar"
    ["business_date"]="trading_date"
    ["market_date"]="trading_date"
    ["import"]="ingestion"
    ["fetch"]="ingestion"
    ["load"]="ingestion"
    ["vendor"]="market_data_provider"
    ["feed"]="data_feed"
    ["source"]="market_data_provider"
    ["verification"]="validation"
    ["check"]="validation"
)

# Files to check
domain_files="src/marketpipe/domain"
all_files="src/marketpipe"

violations_found=0

# Function to check for banned terms
check_banned_terms() {
    local file_pattern=$1
    local context=$2
    
    echo "Checking $context..."
    
    for term in "${!banned_terms[@]}"; do
        suggestion="${banned_terms[$term]}"
        
        # Search for the banned term (case insensitive, word boundaries)
        if grep -r -i -n --include="*.py" "\b$term\b" $file_pattern 2>/dev/null; then
            echo "❌ Found banned term '$term' in $context"
            echo "💡 Suggestion: Use '$suggestion' instead"
            echo ""
            violations_found=$((violations_found + 1))
        fi
    done
}

# Check domain layer (stricter)
if [ -d "$domain_files" ]; then
    check_banned_terms "$domain_files" "domain layer"
else
    echo "ℹ️ Domain layer not found, skipping domain-specific checks"
fi

# Check for generic terms that should be more specific
echo "Checking for overly generic terms..."

generic_terms=("data" "item" "record" "object" "thing")

for term in "${generic_terms[@]}"; do
    if grep -r -i -n --include="*.py" "class.*$term[^a-zA-Z]" "$all_files" 2>/dev/null; then
        echo "⚠️ Found generic term '$term' in class name"
        echo "💡 Suggestion: Use more specific domain terminology"
        echo ""
        violations_found=$((violations_found + 1))
    fi
done

# Check for proper use of approved domain terms
echo "Checking for consistent use of approved terms..."

approved_patterns=(
    "symbol.*Symbol"
    "ohlcv.*OHLCV"
    "trading_date.*trading.date"
    "market_data_provider.*provider"
)

# Check function and variable naming consistency
echo "Checking naming consistency..."

if grep -r -n --include="*.py" "def.*fetch.*data" "$all_files" 2>/dev/null; then
    echo "⚠️ Found 'fetch_data' pattern - consider using 'ingest_data' for consistency"
    violations_found=$((violations_found + 1))
fi

if grep -r -n --include="*.py" "def.*get.*ticker" "$all_files" 2>/dev/null; then
    echo "⚠️ Found 'ticker' in function names - use 'symbol' instead"
    violations_found=$((violations_found + 1))
fi

# Check docstring consistency
echo "Checking docstring terminology..."

if grep -r -i -n --include="*.py" '""".*ticker.*"""' "$all_files" 2>/dev/null; then
    echo "⚠️ Found 'ticker' in docstrings - use 'symbol' for consistency"
    violations_found=$((violations_found + 1))
fi

if grep -r -i -n --include="*.py" '""".*fetch.*"""' "$all_files" 2>/dev/null; then
    echo "⚠️ Found 'fetch' in docstrings - consider using 'ingest' for domain operations"
    violations_found=$((violations_found + 1))
fi

# Summary
echo ""
echo "===================="
echo "UBIQUITOUS LANGUAGE COMPLIANCE SUMMARY"
echo "===================="

if [ $violations_found -eq 0 ]; then
    echo "✅ No ubiquitous language violations found!"
    echo "Great job maintaining domain language consistency."
else
    echo "❌ Found $violations_found ubiquitous language violations"
    echo ""
    echo "Please address these issues to maintain domain language consistency:"
    echo "1. Replace banned terms with approved domain terminology"
    echo "2. Use specific domain concepts instead of generic terms"
    echo "3. Ensure consistency across code, comments, and documentation"
    echo ""
    echo "For reference, see the ubiquitous language guide in:"
    echo "- CLAUDE.md (Domain-Driven Design section)"
    echo "- .cursor/rules/ddd/ubiquitous_language.mdc"
    
    exit 1
fi

echo ""
echo "Domain terminology reference:"
echo "- Use 'symbol' (not ticker, security, instrument)"
echo "- Use 'ohlcv_bar' (not candle, quote, price_data)"
echo "- Use 'trading_date' (not business_date, market_date)"
echo "- Use 'ingestion' (not import, fetch, load)"
echo "- Use 'market_data_provider' (not vendor, source)"
echo "- Use 'validation' (not verification, check)"