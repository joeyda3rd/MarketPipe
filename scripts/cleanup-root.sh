#!/bin/bash
# Root directory cleanup script for MarketPipe
# Removes common development artifacts and temporary files

set -e

echo "🧹 Cleaning up MarketPipe root directory..."

# Remove coverage files
echo "  📊 Removing coverage files..."
find . -maxdepth 1 -name ".coverage*" -delete 2>/dev/null || true

# Remove cache directories
echo "  🗂️  Removing cache directories..."
rm -rf .pytest_cache .ruff_cache htmlcov 2>/dev/null || true

# Remove common temporary files
echo "  🗑️  Removing temporary files..."
find . -maxdepth 1 \( -name "*.tmp" -o -name "*.log" -o -name "*.bak" -o -name "*~" \) -delete 2>/dev/null || true

# Remove any stray database files in root
echo "  🗄️  Removing stray database files..."
find . -maxdepth 1 -name "*.db" -delete 2>/dev/null || true

# Remove any failed pip install artifacts
echo "  📦 Removing pip artifacts..."
find . -maxdepth 1 -name "=*" -delete 2>/dev/null || true

echo "✅ Root directory cleanup complete!"
echo ""
echo "Current root structure:"
ls -la | grep -E '^(d|-)' 