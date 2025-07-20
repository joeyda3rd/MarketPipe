#!/bin/bash
# Clean up MarketPipe generated files and caches
# Usage: scripts/clean [--all]

echo "🧹 Cleaning up MarketPipe artifacts..."

# Python cache files
echo "🐍 Removing Python cache files..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true
find . -name "*.pyo" -delete 2>/dev/null || true

# Test artifacts
echo "🧪 Removing test artifacts..."
rm -rf .pytest_cache/
rm -rf .coverage
rm -rf htmlcov/

# Build artifacts
echo "📦 Removing build artifacts..."
rm -rf build/
rm -rf dist/
rm -rf *.egg-info/

# Data and cache directories (if they exist)
if [ -d "data/" ]; then
    echo "📊 Removing generated data..."
    rm -rf data/
fi

if [ -d ".cache/" ]; then
    echo "💾 Removing cache directory..."
    rm -rf .cache/
fi

# Temporary files
echo "🗑️  Removing temporary files..."
find . -name "*.tmp" -delete 2>/dev/null || true
find . -name "*.log" -delete 2>/dev/null || true
find . -name ".DS_Store" -delete 2>/dev/null || true

# If --all flag is used, clean workspace too
if [ "$1" = "--all" ]; then
    echo "🏠 Cleaning workspace artifacts..."
    rm -rf .workspace/artifacts/ 2>/dev/null || true
    rm -rf .workspace/test-cache/ 2>/dev/null || true
fi

echo "✅ Cleanup complete!"

# Show what's left in the directory
echo ""
echo "📂 Current directory contents:"
ls -la | grep -E "^(d|l)" | head -10
