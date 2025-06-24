#!/bin/bash
# Root directory cleanup script for MarketPipe
# Removes common development artifacts and temporary files

set -e

echo "🧹 Cleaning up MarketPipe root directory..."

# Remove coverage files
echo "  📊 Removing coverage files..."
find . -maxdepth 1 -name ".coverage*" -delete 2>/dev/null || true
rm -f coverage.xml htmlcov/ 2>/dev/null || true

# Remove cache directories
echo "  🗂️  Removing cache directories..."
rm -rf .pytest_cache .ruff_cache .mypy_cache .import_linter_cache htmlcov 2>/dev/null || true

# Remove common temporary files
echo "  🗑️  Removing temporary files..."
find . -maxdepth 1 \( -name "*.tmp" -o -name "*.log" -o -name "*.bak" -o -name "*~" \) -delete 2>/dev/null || true

# Remove any stray database files in root
echo "  🗄️  Removing stray database files..."
find . -maxdepth 1 -name "*.db" -delete 2>/dev/null || true

# Remove any failed pip install artifacts
echo "  📦 Removing pip artifacts..."
find . -maxdepth 1 -name "=*" -delete 2>/dev/null || true

# Remove development debugging files
echo "  🐛 Removing debug files..."
rm -f debug_cli.py debug_*.py 2>/dev/null || true

# Remove large generated files
echo "  📄 Removing large generated files..."
rm -f marketpipe-copyright-check.json 2>/dev/null || true

# Remove Python bytecode
echo "  🐍 Removing Python bytecode..."
find . -name "*.pyc" -delete 2>/dev/null || true
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

echo "✅ Root directory cleanup complete!"
echo ""
echo "📁 Current root structure:"
ls -lah | grep -E '^(d|-)' | wc -l | xargs echo "Total items:"
echo ""
echo "💾 Disk usage:"
du -sh . 2>/dev/null || echo "Unable to calculate disk usage" 