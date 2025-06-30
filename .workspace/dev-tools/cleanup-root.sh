#!/bin/bash
# MarketPipe Root Directory Cleanup
# Organizes development artifacts and removes clutter to make repo contributor-friendly

set -e

echo "🧹 Cleaning up MarketPipe root directory for contributor-friendliness..."

# Create organized directories
mkdir -p dev/databases
mkdir -p dev/reports  
mkdir -p dev/temp
mkdir -p dev/venvs
mkdir -p dev/cache
mkdir -p .artifacts

# Move database files out of root
echo "📁 Moving database files..."
find . -maxdepth 1 -name "*.db" -not -path "./.git/*" -exec mv {} dev/databases/ \; 2>/dev/null || true
find . -maxdepth 1 -name "*.duckdb" -not -path "./.git/*" -exec mv {} dev/databases/ \; 2>/dev/null || true

# Move virtual environments
echo "📁 Moving virtual environments..."
[ -d "venv" ] && mv venv dev/venvs/ 2>/dev/null || true
[ -d "test_venv" ] && mv test_venv dev/venvs/ 2>/dev/null || true

# Move cache directories
echo "📁 Moving cache directories..."
[ -d ".mypy_cache" ] && mv .mypy_cache dev/cache/ 2>/dev/null || true
[ -d ".ruff_cache" ] && mv .ruff_cache dev/cache/ 2>/dev/null || true
[ -d "htmlcov" ] && mv htmlcov dev/cache/ 2>/dev/null || true
[ -d "build" ] && mv build dev/cache/ 2>/dev/null || true

# Move test outputs and temporary files
echo "📁 Moving test outputs..."
[ -d "test_output" ] && mv test_output dev/temp/ 2>/dev/null || true
[ -d "reports" ] && mv reports dev/reports/ 2>/dev/null || true
[ -f "test_metrics_demo.py" ] && mv test_metrics_demo.py dev/temp/ 2>/dev/null || true
[ -f "minimal_test.yaml" ] && mv minimal_test.yaml dev/temp/ 2>/dev/null || true
[ -f ".pip_freeze_snapshot.txt" ] && mv .pip_freeze_snapshot.txt dev/temp/ 2>/dev/null || true

# Move workbook and backlogs to dev
echo "📁 Moving development artifacts..."
[ -d "workbook" ] && mv workbook dev/ 2>/dev/null || true
[ -d "backlogs" ] && mv backlogs dev/ 2>/dev/null || true
[ -d "archive" ] && mv archive dev/ 2>/dev/null || true

# Update .gitignore to exclude new dev directories
echo "📝 Updating .gitignore..."
cat >> .gitignore << 'EOF'

# Development artifacts (keep clean root)
dev/databases/
dev/reports/
dev/temp/
dev/venvs/
dev/cache/
.artifacts/

# Additional cache and temp files
*.duckdb
*.db
test_output/
workbook/
backlogs/
archive/
reports/
venv/
test_venv/
EOF

echo "✅ Root directory cleanup complete!"
echo ""
echo "📋 Summary of changes:"
echo "  • Database files → dev/databases/"
echo "  • Virtual environments → dev/venvs/"
echo "  • Cache directories → dev/cache/"
echo "  • Test outputs → dev/temp/"
echo "  • Development artifacts → dev/"
echo ""
echo "💡 The root is now clean and contributor-friendly!" 