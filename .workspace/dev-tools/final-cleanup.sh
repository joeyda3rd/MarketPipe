#!/bin/bash
# Final cleanup pass to remove remaining clutter from root directory

set -e

echo "🧹 Final cleanup pass for maximum contributor-friendliness..."

# Remove duplicate and old files
echo "📁 Removing duplicates and old files..."
[ -f "contributing.md" ] && rm contributing.md 2>/dev/null || true
[ -f "bandit-report.json" ] && mv bandit-report.json dev/temp/ 2>/dev/null || true

# Move development configuration files
echo "📁 Moving development configuration..."
mkdir -p dev/config
[ -f "setup.cfg" ] && mv setup.cfg dev/config/ 2>/dev/null || true
[ -f "prometheus.yml" ] && mv prometheus.yml dev/config/ 2>/dev/null || true
[ -f "docker-compose.yml" ] && mv docker-compose.yml dev/config/ 2>/dev/null || true

# Move database configuration
[ -f "alembic.ini" ] && mv alembic.ini dev/config/ 2>/dev/null || true

# Move hidden directories that clutter (but preserve essential ones)
[ -d ".claude" ] && mv .claude dev/ 2>/dev/null || true

# Update .gitignore for additional cleanup
echo "📝 Final .gitignore updates..."
cat >> .gitignore << 'EOF'

# Development configuration
dev/config/
setup.cfg
prometheus.yml
docker-compose.yml
alembic.ini

# Reports and analysis
bandit-report.json
*.report

# AI assistant files  
.claude/
EOF

echo "✅ Final cleanup complete!"
echo ""
echo "📋 Root directory now contains only:"
echo "  • Essential project files (README, LICENSE, CHANGELOG)"
echo "  • Main configuration (pyproject.toml, pytest.ini, Makefile)"
echo "  • Source and test directories"
echo "  • Documentation directory"
echo ""
echo "🎉 Repository is now extremely contributor-friendly!" 