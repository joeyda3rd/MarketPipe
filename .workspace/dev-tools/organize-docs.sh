#!/bin/bash
# Organize Documentation Files
# Moves scattered documentation from root to proper directories for better organization

set -e

echo "📚 Organizing documentation for contributor-friendliness..."

# Create directories
mkdir -p dev/internal
mkdir -p docs/guides

# Move internal development documentation to dev/
echo "📁 Moving internal development docs..."
[ -f "CLI_VALIDATION_FRAMEWORK.md" ] && mv CLI_VALIDATION_FRAMEWORK.md dev/internal/ 2>/dev/null || true
[ -f "FAST_TESTING_SETUP.md" ] && mv FAST_TESTING_SETUP.md dev/internal/ 2>/dev/null || true
[ -f "PLAN.md" ] && mv PLAN.md dev/internal/ 2>/dev/null || true
[ -f "AGENTS.md" ] && mv AGENTS.md dev/internal/ 2>/dev/null || true
[ -f "CLAUDE.md" ] && mv CLAUDE.md dev/internal/ 2>/dev/null || true

# Move large reference files to docs/
echo "📁 Moving reference documentation..."
[ -f "CLI_COMMANDS_REFERENCE.md" ] && mv CLI_COMMANDS_REFERENCE.md docs/ 2>/dev/null || true

# Clean up temporary and generated files
echo "🧹 Cleaning up temporary files..."
[ -f "bandit-report.json" ] && mv bandit-report.json dev/temp/ 2>/dev/null || true

# Keep essential files in root
echo "✅ Keeping essential files in root:"
echo "  • README.md (main project intro)"
echo "  • CONTRIBUTING.md (contributor guide)"
echo "  • CHANGELOG.md (release history)"
echo "  • LICENSE (legal)"
echo "  • TODO.md (project status)"

# Update .gitignore for new organization
echo "📝 Updating .gitignore for documentation organization..."
cat >> .gitignore << 'EOF'

# Internal development documentation
dev/internal/
bandit-report.json
EOF

echo "✅ Documentation organization complete!"
echo ""
echo "📋 New structure:"
echo "  • Root: Only essential files (README, CONTRIBUTING, LICENSE, etc.)"
echo "  • docs/: User and developer documentation"
echo "  • dev/internal/: Internal development notes"
echo "  • .workspace/: Advanced features and tools"
echo ""
echo "💡 Root directory is now much cleaner and more welcoming!" 