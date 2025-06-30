#!/bin/bash
#
# Setup Test Hooks - MarketPipe
# Installs git hooks and configures automatic test detection
#

set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "🔧 Setting up MarketPipe test automation..."
echo "========================================"

# Check dependencies
echo "📋 Checking dependencies..."

# Check if pytest-xdist is installed
if ! python3 -c "import xdist" 2>/dev/null; then
    echo "⚠️  Installing pytest-xdist for parallel testing..."
    pip install pytest-xdist
fi

# Check if git is available
if ! command -v git &> /dev/null; then
    echo "❌ Git is required but not installed"
    exit 1
fi

# Install git hooks
echo "🔗 Installing git hooks..."

# Create .git/hooks directory if it doesn't exist
mkdir -p .git/hooks

# Install pre-commit hook
if [ -f ".githooks/pre-commit" ]; then
    cp .githooks/pre-commit .git/hooks/pre-commit
    chmod +x .git/hooks/pre-commit
    echo "✅ Pre-commit hook installed"
else
    echo "❌ Pre-commit hook file not found at .githooks/pre-commit"
    exit 1
fi

# Create git config for core.hooksPath (alternative approach)
git config core.hooksPath .githooks

# Setup test result tracking directory
mkdir -p .pytest_cache
echo "📁 Test cache directory ready"

# Create environment configuration for skipping tests
echo "⚙️  Setting up environment options..."
echo ""
echo "Environment variables you can use:"
echo "  SKIP_TESTS=1        Skip all tests in git hooks"
echo "  PYTEST_ARGS='...'   Pass custom args to pytest"
echo ""

# Verify installation by running a test
echo "🧪 Testing the setup..."
echo ""

# Test the smart test runner
if python3 scripts/smart_test_runner.py --help > /dev/null 2>&1; then
    echo "✅ Smart test runner is working"
else
    echo "❌ Smart test runner failed"
    exit 1
fi

# Test git hook (dry run)
if [ -x .githooks/pre-commit ]; then
    echo "✅ Pre-commit hook is executable"
else
    echo "❌ Pre-commit hook is not executable"
    exit 1
fi

echo ""
echo "🎉 Test automation setup complete!"
echo ""
echo "📚 Usage:"
echo "  make test-smart         # Run tests for changed files"
echo "  make test-smart-all     # Show what tests would run"
echo "  make test-smart-cmd     # Get command to copy-paste"
echo ""
echo "🚫 To skip tests:"
echo "  SKIP_TESTS=1 git commit    # Skip pre-commit tests"
echo "  git commit --no-verify     # Skip all git hooks"
echo ""
echo "⚡ Fast development cycle:"
echo "  1. Edit code"
echo "  2. git add ."
echo "  3. git commit (tests run automatically)"
echo "  4. Before pushing: make test-all"
echo "" 