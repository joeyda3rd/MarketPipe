#!/bin/bash
# MarketPipe development setup
# Usage: scripts/setup

set -e

echo "🚀 Setting up MarketPipe development environment..."

# Check Python version
if ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 9) else 1)" 2>/dev/null; then
    echo "❌ Python 3.9+ required"
    exit 1
fi

# Install in development mode
echo "📦 Installing MarketPipe in development mode..."
pip install -e .

# Install development dependencies if available
if grep -q "dev" pyproject.toml; then
    echo "🔧 Installing development dependencies..."
    pip install -e ".[dev]" 2>/dev/null || echo "⚠️  Dev dependencies not available"
fi

# Create sample config if it doesn't exist
if [ ! -f "config.yaml" ]; then
    echo "📝 Creating sample configuration..."
    cat > config.yaml << 'EOF'
# MarketPipe sample configuration
# Copy from config/example_config.yaml and customize

symbols:
  - AAPL
  - GOOGL

start: "2024-01-01"
end: "2024-01-02"

provider: fake  # No API keys needed for fake provider
output_path: "./data"
EOF
fi

# Create .env template if it doesn't exist
if [ ! -f ".env" ]; then
    echo "🔐 Creating .env template..."
    cat > .env << 'EOF'
# MarketPipe environment variables
# Uncomment and fill in your API keys

# ALPACA_KEY=your_key_here
# ALPACA_SECRET=your_secret_here
# IEX_TOKEN=your_token_here
EOF
fi

# Run a quick test to make sure everything works
echo "🧪 Running quick health check..."
if python -c "import marketpipe; print('✅ MarketPipe imported successfully')" 2>/dev/null; then
    echo "✅ Setup complete!"
    echo ""
    echo "🎯 Next steps:"
    echo "  1. Try the demo:     scripts/demo"
    echo "  2. Run tests:        pytest"
    echo "  3. Format code:      scripts/format"
    echo "  4. Watch tests:      scripts/watch"
    echo ""
    echo "📖 See CONTRIBUTING.md for more details"
else
    echo "❌ Setup failed - MarketPipe not importable"
    exit 1
fi
