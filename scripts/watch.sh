#!/bin/bash
# Watch mode for MarketPipe development
# Usage: scripts/watch

echo "👀 Starting MarketPipe watch mode..."
echo "   Watching src/ and tests/ for changes"
echo "   Press Ctrl+C to stop"
echo ""

# Check if we have pytest-watch or entr available
if command -v ptw &> /dev/null; then
    echo "🔍 Using pytest-watch..."
    ptw --runner "pytest --tb=short -x"
elif command -v entr &> /dev/null; then
    echo "🔍 Using entr for file watching..."
    find src/ tests/ -name "*.py" | entr -c pytest --tb=short -x
elif command -v inotifywait &> /dev/null; then
    echo "🔍 Using inotifywait for file watching..."
    while true; do
        inotifywait -e modify -r src/ tests/ --format '%w%f' 2>/dev/null
        echo "📝 Files changed, running tests..."
        pytest --tb=short -x || true
        echo ""
        echo "👀 Waiting for changes..."
    done
else
    echo "⚠️  No file watcher found. Install one of:"
    echo "   pip install pytest-watch"
    echo "   apt install entr        # On Ubuntu/Debian"
    echo "   brew install entr       # On macOS"
    echo ""
    echo "🔄 Falling back to manual watch loop..."
    echo "   Running tests every 5 seconds..."

    while true; do
        sleep 5
        echo "🧪 Running tests..."
        pytest --tb=short -x || true
        echo ""
    done
fi
