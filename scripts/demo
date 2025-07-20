#!/bin/bash
# MarketPipe quick demo
# Usage: scripts/demo

set -e

echo "🎬 MarketPipe Demo - Collecting sample market data..."

# Use fake provider so no API keys needed
echo "📊 Ingesting sample data (fake provider - no API keys needed)..."
marketpipe ingest --provider fake --symbols AAPL GOOGL MSFT --start 2024-01-01 --end 2024-01-02

echo ""
echo "📈 Sample data collected! Let's query it..."

# Show what we collected
echo ""
echo "🔍 Querying AAPL data:"
marketpipe query --symbol AAPL --limit 5

echo ""
echo "📊 Data summary:"
marketpipe query --symbol AAPL --summary

echo ""
echo "🎉 Demo complete!"
echo ""
echo "📂 Data saved to: ./data/"
echo "🔍 Explore with: marketpipe query --symbol <SYMBOL>"
echo "📖 More examples in: examples/"
