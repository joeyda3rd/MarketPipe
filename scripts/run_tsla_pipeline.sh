#!/bin/bash

# TSLA One Year Pipeline Launcher
# Simple wrapper script to run the complete TSLA data pipeline

set -e  # Exit on any error

echo "🚀 TSLA One Year Data Pipeline"
echo "================================"

# Check if we're in the right directory
if [[ ! -f "scripts/tsla_one_year_pipeline.py" ]]; then
    echo "❌ Error: Please run this script from the MarketPipe project root directory"
    echo "   cd /path/to/MarketPipe && ./scripts/run_tsla_pipeline.sh"
    exit 1
fi

# Check for credentials
if [[ -z "$ALPACA_KEY" || -z "$ALPACA_SECRET" ]]; then
    echo "⚠️  Warning: Alpaca credentials not found in environment"
    echo "   Please set ALPACA_KEY and ALPACA_SECRET environment variables"
    echo "   Or create a .env file in the project root with these variables"
    echo ""
    echo "   Example:"
    echo "   export ALPACA_KEY=\"your_api_key_here\""
    echo "   export ALPACA_SECRET=\"your_api_secret_here\""
    echo ""
    read -p "Do you want to continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Show estimated resource usage
echo "📊 Pipeline Overview:"
echo "   Symbol: TSLA"
echo "   Timeframe: 1 year (ending last trading day)"
echo "   Data source: Alpaca Markets (IEX feed)"
echo "   Estimated download: ~100-500MB"
echo "   Estimated runtime: 15-30 minutes"
echo ""

# Confirm execution
read -p "Start the pipeline? (Y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Nn]$ ]]; then
    echo "Pipeline cancelled"
    exit 0
fi

# Run the pipeline
echo "🔧 Starting TSLA One Year Pipeline..."
python scripts/tsla_one_year_pipeline.py

# Check exit code
if [[ $? -eq 0 ]]; then
    echo ""
    echo "🎉 Pipeline completed successfully!"
    echo ""
    echo "📁 Data is available in:"
    echo "   Raw 1m data:      data/raw/symbol=TSLA/"
    echo "   Aggregated data:  data/aggregated/"
    echo "   Quality reports:  data/validation_reports/"
    echo "   Summary report:   data/tsla_one_year_summary.json"
    echo ""
    echo "📊 Quick data check:"
    if command -v python &> /dev/null; then
        python -m marketpipe query "SELECT COUNT(*) as total_bars FROM 'data/raw/**/*.parquet' WHERE symbol = 'TSLA'" 2>/dev/null || echo "   (Install dependencies to run queries)"
    fi
    echo ""
    echo "🚀 Your TSLA data is ready for signal generation!"
else
    echo ""
    echo "❌ Pipeline failed. Check the logs above for details."
    exit 1
fi 