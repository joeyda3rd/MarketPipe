#!/usr/bin/env python3
"""
TSLA Data Verification Script

Verifies what TSLA data we actually have vs what was requested.
Provides accurate assessment for signal generation readiness.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, Tuple


def log_and_print(message: str, level: str = "INFO") -> None:
    """Log and print message with color coding."""
    colors = {
        "INFO": "\033[94m",      # Blue
        "SUCCESS": "\033[92m",   # Green
        "WARNING": "\033[93m",   # Yellow
        "ERROR": "\033[91m",     # Red
        "HIGHLIGHT": "\033[95m", # Magenta
    }
    reset = "\033[0m"

    colored_message = f"{colors.get(level, '')}{message}{reset}"
    print(colored_message)

def run_query(query: str) -> Tuple[bool, str]:
    """Run a DuckDB query via marketpipe."""
    try:
        result = subprocess.run(
            ["python", "-m", "marketpipe", "query", query],
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.returncode == 0, result.stdout
    except Exception as e:
        return False, str(e)

def get_tsla_summary() -> Dict[str, Any]:
    """Get comprehensive TSLA data summary."""
    summary = {}

    # Total bars
    success, output = run_query(
        "SELECT COUNT(*) as total_bars FROM 'data/raw/**/*.parquet' WHERE symbol = 'TSLA'"
    )
    if success:
        for line in output.split('\n'):
            if line.strip().isdigit():
                summary['total_bars'] = int(line.strip())
                break

    # Date range
    success, output = run_query(
        "SELECT MIN(date) as start_date, MAX(date) as end_date, COUNT(DISTINCT date) as trading_days FROM 'data/raw/**/*.parquet' WHERE symbol = 'TSLA'"
    )
    if success:
        lines = output.strip().split('\n')
        for line in lines:
            if '-' in line and len(line.split()) >= 3:
                parts = line.split()
                summary['start_date'] = parts[0]
                summary['end_date'] = parts[1]
                try:
                    summary['trading_days'] = int(parts[2])
                except:
                    summary['trading_days'] = 0
                break

    # Daily breakdown
    success, output = run_query(
        "SELECT date, COUNT(*) as bars_per_day FROM 'data/raw/**/*.parquet' WHERE symbol = 'TSLA' GROUP BY date ORDER BY date"
    )
    daily_data = []
    if success:
        lines = output.strip().split('\n')
        for line in lines:
            if '-' in line and line.count('-') >= 2:  # Date format
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        daily_data.append({
                            'date': parts[0],
                            'bars': int(parts[1])
                        })
                    except:
                        continue
    summary['daily_breakdown'] = daily_data

    return summary

def assess_data_quality(summary: Dict[str, Any]) -> Dict[str, Any]:
    """Assess data quality for trading applications."""
    assessment = {
        'total_bars': summary.get('total_bars', 0),
        'trading_days': summary.get('trading_days', 0),
        'date_range': f"{summary.get('start_date', 'N/A')} to {summary.get('end_date', 'N/A')}",
        'data_vintage': 'Unknown',
        'completeness': 0,
        'signal_ready': False,
        'recommended_action': 'Unknown'
    }

    total_bars = summary.get('total_bars', 0)
    start_date = summary.get('start_date')

    # Determine data vintage
    if start_date:
        try:
            start_year = int(start_date.split('-')[0])
            current_year = datetime.now().year
            age_years = current_year - start_year

            if age_years >= 4:
                assessment['data_vintage'] = f'Very Old ({age_years} years old)'
            elif age_years >= 2:
                assessment['data_vintage'] = f'Old ({age_years} years old)'
            elif age_years >= 1:
                assessment['data_vintage'] = f'Somewhat Old ({age_years} year old)'
            else:
                assessment['data_vintage'] = 'Recent'
        except:
            pass

    # Calculate completeness
    if total_bars > 0:
        # For 1 trading day = ~390 bars
        expected_bars_per_day = 390
        trading_days = summary.get('trading_days', 0)
        if trading_days > 0:
            expected_total = trading_days * expected_bars_per_day
            assessment['completeness'] = min(100, round((total_bars / expected_total) * 100, 1))

    # Assess signal readiness
    if total_bars >= 50000:  # ~125+ trading days
        assessment['signal_ready'] = True
        assessment['recommended_action'] = 'Ready for signal generation and backtesting'
    elif total_bars >= 20000:  # ~50+ trading days
        assessment['signal_ready'] = True
        assessment['recommended_action'] = 'Suitable for basic signal testing'
    elif total_bars >= 10000:  # ~25+ trading days
        assessment['signal_ready'] = False
        assessment['recommended_action'] = 'Limited data - good for initial exploration'
    else:
        assessment['signal_ready'] = False
        assessment['recommended_action'] = 'Insufficient data for reliable signal work'

    return assessment

def generate_recommendations(assessment: Dict[str, Any]) -> list:
    """Generate actionable recommendations."""
    recommendations = []

    total_bars = assessment['total_bars']
    vintage = assessment['data_vintage']

    if 'Very Old' in vintage or 'Old' in vintage:
        recommendations.append("⚠️ Data is from 2020 - consider if this affects your signal relevance")
        recommendations.append("💡 Modern market conditions may differ significantly from 2020")

    if total_bars < 50000:
        recommendations.append("📈 Consider downloading more symbols (AAPL, GOOGL, NVDA) for broader testing")
        recommendations.append("⏰ More historical periods needed for robust backtesting")

    if total_bars >= 20000:
        recommendations.append("✅ Current dataset suitable for initial signal development")
        recommendations.append("🧪 Good for testing basic trading strategies")

    if 'Very Old' in vintage:
        recommendations.append("🎯 For production signals, consider upgrading to paid data feed")
        recommendations.append("📊 Current data excellent for learning and algorithm development")

    return recommendations

def main():
    """Run TSLA data verification."""
    log_and_print("=" * 70, "INFO")
    log_and_print("🔍 TSLA DATA VERIFICATION REPORT", "HIGHLIGHT")
    log_and_print("=" * 70, "INFO")

    # Get data summary
    log_and_print("Analyzing TSLA dataset...", "INFO")
    summary = get_tsla_summary()

    if not summary.get('total_bars'):
        log_and_print("❌ No TSLA data found!", "ERROR")
        return 1

    # Assess quality
    assessment = assess_data_quality(summary)

    # Print results
    log_and_print("📊 DATA SUMMARY", "HIGHLIGHT")
    log_and_print(f"Total bars: {assessment['total_bars']:,}", "INFO")
    log_and_print(f"Trading days: {assessment['trading_days']}", "INFO")
    log_and_print(f"Date range: {assessment['date_range']}", "INFO")
    log_and_print(f"Data vintage: {assessment['data_vintage']}", "WARNING" if 'Old' in assessment['data_vintage'] else "INFO")
    log_and_print(f"Completeness: {assessment['completeness']}%", "INFO")

    # Signal readiness
    log_and_print("\n🎯 SIGNAL GENERATION READINESS", "HIGHLIGHT")
    if assessment['signal_ready']:
        log_and_print("✅ READY for signal work", "SUCCESS")
    else:
        log_and_print("⚠️ LIMITED readiness", "WARNING")
    log_and_print(f"Status: {assessment['recommended_action']}", "INFO")

    # Daily breakdown
    if summary.get('daily_breakdown'):
        log_and_print("\n📅 DAILY BREAKDOWN", "HIGHLIGHT")
        for day_data in summary['daily_breakdown']:
            log_and_print(f"{day_data['date']}: {day_data['bars']:,} bars", "INFO")

    # Recommendations
    recommendations = generate_recommendations(assessment)
    if recommendations:
        log_and_print("\n💡 RECOMMENDATIONS", "HIGHLIGHT")
        for rec in recommendations:
            log_and_print(rec, "INFO")

    # Export report
    report = {
        'verification_timestamp': datetime.now().isoformat(),
        'summary': summary,
        'assessment': assessment,
        'recommendations': recommendations
    }

    with open('data/tsla_verification_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    log_and_print("\n📄 Full report saved: data/tsla_verification_report.json", "INFO")
    log_and_print("=" * 70, "INFO")

    return 0 if assessment['signal_ready'] else 1

if __name__ == "__main__":
    sys.exit(main())
