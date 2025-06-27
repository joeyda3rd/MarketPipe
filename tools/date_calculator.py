#!/usr/bin/env python3
"""
MarketPipe Date Calculator

Utility for calculating safe date ranges for market data ingestion
that stay within provider rate limits and data availability constraints.
"""

import argparse
from datetime import datetime, timedelta


def calculate_safe_range(days_back: int = 700, range_days: int = 5) -> tuple[str, str]:
    """Calculate a safe date range for testing/ingestion.
    
    Args:
        days_back: How many days back from today to start
        range_days: How many days to include in the range
        
    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
    """
    today = datetime.now()
    start_date = today - timedelta(days=days_back)
    end_date = start_date + timedelta(days=range_days)

    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--days-back',
        type=int,
        default=700,
        help='Days back from today to calculate start date (default: 700)'
    )
    parser.add_argument(
        '--range-days',
        type=int,
        default=5,
        help='Number of days to include in range (default: 5)'
    )
    parser.add_argument(
        '--recent',
        action='store_true',
        help='Calculate a recent date range (last 30 days)'
    )

    args = parser.parse_args()

    if args.recent:
        start, end = calculate_safe_range(days_back=30, range_days=5)
        print("Recent date range:")
    else:
        start, end = calculate_safe_range(args.days_back, args.range_days)
        print("Safe date range:")

    print(f"Start: {start}")
    print(f"End: {end}")

    # Calculate days from today for context
    start_date = datetime.strptime(start, '%Y-%m-%d')
    days_ago = (datetime.now() - start_date).days
    print(f"Days ago: {days_ago}")


if __name__ == "__main__":
    main()
