# SPDX-License-Identifier: Apache-2.0
"""Data query commands."""

from __future__ import annotations

import sys

import typer


def query(
    sql: str = typer.Argument(..., help="DuckDB SQL using views bars_5m|15m|1h|1d"),
    csv: bool = typer.Option(False, "--csv", help="Output CSV to stdout"),
    limit: int = typer.Option(50, "--limit", "-l", help="Limit number of rows in table output"),
):
    """Run an ad-hoc query on aggregated data.

    Available views: bars_5m, bars_15m, bars_1h, bars_4h, bars_1d

    Examples:
        marketpipe query "SELECT * FROM bars_5m WHERE symbol='AAPL' LIMIT 10"
        marketpipe query "SELECT symbol, COUNT(*) FROM bars_1d GROUP BY symbol" --csv
        marketpipe query "SELECT MAX(high), MIN(low) FROM bars_1h WHERE symbol='MSFT'"
        marketpipe query "SELECT COUNT(*) FROM bars_4h WHERE symbol='GOOGL'"
    """
    from marketpipe.bootstrap import bootstrap

    bootstrap()

    try:
        import os as _os

        from marketpipe.aggregation.infrastructure.duckdb_views import query as run_query
        from marketpipe.aggregation.infrastructure.duckdb_views import set_agg_root as _set_agg_root

        # Allow overriding aggregation root via environment variable for end-to-end testing
        _agg_root_env = _os.environ.get("MARKETPIPE_AGG_ROOT")
        if _agg_root_env:
            _set_agg_root(_agg_root_env)

        # Execute the query
        df = run_query(sql)

        if df.empty:
            print("Query returned no results")
            return

        if csv:
            # Output CSV to stdout
            df.to_csv(sys.stdout, index=False)
        else:
            # Output formatted table
            if len(df) > limit:
                print(f"🔍 Showing first {limit} of {len(df)} rows:")
                display_df = df.head(limit)
            else:
                display_df = df

            try:
                # Try to format as markdown table
                print(display_df.to_markdown(index=False, tablefmt="grid"))
            except ImportError:
                # Fallback to plain string representation
                print(display_df.to_string(index=False))

            if len(df) > limit:
                print(f"\n... {len(df) - limit} more rows")

    except Exception as e:
        print(f"❌ Query failed: {e}")
        raise typer.Exit(1) from e
