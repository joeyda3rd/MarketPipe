# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from .value_objects import FrameSpec


class AggregationDomainService:
    """Pure logic for resampling 1-minute bars to higher frames using DuckDB SQL strings."""

    @staticmethod
    def duckdb_sql(frame: FrameSpec, src_table: str = "bars") -> str:
        """Generate DuckDB SQL for aggregating 1-minute bars to specified timeframe."""
        window_ns = frame.seconds * 1_000_000_000

        # Special handling for daily bars - align to market open (13:30 UTC)
        if frame.name == "1d":
            return f"""
            SELECT
                symbol,
                -- Align to market open: convert to UTC date, then add 13.5 hours (13:30 UTC)
                CAST((extract(epoch from date_trunc('day', to_timestamp(ts_ns / 1000000000) AT TIME ZONE 'UTC')) + 13.5 * 3600) * 1000000000 AS BIGINT) AS ts_ns,
                first(open ORDER BY ts_ns)  AS open,
                max(high)    AS high,
                min(low)     AS low,
                last(close ORDER BY ts_ns)  AS close,
                sum(volume)  AS volume
            FROM {src_table}
            GROUP BY symbol, date_trunc('day', to_timestamp(ts_ns / 1000000000) AT TIME ZONE 'UTC')
            ORDER BY symbol, ts_ns
            """
        else:
            # Standard alignment for intraday timeframes
            return f"""
            SELECT
                symbol,
                floor(ts_ns/{window_ns}) * {window_ns} AS ts_ns,
                first(open ORDER BY ts_ns)  AS open,
                max(high)    AS high,
                min(low)     AS low,
                last(close ORDER BY ts_ns)  AS close,
                sum(volume)  AS volume
            FROM {src_table}
            GROUP BY symbol, floor(ts_ns/{window_ns})
            ORDER BY symbol, ts_ns
            """
