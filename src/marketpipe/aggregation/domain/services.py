# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from .value_objects import FrameSpec


class AggregationDomainService:
    """Pure logic for resampling 1-minute bars to higher frames using DuckDB SQL strings."""

    @staticmethod
    def duckdb_sql(frame: FrameSpec, src_table: str = "bars") -> str:
        """Generate DuckDB SQL for aggregating 1-minute bars to specified timeframe."""
        window_ns = frame.seconds * 1_000_000_000

        # Daily bars follow New York trading dates and the DST-aware market open.
        if frame.name == "1d":
            return f"""
            WITH dated_bars AS (
                SELECT *, date_trunc('day',
                    timezone('America/New_York', to_timestamp(ts_ns / 1000000000))) AS trading_day
                FROM {src_table}
            )
            SELECT
                symbol,
                CAST(extract(epoch from timezone('America/New_York',
                    trading_day + INTERVAL '9 hours 30 minutes')) * 1000000000 AS BIGINT) AS ts_ns,
                first(open ORDER BY ts_ns)  AS open,
                max(high)    AS high,
                min(low)     AS low,
                last(close ORDER BY ts_ns)  AS close,
                sum(volume)  AS volume
            FROM dated_bars
            GROUP BY symbol, trading_day
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
