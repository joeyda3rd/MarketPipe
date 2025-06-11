# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from .value_objects import FrameSpec


class AggregationDomainService:
    """Pure logic for resampling 1-minute bars to higher frames using DuckDB SQL strings."""

    @staticmethod
    def duckdb_sql(frame: FrameSpec, src_table: str = "bars") -> str:
        """Generate DuckDB SQL for aggregating 1-minute bars to specified timeframe."""
        window_ns = frame.seconds * 1_000_000_000
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