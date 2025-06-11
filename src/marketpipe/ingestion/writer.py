"""Parquet writing helpers."""

from __future__ import annotations

import datetime as dt
import os
from typing import Dict, List

import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(
    rows: List[Dict],
    output_root: str,
    overwrite: bool = False,
    compression: str = "snappy",
) -> str:
    """Write rows to a partitioned Parquet file."""
    if not rows:
        raise ValueError("No rows supplied")

    ts = dt.datetime.utcfromtimestamp(rows[0]["timestamp"] / 1_000_000_000)
    path = os.path.join(
        output_root,
        f"symbol={rows[0]['symbol']}",
        f"year={ts.year:04d}",
        f"month={ts.month:02d}",
        f"day={ts.day:02d}.parquet",
    )

    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path) and not overwrite:
        return path

    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression=compression)
    return path


__all__ = ["write_parquet"]

