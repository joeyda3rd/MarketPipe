from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import List, Set


class GapDetectorService:  # pylint: disable=too-few-public-methods
    """Detect missing per-day Parquet partitions for a symbol.

    The service inspects a Parquet *root* directory that follows the
    Hive-style layout produced by :pyfunc:`marketpipe.ingestion.writer.write_parquet`::

        <root>/symbol=<SYM>/year=<YYYY>/month=<MM>/day=<DD>.parquet

    It returns the list of *trading dates* (UTC) that have **no** corresponding
    Parquet file on disk.  In V1 we treat an entire day as missing – partial-day
    gaps are ignored.

    The implementation avoids expensive DuckDB scans and merely relies on
    `Path.glob` which is sufficiently fast for a directory tree of a few
    thousand files.
    """

    def __init__(self, parquet_root: Path, timeframe: str = "1m") -> None:
        self._root = Path(parquet_root)
        # Time-frame folder not yet used by the writer, but we keep the argument
        # so that V2 can introduce it without breaking the interface.
        self._timeframe = timeframe

    # ---------------------------------------------------------------------
    # Public helpers
    # ---------------------------------------------------------------------
    def find_missing_days(
        self,
        symbol: str,
        start: dt.date,
        end: dt.date,
    ) -> List[dt.date]:
        """Return all trading days in *[start, end]* with **no** parquet file."""
        existing = self._existing_days(symbol, start, end)
        expected: Set[dt.date] = {
            start + dt.timedelta(days=i) for i in range((end - start).days + 1)
        }
        return sorted(expected - existing)

    async def find_missing_days_async(
        self,
        symbol: str,
        start: dt.date,
        end: dt.date,
    ) -> List[dt.date]:
        """Async wrapper around :pymeth:`find_missing_days`. Useful in trio/asyncio."""
        # Lazily import to avoid mandatory asyncio dependency for sync callers.
        import asyncio  # pylint: disable=import-outside-toplevel

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.find_missing_days, symbol, start, end)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _existing_days(
        self,
        symbol: str,
        start: dt.date,
        end: dt.date,
    ) -> Set[dt.date]:
        """Collect all days that *already* exist on disk for *symbol* in range."""
        base = self._root / f"symbol={symbol.upper()}"
        if not base.exists():
            return set()

        existing: Set[dt.date] = set()
        # Walk year/month directories lazily to avoid many globs when slice is small
        for year_path in base.glob("year=*"):
            try:
                year = int(year_path.name.split("=")[1])
            except (IndexError, ValueError):
                continue  # Skip malformed dirs
            if year < start.year or year > end.year:
                continue
            for month_path in year_path.glob("month=*"):
                try:
                    month = int(month_path.name.split("=")[1])
                except (IndexError, ValueError):
                    continue
                # Quick bounding box check
                first_of_month = dt.date(year, month, 1)
                last_of_month = (first_of_month.replace(day=28) + dt.timedelta(days=4)).replace(
                    day=1
                ) - dt.timedelta(
                    days=1
                )  # noqa: E501
                if last_of_month < start or first_of_month > end:
                    continue
                for file in month_path.glob("day=*.parquet"):
                    try:
                        day = int(file.stem.split("=")[1])
                        existing_date = dt.date(year, month, day)
                        if start <= existing_date <= end:
                            existing.add(existing_date)
                    except (IndexError, ValueError):
                        continue
        return existing


__all__ = ["GapDetectorService"]
