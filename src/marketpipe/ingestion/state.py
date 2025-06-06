"""SQLite checkpoint helper."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional


DEFAULT_PATH = Path.home() / ".marketpipe_state.db"


class SQLiteState:
    """Very small wrapper around a SQLite DB for checkpoints."""

    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = Path(db_path or DEFAULT_PATH)
        self._ensure_table()

    def _ensure_table(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS checkpoints("
                "symbol TEXT PRIMARY KEY, last_ts INTEGER)"
            )

    def get(self, symbol: str) -> Optional[int]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT last_ts FROM checkpoints WHERE symbol=?", (symbol,)
            )
            row = cur.fetchone()
            return row[0] if row else None

    def set(self, symbol: str, ts: int) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO checkpoints(symbol, last_ts) VALUES (?, ?)",
                (symbol, ts),
            )

