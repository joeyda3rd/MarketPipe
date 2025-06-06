"""Row validation utilities for the ingestion pipeline."""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ValidationError


SCHEMA_PATH = Path(__file__).resolve().parents[3] / "schema" / "schema_v1.json"

with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    _SCHEMA_JSON = json.load(f)


class _RowModel(BaseModel):
    symbol: str
    timestamp: int
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None
    trade_count: Optional[int] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    spread: Optional[float] = None
    source: Optional[str] = None
    exchange: Optional[str] = None
    frame: Optional[str] = None
    session: Optional[str] = None
    currency: Optional[str] = None
    status: Optional[str] = None
    adjusted: Optional[bool] = None
    halted: Optional[bool] = None
    ingest_id: Optional[str] = None
    schema_version: int


class SchemaValidator:
    """Validate raw rows against ``schema_v1`` using Pydantic."""

    def __init__(self) -> None:
        self.schema = _SCHEMA_JSON

    def validate(self, row: Dict[str, Any]) -> None:
        """Validate a single row in place."""
        _RowModel(**row)

    def validate_batch(self, rows: List[Dict[str, Any]]) -> None:
        for r in rows:
            self.validate(r)

