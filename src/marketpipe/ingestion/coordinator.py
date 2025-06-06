"""Threaded ingestion coordinator."""

from __future__ import annotations

import datetime as dt
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List

import yaml

from .connectors.alpaca_client import AlpacaClient
from .connectors.models import ClientConfig
from .connectors.auth import HeaderTokenAuth
from .connectors.rate_limit import RateLimiter
from .state import SQLiteState
from .validator import SchemaValidator
from .writer import write_parquet


class IngestionCoordinator:
    """Simple orchestration layer that fans out ingest jobs to workers."""

    def __init__(self, config_path: str, state_path: str | None = None) -> None:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        self.symbols: List[str] = cfg.get("symbols", [])
        self.output_root = cfg["output_path"]
        self.compression = cfg.get("compression", "snappy")
        self.start = dt.datetime.fromisoformat(cfg["start"])
        self.end = dt.datetime.fromisoformat(cfg["end"])
        self.workers = cfg.get("workers", len(self.symbols))

        a_cfg = cfg["alpaca"]
        self.client_cfg = ClientConfig(
            api_key=a_cfg["key"],
            base_url=a_cfg["base_url"],
            rate_limit_per_min=a_cfg.get("rate_limit_per_min"),
        )
        self.auth = HeaderTokenAuth(a_cfg["key"], a_cfg["secret"])
        self.state = SQLiteState(Path(state_path) if state_path else None)

        self.validator = SchemaValidator()

    # ---------------------------------------------------------------
    def _process_symbol(self, symbol: str) -> int:
        limiter = RateLimiter()
        client = AlpacaClient(
            config=self.client_cfg,
            auth=self.auth,
            rate_limiter=limiter,
            state_backend=self.state,
        )

        end_ms = int(self.end.timestamp() * 1000)
        end_ns = end_ms * 1_000_000
        start_ms = int(self.start.timestamp() * 1000)

        checkpoint = self.state.get(symbol)
        if checkpoint is not None and checkpoint >= end_ns:
            return 0

        rows = client.fetch_batch(symbol, start_ms, end_ms)
        if not rows:
            return 0

        self.validator.validate_batch(rows)
        write_parquet(
            rows,
            self.output_root,
            overwrite=True,
            compression=self.compression,
        )

        last_ts = max(r["timestamp"] for r in rows)
        self.state.set(symbol, last_ts)
        return len(rows)

    # ---------------------------------------------------------------
    def run(self) -> Dict[str, int]:
        total_rows = 0
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            results = list(pool.map(self._process_symbol, self.symbols))
        total_rows = sum(results)
        summary = {
            "symbols": len(self.symbols),
            "rows": total_rows,
            "files": len(self.symbols),
        }
        return summary

