"""Threaded ingestion coordinator."""

from __future__ import annotations

import datetime as dt
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

import yaml

from .connectors.alpaca_client import AlpacaClient
from .connectors.models import ClientConfig
from .connectors.auth import HeaderTokenAuth
from .connectors.rate_limit import RateLimiter
from .state import SQLiteState
from .validator import SchemaValidator
from .writer import write_parquet
from marketpipe.metrics import BACKLOG
from marketpipe.metrics_server import run as metrics_server_run


def load_dotenv_file(dotenv_path: str = ".env") -> None:
    """Load environment variables from a .env file."""
    if not os.path.exists(dotenv_path):
        return
    
    with open(dotenv_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                # Remove quotes if present
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                os.environ[key] = value


class IngestionCoordinator:
    """Simple orchestration layer that fans out ingest jobs to workers."""

    def __init__(self, config_path: str, state_path: str | None = None) -> None:
        # Load environment variables from .env file
        load_dotenv_file()
        
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        self.symbols: List[str] = cfg.get("symbols", [])
        self.output_root = cfg["output_path"]
        self.compression = cfg.get("compression", "snappy")

        m_cfg = cfg.get("metrics", {})
        self.metrics_enabled = m_cfg.get("enabled", False)
        self.metrics_port = m_cfg.get("port", 8000)
        if self.metrics_enabled:
            threading.Thread(
                target=metrics_server_run,
                kwargs={"port": self.metrics_port},
                daemon=True,
            ).start()
        
        # Handle date parsing - config might have dates as strings or date objects
        start_val = cfg["start"]
        if isinstance(start_val, str):
            self.start = dt.datetime.fromisoformat(start_val)
        else:
            # Already a date object, convert to datetime
            self.start = dt.datetime.combine(start_val, dt.time.min)
            
        end_val = cfg["end"]
        if isinstance(end_val, str):
            self.end = dt.datetime.fromisoformat(end_val)
        else:
            # Already a date object, convert to datetime
            self.end = dt.datetime.combine(end_val, dt.time.max)
            
        self.workers = cfg.get("workers", len(self.symbols))

        # Get credentials from .env file (loaded above)
        api_key = os.environ.get("ALPACA_KEY")
        api_secret = os.environ.get("ALPACA_SECRET")
        
        if not api_key or not api_secret:
            raise ValueError(
                "ALPACA_KEY and ALPACA_SECRET must be set in .env file. "
                "Create a .env file with:\n"
                "ALPACA_KEY=your_api_key_here\n"
                "ALPACA_SECRET=your_secret_key_here"
            )

        a_cfg = cfg["alpaca"]
        self.client_cfg = ClientConfig(
            api_key=api_key,
            base_url=a_cfg["base_url"],
            rate_limit_per_min=a_cfg.get("rate_limit_per_min"),
        )
        self.auth = HeaderTokenAuth(api_key, api_secret)
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
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self._process_symbol, s) for s in self.symbols]
            BACKLOG.set(len(futures))
            results = []
            try:
                for fut in as_completed(futures):
                    results.append(fut.result())
                    BACKLOG.dec()
            except KeyboardInterrupt:
                pool.shutdown(wait=False, cancel_futures=True)
                raise

        total_rows = sum(results)
        summary = {
            "symbols": len(self.symbols),
            "rows": total_rows,
            "files": len(self.symbols),
        }
        return summary

