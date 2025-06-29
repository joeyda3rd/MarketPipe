from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import List

import typer
import yaml

__all__ = [
    "cli_error",
    "validate_date_range",
    "validate_symbols",
    "validate_output_dir",
    "validate_workers",
    "validate_batch_size",
    "validate_config_file",
    "validate_provider",
    "validate_feed_type",
]


def cli_error(message: str, *, code: int = 2) -> None:  # pragma: no cover
    """Emit a red error message and abort with the given exit code.

    Always use this for user-input errors so we have consistent wording & exit-code 2
    across the entire CLI. The integration-test matrix relies on that.
    """
    typer.secho(f"❌ {message}", fg="red", err=True)
    raise typer.Exit(code)


_DATE_FMT = "%Y-%m-%d"


def _parse_iso(date_str: str) -> dt.date:
    try:
        return dt.date.fromisoformat(date_str)
    except ValueError as exc:  # pragma: no cover
        raise ValueError("invalid date format") from exc


def validate_date_range(start: str | None, end: str | None) -> None:
    """Validate ISO date strings and logical ordering.

    - Both must be ISO-8601 YYYY-MM-DD.
    - Start must not be after end.
    - Dates must not be in the future.
    """
    if start is None and end is None:
        return  # nothing supplied

    if not start or not end:
        cli_error("both --start and --end dates are required when specifying a range")

    try:
        start_date = _parse_iso(start)  # type: ignore[arg-type]
        end_date = _parse_iso(end)  # type: ignore[arg-type]
    except ValueError:
        cli_error("invalid date format; use YYYY-MM-DD", code=2)

    if end_date < start_date:
        cli_error("end date must not be before start date", code=2)

    today = dt.date.today()
    if start_date > today or end_date > today:
        cli_error("date range cannot be in the future", code=2)


_SYMBOL_RE = re.compile(r"^[A-Z0-9\.]{1,10}$")


def validate_symbols(symbols_csv: str | None) -> List[str]:
    if symbols_csv is None:
        return []

    if symbols_csv == "":
        cli_error("empty symbol list supplied", code=2)

    raw = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]

    if len(raw) > 500:
        cli_error("too many symbols supplied; limit is 500", code=2)

    for sym in raw:
        if not _SYMBOL_RE.match(sym):
            cli_error(f"invalid symbol '{sym}'", code=2)

    return raw


def validate_output_dir(output: Path | None) -> None:
    if output is None:
        return
    if not output.exists():
        cli_error(f"output directory not found: {output}", code=2)
    if not output.is_dir():
        cli_error(f"output path is not a directory: {output}", code=2)


def validate_workers(workers: int | None) -> None:
    if workers is None:
        return
    if workers < 1:
        cli_error("--workers must be positive", code=2)
    if workers > 64:
        cli_error("--workers exceeds maximum (64)", code=2)


def validate_batch_size(size: int | None) -> None:
    if size is None:
        return
    if size < 1:
        cli_error("--batch-size must be positive", code=2)
    if size > 10000:
        cli_error("--batch-size exceeds maximum (10000)", code=2)


def validate_config_file(config_path: str) -> None:
    """Validate configuration file exists and has valid YAML."""
    if not config_path:
        return  # Config is optional
    
    path = Path(config_path)
    
    # Check file exists
    if not path.exists():
        cli_error(f"config file not found: {config_path}")
    
    # Check file is readable
    if not path.is_file():
        cli_error(f"config path is not a file: {config_path}")
    
    # Validate YAML format
    try:
        with open(path, 'r') as f:
            content = f.read().strip()
            
        if not content:
            cli_error("config file is empty")
            
        yaml_data = yaml.safe_load(content)
        
        if yaml_data is None:
            cli_error("config file is empty")
            
        if not isinstance(yaml_data, dict):
            cli_error("config file must contain a dictionary at the root level")
            
    except yaml.YAMLError as e:
        cli_error(f"invalid YAML in config file: {e}")
    except Exception as e:
        cli_error(f"error reading config file: {e}")


def validate_provider(provider: str) -> None:
    """Validate provider name."""
    valid_providers = {"alpaca", "polygon", "fake"}
    if provider not in valid_providers:
        cli_error(f"invalid provider: {provider}")


def validate_feed_type(provider: str, feed_type: str | None) -> None:
    """Validate feed type for provider."""
    if provider == "alpaca":
        if not feed_type:
            cli_error("feed type is required for alpaca provider")
        if feed_type not in {"iex", "sip"}:
            cli_error(f"invalid feed type for alpaca: {feed_type}")
    elif provider == "polygon":
        if feed_type and feed_type not in {"delayed", "real-time"}:
            cli_error(f"invalid feed type for polygon: {feed_type}") 