# SPDX-License-Identifier: Apache-2.0
"""Pydantic configuration model for ingestion jobs."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field, field_validator, model_validator

PathLike = Union[str, Path]

# Configuration versioning constants
CURRENT_CONFIG_VERSION = "1"
MIN_SUPPORTED_VERSION = "1"


class IngestionJobConfig(BaseModel):
    """Pydantic model for ingestion job configuration.

    This model validates ingestion job parameters and supports loading
    from YAML files with snake_case or kebab-case field names.
    """

    config_version: str = Field(
        default=CURRENT_CONFIG_VERSION, description="Configuration schema version"
    )
    symbols: List[str] = Field(
        ...,
        description="List of stock symbols to ingest (e.g., ['AAPL', 'MSFT'])",
        min_length=1,
    )
    start: date = Field(..., description="Start date for ingestion (YYYY-MM-DD)")
    end: date = Field(..., description="End date for ingestion (YYYY-MM-DD)")
    batch_size: int = Field(
        default=1000, description="Number of bars per API request", ge=1, le=10000
    )
    provider: str = Field(
        default="alpaca",
        description="Market data provider (future-proof for multiple providers)",
    )
    feed_type: str = Field(default="iex", description="Data feed type (iex for free, sip for paid)")
    output_path: str = Field(default="./data", description="Output directory for data files")
    workers: int = Field(default=4, description="Number of worker threads", ge=1, le=32)

    class Config:
        extra = "forbid"  # Reject unknown keys

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, v: List[str]) -> List[str]:
        """Validate and normalize symbols."""
        if not v:
            raise ValueError("symbols list cannot be empty")

        normalized = []
        for symbol in v:
            if not isinstance(symbol, str):
                raise ValueError(f"Symbol must be string, got {type(symbol)}")

            symbol = symbol.strip().upper()
            if not symbol.isalpha():
                raise ValueError(f"Invalid symbol format: {symbol}")

            if len(symbol) > 10:
                raise ValueError(f"Symbol too long: {symbol}")

            normalized.append(symbol)

        return normalized

    @field_validator("provider")
    @classmethod
    def validate_provider(cls, v: str) -> str:
        """Validate market data provider."""
        # Get available providers from registry
        try:
            from marketpipe.ingestion.infrastructure.provider_registry import (
                list_providers,
            )

            valid_providers = set(list_providers())

            # If no providers are registered (e.g., in test environment),
            # fall back to known providers
            if not valid_providers:
                valid_providers = {"alpaca", "iex", "fake", "polygon", "finnhub"}

        except ImportError:
            # Fallback for cases where registry isn't available
            valid_providers = {"alpaca", "iex", "fake", "polygon", "finnhub"}

        if v.lower() not in valid_providers:
            raise ValueError(f"Unknown provider: {v}. Valid providers: {valid_providers}")
        return v.lower()

    @field_validator("feed_type")
    @classmethod
    def validate_feed_type(cls, v: str) -> str:
        """Validate feed type."""
        valid_feeds = {"iex", "sip"}
        if v.lower() not in valid_feeds:
            raise ValueError(f"Unknown feed type: {v}. Valid feeds: {valid_feeds}")
        return v.lower()

    @model_validator(mode="after")
    def validate_date_range(self) -> IngestionJobConfig:
        """Validate that start date is before end date."""
        if self.start >= self.end:
            raise ValueError("start date must be before end date")
        return self

    @classmethod
    def from_yaml(cls, path: PathLike) -> IngestionJobConfig:
        """Load configuration from YAML file.

        DEPRECATED: Use load_config() from marketpipe.config.loader instead.

        This method is kept for backward compatibility but now delegates
        to the new centralized loader with version validation.

        Args:
            path: Path to YAML configuration file

        Returns:
            IngestionJobConfig instance

        Raises:
            FileNotFoundError: If the YAML file doesn't exist
            ValueError: If the YAML is invalid or contains invalid configuration
        """
        # Import here to avoid circular imports
        from .loader import load_config

        return load_config(path)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            Dictionary representation of the configuration
        """
        return self.model_dump()

    def merge_overrides(self, **overrides: Any) -> IngestionJobConfig:
        """Create a new config with field overrides.

        Args:
            **overrides: Field values to override

        Returns:
            New IngestionJobConfig instance with overrides applied
        """
        current_data = self.to_dict()

        # Apply overrides, filtering out None values
        for key, value in overrides.items():
            if value is not None:
                current_data[key] = value

        return self.__class__(**current_data)
