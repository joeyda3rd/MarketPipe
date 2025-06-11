# SPDX-License-Identifier: Apache-2.0
"""Pydantic configuration model for ingestion jobs."""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Union

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

PathLike = Union[str, Path]


class IngestionJobConfig(BaseModel):
    """Pydantic model for ingestion job configuration.
    
    This model validates ingestion job parameters and supports loading
    from YAML files with snake_case or kebab-case field names.
    """
    
    symbols: List[str] = Field(
        ..., 
        description="List of stock symbols to ingest (e.g., ['AAPL', 'MSFT'])",
        min_length=1
    )
    start: date = Field(
        ..., 
        description="Start date for ingestion (YYYY-MM-DD)"
    )
    end: date = Field(
        ..., 
        description="End date for ingestion (YYYY-MM-DD)"
    )
    batch_size: int = Field(
        default=1000,
        description="Number of bars per API request",
        ge=1,
        le=10000
    )
    provider: str = Field(
        default="alpaca",
        description="Market data provider (future-proof for multiple providers)"
    )
    feed_type: str = Field(
        default="iex",
        description="Data feed type (iex for free, sip for paid)"
    )
    output_path: str = Field(
        default="./data",
        description="Output directory for data files"
    )
    workers: int = Field(
        default=4,
        description="Number of worker threads",
        ge=1,
        le=32
    )
    
    @field_validator('symbols')
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
    
    @field_validator('provider')
    @classmethod
    def validate_provider(cls, v: str) -> str:
        """Validate market data provider."""
        valid_providers = {"alpaca", "polygon", "iex"}  # Future-proof
        if v.lower() not in valid_providers:
            raise ValueError(f"Unknown provider: {v}. Valid providers: {valid_providers}")
        return v.lower()
    
    @field_validator('feed_type')
    @classmethod
    def validate_feed_type(cls, v: str) -> str:
        """Validate feed type."""
        valid_feeds = {"iex", "sip"}
        if v.lower() not in valid_feeds:
            raise ValueError(f"Unknown feed type: {v}. Valid feeds: {valid_feeds}")
        return v.lower()
    
    @model_validator(mode='after')
    def validate_date_range(self) -> IngestionJobConfig:
        """Validate that start date is before end date."""
        if self.start >= self.end:
            raise ValueError("start date must be before end date")
        return self
    
    @classmethod
    def from_yaml(cls, path: PathLike) -> IngestionJobConfig:
        """Load configuration from YAML file.
        
        Supports both snake_case and kebab-case field names in YAML.
        Environment variables in the YAML file are expanded.
        
        Args:
            path: Path to YAML configuration file
            
        Returns:
            IngestionJobConfig instance
            
        Raises:
            FileNotFoundError: If the YAML file doesn't exist
            ValueError: If the YAML is invalid or contains invalid configuration
        """
        yaml_path = Path(path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        
        try:
            with open(yaml_path, 'r') as f:
                yaml_content = f.read()
                
            # Expand environment variables
            expanded_content = os.path.expandvars(yaml_content)
            
            # Load YAML
            raw_data = yaml.safe_load(expanded_content)
            if not isinstance(raw_data, dict):
                raise ValueError("YAML file must contain a dictionary at the root level")
            
            # Convert kebab-case to snake_case for compatibility
            normalized_data = cls._normalize_yaml_keys(raw_data)
            
            return cls(**normalized_data)
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in configuration file: {e}") from e
        except Exception as e:
            raise ValueError(f"Failed to load configuration from {path}: {e}") from e
    
    @staticmethod
    def _normalize_yaml_keys(data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize YAML keys from kebab-case to snake_case.
        
        Args:
            data: Raw YAML data dictionary
            
        Returns:
            Dictionary with normalized keys
        """
        key_mapping = {
            # kebab-case -> snake_case
            "batch-size": "batch_size",
            "feed-type": "feed_type",
            "output-path": "output_path",
            # snake_case (no change)
            "symbols": "symbols",
            "start": "start", 
            "end": "end",
            "batch_size": "batch_size",
            "provider": "provider",
            "feed_type": "feed_type",
            "output_path": "output_path",
            "workers": "workers",
        }
        
        normalized = {}
        for key, value in data.items():
            normalized_key = key_mapping.get(key, key)
            normalized[normalized_key] = value
        
        return normalized
    
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