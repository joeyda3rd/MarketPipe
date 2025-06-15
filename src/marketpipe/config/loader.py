# SPDX-License-Identifier: Apache-2.0
"""Centralized configuration loader with version validation."""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Dict, Any, Union

import yaml

from .ingestion import IngestionJobConfig, CURRENT_CONFIG_VERSION, MIN_SUPPORTED_VERSION

PathLike = Union[str, Path]


class ConfigVersionError(RuntimeError):
    """Error when configuration version is incompatible."""
    pass


def load_config(path: PathLike) -> IngestionJobConfig:
    """Load and validate configuration from YAML file with version checking.
    
    Args:
        path: Path to YAML configuration file
        
    Returns:
        IngestionJobConfig instance
        
    Raises:
        ConfigVersionError: If config version is missing, too old, or incompatible
        FileNotFoundError: If the YAML file doesn't exist
        ValueError: If the YAML is invalid or contains invalid configuration
    """
    yaml_path = Path(path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    try:
        with open(yaml_path, "r") as f:
            yaml_content = f.read()

        # Expand environment variables
        expanded_content = os.path.expandvars(yaml_content)

        # Load YAML
        cfg_dict = yaml.safe_load(expanded_content)
        if not isinstance(cfg_dict, dict):
            raise ValueError(
                "YAML file must contain a dictionary at the root level"
            )

        # Convert kebab-case to snake_case for compatibility first
        normalized_data = _normalize_yaml_keys(cfg_dict)

        # Version validation
        ver = str(normalized_data.get("config_version", ""))
        if not ver:
            raise ConfigVersionError(
                "config_version missing. Add `config_version: \"1\"` to your YAML."
            )

        if ver < MIN_SUPPORTED_VERSION:
            raise ConfigVersionError(
                f"Config version {ver} is too old. "
                f"Minimum supported is {MIN_SUPPORTED_VERSION}. "
                "Please upgrade your configuration."
            )

        if ver > CURRENT_CONFIG_VERSION:
            warnings.warn(
                f"This binary understands config_version {CURRENT_CONFIG_VERSION}, "
                f"but file is {ver}. Attempting best-effort parse.",
                UserWarning,
                stacklevel=2,
            )

        return IngestionJobConfig(**normalized_data)

    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in configuration file: {e}") from e
    except Exception as e:
        if isinstance(e, (ConfigVersionError, FileNotFoundError, ValueError)):
            raise
        raise ValueError(f"Failed to load configuration from {path}: {e}") from e


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
        "config-version": "config_version",
        # snake_case (no change)
        "config_version": "config_version",
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