# SPDX-License-Identifier: Apache-2.0
"""Configuration management for MarketPipe."""

from .ingestion import IngestionJobConfig, CURRENT_CONFIG_VERSION, MIN_SUPPORTED_VERSION
from .loader import load_config, ConfigVersionError

__all__ = [
    "IngestionJobConfig",
    "CURRENT_CONFIG_VERSION", 
    "MIN_SUPPORTED_VERSION",
    "load_config",
    "ConfigVersionError"
]
