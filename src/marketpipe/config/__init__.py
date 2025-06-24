# SPDX-License-Identifier: Apache-2.0
"""Configuration management for MarketPipe."""

from .ingestion import CURRENT_CONFIG_VERSION, MIN_SUPPORTED_VERSION, IngestionJobConfig
from .loader import ConfigVersionError, load_config

__all__ = [
    "IngestionJobConfig",
    "CURRENT_CONFIG_VERSION",
    "MIN_SUPPORTED_VERSION",
    "load_config",
    "ConfigVersionError",
]
