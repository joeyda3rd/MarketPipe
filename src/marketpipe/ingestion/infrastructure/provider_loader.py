# SPDX-License-Identifier: Apache-2.0
"""Provider loader for dynamic market data provider instantiation."""

from __future__ import annotations

import logging
from typing import Any, Dict

from marketpipe.domain.market_data import IMarketDataProvider

from .provider_registry import get, list_providers

logger = logging.getLogger(__name__)


def build_provider(config: Dict[str, Any]) -> IMarketDataProvider:
    """
    Build a market data provider from configuration.

    Args:
        config: Configuration dictionary containing:
            - provider: Provider name (required, e.g., "alpaca", "iex", "fake")
            - Other provider-specific configuration keys

    Returns:
        Configured market data provider instance

    Raises:
        KeyError: If provider is not found
        ValueError: If configuration is invalid
    """
    provider_name = config.get("provider")
    if not provider_name:
        raise ValueError("Provider name is required in configuration")

    # Get provider class from registry
    try:
        provider_cls = get(provider_name)
    except KeyError as e:
        available = list_providers()
        raise KeyError(
            f"Provider '{provider_name}' not found. Available providers: {available}"
        ) from e

    # Try to use from_config class method if available
    if hasattr(provider_cls, "from_config"):
        logger.debug(f"Creating {provider_name} provider using from_config method")
        return provider_cls.from_config(config)

    # Fallback to direct instantiation with config as kwargs
    logger.debug(f"Creating {provider_name} provider using direct instantiation")
    provider_config = config.copy()
    provider_config.pop("provider", None)  # Remove provider key

    try:
        return provider_cls(**provider_config)
    except TypeError as e:
        raise ValueError(f"Failed to create {provider_name} provider: {e}") from e


def get_available_providers() -> list[str]:
    """
    Get list of available provider names.

    Returns:
        List of registered provider names
    """
    return list_providers()


def validate_provider_config(config: Dict[str, Any]) -> bool:
    """
    Validate provider configuration.

    Args:
        config: Provider configuration dictionary

    Returns:
        True if configuration is valid

    Raises:
        ValueError: If configuration is invalid
    """
    if not isinstance(config, dict):
        raise ValueError("Provider configuration must be a dictionary")

    provider_name = config.get("provider")
    if not provider_name:
        raise ValueError("Provider name is required in configuration")

    if not isinstance(provider_name, str):
        raise ValueError("Provider name must be a string")

    # Check if provider exists
    if provider_name not in list_providers():
        available = list_providers()
        raise ValueError(f"Unknown provider '{provider_name}'. Available: {available}")

    return True
