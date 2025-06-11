# SPDX-License-Identifier: Apache-2.0
"""Provider registry for pluggable market data providers."""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import Dict, Type, List

from marketpipe.domain.market_data import IMarketDataProvider

logger = logging.getLogger(__name__)

# Global registry of providers
_REGISTRY: Dict[str, Type[IMarketDataProvider]] = {}
_AUTO_REGISTERED = False


def _auto_register() -> None:
    """Auto-register providers from entry points."""
    global _AUTO_REGISTERED
    if _AUTO_REGISTERED:
        return

    try:
        # Load providers from entry points (Python 3.9+ compatible)
        eps = entry_points()
        marketpipe_providers = eps.get("marketpipe.providers", [])

        for ep in marketpipe_providers:
            try:
                provider_cls = ep.load()
                _REGISTRY[ep.name] = provider_cls
                logger.info(f"Auto-registered provider '{ep.name}' from entry point")
            except Exception as e:
                logger.warning(
                    f"Failed to load provider '{ep.name}' from entry point: {e}"
                )

    except Exception as e:
        logger.warning(f"Failed to load entry points: {e}")

    _AUTO_REGISTERED = True


def register(name: str, cls: Type[IMarketDataProvider]) -> None:
    """
    Register a provider class with the given name.

    Args:
        name: Unique provider identifier (e.g., "alpaca", "iex")
        cls: Provider class implementing IMarketDataProvider
    """
    if not issubclass(cls, IMarketDataProvider):
        raise ValueError(f"Provider class {cls} must implement IMarketDataProvider")

    _REGISTRY[name] = cls
    logger.debug(f"Registered provider '{name}': {cls}")


def get(name: str) -> Type[IMarketDataProvider]:
    """
    Get a provider class by name.

    Args:
        name: Provider identifier

    Returns:
        Provider class

    Raises:
        KeyError: If provider is not found
    """
    if not _REGISTRY and not _AUTO_REGISTERED:
        _auto_register()

    if name not in _REGISTRY:
        available = list(_REGISTRY.keys())
        raise KeyError(f"Provider '{name}' not found. Available providers: {available}")

    return _REGISTRY[name]


def list_providers() -> List[str]:
    """
    Get list of all registered provider names.

    Returns:
        List of provider identifiers
    """
    if not _REGISTRY and not _AUTO_REGISTERED:
        _auto_register()

    return list(_REGISTRY.keys())


def is_registered(name: str) -> bool:
    """
    Check if a provider is registered.

    Args:
        name: Provider identifier

    Returns:
        True if provider is registered, False otherwise
    """
    if not _REGISTRY and not _AUTO_REGISTERED:
        _auto_register()

    return name in _REGISTRY


def clear_registry() -> None:
    """Clear the provider registry (mainly for testing)."""
    global _AUTO_REGISTERED
    _REGISTRY.clear()
    _AUTO_REGISTERED = False


def provider(name: str):
    """
    Decorator to register a provider class.

    Usage:
        @provider("myapi")
        class MyAPIProvider(IMarketDataProvider):
            ...

    Args:
        name: Unique provider identifier

    Returns:
        Decorator function
    """

    def decorator(cls: Type[IMarketDataProvider]) -> Type[IMarketDataProvider]:
        register(name, cls)
        return cls

    return decorator
