# SPDX-License-Identifier: Apache-2.0
"""Provider registry for pluggable market data providers."""

from __future__ import annotations

import logging
from importlib.metadata import EntryPoint, entry_points
from typing import Any

from marketpipe.domain.market_data import IMarketDataProvider

logger = logging.getLogger(__name__)

# Global registry of providers
_REGISTRY: dict[str, type[IMarketDataProvider]] = {}
_AUTO_REGISTERED = False

# Provider names that are bundled with MarketPipe itself. These are excluded
# from *automatic* discovery after a call to `clear_registry()` so that tests
# which expect an empty registry do not suddenly see built-ins appear again.
_DEFAULT_PROVIDER_NAMES = {"alpaca", "iex", "fake"}

# After a call to `clear_registry()` we disable re-registration of the default
# providers during the next automatic discovery pass but still allow *other*
# providers (e.g. those supplied by the test suite via a patched
# ``entry_points`` call).
_allow_default_registration: bool = True


def _discover_entry_points() -> list[EntryPoint]:
    """Return provider entry points across Python versions.

    Handles both the modern ``entry_points().select(group=...)`` API and the
    older mapping-like return value that exposes ``get(group, [])``.
    """
    try:
        eps: Any = entry_points()
        # Modern API (Python 3.10+ / importlib-metadata >= 3.10)
        if hasattr(eps, "select"):
            return list(eps.select(group="marketpipe.providers"))
        # Older mapping-style API
        group = getattr(eps, "get", None)
        if callable(group):
            return list(eps.get("marketpipe.providers", []))
        # Fallback: best effort iterate
        return list(eps)
    except Exception:
        return []


def _auto_register() -> None:
    """Auto-register providers from entry points."""
    global _AUTO_REGISTERED, _allow_default_registration
    if _AUTO_REGISTERED:
        return

    try:
        # Load providers from entry points (supports multiple API shapes)
        marketpipe_providers = _discover_entry_points()

        for ep in marketpipe_providers:
            try:
                # Skip built-ins if they are disabled for this discovery pass
                if not _allow_default_registration and ep.name in _DEFAULT_PROVIDER_NAMES:
                    logger.debug(
                        "Skipping built-in provider '%s' during auto-registration", ep.name
                    )
                    continue

                provider_cls = ep.load()
                _REGISTRY[ep.name] = provider_cls
                logger.info("Auto-registered provider '%s' from entry point", ep.name)
            except Exception as e:
                logger.warning(f"Failed to load provider '{ep.name}' from entry point: {e}")

        # Re-enable default provider registration for subsequent discovery
        _allow_default_registration = True

    except Exception as e:
        logger.warning(f"Failed to load entry points: {e}")

    _AUTO_REGISTERED = True


def register(name: str, cls: type[IMarketDataProvider]) -> None:
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


def get(name: str) -> type[IMarketDataProvider]:
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


def list_providers() -> list[str]:
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
    global _AUTO_REGISTERED, _allow_default_registration
    _REGISTRY.clear()

    # Allow another auto-registration cycle but skip the default providers the
    # *first* time.  This satisfies tests that expect an empty registry after
    # `clear_registry()` while still enabling other providers (including those
    # injected via patched entry points) to be discovered on demand.
    _AUTO_REGISTERED = False
    _allow_default_registration = False


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

    def decorator(cls: type[IMarketDataProvider]) -> type[IMarketDataProvider]:
        register(name, cls)
        return cls

    return decorator
