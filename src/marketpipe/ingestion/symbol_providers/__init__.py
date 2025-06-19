from __future__ import annotations

from typing import Dict, Type

from .base import SymbolProviderBase

_REGISTRY: Dict[str, Type[SymbolProviderBase]] = {}


def register(name: str):
    """Decorator to register a symbol provider with the registry.
    
    Args:
        name: Unique provider identifier (lower-snake case)
        
    Returns:
        Decorator function that registers the provider class
        
    Raises:
        ValueError: If provider name already registered
        TypeError: If class doesn't inherit from SymbolProviderBase
    """
    def decorator(cls: Type[SymbolProviderBase]):
        if name in _REGISTRY:
            raise ValueError(f"Provider name '{name}' already registered")
        if not issubclass(cls, SymbolProviderBase):
            raise TypeError("Class must inherit SymbolProviderBase")
        cls.name = name        # inject canonical name
        _REGISTRY[name] = cls
        return cls
    return decorator


def get(name: str, **kwargs) -> SymbolProviderBase:
    """Get a provider instance by name.
    
    Args:
        name: Provider name as registered
        **kwargs: Configuration arguments passed to provider constructor
        
    Returns:
        Configured provider instance
        
    Raises:
        ValueError: If provider name not found
    """
    try:
        cls = _REGISTRY[name]
    except KeyError:
        raise ValueError(f"Unknown symbol provider '{name}'")
    return cls(**kwargs)


def list_providers() -> list[str]:
    """List all registered provider names.
    
    Returns:
        Sorted list of provider names
    """
    return sorted(_REGISTRY)


# Import providers to trigger registration
from . import dummy
from . import polygon

__all__ = [
    "SymbolProviderBase",
    "register",
    "get", 
    "list_providers",
] 