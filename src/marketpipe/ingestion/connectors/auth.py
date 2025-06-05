from __future__ import annotations

import abc
from typing import Dict


class AuthStrategy(abc.ABC):
    """Base class for authentication strategies."""

    @abc.abstractmethod
    def apply(self, headers: Dict[str, str], params: Dict[str, str]) -> None:
        """Add auth information to request headers or params."""
        ...


class TokenAuth(AuthStrategy):
    """Simple token header auth stub."""

    def __init__(self, token: str) -> None:
        self.token = token

    def apply(self, headers: Dict[str, str], params: Dict[str, str]) -> None:
        headers["Authorization"] = f"Bearer {self.token}"
