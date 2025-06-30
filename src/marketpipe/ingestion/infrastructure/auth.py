from __future__ import annotations

# SPDX-License-Identifier: Apache-2.0
import abc


class AuthStrategy(abc.ABC):
    """Base class for authentication strategies."""

    @abc.abstractmethod
    def apply(self, headers: dict[str, str], params: dict[str, str]) -> None:
        """Add auth information to request headers or params."""
        ...


class TokenAuth(AuthStrategy):
    """Simple token header auth stub."""

    def __init__(self, token: str) -> None:
        self.token = token

    def apply(self, headers: dict[str, str], params: dict[str, str]) -> None:
        headers["Authorization"] = f"Bearer {self.token}"


class HeaderTokenAuth(AuthStrategy):
    """Simple header-based auth for Alpaca."""

    def __init__(self, key_id: str, secret_key: str) -> None:
        self.key_id = key_id
        self.secret_key = secret_key

    def apply(self, headers: dict[str, str], params: dict[str, str]) -> None:
        headers["APCA-API-KEY-ID"] = self.key_id
        headers["APCA-API-SECRET-KEY"] = self.secret_key


__all__ = ["AuthStrategy", "TokenAuth", "HeaderTokenAuth"]
