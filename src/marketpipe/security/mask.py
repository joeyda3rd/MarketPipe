"""Security masking utilities for secrets and API keys."""

from __future__ import annotations
from typing import Optional

import re

_RE_SECRET = re.compile(r"^([A-Za-z0-9]+)([A-Za-z0-9]{4})$")


def mask(value: Optional[str], show: int = 4) -> str:
    """Mask a secret string, showing only the last `show` characters.

    Args:
        value: The secret string to mask
        show: Number of characters to show at the end (default: 4)

    Returns:
        Masked string with asterisks, or "***" for short/empty strings

    Examples:
        >>> mask("ABCD1234EFGH")
        "********EFGH"
        >>> mask("short")
        "***"
        >>> mask(None)
        "***"
    """
    if not value or len(value) <= show + 2:
        return "***"

    # Simple approach: always mask all but the last `show` characters
    if show == 0:
        return "*" * len(value)

    masked_part = "*" * (len(value) - show)
    visible_part = value[-show:]
    return masked_part + visible_part


def safe_for_log(msg: str, *secrets: str) -> str:
    """Replace any secrets in a log message with masked versions.

    Args:
        msg: The log message that may contain secrets
        *secrets: Variable number of secret strings to mask

    Returns:
        Log message with all secrets replaced by masked versions

    Examples:
        >>> safe_for_log("API key is ABCD1234EFGH", "ABCD1234EFGH")
        "API key is ********EFGH"
        >>> safe_for_log("Error with key1: ABCD1234EFGH and key2: WXYZ5678IJKL",
        ...               "ABCD1234EFGH", "WXYZ5678IJKL")
        "Error with key1: ********EFGH and key2: ********IJKL"
    """
    for secret in secrets:
        if secret:
            msg = msg.replace(secret, mask(secret))
    return msg
