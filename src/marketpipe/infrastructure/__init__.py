# SPDX-License-Identifier: Apache-2.0
"""Infrastructure package for MarketPipe.

Contains concrete implementations of domain interfaces including
repositories, external service adapters, and other infrastructure concerns.
"""

from __future__ import annotations

from . import storage

__all__ = ["storage"] 