# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FrameSpec:
    """Specification for a time frame aggregation."""

    name: str  # "5m", "15m", "1h", "1d"
    seconds: int  # 300, 900, 3600, 86400

    def __str__(self) -> str:
        return self.name


DEFAULT_SPECS = [
    FrameSpec("5m", 300),
    FrameSpec("15m", 900),
    FrameSpec("1h", 3600),
    FrameSpec("1d", 86400),
]
