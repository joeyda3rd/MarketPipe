# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from marketpipe.domain.entities import OHLCVBar


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] | None = None
    valid_bars: List[OHLCVBar] | None = None


class FakeDataValidator:
    async def validate_bars(self, bars: List[OHLCVBar]) -> ValidationResult:
        return ValidationResult(is_valid=True, errors=None, valid_bars=bars)
