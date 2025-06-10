# SPDX-License-Identifier: Apache-2.0
"""Validation domain events."""

from __future__ import annotations

from marketpipe.events import DomainEvent
from .value_objects import ValidationResult


class ValidationCompleted(DomainEvent):
    """Event raised when validation completes for a symbol."""
    
    def __init__(self, result: ValidationResult):
        self.result = result 