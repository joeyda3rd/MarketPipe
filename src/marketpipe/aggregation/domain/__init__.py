# SPDX-License-Identifier: Apache-2.0
"""Aggregation domain layer."""

from .services import AggregationDomainService
from .value_objects import DEFAULT_SPECS, FrameSpec

__all__ = ["FrameSpec", "DEFAULT_SPECS", "AggregationDomainService"]
