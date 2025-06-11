# SPDX-License-Identifier: Apache-2.0
"""Aggregation domain layer."""

from .value_objects import FrameSpec, DEFAULT_SPECS
from .services import AggregationDomainService

__all__ = ["FrameSpec", "DEFAULT_SPECS", "AggregationDomainService"] 