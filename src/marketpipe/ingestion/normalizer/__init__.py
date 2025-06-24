"""Symbol normalization module for MarketPipe.

This module handles deduplication and surrogate ID assignment for symbols
from multiple data providers.
"""

from .run_symbol_normalizer import normalize_stage

__all__ = ["normalize_stage"]
