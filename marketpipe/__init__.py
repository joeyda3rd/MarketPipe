from __future__ import annotations

"""Lightweight shim to make ``python -m marketpipe …`` work from a source
checkout without an install step.

If the real package lives under ``src/marketpipe`` we add that directory to the
import machinery and delegate everything to it.
"""

import sys
from pathlib import Path

# Add the real source package directory to this package's search path so that
# sub-modules like ``marketpipe.cli`` resolve correctly when running from a
# source checkout (without installation).
_src_dir = Path(__file__).resolve().parent.parent / "src" / "marketpipe"
if _src_dir.is_dir():
    if str(_src_dir) not in sys.path:
        # Ensure absolute imports of ``marketpipe`` from elsewhere also resolve.
        sys.path.insert(0, str(_src_dir.parent))  # the ``src`` directory
    # Extend this namespace package's __path__ so sub-modules are discoverable.
    __path__.append(str(_src_dir))  # type: ignore

__all__: list[str] = []

# Clean-up of helper names from namespace
del Path, sys 