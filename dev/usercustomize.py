from __future__ import annotations

import logging as _logging
import sys
from pathlib import Path

"""User-level site customisation for local MarketPipe source checkouts.

This module is imported automatically by CPython *after* `sitecustomize`, if it
exists on the import path.  We use it to add the project's ``src`` directory to
``sys.path`` so that the package can be executed directly from the source tree
(e.g. ``python -m marketpipe.cli --help``) without an explicit installation.
"""

_project_root = Path(__file__).resolve().parent
_src_path = _project_root / "src"

if _src_path.is_dir() and str(_src_path) not in sys.path:
    # Prepend to ensure it has priority over any globally installed version.
    sys.path.insert(0, str(_src_path))

# Minimal root logger so that warnings/errors surface in tests (caplog).
if not _logging.getLogger().handlers:
    _logging.basicConfig(level=_logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
