# SPDX-License-Identifier: Apache-2.0
"""Example script running the ingestion CLI."""

from __future__ import annotations

import subprocess
from pathlib import Path


HERE = Path(__file__).resolve().parent
CONFIG = HERE.parent / "config" / "example_config.yaml"


def main() -> None:
    subprocess.run(
        [
            "python",
            "-m",
            "marketpipe",
            "ingest",
            "--config",
            str(CONFIG),
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
