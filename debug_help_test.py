#!/usr/bin/env python3

import sys
sys.path.insert(0, 'src')

from marketpipe.cli import app as root_app
from typer.testing import CliRunner

def main():
    runner = CliRunner()
    result = runner.invoke(root_app, ["symbols", "update", "--help"])
    
    print("Exit code:", result.exit_code)
    print("Output length:", len(result.output))
    print()
    
    # Test each assertion from the test
    test_strings = [
        "Symbol provider(s) to ingest",
        "DuckDB database path", 
        "Parquet dataset root",
        "Back-fill symbols starting this date (YYYY-MM-DD)",
        "Override provider snapshot date (YYYY-MM-DD)",
        "Run pipeline but skip DB / Parquet writes",
        "Skip provider fetch; run diff + SCD update only",
        "Perform writes; without this flag command is read-only"
    ]
    
    for test_string in test_strings:
        found = test_string in result.output
        print(f"{'✓' if found else '✗'} '{test_string}' -> {found}")
        if not found:
            print(f"   Looking in: {repr(result.output[:500])}")
            break
    
    print("\nFull output:")
    print(result.output)

if __name__ == "__main__":
    main() 