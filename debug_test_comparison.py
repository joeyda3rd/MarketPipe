#!/usr/bin/env python3

import sys
sys.path.insert(0, 'src')

from typer.testing import CliRunner
from unittest.mock import patch
import os

# Import exactly like the test does
from marketpipe.cli import app as root_app

def main():
    # Create runner exactly like the test
    runner = CliRunner()
    
    # Run exactly like the test
    result = runner.invoke(root_app, ["symbols", "update", "--help"])
    
    print("Exit code:", result.exit_code)
    print("Output length:", len(result.output))
    print()
    
    # Check the specific assertion that's failing
    target_string = "Back-fill symbols starting this date (YYYY-MM-DD)"
    found = target_string in result.output
    
    print(f"Looking for: '{target_string}'")
    print(f"Found: {found}")
    
    if not found:
        print("\nSearching for variations...")
        variations = [
            "Back-fill symbols starting this date",
            "(YYYY-MM-DD)",
            "backfill",
            "date"
        ]
        
        for var in variations:
            if var in result.output:
                print(f"  Found: '{var}'")
        
        print(f"\nActual output (first 1000 chars):")
        print(repr(result.output[:1000]))
        
        print(f"\nFull output:")
        print(result.output)
    else:
        print("✓ String found successfully!")

if __name__ == "__main__":
    main() 