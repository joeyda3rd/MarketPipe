#!/usr/bin/env python3

import sys
sys.path.insert(0, 'src')

from tests.cli.test_symbols_cli import TestSymbolsUpdateCommand

def main():
    test = TestSymbolsUpdateCommand()
    test.setup_method()
    
    try:
        test.test_help_shows_flags()
        print("✓ Test passed!")
    except AssertionError as e:
        print("✗ Test failed:", str(e))
        print("\nLet me check the actual output...")
        
        from marketpipe.cli import app as root_app
        from typer.testing import CliRunner
        
        runner = CliRunner()
        result = runner.invoke(root_app, ["symbols", "update", "--help"])
        
        print("Exit code:", result.exit_code)
        print("Output length:", len(result.output))
        
        target = "Back-fill symbols starting this date (YYYY-MM-DD)"
        found = target in result.output
        print("Contains target string:", found)
        
        if not found:
            print("\nFirst 1000 chars of output:")
            print(repr(result.output[:1000]))
            print("\nSearching for parts...")
            for part in ["Back-fill", "symbols", "starting", "date", "YYYY-MM-DD"]:
                if part in result.output:
                    print(f"  Found: '{part}'")
                else:
                    print(f"  Missing: '{part}'")
        
    except Exception as e:
        print("✗ Unexpected error:", str(e))
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 