#!/usr/bin/env python3
"""
Test the smart test runner.

Run with: python .workspace/test-runner/test_smart_test.py
"""

import tempfile
import os
import subprocess
from pathlib import Path
import sys

def test_smart_test_runner():
    """Test that the smart test runner works."""
    print("🧪 Testing smart test runner...")
    
    # Test dry run
    result = subprocess.run([
        sys.executable, ".workspace/test-runner/smart_test.py", "--dry-run"
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Smart test runner failed: {result.stderr}")
        return False
    
    print("✅ Smart test runner works")
    print(f"   Output: {result.stdout.strip()}")
    return True

def test_mappings():
    """Test that file mappings are reasonable."""
    print("🗺️  Testing file mappings...")
    
    # Import the runner to check mappings
    sys.path.insert(0, str(Path(".workspace/test-runner")))
    from smart_test import SmartTestRunner
    
    runner = SmartTestRunner()
    
    # Test some common scenarios
    test_cases = [
        ({"src/marketpipe/cli.py"}, "tests/test_cli.py"),
        ({"src/marketpipe/aggregation.py"}, "tests/test_aggregation"),
        ({"pyproject.toml"}, "tests/"),  # Should run all tests
    ]
    
    for changed_files, expected_pattern in test_cases:
        patterns = runner.map_files_to_tests(changed_files)
        if not any(expected_pattern in str(patterns) for pattern in patterns):
            print(f"❌ Mapping failed for {changed_files}: got {patterns}")
            return False
    
    print("✅ File mappings look good")
    return True

def main():
    """Run all tests."""
    os.chdir(Path(__file__).parent.parent.parent)  # Change to repo root
    
    tests = [
        test_smart_test_runner,
        test_mappings,
    ]
    
    print("🧪 Testing the testing system...")
    print()
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"📊 Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All tests passed!")
        return True
    else:
        print("😞 Some tests failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 