#!/usr/bin/env python3
"""Simple script to run PostgreSQL pytest tests."""

import os
import subprocess
import sys

def main():
    """Run PostgreSQL pytest tests."""
    print("🧪 Running PostgreSQL Pytest Tests")
    print("=" * 50)
    
    # Set environment variables
    os.environ['TEST_POSTGRES'] = '1'
    os.environ['POSTGRES_TEST_URL'] = 'postgresql://marketpipe:password@localhost:5433/marketpipe'
    
    print(f"Environment: TEST_POSTGRES={os.environ['TEST_POSTGRES']}")
    print(f"Database URL: {os.environ['POSTGRES_TEST_URL']}")
    print()
    
    # Run pytest
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/test_migrations.py::TestPostgresMigrations",
        "-v", "-s", "--tb=short"
    ]
    
    print(f"Running: {' '.join(cmd)}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, check=False)
        
        if result.returncode == 0:
            print("\n✅ PostgreSQL pytest tests PASSED!")
        else:
            print(f"\n❌ PostgreSQL pytest tests failed with exit code {result.returncode}")
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"❌ Error running pytest: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 