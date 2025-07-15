#!/usr/bin/env python3
"""
Script to clean up test artifacts from the MarketPipe repository.
"""

import os
import shutil
import glob
from pathlib import Path

def cleanup_test_artifacts():
    """Remove test artifacts from the repository."""
    
    # Get the current directory (should be MarketPipe root)
    root_dir = Path.cwd()
    
    artifacts_removed = []
    
    # Remove database files in root
    db_files = [
        "ingestion_jobs.db",
        "metrics.db",
        "core.db"
    ]
    
    for db_file in db_files:
        db_path = root_dir / db_file
        if db_path.exists():
            try:
                db_path.unlink()
                artifacts_removed.append(str(db_path))
                print(f"Removed: {db_path}")
            except Exception as e:
                print(f"Error removing {db_path}: {e}")
    
    # Remove test directories
    test_dirs = [
        "test_data",
        "test_output", 
        "test_relative_path"
    ]
    
    for test_dir in test_dirs:
        test_path = root_dir / test_dir
        if test_path.exists():
            try:
                shutil.rmtree(test_path)
                artifacts_removed.append(str(test_path))
                print(f"Removed directory: {test_path}")
            except Exception as e:
                print(f"Error removing {test_path}: {e}")
    
    # Remove parquet files recursively
    parquet_files = list(root_dir.glob("**/*.parquet"))
    parquet_lock_files = list(root_dir.glob("**/*.parquet.lock"))
    
    for parquet_file in parquet_files + parquet_lock_files:
        try:
            parquet_file.unlink()
            artifacts_removed.append(str(parquet_file))
            print(f"Removed: {parquet_file}")
        except Exception as e:
            print(f"Error removing {parquet_file}: {e}")
    
    # Remove database files in data directory
    data_dir = root_dir / "data"
    if data_dir.exists():
        db_files_in_data = list(data_dir.glob("*.db"))
        for db_file in db_files_in_data:
            try:
                db_file.unlink()
                artifacts_removed.append(str(db_file))
                print(f"Removed: {db_file}")
            except Exception as e:
                print(f"Error removing {db_file}: {e}")
    
    # Remove pytest cache
    pytest_cache = root_dir / ".pytest_cache"
    if pytest_cache.exists():
        try:
            shutil.rmtree(pytest_cache)
            artifacts_removed.append(str(pytest_cache))
            print(f"Removed directory: {pytest_cache}")
        except Exception as e:
            print(f"Error removing {pytest_cache}: {e}")
    
    print(f"\nCleanup complete. Removed {len(artifacts_removed)} artifacts.")
    return artifacts_removed

if __name__ == "__main__":
    cleanup_test_artifacts() 