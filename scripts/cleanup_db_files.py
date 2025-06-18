#!/usr/bin/env python3
"""
Clean up and organize database files scattered in the root directory.

This script:
1. Moves ingestion_jobs.db to data/db/ingestion_jobs.db (if it exists and has data)
2. Cleans up Prometheus multiprocess .db files from the root
3. Sets up proper directory structure for future database organization
"""

import os
import shutil
import sqlite3
from pathlib import Path
from typing import List


def check_database_has_data(db_path: Path) -> bool:
    """Check if a SQLite database has any data."""
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        if not tables:
            conn.close()
            return False
        
        # Check if any table has data
        for table_name, in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            if count > 0:
                conn.close()
                return True
        
        conn.close()
        return False
    except Exception as e:
        print(f"⚠️  Error checking database {db_path}: {e}")
        return False


def find_prometheus_db_files() -> List[Path]:
    """Find Prometheus multiprocess .db files in the root directory."""
    root = Path(".")
    prometheus_files = []
    
    # Pattern: histogram_*, counter_*, gauge_*, summary_* with numbers
    for pattern in ["histogram_*.db", "counter_*.db", "gauge_*.db", "summary_*.db"]:
        prometheus_files.extend(root.glob(pattern))
    
    return prometheus_files


def setup_directories():
    """Set up the proper directory structure for database files."""
    directories = [
        Path("data/db"),
        Path("data/metrics"),
        Path("data/metrics/multiprocess"),
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"📁 Created/verified directory: {directory}")


def cleanup_prometheus_files():
    """Clean up Prometheus multiprocess .db files from root."""
    prometheus_files = find_prometheus_db_files()
    
    if not prometheus_files:
        print("✅ No Prometheus multiprocess .db files found in root directory")
        return
    
    print(f"🧹 Found {len(prometheus_files)} Prometheus multiprocess .db files to clean up")
    
    for db_file in prometheus_files:
        try:
            db_file.unlink()
            print(f"  ✅ Removed: {db_file.name}")
        except Exception as e:
            print(f"  ❌ Failed to remove {db_file.name}: {e}")
    
    print("🧹 Prometheus multiprocess files cleanup completed")


def migrate_ingestion_jobs_db():
    """Migrate ingestion_jobs.db from root to data/db/ if it has data."""
    root_db = Path("ingestion_jobs.db")
    target_db = Path("data/db/ingestion_jobs.db")
    
    if not root_db.exists():
        print("✅ No ingestion_jobs.db found in root directory")
        return
    
    print(f"📦 Found ingestion_jobs.db in root directory")
    
    # Check if it has data
    if not check_database_has_data(root_db):
        print("  📊 Database is empty, removing...")
        root_db.unlink()
        print("  ✅ Removed empty ingestion_jobs.db")
        return
    
    # Check if target already exists
    if target_db.exists():
        print(f"  ⚠️  Target {target_db} already exists")
        if check_database_has_data(target_db):
            print(f"  📊 Target database has data, backing up root database...")
            backup_path = Path(f"data/db/ingestion_jobs_backup_{int(time.time())}.db")
            shutil.move(str(root_db), str(backup_path))
            print(f"  ✅ Moved to backup: {backup_path}")
        else:
            print(f"  📊 Target database is empty, replacing with root database...")
            shutil.move(str(root_db), str(target_db))
            print(f"  ✅ Moved to: {target_db}")
    else:
        # Move the database
        shutil.move(str(root_db), str(target_db))
        print(f"  ✅ Moved to: {target_db}")


def main():
    """Main cleanup function."""
    print("🧹 MarketPipe Database Cleanup")
    print("=" * 40)
    
    # Set up proper directory structure
    print("\n1. Setting up directory structure...")
    setup_directories()
    
    # Migrate ingestion jobs database
    print("\n2. Migrating ingestion_jobs.db...")
    migrate_ingestion_jobs_db()
    
    # Clean up Prometheus files
    print("\n3. Cleaning up Prometheus multiprocess files...")
    cleanup_prometheus_files()
    
    # Set up environment variable for future runs
    print("\n4. Setting up Prometheus multiprocess directory...")
    multiproc_dir = Path("data/metrics/multiprocess")
    print(f"📊 Future Prometheus metrics will use: {multiproc_dir}")
    print(f"💡 Tip: Set PROMETHEUS_MULTIPROC_DIR={multiproc_dir} in your environment")
    
    print("\n✅ Database cleanup completed!")
    print("\n📋 Summary of new locations:")
    print("  - Ingestion jobs: data/db/ingestion_jobs.db")
    print("  - Core database: data/db/core.db")
    print("  - Metrics database: data/metrics.db")
    print("  - Prometheus multiprocess: data/metrics/multiprocess/")


if __name__ == "__main__":
    import time
    main() 