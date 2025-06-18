#!/usr/bin/env python3
"""
Simple script to clear job scheduling conflicts by updating the database directly.
"""

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

def clear_job_conflicts():
    """Clear all job records to resolve scheduling conflicts."""
    
    # Use the new default path
    db_path = Path("data/db/ingestion_jobs.db")
    
    if not db_path.exists():
        print("❌ No ingestion_jobs.db found at data/db/ingestion_jobs.db")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Simply delete all records to clear conflicts
        cursor = conn.execute("DELETE FROM ingestion_jobs")
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"✅ Cleared {deleted_count} job records from database")
        print(f"💡 All scheduling conflicts should now be resolved")
        
        return True
        
    except Exception as e:
        print(f"❌ Error clearing database: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    print("🧹 Clearing job scheduling conflicts...")
    clear_job_conflicts() 