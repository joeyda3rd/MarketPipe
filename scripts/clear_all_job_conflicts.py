#!/usr/bin/env python3
"""
Clear job scheduling conflicts from all ingestion database files.
"""

import sqlite3
from pathlib import Path
import glob

def clear_all_job_conflicts():
    """Clear all job records from all ingestion databases."""
    
    # Find all ingestion database files
    db_files = glob.glob("**/ingestion_jobs.db", recursive=True)
    
    if not db_files:
        print("❌ No ingestion_jobs.db files found")
        return False
    
    print(f"🔍 Found {len(db_files)} ingestion database files:")
    for db_file in db_files:
        print(f"   📄 {db_file}")
    
    total_cleared = 0
    
    for db_file in db_files:
        try:
            conn = sqlite3.connect(db_file)
            
            # Check if table exists
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ingestion_jobs'")
            if not cursor.fetchone():
                print(f"⏭️  {db_file}: No ingestion_jobs table")
                conn.close()
                continue
            
            # Clear all records
            cursor = conn.execute("DELETE FROM ingestion_jobs")
            deleted_count = cursor.rowcount
            conn.commit()
            
            print(f"✅ {db_file}: Cleared {deleted_count} job records")
            total_cleared += deleted_count
            
        except Exception as e:
            print(f"❌ {db_file}: Error - {e}")
        finally:
            conn.close()
    
    print(f"\n🎉 Total: Cleared {total_cleared} job records across all databases")
    print(f"💡 All scheduling conflicts should now be resolved")
    
    return True

if __name__ == "__main__":
    print("🧹 Clearing job scheduling conflicts from all databases...")
    clear_all_job_conflicts() 