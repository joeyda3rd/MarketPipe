#!/usr/bin/env python3
"""
Debug script to check active ingestion jobs and resolve scheduling conflicts.

This script can:
1. List all active jobs
2. Show job details (symbols, state, dates)
3. Clear stuck/stale jobs if needed
"""

import json
import sqlite3
from pathlib import Path


def check_active_jobs():
    """Check what active jobs exist in the database."""

    # Check for SQLite database
    db_files = [
        "ingestion_jobs.db",
        "data/db/ingestion_jobs.db",
        "data/ingestion_jobs.db"
    ]

    db_path = None
    for db_file in db_files:
        if Path(db_file).exists():
            db_path = db_file
            break

    if not db_path:
        print("❌ No ingestion jobs database found")
        print("💡 Checked for:", ", ".join(db_files))
        return

    print(f"📁 Using database: {db_path}")

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name

        # Check what tables exist
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"📋 Available tables: {[t[0] for t in tables]}")

        if not tables:
            print("❌ No tables found in database")
            return

        # Try to find ingestion jobs table
        table_name = None
        for table in tables:
            if 'ingestion' in table[0].lower() or 'job' in table[0].lower():
                table_name = table[0]
                break

        if not table_name:
            table_name = tables[0][0]  # Use first table if none match

        print(f"🔍 Using table: {table_name}")

        # Get schema
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        print("📝 Table schema:")
        for col in columns:
            print(f"   {col[1]} ({col[2]})")

        # Get all rows
        cursor = conn.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()

        print(f"\n📊 Found {len(rows)} total jobs")

        if not rows:
            print("✅ No jobs found - no conflicts!")
            return

        # Show job details
        active_count = 0
        for i, row in enumerate(rows):
            print(f"\n🔍 Job {i+1}:")

            # Try to parse different formats
            row_dict = dict(row)

            # Check if there's a payload column (new format)
            if 'payload' in row_dict and row_dict['payload']:
                try:
                    payload = json.loads(row_dict['payload'])
                    print(f"   Job ID: {payload.get('job_id', 'unknown')}")
                    print(f"   Symbols: {payload.get('symbols', [])}")
                    print(f"   State: {payload.get('state', 'unknown')}")
                    print(f"   Created: {payload.get('created_at', 'unknown')}")

                    if payload.get('state') in ['pending', 'PENDING', 'in_progress', 'IN_PROGRESS']:
                        active_count += 1
                        print("   🔴 ACTIVE JOB - may cause conflicts!")
                    else:
                        print("   ✅ Completed/inactive")

                except json.JSONDecodeError:
                    print(f"   ❌ Invalid JSON payload: {row_dict['payload'][:100]}...")
            else:
                # Old format - show raw columns
                for key, value in row_dict.items():
                    if value is not None:
                        print(f"   {key}: {value}")

                if 'state' in row_dict and row_dict['state'] in ['pending', 'running', 'in_progress']:
                    active_count += 1
                    print("   🔴 ACTIVE JOB - may cause conflicts!")

        print(f"\n📊 Summary: {active_count} active jobs found")

        if active_count > 0:
            print("\n💡 Active jobs may be causing scheduling conflicts")
            print("💡 To clear them, run:")
            print("   python scripts/debug_active_jobs.py --clear")
        else:
            print("\n✅ No active jobs - scheduling conflicts unlikely")

    except Exception as e:
        print(f"❌ Error checking database: {e}")
    finally:
        conn.close()

def clear_active_jobs():
    """Clear active jobs to resolve conflicts."""

    db_files = [
        "ingestion_jobs.db",
        "data/db/ingestion_jobs.db",
        "data/ingestion_jobs.db"
    ]

    db_path = None
    for db_file in db_files:
        if Path(db_file).exists():
            db_path = db_file
            break

    if not db_path:
        print("❌ No ingestion jobs database found")
        return

    try:
        conn = sqlite3.connect(db_path)

        # Find the table
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        table_name = None
        for table in tables:
            if 'ingestion' in table[0].lower() or 'job' in table[0].lower():
                table_name = table[0]
                break

        if not table_name:
            table_name = tables[0][0]

        print(f"🧹 Clearing active jobs from table: {table_name}")

        # Check what format we're dealing with
        cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 1")
        row = cursor.fetchone()

        if row and 'payload' in [desc[0] for desc in cursor.description]:
            # New format with JSON payload
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_before = cursor.fetchone()[0]

            # Update active jobs to completed
            cursor = conn.execute(f"""
                UPDATE {table_name} 
                SET payload = json_set(payload, '$.state', 'COMPLETED'),
                    state = 'COMPLETED'
                WHERE json_extract(payload, '$.state') IN ('PENDING', 'IN_PROGRESS', 'pending', 'in_progress')
            """)
            updated_count = cursor.rowcount

        else:
            # Old format
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_before = cursor.fetchone()[0]

            cursor = conn.execute(f"""
                UPDATE {table_name} 
                SET state = 'done'
                WHERE state IN ('pending', 'running', 'in_progress')
            """)
            updated_count = cursor.rowcount

        conn.commit()

        print(f"✅ Updated {updated_count} active jobs to completed")
        print(f"📊 Total jobs in database: {total_before}")
        print("💡 Scheduling conflicts should now be resolved")

    except Exception as e:
        print(f"❌ Error clearing jobs: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--clear":
        print("🧹 Clearing active jobs...")
        clear_active_jobs()
    else:
        print("🔍 Checking active ingestion jobs...")
        check_active_jobs()
        print("\n💡 To clear active jobs, run: python scripts/debug_active_jobs.py --clear")
