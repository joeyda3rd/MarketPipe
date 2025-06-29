# SPDX-License-Identifier: Apache-2.0
"""Job management commands for MarketPipe."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

import typer

# Job management app
jobs_app = typer.Typer(name="jobs", help="Ingestion job management commands", add_completion=False)

def _get_db_path() -> Optional[str]:
    """Get the path to the ingestion jobs database."""
    possible_paths = [
        "data/ingestion_jobs.db",
        "ingestion_jobs.db", 
        "data/db/core.db"
    ]
    
    for path in possible_paths:
        if Path(path).exists():
            return path
    return None

@jobs_app.command()
def list(
    state: Optional[str] = typer.Option(None, "--state", "-s", help="Filter by job state (PENDING, IN_PROGRESS, COMPLETED, FAILED, CANCELLED)"),
    limit: int = typer.Option(20, "--limit", "-l", help="Number of jobs to show"),
    symbol: Optional[str] = typer.Option(None, "--symbol", help="Filter by symbol"),
):
    """List ingestion jobs with filtering options.
    
    Examples:
        marketpipe jobs list                          # List recent jobs
        marketpipe jobs list --state IN_PROGRESS     # Show running jobs
        marketpipe jobs list --symbol AAPL           # Show AAPL jobs
        marketpipe jobs list --limit 50              # Show more jobs
    """
    
    db_path = _get_db_path()
    if not db_path:
        typer.echo("❌ No ingestion jobs database found")
        typer.echo("💡 Run an ingestion first to create the database")
        raise typer.Exit(1)
    
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Build query based on filters
            query = "SELECT * FROM ingestion_jobs WHERE 1=1"
            params = []
            
            if state:
                query += " AND state = ?"
                params.append(state.upper())
            
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol.upper())
            
            query += " ORDER BY updated_at DESC LIMIT ?"
            params.append(limit)
            
            cursor.execute(query, params)
            jobs = cursor.fetchall()
            
            if not jobs:
                typer.echo("📭 No jobs found matching the criteria")
                return
            
            # Display results
            typer.echo(f"\n📊 Jobs ({len(jobs)} found)")
            typer.echo("=" * 100)
            typer.echo(f"{'ID':<4} {'Symbol':<8} {'Day':<12} {'State':<12} {'Created':<20} {'Updated':<20}")
            typer.echo("-" * 100)
            
            for job in jobs:
                created_dt = datetime.fromisoformat(job['created_at'].replace('Z', '+00:00'))
                updated_dt = datetime.fromisoformat(job['updated_at'].replace('Z', '+00:00'))
                
                typer.echo(f"{job['id']:<4} {job['symbol']:<8} {job['day']:<12} {job['state']:<12} "
                          f"{created_dt.strftime('%m-%d %H:%M:%S'):<20} {updated_dt.strftime('%m-%d %H:%M:%S'):<20}")
            
            typer.echo("=" * 100)
            
    except Exception as e:
        typer.echo(f"❌ Database error: {e}")
        raise typer.Exit(1)

@jobs_app.command()
def status(
    job_id: Optional[int] = typer.Argument(None, help="Job ID to check (optional - shows summary if not provided)"),
):
    """Get detailed status for a specific job or show summary of all jobs.
    
    Examples:
        marketpipe jobs status           # Show summary of all job states
        marketpipe jobs status 109       # Show details for job 109
    """
    
    db_path = _get_db_path()
    if not db_path:
        typer.echo("❌ No ingestion jobs database found")
        raise typer.Exit(1)
    
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if job_id:
                # Show specific job details
                cursor.execute("SELECT * FROM ingestion_jobs WHERE id = ?", (job_id,))
                job = cursor.fetchone()
                
                if not job:
                    typer.echo(f"❌ Job {job_id} not found")
                    raise typer.Exit(1)
                
                typer.echo(f"📊 Job Details: {job_id}")
                typer.echo("=" * 50)
                typer.echo(f"Symbol: {job['symbol']}")
                typer.echo(f"Day: {job['day']}")
                typer.echo(f"State: {job['state']}")
                typer.echo(f"Created: {job['created_at']}")
                typer.echo(f"Updated: {job['updated_at']}")
                
                if job['payload']:
                    import json
                    try:
                        payload = json.loads(job['payload'])
                        if 'error_message' in payload:
                            typer.echo(f"Error: {payload['error_message']}")
                    except json.JSONDecodeError:
                        typer.echo(f"Payload: {job['payload']}")
                
            else:
                # Show summary of all job states
                cursor.execute("""
                    SELECT state, COUNT(*) as count 
                    FROM ingestion_jobs 
                    GROUP BY state 
                    ORDER BY count DESC
                """)
                
                summary = cursor.fetchall()
                
                typer.echo("📊 Job Status Summary")
                typer.echo("=" * 30)
                total_jobs = 0
                for row in summary:
                    typer.echo(f"{row['state']:<12}: {row['count']:>6}")
                    total_jobs += row['count']
                
                typer.echo("-" * 30)
                typer.echo(f"{'TOTAL':<12}: {total_jobs:>6}")
                
                # Show recently active jobs
                cursor.execute("""
                    SELECT * FROM ingestion_jobs 
                    WHERE state IN ('PENDING', 'IN_PROGRESS') 
                    ORDER BY updated_at DESC 
                    LIMIT 10
                """)
                
                active_jobs = cursor.fetchall()
                if active_jobs:
                    typer.echo(f"\n🔄 Active Jobs ({len(active_jobs)})")
                    typer.echo("-" * 50)
                    for job in active_jobs:
                        updated = datetime.fromisoformat(job['updated_at'].replace('Z', '+00:00'))
                        hours_ago = (datetime.now(timezone.utc) - updated).total_seconds() / 3600
                        typer.echo(f"  Job {job['id']}: {job['symbol']} {job['day']} - {job['state']} ({hours_ago:.1f}h ago)")
                
    except Exception as e:
        typer.echo(f"❌ Database error: {e}")
        raise typer.Exit(1)

@jobs_app.command()
def doctor(
    fix: bool = typer.Option(False, "--fix", "-f", help="Automatically fix detected issues"),
    timeout_hours: int = typer.Option(6, "--timeout", "-t", help="Consider jobs stuck after N hours"),
):
    """Diagnose and fix common job issues.
    
    This command detects:
    - Jobs stuck in IN_PROGRESS state for too long
    - Jobs with inconsistent states
    
    Examples:
        marketpipe jobs doctor                      # Diagnose issues
        marketpipe jobs doctor --fix                # Auto-fix issues  
        marketpipe jobs doctor --timeout 12 --fix  # Fix jobs stuck >12 hours
    """
    
    db_path = _get_db_path()
    if not db_path:
        typer.echo("❌ No ingestion jobs database found")
        raise typer.Exit(1)
    
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            typer.echo("🔍 Running job diagnostics...")
            typer.echo("=" * 50)
            
            issues_found = []
            
            # Check for stuck jobs
            stuck_threshold = datetime.now(timezone.utc) - timedelta(hours=timeout_hours)
            
            cursor.execute("""
                SELECT id, symbol, day, state, created_at, updated_at 
                FROM ingestion_jobs 
                WHERE state = 'IN_PROGRESS' 
                AND updated_at < ?
                ORDER BY updated_at DESC
            """, (stuck_threshold.isoformat(),))
            
            stuck_jobs = cursor.fetchall()
            
            for job in stuck_jobs:
                updated = datetime.fromisoformat(job['updated_at'].replace('Z', '+00:00'))
                stuck_hours = (datetime.now(timezone.utc) - updated).total_seconds() / 3600
                
                issue = {
                    "type": "stuck_job",
                    "job_id": job['id'],
                    "symbol": job['symbol'], 
                    "day": job['day'],
                    "description": f"Job {job['id']} ({job['symbol']} {job['day']}) stuck for {stuck_hours:.1f} hours",
                    "severity": "high"
                }
                issues_found.append(issue)
                typer.echo(f"🚨 {issue['description']}")
            
            # Check for very old pending jobs
            old_threshold = datetime.now(timezone.utc) - timedelta(hours=timeout_hours * 2)
            
            cursor.execute("""
                SELECT id, symbol, day, state, created_at, updated_at 
                FROM ingestion_jobs 
                WHERE state = 'PENDING' 
                AND created_at < ?
                ORDER BY created_at DESC
            """, (old_threshold.isoformat(),))
            
            old_pending = cursor.fetchall()
            
            for job in old_pending:
                created = datetime.fromisoformat(job['created_at'].replace('Z', '+00:00'))
                pending_hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600
                
                issue = {
                    "type": "old_pending",
                    "job_id": job['id'],
                    "symbol": job['symbol'],
                    "day": job['day'], 
                    "description": f"Job {job['id']} ({job['symbol']} {job['day']}) pending for {pending_hours:.1f} hours",
                    "severity": "medium"
                }
                issues_found.append(issue)
                typer.echo(f"⚠️  {issue['description']}")
            
            # Summary
            typer.echo(f"\n📋 Diagnostic Summary:")
            typer.echo(f"   Issues found: {len(issues_found)}")
            
            high_severity = len([i for i in issues_found if i["severity"] == "high"])
            medium_severity = len([i for i in issues_found if i["severity"] == "medium"])
            
            if high_severity > 0:
                typer.echo(f"   🚨 High severity: {high_severity}")
            if medium_severity > 0:
                typer.echo(f"   ⚠️  Medium severity: {medium_severity}")
            
            # Fix issues if requested
            if fix and issues_found:
                typer.echo(f"\n🔧 Fixing {len(issues_found)} issues...")
                
                fixed_count = 0
                for issue in issues_found:
                    job_id = issue["job_id"]
                    
                    try:
                        cursor.execute("""
                            UPDATE ingestion_jobs 
                            SET state = 'FAILED', 
                                payload = json_set(COALESCE(payload, '{}'), '$.error_message', 'Auto-fixed: Job was stuck or too old') 
                            WHERE id = ?
                        """, (job_id,))
                        
                        typer.echo(f"   ✅ Fixed Job {job_id} ({issue['symbol']} {issue['day']})")
                        fixed_count += 1
                        
                    except Exception as e:
                        typer.echo(f"   ❌ Failed to fix Job {job_id}: {e}")
                
                conn.commit()
                typer.echo(f"\n🎯 Fixed {fixed_count}/{len(issues_found)} issues")
            
            elif issues_found:
                typer.echo(f"\n💡 Run with --fix to automatically resolve issues")
            
            if len(issues_found) == 0:
                typer.echo("\n🎉 No issues found! All jobs are healthy.")
            
    except Exception as e:
        typer.echo(f"❌ Database error: {e}")
        raise typer.Exit(1)

@jobs_app.command()
def kill(
    job_id: int = typer.Argument(..., help="Job ID to cancel/kill"),
    reason: str = typer.Option("Manual cancellation", "--reason", "-r", help="Reason for cancellation"),
):
    """Cancel or force-kill a specific job.
    
    Examples:
        marketpipe jobs kill 109                        # Cancel job 109
        marketpipe jobs kill 109 --reason "Timeout"    # Cancel with custom reason
    """
    
    db_path = _get_db_path()
    if not db_path:
        typer.echo("❌ No ingestion jobs database found")
        raise typer.Exit(1)
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Check if job exists
            cursor.execute("SELECT id, symbol, day, state FROM ingestion_jobs WHERE id = ?", (job_id,))
            job = cursor.fetchone()
            
            if not job:
                typer.echo(f"❌ Job {job_id} not found")
                raise typer.Exit(1)
            
            current_state = job[3]
            typer.echo(f"📊 Job {job_id} ({job[1]} {job[2]}) current state: {current_state}")
            
            if current_state in ["COMPLETED", "FAILED", "CANCELLED"]:
                typer.echo(f"ℹ️  Job is already in terminal state: {current_state}")
                return
            
            # Update the job
            cursor.execute("""
                UPDATE ingestion_jobs 
                SET state = 'CANCELLED', 
                    payload = json_set(COALESCE(payload, '{}'), '$.error_message', ?) 
                WHERE id = ?
            """, (f"Manual cancellation: {reason}", job_id))
            
            conn.commit()
            typer.echo(f"✅ Job {job_id} cancelled successfully")
            
    except Exception as e:
        typer.echo(f"❌ Database error: {e}")
        raise typer.Exit(1)

# Export the app
__all__ = ["jobs_app"] 