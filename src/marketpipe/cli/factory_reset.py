# SPDX-License-Identifier: Apache-2.0
"""Factory reset command for MarketPipe - nuclear option to wipe all data."""

from __future__ import annotations

import shutil
import sys
import datetime
from pathlib import Path
from typing import List

import typer


def _get_data_paths(base_dir: Path) -> List[Path]:
    """Get all data-related paths that should be wiped in factory reset."""
    paths = []
    
    # Main data directory
    data_dir = base_dir / "data"
    if data_dir.exists():
        paths.append(data_dir)
    
    # Database files in root
    for db_pattern in ["*.db", "*.sqlite", "*.sqlite3"]:
        paths.extend(base_dir.glob(db_pattern))
    
    # Log files
    for log_pattern in ["*.log", "logs/"]:
        paths.extend(base_dir.glob(log_pattern))
    
    # Temporary directories
    for temp_pattern in ["tmp/", "temp/", ".cache/"]:
        temp_path = base_dir / temp_pattern.rstrip("/")
        if temp_path.exists():
            paths.append(temp_path)
    
    # Coverage and test artifacts
    for artifact_pattern in ["htmlcov/", ".coverage*", ".pytest_cache/"]:
        paths.extend(base_dir.glob(artifact_pattern))
    
    return paths


def _format_size(path: Path) -> str:
    """Get human-readable size of a file or directory."""
    try:
        if path.is_file():
            size = path.stat().st_size
        elif path.is_dir():
            size = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
        else:
            return "0 B"
        
        # Convert to human readable
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"
    except (OSError, PermissionError):
        return "unknown"


def factory_reset(
    confirm_nuclear: bool = typer.Option(
        False, 
        "--confirm-nuclear", 
        help="Confirm you want to perform nuclear data wipe"
    ),
    keep_schema: bool = typer.Option(
        False,
        "--keep-schema",
        help="Keep database schema files and README files"
    ),
    backup_before_wipe: bool = typer.Option(
        False,
        "--backup-before-wipe",
        help="Create backup archive before wiping"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-n",
        help="Show what would be deleted without making changes"
    ),
    base_directory: Path = typer.Option(
        Path.cwd(),
        "--base-dir",
        help="Base directory to search for data files"
    )
):
    """
    🚨 NUCLEAR OPTION: Factory reset to wipe ALL data, history, and metrics.
    
    This command will permanently delete:
    - All ingested market data (data/)
    - All databases (*.db, *.sqlite)
    - All metrics and logs (*.log, logs/)
    - All caches and temporary files
    - All test artifacts and coverage reports
    
    USE WITH EXTREME CAUTION - DATA CANNOT BE RECOVERED!
    """
    
    print("🚨 MarketPipe Factory Reset - NUCLEAR DATA WIPE")
    print("=" * 60)
    
    # Get all paths that would be affected
    paths_to_remove = _get_data_paths(base_directory)
    
    if not paths_to_remove:
        print("✅ No data files found - system is already clean")
        return
    
    # Calculate total size
    total_size = 0
    file_count = 0
    
    print("📋 Files and directories to be removed:")
    print("-" * 40)
    
    for path in sorted(paths_to_remove):
        if not path.exists():
            continue
            
        size_str = _format_size(path)
        path_type = "📁" if path.is_dir() else "📄"
        rel_path = path.relative_to(base_directory)
        
        print(f"{path_type} {rel_path} ({size_str})")
        
        # Count files for summary
        if path.is_file():
            file_count += 1
            total_size += path.stat().st_size
        elif path.is_dir():
            try:
                dir_files = list(path.rglob('*'))
                file_count += len([f for f in dir_files if f.is_file()])
                total_size += sum(f.stat().st_size for f in dir_files if f.is_file())
            except (OSError, PermissionError):
                pass
    
    print("-" * 40)
    # Format total size
    total_size_str = _format_size(Path(f"/tmp/dummy_file_of_size_{total_size}"))
    for unit in ['B', 'KB', 'MB', 'GB']:
        if total_size < 1024.0:
            total_size_str = f"{total_size:.1f} {unit}"
            break
        total_size /= 1024.0
    else:
        total_size_str = f"{total_size:.1f} TB"
    
    print(f"📊 Total: {file_count} files, {total_size_str} disk space")
    
    # Safety checks
    if not confirm_nuclear:
        print("\n❌ SAFETY CHECK FAILED")
        print("   Add --confirm-nuclear flag to proceed with data wipe")
        print("   This operation is IRREVERSIBLE and will delete ALL data")
        sys.exit(1)
    
    if dry_run:
        print("\n🔍 DRY RUN - No files would be deleted")
        print("   Remove --dry-run flag to execute factory reset")
        return
    
    # Final confirmation  
    print(f"\n⚠️  You are about to DELETE {file_count} files ({total_size_str})")
    print("   This action is PERMANENT and CANNOT be undone!")
    
    confirmation = typer.prompt(
        "\nType 'FACTORY-RESET-CONFIRMED' to proceed",
        type=str
    )
    
    if confirmation != "FACTORY-RESET-CONFIRMED":
        print("❌ Confirmation failed - factory reset cancelled")
        sys.exit(1)
    
    # Create backup if requested
    if backup_before_wipe:
        backup_name = f"marketpipe-backup-{Path.cwd().name}-{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        backup_path = base_directory.parent / f"{backup_name}.tar.gz"
        
        print(f"\n📦 Creating backup: {backup_path}")
        try:
            import subprocess
            subprocess.run([
                "tar", "-czf", str(backup_path),
                "-C", str(base_directory.parent),
                "--exclude=venv", "--exclude=.git", "--exclude=__pycache__",
                base_directory.name
            ], check=True)
            print(f"✅ Backup created: {backup_path}")
        except Exception as e:
            print(f"❌ Backup failed: {e}")
            if not typer.confirm("Continue without backup?"):
                sys.exit(1)
    
    # Execute nuclear wipe
    print(f"\n🚨 EXECUTING NUCLEAR WIPE...")
    removed_count = 0
    
    for path in paths_to_remove:
        if not path.exists():
            continue
            
        try:
            if keep_schema and path.name in ["README.md", "schema", "migrations"]:
                print(f"🛡️  Keeping (schema): {path.relative_to(base_directory)}")
                continue
                
            if path.is_file():
                path.unlink()
                print(f"🗑️  Removed file: {path.relative_to(base_directory)}")
            elif path.is_dir():
                shutil.rmtree(path)
                print(f"🗑️  Removed directory: {path.relative_to(base_directory)}")
            
            removed_count += 1
            
        except Exception as e:
            print(f"❌ Failed to remove {path.relative_to(base_directory)}: {e}")
    
    print("\n" + "=" * 60)
    print("🎯 FACTORY RESET COMPLETE")
    print(f"✅ Removed {removed_count} items")
    print("✅ System returned to factory state")
    print("💡 Run 'python -m marketpipe migrate' to initialize fresh databases")
    print("=" * 60)


if __name__ == "__main__":
    typer.run(factory_reset)