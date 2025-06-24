"""SQLite migration system for MarketPipe.

Provides automatic schema migration management with version tracking
and idempotent execution of SQL migration files.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


def apply_pending(db_path: Path) -> None:
    """Apply any pending migrations to the database.

    Args:
        db_path: Path to SQLite database file

    This function:
    1. Creates the schema_version table if it doesn't exist
    2. Scans migrations/versions/*.sql files in lexicographic order
    3. Executes scripts whose filename prefix is not yet in schema_version
    4. Uses one transaction per file with rollback on failure
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Get migrations directory relative to this module
    migrations_dir = Path(__file__).parent / "versions"

    # Ensure migrations directory exists
    migrations_dir.mkdir(exist_ok=True)

    # Initialize schema version tracking
    _ensure_schema_version_table(db_path)

    # Get applied migrations
    applied_versions = _get_applied_versions(db_path)

    # Get pending migrations
    migration_files = sorted(migrations_dir.glob("*.sql"))
    pending_migrations = []

    for migration_file in migration_files:
        version = migration_file.stem.split("_")[0]  # Extract version prefix
        if version not in applied_versions:
            pending_migrations.append((version, migration_file))

    if not pending_migrations:
        logger.info("No pending migrations")
        return

    # Apply pending migrations
    logger.info(f"Applying {len(pending_migrations)} pending migrations...")

    for version, migration_file in pending_migrations:
        try:
            _apply_migration(db_path, version, migration_file)
            logger.info(f"Applied migration {version}: {migration_file.name}")
        except Exception as e:
            logger.error(f"Failed to apply migration {version}: {e}")
            raise

    logger.info(f"Successfully applied {len(pending_migrations)} migrations")


def _ensure_schema_version_table(db_path: Path) -> None:
    """Create schema_version table if it doesn't exist."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                version TEXT PRIMARY KEY,
                applied_ts INTEGER NOT NULL
            )
        """
        )
        conn.commit()


def _get_applied_versions(db_path: Path) -> List[str]:
    """Get list of applied migration versions."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("SELECT version FROM schema_version ORDER BY version")
        return [row[0] for row in cursor.fetchall()]


def _apply_migration(db_path: Path, version: str, migration_file: Path) -> None:
    """Apply a single migration file within a transaction."""
    migration_sql = migration_file.read_text(encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        try:
            # Execute migration SQL
            conn.executescript(migration_sql)

            # Record successful application (using INSERT OR IGNORE to prevent duplicates)
            conn.execute(
                "INSERT OR IGNORE INTO schema_version (version, applied_ts) VALUES (?, ?)",
                (version, int(__import__("time").time())),
            )

            conn.commit()

        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Migration {version} failed: {e}") from e
