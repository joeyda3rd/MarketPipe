# SPDX-License-Identifier: Apache-2.0
"""Repository factory for auto-selecting storage backend."""

from __future__ import annotations

import logging
import os

from marketpipe.ingestion.domain.repositories import IIngestionJobRepository

from .postgres_repository import PostgresIngestionJobRepository
from .repositories import SqliteIngestionJobRepository

logger = logging.getLogger(__name__)


def create_ingestion_job_repository() -> IIngestionJobRepository:
    """
    Create appropriate ingestion job repository based on environment configuration.

    Uses DATABASE_URL environment variable to determine backend:
    - If DATABASE_URL starts with "postgres" -> PostgreSQL
      Supports: postgres://, postgresql://, postgresql+asyncpg://, postgresql+psycopg://
    - Otherwise -> SQLite (default)

    Returns:
        Configured repository instance
    """
    database_url = os.getenv("DATABASE_URL")

    if database_url and database_url.split("://", 1)[0].startswith("postgres"):
        logger.info(f"Using PostgreSQL repository with DSN: {_mask_credentials(database_url)}")
        return PostgresIngestionJobRepository(database_url)
    else:
        logger.info("Using SQLite repository (default)")
        return SqliteIngestionJobRepository()


def create_ingestion_job_repository_with_url(
    database_url: Optional[str] = None,
) -> IIngestionJobRepository:
    """
    Create repository with explicit database URL override.

    Args:
        database_url: Optional database URL override

    Returns:
        Configured repository instance
    """
    if database_url and database_url.split("://", 1)[0].startswith("postgres"):
        logger.info(
            f"Using PostgreSQL repository with explicit DSN: {_mask_credentials(database_url)}"
        )
        return PostgresIngestionJobRepository(database_url)
    else:
        logger.info("Using SQLite repository")
        return SqliteIngestionJobRepository()


def _mask_credentials(url: str) -> str:
    """Mask credentials in database URL for safe logging."""
    try:
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(url)
        if parsed.username:
            # Replace username:password with masked version
            masked_netloc = f"{parsed.username}:***@{parsed.hostname}"
            if parsed.port:
                masked_netloc += f":{parsed.port}"

            masked_parsed = parsed._replace(netloc=masked_netloc)
            return urlunparse(masked_parsed)
        return url
    except Exception:
        # Fallback if URL parsing fails
        return url.split("://")[0] + "://***"
