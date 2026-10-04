"""Database initialization must preserve application loggers."""

import logging

from marketpipe.bootstrap.interfaces import AlembicMigrationService


def test_alembic_migrations_keep_existing_validation_logger_enabled(tmp_path, monkeypatch):
    logger = logging.getLogger("marketpipe.symbols.validation")
    monkeypatch.setattr(logger, "disabled", False)
    AlembicMigrationService().apply_migrations(tmp_path / "core.db")
    assert logger.disabled is False
