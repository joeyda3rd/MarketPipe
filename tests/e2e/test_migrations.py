"""Fresh and historical migrations run against real isolated databases."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from urllib.parse import urlencode

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

pytestmark = pytest.mark.e2e


@pytest.mark.parametrize(
    "backend", ["sqlite", pytest.param("postgres", marks=pytest.mark.postgres)]
)
def test_upgrade_preserves_existing_bars_and_is_repeatable(tmp_path, monkeypatch, backend):
    if backend == "postgres":
        url = os.environ.get("MARKETPIPE_TEST_POSTGRES_URL")
        if not url:
            pytest.skip("Dedicated test PostgreSQL URL not configured")
        url = (
            sa.engine.make_url(url)
            .set(drivername="postgresql+psycopg2")
            .render_as_string(hide_password=False)
        )
    else:
        url = f"sqlite:///{tmp_path / 'migration.db'}"
    # Explicit dedicated database only; production DATABASE_URL is never a test input.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    config.set_main_option("sqlalchemy.url", url)
    admin = None
    schema = None
    if backend == "postgres":
        admin = sa.create_engine(url)
        schema = "test_" + uuid.uuid4().hex
        with admin.begin() as connection:
            connection.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
        url += ("&" if "?" in url else "?") + urlencode({"options": f"-csearch_path={schema}"})
        config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    engine = sa.create_engine(url)
    try:
        command.upgrade(config, "0001")
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    """
                INSERT INTO ohlcv_bars
                (id, symbol, timestamp_ns, open_price, high_price, low_price, close_price, volume)
                VALUES ('historical', 'AAPL', 1705329000000000000, '100', '102', '99', '101', 10)
            """
                )
            )
        command.upgrade(config, "head")
        command.upgrade(config, "head")
        with engine.connect() as connection:
            assert connection.execute(
                sa.text(
                    "SELECT symbol, timestamp_ns, close_price, volume, trading_date FROM ohlcv_bars"
                )
            ).fetchall() == [("AAPL", 1705329000000000000, "101", 10, "2024-01-15")]
            assert (
                connection.execute(sa.text("SELECT version_num FROM alembic_version")).scalar()
                == "0005"
            )
            assert "ingestion_jobs" in sa.inspect(connection).get_table_names()
            assert {index["name"] for index in sa.inspect(connection).get_indexes("metrics")} >= {
                "idx_metrics_name_ts",
                "idx_metrics_provider_feed",
                "idx_metrics_name_provider_feed",
            }
    finally:
        engine.dispose()
        if admin is not None:
            with admin.begin() as connection:
                connection.execute(sa.text(f'DROP SCHEMA "{schema}" CASCADE'))
            admin.dispose()
