"""Real PostgreSQL migrations and job repository behavior in disposable databases."""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

pytestmark = pytest.mark.postgres


@pytest.fixture
def postgres_url(monkeypatch):
    url = os.environ.get("MARKETPIPE_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("Dedicated test PostgreSQL URL not configured")
    url = (
        sa.engine.make_url(url)
        .set(drivername="postgresql+psycopg2")
        .render_as_string(hide_password=False)
    )
    monkeypatch.delenv("DATABASE_URL", raising=False)
    admin = sa.create_engine(url, isolation_level="AUTOCOMMIT")
    name = "marketpipe_test_" + uuid.uuid4().hex
    try:
        with admin.connect() as connection:
            connection.execute(sa.text(f'CREATE DATABASE "{name}"'))
        test_url = sa.engine.make_url(url).set(database=name).render_as_string(hide_password=False)
        root = Path(__file__).resolve().parents[2]
        config = Config(str(root / "alembic.ini"))
        config.set_main_option("script_location", str(root / "alembic"))
        config.set_main_option("sqlalchemy.url", test_url.replace("%", "%%"))
        command.upgrade(config, "head")
        yield test_url
    finally:
        with admin.connect() as connection:
            connection.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


def test_postgres_fresh_schema_indexes_and_large_timestamps(postgres_url):
    engine = sa.create_engine(postgres_url)
    try:
        with engine.begin() as connection:
            assert (
                connection.execute(sa.text("SELECT version_num FROM alembic_version")).scalar()
                == "0005"
            )
            connection.execute(
                sa.text(
                    """INSERT INTO ohlcv_bars
                (id, symbol, timestamp_ns, open_price, high_price, low_price, close_price, volume)
                VALUES ('test', 'AAPL', 1705329000000000000, '100', '102', '99', '101', 10)"""
                )
            )
            assert (
                connection.execute(sa.text("SELECT timestamp_ns FROM ohlcv_bars")).scalar()
                == 1705329000000000000
            )
            assert "idx_metrics_name_ts" in {
                index["name"] for index in sa.inspect(connection).get_indexes("metrics")
            }
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_postgres_job_save_fetch_delete_and_concurrent_claims(postgres_url):
    from datetime import date

    from marketpipe.domain.value_objects import Symbol, TimeRange
    from marketpipe.ingestion.domain.entities import IngestionJob, IngestionJobId, ProcessingState
    from marketpipe.ingestion.domain.value_objects import IngestionConfiguration
    from marketpipe.ingestion.infrastructure.postgres_repository import (
        PostgresIngestionJobRepository,
    )

    repository = PostgresIngestionJobRepository(
        postgres_url.replace("postgresql+psycopg2://", "postgresql://")
    )
    try:
        for ticker in ("AAPL", "MSFT", "GOOGL"):
            job = IngestionJob(
                job_id=IngestionJobId(Symbol(ticker), "2024-01-15"),
                symbols=[Symbol(ticker)],
                time_range=TimeRange.from_dates(date(2024, 1, 15), date(2024, 1, 16)),
                configuration=IngestionConfiguration(
                    output_path=Path("unused"),
                    compression="snappy",
                    max_workers=2,
                    batch_size=1000,
                    rate_limit_per_minute=200,
                    feed_type="iex",
                ),
            )
            await repository.save(job)
        identity = IngestionJobId(Symbol("AAPL"), "2024-01-15")
        restored = await repository.get_by_id(identity)
        assert restored.job_id == identity
        first, second = await asyncio.wait_for(
            asyncio.gather(
                repository.fetch_and_lock(ProcessingState.PENDING, 2),
                repository.fetch_and_lock(ProcessingState.PENDING, 2),
            ),
            timeout=5,
        )
        claims = [str(job.job_id) for job in first + second]
        assert len(claims) == len(set(claims)) == 3
        assert await repository.delete(identity)
        assert await repository.get_by_id(identity) is None
    finally:
        await repository.close()
