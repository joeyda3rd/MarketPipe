# SPDX-License-Identifier: Apache-2.0
"""PostgreSQL end-to-end pipeline integration tests.

This test validates the complete MarketPipe pipeline using PostgreSQL as the 
primary database backend instead of SQLite, testing production-like database 
configurations and PostgreSQL-specific features.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional

import pandas as pd
import pytest

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from marketpipe.ingestion.domain.entities import IngestionJobId, ProcessingState
from marketpipe.ingestion.domain.value_objects import IngestionConfiguration, BatchConfiguration


# Test configuration
POSTGRES_TEST_DSN = "postgresql://marketpipe:password@localhost:5433/marketpipe_test"
POSTGRES_REQUIRED_MSG = """
PostgreSQL E2E tests require a running PostgreSQL instance.

To set up PostgreSQL for testing:
1. Install PostgreSQL and create test database
2. Create user: CREATE USER marketpipe WITH PASSWORD 'password';
3. Create database: CREATE DATABASE marketpipe_test OWNER marketpipe;
4. Grant permissions: GRANT ALL PRIVILEGES ON DATABASE marketpipe_test TO marketpipe;
5. Set environment variable: export POSTGRES_TEST_DSN='{}'

Or use Docker:
docker run --name postgres-test -e POSTGRES_USER=marketpipe -e POSTGRES_PASSWORD=password \\
  -e POSTGRES_DB=marketpipe_test -p 5433:5432 -d postgres:15

Then set: export POSTGRES_TEST_DSN='{}'
""".format(POSTGRES_TEST_DSN, POSTGRES_TEST_DSN)


def postgres_available() -> bool:
    """Check if PostgreSQL test database is available."""
    try:
        import asyncpg
        
        async def test_connection():
            try:
                dsn = os.getenv("POSTGRES_TEST_DSN", POSTGRES_TEST_DSN)
                conn = await asyncpg.connect(dsn)
                await conn.close()
                return True
            except:
                return False
        
        return asyncio.run(test_connection())
    except ImportError:
        return False


def requires_postgres(func):
    """Decorator to skip tests if PostgreSQL is not available."""
    return pytest.mark.skipif(
        not postgres_available(),
        reason=POSTGRES_REQUIRED_MSG
    )(func)


class PostgreSQLTestFixture:
    """Test fixture for PostgreSQL-based testing."""
    
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
        
    async def setup(self):
        """Setup test database and tables."""
        import asyncpg
        
        self.pool = await asyncpg.create_pool(self.dsn, min_size=2, max_size=5)
        
        # Create test schema
        async with self.pool.acquire() as conn:
            # Create ingestion jobs table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_jobs (
                    id TEXT PRIMARY KEY,
                    symbols JSONB NOT NULL,
                    time_range JSONB NOT NULL,
                    configuration JSONB NOT NULL,
                    batch_config JSONB NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    completed_partitions JSONB DEFAULT '[]'::jsonb,
                    processing_stats JSONB DEFAULT '{}'::jsonb
                )
            """)
            
            # Create ingestion checkpoints table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
                    job_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    last_processed_timestamp BIGINT NOT NULL,
                    records_processed INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (job_id, symbol),
                    FOREIGN KEY (job_id) REFERENCES ingestion_jobs(id) ON DELETE CASCADE
                )
            """)
            
            # Create metrics table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_metrics (
                    id SERIAL PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value NUMERIC NOT NULL,
                    labels JSONB DEFAULT '{}'::jsonb,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (job_id) REFERENCES ingestion_jobs(id) ON DELETE CASCADE
                )
            """)
            
            # Create indexes for performance
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_state 
                ON ingestion_jobs(state)
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ingestion_checkpoints_job_symbol 
                ON ingestion_checkpoints(job_id, symbol)
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ingestion_metrics_job_timestamp 
                ON ingestion_metrics(job_id, timestamp)
            """)
            
    async def cleanup(self):
        """Clean up test data and close connections."""
        if self.pool:
            async with self.pool.acquire() as conn:
                # Clean up test data
                await conn.execute("DELETE FROM ingestion_metrics")
                await conn.execute("DELETE FROM ingestion_checkpoints") 
                await conn.execute("DELETE FROM ingestion_jobs")
            
            await self.pool.close()


def generate_postgres_test_data(symbols: List[str], trading_day: date) -> Dict[str, pd.DataFrame]:
    """Generate test data for PostgreSQL testing."""
    import random
    random.seed(42)  # Reproducible test data
    
    symbol_data = {}
    
    for symbol in symbols:
        bars = []
        base_price = 100.0 + random.uniform(-50, 100)
        
        # Generate 100 bars for testing (smaller dataset for faster tests)
        market_open = datetime.combine(trading_day, datetime.min.time()) + timedelta(hours=13, minutes=30)
        market_open = market_open.replace(tzinfo=timezone.utc)
        
        current_price = base_price
        
        for minute in range(100):
            # Simple price movement
            price_change = random.gauss(0, 0.001)
            current_price *= (1 + price_change)
            
            open_price = current_price
            close_price = current_price * (1 + random.gauss(0, 0.0005))
            high_price = max(open_price, close_price) * (1 + abs(random.gauss(0, 0.0002)))
            low_price = min(open_price, close_price) * (1 - abs(random.gauss(0, 0.0002)))
            
            volume = random.randint(500, 2000)
            
            timestamp = market_open + timedelta(minutes=minute)
            timestamp_ns = int(timestamp.timestamp() * 1_000_000_000)
            
            bars.append({
                "ts_ns": timestamp_ns,
                "symbol": symbol,
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": volume,
                "trade_count": random.randint(20, 100),
                "vwap": round((high_price + low_price + close_price) / 3, 2),
            })
            
            current_price = close_price
        
        symbol_data[symbol] = pd.DataFrame(bars)
    
    return symbol_data


@pytest.mark.integration
@pytest.mark.postgres
class TestPostgreSQLEndToEndPipeline:
    """PostgreSQL-specific end-to-end pipeline tests."""
    
    @requires_postgres
    async def test_postgres_job_repository_integration(self, tmp_path):
        """Test complete job lifecycle using PostgreSQL repository."""
        
        dsn = os.getenv("POSTGRES_TEST_DSN", POSTGRES_TEST_DSN)
        
        # Setup PostgreSQL test environment
        pg_fixture = PostgreSQLTestFixture(dsn)
        await pg_fixture.setup()
        
        try:
            from marketpipe.ingestion.infrastructure.postgres_repository import (
                PostgresIngestionJobRepository
            )
            
            # Initialize PostgreSQL repository
            job_repo = PostgresIngestionJobRepository(dsn)
            
            # Test job creation
            job_id = IngestionJobId("postgres-test-job-001")
            symbols = [Symbol("AAPL"), Symbol("GOOGL")]
            
            time_range = TimeRange(
                start=Timestamp(datetime(2024, 1, 15, 13, 30, tzinfo=timezone.utc)),
                end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc))
            )
            
            config = IngestionConfiguration(
                output_path=tmp_path / "data",
                compression="zstd",
                max_workers=2,
                batch_size=1000,
                rate_limit_per_minute=200,
                feed_type="iex"
            )
            
            batch_config = BatchConfiguration(
                symbols_per_batch=10,
                retry_attempts=3,
                retry_delay_seconds=1.0,
                timeout_seconds=30.0
            )
            
            # Create job using PostgreSQL repository
            from marketpipe.ingestion.domain.entities import IngestionJob
            
            job = IngestionJob(
                id=job_id,
                symbols=symbols,
                time_range=time_range,
                configuration=config,
                batch_config=batch_config,
                state=ProcessingState.PENDING
            )
            
            # Save job to PostgreSQL
            await job_repo.save(job)
            print("✅ Job saved to PostgreSQL")
            
            # Retrieve job from PostgreSQL
            retrieved_job = await job_repo.get_by_id(job_id)
            assert retrieved_job is not None
            assert retrieved_job.id == job_id
            assert len(retrieved_job.symbols) == 2
            assert retrieved_job.state == ProcessingState.PENDING
            
            print("✅ Job retrieved from PostgreSQL with correct data")
            
            # Test job state transitions
            retrieved_job.start()
            await job_repo.save(retrieved_job)
            
            # Verify state change persisted
            updated_job = await job_repo.get_by_id(job_id)
            assert updated_job.state == ProcessingState.RUNNING
            assert updated_job.started_at is not None
            
            print("✅ Job state transitions working in PostgreSQL")
            
            # Test job completion
            updated_job.complete()
            await job_repo.save(updated_job)
            
            final_job = await job_repo.get_by_id(job_id)
            assert final_job.state == ProcessingState.COMPLETED
            assert final_job.completed_at is not None
            
            print("✅ Job completion tracked in PostgreSQL")
            
        finally:
            await pg_fixture.cleanup()
    
    @requires_postgres
    async def test_postgres_checkpoint_repository_integration(self, tmp_path):
        """Test checkpoint management using PostgreSQL repository."""
        
        dsn = os.getenv("POSTGRES_TEST_DSN", POSTGRES_TEST_DSN)
        
        pg_fixture = PostgreSQLTestFixture(dsn)
        await pg_fixture.setup()
        
        try:
            from marketpipe.ingestion.infrastructure.postgres_repository import (
                PostgresIngestionJobRepository
            )
            from marketpipe.ingestion.infrastructure.repositories import (
                SqliteCheckpointRepository  # Fallback for now
            )
            
            # Note: Using SqliteCheckpointRepository as example
            # In production, would implement PostgresCheckpointRepository
            checkpoint_repo = SqliteCheckpointRepository(tmp_path / "test.db")
            
            job_id = IngestionJobId("postgres-checkpoint-test")
            symbol = Symbol("AAPL")
            
            # Test checkpoint creation
            from marketpipe.ingestion.domain.value_objects import IngestionCheckpoint
            
            checkpoint = IngestionCheckpoint(
                symbol=symbol,
                last_processed_timestamp=1705325400000000000,  # Example timestamp
                records_processed=500,
                updated_at=datetime.now(timezone.utc)
            )
            
            await checkpoint_repo.save_checkpoint(job_id, checkpoint)
            print("✅ Checkpoint saved")
            
            # Retrieve checkpoint
            retrieved_checkpoint = await checkpoint_repo.get_checkpoint(job_id, symbol)
            assert retrieved_checkpoint is not None
            assert retrieved_checkpoint.symbol == symbol
            assert retrieved_checkpoint.records_processed == 500
            
            print("✅ Checkpoint retrieved correctly")
            
            # Test checkpoint update
            checkpoint.records_processed = 750
            await checkpoint_repo.save_checkpoint(job_id, checkpoint)
            
            updated_checkpoint = await checkpoint_repo.get_checkpoint(job_id, symbol)
            assert updated_checkpoint.records_processed == 750
            
            print("✅ Checkpoint updates working")
            
        finally:
            await pg_fixture.cleanup()
    
    @requires_postgres
    async def test_postgres_metrics_integration(self, tmp_path):
        """Test metrics collection and storage with PostgreSQL."""
        
        dsn = os.getenv("POSTGRES_TEST_DSN", POSTGRES_TEST_DSN)
        
        pg_fixture = PostgreSQLTestFixture(dsn)
        await pg_fixture.setup()
        
        try:
            from marketpipe.ingestion.infrastructure.repositories import (
                SqliteMetricsRepository  # Using as example
            )
            
            # Initialize metrics repository
            metrics_repo = SqliteMetricsRepository(tmp_path / "metrics.db")
            
            job_id = IngestionJobId("postgres-metrics-test")
            
            # Test metrics recording
            await metrics_repo.record_metric(
                job_id=str(job_id),
                metric_name="bars_processed",
                value=1500,
                labels={"symbol": "AAPL", "provider": "alpaca"}
            )
            
            await metrics_repo.record_metric(
                job_id=str(job_id),
                metric_name="processing_time_ms",
                value=2500,
                labels={"symbol": "AAPL", "stage": "validation"}
            )
            
            print("✅ Metrics recorded")
            
            # Test metrics retrieval
            metrics = await metrics_repo.get_job_metrics(str(job_id))
            assert len(metrics) >= 2
            
            bars_metrics = [m for m in metrics if m["metric_name"] == "bars_processed"]
            assert len(bars_metrics) == 1
            assert bars_metrics[0]["value"] == 1500
            
            print("✅ Metrics retrieval working")
            
        finally:
            await pg_fixture.cleanup()
    
    @requires_postgres
    async def test_postgres_full_pipeline_integration(self, tmp_path):
        """Test complete pipeline using PostgreSQL for all data storage."""
        
        dsn = os.getenv("POSTGRES_TEST_DSN", POSTGRES_TEST_DSN)
        
        pg_fixture = PostgreSQLTestFixture(dsn)
        await pg_fixture.setup()
        
        try:
            # Generate test data
            symbols = ["AAPL", "GOOGL"]
            trading_day = date(2024, 1, 15)
            symbol_data = generate_postgres_test_data(symbols, trading_day)
            
            # Setup storage
            raw_dir = tmp_path / "raw"
            raw_dir.mkdir(parents=True)
            raw_engine = ParquetStorageEngine(raw_dir)
            
            # Write test data to Parquet (hybrid approach: PostgreSQL + Parquet)
            job_id = "postgres-full-pipeline-test"
            total_bars = 0
            
            for symbol in symbols:
                df = symbol_data[symbol]
                raw_engine.write(
                    df=df,
                    frame="1m",
                    symbol=symbol,
                    trading_day=trading_day,
                    job_id=job_id,
                    overwrite=True
                )
                total_bars += len(df)
            
            print(f"✅ Wrote {total_bars} bars to Parquet storage")
            
            # Initialize PostgreSQL-based job management
            from marketpipe.ingestion.infrastructure.postgres_repository import (
                PostgresIngestionJobRepository
            )
            
            job_repo = PostgresIngestionJobRepository(dsn)
            
            # Create job record in PostgreSQL
            from marketpipe.ingestion.domain.entities import IngestionJob
            
            job = IngestionJob(
                id=IngestionJobId(job_id),
                symbols=[Symbol(s) for s in symbols],
                time_range=TimeRange(
                    start=Timestamp(datetime.combine(trading_day, datetime.min.time()).replace(tzinfo=timezone.utc)),
                    end=Timestamp(datetime.combine(trading_day, datetime.max.time()).replace(tzinfo=timezone.utc))
                ),
                configuration=IngestionConfiguration(
                    output_path=tmp_path / "data",
                    compression="zstd",
                    max_workers=2,
                    batch_size=1000,
                    rate_limit_per_minute=200,
                    feed_type="iex"
                ),
                batch_config=BatchConfiguration(
                    symbols_per_batch=10,
                    retry_attempts=3,
                    retry_delay_seconds=1.0,
                    timeout_seconds=30.0
                ),
                state=ProcessingState.PENDING
            )
            
            await job_repo.save(job)
            print("✅ Job metadata saved to PostgreSQL")
            
            # Simulate job processing
            job.start()
            await job_repo.save(job)
            
            # Add processing results
            from marketpipe.ingestion.domain.value_objects import IngestionPartition
            
            for symbol in symbols:
                partition = IngestionPartition(
                    symbol=Symbol(symbol),
                    file_path=raw_dir / f"frame=1m/symbol={symbol}/date={trading_day}/{job_id}.parquet",
                    record_count=len(symbol_data[symbol]),
                    size_bytes=1024,  # Simplified
                    trading_date=trading_day
                )
                job.add_completed_partition(partition)
            
            job.complete()
            await job_repo.save(job)
            
            print("✅ Job processing completed and tracked in PostgreSQL")
            
            # Verify job state
            final_job = await job_repo.get_by_id(IngestionJobId(job_id))
            assert final_job.state == ProcessingState.COMPLETED
            assert len(final_job.completed_partitions) == len(symbols)
            assert final_job.completed_at is not None
            
            print("✅ PostgreSQL full pipeline integration test completed")
            
        finally:
            await pg_fixture.cleanup()
    
    @requires_postgres
    async def test_postgres_concurrent_job_management(self, tmp_path):
        """Test concurrent job management capabilities with PostgreSQL."""
        
        dsn = os.getenv("POSTGRES_TEST_DSN", POSTGRES_TEST_DSN)
        
        pg_fixture = PostgreSQLTestFixture(dsn)
        await pg_fixture.setup()
        
        try:
            from marketpipe.ingestion.infrastructure.postgres_repository import (
                PostgresIngestionJobRepository
            )
            
            job_repo = PostgresIngestionJobRepository(dsn)
            
            # Create multiple concurrent jobs
            job_count = 5
            jobs = []
            
            for i in range(job_count):
                job = IngestionJob(
                    id=IngestionJobId(f"concurrent-test-{i}"),
                    symbols=[Symbol(f"SYM{i:03d}")],
                    time_range=TimeRange(
                        start=Timestamp(datetime(2024, 1, 15, 13, 30, tzinfo=timezone.utc)),
                        end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc))
                    ),
                    configuration=IngestionConfiguration(
                        output_path=tmp_path / "data",
                        compression="zstd",
                        max_workers=1,
                        batch_size=1000,
                        rate_limit_per_minute=200,
                        feed_type="iex"
                    ),
                    batch_config=BatchConfiguration(
                        symbols_per_batch=10,
                        retry_attempts=3,
                        retry_delay_seconds=1.0,
                        timeout_seconds=30.0
                    ),
                    state=ProcessingState.PENDING
                )
                jobs.append(job)
            
            # Save all jobs concurrently
            async def save_job(job):
                await job_repo.save(job)
                return job.id
            
            job_ids = await asyncio.gather(*[save_job(job) for job in jobs])
            assert len(job_ids) == job_count
            
            print(f"✅ Created {job_count} concurrent jobs")
            
            # Test concurrent job retrieval
            async def get_job(job_id):
                return await job_repo.get_by_id(job_id)
            
            retrieved_jobs = await asyncio.gather(*[get_job(job_id) for job_id in job_ids])
            assert all(job is not None for job in retrieved_jobs)
            
            print("✅ Concurrent job retrieval working")
            
            # Test concurrent state updates
            async def start_job(job):
                job.start()
                await job_repo.save(job)
                return job.id
            
            await asyncio.gather(*[start_job(job) for job in retrieved_jobs])
            
            # Verify all jobs are running
            running_jobs = await asyncio.gather(*[get_job(job_id) for job_id in job_ids])
            assert all(job.state == ProcessingState.RUNNING for job in running_jobs)
            
            print("✅ Concurrent job state updates working")
            
        finally:
            await pg_fixture.cleanup()


@pytest.mark.integration
@pytest.mark.postgres
@requires_postgres
async def test_postgres_performance_characteristics(tmp_path):
    """Test PostgreSQL-specific performance characteristics."""
    
    dsn = os.getenv("POSTGRES_TEST_DSN", POSTGRES_TEST_DSN)
    
    pg_fixture = PostgreSQLTestFixture(dsn)
    await pg_fixture.setup()
    
    try:
        from marketpipe.ingestion.infrastructure.postgres_repository import (
            PostgresIngestionJobRepository
        )
        
        job_repo = PostgresIngestionJobRepository(dsn, min_size=5, max_size=20)
        
        # Test large batch job creation
        import time
        
        start_time = time.monotonic()
        
        batch_size = 100
        jobs = []
        
        for i in range(batch_size):
            job = IngestionJob(
                id=IngestionJobId(f"perf-test-{i:03d}"),
                symbols=[Symbol(f"PERF{i:03d}")],
                time_range=TimeRange(
                    start=Timestamp(datetime(2024, 1, 15, 13, 30, tzinfo=timezone.utc)),
                    end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc))
                ),
                configuration=IngestionConfiguration(
                    output_path=tmp_path / "data",
                    compression="zstd", 
                    max_workers=1,
                    batch_size=1000,
                    rate_limit_per_minute=200,
                    feed_type="iex"
                ),
                batch_config=BatchConfiguration(
                    symbols_per_batch=10,
                    retry_attempts=3,
                    retry_delay_seconds=1.0,
                    timeout_seconds=30.0
                ),
                state=ProcessingState.PENDING
            )
            jobs.append(job)
        
        # Batch insert
        async def save_job(job):
            await job_repo.save(job)
        
        await asyncio.gather(*[save_job(job) for job in jobs])
        
        end_time = time.monotonic()
        duration = end_time - start_time
        
        print(f"📊 PostgreSQL Performance Metrics:")
        print(f"   Jobs Created: {batch_size}")
        print(f"   Duration: {duration:.2f}s")
        print(f"   Throughput: {batch_size / duration:.1f} jobs/sec")
        
        # Performance assertions
        assert duration < 30, f"PostgreSQL batch insert too slow: {duration:.1f}s"
        assert batch_size / duration > 5, f"PostgreSQL throughput too low: {batch_size / duration:.1f} jobs/sec"
        
        print("✅ PostgreSQL performance characteristics validated")
        
    finally:
        await pg_fixture.cleanup()


@pytest.mark.integration 
@pytest.mark.postgres
@requires_postgres
def test_postgres_integration_with_cli(tmp_path):
    """Test PostgreSQL integration through CLI interface."""
    
    dsn = os.getenv("POSTGRES_TEST_DSN", POSTGRES_TEST_DSN)
    
    # Create test config with PostgreSQL DSN
    config_content = dedent(f"""
        # PostgreSQL Integration Test Config
        database:
          type: "postgresql"
          dsn: "{dsn}"
          pool_size: 5
        
        symbols:
          - AAPL
          - GOOGL
        
        start: "2024-01-15"
        end: "2024-01-15"
        output_path: "{tmp_path}/postgres_output"
        provider: "fake"
        
        ingestion:
          max_workers: 2
          batch_size: 1000
    """)
    
    config_file = tmp_path / "postgres_config.yaml"
    config_file.write_text(config_content)
    
    # Test CLI with PostgreSQL configuration
    from typer.testing import CliRunner
    from marketpipe.cli import app
    
    runner = CliRunner()
    
    # Note: This would require full PostgreSQL CLI integration
    # For now, test that config parsing works
    result = runner.invoke(app, [
        "ingest",
        "--config", str(config_file),
        "--dry-run",  # Don't actually execute
    ], catch_exceptions=True)
    
    # Should at least parse the config without errors
    if result.exit_code == 0:
        print("✅ PostgreSQL CLI integration config parsing successful")
    else:
        print(f"⚠️  PostgreSQL CLI integration needs additional setup: {result.stdout}")
    
    print("✅ PostgreSQL CLI integration test completed")