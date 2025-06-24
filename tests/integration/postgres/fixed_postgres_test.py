#!/usr/bin/env python3
"""Fixed PostgreSQL test with proper dependency handling."""

import asyncio
import os
import sys  # Fixed: Added missing import


def run_tests():
    """Run PostgreSQL tests with proper error handling."""

    # Set environment variables
    os.environ["TEST_POSTGRES"] = "1"
    os.environ["POSTGRES_TEST_URL"] = "postgresql://marketpipe:password@localhost:5433/marketpipe"

    print("🐘 PostgreSQL Test Execution (Fixed)")
    print("=" * 50)
    print(f"Database URL: {os.environ['POSTGRES_TEST_URL']}")
    print()

    async def test_connection_and_features():
        """Test PostgreSQL connection and features."""
        try:
            import asyncpg

            # Connect to PostgreSQL
            conn = await asyncpg.connect(os.environ["POSTGRES_TEST_URL"])
            print("✅ PostgreSQL connection successful")

            # Get version info
            version = await conn.fetchval("SELECT version()")
            print(f"📝 PostgreSQL version: {version[:70]}...")

            # Test PostgreSQL-specific features
            print("\n🧪 Testing PostgreSQL-specific features...")

            # Test JSONB
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS test_features (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """
            )

            await conn.execute(
                """
                INSERT INTO test_features (name, metadata)
                VALUES ('test1', '{"type": "test", "value": 123}')
            """
            )

            # Test JSONB query
            result = await conn.fetchval(
                """
                SELECT metadata->>'type' FROM test_features WHERE name = 'test1'
            """
            )
            print(f"✅ JSONB query result: {result}")

            # Test GIN index creation (PostgreSQL specific)
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_metadata_gin ON test_features USING GIN (metadata)
            """
            )
            print("✅ GIN index creation successful")

            # Clean up
            await conn.execute("DROP TABLE IF EXISTS test_features")
            await conn.close()
            print("✅ Basic PostgreSQL features test completed")

            return True

        except ImportError:
            print("❌ asyncpg not available")
            return False
        except Exception as e:
            print(f"❌ PostgreSQL test failed: {e}")
            import traceback

            traceback.print_exc()
            return False

    def test_migrations():
        """Test the actual migration functionality with better error handling."""
        try:
            print("\n🔧 Testing Alembic migrations with PostgreSQL...")

            # Check for psycopg2 first
            try:
                import psycopg2

                print("✅ psycopg2 is available")
            except ImportError:
                print("❌ psycopg2 is missing - installing...")
                import subprocess

                result = subprocess.run(
                    [sys.executable, "-m", "pip", "install", "psycopg2-binary"],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    print(f"❌ Failed to install psycopg2: {result.stderr}")
                    print("Manual installation required:")
                    print("  pip install psycopg2-binary")
                    return False
                else:
                    print("✅ psycopg2-binary installed successfully")

            from sqlalchemy import create_engine, text

            from alembic import command
            from alembic.config import Config

            postgres_url = os.environ["POSTGRES_TEST_URL"]

            # Create Alembic config
            alembic_cfg = Config("alembic.ini")
            alembic_cfg.set_main_option("sqlalchemy.url", postgres_url)

            # Create engine for cleanup
            engine = create_engine(postgres_url)

            # Clean up existing tables
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS alembic_version CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS symbol_bars_aggregates CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ohlcv_bars CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS checkpoints CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS metrics CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ingestion_jobs CASCADE"))
                conn.commit()

            print("✅ Cleaned up existing tables")

            # Run migrations
            command.upgrade(alembic_cfg, "head")
            print("✅ Alembic migrations completed")

            # Verify tables were created
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        """
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    ORDER BY table_name
                """
                    )
                )
                tables = [row[0] for row in result.fetchall()]
                print(f"✅ Created tables: {', '.join(tables)}")

                expected_tables = [
                    "alembic_version",
                    "checkpoints",
                    "ingestion_jobs",
                    "metrics",
                    "ohlcv_bars",
                    "symbol_bars_aggregates",
                ]

                missing_tables = [t for t in expected_tables if t not in tables]
                if missing_tables:
                    print(f"❌ Missing tables: {missing_tables}")
                    return False

                print("✅ All expected tables created")

            # Test idempotent migration (run again)
            command.upgrade(alembic_cfg, "head")
            print("✅ Idempotent migration test passed")

            # Verify version
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version_num FROM alembic_version"))
                version = result.fetchone()[0]
                print(f"✅ Migration version: {version}")

                if version != "0005":
                    print(f"❌ Expected version 0005, got {version}")
                    return False

            print("✅ Migration tests completed successfully")
            return True

        except Exception as e:
            print(f"❌ Migration test failed: {e}")
            import traceback

            traceback.print_exc()
            return False

    # Run tests
    async def run_all_tests():
        print("🚀 Starting PostgreSQL test suite...")

        # Test 1: Basic connection and features
        connection_ok = await test_connection_and_features()

        if connection_ok:
            print("\n" + "=" * 50)
            # Test 2: Migrations
            migration_ok = test_migrations()

            if migration_ok:
                print("\n🎉 ALL POSTGRESQL TESTS PASSED!")
                print("Your PostgreSQL setup is fully functional!")
                return True
            else:
                print("\n❌ Migration tests failed")
                return False
        else:
            print("\n❌ Basic connection tests failed")
            print("Make sure PostgreSQL container is running:")
            print("  docker start marketpipe-postgres")
            return False

    # Execute the tests
    return asyncio.run(run_all_tests())


if __name__ == "__main__":
    success = run_tests()
    if not success:
        sys.exit(1)
