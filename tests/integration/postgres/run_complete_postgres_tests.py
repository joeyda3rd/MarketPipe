#!/usr/bin/env python3
"""
Complete PostgreSQL Test Runner
This script handles Docker setup, dependency installation, and runs comprehensive tests.
"""

import asyncio
import os
import subprocess
import sys
import time


def print_banner(title):
    """Print a formatted banner."""
    print("\n" + "=" * 60)
    print(f"🐘 {title}")
    print("=" * 60)


def print_step(step):
    """Print a formatted step."""
    print(f"\n🔧 {step}")
    print("-" * 50)


def run_command(cmd, description, capture_output=False, check=True):
    """Run a command with error handling."""
    print(f"Running: {cmd}")
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=capture_output, text=True, check=check
        )
        if capture_output:
            return result.stdout.strip(), result.stderr.strip(), result.returncode
        else:
            return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        return False


def check_docker():
    """Check if Docker is available and running."""
    print_step("Checking Docker availability")

    # First check if docker command exists
    if not run_command("which docker", "Docker command check", capture_output=True):
        print("❌ Docker command not found")
        return False

    # Check if Docker daemon is running
    stdout, stderr, code = run_command(
        "docker info", "Docker daemon check", capture_output=True, check=False
    )
    if code != 0:
        print("❌ Docker daemon not running or permission denied")
        print("Solutions:")
        print("  1. Start Docker: sudo systemctl start docker")
        print("  2. Use sudo: sudo docker ...")
        print("  3. Add user to docker group: sudo usermod -aG docker $USER")
        return False

    print("✅ Docker is available and running")
    return True


def setup_postgres_container():
    """Setup PostgreSQL Docker container."""
    print_step("Setting up PostgreSQL container")

    # Check if container exists
    stdout, _, _ = run_command(
        "docker ps -a --format '{{.Names}}' | grep marketpipe-postgres",
        "Check existing container",
        capture_output=True,
        check=False,
    )

    if "marketpipe-postgres" in stdout:
        print("📦 Container 'marketpipe-postgres' exists")

        # Check if it's running
        stdout, _, _ = run_command(
            "docker ps --format '{{.Names}}' | grep marketpipe-postgres",
            "Check running container",
            capture_output=True,
            check=False,
        )

        if "marketpipe-postgres" in stdout:
            print("✅ Container is already running")
        else:
            print("🚀 Starting existing container...")
            if run_command("docker start marketpipe-postgres", "Start container"):
                print("✅ Container started successfully")
            else:
                return False
    else:
        print("📦 Creating new PostgreSQL container...")
        cmd = """docker run -d \
            --name marketpipe-postgres \
            -e POSTGRES_USER=marketpipe \
            -e POSTGRES_PASSWORD=password \
            -e POSTGRES_DB=marketpipe \
            -p 5433:5432 \
            postgres:15"""

        if run_command(cmd, "Create PostgreSQL container"):
            print("✅ Container created successfully")
            print("⏳ Waiting for PostgreSQL to be ready...")
            time.sleep(8)  # Give it time to start
        else:
            return False

    # Test if PostgreSQL is ready
    for attempt in range(10):
        if run_command(
            "docker exec marketpipe-postgres pg_isready -U marketpipe",
            "PostgreSQL ready check",
            capture_output=True,
            check=False,
        ):
            print("✅ PostgreSQL is ready")
            return True
        time.sleep(2)
        print(f"⏳ Waiting for PostgreSQL... attempt {attempt + 1}/10")

    print("❌ PostgreSQL did not become ready")
    return False


def install_dependencies():
    """Install required Python dependencies."""
    print_step("Installing Python dependencies")

    # Check if psycopg2 is available
    import importlib.util

    if importlib.util.find_spec("psycopg2") is not None:
        print("✅ psycopg2 is already available")
    else:
        print("📦 Installing psycopg2-binary...")
        if run_command(f"{sys.executable} -m pip install psycopg2-binary", "Install psycopg2"):
            print("✅ psycopg2-binary installed successfully")
        else:
            return False

    # Check if asyncpg is available
    import importlib.util

    if importlib.util.find_spec("asyncpg") is not None:
        print("✅ asyncpg is already available")
    else:
        print("📦 Installing asyncpg...")
        if run_command(f"{sys.executable} -m pip install asyncpg", "Install asyncpg"):
            print("✅ asyncpg installed successfully")
        else:
            return False

    return True


async def test_basic_connection():
    """Test basic PostgreSQL connection and features."""
    print_step("Testing basic PostgreSQL connection and features")

    try:
        import asyncpg

        # Connect to PostgreSQL
        conn = await asyncpg.connect("postgresql://marketpipe:password@localhost:5433/marketpipe")
        print("✅ PostgreSQL connection successful")

        # Get version
        version = await conn.fetchval("SELECT version()")
        print(f"📝 PostgreSQL version: {version[:60]}...")

        # Test JSONB functionality
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS test_jsonb (
                id SERIAL PRIMARY KEY,
                data JSONB
            )
        """
        )

        await conn.execute(
            """
            INSERT INTO test_jsonb (data) VALUES ('{"test": "value", "number": 42}')
        """
        )

        result = await conn.fetchval("SELECT data->>'test' FROM test_jsonb LIMIT 1")
        print(f"✅ JSONB functionality: {result}")

        # Test GIN index
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_test_jsonb_gin ON test_jsonb USING GIN (data)"
        )
        print("✅ GIN index creation successful")

        # Clean up
        await conn.execute("DROP TABLE IF EXISTS test_jsonb")
        await conn.close()

        print("✅ Basic PostgreSQL features test completed")
        return True

    except Exception as e:
        print(f"❌ Basic PostgreSQL test failed: {e}")
        return False


def test_alembic_migrations():
    """Test Alembic migrations with PostgreSQL."""
    print_step("Testing Alembic migrations")

    try:
        from alembic import command
        from alembic.config import Config
        from sqlalchemy import create_engine, text

        postgres_url = "postgresql://marketpipe:password@localhost:5433/marketpipe"

        # Create Alembic config
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", postgres_url)

        # Create engine
        engine = create_engine(postgres_url)

        # Clean up existing tables
        print("🧹 Cleaning up existing tables...")
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS alembic_version CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS symbol_bars_aggregates CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS ohlcv_bars CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS checkpoints CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS metrics CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS ingestion_jobs CASCADE"))
            conn.commit()

        print("✅ Cleanup completed")

        # Run migrations
        print("🚀 Running Alembic migrations...")
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
            print(f"📋 Created tables: {', '.join(tables)}")

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

        # Test idempotent migration
        print("🔄 Testing idempotent migration...")
        command.upgrade(alembic_cfg, "head")
        print("✅ Idempotent migration test passed")

        # Verify version
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version_num FROM alembic_version"))
            version = result.fetchone()[0]
            print(f"📝 Migration version: {version}")

            if version != "0006":
                print(f"❌ Expected version 0006, got {version}")
                return False

        print("✅ Alembic migration tests completed successfully")
        return True

    except Exception as e:
        print(f"❌ Alembic migration test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def run_pytest_tests():
    """Run pytest PostgreSQL tests."""
    print_step("Running pytest PostgreSQL tests")

    # Set environment variables
    env = os.environ.copy()
    env["TEST_POSTGRES"] = "1"
    env["POSTGRES_TEST_URL"] = "postgresql://marketpipe:password@localhost:5433/marketpipe"

    # Run PostgreSQL-specific tests
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "tests/test_migrations.py::TestPostgresMigrations",
        "-v",
        "--tb=short",
    ]

    print(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)

        print("\n📊 Pytest Output:")
        print("-" * 30)
        print(result.stdout)

        if result.stderr:
            print("\n⚠️ Pytest Warnings/Errors:")
            print(result.stderr)

        if result.returncode == 0:
            print("✅ Pytest PostgreSQL tests passed")
            return True
        else:
            print("❌ Pytest PostgreSQL tests failed")
            return False

    except Exception as e:
        print(f"❌ Failed to run pytest: {e}")
        return False


def show_summary():
    """Show test summary and next steps."""
    print_banner("PostgreSQL Setup Complete")

    print("🎉 PostgreSQL setup and testing completed successfully!")
    print()
    print("📋 What was tested:")
    print("  ✅ Docker container setup")
    print("  ✅ PostgreSQL connection")
    print("  ✅ JSONB functionality")
    print("  ✅ GIN index creation")
    print("  ✅ Alembic migrations")
    print("  ✅ Table creation and verification")
    print("  ✅ Pytest integration")
    print()
    print("🔗 Connection Information:")
    print("  URL: postgresql://marketpipe:password@localhost:5433/marketpipe")
    print("  Container: marketpipe-postgres")
    print("  Port: 5433")
    print()
    print("🛠️ Manual Commands:")
    print("  Start container: docker start marketpipe-postgres")
    print("  Stop container: docker stop marketpipe-postgres")
    print("  View logs: docker logs marketpipe-postgres")
    print("  Connect: psql -h localhost -p 5433 -U marketpipe -d marketpipe")
    print()
    print("🧪 Run Tests Again:")
    print("  Manual tests: python fixed_postgres_test.py")
    print(
        "  Pytest tests: TEST_POSTGRES=1 POSTGRES_TEST_URL=postgresql://marketpipe:password@localhost:5433/marketpipe pytest tests/test_migrations.py::TestPostgresMigrations -v"
    )


async def main():
    """Main function to run all tests."""
    print_banner("MarketPipe PostgreSQL Test Suite")

    # Step 1: Check Docker
    if not check_docker():
        print("\n❌ Docker setup required. Please fix Docker issues and try again.")
        return False

    # Step 2: Setup PostgreSQL container
    if not setup_postgres_container():
        print("\n❌ PostgreSQL container setup failed.")
        return False

    # Step 3: Install dependencies
    if not install_dependencies():
        print("\n❌ Dependency installation failed.")
        return False

    # Step 4: Test basic connection
    if not await test_basic_connection():
        print("\n❌ Basic PostgreSQL connection tests failed.")
        return False

    # Step 5: Test Alembic migrations
    if not test_alembic_migrations():
        print("\n❌ Alembic migration tests failed.")
        return False

    # Step 6: Run pytest tests
    if not run_pytest_tests():
        print("\n❌ Pytest PostgreSQL tests failed.")
        return False

    # Step 7: Show summary
    show_summary()

    return True


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n❌ Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
