# PostgreSQL Setup for MarketPipe

## 🚀 Quick Start

### Option 1: Comprehensive Test Runner (Recommended)
```bash
python run_complete_postgres_tests.py
```
This script automatically:
- Checks Docker availability
- Sets up PostgreSQL container
- Installs dependencies (psycopg2-binary, asyncpg)
- Tests connection and PostgreSQL features
- Runs Alembic migrations
- Executes pytest tests

### Option 2: Manual Setup

#### 1. Fix Docker Permissions (if needed)
```bash
# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Or use sudo for Docker commands
sudo docker start marketpipe-postgres
```

#### 2. Start PostgreSQL Container
```bash
# If container exists
docker start marketpipe-postgres

# If container doesn't exist, create it
docker run -d \
    --name marketpipe-postgres \
    -e POSTGRES_USER=marketpipe \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_DB=marketpipe \
    -p 5433:5432 \
    postgres:15
```

#### 3. Install Dependencies
```bash
pip install psycopg2-binary asyncpg
```

#### 4. Run Tests
```bash
# Manual test script
python fixed_postgres_test.py

# Or pytest with environment variables
TEST_POSTGRES=1 POSTGRES_TEST_URL=postgresql://marketpipe:password@localhost:5433/marketpipe pytest tests/test_migrations.py::TestPostgresMigrations -v
```

## 📋 Connection Information

- **URL**: `postgresql://marketpipe:password@localhost:5433/marketpipe`
- **Container**: `marketpipe-postgres`
- **Port**: `5433` (to avoid conflicts with existing PostgreSQL)
- **Database**: `marketpipe`
- **Username**: `marketpipe`
- **Password**: `password`

## 🛠️ Docker Commands

```bash
# Check container status
docker ps -a | grep marketpipe-postgres

# Start container
docker start marketpipe-postgres

# Stop container
docker stop marketpipe-postgres

# View logs
docker logs marketpipe-postgres

# Remove container (if needed to recreate)
docker rm marketpipe-postgres
```

## 🧪 Test Commands

```bash
# Check PostgreSQL is ready
docker exec marketpipe-postgres pg_isready -U marketpipe

# Connect to PostgreSQL from host (if psql installed)
PGPASSWORD=password psql -h localhost -p 5433 -U marketpipe -d marketpipe

# Run all tests
python run_complete_postgres_tests.py

# Run specific pytest tests
TEST_POSTGRES=1 POSTGRES_TEST_URL=postgresql://marketpipe:password@localhost:5433/marketpipe pytest tests/test_migrations.py::TestPostgresMigrations -v

# Run with more verbose output
TEST_POSTGRES=1 POSTGRES_TEST_URL=postgresql://marketpipe:password@localhost:5433/marketpipe pytest tests/test_migrations.py::TestPostgresMigrations -v -s --tb=long
```

## ❌ Troubleshooting

### Docker Permission Denied
```bash
# Solution 1: Add user to docker group (requires logout/login)
sudo usermod -aG docker $USER
newgrp docker

# Solution 2: Use sudo
sudo docker start marketpipe-postgres
```

### Port 5432 Already in Use
The setup uses port 5433 to avoid conflicts with existing PostgreSQL installations.

### psycopg2 Missing
```bash
pip install psycopg2-binary
```

### Container Won't Start
```bash
# Check if port is in use
sudo netstat -tlnp | grep 5433

# Check container logs
docker logs marketpipe-postgres

# Remove and recreate container
docker rm marketpipe-postgres
docker run -d --name marketpipe-postgres -e POSTGRES_USER=marketpipe -e POSTGRES_PASSWORD=password -e POSTGRES_DB=marketpipe -p 5433:5432 postgres:15
```

### Tests Skipped
Make sure environment variables are set:
```bash
export TEST_POSTGRES=1
export POSTGRES_TEST_URL=postgresql://marketpipe:password@localhost:5433/marketpipe
```

## 📊 What Tests Are Run

1. **Docker Setup**: Container creation and startup
2. **Connection Test**: Basic PostgreSQL connectivity
3. **JSONB Test**: PostgreSQL-specific JSON functionality
4. **GIN Index Test**: PostgreSQL-specific indexing
5. **Alembic Migrations**: Full migration from scratch
6. **Table Verification**: All expected tables created
7. **Idempotent Test**: Migrations can run multiple times
8. **Pytest Integration**: Official test suite execution

## 🎯 Expected Results

All tests should pass with output similar to:
```
✅ PostgreSQL connection successful
✅ JSONB functionality: value
✅ GIN index creation successful
✅ Alembic migrations completed
✅ All expected tables created
✅ Migration version: 0005
✅ Pytest PostgreSQL tests passed
🎉 PostgreSQL setup and testing completed successfully!
``` 