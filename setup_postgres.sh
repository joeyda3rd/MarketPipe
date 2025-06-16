#!/bin/bash
# PostgreSQL Docker Setup Script

echo "🐳 MarketPipe PostgreSQL Setup"
echo "==============================="

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "❌ Docker is not running or permission denied"
        echo "Try one of these solutions:"
        echo "  1. sudo systemctl start docker"
        echo "  2. sudo docker ..."
        echo "  3. Add user to docker group: sudo usermod -aG docker $USER && newgrp docker"
        return 1
    fi
    return 0
}

# Function to start/create PostgreSQL container
setup_postgres() {
    echo "🔍 Checking for existing PostgreSQL container..."
    
    if docker ps -a --format "table {{.Names}}" | grep -q "marketpipe-postgres"; then
        echo "📦 Container 'marketpipe-postgres' exists"
        
        if docker ps --format "table {{.Names}}" | grep -q "marketpipe-postgres"; then
            echo "✅ Container is already running"
        else
            echo "🚀 Starting existing container..."
            docker start marketpipe-postgres
            if [ $? -eq 0 ]; then
                echo "✅ Container started successfully"
            else
                echo "❌ Failed to start container"
                return 1
            fi
        fi
    else
        echo "📦 Creating new PostgreSQL container..."
        docker run -d \
            --name marketpipe-postgres \
            -e POSTGRES_USER=marketpipe \
            -e POSTGRES_PASSWORD=password \
            -e POSTGRES_DB=marketpipe \
            -p 5433:5432 \
            postgres:15
        
        if [ $? -eq 0 ]; then
            echo "✅ Container created and started successfully"
            echo "⏳ Waiting for PostgreSQL to be ready..."
            sleep 5
        else
            echo "❌ Failed to create container"
            return 1
        fi
    fi
    
    return 0
}

# Function to test connection
test_connection() {
    echo "🧪 Testing PostgreSQL connection..."
    
    # Test with docker exec first
    if docker exec marketpipe-postgres pg_isready -U marketpipe > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready inside container"
    else
        echo "❌ PostgreSQL is not ready inside container"
        return 1
    fi
    
    # Test connection from host
    if command -v psql > /dev/null; then
        if PGPASSWORD=password psql -h localhost -p 5433 -U marketpipe -d marketpipe -c "SELECT version();" > /dev/null 2>&1; then
            echo "✅ Connection from host successful"
        else
            echo "⚠️ Cannot connect from host (psql available but connection failed)"
        fi
    else
        echo "ℹ️ psql not available on host (that's okay, Python can still connect)"
    fi
    
    return 0
}

# Function to show container info
show_info() {
    echo ""
    echo "📋 PostgreSQL Container Information"
    echo "===================================="
    echo "Container name: marketpipe-postgres"
    echo "Host port: 5433"
    echo "Database: marketpipe"
    echo "Username: marketpipe"
    echo "Password: password"
    echo ""
    echo "🔗 Connection URLs:"
    echo "DATABASE_URL=postgresql://marketpipe:password@localhost:5433/marketpipe"
    echo "POSTGRES_TEST_URL=postgresql://marketpipe:password@localhost:5433/marketpipe"
    echo ""
}

# Function to install psycopg2 if needed
install_dependencies() {
    echo "📦 Checking Python dependencies..."
    
    if python -c "import psycopg2" 2>/dev/null; then
        echo "✅ psycopg2 is available"
    else
        echo "📦 Installing psycopg2-binary..."
        if pip install psycopg2-binary; then
            echo "✅ psycopg2-binary installed successfully"
        else
            echo "❌ Failed to install psycopg2-binary"
            echo "Try manual installation: pip install psycopg2-binary"
            return 1
        fi
    fi
    
    return 0
}

# Main execution
main() {
    if ! check_docker; then
        echo ""
        echo "🔧 Docker Setup Required"
        echo "========================"
        echo "Run these commands to fix Docker permissions:"
        echo "  sudo systemctl start docker"
        echo "  sudo usermod -aG docker $USER"
        echo "  newgrp docker"
        echo ""
        echo "Or use sudo for Docker commands:"
        echo "  sudo docker start marketpipe-postgres"
        exit 1
    fi
    
    if ! setup_postgres; then
        echo "❌ Failed to setup PostgreSQL container"
        exit 1
    fi
    
    if ! test_connection; then
        echo "❌ PostgreSQL connection test failed"
        exit 1
    fi
    
    if ! install_dependencies; then
        echo "❌ Failed to install dependencies"
        exit 1
    fi
    
    show_info
    
    echo "🎉 PostgreSQL setup completed successfully!"
    echo ""
    echo "Next steps:"
    echo "  1. Run tests: python fixed_postgres_test.py"
    echo "  2. Or run pytest with PostgreSQL: TEST_POSTGRES=1 POSTGRES_TEST_URL=postgresql://marketpipe:password@localhost:5433/marketpipe pytest tests/test_postgres_migrations.py -v"
    echo ""
}

# Execute main function
main "$@" 