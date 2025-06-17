#!/usr/bin/env python3
"""Quick PostgreSQL test."""

import asyncio
import os
import sys

async def check_postgres_connection():
    """Test PostgreSQL connection and basic functionality."""
    try:
        import asyncpg
        
        url = "postgresql://marketpipe:password@localhost:5433/marketpipe"
        print(f"🔌 Testing connection to: {url}")
        
        # Test basic connection
        conn = await asyncpg.connect(url)
        print("✅ Connection successful!")
        
        # Test basic query
        version = await conn.fetchval("SELECT version()")
        print(f"📝 PostgreSQL version: {version[:50]}...")
        
        # Test table creation
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS test_pg_table (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                data JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        print("✅ Table creation successful!")
        
        # Test JSONB functionality (PostgreSQL-specific)
        await conn.execute("""
            INSERT INTO test_pg_table (name, data) 
            VALUES ('test', '{"key": "value", "number": 42}')
        """)
        print("✅ JSONB insert successful!")
        
        # Query JSONB data
        result = await conn.fetchval("""
            SELECT data->>'key' FROM test_pg_table WHERE name = 'test'
        """)
        print(f"✅ JSONB query result: {result}")
        
        # Clean up
        await conn.execute("DROP TABLE test_pg_table")
        await conn.close()
        print("✅ Cleanup completed!")
        
        return True
        
    except ImportError:
        print("❌ asyncpg not installed")
        return False
    except Exception as e:
        print(f"❌ PostgreSQL test failed: {e}")
        return False

def run_migration_test():
    """Run the actual PostgreSQL migration test."""
    try:
        # Set environment variables
        os.environ['TEST_POSTGRES'] = '1'
        os.environ['POSTGRES_TEST_URL'] = 'postgresql://marketpipe:password@localhost:5433/marketpipe'
        
        print("\n🧪 Running PostgreSQL Migration Test")
        print("-" * 50)
        
        # Import test after setting environment
        from tests.test_migrations import TestPostgresMigrations
        
        # Create test instance
        test_instance = TestPostgresMigrations()
        
        # Get postgres URL
        postgres_url = os.environ['POSTGRES_TEST_URL']
        
        # Run the migration test
        test_instance.test_postgres_migration_from_scratch(postgres_url)
        print("✅ PostgreSQL migration test passed!")
        
        # Run idempotent test
        test_instance.test_postgres_idempotent_migration(postgres_url)
        print("✅ PostgreSQL idempotent migration test passed!")
        
        return True
        
    except Exception as e:
        print(f"❌ Migration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Main test function."""
    print("🐘 PostgreSQL Test Suite")
    print("=" * 50)
    
    # Test basic connection first
    connection_ok = await check_postgres_connection()
    
    if connection_ok:
        print("\n" + "=" * 50)
        # Run migration tests
        migration_ok = run_migration_test()
        
        if migration_ok:
            print("\n🎉 All PostgreSQL tests passed!")
            print("Your PostgreSQL setup is working correctly!")
        else:
            print("\n❌ Migration tests failed")
    else:
        print("\n❌ Basic connection test failed")
        print("Check if PostgreSQL container is running:")
        print("  docker ps | grep postgres")
        print("  docker start marketpipe-postgres")

if __name__ == "__main__":
    asyncio.run(main()) 