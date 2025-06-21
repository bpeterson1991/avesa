#!/usr/bin/env python3
"""
Test ClickHouse Cloud connectivity using the credentials from AWS Secrets Manager
"""
import json
import boto3
import requests
import sys
from urllib.parse import quote

def get_clickhouse_credentials():
    """Retrieve ClickHouse credentials from AWS Secrets Manager"""
    try:
        # Initialize Secrets Manager client
        session = boto3.Session(profile_name='AdministratorAccess-123938354448')
        secrets_client = session.client('secretsmanager', region_name='us-east-2')
        
        # Get the secret
        secret_arn = 'arn:aws:secretsmanager:us-east-2:123938354448:secret:clickhouse-connection-dev-V9zSgO'
        response = secrets_client.get_secret_value(SecretId=secret_arn)
        
        # Parse the secret string
        credentials = json.loads(response['SecretString'])
        return credentials
    except Exception as e:
        print(f"❌ Error retrieving credentials: {str(e)}")
        return None

def test_clickhouse_connection(credentials):
    """Test connection to ClickHouse Cloud"""
    try:
        # Build connection URL
        host = credentials['host']
        port = credentials['port']
        username = credentials['username']
        password = credentials['password']
        database = credentials['database']
        
        # URL encode the password to handle special characters
        encoded_password = quote(password)
        
        # Test basic connectivity with a simple query
        url = f"https://{host}:{port}/"
        
        # Test query - get ClickHouse version
        query = "SELECT version()"
        
        print(f"🔍 Testing ClickHouse Cloud connectivity...")
        print(f"   Host: {host}")
        print(f"   Port: {port}")
        print(f"   Database: {database}")
        print(f"   Username: {username}")
        print(f"   SSL: {credentials.get('ssl', True)}")
        print()
        
        # Make the request
        response = requests.get(
            url,
            params={'query': query},
            auth=(username, password),
            timeout=30,
            verify=True  # Verify SSL certificate
        )
        
        if response.status_code == 200:
            version = response.text.strip()
            print(f"✅ Connection successful!")
            print(f"   ClickHouse version: {version}")
            return True
        else:
            print(f"❌ Connection failed with status code: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print(f"❌ Connection timeout - check network connectivity")
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Connection error: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def test_database_operations(credentials):
    """Test basic database operations"""
    try:
        host = credentials['host']
        port = credentials['port']
        username = credentials['username']
        password = credentials['password']
        
        url = f"https://{host}:{port}/"
        
        print(f"\n🔍 Testing database operations...")
        
        # Test 1: Show databases
        print("   Testing: SHOW DATABASES")
        response = requests.get(
            url,
            params={'query': 'SHOW DATABASES'},
            auth=(username, password),
            timeout=30
        )
        
        if response.status_code == 200:
            databases = response.text.strip().split('\n')
            print(f"   ✅ Available databases: {', '.join(databases)}")
        else:
            print(f"   ❌ Failed to show databases: {response.text}")
            return False
        
        # Test 2: Create a test table
        print("   Testing: CREATE TABLE")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS test_connection (
            id UInt32,
            message String,
            timestamp DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY id
        """
        
        response = requests.post(
            url,
            data=create_table_query,
            auth=(username, password),
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"   ✅ Test table created successfully")
        else:
            print(f"   ❌ Failed to create test table: {response.text}")
            return False
        
        # Test 3: Insert test data
        print("   Testing: INSERT")
        insert_query = "INSERT INTO test_connection (id, message) VALUES (1, 'Connection test successful')"
        
        response = requests.post(
            url,
            data=insert_query,
            auth=(username, password),
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"   ✅ Test data inserted successfully")
        else:
            print(f"   ❌ Failed to insert test data: {response.text}")
            return False
        
        # Test 4: Query test data
        print("   Testing: SELECT")
        select_query = "SELECT * FROM test_connection"
        
        response = requests.get(
            url,
            params={'query': select_query},
            auth=(username, password),
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.text.strip()
            print(f"   ✅ Query successful: {result}")
        else:
            print(f"   ❌ Failed to query test data: {response.text}")
            return False
        
        # Test 5: Clean up test table
        print("   Testing: DROP TABLE")
        drop_query = "DROP TABLE test_connection"
        
        response = requests.post(
            url,
            data=drop_query,
            auth=(username, password),
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"   ✅ Test table dropped successfully")
        else:
            print(f"   ❌ Failed to drop test table: {response.text}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Database operations test failed: {str(e)}")
        return False

def main():
    """Main test function"""
    print("🚀 ClickHouse Cloud Connectivity Test")
    print("=" * 50)
    
    # Get credentials from Secrets Manager
    credentials = get_clickhouse_credentials()
    if not credentials:
        sys.exit(1)
    
    # Test basic connectivity
    if not test_clickhouse_connection(credentials):
        sys.exit(1)
    
    # Test database operations
    if not test_database_operations(credentials):
        sys.exit(1)
    
    print(f"\n✅ All tests passed! ClickHouse Cloud is ready for use.")
    print(f"🔗 Connection details verified and working correctly.")

if __name__ == "__main__":
    main()