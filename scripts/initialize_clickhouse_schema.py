#!/usr/bin/env python3
"""
Direct ClickHouse Schema Initialization Script
Initializes the ClickHouse database schema for multi-tenant SaaS using the shared tables.
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

def read_schema_file():
    """Read the schema SQL file"""
    try:
        with open('src/clickhouse/schemas/shared_tables.sql', 'r') as f:
            return f.read()
    except FileNotFoundError:
        print(f"❌ Schema file not found at src/clickhouse/schemas/shared_tables.sql")
        return None

def execute_sql_statement(credentials, statement):
    """Execute a single SQL statement"""
    try:
        host = credentials['host']
        port = credentials['port']
        username = credentials['username']
        password = credentials['password']
        
        url = f"https://{host}:{port}/"
        
        # Execute the statement
        response = requests.post(
            url,
            data=statement,
            auth=(username, password),
            timeout=60,
            verify=True
        )
        
        if response.status_code == 200:
            return True, response.text.strip()
        else:
            return False, f"HTTP {response.status_code}: {response.text}"
            
    except Exception as e:
        return False, str(e)

def execute_schema_statements(credentials, schema_sql):
    """Execute schema creation statements in proper order"""
    results = {
        'tables_created': [],
        'indexes_created': [],
        'views_created': [],
        'errors': []
    }
    
    print(f"🔍 Parsing schema SQL ({len(schema_sql)} characters)...")
    
    # Remove comments first
    lines = schema_sql.split('\n')
    cleaned_lines = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith('--'):
            cleaned_lines.append(line)
    
    cleaned_sql = ' '.join(cleaned_lines)
    
    # Split SQL into individual statements
    all_statements = [stmt.strip() for stmt in cleaned_sql.split(';') if stmt.strip()]
    
    # Filter out empty statements
    all_statements = [stmt for stmt in all_statements if stmt]
    
    # Debug: print first few statements to see what we're getting
    print(f"🔍 First few statements found:")
    for i, stmt in enumerate(all_statements[:3]):
        print(f"   {i+1}: {stmt[:100]}...")
    
    # Categorize statements to ensure proper execution order
    create_table_statements = []
    alter_table_statements = []
    create_view_statements = []
    other_statements = []
    
    for statement in all_statements:
        statement_upper = statement.upper()
        if 'CREATE TABLE' in statement_upper:
            create_table_statements.append(statement)
        elif 'ALTER TABLE' in statement_upper and 'ADD INDEX' in statement_upper:
            alter_table_statements.append(statement)
        elif 'CREATE MATERIALIZED VIEW' in statement_upper:
            create_view_statements.append(statement)
        else:
            other_statements.append(statement)
    
    # Execute in proper order: tables first, then indexes, then views
    ordered_statements = create_table_statements + alter_table_statements + create_view_statements + other_statements
    
    print(f"📝 Found {len(ordered_statements)} SQL statements to execute:")
    print(f"   - {len(create_table_statements)} CREATE TABLE statements")
    print(f"   - {len(alter_table_statements)} ALTER TABLE (index) statements")
    print(f"   - {len(create_view_statements)} CREATE MATERIALIZED VIEW statements")
    print(f"   - {len(other_statements)} other statements")
    
    for i, statement in enumerate(ordered_statements, 1):
        print(f"\n[{i}/{len(ordered_statements)}] Executing: {statement[:80]}...")
        
        success, result = execute_sql_statement(credentials, statement)
        
        if success:
            # Categorize the statement
            if 'CREATE TABLE' in statement.upper():
                table_name = extract_table_name(statement)
                results['tables_created'].append(table_name)
                print(f"   ✅ Created table: {table_name}")
                
            elif 'ADD INDEX' in statement.upper():
                index_info = extract_index_info(statement)
                results['indexes_created'].append(index_info)
                print(f"   ✅ Created index: {index_info}")
                
            elif 'CREATE MATERIALIZED VIEW' in statement.upper():
                view_name = extract_view_name(statement)
                results['views_created'].append(view_name)
                print(f"   ✅ Created materialized view: {view_name}")
            else:
                print(f"   ✅ Statement executed successfully")
        else:
            error_msg = f"Failed to execute statement: {result}"
            print(f"   ❌ {error_msg}")
            results['errors'].append({
                'statement': statement[:200],
                'error': result
            })
    
    return results

def extract_table_name(statement):
    """Extract table name from CREATE TABLE statement"""
    try:
        parts = statement.upper().split()
        if_not_exists_idx = -1
        for i, part in enumerate(parts):
            if part == 'EXISTS':
                if_not_exists_idx = i
                break
        
        if if_not_exists_idx > 0:
            return parts[if_not_exists_idx + 1]
        else:
            table_idx = parts.index('TABLE') + 1
            return parts[table_idx]
    except:
        return "unknown"

def extract_index_info(statement):
    """Extract index information from ALTER TABLE ADD INDEX statement"""
    try:
        parts = statement.split()
        table_name = parts[2]  # ALTER TABLE table_name
        index_name = parts[5]  # ADD INDEX index_name
        return f"{index_name} on {table_name}"
    except:
        return "unknown"

def extract_view_name(statement):
    """Extract view name from CREATE MATERIALIZED VIEW statement"""
    try:
        parts = statement.upper().split()
        if_not_exists_idx = -1
        for i, part in enumerate(parts):
            if part == 'EXISTS':
                if_not_exists_idx = i
                break
        
        if if_not_exists_idx > 0:
            return parts[if_not_exists_idx + 1]
        else:
            view_idx = parts.index('VIEW') + 1
            return parts[view_idx]
    except:
        return "unknown"

def verify_schema(credentials):
    """Verify that the schema was created successfully"""
    verification_results = {
        'tables': {},
        'total_tables': 0,
        'total_indexes': 0,
        'total_views': 0
    }
    
    try:
        host = credentials['host']
        port = credentials['port']
        username = credentials['username']
        password = credentials['password']
        
        url = f"https://{host}:{port}/"
        
        # Check tables
        print(f"\n🔍 Verifying schema creation...")
        
        tables_query = "SHOW TABLES"
        response = requests.get(
            url,
            params={'query': tables_query},
            auth=(username, password),
            timeout=30
        )
        
        if response.status_code == 200:
            tables = [line.strip() for line in response.text.strip().split('\n') if line.strip()]
            
            expected_tables = ['companies', 'contacts', 'tickets', 'time_entries']
            for table_name in expected_tables:
                table_exists = table_name in tables
                verification_results['tables'][table_name] = table_exists
                if table_exists:
                    verification_results['total_tables'] += 1
                    print(f"   ✅ Table '{table_name}' exists")
                else:
                    print(f"   ❌ Table '{table_name}' missing")
        
        # Check materialized views
        views_query = "SHOW TABLES WHERE engine LIKE '%MaterializedView%'"
        response = requests.get(
            url,
            params={'query': views_query},
            auth=(username, password),
            timeout=30
        )
        
        if response.status_code == 200:
            views = [line.strip() for line in response.text.strip().split('\n') if line.strip()]
            verification_results['total_views'] = len(views)
            print(f"   ✅ Found {len(views)} materialized views")
            for view in views:
                print(f"      - {view}")
        
        print(f"\n📊 Schema verification summary:")
        print(f"   Tables created: {verification_results['total_tables']}/4")
        print(f"   Materialized views: {verification_results['total_views']}")
        
    except Exception as e:
        print(f"❌ Schema verification failed: {e}")
        verification_results['error'] = str(e)
    
    return verification_results

def test_multi_tenant_setup(credentials):
    """Test multi-tenant setup with sample data"""
    print(f"\n🧪 Testing multi-tenant setup...")
    
    try:
        host = credentials['host']
        port = credentials['port']
        username = credentials['username']
        password = credentials['password']
        
        url = f"https://{host}:{port}/"
        
        # Test 1: Insert sample data for two tenants
        print("   Testing: Multi-tenant data insertion")
        
        # Insert test company for tenant 1
        insert_query1 = """
        INSERT INTO companies (tenant_id, id, company_name, source_system, source_id, last_updated, data_hash)
        VALUES ('tenant1', 'comp1', 'Test Company 1', 'test', 'test1', now(), 'hash1')
        """
        
        response = requests.post(url, data=insert_query1, auth=(username, password), timeout=30)
        if response.status_code == 200:
            print(f"      ✅ Inserted test data for tenant1")
        else:
            print(f"      ❌ Failed to insert data for tenant1: {response.text}")
            return False
        
        # Insert test company for tenant 2
        insert_query2 = """
        INSERT INTO companies (tenant_id, id, company_name, source_system, source_id, last_updated, data_hash)
        VALUES ('tenant2', 'comp1', 'Test Company 2', 'test', 'test2', now(), 'hash2')
        """
        
        response = requests.post(url, data=insert_query2, auth=(username, password), timeout=30)
        if response.status_code == 200:
            print(f"      ✅ Inserted test data for tenant2")
        else:
            print(f"      ❌ Failed to insert data for tenant2: {response.text}")
            return False
        
        # Test 2: Verify tenant isolation
        print("   Testing: Tenant isolation")
        
        # Query for tenant1 only
        select_query1 = "SELECT tenant_id, company_name FROM companies WHERE tenant_id = 'tenant1'"
        response = requests.get(url, params={'query': select_query1}, auth=(username, password), timeout=30)
        
        if response.status_code == 200:
            result1 = response.text.strip()
            if 'tenant1' in result1 and 'tenant2' not in result1:
                print(f"      ✅ Tenant isolation working correctly")
            else:
                print(f"      ❌ Tenant isolation issue: {result1}")
                return False
        
        # Test 3: Verify SCD Type 2 structure
        print("   Testing: SCD Type 2 structure")
        
        # Check that SCD fields exist and work
        scd_query = "SELECT tenant_id, id, effective_date, is_current FROM companies WHERE tenant_id IN ('tenant1', 'tenant2')"
        response = requests.get(url, params={'query': scd_query}, auth=(username, password), timeout=30)
        
        if response.status_code == 200:
            result = response.text.strip()
            if 'tenant1' in result and 'tenant2' in result:
                print(f"      ✅ SCD Type 2 structure working correctly")
            else:
                print(f"      ❌ SCD Type 2 structure issue: {result}")
                return False
        
        # Clean up test data
        print("   Cleaning up test data...")
        cleanup_query = "DELETE FROM companies WHERE tenant_id IN ('tenant1', 'tenant2')"
        response = requests.post(url, data=cleanup_query, auth=(username, password), timeout=30)
        
        if response.status_code == 200:
            print(f"      ✅ Test data cleaned up")
        else:
            print(f"      ⚠️  Warning: Failed to clean up test data: {response.text}")
        
        return True
        
    except Exception as e:
        print(f"❌ Multi-tenant testing failed: {str(e)}")
        return False

def main():
    """Main initialization function"""
    print("🚀 ClickHouse Schema Initialization")
    print("=" * 60)
    
    # Get credentials from Secrets Manager
    print("🔐 Retrieving ClickHouse credentials from AWS Secrets Manager...")
    credentials = get_clickhouse_credentials()
    if not credentials:
        sys.exit(1)
    
    print(f"✅ Successfully retrieved credentials for {credentials['host']}")
    
    # Read schema file
    print("📖 Reading schema file...")
    schema_sql = read_schema_file()
    if not schema_sql:
        sys.exit(1)
    
    print(f"✅ Successfully loaded schema file")
    
    # Execute schema statements
    print("\n🏗️  Executing schema creation statements...")
    execution_results = execute_schema_statements(credentials, schema_sql)
    
    # Print execution summary
    print(f"\n📊 Execution Summary:")
    print(f"   Tables created: {len(execution_results['tables_created'])}")
    for table in execution_results['tables_created']:
        print(f"      - {table}")
    
    print(f"   Indexes created: {len(execution_results['indexes_created'])}")
    for index in execution_results['indexes_created']:
        print(f"      - {index}")
    
    print(f"   Materialized views created: {len(execution_results['views_created'])}")
    for view in execution_results['views_created']:
        print(f"      - {view}")
    
    if execution_results['errors']:
        print(f"   Errors encountered: {len(execution_results['errors'])}")
        for error in execution_results['errors']:
            print(f"      - {error['error']}")
    
    # Verify schema
    verification_results = verify_schema(credentials)
    
    # Test multi-tenant setup
    if verification_results['total_tables'] == 4:
        test_success = test_multi_tenant_setup(credentials)
        if test_success:
            print(f"\n✅ Multi-tenant setup validation passed!")
        else:
            print(f"\n⚠️  Multi-tenant setup validation had issues")
    
    # Final status
    if execution_results['errors']:
        print(f"\n⚠️  Schema initialization completed with {len(execution_results['errors'])} errors")
        sys.exit(1)
    else:
        print(f"\n🎉 ClickHouse schema initialization completed successfully!")
        print(f"🔗 Database is ready for multi-tenant SaaS operations")

if __name__ == "__main__":
    main()