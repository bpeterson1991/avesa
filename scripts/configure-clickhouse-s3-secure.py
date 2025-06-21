#!/usr/bin/env python3
"""
Secure ClickHouse S3 Integration Configuration Script
Configures S3 access for ClickHouse Cloud using AWS Secrets Manager and IAM roles
"""

import os
import sys
import boto3
import clickhouse_connect
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from shared.credential_manager import get_credential_manager
from shared.logger import get_logger

logger = get_logger(__name__)

def configure_clickhouse_s3_secure(environment: str = 'dev'):
    """Configure ClickHouse S3 integration using secure credential management"""
    
    print("🔧 Secure ClickHouse S3 Integration Configuration")
    print("=" * 60)
    
    # Initialize credential manager
    credential_manager = get_credential_manager()
    
    # Get environment configuration
    env_config = credential_manager.get_environment_config()
    print(f"🌍 Environment: {env_config['environment']}")
    print(f"🏢 Account: {env_config['account_id']}")
    print(f"🌎 Region: {env_config['region']}")
    print(f"🪣 Bucket: {env_config['bucket_name']}")
    
    # Validate AWS credentials first
    print("\n🔍 Validating AWS credentials...")
    if not credential_manager.validate_credentials('s3', environment):
        print("❌ AWS S3 credentials validation failed")
        return False
    print("✅ AWS credentials validated")
    
    # Get ClickHouse credentials from Secrets Manager
    print("\n🔍 Retrieving ClickHouse credentials...")
    try:
        clickhouse_creds = credential_manager.get_clickhouse_credentials(environment)
        print("✅ ClickHouse credentials retrieved from Secrets Manager")
    except Exception as e:
        print(f"❌ Failed to retrieve ClickHouse credentials: {e}")
        print("\n💡 To set up ClickHouse credentials, run:")
        print(f"   python scripts/setup-clickhouse-credentials.py --environment {environment}")
        return False
    
    # Validate ClickHouse credentials
    print("\n🔍 Validating ClickHouse credentials...")
    if not credential_manager.validate_credentials('clickhouse', environment):
        print("❌ ClickHouse credentials validation failed")
        return False
    print("✅ ClickHouse credentials validated")
    
    # Connect to ClickHouse
    try:
        client = clickhouse_connect.get_client(
            host=clickhouse_creds['host'],
            port=clickhouse_creds.get('port', 8443),
            username=clickhouse_creds['username'],
            password=clickhouse_creds['password'],
            secure=clickhouse_creds.get('secure', True),
            verify=clickhouse_creds.get('verify', False)
        )
        print("✅ Connected to ClickHouse Cloud")
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        return False
    
    # Get AWS credentials for S3 access
    try:
        aws_creds = credential_manager.get_aws_credentials_for_service('s3', environment)
        print("✅ Retrieved AWS credentials for S3 access")
    except Exception as e:
        print(f"❌ Failed to get AWS credentials: {e}")
        return False
    
    # Test S3 access with credentials
    print("\n🔍 Testing S3 access from ClickHouse...")
    
    bucket_name = env_config['bucket_name']
    test_path = f"https://s3.{env_config['region']}.amazonaws.com/{bucket_name}/sitetechnology/canonical/companies/companies/*.parquet"
    
    test_queries = []
    
    # Basic S3 access test
    basic_query = f"""
    SELECT count(*) as file_count
    FROM s3(
        '{test_path}',
        '{aws_creds['access_key_id']}',
        '{aws_creds['secret_access_key']}',
        'Parquet'
    )
    LIMIT 1
    """
    test_queries.append(('Basic S3 access', basic_query))
    
    # Test with session token if available
    if aws_creds.get('session_token'):
        session_token_query = f"""
        SELECT count(*) as file_count
        FROM s3(
            '{test_path}',
            '{aws_creds['access_key_id']}',
            '{aws_creds['secret_access_key']}',
            'Parquet',
            'gzip',
            '{aws_creds['session_token']}'
        )
        LIMIT 1
        """
        test_queries.append(('S3 access with session token', session_token_query))
    
    success = False
    for test_name, query in test_queries:
        try:
            print(f"   🧪 Testing: {test_name}")
            result = client.query(query)
            record_count = result.result_rows[0][0] if result.result_rows else 0
            print(f"   ✅ Success: Found {record_count} records")
            success = True
            break
            
        except Exception as e:
            print(f"   ❌ Failed: {str(e)}")
            continue
    
    if not success:
        print("\n❌ All S3 access tests failed")
        return False
    
    # Create secure S3 function for reuse
    print("\n🔧 Creating secure S3 access function...")
    try:
        function_name = f"s3_secure_{environment}"
        
        # Create function with current credentials
        if aws_creds.get('session_token'):
            function_sql = f"""
            CREATE OR REPLACE FUNCTION {function_name} AS (path, format) -> 
            s3(
                path,
                '{aws_creds['access_key_id']}',
                '{aws_creds['secret_access_key']}',
                format,
                'gzip',
                '{aws_creds['session_token']}'
            )
            """
        else:
            function_sql = f"""
            CREATE OR REPLACE FUNCTION {function_name} AS (path, format) -> 
            s3(
                path,
                '{aws_creds['access_key_id']}',
                '{aws_creds['secret_access_key']}',
                format
            )
            """
        
        client.command(function_sql)
        print(f"✅ Created secure S3 function: {function_name}")
        
        # Test the function
        test_query = f"""
        SELECT count(*) as file_count
        FROM {function_name}(
            '{test_path}',
            'Parquet'
        )
        LIMIT 1
        """
        
        result = client.query(test_query)
        record_count = result.result_rows[0][0] if result.result_rows else 0
        print(f"✅ Function test successful: {record_count} records found")
        
        # Store function name for later use
        print(f"\n📝 S3 function created: {function_name}")
        print(f"   Usage: SELECT * FROM {function_name}('s3://path/to/files/*.parquet', 'Parquet')")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to create S3 function: {e}")
        return False

def setup_s3_integration_for_tables(environment: str = 'dev'):
    """Set up S3 integration for all canonical tables"""
    
    print("\n🔧 Setting up S3 integration for canonical tables...")
    
    credential_manager = get_credential_manager()
    env_config = credential_manager.get_environment_config()
    
    try:
        clickhouse_creds = credential_manager.get_clickhouse_credentials(environment)
        client = clickhouse_connect.get_client(
            host=clickhouse_creds['host'],
            port=clickhouse_creds.get('port', 8443),
            username=clickhouse_creds['username'],
            password=clickhouse_creds['password'],
            secure=clickhouse_creds.get('secure', True),
            verify=clickhouse_creds.get('verify', False)
        )
        
        bucket_name = env_config['bucket_name']
        tables = ['companies', 'contacts', 'tickets', 'time_entries']
        
        for table in tables:
            try:
                # Create table-specific S3 function
                function_name = f"s3_{table}_{environment}"
                table_path = f"https://s3.{env_config['region']}.amazonaws.com/{bucket_name}/sitetechnology/canonical/{table}/{table}/*.parquet"
                
                aws_creds = credential_manager.get_aws_credentials_for_service('s3', environment)
                
                if aws_creds.get('session_token'):
                    function_sql = f"""
                    CREATE OR REPLACE FUNCTION {function_name} AS () -> 
                    s3(
                        '{table_path}',
                        '{aws_creds['access_key_id']}',
                        '{aws_creds['secret_access_key']}',
                        'Parquet',
                        'gzip',
                        '{aws_creds['session_token']}'
                    )
                    """
                else:
                    function_sql = f"""
                    CREATE OR REPLACE FUNCTION {function_name} AS () -> 
                    s3(
                        '{table_path}',
                        '{aws_creds['access_key_id']}',
                        '{aws_creds['secret_access_key']}',
                        'Parquet'
                    )
                    """
                
                client.command(function_sql)
                print(f"✅ Created S3 function for {table}: {function_name}")
                
            except Exception as e:
                print(f"❌ Failed to create S3 function for {table}: {e}")
                
        return True
        
    except Exception as e:
        print(f"❌ Failed to set up table S3 integration: {e}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Configure ClickHouse S3 integration securely')
    parser.add_argument('--environment', '-e', default='dev', 
                       choices=['dev', 'staging', 'prod'],
                       help='Environment to configure (default: dev)')
    parser.add_argument('--setup-tables', action='store_true',
                       help='Also set up table-specific S3 functions')
    
    args = parser.parse_args()
    
    print(f"🚀 Starting Secure ClickHouse S3 Configuration")
    print(f"📅 Timestamp: {datetime.utcnow().isoformat()}Z")
    print(f"🌍 Environment: {args.environment}")
    print()
    
    # Configure basic S3 access
    if configure_clickhouse_s3_secure(args.environment):
        print("\n✅ S3 access configured successfully")
        
        # Set up table-specific functions if requested
        if args.setup_tables:
            if setup_s3_integration_for_tables(args.environment):
                print("✅ Table-specific S3 functions created")
            else:
                print("⚠️  Some table S3 functions failed to create")
        
        print("\n🎉 ClickHouse S3 integration setup complete!")
        print("\n📝 Next steps:")
        print("   1. Test data loading with: python scripts/test-clickhouse-data-loading.py")
        print("   2. Set up credential rotation: python scripts/setup-credential-rotation.py")
        sys.exit(0)
    else:
        print("\n❌ Failed to configure S3 access")
        print("\n🔧 Troubleshooting:")
        print("   1. Verify AWS credentials: aws sts get-caller-identity")
        print("   2. Check ClickHouse credentials: python scripts/setup-clickhouse-credentials.py --validate")
        print("   3. Verify S3 bucket access: aws s3 ls s3://data-storage-msp-dev/")
        sys.exit(1)