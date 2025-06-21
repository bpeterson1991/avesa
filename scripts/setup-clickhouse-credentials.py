#!/usr/bin/env python3
"""
Setup ClickHouse Credentials in AWS Secrets Manager
Securely stores ClickHouse connection parameters for the AVESA pipeline
"""

import os
import sys
import json
import boto3
import getpass
import argparse
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from shared.credential_manager import get_credential_manager
from shared.logger import get_logger

logger = get_logger(__name__)

def setup_clickhouse_credentials_interactive(environment: str):
    """Set up ClickHouse credentials interactively"""
    
    print(f"🔧 Setting up ClickHouse credentials for {environment} environment")
    print("=" * 60)
    
    # Get ClickHouse connection details
    print("\n📝 Enter ClickHouse connection details:")
    
    host = input("ClickHouse Host (e.g., wmk4p0wi7n.us-east-2.aws.clickhouse.cloud): ").strip()
    if not host:
        print("❌ Host is required")
        return False
    
    port = input("Port (default: 8443): ").strip() or "8443"
    try:
        port = int(port)
    except ValueError:
        print("❌ Port must be a number")
        return False
    
    username = input("Username (default: default): ").strip() or "default"
    
    # Securely get password
    password = getpass.getpass("Password: ")
    if not password:
        print("❌ Password is required")
        return False
    
    secure = input("Use SSL/TLS (Y/n): ").strip().lower()
    secure = secure != 'n'
    
    verify = input("Verify SSL certificates (y/N): ").strip().lower()
    verify = verify == 'y'
    
    # Create credentials dictionary
    credentials = {
        'host': host,
        'port': port,
        'username': username,
        'password': password,
        'secure': secure,
        'verify': verify,
        'created_at': datetime.utcnow().isoformat(),
        'environment': environment
    }
    
    # Test connection before storing
    print("\n🔍 Testing ClickHouse connection...")
    if test_clickhouse_connection(credentials):
        print("✅ Connection test successful")
    else:
        print("❌ Connection test failed")
        retry = input("Store credentials anyway? (y/N): ").strip().lower()
        if retry != 'y':
            return False
    
    # Store credentials
    credential_manager = get_credential_manager()
    try:
        credential_manager.store_clickhouse_credentials(environment, credentials)
        print(f"✅ ClickHouse credentials stored successfully for {environment}")
        return True
    except Exception as e:
        print(f"❌ Failed to store credentials: {e}")
        return False

def setup_clickhouse_credentials_from_env(environment: str):
    """Set up ClickHouse credentials from environment variables"""
    
    print(f"🔧 Setting up ClickHouse credentials from environment variables")
    print("=" * 60)
    
    # Get credentials from environment
    required_vars = ['CLICKHOUSE_HOST', 'CLICKHOUSE_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("\nRequired environment variables:")
        print("  CLICKHOUSE_HOST - ClickHouse host")
        print("  CLICKHOUSE_PASSWORD - ClickHouse password")
        print("\nOptional environment variables:")
        print("  CLICKHOUSE_PORT - Port (default: 8443)")
        print("  CLICKHOUSE_USERNAME - Username (default: default)")
        print("  CLICKHOUSE_SECURE - Use SSL (default: true)")
        print("  CLICKHOUSE_VERIFY - Verify SSL (default: false)")
        return False
    
    credentials = {
        'host': os.getenv('CLICKHOUSE_HOST'),
        'port': int(os.getenv('CLICKHOUSE_PORT', '8443')),
        'username': os.getenv('CLICKHOUSE_USERNAME', 'default'),
        'password': os.getenv('CLICKHOUSE_PASSWORD'),
        'secure': os.getenv('CLICKHOUSE_SECURE', 'true').lower() == 'true',
        'verify': os.getenv('CLICKHOUSE_VERIFY', 'false').lower() == 'true',
        'created_at': datetime.utcnow().isoformat(),
        'environment': environment
    }
    
    # Test connection
    print("\n🔍 Testing ClickHouse connection...")
    if test_clickhouse_connection(credentials):
        print("✅ Connection test successful")
    else:
        print("❌ Connection test failed")
        return False
    
    # Store credentials
    credential_manager = get_credential_manager()
    try:
        credential_manager.store_clickhouse_credentials(environment, credentials)
        print(f"✅ ClickHouse credentials stored successfully for {environment}")
        return True
    except Exception as e:
        print(f"❌ Failed to store credentials: {e}")
        return False

def test_clickhouse_connection(credentials: dict) -> bool:
    """Test ClickHouse connection with provided credentials"""
    try:
        import clickhouse_connect
        
        client = clickhouse_connect.get_client(
            host=credentials['host'],
            port=credentials['port'],
            username=credentials['username'],
            password=credentials['password'],
            secure=credentials['secure'],
            verify=credentials['verify']
        )
        
        # Test with simple query
        result = client.query("SELECT 1 as test")
        return len(result.result_rows) > 0 and result.result_rows[0][0] == 1
        
    except Exception as e:
        print(f"   Connection error: {e}")
        return False

def validate_existing_credentials(environment: str):
    """Validate existing ClickHouse credentials"""
    
    print(f"🔍 Validating existing ClickHouse credentials for {environment}")
    print("=" * 60)
    
    credential_manager = get_credential_manager()
    
    try:
        # Try to retrieve credentials
        credentials = credential_manager.get_clickhouse_credentials(environment)
        print("✅ Credentials found in Secrets Manager")
        
        # Test connection
        print("\n🔍 Testing connection...")
        if test_clickhouse_connection(credentials):
            print("✅ Connection test successful")
            
            # Show connection info (without password)
            print(f"\n📋 Connection details:")
            print(f"   Host: {credentials['host']}")
            print(f"   Port: {credentials['port']}")
            print(f"   Username: {credentials['username']}")
            print(f"   SSL: {credentials.get('secure', True)}")
            print(f"   Verify SSL: {credentials.get('verify', False)}")
            print(f"   Created: {credentials.get('created_at', 'Unknown')}")
            
            return True
        else:
            print("❌ Connection test failed")
            return False
            
    except Exception as e:
        print(f"❌ Failed to validate credentials: {e}")
        return False

def rotate_clickhouse_password(environment: str):
    """Rotate ClickHouse password"""
    
    print(f"🔄 Rotating ClickHouse password for {environment}")
    print("=" * 60)
    
    # Get new password
    new_password = getpass.getpass("Enter new password: ")
    if not new_password:
        print("❌ Password is required")
        return False
    
    confirm_password = getpass.getpass("Confirm new password: ")
    if new_password != confirm_password:
        print("❌ Passwords do not match")
        return False
    
    # Rotate credentials
    credential_manager = get_credential_manager()
    try:
        if credential_manager.rotate_clickhouse_credentials(environment, new_password):
            print("✅ Password rotated successfully")
            return True
        else:
            print("❌ Password rotation failed")
            return False
    except Exception as e:
        print(f"❌ Failed to rotate password: {e}")
        return False

def list_environments():
    """List all environments with ClickHouse credentials"""
    
    print("🔍 Listing ClickHouse credential environments")
    print("=" * 60)
    
    try:
        secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
        
        # List secrets with AVESA ClickHouse prefix
        response = secrets_client.list_secrets(
            Filters=[
                {
                    'Key': 'name',
                    'Values': ['avesa/clickhouse/']
                }
            ]
        )
        
        if not response['SecretList']:
            print("❌ No ClickHouse credentials found")
            return
        
        print("📋 Found ClickHouse credentials for environments:")
        for secret in response['SecretList']:
            name = secret['Name']
            environment = name.split('/')[-1]
            created = secret.get('CreatedDate', 'Unknown')
            print(f"   🌍 {environment} (created: {created})")
            
    except Exception as e:
        print(f"❌ Failed to list environments: {e}")

def main():
    parser = argparse.ArgumentParser(description='Setup ClickHouse credentials for AVESA pipeline')
    parser.add_argument('--environment', '-e', default='dev',
                       choices=['dev', 'staging', 'prod'],
                       help='Environment to configure (default: dev)')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='Interactive credential setup')
    parser.add_argument('--from-env', action='store_true',
                       help='Setup from environment variables')
    parser.add_argument('--validate', '-v', action='store_true',
                       help='Validate existing credentials')
    parser.add_argument('--rotate', '-r', action='store_true',
                       help='Rotate password for existing credentials')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List all environments with credentials')
    
    args = parser.parse_args()
    
    print(f"🚀 ClickHouse Credential Management")
    print(f"📅 Timestamp: {datetime.utcnow().isoformat()}Z")
    print()
    
    try:
        if args.list:
            list_environments()
        elif args.validate:
            success = validate_existing_credentials(args.environment)
            sys.exit(0 if success else 1)
        elif args.rotate:
            success = rotate_clickhouse_password(args.environment)
            sys.exit(0 if success else 1)
        elif args.from_env:
            success = setup_clickhouse_credentials_from_env(args.environment)
            sys.exit(0 if success else 1)
        elif args.interactive:
            success = setup_clickhouse_credentials_interactive(args.environment)
            sys.exit(0 if success else 1)
        else:
            # Default to interactive mode
            success = setup_clickhouse_credentials_interactive(args.environment)
            sys.exit(0 if success else 1)
            
    except KeyboardInterrupt:
        print("\n❌ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()