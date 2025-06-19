#!/usr/bin/env python3
"""
Service Setup Script

This script sets up a tenant with a service using configuration files.
Service credentials can be provided via JSON file, environment variables, or interactively.
"""

import argparse
import boto3
import json
import sys
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description='Setup a tenant with a service')
    parser.add_argument('--tenant-id', required=True, help='Tenant identifier')
    parser.add_argument('--company-name', required=True, help='Company display name')
    parser.add_argument('--service', required=True, help='Service to configure')
    parser.add_argument('--environment', default='dev', choices=['dev', 'staging', 'prod'],
                       help='Environment (default: dev)')
    parser.add_argument('--region', default='us-east-2', help='AWS region (default: us-east-2)')
    parser.add_argument('--enabled', action='store_true', default=True, 
                       help='Enable service (default: True)')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be created without actually creating')
    
    args = parser.parse_args()
    
    # Load service configuration
    available_services = list_available_services()
    if args.service not in available_services:
        print(f"❌ Unknown service: {args.service}")
        print(f"Available services: {', '.join(available_services)}")
        sys.exit(1)
    
    service_config = load_service_config(args.service)
    
    # Initialize AWS clients
    dynamodb = boto3.client('dynamodb', region_name=args.region)
    secrets = boto3.client('secretsmanager', region_name=args.region)
    
    # Table names and secret pattern
    tenant_services_table = f"TenantServices-{args.environment}"
    secret_name = f"{args.tenant_id}/{args.environment}"
    
    print(f"🔧 Setting up {service_config['name']} for tenant: {args.tenant_id}")
    print(f"Company: {args.company_name}")
    print(f"Environment: {args.environment}")
    print(f"Region: {args.region}")
    print()
    
    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
        print()
    
    try:
        # Get service credentials
        credentials = get_service_credentials(args, service_config)
        
        # Check if tenant service combination already exists
        existing_service = get_tenant_service(dynamodb, tenant_services_table, args.tenant_id, args.service)
        if existing_service and not args.dry_run:
            print(f"⚠️  Service {args.service} for tenant {args.tenant_id} already exists.")
            response = input("Do you want to update this service configuration? (y/N): ")
            if response.lower() != 'y':
                print("Aborted.")
                return
        
        # Store credentials in AWS Secrets Manager
        print(f"📝 Updating secret: {secret_name}")
        if not args.dry_run:
            store_service_credentials(secrets, secret_name, args.service, credentials, args.company_name)
            print("✓ Credentials stored successfully")
        else:
            print("✓ Would store credentials in secret")
        
        # Create tenant service entry in DynamoDB
        service_info = {
            "enabled": args.enabled
        }
        
        print(f"📊 Creating tenant service entry for {args.service}")
        if not args.dry_run:
            create_tenant_service_entry(dynamodb, tenant_services_table, args.tenant_id, args.service, service_info, args.company_name)
            print("✓ Tenant service entry created successfully")
        else:
            print("✓ Would create tenant service entry")
        
        print()
        print("✅ Setup completed successfully!")
        print()
        print("Next steps:")
        print("1. Verify the service configuration:")
        print(f"   aws secretsmanager get-secret-value --secret-id {secret_name} --region {args.region}")
        print()
        print("2. Test the service Lambda function:")
        payload_json = json.dumps({"tenant_id": args.tenant_id})
        lambda_name = f"avesa-{args.service}-ingestion-{args.environment}"
        print(f"   aws lambda invoke --function-name {lambda_name} --payload '{payload_json}' response.json --region {args.region}")
        
    except Exception as e:
        print(f"❌ Error setting up service: {str(e)}", file=sys.stderr)
        sys.exit(1)


def load_service_config(service_name: str) -> Dict[str, Any]:
    """Load service configuration from individual JSON file."""
    config_path = Path(__file__).parent.parent / "mappings" / "services" / f"{service_name}.json"
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Service configuration file not found: {config_path}")
        print(f"Available services can be found in: mappings/services/")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in service configuration file: {e}")
        sys.exit(1)


def list_available_services() -> List[str]:
    """List available services by scanning the services directory."""
    services_dir = Path(__file__).parent.parent / "mappings" / "services"
    if not services_dir.exists():
        return []
    
    services = []
    for file_path in services_dir.glob("*.json"):
        services.append(file_path.stem)
    return services


def get_service_credentials(args, service_config: Dict[str, Any]) -> Dict[str, str]:
    """Get service credentials from environment variables or interactive input."""
    credentials = {}
    
    # Check environment variables
    env_prefix = f"{args.service.upper()}_"
    for field in service_config["required_fields"]:
        env_var = env_prefix + field.upper()
        if env_var in os.environ:
            credentials[field] = os.environ[env_var]
    
    # Interactive input for missing required fields
    missing_fields = [field for field in service_config["required_fields"] if field not in credentials]
    if missing_fields:
        print(f"📝 Please provide the following credentials for {service_config['name']}:")
        for field in missing_fields:
            if 'password' in field.lower() or 'key' in field.lower() or 'secret' in field.lower():
                import getpass
                credentials[field] = getpass.getpass(f"  {field}: ")
            else:
                credentials[field] = input(f"  {field}: ")
    
    # Validate all required fields are present
    missing = [field for field in service_config["required_fields"] if not credentials.get(field)]
    if missing:
        raise ValueError(f"Missing required credentials: {', '.join(missing)}")
    
    return credentials


def get_tenant_service(dynamodb, table_name: str, tenant_id: str, service_name: str) -> Optional[Dict[str, Any]]:
    """Get tenant service configuration from DynamoDB."""
    try:
        response = dynamodb.get_item(
            TableName=table_name,
            Key={
                'tenant_id': {'S': tenant_id},
                'service_name': {'S': service_name}
            }
        )
        return response.get('Item')
    except Exception:
        return None


def store_service_credentials(secrets, secret_name: str, service: str, credentials: Dict[str, str], company_name: str):
    """Store service credentials in AWS Secrets Manager."""
    # Get existing secret or create new one
    try:
        response = secrets.get_secret_value(SecretId=secret_name)
        existing_secret = json.loads(response['SecretString'])
    except secrets.exceptions.ResourceNotFoundException:
        existing_secret = {}
    
    # Update with new service credentials
    existing_secret[service] = credentials
    
    # Store/update secret
    try:
        secrets.create_secret(
            Name=secret_name,
            Description=f"Multi-service credentials for {company_name}",
            SecretString=json.dumps(existing_secret)
        )
    except secrets.exceptions.ResourceExistsException:
        secrets.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(existing_secret)
        )


def create_tenant_service_entry(dynamodb, table_name: str, tenant_id: str, service: str, service_info: Dict[str, Any], company_name: str):
    """Create tenant service entry in DynamoDB."""
    current_time = datetime.now(timezone.utc).isoformat()
    
    # Create the service entry with composite key
    service_entry = {
        'tenant_id': {'S': tenant_id},
        'service_name': {'S': service},
        'company_name': {'S': company_name},
        'enabled': {'BOOL': service_info.get('enabled', True)},
        'created_at': {'S': current_time},
        'updated_at': {'S': current_time}
    }
    
    # Add service-specific configuration (currently just enabled status)
    # Additional service configuration is stored in secrets manager
    
    dynamodb.put_item(
        TableName=table_name,
        Item=service_entry
    )


if __name__ == '__main__':
    main()