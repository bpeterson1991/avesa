#!/usr/bin/env python3
"""
Tenant-Only Setup Script

This script creates a new tenant in the AVESA multi-tenant data pipeline.
It only stores tenant-specific data (ID, company name, enabled status).
Services are configured separately using setup-service.py.
"""

import argparse
import boto3
import json
import sys
from datetime import datetime, timezone
from typing import Dict, Any


def main():
    parser = argparse.ArgumentParser(description='Setup a new tenant (tenant-only data)')
    parser.add_argument('--tenant-id', required=True, help='Unique tenant identifier')
    parser.add_argument('--company-name', required=True, help='Company display name')
    parser.add_argument('--environment', default='dev', choices=['dev', 'staging', 'prod'],
                       help='Environment (default: dev)')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    parser.add_argument('--enabled', action='store_true', default=True, 
                       help='Enable tenant (default: True)')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be created without actually creating')
    
    args = parser.parse_args()
    
    # Initialize AWS clients
    dynamodb = boto3.client('dynamodb', region_name=args.region)
    
    # Table names
    tenant_services_table = f"TenantServices-{args.environment}"
    
    print(f"Setting up tenant: {args.tenant_id}")
    print(f"Company: {args.company_name}")
    print(f"Environment: {args.environment}")
    print(f"Region: {args.region}")
    print(f"Enabled: {args.enabled}")
    print()
    
    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
        print()
    
    try:
        # Check if tenant already exists
        existing_tenant = get_existing_tenant(dynamodb, tenant_services_table, args.tenant_id)
        if existing_tenant and not args.dry_run:
            print(f"⚠️  Tenant {args.tenant_id} already exists. Use --dry-run to see current config.")
            print("Current configuration:")
            print(f"  Company Name: {existing_tenant.get('company_name', {}).get('S', 'N/A')}")
            print(f"  Enabled: {existing_tenant.get('enabled', {}).get('BOOL', False)}")
            print(f"  Created: {existing_tenant.get('created_at', {}).get('S', 'N/A')}")
            
            response = input("Do you want to update this tenant? (y/N): ")
            if response.lower() != 'y':
                print("Aborted.")
                return
        
        # Create tenant configuration in DynamoDB (tenant-only data)
        current_time = datetime.now(timezone.utc).isoformat()
        tenant_config = {
            'tenant_id': {'S': args.tenant_id},
            'company_name': {'S': args.company_name},
            'enabled': {'BOOL': args.enabled},
            'created_at': {'S': existing_tenant.get('created_at', {}).get('S', current_time) if existing_tenant else current_time},
            'updated_at': {'S': current_time}
        }
        
        print(f"Creating/updating tenant configuration in DynamoDB table: {tenant_services_table}")
        if not args.dry_run:
            dynamodb.put_item(
                TableName=tenant_services_table,
                Item=tenant_config
            )
            print("✓ Tenant configuration created/updated successfully")
        else:
            print("✓ Would create/update tenant configuration")
            print("Tenant data:")
            for key, value in tenant_config.items():
                print(f"  {key}: {list(value.values())[0]}")
        
        print()
        print("Setup completed successfully!")
        print()
        print("Next steps:")
        print("1. Add services to this tenant using setup-service.py:")
        print(f"   python scripts/setup-service.py --tenant-id {args.tenant_id} --service connectwise [service-options]")
        print()
        print("2. Verify the tenant configuration:")
        key_json = json.dumps({"tenant_id": {"S": args.tenant_id}})
        print(f"   aws dynamodb get-item --table-name {tenant_services_table} --key '{key_json}' --region {args.region}")
        
    except Exception as e:
        print(f"Error setting up tenant: {str(e)}", file=sys.stderr)
        sys.exit(1)


def get_existing_tenant(dynamodb, table_name: str, tenant_id: str) -> Dict[str, Any]:
    """Check if tenant already exists."""
    try:
        response = dynamodb.get_item(
            TableName=table_name,
            Key={'tenant_id': {'S': tenant_id}}
        )
        return response.get('Item')
    except Exception:
        return None


if __name__ == '__main__':
    main()