#!/usr/bin/env python3
"""
Tenant Setup Script

This script helps configure a new tenant in the ConnectWise data pipeline.
It creates the necessary DynamoDB entries and AWS Secrets Manager secrets.
"""

import argparse
import boto3
import json
import sys
from typing import Dict, Any


def main():
    parser = argparse.ArgumentParser(description='Setup a new tenant for ConnectWise data pipeline')
    parser.add_argument('--tenant-id', required=True, help='Unique tenant identifier')
    parser.add_argument('--company-name', required=True, help='Company name')
    parser.add_argument('--connectwise-url', required=True, help='ConnectWise API base URL')
    parser.add_argument('--company-id', required=True, help='ConnectWise company ID')
    parser.add_argument('--public-key', required=True, help='ConnectWise public key')
    parser.add_argument('--private-key', required=True, help='ConnectWise private key')
    parser.add_argument('--client-id', required=True, help='ConnectWise client ID')
    parser.add_argument('--environment', default='dev', choices=['dev', 'staging', 'prod'],
                       help='Environment (default: dev)')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    parser.add_argument('--tables', nargs='+', 
                       default=['service/tickets', 'time/entries', 'company/companies', 'company/contacts'],
                       help='Tables to sync (default: tickets, time_entries, companies, contacts)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be created without actually creating')
    
    args = parser.parse_args()
    
    # Initialize AWS clients
    dynamodb = boto3.client('dynamodb', region_name=args.region)
    secrets = boto3.client('secretsmanager', region_name=args.region)
    
    # Table names
    tenant_services_table = f"TenantServices-{args.environment}"
    
    # Secret name
    secret_name = f"connectwise/{args.tenant_id}/{args.environment}"
    
    print(f"Setting up tenant: {args.tenant_id}")
    print(f"Environment: {args.environment}")
    print(f"Region: {args.region}")
    print(f"ConnectWise URL: {args.connectwise_url}")
    print(f"Tables to sync: {', '.join(args.tables)}")
    print()
    
    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
        print()
    
    try:
        # Create or update secret in Secrets Manager
        secret_value = {
            "company_id": args.company_id,
            "public_key": args.public_key,
            "private_key": args.private_key,
            "client_id": args.client_id
        }
        
        print(f"Creating/updating secret: {secret_name}")
        if not args.dry_run:
            try:
                secrets.create_secret(
                    Name=secret_name,
                    Description=f"ConnectWise API credentials for {args.company_name}",
                    SecretString=json.dumps(secret_value)
                )
                print("✓ Secret created successfully")
            except secrets.exceptions.ResourceExistsException:
                secrets.update_secret(
                    SecretId=secret_name,
                    SecretString=json.dumps(secret_value)
                )
                print("✓ Secret updated successfully")
        else:
            print("✓ Would create/update secret")
        
        # Create tenant configuration in DynamoDB
        tenant_config = {
            'tenant_id': {'S': args.tenant_id},
            'company_name': {'S': args.company_name},
            'connectwise_url': {'S': args.connectwise_url},
            'secret_name': {'S': secret_name},
            'enabled': {'BOOL': True},
            'tables': {'L': [{'S': table} for table in args.tables]},
            'custom_config': {'S': json.dumps({
                'sync_frequency_minutes': 15,
                'max_records_per_batch': 1000,
                'enable_data_validation': True
            })},
            'created_at': {'S': '2024-01-01T00:00:00Z'},
            'updated_at': {'S': '2024-01-01T00:00:00Z'}
        }
        
        print(f"Creating tenant configuration in DynamoDB table: {tenant_services_table}")
        if not args.dry_run:
            dynamodb.put_item(
                TableName=tenant_services_table,
                Item=tenant_config
            )
            print("✓ Tenant configuration created successfully")
        else:
            print("✓ Would create tenant configuration")
        
        print()
        print("Setup completed successfully!")
        print()
        print("Next steps:")
        print("1. Verify the tenant configuration:")
        key_json = json.dumps({"tenant_id": {"S": args.tenant_id}})
        print(f"   aws dynamodb get-item --table-name {tenant_services_table} --key '{key_json}' --region {args.region}")
        print()
        print("2. Test the raw ingestion Lambda with this tenant:")
        payload_json = json.dumps({"tenant_id": args.tenant_id})
        print(f"   aws lambda invoke --function-name connectwise-raw-ingestion-{args.environment} --payload '{payload_json}' response.json --region {args.region}")
        print()
        print("3. Monitor the logs:")
        print(f"   aws logs tail /aws/lambda/connectwise-raw-ingestion-{args.environment} --follow --region {args.region}")
        
    except Exception as e:
        print(f"Error setting up tenant: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()