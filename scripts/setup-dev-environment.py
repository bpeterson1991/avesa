#!/usr/bin/env python3
"""
Dev Environment Setup Script

This script sets up a complete dev environment for testing the AVESA data pipeline:
1. Creates missing DynamoDB tables for dev environment
2. Creates dev S3 bucket if missing
3. Sets up sample tenant configurations for testing
4. Validates Lambda function functionality
5. Provides testing instructions
"""

import argparse
import boto3
import json
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List


def main():
    parser = argparse.ArgumentParser(description='Setup complete dev environment for AVESA pipeline')
    parser.add_argument('--region', default='us-east-2', help='AWS region (default: us-east-2)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be created without creating')
    parser.add_argument('--skip-tables', action='store_true', help='Skip DynamoDB table creation')
    parser.add_argument('--skip-bucket', action='store_true', help='Skip S3 bucket creation')
    parser.add_argument('--skip-tenant', action='store_true', help='Skip test tenant creation')
    
    args = parser.parse_args()
    
    # Initialize AWS clients
    dynamodb = boto3.client('dynamodb', region_name=args.region)
    s3 = boto3.client('s3', region_name=args.region)
    secrets = boto3.client('secretsmanager', region_name=args.region)
    
    print("🚀 Setting up AVESA Dev Environment")
    print(f"Region: {args.region}")
    print(f"Dry Run: {args.dry_run}")
    print()
    
    try:
        # 1. Create DynamoDB tables for dev environment
        if not args.skip_tables:
            create_dev_tables(dynamodb, args.region, args.dry_run)
        
        # 2. Create S3 bucket for dev environment
        if not args.skip_bucket:
            create_dev_bucket(s3, args.region, args.dry_run)
        
        # 3. Upload mapping configurations
        upload_mapping_configs(s3, args.region, args.dry_run)
        
        # 4. Create test tenant
        if not args.skip_tenant:
            create_test_tenant(dynamodb, secrets, args.region, args.dry_run)
        
        # 5. Validate setup
        validate_setup(dynamodb, s3, args.region, args.dry_run)
        
        print()
        print("✅ Dev environment setup completed successfully!")
        print()
        print_testing_instructions(args.region)
        
    except Exception as e:
        print(f"❌ Error setting up dev environment: {str(e)}", file=sys.stderr)
        sys.exit(1)


def create_dev_tables(dynamodb, region: str, dry_run: bool):
    """Create DynamoDB tables for dev environment."""
    print("📊 Setting up DynamoDB tables for dev environment...")
    
    tables_to_create = [
        {
            'name': 'TenantServices-dev',
            'partition_key': 'tenant_id',
            'sort_key': 'service'
        },
        {
            'name': 'LastUpdated-dev',
            'partition_key': 'tenant_service',
            'sort_key': 'table_name'
        }
    ]
    
    for table_config in tables_to_create:
        table_name = table_config['name']
        
        # Check if table exists
        try:
            dynamodb.describe_table(TableName=table_name)
            print(f"  ✓ Table {table_name} already exists")
            continue
        except dynamodb.exceptions.ResourceNotFoundException:
            pass
        
        if dry_run:
            print(f"  ✓ Would create table: {table_name}")
            continue
        
        # Create table
        print(f"  📝 Creating table: {table_name}")
        
        key_schema = [
            {'AttributeName': table_config['partition_key'], 'KeyType': 'HASH'}
        ]
        attribute_definitions = [
            {'AttributeName': table_config['partition_key'], 'AttributeType': 'S'}
        ]
        
        if table_config.get('sort_key'):
            key_schema.append({'AttributeName': table_config['sort_key'], 'KeyType': 'RANGE'})
            attribute_definitions.append({'AttributeName': table_config['sort_key'], 'AttributeType': 'S'})
        
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            BillingMode='PAY_PER_REQUEST',
            Tags=[
                {'Key': 'Environment', 'Value': 'dev'},
                {'Key': 'Project', 'Value': 'AVESA'},
                {'Key': 'Purpose', 'Value': 'DataPipeline'}
            ]
        )
        
        # Wait for table to be active
        print(f"  ⏳ Waiting for table {table_name} to be active...")
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name, WaiterConfig={'Delay': 5, 'MaxAttempts': 12})
        print(f"  ✅ Table {table_name} created successfully")


def create_dev_bucket(s3, region: str, dry_run: bool):
    """Create S3 bucket for dev environment."""
    print("🪣 Setting up S3 bucket for dev environment...")
    
    bucket_name = 'data-storage-msp-dev'
    
    # Check if bucket exists
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"  ✓ Bucket {bucket_name} already exists")
        return
    except s3.exceptions.NoSuchBucket:
        pass
    except Exception as e:
        print(f"  ⚠️  Error checking bucket: {e}")
    
    if dry_run:
        print(f"  ✓ Would create bucket: {bucket_name}")
        return
    
    print(f"  📝 Creating bucket: {bucket_name}")
    
    try:
        if region == 'us-east-1':
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        
        # Enable versioning
        s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        
        # Block public access
        s3.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': True,
                'BlockPublicPolicy': True,
                'RestrictPublicBuckets': True
            }
        )
        
        print(f"  ✅ Bucket {bucket_name} created successfully")
        
    except Exception as e:
        print(f"  ❌ Failed to create bucket: {e}")
        raise


def upload_mapping_configs(s3, region: str, dry_run: bool):
    """Upload mapping configurations to S3."""
    print("📋 Uploading mapping configurations...")
    
    bucket_name = 'data-storage-msp-dev'
    
    mapping_files = [
        'mappings/canonical_mappings.json',
        'mappings/integrations/connectwise_endpoints.json',
        'mappings/integrations/servicenow_endpoints.json',
        'mappings/integrations/salesforce_endpoints.json',
        'mappings/tickets.json',
        'mappings/time_entries.json',
        'mappings/companies.json',
        'mappings/contacts.json'
    ]
    
    for file_path in mapping_files:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            s3_key = file_path
            
            if dry_run:
                print(f"  ✓ Would upload: {file_path} -> s3://{bucket_name}/{s3_key}")
                continue
            
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=content,
                ContentType='application/json'
            )
            print(f"  ✅ Uploaded: {file_path}")
            
        except FileNotFoundError:
            print(f"  ⚠️  File not found: {file_path}")
        except Exception as e:
            print(f"  ❌ Failed to upload {file_path}: {e}")


def create_test_tenant(dynamodb, secrets, region: str, dry_run: bool):
    """Create a test tenant for development."""
    print("👤 Creating test tenant...")
    
    tenant_id = "test-tenant"
    company_name = "Test Company"
    
    # Create tenant in DynamoDB
    current_time = datetime.now(timezone.utc).isoformat()
    
    tenant_config = {
        'tenant_id': {'S': tenant_id},
        'company_name': {'S': company_name},
        'enabled': {'BOOL': True},
        'created_at': {'S': current_time},
        'updated_at': {'S': current_time},
        'services': {
            'M': {
                'connectwise': {
                    'M': {
                        'enabled': {'S': 'True'},
                        'api_url': {'S': 'https://api-na.myconnectwise.net'},
                        'tables': {
                            'L': [
                                {'S': 'service/tickets'},
                                {'S': 'time/entries'},
                                {'S': 'company/companies'},
                                {'S': 'company/contacts'}
                            ]
                        }
                    }
                }
            }
        }
    }
    
    if dry_run:
        print(f"  ✓ Would create tenant: {tenant_id}")
        print(f"  ✓ Would create secret: tenant/{tenant_id}/dev")
        return
    
    # Check if tenant exists
    try:
        response = dynamodb.get_item(
            TableName='TenantServices-dev',
            Key={
                'tenant_id': {'S': tenant_id},
                'service': {'S': 'connectwise'}
            }
        )
        if 'Item' in response:
            print(f"  ✓ Test tenant {tenant_id} already exists")
        else:
            # Update tenant config to include service as sort key
            tenant_config['service'] = {'S': 'connectwise'}
            dynamodb.put_item(
                TableName='TenantServices-dev',
                Item=tenant_config
            )
            print(f"  ✅ Created tenant: {tenant_id}")
    except Exception as e:
        print(f"  ❌ Failed to create tenant: {e}")
        return
    
    # Create test credentials in Secrets Manager
    secret_name = f"tenant/{tenant_id}/dev"
    test_credentials = {
        "connectwise": {
            "company_id": "TestCompany",
            "public_key": "test-public-key",
            "private_key": "test-private-key",
            "client_id": "test-client-id"
        }
    }
    
    try:
        secrets.create_secret(
            Name=secret_name,
            Description=f"Test credentials for {company_name}",
            SecretString=json.dumps(test_credentials)
        )
        print(f"  ✅ Created secret: {secret_name}")
    except secrets.exceptions.ResourceExistsException:
        secrets.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(test_credentials)
        )
        print(f"  ✓ Updated existing secret: {secret_name}")
    except Exception as e:
        print(f"  ❌ Failed to create secret: {e}")


def validate_setup(dynamodb, s3, region: str, dry_run: bool):
    """Validate the dev environment setup."""
    print("🔍 Validating dev environment setup...")
    
    if dry_run:
        print("  ✓ Skipping validation in dry-run mode")
        return
    
    # Check DynamoDB tables
    required_tables = ['TenantServices-dev', 'LastUpdated-dev']
    for table_name in required_tables:
        try:
            response = dynamodb.describe_table(TableName=table_name)
            status = response['Table']['TableStatus']
            print(f"  ✅ Table {table_name}: {status}")
        except Exception as e:
            print(f"  ❌ Table {table_name}: {e}")
    
    # Check S3 bucket
    bucket_name = 'data-storage-msp-dev'
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"  ✅ Bucket {bucket_name}: Available")
    except Exception as e:
        print(f"  ❌ Bucket {bucket_name}: {e}")
    
    # Check test tenant
    try:
        response = dynamodb.get_item(
            TableName='TenantServices-dev',
            Key={
                'tenant_id': {'S': 'test-tenant'},
                'service': {'S': 'connectwise'}
            }
        )
        if 'Item' in response:
            print(f"  ✅ Test tenant: Configured")
        else:
            print(f"  ❌ Test tenant: Not found")
    except Exception as e:
        print(f"  ❌ Test tenant: {e}")


def print_testing_instructions(region: str):
    """Print testing instructions."""
    print("📋 Testing Instructions")
    print("=" * 50)
    print()
    print("1. Test ConnectWise Lambda Function:")
    print(f"   aws lambda invoke \\")
    print(f"     --function-name avesa-connectwise-ingestion-dev \\")
    print(f"     --payload '{{\"tenant_id\": \"test-tenant\"}}' \\")
    print(f"     --cli-binary-format raw-in-base64-out \\")
    print(f"     response.json --region {region}")
    print()
    print("2. Check Lambda Logs:")
    print(f"   aws logs tail /aws/lambda/avesa-connectwise-ingestion-dev \\")
    print(f"     --follow --region {region}")
    print()
    print("3. Test Canonical Transform Functions:")
    print(f"   aws lambda invoke \\")
    print(f"     --function-name avesa-canonical-transform-tickets-dev \\")
    print(f"     --payload '{{\"tenant_id\": \"test-tenant\"}}' \\")
    print(f"     --cli-binary-format raw-in-base64-out \\")
    print(f"     response.json --region {region}")
    print()
    print("4. Check DynamoDB Data:")
    print(f"   aws dynamodb scan --table-name TenantServices-dev --region {region}")
    print()
    print("5. Check S3 Data:")
    print(f"   aws s3 ls s3://data-storage-msp-dev/ --recursive --region {region}")
    print()
    print("6. Add Real ConnectWise Credentials:")
    print(f"   python scripts/setup-service.py \\")
    print(f"     --tenant-id test-tenant --service connectwise \\")
    print(f"     --connectwise-url 'https://api-na.myconnectwise.net' \\")
    print(f"     --company-id 'YourCompanyID' \\")
    print(f"     --public-key 'your-public-key' \\")
    print(f"     --private-key 'your-private-key' \\")
    print(f"     --client-id 'your-client-id' \\")
    print(f"     --environment dev --region {region}")
    print()
    print("🔧 Troubleshooting:")
    print("- If Lambda functions fail with import errors, redeploy with fixed dependencies")
    print("- Check CloudWatch logs for detailed error messages")
    print("- Ensure all environment variables are set correctly")
    print("- Verify IAM permissions for Lambda execution role")


if __name__ == '__main__':
    main()