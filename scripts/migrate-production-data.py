#!/usr/bin/env python3
"""
Production Data Migration Script
Migrates data from current account to production account for hybrid AWS account strategy.

This script migrates:
- DynamoDB tables (TenantServices-prod -> TenantServices, LastUpdated-prod -> LastUpdated)
- S3 data (data-storage-msp -> data-storage-msp-prod)
- Secrets Manager secrets (tenant/*/prod -> tenant/*/prod)

Usage:
    python3 scripts/migrate-production-data.py --dry-run
    python3 scripts/migrate-production-data.py --execute
"""

import argparse
import boto3
import json
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError


class ProductionDataMigrator:
    """Handles migration of production data to dedicated AWS account."""
    
    def __init__(self, source_profile: str = "default", dest_profile: str = "avesa-production", region: str = "us-east-1"):
        """Initialize migrator with AWS profiles."""
        self.region = region
        
        # Source clients (current account)
        if source_profile == "default":
            self.source_dynamodb = boto3.client('dynamodb', region_name=region)
            self.source_s3 = boto3.client('s3', region_name=region)
            self.source_secrets = boto3.client('secretsmanager', region_name=region)
        else:
            source_session = boto3.Session(profile_name=source_profile)
            self.source_dynamodb = source_session.client('dynamodb', region_name=region)
            self.source_s3 = source_session.client('s3', region_name=region)
            self.source_secrets = source_session.client('secretsmanager', region_name=region)
        
        # Destination clients (production account)
        try:
            dest_session = boto3.Session(profile_name=dest_profile)
            self.dest_dynamodb = dest_session.client('dynamodb', region_name=region)
            self.dest_s3 = dest_session.client('s3', region_name=region)
            self.dest_secrets = dest_session.client('secretsmanager', region_name=region)
        except Exception as e:
            print(f"Error: Could not initialize production account session with profile '{dest_profile}'")
            print(f"Please ensure the profile is configured: aws configure --profile {dest_profile}")
            raise e
    
    def migrate_dynamodb_data(self, dry_run: bool = True) -> bool:
        """Migrate DynamoDB data from current account to production account."""
        print("\n=== DynamoDB Migration ===")
        
        tables_to_migrate = [
            ("TenantServices-prod", "TenantServices"),
            ("LastUpdated-prod", "LastUpdated")
        ]
        
        for source_table, dest_table in tables_to_migrate:
            print(f"\nMigrating {source_table} -> {dest_table}")
            
            try:
                # Check if source table exists
                self.source_dynamodb.describe_table(TableName=source_table)
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    print(f"  ⚠️  Source table {source_table} not found, skipping...")
                    continue
                else:
                    raise e
            
            if not dry_run:
                # Check if destination table exists
                try:
                    self.dest_dynamodb.describe_table(TableName=dest_table)
                    print(f"  ✓ Destination table {dest_table} exists")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundException':
                        print(f"  ❌ Destination table {dest_table} does not exist!")
                        print(f"     Please create it first using CDK deployment")
                        return False
                    else:
                        raise e
            
            # Scan source table and migrate data
            migrated_count = 0
            try:
                paginator = self.source_dynamodb.get_paginator('scan')
                page_iterator = paginator.paginate(TableName=source_table)
                
                for page in page_iterator:
                    items = page.get('Items', [])
                    for item in items:
                        if dry_run:
                            print(f"  📋 Would migrate item: {item.get('tenant_id', {}).get('S', 'Unknown')}")
                        else:
                            try:
                                self.dest_dynamodb.put_item(TableName=dest_table, Item=item)
                                migrated_count += 1
                                if migrated_count % 10 == 0:
                                    print(f"  ✓ Migrated {migrated_count} items...")
                            except Exception as e:
                                print(f"  ❌ Error migrating item: {e}")
                                return False
                
                if dry_run:
                    print(f"  📋 Would migrate {len(page.get('Items', []))} items total")
                else:
                    print(f"  ✓ Successfully migrated {migrated_count} items")
                    
            except Exception as e:
                print(f"  ❌ Error scanning source table: {e}")
                return False
        
        return True
    
    def migrate_s3_data(self, dry_run: bool = True) -> bool:
        """Migrate S3 data from current account to production account."""
        print("\n=== S3 Migration ===")
        
        source_bucket = "data-storage-msp"
        dest_bucket = "data-storage-msp-prod"
        
        print(f"Migrating {source_bucket} -> {dest_bucket}")
        
        # Check if source bucket exists
        try:
            self.source_s3.head_bucket(Bucket=source_bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"  ⚠️  Source bucket {source_bucket} not found, skipping...")
                return True
            else:
                raise e
        
        if not dry_run:
            # Check if destination bucket exists
            try:
                self.dest_s3.head_bucket(Bucket=dest_bucket)
                print(f"  ✓ Destination bucket {dest_bucket} exists")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(f"  ❌ Destination bucket {dest_bucket} does not exist!")
                    print(f"     Please create it first: aws s3 mb s3://{dest_bucket} --profile avesa-production")
                    return False
                else:
                    raise e
        
        # List and migrate objects
        migrated_count = 0
        try:
            paginator = self.source_s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=source_bucket)
            
            for page in page_iterator:
                objects = page.get('Contents', [])
                for obj in objects:
                    key = obj['Key']
                    size_mb = obj['Size'] / (1024 * 1024)
                    
                    if dry_run:
                        print(f"  📋 Would migrate: {key} ({size_mb:.2f} MB)")
                    else:
                        try:
                            # Download object from source bucket
                            response = self.source_s3.get_object(Bucket=source_bucket, Key=key)
                            object_data = response['Body'].read()
                            
                            # Upload object to destination bucket
                            self.dest_s3.put_object(
                                Bucket=dest_bucket,
                                Key=key,
                                Body=object_data,
                                ContentType=response.get('ContentType', 'binary/octet-stream'),
                                Metadata=response.get('Metadata', {})
                            )
                            migrated_count += 1
                            print(f"  ✓ Migrated: {key} ({size_mb:.2f} MB)")
                            if migrated_count % 10 == 0:
                                print(f"  ✓ Progress: {migrated_count} objects migrated...")
                        except Exception as e:
                            print(f"  ❌ Error migrating {key}: {e}")
                            return False
            
            if dry_run:
                total_objects = sum(len(page.get('Contents', [])) for page in page_iterator)
                print(f"  📋 Would migrate {total_objects} objects total")
            else:
                print(f"  ✓ Successfully migrated {migrated_count} objects")
                
        except Exception as e:
            print(f"  ❌ Error listing source bucket: {e}")
            return False
        
        return True
    
    def migrate_secrets(self, dry_run: bool = True) -> bool:
        """Migrate secrets from current account to production account."""
        print("\n=== Secrets Migration ===")
        
        # List all secrets with tenant/ prefix and /prod suffix
        migrated_count = 0
        try:
            paginator = self.source_secrets.get_paginator('list_secrets')
            page_iterator = paginator.paginate()
            
            for page in page_iterator:
                secrets = page.get('SecretList', [])
                for secret in secrets:
                    secret_name = secret['Name']
                    
                    # Only migrate production tenant secrets
                    if 'tenant/' in secret_name and '/prod' in secret_name:
                        if dry_run:
                            print(f"  📋 Would migrate secret: {secret_name}")
                        else:
                            try:
                                # Get secret value from source
                                response = self.source_secrets.get_secret_value(SecretId=secret_name)
                                secret_value = response['SecretString']
                                
                                # Create in destination account
                                try:
                                    self.dest_secrets.create_secret(
                                        Name=secret_name,
                                        Description=secret.get('Description', f"Migrated from source account on {datetime.now().isoformat()}"),
                                        SecretString=secret_value
                                    )
                                    print(f"  ✓ Created secret: {secret_name}")
                                except ClientError as e:
                                    if e.response['Error']['Code'] == 'ResourceExistsException':
                                        # Update existing secret
                                        self.dest_secrets.update_secret(
                                            SecretId=secret_name,
                                            SecretString=secret_value
                                        )
                                        print(f"  ✓ Updated secret: {secret_name}")
                                    else:
                                        raise e
                                
                                migrated_count += 1
                                
                            except Exception as e:
                                print(f"  ❌ Error migrating secret {secret_name}: {e}")
                                return False
            
            if dry_run:
                prod_secrets = [s['Name'] for s in secrets if 'tenant/' in s['Name'] and '/prod' in s['Name']]
                print(f"  📋 Would migrate {len(prod_secrets)} production secrets")
            else:
                print(f"  ✓ Successfully migrated {migrated_count} secrets")
                
        except Exception as e:
            print(f"  ❌ Error listing secrets: {e}")
            return False
        
        return True
    
    def validate_migration(self) -> bool:
        """Validate that migration was successful."""
        print("\n=== Migration Validation ===")
        
        success = True
        
        # Validate DynamoDB tables
        tables_to_check = ["TenantServices", "LastUpdated"]
        for table_name in tables_to_check:
            try:
                response = self.dest_dynamodb.describe_table(TableName=table_name)
                item_count = response['Table'].get('ItemCount', 0)
                print(f"  ✓ Table {table_name}: {item_count} items")
            except Exception as e:
                print(f"  ❌ Table {table_name}: {e}")
                success = False
        
        # Validate S3 bucket
        try:
            response = self.dest_s3.list_objects_v2(Bucket="data-storage-msp-prod", MaxKeys=1)
            object_count = response.get('KeyCount', 0)
            print(f"  ✓ S3 bucket data-storage-msp-prod: accessible ({object_count}+ objects)")
        except Exception as e:
            print(f"  ❌ S3 bucket data-storage-msp-prod: {e}")
            success = False
        
        # Validate secrets
        try:
            response = self.dest_secrets.list_secrets(MaxResults=1)
            secret_count = len(response.get('SecretList', []))
            print(f"  ✓ Secrets Manager: accessible ({secret_count}+ secrets)")
        except Exception as e:
            print(f"  ❌ Secrets Manager: {e}")
            success = False
        
        return success


def main():
    """Main migration function."""
    parser = argparse.ArgumentParser(description='Migrate production data to dedicated AWS account')
    parser.add_argument('--dry-run', action='store_true', default=True,
                       help='Show what would be migrated without actually migrating (default)')
    parser.add_argument('--execute', action='store_true',
                       help='Actually perform the migration')
    parser.add_argument('--source-profile', default='default',
                       help='AWS profile for source account (default: default)')
    parser.add_argument('--dest-profile', default='avesa-production',
                       help='AWS profile for destination account (default: avesa-production)')
    parser.add_argument('--region', default='us-east-1',
                       help='AWS region (default: us-east-1)')
    
    args = parser.parse_args()
    
    # Determine if this is a dry run
    dry_run = not args.execute
    
    print("=" * 60)
    print("AVESA Production Data Migration")
    print("=" * 60)
    print(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")
    print(f"Source Profile: {args.source_profile}")
    print(f"Destination Profile: {args.dest_profile}")
    print(f"Region: {args.region}")
    print(f"Started: {datetime.now().isoformat()}")
    
    if dry_run:
        print("\n⚠️  DRY RUN MODE - No changes will be made")
        print("   Use --execute to perform actual migration")
    else:
        print("\n🚨 EXECUTE MODE - Changes will be made!")
        response = input("Are you sure you want to proceed? (yes/no): ")
        if response.lower() != 'yes':
            print("Migration cancelled.")
            sys.exit(0)
    
    try:
        # Initialize migrator
        migrator = ProductionDataMigrator(
            source_profile=args.source_profile,
            dest_profile=args.dest_profile,
            region=args.region
        )
        
        # Perform migrations
        success = True
        
        success &= migrator.migrate_dynamodb_data(dry_run=dry_run)
        success &= migrator.migrate_s3_data(dry_run=dry_run)
        success &= migrator.migrate_secrets(dry_run=dry_run)
        
        if not dry_run and success:
            success &= migrator.validate_migration()
        
        print("\n" + "=" * 60)
        if success:
            if dry_run:
                print("✅ DRY RUN COMPLETED - Ready for migration")
                print("\nNext steps:")
                print("1. Create production AWS account and configure profile")
                print("2. Deploy infrastructure: cdk deploy --context environment=prod --all")
                print("3. Run migration: python3 scripts/migrate-production-data.py --execute")
            else:
                print("✅ MIGRATION COMPLETED SUCCESSFULLY")
                print("\nNext steps:")
                print("1. Update DNS/endpoints to point to production account")
                print("2. Test production environment")
                print("3. Monitor for any issues")
        else:
            print("❌ MIGRATION FAILED")
            if not dry_run:
                print("Please review errors above and retry")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Migration failed with error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()