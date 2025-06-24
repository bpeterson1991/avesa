#!/usr/bin/env python3
"""
Consolidated ClickHouse Canonical Data Loader

This script consolidates 8+ duplicate scripts into a single, configurable loader.
Based on analysis, only 2 distinct approaches are needed:

1. Direct Mode: Uses ClickHouse S3 table function (fastest, requires S3 access from ClickHouse)
2. Python Mode: Downloads files via Python client (most reliable, works with ClickHouse Cloud)

Usage:
    python load-canonical-data.py [options]

Options:
    --mode {direct|python}        Loading approach (default: python)
    --with-creds                  Use S3 credentials for ClickHouse access (direct mode only)
    --tenant-id TEXT              Tenant ID to load data for (default: sitetechnology)
    --clear-existing              Clear existing data before loading
    --dry-run                     Show what would be done without executing
    --help                        Show this help message

Examples:
    # Most reliable approach (recommended)
    python load-canonical-data.py --mode python --tenant-id sitetechnology

    # Fastest approach (if ClickHouse has S3 access)
    python load-canonical-data.py --mode direct --with-creds

    # Clear existing data first
    python load-canonical-data.py --mode python --clear-existing
"""

import os
import sys
import json
import boto3
import clickhouse_connect
import pandas as pd
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Optional
from io import BytesIO

def get_clickhouse_connection():
    """Get ClickHouse connection using credentials from AWS Secrets Manager."""
    secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
    secret_name = 'clickhouse-connection-dev'
    
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        
        client = clickhouse_connect.get_client(
            host=secret['host'],
            port=secret.get('port', 8443),
            username=secret['username'],
            password=secret['password'],
            database=secret.get('database', 'default'),
            secure=True,
            verify=False,
            connect_timeout=30,
            send_receive_timeout=300
        )
        
        print(f"✅ Connected to ClickHouse: {secret['host']}")
        return client
        
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        raise

def get_s3_credentials() -> Optional[Dict[str, str]]:
    """Get S3 credentials for ClickHouse access."""
    try:
        secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
        response = secrets_client.get_secret_value(SecretId='clickhouse-s3-credentials')
        credentials = json.loads(response['SecretString'])
        
        return {
            'access_key_id': credentials['access_key_id'],
            'secret_access_key': credentials['secret_access_key']
        }
    except Exception as e:
        print(f"⚠️  Could not get S3 credentials: {e}")
        return None

def safe_str(value):
    """Convert value to string, handling None values."""
    if value is None or pd.isna(value):
        return ''
    return str(value)

def safe_datetime(value):
    """Convert value to datetime string, handling None values."""
    if value is None or pd.isna(value):
        return None
    try:
        return pd.to_datetime(value).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return None

def create_tables(client, dry_run: bool = False):
    """Create simplified tables for loading canonical data."""
    print("🔄 Creating tables...")
    
    tables = ['companies', 'contacts', 'tickets', 'time_entries']
    
    # Drop existing tables
    for table in tables:
        drop_sql = f"DROP TABLE IF EXISTS {table}"
        if dry_run:
            print(f"   [DRY RUN] Would execute: {drop_sql}")
        else:
            try:
                client.command(drop_sql)
                print(f"   🗑️  Dropped {table}")
            except:
                pass
    
    # Table definitions
    table_sqls = {
        'companies': """
        CREATE TABLE companies (
            tenant_id String,
            id String,
            company_name String,
            company_identifier String,
            company_type String,
            status String,
            effective_date DateTime,
            expiration_date Nullable(DateTime),
            is_current Bool,
            source_system String,
            source_id String,
            last_updated DateTime,
            created_date DateTime,
            data_hash String,
            record_version UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (tenant_id, id, effective_date)
        SETTINGS index_granularity = 8192
        """,
        
        'contacts': """
        CREATE TABLE contacts (
            tenant_id String,
            id String,
            company_id String,
            first_name String,
            last_name String,
            effective_date DateTime,
            expiration_date Nullable(DateTime),
            is_current Bool,
            source_system String,
            source_id String,
            last_updated DateTime,
            created_date DateTime,
            data_hash String,
            record_version UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (tenant_id, id, effective_date)
        SETTINGS index_granularity = 8192
        """,
        
        'tickets': """
        CREATE TABLE tickets (
            tenant_id String,
            id String,
            ticket_number String,
            summary String,
            description String,
            status String,
            priority String,
            created_date DateTime,
            effective_date DateTime,
            expiration_date Nullable(DateTime),
            is_current Bool,
            source_system String,
            source_id String,
            last_updated DateTime,
            data_hash String,
            record_version UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (tenant_id, id, effective_date)
        SETTINGS index_granularity = 8192
        """,
        
        'time_entries': """
        CREATE TABLE time_entries (
            tenant_id String,
            id String,
            hours Float64,
            description String,
            date_entered DateTime,
            effective_date DateTime,
            expiration_date Nullable(DateTime),
            is_current Bool,
            source_system String,
            source_id String,
            last_updated DateTime,
            data_hash String,
            record_version UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (tenant_id, id, effective_date)
        SETTINGS index_granularity = 8192
        """
    }
    
    # Execute table creation
    for table_name, sql in table_sqls.items():
        if dry_run:
            print(f"   [DRY RUN] Would create {table_name} table")
        else:
            try:
                client.command(sql)
                print(f"   ✅ Created {table_name} table")
            except Exception as e:
                print(f"   ❌ Failed to create {table_name}: {e}")
                raise
    
    if not dry_run:
        print("✅ All tables created successfully")

def get_canonical_files(tenant_id: str, table_name: str) -> List[Dict]:
    """Get list of canonical Parquet files from S3."""
    s3_client = boto3.client('s3', region_name='us-east-2')
    bucket_name = 'data-storage-msp-dev'
    s3_prefix = f"{tenant_id}/canonical/{table_name}/"
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=s3_prefix
        )
        
        if 'Contents' not in response:
            print(f"⚠️  No canonical data found for {tenant_id}/{table_name}")
            return []
        
        parquet_files = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                parquet_files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified']
                })
        
        print(f"📁 Found {len(parquet_files)} Parquet files for {tenant_id}/{table_name}")
        return parquet_files
        
    except Exception as e:
        print(f"❌ Failed to list S3 files: {e}")
        raise

def clear_existing_data(client, tenant_id: str, table_name: str, dry_run: bool = False):
    """Clear existing data for the tenant before loading new data."""
    delete_query = f"DELETE FROM {table_name} WHERE tenant_id = '{tenant_id}'"
    
    if dry_run:
        print(f"   [DRY RUN] Would execute: {delete_query}")
    else:
        try:
            client.command(delete_query)
            print(f"   🗑️  Cleared existing data for {tenant_id} in {table_name}")
        except Exception as e:
            print(f"   ⚠️  Warning: Could not clear existing data: {e}")

def load_data_direct_mode(client, tenant_id: str, table_name: str, files: List[Dict], 
                         s3_creds: Optional[Dict] = None, dry_run: bool = False) -> int:
    """Load data using ClickHouse S3 table function (direct mode)."""
    if not files:
        print(f"⚠️  No files to process for {tenant_id}/{table_name}")
        return 0
    
    total_loaded = 0
    
    print(f"🔄 Processing {len(files)} files for {table_name} (direct mode)...")
    
    for file_info in files:
        file_key = file_info['key']
        print(f"   📄 Processing: {file_key} ({file_info['size']} bytes)")
        
        if dry_run:
            print(f"      [DRY RUN] Would load data from S3 using table function")
            total_loaded += 1
            continue
        
        try:
            s3_url = f"https://s3.us-east-2.amazonaws.com/data-storage-msp-dev/{file_key}"
            
            # Build S3 function call with or without credentials
            if s3_creds:
                s3_function = f"s3('{s3_url}', '{s3_creds['access_key_id']}', '{s3_creds['secret_access_key']}', 'Parquet')"
            else:
                s3_function = f"s3('{s3_url}', 'Parquet')"
            
            # Table-specific insert queries
            if table_name == 'companies':
                insert_query = f"""
                INSERT INTO companies 
                SELECT 
                    '{tenant_id}' as tenant_id,
                    toString(company_id) as id,
                    name as company_name,
                    toString(company_id) as company_identifier,
                    ifNull(type, '') as company_type,
                    status,
                    parseDateTime64BestEffort(effective_start_date) as effective_date,
                    if(effective_end_date = '', NULL, parseDateTime64BestEffort(effective_end_date)) as expiration_date,
                    is_current,
                    source_system,
                    toString(company_id) as source_id,
                    parseDateTime64BestEffort(updated_date) as last_updated,
                    parseDateTime64BestEffort(created_date) as created_date,
                    record_hash as data_hash,
                    1 as record_version
                FROM {s3_function}
                """
            elif table_name == 'contacts':
                insert_query = f"""
                INSERT INTO contacts 
                SELECT 
                    '{tenant_id}' as tenant_id,
                    toString(contact_id) as id,
                    toString(company_id) as company_id,
                    first_name,
                    ifNull(last_name, '') as last_name,
                    parseDateTime64BestEffort(effective_start_date) as effective_date,
                    if(effective_end_date = '', NULL, parseDateTime64BestEffort(effective_end_date)) as expiration_date,
                    is_current,
                    source_system,
                    toString(contact_id) as source_id,
                    parseDateTime64BestEffort(updated_date) as last_updated,
                    parseDateTime64BestEffort(updated_date) as created_date,
                    record_hash as data_hash,
                    1 as record_version
                FROM {s3_function}
                """
            elif table_name == 'tickets':
                insert_query = f"""
                INSERT INTO tickets 
                SELECT 
                    '{tenant_id}' as tenant_id,
                    toString(ticket_id) as id,
                    toString(ticket_id) as ticket_number,
                    summary,
                    ifNull(description, '') as description,
                    status,
                    priority,
                    parseDateTime64BestEffort(created_date) as created_date,
                    parseDateTime64BestEffort(effective_start_date) as effective_date,
                    if(effective_end_date = '', NULL, parseDateTime64BestEffort(effective_end_date)) as expiration_date,
                    is_current,
                    source_system,
                    toString(ticket_id) as source_id,
                    parseDateTime64BestEffort(updated_date) as last_updated,
                    record_hash as data_hash,
                    1 as record_version
                FROM {s3_function}
                """
            elif table_name == 'time_entries':
                insert_query = f"""
                INSERT INTO time_entries 
                SELECT 
                    '{tenant_id}' as tenant_id,
                    toString(entry_id) as id,
                    hours,
                    description,
                    parseDateTime64BestEffort(date_start) as date_entered,
                    parseDateTime64BestEffort(effective_start_date) as effective_date,
                    if(effective_end_date = '', NULL, parseDateTime64BestEffort(effective_end_date)) as expiration_date,
                    is_current,
                    source_system,
                    toString(entry_id) as source_id,
                    parseDateTime64BestEffort(date_start) as last_updated,
                    record_hash as data_hash,
                    1 as record_version
                FROM {s3_function}
                """
            else:
                raise ValueError(f"Unknown table type: {table_name}")
            
            client.command(insert_query)
            
            # Get count of inserted records
            count_query = f"SELECT count() FROM {s3_function}"
            count_result = client.query(count_query)
            record_count = count_result.result_rows[0][0] if count_result.result_rows else 0
            
            total_loaded += record_count
            print(f"      ✅ Loaded {record_count} records")
            
        except Exception as e:
            print(f"      ❌ Failed to load {file_key}: {e}")
            continue
    
    return total_loaded

def map_data_for_table(df: pd.DataFrame, table_name: str, tenant_id: str) -> pd.DataFrame:
    """Map canonical data to ClickHouse schema with proper null handling."""
    mapped_df = pd.DataFrame()
    mapped_df['tenant_id'] = tenant_id
    
    if table_name == 'companies':
        mapped_df['id'] = df['company_id'].astype(str)
        mapped_df['company_name'] = df['name'].apply(safe_str)
        mapped_df['company_identifier'] = df['company_id'].astype(str)
        mapped_df['company_type'] = df['type'].apply(safe_str)
        mapped_df['status'] = df['status'].apply(safe_str)
        mapped_df['effective_date'] = df['effective_start_date'].apply(safe_datetime)
        mapped_df['expiration_date'] = df['effective_end_date'].apply(safe_datetime)
        mapped_df['is_current'] = df['is_current']
        mapped_df['source_system'] = df['source_system'].apply(safe_str)
        mapped_df['source_id'] = df['company_id'].astype(str)
        mapped_df['last_updated'] = df['updated_date'].apply(safe_datetime)
        mapped_df['created_date'] = df['created_date'].apply(safe_datetime)
        mapped_df['data_hash'] = df['record_hash'].apply(safe_str)
        mapped_df['record_version'] = 1
        
    elif table_name == 'contacts':
        mapped_df['id'] = df['contact_id'].astype(str)
        mapped_df['company_id'] = df['company_id'].fillna('').astype(str)
        mapped_df['first_name'] = df['first_name'].apply(safe_str)
        mapped_df['last_name'] = df['last_name'].apply(safe_str)
        mapped_df['effective_date'] = df['effective_start_date'].apply(safe_datetime)
        mapped_df['expiration_date'] = df['effective_end_date'].apply(safe_datetime)
        mapped_df['is_current'] = df['is_current']
        mapped_df['source_system'] = df['source_system'].apply(safe_str)
        mapped_df['source_id'] = df['contact_id'].astype(str)
        mapped_df['last_updated'] = df['updated_date'].apply(safe_datetime)
        mapped_df['created_date'] = df['updated_date'].apply(safe_datetime)  # Use updated_date as fallback
        mapped_df['data_hash'] = df['record_hash'].apply(safe_str)
        mapped_df['record_version'] = 1
        
    elif table_name == 'tickets':
        mapped_df['id'] = df['ticket_id'].astype(str)
        mapped_df['ticket_number'] = df['ticket_id'].astype(str)
        mapped_df['summary'] = df['summary'].apply(safe_str)
        mapped_df['description'] = df['description'].apply(safe_str)
        mapped_df['status'] = df['status'].apply(safe_str)
        mapped_df['priority'] = df['priority'].apply(safe_str)
        mapped_df['created_date'] = df['created_date'].apply(safe_datetime)
        mapped_df['effective_date'] = df['effective_start_date'].apply(safe_datetime)
        mapped_df['expiration_date'] = df['effective_end_date'].apply(safe_datetime)
        mapped_df['is_current'] = df['is_current']
        mapped_df['source_system'] = df['source_system'].apply(safe_str)
        mapped_df['source_id'] = df['ticket_id'].astype(str)
        mapped_df['last_updated'] = df['updated_date'].apply(safe_datetime)
        mapped_df['data_hash'] = df['record_hash'].apply(safe_str)
        mapped_df['record_version'] = 1
        
    elif table_name == 'time_entries':
        mapped_df['id'] = df['entry_id'].astype(str)
        mapped_df['hours'] = df['hours']
        mapped_df['description'] = df['description'].apply(safe_str)
        mapped_df['date_entered'] = df['date_start'].apply(safe_datetime)
        mapped_df['effective_date'] = df['effective_start_date'].apply(safe_datetime)
        mapped_df['expiration_date'] = df['effective_end_date'].apply(safe_datetime)
        mapped_df['is_current'] = df['is_current']
        mapped_df['source_system'] = df['source_system'].apply(safe_str)
        mapped_df['source_id'] = df['entry_id'].astype(str)
        mapped_df['last_updated'] = df['date_start'].apply(safe_datetime)
        mapped_df['data_hash'] = df['record_hash'].apply(safe_str)
        mapped_df['record_version'] = 1
    
    return mapped_df

def load_data_python_mode(client, tenant_id: str, table_name: str, files: List[Dict], 
                         dry_run: bool = False) -> int:
    """Load data by downloading from S3 and inserting via Python client."""
    if not files:
        print(f"⚠️  No files to process for {tenant_id}/{table_name}")
        return 0
    
    s3_client = boto3.client('s3', region_name='us-east-2')
    bucket_name = 'data-storage-msp-dev'
    total_loaded = 0
    
    print(f"🔄 Processing {len(files)} files for {table_name} (python mode)...")
    
    for file_info in files:
        try:
            print(f"   📄 Processing: {file_info['key']}")
            
            if dry_run:
                print(f"      [DRY RUN] Would download and process file")
                total_loaded += 100  # Mock count
                continue
            
            # Download Parquet file from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=file_info['key'])
            parquet_data = response['Body'].read()
            
            # Read Parquet data with pandas
            df = pd.read_parquet(BytesIO(parquet_data))
            print(f"      📊 Raw data: {df.shape[0]} rows, {df.shape[1]} columns")
            
            # Map data to ClickHouse schema
            mapped_df = map_data_for_table(df, table_name, tenant_id)
            print(f"      🔄 Mapped data: {mapped_df.shape[0]} rows, {mapped_df.shape[1]} columns")
            
            # Convert DataFrame to list of tuples for ClickHouse insertion
            data_tuples = []
            for _, row in mapped_df.iterrows():
                tuple_row = []
                for value in row:
                    if pd.isna(value) or value is None:
                        tuple_row.append(None)
                    else:
                        tuple_row.append(value)
                data_tuples.append(tuple(tuple_row))
            
            # Insert data into ClickHouse
            if data_tuples:
                client.insert(table_name, data_tuples, column_names=list(mapped_df.columns))
                total_loaded += len(data_tuples)
                print(f"      ✅ Loaded {len(data_tuples)} records")
            
        except Exception as e:
            print(f"      ❌ Failed to load {file_info['key']}: {e}")
            continue
    
    return total_loaded

def main():
    """Main function to load canonical data into ClickHouse."""
    parser = argparse.ArgumentParser(
        description='Consolidated ClickHouse Canonical Data Loader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Most reliable approach (recommended)
  python load-canonical-data.py --mode python --tenant-id sitetechnology

  # Fastest approach (if ClickHouse has S3 access)
  python load-canonical-data.py --mode direct --with-creds

  # Clear existing data first
  python load-canonical-data.py --mode python --clear-existing
        """
    )
    
    parser.add_argument('--mode', choices=['direct', 'python'], default='python',
                       help='Loading approach (default: python)')
    parser.add_argument('--with-creds', action='store_true',
                       help='Use S3 credentials for ClickHouse access (direct mode only)')
    parser.add_argument('--tenant-id', default='sitetechnology',
                       help='Tenant ID to load data for (default: sitetechnology)')
    parser.add_argument('--clear-existing', action='store_true',
                       help='Clear existing data before loading')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without executing')
    
    args = parser.parse_args()
    
    print(f"🚀 Consolidated ClickHouse Canonical Data Loader")
    print(f"   Mode: {args.mode}")
    print(f"   Tenant: {args.tenant_id}")
    print(f"   Clear existing: {args.clear_existing}")
    print(f"   Dry run: {args.dry_run}")
    print("=" * 60)
    
    # Configuration
    table_types = ["companies", "contacts", "tickets", "time_entries"]
    
    try:
        # Connect to ClickHouse
        client = get_clickhouse_connection()
        
        # Get S3 credentials if needed
        s3_creds = None
        if args.mode == 'direct' and args.with_creds:
            s3_creds = get_s3_credentials()
            if s3_creds:
                print(f"✅ Retrieved S3 credentials for ClickHouse access")
        
        # Create tables
        create_tables(client, args.dry_run)
        
        # Process each table type
        total_records_loaded = 0
        
        for table_name in table_types:
            print(f"\n📋 Processing {table_name.upper()}")
            print("-" * 40)
            
            # Clear existing data if requested
            if args.clear_existing:
                clear_existing_data(client, args.tenant_id, table_name, args.dry_run)
            
            # Get canonical files
            files = get_canonical_files(args.tenant_id, table_name)
            
            if files:
                if args.mode == 'direct':
                    records_loaded = load_data_direct_mode(
                        client, args.tenant_id, table_name, files, s3_creds, args.dry_run
                    )
                else:  # python mode
                    records_loaded = load_data_python_mode(
                        client, args.tenant_id, table_name, files, args.dry_run
                    )
                
                total_records_loaded += records_loaded
                print(f"   ✅ Total loaded for {table_name}: {records_loaded} records from {len(files)} files")
            else:
                print(f"   ⚠️  No files found for {table_name}")
        
        print(f"\n📊 FINAL SUMMARY")
        print("=" * 60)
        print(f"✅ Total records loaded: {total_records_loaded}")
        print(f"🎯 Tenant: {args.tenant_id}")
        print(f"📅 Timestamp: {datetime.now(timezone.utc).isoformat()}")
        
        # Verification queries
        if not args.dry_run:
            print(f"\n🔍 VERIFICATION QUERIES")
            print("-" * 40)
            
            for table_name in table_types:
                try:
                    count_query = f"SELECT count() FROM {table_name} WHERE tenant_id = '{args.tenant_id}'"
                    result = client.query(count_query)
                    count = result.result_rows[0][0] if result.result_rows else 0
                    print(f"   📊 {table_name}: {count} records")
                except Exception as e:
                    print(f"   ❌ {table_name}: Error - {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Close connection
        try:
            if 'client' in locals():
                client.close()
                print(f"\n🔌 ClickHouse connection closed")
        except:
            pass

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)