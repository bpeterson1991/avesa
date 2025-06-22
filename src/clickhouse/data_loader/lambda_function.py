"""
ClickHouse Data Loader Lambda Function

This function loads data from S3 canonical files into ClickHouse shared tables.
It handles tenant isolation, respects SCD type configuration, and provides data quality validation.
"""

import json
import os
import logging
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# Import shared components
from shared import ClickHouseClient, AWSClientFactory
from shared.scd_config import SCDConfigManager, get_scd_type, is_scd_type_1, is_scd_type_2

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_tenant_list() -> List[str]:
    """Get list of active tenants from DynamoDB."""
    clients = AWSClientFactory.get_client_bundle(['dynamodb'])
    dynamodb = clients['dynamodb']
    table = dynamodb.Table(os.environ['TENANT_SERVICES_TABLE'])
    
    try:
        response = table.scan(
            ProjectionExpression='tenant_id',
            FilterExpression='enabled = :enabled',
            ExpressionAttributeValues={':enabled': True}
        )
        
        tenants = list(set([item['tenant_id'] for item in response['Items']]))
        logger.info(f"Found {len(tenants)} active tenants: {tenants}")
        return tenants
        
    except Exception as e:
        logger.error(f"Failed to get tenant list: {e}")
        raise

def get_last_updated_timestamp(tenant_id: str, table_name: str) -> Optional[str]:
    """Get the last updated timestamp for incremental processing."""
    clients = AWSClientFactory.get_client_bundle(['dynamodb'])
    dynamodb = clients['dynamodb']
    table = dynamodb.Table(os.environ['LAST_UPDATED_TABLE'])
    
    try:
        response = table.get_item(
            Key={
                'tenant_id': tenant_id,
                'table_name': table_name
            }
        )
        
        if 'Item' in response:
            return response['Item'].get('last_updated')
        return None
        
    except Exception as e:
        logger.error(f"Failed to get last updated timestamp: {e}")
        return None

def update_last_updated_timestamp(tenant_id: str, table_name: str, timestamp: str):
    """Update the last updated timestamp after successful processing."""
    clients = AWSClientFactory.get_client_bundle(['dynamodb'])
    dynamodb = clients['dynamodb']
    table = dynamodb.Table(os.environ['LAST_UPDATED_TABLE'])
    
    try:
        table.put_item(
            Item={
                'tenant_id': tenant_id,
                'table_name': table_name,
                'last_updated': timestamp,
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
        )
        logger.info(f"Updated last_updated timestamp for {tenant_id}/{table_name}: {timestamp}")
        
    except Exception as e:
        logger.error(f"Failed to update last updated timestamp: {e}")
        raise

def load_canonical_data_from_s3(tenant_id: str, table_name: str) -> List[Dict]:
    """Load canonical data from S3 for a specific tenant and table."""
    clients = AWSClientFactory.get_client_bundle(['s3'])
    s3_client = clients['s3']
    bucket_name = os.environ['S3_BUCKET_NAME']
    
    # Construct S3 path for canonical data
    s3_prefix = f"{tenant_id}/canonical/{table_name}/"
    
    try:
        # List objects in the canonical folder
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=s3_prefix
        )
        
        if 'Contents' not in response:
            logger.warning(f"No canonical data found for {tenant_id}/{table_name}")
            return []
        
        # Load and combine all Parquet files (canonical data is in Parquet format)
        all_records = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                try:
                    # For now, we'll use a simple approach to read Parquet via ClickHouse
                    # In production, you might want to use pyarrow or similar
                    logger.info(f"Found Parquet file: {obj['Key']} ({obj['Size']} bytes)")
                    # We'll process this file later using ClickHouse's S3 integration
                    all_records.append({
                        'file_path': obj['Key'],
                        'file_size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
                        
                except Exception as e:
                    logger.error(f"Failed to process {obj['Key']}: {e}")
                    continue
        
        logger.info(f"Found {len(all_records)} Parquet files for {tenant_id}/{table_name}")
        return all_records
            
    except Exception as e:
        logger.error(f"Failed to load canonical data from S3: {e}")
        raise

def calculate_data_hash(row_data: Dict) -> str:
    """Calculate hash for data quality and change detection."""
    # Create a consistent string representation of the data
    data_str = json.dumps(row_data, sort_keys=True, default=str)
    return hashlib.md5(data_str.encode()).hexdigest()

def check_if_already_processed(tenant_id: str, table_name: str, s3_files: List[Dict]) -> bool:
    """Check if the current batch of files has already been processed."""
    if not s3_files:
        return True
    
    try:
        clients = AWSClientFactory.get_client_bundle(['dynamodb'])
        dynamodb = clients['dynamodb']
        table = dynamodb.Table(os.environ.get('LAST_UPDATED_TABLE', 'LastUpdated-dev'))
        
        # Get the last processed timestamp
        response = table.get_item(
            Key={
                'tenant_id': tenant_id,
                'table_name': table_name
            }
        )
        
        if 'Item' not in response:
            return False  # Never processed before
        
        last_processed = response['Item'].get('last_updated')
        if not last_processed:
            return False
        
        # Check if any S3 files are newer than last processed timestamp
        last_processed_dt = datetime.fromisoformat(last_processed.replace('Z', '+00:00'))
        
        for file_info in s3_files:
            file_modified = file_info['last_modified']
            if isinstance(file_modified, str):
                file_modified = datetime.fromisoformat(file_modified.replace('Z', '+00:00'))
            
            if file_modified > last_processed_dt:
                logger.info(f"Found newer file: {file_info['file_path']} modified {file_modified}")
                return False  # Found newer data
        
        logger.info(f"All files for {tenant_id}/{table_name} already processed")
        return True  # All files already processed
        
    except Exception as e:
        logger.error(f"Error checking processing status: {e}")
        return False  # Assume not processed on error

def load_parquet_data_via_clickhouse(client, tenant_id: str, table_name: str, s3_files: List[Dict]) -> int:
    """Load Parquet data directly from S3 into ClickHouse using appropriate SCD logic based on table configuration."""
    if not s3_files:
        logger.info(f"No files to process for {tenant_id}/{table_name}")
        return 0
    
    # Get SCD type for this table
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    scd_type = get_scd_type(table_name, bucket_name)
    logger.info(f"Table {table_name} uses SCD {scd_type} processing")
    
    if scd_type == 'type_1':
        return load_data_scd_type_1(client, tenant_id, table_name, s3_files)
    else:
        return load_data_scd_type_2(client, tenant_id, table_name, s3_files)


def load_data_scd_type_1(client, tenant_id: str, table_name: str, s3_files: List[Dict]) -> int:
    """Load data using SCD Type 1 logic (simple upsert)."""
    total_records = 0
    current_time = datetime.now(timezone.utc)
    current_time_str = current_time.isoformat()
    
    logger.info(f"Processing {len(s3_files)} files for {tenant_id}/{table_name} with SCD Type 1 logic")
    
    for file_info in s3_files:
        file_path = file_info['file_path']
        logger.info(f"Processing file: {file_path}")
        
        try:
            # Use ClickHouse S3 table function to read Parquet directly
            s3_url = f"s3://data-storage-msp-dev/{file_path}"
            
            # Create a temporary staging table for this batch
            staging_table = f"{table_name}_staging_{int(current_time.timestamp() * 1000000)}"
            
            # Step 1: Create staging table with new data
            create_staging_query = f"""
            CREATE TABLE {staging_table} AS
            SELECT DISTINCT
                s.*,
                '{tenant_id}' as tenant_id,
                '{current_time_str}' as effective_date,
                NULL as expiration_date,
                true as is_current,
                'canonical_transform' as source_system,
                '{current_time_str}' as date_entered,
                md5(toString(s.*)) as data_hash,
                1 as record_version
            FROM s3('{s3_url}', 'Parquet') s
            """
            
            client.command(create_staging_query)
            logger.info(f"Created staging table {staging_table}")
            
            # Step 2: For SCD Type 1, delete existing records that will be replaced
            delete_query = f"""
            ALTER TABLE {table_name}
            DELETE WHERE tenant_id = '{tenant_id}'
            AND id IN (SELECT id FROM {staging_table})
            """
            
            client.command(delete_query)
            logger.info(f"Deleted existing records for SCD Type 1 replacement")
            
            # Step 3: Insert all new records
            insert_query = f"""
            INSERT INTO {table_name}
            SELECT * FROM {staging_table}
            """
            
            result = client.command(insert_query)
            
            # Step 4: Clean up staging table
            client.command(f"DROP TABLE {staging_table}")
            
            logger.info(f"Successfully processed data from {file_path} with SCD Type 1 logic")
            total_records += 1
            
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            continue
    
    return total_records


def load_data_scd_type_2(client, tenant_id: str, table_name: str, s3_files: List[Dict]) -> int:
    """Load data using SCD Type 2 logic (historical versioning)."""
    total_records = 0
    current_time = datetime.now(timezone.utc)
    current_time_str = current_time.isoformat()
    
    logger.info(f"Processing {len(s3_files)} files for {tenant_id}/{table_name} with SCD Type 2 logic")
    
    for file_info in s3_files:
        file_path = file_info['file_path']
        logger.info(f"Processing file: {file_path}")
        
        try:
            # Use ClickHouse S3 table function to read Parquet directly
            s3_url = f"s3://data-storage-msp-dev/{file_path}"
            
            # Create a temporary staging table for this batch
            staging_table = f"{table_name}_staging_{int(current_time.timestamp() * 1000000)}"
            
            # Step 1: Create staging table with new data
            create_staging_query = f"""
            CREATE TABLE {staging_table} AS
            SELECT DISTINCT
                s.*,
                '{tenant_id}' as tenant_id,
                '{current_time_str}' as effective_date,
                NULL as expiration_date,
                true as is_current,
                'canonical_transform' as source_system,
                '{current_time_str}' as date_entered,
                md5(toString(s.*)) as data_hash,
                1 as record_version
            FROM s3('{s3_url}', 'Parquet') s
            """
            
            client.command(create_staging_query)
            logger.info(f"Created staging table {staging_table}")
            
            # Step 2: Expire existing records that will be updated (atomic operation)
            expire_query = f"""
            ALTER TABLE {table_name}
            UPDATE
                expiration_date = '{current_time_str}',
                is_current = false
            WHERE tenant_id = '{tenant_id}'
            AND is_current = true
            AND id IN (SELECT id FROM {staging_table})
            AND (data_hash != (SELECT data_hash FROM {staging_table} st WHERE st.id = {table_name}.id LIMIT 1)
                 OR data_hash IS NULL)
            """
            
            client.command(expire_query)
            logger.info(f"Expired changed records atomically")
            
            # Step 3: Insert only new or changed records
            insert_query = f"""
            INSERT INTO {table_name}
            SELECT * FROM {staging_table} st
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name} t
                WHERE t.tenant_id = st.tenant_id
                AND t.id = st.id
                AND t.is_current = true
                AND t.data_hash = st.data_hash
            )
            """
            
            result = client.command(insert_query)
            
            # Step 4: Clean up staging table
            client.command(f"DROP TABLE {staging_table}")
            
            logger.info(f"Successfully processed data from {file_path} with SCD Type 2 logic")
            total_records += 1
            
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            continue
    
    return total_records

def check_existing_records(client, tenant_id: str, table_name: str, record_ids: List[str]) -> Dict[str, str]:
    """Check for existing records to handle SCD Type 2 updates."""
    if not record_ids:
        return {}
    
    try:
        # Query for existing current records
        ids_str = "', '".join(record_ids)
        query = f"""
        SELECT id, data_hash
        FROM {table_name}
        WHERE tenant_id = '{tenant_id}'
        AND id IN ('{ids_str}')
        AND is_current = true
        """
        
        result = client.query(query)
        existing_records = {row[0]: row[1] for row in result.result_rows}
        
        logger.info(f"Found {len(existing_records)} existing records for {tenant_id}/{table_name}")
        return existing_records
        
    except Exception as e:
        logger.error(f"Failed to check existing records: {e}")
        return {}

def expire_changed_records(client, tenant_id: str, table_name: str, changed_ids: List[str]):
    """Expire records that have changed (SCD Type 2)."""
    if not changed_ids:
        return
    
    try:
        current_time = datetime.now(timezone.utc)
        ids_str = "', '".join(changed_ids)
        
        update_query = f"""
        ALTER TABLE {table_name}
        UPDATE 
            expiration_date = '{current_time.isoformat()}',
            is_current = false
        WHERE tenant_id = '{tenant_id}'
        AND id IN ('{ids_str}')
        AND is_current = true
        """
        
        client.command(update_query)
        logger.info(f"Expired {len(changed_ids)} changed records in {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to expire changed records: {e}")
        raise

def process_tenant_table(client, tenant_id: str, table_name: str) -> Dict[str, Any]:
    """Process data for a specific tenant and table with idempotency checks."""
    logger.info(f"Processing {tenant_id}/{table_name}")
    
    try:
        # Load canonical data file list from S3
        s3_files = load_canonical_data_from_s3(tenant_id, table_name)
        
        if not s3_files:
            return {
                'tenant_id': tenant_id,
                'table_name': table_name,
                'status': 'no_data',
                'records_processed': 0
            }
        
        # Check if this data has already been processed (idempotency)
        if check_if_already_processed(tenant_id, table_name, s3_files):
            return {
                'tenant_id': tenant_id,
                'table_name': table_name,
                'status': 'already_processed',
                'reason': 'All files already processed - skipping to prevent duplicates'
            }
        
        # Load data directly from S3 Parquet files into ClickHouse
        records_processed = load_parquet_data_via_clickhouse(client, tenant_id, table_name, s3_files)
        
        # Update last updated timestamp
        current_timestamp = datetime.now(timezone.utc).isoformat()
        update_last_updated_timestamp(tenant_id, table_name, current_timestamp)
        
        return {
            'tenant_id': tenant_id,
            'table_name': table_name,
            'status': 'success',
            'files_processed': len(s3_files),
            'records_processed': records_processed,
            'last_updated': current_timestamp
        }
        
    except Exception as e:
        logger.error(f"Failed to process {tenant_id}/{table_name}: {e}")
        return {
            'tenant_id': tenant_id,
            'table_name': table_name,
            'status': 'error',
            'error': str(e)
        }

def lambda_handler(event, context):
    """
    Lambda handler for ClickHouse data loading.
    
    Args:
        event: Lambda event (can specify tenant_id and table_name)
        context: Lambda context
        
    Returns:
        Dict with processing results
    """
    logger.info(f"Starting ClickHouse data loading")
    logger.info(f"Event: {json.dumps(event, default=str)}")
    
    target_table = os.environ.get('TARGET_TABLE')
    
    try:
        # Get environment configuration using proper pattern
        from shared.environment import Environment
        env_name = os.environ.get('ENVIRONMENT', 'dev')
        config = Environment.get_config(env_name)
        
        # Get ClickHouse connection using shared client
        client = ClickHouseClient.from_environment(os.environ.get('ENVIRONMENT', 'dev'))
        logger.info("Successfully connected to ClickHouse")
        
        # Determine processing scope
        if 'tenant_id' in event and 'table_name' in event:
            # Process specific tenant/table
            tenant_id = event['tenant_id']
            table_name = event['table_name']
            results = [process_tenant_table(client, tenant_id, table_name)]
        elif 'tenant_id' in event:
            # Process all tables for specific tenant
            tenant_id = event['tenant_id']
            table_name = target_table or 'companies'  # Default to companies if not specified
            results = [process_tenant_table(client, tenant_id, table_name)]
        else:
            # Process target table for all tenants
            table_name = target_table or 'companies'
            tenants = get_tenant_list()
            results = []
            
            for tenant_id in tenants:
                result = process_tenant_table(client, tenant_id, table_name)
                results.append(result)
        
        # Summarize results
        total_files = sum(r.get('files_processed', 0) for r in results)
        total_processed = sum(r.get('records_processed', 0) for r in results)
        errors = [r for r in results if r.get('status') == 'error']
        
        response = {
            'statusCode': 200 if not errors else 207,
            'body': {
                'message': f'ClickHouse data loading completed for {table_name}',
                'summary': {
                    'total_tenants_processed': len(results),
                    'total_files_processed': total_files,
                    'total_records_processed': total_processed,
                    'errors': len(errors)
                },
                'results': results,
                'environment': os.environ.get('ENVIRONMENT', 'unknown')
            }
        }
        
        logger.info(f"Data loading completed: {response['body']['summary']}")
        return response
        
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        return {
            'statusCode': 500,
            'body': {
                'message': 'ClickHouse data loading failed',
                'error': str(e),
                'environment': os.environ.get('ENVIRONMENT', 'unknown')
            }
        }
    
    finally:
        # Close connection if it exists
        try:
            if 'client' in locals():
                client.close()
        except:
            pass