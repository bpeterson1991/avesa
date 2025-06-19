"""
Canonical SCD Type 2 Transformation Lambda Function

This function transforms raw data to canonical form using JSON mapping files.
Performs SCD Type 2 tracking by detecting changes in records and writing new
versions with effective_start, effective_end, and is_current fields.
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple

# Add shared module to path
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))

from shared import (
    Config, TenantConfig,
    get_dynamodb_client, get_s3_client,
    PipelineLogger, get_timestamp, get_s3_key,
    validate_tenant_config, safe_get, normalize_datetime
)

# Initialize clients
dynamodb = get_dynamodb_client()
s3 = get_s3_client()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for canonical transformation.
    
    Args:
        event: Lambda event (can contain tenant_id and table_name for targeted runs)
        context: Lambda context
        
    Returns:
        Execution summary
    """
    logger = PipelineLogger("canonical_transform")
    config = Config.from_environment()
    
    try:
        logger.info("Starting canonical transformation", execution_id=context.aws_request_id)
        
        # Get target tenant and table from event or environment variable
        target_tenant = event.get('tenant_id')
        target_table = event.get('table_name') or os.environ.get('CANONICAL_TABLE')
        
        # Get tenant configurations
        tenants = get_tenant_configurations(config, target_tenant)
        
        results = []
        for tenant_config in tenants:
            tenant_logger = PipelineLogger("canonical_transform", tenant_config.tenant_id)
            
            try:
                # Get canonical table mappings
                mappings = get_canonical_mappings(config, tenant_config.tenant_id)
                
                # Filter mappings if specific table requested
                if target_table:
                    mappings = {k: v for k, v in mappings.items() if k == target_table}
                
                for canonical_table, mapping in mappings.items():
                    table_logger = PipelineLogger("canonical_transform", tenant_config.tenant_id, canonical_table)
                    
                    try:
                        result = process_canonical_table(
                            config=config,
                            tenant_config=tenant_config,
                            canonical_table=canonical_table,
                            mapping=mapping,
                            logger=table_logger
                        )
                        results.append(result)
                        
                    except Exception as e:
                        table_logger.error(f"Failed to process canonical table {canonical_table}: {str(e)}")
                        results.append({
                            'tenant_id': tenant_config.tenant_id,
                            'canonical_table': canonical_table,
                            'status': 'error',
                            'error': str(e)
                        })
                        
            except Exception as e:
                tenant_logger.error(f"Failed to process tenant {tenant_config.tenant_id}: {str(e)}")
                results.append({
                    'tenant_id': tenant_config.tenant_id,
                    'status': 'error',
                    'error': str(e)
                })
        
        # Calculate summary
        successful = len([r for r in results if r.get('status') == 'success'])
        failed = len([r for r in results if r.get('status') == 'error'])
        total_records = sum(r.get('record_count', 0) for r in results)
        
        logger.info(
            "Canonical transformation completed",
            successful_jobs=successful,
            failed_jobs=failed,
            total_records=total_records
        )
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Canonical transformation completed',
                'successful_jobs': successful,
                'failed_jobs': failed,
                'total_records': total_records,
                'results': results
            }
        }
        
    except Exception as e:
        logger.error(f"Canonical transformation failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'message': 'Canonical transformation failed',
                'error': str(e)
            }
        }


def get_tenant_configurations(config: Config, target_tenant: Optional[str] = None) -> List[TenantConfig]:
    """Get tenant configurations from DynamoDB."""
    try:
        if target_tenant:
            # Get specific tenant
            response = dynamodb.get_item(
                TableName=config.tenant_services_table,
                Key={'tenant_id': {'S': target_tenant}}
            )
            if 'Item' not in response:
                raise ValueError(f"Tenant {target_tenant} not found")
            
            item = response['Item']
            tenant_data = {
                'tenant_id': item['tenant_id']['S'],
                'connectwise_url': item['connectwise_url']['S'],
                'secret_name': item['secret_name']['S'],
                'enabled': item.get('enabled', {'BOOL': True})['BOOL'],
                'tables': [t['S'] for t in item.get('tables', {'L': []})['L']],
                'custom_config': json.loads(item.get('custom_config', {'S': '{}'})['S'])
            }
            return [validate_tenant_config(tenant_data)]
        else:
            # Get all enabled tenants
            response = dynamodb.scan(
                TableName=config.tenant_services_table,
                FilterExpression='enabled = :enabled',
                ExpressionAttributeValues={':enabled': {'BOOL': True}}
            )
            
            tenants = []
            for item in response['Items']:
                tenant_data = {
                    'tenant_id': item['tenant_id']['S'],
                    'connectwise_url': item['connectwise_url']['S'],
                    'secret_name': item['secret_name']['S'],
                    'enabled': item.get('enabled', {'BOOL': True})['BOOL'],
                    'tables': [t['S'] for t in item.get('tables', {'L': []})['L']],
                    'custom_config': json.loads(item.get('custom_config', {'S': '{}'})['S'])
                }
                tenants.append(validate_tenant_config(tenant_data))
            
            return tenants
            
    except Exception as e:
        raise Exception(f"Failed to get tenant configurations: {str(e)}")


def get_canonical_mappings(config: Config, tenant_id: str) -> Dict[str, Dict[str, Any]]:
    """Get canonical table mappings from S3 (one file per canonical table)."""
    try:
        mappings = {}
        
        # List all mapping files in the mappings directory
        try:
            # Try tenant-specific mappings first
            mapping_prefix = f"{tenant_id}/mappings/"
            response = s3.list_objects_v2(Bucket=config.bucket_name, Prefix=mapping_prefix)
            
            if 'Contents' not in response:
                # Fall back to default mappings
                mapping_prefix = "mappings/"
                response = s3.list_objects_v2(Bucket=config.bucket_name, Prefix=mapping_prefix)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.json'):
                        # Extract canonical table name from filename
                        filename = obj['Key'].split('/')[-1]
                        canonical_table = filename.replace('.json', '')
                        
                        # Read the mapping file
                        mapping_response = s3.get_object(Bucket=config.bucket_name, Key=obj['Key'])
                        mapping_data = json.loads(mapping_response['Body'].read().decode('utf-8'))
                        mappings[canonical_table] = mapping_data
        
        except Exception:
            # If S3 operations fail, fall back to local mappings directory
            import os
            mappings_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'mappings')
            if os.path.exists(mappings_dir):
                for filename in os.listdir(mappings_dir):
                    if filename.endswith('.json'):
                        canonical_table = filename.replace('.json', '')
                        filepath = os.path.join(mappings_dir, filename)
                        with open(filepath, 'r') as f:
                            mappings[canonical_table] = json.load(f)
        
        return mappings
        
    except Exception as e:
        raise Exception(f"Failed to get canonical mappings: {str(e)}")


def process_canonical_table(
    config: Config,
    tenant_config: TenantConfig,
    canonical_table: str,
    mapping: Dict[str, Any],
    logger: PipelineLogger
) -> Dict[str, Any]:
    """Process a single canonical table for a tenant."""
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting processing for canonical table {canonical_table}")
        
        # Get source table information from mapping
        source_info = get_source_table_info(mapping)
        if not source_info:
            raise ValueError(f"No source table information found in mapping for {canonical_table}")
        
        source_service, source_table = source_info
        
        # Read raw data from S3
        raw_data = read_raw_data_from_s3(
            config=config,
            tenant_id=tenant_config.tenant_id,
            service=source_service,
            table_name=source_table,
            logger=logger
        )
        
        if not raw_data:
            logger.info(f"No raw data found for canonical table {canonical_table}")
            return {
                'tenant_id': tenant_config.tenant_id,
                'canonical_table': canonical_table,
                'status': 'success',
                'record_count': 0,
                'message': 'No raw data found'
            }
        
        # Transform data using mapping
        transformed_data = transform_data(raw_data, mapping, logger)
        
        # Read existing canonical data
        existing_data = read_canonical_data_from_s3(
            config=config,
            tenant_id=tenant_config.tenant_id,
            canonical_table=canonical_table,
            logger=logger
        )
        
        # Apply SCD Type 2 logic
        scd2_data = apply_scd2_logic(
            new_data=transformed_data,
            existing_data=existing_data,
            logger=logger
        )
        
        if not scd2_data:
            logger.info(f"No changes detected for canonical table {canonical_table}")
            return {
                'tenant_id': tenant_config.tenant_id,
                'canonical_table': canonical_table,
                'status': 'success',
                'record_count': 0,
                'message': 'No changes detected'
            }
        
        # Write to S3
        timestamp = get_timestamp()
        s3_key = get_s3_key(tenant_config.tenant_id, 'canonical', 'connectwise', canonical_table, timestamp)
        
        write_parquet_to_s3(
            config=config,
            s3_key=s3_key,
            data=scd2_data,
            logger=logger
        )
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        logger.log_data_processing(
            operation="canonical_transform",
            record_count=len(scd2_data),
            execution_time=execution_time
        )
        
        return {
            'tenant_id': tenant_config.tenant_id,
            'canonical_table': canonical_table,
            'status': 'success',
            'record_count': len(scd2_data),
            'execution_time': execution_time,
            's3_key': s3_key
        }
        
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"Failed to process canonical table {canonical_table}: {str(e)}", execution_time=execution_time)
        raise


def get_source_table_info(mapping: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """Extract source service and table information from mapping."""
    try:
        # Mapping format: {"connectwise": {"tickets": {"id": "ticketId", ...}}}
        for service, tables in mapping.items():
            for table_name in tables.keys():
                return service, table_name
        return None
    except Exception:
        return None


def read_raw_data_from_s3(
    config: Config,
    tenant_id: str,
    service: str,
    table_name: str,
    logger: PipelineLogger
) -> List[Dict[str, Any]]:
    """Read raw data from S3 Parquet files."""
    import pandas as pd
    
    try:
        prefix = f"{tenant_id}/raw/{service}/{table_name}/"
        
        response = s3.list_objects_v2(
            Bucket=config.bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            return []
        
        # Get recent files (last 24 hours worth)
        recent_files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[:50]
        
        all_data = []
        for obj in recent_files:
            try:
                obj_response = s3.get_object(Bucket=config.bucket_name, Key=obj['Key'])
                df = pd.read_parquet(obj_response['Body'])
                all_data.extend(df.to_dict('records'))
            except Exception as e:
                logger.warning(f"Failed to read file {obj['Key']}: {str(e)}")
                continue
        
        # Deduplicate by ID (keep latest)
        if all_data:
            df_all = pd.DataFrame(all_data)
            if 'id' in df_all.columns and '_info__lastUpdated' in df_all.columns:
                # Sort by lastUpdated and keep latest for each ID
                df_all = df_all.sort_values('_info__lastUpdated').drop_duplicates('id', keep='last')
                all_data = df_all.to_dict('records')
        
        logger.info(f"Read {len(all_data)} records from raw data")
        return all_data
        
    except Exception as e:
        logger.error(f"Failed to read raw data: {str(e)}")
        return []


def read_canonical_data_from_s3(
    config: Config,
    tenant_id: str,
    canonical_table: str,
    logger: PipelineLogger
) -> List[Dict[str, Any]]:
    """Read existing canonical data from S3."""
    import pandas as pd
    
    try:
        prefix = f"{tenant_id}/canonical/connectwise/{canonical_table}/"
        
        response = s3.list_objects_v2(
            Bucket=config.bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            return []
        
        # Get all files and read current records
        all_data = []
        for obj in response['Contents']:
            try:
                obj_response = s3.get_object(Bucket=config.bucket_name, Key=obj['Key'])
                df = pd.read_parquet(obj_response['Body'])
                all_data.extend(df.to_dict('records'))
            except Exception as e:
                logger.warning(f"Failed to read canonical file {obj['Key']}: {str(e)}")
                continue
        
        # Filter to only current records
        current_data = [record for record in all_data if record.get('is_current', False)]
        
        logger.info(f"Read {len(current_data)} current canonical records")
        return current_data
        
    except Exception as e:
        logger.error(f"Failed to read canonical data: {str(e)}")
        return []


def transform_data(raw_data: List[Dict[str, Any]], mapping: Dict[str, Any], logger: PipelineLogger) -> List[Dict[str, Any]]:
    """Transform raw data using the provided mapping."""
    try:
        # Get the field mapping (assumes single service/table in mapping)
        field_mapping = None
        for service, tables in mapping.items():
            for table_name, fields in tables.items():
                field_mapping = fields
                break
            if field_mapping:
                break
        
        if not field_mapping:
            raise ValueError("No field mapping found")
        
        transformed_data = []
        for record in raw_data:
            transformed_record = {}
            
            # Apply field mappings
            for canonical_field, source_field in field_mapping.items():
                value = safe_get(record, source_field)
                
                # Apply data type conversions
                if canonical_field.endswith('_date') or canonical_field.endswith('_time'):
                    value = normalize_datetime(value)
                
                transformed_record[canonical_field] = value
            
            # Add metadata fields
            transformed_record['source_system'] = 'connectwise'
            transformed_record['ingestion_timestamp'] = get_timestamp()
            
            transformed_data.append(transformed_record)
        
        logger.info(f"Transformed {len(transformed_data)} records")
        return transformed_data
        
    except Exception as e:
        logger.error(f"Failed to transform data: {str(e)}")
        raise


def apply_scd2_logic(
    new_data: List[Dict[str, Any]],
    existing_data: List[Dict[str, Any]],
    logger: PipelineLogger
) -> List[Dict[str, Any]]:
    """Apply SCD Type 2 logic to detect changes and create new versions."""
    try:
        current_time = get_timestamp()
        scd2_records = []
        
        # Create lookup for existing records by ID
        existing_lookup = {record['id']: record for record in existing_data if record.get('id')}
        
        for new_record in new_data:
            record_id = new_record.get('id')
            if not record_id:
                continue
            
            existing_record = existing_lookup.get(record_id)
            
            if not existing_record:
                # New record
                scd2_record = new_record.copy()
                scd2_record.update({
                    'effective_start': current_time,
                    'effective_end': None,
                    'is_current': True,
                    'change_type': 'INSERT'
                })
                scd2_records.append(scd2_record)
                
            else:
                # Check if record has changed
                if has_record_changed(new_record, existing_record):
                    # Close existing record
                    closed_record = existing_record.copy()
                    closed_record.update({
                        'effective_end': current_time,
                        'is_current': False,
                        'change_type': 'UPDATE'
                    })
                    scd2_records.append(closed_record)
                    
                    # Create new version
                    new_version = new_record.copy()
                    new_version.update({
                        'effective_start': current_time,
                        'effective_end': None,
                        'is_current': True,
                        'change_type': 'UPDATE'
                    })
                    scd2_records.append(new_version)
        
        logger.info(f"Applied SCD2 logic: {len(scd2_records)} records to write")
        return scd2_records
        
    except Exception as e:
        logger.error(f"Failed to apply SCD2 logic: {str(e)}")
        raise


def has_record_changed(new_record: Dict[str, Any], existing_record: Dict[str, Any]) -> bool:
    """Check if a record has changed by comparing key fields."""
    # Exclude metadata fields from comparison
    exclude_fields = {
        'effective_start', 'effective_end', 'is_current', 'change_type',
        'ingestion_timestamp', 'source_system'
    }
    
    for key, new_value in new_record.items():
        if key in exclude_fields:
            continue
        
        existing_value = existing_record.get(key)
        
        # Handle None values
        if new_value is None and existing_value is None:
            continue
        if new_value is None or existing_value is None:
            return True
        
        # Convert to string for comparison to handle type differences
        if str(new_value) != str(existing_value):
            return True
    
    return False


def write_parquet_to_s3(config: Config, s3_key: str, data: List[Dict[str, Any]], logger: PipelineLogger):
    """Write data to S3 as Parquet format."""
    import pandas as pd
    import io
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Write to Parquet in memory
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        
        # Upload to S3
        s3.put_object(
            Bucket=config.bucket_name,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        logger.log_s3_operation(
            operation="put_object",
            bucket=config.bucket_name,
            key=s3_key,
            size_bytes=len(buffer.getvalue())
        )
        
    except Exception as e:
        raise Exception(f"Failed to write Parquet to S3: {str(e)}")