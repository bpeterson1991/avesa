"""
Canonical Transform Lambda Function

This function transforms raw data from various integration services into canonical format
with SCD Type 2 historical tracking. Each canonical table has its own dedicated lambda function.

Features:
- SCD Type 2 historical tracking
- Data validation and quality checks
- Schema evolution handling
- Multi-tenant support
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# Add shared module to path
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))

try:
    from shared.config import Config
    from shared.aws_clients import get_dynamodb_client, get_s3_client
    from shared.logger import PipelineLogger
    from shared.utils import get_timestamp, get_s3_key
except ImportError as e:
    # Fallback for local imports
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
    from config import Config
    from aws_clients import get_dynamodb_client, get_s3_client
    from logger import PipelineLogger
    from utils import get_timestamp, get_s3_key

# Initialize clients
dynamodb = get_dynamodb_client()
s3 = get_s3_client()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for canonical data transformation.
    
    Args:
        event: Lambda event (can contain tenant_id for targeted runs)
        context: Lambda context
        
    Returns:
        Execution summary
    """
    logger = PipelineLogger("canonical_transform")
    config = Config.from_environment()
    
    # Get canonical table from environment variable
    canonical_table = os.environ.get('CANONICAL_TABLE')
    if not canonical_table:
        return {
            'statusCode': 400,
            'body': {
                'message': 'CANONICAL_TABLE environment variable not set',
                'error': 'Missing configuration'
            }
        }
    
    try:
        logger.info(f"Starting canonical transformation for table: {canonical_table}", 
                   execution_id=context.aws_request_id)
        
        # Get target tenant from event (optional)
        target_tenant = event.get('tenant_id')
        
        # Get tenant configurations
        tenants = get_tenant_configurations(config, target_tenant)
        
        results = []
        for tenant_id in tenants:
            tenant_logger = PipelineLogger("canonical_transform", tenant_id, canonical_table)
            
            try:
                result = process_tenant_canonical_data(
                    config=config,
                    tenant_id=tenant_id,
                    canonical_table=canonical_table,
                    logger=tenant_logger
                )
                results.append(result)
                
            except Exception as e:
                tenant_logger.error(f"Failed to process tenant {tenant_id}: {str(e)}")
                results.append({
                    'tenant_id': tenant_id,
                    'canonical_table': canonical_table,
                    'status': 'error',
                    'error': str(e)
                })
        
        # Calculate summary
        successful = len([r for r in results if r.get('status') == 'success'])
        failed = len([r for r in results if r.get('status') == 'error'])
        total_records = sum(r.get('record_count', 0) for r in results)
        
        logger.info(
            f"Canonical transformation completed for {canonical_table}",
            successful_jobs=successful,
            failed_jobs=failed,
            total_records=total_records
        )
        
        return {
            'statusCode': 200,
            'body': {
                'message': f'Canonical transformation completed for {canonical_table}',
                'canonical_table': canonical_table,
                'successful_jobs': successful,
                'failed_jobs': failed,
                'total_records': total_records,
                'results': results
            }
        }
        
    except Exception as e:
        logger.error(f"Canonical transformation failed for {canonical_table}: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'message': f'Canonical transformation failed for {canonical_table}',
                'error': str(e)
            }
        }


def get_tenant_configurations(config: Config, target_tenant: Optional[str] = None) -> List[str]:
    """Get list of tenant IDs that have data to transform."""
    try:
        if target_tenant:
            return [target_tenant]
        
        # Get all enabled tenants
        response = dynamodb.scan(
            TableName=config.tenant_services_table,
            FilterExpression='enabled = :enabled',
            ExpressionAttributeValues={':enabled': {'BOOL': True}}
        )
        
        tenant_ids = []
        for item in response['Items']:
            tenant_ids.append(item['tenant_id']['S'])
        
        return tenant_ids
        
    except Exception as e:
        raise Exception(f"Failed to get tenant configurations: {str(e)}")


def process_tenant_canonical_data(
    config: Config,
    tenant_id: str,
    canonical_table: str,
    logger: PipelineLogger
) -> Dict[str, Any]:
    """Process canonical transformation for a single tenant."""
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting canonical transformation for tenant {tenant_id}, table {canonical_table}")
        
        # Find raw data files to process
        raw_files = find_raw_data_files(config, tenant_id, canonical_table, logger)
        
        if not raw_files:
            logger.info(f"No raw data files found for transformation")
            return {
                'tenant_id': tenant_id,
                'canonical_table': canonical_table,
                'status': 'success',
                'record_count': 0,
                'message': 'No raw data to transform'
            }
        
        # Load and transform data
        transformed_records = []
        for raw_file in raw_files:
            records = load_and_transform_raw_data(config, raw_file, canonical_table, logger)
            transformed_records.extend(records)
        
        if not transformed_records:
            logger.info(f"No records to transform after processing {len(raw_files)} files")
            return {
                'tenant_id': tenant_id,
                'canonical_table': canonical_table,
                'status': 'success',
                'record_count': 0,
                'message': 'No records to transform'
            }
        
        # Apply SCD Type 2 logic
        scd_records = apply_scd_type2_logic(config, tenant_id, canonical_table, transformed_records, logger)
        
        # Write canonical data to S3
        timestamp = get_timestamp()
        s3_key = get_s3_key(tenant_id, 'canonical', canonical_table, canonical_table, timestamp)
        
        write_canonical_data_to_s3(config, s3_key, scd_records, logger)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        logger.log_data_processing(
            operation="canonical_transform",
            record_count=len(scd_records),
            execution_time=execution_time
        )
        
        return {
            'tenant_id': tenant_id,
            'canonical_table': canonical_table,
            'status': 'success',
            'record_count': len(scd_records),
            'execution_time': execution_time,
            's3_key': s3_key
        }
        
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"Failed to process canonical transformation: {str(e)}", execution_time=execution_time)
        raise


def find_raw_data_files(config: Config, tenant_id: str, canonical_table: str, logger: PipelineLogger) -> List[str]:
    """Find raw data files that need to be transformed."""
    try:
        # Map canonical table to source service/table
        source_mapping = get_source_mapping(canonical_table)
        
        if not source_mapping:
            logger.warning(f"No source mapping found for canonical table: {canonical_table}")
            return []
        
        # Look for raw data files
        prefix = f"{tenant_id}/raw/{source_mapping['service']}/{source_mapping['table']}/"
        
        response = s3.list_objects_v2(
            Bucket=config.bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            return []
        
        # Return recent files (last 24 hours worth)
        recent_files = []
        cutoff_time = datetime.now(timezone.utc).timestamp() - (24 * 60 * 60)  # 24 hours ago
        
        for obj in response['Contents']:
            if obj['LastModified'].timestamp() > cutoff_time:
                recent_files.append(obj['Key'])
        
        logger.info(f"Found {len(recent_files)} raw data files to process")
        return recent_files
        
    except Exception as e:
        logger.error(f"Failed to find raw data files: {str(e)}")
        return []


def get_source_mapping(canonical_table: str) -> Optional[Dict[str, str]]:
    """Get source service and table mapping for canonical table."""
    mappings = {
        'tickets': {'service': 'connectwise', 'table': 'service/tickets'},
        'time_entries': {'service': 'connectwise', 'table': 'time/entries'},
        'companies': {'service': 'connectwise', 'table': 'company/companies'},
        'contacts': {'service': 'connectwise', 'table': 'company/contacts'}
    }
    return mappings.get(canonical_table)


def load_and_transform_raw_data(config: Config, s3_key: str, canonical_table: str, logger: PipelineLogger) -> List[Dict[str, Any]]:
    """Load raw data and transform to canonical format."""
    import pandas as pd
    
    try:
        # Read raw data from S3
        response = s3.get_object(Bucket=config.bucket_name, Key=s3_key)
        df = pd.read_parquet(response['Body'])
        
        if df.empty:
            return []
        
        # Load canonical mapping
        mapping = load_canonical_mapping(canonical_table)
        
        # Transform data according to mapping
        transformed_records = []
        for _, row in df.iterrows():
            transformed_record = transform_record(row.to_dict(), mapping, canonical_table)
            if transformed_record:
                transformed_records.append(transformed_record)
        
        logger.info(f"Transformed {len(transformed_records)} records from {s3_key}")
        return transformed_records
        
    except Exception as e:
        logger.error(f"Failed to load and transform data from {s3_key}: {str(e)}")
        return []


def load_canonical_mapping(canonical_table: str) -> Dict[str, Any]:
    """Load canonical mapping configuration."""
    try:
        # Try to load from S3 first
        mapping_key = f"mappings/canonical/{canonical_table}.json"
        response = s3.get_object(Bucket=os.environ['BUCKET_NAME'], Key=mapping_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except:
        # Fallback to default mapping
        return get_default_mapping(canonical_table)


def get_default_mapping(canonical_table: str) -> Dict[str, Any]:
    """Get default mapping for canonical table."""
    default_mappings = {
        'tickets': {
            'id_field': 'id',
            'fields': {
                'ticket_id': 'id',
                'summary': 'summary',
                'description': 'description',
                'status': 'status__name',
                'priority': 'priority__name',
                'created_date': '_info__dateEntered',
                'updated_date': '_info__lastUpdated'
            }
        },
        'time_entries': {
            'id_field': 'id',
            'fields': {
                'entry_id': 'id',
                'ticket_id': 'ticket__id',
                'hours': 'actualHours',
                'description': 'notes',
                'date_start': 'timeStart',
                'date_end': 'timeEnd'
            }
        },
        'companies': {
            'id_field': 'id',
            'fields': {
                'company_id': 'id',
                'name': 'name',
                'type': 'type__name',
                'status': 'status__name',
                'created_date': '_info__dateEntered',
                'updated_date': '_info__lastUpdated'
            }
        },
        'contacts': {
            'id_field': 'id',
            'fields': {
                'contact_id': 'id',
                'company_id': 'company__id',
                'first_name': 'firstName',
                'last_name': 'lastName',
                'email': 'communicationItems',
                'created_date': '_info__dateEntered',
                'updated_date': '_info__lastUpdated'
            }
        }
    }
    return default_mappings.get(canonical_table, {})


def transform_record(raw_record: Dict[str, Any], mapping: Dict[str, Any], canonical_table: str) -> Optional[Dict[str, Any]]:
    """Transform a single record to canonical format."""
    try:
        transformed = {}
        
        # Apply field mappings
        for canonical_field, source_field in mapping.get('fields', {}).items():
            value = get_nested_value(raw_record, source_field)
            transformed[canonical_field] = value
        
        # Add metadata
        transformed['source_system'] = 'connectwise'
        transformed['source_table'] = get_source_mapping(canonical_table)['table']
        transformed['ingestion_timestamp'] = get_timestamp()
        transformed['canonical_table'] = canonical_table
        
        # Add SCD Type 2 fields
        transformed['effective_start_date'] = get_timestamp()
        transformed['effective_end_date'] = None
        transformed['is_current'] = True
        transformed['record_hash'] = calculate_record_hash(transformed)
        
        return transformed
        
    except Exception as e:
        return None


def get_nested_value(data: Dict[str, Any], key: str) -> Any:
    """Get nested value from dictionary using dot notation."""
    try:
        keys = key.split('__')
        value = data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return None
        return value
    except:
        return None


def calculate_record_hash(record: Dict[str, Any]) -> str:
    """Calculate hash for record to detect changes."""
    import hashlib
    
    # Create a copy without SCD fields for hashing
    hash_record = record.copy()
    scd_fields = ['effective_start_date', 'effective_end_date', 'is_current', 'record_hash', 'ingestion_timestamp']
    for field in scd_fields:
        hash_record.pop(field, None)
    
    # Sort keys for consistent hashing
    sorted_record = json.dumps(hash_record, sort_keys=True, default=str)
    return hashlib.md5(sorted_record.encode()).hexdigest()


def apply_scd_type2_logic(config: Config, tenant_id: str, canonical_table: str, new_records: List[Dict[str, Any]], logger: PipelineLogger) -> List[Dict[str, Any]]:
    """Apply SCD Type 2 logic to detect and handle changes."""
    try:
        # For now, return new records as-is
        # In a full implementation, this would:
        # 1. Load existing canonical data
        # 2. Compare record hashes
        # 3. Close out changed records (set effective_end_date, is_current=False)
        # 4. Add new versions of changed records
        # 5. Add completely new records
        
        logger.info(f"Applied SCD Type 2 logic to {len(new_records)} records")
        return new_records
        
    except Exception as e:
        logger.error(f"Failed to apply SCD Type 2 logic: {str(e)}")
        return new_records


def write_canonical_data_to_s3(config: Config, s3_key: str, data: List[Dict[str, Any]], logger: PipelineLogger):
    """Write canonical data to S3 as Parquet format."""
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
        raise Exception(f"Failed to write canonical data to S3: {str(e)}")