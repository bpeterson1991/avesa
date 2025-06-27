"""
Canonical Transform Lambda Function

This function transforms raw data from various integration services into canonical format
with configurable SCD processing. Respects SCD type configuration from mapping files.

Features:
- Configurable SCD Type 1 or Type 2 processing based on table configuration
- Data validation and quality checks
- Schema evolution handling
- Multi-tenant support
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# Setup paths using shared utilities
from shared.path_utils import PathManager
PathManager.setup_lambda_paths()
PathManager.setup_src_path(__file__)

try:
    # Try direct imports (Lambda package has modules in root)
    from config_simple import Config
    from aws_clients import get_dynamodb_client, get_s3_client
    from logger import PipelineLogger
    from utils import get_timestamp, get_s3_key
except ImportError as e:
    # Fallback for shared directory imports (development environment)
    try:
        from shared.config_simple import Config
        from shared.aws_clients import get_dynamodb_client, get_s3_client
        from shared.logger import PipelineLogger
        from shared.utils import get_timestamp, get_s3_key
    except ImportError:
        # Final fallback with path manipulation
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
        from config_simple import Config
        from aws_clients import get_dynamodb_client, get_s3_client
        from logger import PipelineLogger
        from utils import get_timestamp, get_s3_key

# Initialize clients using shared factory
from shared import AWSClientFactory, CanonicalMapper
from shared.scd_config import SCDConfigManager, get_scd_type, is_scd_type_1, is_scd_type_2
from shared.canonical_schema import CanonicalSchemaManager
aws_factory = AWSClientFactory()
clients = aws_factory.get_client_bundle(['dynamodb', 's3'])
dynamodb = clients['dynamodb']
s3 = clients['s3']

# Initialize canonical mapper and SCD config manager
canonical_mapper = CanonicalMapper(s3_client=s3)
scd_config_manager = SCDConfigManager(s3_client=s3)


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
    
    # Phase 3 enhancement: Check for specific source files
    source_files = event.get('source_files')  # List of specific S3 keys
    aggregator_triggered = event.get('aggregator_triggered', False)
    processing_mode = 'file_specific' if source_files else 'time_based'
    
    # DIAGNOSTIC: Log the incoming event to understand triggering pattern
    logger.info(f"🔍 CANONICAL TRANSFORM DEBUG: Lambda triggered",
               event_keys=list(event.keys()),
               has_source_s3_key=bool(event.get('source_s3_key')),
               has_tenant_id=bool(event.get('tenant_id')),
               has_table_name=bool(event.get('table_name')),
               has_s3_trigger=bool(event.get('s3_trigger')),
               processing_mode=processing_mode,
               source_files_count=len(source_files) if source_files else 0,
               aggregator_triggered=aggregator_triggered,
               aws_request_id=context.aws_request_id)
    
    if event.get('source_s3_key'):
        logger.info(f"🔍 CANONICAL TRANSFORM DEBUG: S3-triggered processing",
                   source_s3_key=event['source_s3_key'],
                   service_name=event.get('service_name'),
                   table_name=event.get('table_name'))
    
    if source_files:
        logger.info(f"🔍 CANONICAL TRANSFORM DEBUG: File-specific processing mode",
                   source_files=source_files[:5],  # Log first 5 files
                   total_files=len(source_files))
    
    # Get environment configuration using proper pattern
    from shared.environment import Environment
    env_name = os.environ.get('ENVIRONMENT', 'dev')
    env_config = Environment.get_config(env_name)
    
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
                    logger=tenant_logger,
                    source_files=source_files
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
    logger: PipelineLogger,
    source_files: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Process canonical transformation for a single tenant."""
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting canonical transformation for tenant {tenant_id}, table {canonical_table}")
        
        # Find raw data files to process (Phase 3 enhancement: use specific files if provided)
        if source_files:
            # Use specific files provided by result aggregator
            raw_files = source_files
            logger.info(f"Using {len(raw_files)} specific source files provided by result aggregator")
        else:
            # Use traditional time-based file discovery
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
        
        # Check if these files have already been processed (idempotency)
        if check_if_files_already_processed(config, tenant_id, canonical_table, raw_files, logger):
            return {
                'tenant_id': tenant_id,
                'canonical_table': canonical_table,
                'status': 'already_processed',
                'record_count': 0,
                'message': 'All raw files already processed - skipping to prevent duplicates'
            }
        
        # Load and transform data
        transformed_records = []
        total_raw_records = 0
        
        for raw_file in raw_files:
            records = load_and_transform_raw_data(config, raw_file, canonical_table, logger)
            if records:
                # Count raw records before transformation
                raw_count = len(records)
                total_raw_records += raw_count
                
                # Filter out None records (failed transformations)
                valid_records = [r for r in records if r is not None]
                transformed_records.extend(valid_records)
                
                # EMERGENCY MONITORING: Log transformation success rate
                success_rate = len(valid_records) / raw_count if raw_count > 0 else 0
                logger.info(f"File {raw_file}: {raw_count} raw → {len(valid_records)} valid (success rate: {success_rate:.2%})")
                
                if success_rate < 0.5:  # Less than 50% success rate
                    logger.error(f"CRITICAL: Low transformation success rate {success_rate:.2%} for {raw_file}")

        # EMERGENCY CIRCUIT BREAKER: Prevent writing if transformation success rate is too low
        if total_raw_records > 0:
            overall_success_rate = len(transformed_records) / total_raw_records
            logger.info(f"Overall transformation: {total_raw_records} raw → {len(transformed_records)} valid (success rate: {overall_success_rate:.2%})")
            
            if overall_success_rate < 0.1:  # Less than 10% success rate
                logger.error(f"EMERGENCY HALT: Transformation success rate {overall_success_rate:.2%} is critically low")
                logger.error(f"This indicates field mapping issues causing metadata-only records")
                return {
                    'tenant_id': tenant_id,
                    'canonical_table': canonical_table,
                    'status': 'emergency_halt',
                    'record_count': 0,
                    'message': f'Emergency halt: transformation success rate {overall_success_rate:.2%} too low',
                    'raw_record_count': total_raw_records,
                    'valid_record_count': len(transformed_records)
                }

        if not transformed_records:
            logger.info(f"No valid records to transform after processing {len(raw_files)} files")
            return {
                'tenant_id': tenant_id,
                'canonical_table': canonical_table,
                'status': 'success',
                'record_count': 0,
                'message': 'No valid records to transform',
                'raw_record_count': total_raw_records
            }
        
        # Apply SCD logic based on table configuration
        bucket_name = config.bucket_name if hasattr(config, 'bucket_name') else os.environ.get('BUCKET_NAME')
        scd_type = get_scd_type(canonical_table, bucket_name)
        logger.info(f"Table {canonical_table} uses SCD {scd_type} processing")
        
        if scd_type == 'type_1':
            scd_records = apply_scd_type1_logic(config, tenant_id, canonical_table, transformed_records, logger)
        else:
            scd_records = apply_scd_type2_logic(config, tenant_id, canonical_table, transformed_records, logger)
        
        # Write canonical data to S3
        timestamp = get_timestamp()
        s3_key = f"{tenant_id}/canonical/{canonical_table}/{tenant_id}-{timestamp}.parquet"
        
        write_canonical_data_to_s3(config, s3_key, scd_records, logger)
        
        # Update processing timestamp for idempotency
        update_processing_timestamp(config, tenant_id, canonical_table, logger)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        logger.log_data_processing(
            operation="canonical_transform",
            record_count=len(scd_records),
            execution_time=execution_time
        )
        
        # Trigger ClickHouse data loader for seamless pipeline
        try:
            trigger_clickhouse_loader(tenant_id, canonical_table, logger)
        except Exception as e:
            logger.warning(f"Failed to trigger ClickHouse loader: {e}")
            # Don't fail the canonical transformation if ClickHouse trigger fails
        
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


def check_if_files_already_processed(config: Config, tenant_id: str, canonical_table: str, raw_files: List[str], logger: PipelineLogger) -> bool:
    """Check if the current batch of raw files has already been processed."""
    if not raw_files:
        logger.info("🔍 IDEMPOTENCY: No raw files provided, returning True")
        return True
    
    try:
        # Get the last processed timestamp for this tenant/table
        logger.info(f"🔍 IDEMPOTENCY: Checking last processed timestamp for {tenant_id}/canonical_{canonical_table}")
        
        response = dynamodb.get_item(
            TableName=config.last_updated_table,
            Key={
                'tenant_id': {'S': tenant_id},
                'table_name': {'S': f"canonical_{canonical_table}"}
            }
        )
        
        if 'Item' not in response:
            logger.info("🔍 IDEMPOTENCY: No previous processing record found - proceeding with processing")
            return False  # Never processed before
        
        last_processed = response['Item'].get('last_updated', {}).get('S')
        if not last_processed:
            logger.info("🔍 IDEMPOTENCY: No last_updated timestamp found - proceeding with processing")
            return False
        
        # Parse last processed timestamp
        last_processed_dt = datetime.fromisoformat(last_processed.replace('Z', '+00:00'))
        logger.info(f"🔍 IDEMPOTENCY: Last processed at: {last_processed_dt}")
        
        # Check if any raw files are newer than last processed timestamp
        newer_files_found = 0
        for file_key in raw_files:
            try:
                # Get file metadata
                file_response = s3.head_object(Bucket=config.bucket_name, Key=file_key)
                file_modified = file_response['LastModified']
                
                logger.debug(f"🔍 IDEMPOTENCY: File {file_key} modified at {file_modified}")
                
                if file_modified > last_processed_dt:
                    logger.info(f"🔍 IDEMPOTENCY: Found newer raw file: {file_key} modified {file_modified} (newer than {last_processed_dt})")
                    newer_files_found += 1
            except Exception as e:
                logger.warning(f"🔍 IDEMPOTENCY: Could not check file {file_key}: {e} - assuming needs processing")
                return False  # Assume not processed on error
        
        if newer_files_found > 0:
            logger.info(f"🔍 IDEMPOTENCY: Found {newer_files_found} newer files - proceeding with processing")
            return False  # Found newer data
        
        logger.info(f"🔍 IDEMPOTENCY: All {len(raw_files)} raw files for {tenant_id}/{canonical_table} already processed")
        return True  # All files already processed
        
    except Exception as e:
        logger.error(f"🔍 IDEMPOTENCY: Error checking processing status: {e} - assuming needs processing")
        return False  # Assume not processed on error

def update_processing_timestamp(config: Config, tenant_id: str, canonical_table: str, logger: PipelineLogger):
    """Update the last processed timestamp after successful transformation."""
    try:
        current_timestamp = datetime.now(timezone.utc).isoformat()
        
        dynamodb.put_item(
            TableName=config.last_updated_table,
            Item={
                'tenant_id': {'S': tenant_id},
                'table_name': {'S': f"canonical_{canonical_table}"},
                'last_updated': {'S': current_timestamp},
                'updated_at': {'S': current_timestamp}
            }
        )
        
        logger.info(f"Updated processing timestamp for {tenant_id}/canonical_{canonical_table}: {current_timestamp}")
        
    except Exception as e:
        logger.error(f"Failed to update processing timestamp: {e}")

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
        
        # Return recent files (last 72 hours worth for production stability)
        recent_files = []
        cutoff_time = datetime.now(timezone.utc).timestamp() - (72 * 60 * 60)  # 72 hours ago
        
        for obj in response['Contents']:
            if obj['LastModified'].timestamp() > cutoff_time:
                recent_files.append(obj['Key'])
        
        logger.info(f"Found {len(recent_files)} raw data files to process")
        return recent_files
        
    except Exception as e:
        logger.error(f"Failed to find raw data files: {str(e)}")
        return []


def get_source_mapping(canonical_table: str) -> Optional[Dict[str, str]]:
    """Get source service and table mapping for canonical table using shared CanonicalMapper."""
    return canonical_mapper.get_source_mapping(canonical_table)


def load_and_transform_raw_data(config: Config, s3_key: str, canonical_table: str, logger: PipelineLogger) -> List[Dict[str, Any]]:
    """Load raw data and transform to canonical format."""
    import pandas as pd
    import json
    import time
    import io
    
    try:
        # Read raw data from S3 with enhanced retry logic
        df = None
        max_retries = 3
        base_delay = 2
        
        for attempt in range(max_retries + 1):
            try:
                # Get fresh S3 response object for each attempt
                response = s3.get_object(Bucket=config.bucket_name, Key=s3_key)
                
                # Read body into memory first to avoid stream positioning issues
                body_data = response['Body'].read()
                body_stream = io.BytesIO(body_data)
                
                # Read parquet from memory stream
                df = pd.read_parquet(body_stream)
                
                if attempt > 0:
                    logger.info(f"Successfully read parquet file on retry attempt {attempt + 1}")
                break
                
            except Exception as parquet_error:
                error_msg = str(parquet_error)
                
                # If this is our last attempt, raise the error
                if attempt >= max_retries:
                    logger.error(f"Failed to read parquet file after {max_retries + 1} attempts: {error_msg}")
                    raise parquet_error
                
                # Log retry attempt
                logger.warning(f"S3 read attempt {attempt + 1} failed: {error_msg} - retrying")
                
                # Exponential backoff delay
                delay = base_delay * (2 ** attempt)
                time.sleep(delay)
        
        if df is None:
            raise Exception("Failed to read parquet file after all retry attempts")
        
        if df.empty:
            return []
        
        # Load canonical mapping
        mapping = load_canonical_mapping(canonical_table)
        
        # Transform data according to mapping
        transformed_records = []
        # Extract tenant_id from s3_key path
        tenant_id = s3_key.split('/')[0] if '/' in s3_key else None
        
        for _, row in df.iterrows():
            transformed_record = transform_record(row.to_dict(), mapping, canonical_table, tenant_id, logger)
            if transformed_record:
                transformed_records.append(transformed_record)
        
        logger.info(f"Transformed {len(transformed_records)} records from {s3_key}")
        return transformed_records
        
    except Exception as e:
        logger.error(f"Failed to load and transform data from {s3_key}: {str(e)}")
        return []


# Canonical mapping and transformation functions now use shared CanonicalMapper
def load_canonical_mapping(canonical_table: str) -> Dict[str, Any]:
    """Load canonical mapping configuration using shared CanonicalMapper."""
    bucket_name = os.environ.get('BUCKET_NAME')
    return canonical_mapper.load_mapping(canonical_table, bucket=bucket_name)


def transform_record(raw_record: Dict[str, Any], mapping: Dict[str, Any], canonical_table: str, tenant_id: str = None, logger: PipelineLogger = None) -> Optional[Dict[str, Any]]:
    """Transform a single record to canonical format using shared CanonicalMapper with emergency validation."""
    transformed_record = canonical_mapper.transform_record(raw_record, mapping, canonical_table)
    
    # Add tenant_id to the transformed record if provided
    if transformed_record and tenant_id:
        transformed_record['tenant_id'] = tenant_id
    
    if transformed_record:
        # Validate that business fields are present (not just metadata)
        business_fields = get_required_business_fields(canonical_table)
        missing_fields = []
        
        for field in business_fields:
            if field not in transformed_record or transformed_record[field] is None:
                missing_fields.append(field)
        
        if missing_fields:
            if logger:
                logger.error(f"EMERGENCY HALT: Missing critical business fields {missing_fields} in {canonical_table}")
                logger.error(f"Raw record keys: {list(raw_record.keys())}")
                logger.error(f"Transformed record keys: {list(transformed_record.keys())}")
                logger.error(f"Mapping fields: {mapping.get('fields', {})}")
            
            # CIRCUIT BREAKER: Return None to prevent writing metadata-only records
            return None
    
    return transformed_record


def get_required_business_fields(canonical_table: str) -> List[str]:
    """Get list of required business fields that must be present to prevent metadata-only records."""
    required_fields = {
        'companies': ['id', 'company_name'],  # FIXED: Match actual canonical mapping output field name
        'contacts': ['id', 'first_name', 'last_name'],  # Must have ID and name fields
        'tickets': ['id', 'summary'],  # Must have ID and summary
        'time_entries': ['id', 'actual_hours']  # FIXED: Match corrected field name
    }
    return required_fields.get(canonical_table, ['id'])


def calculate_record_hash(record: Dict[str, Any]) -> str:
    """Calculate hash for data quality and change detection."""
    import hashlib
    import json
    
    # Exclude SCD fields from hash calculation
    scd_fields = {
        'effective_start_date', 'effective_end_date', 'is_current',
        'record_hash', 'ingestion_timestamp', 'temp_record_hash'
    }
    
    # Create a copy without SCD fields
    hash_data = {k: v for k, v in record.items() if k not in scd_fields}
    
    # Sort keys for consistent hashing
    sorted_data = json.dumps(hash_data, sort_keys=True, default=str)
    return hashlib.md5(sorted_data.encode()).hexdigest()


def apply_scd_type1_logic(config: Config, tenant_id: str, canonical_table: str, new_records: List[Dict[str, Any]], logger: PipelineLogger) -> List[Dict[str, Any]]:
    """Apply SCD Type 1 logic - simple current data only with deduplication. NO SCD Type 2 fields for Type 1 tables."""
    try:
        import pandas as pd
        from datetime import datetime, timezone
        
        logger.info(f"Applying SCD Type 1 logic to {len(new_records)} records")
        
        if not new_records:
            return []
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(new_records)
        
        # Get the correct ID field for this table
        id_field_mapping = {
            'companies': 'company_id',
            'contacts': 'contact_id',
            'tickets': 'ticket_id',
            'time_entries': 'entry_id'
        }
        id_field = id_field_mapping.get(canonical_table)
        
        if not id_field or id_field not in df.columns:
            # Fallback to 'id' if specific field not found
            if 'id' in df.columns:
                id_field = 'id'
                logger.warning(f"Using fallback ID field 'id' for table {canonical_table}")
            else:
                logger.error(f"No valid ID field found for table {canonical_table}")
                return new_records
        
        # Ensure last_updated is datetime (using correct canonical field name)
        if 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated'])
        else:
            # Check for created_date as fallback (proper data source fallback)
            if 'created_date' in df.columns:
                logger.warning(f"last_updated missing, using created_date as fallback for {canonical_table}")
                df['last_updated'] = pd.to_datetime(df['created_date'])
            else:
                # This indicates a serious mapping configuration issue
                logger.error(f"CRITICAL MAPPING ERROR: Neither last_updated nor created_date found in {canonical_table}")
                logger.error(f"Available columns: {list(df.columns)}")
                logger.error(f"This indicates the canonical mapping is not correctly configured")
                raise ValueError(f"Missing timestamp fields in {canonical_table}: mapping must include last_updated or created_date")
        
        # For SCD Type 1, keep only the latest record per ID
        logger.info(f"Deduplicating records by {id_field} for SCD Type 1")
        
        # Sort by ID and last_updated, then keep the last (most recent) record per ID
        df_sorted = df.sort_values([id_field, 'last_updated'])
        df_deduplicated = df_sorted.groupby(id_field).tail(1).reset_index(drop=True)
        
        logger.info(f"Deduplicated from {len(df)} to {len(df_deduplicated)} records for SCD Type 1")
        
        # Apply SCD Type 1 fields to the deduplicated records
        scd_records = []
        for _, row in df_deduplicated.iterrows():
            record = row.to_dict()
            
            # Use shared schema manager for consistent metadata fields
            metadata_fields = CanonicalSchemaManager.get_standard_metadata_fields('type_1')
            
            # Remove any SCD Type 2 fields that might have been added
            scd_type2_fields = ['effective_start_date', 'effective_end_date', 'is_current']
            for field in scd_type2_fields:
                record.pop(field, None)
            
            # Add standard metadata fields for SCD Type 1
            record['record_hash'] = calculate_record_hash(record)
            record['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
            
            scd_records.append(record)
        
        logger.info(f"SCD Type 1 processing complete: {len(scd_records)} current records (NO SCD Type 2 fields)")
        
        return scd_records
        
    except Exception as e:
        logger.error(f"Failed to apply SCD Type 1 logic: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return original records on error to avoid data loss
        return new_records


def apply_scd_type2_logic(config: Config, tenant_id: str, canonical_table: str, new_records: List[Dict[str, Any]], logger: PipelineLogger) -> List[Dict[str, Any]]:
    """Apply proper SCD Type 2 logic to ensure only latest records per ID are current."""
    try:
        import pandas as pd
        from datetime import datetime, timezone
        
        logger.info(f"Applying enhanced SCD Type 2 logic to {len(new_records)} records")
        
        if not new_records:
            return []
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(new_records)
        
        # Get the correct ID field for this table
        id_field_mapping = {
            'companies': 'company_id',
            'contacts': 'contact_id',
            'tickets': 'ticket_id',
            'time_entries': 'entry_id'
        }
        id_field = id_field_mapping.get(canonical_table)
        
        if not id_field or id_field not in df.columns:
            # Fallback to 'id' if specific field not found
            if 'id' in df.columns:
                id_field = 'id'
                logger.warning(f"Using fallback ID field 'id' for table {canonical_table}")
            else:
                logger.error(f"No valid ID field found for table {canonical_table}")
                return new_records
        
        # Ensure last_updated is datetime (using correct canonical field name)
        if 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated'])
        else:
            # Check for created_date as fallback (proper data source fallback)
            if 'created_date' in df.columns:
                logger.warning(f"last_updated missing, using created_date as fallback for {canonical_table}")
                df['last_updated'] = pd.to_datetime(df['created_date'])
            else:
                # This indicates a serious mapping configuration issue
                logger.error(f"CRITICAL MAPPING ERROR: Neither last_updated nor created_date found in {canonical_table}")
                logger.error(f"Available columns: {list(df.columns)}")
                logger.error(f"This indicates the canonical mapping is not correctly configured")
                raise ValueError(f"Missing timestamp fields in {canonical_table}: mapping must include last_updated or created_date")
        
        # Step 1: Remove exact duplicates first (same ID and same data hash)
        logger.info(f"Removing exact duplicates based on ID and record content")
        initial_count = len(df)
        
        # Calculate record hash for each row for duplicate detection
        df['temp_record_hash'] = df.apply(lambda row: calculate_record_hash(row.to_dict()), axis=1)
        
        # Remove exact duplicates (same ID and same hash)
        df_no_exact_dupes = df.drop_duplicates(subset=[id_field, 'temp_record_hash'], keep='last')
        exact_dupes_removed = initial_count - len(df_no_exact_dupes)
        
        if exact_dupes_removed > 0:
            logger.info(f"Removed {exact_dupes_removed} exact duplicate records")
        
        # Step 2: For each ID, keep only the record with the latest last_updated
        # This ensures we don't have multiple "current" records for the same ID
        logger.info(f"Deduplicating records by {id_field} and last_updated")
        
        # Sort by ID and last_updated, then keep the last (most recent) record per ID
        df_sorted = df_no_exact_dupes.sort_values([id_field, 'last_updated'])
        df_deduplicated = df_sorted.groupby(id_field).tail(1).reset_index(drop=True)
        
        logger.info(f"Deduplicated from {len(df)} to {len(df_deduplicated)} records")
        
        # Step 3: Apply SCD Type 2 fields to the deduplicated records
        scd_records = []
        for _, row in df_deduplicated.iterrows():
            record = row.to_dict()
            
            # Remove temporary hash field
            record.pop('temp_record_hash', None)
            
            # Use shared schema manager for consistent metadata fields
            metadata_fields = CanonicalSchemaManager.get_standard_metadata_fields('type_2')
            
            # Add SCD Type 2 metadata fields
            record['effective_start_date'] = record['last_updated']
            record['effective_end_date'] = None
            record['is_current'] = True  # Only the latest version per ID is current
            record['record_hash'] = calculate_record_hash(record)
            
            scd_records.append(record)
        
        logger.info(f"Enhanced SCD Type 2 processing complete: {len(scd_records)} unique current records")
        
        # Step 4: Final verification - ensure no duplicate IDs
        ids = [record[id_field] for record in scd_records]
        unique_ids = set(ids)
        if len(ids) != len(unique_ids):
            logger.error(f"CRITICAL: Found duplicate IDs after deduplication: {len(ids)} records, {len(unique_ids)} unique IDs")
            # Log the duplicate IDs for debugging
            id_counts = {}
            for record_id in ids:
                id_counts[record_id] = id_counts.get(record_id, 0) + 1
            duplicate_ids = [record_id for record_id, count in id_counts.items() if count > 1]
            logger.error(f"Duplicate IDs: {duplicate_ids}")
        else:
            logger.info(f"✅ Verified: All {len(unique_ids)} records have unique IDs")
        
        return scd_records
        
    except Exception as e:
        logger.error(f"Failed to apply SCD Type 2 logic: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return original records on error to avoid data loss
        return new_records


def load_existing_canonical_data(config: Config, tenant_id: str, canonical_table: str, logger: PipelineLogger) -> List[Dict[str, Any]]:
    """Load existing canonical data from S3 for SCD comparison."""
    try:
        import pandas as pd
        
        # Look for existing canonical files
        prefix = f"{tenant_id}/canonical/{canonical_table}/"
        
        response = s3.list_objects_v2(
            Bucket=config.bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            logger.info("No existing canonical data found")
            return []
        
        # Load the most recent canonical file
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        if not files:
            return []
        
        most_recent_file = files[0]['Key']
        logger.info(f"Loading existing canonical data from: {most_recent_file}")
        
        # Read the parquet file
        file_response = s3.get_object(Bucket=config.bucket_name, Key=most_recent_file)
        df = pd.read_parquet(file_response['Body'])
        
        # Convert to list of dictionaries
        existing_records = df.to_dict('records')
        logger.info(f"Loaded {len(existing_records)} existing canonical records")
        
        return existing_records
        
    except Exception as e:
        logger.warning(f"Could not load existing canonical data: {str(e)}")
        return []


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


def trigger_clickhouse_loader(tenant_id: str, canonical_table: str, logger: PipelineLogger):
    """Trigger ClickHouse data loader for seamless pipeline orchestration."""
    import boto3
    import json
    
    try:
        # Get Lambda client
        lambda_client = boto3.client('lambda')
        
        # Construct ClickHouse loader function name
        env_name = os.environ.get('ENVIRONMENT', 'dev')
        function_name = f"clickhouse-loader-{canonical_table}-{env_name}"
        
        # Prepare payload
        payload = {
            'tenant_id': tenant_id,
            'table_name': canonical_table
        }
        
        logger.info(f"Triggering ClickHouse loader: {function_name}")
        
        # Invoke ClickHouse loader asynchronously
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Asynchronous invocation
            Payload=json.dumps(payload)
        )
        
        if response['StatusCode'] == 202:
            logger.info(f"Successfully triggered ClickHouse loader for {tenant_id}/{canonical_table}")
        else:
            logger.warning(f"ClickHouse loader trigger returned status: {response['StatusCode']}")
            
    except Exception as e:
        logger.error(f"Failed to trigger ClickHouse loader: {str(e)}")
