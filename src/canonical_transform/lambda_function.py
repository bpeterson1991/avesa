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
import gc
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
from shared.canonical_schema import CanonicalSchemaManager
aws_factory = AWSClientFactory()
clients = aws_factory.get_client_bundle(['dynamodb', 's3'])
dynamodb = clients['dynamodb']
s3 = clients['s3']

# MEMORY OPTIMIZATION: Move canonical mapper and SCD config manager to function scope
# This prevents memory persistence across Lambda invocations


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
    
    # MEMORY OPTIMIZATION: Create objects at function scope to prevent persistence
    # This ensures clean memory state for each Lambda invocation
    canonical_mapper = CanonicalMapper(s3_client=s3)
    
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
                    source_files=source_files,
                    canonical_mapper=canonical_mapper
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


def check_memory_usage(logger: PipelineLogger, max_memory_mb: int = 200, context: str = "") -> bool:
    """
    Check current memory usage and trigger cleanup if needed.
    
    Args:
        logger: Logger instance
        max_memory_mb: Maximum memory usage in MB before triggering cleanup
        context: Description of current operation for better diagnostics
        
    Returns:
        True if memory cleanup was triggered, False otherwise
    """
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        virtual_mb = memory_info.vms / 1024 / 1024
        
        # Get Lambda specific memory limit (from environment)
        lambda_memory_limit = int(os.environ.get('AWS_LAMBDA_FUNCTION_MEMORY_SIZE', '1024'))
        memory_percentage = (memory_mb / lambda_memory_limit) * 100
        
        logger.info(f"Memory usage{' - ' + context if context else ''}: {memory_mb:.1f}MB / {lambda_memory_limit}MB ({memory_percentage:.1f}%), Virtual: {virtual_mb:.1f}MB")
        
        # If we're using more than 80% of Lambda memory, it's critical
        if memory_percentage > 80:
            logger.error(f"CRITICAL: Memory usage at {memory_percentage:.1f}% of Lambda limit!")
        
        if memory_mb > max_memory_mb:
            logger.warning(f"High memory usage: {memory_mb:.1f}MB, triggering cleanup")
            # Force garbage collection
            gc.collect(2)  # Full collection
            
            # Check memory after cleanup
            new_memory_mb = process.memory_info().rss / 1024 / 1024
            logger.info(f"Memory after cleanup: {new_memory_mb:.1f}MB (freed {memory_mb - new_memory_mb:.1f}MB)")
            return True
        return False
    except ImportError:
        # psutil not available, use basic memory check
        import resource
        usage = resource.getrusage(resource.RUSAGE_SELF)
        memory_mb = usage.ru_maxrss / 1024  # On Linux, ru_maxrss is in KB
        logger.info(f"Memory usage (resource){' - ' + context if context else ''}: {memory_mb:.1f}MB")
        return False
    except Exception as e:
        logger.warning(f"Failed to check memory usage: {e}")
        return False


def aggressive_memory_cleanup(logger: PipelineLogger):
    """Perform aggressive memory cleanup between file processing."""
    try:
        import psutil
        process = psutil.Process()
        memory_before = process.memory_info().rss / 1024 / 1024
        
        # Triple garbage collection for maximum cleanup
        gc.collect(0)  # Young generation
        gc.collect(1)  # Middle generation
        gc.collect(2)  # Full collection
        
        # Additional cleanup for pandas/pyarrow
        try:
            import pandas as pd
            import pyarrow as pa
            # Clear any cached pandas operations
            pd.options.mode.chained_assignment = None
            # Force pyarrow memory cleanup if available
            if hasattr(pa, 'total_allocated_bytes'):
                logger.info(f"PyArrow allocated bytes: {pa.total_allocated_bytes()}")
        except ImportError:
            pass
        
        memory_after = process.memory_info().rss / 1024 / 1024
        memory_freed = memory_before - memory_after
        
        logger.info(f"🧹 AGGRESSIVE CLEANUP: {memory_before:.1f}MB → {memory_after:.1f}MB (freed {memory_freed:.1f}MB)")
        
    except Exception as e:
        logger.warning(f"Aggressive memory cleanup failed: {e}")
        # Fallback to basic cleanup
        gc.collect(2)


def process_records_in_batches(records: List[Dict[str, Any]], batch_size: int = 1000):
    """
    Process records in smaller batches to reduce memory usage.
    
    Args:
        records: List of records to process
        batch_size: Number of records per batch
        
    Yields:
        Batches of records
    """
    for i in range(0, len(records), batch_size):
        yield records[i:i + batch_size]


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
    source_files: Optional[List[str]] = None,
    canonical_mapper: CanonicalMapper = None
) -> Dict[str, Any]:
    """Process canonical transformation for a single tenant using sequential file processing."""
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting sequential canonical transformation for tenant {tenant_id}, table {canonical_table}")
        
        # Find raw data files to process
        if source_files:
            raw_files = source_files
            logger.info(f"Using {len(raw_files)} specific source files provided by result aggregator")
        else:
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
        
        logger.info(f"📁 SEQUENTIAL PROCESSING: Processing {len(raw_files)} files one-by-one to prevent memory accumulation")
        
        # Sequential processing variables
        total_records_processed = 0
        total_raw_records = 0
        files_processed = 0
        output_files = []
        
        # Check initial memory
        check_memory_usage(logger, max_memory_mb=100, context="Initial memory state")
        
        # Process each file individually (1:1 transformation)
        for idx, raw_file in enumerate(raw_files):
            logger.info(f"🔄 Processing file {idx+1}/{len(raw_files)}: {raw_file}")
            
            # Check memory before processing each file
            memory_before = check_memory_usage(logger, max_memory_mb=100, context=f"Before file {idx+1}")
            
            # Load and transform ONLY this single file
            try:
                single_file_records = load_and_transform_raw_data(config, raw_file, canonical_table, logger, canonical_mapper)
                
                if single_file_records:
                    raw_count = len(single_file_records)
                    total_raw_records += raw_count
                    
                    # Filter valid records
                    valid_records = [r for r in single_file_records if r is not None]
                    
                    # Add basic metadata (NO SCD processing)
                    processed_records = []
                    for record in valid_records:
                        # Add only essential metadata fields
                        record['record_hash'] = calculate_record_hash(record)
                        record['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
                        processed_records.append(record)
                    
                    if processed_records:
                        # Write this file's data immediately to S3
                        timestamp = get_timestamp()
                        file_number = str(idx + 1).zfill(4)  # 0001, 0002, etc.
                        s3_key = f"{tenant_id}/canonical/{canonical_table}/{tenant_id}-{canonical_table}-{timestamp}-{file_number}.parquet"
                        
                        write_canonical_data_to_s3(config, s3_key, processed_records, logger)
                        output_files.append(s3_key)
                        
                        total_records_processed += len(processed_records)
                        files_processed += 1
                        
                        logger.info(f"✅ File {idx+1} processed: {raw_count} raw → {len(processed_records)} valid → written to {s3_key}")
                    
                    # CRITICAL: Clear memory immediately after processing each file
                    del single_file_records
                    del valid_records
                    del processed_records
                    aggressive_memory_cleanup(logger)
                    
                    # Check memory after cleanup
                    check_memory_usage(logger, max_memory_mb=100, context=f"After file {idx+1} cleanup")
                
            except Exception as file_error:
                logger.error(f"Failed to process file {raw_file}: {str(file_error)}")
                # Continue processing other files
                continue
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # Log sequential processing results
        logger.info(f"🎯 SEQUENTIAL PROCESSING COMPLETE:")
        logger.info(f"   • Files processed: {files_processed}/{len(raw_files)}")
        logger.info(f"   • Total records: {total_raw_records} raw → {total_records_processed} canonical")
        logger.info(f"   • Output files: {len(output_files)}")
        logger.info(f"   • Memory efficient: No accumulation across files")
        logger.info(f"   • Execution time: {execution_time:.2f}s")
        
        # Final memory check
        check_memory_usage(logger, max_memory_mb=100, context="Final memory state")
        
        # Trigger ClickHouse data loader for seamless pipeline
        try:
            trigger_clickhouse_loader(tenant_id, canonical_table, output_files, logger)
        except Exception as e:
            logger.warning(f"Failed to trigger ClickHouse loader: {e}")
        
        return {
            'tenant_id': tenant_id,
            'canonical_table': canonical_table,
            'status': 'success',
            'record_count': total_records_processed,
            'execution_time': execution_time,
            'files_processed': files_processed,
            'output_files': output_files,
            'processing_mode': 'sequential_1to1'
        }
        
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"Failed to process sequential canonical transformation: {str(e)}", execution_time=execution_time)
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


def get_source_mapping(canonical_table: str, mapper: CanonicalMapper = None) -> Optional[Dict[str, str]]:
    """Get source service and table mapping for canonical table using shared CanonicalMapper."""
    if mapper is None:
        # Fallback for backward compatibility - create temporary mapper
        mapper = CanonicalMapper(s3_client=s3)
    return mapper.get_source_mapping(canonical_table)


def load_and_transform_raw_data(config: Config, s3_key: str, canonical_table: str, logger: PipelineLogger, canonical_mapper: CanonicalMapper = None) -> List[Dict[str, Any]]:
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
        mapping = load_canonical_mapping(canonical_table, canonical_mapper)
        
        # Transform data according to mapping
        transformed_records = []
        # Extract tenant_id from s3_key path
        tenant_id = s3_key.split('/')[0] if '/' in s3_key else None
        
        for _, row in df.iterrows():
            transformed_record = transform_record(row.to_dict(), mapping, canonical_table, tenant_id, logger, canonical_mapper)
            if transformed_record:
                transformed_records.append(transformed_record)
        
        logger.info(f"Transformed {len(transformed_records)} records from {s3_key}")
        return transformed_records
        
    except Exception as e:
        logger.error(f"Failed to load and transform data from {s3_key}: {str(e)}")
        return []


# Canonical mapping and transformation functions now use shared CanonicalMapper
def load_canonical_mapping(canonical_table: str, mapper: CanonicalMapper = None) -> Dict[str, Any]:
    """Load canonical mapping configuration using shared CanonicalMapper."""
    if mapper is None:
        # Fallback for backward compatibility - create temporary mapper
        mapper = CanonicalMapper(s3_client=s3)
    bucket_name = os.environ.get('BUCKET_NAME')
    return mapper.load_mapping(canonical_table, bucket=bucket_name)


def transform_record(raw_record: Dict[str, Any], mapping: Dict[str, Any], canonical_table: str, tenant_id: str = None, logger: PipelineLogger = None, mapper: CanonicalMapper = None) -> Optional[Dict[str, Any]]:
    """Transform a single record to canonical format using shared CanonicalMapper with emergency validation."""
    if mapper is None:
        # Fallback for backward compatibility - create temporary mapper
        mapper = CanonicalMapper(s3_client=s3)
    transformed_record = mapper.transform_record(raw_record, mapping, canonical_table)
    
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
        'contacts': ['id', 'first_name'],  # Must have ID and name fields
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


# SCD logic has been removed from canonical transform
# SCD processing will be handled by ClickHouse ReplacingMergeTree engine
# This eliminates memory accumulation issues in Lambda


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


def trigger_clickhouse_loader(tenant_id: str, canonical_table: str, output_files: List[str], logger: PipelineLogger):
    """Trigger ClickHouse data loader with specific canonical file paths for 1:1 transformation support."""
    import boto3
    import json
    
    try:
        # Get Lambda client
        lambda_client = boto3.client('lambda')
        
        # Construct ClickHouse loader function name
        env_name = os.environ.get('ENVIRONMENT', 'dev')
        function_name = f"clickhouse-loader-{canonical_table}-{env_name}"
        
        # Prepare enhanced payload with specific file paths
        payload = {
            'tenant_id': tenant_id,
            'table_name': canonical_table,
            'canonical_files': output_files,  # NEW: Pass specific canonical file paths
            'processing_mode': 'multi_file_1to1',  # Indicate this is from 1:1 transformation
            'file_count': len(output_files)
        }
        
        logger.info(f"🔗 TRIGGERING CLICKHOUSE LOADER: {function_name}")
        logger.info(f"   • Tenant: {tenant_id}")
        logger.info(f"   • Table: {canonical_table}")
        logger.info(f"   • Files: {len(output_files)} canonical parts")
        logger.info(f"   • Sample files: {output_files[:3]}{'...' if len(output_files) > 3 else ''}")
        
        # Invoke ClickHouse loader asynchronously
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Asynchronous invocation
            Payload=json.dumps(payload)
        )
        
        if response['StatusCode'] == 202:
            logger.info(f"✅ Successfully triggered ClickHouse loader for {tenant_id}/{canonical_table} with {len(output_files)} files")
        else:
            logger.warning(f"ClickHouse loader trigger returned status: {response['StatusCode']}")
            
    except Exception as e:
        logger.error(f"Failed to trigger ClickHouse loader: {str(e)}")
