"""
ClickHouse Data Loader Lambda Function

Loads canonical data from S3 into ClickHouse tables with proper error handling
and tenant isolation.
"""

import json
import os
import sys
import logging
import boto3
from datetime import datetime
from typing import Dict, List, Any, Optional
import clickhouse_connect
from clickhouse_connect.driver.client import Client

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def check_memory_usage(context: str = "") -> bool:
    """Check current memory usage for ClickHouse loader."""
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        
        # Get Lambda specific memory limit
        lambda_memory_limit = int(os.environ.get('AWS_LAMBDA_FUNCTION_MEMORY_SIZE', '1024'))
        memory_percentage = (memory_mb / lambda_memory_limit) * 100
        
        logger.info(f"💾 Memory usage{' - ' + context if context else ''}: {memory_mb:.1f}MB / {lambda_memory_limit}MB ({memory_percentage:.1f}%)")
        
        # If we're using more than 80% of Lambda memory, it's critical
        if memory_percentage > 80:
            logger.error(f"🚨 CRITICAL: Memory usage at {memory_percentage:.1f}% of Lambda limit!")
        
        return memory_percentage > 80
        
    except ImportError:
        logger.info(f"Memory monitoring unavailable{' - ' + context if context else ''}")
        return False
    except Exception as e:
        logger.warning(f"Failed to check memory usage: {e}")
        return False

def aggressive_memory_cleanup(context: str = ""):
    """Perform aggressive memory cleanup between file processing."""
    try:
        import psutil
        import gc
        
        process = psutil.Process()
        memory_before = process.memory_info().rss / 1024 / 1024
        
        # Triple garbage collection for maximum cleanup
        gc.collect(0)  # Young generation
        gc.collect(1)  # Middle generation
        gc.collect(2)  # Full collection
        
        memory_after = process.memory_info().rss / 1024 / 1024
        memory_freed = memory_before - memory_after
        
        logger.info(f"🧹 CLEANUP{' - ' + context if context else ''}: {memory_before:.1f}MB → {memory_after:.1f}MB (freed {memory_freed:.1f}MB)")
        
    except Exception as e:
        logger.warning(f"Memory cleanup failed: {e}")
        # Fallback to basic cleanup
        import gc
        gc.collect(2)

def lambda_handler(event, context):
    """
    Main Lambda handler for loading data from S3 to ClickHouse.
    
    Expected event structure (MODERN):
    {
        "tenant_id": "sitetechnology",
        "table_name": "companies",
        "date": "2025-06-24",
        "bucket_name": "avesa-data-bucket",
        "debug": false  # Optional: for testing imports only
    }
    
    Alternative event structure (LEGACY):
    {
        "tenant_id": "sitetechnology",
        "s3_key": "sitetechnology/canonical/companies/2025/06/24/companies.parquet",
        "bucket_name": "avesa-data-bucket",
        "debug": false
    }
    """
    logger.info(f"ClickHouse Data Loader starting")
    logger.info(f"Event: {json.dumps(event, default=str)}")
    
    # If debug mode, just test imports
    if event.get('debug', False):
        return test_imports()
    
    try:
        # Get environment variables
        target_table = os.environ.get('TARGET_TABLE', 'companies')
        clickhouse_secret_name = os.environ.get('CLICKHOUSE_SECRET_NAME')
        s3_bucket_name = os.environ.get('S3_BUCKET_NAME')
        environment = os.environ.get('ENVIRONMENT', 'dev')
        
        # Validate required parameters
        tenant_id = event.get('tenant_id')
        if not tenant_id:
            raise ValueError("tenant_id is required")
        
        # NEW: Handle specific canonical files from 1:1 transformation
        canonical_files = event.get('canonical_files')
        processing_mode = event.get('processing_mode', 'legacy')
        
        # Initialize AWS clients
        s3_client = boto3.client('s3')
        secrets_client = boto3.client('secretsmanager')
        
        # Get ClickHouse connection details
        clickhouse_client = get_clickhouse_client(secrets_client, clickhouse_secret_name)
        
        if canonical_files and processing_mode == 'multi_file_1to1':
            # New 1:1 transformation mode with STREAMING processing to prevent memory accumulation
            logger.info(f"🔗 1:1 STREAMING MODE: Processing {len(canonical_files)} canonical files for tenant: {tenant_id}, table: {target_table}")
            logger.info(f"   • Files: {[f.split('/')[-1] for f in canonical_files[:3]]}{'...' if len(canonical_files) > 3 else ''}")
            
            # MEMORY OPTIMIZATION: Process files one-by-one with immediate ClickHouse insertion
            total_records_inserted = 0
            files_processed = 0
            
            # Check initial memory
            check_memory_usage(f"Initial state - processing {len(canonical_files)} files")
            
            for idx, file_path in enumerate(canonical_files):
                try:
                    logger.info(f"   📄 Processing file {idx+1}/{len(canonical_files)}: {file_path}")
                    
                    # Check memory before each file
                    is_memory_critical = check_memory_usage(f"Before file {idx+1}")
                    if is_memory_critical:
                        logger.warning(f"⚠️ Memory critical before processing file {idx+1}, performing cleanup")
                        aggressive_memory_cleanup(f"Pre-processing file {idx+1}")
                    
                    # Load ONLY this single file's data
                    file_data = load_data_from_s3(s3_client, s3_bucket_name, file_path)
                    
                    if file_data:
                        # Insert this file's data immediately to ClickHouse
                        records_inserted = load_data_to_clickhouse(
                            clickhouse_client,
                            target_table,
                            file_data,
                            tenant_id
                        )
                        
                        total_records_inserted += records_inserted
                        files_processed += 1
                        
                        logger.info(f"   ✅ Processed {len(file_data)} records → inserted {records_inserted} to ClickHouse from {file_path.split('/')[-1]}")
                    
                    # CRITICAL: Clear memory immediately after processing each file
                    del file_data
                    aggressive_memory_cleanup(f"After file {idx+1}")
                    
                    # Check memory after cleanup
                    check_memory_usage(f"After file {idx+1} cleanup")
                    
                except Exception as file_error:
                    logger.error(f"   ❌ Failed to process {file_path}: {file_error}")
                    # Continue with other files
                    continue
            
            # Final memory check after all files processed
            final_memory_check = check_memory_usage("Final state after all files processed")
            
            logger.info(f"🎯 STREAMING PROCESSING COMPLETE: {files_processed}/{len(canonical_files)} files, {total_records_inserted} total records")
            
            # One final cleanup for good measure
            aggressive_memory_cleanup("Final cleanup")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Data loaded successfully via streaming',
                    'tenant_id': tenant_id,
                    'table': target_table,
                    's3_key': f"STREAMED: {len(canonical_files)} files",
                    'records_processed': total_records_inserted,
                    'processing_mode': 'streaming_multi_file_1to1',
                    'files_processed': files_processed,
                    'memory_status': 'optimized'
                })
            }
            
        else:
            # Legacy mode: get S3 key from event or construct default
            s3_key = event.get('s3_key')
            if not s3_key:
                # Construct default S3 key for today's data (using Parquet format)
                today = datetime.now()
                s3_key = f"{tenant_id}/canonical/{target_table}/{today.year}/{today.month:02d}/{today.day:02d}/{target_table}.parquet"
            
            logger.info(f"🔄 LEGACY MODE: Loading data for tenant: {tenant_id}, table: {target_table}, s3_key: {s3_key}")
            
            # Load data from S3
            data = load_data_from_s3(s3_client, s3_bucket_name, s3_key)
        
        if not data:
            logger.warning(f"No data found for loading: {s3_key}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data to load',
                    'tenant_id': tenant_id,
                    'table': target_table,
                    's3_key': s3_key,
                    'records_processed': 0,
                    'processing_mode': processing_mode
                })
            }
        
        # Load data into ClickHouse
        records_inserted = load_data_to_clickhouse(
            clickhouse_client,
            target_table,
            data,
            tenant_id
        )
        
        logger.info(f"✅ Successfully loaded {records_inserted} records for tenant {tenant_id} (mode: {processing_mode})")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data loaded successfully',
                'tenant_id': tenant_id,
                'table': target_table,
                's3_key': s3_key,
                'records_processed': records_inserted,
                'processing_mode': processing_mode,
                'files_processed': len(canonical_files) if canonical_files else 1
            })
        }
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Failed to load data'
            })
        }

def test_imports():
    """Test import functionality for debugging."""
    logger.info("Testing imports...")
    
    results = {}
    
    # Test LZ4
    try:
        import lz4
        results['lz4'] = 'SUCCESS'
        logger.info("✅ LZ4 imported successfully")
    except Exception as e:
        results['lz4'] = f'FAILED: {str(e)}'
        logger.error(f"❌ LZ4 import failed: {e}")
    
    # Test zstandard
    try:
        import zstandard
        results['zstandard'] = 'SUCCESS'
        logger.info("✅ zstandard imported successfully")
    except Exception as e:
        results['zstandard'] = f'FAILED: {str(e)}'
        logger.error(f"❌ zstandard import failed: {e}")
    
    # Test clickhouse_connect
    try:
        import clickhouse_connect
        results['clickhouse_connect'] = 'SUCCESS'
        logger.info("✅ clickhouse_connect imported successfully")
    except Exception as e:
        results['clickhouse_connect'] = f'FAILED: {str(e)}'
        logger.error(f"❌ clickhouse_connect import failed: {e}")
    
    # Test pandas (critical for Parquet processing)
    try:
        import pandas as pd
        results['pandas'] = 'SUCCESS'
        logger.info("✅ pandas imported successfully")
    except Exception as e:
        results['pandas'] = f'FAILED: {str(e)}'
        logger.error(f"❌ pandas import failed: {e}")
    
    # Test pyarrow (critical for Parquet processing)
    try:
        import pyarrow
        results['pyarrow'] = 'SUCCESS'
        logger.info("✅ pyarrow imported successfully")
    except Exception as e:
        results['pyarrow'] = f'FAILED: {str(e)}'
        logger.error(f"❌ pyarrow import failed: {e}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Import test completed',
            'results': results
        })
    }

def get_clickhouse_client(secrets_client, secret_name: str) -> Client:
    """Get ClickHouse client using AWS Secrets Manager."""
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
        
        logger.info(f"✅ Connected to ClickHouse: {secret['host']}")
        return client
        
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        raise

def load_data_from_s3(s3_client, bucket_name: str, s3_key: str) -> List[Dict[str, Any]]:
    """Load data from S3 with support for multiple canonical files (1:1 transformation output)."""
    try:
        logger.info(f"Loading data from S3: s3://{bucket_name}/{s3_key}")
        
        # First try the exact path provided
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            content = response['Body'].read()
            logger.info(f"✅ Found file at exact path: {s3_key}")
            return process_single_file_content(content, s3_key)
            
        except s3_client.exceptions.NoSuchKey:
            logger.info(f"File not found at exact path: {s3_key}")
            
            # Extract tenant_id and table_name from the expected path format
            path_parts = s3_key.split('/')
            if len(path_parts) >= 4 and path_parts[1] == 'canonical':
                tenant_id = path_parts[0]
                table_name = path_parts[2]
                
                # NEW: Support for multiple canonical files from 1:1 transformation
                # Look for all canonical files with format: {tenant_id}-{canonical_table}-{timestamp}-{file_number}.parquet
                search_prefix = f"{tenant_id}/canonical/{table_name}/"
                logger.info(f"🔍 MULTIPLE FILE PROCESSING: Searching for canonical files with prefix: {search_prefix}")
                
                # List all files in the canonical directory for this table
                paginator = s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=bucket_name, Prefix=search_prefix)
                
                files = []
                for page in pages:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            if obj['Key'].endswith('.parquet'):
                                files.append({
                                    'key': obj['Key'],
                                    'last_modified': obj['LastModified']
                                })
                
                if not files:
                    logger.warning(f"No parquet files found with prefix: {search_prefix}")
                    return []
                
                # Sort by last modified time (newest first) to get recent canonical files
                files.sort(key=lambda x: x['last_modified'], reverse=True)
                
                # NEW: Load ALL recent canonical files for comprehensive data loading
                # Group files by timestamp (same transformation batch)
                import re
                from collections import defaultdict
                
                file_groups = defaultdict(list)
                timestamp_pattern = r'-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})'
                
                for file_info in files:
                    key = file_info['key']
                    match = re.search(timestamp_pattern, key)
                    if match:
                        timestamp = match.group(1)
                        file_groups[timestamp].append(file_info)
                    else:
                        # Fallback for files without timestamp pattern
                        file_groups['unknown'].append(file_info)
                
                if not file_groups:
                    logger.warning(f"No timestamped canonical files found")
                    return []
                
                # Get the most recent timestamp group
                latest_timestamp = max(file_groups.keys()) if 'unknown' not in file_groups or len(file_groups) > 1 else 'unknown'
                latest_files = file_groups[latest_timestamp]
                
                logger.info(f"📁 BATCH PROCESSING: Found {len(file_groups)} timestamp groups. Loading {len(latest_files)} files from latest batch: {latest_timestamp}")
                
                # Load and combine data from all files in the latest batch
                all_data = []
                for file_info in latest_files:
                    try:
                        logger.info(f"   • Loading file: {file_info['key']}")
                        response = s3_client.get_object(Bucket=bucket_name, Key=file_info['key'])
                        content = response['Body'].read()
                        file_data = process_single_file_content(content, file_info['key'])
                        all_data.extend(file_data)
                        logger.info(f"   ✅ Loaded {len(file_data)} records from {file_info['key']}")
                    except Exception as file_error:
                        logger.error(f"   ❌ Failed to load {file_info['key']}: {file_error}")
                        # Continue with other files
                        continue
                
                logger.info(f"🎯 BATCH LOADING COMPLETE: Combined {len(all_data)} records from {len(latest_files)} canonical files")
                return all_data
                
            else:
                # Path format not recognized, raise the original error
                raise
        
    except Exception as e:
        logger.error(f"Failed to load data from S3: {e}")
        raise


def process_single_file_content(content: bytes, s3_key: str) -> List[Dict[str, Any]]:
    """Process content from a single S3 file."""
    # Determine file format based on extension
    if s3_key.lower().endswith('.parquet'):
        # Handle Parquet files
        import pandas as pd
        import io
        
        df = pd.read_parquet(io.BytesIO(content))
        
        # Convert DataFrame to list of dictionaries
        data = df.to_dict('records')
        
        # Convert any datetime objects to strings for JSON serialization
        for record in data:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
                elif hasattr(value, 'isoformat'):  # datetime objects
                    record[key] = value.isoformat()
                    
    else:
        # Handle JSON files
        content_str = content.decode('utf-8')
        data = json.loads(content_str)
        
        # Handle both single objects and arrays
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            raise ValueError(f"Expected JSON object or array, got {type(data)}")
    
    return data

def load_data_to_clickhouse(
    client: Client,
    table_name: str,
    data: List[Dict[str, Any]],
    tenant_id: str
) -> int:
    """Load data into ClickHouse table with automatic table creation and schema alignment."""
    try:
        if not data:
            return 0
        
        # Check if table exists and create if needed
        if not table_exists(client, table_name):
            logger.info(f"Table {table_name} does not exist. Creating it automatically...")
            if not create_table_from_mapping(client, table_name):
                raise Exception(f"Failed to create table {table_name}")
        
        # Get ClickHouse table schema to filter columns
        table_columns = get_table_columns(client, table_name)
        logger.info(f"ClickHouse table {table_name} has {len(table_columns)} columns")
        
        # Add tenant_id to all records if not present
        for record in data:
            if 'tenant_id' not in record:
                record['tenant_id'] = tenant_id
        
        # Filter data to only include columns that exist in ClickHouse table
        filtered_data = []
        source_columns = set(data[0].keys()) if data else set()
        logger.info(f"Source data has {len(source_columns)} columns")
        
        # Find columns to include (intersection of source and table columns)
        columns_to_include = table_columns.intersection(source_columns)
        columns_missing_in_source = table_columns - source_columns
        columns_extra_in_source = source_columns - table_columns
        
        logger.info(f"Columns to include in insert: {len(columns_to_include)}")
        if columns_missing_in_source:
            logger.warning(f"Columns missing in source data: {sorted(columns_missing_in_source)}")
        if columns_extra_in_source:
            logger.info(f"Extra columns in source (will be ignored): {sorted(columns_extra_in_source)}")
        
        # Filter each record to only include valid columns
        for record in data:
            filtered_record = {col: record.get(col) for col in columns_to_include}
            filtered_data.append(filtered_record)
        
        # Apply data type normalization for ClickHouse compatibility
        logger.info("Applying data type normalization for ClickHouse compatibility")
        
        # Get table schema once for efficiency
        table_schema = get_table_schema(client, table_name)
        
        for record in filtered_data:
            for col_name in columns_to_include:
                value = record.get(col_name)
                if value is None:
                    record[col_name] = None  # Keep nulls as None
                else:
                    # Apply type-specific conversion based on ClickHouse schema
                    clickhouse_type = table_schema.get(col_name, '')
                    record[col_name] = convert_value_for_clickhouse(value, clickhouse_type, col_name)
        
        # Insert filtered data using clickhouse_connect
        logger.info(f"Inserting {len(filtered_data)} records into {table_name}")
        
        # Use native SQL INSERT approach (most compatible with ClickHouse)
        if filtered_data:
            # Get column names in consistent order
            column_names = sorted(columns_to_include)
            
            # Prepare data in row format for SQL insertion
            rows_to_insert = []
            for record in filtered_data:
                row_values = []
                for col_name in column_names:
                    value = record.get(col_name)
                    # Handle None values and ensure proper formatting
                    if value is None:
                        row_values.append(None)
                    else:
                        row_values.append(value)
                rows_to_insert.append(row_values)
            
            # Debug logging
            logger.info(f"Prepared {len(rows_to_insert)} rows for SQL insertion")
            logger.info(f"Using {len(column_names)} columns: {column_names[:5]}...")
            
            # Use standard insert method with row data format
            client.insert(table_name, rows_to_insert, column_names=column_names)
        else:
            logger.warning("No data to insert")
        
        logger.info(f"✅ Successfully inserted {len(filtered_data)} records into {table_name}")
        
        # Force immediate deduplication with OPTIMIZE
        logger.info(f"Running OPTIMIZE on {table_name} to ensure immediate deduplication...")
        try:
            client.query(f"OPTIMIZE TABLE {table_name} FINAL")
            logger.info(f"✅ Successfully optimized {table_name} for deduplication")
        except Exception as e:
            logger.warning(f"OPTIMIZE operation failed for {table_name}: {e}")
            # Don't fail the entire operation if OPTIMIZE fails
        
        return len(filtered_data)
        
    except Exception as e:
        logger.error(f"Failed to insert data into ClickHouse: {e}")
        raise

def get_table_columns(client: Client, table_name: str) -> set:
    """Get the set of column names for a ClickHouse table."""
    try:
        query = f"DESCRIBE TABLE {table_name}"
        result = client.query(query)
        columns = {row[0] for row in result.result_rows}
        return columns
    except Exception as e:
        logger.error(f"Failed to get table columns for {table_name}: {e}")
        raise

def get_table_schema(client: Client, table_name: str) -> Dict[str, str]:
    """Get the schema of a ClickHouse table as a dictionary of field_name -> field_type."""
    try:
        query = f"DESCRIBE TABLE {table_name}"
        result = client.query(query)
        schema = {}
        for row in result.result_rows:
            field_name = row[0]
            field_type = row[1]
            schema[field_name] = field_type
        return schema
    except Exception as e:
        logger.error(f"Failed to get table schema for {table_name}: {e}")
        return {}

def table_exists(client: Client, table_name: str) -> bool:
    """Check if table exists in ClickHouse."""
    try:
        query = f"EXISTS TABLE {table_name}"
        result = client.query(query)
        return bool(result.result_rows[0][0])
    except Exception as e:
        logger.warning(f"Failed to check if table {table_name} exists: {e}")
        return False

def create_table_from_mapping(client: Client, table_name: str) -> bool:
    """Create ClickHouse table from canonical mapping using dynamic schema manager."""
    try:
        # Import the dynamic schema manager
        import sys
        import os
        
        # Add schema_init path
        schema_init_path = os.path.join(os.path.dirname(__file__), '..', 'schema_init')
        if schema_init_path not in sys.path:
            sys.path.insert(0, schema_init_path)
        
        from dynamic_schema_manager import DynamicClickHouseSchemaManager
        
        # Create schema manager instance
        schema_manager = DynamicClickHouseSchemaManager(client)
        
        # Create table with ordered schema (matching mapping file order)
        logger.info(f"Creating table {table_name} using dynamic schema manager...")
        success = schema_manager.create_table_from_mapping(table_name, use_ordered_schema=True)
        
        if success:
            logger.info(f"✅ Successfully created table {table_name}")
        else:
            logger.error(f"❌ Failed to create table {table_name}")
        
        return success
        
    except Exception as e:
        logger.error(f"Failed to create table {table_name}: {e}")
        return False

def convert_value_for_clickhouse(value: Any, clickhouse_type: str, field_name: str) -> Any:
    """Convert a value to the appropriate type for ClickHouse insertion."""
    if value is None:
        return None
    
    # Handle nullable types
    is_nullable = clickhouse_type.startswith('Nullable(')
    if is_nullable:
        # Extract the inner type from Nullable(Type)
        inner_type = clickhouse_type[9:-1]  # Remove 'Nullable(' and ')'
    else:
        inner_type = clickhouse_type
    
    try:
        # Date type conversion - COMPREHENSIVE FIX for ClickHouse Date serialization
        if 'Date' in inner_type and inner_type != 'DateTime':
            from datetime import datetime, date
            import math
            
            # Handle None/empty/invalid values first - CRITICAL for ClickHouse compatibility
            if value is None:
                return None
            
            # Handle NaN float values (common in pandas DataFrames)
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                return None
            
            # Handle string representations of null/empty values
            if isinstance(value, str):
                value_lower = value.strip().lower()
                if not value_lower or value_lower in ['nan', 'none', 'null', 'na', '', '0000-00-00']:
                    return None
            
            # Handle numeric values (timestamp/epoch conversions)
            if isinstance(value, (int, float)):
                # Handle common "null" numeric representations
                if value == 0 or value < 0:
                    return None
                
                # Handle Unix timestamps (seconds since epoch)
                try:
                    if 1000000000 <= value <= 2147483647:  # Valid Unix timestamp range (2001-2038)
                        parsed_date = datetime.fromtimestamp(value)
                        return parsed_date.date()
                    elif 1000000000000 <= value <= 2147483647000:  # Unix timestamp in milliseconds
                        parsed_date = datetime.fromtimestamp(value / 1000)
                        return parsed_date.date()
                    else:
                        # Other numeric values are invalid for dates
                        logger.warning(f"Invalid numeric date value {value} for field {field_name}, setting to None")
                        return None
                except (ValueError, OSError, OverflowError):
                    logger.warning(f"Could not convert numeric timestamp {value} for field {field_name}, setting to None")
                    return None
            
            # Handle string dates
            if isinstance(value, str):
                value_trimmed = value.strip()
                if not value_trimmed:
                    return None
                
                # Try common date formats with enhanced error handling
                date_formats = [
                    '%Y-%m-%d',                    # 2023-01-15
                    '%m/%d/%Y',                    # 01/15/2023
                    '%d/%m/%Y',                    # 15/01/2023
                    '%Y-%m-%dT%H:%M:%S%z',        # 2023-01-15T12:00:00+00:00
                    '%Y-%m-%dT%H:%M:%S',          # 2023-01-15T12:00:00
                    '%Y-%m-%dT%H:%M:%S.%fZ',      # 2023-01-15T12:00:00.000Z
                    '%Y-%m-%dT%H:%M:%SZ',         # 2023-01-15T12:00:00Z
                    '%Y/%m/%d',                    # 2023/01/15
                    '%d-%m-%Y',                    # 15-01-2023
                ]
                
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(value_trimmed, fmt)
                        result_date = parsed_date.date()
                        
                        # Validate reasonable date range (avoid ancient or far future dates)
                        if 1900 <= result_date.year <= 2100:
                            return result_date
                        else:
                            logger.warning(f"Date {result_date} outside reasonable range for field {field_name}, setting to None")
                            return None
                            
                    except ValueError:
                        continue
                
                # Pandas fallback with enhanced error handling
                try:
                    import pandas as pd
                    parsed_date = pd.to_datetime(value_trimmed, errors='coerce', utc=True)
                    if pd.notna(parsed_date):
                        result_date = parsed_date.date()
                        # Additional validation for pandas results
                        if 1900 <= result_date.year <= 2100:
                            return result_date
                        else:
                            logger.warning(f"Pandas parsed date {result_date} outside reasonable range for field {field_name}, setting to None")
                            return None
                    else:
                        logger.debug(f"Pandas could not parse date value '{value}' for field {field_name}, setting to None")
                        return None
                except Exception as e:
                    logger.debug(f"Pandas date parsing failed for '{value}' on field {field_name}: {e}, setting to None")
                    return None
            
            # Handle datetime objects
            elif hasattr(value, 'date') and callable(getattr(value, 'date')):
                try:
                    result_date = value.date()
                    if 1900 <= result_date.year <= 2100:
                        return result_date
                    else:
                        logger.warning(f"Datetime object date {result_date} outside reasonable range for field {field_name}, setting to None")
                        return None
                except Exception as e:
                    logger.warning(f"Could not extract date from datetime object for field {field_name}: {e}, setting to None")
                    return None
            
            # Handle date objects directly
            elif isinstance(value, date):
                if 1900 <= value.year <= 2100:
                    return value
                else:
                    logger.warning(f"Date object {value} outside reasonable range for field {field_name}, setting to None")
                    return None
            
            # For any other type, convert to None (safer than attempting conversion)
            else:
                logger.warning(f"Unexpected date value type {type(value)} for field {field_name}: '{value}', setting to None")
                return None
        
        # DateTime type conversion
        elif 'DateTime' in inner_type:
            if isinstance(value, str):
                from datetime import datetime
                try:
                    # Try ISO format first
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except:
                    try:
                        import pandas as pd
                        return pd.to_datetime(value).to_pydatetime()
                    except:
                        logger.warning(f"Could not parse datetime value '{value}' for field {field_name}, keeping as string")
                        return str(value)
            return value
        
        # Numeric type conversions
        elif 'Int' in inner_type or 'UInt' in inner_type:
            if isinstance(value, str) and value.strip() == '':
                return None
            return int(float(value))  # Handle cases where int is stored as float string
        
        elif 'Float' in inner_type:
            if isinstance(value, str) and value.strip() == '':
                return None
            return float(value)
        
        # Boolean type conversion
        elif 'Bool' in inner_type:
            if isinstance(value, str):
                value_lower = value.lower()
                if value_lower in ['true', '1', 'yes', 'on']:
                    return True
                elif value_lower in ['false', '0', 'no', 'off']:
                    return False
                else:
                    return bool(value)
            return bool(value)
        
        # String types - convert to string but preserve None
        elif 'String' in inner_type:
            return str(value)
        
        else:
            # For unknown types, convert to string as fallback
            logger.debug(f"Unknown ClickHouse type '{clickhouse_type}' for field {field_name}, converting to string")
            return str(value)
            
    except Exception as e:
        logger.warning(f"Error converting value '{value}' for field {field_name} to type {clickhouse_type}: {e}")
        # Fallback to string conversion
        return str(value)