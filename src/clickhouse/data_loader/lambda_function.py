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
        
        # Get S3 key from event or construct default
        s3_key = event.get('s3_key')
        if not s3_key:
            # Construct default S3 key for today's data (using Parquet format)
            # FIXED: Use correct path format that matches canonical transform output
            today = datetime.now()
            s3_key = f"{tenant_id}/canonical/{target_table}/{today.year}/{today.month:02d}/{today.day:02d}/{target_table}.parquet"
        
        logger.info(f"Loading data for tenant: {tenant_id}, table: {target_table}, s3_key: {s3_key}")
        
        # Initialize AWS clients
        s3_client = boto3.client('s3')
        secrets_client = boto3.client('secretsmanager')
        
        # Get ClickHouse connection details
        clickhouse_client = get_clickhouse_client(secrets_client, clickhouse_secret_name)
        
        # Load data from S3
        data = load_data_from_s3(s3_client, s3_bucket_name, s3_key)
        
        if not data:
            logger.warning(f"No data found in S3 key: {s3_key}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data to load',
                    'tenant_id': tenant_id,
                    'table': target_table,
                    's3_key': s3_key,
                    'records_processed': 0
                })
            }
        
        # Load data into ClickHouse
        records_inserted = load_data_to_clickhouse(
            clickhouse_client,
            target_table,
            data,
            tenant_id
        )
        
        logger.info(f"Successfully loaded {records_inserted} records for tenant {tenant_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data loaded successfully',
                'tenant_id': tenant_id,
                'table': target_table,
                's3_key': s3_key,
                'records_processed': records_inserted
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
    """Load data from S3 (supports both JSON and Parquet formats with intelligent path resolution)."""
    try:
        logger.info(f"Loading data from S3: s3://{bucket_name}/{s3_key}")
        
        # First try the exact path provided
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            content = response['Body'].read()
            logger.info(f"✅ Found file at exact path: {s3_key}")
            
        except s3_client.exceptions.NoSuchKey:
            logger.info(f"File not found at exact path: {s3_key}")
            
            # Try to find timestamp-based files in the directory
            # Extract tenant_id and table_name from the expected path format
            # Expected: sitetechnology/canonical/companies/2025/06/24/companies.parquet
            # Actual:   sitetechnology/canonical/companies/2025-06-24T04:54:35.574050Z.parquet
            
            path_parts = s3_key.split('/')
            if len(path_parts) >= 4 and path_parts[1] == 'canonical':
                tenant_id = path_parts[0]
                table_name = path_parts[2]
                
                # Look for timestamp-based files in the canonical directory
                search_prefix = f"{tenant_id}/canonical/{table_name}/"
                logger.info(f"Searching for timestamp-based files with prefix: {search_prefix}")
                
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
                
                # Sort by last modified time (newest first) and take the most recent
                files.sort(key=lambda x: x['last_modified'], reverse=True)
                latest_file = files[0]
                
                logger.info(f"Found {len(files)} files. Using latest: {latest_file['key']} (modified: {latest_file['last_modified']})")
                
                # Load the latest file
                response = s3_client.get_object(Bucket=bucket_name, Key=latest_file['key'])
                content = response['Body'].read()
                s3_key = latest_file['key']  # Update for logging
                
            else:
                # Path format not recognized, raise the original error
                raise
        
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
        
        logger.info(f"✅ Successfully loaded {len(data)} records from S3 (final path: {s3_key})")
        return data
        
    except Exception as e:
        logger.error(f"Failed to load data from S3: {e}")
        raise

def load_data_to_clickhouse(
    client: Client,
    table_name: str,
    data: List[Dict[str, Any]],
    tenant_id: str
) -> int:
    """Load data into ClickHouse table with schema alignment."""
    try:
        if not data:
            return 0
        
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
        # Date type conversion
        if 'Date' in inner_type and inner_type != 'DateTime':
            # Convert string dates to datetime.date objects
            if isinstance(value, str):
                from datetime import datetime
                # Try common date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S']:
                    try:
                        parsed_date = datetime.strptime(value, fmt)
                        return parsed_date.date()
                    except ValueError:
                        continue
                
                # If no format matches, try pandas date parsing as fallback
                try:
                    import pandas as pd
                    parsed_date = pd.to_datetime(value)
                    return parsed_date.date()
                except:
                    logger.warning(f"Could not parse date value '{value}' for field {field_name}, keeping as string")
                    return str(value)
            return value
        
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