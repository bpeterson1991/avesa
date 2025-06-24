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
    
    Expected event structure:
    {
        "tenant_id": "sitetechnology",
        "s3_key": "canonical/companies/sitetechnology/2024/12/23/companies.json",
        "debug": false  # Optional: for testing imports only
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
            # Construct default S3 key for today's data
            today = datetime.now()
            s3_key = f"canonical/{target_table}/{tenant_id}/{today.year}/{today.month:02d}/{today.day:02d}/{target_table}.json"
        
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
    """Load data from S3 (supports both JSON and Parquet formats)."""
    try:
        logger.info(f"Loading data from S3: s3://{bucket_name}/{s3_key}")
        
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        content = response['Body'].read()
        
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
        
        logger.info(f"Loaded {len(data)} records from S3")
        return data
        
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"S3 key not found: {s3_key}")
        return []
    except Exception as e:
        logger.error(f"Failed to load data from S3: {e}")
        raise

def load_data_to_clickhouse(
    client: Client,
    table_name: str,
    data: List[Dict[str, Any]],
    tenant_id: str
) -> int:
    """Load data into ClickHouse table."""
    try:
        if not data:
            return 0
        
        # Add tenant_id to all records if not present
        for record in data:
            if 'tenant_id' not in record:
                record['tenant_id'] = tenant_id
        
        # Insert data using clickhouse_connect
        logger.info(f"Inserting {len(data)} records into {table_name}")
        
        # Use insert method with data list
        client.insert(table_name, data)
        
        logger.info(f"✅ Successfully inserted {len(data)} records into {table_name}")
        return len(data)
        
    except Exception as e:
        logger.error(f"Failed to insert data into ClickHouse: {e}")
        raise