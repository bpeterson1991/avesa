"""
ConnectWise Integration Lambda Function

This function pulls data from ConnectWise REST API per tenant, flattens the JSON,
filters out already-seen records based on `id`, and writes to S3 in Parquet format.

This is a ConnectWise-specific lambda function that handles multiple ConnectWise endpoints/tables.
This is a multi-tenant system where multiple customer organizations (tenants) share this lambda function.

ConnectWise-specific features:
- Uses ConnectWise API authentication (Basic Auth with API keys)
- Handles ConnectWise-specific field structures and naming conventions
- Supports ConnectWise pagination and rate limiting
- Processes ConnectWise endpoints: service/tickets, time/entries, company/companies, company/contacts, etc.
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# Add shared module to path
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))

from shared import (
    Config, TenantConfig, ConnectWiseCredentials,
    get_dynamodb_client, get_s3_client, get_secrets_client,
    PipelineLogger, flatten_json, get_timestamp, get_s3_key,
    validate_tenant_config, chunk_list, safe_get
)

# Initialize clients
dynamodb = get_dynamodb_client()
s3 = get_s3_client()
secrets = get_secrets_client()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for raw data ingestion.
    
    Args:
        event: Lambda event (can contain tenant_id and table_name for targeted runs)
        context: Lambda context
        
    Returns:
        Execution summary
    """
    logger = PipelineLogger("raw_ingestion")
    config = Config.from_environment()
    
    try:
        logger.info("Starting raw data ingestion", execution_id=context.aws_request_id)
        
        # Get target tenant and table from event (optional)
        target_tenant = event.get('tenant_id')
        target_table = event.get('table_name')
        
        # Get tenant configurations
        tenants = get_tenant_configurations(config, target_tenant)
        
        results = []
        for tenant_config in tenants:
            tenant_logger = PipelineLogger("raw_ingestion", tenant_config.tenant_id)
            
            try:
                # Get ConnectWise credentials
                credentials = get_connectwise_credentials(tenant_config.secret_name)
                
                # Get tables to process
                tables = [target_table] if target_table else tenant_config.tables
                
                for table_name in tables:
                    table_logger = PipelineLogger("raw_ingestion", tenant_config.tenant_id, table_name)
                    
                    try:
                        result = process_table(
                            config=config,
                            tenant_config=tenant_config,
                            credentials=credentials,
                            table_name=table_name,
                            logger=table_logger
                        )
                        results.append(result)
                        
                    except Exception as e:
                        table_logger.error(f"Failed to process table {table_name}: {str(e)}")
                        results.append({
                            'tenant_id': tenant_config.tenant_id,
                            'table_name': table_name,
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
            "Raw data ingestion completed",
            successful_jobs=successful,
            failed_jobs=failed,
            total_records=total_records
        )
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Raw data ingestion completed',
                'successful_jobs': successful,
                'failed_jobs': failed,
                'total_records': total_records,
                'results': results
            }
        }
        
    except Exception as e:
        logger.error(f"Raw data ingestion failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'message': 'Raw data ingestion failed',
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
                'tables': get_enabled_endpoints_for_tenant(item['tenant_id']['S']),
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
                    'tables': get_enabled_endpoints_for_tenant(item['tenant_id']['S']),
                    'custom_config': json.loads(item.get('custom_config', {'S': '{}'})['S'])
                }
                tenants.append(validate_tenant_config(tenant_data))
            
            return tenants
            
    except Exception as e:
        raise Exception(f"Failed to get tenant configurations: {str(e)}")


def get_enabled_endpoints_for_tenant(tenant_id: str) -> List[str]:
    """Get enabled endpoints for ConnectWise from endpoint configuration."""
    try:
        # Try to get tenant-specific endpoint config first
        try:
            endpoint_key = f"{tenant_id}/mappings/integrations/connectwise_endpoints.json"
            response = s3.get_object(Bucket=os.environ['BUCKET_NAME'], Key=endpoint_key)
            endpoint_config = json.loads(response['Body'].read().decode('utf-8'))
        except:
            # Fall back to default endpoint config
            endpoint_key = "mappings/integrations/connectwise_endpoints.json"
            response = s3.get_object(Bucket=os.environ['BUCKET_NAME'], Key=endpoint_key)
            endpoint_config = json.loads(response['Body'].read().decode('utf-8'))
        
        # Return only enabled endpoints
        enabled_endpoints = []
        for endpoint, config in endpoint_config.get('endpoints', {}).items():
            if config.get('enabled', False):
                enabled_endpoints.append(endpoint)
        
        return enabled_endpoints
        
    except Exception as e:
        # Fall back to default endpoints if config can't be read
        return ['service/tickets', 'time/entries', 'company/companies', 'company/contacts']


def get_connectwise_credentials(secret_name: str) -> ConnectWiseCredentials:
    """Get ConnectWise credentials from AWS Secrets Manager."""
    try:
        response = secrets.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        return ConnectWiseCredentials(**secret_data)
    except Exception as e:
        raise Exception(f"Failed to get credentials from {secret_name}: {str(e)}")


def process_table(
    config: Config,
    tenant_config: TenantConfig,
    credentials: ConnectWiseCredentials,
    table_name: str,
    logger: PipelineLogger
) -> Dict[str, Any]:
    """Process a single table for a tenant."""
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting processing for table {table_name}")
        
        # Get last updated timestamp
        last_updated = get_last_updated_timestamp(config, tenant_config.tenant_id, table_name)
        
        # Fetch data from ConnectWise API
        raw_data = fetch_connectwise_data(
            tenant_config=tenant_config,
            credentials=credentials,
            table_name=table_name,
            last_updated=last_updated,
            config=config,
            logger=logger
        )
        
        if not raw_data:
            logger.info(f"No new data found for table {table_name}")
            return {
                'tenant_id': tenant_config.tenant_id,
                'table_name': table_name,
                'status': 'success',
                'record_count': 0,
                'message': 'No new data'
            }
        
        # Flatten JSON data
        flattened_data = [flatten_json(record) for record in raw_data]
        
        # Filter out existing records
        new_records = filter_existing_records(
            config=config,
            tenant_id=tenant_config.tenant_id,
            table_name=table_name,
            records=flattened_data,
            logger=logger
        )
        
        if not new_records:
            logger.info(f"No new records after filtering for table {table_name}")
            return {
                'tenant_id': tenant_config.tenant_id,
                'table_name': table_name,
                'status': 'success',
                'record_count': 0,
                'message': 'No new records after filtering'
            }
        
        # Write to S3
        timestamp = get_timestamp()
        s3_key = get_s3_key(tenant_config.tenant_id, 'raw', 'connectwise', table_name, timestamp)
        
        write_parquet_to_s3(
            config=config,
            s3_key=s3_key,
            data=new_records,
            logger=logger
        )
        
        # Update last updated timestamp
        new_last_updated = get_max_last_updated(new_records)
        if new_last_updated:
            update_last_updated_timestamp(
                config=config,
                tenant_id=tenant_config.tenant_id,
                table_name=table_name,
                timestamp=new_last_updated
            )
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        logger.log_data_processing(
            operation="raw_ingestion",
            record_count=len(new_records),
            execution_time=execution_time
        )
        
        return {
            'tenant_id': tenant_config.tenant_id,
            'table_name': table_name,
            'status': 'success',
            'record_count': len(new_records),
            'execution_time': execution_time,
            's3_key': s3_key
        }
        
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"Failed to process table {table_name}: {str(e)}", execution_time=execution_time)
        raise


def get_last_updated_timestamp(config: Config, tenant_id: str, table_name: str) -> Optional[str]:
    """Get the last updated timestamp for a tenant/table combination."""
    try:
        response = dynamodb.get_item(
            TableName=config.last_updated_table,
            Key={
                'tenant_id': {'S': tenant_id},
                'table_name': {'S': table_name}
            }
        )
        
        if 'Item' in response:
            return response['Item']['last_updated']['S']
        return None
        
    except Exception:
        return None


def update_last_updated_timestamp(config: Config, tenant_id: str, table_name: str, timestamp: str):
    """Update the last updated timestamp for a tenant/table combination."""
    dynamodb.put_item(
        TableName=config.last_updated_table,
        Item={
            'tenant_id': {'S': tenant_id},
            'table_name': {'S': table_name},
            'last_updated': {'S': timestamp},
            'updated_at': {'S': get_timestamp()}
        }
    )


def fetch_connectwise_data(
    tenant_config: TenantConfig,
    credentials: ConnectWiseCredentials,
    table_name: str,
    last_updated: Optional[str],
    config: Config,
    logger: PipelineLogger
) -> List[Dict[str, Any]]:
    """Fetch data from ConnectWise API with pagination."""
    import requests
    
    all_records = []
    page = 1
    
    # Build API endpoint
    endpoint = f"v4_6_release/apis/3.0/{table_name}"
    url = tenant_config.get_api_url(endpoint)
    
    # Build headers
    headers = {
        'Authorization': credentials.get_auth_header(),
        'ClientId': credentials.client_id,
        'Content-Type': 'application/json'
    }
    
    # Build query parameters
    params = {
        'pageSize': config.page_size,
        'orderBy': 'id asc'
    }
    
    # Add conditions for incremental sync
    if last_updated:
        params['conditions'] = f"_info/lastUpdated > [{last_updated}]"
    
    while True:
        params['page'] = page
        
        try:
            logger.debug(f"Fetching page {page} from {url}")
            
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=config.api_timeout
            )
            
            logger.log_api_call(
                endpoint=endpoint,
                status_code=response.status_code,
                response_time=response.elapsed.total_seconds(),
                record_count=0
            )
            
            if response.status_code != 200:
                raise Exception(f"API request failed with status {response.status_code}: {response.text}")
            
            data = response.json()
            
            if not data:
                break
            
            all_records.extend(data)
            logger.debug(f"Fetched {len(data)} records from page {page}")
            
            # Check if we have more pages
            if len(data) < config.page_size:
                break
            
            page += 1
            
        except Exception as e:
            logger.error(f"Failed to fetch data from page {page}: {str(e)}")
            raise
    
    logger.info(f"Fetched {len(all_records)} total records from ConnectWise API")
    return all_records


def filter_existing_records(
    config: Config,
    tenant_id: str,
    table_name: str,
    records: List[Dict[str, Any]],
    logger: PipelineLogger
) -> List[Dict[str, Any]]:
    """Filter out records that already exist in S3."""
    try:
        # Get existing record IDs from S3
        existing_ids = get_existing_record_ids(config, tenant_id, table_name)
        
        if not existing_ids:
            logger.info("No existing records found, all records are new")
            return records
        
        # Filter out existing records
        new_records = [record for record in records if record.get('id') not in existing_ids]
        
        logger.info(
            f"Filtered records: {len(records)} total, {len(new_records)} new, {len(existing_ids)} existing"
        )
        
        return new_records
        
    except Exception as e:
        logger.warning(f"Failed to filter existing records, processing all: {str(e)}")
        return records


def get_existing_record_ids(config: Config, tenant_id: str, table_name: str) -> set:
    """Get existing record IDs from S3 Parquet files."""
    import pandas as pd
    
    try:
        prefix = f"{tenant_id}/raw/connectwise/{table_name}/"
        
        response = s3.list_objects_v2(
            Bucket=config.bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            return set()
        
        existing_ids = set()
        
        # Read IDs from recent files (limit to avoid memory issues)
        recent_files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[:10]
        
        for obj in recent_files:
            try:
                # Read Parquet file
                obj_response = s3.get_object(Bucket=config.bucket_name, Key=obj['Key'])
                df = pd.read_parquet(obj_response['Body'])
                
                if 'id' in df.columns:
                    existing_ids.update(df['id'].dropna().astype(str).tolist())
                    
            except Exception as e:
                # Log but continue with other files
                continue
        
        return existing_ids
        
    except Exception:
        return set()


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


def get_max_last_updated(records: List[Dict[str, Any]]) -> Optional[str]:
    """Get the maximum _info__lastUpdated value from records."""
    try:
        last_updated_values = [
            record.get('_info__lastUpdated')
            for record in records
            if record.get('_info__lastUpdated')
        ]
        
        if not last_updated_values:
            return None
        
        return max(last_updated_values)
        
    except Exception:
        return None