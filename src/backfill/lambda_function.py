"""
Backfill Lambda Function

This function handles historical data backfilling when a tenant connects a new service
for the first time. It performs full historical sync with configurable date ranges,
chunking, and progress tracking.

Key Features:
- Configurable backfill date ranges per service/endpoint
- Chunked processing to avoid Lambda timeouts
- Progress tracking and resumable backfills
- Rate limiting and error handling
- Automatic canonical transformation triggering
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple

# Add shared module to path
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))

from shared import (
    Config, TenantConfig, ConnectWiseCredentials,
    get_dynamodb_client, get_s3_client, get_secrets_client, get_lambda_client,
    PipelineLogger, flatten_json, get_timestamp, get_s3_key,
    validate_tenant_config, chunk_list, safe_get
)

# Initialize clients
dynamodb = get_dynamodb_client()
s3 = get_s3_client()
secrets = get_secrets_client()
lambda_client = get_lambda_client()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for backfill operations.
    
    Args:
        event: Lambda event containing:
            - tenant_id: Target tenant ID
            - service: Service name (e.g., 'connectwise')
            - table_name: Optional specific table to backfill
            - start_date: Optional start date (ISO format)
            - end_date: Optional end date (ISO format)
            - chunk_size_days: Optional chunk size in days (default: 30)
            - resume_job_id: Optional job ID to resume
        context: Lambda context
        
    Returns:
        Execution summary
    """
    logger = PipelineLogger("backfill")
    config = Config.from_environment()
    
    try:
        logger.info("Starting backfill operation", execution_id=context.aws_request_id)
        
        # Parse event parameters
        tenant_id = event.get('tenant_id')
        service = event.get('service')
        table_name = event.get('table_name')
        start_date = event.get('start_date')
        end_date = event.get('end_date')
        chunk_size_days = event.get('chunk_size_days', 30)
        resume_job_id = event.get('resume_job_id')
        
        if not tenant_id or not service:
            raise ValueError("tenant_id and service are required")
        
        # Get or create backfill job
        if resume_job_id:
            job = get_backfill_job(config, resume_job_id)
            if not job:
                raise ValueError(f"Backfill job {resume_job_id} not found")
        else:
            job = create_backfill_job(
                config=config,
                tenant_id=tenant_id,
                service=service,
                table_name=table_name,
                start_date=start_date,
                end_date=end_date,
                chunk_size_days=chunk_size_days,
                logger=logger
            )
        
        # Process backfill job
        result = process_backfill_job(config, job, context, logger)
        
        logger.info("Backfill operation completed", job_id=job['job_id'])
        return result
        
    except Exception as e:
        logger.error(f"Backfill operation failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'message': 'Backfill operation failed',
                'error': str(e)
            }
        }


def create_backfill_job(
    config: Config,
    tenant_id: str,
    service: str,
    table_name: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    chunk_size_days: int,
    logger: PipelineLogger
) -> Dict[str, Any]:
    """Create a new backfill job with chunked date ranges."""
    
    # Get tenant configuration
    tenant_config = get_tenant_configuration(config, tenant_id)
    
    # Get service endpoint configuration
    endpoints = get_service_endpoints(config, tenant_id, service)
    
    # Filter endpoints if specific table requested
    if table_name:
        endpoints = {k: v for k, v in endpoints.items() if k == table_name}
    
    # Determine date range
    if not start_date:
        start_date = get_default_backfill_start_date(service)
    if not end_date:
        end_date = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    
    # Create date chunks
    chunks = create_date_chunks(start_date, end_date, chunk_size_days)
    
    # Create job record
    job_id = f"backfill_{tenant_id}_{service}_{get_timestamp().replace(':', '-')}"
    job = {
        'job_id': job_id,
        'tenant_id': tenant_id,
        'service': service,
        'table_name': table_name,
        'start_date': start_date,
        'end_date': end_date,
        'chunk_size_days': chunk_size_days,
        'status': 'running',
        'total_chunks': len(chunks) * len(endpoints),
        'completed_chunks': 0,
        'failed_chunks': 0,
        'chunks': [],
        'created_at': get_timestamp(),
        'updated_at': get_timestamp()
    }
    
    # Create individual chunk jobs
    for endpoint, endpoint_config in endpoints.items():
        for chunk_start, chunk_end in chunks:
            chunk = {
                'endpoint': endpoint,
                'start_date': chunk_start,
                'end_date': chunk_end,
                'status': 'pending',
                'attempts': 0,
                'max_attempts': 3
            }
            job['chunks'].append(chunk)
    
    # Save job to DynamoDB
    save_backfill_job(config, job)
    
    logger.info(
        f"Created backfill job {job_id}",
        total_chunks=job['total_chunks'],
        endpoints=list(endpoints.keys()),
        date_range=f"{start_date} to {end_date}"
    )
    
    return job


def process_backfill_job(
    config: Config,
    job: Dict[str, Any],
    context: Any,
    logger: PipelineLogger
) -> Dict[str, Any]:
    """Process chunks from a backfill job."""
    
    tenant_id = job['tenant_id']
    service = job['service']
    
    # Get tenant configuration and credentials
    tenant_config = get_tenant_configuration(config, tenant_id)
    credentials = get_service_credentials(service, tenant_config.secret_name)
    
    processed_chunks = 0
    failed_chunks = 0
    remaining_time = get_remaining_execution_time(context)
    
    # Process pending chunks
    for i, chunk in enumerate(job['chunks']):
        if chunk['status'] != 'pending':
            continue
        
        # Check remaining execution time (leave 30 seconds buffer)
        if remaining_time() < 30:
            logger.info("Approaching timeout, scheduling continuation")
            schedule_backfill_continuation(config, job['job_id'])
            break
        
        try:
            logger.info(
                f"Processing chunk {i+1}/{len(job['chunks'])}",
                endpoint=chunk['endpoint'],
                date_range=f"{chunk['start_date']} to {chunk['end_date']}"
            )
            
            # Process single chunk
            result = process_backfill_chunk(
                config=config,
                tenant_config=tenant_config,
                credentials=credentials,
                service=service,
                chunk=chunk,
                logger=logger
            )
            
            # Update chunk status
            chunk['status'] = 'completed'
            chunk['completed_at'] = get_timestamp()
            chunk['record_count'] = result.get('record_count', 0)
            processed_chunks += 1
            
        except Exception as e:
            chunk['attempts'] += 1
            chunk['last_error'] = str(e)
            chunk['last_attempt_at'] = get_timestamp()
            
            if chunk['attempts'] >= chunk['max_attempts']:
                chunk['status'] = 'failed'
                failed_chunks += 1
                logger.error(f"Chunk failed permanently: {str(e)}")
            else:
                logger.warning(f"Chunk failed, will retry: {str(e)}")
            
        # Update job progress
        job['completed_chunks'] = len([c for c in job['chunks'] if c['status'] == 'completed'])
        job['failed_chunks'] = len([c for c in job['chunks'] if c['status'] == 'failed'])
        job['updated_at'] = get_timestamp()
        
        # Save progress periodically
        if (i + 1) % 10 == 0:
            save_backfill_job(config, job)
    
    # Check if job is complete
    pending_chunks = [c for c in job['chunks'] if c['status'] == 'pending']
    if not pending_chunks:
        job['status'] = 'completed' if failed_chunks == 0 else 'completed_with_errors'
        job['completed_at'] = get_timestamp()
        
        # Trigger canonical transformation for completed tables
        trigger_canonical_transformation(config, tenant_id, job, logger)
    
    # Save final job state
    save_backfill_job(config, job)
    
    return {
        'statusCode': 200,
        'body': {
            'message': 'Backfill processing completed',
            'job_id': job['job_id'],
            'status': job['status'],
            'processed_chunks': processed_chunks,
            'failed_chunks': failed_chunks,
            'total_chunks': job['total_chunks'],
            'completion_percentage': round((job['completed_chunks'] / job['total_chunks']) * 100, 2)
        }
    }


def process_backfill_chunk(
    config: Config,
    tenant_config: TenantConfig,
    credentials: Any,
    service: str,
    chunk: Dict[str, Any],
    logger: PipelineLogger
) -> Dict[str, Any]:
    """Process a single backfill chunk."""
    
    endpoint = chunk['endpoint']
    start_date = chunk['start_date']
    end_date = chunk['end_date']
    
    # Fetch data from service API
    if service == 'connectwise':
        raw_data = fetch_connectwise_backfill_data(
            tenant_config=tenant_config,
            credentials=credentials,
            endpoint=endpoint,
            start_date=start_date,
            end_date=end_date,
            config=config,
            logger=logger
        )
    else:
        raise ValueError(f"Unsupported service for backfill: {service}")
    
    if not raw_data:
        return {'record_count': 0}
    
    # Flatten JSON data
    flattened_data = [flatten_json(record) for record in raw_data]
    
    # Add backfill metadata
    for record in flattened_data:
        record['_backfill_job_id'] = chunk.get('job_id')
        record['_backfill_chunk_start'] = start_date
        record['_backfill_chunk_end'] = end_date
        record['_backfill_processed_at'] = get_timestamp()
    
    # Write to S3
    timestamp = get_timestamp()
    s3_key = get_s3_key(tenant_config.tenant_id, 'raw', service, endpoint.replace('/', '_'), timestamp)
    
    write_parquet_to_s3(
        config=config,
        s3_key=s3_key,
        data=flattened_data,
        logger=logger
    )
    
    logger.info(f"Wrote {len(flattened_data)} records to {s3_key}")
    
    return {
        'record_count': len(flattened_data),
        's3_key': s3_key
    }


def fetch_connectwise_backfill_data(
    tenant_config: TenantConfig,
    credentials: ConnectWiseCredentials,
    endpoint: str,
    start_date: str,
    end_date: str,
    config: Config,
    logger: PipelineLogger
) -> List[Dict[str, Any]]:
    """Fetch historical data from ConnectWise API for backfill."""
    import requests
    
    all_records = []
    page = 1
    
    # Build API endpoint
    api_endpoint = f"v4_6_release/apis/3.0/{endpoint}"
    url = tenant_config.get_api_url(api_endpoint)
    
    # Build headers
    headers = {
        'Authorization': credentials.get_auth_header(),
        'ClientId': credentials.client_id,
        'Content-Type': 'application/json'
    }
    
    # Build query parameters for date range
    params = {
        'pageSize': config.page_size,
        'orderBy': 'id asc',
        'conditions': f"_info/lastUpdated >= [{start_date}] AND _info/lastUpdated < [{end_date}]"
    }
    
    while True:
        params['page'] = page
        
        try:
            logger.debug(f"Fetching backfill page {page} from {url}")
            
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=config.api_timeout
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
            logger.error(f"Failed to fetch backfill data from page {page}: {str(e)}")
            raise
    
    logger.info(f"Fetched {len(all_records)} total records for backfill")
    return all_records


def create_date_chunks(start_date: str, end_date: str, chunk_size_days: int) -> List[Tuple[str, str]]:
    """Create date range chunks for backfill processing."""
    
    start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    chunks = []
    current_start = start_dt
    
    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=chunk_size_days), end_dt)
        
        chunks.append((
            current_start.isoformat().replace('+00:00', 'Z'),
            current_end.isoformat().replace('+00:00', 'Z')
        ))
        
        current_start = current_end
    
    return chunks


def get_default_backfill_start_date(service: str) -> str:
    """Get default backfill start date for a service."""
    # Default to 2 years ago for most services
    default_days = {
        'connectwise': 730,  # 2 years
        'servicenow': 365,   # 1 year
        'salesforce': 1095   # 3 years
    }
    
    days_back = default_days.get(service, 730)
    start_date = datetime.now(timezone.utc) - timedelta(days=days_back)
    return start_date.isoformat().replace('+00:00', 'Z')


def save_backfill_job(config: Config, job: Dict[str, Any]):
    """Save backfill job to DynamoDB."""
    
    # Convert job to DynamoDB format
    item = {
        'job_id': {'S': job['job_id']},
        'tenant_id': {'S': job['tenant_id']},
        'service': {'S': job['service']},
        'status': {'S': job['status']},
        'total_chunks': {'N': str(job['total_chunks'])},
        'completed_chunks': {'N': str(job['completed_chunks'])},
        'failed_chunks': {'N': str(job['failed_chunks'])},
        'created_at': {'S': job['created_at']},
        'updated_at': {'S': job['updated_at']},
        'job_data': {'S': json.dumps(job)}
    }
    
    if job.get('table_name'):
        item['table_name'] = {'S': job['table_name']}
    if job.get('completed_at'):
        item['completed_at'] = {'S': job['completed_at']}
    
    dynamodb.put_item(
        TableName=f"BackfillJobs-{config.environment}",
        Item=item
    )


def get_backfill_job(config: Config, job_id: str) -> Optional[Dict[str, Any]]:
    """Get backfill job from DynamoDB."""
    
    try:
        response = dynamodb.get_item(
            TableName=f"BackfillJobs-{config.environment}",
            Key={'job_id': {'S': job_id}}
        )
        
        if 'Item' not in response:
            return None
        
        job_data = response['Item']['job_data']['S']
        return json.loads(job_data)
        
    except Exception:
        return None


def schedule_backfill_continuation(config: Config, job_id: str):
    """Schedule continuation of backfill job."""
    
    payload = {
        'resume_job_id': job_id
    }
    
    lambda_client.invoke(
        FunctionName=f"avesa-backfill-{config.environment}",
        InvocationType='Event',  # Async invocation
        Payload=json.dumps(payload)
    )


def trigger_canonical_transformation(
    config: Config,
    tenant_id: str,
    job: Dict[str, Any],
    logger: PipelineLogger
):
    """Trigger canonical transformation for backfilled data."""
    
    # Get unique tables from completed chunks
    completed_tables = set()
    for chunk in job['chunks']:
        if chunk['status'] == 'completed':
            completed_tables.add(chunk['endpoint'])
    
    # Trigger transformation for each table
    for table in completed_tables:
        payload = {
            'tenant_id': tenant_id,
            'table_name': table,
            'backfill_mode': True
        }
        
        try:
            lambda_client.invoke(
                FunctionName=f"avesa-canonical-transform-{config.environment}",
                InvocationType='Event',
                Payload=json.dumps(payload)
            )
            logger.info(f"Triggered canonical transformation for {table}")
        except Exception as e:
            logger.error(f"Failed to trigger canonical transformation for {table}: {str(e)}")


# Helper functions (reuse from existing modules)
def get_tenant_configuration(config: Config, tenant_id: str) -> TenantConfig:
    """Get tenant configuration from DynamoDB."""
    response = dynamodb.get_item(
        TableName=config.tenant_services_table,
        Key={'tenant_id': {'S': tenant_id}}
    )
    
    if 'Item' not in response:
        raise ValueError(f"Tenant {tenant_id} not found")
    
    item = response['Item']
    tenant_data = {
        'tenant_id': item['tenant_id']['S'],
        'connectwise_url': item['connectwise_url']['S'],
        'secret_name': item['secret_name']['S'],
        'enabled': item.get('enabled', {'BOOL': True})['BOOL'],
        'tables': [t['S'] for t in item.get('tables', {'L': []})['L']],
        'custom_config': json.loads(item.get('custom_config', {'S': '{}'})['S'])
    }
    return validate_tenant_config(tenant_data)


def get_service_endpoints(config: Config, tenant_id: str, service: str) -> Dict[str, Any]:
    """Get service endpoint configuration."""
    try:
        # Try tenant-specific config first
        endpoint_key = f"{tenant_id}/mappings/integrations/{service}_endpoints.json"
        response = s3.get_object(Bucket=config.bucket_name, Key=endpoint_key)
        endpoint_config = json.loads(response['Body'].read().decode('utf-8'))
    except:
        # Fall back to default config
        endpoint_key = f"mappings/integrations/{service}_endpoints.json"
        response = s3.get_object(Bucket=config.bucket_name, Key=endpoint_key)
        endpoint_config = json.loads(response['Body'].read().decode('utf-8'))
    
    # Return only enabled endpoints
    enabled_endpoints = {}
    for endpoint, config_data in endpoint_config.get('endpoints', {}).items():
        if config_data.get('enabled', False):
            enabled_endpoints[endpoint] = config_data
    
    return enabled_endpoints


def get_service_credentials(service: str, secret_name: str) -> Any:
    """Get service credentials from AWS Secrets Manager."""
    response = secrets.get_secret_value(SecretId=secret_name)
    secret_data = json.loads(response['SecretString'])
    
    if service == 'connectwise':
        return ConnectWiseCredentials(**secret_data)
    else:
        raise ValueError(f"Unsupported service: {service}")


def get_remaining_execution_time(context: Any) -> callable:
    """Get function to check remaining execution time."""
    def remaining_time():
        return (context.get_remaining_time_in_millis() / 1000) if context else 900
    return remaining_time


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