#!/usr/bin/env python3
"""
Backfill Lambda Function

This function handles the actual backfill processing for historical data
when tenants connect new services.
"""

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
import logging
import time

# Import shared components
from shared import AWSClientFactory, validate_connectwise_credentials
from shared.environment import Environment

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get environment configuration using proper pattern
env_name = os.environ.get('ENVIRONMENT', 'dev')
config = Environment.get_config(env_name)

# Initialize AWS clients using shared factory
factory = AWSClientFactory()
clients = factory.get_client_bundle(['dynamodb', 's3', 'secretsmanager', 'lambda'])
dynamodb = clients['dynamodb']
s3_client = clients['s3']
secrets_client = clients['secretsmanager']
lambda_client = clients['lambda']

# Environment variables
BUCKET_NAME = os.environ.get('BUCKET_NAME')
TENANT_SERVICES_TABLE = os.environ.get('TENANT_SERVICES_TABLE')
LAST_UPDATED_TABLE = os.environ.get('LAST_UPDATED_TABLE')
BACKFILL_JOBS_TABLE = os.environ.get('BACKFILL_JOBS_TABLE')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Main Lambda handler for backfill processing.
    
    Processes historical data for a specific tenant/service/table combination.
    """
    try:
        logger.info(f"Received backfill event: {json.dumps(event)}")
        
        # Extract parameters
        tenant_id = event.get('tenant_id')
        service = event.get('service')
        table_name = event.get('table_name')
        start_date = event.get('start_date')
        end_date = event.get('end_date')
        chunk_size_days = event.get('chunk_size_days', 30)
        resume_job_id = event.get('resume_job_id')
        
        if not tenant_id or not service:
            raise ValueError("tenant_id and service are required")
        
        # Process the backfill
        result = process_backfill(
            tenant_id=tenant_id,
            service=service,
            table_name=table_name,
            start_date=start_date,
            end_date=end_date,
            chunk_size_days=chunk_size_days,
            resume_job_id=resume_job_id
        )
        
        return {
            'statusCode': 200,
            'body': result
        }
        
    except Exception as e:
        logger.error(f"Error in backfill processing: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Failed to process backfill'
            }
        }


def process_backfill(
    tenant_id: str,
    service: str,
    table_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    chunk_size_days: int = 30,
    resume_job_id: Optional[str] = None
) -> Dict[str, Any]:
    """Process backfill for the specified parameters."""
    
    logger.info(f"Processing backfill for tenant {tenant_id}, service {service}")
    
    # Get service configuration
    service_config = get_service_config(tenant_id, service)
    if not service_config:
        raise ValueError(f"No configuration found for tenant {tenant_id}, service {service}")
    
    logger.info(f"Retrieved service config: {json.dumps(service_config, default=str)}")
    
    # Get and validate service credentials
    credentials = get_service_credentials(service_config, service)
    if not credentials:
        raise ValueError(f"No credentials found for tenant {tenant_id}, service {service}")
    
    # Validate credentials based on service type
    if service == 'connectwise':
        if not validate_connectwise_credentials(credentials):
            raise ValueError(f"Invalid ConnectWise credentials for tenant {tenant_id}")
        logger.info("ConnectWise credentials validated successfully")
    
    logger.info(f"Service credentials retrieved and validated for {service}")
    
    # Determine tables to backfill
    tables_to_process = []
    if table_name:
        tables_to_process = [table_name]
    else:
        # Get default tables for the service
        tables_to_process = get_default_tables_for_service(service)
    
    logger.info(f"Tables to process: {tables_to_process}")
    
    # Set date range
    if not end_date:
        end_date = datetime.now(timezone.utc).isoformat()
    
    if not start_date:
        # Default to 2 years back
        start_dt = datetime.now(timezone.utc) - timedelta(days=730)
        start_date = start_dt.isoformat()
    
    # Process each table
    processed_tables = []
    total_records = 0
    errors = []
    
    for table in tables_to_process:
        try:
            logger.info(f"Processing table: {table}")
            
            table_result = process_table_backfill(
                tenant_id=tenant_id,
                service=service,
                table_name=table,
                service_config=service_config,
                start_date=start_date,
                end_date=end_date,
                chunk_size_days=chunk_size_days
            )
            
            processed_tables.append(table_result)
            total_records += table_result.get('records_processed', 0)
            
        except Exception as e:
            error_msg = f"Error processing table {table}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
    
    # Update job status
    if resume_job_id:
        update_job_status(
            job_id=resume_job_id,
            status='completed' if not errors else 'completed_with_errors',
            processed_tables=len(processed_tables),
            total_records=total_records,
            errors=errors
        )
    
    # Trigger canonical transformation for processed data
    if processed_tables:
        trigger_canonical_transformation(tenant_id, service, processed_tables)
    
    return {
        'status': 'completed' if not errors else 'completed_with_errors',
        'tenant_id': tenant_id,
        'service': service,
        'processed_tables': processed_tables,
        'total_records': total_records,
        'errors': errors,
        'job_id': resume_job_id
    }


def get_service_config(tenant_id: str, service: str) -> Optional[Dict[str, Any]]:
    """Get service configuration for the tenant."""
    try:
        tenant_services_table = dynamodb.Table(TENANT_SERVICES_TABLE)
        
        response = tenant_services_table.get_item(
            Key={
                'tenant_id': tenant_id,
                'service': service
            }
        )
        
        return response.get('Item')
        
    except Exception as e:
        logger.error(f"Error getting service config: {str(e)}")
        return None


def get_service_credentials(service_config: Dict[str, Any], service: str) -> Optional[Dict[str, Any]]:
    """
    Get service credentials from AWS Secrets Manager with support for nested structures.
    
    Args:
        service_config: Service configuration from DynamoDB
        service: Service name (e.g., 'connectwise')
        
    Returns:
        Credentials dictionary or None if not found
    """
    try:
        secret_name = service_config.get('secret_name')
        if not secret_name:
            logger.error(f"No secret_name found in service config for {service}")
            return None
        
        logger.info(f"Retrieving secret: {secret_name}")
        
        # Get secret from AWS Secrets Manager
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_string = response['SecretString']
        secret_data = json.loads(secret_string)
        
        logger.info(f"Secret structure keys: {list(secret_data.keys())}")
        
        # Handle nested credential structure
        # First, try to find credentials under the service name key (e.g., 'connectwise')
        if service in secret_data:
            logger.info(f"Found nested credentials under '{service}' key")
            return secret_data[service]
        
        # Fallback: check for common nested structures
        nested_keys = ['credentials', 'auth', 'api_credentials']
        for nested_key in nested_keys:
            if nested_key in secret_data:
                nested_data = secret_data[nested_key]
                if service in nested_data:
                    logger.info(f"Found nested credentials under '{nested_key}.{service}' key")
                    return nested_data[service]
        
        # Fallback: assume flat structure (for backward compatibility)
        # Check if the secret contains credential fields directly
        credential_fields = ['company_id', 'public_key', 'private_key', 'client_id', 'api_base_url']
        if any(field in secret_data for field in credential_fields):
            logger.info("Using flat credential structure")
            return secret_data
        
        logger.error(f"No valid credential structure found in secret for service {service}")
        logger.error(f"Available keys: {list(secret_data.keys())}")
        return None
        
    except Exception as e:
        logger.error(f"Error retrieving service credentials: {str(e)}")
        return None


# validate_connectwise_credentials is now imported from shared module


def get_default_tables_for_service(service: str) -> List[str]:
    """Get default tables for a service."""
    service_tables = {
        'connectwise': ['service/tickets', 'time/entries', 'company/companies', 'company/contacts'],
        'servicenow': ['incident', 'change_request', 'problem', 'user'],
        'salesforce': ['Account', 'Contact', 'Opportunity', 'Case', 'Lead']
    }
    
    return service_tables.get(service, [])


def process_table_backfill(
    tenant_id: str,
    service: str,
    table_name: str,
    service_config: Dict[str, Any],
    start_date: str,
    end_date: str,
    chunk_size_days: int
) -> Dict[str, Any]:
    """Process backfill for a specific table."""
    
    logger.info(f"🔄 Processing backfill for table {table_name}")
    logger.info(f"   Date range: {start_date} to {end_date}")
    logger.info(f"   Chunk size: {chunk_size_days} days")
    
    # Determine the appropriate ingestion Lambda
    ingestion_lambda_name = f"avesa-{service}-ingestion-{ENVIRONMENT}"
    logger.info(f"   Target ingestion lambda: {ingestion_lambda_name}")
    
    # Note: Skipping lambda existence check due to permission constraints
    # The lambda invocation will fail gracefully if the function doesn't exist
    logger.info(f"🎯 Target ingestion lambda: {ingestion_lambda_name}")
    
    # Create chunks based on date range with better parsing
    try:
        # Handle various date formats
        if start_date.endswith('Z'):
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start_dt = datetime.fromisoformat(start_date)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
                
        if end_date.endswith('Z'):
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end_dt = datetime.fromisoformat(end_date)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
                
        logger.info(f"   Parsed dates: {start_dt} to {end_dt}")
        
    except Exception as e:
        error_msg = f"❌ Error parsing dates: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    chunks = create_date_chunks(start_dt, end_dt, chunk_size_days)
    logger.info(f"   Created {len(chunks)} date chunks for processing")
    
    records_processed = 0
    chunks_processed = 0
    chunk_errors = []
    
    for chunk_start, chunk_end in chunks:
        try:
            # Prepare payload for ingestion Lambda
            # Use the same format as the regular ingestion lambda expects
            payload = {
                'tenant_id': tenant_id,
                'service_name': service,
                'table_name': table_name,
                'start_date': chunk_start.isoformat(),
                'end_date': chunk_end.isoformat(),
                'backfill_mode': True,
                'force_full_sync': True,
                'chunk_number': chunks_processed + 1,
                'total_chunks': len(chunks)
            }
            
            logger.info(f"Processing chunk {chunks_processed + 1}/{len(chunks)}: {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            logger.info(f"Payload: {json.dumps(payload, default=str)}")
            
            # Invoke ingestion Lambda
            try:
                response = lambda_client.invoke(
                    FunctionName=ingestion_lambda_name,
                    InvocationType='RequestResponse',
                    Payload=json.dumps(payload, default=str)
                )
                
                # Parse response
                response_payload = json.loads(response['Payload'].read().decode('utf-8'))
                
                logger.info(f"Ingestion lambda response: {json.dumps(response_payload, default=str)}")
                
                if response_payload.get('statusCode') == 200:
                    body = response_payload.get('body', {})
                    # Handle both string and dict body formats
                    if isinstance(body, str):
                        body = json.loads(body)
                    
                    chunk_records = body.get('records_processed', 0)
                    records_processed += chunk_records
                    chunks_processed += 1
                    logger.info(f"✅ Chunk processed successfully: {chunk_records} records")
                    
                    # Log additional details if available
                    if 'details' in body:
                        logger.info(f"   Details: {body['details']}")
                else:
                    error_msg = f"❌ Chunk processing failed with status {response_payload.get('statusCode')}: {response_payload}"
                    logger.error(error_msg)
                    chunk_errors.append(error_msg)
                    
            except Exception as invoke_error:
                error_msg = f"❌ Error invoking ingestion lambda: {str(invoke_error)}"
                logger.error(error_msg)
                chunk_errors.append(error_msg)
                # Continue with next chunk instead of failing completely
            
            # Add small delay between chunks to avoid rate limiting
            time.sleep(1)
            
        except Exception as e:
            error_msg = f"Error processing chunk {chunk_start} to {chunk_end}: {str(e)}"
            logger.error(error_msg)
            chunk_errors.append(error_msg)
    
    # Log summary
    logger.info(f"📊 BACKFILL SUMMARY for {table_name}:")
    logger.info(f"   ✅ Chunks processed: {chunks_processed}/{len(chunks)}")
    logger.info(f"   📊 Records processed: {records_processed}")
    logger.info(f"   ❌ Errors: {len(chunk_errors)}")
    
    if chunk_errors:
        logger.error(f"   Error details:")
        for i, error in enumerate(chunk_errors[:5]):  # Show first 5 errors
            logger.error(f"     {i+1}. {error}")
    
    # Store backfill metadata in S3
    store_backfill_metadata(
        tenant_id=tenant_id,
        service=service,
        table_name=table_name,
        start_date=start_date,
        end_date=end_date,
        records_processed=records_processed,
        chunks_processed=chunks_processed
    )
    
    return {
        'table_name': table_name,
        'records_processed': records_processed,
        'chunks_processed': chunks_processed,
        'total_chunks': len(chunks),
        'start_date': start_date,
        'end_date': end_date,
        'errors': chunk_errors,
        'success_rate': f"{chunks_processed}/{len(chunks)}" if len(chunks) > 0 else "0/0"
    }


def create_date_chunks(start_dt: datetime, end_dt: datetime, chunk_size_days: int) -> List[tuple]:
    """Create date chunks for processing."""
    chunks = []
    current_start = start_dt
    
    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=chunk_size_days), end_dt)
        chunks.append((current_start, current_end))
        current_start = current_end
    
    return chunks


def store_backfill_metadata(
    tenant_id: str,
    service: str,
    table_name: str,
    start_date: str,
    end_date: str,
    records_processed: int,
    chunks_processed: int
) -> None:
    """Store backfill metadata in S3."""
    try:
        metadata = {
            'tenant_id': tenant_id,
            'service': service,
            'table_name': table_name,
            'start_date': start_date,
            'end_date': end_date,
            'records_processed': records_processed,
            'chunks_processed': chunks_processed,
            'backfill_completed_at': datetime.now(timezone.utc).isoformat(),
            'environment': ENVIRONMENT
        }
        
        # Store in S3
        key = f"{tenant_id}/backfill_metadata/{service}/{table_name}/backfill_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Stored backfill metadata: {key}")
        
    except Exception as e:
        logger.error(f"Error storing backfill metadata: {str(e)}")


def update_job_status(
    job_id: str,
    status: str,
    processed_tables: int = 0,
    total_records: int = 0,
    errors: List[str] = None
) -> None:
    """Update backfill job status."""
    try:
        backfill_jobs_table = dynamodb.Table(BACKFILL_JOBS_TABLE)
        
        update_expression = 'SET job_status = :status, updated_at = :updated, processed_tables = :tables, total_records = :records'
        expression_values = {
            ':status': status,
            ':updated': datetime.now(timezone.utc).isoformat(),
            ':tables': processed_tables,
            ':records': total_records
        }
        
        if errors:
            update_expression += ', errors = :errors'
            expression_values[':errors'] = errors
        
        backfill_jobs_table.update_item(
            Key={'job_id': job_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values
        )
        
        logger.info(f"Updated job status: {job_id} -> {status}")
        
    except Exception as e:
        logger.error(f"Error updating job status: {str(e)}")


def trigger_canonical_transformation(tenant_id: str, service: str, processed_tables: List[Dict[str, Any]]) -> None:
    """Trigger canonical transformation for backfilled data."""
    try:
        for table_info in processed_tables:
            table_name = table_info['table_name']
            
            # Extract table name for lambda naming (remove service prefix if present)
            # Convert table names like 'service/tickets' to 'tickets'
            clean_table_name = table_name.split('/')[-1] if '/' in table_name else table_name
            
            # Use table-specific canonical transform lambda naming
            canonical_lambda_name = f"avesa-canonical-transform-{clean_table_name}-{ENVIRONMENT}"
            
            # Prepare canonical transformation payload
            payload = {
                'tenant_id': tenant_id,
                'service_name': service,
                'table_name': table_name,
                'backfill_mode': True,
                'force_reprocess': True
            }
            
            logger.info(f"Invoking table-specific canonical lambda: {canonical_lambda_name}")
            
            # Invoke canonical transformation Lambda
            lambda_client.invoke(
                FunctionName=canonical_lambda_name,
                InvocationType='Event',  # Async invocation
                Payload=json.dumps(payload)
            )
            
            logger.info(f"Triggered canonical transformation for {table_name} using {canonical_lambda_name}")
            
    except Exception as e:
        logger.error(f"Error triggering canonical transformation: {str(e)}")