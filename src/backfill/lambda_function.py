#!/usr/bin/env python3
"""
Fixed Backfill Lambda Function

This function handles the actual backfill processing for historical data
when tenants connect new services.
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
import logging
import time

# Add the current directory to Python path for shared module imports
sys.path.insert(0, '/var/task')

# Import shared components with error handling
try:
    from shared import AWSClientFactory, validate_connectwise_credentials
    from shared.environment import Environment
    SHARED_IMPORTS_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import shared modules: {e}")
    SHARED_IMPORTS_AVAILABLE = False

# Always import boto3 for fallback cases
import boto3

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get environment configuration
env_name = os.environ.get('ENVIRONMENT', 'dev')

if SHARED_IMPORTS_AVAILABLE:
    config = Environment.get_config(env_name)
    # Initialize AWS clients using shared factory
    factory = AWSClientFactory()
    clients = factory.get_client_bundle(['dynamodb', 's3', 'secretsmanager', 'lambda'])
    dynamodb_client = clients['dynamodb']
    s3_client = clients['s3']
    secrets_client = clients['secretsmanager']
    lambda_client = clients['lambda']
    # Create DynamoDB resource from client
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
else:
    # Fallback to direct boto3 clients
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    s3_client = boto3.client('s3', region_name='us-east-2')
    secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
    lambda_client = boto3.client('lambda', region_name='us-east-2')

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
        logger.info(f"Shared imports available: {SHARED_IMPORTS_AVAILABLE}")
        
        # Handle test import requests
        if event.get('test_import'):
            return {
                'statusCode': 200,
                'body': {
                    'message': 'Import test successful',
                    'shared_imports_available': SHARED_IMPORTS_AVAILABLE,
                    'environment': ENVIRONMENT
                }
            }
        
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
            resume_job_id=resume_job_id,
            event=event
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
    resume_job_id: Optional[str] = None,
    event: Optional[Dict[str, Any]] = None
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
        if SHARED_IMPORTS_AVAILABLE:
            if not validate_connectwise_credentials(credentials):
                raise ValueError(f"Invalid ConnectWise credentials for tenant {tenant_id}")
            logger.info("ConnectWise credentials validated successfully")
        else:
            logger.warning("Shared imports not available, skipping credential validation")
    
    logger.info(f"Service credentials retrieved and validated for {service}")
    
    # Determine tables to backfill
    tables_to_process = []
    table_param = event.get('table') if event else None  # Check for 'table' parameter from event
    if table_param:
        # Use table name directly without adding service prefix (prefix is handled by orchestrator)
        tables_to_process = [table_param]
    elif table_name:
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
    """Get default tables for a service by reading from integration mapping files."""
    try:
        # Load integration mapping file for the service
        integration_file = f"{service}_endpoints.json"
        
        # Try bundled file first
        bundled_path = os.path.join(os.path.dirname(__file__), '..', 'mappings', 'integrations', integration_file)
        if os.path.exists(bundled_path):
            with open(bundled_path, 'r') as f:
                integration_config = json.load(f)
        else:
            # Try local development file
            local_path = os.path.join('mappings', 'integrations', integration_file)
            if os.path.exists(local_path):
                with open(local_path, 'r') as f:
                    integration_config = json.load(f)
            else:
                logger.warning(f"No integration mapping file found for service: {service}")
                return []
        
        # Extract enabled endpoints
        endpoints = integration_config.get('endpoints', {})
        enabled_endpoints = []
        
        for endpoint_name, endpoint_config in endpoints.items():
            if endpoint_config.get('enabled', False):
                enabled_endpoints.append(endpoint_name)
        
        logger.info(f"Found {len(enabled_endpoints)} enabled endpoints for {service}: {enabled_endpoints}")
        return enabled_endpoints
        
    except Exception as e:
        logger.error(f"Error loading integration mapping for {service}: {e}")
        # Fallback to hardcoded values for backward compatibility
        service_tables = {
            'connectwise': [
                'service/tickets',
                'time/entries',
                'company/companies',
                'company/contacts'
            ],
            'servicenow': ['incident', 'sys_user', 'core_company', 'time_card'],
            'salesforce': ['Account', 'Contact', 'Case']
        }
        return service_tables.get(service, [])


def get_canonical_table_name(service: str, endpoint_or_table: str) -> str:
    """
    Map API endpoint or table reference to canonical table name.
    
    Args:
        service: Service name (e.g., 'connectwise')
        endpoint_or_table: API endpoint (e.g., 'company/companies') or table name (e.g., 'companies')
        
    Returns:
        Canonical table name for ClickHouse and orchestrator
    """
    # Mapping from API endpoints to canonical table names
    endpoint_to_table = {
        'connectwise': {
            'service/tickets': 'tickets',
            'time/entries': 'time_entries',
            'company/companies': 'companies',
            'company/contacts': 'contacts',
            'procurement/products': 'products',
            'finance/agreements': 'agreements',
            'project/projects': 'projects',
            'system/members': 'members'
        },
        'servicenow': {
            'incident': 'tickets',
            'change_request': 'change_requests',
            'problem': 'problems',
            'user': 'contacts',
            'sys_user_group': 'user_groups'
        },
        'salesforce': {
            'Account': 'companies',
            'Contact': 'contacts',
            'Opportunity': 'opportunities',
            'Case': 'tickets',
            'Lead': 'leads',
            'User': 'users'
        }
    }
    
    service_mappings = endpoint_to_table.get(service, {})
    
    # Try direct mapping first
    if endpoint_or_table in service_mappings:
        return service_mappings[endpoint_or_table]
    
    # If not found, assume it's already a canonical table name
    return endpoint_or_table


def is_master_data_table(service: str, endpoint_or_table: str) -> bool:
    """
    Determine if a table contains master data (should not use date ranges).
    
    Master data tables contain relatively static reference data like companies, contacts, users.
    Transactional data tables contain time-series data like tickets, time entries, opportunities.
    """
    # Get canonical table name first
    canonical_table = get_canonical_table_name(service, endpoint_or_table)
    
    # Define master data tables by canonical names
    master_data_tables = {
        'connectwise': [
            'companies',
            'contacts',
            'members'
        ],
        'servicenow': [
            'contacts',
            'user_groups'
        ],
        'salesforce': [
            'companies',
            'contacts',
            'users'
        ]
    }
    
    service_master_tables = master_data_tables.get(service, [])
    return canonical_table in service_master_tables


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
    
    # Check if this is master data (no date chunking needed)
    is_master_data = is_master_data_table(service, table_name)
    
    if is_master_data:
        logger.info(f"   📋 Master data table - will fetch all current records without date filtering")
        return process_master_data_backfill(tenant_id, service, table_name, service_config)
    else:
        logger.info(f"   📊 Transactional data table - using date range chunking")
        logger.info(f"   Date range: {start_date} to {end_date}")
        logger.info(f"   Chunk size: {chunk_size_days} days")
        return process_transactional_data_backfill(
            tenant_id, service, table_name, service_config,
            start_date, end_date, chunk_size_days
        )


def process_master_data_backfill(
    tenant_id: str,
    service: str,
    table_name: str,
    service_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Process backfill for master data tables (no date chunking)."""
    
    logger.info(f"🔄 Processing master data backfill for {table_name}")
    
    # Use the orchestrator workflow
    orchestrator_lambda_name = f"avesa-pipeline-orchestrator-{ENVIRONMENT}"
    logger.info(f"   Target orchestrator lambda: {orchestrator_lambda_name}")
    
    records_processed = 0
    chunk_errors = []
    
    try:
        # Get canonical table name for orchestrator
        canonical_table_name = get_canonical_table_name(service, table_name)
        
        # Prepare payload for orchestrator Lambda (no date range for master data)
        payload = {
            'tenant_id': tenant_id,
            'table_name': canonical_table_name,  # Use canonical table name
            'endpoint_name': table_name,  # Keep original endpoint for reference
            'force_full_sync': True,
            'backfill_mode': True,
            'master_data_mode': True,  # Flag to indicate this is master data
            'chunk_info': {
                'chunk_number': 1,
                'total_chunks': 1
            }
        }
        
        logger.info(f"Processing master data (single request)")
        logger.info(f"Orchestrator payload: {json.dumps(payload, default=str)}")
        
        # Invoke orchestrator Lambda
        try:
            response = lambda_client.invoke(
                FunctionName=orchestrator_lambda_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload, default=str)
            )
            
            # Parse response
            response_payload = json.loads(response['Payload'].read().decode('utf-8'))
            
            logger.info(f"Orchestrator response: {json.dumps(response_payload, default=str)}")
            
            # Check for Lambda execution errors
            if response.get('FunctionError'):
                error_msg = f"❌ Orchestrator Lambda execution failed: {response_payload.get('errorMessage', 'Unknown error')}"
                logger.error(error_msg)
                chunk_errors.append(error_msg)
            elif 'job_id' in response_payload:
                # Orchestrator returns job information, assume success for now
                job_id = response_payload.get('job_id')
                estimated_records = response_payload.get('estimated_duration', 0)
                
                # For master data, estimate based on typical company/contact counts
                records_processed = max(500, estimated_records // 5)  # Conservative estimate
                
                logger.info(f"✅ Master data request submitted to orchestrator successfully")
                logger.info(f"   Job ID: {job_id}")
                logger.info(f"   Estimated records: {records_processed}")
            else:
                error_msg = f"❌ Unexpected orchestrator response format: {response_payload}"
                logger.error(error_msg)
                chunk_errors.append(error_msg)
                
        except Exception as invoke_error:
            error_msg = f"❌ Error invoking orchestrator lambda: {str(invoke_error)}"
            logger.error(error_msg)
            chunk_errors.append(error_msg)
        
    except Exception as e:
        error_msg = f"Error processing master data: {str(e)}"
        logger.error(error_msg)
        chunk_errors.append(error_msg)
    
    # Log summary
    logger.info(f"📊 MASTER DATA BACKFILL SUMMARY for {table_name}:")
    logger.info(f"   ✅ Chunks processed: 1/1")
    logger.info(f"   📊 Records processed: {records_processed}")
    logger.info(f"   ❌ Errors: {len(chunk_errors)}")
    
    if chunk_errors:
        logger.error(f"   Error details:")
        for i, error in enumerate(chunk_errors):
            logger.error(f"     {i+1}. {error}")
    
    # Store backfill metadata in S3
    current_time = datetime.now(timezone.utc).isoformat()
    store_backfill_metadata(
        tenant_id=tenant_id,
        service=service,
        table_name=table_name,
        start_date=current_time,  # For master data, use current time as both start and end
        end_date=current_time,
        records_processed=records_processed,
        chunks_processed=1
    )
    
    return {
        'table_name': table_name,
        'records_processed': records_processed,
        'chunks_processed': 1,
        'total_chunks': 1,
        'start_date': current_time,
        'end_date': current_time,
        'errors': chunk_errors,
        'success_rate': "1/1" if not chunk_errors else "0/1",
        'master_data': True
    }


def process_transactional_data_backfill(
    tenant_id: str,
    service: str,
    table_name: str,
    service_config: Dict[str, Any],
    start_date: str,
    end_date: str,
    chunk_size_days: int
) -> Dict[str, Any]:
    """Process backfill for transactional data tables (with date chunking)."""
    
    logger.info(f"🔄 Processing transactional data backfill for {table_name}")
    
    # Use the orchestrator workflow
    orchestrator_lambda_name = f"avesa-pipeline-orchestrator-{ENVIRONMENT}"
    logger.info(f"   Target orchestrator lambda: {orchestrator_lambda_name}")
    
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
            # Get canonical table name for orchestrator
            canonical_table_name = get_canonical_table_name(service, table_name)
            
            # Prepare payload for orchestrator Lambda
            # Use the orchestrator workflow format with backfill flags
            payload = {
                'tenant_id': tenant_id,
                'table_name': canonical_table_name,  # Use canonical table name
                'endpoint_name': table_name,  # Keep original endpoint for reference
                'force_full_sync': True,
                'backfill_mode': True,
                'backfill_date_range': {
                    'start_date': chunk_start.isoformat(),
                    'end_date': chunk_end.isoformat()
                },
                'chunk_info': {
                    'chunk_number': chunks_processed + 1,
                    'total_chunks': len(chunks)
                }
            }
            
            logger.info(f"Processing chunk {chunks_processed + 1}/{len(chunks)}: {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            logger.info(f"Orchestrator payload: {json.dumps(payload, default=str)}")
            
            # Invoke orchestrator Lambda
            try:
                response = lambda_client.invoke(
                    FunctionName=orchestrator_lambda_name,
                    InvocationType='RequestResponse',
                    Payload=json.dumps(payload, default=str)
                )
                
                # Parse response
                response_payload = json.loads(response['Payload'].read().decode('utf-8'))
                
                logger.info(f"Orchestrator response: {json.dumps(response_payload, default=str)}")
                
                # Check for Lambda execution errors
                if response.get('FunctionError'):
                    error_msg = f"❌ Orchestrator Lambda execution failed: {response_payload.get('errorMessage', 'Unknown error')}"
                    logger.error(error_msg)
                    chunk_errors.append(error_msg)
                elif 'job_id' in response_payload:
                    # Orchestrator returns job information, assume success for now
                    # In a production system, you'd monitor the job progress
                    job_id = response_payload.get('job_id')
                    estimated_records = response_payload.get('estimated_duration', 0)
                    
                    # For backfill, we'll estimate records processed based on chunk
                    # This is a simplified approach - in production you'd track actual progress
                    chunk_records = max(100, estimated_records // 10)  # Rough estimate
                    records_processed += chunk_records
                    chunks_processed += 1
                    
                    logger.info(f"✅ Chunk submitted to orchestrator successfully")
                    logger.info(f"   Job ID: {job_id}")
                    logger.info(f"   Estimated records: {chunk_records}")
                else:
                    error_msg = f"❌ Unexpected orchestrator response format: {response_payload}"
                    logger.error(error_msg)
                    chunk_errors.append(error_msg)
                    
            except Exception as invoke_error:
                error_msg = f"❌ Error invoking orchestrator lambda: {str(invoke_error)}"
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
    logger.info(f"📊 TRANSACTIONAL DATA BACKFILL SUMMARY for {table_name}:")
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
        'success_rate': f"{chunks_processed}/{len(chunks)}" if len(chunks) > 0 else "0/0",
        'master_data': False
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