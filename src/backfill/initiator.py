#!/usr/bin/env python3
"""
Backfill Initiator Lambda Function

This function handles the initiation of backfill operations for tenants
when they connect new services or when manual backfills are requested.
"""

import json
import boto3
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')
lambda_client = boto3.client('lambda')

# Environment variables
TENANT_SERVICES_TABLE = os.environ.get('TENANT_SERVICES_TABLE')
BACKFILL_JOBS_TABLE = os.environ.get('BACKFILL_JOBS_TABLE')
BACKFILL_STATE_MACHINE_ARN = os.environ.get('BACKFILL_STATE_MACHINE_ARN')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')


def lambda_handler(event: Dict[str, Any], _context) -> Dict[str, Any]:
    """
    Main Lambda handler for backfill initiation.
    
    Supports two modes:
    1. auto_detect: Automatically detect services needing backfill
    2. manual_trigger: Manually trigger backfill for specific tenant/service
    """
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        action = event.get('action', 'manual_trigger')
        
        if action == 'auto_detect':
            result = handle_auto_detection(event)
        elif action == 'manual_trigger':
            result = handle_manual_trigger(event)
        else:
            raise ValueError(f"Unknown action: {action}")
        
        return {
            'statusCode': 200,
            'body': result
        }
        
    except Exception as e:
        logger.error(f"Error in backfill initiator: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Failed to initiate backfill'
            }
        }


def handle_auto_detection(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle automatic detection of services needing backfill."""
    logger.info("Starting auto-detection of services needing backfill")
    
    # Get all tenant services that might need backfill
    tenant_services_table = dynamodb.Table(TENANT_SERVICES_TABLE)
    
    try:
        # Scan for recently added services (within last 24 hours)
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
        cutoff_timestamp = cutoff_time.isoformat()
        
        response = tenant_services_table.scan(
            FilterExpression='attribute_exists(created_at) AND created_at > :cutoff',
            ExpressionAttributeValues={
                ':cutoff': cutoff_timestamp
            }
        )
        
        services_needing_backfill = []
        
        for item in response.get('Items', []):
            tenant_id = item.get('tenant_id')
            service_name = item.get('service_name')
            
            if tenant_id and service_name:
                # Check if backfill already exists for this service
                if not backfill_exists(tenant_id, service_name):
                    services_needing_backfill.append({
                        'tenant_id': tenant_id,
                        'service': service_name,
                        'created_at': item.get('created_at')
                    })
        
        logger.info(f"Found {len(services_needing_backfill)} services needing backfill")
        
        # Trigger backfills for detected services
        triggered_backfills = []
        for service_info in services_needing_backfill:
            try:
                backfill_result = trigger_backfill_for_service(
                    tenant_id=service_info['tenant_id'],
                    service=service_info['service']
                )
                triggered_backfills.append(backfill_result)
            except Exception as e:
                logger.error(f"Failed to trigger backfill for {service_info}: {str(e)}")
        
        return {
            'action': 'auto_detect',
            'services_detected': len(services_needing_backfill),
            'backfills_triggered': len(triggered_backfills),
            'triggered_backfills': triggered_backfills
        }
        
    except Exception as e:
        logger.error(f"Error in auto-detection: {str(e)}")
        raise


def handle_manual_trigger(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle manual backfill trigger."""
    logger.info("Starting manual backfill trigger")
    
    # Extract parameters
    tenant_id = event.get('tenant_id')
    service = event.get('service')
    table_name = event.get('table_name')
    start_date = event.get('start_date')
    end_date = event.get('end_date')
    chunk_size_days = event.get('chunk_size_days', 30)
    
    if not tenant_id or not service:
        raise ValueError("tenant_id and service are required for manual trigger")
    
    # Trigger the backfill
    backfill_result = trigger_backfill_for_service(
        tenant_id=tenant_id,
        service=service,
        table_name=table_name,
        start_date=start_date,
        end_date=end_date,
        chunk_size_days=chunk_size_days
    )
    
    return {
        'action': 'manual_trigger',
        'tenant_id': tenant_id,
        'service': service,
        'table_name': table_name,
        'backfill_result': backfill_result
    }


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


def backfill_exists(tenant_id: str, service: str) -> bool:
    """Check if a backfill job already exists for the given tenant/service."""
    try:
        backfill_jobs_table = dynamodb.Table(BACKFILL_JOBS_TABLE)
        
        # Query for existing backfill jobs
        response = backfill_jobs_table.scan(
            FilterExpression='tenant_id = :tenant_id AND service_name = :service AND job_status IN (:active, :pending)',
            ExpressionAttributeValues={
                ':tenant_id': tenant_id,
                ':service': service,
                ':active': 'active',
                ':pending': 'pending'
            }
        )
        
        return len(response.get('Items', [])) > 0
        
    except Exception as e:
        logger.error(f"Error checking backfill existence: {str(e)}")
        return False


def trigger_backfill_for_service(
    tenant_id: str,
    service: str,
    table_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    chunk_size_days: int = 30
) -> Dict[str, Any]:
    """Trigger a backfill for a specific service."""
    
    # Generate unique job ID
    job_id = str(uuid.uuid4())
    
    # Check if this is master data (should not use date ranges)
    is_master_data = table_name and is_master_data_table(service, table_name)
    
    if is_master_data:
        # Master data should not use date ranges - let backfill function handle without dates
        logger.info(f"Master data table detected ({table_name}) - skipping date range setup")
        start_date = None
        end_date = None
    else:
        # Set default dates if not provided for transactional data
        if not end_date:
            end_date = datetime.now(timezone.utc).isoformat()
        
        if not start_date:
            # Default to 2 years back for comprehensive backfill
            start_dt = datetime.now(timezone.utc) - timedelta(days=730)
            start_date = start_dt.isoformat()
    
    # Create backfill job record
    backfill_jobs_table = dynamodb.Table(BACKFILL_JOBS_TABLE)
    
    job_record = {
        'job_id': job_id,
        'tenant_id': tenant_id,
        'service_name': service,
        'table_name': table_name,
        'start_date': start_date,
        'end_date': end_date,
        'chunk_size_days': chunk_size_days,
        'job_status': 'pending',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'updated_at': datetime.now(timezone.utc).isoformat()
    }
    
    try:
        # Store job record
        backfill_jobs_table.put_item(Item=job_record)
        logger.info(f"Created backfill job record: {job_id}")
        
        # Prepare Step Functions input
        step_input = {
            'tenant_id': tenant_id,
            'service': service,
            'table_name': table_name,
            'start_date': start_date,
            'end_date': end_date,
            'chunk_size_days': chunk_size_days,
            'resume_job_id': job_id
        }
        
        # Start Step Functions execution
        if BACKFILL_STATE_MACHINE_ARN:
            execution_response = stepfunctions.start_execution(
                stateMachineArn=BACKFILL_STATE_MACHINE_ARN,
                name=f"backfill-{job_id}",
                input=json.dumps(step_input)
            )
            
            execution_arn = execution_response['executionArn']
            logger.info(f"Started Step Functions execution: {execution_arn}")
            
            # Update job record with execution ARN
            backfill_jobs_table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET execution_arn = :arn, job_status = :status, updated_at = :updated',
                ExpressionAttributeValues={
                    ':arn': execution_arn,
                    ':status': 'active',
                    ':updated': datetime.now(timezone.utc).isoformat()
                }
            )
            
            return {
                'job_id': job_id,
                'execution_arn': execution_arn,
                'status': 'started',
                'step_input': step_input
            }
        else:
            # Fallback: directly invoke backfill Lambda
            logger.warning("No Step Functions ARN configured, falling back to direct Lambda invocation")
            
            backfill_lambda_name = f"avesa-backfill-{ENVIRONMENT}"
            
            lambda_response = lambda_client.invoke(
                FunctionName=backfill_lambda_name,
                InvocationType='Event',  # Async invocation
                Payload=json.dumps(step_input)
            )
            
            logger.info(f"Invoked backfill Lambda: {backfill_lambda_name}")
            
            return {
                'job_id': job_id,
                'lambda_response': lambda_response['StatusCode'],
                'status': 'started',
                'step_input': step_input
            }
            
    except Exception as e:
        logger.error(f"Error triggering backfill: {str(e)}")
        
        # Update job status to failed
        try:
            backfill_jobs_table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET job_status = :status, error_message = :error, updated_at = :updated',
                ExpressionAttributeValues={
                    ':status': 'failed',
                    ':error': str(e),
                    ':updated': datetime.now(timezone.utc).isoformat()
                }
            )
        except Exception as update_error:
            logger.error(f"Failed to update job status: {str(update_error)}")
        
        raise