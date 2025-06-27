#!/usr/bin/env python3
"""
Backfill Initiator for Performance Optimization Stack

Handles initiation of backfill operations integrated with the optimized pipeline.
"""

import json
import os
import boto3
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
import logging
import uuid

# Import shared components
from shared.aws_clients import get_dynamodb_client, get_lambda_client
from shared.logger import get_logger

logger = get_logger(__name__)

# Environment variables
TENANT_SERVICES_TABLE = os.environ.get('TENANT_SERVICES_TABLE')
BACKFILL_JOBS_TABLE = os.environ.get('BACKFILL_JOBS_TABLE')
ORCHESTRATOR_STATE_MACHINE_ARN = None  # Will be discovered at runtime
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')

def _get_state_machine_arn() -> str:
    """Discover the state machine ARN at runtime."""
    try:
        env_name = ENVIRONMENT
        stepfunctions = boto3.client('stepfunctions')
        
        # Look for the pipeline orchestrator state machine by name
        state_machine_name = f"PipelineOrchestrator-{env_name}"
        
        # List state machines and find ours
        paginator = stepfunctions.get_paginator('list_state_machines')
        for page in paginator.paginate():
            for state_machine in page['stateMachines']:
                if state_machine['name'] == state_machine_name:
                    logger.info(f"Discovered state machine ARN: {state_machine['stateMachineArn']}")
                    return state_machine['stateMachineArn']
        
        # If not found by name, fall back to environment variable
        state_machine_arn = os.environ.get('STATE_MACHINE_ARN')
        if state_machine_arn:
            logger.info(f"Using STATE_MACHINE_ARN from environment: {state_machine_arn}")
            return state_machine_arn
            
        raise ValueError(f"Could not find state machine '{state_machine_name}' or STATE_MACHINE_ARN environment variable")
        
    except Exception as e:
        logger.error(f"Error discovering state machine ARN: {str(e)}")
        raise

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Main Lambda handler for backfill initiation.
    
    Supports:
    1. auto_detect: Automatically detect services needing backfill
    2. manual_trigger: Manually trigger backfill for specific tenant/service
    """
    try:
        logger.info(f"Received backfill initiation event: {json.dumps(event)}")
        
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
        raise ValueError("tenant_id and service are required")
    
    # Trigger the backfill using the optimized pipeline
    backfill_result = trigger_optimized_backfill(
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

def trigger_optimized_backfill(
    tenant_id: str,
    service: str,
    table_name: str = None,
    start_date: str = None,
    end_date: str = None,
    chunk_size_days: int = 30
) -> Dict[str, Any]:
    """Trigger backfill using the optimized pipeline architecture."""
    
    # Create backfill job record
    job_id = str(uuid.uuid4())
    
    try:
        dynamodb_resource = boto3.resource('dynamodb')
        backfill_jobs_table = dynamodb_resource.Table(BACKFILL_JOBS_TABLE)
        
        # Create job record
        job_record = {
            'job_id': job_id,
            'tenant_id': tenant_id,
            'service_name': service,
            'table_name': table_name or 'all',
            'start_date': start_date,
            'end_date': end_date,
            'chunk_size_days': chunk_size_days,
            'job_status': 'initiated',
            'created_at': datetime.now(timezone.utc).isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        }
        
        backfill_jobs_table.put_item(Item=job_record)
        logger.info(f"Created backfill job record: {job_id}")
        
        # Prepare orchestrator payload with backfill flags
        orchestrator_payload = {
            'source': 'backfill',
            'mode': 'single-tenant',
            'job_id': job_id,
            'backfill_mode': True,
            'tenants': [{
                'tenant_id': tenant_id,
                'service': service
            }]
        }
        
        # Add backfill-specific parameters
        if table_name:
            orchestrator_payload['table_name'] = table_name
            
        if start_date and end_date:
            orchestrator_payload['backfill_date_range'] = {
                'start_date': start_date,
                'end_date': end_date,
                'chunk_size_days': chunk_size_days
            }
        elif start_date or end_date:
            # Set default date range if only one date provided
            if not end_date:
                end_date = datetime.now(timezone.utc).isoformat()
            if not start_date:
                start_dt = datetime.now(timezone.utc) - timedelta(days=730)  # 2 years back
                start_date = start_dt.isoformat()
            
            orchestrator_payload['backfill_date_range'] = {
                'start_date': start_date,
                'end_date': end_date,
                'chunk_size_days': chunk_size_days
            }
        
        # Trigger the optimized pipeline orchestrator via Step Functions
        try:
            state_machine_arn = _get_state_machine_arn()
            stepfunctions = boto3.client('stepfunctions', region_name='us-east-2')
            
            execution_response = stepfunctions.start_execution(
                stateMachineArn=state_machine_arn,
                name=f"backfill-{job_id}",
                input=json.dumps(orchestrator_payload)
            )
            
            execution_arn = execution_response['executionArn']
            logger.info(f"Started Step Functions execution: {execution_arn}")
            
            # Update job record with execution details
            backfill_jobs_table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET job_status = :status, execution_arn = :arn, updated_at = :updated',
                ExpressionAttributeValues={
                    ':status': 'running',
                    ':arn': execution_arn,
                    ':updated': datetime.now(timezone.utc).isoformat()
                }
            )
            
            return {
                'job_id': job_id,
                'execution_arn': execution_arn,
                'status': 'running',
                'orchestrator_payload': orchestrator_payload
            }
        except Exception as discovery_error:
            logger.error(f"Failed to discover or trigger state machine: {str(discovery_error)}")
            raise ValueError(f"No orchestrator state machine ARN configured: {str(discovery_error)}")
            
    except Exception as e:
        logger.error(f"Error triggering optimized backfill: {str(e)}")
        
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
            logger.error(f"Failed to update job status: {update_error}")
        
        raise e

def handle_auto_detection(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle automatic detection of services needing backfill."""
    logger.info("Starting auto-detection of services needing backfill")
    
    # Get all tenant services that might need backfill
    dynamodb_resource = boto3.resource('dynamodb')
    tenant_services_table = dynamodb_resource.Table(TENANT_SERVICES_TABLE)
    
    try:
        response = tenant_services_table.scan()
        
        services_needing_backfill = []
        
        for item in response.get('Items', []):
            tenant_id = item.get('tenant_id')
            service_name = item.get('service')
            
            if tenant_id and service_name:
                # Check if backfill already exists for this service
                if not backfill_exists(tenant_id, service_name):
                    services_needing_backfill.append({
                        'tenant_id': tenant_id,
                        'service': service_name
                    })
        
        logger.info(f"Found {len(services_needing_backfill)} services needing backfill")
        
        # Trigger backfills for detected services
        triggered_backfills = []
        for service_info in services_needing_backfill:
            try:
                backfill_result = trigger_optimized_backfill(
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
        raise e

def backfill_exists(tenant_id: str, service: str) -> bool:
    """Check if a backfill job already exists for the given tenant/service."""
    try:
        dynamodb_resource = boto3.resource('dynamodb')
        backfill_jobs_table = dynamodb_resource.Table(BACKFILL_JOBS_TABLE)
        
        # Query for existing backfill jobs using GSI
        response = backfill_jobs_table.scan(
            FilterExpression='tenant_id = :tenant_id AND service_name = :service AND job_status IN (:active, :pending)',
            ExpressionAttributeValues={
                ':tenant_id': tenant_id,
                ':service': service,
                ':active': 'running',
                ':pending': 'initiated'
            }
        )
        
        return len(response.get('Items', [])) > 0
        
    except Exception as e:
        logger.error(f"Error checking backfill existence: {str(e)}")
        return False

def is_master_data_table(service: str, table_name: str) -> bool:
    """Determine if a table contains master data (should not use date ranges)."""
    master_data_tables = {
        'connectwise': ['companies', 'contacts', 'members'],
        'servicenow': ['contacts', 'user_groups'],
        'salesforce': ['companies', 'contacts', 'users']
    }
    
    service_master_tables = master_data_tables.get(service, [])
    return table_name in service_master_tables