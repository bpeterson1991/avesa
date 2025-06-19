"""
Backfill Initiator Lambda Function

This function handles the initiation of backfill jobs when tenants connect
new services. It can be triggered manually or automatically when new
tenant services are detected.

Key Features:
- Automatic detection of new tenant services
- Manual backfill triggering via API/events
- Validation of tenant configurations
- Step Functions orchestration
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

# Add shared module to path
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))

from shared import (
    Config, TenantConfig,
    get_dynamodb_client, get_s3_client, get_stepfunctions_client,
    PipelineLogger, get_timestamp,
    validate_tenant_config
)

# Initialize clients
dynamodb = get_dynamodb_client()
s3 = get_s3_client()
stepfunctions = get_stepfunctions_client()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for backfill initiation.
    
    Args:
        event: Lambda event containing:
            - action: 'auto_detect' or 'manual_trigger'
            - tenant_id: For manual triggers
            - service: For manual triggers
            - table_name: Optional specific table
            - start_date: Optional start date
            - end_date: Optional end date
            - chunk_size_days: Optional chunk size
        context: Lambda context
        
    Returns:
        Execution summary
    """
    logger = PipelineLogger("backfill_initiator")
    config = Config.from_environment()
    
    try:
        logger.info("Starting backfill initiation", execution_id=context.aws_request_id)
        
        action = event.get('action', 'auto_detect')
        
        if action == 'auto_detect':
            result = auto_detect_new_services(config, logger)
        elif action == 'manual_trigger':
            result = manual_trigger_backfill(config, event, logger)
        else:
            raise ValueError(f"Unknown action: {action}")
        
        logger.info("Backfill initiation completed")
        return result
        
    except Exception as e:
        logger.error(f"Backfill initiation failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'message': 'Backfill initiation failed',
                'error': str(e)
            }
        }


def auto_detect_new_services(config: Config, logger: PipelineLogger) -> Dict[str, Any]:
    """Automatically detect new tenant services that need backfilling."""
    
    # Get all enabled tenants
    tenants = get_all_tenants(config)
    
    new_services = []
    
    for tenant in tenants:
        tenant_id = tenant['tenant_id']
        
        # Check for services that haven't been backfilled
        services_needing_backfill = detect_services_needing_backfill(
            config, tenant_id, logger
        )
        
        for service in services_needing_backfill:
            new_services.append({
                'tenant_id': tenant_id,
                'service': service,
                'detected_at': get_timestamp()
            })
    
    # Trigger backfills for detected services
    triggered_jobs = []
    for service_info in new_services:
        try:
            job_id = trigger_backfill_job(
                config=config,
                tenant_id=service_info['tenant_id'],
                service=service_info['service'],
                logger=logger
            )
            triggered_jobs.append({
                'tenant_id': service_info['tenant_id'],
                'service': service_info['service'],
                'job_id': job_id,
                'status': 'triggered'
            })
        except Exception as e:
            logger.error(
                f"Failed to trigger backfill for {service_info['tenant_id']}/{service_info['service']}: {str(e)}"
            )
            triggered_jobs.append({
                'tenant_id': service_info['tenant_id'],
                'service': service_info['service'],
                'status': 'failed',
                'error': str(e)
            })
    
    return {
        'statusCode': 200,
        'body': {
            'message': 'Auto-detection completed',
            'new_services_detected': len(new_services),
            'backfill_jobs_triggered': len([j for j in triggered_jobs if j['status'] == 'triggered']),
            'triggered_jobs': triggered_jobs
        }
    }


def manual_trigger_backfill(config: Config, event: Dict[str, Any], logger: PipelineLogger) -> Dict[str, Any]:
    """Manually trigger a backfill job."""
    
    tenant_id = event.get('tenant_id')
    service = event.get('service')
    
    if not tenant_id or not service:
        raise ValueError("tenant_id and service are required for manual trigger")
    
    # Validate tenant exists and is enabled
    tenant = get_tenant_configuration(config, tenant_id)
    if not tenant.enabled:
        raise ValueError(f"Tenant {tenant_id} is not enabled")
    
    # Trigger backfill job
    job_id = trigger_backfill_job(
        config=config,
        tenant_id=tenant_id,
        service=service,
        logger=logger,
        table_name=event.get('table_name'),
        start_date=event.get('start_date'),
        end_date=event.get('end_date'),
        chunk_size_days=event.get('chunk_size_days', 30)
    )
    
    return {
        'statusCode': 200,
        'body': {
            'message': 'Backfill job triggered successfully',
            'job_id': job_id,
            'tenant_id': tenant_id,
            'service': service
        }
    }


def detect_services_needing_backfill(
    config: Config,
    tenant_id: str,
    logger: PipelineLogger
) -> List[str]:
    """Detect services for a tenant that need backfilling."""
    
    services_needing_backfill = []
    
    # Get tenant configuration
    tenant = get_tenant_configuration(config, tenant_id)
    
    # Check each potential service
    potential_services = ['connectwise', 'servicenow', 'salesforce']  # Add more as needed
    
    for service in potential_services:
        # Check if tenant has this service configured
        if has_service_configured(config, tenant_id, service):
            # Check if backfill has been completed for this service
            if not has_backfill_completed(config, tenant_id, service):
                # Check if there's no recent data (indicating new service)
                if is_new_service_connection(config, tenant_id, service):
                    services_needing_backfill.append(service)
                    logger.info(f"Detected new service needing backfill: {tenant_id}/{service}")
    
    return services_needing_backfill


def has_service_configured(config: Config, tenant_id: str, service: str) -> bool:
    """Check if tenant has a specific service configured."""
    
    try:
        # Check if tenant has credentials for this service
        tenant = get_tenant_configuration(config, tenant_id)
        
        # For ConnectWise, check if connectwise_url is configured
        if service == 'connectwise':
            return bool(tenant.connectwise_url)
        
        # For other services, check for service-specific configuration
        # This would be expanded based on actual service configurations
        return False
        
    except Exception:
        return False


def has_backfill_completed(config: Config, tenant_id: str, service: str) -> bool:
    """Check if backfill has been completed for a tenant/service."""
    
    try:
        # Query backfill jobs table for completed jobs
        response = dynamodb.query(
            TableName=f"BackfillJobs-{config.environment}",
            IndexName="tenant-status-index",
            KeyConditionExpression="tenant_id = :tenant_id AND #status = :status",
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':tenant_id': {'S': tenant_id},
                ':status': {'S': 'completed'}
            },
            FilterExpression="service = :service",
            ExpressionAttributeValues={
                **{':service': {'S': service}}
            }
        )
        
        return len(response.get('Items', [])) > 0
        
    except Exception:
        return False


def is_new_service_connection(config: Config, tenant_id: str, service: str) -> bool:
    """Check if this appears to be a new service connection."""
    
    try:
        # Check if there's very little or no raw data for this service
        prefix = f"{tenant_id}/raw/{service}/"
        
        response = s3.list_objects_v2(
            Bucket=config.bucket_name,
            Prefix=prefix,
            MaxKeys=10
        )
        
        # If there are fewer than 5 objects, consider it a new connection
        object_count = len(response.get('Contents', []))
        
        # Also check the age of the oldest object
        if object_count > 0:
            oldest_object = min(response['Contents'], key=lambda x: x['LastModified'])
            age_days = (datetime.now(timezone.utc) - oldest_object['LastModified']).days
            
            # If oldest data is less than 7 days old, consider it new
            return age_days < 7
        
        return object_count < 5
        
    except Exception:
        # If we can't determine, err on the side of caution
        return False


def trigger_backfill_job(
    config: Config,
    tenant_id: str,
    service: str,
    logger: PipelineLogger,
    table_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    chunk_size_days: int = 30
) -> str:
    """Trigger a backfill job using Step Functions."""
    
    # Prepare Step Functions input
    execution_input = {
        'tenant_id': tenant_id,
        'service': service,
        'chunk_size_days': chunk_size_days
    }
    
    if table_name:
        execution_input['table_name'] = table_name
    if start_date:
        execution_input['start_date'] = start_date
    if end_date:
        execution_input['end_date'] = end_date
    
    # Generate execution name
    execution_name = f"backfill-{tenant_id}-{service}-{get_timestamp().replace(':', '-')}"
    
    # Construct state machine ARN dynamically
    # Format: arn:aws:states:region:account:stateMachine:BackfillOrchestrator-{environment}
    import boto3
    session = boto3.Session()
    region = session.region_name or 'us-east-2'
    account_id = boto3.client('sts').get_caller_identity()['Account']
    state_machine_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:BackfillOrchestrator-{config.environment}"
    
    response = stepfunctions.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(execution_input)
    )
    
    execution_arn = response['executionArn']
    
    logger.info(
        f"Started backfill execution: {execution_name}",
        execution_arn=execution_arn,
        tenant_id=tenant_id,
        service=service
    )
    
    return execution_arn


def get_all_tenants(config: Config) -> List[Dict[str, Any]]:
    """Get all enabled tenants from DynamoDB."""
    
    response = dynamodb.scan(
        TableName=config.tenant_services_table,
        FilterExpression='enabled = :enabled',
        ExpressionAttributeValues={':enabled': {'BOOL': True}}
    )
    
    tenants = []
    for item in response['Items']:
        tenant_data = {
            'tenant_id': item['tenant_id']['S'],
            'connectwise_url': item.get('connectwise_url', {'S': ''})['S'],
            'secret_name': item['secret_name']['S'],
            'enabled': item.get('enabled', {'BOOL': True})['BOOL'],
            'tables': [t['S'] for t in item.get('tables', {'L': []})['L']],
            'custom_config': json.loads(item.get('custom_config', {'S': '{}'})['S'])
        }
        tenants.append(tenant_data)
    
    return tenants


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
        'connectwise_url': item.get('connectwise_url', {'S': ''})['S'],
        'secret_name': item['secret_name']['S'],
        'enabled': item.get('enabled', {'BOOL': True})['BOOL'],
        'tables': [t['S'] for t in item.get('tables', {'L': []})['L']],
        'custom_config': json.loads(item.get('custom_config', {'S': '{}'})['S'])
    }
    return validate_tenant_config(tenant_data)