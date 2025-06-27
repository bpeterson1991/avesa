# Backfill Stack Integration Plan

## 📋 Overview

This document outlines the detailed implementation plan for consolidating the AVESABackfill Stack into the Performance Optimization Stack. The goal is to eliminate architectural redundancy while preserving all backfill-specific functionality.

## 🎯 Integration Strategy

### Phase 1: Infrastructure Migration (1-2 days)
### Phase 2: Code Integration (2-3 days) 
### Phase 3: Testing & Validation (1-2 days)
### Phase 4: Cleanup & Documentation (1 day)

**Total Estimated Time: 5-8 days**

---

## 📋 Phase 1: Infrastructure Migration

### 1.1 Add BackfillJobs Table to Performance Optimization Stack

**File: `infrastructure/stacks/performance_optimization_stack.py`**

```python
def _create_backfill_jobs_table(self) -> dynamodb.Table:
    """Create DynamoDB table for tracking backfill jobs."""
    table_name = f"BackfillJobs-{self.env_name}"
    
    table = dynamodb.Table(
        self,
        "BackfillJobsTable",
        table_name=table_name,
        partition_key=dynamodb.Attribute(
            name="job_id",
            type=dynamodb.AttributeType.STRING
        ),
        # Add GSI for tenant-based queries
        global_secondary_indexes=[
            dynamodb.GlobalSecondaryIndex(
                index_name="TenantServiceIndex",
                partition_key=dynamodb.Attribute(
                    name="tenant_id",
                    type=dynamodb.AttributeType.STRING
                ),
                sort_key=dynamodb.Attribute(
                    name="service_name",
                    type=dynamodb.AttributeType.STRING
                )
            )
        ],
        billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        removal_policy=RemovalPolicy.RETAIN,
        point_in_time_recovery=True
    )
    return table
```

**Add to `__init__` method:**
```python
# Create DynamoDB tables for optimization
self.processing_jobs_table = self._create_processing_jobs_table()
self.chunk_progress_table = self._create_chunk_progress_table()
self.backfill_jobs_table = self._create_backfill_jobs_table()  # NEW
```

### 1.2 Update IAM Permissions

**Add to `_create_lambda_execution_role` method:**
```python
# DynamoDB permissions - add backfill table
resources=[
    self.processing_jobs_table.table_arn,
    self.chunk_progress_table.table_arn,
    self.backfill_jobs_table.table_arn,  # NEW
    self.tenant_services_table.table_arn,
    self.last_updated_table.table_arn,
    # Include GSI ARNs
    f"{self.processing_jobs_table.table_arn}/index/*",
    f"{self.chunk_progress_table.table_arn}/index/*",
    f"{self.backfill_jobs_table.table_arn}/index/*",  # NEW
]
```

### 1.3 Add Backfill Initiator Lambda Function

**Add to `_create_all_lambda_functions_only` method:**
```python
# Backfill Initiator
functions['backfill_initiator'] = _lambda.Function(
    self,
    "BackfillInitiatorLambda",
    function_name=f"avesa-backfill-initiator-{self.env_name}",
    runtime=_lambda.Runtime.PYTHON_3_9,
    handler="backfill_initiator.lambda_handler",
    code=_lambda.Code.from_asset(
        "../src",
        bundling=BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_9.bundling_image,
            command=[
                "bash", "-c",
                "cp -r /asset-input/optimized/helpers/* /asset-output/ && "
                "cp -r /asset-input/shared /asset-output/"
            ]
        )
    ),
    role=self.lambda_execution_role,
    memory_size=512,
    timeout=Duration.seconds(300),
    environment=common_env,
    log_retention=logs.RetentionDays.ONE_MONTH
)
```

---

## 📋 Phase 2: Code Integration

### 2.1 Create Backfill Initiator Lambda

**File: `src/optimized/helpers/backfill_initiator.py`**

```python
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
ORCHESTRATOR_STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')

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
        dynamodb = get_dynamodb_client()
        backfill_jobs_table = dynamodb.Table(BACKFILL_JOBS_TABLE)
        
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
            'force_full_sync': True,
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
        if ORCHESTRATOR_STATE_MACHINE_ARN:
            stepfunctions = boto3.client('stepfunctions', region_name='us-east-2')
            
            execution_response = stepfunctions.start_execution(
                stateMachineArn=ORCHESTRATOR_STATE_MACHINE_ARN,
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
        else:
            raise ValueError("No orchestrator state machine ARN configured")
            
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
    
    # Implementation for auto-detection logic
    # This would scan tenant services and detect new services needing backfill
    # For now, return placeholder
    return {
        'action': 'auto_detect',
        'message': 'Auto-detection not yet implemented in optimized version'
    }
```

### 2.2 Enhance Pipeline Orchestrator Lambda

**File: `src/optimized/orchestrator/lambda_function.py` - Add backfill support**

```python
# Add to the lambda_handler function
def lambda_handler(event, context):
    """Enhanced orchestrator with backfill support."""
    try:
        logger.info(f"Pipeline orchestrator invoked with event: {json.dumps(event, default=str)}")
        
        # Extract backfill-specific parameters
        backfill_mode = event.get('backfill_mode', False)
        backfill_date_range = event.get('backfill_date_range')
        job_id = event.get('job_id')
        
        # Handle backfill mode
        if backfill_mode:
            logger.info("Running in backfill mode")
            return handle_backfill_orchestration(event, context)
        else:
            # Regular processing mode
            return handle_regular_orchestration(event, context)
            
    except Exception as e:
        logger.error(f"Error in pipeline orchestrator: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e)
        }

def handle_backfill_orchestration(event, context):
    """Handle backfill-specific orchestration logic."""
    
    # Extract backfill parameters
    backfill_date_range = event.get('backfill_date_range')
    chunk_size_days = backfill_date_range.get('chunk_size_days', 30) if backfill_date_range else 30
    job_id = event.get('job_id')
    
    # Update job status to processing
    if job_id:
        update_backfill_job_status(job_id, 'processing')
    
    # Process based on whether date range is specified
    if backfill_date_range:
        return handle_date_range_backfill(event, context)
    else:
        return handle_full_sync_backfill(event, context)

def handle_date_range_backfill(event, context):
    """Handle backfill with specific date ranges (transactional data)."""
    
    backfill_date_range = event['backfill_date_range']
    start_date = backfill_date_range['start_date']
    end_date = backfill_date_range['end_date']
    chunk_size_days = backfill_date_range.get('chunk_size_days', 30)
    
    # Create date chunks
    chunks = create_date_chunks(start_date, end_date, chunk_size_days)
    
    # Enhance tenants with chunk information
    tenants = event.get('tenants', [])
    enhanced_tenants = []
    
    for tenant in tenants:
        for i, (chunk_start, chunk_end) in enumerate(chunks):
            chunk_tenant = tenant.copy()
            chunk_tenant['backfill_chunk'] = {
                'chunk_number': i + 1,
                'total_chunks': len(chunks),
                'start_date': chunk_start,
                'end_date': chunk_end
            }
            enhanced_tenants.append(chunk_tenant)
    
    # Update event with enhanced tenants
    enhanced_event = event.copy()
    enhanced_event['tenants'] = enhanced_tenants
    
    # Trigger the state machine with enhanced event
    return trigger_state_machine(enhanced_event)

def handle_full_sync_backfill(event, context):
    """Handle full sync backfill (master data)."""
    
    # For master data, we don't need date chunking
    # Just trigger the regular pipeline with force_full_sync=True
    enhanced_event = event.copy()
    enhanced_event['force_full_sync'] = True
    enhanced_event['master_data_mode'] = True
    
    return trigger_state_machine(enhanced_event)

def create_date_chunks(start_date: str, end_date: str, chunk_size_days: int) -> List[tuple]:
    """Create date chunks for processing."""
    from datetime import datetime, timedelta
    
    # Parse dates
    start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    chunks = []
    current_start = start_dt
    
    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=chunk_size_days), end_dt)
        chunks.append((current_start.isoformat(), current_end.isoformat()))
        current_start = current_end
    
    return chunks

def update_backfill_job_status(job_id: str, status: str, error_message: str = None):
    """Update backfill job status in DynamoDB."""
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
        table_name = f"BackfillJobs-{os.environ.get('ENVIRONMENT', 'dev')}"
        table = dynamodb.Table(table_name)
        
        update_expression = 'SET job_status = :status, updated_at = :updated'
        expression_values = {
            ':status': status,
            ':updated': datetime.now(timezone.utc).isoformat()
        }
        
        if error_message:
            update_expression += ', error_message = :error'
            expression_values[':error'] = error_message
        
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values
        )
        
        logger.info(f"Updated backfill job {job_id} status to {status}")
        
    except Exception as e:
        logger.error(f"Error updating backfill job status: {str(e)}")
```

### 2.3 Enhance Processors for Backfill Support

**File: `src/optimized/processors/chunk_processor.py` - Add backfill handling**

```python
# Add to the lambda_handler function
def lambda_handler(event, context):
    """Enhanced chunk processor with backfill support."""
    try:
        # Extract backfill information
        backfill_chunk = event.get('backfill_chunk')
        master_data_mode = event.get('master_data_mode', False)
        
        if backfill_chunk:
            logger.info(f"Processing backfill chunk {backfill_chunk['chunk_number']}/{backfill_chunk['total_chunks']}")
            logger.info(f"Date range: {backfill_chunk['start_date']} to {backfill_chunk['end_date']}")
            
            # Add date range to chunk config for API calls
            chunk_config = event.get('chunk_config', {})
            chunk_config['date_range'] = {
                'start_date': backfill_chunk['start_date'],
                'end_date': backfill_chunk['end_date']
            }
            event['chunk_config'] = chunk_config
        
        if master_data_mode:
            logger.info("Processing in master data mode - no date filtering")
            
        # Continue with regular chunk processing
        return process_chunk(event, context)
        
    except Exception as e:
        logger.error(f"Error in enhanced chunk processor: {str(e)}")
        raise e
```

---

## 📋 Phase 3: Environment Variables & Configuration

### 3.1 Update Environment Variables

**Add to Performance Optimization Stack common_env:**
```python
common_env = {
    "BUCKET_NAME": self.data_bucket.bucket_name,
    "TENANT_SERVICES_TABLE": self.tenant_services_table.table_name,
    "LAST_UPDATED_TABLE": self.last_updated_table.table_name,
    "ENVIRONMENT": self.env_name,
    "PROCESSING_JOBS_TABLE": self.processing_jobs_table.table_name,
    "CHUNK_PROGRESS_TABLE": self.chunk_progress_table.table_name,
    "BACKFILL_JOBS_TABLE": self.backfill_jobs_table.table_name,  # NEW
}
```

### 3.2 Update Orchestrator Environment

**Add STATE_MACHINE_ARN to backfill_initiator environment:**
```python
# In the backfill_initiator function creation
environment=common_env.copy().update({
    "STATE_MACHINE_ARN": ""  # Will be set after state machine creation
})

# After state machine creation, update the environment
functions['backfill_initiator'].add_environment(
    "STATE_MACHINE_ARN",
    pipeline_orchestrator_state_machine.state_machine_arn
)
```

---

## 📋 Phase 4: Migration Scripts & Tools

### 4.1 Update Trigger Scripts

**File: `scripts/trigger-backfill.py` - Update to use Performance Optimization Stack**

```python
def trigger_manual_backfill(
    lambda_client,
    tenant_id: str,
    service: str,
    table_name: str,
    start_date: str,
    end_date: str,
    chunk_size_days: int,
    environment: str,
    dry_run: bool
) -> Dict[str, Any]:
    """Trigger manual backfill using the Performance Optimization Stack."""
    
    # Use the new backfill initiator function
    function_name = f"avesa-backfill-initiator-{environment}"
    
    payload = {
        'action': 'manual_trigger',
        'tenant_id': tenant_id,
        'service': service,
        'table_name': table_name,
        'start_date': start_date,
        'end_date': end_date,
        'chunk_size_days': chunk_size_days
    }
    
    if dry_run:
        print(f"DRY RUN - Would invoke: {function_name}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        return {'dry_run': True, 'function_name': function_name, 'payload': payload}
    
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    
    return json.loads(response['Payload'].read().decode('utf-8'))
```

### 4.2 Create Migration Validation Script

**File: `scripts/validate-backfill-migration.py`**

```python
#!/usr/bin/env python3
"""
Validate Backfill Migration

This script validates that the backfill functionality has been successfully
migrated from the separate Backfill Stack to the Performance Optimization Stack.
"""

import boto3
import json
import argparse
from typing import Dict, Any

def validate_infrastructure(environment: str, region: str) -> Dict[str, bool]:
    """Validate that required infrastructure exists."""
    
    results = {}
    
    # Check DynamoDB tables
    dynamodb = boto3.client('dynamodb', region_name=region)
    
    required_tables = [
        f"ProcessingJobs-{environment}",
        f"ChunkProgress-{environment}",
        f"BackfillJobs-{environment}"  # Should now exist in Performance Optimization Stack
    ]
    
    for table_name in required_tables:
        try:
            response = dynamodb.describe_table(TableName=table_name)
            results[f"table_{table_name}"] = True
            print(f"✓ Table {table_name} exists")
        except dynamodb.exceptions.ResourceNotFoundException:
            results[f"table_{table_name}"] = False
            print(f"✗ Table {table_name} missing")
    
    # Check Lambda functions
    lambda_client = boto3.client('lambda', region_name=region)
    
    required_functions = [
        f"avesa-pipeline-orchestrator-{environment}",
        f"avesa-backfill-initiator-{environment}",  # Should now be in Performance Optimization Stack
        f"avesa-tenant-processor-{environment}",
        f"avesa-table-processor-{environment}",
        f"avesa-chunk-processor-{environment}"
    ]
    
    for function_name in required_functions:
        try:
            lambda_client.get_function(FunctionName=function_name)
            results[f"function_{function_name}"] = True
            print(f"✓ Function {function_name} exists")
        except lambda_client.exceptions.ResourceNotFoundException:
            results[f"function_{function_name}"] = False
            print(f"✗ Function {function_name} missing")
    
    # Check Step Functions
    sfn_client = boto3.client('stepfunctions', region_name=region)
    
    required_state_machines = [
        f"PipelineOrchestrator-{environment}",
        f"TenantProcessor-{environment}",
        f"TableProcessor-{environment}"
    ]
    
    try:
        response = sfn_client.list_state_machines()
        existing_sms = [sm['name'] for sm in response['stateMachines']]
        
        for sm_name in required_state_machines:
            if sm_name in existing_sms:
                results[f"sm_{sm_name}"] = True
                print(f"✓ State Machine {sm_name} exists")
            else:
                results[f"sm_{sm_name}"] = False
                print(f"✗ State Machine {sm_name} missing")
    
    except Exception as e:
        print(f"Error checking state machines: {e}")
    
    return results

def test_backfill_initiation(environment: str, region: str) -> bool:
    """Test that backfill initiation works."""
    
    lambda_client = boto3.client('lambda', region_name=region)
    function_name = f"avesa-backfill-initiator-{environment}"
    
    # Test payload
    test_payload = {
        'action': 'manual_trigger',
        'tenant_id': 'test-tenant',
        'service': 'connectwise',
        'table_name': 'companies',
        'start_date': '2024-01-01T00:00:00Z',
        'end_date': '2024-01-02T00:00:00Z',
        'chunk_size_days': 1
    }
    
    try:
        # Dry run test - just check if function can be invoked
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(test_payload)
        )
        
        result = json.loads(response['Payload'].read().decode('utf-8'))
        
        if result.get('statusCode') == 200:
            print(f"✓ Backfill initiation test passed")
            return True
        else:
            print(f"✗ Backfill initiation test failed: {result}")
            return False
            
    except Exception as e:
        print(f"✗ Backfill initiation test error: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Validate backfill migration')
    parser.add_argument('--environment', default='dev', help='Environment to validate')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    
    args = parser.parse_args()
    
    print(f"Validating backfill migration for environment: {args.environment}")
    print("=" * 60)
    
    # Validate infrastructure
    print("\n1. Validating Infrastructure...")
    infrastructure_results = validate_infrastructure(args.environment, args.region)
    
    # Test backfill initiation
    print("\n2. Testing Backfill Initiation...")
    initiation_test = test_backfill_initiation(args.environment, args.region)
    
    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    
    total_checks = len(infrastructure_results) + 1
    passed_checks = sum(infrastructure_results.values()) + (1 if initiation_test else 0)
    
    print(f"Total checks: {total_checks}")
    print(f"Passed: {passed_checks}")
    print(f"Failed: {total_checks - passed_checks}")
    
    if passed_checks == total_checks:
        print("\n🎉 All validation checks passed! Migration successful.")
        return 0
    else:
        print(f"\n❌ {total_checks - passed_checks} validation checks failed.")
        return 1

if __name__ == "__main__":
    exit(main())
```

---

## 📋 Phase 5: Cleanup & Removal

### 5.1 Remove Backfill Stack from CDK App

**File: `infrastructure/app.py` - Remove backfill stack deployment**

```python
# Remove these lines:
# backfill_stack = BackfillStack(
#     app,
#     f"AVESABackfill{env_config['table_suffix']}",
#     env=env,
#     environment=target_env,
#     data_bucket_name=env_config["bucket_name"],
#     tenant_services_table_name=env_config["tenant_services_table"],
#     lambda_memory=1024,
#     lambda_timeout=900
# )
```

### 5.2 Archive Backfill Stack Files

Create archive directory and move files:

```bash
# Create archive directory
mkdir -p infrastructure/stacks/archive/

# Move backfill stack files
mv infrastructure/stacks/backfill_stack.py infrastructure/stacks/archive/
mv src/backfill/ src/archive/backfill/
```

### 5.3 Update Imports and References

**File: `infrastructure/stacks/__init__.py`**

```python
# Remove backfill stack import
from .performance_optimization_stack import PerformanceOptimizationStack
# from .backfill_stack import BackfillStack  # REMOVED
from .clickhouse_stack import ClickHouseStack

__all__ = ["PerformanceOptimizationStack", "ClickHouseStack"]  # Removed BackfillStack
```

---

## 📋 Testing & Validation Checklist

### Pre-Migration Testing
- [ ] Document current backfill functionality and test cases
- [ ] Create backup of existing BackfillJobs table data
- [ ] Test current backfill workflows to establish baseline

### During Migration Testing
- [ ] Deploy Performance Optimization Stack with new backfill features
- [ ] Run infrastructure validation script
- [ ] Test backfill initiation with dry-run mode
- [ ] Validate DynamoDB table creation and permissions

### Post-Migration Testing
- [ ] Run full backfill test with real tenant data
- [ ] Verify job tracking in BackfillJobs table
- [ ] Test date chunking functionality
- [ ] Validate master data vs transactional data handling
- [ ] Test error handling and retry logic
- [ ] Monitor CloudWatch logs for issues

### Cleanup Validation
- [ ] Verify old backfill stack resources are removed
- [ ] Confirm no orphaned Lambda functions or state machines
- [ ] Validate script updates work correctly
- [ ] Test backwards compatibility for existing integrations

---

## 📋 Rollback Plan

In case of issues during migration:

### Emergency Rollback Steps
1. **Redeploy original Backfill Stack** from git history
2. **Restore BackfillJobs table data** from backup
3. **Update scripts** to point back to original backfill functions
4. **Communicate rollback** to team and stakeholders

### Rollback Validation
- [ ] Test original backfill functionality
- [ ] Verify all integrations work
- [ ] Confirm no data loss occurred

---

## 📊 Success Metrics

### Technical Metrics
- **Infrastructure Reduction**: 50% fewer CDK stacks (2 instead of 3)
- **Lambda Function Reduction**: ~5 fewer functions
- **State Machine Reduction**: 1 fewer state machine
- **Code Duplication**: Eliminated duplicate processing logic

### Operational Metrics
- **Deployment Time**: Faster due to fewer stacks
- **Monitoring**: Unified dashboards and logging
- **Maintenance**: Single codebase for all data processing

### Performance Metrics
- **Parallelization**: Backfills now leverage Step Functions parallelization
- **Resource Utilization**: Better scaling through optimized pipeline
- **Error Handling**: Consistent retry and error handling

---

This integration plan provides a comprehensive, step-by-step approach to consolidating the Backfill Stack into the Performance Optimization Stack while preserving all functionality and improving the overall architecture.