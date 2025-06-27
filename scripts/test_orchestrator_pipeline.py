#!/usr/bin/env python3
"""
Test script to invoke the OrchestratorPipeline-dev state machine
for testing companies table for sitetechnology (ConnectWise service)
"""

import json
import boto3
import uuid
from datetime import datetime, timezone

def generate_test_payload():
    """Generate the correct payload for testing orchestrator pipeline."""
    
    # Generate unique job ID for this test
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    unique_id = str(uuid.uuid4())[:8]
    job_id = f"test-job-{timestamp}-{unique_id}"
    
    # Create test payload structure matching what orchestrator expects
    payload = {
        "job_id": job_id,
        "tenant_id": "sitetechnology",  # Specific tenant for ConnectWise
        "table_name": "tickets",      # Test companies table
        "record_limit": 200000,           # Limit records for testing
        "source": "manual_test",
        "backfill_mode": False,        # Regular processing mode
        "tenants": [
            {
                "tenant_id": "sitetechnology",
                "services": [
                    {
                        "service_name": "connectwise",
                        "enabled": True,
                        "config": {
                            "endpoint_base": "service/tickets",
                            "page_size": 1000
                        }
                    }
                ],
                "enabled": True
            }
        ],
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    return payload

def invoke_step_functions_execution():
    """Invoke the Step Functions state machine for testing."""
    
    try:
        # Initialize AWS clients
        stepfunctions_client = boto3.client('stepfunctions')
        
        # Get the state machine ARN for dev environment
        state_machine_name = "PipelineOrchestrator-dev"
        
        # List state machines to find the correct ARN
        response = stepfunctions_client.list_state_machines()
        state_machine_arn = None
        
        for state_machine in response.get('stateMachines', []):
            if state_machine['name'] == state_machine_name:
                state_machine_arn = state_machine['stateMachineArn']
                break
        
        if not state_machine_arn:
            print(f"❌ State machine '{state_machine_name}' not found")
            print("Available state machines:")
            for sm in response.get('stateMachines', []):
                print(f"  - {sm['name']}: {sm['stateMachineArn']}")
            return False
        
        print(f"✅ Found state machine: {state_machine_arn}")
        
        # Generate test payload
        payload = generate_test_payload()
        
        print(f"📋 Test payload:")
        print(json.dumps(payload, indent=2))
        
        # Generate execution name
        execution_name = f"test-tickets-{payload['job_id']}-{datetime.now().strftime('%H%M%S')}"
        
        print(f"🚀 Starting execution: {execution_name}")
        
        # Start the execution
        response = stepfunctions_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(payload)
        )
        
        execution_arn = response['executionArn']
        print(f"✅ Successfully started execution:")
        print(f"   Execution ARN: {execution_arn}")
        print(f"   Execution Name: {execution_name}")
        
        # Get execution status
        describe_response = stepfunctions_client.describe_execution(
            executionArn=execution_arn
        )
        
        print(f"📊 Execution Status: {describe_response['status']}")
        print(f"📅 Start Date: {describe_response['startDate']}")
        
        print(f"\n🔍 Monitor execution in AWS Console:")
        print(f"   https://console.aws.amazon.com/states/home#/executions/details/{execution_arn}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error invoking Step Functions: {str(e)}")
        return False

def test_orchestrator_lambda_directly():
    """Test the orchestrator Lambda function directly as fallback."""
    
    try:
        # Initialize Lambda client
        lambda_client = boto3.client('lambda')
        
        # Get the orchestrator Lambda function name
        function_name = "avesa-pipeline-orchestrator-dev"
        
        # Generate test payload
        payload = generate_test_payload()
        
        print(f"🧪 Testing orchestrator Lambda directly...")
        print(f"📋 Test payload:")
        print(json.dumps(payload, indent=2))
        
        # Invoke the Lambda function
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Parse response
        response_payload = json.loads(response['Payload'].read())
        
        print(f"✅ Lambda invocation successful:")
        print(f"   Status Code: {response.get('StatusCode')}")
        print(f"📄 Response:")
        print(json.dumps(response_payload, indent=2))
        
        return True
        
    except Exception as e:
        print(f"❌ Error invoking Lambda directly: {str(e)}")
        return False

if __name__ == "__main__":
    print("🧪 Testing OrchestratorPipeline-dev for tickets table (sitetechnology/ConnectWise)")
    print("=" * 80)
    
    # Try Step Functions first
    success = invoke_step_functions_execution()
    
    if not success:
        print("\n⚠️  Step Functions execution failed, trying direct Lambda invocation...")
        success = test_orchestrator_lambda_directly()
    
    if success:
        print("\n✅ Test execution initiated successfully!")
        print("   Check AWS CloudWatch Logs for detailed execution logs")
        print("   Monitor DynamoDB ProcessingJobs table for job status")
    else:
        print("\n❌ Test execution failed!")