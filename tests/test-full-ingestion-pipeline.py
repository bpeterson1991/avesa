#!/usr/bin/env python3
"""
AVESA Full Ingestion Pipeline Test

This script tests the complete end-to-end ingestion pipeline:
1. Triggers the optimized pipeline
2. Monitors job progression through all stages
3. Validates data ingestion and transformation
4. Checks S3 data storage
5. Provides detailed diagnostics
"""

import boto3
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

class FullIngestionTester:
    def __init__(self, region='us-east-2', environment='dev'):
        self.region = region
        self.environment = environment
        self.session = boto3.Session(region_name=region)
        
        # Initialize AWS clients
        self.lambda_client = self.session.client('lambda')
        self.stepfunctions_client = self.session.client('stepfunctions')
        self.dynamodb = self.session.resource('dynamodb')
        self.s3_client = self.session.client('s3')
        self.logs_client = self.session.client('logs')
        
        # Get table references
        self.tenant_services_table = self.dynamodb.Table(f'TenantServices-{environment}')
        self.processing_jobs_table = self.dynamodb.Table(f'ProcessingJobs-{environment}')
        self.chunk_progress_table = self.dynamodb.Table(f'ChunkProgress-{environment}')
        
        # Test configuration
        self.tenant_id = "sitetechnology"
        self.test_table = "companies"  # Start with companies table
        self.test_execution_id = f"ingestion-test-{uuid.uuid4().hex[:8]}"
        self.data_bucket = f"data-storage-msp-{environment}"

    def check_existing_jobs_status(self):
        """Check status of existing jobs to understand current state"""
        print("🔍 Checking existing job statuses...")
        
        try:
            # Get recent jobs for sitetechnology
            response = self.processing_jobs_table.scan(
                FilterExpression='tenant_id = :tenant_id',
                ExpressionAttributeValues={':tenant_id': self.tenant_id}
            )
            
            jobs = response.get('Items', [])
            if not jobs:
                print("📋 No existing jobs found")
                return
            
            print(f"📋 Found {len(jobs)} existing jobs:")
            
            status_counts = {}
            for job in jobs:
                job_id = job.get('job_id', 'Unknown')
                status = job.get('status', 'Unknown')
                created_at = job.get('created_at', 'Unknown')
                progress = job.get('progress', {})
                
                status_counts[status] = status_counts.get(status, 0) + 1
                
                print(f"   📋 {job_id}: {status} (created: {created_at})")
                if progress:
                    tables = progress.get('tables_processed', 0)
                    chunks = progress.get('chunks_processed', 0)
                    records = progress.get('records_processed', 0)
                    print(f"      Progress: {tables} tables, {chunks} chunks, {records} records")
            
            print(f"📊 Status Summary: {status_counts}")
            
        except Exception as e:
            print(f"❌ Error checking existing jobs: {str(e)}")

    def check_step_functions_executions(self):
        """Check Step Functions executions to see if workflows are running"""
        print("🔍 Checking Step Functions executions...")
        
        try:
            # Get state machines
            response = self.stepfunctions_client.list_state_machines()
            state_machines = [sm for sm in response['stateMachines'] if 'dev' in sm['name']]
            
            for sm in state_machines:
                sm_name = sm['name']
                sm_arn = sm['stateMachineArn']
                
                print(f"📋 Checking {sm_name}...")
                
                # Get recent executions
                executions_response = self.stepfunctions_client.list_executions(
                    stateMachineArn=sm_arn,
                    maxResults=5
                )
                
                executions = executions_response.get('executions', [])
                if executions:
                    print(f"   Found {len(executions)} recent executions:")
                    for execution in executions:
                        name = execution['name']
                        status = execution['status']
                        start_date = execution['startDate']
                        print(f"   📋 {name}: {status} (started: {start_date})")
                else:
                    print(f"   No recent executions found")
                    
        except Exception as e:
            print(f"❌ Error checking Step Functions: {str(e)}")

    def check_lambda_logs(self, function_name: str, minutes: int = 30):
        """Check recent Lambda logs for errors"""
        print(f"📋 Checking logs for {function_name}...")
        
        try:
            log_group = f"/aws/lambda/{function_name}"
            
            # Get recent log streams
            streams_response = self.logs_client.describe_log_streams(
                logGroupName=log_group,
                orderBy='LastEventTime',
                descending=True,
                limit=5
            )
            
            if not streams_response['logStreams']:
                print(f"📋 No log streams found for {function_name}")
                return
            
            # Check each recent stream for errors
            for stream in streams_response['logStreams'][:3]:  # Check top 3 streams
                stream_name = stream['logStreamName']
                start_time = int((datetime.now() - timedelta(minutes=minutes)).timestamp() * 1000)
                
                try:
                    events_response = self.logs_client.get_log_events(
                        logGroupName=log_group,
                        logStreamName=stream_name,
                        startTime=start_time
                    )
                    
                    events = events_response.get('events', [])
                    error_events = [e for e in events if 'ERROR' in e['message'] or 'Exception' in e['message']]
                    
                    if error_events:
                        print(f"   ⚠️  Found {len(error_events)} error events in {stream_name}:")
                        for event in error_events[-3:]:  # Show last 3 errors
                            timestamp = datetime.fromtimestamp(event['timestamp'] / 1000)
                            message = event['message'].strip()
                            print(f"      {timestamp.strftime('%H:%M:%S')}: {message}")
                    else:
                        print(f"   ✅ No errors in {stream_name}")
                        
                except Exception as e:
                    print(f"   ⚠️  Error reading stream {stream_name}: {str(e)}")
                    
        except Exception as e:
            print(f"❌ Error checking logs for {function_name}: {str(e)}")

    def check_s3_data(self):
        """Check if data is being written to S3"""
        print("🔍 Checking S3 data storage...")
        
        try:
            # Check for recent data in S3
            prefix = f"raw/{self.tenant_id}/"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.data_bucket,
                Prefix=prefix,
                MaxKeys=10
            )
            
            objects = response.get('Contents', [])
            if objects:
                print(f"✅ Found {len(objects)} objects in S3:")
                for obj in objects:
                    key = obj['Key']
                    size = obj['Size']
                    modified = obj['LastModified']
                    print(f"   📄 {key} ({size} bytes, modified: {modified})")
            else:
                print(f"📄 No objects found in S3 bucket {self.data_bucket} with prefix {prefix}")
                
        except Exception as e:
            print(f"❌ Error checking S3: {str(e)}")

    def trigger_test_ingestion(self):
        """Trigger a test ingestion and monitor it closely"""
        print(f"🚀 Triggering test ingestion for {self.tenant_id}/{self.test_table}...")
        
        payload = {
            'tenant_id': self.tenant_id,
            'table_name': self.test_table,
            'force_full_sync': True,  # Force full sync for testing
            'execution_id': self.test_execution_id,
            'ingestion_test': True
        }
        
        try:
            response = self.lambda_client.invoke(
                FunctionName=f'avesa-pipeline-orchestrator-{self.environment}',
                InvocationType='RequestResponse',  # Synchronous for immediate feedback
                Payload=json.dumps(payload)
            )
            
            if response['StatusCode'] == 200:
                payload_response = json.loads(response['Payload'].read())
                
                if 'errorMessage' in payload_response:
                    print(f"❌ Pipeline error: {payload_response['errorMessage']}")
                    if 'errorType' in payload_response:
                        print(f"   Error type: {payload_response['errorType']}")
                    if 'stackTrace' in payload_response:
                        print(f"   Stack trace: {payload_response['stackTrace']}")
                    return False
                else:
                    print(f"✅ Pipeline triggered successfully")
                    print(f"   Response: {json.dumps(payload_response, indent=2)}")
                    return True
            else:
                print(f"❌ HTTP error: {response['StatusCode']}")
                return False
                
        except Exception as e:
            print(f"❌ Error triggering pipeline: {str(e)}")
            return False

    def monitor_test_execution(self, timeout_minutes: int = 15):
        """Monitor the test execution with detailed tracking"""
        print(f"👀 Monitoring test execution for {timeout_minutes} minutes...")
        
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        
        while time.time() - start_time < timeout_seconds:
            try:
                # Look for our specific job
                current_time = datetime.now()
                scan_start_time = current_time - timedelta(minutes=20)
                
                response = self.processing_jobs_table.scan(
                    FilterExpression='tenant_id = :tenant_id AND created_at > :start_time',
                    ExpressionAttributeValues={
                        ':tenant_id': self.tenant_id,
                        ':start_time': scan_start_time.isoformat()
                    }
                )
                
                jobs = response.get('Items', [])
                
                if jobs:
                    # Find the most recent job
                    latest_job = max(jobs, key=lambda x: x.get('created_at', ''))
                    job_id = latest_job.get('job_id')
                    status = latest_job.get('status', 'UNKNOWN')
                    progress = latest_job.get('progress', {})
                    
                    print(f"📊 Job {job_id}: {status}")
                    if progress:
                        tables = progress.get('tables_processed', 0)
                        chunks = progress.get('chunks_processed', 0)
                        records = progress.get('records_processed', 0)
                        errors = progress.get('errors', 0)
                        print(f"   Progress: {tables} tables, {chunks} chunks, {records} records")
                        if errors > 0:
                            print(f"   ⚠️  Errors: {errors}")
                    
                    # Check chunk progress
                    if job_id:
                        chunk_response = self.chunk_progress_table.query(
                            KeyConditionExpression='job_id = :job_id',
                            ExpressionAttributeValues={':job_id': job_id}
                        )
                        
                        chunks = chunk_response.get('Items', [])
                        if chunks:
                            chunk_statuses = {}
                            for chunk in chunks:
                                chunk_status = chunk.get('status', 'unknown')
                                chunk_statuses[chunk_status] = chunk_statuses.get(chunk_status, 0) + 1
                            
                            print(f"   📦 Chunks: {chunk_statuses}")
                    
                    if status in ['COMPLETED', 'FAILED']:
                        return status, latest_job
                else:
                    print("⏳ No recent jobs found, waiting...")
                
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                print(f"⚠️  Error monitoring: {str(e)}")
                time.sleep(30)
        
        print(f"⏰ Monitoring timeout after {timeout_minutes} minutes")
        return 'TIMEOUT', None

    def run_comprehensive_ingestion_test(self):
        """Run comprehensive ingestion pipeline test"""
        print("🚀 AVESA Full Ingestion Pipeline Test")
        print("=" * 60)
        print(f"Region: {self.region}")
        print(f"Environment: {self.environment}")
        print(f"Tenant: {self.tenant_id}")
        print(f"Table: {self.test_table}")
        print(f"Test Time: {datetime.now().isoformat()}")
        print()
        
        # Step 1: Check current system state
        print("🔍 STEP 1: System Diagnostics")
        print("-" * 40)
        self.check_existing_jobs_status()
        self.check_step_functions_executions()
        self.check_s3_data()
        
        # Step 2: Check for errors in Lambda logs
        print("\n🔍 STEP 2: Lambda Function Diagnostics")
        print("-" * 40)
        lambda_functions = [
            f'avesa-pipeline-orchestrator-{self.environment}',
            f'avesa-tenant-processor-{self.environment}',
            f'avesa-table-processor-{self.environment}',
            f'avesa-chunk-processor-{self.environment}'
        ]
        
        for func in lambda_functions:
            self.check_lambda_logs(func, minutes=60)
        
        # Step 3: Trigger new test ingestion
        print("\n🚀 STEP 3: Test Ingestion Trigger")
        print("-" * 40)
        
        if not self.trigger_test_ingestion():
            print("❌ Failed to trigger test ingestion")
            return False
        
        # Step 4: Monitor execution
        print("\n👀 STEP 4: Execution Monitoring")
        print("-" * 40)
        
        status, job_data = self.monitor_test_execution()
        
        # Step 5: Final validation
        print("\n✅ STEP 5: Final Validation")
        print("-" * 40)
        
        if status == 'COMPLETED':
            print("🎉 Ingestion test COMPLETED successfully!")
            self.check_s3_data()  # Check if data was written
            return True
        elif status == 'FAILED':
            print("❌ Ingestion test FAILED")
            if job_data:
                error_details = job_data.get('error_details', 'No error details')
                print(f"   Error: {error_details}")
            return False
        else:
            print(f"⚠️  Ingestion test status: {status}")
            print("💡 The pipeline may need more time or there may be configuration issues")
            return False

def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='AVESA Full Ingestion Pipeline Tester')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--environment', default='dev', help='Environment (dev/staging/prod)')
    parser.add_argument('--tenant', default='sitetechnology', help='Tenant ID to test')
    parser.add_argument('--table', default='companies', help='Table to test')
    
    args = parser.parse_args()
    
    tester = FullIngestionTester(region=args.region, environment=args.environment)
    tester.tenant_id = args.tenant
    tester.test_table = args.table
    
    success = tester.run_comprehensive_ingestion_test()
    
    exit(0 if success else 1)

if __name__ == "__main__":
    main()