"""
Result Aggregator Lambda Function

Aggregates results from tenant processing and updates job status.
Calculates final metrics and prepares completion data.
"""

import json
import boto3
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# Fallback implementations for when shared modules aren't available
try:
    import sys
    import os
    sys.path.append('/opt/python')
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
    from config_simple import Config
    from logger import PipelineLogger
    from aws_clients import get_dynamodb_client, get_cloudwatch_client
    from utils import get_timestamp
except ImportError:
    import os
    
    class Config:
        def __init__(self, **kwargs):
            self.environment = kwargs.get('environment', 'dev')
            self.processing_jobs_table = kwargs.get('processing_jobs_table')
        
        @classmethod
        def from_environment(cls):
            return cls(
                environment=os.environ.get("ENVIRONMENT", "dev"),
                processing_jobs_table=os.environ.get("PROCESSING_JOBS_TABLE")
            )
    
    class PipelineLogger:
        def __init__(self, name):
            self.name = name
        
        def info(self, message, **kwargs):
            print(f"INFO [{self.name}]: {message} {kwargs}")
        
        def error(self, message, **kwargs):
            print(f"ERROR [{self.name}]: {message} {kwargs}")
        
        def warning(self, message, **kwargs):
            print(f"WARNING [{self.name}]: {message} {kwargs}")
    
    def get_dynamodb_client():
        return boto3.client('dynamodb')
    
    def get_cloudwatch_client():
        return boto3.client('cloudwatch')
    
    def get_timestamp():
        return datetime.now(timezone.utc).isoformat()


class ResultAggregator:
    """Aggregates results from pipeline execution."""
    
    def __init__(self):
        self.config = Config.from_environment()
        self.logger = PipelineLogger("result-aggregator")
        self.dynamodb = get_dynamodb_client()
        self.cloudwatch = get_cloudwatch_client()
        
        # DynamoDB table names
        self.processing_jobs_table = f"ProcessingJobs-{self.config.environment}"
        self.chunk_progress_table = f"ChunkProgress-{self.config.environment}"
    
    def aggregate_results(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """
        Aggregate results from tenant processing.
        
        Args:
            event: Aggregation event with results
            context: Lambda context
            
        Returns:
            Aggregated results
        """
        try:
            self.logger.info("Aggregating pipeline results", event=event)
            
            job_id = event.get('job_id')
            processing_mode = event.get('processing_mode', 'unknown')
            
            if processing_mode == 'multi-tenant':
                results = self._aggregate_multi_tenant_results(event)
            else:
                results = self._aggregate_single_tenant_results(event)
            
            # Update job status
            self._update_job_completion(job_id, results)
            
            # Send completion metrics
            self._send_completion_metrics(job_id, results)
            
            self.logger.info(
                "Pipeline results aggregated successfully",
                job_id=job_id,
                processing_mode=processing_mode,
                total_records=results.get('total_records_processed', 0)
            )
            
            return results
            
        except Exception as e:
            self.logger.error(f"Result aggregation failed: {str(e)}", error=str(e))
            raise
    
    def _aggregate_multi_tenant_results(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Aggregate results from multiple tenant executions."""
        tenant_results = event.get('tenant_results', [])
        
        total_records = 0
        successful_tenants = 0
        failed_tenants = 0
        tenant_summaries = []
        
        for result in tenant_results:
            if isinstance(result, dict):
                # Extract result from Step Functions execution
                output = result.get('Output', {})
                if isinstance(output, str):
                    try:
                        output = json.loads(output)
                    except json.JSONDecodeError:
                        output = {}
                
                tenant_id = output.get('tenant_id', 'unknown')
                status = output.get('status', 'unknown')
                records_processed = output.get('records_processed', 0)
                
                total_records += records_processed
                
                if status == 'completed':
                    successful_tenants += 1
                else:
                    failed_tenants += 1
                
                tenant_summaries.append({
                    'tenant_id': tenant_id,
                    'status': status,
                    'records_processed': records_processed,
                    'tables_processed': output.get('tables_processed', 0),
                    'processing_time': output.get('processing_time', 0)
                })
        
        return {
            'processing_mode': 'multi-tenant',
            'total_tenants': len(tenant_results),
            'successful_tenants': successful_tenants,
            'failed_tenants': failed_tenants,
            'total_records_processed': total_records,
            'tenant_summaries': tenant_summaries,
            'aggregated_at': get_timestamp()
        }
    
    def _aggregate_single_tenant_results(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Aggregate results from single tenant execution."""
        tenant_result = event.get('tenant_result', {})
        
        # Extract result from Step Functions execution
        output = tenant_result.get('Output', {})
        if isinstance(output, str):
            try:
                output = json.loads(output)
            except json.JSONDecodeError:
                output = {}
        
        tenant_id = output.get('tenant_id', 'unknown')
        status = output.get('status', 'unknown')
        records_processed = output.get('records_processed', 0)
        
        return {
            'processing_mode': 'single-tenant',
            'tenant_id': tenant_id,
            'status': status,
            'total_records_processed': records_processed,
            'tables_processed': output.get('tables_processed', 0),
            'processing_time': output.get('processing_time', 0),
            'successful_tenants': 1 if status == 'completed' else 0,
            'failed_tenants': 1 if status != 'completed' else 0,
            'aggregated_at': get_timestamp()
        }
    
    def _update_job_completion(self, job_id: str, results: Dict[str, Any]):
        """Update job completion status in DynamoDB."""
        try:
            if not job_id:
                self.logger.warning("No job_id provided for completion update")
                return
            
            # Determine overall status
            failed_tenants = results.get('failed_tenants', 0)
            successful_tenants = results.get('successful_tenants', 0)
            
            if failed_tenants > 0 and successful_tenants == 0:
                status = 'failed'
            elif failed_tenants > 0:
                status = 'partial_success'
            else:
                status = 'completed'
            
            # Update job record
            update_expression = """
                SET #status = :status,
                    updated_at = :updated_at,
                    completed_at = :completed_at,
                    total_records_processed = :total_records,
                    completed_tenants = :completed_tenants,
                    failed_tenants = :failed_tenants,
                    results = :results
            """
            
            expression_attribute_names = {'#status': 'status'}
            expression_attribute_values = {
                ':status': {'S': status},
                ':updated_at': {'S': get_timestamp()},
                ':completed_at': {'S': get_timestamp()},
                ':total_records': {'N': str(results.get('total_records_processed', 0))},
                ':completed_tenants': {'N': str(successful_tenants)},
                ':failed_tenants': {'N': str(failed_tenants)},
                ':results': {'S': json.dumps(results)}
            }
            
            self.dynamodb.update_item(
                TableName=self.processing_jobs_table,
                Key={'job_id': {'S': job_id}},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            self.logger.info(f"Updated job completion status to {status}", job_id=job_id)
            
        except Exception as e:
            self.logger.error(f"Failed to update job completion: {str(e)}", job_id=job_id)
    
    def _send_completion_metrics(self, job_id: str, results: Dict[str, Any]):
        """Send completion metrics to CloudWatch."""
        try:
            metrics = [
                {
                    'MetricName': 'PipelineCompleted',
                    'Dimensions': [
                        {'Name': 'Environment', 'Value': self.config.environment},
                        {'Name': 'ProcessingMode', 'Value': results.get('processing_mode', 'unknown')}
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'RecordsProcessed',
                    'Dimensions': [
                        {'Name': 'Environment', 'Value': self.config.environment}
                    ],
                    'Value': results.get('total_records_processed', 0),
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'SuccessfulTenants',
                    'Dimensions': [
                        {'Name': 'Environment', 'Value': self.config.environment}
                    ],
                    'Value': results.get('successful_tenants', 0),
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'FailedTenants',
                    'Dimensions': [
                        {'Name': 'Environment', 'Value': self.config.environment}
                    ],
                    'Value': results.get('failed_tenants', 0),
                    'Unit': 'Count'
                }
            ]
            
            if job_id:
                metrics.append({
                    'MetricName': 'JobCompleted',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id},
                        {'Name': 'Environment', 'Value': self.config.environment}
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                })
            
            self.cloudwatch.put_metric_data(
                Namespace='AVESA/DataPipeline/Completion',
                MetricData=metrics
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to send completion metrics: {str(e)}")


def lambda_handler(event, context):
    """Lambda entry point."""
    aggregator = ResultAggregator()
    return aggregator.aggregate_results(event, context)