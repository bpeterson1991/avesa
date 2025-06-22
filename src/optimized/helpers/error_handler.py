"""
Error Handler Lambda Function

Handles errors and failures in the optimized pipeline execution.
Logs errors, updates job status, and sends notifications.
"""

import json
import boto3
from datetime import datetime, timezone
from typing import Dict, Any

# Import shared modules from root shared directory
from shared.config_simple import Config
from shared.logger import PipelineLogger
from shared.aws_clients import get_dynamodb_client, get_cloudwatch_client
from shared.utils import get_timestamp


class ErrorHandler:
    """Handles pipeline errors and failures."""
    
    def __init__(self):
        self.config = Config.from_environment()
        self.logger = PipelineLogger("error-handler")
        self.dynamodb = get_dynamodb_client()
        self.cloudwatch = get_cloudwatch_client()
        
        # DynamoDB table names
        self.processing_jobs_table = f"ProcessingJobs-{self.config.environment}"
    
    def handle_error(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """
        Handle pipeline errors and update job status.
        
        Args:
            event: Error event details
            context: Lambda context
            
        Returns:
            Error handling result
        """
        try:
            self.logger.info("Handling pipeline error", event=event)
            
            error_type = event.get('error_type', 'unknown')
            error_details = event.get('error_details', {})
            
            # Extract job information
            job_id = self._extract_job_id(event)
            
            # Update job status to failed
            if job_id:
                self._update_job_status(job_id, 'failed', error_details)
            
            # Send error metrics
            self._send_error_metrics(error_type, job_id)
            
            # Log error details
            self.logger.error(
                f"Pipeline error handled: {error_type}",
                job_id=job_id,
                error_details=error_details
            )
            
            result = {
                'error_handled': True,
                'error_type': error_type,
                'job_id': job_id,
                'timestamp': get_timestamp()
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error handler failed: {str(e)}", error=str(e))
            # Don't re-raise to avoid infinite error loops
            return {
                'error_handled': False,
                'handler_error': str(e),
                'timestamp': get_timestamp()
            }
    
    def _extract_job_id(self, event: Dict[str, Any]) -> str:
        """Extract job ID from error event."""
        # Try different possible locations for job_id
        if 'job_id' in event:
            return event['job_id']
        
        if 'pipeline_config' in event and 'job_id' in event['pipeline_config']:
            return event['pipeline_config']['job_id']
        
        if 'input' in event and isinstance(event['input'], dict):
            if 'job_id' in event['input']:
                return event['input']['job_id']
            if 'pipeline_config' in event['input'] and 'job_id' in event['input']['pipeline_config']:
                return event['input']['pipeline_config']['job_id']
        
        return None
    
    def _update_job_status(self, job_id: str, status: str, error_details: Dict[str, Any]):
        """Update job status in DynamoDB."""
        try:
            update_expression = "SET #status = :status, updated_at = :updated_at"
            expression_attribute_names = {'#status': 'status'}
            expression_attribute_values = {
                ':status': {'S': status},
                ':updated_at': {'S': get_timestamp()}
            }
            
            # Add error details if provided
            if error_details:
                update_expression += ", error_details = :error_details"
                expression_attribute_values[':error_details'] = {
                    'S': json.dumps(error_details)
                }
            
            self.dynamodb.update_item(
                TableName=self.processing_jobs_table,
                Key={'job_id': {'S': job_id}},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            self.logger.info(f"Updated job status to {status}", job_id=job_id)
            
        except Exception as e:
            self.logger.error(f"Failed to update job status: {str(e)}", job_id=job_id)
    
    def _send_error_metrics(self, error_type: str, job_id: str):
        """Send error metrics to CloudWatch."""
        try:
            metrics = [
                {
                    'MetricName': 'PipelineError',
                    'Dimensions': [
                        {'Name': 'ErrorType', 'Value': error_type},
                        {'Name': 'Environment', 'Value': self.config.environment}
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
            
            if job_id:
                metrics.append({
                    'MetricName': 'JobFailed',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id},
                        {'Name': 'Environment', 'Value': self.config.environment}
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                })
            
            self.cloudwatch.put_metric_data(
                Namespace='AVESA/DataPipeline/Errors',
                MetricData=metrics
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to send error metrics: {str(e)}")


def lambda_handler(event, context):
    """Lambda entry point."""
    handler = ErrorHandler()
    return handler.handle_error(event, context)