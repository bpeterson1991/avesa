"""
Completion Notifier Lambda Function

Sends notifications and final updates when pipeline execution completes.
Handles success and failure notifications.
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


class CompletionNotifier:
    """Handles pipeline completion notifications."""
    
    def __init__(self):
        self.config = Config.from_environment()
        self.logger = PipelineLogger("completion-notifier")
        self.cloudwatch = get_cloudwatch_client()
    
    def notify_completion(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """
        Send completion notifications.
        
        Args:
            event: Completion event with results
            context: Lambda context
            
        Returns:
            Notification result
        """
        try:
            self.logger.info("Sending pipeline completion notification", event=event)
            
            job_id = event.get('job_id')
            results = event.get('results', {})
            execution_arn = event.get('execution_arn')
            
            # Determine notification type
            failed_tenants = results.get('failed_tenants', 0)
            successful_tenants = results.get('successful_tenants', 0)
            
            if failed_tenants > 0 and successful_tenants == 0:
                notification_type = 'failure'
            elif failed_tenants > 0:
                notification_type = 'partial_success'
            else:
                notification_type = 'success'
            
            # Send notification metrics
            self._send_notification_metrics(job_id, notification_type, results)
            
            # Log completion summary
            self._log_completion_summary(job_id, notification_type, results)
            
            result = {
                'notification_sent': True,
                'notification_type': notification_type,
                'job_id': job_id,
                'execution_arn': execution_arn,
                'summary': self._create_summary(results),
                'notified_at': get_timestamp()
            }
            
            self.logger.info(
                f"Pipeline completion notification sent: {notification_type}",
                job_id=job_id,
                total_records=results.get('total_records_processed', 0)
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Completion notification failed: {str(e)}", error=str(e))
            # Don't re-raise to avoid blocking pipeline completion
            return {
                'notification_sent': False,
                'error': str(e),
                'notified_at': get_timestamp()
            }
    
    def _send_notification_metrics(self, job_id: str, notification_type: str, results: Dict[str, Any]):
        """Send notification metrics to CloudWatch."""
        try:
            metrics = [
                {
                    'MetricName': 'PipelineNotification',
                    'Dimensions': [
                        {'Name': 'NotificationType', 'Value': notification_type},
                        {'Name': 'Environment', 'Value': self.config.environment}
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
            
            # Add performance metrics
            total_records = results.get('total_records_processed', 0)
            if total_records > 0:
                metrics.append({
                    'MetricName': 'TotalRecordsProcessed',
                    'Dimensions': [
                        {'Name': 'Environment', 'Value': self.config.environment}
                    ],
                    'Value': total_records,
                    'Unit': 'Count'
                })
            
            processing_time = results.get('processing_time', 0)
            if processing_time > 0:
                metrics.append({
                    'MetricName': 'TotalProcessingTime',
                    'Dimensions': [
                        {'Name': 'Environment', 'Value': self.config.environment}
                    ],
                    'Value': processing_time,
                    'Unit': 'Seconds'
                })
                
                # Calculate throughput (records per second)
                if total_records > 0:
                    throughput = total_records / processing_time
                    metrics.append({
                        'MetricName': 'ProcessingThroughput',
                        'Dimensions': [
                            {'Name': 'Environment', 'Value': self.config.environment}
                        ],
                        'Value': throughput,
                        'Unit': 'Count/Second'
                    })
            
            self.cloudwatch.put_metric_data(
                Namespace='AVESA/DataPipeline/Notifications',
                MetricData=metrics
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to send notification metrics: {str(e)}")
    
    def _log_completion_summary(self, job_id: str, notification_type: str, results: Dict[str, Any]):
        """Log detailed completion summary."""
        summary = {
            'job_id': job_id,
            'notification_type': notification_type,
            'processing_mode': results.get('processing_mode', 'unknown'),
            'total_records_processed': results.get('total_records_processed', 0),
            'successful_tenants': results.get('successful_tenants', 0),
            'failed_tenants': results.get('failed_tenants', 0),
            'processing_time': results.get('processing_time', 0),
            'tables_processed': results.get('tables_processed', 0)
        }
        
        if notification_type == 'success':
            self.logger.info("Pipeline completed successfully", **summary)
        elif notification_type == 'partial_success':
            self.logger.warning("Pipeline completed with some failures", **summary)
        else:
            self.logger.error("Pipeline failed", **summary)
    
    def _create_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Create a summary of pipeline execution results."""
        return {
            'processing_mode': results.get('processing_mode', 'unknown'),
            'total_records_processed': results.get('total_records_processed', 0),
            'successful_tenants': results.get('successful_tenants', 0),
            'failed_tenants': results.get('failed_tenants', 0),
            'tables_processed': results.get('tables_processed', 0),
            'processing_time_seconds': results.get('processing_time', 0),
            'aggregated_at': results.get('aggregated_at'),
            'tenant_summaries': results.get('tenant_summaries', [])
        }


def lambda_handler(event, context):
    """Lambda entry point."""
    notifier = CompletionNotifier()
    return notifier.notify_completion(event, context)