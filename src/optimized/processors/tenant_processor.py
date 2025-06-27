"""
Tenant Processor Lambda Function

Handles processing of all enabled tables for a single tenant.
Implements parallel table processing and comprehensive error handling.
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import boto3
from botocore.exceptions import ClientError

# Import shared modules from root shared directory
from shared.config_simple import Config, TenantConfig
from shared.logger import PipelineLogger
from shared.aws_clients import get_dynamodb_client, get_cloudwatch_client, get_secrets_client
from shared.utils import get_timestamp


class TenantProcessor:
    """Processes all tables for a single tenant with parallel execution."""
    
    def __init__(self):
        self.config = Config.from_environment()
        self.logger = PipelineLogger("tenant-processor")
        self.dynamodb = get_dynamodb_client()
        self.cloudwatch = get_cloudwatch_client()
        self.secrets_client = get_secrets_client()
        
        # Table names
        self.processing_jobs_table = f"ProcessingJobs-{self.config.environment}"
        self.chunk_progress_table = f"ChunkProgress-{self.config.environment}"
    
    def lambda_handler(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """
        Main Lambda handler for tenant processing.
        
        Args:
            event: Event containing tenant configuration and job details
            context: Lambda context
            
        Returns:
            Tenant processing results
        """
        try:
            tenant_config = event['tenant_config']
            job_id = event['job_id']
            tenant_id = tenant_config['tenant_id']
            
            self.logger = PipelineLogger("tenant-processor", tenant_id=tenant_id)
            self.logger.info("Starting tenant processing", job_id=job_id)
            
            # Discover enabled tables for this tenant
            enabled_tables = self._discover_tenant_tables(tenant_config, event.get('table_name'))
            
            if not enabled_tables:
                self.logger.info("No enabled tables found for tenant")
                return {
                    'tenant_id': tenant_id,
                    'status': 'completed',
                    'message': 'No enabled tables found',
                    'table_count': 0,
                    'tables_processed': 0,
                    'records_processed': 0
                }
            
            # Update job status
            self._update_tenant_status(job_id, tenant_id, 'processing', len(enabled_tables))
            
            # Send metrics
            self._send_tenant_metrics(job_id, tenant_id, 'started', len(enabled_tables))
            
            # Return table discovery results for Step Functions orchestration
            result = {
                'tenant_id': tenant_id,
                'status': 'discovered',
                'table_discovery': {
                    'enabled_tables': enabled_tables,
                    'table_count': len(enabled_tables)
                },
                'job_id': job_id,
                'processing_time': self._calculate_processing_time(context),
                'discovered_at': get_timestamp()
            }
            
            self.logger.info(
                "Tenant table discovery completed",
                tenant_id=tenant_id,
                table_count=len(enabled_tables)
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Tenant processing failed: {str(e)}", error=str(e))
            
            # Update job status to failed
            if 'job_id' in locals() and 'tenant_id' in locals():
                self._update_tenant_status(job_id, tenant_id, 'failed', 0, str(e))
            
            raise
    
    def _discover_tenant_tables(
        self, 
        tenant_config: Dict[str, Any], 
        specific_table: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Discover enabled tables for the tenant.
        
        Args:
            tenant_config: Tenant configuration
            specific_table: Specific table to process or None for all
            
        Returns:
            List of enabled table configurations
        """
        try:
            tenant_id = tenant_config['tenant_id']
            
            # Get tenant service configuration from DynamoDB
            response = self.dynamodb.query(
                TableName=self.config.tenant_services_table,
                KeyConditionExpression='tenant_id = :tenant_id',
                ExpressionAttributeValues={':tenant_id': {'S': tenant_id}}
            )
            
            if not response.get('Items'):
                self.logger.warning(f"No services found for tenant {tenant_id}")
                return []
            
            enabled_tables = []
            
            # Process each service for the tenant
            for item in response['Items']:
                service_name = item['service']['S']
                service_enabled = item.get('enabled', {'BOOL': True})['BOOL']
                
                if not service_enabled:
                    continue
                
                # Get service configuration
                service_config = self._get_service_config(service_name)
                if not service_config:
                    continue
                
                # Get tables for this service
                tables = service_config.get('tables', [])
                
                for table in tables:
                    table_name = table.get('table_name')
                    table_enabled = table.get('enabled', True)
                    
                    # Skip if table is disabled
                    if not table_enabled:
                        continue
                    
                    # Skip if specific table requested and this isn't it
                    if specific_table and table_name != specific_table:
                        continue
                    
                    # Get tenant-specific credentials
                    credentials = self._get_tenant_credentials(tenant_id, service_name, item)
                    
                    table_config = {
                        'table_name': table_name,
                        'service_name': service_name,
                        'tenant_id': tenant_id,
                        'endpoint': table.get('endpoint'),
                        'enabled': table_enabled,
                        'credentials': credentials,
                        'api_config': table.get('api_config', {}),
                        'processing_config': table.get('processing_config', {})
                    }
                    
                    enabled_tables.append(table_config)
            
            self.logger.info(f"Discovered {len(enabled_tables)} enabled tables")
            return enabled_tables
            
        except Exception as e:
            self.logger.error(f"Failed to discover tenant tables: {str(e)}")
            raise
    
    def _get_service_config(self, service_name: str) -> Optional[Dict[str, Any]]:
        """Get service configuration from mappings dynamically."""
        try:
            # Import dynamic utilities with correct path
            build_service_table_configurations = None
            
            try:
                sys.path.insert(0, os.path.join('/var/task', 'src', 'shared'))
                from shared.utils import build_service_table_configurations
                except ImportError:
                self.logger.error(f"Could not import build_service_table_configurations")
            return None
            
            if not build_service_table_configurations:
                self.logger.error(f"build_service_table_configurations function not available")
                return None
            
            # Build table configurations dynamically from mappings
            table_configs = build_service_table_configurations(service_name)
            
            if table_configs:
                self.logger.info(f"Found {len(table_configs)} table configurations for {service_name}")
                return {'tables': table_configs}
            
            self.logger.warning(f"No table configurations found for {service_name}")
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get service config for {service_name}: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _get_tenant_credentials(
        self, 
        tenant_id: str, 
        service_name: str, 
        service_item: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get tenant credentials for the service."""
        try:
            # Get secret name from service configuration
            secret_name = service_item.get('secret_name', {}).get('S')
            if not secret_name:
                secret_name = f"{tenant_id}-{service_name}-credentials"
            
            # Retrieve credentials from Secrets Manager
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            secret_data = json.loads(response['SecretString'])
            
            # Extract service-specific credentials
            if service_name in secret_data:
                return secret_data[service_name]
            else:
                # Return the whole secret if service name not found
                return secret_data
            
        except Exception as e:
            self.logger.error(f"Failed to get credentials for {tenant_id}/{service_name}: {str(e)}")
            # Return empty credentials - will be handled by downstream processors
            return {}
    
    def _update_tenant_status(
        self, 
        job_id: str, 
        tenant_id: str, 
        status: str, 
        table_count: int,
        error_message: Optional[str] = None
    ):
        """Update tenant processing status in the job record."""
        try:
            update_expression = 'SET #status = :status, updated_at = :updated_at'
            expression_values = {
                ':status': {'S': status},
                ':updated_at': {'S': get_timestamp()}
            }
            
            if table_count > 0:
                update_expression += ', table_count = :table_count'
                expression_values[':table_count'] = {'N': str(table_count)}
            
            if error_message:
                update_expression += ', error_message = :error_message'
                expression_values[':error_message'] = {'S': error_message}
            
            self.dynamodb.update_item(
                TableName=self.processing_jobs_table,
                Key={
                    'job_id': {'S': job_id},
                    'tenant_id': {'S': tenant_id}
                },
                UpdateExpression=update_expression,
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues=expression_values
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to update tenant status: {str(e)}")
    
    
    
    
    def _calculate_processing_time(self, context) -> float:
        """Calculate processing time from Lambda context."""
        try:
            remaining_time = context.get_remaining_time_in_millis()
            total_time = 300000  # 5 minutes default timeout
            used_time = total_time - remaining_time
            return used_time / 1000.0  # Convert to seconds
        except:
            return 0.0

    def _send_tenant_metrics(self, job_id: str, tenant_id: str, event_type: str, table_count: int):
        """Send CloudWatch metrics for tenant processing."""
        try:
            metrics = [
                {
                    'MetricName': f'TenantProcessing{event_type.title()}',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id},
                        {'Name': 'TenantId', 'Value': tenant_id}
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
            
            if table_count > 0:
                metrics.append({
                    'MetricName': 'TenantTableCount',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id},
                        {'Name': 'TenantId', 'Value': tenant_id}
                    ],
                    'Value': table_count,
                    'Unit': 'Count'
                })
            
            self.cloudwatch.put_metric_data(
                Namespace='AVESA/DataPipeline',
                MetricData=metrics
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to send tenant metrics: {str(e)}")


def lambda_handler(event, context):
    """Lambda entry point."""
    processor = TenantProcessor()
    return processor.lambda_handler(event, context)