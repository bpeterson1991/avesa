"""
Pipeline Orchestrator Lambda Function

This is the main entry point for the optimized AVESA data pipeline.
It initializes pipeline execution, discovers tenants, and coordinates
the overall processing workflow through Step Functions.
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import boto3
from botocore.exceptions import ClientError

# Import shared modules from local shared directory
from shared.config_simple import Config, TenantConfig
from shared.logger import PipelineLogger
from shared.aws_clients import get_dynamodb_client, get_cloudwatch_client
from shared.utils import get_timestamp


class PipelineOrchestrator:
    """Main orchestrator for the optimized data pipeline."""
    
    def __init__(self):
        self.config = Config.from_environment()
        self.logger = PipelineLogger("pipeline-orchestrator")
        self.dynamodb = get_dynamodb_client()
        self.cloudwatch = get_cloudwatch_client()
        self.stepfunctions = boto3.client('stepfunctions')
        
        # Initialize new DynamoDB tables for optimization
        self.processing_jobs_table = f"ProcessingJobs-{self.config.environment}"
        self.chunk_progress_table = f"ChunkProgress-{self.config.environment}"
        
        # Step Functions state machine ARN
        self.state_machine_arn = os.environ.get('STATE_MACHINE_ARN')
    
    def lambda_handler(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """
        Main Lambda handler for pipeline orchestration.
        
        Args:
            event: Lambda event containing pipeline configuration
            context: Lambda context
            
        Returns:
            Pipeline configuration for Step Functions execution
        """
        try:
            self.logger.info("Starting pipeline orchestration", event=event)
            
            # Generate unique job ID
            job_id = self._generate_job_id()
            
            # Determine processing mode and discover tenants
            tenant_id = event.get('tenant_id')
            table_name = event.get('table_name')
            force_full_sync = event.get('force_full_sync', False)
            
            tenants = self._discover_tenants(tenant_id)
            processing_mode = 'multi-tenant' if len(tenants) > 1 else 'single-tenant'
            
            # Create processing job record
            job_record = self._create_processing_job(
                job_id=job_id,
                tenants=tenants,
                processing_mode=processing_mode,
                table_name=table_name,
                force_full_sync=force_full_sync,
                execution_id=context.aws_request_id
            )
            
            # Calculate processing estimates
            estimated_duration = self._calculate_processing_estimate(tenants, table_name)
            
            # Send initial metrics
            self._send_initialization_metrics(job_id, len(tenants), processing_mode)
            
            result = {
                'job_id': job_id,
                'mode': processing_mode,
                'tenants': tenants,
                'estimated_duration': estimated_duration,
                'table_name': table_name,
                'force_full_sync': force_full_sync,
                'created_at': get_timestamp()
            }
            
            # Trigger Step Functions workflow
            if self.state_machine_arn:
                execution_name = f"{job_id}-{context.aws_request_id[:8]}"
                self._trigger_step_functions(execution_name, result)
                result['step_functions_execution'] = execution_name
                self.logger.info(f"Triggered Step Functions execution: {execution_name}")
            else:
                self.logger.warning("STATE_MACHINE_ARN not configured, skipping Step Functions trigger")
            
            self.logger.info(
                "Pipeline orchestration completed successfully",
                job_id=job_id,
                mode=processing_mode,
                tenant_count=len(tenants),
                estimated_duration=estimated_duration
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Pipeline orchestration failed: {str(e)}", error=str(e))
            raise
    
    def _generate_job_id(self) -> str:
        """Generate unique job ID."""
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        return f"job-{timestamp}-{unique_id}"
    
    def _discover_tenants(self, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Discover tenants for processing.
        
        Args:
            tenant_id: Specific tenant ID or None for all tenants
            
        Returns:
            List of tenant configurations
        """
        try:
            if tenant_id:
                # Process specific tenant
                response = self.dynamodb.query(
                    TableName=self.config.tenant_services_table,
                    KeyConditionExpression='tenant_id = :tenant_id',
                    ExpressionAttributeValues={':tenant_id': {'S': tenant_id}}
                )
                
                if not response.get('Items'):
                    raise ValueError(f"Tenant {tenant_id} not found")
                
                return [self._format_tenant_config(response['Items'])]
            else:
                # Process all enabled tenants
                response = self.dynamodb.scan(
                    TableName=self.config.tenant_services_table,
                    FilterExpression='attribute_exists(enabled) AND enabled = :enabled',
                    ExpressionAttributeValues={':enabled': {'BOOL': True}}
                )
                
                # Group by tenant_id
                tenants_by_id = {}
                for item in response.get('Items', []):
                    tid = item['tenant_id']['S']
                    if tid not in tenants_by_id:
                        tenants_by_id[tid] = []
                    tenants_by_id[tid].append(item)
                
                return [
                    self._format_tenant_config(items) 
                    for items in tenants_by_id.values()
                ]
                
        except Exception as e:
            self.logger.error(f"Failed to discover tenants: {str(e)}")
            raise
    
    def _format_tenant_config(self, tenant_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Format tenant configuration from DynamoDB items."""
        if not tenant_items:
            raise ValueError("No tenant items provided")
        
        # Get tenant info from first item
        first_item = tenant_items[0]
        tenant_id = first_item['tenant_id']['S']
        
        # Extract services
        services = []
        for item in tenant_items:
            service_name = item['service']['S']
            enabled = item.get('enabled', {'BOOL': True})['BOOL']
            
            if enabled:
                services.append({
                    'service_name': service_name,
                    'enabled': enabled,
                    'config': item.get('config', {})
                })
        
        return {
            'tenant_id': tenant_id,
            'services': services,
            'enabled': True
        }
    
    def _create_processing_job(
        self,
        job_id: str,
        tenants: List[Dict[str, Any]],
        processing_mode: str,
        table_name: Optional[str],
        force_full_sync: bool,
        execution_id: str
    ) -> Dict[str, Any]:
        """Create processing job record in DynamoDB."""
        try:
            job_record = {
                'job_id': {'S': job_id},
                'tenant_id': {'S': 'ALL' if processing_mode == 'multi-tenant' else tenants[0]['tenant_id']},
                'status': {'S': 'initializing'},
                'processing_mode': {'S': processing_mode},
                'tenant_count': {'N': str(len(tenants))},
                'table_name': {'S': table_name or 'ALL'},
                'force_full_sync': {'BOOL': force_full_sync},
                'execution_id': {'S': execution_id},
                'created_at': {'S': get_timestamp()},
                'updated_at': {'S': get_timestamp()},
                'estimated_duration': {'N': str(self._calculate_processing_estimate(tenants, table_name))},
                'completed_tenants': {'N': '0'},
                'failed_tenants': {'N': '0'},
                'total_records_processed': {'N': '0'}
            }
            
            self.dynamodb.put_item(
                TableName=self.processing_jobs_table,
                Item=job_record
            )
            
            self.logger.info(f"Created processing job record", job_id=job_id)
            return job_record
            
        except Exception as e:
            self.logger.error(f"Failed to create processing job: {str(e)}")
            raise
    
    def _calculate_processing_estimate(
        self,
        tenants: List[Dict[str, Any]],
        table_name: Optional[str]
    ) -> int:
        """
        Calculate estimated processing duration in seconds using dynamic service discovery.
        
        Args:
            tenants: List of tenant configurations
            table_name: Specific table or None for all tables
            
        Returns:
            Estimated duration in seconds
        """
        try:
            # Import dynamic utilities for service discovery
            from shared.utils import discover_canonical_tables, get_canonical_table_for_endpoint
            
            # Dynamic estimates based on canonical table types (in seconds)
            canonical_estimates = {
                'tickets': 300,          # 5 minutes - complex data
                'time_entries': 180,     # 3 minutes - medium complexity
                'companies': 120,        # 2 minutes - simple structure
                'contacts': 240,         # 4 minutes - medium complexity
                'products': 150,         # 2.5 minutes - simple structure
                'agreements': 200,       # 3.3 minutes - medium complexity
                'projects': 180,         # 3 minutes - medium complexity
                'members': 90            # 1.5 minutes - simple structure
            }
            
            if table_name:
                # Single table processing - try to get canonical table type
                total_time = 0
                for tenant in tenants:
                    # Get services for this tenant
                    services = tenant.get('services', [])
                    for service in services:
                        service_name = service.get('service_name', 'unknown')
                        # Try to find canonical table for this specific table
                        canonical_table = get_canonical_table_for_endpoint(service_name, table_name)
                        base_time = canonical_estimates.get(canonical_table, 180)  # Default 3 minutes
                        total_time += base_time
                
                return total_time if total_time > 0 else 180 * len(tenants)
            else:
                # All tables processing - estimate based on discovered services
                total_time = 0
                for tenant in tenants:
                    services = tenant.get('services', [])
                    tenant_time = 0
                    
                    if services:
                        # Calculate based on actual services configured for tenant
                        for service in services:
                            service_name = service.get('service_name', 'unknown')
                            # Get all canonical tables for this service
                            canonical_tables = discover_canonical_tables()
                            for canonical_table in canonical_tables:
                                # Check if this service contributes to this canonical table
                                try:
                                    from shared.utils import get_service_tables_for_canonical
                                    service_tables = get_service_tables_for_canonical(canonical_table)
                                    if service_name in service_tables:
                                        tenant_time += canonical_estimates.get(canonical_table, 180)
                                except:
                                    pass
                    
                    # Fallback if no services found - use average estimate
                    if tenant_time == 0:
                        tenant_time = sum(canonical_estimates.values()) // len(canonical_estimates) * 4  # Assume 4 tables average
                    
                    total_time += tenant_time
                
                # Account for parallelization (assume 50% efficiency)
                return int(total_time * 0.5) if total_time > 0 else 600  # Default 10 minutes
                
        except Exception as e:
            self.logger.warning(f"Failed to calculate dynamic estimates, using fallback: {e}")
            # Fallback to simple calculation
            base_time = 180  # 3 minutes default
            if table_name:
                return base_time * len(tenants)
            else:
                return base_time * len(tenants) * 4  # Assume 4 tables per tenant
    
    def _trigger_step_functions(self, execution_name: str, pipeline_config: Dict[str, Any]):
        """Trigger Step Functions state machine execution."""
        try:
            response = self.stepfunctions.start_execution(
                stateMachineArn=self.state_machine_arn,
                name=execution_name,
                input=json.dumps(pipeline_config)
            )
            
            self.logger.info(
                "Step Functions execution started",
                execution_arn=response['executionArn'],
                execution_name=execution_name
            )
            
            return response
            
        except Exception as e:
            self.logger.error(f"Failed to trigger Step Functions: {str(e)}")
            raise
    
    def _send_initialization_metrics(self, job_id: str, tenant_count: int, processing_mode: str):
        """Send CloudWatch metrics for pipeline initialization."""
        try:
            metrics = [
                {
                    'MetricName': 'PipelineInitialized',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id},
                        {'Name': 'ProcessingMode', 'Value': processing_mode}
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'TenantCount',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id}
                    ],
                    'Value': tenant_count,
                    'Unit': 'Count'
                }
            ]
            
            self.cloudwatch.put_metric_data(
                Namespace='AVESA/DataPipeline',
                MetricData=metrics
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to send initialization metrics: {str(e)}")


def lambda_handler(event, context):
    """Lambda entry point."""
    orchestrator = PipelineOrchestrator()
    return orchestrator.lambda_handler(event, context)