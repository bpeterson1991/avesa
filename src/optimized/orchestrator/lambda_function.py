"""
Pipeline Orchestrator Lambda Function

"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import boto3
from botocore.exceptions import ClientError

# Import shared modules from root shared directory
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

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
        
        # Step Functions state machine ARN (will be discovered at runtime)
        self.state_machine_arn = None
    
    def lambda_handler(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """
        Enhanced Lambda handler for pipeline orchestration with backfill support.
        
        Args:
            event: Lambda event containing pipeline configuration
            context: Lambda context
            
        Returns:
            Pipeline configuration for Step Functions execution
        """
        try:
            self.logger.info(f"Lambda handler invoked - AWS Request ID: {context.aws_request_id}")
            self.logger.info("Starting pipeline orchestration", event=event)
            
            # Check for backfill mode
            backfill_mode = event.get('backfill_mode', False)
            backfill_date_range = event.get('backfill_date_range')
            
            if backfill_mode:
                self.logger.info("Running in backfill mode")
                return self._handle_backfill_orchestration(event, context)
            else:
                return self._handle_regular_orchestration(event, context)
                
        except Exception as e:
            self.logger.error(f"Pipeline orchestration failed: {str(e)}", error=str(e))
            raise
    
    def _handle_regular_orchestration(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """Handle regular (non-backfill) orchestration logic."""
        try:
            # Generate unique job ID
            job_id = self._generate_job_id()
            
            # Determine processing mode and discover tenants
            tenant_id = event.get('tenant_id')
            table_name = event.get('table_name')
            
            tenants = self._discover_tenants(tenant_id)
            processing_mode = 'multi-tenant' if len(tenants) > 1 else 'single-tenant'
            
            # CRITICAL DEBUG: Log discovered tenants
            self.logger.info(f"🔍 TENANT DISCOVERY: Found {len(tenants)} tenants",
                           tenant_count=len(tenants),
                           tenant_ids=[t.get('tenant_id') for t in tenants],
                           processing_mode=processing_mode)
            
            # Create processing job record
            job_record = self._create_processing_job(
                job_id=job_id,
                tenants=tenants,
                processing_mode=processing_mode,
                table_name=table_name,
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
                'record_limit': event.get('record_limit'),  # Pass through record limit
                'created_at': get_timestamp()
            }
            
            # Check if this is a manual backfill request
            source = event.get('source')
            if source == 'manual_backfill' or event.get('tenants'):
                self.logger.info("Detected manual backfill request, executing workflow directly")
                # Use the job_id from the manual request if provided
                if event.get('job_id'):
                    job_id = event.get('job_id')
                    result['job_id'] = job_id
                # CRITICAL FIX: Pass discovered tenants to workflow execution
                enhanced_event = event.copy()
                enhanced_event['tenants'] = tenants  # Pass discovered tenants
                enhanced_event['job_id'] = job_id
                # Execute the pipeline workflow directly for manual backfill
                workflow_result = self._execute_pipeline_workflow(enhanced_event, context)
                result.update(workflow_result)
            else:
                # Trigger Step Functions workflow for regular operations
                try:
                    state_machine_arn = self._get_state_machine_arn()
                    execution_name = f"{job_id}-{context.aws_request_id[:8]}"
                    self._trigger_step_functions(state_machine_arn, execution_name, result)
                    result['step_functions_execution'] = execution_name
                    self.logger.info(f"Triggered Step Functions execution: {execution_name}")
                except Exception as e:
                    self.logger.warning(f"Could not trigger Step Functions: {str(e)}")
                    # Fallback to direct execution if Step Functions fails
                    self.logger.info("Falling back to direct workflow execution")
                    # CRITICAL FIX: Pass discovered tenants to fallback workflow execution
                    enhanced_event = event.copy()
                    enhanced_event['tenants'] = tenants  # Pass discovered tenants
                    enhanced_event['job_id'] = job_id
                    workflow_result = self._execute_pipeline_workflow(enhanced_event, context)
                    result.update(workflow_result)
            
            self.logger.info(
                "Pipeline orchestration completed successfully",
                job_id=job_id,
                mode=processing_mode,
                tenant_count=len(tenants),
                estimated_duration=estimated_duration
            )
            
            return result
        
        except Exception as e:
            self.logger.error(f"Regular orchestration failed: {str(e)}", error=str(e))
            raise
    
    def _handle_backfill_orchestration(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """Handle backfill-specific orchestration logic."""
        try:
            # Extract backfill parameters
            job_id = event.get('job_id', self._generate_job_id())
            backfill_date_range = event.get('backfill_date_range')
            tenants = event.get('tenants', [])
            table_name = event.get('table_name')
            
            self.logger.info(f"Processing backfill job: {job_id}")
            
            # Update backfill job status to processing
            if 'BACKFILL_JOBS_TABLE' in os.environ:
                self._update_backfill_job_status(job_id, 'processing')
            
            # Process based on whether date range is specified
            if backfill_date_range:
                return self._handle_date_range_backfill(event, context, job_id)
            else:
                return self._handle_full_sync_backfill(event, context, job_id)
                
        except Exception as e:
            self.logger.error(f"Backfill orchestration failed: {str(e)}", error=str(e))
            # Update job status to failed
            if 'job_id' in locals():
                self._update_backfill_job_status(job_id, 'failed', str(e))
            raise
    
    def _handle_date_range_backfill(self, event: Dict[str, Any], context, job_id: str) -> Dict[str, Any]:
        """Handle backfill with specific date ranges (transactional data)."""
        backfill_date_range = event['backfill_date_range']
        start_date = backfill_date_range['start_date']
        end_date = backfill_date_range['end_date']
        chunk_size_days = backfill_date_range.get('chunk_size_days', 30)
        
        self.logger.info(f"Processing date range backfill: {start_date} to {end_date}")
        
        # Create date chunks
        chunks = self._create_date_chunks(start_date, end_date, chunk_size_days)
        
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
        
        # Create enhanced event for Step Functions
        enhanced_event = event.copy()
        enhanced_event.update({
            'job_id': job_id,
            'mode': 'single-tenant',
            'tenants': enhanced_tenants,
            'backfill_mode': True,
            'created_at': get_timestamp()
        })
        
        return enhanced_event
    
    def _handle_full_sync_backfill(self, event: Dict[str, Any], context, job_id: str) -> Dict[str, Any]:
        """Handle full sync backfill (master data)."""
        self.logger.info("Processing full sync backfill (master data)")
        
        # For master data, we don't need date chunking
        enhanced_event = event.copy()
        enhanced_event.update({
            'job_id': job_id,
            'mode': 'single-tenant',
            'master_data_mode': True,
            'backfill_mode': True,
            'created_at': get_timestamp()
        })
        
        # CRITICAL FIX: Actually execute the pipeline workflow for full sync backfill
        self.logger.info(f"Triggering actual data processing for backfill job: {job_id}")
        
        try:
            # Execute pipeline workflow directly for backfill processing
            workflow_result = self._execute_pipeline_workflow(enhanced_event, context)
            enhanced_event.update(workflow_result)
            
            # Update backfill job status to completed
            if 'BACKFILL_JOBS_TABLE' in os.environ:
                self._update_backfill_job_status(job_id, 'completed')
                
            self.logger.info(f"Full sync backfill completed successfully for job: {job_id}")
            
        except Exception as e:
            self.logger.error(f"Full sync backfill failed for job {job_id}: {str(e)}")
            
            # Update backfill job status to failed
            if 'BACKFILL_JOBS_TABLE' in os.environ:
                self._update_backfill_job_status(job_id, 'failed', str(e))
            
            # Re-raise the exception to ensure the orchestrator reports failure
            raise
        
        return enhanced_event
    
    def _create_date_chunks(self, start_date: str, end_date: str, chunk_size_days: int) -> List[tuple]:
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
        
        self.logger.info(f"Created {len(chunks)} date chunks for backfill processing")
        return chunks
    
    def _update_backfill_job_status(self, job_id: str, status: str, error_message: str = None):
        """Update backfill job status in DynamoDB."""
        try:
            backfill_jobs_table = os.environ.get('BACKFILL_JOBS_TABLE')
            if not backfill_jobs_table:
                return
                
            update_expression = 'SET job_status = :status, updated_at = :updated'
            expression_values = {
                ':status': status,
                ':updated': get_timestamp()
            }
            
            if error_message:
                update_expression += ', error_message = :error'
                expression_values[':error'] = error_message
            
            self.dynamodb.update_item(
                TableName=backfill_jobs_table,
                Key={'job_id': {'S': job_id}},
                UpdateExpression=update_expression,
                ExpressionAttributeValues={k: {'S': v} for k, v in expression_values.items()}
            )
            
            self.logger.info(f"Updated backfill job {job_id} status to {status}")
            
        except Exception as e:
            self.logger.error(f"Error updating backfill job status: {str(e)}")
    
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
        execution_id: str
    ) -> Dict[str, Any]:
        """Create processing job record in DynamoDB."""
        try:
            # Determine tenant_id for composite key
            tenant_id_value = 'ALL' if processing_mode == 'multi-tenant' else tenants[0]['tenant_id']
            
            job_record = {
                'job_id': {'S': job_id},
                'tenant_id': {'S': tenant_id_value},
                'status': {'S': 'initializing'},
                'processing_mode': {'S': processing_mode},
                'tenant_count': {'N': str(len(tenants))},
                'table_name': {'S': table_name or 'ALL'},
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
    
    def _get_state_machine_arn(self) -> str:
        """Get Step Functions state machine ARN at runtime."""
        try:
            if self.state_machine_arn:
                return self.state_machine_arn
                
            # Try to discover the state machine ARN
            response = self.stepfunctions.list_state_machines()
            
            # Look for our pipeline orchestrator state machine
            state_machine_name = f"PipelineOrchestrator-{self.config.environment}"
            
            for state_machine in response.get('stateMachines', []):
                if state_machine['name'] == state_machine_name:
                    self.state_machine_arn = state_machine['stateMachineArn']
                    self.logger.info(f"Discovered state machine ARN: {self.state_machine_arn}")
                    return self.state_machine_arn
            
            raise ValueError(f"State machine {state_machine_name} not found")
            
        except Exception as e:
            self.logger.error(f"Failed to get state machine ARN: {str(e)}")
            raise

    def _trigger_step_functions(self, state_machine_arn: str, execution_name: str, pipeline_config: Dict[str, Any]):
        """Trigger Step Functions state machine execution."""
        try:
            response = self.stepfunctions.start_execution(
                stateMachineArn=state_machine_arn,
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
    
    def _execute_pipeline_workflow(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """Execute pipeline workflow directly in Lambda - trigger actual data processing."""
        try:
            # CRITICAL DEBUG: Track method invocation to detect multiple calls
            self.logger.info(f"🚀 WORKFLOW EXECUTION START: _execute_pipeline_workflow called - AWS Request ID: {context.aws_request_id}")
            self.logger.info("Executing pipeline workflow directly")
            
            # Extract job details
            job_id = event.get('job_id')
            tenants = event.get('tenants', [])
            table_name = event.get('table_name')
            
            if not tenants:
                return {"status": "completed", "message": "No tenants to process"}
            
            # Initialize Lambda client for invoking actual processing functions
            lambda_client = boto3.client('lambda')
            
            total_records_processed = 0
            processed_tenants = 0
            
            self.logger.info(f"🔍 ORCHESTRATOR DEBUG: Processing {len(tenants)} tenants for job {job_id}")
            
            # CRITICAL DEBUG: Log all tenant IDs to identify duplicates
            tenant_ids = [tenant.get('tenant_id') for tenant in tenants]
            self.logger.info(f"🔍 TENANT DEBUG: All tenant IDs discovered: {tenant_ids}")
            
            # Check for duplicate tenant IDs
            duplicate_tenants = [tid for tid in set(tenant_ids) if tenant_ids.count(tid) > 1]
            if duplicate_tenants:
                self.logger.error(f"🚨 DUPLICATE TENANTS DETECTED: {duplicate_tenants}")
            
            # CRITICAL FIX: Process all canonical tables when table_name is None
            if table_name is None:
                # Discover all canonical tables and process each one
                from shared.utils import discover_canonical_tables
                canonical_tables = discover_canonical_tables()
                self.logger.info(f"🔍 MULTI-TABLE MODE: Processing all canonical tables: {canonical_tables}")
                
                if not canonical_tables:
                    self.logger.warning("⚠️ No canonical tables discovered, using hardcoded fallback list")
                    # Hardcoded fallback list based on known canonical tables in S3
                    canonical_tables = ["companies", "contacts", "tickets", "time_entries"]
                    self.logger.info(f"🔧 FALLBACK MODE: Using hardcoded canonical tables: {canonical_tables}")
            else:
                # Single table mode
                canonical_tables = [table_name]
                self.logger.info(f"🎯 SINGLE-TABLE MODE: Processing specific table: {table_name}")
            
            # Process each table for each tenant
            chunk_counter = 0
            for i, tenant in enumerate(tenants):
                tenant_id = tenant.get('tenant_id')
                
                for table_idx, current_table in enumerate(canonical_tables):
                    self.logger.info(f"🎯 ORCHESTRATOR DEBUG: Processing tenant {i+1}/{len(tenants)}: {tenant_id} for table: {current_table} ({table_idx+1}/{len(canonical_tables)})")
                    
                    # CRITICAL DEBUG: Log the exact iteration to track multiple invocations
                    self.logger.info(f"🔄 ITERATION DEBUG: About to invoke chunk processor - tenant: {tenant_id}, table: {current_table}, chunk_counter: {chunk_counter}")
                    
                    try:
                        # Get service configuration for the current table
                        service_config = self._get_service_config_for_table(tenant, current_table)
                        if not service_config:
                            self.logger.warning(f"⚠️ No service configuration found for table {current_table} and tenant {tenant_id}, skipping")
                            continue
                        
                        # CRITICAL DEBUG: Check if this tenant has already been processed
                        unique_chunk_id = f"{job_id}-{tenant_id}-{current_table}-{chunk_counter}"
                        self.logger.info(f"🔍 DUPLICATE CHECK: About to process chunk_id: {unique_chunk_id}")
                        
                        # Trigger actual data processing by invoking the chunk processor
                        chunk_processor_function = f"avesa-chunk-processor-{self.config.environment}"
                        
                        # Create payload for chunk processor with full service configuration
                        record_limit = event.get('record_limit')  # Default fallback
                        
                        self.logger.info(f"🔧 ORCHESTRATOR: Setting record limit for {tenant_id} table {current_table}: {record_limit}",
                                       record_limit=record_limit,
                                       table_name=current_table,
                                       event_limit=event.get('record_limit'))
                        
                        chunk_payload = {
                            "chunk_config": {
                                "chunk_id": unique_chunk_id,  # Use the pre-calculated unique chunk_id
                                "tenant_id": tenant_id,
                                "table_name": current_table,
                                "start_offset": 0,
                                "end_offset": 999999,  # Large range for full sync
                                "estimated_records": record_limit,  # Use actual limit from trigger
                                "record_limit": record_limit,  # Add explicit record limit
                                "page_size": 1000,  # Use higher page size for pagination fix
                                "offset": 0
                            },
                            "table_config": {
                                "table_name": current_table,
                                "service_name": service_config['service_name'],
                                "endpoint": service_config['endpoint'],
                                "credentials": service_config['credentials'],
                                "page_size": service_config.get('page_size', 1000)
                            },
                            "tenant_config": {
                                "tenant_id": tenant_id,
                                "services": tenant.get('services', [])
                            },
                            "job_id": job_id
                        }
                        
                        chunk_counter += 1
                        
                        self.logger.info(f"🔧 ORCHESTRATOR DEBUG: Invoking chunk processor for tenant {tenant_id}",
                                       function_name=chunk_processor_function,
                                       invocation_type='RequestResponse')
                        
                        # Invoke chunk processor asynchronously
                        import time
                        invoke_start_time = time.time()
                        
                        self.logger.info(f"🚀 ORCHESTRATOR DEBUG: About to invoke chunk processor",
                                       function_name=chunk_processor_function,
                                       payload_size=len(json.dumps(chunk_payload)),
                                       tenant_id=tenant_id,
                                       unique_chunk_id=unique_chunk_id,
                                       iteration_number=i+1,
                                       total_tenants=len(tenants))
                        
                        # CRITICAL DEBUG: This is where the actual Lambda invocation happens
                        self.logger.info(f"🚨 INVOCATION POINT: Invoking chunk processor NOW for {unique_chunk_id}")
                        
                        # Use asynchronous invocation with completion callback mechanism
                        response = lambda_client.invoke(
                            FunctionName=chunk_processor_function,
                            InvocationType='Event',  # Asynchronous to prevent timeouts
                            Payload=json.dumps(chunk_payload)
                        )
                        
                        # CRITICAL DEBUG: Log immediately after invocation
                        self.logger.info(f"✅ INVOCATION COMPLETE: Chunk processor invoked asynchronously for {unique_chunk_id}")
                        
                        invoke_duration = time.time() - invoke_start_time
                        
                        self.logger.info(f"🔧 ORCHESTRATOR DEBUG: Async Lambda invoke completed",
                                       status_code=response.get('StatusCode'),
                                       function_error=response.get('FunctionError'),
                                       executed_version=response.get('ExecutedVersion'),
                                       invoke_duration_seconds=invoke_duration)
                        
                        # Check for Lambda execution errors
                        if response.get('FunctionError'):
                            self.logger.error(f"🚨 ORCHESTRATOR DEBUG: Lambda function error detected",
                                            function_error=response.get('FunctionError'),
                                            status_code=response.get('StatusCode'))
                            raise Exception(f"Chunk processor invocation failed: {response.get('FunctionError')}")
                        
                        # For asynchronous invocations, we don't get payload response immediately
                        # Instead, we'll track the job in DynamoDB and the chunk processor will trigger result aggregator when done
                        self.logger.info(f"📊 ORCHESTRATOR DEBUG: Chunk processor invoked asynchronously, tracking job {unique_chunk_id}")
                        
                        # Create chunk tracking record in DynamoDB for callback mechanism
                        chunk_record = {
                            'job_id': {'S': job_id},
                            'chunk_id': {'S': unique_chunk_id},
                            'tenant_id': {'S': tenant_id},
                            'table_name': {'S': current_table},
                            'status': {'S': 'processing'},
                            'processing_mode': {'S': 'async_chunk'},
                            'created_at': {'S': get_timestamp()},
                            'updated_at': {'S': get_timestamp()}
                        }
                        
                        try:
                            self.dynamodb.put_item(
                                TableName=self.chunk_progress_table,
                                Item=chunk_record
                            )
                            self.logger.info(f"📊 ORCHESTRATOR DEBUG: Created chunk tracking record for {unique_chunk_id}")
                        except Exception as db_error:
                            self.logger.error(f"Failed to create chunk tracking record: {str(db_error)}")
                            # Continue processing even if tracking fails
                        
                        # For async processing, we'll assume successful invocation
                        # The chunk processor will trigger result aggregator directly when complete
                        processed_tenants += 1
                        response_payload = {
                            'status': 'processing_async',
                            'records_processed': 0,  # Will be updated by chunk processor
                            'chunk_id': unique_chunk_id
                        }
                        
                        self.logger.info(f"🔧 ORCHESTRATOR DEBUG: Complete response payload: {response_payload}")
                        
                        # For async processing, validate invocation success (not final processing results)
                        if response.get('StatusCode') == 202:
                            self.logger.info(f"✅ ORCHESTRATOR DEBUG: Async invocation successful for tenant {tenant_id} table {current_table}")
                            
                            # Create job tracking record for async invocation
                            job_record = {
                                'job_id': {'S': job_id},
                                'tenant_id': {'S': tenant_id},
                                'status': {'S': 'processing'},  # Will be updated by chunk processor
                                'table_name': {'S': current_table},
                                'records_processed': {'N': '0'},  # Will be updated by chunk processor
                                'processing_mode': {'S': 'async_backfill'},
                                'chunk_id': {'S': unique_chunk_id},
                                'created_at': {'S': get_timestamp()},
                                'updated_at': {'S': get_timestamp()}
                            }
                        else:
                            # Handle invocation failure (not processing failure)
                            error_msg = f"Chunk processor invocation failed - Status: {response.get('StatusCode')}, Error: {response.get('FunctionError', 'Unknown')}"
                            self.logger.error(f"❌ INVOCATION FAILED: {error_msg} for tenant {tenant_id} table {current_table}")
                            
                            job_record = {
                                'job_id': {'S': job_id},
                                'tenant_id': {'S': tenant_id},
                                'status': {'S': 'invocation_failed'},
                                'table_name': {'S': current_table},
                                'records_processed': {'N': '0'},
                                'processing_mode': {'S': 'async_backfill'},
                                'error_message': {'S': error_msg},
                                'created_at': {'S': get_timestamp()},
                                'updated_at': {'S': get_timestamp()},
                                'failed_at': {'S': get_timestamp()}
                            }
                        
                        # Store job record in DynamoDB
                        self.dynamodb.put_item(
                            TableName=self.processing_jobs_table,
                            Item=job_record
                        )
                        
                        self.logger.info(f"Created job record for tenant {tenant_id} table {current_table}")
                        
                    except Exception as e:
                        self.logger.error(f"Failed to process tenant {tenant_id} table {current_table}: {str(e)}")
                        
                        # Create failed job record
                        job_record = {
                            'job_id': {'S': job_id},
                            'tenant_id': {'S': tenant_id},
                            'status': {'S': 'failed'},
                            'table_name': {'S': current_table},
                            'records_processed': {'N': '0'},
                            'processing_mode': {'S': 'backfill'},
                            'error_message': {'S': str(e)},
                            'created_at': {'S': get_timestamp()},
                            'updated_at': {'S': get_timestamp()},
                            'failed_at': {'S': get_timestamp()}
                        }
                        
                        self.dynamodb.put_item(
                            TableName=self.processing_jobs_table,
                            Item=job_record
                        )
            
            self.logger.info(f"🏁 ORCHESTRATOR DEBUG: Workflow execution completed - returning result",
                           job_id=job_id,
                           tenants_processed=processed_tenants,
                           tables_processed=len(canonical_tables),
                           total_chunks_created=chunk_counter,
                           total_records_processed=total_records_processed)
            
            return {
                "status": "completed",
                "job_id": job_id,
                "tenants_processed": processed_tenants,
                "tables_processed": len(canonical_tables),
                "chunks_created": chunk_counter,
                "total_records_processed": total_records_processed,
                "canonical_tables": canonical_tables,
                "message": f"Pipeline workflow executed successfully - processed {len(canonical_tables)} tables across {processed_tenants} tenants with {chunk_counter} chunks created"
            }
            
        except Exception as e:
            self.logger.error(f"Pipeline workflow execution failed: {str(e)}")
            raise

    def _get_service_config_for_table(self, tenant: Dict[str, Any], table_name: str) -> Optional[Dict[str, Any]]:
        """Get service configuration including credentials for a specific table."""
        try:
            tenant_id = tenant['tenant_id']
            
            # Get all services for this tenant from DynamoDB
            response = self.dynamodb.query(
                TableName=self.config.tenant_services_table,
                KeyConditionExpression='tenant_id = :tenant_id',
                ExpressionAttributeValues={':tenant_id': {'S': tenant_id}}
            )
            
            if not response.get('Items'):
                self.logger.warning(f"No services found for tenant {tenant_id}")
                return None
            
            # Look through the services to find one that can handle this table
            for item in response['Items']:
                service_name = item.get('service', {}).get('S', '')
                secret_name = item.get('secret_name', {}).get('S', '')
                enabled = item.get('enabled', {}).get('BOOL', False)
                
                if not enabled:
                    continue
                    
                self.logger.info(f"Found service {service_name} for tenant {tenant_id}")
                
                # For ConnectWise service, determine the appropriate endpoint
                if 'connectwise' in service_name.lower():
                    # Get credentials using the secret_name from the record
                    credentials = self._get_service_credentials_by_secret(secret_name)
                    if credentials:
                        # Map table names to ConnectWise endpoints
                        endpoint_map = {
                            'companies': 'company/companies',
                            'contacts': 'company/contacts',
                            'tickets': 'service/tickets',
                            'time_entries': 'time/entries',
                            'agreements': 'finance/agreements',
                            'projects': 'project/projects'
                        }
                        
                        endpoint = endpoint_map.get(table_name)
                        if endpoint:
                            return {
                                'service_name': 'connectwise',
                                'endpoint': endpoint,
                                'credentials': credentials,
                                'page_size': 1000
                            }
                
                # For other services, add similar logic here as needed
                # ...
            
            self.logger.warning(f"No suitable service found for table {table_name} and tenant {tenant_id}")
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get service config for table {table_name}: {str(e)}")
            return None
    
    def _get_service_credentials(self, tenant_id: str, service_name: str) -> Optional[Dict[str, Any]]:
        """Get service credentials from AWS Secrets Manager."""
        try:
            import boto3
            secrets_client = boto3.client('secretsmanager')
            
            # Try different secret name patterns
            secret_patterns = [
                f"{tenant_id}-{service_name}-credentials",
                f"{tenant_id}/{service_name}",
                f"{service_name}-{tenant_id}",
                f"avesa/{tenant_id}/{service_name}"
            ]
            
            for secret_name in secret_patterns:
                try:
                    response = secrets_client.get_secret_value(SecretId=secret_name)
                    secret_data = json.loads(response['SecretString'])
                    self.logger.info(f"Found credentials for {tenant_id}-{service_name} in secret: {secret_name}")
                    return secret_data
                except ClientError as e:
                    if e.response['Error']['Code'] != 'ResourceNotFoundException':
                        self.logger.warning(f"Error accessing secret {secret_name}: {str(e)}")
                    continue
            
            self.logger.warning(f"No credentials found for {tenant_id}-{service_name} in any expected secret location")
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get credentials for {tenant_id}-{service_name}: {str(e)}")
            return None

    def _get_service_credentials_by_secret(self, secret_name: str) -> Optional[Dict[str, Any]]:
        """Get service credentials from AWS Secrets Manager using explicit secret name."""
        try:
            import boto3
            secrets_client = boto3.client('secretsmanager')
            
            self.logger.info(f"Attempting to retrieve credentials from secret: {secret_name}")
            
            try:
                response = secrets_client.get_secret_value(SecretId=secret_name)
                secret_data = json.loads(response['SecretString'])
                self.logger.info(f"Successfully retrieved credentials from secret: {secret_name}")
                
                # Extract the service-specific credentials from the nested structure
                # The secret contains {"connectwise": {...actual_credentials...}}
                # We need to return the inner credentials object directly
                if 'connectwise' in secret_data:
                    self.logger.info("Extracting ConnectWise credentials from nested structure")
                    return secret_data['connectwise']
                elif len(secret_data) == 1:
                    # If there's only one key, assume it's the service name and extract its value
                    service_key = list(secret_data.keys())[0]
                    self.logger.info(f"Extracting {service_key} credentials from nested structure")
                    return secret_data[service_key]
                else:
                    # Return as-is if it's already flat or unknown structure
                    self.logger.info("Using credentials as-is (flat structure)")
                    return secret_data
                    
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    self.logger.error(f"Secret not found: {secret_name}")
                else:
                    self.logger.error(f"Error accessing secret {secret_name}: {str(e)}")
                return None
            
        except Exception as e:
            self.logger.error(f"Failed to get credentials from secret {secret_name}: {str(e)}")
            return None

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