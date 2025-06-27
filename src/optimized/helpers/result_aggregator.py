"""
Result Aggregator Lambda Function

Aggregates results from tenant processing and updates job status.
Calculates final metrics and prepares completion data.
"""

import json
import boto3
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# Import shared modules from root shared directory
from shared.config_simple import Config
from shared.logger import PipelineLogger
from shared.aws_clients import get_dynamodb_client, get_cloudwatch_client
from shared.canonical_mapper import CanonicalMapper
from shared.utils import get_timestamp


class ResultAggregator:
    """Aggregates results from pipeline execution."""
    
    def __init__(self):
        self.config = Config.from_environment()
        self.logger = PipelineLogger("result-aggregator")
        self.dynamodb = get_dynamodb_client()
        self.cloudwatch = get_cloudwatch_client()
        
        # Initialize Lambda client for canonical transformation triggering
        self.lambda_client = boto3.client('lambda', region_name='us-east-2')
        
        # DynamoDB table names
        self.processing_jobs_table = f"ProcessingJobs-{self.config.environment}"
        self.chunk_progress_table = f"ChunkProgress-{self.config.environment}"
    
    def aggregate_results(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """
        Aggregate results from tenant processing or chunk completion callbacks.
        
        Args:
            event: Aggregation event with results
            context: Lambda context
            
        Returns:
            Aggregated results
        """
        try:
            self.logger.info("Aggregating pipeline results", event=event)
            
            job_id = event.get('job_id')
            
            # Handle chunk completion callback from chunk processor
            if 'chunk_results' in event:
                self.logger.info("Processing chunk completion callback", job_id=job_id)
                results = self._process_chunk_completion(event)
            else:
                # Handle normal tenant result aggregation
                processing_mode = event.get('processing_mode', 'unknown')
                
                if processing_mode == 'multi-tenant':
                    results = self._aggregate_multi_tenant_results(event)
                else:
                    results = self._aggregate_single_tenant_results(event)
                
                # Update job status only for full aggregation, not chunk callbacks
                if job_id:
                    self._update_job_completion(job_id, results)
            
            # Trigger canonical transforms for completed chunks
            if job_id and (results.get('successful_chunks', 0) > 0 or results.get('successful_tenants', 0) > 0):
                self._trigger_table_canonical_transforms(job_id, results)
            
            # Send completion metrics only for full aggregation
            if 'chunk_results' not in event:
                self._send_completion_metrics(job_id, results)
            
            self.logger.info(
                "Pipeline results aggregated successfully",
                job_id=job_id,
                processing_mode=results.get('processing_mode', 'chunk_callback'),
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
        
        # Extract result from Step Functions execution - try both Payload and Output
        output = tenant_result.get('Payload', tenant_result.get('Output', {}))
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

    def _trigger_table_canonical_transforms(self, job_id: str, results: Dict[str, Any]):
        """
        Trigger canonical transforms for all tables that had chunks processed.
        
        Args:
            job_id: Processing job identifier
            results: Aggregated results containing chunk data
        """
        try:
            self.logger.info("Starting canonical transform coordination", job_id=job_id)
            
            # Handle chunk callback results differently from full aggregation
            if results.get('processing_mode') == 'chunk_callback':
                # Use S3 files directly from chunk completion callback
                s3_files_by_table = results.get('s3_files_by_table', {})
                self.logger.info(f"Using S3 files from chunk callback: {len(s3_files_by_table)} tables")
            else:
                # Collect S3 files by table from chunk progress data for full aggregation
                s3_files_by_table = self._collect_s3_files_by_table(job_id)
            
            if not s3_files_by_table:
                self.logger.info("No S3 files found for canonical transformation", job_id=job_id)
                return
            
            # Trigger canonical transform for each table
            for table_key, s3_files in s3_files_by_table.items():
                if s3_files:  # Only trigger if files were written
                    # Parse table key back to components
                    tenant_id, service_name, table_name = table_key.split(':', 2)
                    
                    success = self._trigger_single_canonical_transform(
                        tenant_id, service_name, table_name, s3_files
                    )
                    
                    if success:
                        self.logger.info(
                            f"Successfully triggered canonical transform for {table_name}",
                            tenant_id=tenant_id,
                            service_name=service_name,
                            table_name=table_name,
                            files_count=len(s3_files)
                        )
                    else:
                        self.logger.warning(
                            f"Failed to trigger canonical transform for {table_name}",
                            tenant_id=tenant_id,
                            service_name=service_name,
                            table_name=table_name
                        )
                        
        except Exception as e:
            self.logger.error(f"Failed to trigger canonical transforms: {str(e)}", job_id=job_id)

    def _collect_s3_files_by_table(self, job_id: str) -> Dict[tuple, List[str]]:
        """
        Collect S3 files from chunk progress data, grouped by table.
        
        Args:
            job_id: Processing job identifier
            
        Returns:
            Dictionary mapping (tenant_id, service_name, table_name) to list of S3 files
        """
        try:
            table_files = {}
            
            # Query chunk progress table for all chunks of this job
            response = self.dynamodb.query(
                TableName=self.chunk_progress_table,
                KeyConditionExpression='job_id = :job_id',
                ExpressionAttributeValues={':job_id': {'S': job_id}}
            )
            
            for item in response.get('Items', []):
                # Extract chunk metadata
                chunk_id = item.get('chunk_id', {}).get('S', '')
                tenant_id = item.get('tenant_id', {}).get('S', '')
                table_name = item.get('table_name', {}).get('S', '')
                status = item.get('status', {}).get('S', '')
                
                # Only process completed chunks
                if status != 'completed':
                    continue
                
                # Get S3 files from chunk results (stored in DynamoDB or extract from chunk_id)
                s3_files = self._extract_s3_files_from_chunk(item, job_id)
                
                if s3_files:
                    # Derive service_name from chunk data or use default mapping
                    service_name = self._derive_service_name_from_chunk(item, table_name)
                    
                    table_key = (tenant_id, service_name, table_name)
                    if table_key not in table_files:
                        table_files[table_key] = []
                    
                    table_files[table_key].extend(s3_files)
            
            self.logger.info(
                f"Collected S3 files for {len(table_files)} tables",
                job_id=job_id,
                tables=list(table_files.keys())
            )
            
            return table_files
            
        except Exception as e:
            self.logger.error(f"Failed to collect S3 files by table: {str(e)}", job_id=job_id)
            return {}

    def _extract_s3_files_from_chunk(self, chunk_item: Dict[str, Any], job_id: str) -> List[str]:
        """
        Extract S3 file paths from chunk data.
        
        Args:
            chunk_item: DynamoDB chunk item
            job_id: Processing job identifier
            
        Returns:
            List of S3 file keys
        """
        try:
            # S3 files might be stored in chunk progress or derived from chunk_id
            chunk_id = chunk_item.get('chunk_id', {}).get('S', '')
            tenant_id = chunk_item.get('tenant_id', {}).get('S', '')
            table_name = chunk_item.get('table_name', {}).get('S', '')
            
            if not chunk_id or not tenant_id or not table_name:
                return []
            
            # Derive service name from table mapping
            service_name = self._derive_service_name_from_chunk(chunk_item, table_name)
            
            # For now, we'll derive S3 files from the chunk_id pattern
            # In practice, this should be stored in the chunk progress record
            s3_files = []
            
            # Construct expected S3 file pattern based on chunk processor logic
            # Pattern: raw/{tenant_id}/{service_name}/{table_name}/YYYY/MM/DD/{chunk_id}_batch{N}.parquet
            from datetime import datetime
            today = datetime.now()
            date_path = f"{today.year:04d}/{today.month:02d}/{today.day:02d}"
            
            # For simplicity, assume 1-3 batch files per chunk (this should be tracked better)
            for batch_num in range(1, 4):  # Most chunks will have 1-3 batches
                s3_key = f"raw/{tenant_id}/{service_name}/{table_name}/{date_path}/{chunk_id}_batch{batch_num:03d}.parquet"
                s3_files.append(s3_key)
            
            return s3_files
            
        except Exception as e:
            self.logger.warning(f"Failed to extract S3 files from chunk: {str(e)}")
            return []

    def _derive_service_name_from_chunk(self, chunk_item: Dict[str, Any], table_name: str) -> str:
        """
        Derive service name from chunk data or table name.
        
        Args:
            chunk_item: DynamoDB chunk item
            table_name: Table name
            
        Returns:
            Service name
        """
        # This is a simplified derivation - in practice should be stored in chunk metadata
        # or derived from a mapping table
        
        # Common table to service mappings
        table_service_mapping = {
            'tickets': 'connectwise',
            'time_entries': 'connectwise',
            'companies': 'connectwise',
            'contacts': 'connectwise',
            'products': 'connectwise',
            'agreements': 'connectwise',
            'projects': 'connectwise',
            'members': 'connectwise'
        }
        
        return table_service_mapping.get(table_name, 'connectwise')  # Default to connectwise

    def _trigger_single_canonical_transform(
        self,
        tenant_id: str,
        service_name: str,
        table_name: str,
        s3_files: List[str]
    ) -> bool:
        """
        Trigger canonical transformation for a single table with specific files.
        
        Args:
            tenant_id: Tenant identifier
            service_name: Service name (e.g., 'connectwise')
            table_name: Table name (e.g., 'tickets')
            s3_files: List of S3 file keys to process
            
        Returns:
            bool: True if trigger was successful, False otherwise
        """
        try:
            # Use table-specific canonical transform lambda naming
            clean_table_name = table_name.replace('_', '-')
            canonical_lambda_name = f"avesa-canonical-transform-{clean_table_name}-{self.config.environment}"
            
            # Prepare canonical transformation payload with specific files
            payload = {
                'tenant_id': tenant_id,
                'service_name': service_name,
                'table_name': table_name,
                'backfill_mode': False,
                'force_reprocess': False,
                's3_trigger': True,
                'source_files': s3_files,  # NEW: Specific files to process
                'aggregator_triggered': True  # Flag to indicate this was triggered by result aggregator
            }
            
            self.logger.info(
                f"Triggering canonical transformation for {table_name}",
                canonical_lambda_name=canonical_lambda_name,
                tenant_id=tenant_id,
                service_name=service_name,
                files_count=len(s3_files)
            )
            
            # Invoke canonical transformation Lambda asynchronously
            response = self.lambda_client.invoke(
                FunctionName=canonical_lambda_name,
                InvocationType='Event',  # Async invocation
                Payload=json.dumps(payload)
            )
            
            # Check if invocation was successful
            if response.get('StatusCode') == 202:
                self.logger.info(
                    f"Successfully triggered canonical transformation for {table_name}",
                    canonical_lambda_name=canonical_lambda_name,
                    response_status=response.get('StatusCode'),
                    files_count=len(s3_files)
                )
                return True
            else:
                self.logger.warning(
                    f"Unexpected response status from canonical transformation trigger",
                    canonical_lambda_name=canonical_lambda_name,
                    response_status=response.get('StatusCode'),
                    response=response
                )
                return False
                
        except Exception as e:
            self.logger.error(
                f"Error triggering canonical transformation for {table_name}: {str(e)}",
                tenant_id=tenant_id,
                service_name=service_name,
                table_name=table_name,
                error=str(e)
            )
            return False
    
    def _process_chunk_completion(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process chunk completion callback from chunk processor.
        
        Args:
            event: Event containing chunk_results
            
        Returns:
            Processed results
        """
        chunk_results = event.get('chunk_results', [])
        job_id = event.get('job_id')
        
        self.logger.info(f"Processing {len(chunk_results)} chunk completion(s)", job_id=job_id)
        
        # For chunk callbacks, we don't aggregate all results, just process the specific chunks
        total_records = 0
        successful_chunks = 0
        s3_files_by_table = {}
        
        for chunk_result in chunk_results:
            chunk_id = chunk_result.get('chunk_id')
            status = chunk_result.get('status')
            records_processed = chunk_result.get('records_processed', 0)
            s3_files = chunk_result.get('s3_files_written', [])
            table_metadata = chunk_result.get('table_metadata', {})
            
            total_records += records_processed
            
            if status == 'completed' and s3_files:
                successful_chunks += 1
                
                # Collect S3 files by table for canonical transformation
                tenant_id = table_metadata.get('tenant_id', 'unknown')
                service_name = table_metadata.get('service_name', 'unknown')
                table_name = table_metadata.get('table_name', 'unknown')
                
                table_key = f"{tenant_id}:{service_name}:{table_name}"
                if table_key not in s3_files_by_table:
                    s3_files_by_table[table_key] = []
                s3_files_by_table[table_key].extend(s3_files)
                
                self.logger.info(
                    f"Chunk {chunk_id} completed successfully",
                    chunk_id=chunk_id,
                    records_processed=records_processed,
                    s3_files_count=len(s3_files),
                    table_name=table_name
                )
        
        # Return results suitable for canonical transformation triggering
        return {
            'processing_mode': 'chunk_callback',
            'job_id': job_id,
            'successful_chunks': successful_chunks,
            'total_records_processed': total_records,
            's3_files_by_table': s3_files_by_table,
            'aggregated_at': get_timestamp()
        }


def lambda_handler(event, context):
    """Lambda entry point."""
    aggregator = ResultAggregator()
    return aggregator.aggregate_results(event, context)