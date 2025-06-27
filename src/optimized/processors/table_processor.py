"""
Table Processor Lambda Function

Handles processing of a single table with intelligent chunking,
progress tracking, and resumable execution.
"""

import json
import math
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import boto3
from botocore.exceptions import ClientError

# Import shared modules from root shared directory
from shared.config_simple import Config
from shared.logger import PipelineLogger
from shared.aws_clients import get_dynamodb_client, get_cloudwatch_client
from shared.utils import get_timestamp


class TableProcessor:
    """Processes a single table with intelligent chunking and progress tracking."""
    
    def __init__(self):
        self.config = Config.from_environment()
        self.logger = PipelineLogger("table-processor")
        self.dynamodb = get_dynamodb_client()
        self.cloudwatch = get_cloudwatch_client()
        
        # Table names
        self.processing_jobs_table = f"ProcessingJobs-{self.config.environment}"
        self.chunk_progress_table = f"ChunkProgress-{self.config.environment}"
        self.last_updated_table = self.config.last_updated_table
    
    def lambda_handler(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """
        Main Lambda handler for table processing.
        
        Args:
            event: Event containing table configuration and job details
            context: Lambda context
            
        Returns:
            Table processing results
        """
        try:
            table_config = event['table_config']
            tenant_config = event['tenant_config']
            job_id = event['job_id']
            
            table_name = table_config['table_name']
            tenant_id = tenant_config['tenant_id']
            
            self.logger = PipelineLogger("table-processor", tenant_id=tenant_id, table_name=table_name)
            self.logger.info("Starting table processing", job_id=job_id)
            
            # Initialize table processing state
            table_state = self._initialize_table_processing(table_config, tenant_config, job_id)
            
            # Calculate intelligent chunks
            chunk_plan = self._calculate_chunks(table_config, tenant_config, table_state)
            
            if chunk_plan['total_chunks'] == 0:
                self.logger.info("No data to process for table")
                return {
                    'table_name': table_name,
                    'tenant_id': tenant_id,
                    'status': 'completed',
                    'message': 'No new data to process',
                    'records_processed': 0,
                    'chunks_processed': 0,
                    'chunk_plan': chunk_plan,  # Include chunk plan for Step Functions
                    'table_state': table_state  # Include table state for Step Functions
                }
            
            # Send table processing metrics
            self._send_table_metrics(job_id, tenant_id, table_name, 'started', chunk_plan['total_chunks'])
            
            # Return chunk plan and table state for Step Functions orchestration
            result = {
                'table_name': table_name,
                'tenant_id': tenant_id,
                'status': 'initialized',
                'chunk_plan': chunk_plan,  # Step Functions will use this
                'table_state': table_state,  # Step Functions will use this
                'processing_time': self._calculate_processing_time(context),
                'initialized_at': get_timestamp()
            }
            
            self.logger.info(
                "Table processing initialized",
                job_id=job_id,
                total_chunks=chunk_plan['total_chunks'],
                estimated_records=chunk_plan['estimated_total_records']
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Table processing failed: {str(e)}", error=str(e))
            raise
    
    def _initialize_table_processing(
        self, 
        table_config: Dict[str, Any], 
        tenant_config: Dict[str, Any],
        job_id: str
    ) -> Dict[str, Any]:
        """Initialize table processing state."""
        try:
            table_name = table_config['table_name']
            tenant_id = tenant_config['tenant_id']
            
            # Get last updated timestamp for incremental processing
            last_updated = self._get_last_updated_timestamp(tenant_id, table_name)
            
            # Determine if this is a full sync or incremental
            backfill_mode = table_config.get('backfill_mode', False)
            is_full_sync = backfill_mode or not last_updated
            
            # Estimate total records (this would typically call the API to get count)
            estimated_total_records = self._estimate_total_records(table_config, is_full_sync)
            
            table_state = {
                'table_name': table_name,
                'tenant_id': tenant_id,
                'job_id': job_id,
                'last_updated': last_updated,
                'is_full_sync': is_full_sync,
                'estimated_total_records': estimated_total_records,
                'processing_started_at': get_timestamp(),
                'status': 'initialized'
            }
            
            self.logger.info(
                "Table processing initialized",
                is_full_sync=is_full_sync,
                estimated_records=estimated_total_records,
                last_updated=last_updated
            )
            
            return table_state
            
        except Exception as e:
            self.logger.error(f"Failed to initialize table processing: {str(e)}")
            raise
    
    def _get_last_updated_timestamp(self, tenant_id: str, table_name: str) -> Optional[str]:
        """Get last updated timestamp for incremental processing."""
        try:
            response = self.dynamodb.get_item(
                TableName=self.last_updated_table,
                Key={
                    'tenant_id': {'S': tenant_id},
                    'table_name': {'S': table_name}
                }
            )
            
            if 'Item' in response:
                return response['Item'].get('last_updated', {}).get('S')
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Failed to get last updated timestamp: {str(e)}")
            return None
    
    def _calculate_chunks(
        self,
        table_config: Dict[str, Any],
        tenant_config: Dict[str, Any],
        table_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate intelligent chunks for processing."""
        try:
            table_name = table_config['table_name']
            estimated_records = table_state['estimated_total_records']
            
            # Store current table config for dynamic functions to access
            self._current_table_config = table_config
            
            if estimated_records == 0:
                return {
                    'chunks': [],
                    'total_chunks': 0,
                    'estimated_total_records': 0
                }
            
            # Calculate optimal chunk size using intelligent chunking strategy
            chunk_size = self._calculate_optimal_chunk_size(table_name, estimated_records)
            
            # Create chunk definitions
            chunks = []
            total_chunks = math.ceil(estimated_records / chunk_size)
            
            for i in range(total_chunks):
                start_offset = i * chunk_size
                end_offset = min((i + 1) * chunk_size, estimated_records)
                estimated_chunk_records = end_offset - start_offset
                
                chunk_id = f"{table_name}_{table_state['tenant_id']}_{start_offset}_{end_offset}"
                
                chunk_config = {
                    'chunk_id': chunk_id,
                    'chunk_index': i,
                    'start_offset': start_offset,
                    'end_offset': end_offset,
                    'estimated_records': estimated_chunk_records,
                    'table_name': table_name,
                    'tenant_id': table_state['tenant_id'],
                    'job_id': table_state['job_id'],
                    'priority': self._calculate_chunk_priority(table_name, i),
                    'created_at': get_timestamp()
                }
                
                chunks.append(chunk_config)
            
            chunk_plan = {
                'chunks': chunks,
                'total_chunks': total_chunks,
                'estimated_total_records': estimated_records,
                'chunk_size': chunk_size,
                'chunking_strategy': 'intelligent',
                'created_at': get_timestamp()
            }
            
            self.logger.info(
                "Chunk plan calculated",
                total_chunks=total_chunks,
                chunk_size=chunk_size,
                estimated_records=estimated_records
            )
            
            return chunk_plan
            
        except Exception as e:
            self.logger.error(f"Failed to calculate chunks: {str(e)}")
            raise
    
    def _calculate_optimal_chunk_size(self, table_name: str, estimated_records: int) -> int:
        """Calculate optimal chunk size based on table characteristics."""
        # Base chunk size configuration
        base_chunk_size = 5000
        max_chunk_size = 15000
        min_chunk_size = 1000
        
        # Dynamic complexity factors based on canonical table types
        try:
            from shared.utils import get_canonical_table_for_endpoint
            
            # Try to get canonical table for better complexity estimation
            canonical_table = None
            if hasattr(self, '_current_table_config'):
                service_name = self._current_table_config.get('service_name', 'unknown')
                endpoint = self._current_table_config.get('endpoint', table_name)
                canonical_table = get_canonical_table_for_endpoint(service_name, endpoint)
            
            # Canonical table complexity factors (lower = more complex, smaller chunks)
            canonical_complexity_factors = {
                'tickets': 0.7,      # Complex nested data
                'time_entries': 1.3, # Simpler structure
                'companies': 1.0,    # Medium complexity
                'contacts': 1.2,     # Simple structure
                'products': 1.1,     # Simple structure
                'agreements': 0.8,   # Medium complexity
                'projects': 0.9,     # Medium complexity
                'members': 1.4       # Simple structure
            }
            
            # Get complexity factor for this canonical table
            complexity_factor = canonical_complexity_factors.get(canonical_table, 1.0)
            
        except Exception as e:
            self.logger.warning(f"Failed to get canonical complexity factor: {e}")
            complexity_factor = 1.0
        
        # Calculate base size adjusted for complexity
        adjusted_base_size = int(base_chunk_size * complexity_factor)
        
        # Adjust based on total dataset size
        if estimated_records < 5000:
            # Small dataset - use smaller chunks
            optimal_size = min(adjusted_base_size, estimated_records)
        elif estimated_records > 100000:
            # Large dataset - use larger chunks for efficiency
            optimal_size = min(max_chunk_size, adjusted_base_size * 1.5)
        else:
            # Medium dataset - use base size
            optimal_size = adjusted_base_size
        
        # Apply bounds
        optimal_size = max(min_chunk_size, min(max_chunk_size, int(optimal_size)))
        
        self.logger.debug(
            "Calculated optimal chunk size",
            table_name=table_name,
            estimated_records=estimated_records,
            complexity_factor=complexity_factor,
            optimal_size=optimal_size
        )
        
        return optimal_size
    
    def _calculate_chunk_priority(self, table_name: str, chunk_index: int) -> int:
        """Calculate processing priority for chunk (1=highest, 10=lowest)."""
        # Dynamic priority based on canonical table types
        try:
            from shared.utils import get_canonical_table_for_endpoint
            
            # Try to get canonical table for better priority estimation
            canonical_table = None
            if hasattr(self, '_current_table_config'):
                service_name = self._current_table_config.get('service_name', 'unknown')
                endpoint = self._current_table_config.get('endpoint', table_name)
                canonical_table = get_canonical_table_for_endpoint(service_name, endpoint)
            
            # Canonical table priorities (1=highest, 10=lowest)
            canonical_priorities = {
                'companies': 1,      # Highest priority (dependencies)
                'tickets': 2,        # High priority
                'contacts': 3,       # Medium-high priority
                'time_entries': 4,   # Medium priority
                'products': 5,       # Medium priority
                'agreements': 6,     # Lower priority
                'projects': 7,       # Lower priority
                'members': 8         # Lowest priority
            }
            
            base_priority = canonical_priorities.get(canonical_table, 5)
            
        except Exception as e:
            self.logger.warning(f"Failed to get canonical priority: {e}")
            base_priority = 5
        
        # First chunks get higher priority
        if chunk_index == 0:
            return max(1, base_priority - 1)
        elif chunk_index < 3:
            return base_priority
        else:
            return min(10, base_priority + 1)
    
    
    
    
    def _update_last_updated_timestamp(self, tenant_id: str, table_name: str):
        """Update last updated timestamp for incremental processing."""
        try:
            current_timestamp = get_timestamp()
            
            self.dynamodb.put_item(
                TableName=self.last_updated_table,
                Item={
                    'tenant_id': {'S': tenant_id},
                    'table_name': {'S': table_name},
                    'last_updated': {'S': current_timestamp},
                    'updated_at': {'S': current_timestamp}
                }
            )
            
            self.logger.info(f"Updated last updated timestamp for {tenant_id}/{table_name}")
            
        except Exception as e:
            self.logger.warning(f"Failed to update last updated timestamp: {str(e)}")
    
    def _calculate_processing_time(self, context) -> float:
        """Calculate processing time from Lambda context."""
        try:
            remaining_time = context.get_remaining_time_in_millis()
            total_time = 300000  # 5 minutes default timeout
            used_time = total_time - remaining_time
            return used_time / 1000.0  # Convert to seconds
        except:
            return 0.0

    def _send_table_metrics(
        self,
        job_id: str,
        tenant_id: str,
        table_name: str,
        event_type: str,
        chunk_count: int
    ):
        """Send CloudWatch metrics for table processing."""
        try:
            metrics = [
                {
                    'MetricName': f'TableProcessing{event_type.title()}',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id},
                        {'Name': 'TenantId', 'Value': tenant_id},
                        {'Name': 'TableName', 'Value': table_name}
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
            
            if chunk_count > 0:
                metrics.append({
                    'MetricName': 'TableChunkCount',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id},
                        {'Name': 'TenantId', 'Value': tenant_id},
                        {'Name': 'TableName', 'Value': table_name}
                    ],
                    'Value': chunk_count,
                    'Unit': 'Count'
                })
            
            self.cloudwatch.put_metric_data(
                Namespace='AVESA/DataPipeline',
                MetricData=metrics
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to send table metrics: {str(e)}")


def lambda_handler(event, context):
    """Lambda entry point."""
    processor = TableProcessor()
    return processor.lambda_handler(event, context)