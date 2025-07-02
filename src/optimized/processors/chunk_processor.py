"""
Chunk Processor Lambda Function

Handles processing of individual data chunks with optimized API calls,
timeout handling, and progress tracking.

Updated: 2025-07-01 20:36 - Fixed batch_records initialization issue
"""

import json
import time
import gc
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import boto3
from botocore.exceptions import ClientError

# Import shared modules from root shared directory
from shared.config_simple import Config
from shared.logger import PipelineLogger
from shared.aws_clients import get_dynamodb_client, get_cloudwatch_client, get_s3_client
from shared.utils import get_timestamp, get_s3_key

# Define ServiceCredentials class for API authentication
import base64

class ServiceCredentials:
    """Generic service credentials class that can handle any service type."""
    def __init__(self, **kwargs):
        # Store all credential fields dynamically
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)
    
    def get_auth_header(self):
        """Generate authentication header based on service type."""
        # ConnectWise-style authentication
        if hasattr(self, 'company_id') and hasattr(self, 'public_key') and hasattr(self, 'private_key'):
            auth_string = f"{self.company_id}+{self.public_key}:{self.private_key}"
            encoded_auth = base64.b64encode(auth_string.encode()).decode()
            return f"Basic {encoded_auth}"
        
        # Salesforce-style authentication (Bearer token)
        elif hasattr(self, 'access_token'):
            return f"Bearer {self.access_token}"
        
        # ServiceNow-style authentication (Basic auth with username/password)
        elif hasattr(self, 'username') and hasattr(self, 'password'):
            auth_string = f"{self.username}:{self.password}"
            encoded_auth = base64.b64encode(auth_string.encode()).decode()
            return f"Basic {encoded_auth}"
        
        # API Key authentication
        elif hasattr(self, 'api_key'):
            return f"ApiKey {self.api_key}"
        
        # Default to no authentication
        else:
            return ""


class TimeoutHandler:
    """Handles Lambda timeout detection and graceful continuation."""
    
    def __init__(self, context, buffer_seconds: int = 15):
        self.context = context
        self.buffer_seconds = buffer_seconds
        self.start_time = time.time()
        
        # Calculate when we should stop processing
        remaining_time_ms = context.get_remaining_time_in_millis()
        self.timeout_threshold = self.start_time + (remaining_time_ms / 1000) - buffer_seconds
    
    def should_continue(self) -> bool:
        """Check if we should continue processing or prepare for timeout."""
        return time.time() < self.timeout_threshold
    
    def get_remaining_time(self) -> float:
        """Get remaining processing time in seconds."""
        return max(0, self.timeout_threshold - time.time())


class ChunkProcessor:
    """Processes individual data chunks with optimized API calls."""
    
    def _check_memory_usage(self, max_memory_mb: int = 400) -> bool:
        """
        Check current memory usage and trigger cleanup if needed.
        
        Args:
            max_memory_mb: Maximum memory usage in MB before triggering cleanup (default: 450MB = ~44% of 1024MB Lambda limit)
            
        Returns:
            True if memory cleanup was triggered, False otherwise
        """
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_percent = (memory_mb / 1024) * 100  # Percentage of Lambda's 1024MB limit
            
            self.logger.info(f"Memory usage: {memory_mb:.1f}MB ({memory_percent:.1f}% of Lambda limit)")
            
            if memory_mb > max_memory_mb:
                self.logger.warning(f"HIGH MEMORY USAGE: {memory_mb:.1f}MB ({memory_percent:.1f}%), triggering aggressive cleanup")
                
                # Force aggressive garbage collection
                gc.collect()
                gc.collect()  # Double collection for better cleanup
                gc.collect()  # Triple collection for maximum cleanup
                
                # Check memory after cleanup
                memory_after = process.memory_info().rss / 1024 / 1024
                memory_freed = memory_mb - memory_after
                self.logger.info(f"Memory cleanup freed {memory_freed:.1f}MB, new usage: {memory_after:.1f}MB")
                
                return True
            return False
        except ImportError:
            # psutil not available, skip memory monitoring
            self.logger.warning("psutil not available - memory monitoring disabled")
            return False
        except Exception as e:
            self.logger.warning(f"Failed to check memory usage: {e}")
            return False
    
    def __init__(self):
        self.config = Config.from_environment()
        self.logger = PipelineLogger("chunk-processor")
        self.dynamodb = get_dynamodb_client()
        self.cloudwatch = get_cloudwatch_client()
        self.s3_client = get_s3_client()
        
        # Note: Lambda client for canonical transformation removed - now handled by result aggregator
        
        # Table names
        self.chunk_progress_table = f"ChunkProgress-{self.config.environment}"
        self.last_updated_table = self.config.last_updated_table
    
    def lambda_handler(self, event: Dict[str, Any], context) -> Dict[str, Any]:
        """
        Main Lambda handler for chunk processing.
        
        Args:
            event: Event containing chunk configuration
            context: Lambda context
            
        Returns:
            Chunk processing results
        """
        try:
            self.logger.info(f"Chunk processor started - AWS Request ID: {context.aws_request_id}")
            
            chunk_config = event['chunk_config']
            table_config = event['table_config']
            tenant_config = event['tenant_config']
            job_id = event['job_id']
            
            chunk_id = chunk_config['chunk_id']
            table_name = table_config['table_name']
            tenant_id = tenant_config['tenant_id']
            
            self.logger = PipelineLogger("chunk-processor", tenant_id=tenant_id, table_name=table_name)
            self.logger.info(f"Starting chunk processing",
                           job_id=job_id,
                           chunk_id=chunk_id)
            
            # Initialize timeout handler
            timeout_handler = TimeoutHandler(context, buffer_seconds=15)
            
            # Initialize chunk progress tracking
            self._initialize_chunk_progress(chunk_config, job_id)
            
            # Process the chunk
            processing_result = self._process_chunk(
                chunk_config, 
                table_config, 
                tenant_config, 
                timeout_handler
            )
            
            # Update progress
            self._update_chunk_progress(
                chunk_id, 
                job_id, 
                'completed' if processing_result['completed'] else 'timeout_continuation',
                processing_result
            )
            
            # Send metrics
            self._send_chunk_metrics(job_id, tenant_id, table_name, chunk_id, processing_result)
            
            result = {
                'chunk_id': chunk_id,
                'status': 'completed' if processing_result['completed'] else 'timeout_continuation',
                'records_processed': processing_result['records_processed'],
                'processing_time': processing_result['processing_time'],
                'continuation_state': processing_result.get('continuation_state'),
                's3_files_written': processing_result.get('s3_files_written', []),
                's3_files_count': processing_result.get('s3_files_count', 0),
                'table_metadata': {
                    'tenant_id': tenant_id,
                    'service_name': table_config['service_name'],
                    'table_name': table_config['table_name']
                }
            }
            
            # COMPLETION CALLBACK: Trigger result aggregator when chunk processing completes successfully
            if (processing_result['completed'] and
                processing_result.get('s3_files_written') and
                len(processing_result.get('s3_files_written', [])) > 0):
                
                self.logger.info(f"🚀 COMPLETION CALLBACK: Triggering result aggregator for completed chunk {chunk_id}")
                
                try:
                    import boto3
                    lambda_client = boto3.client('lambda')
                    
                    # Get environment for result aggregator function name
                    environment = self.config.environment
                    result_aggregator_function = f"avesa-result-aggregator-{environment}"
                    
                    # Create aggregator payload with completed chunk information
                    aggregator_payload = {
                        "job_id": job_id,
                        "chunk_results": [
                            {
                                "chunk_id": chunk_id,
                                "tenant_id": tenant_id,
                                "table_name": table_name,
                                "status": "completed",
                                "records_processed": processing_result['records_processed'],
                                "s3_files_written": processing_result.get('s3_files_written', []),
                                "s3_files_count": processing_result.get('s3_files_count', 0),
                                "table_metadata": result['table_metadata'],
                                "processing_time_seconds": processing_result['processing_time']
                            }
                        ]
                    }
                    
                    # Invoke result aggregator asynchronously
                    aggregator_response = lambda_client.invoke(
                        FunctionName=result_aggregator_function,
                        InvocationType='Event',  # Async to avoid blocking
                        Payload=json.dumps(aggregator_payload)
                    )
                    
                    self.logger.info(f"✅ COMPLETION CALLBACK SUCCESS: Result aggregator triggered for chunk {chunk_id}",
                                   aggregator_status=aggregator_response.get('StatusCode'),
                                   job_id=job_id,
                                   tenant_id=tenant_id)
                                   
                except Exception as callback_error:
                    self.logger.error(f"❌ COMPLETION CALLBACK FAILED: Could not trigger result aggregator for chunk {chunk_id}: {str(callback_error)}")
                    # Don't fail the chunk processing due to callback failure
            
            # CRITICAL DEBUG: Log response before returning
            import sys
            result_size = sys.getsizeof(str(result))
            self.logger.info("🔍 CHUNK DEBUG: About to return response",
                           job_id=job_id,
                           chunk_id=chunk_id,
                           records_processed=processing_result['records_processed'],
                           completed=processing_result['completed'],
                           result_size_bytes=result_size,
                           response_keys=list(result.keys()))
            
            self.logger.info("🔍 CHUNK DEBUG: Full response payload", response=result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"🚨 CHUNK DEBUG: Chunk processing failed with exception",
                            error=str(e),
                            error_type=type(e).__name__,
                            chunk_id=locals().get('chunk_id', 'unknown'),
                            job_id=locals().get('job_id', 'unknown'))
            
            # Update chunk progress to failed
            if 'chunk_id' in locals() and 'job_id' in locals():
                self._update_chunk_progress(chunk_id, job_id, 'failed', {'error': str(e)})
            
            # Return error response instead of raising to see if this helps
            return {
                'chunk_id': locals().get('chunk_id', 'unknown'),
                'status': 'failed',
                'records_processed': 0,
                'processing_time': 0,
                'error': str(e),
                'errorMessage': str(e)  # Lambda standard error field
            }
    
    def _initialize_chunk_progress(self, chunk_config: Dict[str, Any], job_id: str):
        """Initialize chunk progress tracking."""
        try:
            chunk_id = chunk_config['chunk_id']
            
            progress_item = {
                'job_id': {'S': job_id},
                'chunk_id': {'S': chunk_id},
                'tenant_id': {'S': chunk_config.get('tenant_id', 'unknown')},
                'table_name': {'S': chunk_config.get('table_name', 'unknown')},
                'status': {'S': 'processing'},
                'records_processed': {'N': '0'},
                'created_at': {'S': get_timestamp()},
                'updated_at': {'S': get_timestamp()}
            }
            
            # Add optional numeric fields only if they are not None
            if chunk_config.get('start_offset') is not None:
                progress_item['start_offset'] = {'N': str(chunk_config['start_offset'])}
            else:
                progress_item['start_offset'] = {'N': '0'}
                
            if chunk_config.get('end_offset') is not None:
                progress_item['end_offset'] = {'N': str(chunk_config['end_offset'])}
            else:
                progress_item['end_offset'] = {'N': '0'}
                
            if chunk_config.get('estimated_records') is not None:
                progress_item['estimated_records'] = {'N': str(chunk_config['estimated_records'])}
            else:
                progress_item['estimated_records'] = {'N': '0'}
            
            self.dynamodb.put_item(
                TableName=self.chunk_progress_table,
                Item=progress_item
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to initialize chunk progress: {str(e)}")
    
    def _process_chunk(
        self,
        chunk_config: Dict[str, Any],
        table_config: Dict[str, Any],
        tenant_config: Dict[str, Any],
        timeout_handler: TimeoutHandler
    ) -> Dict[str, Any]:
        """Process a single chunk of data with memory-efficient streaming to S3."""
        start_time = time.time()
        records_processed = 0
        s3_files_written = []
        
        try:
            # Get credentials
            credentials = table_config.get('credentials', {})
            if not credentials:
                raise ValueError("No credentials provided for API access")
            
            service_credentials = ServiceCredentials.from_dict(credentials)
            service_name = table_config.get('service_name', 'unknown')
            
            # Get configured page size from endpoint configuration
            configured_page_size = table_config.get('page_size', 1000)
            
            # Memory optimization: Write to S3 every N batches to avoid memory accumulation
            write_batch_size = 25  # Reduced from 40 to 25 API batches for better memory management
            batch_buffer = []
            batch_count = 0
            file_batch_number = 0  # Cumulative file counter (never resets)
            
            # CRITICAL DEBUG: Log all record limit sources and disable enforcement for backfill
            explicit_record_limit = chunk_config.get('record_limit')
            estimated_records = chunk_config.get('estimated_records')
            
            # For backfill operations, we want to fetch ALL available data, not limit by estimates
            # Only use explicit record_limit if it's specifically set by orchestrator
            if explicit_record_limit:
                record_limit = explicit_record_limit
                self.logger.info(f"🎯 USING EXPLICIT RECORD LIMIT: {record_limit}")
            else:
                # For backfill, ignore estimated_records and fetch all data
                record_limit = None
                self.logger.info(f"🎯 BACKFILL MODE: No record limit enforced (ignoring estimated_records: {estimated_records})")
            
            # For backfill operations, we want to fetch ALL available data up to the limit
            current_page = 1
            current_offset = 0
            
            # Continue fetching until we get no more records or timeout
            while timeout_handler.should_continue():
                # RECORD LIMIT ENFORCEMENT: Stop if we've reached the limit
                if record_limit and records_processed >= record_limit:
                    self.logger.info("🛑 RECORD LIMIT REACHED: Stopping data fetching",
                                   records_processed=records_processed,
                                   record_limit=record_limit)
                    break
                
                # Adjust batch size to not exceed record limit
                effective_batch_size = configured_page_size
                if record_limit:
                    remaining_records = record_limit - records_processed
                    if remaining_records <= 0:
                        break
                    effective_batch_size = min(configured_page_size, remaining_records)
                
                # Initialize batch_records to empty list to prevent UnboundLocalError
                batch_records = []
                
                # Fetch batch of data using adjusted batch size
                try:
                    batch_records = self._fetch_data_batch(
                        table_config,
                        service_credentials,
                        current_offset,
                        effective_batch_size
                    )
                except Exception as fetch_error:
                    self.logger.error(f"Failed to fetch data batch: {str(fetch_error)}",
                                    error_type=type(fetch_error).__name__,
                                    offset=current_offset,
                                    batch_size=effective_batch_size)
                    # batch_records remains empty list, so the loop will break naturally
                
                # If no records returned, we've reached the end
                if not batch_records:
                    self.logger.info(
                        f"🛑 STOPPING: No more records returned from API",
                        current_page=current_page,
                        total_processed=records_processed,
                        api_url=f"{table_config.get('service_name', 'unknown')} {table_config.get('endpoint', 'unknown')}",
                        offset=current_offset,
                        requested_batch_size=effective_batch_size
                    )
                    break
                
                # MEMORY OPTIMIZATION: Pre-allocation check before extending buffer
                if batch_records:
                    # Check memory BEFORE adding to buffer
                    memory_cleanup_triggered = self._check_memory_usage(max_memory_mb=400)  # Reduced from 700MB to 400MB
                    
                    if memory_cleanup_triggered:
                        self.logger.info("Memory cleanup triggered before buffer extend - checking if emergency write needed")
                        # If we're near memory limit, write current buffer immediately
                        if batch_buffer and len(batch_buffer) > 1000:  # Emergency threshold
                            self.logger.warning(f"EMERGENCY WRITE: Writing {len(batch_buffer)} records due to memory pressure")
                            file_batch_number += 1
                            s3_key = self._write_batch_to_s3(
                                batch_buffer,
                                chunk_config,
                                table_config,
                                tenant_config,
                                file_batch_number
                            )
                            if s3_key:
                                s3_files_written.append(s3_key)
                            
                            # Clear buffer and reset
                            del batch_buffer[:]  # More aggressive than clear()
                            batch_count = 0
                            gc.collect()
                
                # Add records to current batch buffer
                batch_buffer.extend(batch_records)
                records_processed += len(batch_records)
                batch_count += 1
                
                # Store batch_records length before cleanup for pagination
                batch_records_len = len(batch_records)
                
                # Update offset for next iteration only if we have records
                if batch_records_len > 0:
                    if service_name.lower() == 'connectwise':
                        # ConnectWise uses page-based pagination
                        current_page += 1
                        current_offset = (current_page - 1) * configured_page_size
                    else:
                        # Other services may use offset-based pagination
                        current_offset += batch_records_len
                
                # Log progress with record limit status
                progress_info = {
                    "batch_size": batch_records_len,
                    "current_page": current_page,
                    "current_offset": current_offset,
                    "total_processed": records_processed,
                    "buffer_size": len(batch_buffer),
                    "service": service_name
                }
                
                if record_limit:
                    progress_info["record_limit"] = record_limit
                    progress_info["remaining"] = record_limit - records_processed
                    
                self.logger.info(f"Processed batch: {batch_records_len} records", **progress_info)
                
                # Aggressive cleanup of batch_records after all operations
                if 'batch_records' in locals() and batch_records:
                    del batch_records
                gc.collect()
                
                # CRITICAL FIX: Write to S3 when buffer reaches write_batch_size batches
                should_write = batch_count >= write_batch_size
                
                self.logger.info(f"🔍 BATCH WRITE CHECK: batch_count={batch_count}, write_batch_size={write_batch_size}, should_write={should_write}, buffer_size={len(batch_buffer)}")
                
                if should_write and batch_buffer:
                    self.logger.info(f"📝 WRITING BATCH TO S3: file_batch_number will be={file_batch_number + 1}, buffer_size={len(batch_buffer)}")
                    file_batch_number += 1  # Increment file counter
                    s3_key = self._write_batch_to_s3(
                        batch_buffer,
                        chunk_config,
                        table_config,
                        tenant_config,
                        file_batch_number
                    )
                    if s3_key:
                        s3_files_written.append(s3_key)
                    
                    self.logger.info(
                        f"Wrote batch buffer to S3: {len(batch_buffer)} records",
                        buffer_size=len(batch_buffer),
                        total_processed=records_processed,
                        s3_files_count=len(s3_files_written)
                    )
                    
                    # Aggressive cleanup to free memory
                    del batch_buffer[:]  # More aggressive than clear()
                    batch_count = 0
                    gc.collect()  # Force garbage collection after clearing
                    gc.collect()  # Double collect for aggressive cleanup
                    
                    # Check memory after cleanup
                    self._check_memory_usage(max_memory_mb=400)
                
                # Only stop if we get zero records - partial pages may still have more data
                # Note: batch_records_len is already captured above before cleanup
                if batch_records_len == 0:
                    self.logger.info(
                        f"No more records returned from API, reached end of data",
                        current_page=current_page,
                        total_processed=records_processed
                    )
                    break
                
                # Log when we get partial pages but continue processing
                if batch_records_len < configured_page_size:
                    self.logger.info(
                        f"Received partial page but continuing (API may have more data)",
                        received=batch_records_len,
                        page_size=configured_page_size,
                        current_page=current_page,
                        total_processed=records_processed
                    )
            
            # MEMORY OPTIMIZATION: Write any remaining records in buffer
            if batch_buffer:
                file_batch_number += 1  # Fix: Increment for final batch
                s3_key = self._write_batch_to_s3(
                    batch_buffer,
                    chunk_config,
                    table_config,
                    tenant_config,
                    file_batch_number
                )
                if s3_key:
                    s3_files_written.append(s3_key)
                
                self.logger.info(
                    f"Wrote final batch buffer to S3: {len(batch_buffer)} remaining records",
                    buffer_size=len(batch_buffer),
                    total_processed=records_processed,
                    s3_files_count=len(s3_files_written)
                )
            else:
                self.logger.info("No remaining records - all data written in intermediate batches")
            
            # Aggressive final cleanup
            del batch_buffer
            gc.collect()
            gc.collect()  # Double collection for better cleanup
            self.logger.info("Final memory cleanup completed")
            
            # Final memory check
            self._check_memory_usage(max_memory_mb=400)
            
            processing_time = time.time() - start_time
            
            # For backfill operations, we consider it completed when we've fetched all available data
            completed = True
            
            result = {
                'completed': completed,
                'records_processed': records_processed,
                'processing_time': processing_time,
                'final_page': current_page,
                'final_offset': current_offset,
                's3_files_written': s3_files_written,
                's3_files_count': len(s3_files_written)
            }
            
            self.logger.info(
                f"Memory-efficient chunk processing completed",
                records_processed=records_processed,
                processing_time=processing_time,
                final_page=current_page,
                s3_files_count=len(s3_files_written),
                service=service_name
            )
            
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            self.logger.error(f"Chunk processing error: {str(e)}")
            
            return {
                'completed': False,
                'records_processed': records_processed,
                'processing_time': processing_time,
                'error': str(e)
            }
    
    def _fetch_data_batch(
        self,
        table_config: Dict[str, Any],
        credentials: ServiceCredentials,
        offset: int,
        batch_size: int,
        current_page: int = None,
        total_processed: int = None
    ) -> List[Dict[str, Any]]:
        """Fetch a batch of data from the API with proper pagination handling."""
        import time
        
        start_time = time.time()
        
        try:
            endpoint = table_config['endpoint']
            service_name = table_config.get('service_name', 'unknown')
            
            # Get API base URL from credentials
            api_base_url = getattr(credentials, 'api_base_url', None) or getattr(credentials, 'instance_url', None) or getattr(credentials, 'base_url', None)
            
            if not api_base_url:
                raise ValueError(f"No API base URL found in credentials for service {service_name}")
            
            # Build API URL
            url = f"{api_base_url.rstrip('/')}/{endpoint.lstrip('/')}"
            
            # Use the provided batch_size (which may be adjusted for record limits)
            effective_page_size = batch_size
            
            # Build headers
            headers = {
                'Content-Type': 'application/json'
            }
            
            # Add authentication header
            auth_header = credentials.get_auth_header()
            if auth_header:
                headers['Authorization'] = auth_header
            
            # Add service-specific headers
            if hasattr(credentials, 'client_id') and credentials.client_id:
                headers['ClientId'] = credentials.client_id
            
            # Handle service-specific pagination
            if service_name.lower() == 'connectwise':
                # ConnectWise uses page-based pagination (1-indexed)
                page_number = (offset // effective_page_size) + 1
                params = {
                    'page': page_number,
                    'pageSize': effective_page_size,
                    'orderBy': table_config.get('order_by', 'id asc')
                }
                
                # Add incremental sync conditions if configured
                incremental_field = table_config.get('incremental_field')
                if incremental_field:
                    # This would be implemented based on last_updated timestamp
                    # For now, we'll fetch all records during backfill
                    pass
                    
            elif service_name.lower() == 'salesforce':
                # Salesforce uses offset-based pagination
                params = {
                    'limit': effective_page_size,
                    'offset': offset
                }
            elif service_name.lower() == 'servicenow':
                # ServiceNow uses offset-based pagination
                params = {
                    'sysparm_limit': effective_page_size,
                    'sysparm_offset': offset,
                    'sysparm_order_by': 'sys_id'
                }
            else:
                # Default to page-based pagination
                page_number = (offset // effective_page_size) + 1
                params = {
                    'page': page_number,
                    'pageSize': effective_page_size,
                    'orderBy': 'id asc'
                }
            
            # Build URL with parameters
            if params:
                url_params = urllib.parse.urlencode(params)
                full_url = f"{url}?{url_params}"
            else:
                full_url = url
            
            # CRITICAL API DEBUG: Log the actual request being made
            self.logger.info(f"🌐 API REQUEST DEBUG",
                           full_url=full_url,
                           headers_count=len(headers),
                           auth_header_present=bool(headers.get('Authorization')),
                           service_name=service_name,
                           endpoint=endpoint,
                           params=params if 'params' in locals() else {},
                           effective_page_size=effective_page_size,
                           offset=offset)
            
            # Create request with headers
            request = urllib.request.Request(full_url, headers=headers)
            
            # Make the request
            with urllib.request.urlopen(request, timeout=30) as response:
                # Get response headers for pagination metadata
                response_headers = dict(response.headers)
                response_data = response.read().decode('utf-8')
                
                # CRITICAL API DEBUG: Log raw response details
                self.logger.info(f"🌐 API RESPONSE DEBUG",
                                status_code=response.getcode(),
                                response_size_bytes=len(response_data),
                                response_headers_count=len(response_headers),
                                content_type=response_headers.get('content-type', 'unknown'),
                                response_preview=response_data[:500] if response_data else 'empty')
                
                # MEMORY OPTIMIZATION: Check memory before JSON parsing
                if len(response_data) > 10 * 1024 * 1024:  # If response > 10MB
                    self.logger.warning(f"Large API response detected: {len(response_data) / 1024 / 1024:.1f}MB")
                    self._check_memory_usage(max_memory_mb=400)
                
                # Parse JSON and measure time
                json_parse_start = time.time()
                data = json.loads(response_data)
                json_parse_time = time.time() - json_parse_start
                
                # Immediately free response_data memory after parsing
                del response_data
            
            # Handle different response formats
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict) and 'data' in data:
                records = data['data']
            else:
                records = []
            
            # CRITICAL API DEBUG: Log data structure and record count
            self.logger.info(f"🌐 API DATA DEBUG",
                           data_type=type(data).__name__,
                           is_list=isinstance(data, list),
                           is_dict=isinstance(data, dict),
                           dict_keys=list(data.keys()) if isinstance(data, dict) else None,
                           records_count=len(records),
                           first_record_keys=list(records[0].keys()) if records else None,
                           json_parse_time=json_parse_time)
            
            # Log the full API call time
            api_call_time = time.time() - start_time
            self.logger.info(f"🌐 API TIMING DEBUG",
                           total_api_call_time=api_call_time,
                           json_parse_time=json_parse_time,
                           network_time=api_call_time - json_parse_time,
                           records_returned=len(records))
            
            # MEMORY OPTIMIZATION: Clean up parsed data structure if it's not records
            if isinstance(data, dict) and 'data' in data:
                # Free the wrapper dict memory
                del data
                gc.collect()
            
            return records
                
        except urllib.error.HTTPError as e:
            error_msg = f"HTTP {e.code} error fetching data batch"
            try:
                error_body = e.read().decode('utf-8')
                self.logger.error(f"{error_msg}: {error_body}")
            except:
                self.logger.error(f"{error_msg}: {str(e)}")
            return []
        except urllib.error.URLError as e:
            self.logger.error(f"URL error fetching data batch: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch data batch: {str(e)}")
            return []
    
    def _write_batch_to_s3(
        self,
        records: List[Dict[str, Any]],
        chunk_config: Dict[str, Any],
        table_config: Dict[str, Any],
        tenant_config: Dict[str, Any],
        batch_number: int
    ) -> Optional[str]:
        """Write a batch of records to S3 with memory-efficient processing."""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            from io import BytesIO
            
            tenant_id = tenant_config['tenant_id']
            # Use configured table_name from table_config, not derived from endpoint
            table_name = table_config['table_name']
            service_name = table_config['service_name']
            
            # Generate S3 key using configured table name
            timestamp = get_timestamp()
            s3_key = get_s3_key(tenant_id, 'raw', service_name, table_name, timestamp)
            
            # Add chunk and batch identifiers to ensure unique files
            chunk_id = chunk_config['chunk_id']
            s3_key = s3_key.replace('.parquet', f'_{chunk_id}_batch{batch_number:03d}.parquet')
            
            # Convert records to DataFrame and then to Parquet
            df = pd.DataFrame(records)
            
            # Convert DataFrame to Parquet in memory
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            parquet_data = parquet_buffer.getvalue()
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.config.bucket_name,
                Key=s3_key,
                Body=parquet_data,
                ContentType='application/octet-stream'
            )
            
            self.logger.info(
                f"Wrote batch {batch_number} with {len(records)} records to S3 as Parquet",
                s3_key=s3_key,
                record_count=len(records),
                batch_number=batch_number,
                table_name=table_name,
                service_name=service_name
            )
            
            # Aggressive memory cleanup
            del df
            del parquet_buffer
            del parquet_data
            del records
            gc.collect()
            gc.collect()  # Double gc.collect() for better cleanup
            
            # Note: Canonical transformation now triggered by result aggregator after all chunks complete
            # This eliminates race conditions and duplicate processing
            self.logger.info(
                f"S3 write completed for {table_name} batch {batch_number}",
                table_name=table_name,
                service_name=service_name,
                batch_number=batch_number,
                s3_key=s3_key
            )
            
            return s3_key
            
        except Exception as e:
            self.logger.error(f"Failed to write batch {batch_number} to S3: {str(e)}")
            return None

    def _write_to_s3(
        self,
        records: List[Dict[str, Any]],
        chunk_config: Dict[str, Any],
        table_config: Dict[str, Any],
        tenant_config: Dict[str, Any]
    ):
        """Write processed records to S3 (legacy method - kept for compatibility)."""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            from io import BytesIO
            
            tenant_id = tenant_config['tenant_id']
            # Use configured table_name from table_config, not derived from endpoint
            table_name = table_config['table_name']
            service_name = table_config['service_name']
            
            # Generate S3 key using configured table name
            timestamp = get_timestamp()
            s3_key = get_s3_key(tenant_id, 'raw', service_name, table_name, timestamp)
            
            # Add chunk identifier to the key (keep the naming convention but use parquet)
            chunk_id = chunk_config['chunk_id']
            s3_key = s3_key.replace('.parquet', f'_{chunk_id}.parquet')
            
            # Convert records to DataFrame and then to Parquet
            df = pd.DataFrame(records)
            
            # Convert DataFrame to Parquet in memory
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            parquet_data = parquet_buffer.getvalue()
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.config.bucket_name,
                Key=s3_key,
                Body=parquet_data,
                ContentType='application/octet-stream'
            )
            
            self.logger.info(
                f"Wrote {len(records)} records to S3 as Parquet",
                s3_key=s3_key,
                record_count=len(records),
                table_name=table_name,
                service_name=service_name
            )
            
            # Note: Canonical transformation now triggered by result aggregator after all chunks complete
            # This eliminates race conditions and duplicate processing
            self.logger.info(
                f"S3 write completed for {table_name}",
                table_name=table_name,
                service_name=service_name,
                s3_key=s3_key
            )
            
        except Exception as e:
            self.logger.error(f"Failed to write to S3: {str(e)}")
            raise
    
    # Note: _get_canonical_table_name method removed - functionality moved to result aggregator
    
    # Note: _trigger_canonical_transformation method removed - functionality moved to result aggregator
    
    def _update_chunk_progress(
        self, 
        chunk_id: str, 
        job_id: str, 
        status: str, 
        processing_result: Dict[str, Any]
    ):
        """Update chunk progress in DynamoDB."""
        try:
            update_expression = 'SET #status = :status, updated_at = :updated_at'
            expression_values = {
                ':status': {'S': status},
                ':updated_at': {'S': get_timestamp()}
            }
            expression_names = {'#status': 'status'}
            
            if 'records_processed' in processing_result:
                update_expression += ', records_processed = :records_processed'
                expression_values[':records_processed'] = {'N': str(processing_result['records_processed'])}
            
            if 'processing_time' in processing_result:
                update_expression += ', processing_time = :processing_time'
                expression_values[':processing_time'] = {'N': str(processing_result['processing_time'])}
            
            if 'error' in processing_result:
                update_expression += ', error_message = :error_message'
                expression_values[':error_message'] = {'S': processing_result['error']}
            
            self.dynamodb.update_item(
                TableName=self.chunk_progress_table,
                Key={
                    'job_id': {'S': job_id},
                    'chunk_id': {'S': chunk_id}
                },
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_names,
                ExpressionAttributeValues=expression_values
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to update chunk progress: {str(e)}")
    
    def _send_chunk_metrics(
        self, 
        job_id: str, 
        tenant_id: str, 
        table_name: str, 
        chunk_id: str,
        processing_result: Dict[str, Any]
    ):
        """Send CloudWatch metrics for chunk processing."""
        try:
            metrics = [
                {
                    'MetricName': 'ChunkProcessed',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id},
                        {'Name': 'TenantId', 'Value': tenant_id},
                        {'Name': 'TableName', 'Value': table_name}
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
            
            if 'records_processed' in processing_result:
                metrics.append({
                    'MetricName': 'ChunkRecordsProcessed',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id},
                        {'Name': 'TenantId', 'Value': tenant_id},
                        {'Name': 'TableName', 'Value': table_name}
                    ],
                    'Value': processing_result['records_processed'],
                    'Unit': 'Count'
                })
            
            if 'processing_time' in processing_result:
                metrics.append({
                    'MetricName': 'ChunkProcessingTime',
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': job_id},
                        {'Name': 'TenantId', 'Value': tenant_id},
                        {'Name': 'TableName', 'Value': table_name}
                    ],
                    'Value': processing_result['processing_time'],
                    'Unit': 'Seconds'
                })
            
            self.cloudwatch.put_metric_data(
                Namespace='AVESA/DataPipeline',
                MetricData=metrics
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to send chunk metrics: {str(e)}")


def lambda_handler(event, context):
    """Lambda entry point."""
    processor = ChunkProcessor()
    return processor.lambda_handler(event, context)# Force rebuild timestamp: Tue Jul  1 20:45:15 PDT 2025
