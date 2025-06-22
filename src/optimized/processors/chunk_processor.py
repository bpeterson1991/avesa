"""
Chunk Processor Lambda Function

Handles processing of individual data chunks with optimized API calls,
timeout handling, and progress tracking.
"""

import json
import time
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
    
    def __init__(self, context, buffer_seconds: int = 60):
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
    
    def __init__(self):
        self.config = Config.from_environment()
        self.logger = PipelineLogger("chunk-processor")
        self.dynamodb = get_dynamodb_client()
        self.cloudwatch = get_cloudwatch_client()
        self.s3_client = get_s3_client()
        
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
            chunk_config = event['chunk_config']
            table_config = event['table_config']
            tenant_config = event['tenant_config']
            job_id = event['job_id']
            
            chunk_id = chunk_config['chunk_id']
            table_name = table_config['table_name']
            tenant_id = tenant_config['tenant_id']
            
            self.logger = PipelineLogger("chunk-processor", tenant_id=tenant_id, table_name=table_name)
            self.logger.info("Starting chunk processing", job_id=job_id, chunk_id=chunk_id)
            
            # Initialize timeout handler
            timeout_handler = TimeoutHandler(context, buffer_seconds=60)
            
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
                'continuation_state': processing_result.get('continuation_state')
            }
            
            self.logger.info(
                "Chunk processing completed",
                job_id=job_id,
                chunk_id=chunk_id,
                records_processed=processing_result['records_processed'],
                completed=processing_result['completed']
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Chunk processing failed: {str(e)}", error=str(e))
            
            # Update chunk progress to failed
            if 'chunk_id' in locals() and 'job_id' in locals():
                self._update_chunk_progress(chunk_id, job_id, 'failed', {'error': str(e)})
            
            raise
    
    def _initialize_chunk_progress(self, chunk_config: Dict[str, Any], job_id: str):
        """Initialize chunk progress tracking."""
        try:
            chunk_id = chunk_config['chunk_id']
            
            progress_item = {
                'job_id': {'S': job_id},
                'chunk_id': {'S': chunk_id},
                'tenant_id': {'S': chunk_config['tenant_id']},
                'table_name': {'S': chunk_config['table_name']},
                'status': {'S': 'processing'},
                'start_offset': {'N': str(chunk_config['start_offset'])},
                'end_offset': {'N': str(chunk_config['end_offset'])},
                'estimated_records': {'N': str(chunk_config['estimated_records'])},
                'records_processed': {'N': '0'},
                'created_at': {'S': get_timestamp()},
                'updated_at': {'S': get_timestamp()}
            }
            
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
        """Process a single chunk of data."""
        start_time = time.time()
        records_processed = 0
        all_records = []
        
        try:
            # Get credentials
            credentials = table_config.get('credentials', {})
            if not credentials:
                raise ValueError("No credentials provided for API access")
            
            service_credentials = ServiceCredentials.from_dict(credentials)
            
            # Calculate pagination parameters
            start_offset = chunk_config['start_offset']
            end_offset = chunk_config['end_offset']
            chunk_size = end_offset - start_offset
            
            # Use optimized page size for API calls
            page_size = min(2000, max(1000, chunk_size // 5))
            
            # Process data in batches within the chunk
            current_offset = start_offset
            
            while current_offset < end_offset and timeout_handler.should_continue():
                # Calculate batch size for this iteration
                remaining_records = end_offset - current_offset
                batch_size = min(page_size, remaining_records)
                
                # Fetch batch of data
                batch_records = self._fetch_data_batch(
                    table_config,
                    service_credentials,
                    current_offset,
                    batch_size
                )
                
                if not batch_records:
                    break
                
                all_records.extend(batch_records)
                records_processed += len(batch_records)
                current_offset += len(batch_records)
                
                # Log progress
                self.logger.debug(
                    f"Processed batch: {len(batch_records)} records",
                    current_offset=current_offset,
                    total_processed=records_processed
                )
            
            # Write data to S3 if we have records
            if all_records:
                self._write_to_s3(all_records, chunk_config, table_config, tenant_config)
            
            processing_time = time.time() - start_time
            completed = current_offset >= end_offset
            
            result = {
                'completed': completed,
                'records_processed': records_processed,
                'processing_time': processing_time,
                'current_offset': current_offset
            }
            
            # Add continuation state if not completed
            if not completed:
                result['continuation_state'] = {
                    'current_offset': current_offset,
                    'remaining_records': end_offset - current_offset
                }
            
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
        batch_size: int
    ) -> List[Dict[str, Any]]:
        """Fetch a batch of data from the API."""
        try:
            # Using synchronous requests for Lambda compatibility
            # This provides reliable data fetching without async complexity
            
            endpoint = table_config['endpoint']
            service_name = table_config.get('service_name', 'unknown')
            
            # Get API base URL from credentials
            api_base_url = getattr(credentials, 'api_base_url', None) or getattr(credentials, 'instance_url', None) or getattr(credentials, 'base_url', None)
            
            if not api_base_url:
                raise ValueError(f"No API base URL found in credentials for service {service_name}")
            
            # Build API URL
            url = f"{api_base_url.rstrip('/')}/{endpoint.lstrip('/')}"
            
            # Calculate page number (most APIs use 1-based pagination)
            page_number = (offset // batch_size) + 1
            
            # Build headers - start with common headers
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
            
            # Build parameters - use service-agnostic parameter names
            params = {
                'page': page_number,
                'pageSize': batch_size,
                'orderBy': 'id asc'
            }
            
            # Handle service-specific parameter variations
            if service_name.lower() == 'salesforce':
                # Salesforce uses different parameter names
                params = {
                    'limit': batch_size,
                    'offset': offset
                }
            elif service_name.lower() == 'servicenow':
                # ServiceNow uses different parameter names
                params = {
                    'sysparm_limit': batch_size,
                    'sysparm_offset': offset,
                    'sysparm_order_by': 'sys_id'
                }
            
            # Add incremental sync conditions if needed
            # This would be implemented based on the last_updated timestamp
            
            # Make API request using urllib
            # Build URL with parameters
            if params:
                url_params = urllib.parse.urlencode(params)
                full_url = f"{url}?{url_params}"
            else:
                full_url = url
            
            # Create request with headers
            request = urllib.request.Request(full_url, headers=headers)
            
            # Make the request
            with urllib.request.urlopen(request, timeout=30) as response:
                response_data = response.read().decode('utf-8')
                data = json.loads(response_data)
            
            # Handle different response formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'data' in data:
                return data['data']
            else:
                return []
                
        except urllib.error.HTTPError as e:
            self.logger.error(f"HTTP error fetching data batch: {e.code} - {str(e)}")
            return []
        except urllib.error.URLError as e:
            self.logger.error(f"URL error fetching data batch: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch data batch: {str(e)}")
            return []
    
    def _write_to_s3(
        self,
        records: List[Dict[str, Any]],
        chunk_config: Dict[str, Any],
        table_config: Dict[str, Any],
        tenant_config: Dict[str, Any]
    ):
        """Write processed records to S3."""
        try:
            tenant_id = tenant_config['tenant_id']
            # Use configured table_name from table_config, not derived from endpoint
            table_name = table_config['table_name']
            service_name = table_config['service_name']
            
            # Generate S3 key using configured table name
            timestamp = get_timestamp()
            s3_key = get_s3_key(tenant_id, 'raw', service_name, table_name, timestamp)
            
            # Add chunk identifier to the key
            chunk_id = chunk_config['chunk_id']
            s3_key = s3_key.replace('.parquet', f'_{chunk_id}.json')
            
            # Convert records to JSON
            json_data = json.dumps(records, indent=2, default=str)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.config.bucket_name,
                Key=s3_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            
            self.logger.info(
                f"Wrote {len(records)} records to S3",
                s3_key=s3_key,
                record_count=len(records),
                table_name=table_name,
                service_name=service_name
            )
            
        except Exception as e:
            self.logger.error(f"Failed to write to S3: {str(e)}")
            raise
    
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
    return processor.lambda_handler(event, context)