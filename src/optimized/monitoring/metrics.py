"""
CloudWatch Metrics Module

Provides centralized metrics collection and reporting for the optimized
AVESA data pipeline with custom metrics and performance tracking.
"""

import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import boto3
from botocore.exceptions import ClientError

# Import shared modules
import sys
import os
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))

try:
    from logger import PipelineLogger
    from aws_clients import get_cloudwatch_client
except ImportError as e:
    print(f"Import error: {e}")


class MetricsCollector:
    """Centralized metrics collection for the optimized pipeline."""
    
    def __init__(self, namespace: str = "AVESA/DataPipeline"):
        self.namespace = namespace
        self.cloudwatch = get_cloudwatch_client()
        self.logger = PipelineLogger("metrics-collector")
        self._metric_buffer = []
        self._buffer_size = 20  # CloudWatch limit is 20 metrics per request
    
    def record_pipeline_initialization(
        self, 
        job_id: str, 
        tenant_count: int, 
        processing_mode: str,
        estimated_duration: int
    ):
        """Record pipeline initialization metrics."""
        metrics = [
            {
                'MetricName': 'PipelineInitialized',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'ProcessingMode', 'Value': processing_mode}
                ],
                'Value': 1,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            },
            {
                'MetricName': 'TenantCount',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id}
                ],
                'Value': tenant_count,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            },
            {
                'MetricName': 'EstimatedDuration',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id}
                ],
                'Value': estimated_duration,
                'Unit': 'Seconds',
                'Timestamp': datetime.now(timezone.utc)
            }
        ]
        
        self._add_metrics_to_buffer(metrics)
    
    def record_tenant_processing(
        self, 
        job_id: str, 
        tenant_id: str, 
        event_type: str,
        table_count: int = 0,
        processing_time: float = 0
    ):
        """Record tenant-level processing metrics."""
        metrics = [
            {
                'MetricName': f'TenantProcessing{event_type.title()}',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id}
                ],
                'Value': 1,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
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
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        if processing_time > 0:
            metrics.append({
                'MetricName': 'TenantProcessingTime',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id}
                ],
                'Value': processing_time,
                'Unit': 'Seconds',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        self._add_metrics_to_buffer(metrics)
    
    def record_table_processing(
        self, 
        job_id: str, 
        tenant_id: str, 
        table_name: str,
        event_type: str,
        chunk_count: int = 0,
        records_processed: int = 0,
        processing_time: float = 0
    ):
        """Record table-level processing metrics."""
        metrics = [
            {
                'MetricName': f'TableProcessing{event_type.title()}',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id},
                    {'Name': 'TableName', 'Value': table_name}
                ],
                'Value': 1,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
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
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        if records_processed > 0:
            metrics.append({
                'MetricName': 'TableRecordsProcessed',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id},
                    {'Name': 'TableName', 'Value': table_name}
                ],
                'Value': records_processed,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        if processing_time > 0:
            metrics.append({
                'MetricName': 'TableProcessingTime',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id},
                    {'Name': 'TableName', 'Value': table_name}
                ],
                'Value': processing_time,
                'Unit': 'Seconds',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        self._add_metrics_to_buffer(metrics)
    
    def record_chunk_processing(
        self, 
        job_id: str, 
        tenant_id: str, 
        table_name: str,
        chunk_id: str,
        records_processed: int,
        processing_time: float,
        api_calls: int = 0,
        error_count: int = 0
    ):
        """Record chunk-level processing metrics."""
        metrics = [
            {
                'MetricName': 'ChunkProcessed',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id},
                    {'Name': 'TableName', 'Value': table_name}
                ],
                'Value': 1,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            },
            {
                'MetricName': 'ChunkRecordsProcessed',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id},
                    {'Name': 'TableName', 'Value': table_name}
                ],
                'Value': records_processed,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            },
            {
                'MetricName': 'ChunkProcessingTime',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id},
                    {'Name': 'TableName', 'Value': table_name}
                ],
                'Value': processing_time,
                'Unit': 'Seconds',
                'Timestamp': datetime.now(timezone.utc)
            }
        ]
        
        if api_calls > 0:
            metrics.append({
                'MetricName': 'ChunkApiCalls',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id},
                    {'Name': 'TableName', 'Value': table_name}
                ],
                'Value': api_calls,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        if error_count > 0:
            metrics.append({
                'MetricName': 'ChunkErrors',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id},
                    {'Name': 'TableName', 'Value': table_name}
                ],
                'Value': error_count,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        # Calculate throughput metrics
        if processing_time > 0:
            records_per_second = records_processed / processing_time
            metrics.append({
                'MetricName': 'ChunkThroughput',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id},
                    {'Name': 'TenantId', 'Value': tenant_id},
                    {'Name': 'TableName', 'Value': table_name}
                ],
                'Value': records_per_second,
                'Unit': 'Count/Second',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        self._add_metrics_to_buffer(metrics)
    
    def record_error_metrics(
        self, 
        job_id: str, 
        error_type: str,
        tenant_id: Optional[str] = None,
        table_name: Optional[str] = None,
        error_count: int = 1
    ):
        """Record error metrics."""
        dimensions = [
            {'Name': 'JobId', 'Value': job_id},
            {'Name': 'ErrorType', 'Value': error_type}
        ]
        
        if tenant_id:
            dimensions.append({'Name': 'TenantId', 'Value': tenant_id})
        
        if table_name:
            dimensions.append({'Name': 'TableName', 'Value': table_name})
        
        metrics = [
            {
                'MetricName': 'ProcessingErrors',
                'Dimensions': dimensions,
                'Value': error_count,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            }
        ]
        
        self._add_metrics_to_buffer(metrics)
    
    def record_performance_metrics(
        self, 
        job_id: str,
        total_records: int,
        total_time: float,
        api_efficiency: float,
        cost_per_record: float = 0
    ):
        """Record overall performance metrics."""
        metrics = [
            {
                'MetricName': 'TotalRecordsProcessed',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id}
                ],
                'Value': total_records,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            },
            {
                'MetricName': 'TotalProcessingTime',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id}
                ],
                'Value': total_time,
                'Unit': 'Seconds',
                'Timestamp': datetime.now(timezone.utc)
            },
            {
                'MetricName': 'ApiEfficiency',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id}
                ],
                'Value': api_efficiency,
                'Unit': 'Percent',
                'Timestamp': datetime.now(timezone.utc)
            }
        ]
        
        if total_time > 0:
            throughput = total_records / total_time
            metrics.append({
                'MetricName': 'OverallThroughput',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id}
                ],
                'Value': throughput,
                'Unit': 'Count/Second',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        if cost_per_record > 0:
            metrics.append({
                'MetricName': 'CostPerRecord',
                'Dimensions': [
                    {'Name': 'JobId', 'Value': job_id}
                ],
                'Value': cost_per_record,
                'Unit': 'None',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        self._add_metrics_to_buffer(metrics)
    
    def _add_metrics_to_buffer(self, metrics: List[Dict[str, Any]]):
        """Add metrics to buffer and flush if needed."""
        self._metric_buffer.extend(metrics)
        
        if len(self._metric_buffer) >= self._buffer_size:
            self.flush_metrics()
    
    def flush_metrics(self):
        """Flush all buffered metrics to CloudWatch."""
        if not self._metric_buffer:
            return
        
        try:
            # Send metrics in batches
            while self._metric_buffer:
                batch = self._metric_buffer[:self._buffer_size]
                self._metric_buffer = self._metric_buffer[self._buffer_size:]
                
                self.cloudwatch.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch
                )
                
                self.logger.debug(f"Sent {len(batch)} metrics to CloudWatch")
            
        except Exception as e:
            self.logger.error(f"Failed to send metrics to CloudWatch: {str(e)}")
            # Clear buffer to prevent memory issues
            self._metric_buffer.clear()
    
    def __del__(self):
        """Ensure metrics are flushed when object is destroyed."""
        try:
            self.flush_metrics()
        except:
            pass


class PerformanceTracker:
    """Tracks performance metrics for optimization analysis."""
    
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.metrics_collector = MetricsCollector()
        self.start_time = time.time()
        self.performance_data = {
            'api_calls': 0,
            'records_processed': 0,
            'errors': 0,
            'chunks_processed': 0,
            'tenants_processed': 0,
            'tables_processed': 0
        }
    
    def track_api_call(self, response_time: float, records_returned: int, success: bool = True):
        """Track individual API call performance."""
        self.performance_data['api_calls'] += 1
        self.performance_data['records_processed'] += records_returned
        
        if not success:
            self.performance_data['errors'] += 1
    
    def track_chunk_completion(self, records_processed: int, processing_time: float):
        """Track chunk processing completion."""
        self.performance_data['chunks_processed'] += 1
        self.performance_data['records_processed'] += records_processed
    
    def track_table_completion(self, records_processed: int):
        """Track table processing completion."""
        self.performance_data['tables_processed'] += 1
        self.performance_data['records_processed'] += records_processed
    
    def track_tenant_completion(self, tables_processed: int, records_processed: int):
        """Track tenant processing completion."""
        self.performance_data['tenants_processed'] += 1
        self.performance_data['tables_processed'] += tables_processed
        self.performance_data['records_processed'] += records_processed
    
    def calculate_performance_summary(self) -> Dict[str, Any]:
        """Calculate overall performance summary."""
        total_time = time.time() - self.start_time
        
        summary = {
            'job_id': self.job_id,
            'total_time': total_time,
            'performance_data': self.performance_data.copy()
        }
        
        # Calculate derived metrics
        if total_time > 0:
            summary['throughput'] = self.performance_data['records_processed'] / total_time
        
        if self.performance_data['api_calls'] > 0:
            summary['api_efficiency'] = (
                (self.performance_data['api_calls'] - self.performance_data['errors']) / 
                self.performance_data['api_calls'] * 100
            )
            summary['avg_records_per_api_call'] = (
                self.performance_data['records_processed'] / self.performance_data['api_calls']
            )
        
        return summary
    
    def send_performance_metrics(self):
        """Send final performance metrics to CloudWatch."""
        summary = self.calculate_performance_summary()
        
        self.metrics_collector.record_performance_metrics(
            job_id=self.job_id,
            total_records=summary['performance_data']['records_processed'],
            total_time=summary['total_time'],
            api_efficiency=summary.get('api_efficiency', 0)
        )
        
        self.metrics_collector.flush_metrics()
        
        return summary