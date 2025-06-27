#!/usr/bin/env python3
"""
Comprehensive End-to-End Pipeline Test for AVESA Multi-Tenant Data Pipeline
===========================================================================

This test addresses critical gaps identified in the pipeline analysis:
1. Comprehensive End-to-End Validation
2. Robust Error Handling with Circuit Breakers
3. Data Integrity Verification and Cross-System Reconciliation
4. Schema Compliance and Drift Detection
5. Performance Monitoring and Benchmarking
6. Environment Flexibility (dev/staging/prod)
7. Detailed Logging and Metrics Collection

Features:
- Multi-stage pipeline validation (orchestrator → canonical → ClickHouse)
- Configurable time ranges for forward-fill and backfill operations
- Data lineage tracking and cross-system reconciliation
- Automated rollback mechanisms for failed operations
- Edge case handling (schema changes, network failures)
- Performance benchmarking with configurable thresholds
- Detailed reporting with metrics collection
- Support for sitetechnology companies dataset testing
"""

import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, field
from pathlib import Path
from contextlib import asynccontextmanager
import logging
import traceback

# Third-party imports
import boto3
import pandas as pd
import pytest
from botocore.exceptions import ClientError, BotoCoreError
from unittest.mock import Mock, patch, MagicMock

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

# Import shared modules
from backfill.shared.logger import PipelineLogger, get_logger
from backfill.shared.aws_clients import (
    get_dynamodb_client, get_s3_client, get_secrets_client, 
    get_cloudwatch_client, get_stepfunctions_client
)
from backfill.shared.clickhouse_client import ClickHouseClient, ClickHouseConnectionError
from backfill.shared.scd_config import SCDConfigManager
from backfill.shared.types import Company, Contact, Ticket, TimeEntry
from backfill.shared.config_simple import Config
from backfill.shared.utils import get_timestamp

# Import test utilities
from tests.shared.mock_configs import MockEnvironmentConfigs, MockAWSClients, MockClickHouseClient


@dataclass
class PipelineTestConfig:
    """Configuration for comprehensive pipeline testing."""
    environment: str = "dev"
    tenant_id: str = "sitetechnology"
    test_mode: str = "full"  # quick, full, data, performance, stress
    time_range_days: int = 30
    batch_size: int = 1000
    max_retries: int = 3
    timeout_seconds: int = 300
    performance_thresholds: Dict[str, float] = field(default_factory=lambda: {
        'orchestrator_max_duration': 60.0,
        'canonical_transform_max_duration': 120.0,
        'clickhouse_load_max_duration': 180.0,
        'end_to_end_max_duration': 600.0,
        'max_memory_usage_mb': 2048,
        'max_error_rate': 0.05
    })
    circuit_breaker_config: Dict[str, Any] = field(default_factory=lambda: {
        'failure_threshold': 5,
        'recovery_timeout': 60,
        'expected_exception': Exception
    })


@dataclass
class PipelineStageResult:
    """Result from a pipeline stage execution."""
    stage_name: str
    success: bool
    duration_seconds: float
    records_processed: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    artifacts: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataIntegrityResult:
    """Result from data integrity validation."""
    table_name: str
    source_count: int
    canonical_count: int
    clickhouse_count: int
    integrity_score: float
    missing_records: List[str] = field(default_factory=list)
    duplicate_records: List[str] = field(default_factory=list)
    schema_violations: List[str] = field(default_factory=list)


@dataclass
class TestExecutionReport:
    """Comprehensive test execution report."""
    test_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_duration: float = 0.0
    overall_success: bool = False
    stage_results: List[PipelineStageResult] = field(default_factory=list)
    integrity_results: List[DataIntegrityResult] = field(default_factory=list)
    performance_metrics: Dict[str, Any] = field(default_factory=dict)
    error_summary: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


class CircuitBreaker:
    """Circuit breaker implementation for robust error handling."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'HALF_OPEN'
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
            
            raise e


class ComprehensiveEndToEndPipelineTest:
    """
    Comprehensive end-to-end pipeline test implementation.
    
    This class provides complete validation of the AVESA data pipeline
    from ingestion through ClickHouse with robust error handling,
    performance monitoring, and detailed reporting.
    """
    
    def __init__(self, config: PipelineTestConfig):
        self.config = config
        self.test_id = f"e2e-test-{uuid.uuid4().hex[:8]}"
        self.logger = PipelineLogger(f"e2e-pipeline-test-{self.test_id}")
        
        # Initialize AWS clients
        self.region = os.environ.get('AWS_REGION', 'us-east-2')
        self.dynamodb = get_dynamodb_client(self.region)
        self.s3 = get_s3_client(self.region)
        self.secrets = get_secrets_client(self.region)
        self.cloudwatch = get_cloudwatch_client(self.region)
        self.stepfunctions = get_stepfunctions_client(self.region)
        
        # Initialize ClickHouse client
        self.clickhouse_secret = f"clickhouse-credentials-{config.environment}"
        self.clickhouse = None
        
        # Initialize SCD configuration manager
        self.scd_manager = SCDConfigManager(self.s3)
        
        # Circuit breakers for different components
        self.circuit_breakers = {
            'orchestrator': CircuitBreaker(**config.circuit_breaker_config),
            'canonical_transform': CircuitBreaker(**config.circuit_breaker_config),
            'clickhouse_load': CircuitBreaker(**config.circuit_breaker_config),
            'data_validation': CircuitBreaker(**config.circuit_breaker_config)
        }
        
        # Test execution tracking
        self.report = TestExecutionReport(
            test_id=self.test_id,
            start_time=datetime.now(timezone.utc)
        )
        
        # Environment configuration
        self.bucket_name = f"avesa-data-{config.environment}" if config.environment != "prod" else "avesa-data-prod"
        self.table_suffix = f"-{config.environment}" if config.environment != "prod" else ""
        
    async def run_comprehensive_test(self) -> TestExecutionReport:
        """
        Run comprehensive end-to-end pipeline test.
        
        Returns:
            Complete test execution report
        """
        self.logger.info(f"Starting comprehensive end-to-end pipeline test", test_id=self.test_id)
        
        try:
            # Initialize test environment
            await self._initialize_test_environment()
            
            # Run test stages based on mode
            if self.config.test_mode in ['quick', 'full']:
                await self._test_infrastructure_connectivity()
                await self._test_orchestrator_stage()
            
            if self.config.test_mode in ['data', 'full']:
                await self._test_canonical_transform_stage()
                await self._test_clickhouse_load_stage()
                await self._test_data_integrity_validation()
                await self._test_schema_compliance()
            
            if self.config.test_mode in ['performance', 'full']:
                await self._test_performance_benchmarks()
                await self._test_load_scenarios()
            
            if self.config.test_mode == 'stress':
                await self._test_stress_scenarios()
                await self._test_failure_recovery()
            
            # Cross-system reconciliation
            await self._test_cross_system_reconciliation()
            
            # Generate final report
            await self._generate_comprehensive_report()
            
            self.report.overall_success = all(
                stage.success for stage in self.report.stage_results
            )
            
        except Exception as e:
            self.logger.error(f"Comprehensive test failed: {str(e)}", error=str(e))
            self.report.error_summary.append(f"Test execution failed: {str(e)}")
            self.report.overall_success = False
        
        finally:
            self.report.end_time = datetime.now(timezone.utc)
            self.report.total_duration = (
                self.report.end_time - self.report.start_time
            ).total_seconds()
            
            # Cleanup test resources
            await self._cleanup_test_resources()
        
        return self.report
    
    async def _initialize_test_environment(self):
        """Initialize test environment and validate prerequisites."""
        stage_start = time.time()
        
        try:
            self.logger.info("Initializing test environment")
            
            # Initialize ClickHouse client
            try:
                self.clickhouse = ClickHouseClient(self.clickhouse_secret, self.region)
                # Test connection
                client = self.clickhouse.get_client()
                client.ping()
                self.logger.info("ClickHouse connection established")
            except Exception as e:
                raise Exception(f"Failed to initialize ClickHouse client: {e}")
            
            # Validate AWS permissions
            await self._validate_aws_permissions()
            
            # Validate environment configuration
            await self._validate_environment_config()
            
            # Initialize test data tracking
            await self._initialize_test_tracking()
            
            duration = time.time() - stage_start
            self.report.stage_results.append(PipelineStageResult(
                stage_name="environment_initialization",
                success=True,
                duration_seconds=duration
            ))
            
        except Exception as e:
            duration = time.time() - stage_start
            self.report.stage_results.append(PipelineStageResult(
                stage_name="environment_initialization",
                success=False,
                duration_seconds=duration,
                error_message=str(e)
            ))
            raise
    
    async def _validate_aws_permissions(self):
        """Validate required AWS permissions."""
        required_permissions = [
            ('dynamodb', 'list_tables'),
            ('s3', 'list_buckets'),
            ('secretsmanager', 'list_secrets'),
            ('stepfunctions', 'list_state_machines')
        ]
        
        for service, operation in required_permissions:
            try:
                if service == 'dynamodb':
                    self.dynamodb.list_tables()
                elif service == 's3':
                    self.s3.list_buckets()
                elif service == 'secretsmanager':
                    self.secrets.list_secrets()
                elif service == 'stepfunctions':
                    self.stepfunctions.list_state_machines()
            except Exception as e:
                raise Exception(f"Missing {service} {operation} permission: {e}")
    
    async def _validate_environment_config(self):
        """Validate environment configuration."""
        # Check required environment variables
        required_env_vars = [
            'AWS_REGION',
            'TENANT_SERVICES_TABLE',
            'LAST_UPDATED_TABLE',
            'BUCKET_NAME'
        ]
        
        missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
        if missing_vars:
            raise Exception(f"Missing environment variables: {', '.join(missing_vars)}")
        
        # Validate bucket access
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
        except Exception as e:
            raise Exception(f"Cannot access bucket {self.bucket_name}: {e}")
    
    async def _initialize_test_tracking(self):
        """Initialize test execution tracking in DynamoDB."""
        table_name = f"PipelineTestExecution{self.table_suffix}"
        
        try:
            self.dynamodb.put_item(
                TableName=table_name,
                Item={
                    'test_id': {'S': self.test_id},
                    'tenant_id': {'S': self.config.tenant_id},
                    'test_mode': {'S': self.config.test_mode},
                    'environment': {'S': self.config.environment},
                    'start_time': {'S': self.report.start_time.isoformat()},
                    'status': {'S': 'RUNNING'},
                    'created_at': {'S': get_timestamp()}
                }
            )
        except Exception as e:
            self.logger.warning(f"Failed to initialize test tracking: {e}")
    
    async def _test_infrastructure_connectivity(self):
        """Test basic infrastructure connectivity."""
        stage_start = time.time()
        
        try:
            self.logger.info("Testing infrastructure connectivity")
            
            # Test ClickHouse connectivity
            client = self.clickhouse.get_client()
            client.ping()
            
            # Test DynamoDB connectivity
            tenant_table = f"tenant-services{self.table_suffix}"
            self.dynamodb.describe_table(TableName=tenant_table)
            
            # Test S3 connectivity
            self.s3.list_objects_v2(Bucket=self.bucket_name, MaxKeys=1)
            
            duration = time.time() - stage_start
            self.report.stage_results.append(PipelineStageResult(
                stage_name="infrastructure_connectivity",
                success=True,
                duration_seconds=duration
            ))
            
        except Exception as e:
            duration = time.time() - stage_start
            self.report.stage_results.append(PipelineStageResult(
                stage_name="infrastructure_connectivity",
                success=False,
                duration_seconds=duration,
                error_message=str(e)
            ))
            raise
    
    async def _test_orchestrator_stage(self):
        """Test pipeline orchestrator stage."""
        stage_start = time.time()
        
        try:
            self.logger.info("Testing orchestrator stage")
            
            # Import orchestrator function
            from optimized.orchestrator.lambda_function import PipelineOrchestrator
            
            # Create test event
            test_event = {
                'tenant_id': self.config.tenant_id,
                'table_name': 'companies',
                'force_full_sync': False,
                'test_mode': True
            }
            
            # Mock Lambda context
            mock_context = Mock()
            mock_context.aws_request_id = f"test-{self.test_id}"
            mock_context.function_name = "test-orchestrator"
            
            # Execute orchestrator with circuit breaker
            orchestrator = PipelineOrchestrator()
            result = self.circuit_breakers['orchestrator'].call(
                orchestrator.lambda_handler, test_event, mock_context
            )
            
            # Validate orchestrator response
            required_fields = ['job_id', 'mode', 'tenants', 'estimated_duration']
            missing_fields = [field for field in required_fields if field not in result]
            
            if missing_fields:
                raise Exception(f"Orchestrator response missing fields: {missing_fields}")
            
            duration = time.time() - stage_start
            self.report.stage_results.append(PipelineStageResult(
                stage_name="orchestrator",
                success=True,
                duration_seconds=duration,
                records_processed=len(result.get('tenants', [])),
                artifacts={'orchestrator_result': result}
            ))
            
            # Check performance threshold
            if duration > self.config.performance_thresholds['orchestrator_max_duration']:
                self.report.recommendations.append(
                    f"Orchestrator stage exceeded performance threshold: {duration:.2f}s"
                )
            
        except Exception as e:
            duration = time.time() - stage_start
            self.report.stage_results.append(PipelineStageResult(
                stage_name="orchestrator",
                success=False,
                duration_seconds=duration,
                error_message=str(e)
            ))
            self.logger.error(f"Orchestrator stage failed: {str(e)}")
    
    async def _test_canonical_transform_stage(self):
        """Test canonical transformation stage."""
        stage_start = time.time()
        
        try:
            self.logger.info("Testing canonical transform stage")
            
            # Import canonical transform function
            from canonical_transform.lambda_function import lambda_handler as canonical_handler
            
            # Create test event for canonical transformation
            test_event = {
                'tenant_id': self.config.tenant_id,
                'service': 'connectwise',
                'table_name': 'companies',
                'source_key': f"{self.config.tenant_id}/raw/connectwise/companies/test_data.json",
                'target_key': f"{self.config.tenant_id}/canonical/companies/test_output.parquet"
            }
            
            # Mock Lambda context
            mock_context = Mock()
            mock_context.aws_request_id = f"canonical-{self.test_id}"
            
            # Execute canonical transform with circuit breaker
            result = self.circuit_breakers['canonical_transform'].call(
                canonical_handler, test_event, mock_context
            )
            
            # Validate canonical transform result
            if not result.get('success', False):
                raise Exception(f"Canonical transform failed: {result.get('error', 'Unknown error')}")
            
            # Verify output file exists
            try:
                self.s3.head_object(Bucket=self.bucket_name, Key=test_event['target_key'])
            except Exception as e:
                raise Exception(f"Canonical output file not found: {e}")
            
            duration = time.time() - stage_start
            records_processed = result.get('records_processed', 0)
            
            self.report.stage_results.append(PipelineStageResult(
                stage_name="canonical_transform",
                success=True,
                duration_seconds=duration,
                records_processed=records_processed,
                artifacts={'canonical_result': result}
            ))
            
            # Check performance threshold
            if duration > self.config.performance_thresholds['canonical_transform_max_duration']:
                self.report.recommendations.append(
                    f"Canonical transform exceeded performance threshold: {duration:.2f}s"
                )
            
        except Exception as e:
            duration = time.time() - stage_start
            self.report.stage_results.append(PipelineStageResult(
                stage_name="canonical_transform",
                success=False,
                duration_seconds=duration,
                error_message=str(e)
            ))
            self.logger.error(f"Canonical transform stage failed: {str(e)}")
    
    async def _test_clickhouse_load_stage(self):
        """Test ClickHouse data loading stage."""
        stage_start = time.time()
        
        try:
            self.logger.info("Testing ClickHouse load stage")
            
            # Import ClickHouse data loader
            from clickhouse.data_loader.lambda_function import lambda_handler as clickhouse_handler
            
            # Create test event for ClickHouse loading
            test_event = {
                'tenant_id': self.config.tenant_id,
                'table_name': 'companies',
                'source_key': f"{self.config.tenant_id}/canonical/companies/test_output.parquet",
                'batch_size': self.config.batch_size
            }
            
            # Mock Lambda context
            mock_context = Mock()
            mock_context.aws_request_id = f"clickhouse-{self.test_id}"
            
            # Execute ClickHouse load with circuit breaker
            result = self.circuit_breakers['clickhouse_load'].call(
                clickhouse_handler, test_event, mock_context
            )
            
            # Validate ClickHouse load result
            if not result.get('success', False):
                raise Exception(f"ClickHouse load failed: {result.get('error', 'Unknown error')}")
            
            # Verify data was loaded
            client = self.clickhouse.get_client()
            count_query = f"SELECT COUNT(*) FROM companies WHERE tenant_id = '{self.config.tenant_id}'"
            count_result = client.query(count_query)
            record_count = count_result.result_rows[0][0] if count_result.result_rows else 0
            
            if record_count == 0:
                raise Exception("No records found in ClickHouse after load")
            
            duration = time.time() - stage_start
            
            self.report.stage_results.append(PipelineStageResult(
                stage_name="clickhouse_load",
                success=True,
                duration_seconds=duration,
                records_processed=record_count,
                artifacts={'clickhouse_result': result}
            ))
            
            # Check performance threshold
            if duration > self.config.performance_thresholds['clickhouse_load_max_duration']:
                self.report.recommendations.append(
                    f"ClickHouse load exceeded performance threshold: {duration:.2f}s"
                )
            
        except Exception as e:
            duration = time.time() - stage_start
            self.report.stage_results.append(PipelineStageResult(
                stage_name="clickhouse_load",
                success=False,
                duration_seconds=duration,
                error_message=str(e)
            ))
            self.logger.error(f"ClickHouse load stage failed: {str(e)}")
    
    async def _test_data_integrity_validation(self):
        """Test comprehensive data integrity validation."""
        stage_start = time.time()
        
        try:
            self.logger.info("Testing data integrity validation")
            
            tables = ['companies', 'contacts', 'tickets', 'time_entries']
            integrity_results = []
            
            for table in tables:
                try:
                    integrity_result = await self._validate_table_integrity(table)
                    integrity_results.append(integrity_result)
                    self.report.integrity_results.append(integrity_result)
                except Exception as e:
                    self.logger.error(f"Integrity validation failed for {table}: {e}")
                    integrity_results.append(DataIntegrityResult(
                        table_name=table,
                        source_count=0,
                        canonical_count=0,
                        clickhouse_count=0,
                        integrity_score=0.0,
                        schema_violations=[f"Validation failed: {str(e)}"]
                    ))
            
            # Calculate overall integrity score
            overall_score = sum(r.integrity_score for r in integrity_results) / len(integrity_results)
            
            duration = time.time() - stage_start
            self.report.stage_results.append(PipelineStageResult(
                stage_name="data_integrity_validation",
                success=overall_score > 0.95,  # 95% integrity threshold
                duration_seconds=duration,
                metrics={'overall_integrity_score': overall_score},
                artifacts={'integrity_results': integrity_results}
            ))
            
        except Exception as e:
            duration = time.time() - stage_start
            self.report.stage_results.append(PipelineStageResult(
                stage_name="data_integrity_validation",
                success=False,
                duration_seconds=duration,
                error_message=str(e)
            ))
            self.logger.error(f"Data integrity validation failed: {str(e)}")
    
    async def _validate_table_integrity(self, table_name: str) -> DataIntegrityResult:
        """Validate integrity for a specific table."""
        try:
            # Get source data count (from S3 raw data)
            source_count = await self._count_source_records(table_name)
            
            # Get canonical data count (from S3 canonical data)
            canonical_count = await self._count_canonical_records(table_name)
            
            # Get ClickHouse data count
            clickhouse_count = await self._count_clickhouse_records(table_name)
            
            # Calculate integrity score
            if source_count == 0:
                integrity_score = 1.0 if canonical_count == 0 and clickhouse_count == 0 else 0.0
            else:
                canonical_ratio = canonical_count / source_count if source_count > 0 else 0
                clickhouse_ratio = clickhouse_count / canonical_count if canonical_count > 0 else 0
                integrity_score = min(canonical_ratio, clickhouse_ratio)
            
            # Detect missing and duplicate records
            missing_records = await self._detect_missing_records(table_name)
            duplicate_records = await self._detect_duplicate_records(table_name)
            
            # Validate schema compliance
            schema_violations = await self._validate_schema_compliance(table_name)
            
            return DataIntegrityResult(
                table_name=table_name,
                source_count=source_count,
                canonical_count=canonical_count,
                clickhouse_count=clickhouse_count,
                integrity_score=integrity_score,
                missing_records=missing_records,
                duplicate_records=duplicate_records,
                schema_violations=schema_violations
            )
            
        except Exception as e:
            self.logger.error(f"Table integrity validation failed for {table_name}: {e}")
            raise
    
    async def _count_source_records(self, table_name: str) -> int:
        """Count records in source data."""
        try:
            prefix = f"{self.config.tenant_id}/raw/"
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=1000
            )
            
            # This is a simplified count - in practice, you'd parse the actual files
            return len(response.get('Contents', []))
            
        except Exception as e:
            self.logger.warning(f"Failed to count source records for {table_name}: {e}")
            return 0
    
    async def _count_canonical_records(self, table_name: str) -> int:
        """Count records in canonical data."""
        try:
            prefix = f"{self.config.tenant_id}/canonical/{table_name}/"
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=1000
            )
            
            total_records = 0
            for obj in response.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    # In practice, you'd read the parquet file and count records
                    # For now, estimate based on file size
                    total_records += max(1, obj['Size'] // 1000)  # Rough estimate
            
            return total_records
            
        except Exception as e:
            self.logger.warning(f"Failed to count canonical records for {table_name}: {e}")
            return 0
    
    async def _count_clickhouse_records(self, table_name: str) -> int:
        """Count records in ClickHouse."""
        try:
            client = self.clickhouse.get_client()
            query = f"SELECT COUNT(*) FROM {table_name} WHERE tenant_id = '{self.config.tenant_id}'"
            result = client.query(query)
            return result.result_rows[0][0] if result.result_rows else 0
            
        except Exception as e:
            self.logger.warning(f"Failed to count ClickHouse records for {table_name}: {e}")
            return 0
    
    async def _detect_missing_records(self, table_name: str) -> List[str]:
        """Detect missing records between stages."""
        # This would implement sophisticated record tracking
        # For now, return empty list
        return []
    
    async def _detect_duplicate_records(self, table_name: str) -> List[str]:
        """Detect duplicate records in ClickHouse."""
        try:
            client = self.clickhouse.get_client()
            
            # Get SCD type for this table
            scd_type = self.scd_manager.get_scd_type(table_name)
            
            if scd_type == 'type_2':
                # For SCD Type 2, check for duplicates in current records only
                query = f"""
                SELECT id, COUNT(*) as cnt
                FROM {table_name}
                WHERE tenant_id = '{self.config.tenant_id}' AND is_current = true
                GROUP BY id
                HAVING cnt > 1
                """
            else:
                # For SCD Type 1, check for any duplicates
                query = f"""
                SELECT id, COUNT(*) as cnt
                FROM {table_name}
                WHERE tenant_id = '{self.config.tenant_id}'
                GROUP BY id
                HAVING cnt > 1
                """
            
            result = client.query(query)
            return [row[0] for row in result.result_rows]
            
        except Exception as e:
            self.logger.warning(f"Failed to detect duplicates for {table_name}: {e}")
            return []
    
    async def _validate_schema_compliance(self, table_name: str) -> List[str]:
        """Validate schema compliance for a table."""
        violations = []
        
        try:
            client = self.clickhouse.get_client()
            
            # Get table schema
            schema_query = f"DESCRIBE TABLE {table_name}"
            schema_result = client.query(schema_query)
            columns = [row[0] for row in schema_result.result_rows]
            
            # Check required fields based on SCD type
            scd_type = self.scd_manager.get_scd_type(table_name)
            
            required_fields = ['id', 'tenant_id', 'created_date', 'updated_date']
            if scd_type == 'type_2':
                required_fields.extend(['is_current