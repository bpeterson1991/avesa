"""
Performance Optimization Stack

CDK stack for the optimized AVESA data pipeline infrastructure including
DynamoDB tables, Lambda functions, Step Functions, and monitoring.
"""

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    BundlingOptions,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_iam as iam,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_events as events,
    aws_events_targets as targets
)
from constructs import Construct
from typing import Dict, Any
import json
import os
import time

# Import dashboard configuration
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'optimized', 'monitoring'))
try:
    from dashboards import PipelineDashboards
except ImportError:
    PipelineDashboards = None

# Note: Removed bundling utilities import - using simple asset packaging for optimized functions


class PerformanceOptimizationStack(Stack):
    """CDK stack for the optimized AVESA data pipeline."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        data_bucket_name: str,
        tenant_services_table_name: str,
        last_updated_table_name: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = environment
        self.data_bucket_name = data_bucket_name
        self.tenant_services_table_name = tenant_services_table_name
        self.last_updated_table_name = last_updated_table_name
        self.timestamp = str(int(time.time()))

        # Create S3 bucket for data storage
        self.data_bucket = self._create_data_bucket()

        # Create base DynamoDB tables
        self.tenant_services_table = self._create_tenant_services_table()
        self.last_updated_table = self._create_last_updated_table()
        
        # Create DynamoDB tables for optimization
        self.processing_jobs_table = self._create_processing_jobs_table()
        self.chunk_progress_table = self._create_chunk_progress_table()

        # Create IAM roles
        self.lambda_execution_role = self._create_lambda_execution_role()
        self.step_functions_role = self._create_step_functions_role()

        # Create Lambda functions (which also creates state machines)
        self.lambda_functions, self.state_machines = self._create_lambda_functions()

        # Create CloudWatch dashboards
        self._create_dashboards()

        # Create EventBridge rules for scheduling
        self._create_scheduled_rules()

    def _create_data_bucket(self) -> s3.Bucket:
        """Import existing S3 bucket for data storage."""
        # Import existing bucket - CDK will handle gracefully if it doesn't exist during deployment
        bucket = s3.Bucket.from_bucket_name(
            self,
            "DataBucket",
            bucket_name=self.data_bucket_name
        )
        return bucket

    def _create_processing_jobs_table(self) -> dynamodb.Table:
        """Import existing DynamoDB table for tracking processing jobs."""
        table_name = f"ProcessingJobs-{self.env_name}"
        
        # Import existing table
        table = dynamodb.Table.from_table_name(
            self,
            "ProcessingJobsTable",
            table_name=table_name
        )
        return table

    def _create_chunk_progress_table(self) -> dynamodb.Table:
        """Import existing DynamoDB table for tracking chunk processing progress."""
        table_name = f"ChunkProgress-{self.env_name}"
        
        # Import existing table
        table = dynamodb.Table.from_table_name(
            self,
            "ChunkProgressTable",
            table_name=table_name
        )
        return table

    def _create_tenant_services_table(self) -> dynamodb.Table:
        """Import existing DynamoDB table for tenant service configuration."""
        table_name = self.tenant_services_table_name
        
        # Import existing table
        table = dynamodb.Table.from_table_name(
            self,
            "TenantServicesTable",
            table_name=table_name
        )
        return table

    def _create_last_updated_table(self) -> dynamodb.Table:
        """Import existing DynamoDB table for tracking last updated timestamps."""
        table_name = self.last_updated_table_name
        
        # Import existing table
        table = dynamodb.Table.from_table_name(
            self,
            "LastUpdatedTable",
            table_name=table_name
        )
        return table

    def _create_lambda_execution_role(self) -> iam.Role:
        """Create IAM role for Lambda functions with necessary permissions."""
        role = iam.Role(
            self,
            "OptimizedLambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )

        # Add custom policy for optimized pipeline operations
        policy = iam.Policy(
            self,
            "OptimizedPipelinePolicy",
            statements=[
                # DynamoDB permissions
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:Scan",
                        "dynamodb:Query",
                        "dynamodb:BatchGetItem",
                        "dynamodb:BatchWriteItem"
                    ],
                    resources=[
                        self.processing_jobs_table.table_arn,
                        self.chunk_progress_table.table_arn,
                        self.tenant_services_table.table_arn,
                        self.last_updated_table.table_arn,
                        # Include GSI ARNs
                        f"{self.processing_jobs_table.table_arn}/index/*",
                        f"{self.chunk_progress_table.table_arn}/index/*"
                    ]
                ),
                # Secrets Manager permissions
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "secretsmanager:GetSecretValue"
                    ],
                    resources=["*"]
                ),
                # S3 permissions
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:ListBucket"
                    ],
                    resources=[
                        self.data_bucket.bucket_arn,
                        f"{self.data_bucket.bucket_arn}/*"
                    ]
                ),
                # CloudWatch permissions
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "cloudwatch:PutMetricData"
                    ],
                    resources=["*"]
                ),
                # Step Functions permissions (for nested executions)
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                        "states:DescribeExecution",
                        "states:StopExecution"
                    ],
                    resources=["*"]
                ),
                # Lambda invoke permissions (for cross-Lambda calls)
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "lambda:InvokeFunction"
                    ],
                    resources=[
                        f"arn:aws:lambda:{self.region}:{self.account}:function:avesa-*-{self.env_name}",
                        f"arn:aws:lambda:{self.region}:{self.account}:function:clickhouse-*-{self.env_name}"
                    ]
                )
            ]
        )
        role.attach_inline_policy(policy)
        return role

    def _create_step_functions_role(self) -> iam.Role:
        """Create IAM role for Step Functions with necessary permissions."""
        role = iam.Role(
            self,
            "OptimizedStepFunctionsRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com")
        )

        # Add policy for Step Functions operations
        policy = iam.Policy(
            self,
            "OptimizedStepFunctionsPolicy",
            statements=[
                # Lambda invocation permissions
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "lambda:InvokeFunction"
                    ],
                    resources=["*"]  # Will be restricted to specific functions
                ),
                # Step Functions execution permissions
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                        "states:DescribeExecution",
                        "states:StopExecution"
                    ],
                    resources=["*"]
                ),
                # CloudWatch Logs permissions
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:DescribeLogGroups",
                        "logs:DescribeLogStreams"
                    ],
                    resources=["*"]
                ),
                # EventBridge permissions for managed rules
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "events:PutRule",
                        "events:DeleteRule",
                        "events:DescribeRule",
                        "events:EnableRule",
                        "events:DisableRule",
                        "events:ListRules",
                        "events:PutTargets",
                        "events:RemoveTargets",
                        "events:ListTargetsByRule"
                    ],
                    resources=["*"]
                )
            ]
        )
        role.attach_inline_policy(policy)
        return role

    def _create_lambda_functions(self) -> tuple[Dict[str, _lambda.Function], Dict[str, sfn.StateMachine]]:
        """Create all Lambda functions for the optimized pipeline."""
        functions = {}
        
        # Common environment variables
        common_env = {
            "BUCKET_NAME": self.data_bucket.bucket_name,
            "TENANT_SERVICES_TABLE": self.tenant_services_table.table_name,
            "LAST_UPDATED_TABLE": self.last_updated_table.table_name,
            "ENVIRONMENT": self.env_name,
            "PROCESSING_JOBS_TABLE": self.processing_jobs_table.table_name,
            "CHUNK_PROGRESS_TABLE": self.chunk_progress_table.table_name
        }

        # Create state machines first so we can reference them
        state_machines = self._create_state_machines_early()
        
        # EMERGENCY FIX: Use simple asset packaging to avoid import issues
        # from infrastructure.shared.bundling_utils import BundlingOptionsFactory

        # Pipeline Orchestrator with STATE_MACHINE_ARN
        orchestrator_env = common_env.copy()
        orchestrator_env["STATE_MACHINE_ARN"] = state_machines['pipeline_orchestrator'].state_machine_arn
        
        functions['orchestrator'] = _lambda.Function(
            self,
            "PipelineOrchestratorLambda",
            function_name=f"avesa-pipeline-orchestrator-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../src/optimized/orchestrator"),
            role=self.lambda_execution_role,
            memory_size=512,
            timeout=Duration.seconds(300),
            environment=orchestrator_env,
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        # Tenant Processor
        functions['tenant_processor'] = _lambda.Function(
            self,
            "TenantProcessorLambda",
            function_name=f"avesa-tenant-processor-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="tenant_processor.lambda_handler",
            code=_lambda.Code.from_asset("../src/optimized/processors"),
            role=self.lambda_execution_role,
            memory_size=512,
            timeout=Duration.seconds(300),
            environment=common_env,
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        # Table Processor
        functions['table_processor'] = _lambda.Function(
            self,
            "TableProcessorLambda",
            function_name=f"avesa-table-processor-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="table_processor.lambda_handler",
            code=_lambda.Code.from_asset("../src/optimized/processors"),
            role=self.lambda_execution_role,
            memory_size=512,
            timeout=Duration.seconds(300),
            environment=common_env,
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        # Chunk Processor
        functions['chunk_processor'] = _lambda.Function(
            self,
            "ChunkProcessorLambda",
            function_name=f"avesa-chunk-processor-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="chunk_processor.lambda_handler",
            code=_lambda.Code.from_asset("../src/optimized/processors"),
            role=self.lambda_execution_role,
            memory_size=1024,  # Higher memory for data processing
            timeout=Duration.seconds(900),  # 15 minutes
            environment=common_env,
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        # Error Handler
        functions['error_handler'] = _lambda.Function(
            self,
            "ErrorHandlerLambda",
            function_name=f"avesa-error-handler-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="error_handler.lambda_handler",
            code=_lambda.Code.from_asset("../src/optimized/helpers"),
            role=self.lambda_execution_role,
            memory_size=256,
            timeout=Duration.seconds(60),
            environment=common_env,
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        # Result Aggregator
        functions['result_aggregator'] = _lambda.Function(
            self,
            "ResultAggregatorLambda",
            function_name=f"avesa-result-aggregator-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="result_aggregator.lambda_handler",
            code=_lambda.Code.from_asset("../src/optimized/helpers"),
            role=self.lambda_execution_role,
            memory_size=512,
            timeout=Duration.seconds(300),
            environment=common_env,
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        # Completion Notifier
        functions['completion_notifier'] = _lambda.Function(
            self,
            "CompletionNotifierLambda",
            function_name=f"avesa-completion-notifier-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="completion_notifier.lambda_handler",
            code=_lambda.Code.from_asset("../src/optimized/helpers"),
            role=self.lambda_execution_role,
            memory_size=256,
            timeout=Duration.seconds(60),
            environment=common_env,
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        # Create Lambda layers for canonical transform functions
        # AWS managed pandas layer
        aws_pandas_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "AWSPandasLayer",
            layer_version_arn="arn:aws:lambda:us-east-2:336392948345:layer:AWSSDKPandas-Python39:13"
        )
        
        # Existing ClickHouse dependencies layer
        clickhouse_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "ClickHouseLayer",
            layer_version_arn="arn:aws:lambda:us-east-2:123938354448:layer:clickhouse-dependencies-dev:5"
        )

        # Canonical Transform Functions using simple asset packaging with Lambda layers
        # NOTE: These are the ONLY canonical transform functions - removed from deprecated ConnectWise stack
        canonical_tables = ['companies', 'contacts', 'tickets', 'time_entries']
        for table in canonical_tables:
            # Add CANONICAL_TABLE environment variable for each function
            canonical_env = common_env.copy()
            canonical_env["CANONICAL_TABLE"] = table
            
            functions[f'canonical_transform_{table}'] = _lambda.Function(
                self,
                f"CanonicalTransform{table.title().replace('_', '')}Lambda",
                function_name=f"avesa-canonical-transform-{table.replace('_', '-')}-{self.env_name}",
                runtime=_lambda.Runtime.PYTHON_3_9,
                handler="lambda_function.lambda_handler",
                code=_lambda.Code.from_asset("../src/canonical_transform"),
                role=self.lambda_execution_role,
                memory_size=1024,
                timeout=Duration.seconds(900),
                environment=canonical_env,
                layers=[aws_pandas_layer, clickhouse_layer],  # Add both layers for pandas/pyarrow and ClickHouse dependencies
                log_retention=logs.RetentionDays.ONE_MONTH
            )

        # NOTE: ClickHouse Loader Functions are created in ClickHouseStack - removed duplicates

        return functions, state_machines

    def _create_state_machines_early(self) -> Dict[str, sfn.StateMachine]:
        """Create Step Functions state machines using CDK native constructs."""
        state_machines = {}

        # Create placeholder Lambda functions for state machine references
        # These will be replaced with actual functions later
        placeholder_lambda = _lambda.Function(
            self,
            "PlaceholderLambda",
            function_name=f"avesa-placeholder-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_inline("def handler(event, context): return event"),
            role=self.lambda_execution_role,
            timeout=Duration.seconds(30)
        )

        # Pipeline Orchestrator State Machine using CDK native constructs
        # Initialize pipeline
        initialize_pipeline = sfn.Pass(
            self,
            "InitializePipeline",
            comment="Pipeline already initialized by orchestrator Lambda",
            result=sfn.Result.from_string("Pipeline initialized"),
            result_path="$.initialization_status"
        )

        # Determine processing mode
        determine_mode = sfn.Choice(self, "DetermineProcessingMode")
        
        # Multi-tenant processing with iterator
        tenant_processing_task = sfn_tasks.LambdaInvoke(
            self,
            "ProcessTenant",
            lambda_function=placeholder_lambda,  # Will be updated with actual function
            payload=sfn.TaskInput.from_object({
                "tenant_config.$": "$$.Map.Item.Value",
                "job_id.$": "$.job_id",
                "table_name.$": "$.table_name",
                "force_full_sync.$": "$.force_full_sync",
                "execution_id.$": "$$.Execution.Name"
            })
        )
        
        multi_tenant_processing = sfn.Map(
            self,
            "MultiTenantProcessing",
            items_path="$.tenants",
            max_concurrency=10,
            parameters={
                "tenant_config.$": "$$.Map.Item.Value",
                "job_id.$": "$.job_id",
                "table_name.$": "$.table_name",
                "force_full_sync.$": "$.force_full_sync",
                "execution_id.$": "$$.Execution.Name"
            }
        ).iterator(tenant_processing_task)

        # Single tenant processing - use Lambda invoke instead of Step Functions for now
        single_tenant_processing = sfn_tasks.LambdaInvoke(
            self,
            "SingleTenantProcessing",
            lambda_function=placeholder_lambda,  # Will be updated with actual function
            payload=sfn.TaskInput.from_object({
                "tenant_config.$": "$.tenants[0]",
                "job_id.$": "$.job_id",
                "table_name.$": "$.table_name",
                "force_full_sync.$": "$.force_full_sync",
                "execution_id.$": "$$.Execution.Name"
            }),
            result_path="$.tenant_result"
        )

        # Result aggregation
        aggregate_results = sfn_tasks.LambdaInvoke(
            self,
            "AggregateResults",
            lambda_function=placeholder_lambda,  # Will be updated with actual function
            payload=sfn.TaskInput.from_object({
                "job_id.$": "$.job_id",
                "results.$": "$",
                "processing_mode": "multi-tenant"
            }),
            result_path="$.aggregation_result"
        )

        # Completion notification
        notify_completion = sfn_tasks.LambdaInvoke(
            self,
            "NotifyCompletion",
            lambda_function=placeholder_lambda,  # Will be updated with actual function
            payload=sfn.TaskInput.from_object({
                "job_id.$": "$.job_id",
                "results.$": "$",
                "execution_arn.$": "$$.Execution.Name"
            })
        )

        # Error handling
        handle_invalid_mode = sfn.Fail(
            self,
            "HandleInvalidMode",
            cause="Invalid processing mode specified",
            error="InvalidProcessingMode"
        )

        # Build the state machine definition with separate aggregation paths
        definition = initialize_pipeline.next(
            determine_mode
            .when(
                sfn.Condition.string_equals("$.mode", "multi-tenant"),
                multi_tenant_processing.next(aggregate_results).next(notify_completion)
            )
            .when(
                sfn.Condition.string_equals("$.mode", "single-tenant"),
                single_tenant_processing.next(notify_completion)
            )
            .otherwise(handle_invalid_mode)
        )

        state_machines['pipeline_orchestrator'] = sfn.StateMachine(
            self,
            "PipelineOrchestratorStateMachine",
            state_machine_name=f"PipelineOrchestrator-{self.env_name}",
            definition=definition,
            role=self.step_functions_role,
            timeout=Duration.hours(6),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self,
                    "PipelineOrchestratorLogGroup",
                    log_group_name=f"/aws/stepfunctions/OptimizedPipelineOrchestrator-{self.env_name}-{self.timestamp}",
                    retention=logs.RetentionDays.ONE_MONTH
                ),
                level=sfn.LogLevel.ALL
            )
        )

        # Tenant Processor State Machine using CDK native constructs
        discover_tables = sfn_tasks.LambdaInvoke(
            self,
            "DiscoverTenantTables",
            lambda_function=placeholder_lambda,  # Will be updated with actual function
            payload=sfn.TaskInput.from_object({
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "table_name.$": "$.table_name",
                "force_full_sync.$": "$.force_full_sync"
            }),
            result_path="$.table_discovery"
        )

        validate_discovery = sfn.Choice(self, "ValidateTableDiscovery")
        
        # Table processing task for iterator
        table_processing_task = sfn_tasks.LambdaInvoke(
            self,
            "ProcessTable",
            lambda_function=placeholder_lambda,  # Will be updated with actual function
            payload=sfn.TaskInput.from_object({
                "table_config.$": "$$.Map.Item.Value",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "force_full_sync.$": "$.force_full_sync",
                "execution_id.$": "$.execution_id"
            })
        )
        
        parallel_table_processing = sfn.Map(
            self,
            "ParallelTableProcessing",
            items_path="$.table_discovery.table_discovery.enabled_tables",
            max_concurrency=4,
            parameters={
                "table_config.$": "$$.Map.Item.Value",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "force_full_sync.$": "$.force_full_sync",
                "execution_id.$": "$.execution_id"
            }
        ).iterator(table_processing_task)

        evaluate_results = sfn_tasks.LambdaInvoke(
            self,
            "EvaluateTableResults",
            lambda_function=placeholder_lambda,  # Will be updated with actual function
            payload=sfn.TaskInput.from_object({
                "tenant_id.$": "$.tenant_config.tenant_id",
                "job_id.$": "$.job_id",
                "table_results.$": "$.table_results"
            }),
            result_path="$.evaluation"
        )

        no_tables_found = sfn.Pass(
            self,
            "NoTablesFound",
            parameters={
                "tenant_id.$": "$.tenant_config.tenant_id",
                "status": "completed",
                "message": "No tables found for tenant",
                "table_count": 0
            }
        )

        tenant_definition = discover_tables.next(
            validate_discovery
            .when(
                sfn.Condition.is_present("$.table_discovery.table_discovery.enabled_tables"),
                parallel_table_processing.next(evaluate_results)
            )
            .otherwise(no_tables_found)
        )

        state_machines['tenant_processor'] = sfn.StateMachine(
            self,
            "TenantProcessorStateMachine",
            state_machine_name=f"TenantProcessor-{self.env_name}",
            definition=tenant_definition,
            role=self.step_functions_role,
            timeout=Duration.hours(4),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self,
                    "TenantProcessorLogGroup",
                    log_group_name=f"/aws/stepfunctions/OptimizedTenantProcessor-{self.env_name}-{self.timestamp}",
                    retention=logs.RetentionDays.ONE_MONTH
                ),
                level=sfn.LogLevel.ALL
            )
        )

        # Table Processor State Machine using CDK native constructs
        initialize_table = sfn_tasks.LambdaInvoke(
            self,
            "InitializeTableProcessing",
            lambda_function=placeholder_lambda,  # Will be updated with actual function
            payload=sfn.TaskInput.from_object({
                "table_config.$": "$.table_config",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "force_full_sync.$": "$.force_full_sync"
            }),
            result_path="$.table_processing_result"
        )

        validate_chunks = sfn.Choice(self, "ValidateChunkPlan")
        
        # Chunk processing task for iterator
        chunk_processing_task = sfn_tasks.LambdaInvoke(
            self,
            "ProcessChunk",
            lambda_function=placeholder_lambda,  # Will be updated with actual function
            payload=sfn.TaskInput.from_object({
                "chunk_config.$": "$$.Map.Item.Value",
                "table_config.$": "$.table_config",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "table_state.$": "$.table_processing_result.table_state"
            })
        )
        
        process_chunks = sfn.Map(
            self,
            "ProcessChunks",
            items_path="$.table_processing_result.chunk_plan.chunks",
            max_concurrency=3,
            parameters={
                "chunk_config.$": "$$.Map.Item.Value",
                "table_config.$": "$.table_config",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "table_state.$": "$.table_processing_result.table_state"
            }
        ).iterator(chunk_processing_task)

        evaluate_chunk_results = sfn_tasks.LambdaInvoke(
            self,
            "EvaluateChunkResults",
            lambda_function=placeholder_lambda,  # Will be updated with actual function
            payload=sfn.TaskInput.from_object({
                "job_id.$": "$.job_id",
                "table_name.$": "$.table_config.table_name",
                "tenant_id.$": "$.tenant_config.tenant_id",
                "chunk_results.$": "$.chunk_results"
            }),
            result_path="$.table_evaluation"
        )

        no_data_to_process = sfn.Pass(
            self,
            "NoDataToProcess",
            parameters={
                "table_name.$": "$.table_config.table_name",
                "tenant_id.$": "$.tenant_config.tenant_id",
                "status": "completed",
                "message": "No new data to process",
                "records_processed": 0
            }
        )

        table_definition = initialize_table.next(
            validate_chunks
            .when(
                sfn.Condition.number_greater_than("$.table_processing_result.chunk_plan.total_chunks", 0),
                process_chunks.next(evaluate_chunk_results)
            )
            .otherwise(no_data_to_process)
        )

        state_machines['table_processor'] = sfn.StateMachine(
            self,
            "TableProcessorStateMachine",
            state_machine_name=f"TableProcessor-{self.env_name}",
            definition=table_definition,
            role=self.step_functions_role,
            timeout=Duration.hours(2),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self,
                    "TableProcessorLogGroup",
                    log_group_name=f"/aws/stepfunctions/OptimizedTableProcessor-{self.env_name}-{self.timestamp}",
                    retention=logs.RetentionDays.ONE_MONTH
                ),
                level=sfn.LogLevel.ALL
            )
        )

        return state_machines

    def _create_dashboards(self):
        """Create CloudWatch dashboards for monitoring."""
        # Skip dashboard creation for now to avoid CDK issues
        # TODO: Fix dashboard creation with proper scope handling
        pass

    def _create_scheduled_rules(self):
        """Create EventBridge rules for scheduled execution."""
        # Optimized pipeline execution - every hour
        optimized_rule = events.Rule(
            self,
            "OptimizedPipelineSchedule",
            schedule=events.Schedule.rate(Duration.hours(1)),
            description="Trigger optimized pipeline execution every hour"
        )
        
        optimized_rule.add_target(
            targets.SfnStateMachine(
                self.state_machines['pipeline_orchestrator'],
                input=events.RuleTargetInput.from_object({
                    "source": "scheduled",
                    "force_full_sync": False
                })
            )
        )

        # Daily full sync - at 2 AM
        full_sync_rule = events.Rule(
            self,
            "OptimizedFullSyncSchedule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="2",
                day="*",
                month="*",
                year="*"
            ),
            description="Trigger full sync daily at 2 AM"
        )
        
        full_sync_rule.add_target(
            targets.SfnStateMachine(
                self.state_machines['pipeline_orchestrator'],
                input=events.RuleTargetInput.from_object({
                    "source": "scheduled",
                    "force_full_sync": True
                })
            )
        )