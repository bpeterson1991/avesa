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
        self.backfill_jobs_table = self._create_backfill_jobs_table()

        # Create IAM roles
        self.lambda_execution_role = self._create_lambda_execution_role()
        self.step_functions_role = self._create_step_functions_role()

        # Create Lambda functions (which also creates state machines)
        self.lambda_functions, self.state_machines = self._create_lambda_functions()

        # Create CloudWatch dashboards
        self._create_dashboards()

        # Create EventBridge rules for scheduling (after all dependencies are resolved)
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

    def _create_backfill_jobs_table(self) -> dynamodb.Table:
        """Create DynamoDB table for tracking backfill jobs."""
        table_name = f"BackfillJobs-{self.env_name}"
        
        table = dynamodb.Table(
            self,
            "BackfillJobsTable",
            table_name=table_name,
            partition_key=dynamodb.Attribute(
                name="job_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True
        )
        
        # Add GSI for tenant-based queries
        table.add_global_secondary_index(
            index_name="TenantServiceIndex",
            partition_key=dynamodb.Attribute(
                name="tenant_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="service_name",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
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
                        self.backfill_jobs_table.table_arn,
                        self.tenant_services_table.table_arn,
                        self.last_updated_table.table_arn,
                        # Include GSI ARNs
                        f"{self.processing_jobs_table.table_arn}/index/*",
                        f"{self.chunk_progress_table.table_arn}/index/*",
                        f"{self.backfill_jobs_table.table_arn}/index/*"
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
                # Step Functions permissions (for nested executions and discovery)
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                        "states:DescribeExecution",
                        "states:StopExecution",
                        "states:ListStateMachines"
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
                # Lambda invocation permissions - restrict to this environment
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "lambda:InvokeFunction"
                    ],
                    resources=[
                        f"arn:aws:lambda:{self.region}:{self.account}:function:avesa-*-{self.env_name}"
                    ]
                ),
                # Step Functions execution permissions - restrict to this environment
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                        "states:DescribeExecution",
                        "states:StopExecution"
                    ],
                    resources=[
                        f"arn:aws:states:{self.region}:{self.account}:stateMachine:*-{self.env_name}"
                    ]
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
        """Create all Lambda functions and state machines in correct order."""
        
        # Common environment variables
        common_env = {
            "BUCKET_NAME": self.data_bucket.bucket_name,
            "TENANT_SERVICES_TABLE": self.tenant_services_table.table_name,
            "LAST_UPDATED_TABLE": self.last_updated_table.table_name,
            "ENVIRONMENT": self.env_name,
            "PROCESSING_JOBS_TABLE": self.processing_jobs_table.table_name,
            "CHUNK_PROGRESS_TABLE": self.chunk_progress_table.table_name,
            "BACKFILL_JOBS_TABLE": self.backfill_jobs_table.table_name
        }

        # Step 1: Create ALL Lambda functions first (no state machine references)
        functions = self._create_all_lambda_functions_only(common_env)
        
        # Step 2: Create state machines using Lambda function references
        state_machines = self._create_all_state_machines(functions)
        
        return functions, state_machines

    def _create_all_lambda_functions_only(self, common_env: Dict[str, str]) -> Dict[str, _lambda.Function]:
        """Create all Lambda functions without any state machine dependencies."""
        functions = {}

        # Pipeline Orchestrator - STATE_MACHINE_ARN will be set after state machine creation
        orchestrator_env = common_env.copy()
        # Note: STATE_MACHINE_ARN will be added later via add_environment()
        
        functions['orchestrator'] = _lambda.Function(
            self,
            "PipelineOrchestratorLambda",
            function_name=f"avesa-pipeline-orchestrator-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/optimized/orchestrator/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.lambda_execution_role,
            memory_size=512,
            timeout=Duration.seconds(300),
            environment=orchestrator_env
        )

        # Tenant Processor
        functions['tenant_processor'] = _lambda.Function(
            self,
            "TenantProcessorLambda",
            function_name=f"avesa-tenant-processor-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="tenant_processor.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/optimized/processors/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.lambda_execution_role,
            memory_size=512,
            timeout=Duration.seconds(300),
            environment=common_env
        )

        # Table Processor
        functions['table_processor'] = _lambda.Function(
            self,
            "TableProcessorLambda",
            function_name=f"avesa-table-processor-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="table_processor.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/optimized/processors/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.lambda_execution_role,
            memory_size=512,
            timeout=Duration.seconds(300),
            environment=common_env
        )

        # AWS managed pandas layer (needed for chunk processor Parquet support)
        aws_pandas_layer_chunk = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "AWSPandasLayerForChunkProcessor",
            layer_version_arn="arn:aws:lambda:us-east-2:336392948345:layer:AWSSDKPandas-Python39:13"
        )

        # Chunk Processor
        functions['chunk_processor'] = _lambda.Function(
            self,
            "ChunkProcessorLambda",
            function_name=f"avesa-chunk-processor-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="chunk_processor.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/optimized/processors/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.lambda_execution_role,
            memory_size=1024,  # Higher memory for data processing
            timeout=Duration.seconds(180),  # 3 minutes
            environment=common_env,
            layers=[aws_pandas_layer_chunk]  # Add AWS Pandas layer for Parquet support
        )

        # Error Handler
        functions['error_handler'] = _lambda.Function(
            self,
            "ErrorHandlerLambda",
            function_name=f"avesa-error-handler-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="error_handler.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/optimized/helpers/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.lambda_execution_role,
            memory_size=256,
            timeout=Duration.seconds(60),
            environment=common_env
        )

        # Result Aggregator
        functions['result_aggregator'] = _lambda.Function(
            self,
            "ResultAggregatorLambda",
            function_name=f"avesa-result-aggregator-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="result_aggregator.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/optimized/helpers/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.lambda_execution_role,
            memory_size=512,
            timeout=Duration.seconds(300),
            environment=common_env
        )

        # Completion Notifier
        functions['completion_notifier'] = _lambda.Function(
            self,
            "CompletionNotifierLambda",
            function_name=f"avesa-completion-notifier-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="completion_notifier.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/optimized/helpers/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.lambda_execution_role,
            memory_size=256,
            timeout=Duration.seconds(60),
            environment=common_env
        )

        # Backfill Initiator
        functions['backfill_initiator'] = _lambda.Function(
            self,
            "BackfillInitiatorLambda",
            function_name=f"avesa-backfill-initiator-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="backfill_initiator.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/optimized/helpers/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.lambda_execution_role,
            memory_size=512,
            timeout=Duration.seconds(180),
            environment=common_env
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
                code=_lambda.Code.from_asset(
                    "../src",
                    bundling=BundlingOptions(
                        image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                        command=[
                            "bash", "-c",
                            "cp -r /asset-input/canonical_transform/* /asset-output/ && "
                            "cp -r /asset-input/shared /asset-output/"
                        ]
                    )
                ),
                role=self.lambda_execution_role,
                memory_size=1024,  # Keep at 1GB - should be sufficient
                timeout=Duration.seconds(300),  # 5 minutes timeout
                environment=canonical_env,
                layers=[aws_pandas_layer, clickhouse_layer]
            )

        return functions

    def _create_all_state_machines(self, functions: Dict[str, _lambda.Function]) -> Dict[str, sfn.StateMachine]:
        """Create independent state machines that discover each other at runtime."""
        state_machines = {}

        # Create Table Processor State Machine - Independent, only calls Lambda functions
        table_processor_definition = self._create_simple_table_processor_definition(functions)
        table_processor_state_machine = sfn.StateMachine(
            self,
            "TableProcessorStateMachine",
            state_machine_name=f"TableProcessor-{self.env_name}",
            definition_body=sfn.DefinitionBody.from_chainable(table_processor_definition),
            role=self.step_functions_role,
            timeout=Duration.hours(2),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self, "TableProcessorLogGroup",
                    log_group_name=f"/aws/stepfunctions/TableProcessor-{self.env_name}",
                    removal_policy=RemovalPolicy.DESTROY,
                    retention=logs.RetentionDays.ONE_MONTH
                ),
                level=sfn.LogLevel.ALL
            )
        )
        state_machines['table_processor'] = table_processor_state_machine

        # Create Tenant Processor State Machine - Independent, runtime discovery of TableProcessor
        tenant_processor_definition = self._create_simple_tenant_processor_definition(functions)
        tenant_processor_state_machine = sfn.StateMachine(
            self,
            "TenantProcessorStateMachine",
            state_machine_name=f"TenantProcessor-{self.env_name}",
            definition_body=sfn.DefinitionBody.from_chainable(tenant_processor_definition),
            role=self.step_functions_role,
            timeout=Duration.hours(4),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self, "TenantProcessorLogGroup",
                    log_group_name=f"/aws/stepfunctions/TenantProcessor-{self.env_name}",
                    removal_policy=RemovalPolicy.DESTROY,
                    retention=logs.RetentionDays.ONE_MONTH
                ),
                level=sfn.LogLevel.ALL
            )
        )
        state_machines['tenant_processor'] = tenant_processor_state_machine

        # Create Pipeline Orchestrator State Machine - Independent, runtime discovery
        orchestrator_task = sfn_tasks.LambdaInvoke(
            self, "CallPipelineOrchestrator",
            lambda_function=functions['orchestrator'],
            comment="Call enhanced pipeline orchestrator Lambda for workflow execution",
            result_path="$.orchestrator_result"
        )
        
        pipeline_orchestrator_state_machine = sfn.StateMachine(
            self,
            "PipelineOrchestratorStateMachine",
            state_machine_name=f"PipelineOrchestrator-{self.env_name}",
            definition_body=sfn.DefinitionBody.from_chainable(orchestrator_task),
            role=self.step_functions_role,
            timeout=Duration.hours(6),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self, "PipelineOrchestratorLogGroup",
                    log_group_name=f"/aws/stepfunctions/PipelineOrchestrator-{self.env_name}",
                    removal_policy=RemovalPolicy.DESTROY,
                    retention=logs.RetentionDays.ONE_MONTH
                ),
                level=sfn.LogLevel.ALL
            )
        )
        state_machines['pipeline_orchestrator'] = pipeline_orchestrator_state_machine
        
        return state_machines

    def _create_simple_tenant_processor_definition(self, functions: Dict[str, _lambda.Function]) -> sfn.Chain:
        """Create simplified tenant processor that discovers TableProcessor at runtime."""
        
        # Get Lambda function references
        tenant_processor_lambda = functions['tenant_processor']
        
        # Single Lambda task that handles full tenant processing workflow
        # This Lambda will discover and invoke TableProcessor state machine at runtime
        tenant_processing_task = sfn_tasks.LambdaInvoke(
            self, "ProcessTenantWithDiscovery",
            lambda_function=tenant_processor_lambda,
            comment="Process tenant with runtime discovery of TableProcessor state machine",
            result_path="$.tenant_result"
        ).add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(5),
            max_attempts=3,
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "TenantProcessingFailed",
                cause="Tenant processing failed",
                error="TenantProcessingFailure"
            ),
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        return tenant_processing_task

    def _create_simple_table_processor_definition(self, functions: Dict[str, _lambda.Function]) -> sfn.Chain:
        """Create simplified table processor that only calls Lambda functions."""
        
        # Get Lambda function references
        table_processor_lambda = functions['table_processor']
        
        # Single Lambda task that handles full table processing workflow
        table_processing_task = sfn_tasks.LambdaInvoke(
            self, "ProcessTableWithChunks",
            lambda_function=table_processor_lambda,
            comment="Process table with integrated chunk processing logic",
            result_path="$.table_result"
        ).add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(5),
            max_attempts=3,
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "TableProcessingFailed",
                cause="Table processing failed",
                error="TableProcessingFailure"
            ),
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        return table_processing_task

    def _create_tenant_processor_definition(self, functions: Dict[str, _lambda.Function], table_processor_state_machine: sfn.StateMachine) -> sfn.Chain:
        """Create the tenant processor state machine definition using CDK constructs."""
        
        # Get Lambda function references
        tenant_processor_lambda = functions['tenant_processor']
        result_aggregator_lambda = functions['result_aggregator']
        error_handler_lambda = functions['error_handler']
        
        # Discover Tenant Tables - matches the JSON exactly
        discover_tenant_tables = sfn_tasks.LambdaInvoke(
            self, "DiscoverTenantTables",
            lambda_function=tenant_processor_lambda,
            payload=sfn.TaskInput.from_object({
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "table_name.$": "$.table_name",
                "force_full_sync.$": "$.force_full_sync"
            }),
            result_path="$.table_discovery"
        ).add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(5),
            max_attempts=3,
            backoff_rate=2.0
        ).add_catch(
            sfn_tasks.LambdaInvoke(
                self, "TenantDiscoveryFailedHandler",
                lambda_function=error_handler_lambda,
                payload=sfn.TaskInput.from_object({
                    "error_type": "tenant_discovery_failure",
                    "tenant_id.$": "$.tenant_config.tenant_id",
                    "job_id.$": "$.job_id",
                    "error_details.$": "$.error"
                })
            ).next(sfn.Fail(self, "TenantDiscoveryFailed",
                cause="Tenant processing failed",
                error="TenantProcessingFailure"
            )),
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        # Validate Table Discovery
        validate_table_discovery = sfn.Choice(self, "ValidateTableDiscovery")
        
        # Check Table Count
        check_table_count = sfn.Choice(self, "CheckTableCount")
        
        # Parallel Table Processing Map
        parallel_table_processing = sfn.Map(
            self, "ParallelTableProcessing",
            items_path="$.table_discovery.table_discovery.enabled_tables",
            max_concurrency=4,
            parameters={
                "table_config.$": "$$.Map.Item.Value",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "force_full_sync.$": "$.force_full_sync",
                "execution_id.$": "$.execution_id"
            },
            result_path="$.table_results"
        )
        
        # Process Table iterator - calls table processor state machine
        process_table = sfn_tasks.StepFunctionsStartExecution(
            self, "ProcessTable",
            state_machine=table_processor_state_machine,
            input=sfn.TaskInput.from_object({
                "table_config.$": "$.table_config",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "force_full_sync.$": "$.force_full_sync",
                "execution_id.$": "$.execution_id"
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB
        ).add_retry(
            errors=["States.ExecutionLimitExceeded"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2.0
        ).add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(15),
            max_attempts=2,
            backoff_rate=2.0
        ).add_catch(
            sfn.Pass(self, "TableProcessingFailedHandler",
                parameters={
                    "table_name.$": "$.table_config.table_name",
                    "tenant_id.$": "$.tenant_config.tenant_id",
                    "status": "failed",
                    "error.$": "$.error"
                }
            ),
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        # Set the iterator for the map
        parallel_table_processing.iterator(process_table)
        
        # Evaluate Table Results
        evaluate_table_results = sfn_tasks.LambdaInvoke(
            self, "EvaluateTableResults",
            lambda_function=result_aggregator_lambda,
            payload=sfn.TaskInput.from_object({
                "tenant_id.$": "$.tenant_config.tenant_id",
                "job_id.$": "$.job_id",
                "table_results.$": "$.table_results"
            }),
            result_path="$.evaluation"
        ).add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(5),
            max_attempts=2,
            backoff_rate=2.0
        )
        
        # Completion states
        tenant_processing_complete = sfn.Pass(
            self, "TenantProcessingComplete",
            parameters={
                "tenant_id.$": "$.tenant_config.tenant_id",
                "status": "completed",
                "table_results.$": "$.table_results",
                "evaluation.$": "$.evaluation"
            }
        )
        
        no_tables_found = sfn.Pass(
            self, "NoTablesFound",
            parameters={
                "tenant_id.$": "$.tenant_config.tenant_id",
                "status": "completed",
                "message": "No tables found for tenant",
                "table_count": 0
            }
        )
        
        no_tables_enabled = sfn.Pass(
            self, "NoTablesEnabled",
            parameters={
                "tenant_id.$": "$.tenant_config.tenant_id",
                "status": "completed",
                "message": "No tables enabled for processing",
                "table_count": 0
            }
        )
        
        # Build the workflow chain exactly like the JSON
        definition = discover_tenant_tables.next(
            validate_table_discovery
                .when(
                    sfn.Condition.is_present("$.table_discovery.table_discovery.enabled_tables"),
                    check_table_count
                        .when(
                            sfn.Condition.number_greater_than("$.table_discovery.table_discovery.table_count", 0),
                            parallel_table_processing.next(evaluate_table_results).next(tenant_processing_complete)
                        )
                        .otherwise(no_tables_enabled)
                )
                .otherwise(no_tables_found)
        )
        
        return definition

    def _create_table_processor_definition(self, functions: Dict[str, _lambda.Function]) -> sfn.Chain:
        """Create the table processor state machine definition using CDK constructs."""
        
        # Get Lambda function references
        table_processor_lambda = functions['table_processor']
        chunk_processor_lambda = functions['chunk_processor']
        result_aggregator_lambda = functions['result_aggregator']
        error_handler_lambda = functions['error_handler']
        
        # Initialize Table Processing - matches JSON exactly
        initialize_table_processing = sfn_tasks.LambdaInvoke(
            self, "InitializeTableProcessing",
            lambda_function=table_processor_lambda,
            payload=sfn.TaskInput.from_object({
                "table_config.$": "$.table_config",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "force_full_sync.$": "$.force_full_sync"
            }),
            result_path="$.table_processing_result"
        ).add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(5),
            max_attempts=3,
            backoff_rate=2.0
        ).add_catch(
            sfn_tasks.LambdaInvoke(
                self, "TableInitializationFailedHandler",
                lambda_function=error_handler_lambda,
                payload=sfn.TaskInput.from_object({
                    "error_type": "table_initialization_failure",
                    "table_name.$": "$.table_config.table_name",
                    "tenant_id.$": "$.tenant_config.tenant_id",
                    "job_id.$": "$.job_id",
                    "error_details.$": "$.error"
                })
            ).next(sfn.Fail(self, "TableInitializationFailed",
                cause="Table processing failed",
                error="TableProcessingFailure"
            )),
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        # Validate Chunk Plan
        validate_chunk_plan = sfn.Choice(self, "ValidateChunkPlan")
        
        # Process Chunks Map - parallel processing
        process_chunks = sfn.Map(
            self, "ProcessChunks",
            items_path="$.table_processing_result.chunk_plan.chunks",
            max_concurrency=3,
            parameters={
                "chunk_config.$": "$$.Map.Item.Value",
                "table_config.$": "$.table_config",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "table_state.$": "$.table_processing_result.table_state"
            },
            result_path="$.chunk_results"
        )
        
        # Process Chunk - individual chunk processing
        process_chunk = sfn_tasks.LambdaInvoke(
            self, "ProcessChunk",
            lambda_function=chunk_processor_lambda,
            payload=sfn.TaskInput.from_object({
                "chunk_config.$": "$.chunk_config",
                "table_config.$": "$.table_config",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id"
            }),
            timeout=Duration.seconds(180)
        ).add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2.0
        ).add_retry(
            errors=["Lambda.TooManyRequestsException"],
            interval=Duration.seconds(60),
            max_attempts=5,
            backoff_rate=2.0
        )
        
        # Handle Chunk Timeout
        handle_chunk_timeout = sfn.Pass(
            self, "HandleChunkTimeout",
            parameters={
                "job_id.$": "$.job_id",
                "chunk_config.$": "$.chunk_config",
                "timeout_error.$": "$.timeout_error",
                "status": "timeout_handled"
            }
        )
        
        # Schedule Chunk Resumption - For now we'll use a Pass state
        # TODO: Implement proper chunk resumption logic
        schedule_chunk_resumption = sfn.Pass(
            self, "ScheduleChunkResumption",
            parameters={
                "table_config.$": "$.table_config",
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "resume_chunk_id.$": "$.chunk_config.chunk_id",
                "status": "resumption_scheduled"
            }
        )
        
        # Update Chunk Progress
        update_chunk_progress = sfn.Pass(
            self, "UpdateChunkProgress",
            parameters={
                "job_id.$": "$.job_id",
                "chunk_id.$": "$.chunk_config.chunk_id",
                "status": "completed",
                "result.$": "$"
            }
        )
        
        # Chunk Processing Failed - using progress tracker (will use a placeholder function)
        chunk_processing_failed = sfn.Pass(
            self, "ChunkProcessingFailed",
            parameters={
                "job_id.$": "$.job_id",
                "chunk_id.$": "$.chunk_config.chunk_id",
                "status": "failed",
                "error.$": "$.error"
            }
        )
        
        # Add catch handlers to process_chunk
        process_chunk.add_catch(
            handle_chunk_timeout.next(schedule_chunk_resumption),
            errors=["States.Timeout"],
            result_path="$.timeout_error"
        ).add_catch(
            chunk_processing_failed,
            errors=["States.ALL"],
            result_path="$.error"
        ).next(update_chunk_progress)
        
        # Set up the map iterator
        process_chunks.iterator(process_chunk)
        
        # Evaluate Chunk Results
        evaluate_chunk_results = sfn_tasks.LambdaInvoke(
            self, "EvaluateChunkResults",
            lambda_function=result_aggregator_lambda,
            payload=sfn.TaskInput.from_object({
                "job_id.$": "$.job_id",
                "table_name.$": "$.table_config.table_name",
                "tenant_id.$": "$.tenant_config.tenant_id",
                "chunk_results.$": "$.chunk_results"
            }),
            result_path="$.table_evaluation"
        ).add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(5),
            max_attempts=2,
            backoff_rate=2.0
        )
        
        # Completion states
        table_processing_complete = sfn.Pass(
            self, "TableProcessingComplete",
            parameters={
                "table_name.$": "$.table_config.table_name",
                "tenant_id.$": "$.tenant_config.tenant_id",
                "status": "completed",
                "chunk_results.$": "$.chunk_results",
                "evaluation.$": "$.table_evaluation"
            }
        )
        
        no_data_to_process = sfn.Pass(
            self, "NoDataToProcess",
            parameters={
                "table_name.$": "$.table_config.table_name",
                "tenant_id.$": "$.tenant_config.tenant_id",
                "status": "completed",
                "message": "No new data to process",
                "records_processed": 0
            }
        )
        
        # Build the workflow chain exactly like the JSON
        definition = initialize_table_processing.next(
            validate_chunk_plan
                .when(
                    sfn.Condition.number_greater_than("$.table_processing_result.chunk_plan.total_chunks", 0),
                    process_chunks.next(evaluate_chunk_results).next(table_processing_complete)
                )
                .otherwise(no_data_to_process)
        )
        
        return definition

    def _create_pipeline_orchestrator_state_machine(
        self,
        result_aggregator_lambda: _lambda.Function,
        completion_notifier_lambda: _lambda.Function,
        error_handler_lambda: _lambda.Function,
        tenant_processor_state_machine: sfn.StateMachine,
        table_processor_state_machine: sfn.StateMachine
    ) -> sfn.StateMachine:
        """Create the pipeline orchestrator state machine using CDK constructs."""
        
        # Initialize Pipeline step (Pass state from JSON)
        initialize_pipeline = sfn.Pass(
            self, "InitializePipeline",
            comment="Pipeline already initialized by orchestrator Lambda",
            result=sfn.Result.from_string("Pipeline initialized"),
            result_path="$.initialization_status"
        )
        
        # Multi-tenant processing - Map state for processing multiple tenants
        process_tenant_task = sfn_tasks.StepFunctionsStartExecution(
            self, "ProcessTenant",
            state_machine=tenant_processor_state_machine,
            input=sfn.TaskInput.from_object({
                "tenant_config.$": "$.tenant_config",
                "job_id.$": "$.job_id",
                "table_name.$": "$.table_name",
                "force_full_sync.$": "$.force_full_sync",
                "execution_id.$": "$.execution_id"
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB
        ).add_retry(
            max_attempts=5,
            interval=Duration.seconds(60),
            backoff_rate=2.0,
            errors=["States.ExecutionLimitExceeded"]
        ).add_catch(
            sfn.Pass(self, "TenantProcessingFailed",
                    parameters={
                        "tenant_id.$": "$.tenant_config.tenant_id",
                        "status": "failed",
                        "error.$": "$.error"
                    }),
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        multi_tenant_processing = sfn.Map(
            self, "MultiTenantProcessing",
            items_path="$.tenants",
            max_concurrency=10,
            parameters={
                "tenant_config.$": "$$.Map.Item.Value",
                "job_id.$": "$.job_id",
                "table_name.$": "$.table_name",
                "force_full_sync.$": "$.force_full_sync",
                "execution_id.$": "$$.Execution.Name"
            },
            result_path="$.tenant_results"
        ).iterator(process_tenant_task)
        
        # Single-tenant processing - Direct task execution
        single_tenant_processing = sfn_tasks.StepFunctionsStartExecution(
            self, "SingleTenantProcessing",
            state_machine=tenant_processor_state_machine,
            input=sfn.TaskInput.from_object({
                "tenant_config.$": "$.tenants[0]",
                "job_id.$": "$.job_id",
                "table_name.$": "$.table_name",
                "force_full_sync.$": "$.force_full_sync",
                "execution_id.$": "$$.Execution.Name"
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.tenant_result"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(60),
            backoff_rate=2.0,
            errors=["States.ExecutionLimitExceeded"]
        ).add_catch(
            sfn_tasks.LambdaInvoke(
                self, "HandleSingleTenantFailure",
                lambda_function=error_handler_lambda,
                payload=sfn.TaskInput.from_object({
                    "error_type": "single_tenant_failure",
                    "error_details.$": "$.error",
                    "tenant_config.$": "$.tenants[0]"
                })
            ).next(
                sfn.Fail(self, "PipelineFailure",
                        cause="Pipeline execution failed",
                        error="PipelineExecutionFailure")
            ),
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        # Determine processing mode choice
        determine_processing_mode = sfn.Choice(self, "DetermineProcessingMode")
        determine_processing_mode.when(
            sfn.Condition.string_equals("$.mode", "multi-tenant"),
            multi_tenant_processing.next(
                sfn_tasks.LambdaInvoke(
                    self, "AggregateMultiTenantResults",
                    lambda_function=result_aggregator_lambda,
                    payload=sfn.TaskInput.from_object({
                        "job_id.$": "$.job_id",
                        "tenant_results.$": "$.tenant_results",
                        "processing_mode": "multi-tenant"
                    }),
                    result_path="$.aggregation_result"
                ).add_retry(
                    max_attempts=2,
                    interval=Duration.seconds(5),
                    backoff_rate=2.0,
                    errors=["States.TaskFailed"]
                )
            )
        ).when(
            sfn.Condition.string_equals("$.mode", "single-tenant"),
            single_tenant_processing.next(
                sfn_tasks.LambdaInvoke(
                    self, "AggregateSingleTenantResults",
                    lambda_function=result_aggregator_lambda,
                    payload=sfn.TaskInput.from_object({
                        "job_id.$": "$.job_id",
                        "tenant_result.$": "$.tenant_result",
                        "processing_mode": "single-tenant"
                    }),
                    result_path="$.aggregation_result"
                ).add_retry(
                    max_attempts=2,
                    interval=Duration.seconds(5),
                    backoff_rate=2.0,
                    errors=["States.TaskFailed"]
                )
            )
        ).otherwise(
            sfn.Fail(self, "HandleInvalidMode",
                    cause="Invalid processing mode specified",
                    error="InvalidProcessingMode")
        )
        
        # Notify completion step
        notify_completion = sfn_tasks.LambdaInvoke(
            self, "NotifyCompletion",
            lambda_function=completion_notifier_lambda,
            payload=sfn.TaskInput.from_object({
                "job_id.$": "$.job_id",
                "results.$": "$",
                "execution_arn.$": "$$.Execution.Name"
            })
        ).add_retry(
            max_attempts=2,
            interval=Duration.seconds(5),
            backoff_rate=2.0,
            errors=["States.TaskFailed"]
        )
        
        # Define the main workflow chain
        definition = initialize_pipeline.next(
            determine_processing_mode.afterwards().next(notify_completion)
        )
        
        # Create the state machine
        pipeline_orchestrator_state_machine = sfn.StateMachine(
            self,
            "PipelineOrchestratorStateMachine",
            state_machine_name=f"PipelineOrchestrator-{self.env_name}",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            role=self.step_functions_role,
            timeout=Duration.hours(6),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self, "PipelineOrchestratorLogGroup",
                    log_group_name=f"/aws/stepfunctions/PipelineOrchestrator-{self.env_name}",
                    removal_policy=RemovalPolicy.DESTROY,
                    retention=logs.RetentionDays.ONE_MONTH
                ),
                level=sfn.LogLevel.ALL
            )
        )
        
        return pipeline_orchestrator_state_machine

    def _create_dashboards(self):
        """Create CloudWatch dashboards for monitoring."""
        # Skip dashboard creation for now to avoid CDK issues
        # TODO: Fix dashboard creation with proper scope handling
        pass

    def _create_scheduled_rules(self):
        """Create EventBridge rules for scheduled execution."""
        # Skip scheduled rules for now to avoid circular dependencies
        # TODO: Create EventBridge rules after deployment using AWS CLI or separate stack
        # The pipeline can be triggered manually via Lambda console or API
        pass