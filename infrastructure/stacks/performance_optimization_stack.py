"""
Performance Optimization Stack

CDK stack for the optimized AVESA data pipeline infrastructure including
DynamoDB tables, Lambda functions, Step Functions, and monitoring.
"""

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
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

        # Create or import base DynamoDB tables
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

    def _create_processing_jobs_table(self) -> dynamodb.Table:
        """Import existing DynamoDB table for tracking processing jobs."""
        table_name = f"ProcessingJobs-{self.env_name}"
        
        # Import existing table instead of creating new one
        table = dynamodb.Table.from_table_name(
            self,
            "ProcessingJobsTable",
            table_name=table_name
        )
        
        return table

    def _create_chunk_progress_table(self) -> dynamodb.Table:
        """Import existing DynamoDB table for tracking chunk processing progress."""
        table_name = f"ChunkProgress-{self.env_name}"
        
        # Import existing table instead of creating new one
        table = dynamodb.Table.from_table_name(
            self,
            "ChunkProgressTable",
            table_name=table_name
        )
        
        return table

    def _create_tenant_services_table(self) -> dynamodb.Table:
        """
        Import existing DynamoDB table for tenant service configuration.
        
        Complete Schema:
        - Partition Key: tenant_id (STRING) - Identifies the tenant organization
        - Sort Key: service (STRING) - Identifies the specific service (e.g., 'connectwise', 'servicenow')
        - Additional Attributes:
          * enabled (BOOLEAN) - Whether the service is enabled for this tenant
          * secret_name (STRING) - AWS Secrets Manager secret name for service credentials
          * updated_at (STRING) - ISO timestamp of last configuration update
          * created_at (STRING) - ISO timestamp of initial configuration creation
        
        This table stores complete configuration for which services are enabled for each tenant,
        including service status, credential references, and audit timestamps.
        """
        table_name = self.tenant_services_table_name
        
        # Import existing table instead of creating new one
        table = dynamodb.Table.from_table_name(
            self,
            "TenantServicesTable",
            table_name=table_name
        )
        
        return table

    def _create_last_updated_table(self) -> dynamodb.Table:
        """Import existing DynamoDB table for tracking last updated timestamps."""
        table_name = self.last_updated_table_name
        
        # Import existing table instead of creating new one
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
                        f"arn:aws:s3:::{self.data_bucket_name}",
                        f"arn:aws:s3:::{self.data_bucket_name}/*"
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
                        f"arn:aws:lambda:{self.region}:{self.account}:function:avesa-*-{self.env_name}"
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
            "BUCKET_NAME": self.data_bucket_name,
            "TENANT_SERVICES_TABLE": self.tenant_services_table.table_name,
            "LAST_UPDATED_TABLE": self.last_updated_table.table_name,
            "ENVIRONMENT": self.env_name,
            "PROCESSING_JOBS_TABLE": self.processing_jobs_table.table_name,
            "CHUNK_PROGRESS_TABLE": self.chunk_progress_table.table_name
        }

        # Create state machines first so we can reference them
        state_machines = self._create_state_machines_early()
        
        # Pipeline Orchestrator with STATE_MACHINE_ARN
        orchestrator_env = common_env.copy()
        orchestrator_env["STATE_MACHINE_ARN"] = state_machines['pipeline_orchestrator'].state_machine_arn
        
        functions['orchestrator'] = _lambda.Function(
            self,
            "PipelineOrchestratorLambda",
            function_name=f"avesa-pipeline-orchestrator-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../lambda-packages/optimized-orchestrator.zip"),
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
            code=_lambda.Code.from_asset("../lambda-packages/optimized-processors.zip"),
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
            code=_lambda.Code.from_asset("../lambda-packages/optimized-processors.zip"),
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
            code=_lambda.Code.from_asset("../lambda-packages/optimized-processors.zip"),
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
            code=_lambda.Code.from_asset("../lambda-packages/optimized-processors.zip"),
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
            code=_lambda.Code.from_asset("../lambda-packages/optimized-processors.zip"),
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
            code=_lambda.Code.from_asset("../lambda-packages/optimized-processors.zip"),
            role=self.lambda_execution_role,
            memory_size=256,
            timeout=Duration.seconds(60),
            environment=common_env,
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        return functions, state_machines

    def _create_state_machines_early(self) -> Dict[str, sfn.StateMachine]:
        """Create Step Functions state machines early so Lambda can reference them."""
        state_machines = {}

        # Load state machine definitions
        state_machine_dir = "../src/optimized/state_machines"
        
        # Pipeline Orchestrator State Machine
        with open(f"{state_machine_dir}/pipeline_orchestrator.json", 'r') as f:
            orchestrator_definition = json.load(f)
        
        # Replace placeholders in the definition
        orchestrator_definition_str = json.dumps(orchestrator_definition)
        orchestrator_definition_str = orchestrator_definition_str.replace(
            "${AWS::Region}", self.region
        ).replace(
            "${AWS::AccountId}", self.account
        ).replace(
            "${Environment}", self.env_name
        )
        orchestrator_definition = json.loads(orchestrator_definition_str)

        state_machines['pipeline_orchestrator'] = sfn.StateMachine(
            self,
            "PipelineOrchestratorStateMachine",
            state_machine_name=f"PipelineOrchestrator-{self.env_name}",
            definition_body=sfn.DefinitionBody.from_string(
                json.dumps(orchestrator_definition)
            ),
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

        # Tenant Processor State Machine
        with open(f"{state_machine_dir}/tenant_processor.json", 'r') as f:
            tenant_definition = json.load(f)
        
        tenant_definition_str = json.dumps(tenant_definition)
        tenant_definition_str = tenant_definition_str.replace(
            "${AWS::Region}", self.region
        ).replace(
            "${AWS::AccountId}", self.account
        ).replace(
            "${Environment}", self.env_name
        )
        tenant_definition = json.loads(tenant_definition_str)

        state_machines['tenant_processor'] = sfn.StateMachine(
            self,
            "TenantProcessorStateMachine",
            state_machine_name=f"TenantProcessor-{self.env_name}",
            definition_body=sfn.DefinitionBody.from_string(
                json.dumps(tenant_definition)
            ),
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

        # Table Processor State Machine
        with open(f"{state_machine_dir}/table_processor.json", 'r') as f:
            table_definition = json.load(f)
        
        table_definition_str = json.dumps(table_definition)
        table_definition_str = table_definition_str.replace(
            "${AWS::Region}", self.region
        ).replace(
            "${AWS::AccountId}", self.account
        ).replace(
            "${Environment}", self.env_name
        )
        table_definition = json.loads(table_definition_str)

        state_machines['table_processor'] = sfn.StateMachine(
            self,
            "TableProcessorStateMachine",
            state_machine_name=f"TableProcessor-{self.env_name}",
            definition_body=sfn.DefinitionBody.from_string(
                json.dumps(table_definition)
            ),
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