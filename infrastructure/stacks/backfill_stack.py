"""
Backfill infrastructure stack for handling historical data ingestion
when tenants connect new services for the first time.
"""

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    BundlingOptions,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks
)
from constructs import Construct
from typing import Dict, Any


class BackfillStack(Stack):
    """Stack for backfill infrastructure and orchestration."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        data_bucket_name: str,
        tenant_services_table_name: str,
        lambda_memory: int = 1024,
        lambda_timeout: int = 180,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = environment
        self.data_bucket_name = data_bucket_name
        self.tenant_services_table_name = tenant_services_table_name

        # Create DynamoDB table for backfill job tracking
        self.backfill_jobs_table = self._create_backfill_jobs_table()

        # Create IAM role for backfill Lambda
        self.backfill_lambda_role = self._create_backfill_lambda_role()

        # Create backfill Lambda function
        self.backfill_lambda = self._create_backfill_lambda(
            memory=lambda_memory,
            timeout=lambda_timeout
        )

        # Create Lambda for backfill initiation
        self.backfill_initiator_lambda = self._create_backfill_initiator_lambda()

        # Create Step Functions for backfill orchestration
        self.backfill_state_machine = self._create_backfill_state_machine()

    def _create_backfill_jobs_table(self) -> dynamodb.Table:
        """Create DynamoDB table for tracking backfill jobs."""
        table_name = "BackfillJobs" if self.env_name == "prod" else f"BackfillJobs-{self.env_name}"
        
        table = dynamodb.Table(
            self,
            "BackfillJobsTable",
            table_name=table_name,
            partition_key=dynamodb.Attribute(
                name="job_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,  # Keep data safe in production
            point_in_time_recovery=True
        )
        return table

    def _create_backfill_lambda_role(self) -> iam.Role:
        """Create IAM role for backfill Lambda functions."""
        role = iam.Role(
            self,
            "BackfillLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )

        # Add custom policy for backfill operations
        policy = iam.Policy(
            self,
            "BackfillPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:Scan",
                        "dynamodb:Query"
                    ],
                    resources=[
                        self.backfill_jobs_table.table_arn,
                        f"{self.backfill_jobs_table.table_arn}/index/*",
                        f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.tenant_services_table_name}",
                        f"arn:aws:dynamodb:{self.region}:{self.account}:table/LastUpdated" if self.env_name == "prod" else f"arn:aws:dynamodb:{self.region}:{self.account}:table/LastUpdated-{self.env_name}"
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "secretsmanager:GetSecretValue"
                    ],
                    resources=["*"]
                ),
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
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "lambda:InvokeFunction"
                    ],
                    resources=[
                        f"arn:aws:lambda:{self.region}:{self.account}:function:avesa-*-{self.env_name}",
                        f"arn:aws:lambda:{self.region}:{self.account}:function:clickhouse-*-{self.env_name}"
                    ]
                ),
                # Note: State machine permissions will be granted after creation to avoid circular dependency
            ]
        )
        role.attach_inline_policy(policy)
        return role

    def _create_backfill_lambda(self, memory: int, timeout: int) -> _lambda.Function:
        """Create Lambda function for backfill processing."""
        # Note: Original backfill implementation has been archived
        # This creates a placeholder function that references the archived code
        # TODO: Implement optimized backfill using the new architecture
        function = _lambda.Function(
            self,
            "BackfillLambda",
            function_name=f"avesa-backfill-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/backfill/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.backfill_lambda_role,
            memory_size=memory,
            timeout=Duration.seconds(timeout),
            environment={
                "BUCKET_NAME": self.data_bucket_name,
                "TENANT_SERVICES_TABLE": self.tenant_services_table_name,
                "LAST_UPDATED_TABLE": "LastUpdated" if self.env_name == "prod" else f"LastUpdated-{self.env_name}",
                "BACKFILL_JOBS_TABLE": self.backfill_jobs_table.table_name,
                "ENVIRONMENT": self.env_name
            },
            log_retention=logs.RetentionDays.ONE_MONTH
            # Note: Reserved concurrency removed due to account limits
        )
        return function

    def _create_backfill_initiator_lambda(self) -> _lambda.Function:
        """Create Lambda function for initiating backfills."""
        # Note: Original backfill implementation has been archived
        # This creates a placeholder function that references the archived code
        # TODO: Implement optimized backfill using the new architecture
        function = _lambda.Function(
            self,
            "BackfillInitiatorLambda",
            function_name=f"avesa-backfill-initiator-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="initiator.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/backfill/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.backfill_lambda_role,
            memory_size=512,
            timeout=Duration.seconds(180),
            environment={
                "BUCKET_NAME": self.data_bucket_name,
                "TENANT_SERVICES_TABLE": self.tenant_services_table_name,
                "BACKFILL_JOBS_TABLE": self.backfill_jobs_table.table_name,
                "BACKFILL_STATE_MACHINE_ARN": "",  # Will be updated after state machine creation
                "ENVIRONMENT": self.env_name
            },
            log_retention=logs.RetentionDays.ONE_MONTH
        )
        return function

    def _create_backfill_state_machine(self) -> sfn.StateMachine:
        """Create Step Functions state machine for backfill orchestration."""
        
        # Define the backfill task
        backfill_task = tasks.LambdaInvoke(
            self,
            "BackfillTask",
            lambda_function=self.backfill_lambda,
            payload=sfn.TaskInput.from_object({
                "tenant_id.$": "$.tenant_id",
                "service.$": "$.service",
                "table_name.$": "$.table_name",
                "start_date.$": "$.start_date",
                "end_date.$": "$.end_date",
                "chunk_size_days.$": "$.chunk_size_days",
                "resume_job_id.$": "$.resume_job_id"
            }),
            result_path="$.backfill_result"
        )

        # Define wait state for continuation
        wait_state = sfn.Wait(
            self,
            "WaitForContinuation",
            time=sfn.WaitTime.duration(Duration.seconds(30))
        )

        # Define choice state to check if backfill is complete
        choice_state = sfn.Choice(self, "CheckBackfillStatus")
        
        # Define success state
        success_state = sfn.Succeed(
            self,
            "BackfillCompleted",
            comment="Backfill completed successfully"
        )

        # Define failure state
        failure_state = sfn.Fail(
            self,
            "BackfillFailed",
            comment="Backfill failed"
        )

        # Build the state machine definition
        definition = backfill_task.next(
            choice_state
            .when(
                sfn.Condition.string_equals("$.backfill_result.Payload.body.status", "completed"),
                success_state
            )
            .when(
                sfn.Condition.string_equals("$.backfill_result.Payload.body.status", "completed_with_errors"),
                success_state
            )
            .when(
                sfn.Condition.string_equals("$.backfill_result.Payload.statusCode", "500"),
                failure_state
            )
            .otherwise(
                wait_state.next(backfill_task)
            )
        )

        # Create the state machine
        state_machine = sfn.StateMachine(
            self,
            "BackfillOrchestrator",
            state_machine_name=f"BackfillOrchestrator-{self.env_name}",
            definition=definition,
            timeout=Duration.hours(24)  # Max 24 hours for backfill
        )

        # Note: Permissions will be granted after stack creation to avoid circular dependency
        return state_machine

    def get_backfill_lambda_arn(self) -> str:
        """Get the ARN of the backfill Lambda function."""
        return self.backfill_lambda.function_arn

    def get_backfill_initiator_lambda_arn(self) -> str:
        """Get the ARN of the backfill initiator Lambda function."""
        return self.backfill_initiator_lambda.function_arn

    def get_backfill_jobs_table_name(self) -> str:
        """Get the name of the backfill jobs table."""
        return self.backfill_jobs_table.table_name