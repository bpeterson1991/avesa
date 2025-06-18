"""
Main data pipeline stack containing Lambda functions, DynamoDB tables,
S3 bucket, and IAM roles for the AVESA multi-tenant data pipeline.

Supports 30+ integration services (ConnectWise, ServiceNow, Salesforce, etc.)
with multi-tenant architecture where each service has dedicated lambda functions.
"""

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs
)
from constructs import Construct
from typing import Dict, Any


class DataPipelineStack(Stack):
    """Main stack for the AVESA multi-tenant data pipeline infrastructure."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        bucket_name: str,
        lambda_memory: int = 512,
        lambda_timeout: int = 300,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.bucket_name = bucket_name

        # Create S3 bucket for data storage
        self.data_bucket = self._create_data_bucket()

        # Create DynamoDB tables
        self.tenant_services_table = self._create_tenant_services_table()
        self.last_updated_table = self._create_last_updated_table()

        # Create IAM role for Lambda functions
        self.lambda_role = self._create_lambda_role()

        # Create Lambda functions
        self.raw_ingestion_lambda = self._create_raw_ingestion_lambda(
            memory=lambda_memory,
            timeout=lambda_timeout
        )
        self.canonical_transform_lambda = self._create_canonical_transform_lambda(
            memory=lambda_memory,
            timeout=lambda_timeout
        )

        # Create EventBridge rules for scheduling
        self._create_scheduled_rules()

    def _create_data_bucket(self) -> s3.Bucket:
        """Create S3 bucket for data storage with proper lifecycle policies."""
        bucket = s3.Bucket(
            self,
            "DataBucket",
            bucket_name=self.bucket_name,
            versioning=s3.BucketVersioning.ENABLED,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="raw-data-lifecycle",
                    enabled=True,
                    prefix="*/raw/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30)
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90)
                        )
                    ]
                ),
                s3.LifecycleRule(
                    id="canonical-data-lifecycle",
                    enabled=True,
                    prefix="*/canonical/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(90)
                        )
                    ]
                )
            ]
        )
        return bucket

    def _create_tenant_services_table(self) -> dynamodb.Table:
        """Create DynamoDB table for tenant service configuration."""
        table = dynamodb.Table(
            self,
            "TenantServicesTable",
            table_name=f"TenantServices-{self.environment}",
            partition_key=dynamodb.Attribute(
                name="tenant_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True
        )
        return table

    def _create_last_updated_table(self) -> dynamodb.Table:
        """Create DynamoDB table for tracking last updated timestamps."""
        table = dynamodb.Table(
            self,
            "LastUpdatedTable",
            table_name=f"LastUpdated-{self.environment}",
            partition_key=dynamodb.Attribute(
                name="tenant_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="table_name",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True
        )
        return table

    def _create_lambda_role(self) -> iam.Role:
        """Create IAM role with necessary permissions for Lambda functions."""
        role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )

        # Add custom policy for pipeline operations
        policy = iam.Policy(
            self,
            "DataPipelinePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:Scan",
                        "dynamodb:Query"
                    ],
                    resources=[
                        self.tenant_services_table.table_arn,
                        self.last_updated_table.table_arn
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
                        self.data_bucket.bucket_arn,
                        f"{self.data_bucket.bucket_arn}/*"
                    ]
                )
            ]
        )
        role.attach_inline_policy(policy)
        return role

    def _create_raw_ingestion_lambda(self, memory: int, timeout: int) -> _lambda.Function:
        """Create Lambda function for raw data ingestion (supports multiple integration services)."""
        function = _lambda.Function(
            self,
            "RawIngestionLambda",
            function_name=f"avesa-raw-ingestion-{self.environment}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../src/raw_ingestion"),
            role=self.lambda_role,
            memory_size=memory,
            timeout=Duration.seconds(timeout),
            environment={
                "BUCKET_NAME": self.data_bucket.bucket_name,
                "TENANT_SERVICES_TABLE": self.tenant_services_table.table_name,
                "LAST_UPDATED_TABLE": self.last_updated_table.table_name,
                "ENVIRONMENT": self.environment,
                "SERVICE_NAME": "connectwise"  # This can be parameterized for different services
            },
            log_retention=logs.RetentionDays.ONE_MONTH
        )
        return function

    def _create_canonical_transform_lambda(self, memory: int, timeout: int) -> _lambda.Function:
        """Create Lambda function for canonical transformation (processes all integration services)."""
        function = _lambda.Function(
            self,
            "CanonicalTransformLambda",
            function_name=f"avesa-canonical-transform-{self.environment}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../src/canonical_transform"),
            role=self.lambda_role,
            memory_size=memory,
            timeout=Duration.seconds(timeout),
            environment={
                "BUCKET_NAME": self.data_bucket.bucket_name,
                "TENANT_SERVICES_TABLE": self.tenant_services_table.table_name,
                "ENVIRONMENT": self.environment
            },
            log_retention=logs.RetentionDays.ONE_MONTH
        )
        return function

    def _create_scheduled_rules(self) -> None:
        """Create EventBridge rules for scheduled execution."""
        # Raw ingestion - every 15 minutes
        raw_ingestion_rule = events.Rule(
            self,
            "RawIngestionSchedule",
            schedule=events.Schedule.rate(Duration.minutes(15)),
            description="Trigger raw data ingestion every 15 minutes"
        )
        raw_ingestion_rule.add_target(
            targets.LambdaFunction(self.raw_ingestion_lambda)
        )

        # Canonical transformation - every 30 minutes (offset by 10 minutes)
        canonical_transform_rule = events.Rule(
            self,
            "CanonicalTransformSchedule",
            schedule=events.Schedule.cron(
                minute="10,40",
                hour="*",
                day="*",
                month="*",
                year="*"
            ),
            description="Trigger canonical transformation every 30 minutes"
        )
        canonical_transform_rule.add_target(
            targets.LambdaFunction(self.canonical_transform_lambda)
        )