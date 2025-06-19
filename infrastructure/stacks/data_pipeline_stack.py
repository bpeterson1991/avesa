"""
Main data pipeline stack containing Lambda functions, DynamoDB tables,
S3 bucket, and IAM roles for the AVESA multi-tenant data pipeline.

Supports 30+ integration services (ConnectWise, ServiceNow, Salesforce, etc.)
with multi-tenant architecture where each service has dedicated lambda functions.

DYNAMODB SCHEMA DOCUMENTATION:

1. TenantServices Table:
   - Partition Key: tenant_id (STRING) - Identifies the tenant organization
   - Sort Key: service (STRING) - Identifies the integration service (e.g., 'connectwise', 'servicenow')
   - Purpose: Stores which services are enabled for each tenant, along with configuration metadata
   - Example Key: tenant_id='acme-corp', service='connectwise'

2. LastUpdated Table:
   - Partition Key: tenant_id (STRING) - Identifies the tenant organization
   - Sort Key: table_name (STRING) - Identifies the specific table/endpoint being tracked
   - Purpose: Tracks incremental sync timestamps for each tenant/table combination
   - Example Key: tenant_id='acme-corp', table_name='service/tickets'
   
CRITICAL: The LastUpdated table schema was corrected from using 'tenant_service' as partition key
to 'tenant_id' to match Lambda function expectations and ensure proper incremental sync functionality.
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

        self.env_name = environment
        self.bucket_name = bucket_name

        # Create S3 bucket for data storage
        self.data_bucket = self._create_data_bucket()

        # Create DynamoDB tables
        self.tenant_services_table = self._create_tenant_services_table()
        self.last_updated_table = self._create_last_updated_table()

        # Create IAM role for Lambda functions
        self.lambda_role = self._create_lambda_role()

        # Create Lambda functions
        self.connectwise_lambda = self._create_connectwise_lambda(
            memory=lambda_memory,
            timeout=lambda_timeout
        )
        
        # Create separate Lambda functions for each canonical table
        self.canonical_transform_lambdas = self._create_canonical_transform_lambdas(
            memory=lambda_memory,
            timeout=lambda_timeout
        )

        # Create EventBridge rules for scheduling
        self._create_scheduled_rules()

    def _create_data_bucket(self) -> s3.Bucket:
        """Create S3 bucket for data storage."""
        bucket = s3.Bucket(
            self,
            "DataBucket",
            bucket_name=self.bucket_name,
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,  # Keep data safe in production
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteIncompleteMultipartUploads",
                    abort_incomplete_multipart_upload_after=Duration.days(7)
                )
            ]
        )
        return bucket

    def _create_tenant_services_table(self) -> dynamodb.Table:
        """
        Create DynamoDB table for tenant service configuration.
        
        Schema:
        - Partition Key: tenant_id (STRING) - Identifies the tenant
        - Sort Key: service (STRING) - Identifies the specific service (e.g., 'connectwise', 'servicenow')
        
        This table stores configuration for which services are enabled for each tenant,
        along with metadata like company name, secret references, and service status.
        """
        # Remove environment suffix for production (hybrid account approach)
        table_name = "TenantServices" if self.env_name == "prod" else f"TenantServices-{self.env_name}"
        
        table = dynamodb.Table(
            self,
            "TenantServicesTable",
            table_name=table_name,
            partition_key=dynamodb.Attribute(
                name="tenant_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="service",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,  # Keep data safe in production
            point_in_time_recovery=True
        )
        return table

    def _create_last_updated_table(self) -> dynamodb.Table:
        """
        Create DynamoDB table for tracking last updated timestamps.
        
        Schema:
        - Partition Key: tenant_id (STRING) - Identifies the tenant
        - Sort Key: table_name (STRING) - Identifies the specific table/endpoint
        
        This table tracks incremental sync timestamps for each tenant/table combination
        to enable efficient incremental data processing.
        """
        # Remove environment suffix for production (hybrid account approach)
        table_name = "LastUpdated" if self.env_name == "prod" else f"LastUpdated-{self.env_name}"
        
        table = dynamodb.Table(
            self,
            "LastUpdatedTable",
            table_name=table_name,
            partition_key=dynamodb.Attribute(
                name="tenant_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="table_name",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,  # Keep data safe in production
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

    def _create_connectwise_lambda(self, memory: int, timeout: int) -> _lambda.Function:
        """Create Lambda function for ConnectWise data ingestion."""
        function = _lambda.Function(
            self,
            "ConnectWiseLambda",
            function_name=f"avesa-connectwise-ingestion-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../src/integrations/connectwise"),
            role=self.lambda_role,
            memory_size=memory,
            timeout=Duration.seconds(timeout),
            environment={
                "BUCKET_NAME": self.data_bucket.bucket_name,
                "TENANT_SERVICES_TABLE": self.tenant_services_table.table_name,
                "LAST_UPDATED_TABLE": self.last_updated_table.table_name,
                "ENVIRONMENT": self.env_name,
                "SERVICE_NAME": "connectwise"
            },
            log_retention=logs.RetentionDays.ONE_MONTH
        )
        return function

    def _create_canonical_transform_lambdas(self, memory: int, timeout: int) -> Dict[str, _lambda.Function]:
        """Create separate Lambda functions for each canonical table transformation."""
        canonical_tables = ["tickets", "time_entries", "companies", "contacts"]
        lambdas = {}
        
        for table in canonical_tables:
            function = _lambda.Function(
                self,
                f"CanonicalTransform{table.title().replace('_', '')}Lambda",
                function_name=f"avesa-canonical-transform-{table.replace('_', '-')}-{self.env_name}",
                runtime=_lambda.Runtime.PYTHON_3_9,
                handler="lambda_function.lambda_handler",
                code=_lambda.Code.from_asset("../src/canonical_transform"),
                role=self.lambda_role,
                memory_size=memory,
                timeout=Duration.seconds(timeout),
                environment={
                    "BUCKET_NAME": self.data_bucket.bucket_name,
                    "TENANT_SERVICES_TABLE": self.tenant_services_table.table_name,
                    "ENVIRONMENT": self.env_name,
                    "CANONICAL_TABLE": table
                },
                log_retention=logs.RetentionDays.ONE_MONTH
            )
            lambdas[table] = function
        
        return lambdas

    def _create_scheduled_rules(self) -> None:
        """Create EventBridge rules for scheduled execution."""
        # ConnectWise ingestion - every hour
        connectwise_rule = events.Rule(
            self,
            "ConnectWiseIngestionSchedule",
            schedule=events.Schedule.rate(Duration.hours(1)),
            description="Trigger ConnectWise data ingestion every hour"
        )
        connectwise_rule.add_target(
            targets.LambdaFunction(self.connectwise_lambda)
        )

        # Canonical transformation - every hour (offset by 15 minutes after ingestion)
        # Create separate schedules for each canonical table
        canonical_tables = ["tickets", "time_entries", "companies", "contacts"]
        for i, table in enumerate(canonical_tables):
            # Stagger each transformation by 5 minutes to avoid conflicts
            minute_offset = 15 + (i * 5)  # 15, 20, 25, 30
            
            canonical_transform_rule = events.Rule(
                self,
                f"CanonicalTransform{table.title().replace('_', '')}Schedule",
                schedule=events.Schedule.cron(
                    minute=str(minute_offset),
                    hour="*",
                    day="*",
                    month="*",
                    year="*"
                ),
                description=f"Trigger {table} canonical transformation every hour at :{minute_offset:02d}"
            )
            canonical_transform_rule.add_target(
                targets.LambdaFunction(self.canonical_transform_lambdas[table])
            )