"""
ClickHouse Cloud Multi-Tenant Infrastructure Stack

This stack deploys the foundational infrastructure for ClickHouse Cloud
with shared tables approach for multi-tenant SaaS application.

Features:
- ClickHouse Cloud deployment with VPC PrivateLink
- Shared tables with tenant isolation via row-level security
- Optimized indexing for query performance
- IAM roles and security policies
- Network security groups and access controls
"""

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    BundlingOptions,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    CfnOutput
)
from constructs import Construct
from typing import Dict, Any
# Removed bundling utils import - using simple asset deployment instead


class ClickHouseStack(Stack):
    """ClickHouse Cloud infrastructure stack for multi-tenant SaaS."""

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
        self.tenant_services_table = tenant_services_table_name
        self.last_updated_table = last_updated_table_name

        # Create VPC for ClickHouse connectivity
        self.vpc = self._create_vpc()
        
        # Create security groups
        self.security_groups = self._create_security_groups()
        
        # Create ClickHouse connection secrets
        self.clickhouse_secret = self._create_clickhouse_secret()
        
        # Create IAM roles
        self.lambda_role = self._create_lambda_role()
        self.step_functions_role = self._create_step_functions_role()
        
        # Create Lambda functions for data movement
        self.data_movement_lambdas = self._create_data_movement_lambdas()
        
        # Create Step Functions for orchestration
        self.orchestration_state_machine = self._create_orchestration_state_machine()
        
        # Create monitoring and alerting
        self.monitoring = self._create_monitoring()
        
        # Output important values
        self._create_outputs()

    def _create_vpc(self) -> ec2.Vpc:
        """Create VPC with private subnets for ClickHouse connectivity."""
        vpc = ec2.Vpc(
            self,
            "ClickHouseVPC",
            vpc_name=f"clickhouse-vpc-{self.env_name}",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                )
            ],
            enable_dns_hostnames=True,
            enable_dns_support=True
        )
        
        # Add VPC endpoints for AWS services
        vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3
        )
        
        vpc.add_interface_endpoint(
            "SecretsManagerEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
        )
        
        return vpc

    def _create_security_groups(self) -> Dict[str, ec2.SecurityGroup]:
        """Create security groups for ClickHouse access."""
        # Lambda security group
        lambda_sg = ec2.SecurityGroup(
            self,
            "ClickHouseLambdaSG",
            vpc=self.vpc,
            description="Security group for Lambda functions accessing ClickHouse",
            allow_all_outbound=True
        )
        
        # ClickHouse client security group
        clickhouse_client_sg = ec2.SecurityGroup(
            self,
            "ClickHouseClientSG",
            vpc=self.vpc,
            description="Security group for ClickHouse client connections",
            allow_all_outbound=True
        )
        
        # Allow HTTPS outbound for ClickHouse Cloud
        clickhouse_client_sg.add_egress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(443),
            description="HTTPS to ClickHouse Cloud"
        )
        
        # Allow ClickHouse native protocol (8443 for secure)
        clickhouse_client_sg.add_egress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(8443),
            description="ClickHouse native protocol (secure)"
        )
        
        return {
            "lambda": lambda_sg,
            "clickhouse_client": clickhouse_client_sg
        }

    def _create_clickhouse_secret(self) -> secretsmanager.Secret:
        """Create AWS Secrets Manager secret for ClickHouse connection."""
        secret = secretsmanager.Secret(
            self,
            "ClickHouseSecret",
            secret_name=f"clickhouse-connection-{self.env_name}",
            description="ClickHouse Cloud connection credentials and configuration",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username": "default"}',
                generate_string_key="password",
                exclude_characters=" %+~`#$&*()|[]{}:;<>?!'/\"\\",
                password_length=32
            )
        )
        
        return secret

    def _create_lambda_role(self) -> iam.Role:
        """Create IAM role for Lambda functions with ClickHouse permissions."""
        role = iam.Role(
            self,
            "ClickHouseLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                )
            ]
        )
        
        # Add custom policy for ClickHouse operations
        policy = iam.Policy(
            self,
            "ClickHouseDataMovementPolicy",
            statements=[
                # DynamoDB access
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
                        f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.tenant_services_table}",
                        f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.last_updated_table}"
                    ]
                ),
                # S3 access
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    resources=[
                        f"arn:aws:s3:::{self.data_bucket_name}",
                        f"arn:aws:s3:::{self.data_bucket_name}/*"
                    ]
                ),
                # Secrets Manager access
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "secretsmanager:GetSecretValue"
                    ],
                    resources=[
                        self.clickhouse_secret.secret_arn
                    ]
                ),
                # CloudWatch Logs
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources=["*"]
                ),
                # Step Functions execution
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:SendTaskSuccess",
                        "states:SendTaskFailure",
                        "states:SendTaskHeartbeat"
                    ],
                    resources=["*"]
                )
            ]
        )
        role.attach_inline_policy(policy)
        
        return role

    def _create_step_functions_role(self) -> iam.Role:
        """Create IAM role for Step Functions orchestration."""
        role = iam.Role(
            self,
            "ClickHouseStepFunctionsRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com")
        )
        
        # Add policy for Lambda invocation
        policy = iam.Policy(
            self,
            "ClickHouseOrchestrationPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "lambda:InvokeFunction"
                    ],
                    resources=[
                        f"arn:aws:lambda:{self.region}:{self.account}:function:clickhouse-*-{self.env_name}"
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:CreateLogDelivery",
                        "logs:GetLogDelivery",
                        "logs:UpdateLogDelivery",
                        "logs:DeleteLogDelivery",
                        "logs:ListLogDeliveries",
                        "logs:PutResourcePolicy",
                        "logs:DescribeResourcePolicies",
                        "logs:DescribeLogGroups"
                    ],
                    resources=["*"]
                )
            ]
        )
        role.attach_inline_policy(policy)
        
        return role

    def _create_data_movement_lambdas(self) -> Dict[str, _lambda.Function]:
        """Create Lambda functions for S3 to ClickHouse data movement."""
        lambdas = {}
        
        # Create ClickHouse Lambda layer with proper path resolution
        import os
        layer_path = os.path.join(os.path.dirname(__file__), "..", "..", "lambda-layers", "clickhouse")
        
        clickhouse_layer = _lambda.LayerVersion(
            self,
            "ClickHouseLayer",
            layer_version_name=f"clickhouse-dependencies-{self.env_name}",
            code=_lambda.Code.from_asset(layer_path),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_10],
            description=f"ClickHouse dependencies for {self.env_name}: clickhouse-connect==0.8.17, lz4==4.4.4, zstandard==0.23.0, certifi, urllib3, pytz",
            removal_policy=RemovalPolicy.RETAIN  # Retain layer versions for rollback capability
        )
        
        # Store layer reference for outputs
        self.clickhouse_layer = clickhouse_layer
        
        # AWS Pandas Layer for data processing
        aws_pandas_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "AWSDataWranglerPandasLayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSSDKPandas-Python310:11"
        )
        
        # Schema initialization Lambda
        lambdas["schema_init"] = _lambda.Function(
            self,
            "ClickHouseSchemaInit",
            function_name=f"clickhouse-schema-init-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_10.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/clickhouse/schema_init/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.lambda_role,
            timeout=Duration.seconds(180),
            memory_size=512,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            security_groups=[self.security_groups["lambda"], self.security_groups["clickhouse_client"]],
            environment={
                "CLICKHOUSE_SECRET_NAME": self.clickhouse_secret.secret_name,
                "ENVIRONMENT": self.env_name
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
            layers=[clickhouse_layer, aws_pandas_layer]
        )
        
        # Data loader Lambda for each table
        tables = ["companies", "contacts", "tickets", "time_entries"]
        for table in tables:
            lambdas[f"loader_{table}"] = _lambda.Function(
                self,
                f"ClickHouseLoader{table.title().replace('_', '')}",
                function_name=f"clickhouse-loader-{table.replace('-', '_')}-{self.env_name}",
                runtime=_lambda.Runtime.PYTHON_3_10,
                handler="lambda_function.lambda_handler",
                code=_lambda.Code.from_asset(
                    "../src",
                    bundling=BundlingOptions(
                        image=_lambda.Runtime.PYTHON_3_10.bundling_image,
                        command=[
                            "bash", "-c",
                            "cp -r /asset-input/clickhouse/data_loader/* /asset-output/ && "
                            "cp -r /asset-input/shared /asset-output/"
                        ]
                    )
                ),
                role=self.lambda_role,
                timeout=Duration.seconds(180),
                memory_size=1024,
                vpc=self.vpc,
                vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
                security_groups=[self.security_groups["lambda"], self.security_groups["clickhouse_client"]],
                environment={
                    "CLICKHOUSE_SECRET_NAME": self.clickhouse_secret.secret_name,
                    "S3_BUCKET_NAME": self.data_bucket_name,
                    "TENANT_SERVICES_TABLE": self.tenant_services_table,
                    "LAST_UPDATED_TABLE": self.last_updated_table,
                    "TARGET_TABLE": table,
                    "ENVIRONMENT": self.env_name
                },
                log_retention=logs.RetentionDays.ONE_MONTH,
                layers=[clickhouse_layer, aws_pandas_layer]
            )
        
        # SCD Type 2 processor Lambda
        lambdas["scd_processor"] = _lambda.Function(
            self,
            "ClickHouseSCDProcessor",
            function_name=f"clickhouse-scd-processor-{self.env_name}",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                "../src",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_10.bundling_image,
                    command=[
                        "bash", "-c",
                        "cp -r /asset-input/clickhouse/scd_processor/* /asset-output/ && "
                        "cp -r /asset-input/shared /asset-output/"
                    ]
                )
            ),
            role=self.lambda_role,
            timeout=Duration.seconds(180),
            memory_size=1024,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            security_groups=[self.security_groups["lambda"], self.security_groups["clickhouse_client"]],
            environment={
                "CLICKHOUSE_SECRET_NAME": self.clickhouse_secret.secret_name,
                "ENVIRONMENT": self.env_name
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
            layers=[clickhouse_layer, aws_pandas_layer]
        )
        
        return lambdas

    def _create_orchestration_state_machine(self) -> sfn.StateMachine:
        """Create Step Functions state machine for data pipeline orchestration."""
        # Define the state machine definition
        schema_init_task = sfn_tasks.LambdaInvoke(
            self,
            "InitializeSchema",
            lambda_function=self.data_movement_lambdas["schema_init"],
            result_path="$.schema_result"
        )
        
        # Create Choice state to check if table_name is provided
        table_name_choice = sfn.Choice(
            self,
            "CheckTableNameProvided",
            comment="Route to single table or parallel processing based on table_name parameter"
        )
        
        # Create single table loading tasks
        tables = ["companies", "contacts", "tickets", "time_entries"]
        single_table_tasks = {}
        
        for table in tables:
            single_table_tasks[table] = sfn_tasks.LambdaInvoke(
                self,
                f"Load{table.title().replace('_', '')}DataSingle",
                lambda_function=self.data_movement_lambdas[f"loader_{table}"],
                output_path="$.Payload"
            )
        
        # Parallel data loading for all tables (backward compatibility)
        parallel_loading = sfn.Parallel(
            self,
            "ParallelDataLoading",
            comment="Load data for all tables in parallel"
        )
        
        for table in tables:
            load_task = sfn_tasks.LambdaInvoke(
                self,
                f"Load{table.title().replace('_', '')}Data",
                lambda_function=self.data_movement_lambdas[f"loader_{table}"],
                output_path="$.Payload"
            )
            parallel_loading.branch(load_task)
        
        # Success state
        success_state = sfn.Succeed(
            self,
            "DataPipelineSuccess",
            comment="Data pipeline completed successfully"
        )
        
        # Error handling
        error_state = sfn.Fail(
            self,
            "DataPipelineError",
            comment="Data pipeline failed"
        )
        
        # Create separate SCD tasks for each path to avoid state reuse issues
        # Only create SCD tasks for tables that actually need SCD processing (Type 2 tables)
        scd_task_parallel = sfn_tasks.LambdaInvoke(
            self,
            "ProcessSCDType2Parallel",
            lambda_function=self.data_movement_lambdas["scd_processor"],
            output_path="$.Payload"
        ).add_catch(error_state, errors=["States.ALL"])
        
        # Only tickets is SCD Type 2, so only create SCD task for tickets
        scd_task_tickets = sfn_tasks.LambdaInvoke(
            self,
            "ProcessSCDType2Tickets",
            lambda_function=self.data_movement_lambdas["scd_processor"],
            output_path="$.Payload"
        ).add_catch(error_state, errors=["States.ALL"])
        
        # Add error handling to tasks
        schema_init_with_catch = schema_init_task.add_catch(
            error_state,
            errors=["States.ALL"]
        )
        
        parallel_loading_with_catch = parallel_loading.add_catch(
            error_state,
            errors=["States.ALL"]
        )
        
        # Add error handling to single table tasks
        for table, task in single_table_tasks.items():
            single_table_tasks[table] = task.add_catch(
                error_state,
                errors=["States.ALL"]
            )
        
        # Configure Choice state conditions - Type 1 tables skip SCD processing
        table_name_choice.when(
            sfn.Condition.string_equals("$.table_name", "companies"),
            single_table_tasks["companies"].next(success_state)  # Type 1: Skip SCD processing
        ).when(
            sfn.Condition.string_equals("$.table_name", "contacts"),
            single_table_tasks["contacts"].next(success_state)  # Type 1: Skip SCD processing
        ).when(
            sfn.Condition.string_equals("$.table_name", "tickets"),
            single_table_tasks["tickets"].next(scd_task_tickets.next(success_state))  # Type 2: Include SCD processing
        ).when(
            sfn.Condition.string_equals("$.table_name", "time_entries"),
            single_table_tasks["time_entries"].next(success_state)  # Type 1: Skip SCD processing
        ).otherwise(
            parallel_loading_with_catch.next(scd_task_parallel.next(success_state))
        )
        
        # Chain the states: InitializeSchema -> CheckTableNameProvided -> [Single OR Parallel] -> SCD -> Success
        definition = schema_init_with_catch.next(table_name_choice)
        
        # Create the state machine
        state_machine = sfn.StateMachine(
            self,
            "ClickHouseDataPipeline",
            state_machine_name=f"clickhouse-data-pipeline-{self.env_name}",
            definition=definition,
            role=self.step_functions_role
        )
        
        return state_machine

    def _create_monitoring(self) -> Dict[str, Any]:
        """Create CloudWatch monitoring and alerting."""
        # SNS topic for alerts
        alert_topic = sns.Topic(
            self,
            "ClickHouseAlerts",
            topic_name=f"clickhouse-alerts-{self.env_name}",
            display_name="ClickHouse Pipeline Alerts"
        )
        
        # CloudWatch dashboard
        dashboard = cloudwatch.Dashboard(
            self,
            "ClickHouseDashboard",
            dashboard_name=f"ClickHouse-Pipeline-{self.env_name}"
        )
        
        # Lambda metrics
        lambda_widgets = []
        for name, function in self.data_movement_lambdas.items():
            lambda_widgets.append(
                cloudwatch.GraphWidget(
                    title=f"Lambda {name} Metrics",
                    left=[
                        function.metric_invocations(),
                        function.metric_errors(),
                        function.metric_duration()
                    ],
                    width=12,
                    height=6
                )
            )
        
        # Step Functions metrics
        sfn_widget = cloudwatch.GraphWidget(
            title="Step Functions Metrics",
            left=[
                self.orchestration_state_machine.metric_started(),
                self.orchestration_state_machine.metric_succeeded(),
                self.orchestration_state_machine.metric_failed()
            ],
            width=12,
            height=6
        )
        
        # Add widgets to dashboard
        dashboard.add_widgets(*lambda_widgets, sfn_widget)
        
        # Create alarms
        alarms = {}
        
        # Lambda error alarms
        for name, function in self.data_movement_lambdas.items():
            alarm = cloudwatch.Alarm(
                self,
                f"ClickHouseLambda{name.title()}ErrorAlarm",
                alarm_name=f"clickhouse-lambda-{name}-errors-{self.env_name}",
                metric=function.metric_errors(),
                threshold=1,
                evaluation_periods=1,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
            )
            alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))
            alarms[f"lambda_{name}_errors"] = alarm
        
        # Step Functions failure alarm
        sfn_alarm = cloudwatch.Alarm(
            self,
            "ClickHouseStepFunctionsFailureAlarm",
            alarm_name=f"clickhouse-stepfunctions-failures-{self.env_name}",
            metric=self.orchestration_state_machine.metric_failed(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
        )
        sfn_alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))
        alarms["stepfunctions_failures"] = sfn_alarm
        
        return {
            "alert_topic": alert_topic,
            "dashboard": dashboard,
            "alarms": alarms
        }

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs for important resources."""
        CfnOutput(
            self,
            "ClickHouseSecretArn",
            value=self.clickhouse_secret.secret_arn,
            description="ARN of the ClickHouse connection secret"
        )
        
        CfnOutput(
            self,
            "ClickHouseLayerArn",
            value=self.clickhouse_layer.layer_version_arn,
            description="ARN of the ClickHouse Lambda layer with dependencies"
        )
        
        CfnOutput(
            self,
            "VPCId",
            value=self.vpc.vpc_id,
            description="VPC ID for ClickHouse connectivity"
        )
        
        CfnOutput(
            self,
            "PrivateSubnetIds",
            value=",".join([subnet.subnet_id for subnet in self.vpc.private_subnets]),
            description="Private subnet IDs for ClickHouse Lambda functions"
        )
        
        CfnOutput(
            self,
            "StateMachineArn",
            value=self.orchestration_state_machine.state_machine_arn,
            description="ARN of the ClickHouse data pipeline state machine"
        )
        
        CfnOutput(
            self,
            "AlertTopicArn",
            value=self.monitoring["alert_topic"].topic_arn,
            description="SNS topic ARN for ClickHouse alerts"
        )