from aws_cdk import (
    Stack,
    Duration,
    BundlingOptions,
    aws_lambda as _lambda,
    aws_apigateway as apigateway,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_logs as logs,
    aws_secretsmanager as secretsmanager,
    CfnOutput,
    Environment
)
from constructs import Construct
import os


class WebApplicationStack(Stack):
    """
    CDK Stack for ClickHouse API Web Application
    Deploys Lambda function with API Gateway for serverless Express API
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get environment from context or environment variables
        environment = self.node.try_get_context("environment") or os.getenv("ENVIRONMENT", "dev")
        
        # Import existing VPC and security group (created by ClickHouse stack)
        vpc = ec2.Vpc.from_lookup(
            self, "ExistingVPC",
            vpc_name=f"clickhouse-vpc-{environment}"
        )
        
        # Import existing security group for ClickHouse access
        security_group = ec2.SecurityGroup.from_lookup_by_id(
            self, "ClickHouseSG",
            security_group_id="sg-093479e258888db17"  # ClickHouse Lambda security group from existing infrastructure
        )

        # Create IAM role for Lambda function
        lambda_role = iam.Role(
            self, "ClickHouseAPILambdaRole",
            role_name=f"ClickHouseAPILambda-{environment}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        # Add Secrets Manager permissions
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret"
                ],
                resources=[
                    f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:clickhouse-*",
                    f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:avesa/*"
                ]
            )
        )

        # Add CloudWatch permissions for enhanced monitoring
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "cloudwatch:PutMetricData",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                resources=["*"]
            )
        )

        # Environment-specific configuration
        memory_size = {
            "dev": 512,
            "staging": 1024,
            "prod": 2048
        }.get(environment, 512)

        timeout = {
            "dev": 30,
            "staging": 60,
            "prod": 300
        }.get(environment, 30)

        # Create Lambda function with bundling to include mappings
        api_lambda = _lambda.Function(
            self, "ClickHouseAPIFunction",
            function_name=f"clickhouse-api-{environment}-v2",
            runtime=_lambda.Runtime.NODEJS_18_X,
            handler="lambda.handler",
            code=_lambda.Code.from_asset(
                "../",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.NODEJS_18_X.bundling_image,
                    command=[
                        "bash", "-c",
                        # Copy API code to output
                        "cp -r /asset-input/src/clickhouse/api/* /asset-output/ && " +
                        # Create mappings directory structure in output
                        "mkdir -p /asset-output/mappings && " +
                        # Copy canonical mappings
                        "cp -r /asset-input/mappings/canonical /asset-output/mappings/"
                    ]
                )
            ),
            role=lambda_role,
            timeout=Duration.seconds(timeout),
            memory_size=memory_size,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[security_group],
            environment={
                "NODE_ENV": environment,
                "ENVIRONMENT": environment,
                "CLICKHOUSE_HOST": f"clickhouse-{environment}.internal",
                "CLICKHOUSE_PORT": "8123",
                "CLICKHOUSE_DATABASE": f"analytics_{environment}",
                "CLICKHOUSE_SECRET_NAME": f"clickhouse-connection-{environment}",
                "REGION": self.region,
                "JWT_SECRET": self._generate_jwt_secret(environment)
            },
            log_retention=logs.RetentionDays.ONE_WEEK if environment == "dev" else logs.RetentionDays.ONE_MONTH
        )

        # Create API Gateway
        api = apigateway.RestApi(
            self, "ClickHouseAPI",
            rest_api_name=f"ClickHouse Analytics API - {environment}",
            description=f"Serverless API for ClickHouse analytics data - {environment}",
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=self._get_cors_origins(environment),
                allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                allow_headers=[
                    "Content-Type",
                    "X-Amz-Date",
                    "Authorization",
                    "X-Api-Key",
                    "X-Amz-Security-Token",
                    "X-Amz-User-Agent",
                    "X-Tenant-ID"
                ],
                allow_credentials=True
            ),
            deploy_options=apigateway.StageOptions(
                stage_name=environment,
                throttling_rate_limit=100 if environment == "prod" else 50,
                throttling_burst_limit=200 if environment == "prod" else 100,
                metrics_enabled=True
            )
        )

        # Create Lambda integration
        lambda_integration = apigateway.LambdaIntegration(
            api_lambda,
            proxy=True
        )

        # Add proxy resource to handle all paths
        proxy_resource = api.root.add_proxy(
            default_integration=lambda_integration,
            any_method=True,
            default_method_options=apigateway.MethodOptions(
                method_responses=[
                    apigateway.MethodResponse(
                        status_code="200",
                        response_parameters={
                            "method.response.header.Access-Control-Allow-Origin": True
                        }
                    )
                ]
            )
        )

        # Add specific health check method (for better monitoring)
        health_resource = api.root.add_resource("health")
        health_resource.add_method(
            "GET",
            lambda_integration,
            method_responses=[
                apigateway.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True
                    }
                )
            ]
        )

        # Create CloudWatch dashboard for monitoring
        # (Optional: You can uncomment this if you want a dashboard)
        # self._create_monitoring_dashboard(api_lambda, api, environment)

        # Output the API Gateway URL
        CfnOutput(
            self, "APIGatewayURL",
            value=api.url,
            description=f"API Gateway endpoint URL for {environment}",
            export_name=f"ClickHouseAPI-URL-{environment}"
        )

        CfnOutput(
            self, "APIGatewayId",
            value=api.rest_api_id,
            description=f"API Gateway ID for {environment}",
            export_name=f"ClickHouseAPI-ID-{environment}"
        )

        CfnOutput(
            self, "LambdaFunctionName",
            value=api_lambda.function_name,
            description=f"Lambda function name for {environment}",
            export_name=f"ClickHouseAPI-Lambda-{environment}"
        )

        CfnOutput(
            self, "LambdaFunctionArn",
            value=api_lambda.function_arn,
            description=f"Lambda function ARN for {environment}",
            export_name=f"ClickHouseAPI-Lambda-ARN-{environment}"
        )

    def _get_cors_origins(self, environment: str) -> list:
        """Get CORS origins based on environment"""
        origins_map = {
            "dev": [
                "http://localhost:3000",
                "http://localhost:8080",
                "https://d1x7a5qijl62wd.cloudfront.net",
                "https://dev.yourdomain.com"
            ],
            "staging": [
                "https://staging.yourdomain.com"
            ],
            "prod": [
                "https://yourdomain.com",
                "https://www.yourdomain.com"
            ]
        }
        return origins_map.get(environment, ["*"])

    def _generate_jwt_secret(self, environment: str) -> str:
        """Generate a secure JWT secret for the environment"""
        import hashlib
        import json
        
        # Generate a deterministic but secure secret based on account, region, and environment
        # This ensures the secret is consistent across deployments but unique per environment
        secret_components = {
            "account": self.account,
            "region": self.region,
            "environment": environment,
            "service": "clickhouse-api",
            "version": "v2"
        }
        
        # Create a hash of the components
        secret_string = json.dumps(secret_components, sort_keys=True)
        secret_hash = hashlib.sha256(secret_string.encode()).hexdigest()
        
        # Add a prefix to make it more secure
        return f"avesa-jwt-{environment}-{secret_hash[:32]}"

    def _create_monitoring_dashboard(self, lambda_function, api_gateway, environment):
        """Create CloudWatch dashboard for monitoring (optional)"""
        from aws_cdk import aws_cloudwatch as cloudwatch
        
        dashboard = cloudwatch.Dashboard(
            self, "ClickHouseAPIDashboard",
            dashboard_name=f"ClickHouse-API-{environment}"
        )

        # Lambda metrics
        lambda_widget = cloudwatch.GraphWidget(
            title="Lambda Function Metrics",
            left=[
                lambda_function.metric_invocations(),
                lambda_function.metric_errors(),
                lambda_function.metric_duration()
            ],
            width=12,
            height=6
        )

        # API Gateway metrics
        api_widget = cloudwatch.GraphWidget(
            title="API Gateway Metrics",
            left=[
                api_gateway.metric_count(),
                api_gateway.metric_client_error(),
                api_gateway.metric_server_error(),
                api_gateway.metric_latency()
            ],
            width=12,
            height=6
        )

        dashboard.add_widgets(lambda_widget, api_widget)