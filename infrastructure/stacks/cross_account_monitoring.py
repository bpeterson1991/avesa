"""
Cross-Account Monitoring Stack for AVESA Data Pipeline

This stack sets up monitoring and observability across the hybrid account setup,
allowing the non-production account to monitor production resources.
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_cloudwatch as cloudwatch,
    aws_iam as iam,
    aws_logs as logs,
    aws_sns as sns,
    aws_cloudwatch_actions as cw_actions,
    CfnOutput
)
from constructs import Construct
from typing import Dict, Any, Optional


class CrossAccountMonitoringStack(Stack):
    """Stack for cross-account monitoring and alerting."""
    
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        production_account_id: Optional[str] = None,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.env_name = environment
        self.production_account_id = production_account_id
        
        # Create SNS topic for alerts
        self.alert_topic = self._create_alert_topic()
        
        # Create cross-account monitoring role (only for non-prod accounts)
        if self.env_name != "prod" and production_account_id:
            self.monitoring_role = self._create_cross_account_monitoring_role()
        
        # Create dashboards
        self.main_dashboard = self._create_main_dashboard()
        
        # Create alarms
        self._create_alarms()
        
        # Create log insights queries
        self._create_log_insights_queries()
    
    def _create_alert_topic(self) -> sns.Topic:
        """Create SNS topic for alerts."""
        topic = sns.Topic(
            self,
            "AVESAAlertTopic",
            topic_name=f"avesa-alerts-{self.env_name}",
            display_name=f"AVESA Data Pipeline Alerts ({self.env_name.upper()})"
        )
        
        # Output the topic ARN for easy subscription
        CfnOutput(
            self,
            "AlertTopicArn",
            value=topic.topic_arn,
            description="SNS topic ARN for AVESA alerts"
        )
        
        return topic
    
    def _create_cross_account_monitoring_role(self) -> iam.Role:
        """Create IAM role for cross-account monitoring access."""
        role = iam.Role(
            self,
            "CrossAccountMonitoringRole",
            role_name=f"AVESACrossAccountMonitoring-{self.env_name}",
            assumed_by=iam.AccountPrincipal(self.production_account_id),
            description="Role for cross-account monitoring of AVESA production resources"
        )
        
        # Add CloudWatch read permissions
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchReadOnlyAccess")
        )
        
        # Add CloudWatch Logs read permissions
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsReadOnlyAccess")
        )
        
        # Add custom policy for specific AVESA resources
        custom_policy = iam.Policy(
            self,
            "AVESAMonitoringPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "lambda:GetFunction",
                        "lambda:ListFunctions",
                        "lambda:GetFunctionConfiguration",
                        "dynamodb:DescribeTable",
                        "dynamodb:ListTables",
                        "s3:GetBucketLocation",
                        "s3:ListBucket",
                        "s3:GetBucketNotification"
                    ],
                    resources=["*"],
                    conditions={
                        "StringLike": {
                            "aws:ResourceTag/Project": "AVESA*"
                        }
                    }
                )
            ]
        )
        role.attach_inline_policy(custom_policy)
        
        # Output the role ARN
        CfnOutput(
            self,
            "CrossAccountMonitoringRoleArn",
            value=role.role_arn,
            description="Cross-account monitoring role ARN"
        )
        
        return role
    
    def _create_main_dashboard(self) -> cloudwatch.Dashboard:
        """Create main monitoring dashboard."""
        dashboard = cloudwatch.Dashboard(
            self,
            "AVESAMainDashboard",
            dashboard_name=f"AVESA-DataPipeline-{self.env_name.upper()}",
            period_override=cloudwatch.PeriodOverride.AUTO
        )
        
        # Lambda metrics widgets
        lambda_widgets = self._create_lambda_widgets()
        dashboard.add_widgets(*lambda_widgets)
        
        # DynamoDB metrics widgets
        dynamodb_widgets = self._create_dynamodb_widgets()
        dashboard.add_widgets(*dynamodb_widgets)
        
        # S3 metrics widgets
        s3_widgets = self._create_s3_widgets()
        dashboard.add_widgets(*s3_widgets)
        
        # Error rate and latency widgets
        error_widgets = self._create_error_widgets()
        dashboard.add_widgets(*error_widgets)
        
        return dashboard
    
    def _create_lambda_widgets(self) -> list:
        """Create Lambda monitoring widgets."""
        # Get function names based on environment
        function_names = self._get_lambda_function_names()
        
        widgets = []
        
        # Invocations widget
        invocations_widget = cloudwatch.GraphWidget(
            title="Lambda Invocations",
            width=12,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Invocations",
                    dimensions_map={"FunctionName": func_name},
                    statistic="Sum",
                    period=Duration.minutes(5)
                ) for func_name in function_names
            ]
        )
        widgets.append(invocations_widget)
        
        # Duration widget
        duration_widget = cloudwatch.GraphWidget(
            title="Lambda Duration",
            width=12,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Duration",
                    dimensions_map={"FunctionName": func_name},
                    statistic="Average",
                    period=Duration.minutes(5)
                ) for func_name in function_names
            ]
        )
        widgets.append(duration_widget)
        
        # Errors widget
        errors_widget = cloudwatch.GraphWidget(
            title="Lambda Errors",
            width=12,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Errors",
                    dimensions_map={"FunctionName": func_name},
                    statistic="Sum",
                    period=Duration.minutes(5)
                ) for func_name in function_names
            ]
        )
        widgets.append(errors_widget)
        
        return widgets
    
    def _create_dynamodb_widgets(self) -> list:
        """Create DynamoDB monitoring widgets."""
        table_names = self._get_dynamodb_table_names()
        
        widgets = []
        
        # Read/Write capacity widget
        capacity_widget = cloudwatch.GraphWidget(
            title="DynamoDB Read/Write Capacity",
            width=12,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/DynamoDB",
                    metric_name="ConsumedReadCapacityUnits",
                    dimensions_map={"TableName": table_name},
                    statistic="Sum",
                    period=Duration.minutes(5)
                ) for table_name in table_names
            ],
            right=[
                cloudwatch.Metric(
                    namespace="AWS/DynamoDB",
                    metric_name="ConsumedWriteCapacityUnits",
                    dimensions_map={"TableName": table_name},
                    statistic="Sum",
                    period=Duration.minutes(5)
                ) for table_name in table_names
            ]
        )
        widgets.append(capacity_widget)
        
        # Throttles widget
        throttles_widget = cloudwatch.GraphWidget(
            title="DynamoDB Throttles",
            width=12,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/DynamoDB",
                    metric_name="ReadThrottles",
                    dimensions_map={"TableName": table_name},
                    statistic="Sum",
                    period=Duration.minutes(5)
                ) for table_name in table_names
            ] + [
                cloudwatch.Metric(
                    namespace="AWS/DynamoDB",
                    metric_name="WriteThrottles",
                    dimensions_map={"TableName": table_name},
                    statistic="Sum",
                    period=Duration.minutes(5)
                ) for table_name in table_names
            ]
        )
        widgets.append(throttles_widget)
        
        return widgets
    
    def _create_s3_widgets(self) -> list:
        """Create S3 monitoring widgets."""
        bucket_name = self._get_s3_bucket_name()
        
        widgets = []
        
        # S3 requests widget
        requests_widget = cloudwatch.GraphWidget(
            title="S3 Requests",
            width=12,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/S3",
                    metric_name="AllRequests",
                    dimensions_map={
                        "BucketName": bucket_name,
                        "FilterId": "EntireBucket"
                    },
                    statistic="Sum",
                    period=Duration.hours(1)
                )
            ]
        )
        widgets.append(requests_widget)
        
        return widgets
    
    def _create_error_widgets(self) -> list:
        """Create error monitoring widgets."""
        widgets = []
        
        # Custom error rate widget (based on log patterns)
        error_rate_widget = cloudwatch.GraphWidget(
            title="Error Rate (from Logs)",
            width=24,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/Logs",
                    metric_name="IncomingLogEvents",
                    dimensions_map={"LogGroupName": f"/aws/lambda/avesa-connectwise-ingestion-{self.env_name}"},
                    statistic="Sum",
                    period=Duration.minutes(5)
                )
            ]
        )
        widgets.append(error_rate_widget)
        
        return widgets
    
    def _create_alarms(self) -> None:
        """Create CloudWatch alarms."""
        function_names = self._get_lambda_function_names()
        
        for func_name in function_names:
            # Error rate alarm
            error_alarm = cloudwatch.Alarm(
                self,
                f"LambdaErrorAlarm-{func_name.replace('-', '')}",
                alarm_name=f"AVESA-Lambda-Errors-{func_name}",
                alarm_description=f"High error rate for {func_name}",
                metric=cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Errors",
                    dimensions_map={"FunctionName": func_name},
                    statistic="Sum",
                    period=Duration.minutes(5)
                ),
                threshold=5,
                evaluation_periods=2,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
            )
            error_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
            
            # Duration alarm
            duration_alarm = cloudwatch.Alarm(
                self,
                f"LambdaDurationAlarm-{func_name.replace('-', '')}",
                alarm_name=f"AVESA-Lambda-Duration-{func_name}",
                alarm_description=f"High duration for {func_name}",
                metric=cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Duration",
                    dimensions_map={"FunctionName": func_name},
                    statistic="Average",
                    period=Duration.minutes(5)
                ),
                threshold=240000,  # 4 minutes in milliseconds
                evaluation_periods=3,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD
            )
            duration_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
    
    def _create_log_insights_queries(self) -> None:
        """Create CloudWatch Logs Insights queries for troubleshooting."""
        # This would typically be done through the console or CLI
        # Here we output the queries for reference
        
        queries = {
            "Error Analysis": """
                fields @timestamp, @message
                | filter @message like /ERROR/
                | sort @timestamp desc
                | limit 100
            """,
            "Performance Analysis": """
                fields @timestamp, @duration, @requestId
                | filter @type = "REPORT"
                | sort @duration desc
                | limit 50
            """,
            "Tenant Activity": """
                fields @timestamp, @message
                | filter @message like /tenant_id/
                | stats count() by bin(5m)
            """
        }
        
        for query_name, query in queries.items():
            CfnOutput(
                self,
                f"LogInsightsQuery{query_name.replace(' ', '')}",
                value=query,
                description=f"CloudWatch Logs Insights query for {query_name}"
            )
    
    def _get_lambda_function_names(self) -> list:
        """Get Lambda function names based on environment."""
        if self.env_name == "prod":
            return [
                "avesa-connectwise-ingestion-prod",
                "avesa-canonical-transform-tickets-prod",
                "avesa-canonical-transform-time-entries-prod",
                "avesa-canonical-transform-companies-prod",
                "avesa-canonical-transform-contacts-prod"
            ]
        else:
            return [
                f"avesa-connectwise-ingestion-{self.environment}",
                f"avesa-canonical-transform-tickets-{self.environment}",
                f"avesa-canonical-transform-time-entries-{self.environment}",
                f"avesa-canonical-transform-companies-{self.environment}",
                f"avesa-canonical-transform-contacts-{self.environment}"
            ]
    
    def _get_dynamodb_table_names(self) -> list:
        """Get DynamoDB table names based on environment."""
        if self.env_name == "prod":
            return ["TenantServices", "LastUpdated"]
        else:
            return [f"TenantServices-{self.env_name}", f"LastUpdated-{self.env_name}"]
    
    def _get_s3_bucket_name(self) -> str:
        """Get S3 bucket name based on environment."""
        if self.env_name == "prod":
            return "data-storage-msp-prod"
        else:
            return f"data-storage-msp-{self.env_name}"