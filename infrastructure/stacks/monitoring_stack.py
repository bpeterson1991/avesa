"""
Monitoring stack for the ConnectWise data pipeline including
CloudWatch dashboards, alarms, and SNS notifications.
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_logs as logs
)
from constructs import Construct
from .data_pipeline_stack import DataPipelineStack


class MonitoringStack(Stack):
    """Stack for monitoring and alerting infrastructure."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        data_pipeline_stack: DataPipelineStack,
        environment: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.data_pipeline_stack = data_pipeline_stack
        self.environment = environment

        # Create SNS topic for alerts
        self.alert_topic = self._create_alert_topic()

        # Create CloudWatch dashboard
        self.dashboard = self._create_dashboard()

        # Create alarms
        self._create_lambda_alarms()
        self._create_data_quality_alarms()

    def _create_alert_topic(self) -> sns.Topic:
        """Create SNS topic for pipeline alerts."""
        topic = sns.Topic(
            self,
            "PipelineAlerts",
            topic_name=f"connectwise-pipeline-alerts-{self.environment}",
            display_name="ConnectWise Pipeline Alerts"
        )

        # Add email subscription (can be configured via context)
        alert_email = self.node.try_get_context("alert_email")
        if alert_email:
            topic.add_subscription(
                subscriptions.EmailSubscription(alert_email)
            )

        return topic

    def _create_dashboard(self) -> cloudwatch.Dashboard:
        """Create CloudWatch dashboard for pipeline monitoring."""
        dashboard = cloudwatch.Dashboard(
            self,
            "PipelineDashboard",
            dashboard_name=f"ConnectWise-Pipeline-{self.environment}"
        )

        # Lambda function metrics
        lambda_widgets = [
            cloudwatch.GraphWidget(
                title="Lambda Invocations",
                left=[
                    self.data_pipeline_stack.raw_ingestion_lambda.metric_invocations(),
                    self.data_pipeline_stack.canonical_transform_lambda.metric_invocations()
                ],
                period=Duration.minutes(5)
            ),
            cloudwatch.GraphWidget(
                title="Lambda Duration",
                left=[
                    self.data_pipeline_stack.raw_ingestion_lambda.metric_duration(),
                    self.data_pipeline_stack.canonical_transform_lambda.metric_duration()
                ],
                period=Duration.minutes(5)
            ),
            cloudwatch.GraphWidget(
                title="Lambda Errors",
                left=[
                    self.data_pipeline_stack.raw_ingestion_lambda.metric_errors(),
                    self.data_pipeline_stack.canonical_transform_lambda.metric_errors()
                ],
                period=Duration.minutes(5)
            )
        ]

        # DynamoDB metrics
        dynamodb_widgets = [
            cloudwatch.GraphWidget(
                title="DynamoDB Read/Write Capacity",
                left=[
                    self.data_pipeline_stack.tenant_services_table.metric_consumed_read_capacity_units(),
                    self.data_pipeline_stack.last_updated_table.metric_consumed_read_capacity_units()
                ],
                right=[
                    self.data_pipeline_stack.tenant_services_table.metric_consumed_write_capacity_units(),
                    self.data_pipeline_stack.last_updated_table.metric_consumed_write_capacity_units()
                ],
                period=Duration.minutes(5)
            )
        ]

        # S3 metrics
        s3_widgets = [
            cloudwatch.GraphWidget(
                title="S3 Requests",
                left=[
                    cloudwatch.Metric(
                        namespace="AWS/S3",
                        metric_name="NumberOfObjects",
                        dimensions_map={
                            "BucketName": self.data_pipeline_stack.data_bucket.bucket_name,
                            "StorageType": "AllStorageTypes"
                        }
                    )
                ],
                period=Duration.hours(1)
            )
        ]

        # Add widgets to dashboard
        for widget in lambda_widgets + dynamodb_widgets + s3_widgets:
            dashboard.add_widgets(widget)

        return dashboard

    def _create_lambda_alarms(self) -> None:
        """Create CloudWatch alarms for Lambda functions."""
        # Raw ingestion Lambda alarms
        raw_error_alarm = cloudwatch.Alarm(
            self,
            "RawIngestionErrorAlarm",
            alarm_name=f"connectwise-raw-ingestion-errors-{self.environment}",
            metric=self.data_pipeline_stack.raw_ingestion_lambda.metric_errors(
                period=Duration.minutes(5)
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="Raw ingestion Lambda function errors"
        )
        raw_error_alarm.add_alarm_action(
            cw_actions.SnsAction(self.alert_topic)
        )

        raw_duration_alarm = cloudwatch.Alarm(
            self,
            "RawIngestionDurationAlarm",
            alarm_name=f"connectwise-raw-ingestion-duration-{self.environment}",
            metric=self.data_pipeline_stack.raw_ingestion_lambda.metric_duration(
                period=Duration.minutes(5)
            ),
            threshold=240000,  # 4 minutes in milliseconds
            evaluation_periods=2,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            alarm_description="Raw ingestion Lambda function duration too high"
        )
        raw_duration_alarm.add_alarm_action(
            cw_actions.SnsAction(self.alert_topic)
        )

        # Canonical transform Lambda alarms
        canonical_error_alarm = cloudwatch.Alarm(
            self,
            "CanonicalTransformErrorAlarm",
            alarm_name=f"connectwise-canonical-transform-errors-{self.environment}",
            metric=self.data_pipeline_stack.canonical_transform_lambda.metric_errors(
                period=Duration.minutes(5)
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="Canonical transform Lambda function errors"
        )
        canonical_error_alarm.add_alarm_action(
            cw_actions.SnsAction(self.alert_topic)
        )

        canonical_duration_alarm = cloudwatch.Alarm(
            self,
            "CanonicalTransformDurationAlarm",
            alarm_name=f"connectwise-canonical-transform-duration-{self.environment}",
            metric=self.data_pipeline_stack.canonical_transform_lambda.metric_duration(
                period=Duration.minutes(5)
            ),
            threshold=480000,  # 8 minutes in milliseconds
            evaluation_periods=2,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            alarm_description="Canonical transform Lambda function duration too high"
        )
        canonical_duration_alarm.add_alarm_action(
            cw_actions.SnsAction(self.alert_topic)
        )

    def _create_data_quality_alarms(self) -> None:
        """Create custom metric alarms for data quality monitoring."""
        # Custom metric for data freshness
        data_freshness_alarm = cloudwatch.Alarm(
            self,
            "DataFreshnessAlarm",
            alarm_name=f"connectwise-data-freshness-{self.environment}",
            metric=cloudwatch.Metric(
                namespace="ConnectWise/Pipeline",
                metric_name="DataFreshness",
                dimensions_map={
                    "Environment": self.environment
                },
                period=Duration.minutes(30)
            ),
            threshold=3600,  # 1 hour in seconds
            evaluation_periods=2,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            alarm_description="Data has not been updated in over 1 hour",
            treat_missing_data=cloudwatch.TreatMissingData.BREACHING
        )
        data_freshness_alarm.add_alarm_action(
            cw_actions.SnsAction(self.alert_topic)
        )

        # Custom metric for record count anomalies
        record_count_alarm = cloudwatch.Alarm(
            self,
            "RecordCountAnomalyAlarm",
            alarm_name=f"connectwise-record-count-anomaly-{self.environment}",
            metric=cloudwatch.Metric(
                namespace="ConnectWise/Pipeline",
                metric_name="RecordCountAnomaly",
                dimensions_map={
                    "Environment": self.environment
                },
                period=Duration.minutes(15)
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="Unusual record count detected in pipeline",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING
        )
        record_count_alarm.add_alarm_action(
            cw_actions.SnsAction(self.alert_topic)
        )