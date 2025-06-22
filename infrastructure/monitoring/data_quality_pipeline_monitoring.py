"""
Universal Data Quality Pipeline Monitoring Infrastructure
========================================================

This module creates comprehensive monitoring for the entire canonical data pipeline:
1. Raw Data Ingestion Quality
2. Canonical Transformation Quality  
3. ClickHouse Loading Quality
4. Final Data State Quality

Monitors ALL canonical tables (companies, contacts, tickets, time_entries) across
the complete data transformation pipeline.
"""

from aws_cdk import (
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    Duration
)
from constructs import Construct
from typing import Dict, Any, List

class DataQualityPipelineMonitoringStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # All canonical tables to monitor
        self.canonical_tables = ['companies', 'contacts', 'tickets', 'time_entries']
        
        # Pipeline stages to monitor
        self.pipeline_stages = {
            'ingestion': 'Raw Data Ingestion',
            'transformation': 'Canonical Transformation',
            'loading': 'ClickHouse Loading',
            'validation': 'Final Data Validation'
        }
        
        # Table display names
        self.table_display_names = {
            'companies': 'Companies',
            'contacts': 'Contacts',
            'tickets': 'Tickets',
            'time_entries': 'Time Entries'
        }
        
        # Create SNS topic for alerts
        self.alert_topic = sns.Topic(
            self, "DataQualityPipelineAlerts",
            topic_name="data-quality-pipeline-alerts",
            display_name="Data Quality Pipeline Alerts"
        )
        
        # Create CloudWatch dashboard
        self.create_pipeline_dashboard()
        
        # Create CloudWatch alarms for all stages
        self.create_pipeline_alarms()
        
        # Create scheduled monitoring Lambda
        self.create_monitoring_lambda()
    
    def create_pipeline_dashboard(self):
        """Create comprehensive CloudWatch dashboard for entire pipeline"""
        
        dashboard = cloudwatch.Dashboard(
            self, "DataQualityPipelineDashboard",
            dashboard_name="DataQuality-Pipeline-Dashboard"
        )
        
        # === OVERALL SYSTEM HEALTH ===
        overall_health_widget = cloudwatch.SingleValueWidget(
            title="Overall Pipeline Health Score",
            metrics=[
                cloudwatch.Metric(
                    namespace="DataQuality/Pipeline",
                    metric_name="OverallHealthScore",
                    statistic="Average"
                )
            ],
            width=6,
            height=6
        )
        
        pipeline_throughput_widget = cloudwatch.SingleValueWidget(
            title="Pipeline Throughput (Records/Hour)",
            metrics=[
                cloudwatch.Metric(
                    namespace="DataQuality/Pipeline",
                    metric_name="RecordsProcessedPerHour",
                    statistic="Average"
                )
            ],
            width=6,
            height=6
        )
        
        # === STAGE-BY-STAGE HEALTH ===
        stage_health_metrics = []
        for stage_key, stage_name in self.pipeline_stages.items():
            stage_health_metrics.append(
                cloudwatch.Metric(
                    namespace="DataQuality/Pipeline",
                    metric_name="StageHealthScore",
                    dimensions_map={"Stage": stage_key},
                    statistic="Average",
                    label=f"{stage_name} Health"
                )
            )
        
        stage_health_widget = cloudwatch.GraphWidget(
            title="Pipeline Stage Health Scores",
            left=stage_health_metrics,
            width=12,
            height=6
        )
        
        # === DATA QUALITY METRICS BY TABLE ===
        quality_metrics_by_table = []
        for table in self.canonical_tables:
            quality_metrics_by_table.extend([
                cloudwatch.Metric(
                    namespace="DataQuality/Tables",
                    metric_name="DataQualityScore",
                    dimensions_map={"TableName": table},
                    statistic="Average",
                    label=f"{self.table_display_names[table]} Quality"
                )
            ])
        
        table_quality_widget = cloudwatch.GraphWidget(
            title="Data Quality Scores by Table",
            left=quality_metrics_by_table,
            width=12,
            height=6
        )
        
        # === PIPELINE STAGE METRICS ===
        
        # 1. Ingestion Quality
        ingestion_metrics = []
        for table in self.canonical_tables:
            ingestion_metrics.extend([
                cloudwatch.Metric(
                    namespace="DataQuality/Ingestion",
                    metric_name="RecordsIngested",
                    dimensions_map={"TableName": table},
                    statistic="Sum",
                    label=f"{self.table_display_names[table]} Ingested"
                ),
                cloudwatch.Metric(
                    namespace="DataQuality/Ingestion",
                    metric_name="IngestionErrors",
                    dimensions_map={"TableName": table},
                    statistic="Sum",
                    label=f"{self.table_display_names[table]} Errors"
                )
            ])
        
        ingestion_widget = cloudwatch.GraphWidget(
            title="Data Ingestion: Records & Errors",
            left=[m for m in ingestion_metrics if "Ingested" in m.label],
            right=[m for m in ingestion_metrics if "Errors" in m.label],
            width=12,
            height=6
        )
        
        # 2. Transformation Quality
        transformation_metrics = []
        for table in self.canonical_tables:
            transformation_metrics.extend([
                cloudwatch.Metric(
                    namespace="DataQuality/Transformation",
                    metric_name="TransformationSuccessRate",
                    dimensions_map={"TableName": table},
                    statistic="Average",
                    label=f"{self.table_display_names[table]} Success Rate"
                ),
                cloudwatch.Metric(
                    namespace="DataQuality/Transformation",
                    metric_name="FieldMappingErrors",
                    dimensions_map={"TableName": table},
                    statistic="Sum",
                    label=f"{self.table_display_names[table]} Mapping Errors"
                )
            ])
        
        transformation_widget = cloudwatch.GraphWidget(
            title="Canonical Transformation Quality",
            left=[m for m in transformation_metrics if "Success Rate" in m.label],
            right=[m for m in transformation_metrics if "Errors" in m.label],
            width=12,
            height=6
        )
        
        # 3. ClickHouse Loading Quality
        loading_metrics = []
        for table in self.canonical_tables:
            loading_metrics.extend([
                cloudwatch.Metric(
                    namespace="DataQuality/ClickHouse",
                    metric_name="DuplicateCount",
                    dimensions_map={"TableName": table},
                    statistic="Average",
                    label=f"{self.table_display_names[table]} Duplicates"
                ),
                cloudwatch.Metric(
                    namespace="DataQuality/ClickHouse",
                    metric_name="SCDViolations",
                    dimensions_map={"TableName": table},
                    statistic="Average",
                    label=f"{self.table_display_names[table]} SCD Violations"
                )
            ])
        
        loading_widget = cloudwatch.GraphWidget(
            title="ClickHouse Loading Quality Issues",
            left=[m for m in loading_metrics if "Duplicates" in m.label],
            right=[m for m in loading_metrics if "SCD" in m.label],
            width=12,
            height=6
        )
        
        # 4. Data Completeness & Freshness
        completeness_metrics = []
        for table in self.canonical_tables:
            completeness_metrics.extend([
                cloudwatch.Metric(
                    namespace="DataQuality/Completeness",
                    metric_name="DataFreshness",
                    dimensions_map={"TableName": table},
                    statistic="Average",
                    label=f"{self.table_display_names[table]} Freshness (hours)"
                ),
                cloudwatch.Metric(
                    namespace="DataQuality/Completeness",
                    metric_name="CompletenessScore",
                    dimensions_map={"TableName": table},
                    statistic="Average",
                    label=f"{self.table_display_names[table]} Completeness %"
                )
            ])
        
        completeness_widget = cloudwatch.GraphWidget(
            title="Data Freshness & Completeness",
            left=[m for m in completeness_metrics if "Freshness" in m.label],
            right=[m for m in completeness_metrics if "Completeness" in m.label],
            width=12,
            height=6
        )
        
        # Add widgets to dashboard
        dashboard.add_widgets(
            overall_health_widget,
            pipeline_throughput_widget
        )
        dashboard.add_widgets(
            stage_health_widget
        )
        dashboard.add_widgets(
            table_quality_widget
        )
        dashboard.add_widgets(
            ingestion_widget
        )
        dashboard.add_widgets(
            transformation_widget
        )
        dashboard.add_widgets(
            loading_widget
        )
        dashboard.add_widgets(
            completeness_widget
        )
    
    def create_pipeline_alarms(self):
        """Create CloudWatch alarms for entire pipeline monitoring"""
        
        # === CRITICAL SYSTEM ALARMS ===
        
        # Overall pipeline health alarm
        overall_health_alarm = cloudwatch.Alarm(
            self, "OverallPipelineHealthAlarm",
            alarm_name="DataQuality-OverallPipelineHealth",
            alarm_description="Alert when overall pipeline health score drops below 70",
            metric=cloudwatch.Metric(
                namespace="DataQuality/Pipeline",
                metric_name="OverallHealthScore",
                statistic="Average"
            ),
            threshold=70,
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            evaluation_periods=2,
            datapoints_to_alarm=2
        )
        overall_health_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
        
        # Pipeline throughput alarm
        throughput_alarm = cloudwatch.Alarm(
            self, "PipelineThroughputAlarm",
            alarm_name="DataQuality-LowPipelineThroughput",
            alarm_description="Alert when pipeline throughput drops significantly",
            metric=cloudwatch.Metric(
                namespace="DataQuality/Pipeline",
                metric_name="RecordsProcessedPerHour",
                statistic="Average"
            ),
            threshold=100,  # Minimum records per hour
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            evaluation_periods=3,
            datapoints_to_alarm=2
        )
        throughput_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
        
        # === STAGE-SPECIFIC ALARMS ===
        
        for stage_key, stage_name in self.pipeline_stages.items():
            stage_health_alarm = cloudwatch.Alarm(
                self, f"{stage_key.title()}StageHealthAlarm",
                alarm_name=f"DataQuality-{stage_name.replace(' ', '')}-Health",
                alarm_description=f"Alert when {stage_name} health score drops below 75",
                metric=cloudwatch.Metric(
                    namespace="DataQuality/Pipeline",
                    metric_name="StageHealthScore",
                    dimensions_map={"Stage": stage_key},
                    statistic="Average"
                ),
                threshold=75,
                comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
                evaluation_periods=2,
                datapoints_to_alarm=2
            )
            stage_health_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
        
        # === TABLE-SPECIFIC ALARMS ===
        
        for table in self.canonical_tables:
            table_display_name = self.table_display_names[table]
            
            # Data quality score alarm
            quality_alarm = cloudwatch.Alarm(
                self, f"{table.title()}DataQualityAlarm",
                alarm_name=f"DataQuality-{table_display_name.replace(' ', '')}-QualityScore",
                alarm_description=f"Alert when {table_display_name} data quality score drops below 80",
                metric=cloudwatch.Metric(
                    namespace="DataQuality/Tables",
                    metric_name="DataQualityScore",
                    dimensions_map={"TableName": table},
                    statistic="Average"
                ),
                threshold=80,
                comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
                evaluation_periods=2,
                datapoints_to_alarm=2
            )
            quality_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
            
            # High ingestion error rate alarm
            ingestion_error_alarm = cloudwatch.Alarm(
                self, f"{table.title()}IngestionErrorAlarm",
                alarm_name=f"DataQuality-{table_display_name.replace(' ', '')}-IngestionErrors",
                alarm_description=f"Alert when {table_display_name} ingestion error rate exceeds 5%",
                metric=cloudwatch.Metric(
                    namespace="DataQuality/Ingestion",
                    metric_name="IngestionErrorRate",
                    dimensions_map={"TableName": table},
                    statistic="Average"
                ),
                threshold=5.0,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
                evaluation_periods=2,
                datapoints_to_alarm=2
            )
            ingestion_error_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
            
            # Low transformation success rate alarm
            transformation_alarm = cloudwatch.Alarm(
                self, f"{table.title()}TransformationAlarm",
                alarm_name=f"DataQuality-{table_display_name.replace(' ', '')}-TransformationSuccess",
                alarm_description=f"Alert when {table_display_name} transformation success rate drops below 95%",
                metric=cloudwatch.Metric(
                    namespace="DataQuality/Transformation",
                    metric_name="TransformationSuccessRate",
                    dimensions_map={"TableName": table},
                    statistic="Average"
                ),
                threshold=95.0,
                comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
                evaluation_periods=2,
                datapoints_to_alarm=2
            )
            transformation_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
            
            # Data freshness alarm
            freshness_alarm = cloudwatch.Alarm(
                self, f"{table.title()}FreshnessAlarm",
                alarm_name=f"DataQuality-{table_display_name.replace(' ', '')}-DataFreshness",
                alarm_description=f"Alert when {table_display_name} data is older than 24 hours",
                metric=cloudwatch.Metric(
                    namespace="DataQuality/Completeness",
                    metric_name="DataFreshness",
                    dimensions_map={"TableName": table},
                    statistic="Average"
                ),
                threshold=24.0,  # Hours
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
                evaluation_periods=1,
                datapoints_to_alarm=1
            )
            freshness_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
    
    def create_monitoring_lambda(self):
        """Create Lambda function for comprehensive pipeline monitoring"""
        
        # Create monitoring Lambda function
        monitoring_lambda = lambda_.Function(
            self, "DataQualityPipelineMonitoringLambda",
            function_name="data-quality-pipeline-monitor",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("monitoring"),
            timeout=Duration.minutes(15),  # Increased for comprehensive monitoring
            memory_size=1024,  # Increased for comprehensive analysis
            environment={
                "ALERT_THRESHOLD": "50",
                "CLICKHOUSE_SECRET_NAME": "clickhouse-connection-dev",
                "SNS_TOPIC_ARN": self.alert_topic.topic_arn,
                "MONITOR_FULL_PIPELINE": "true",
                "CANONICAL_TABLES": ",".join(self.canonical_tables)
            }
        )
        
        # Grant permissions
        self.alert_topic.grant_publish(monitoring_lambda)
        
        # Grant comprehensive AWS permissions for pipeline monitoring
        monitoring_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "secretsmanager:GetSecretValue"
                ],
                resources=[
                    f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:clickhouse-connection-*"
                ]
            )
        )
        
        monitoring_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "cloudwatch:PutMetricData",
                    "cloudwatch:GetMetricStatistics"
                ],
                resources=["*"]
            )
        )
        
        # Grant S3 access for pipeline data analysis
        monitoring_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                resources=[
                    "arn:aws:s3:::avesa-*",
                    "arn:aws:s3:::avesa-*/*"
                ]
            )
        )
        
        # Grant DynamoDB access for pipeline state monitoring
        monitoring_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:GetItem"
                ],
                resources=[
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/TenantServices*",
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/LastUpdated*",
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/ProcessingJobs*"
                ]
            )
        )
        
        # Grant Step Functions access for pipeline execution monitoring
        monitoring_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "states:ListExecutions",
                    "states:DescribeExecution"
                ],
                resources=[
                    f"arn:aws:states:{self.region}:{self.account}:stateMachine:*"
                ]
            )
        )
        
        # Create EventBridge rule for scheduled execution
        monitoring_schedule = events.Rule(
            self, "DataQualityPipelineMonitoringSchedule",
            rule_name="data-quality-pipeline-monitoring-schedule",
            description="Schedule for comprehensive data quality pipeline monitoring",
            schedule=events.Schedule.rate(Duration.hours(4))  # Run every 4 hours
        )
        
        # Add Lambda as target
        monitoring_schedule.add_target(
            targets.LambdaFunction(monitoring_lambda)
        )


# Configuration for different environments
PIPELINE_MONITORING_CONFIG = {
    'dev': {
        'overall_health_threshold': 70,
        'stage_health_threshold': 75,
        'table_quality_threshold': 80,
        'ingestion_error_threshold': 5.0,
        'transformation_success_threshold': 95.0,
        'data_freshness_threshold': 24.0,
        'schedule_rate_hours': 4,
        'sns_email_endpoints': ['dev-alerts@company.com']
    },
    'staging': {
        'overall_health_threshold': 75,
        'stage_health_threshold': 80,
        'table_quality_threshold': 85,
        'ingestion_error_threshold': 3.0,
        'transformation_success_threshold': 97.0,
        'data_freshness_threshold': 12.0,
        'schedule_rate_hours': 3,
        'sns_email_endpoints': ['staging-alerts@company.com']
    },
    'prod': {
        'overall_health_threshold': 80,
        'stage_health_threshold': 85,
        'table_quality_threshold': 90,
        'ingestion_error_threshold': 1.0,
        'transformation_success_threshold': 99.0,
        'data_freshness_threshold': 6.0,
        'schedule_rate_hours': 2,
        'sns_email_endpoints': ['prod-alerts@company.com', 'data-team@company.com']
    }
}