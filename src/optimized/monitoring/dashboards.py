"""
CloudWatch Dashboards Configuration

Defines CloudWatch dashboards for monitoring the optimized AVESA data pipeline
with real-time metrics, performance tracking, and alerting.
"""

from typing import Dict, List, Any
from aws_cdk import (
    aws_cloudwatch as cloudwatch,
    Duration
)


class PipelineDashboards:
    """Creates CloudWatch dashboards for pipeline monitoring."""
    
    def __init__(self, environment: str):
        self.environment = environment
        self.namespace = "AVESA/DataPipeline"
    
    def create_main_dashboard(self) -> cloudwatch.Dashboard:
        """Create the main pipeline monitoring dashboard."""
        dashboard = cloudwatch.Dashboard(
            None,  # Will be set by the calling stack
            f"AVESA-Pipeline-{self.environment}",
            dashboard_name=f"AVESA-Pipeline-{self.environment}"
        )
        
        # Row 1: Pipeline Overview
        dashboard.add_widgets(
            # Pipeline Initializations
            cloudwatch.GraphWidget(
                title="Pipeline Initializations",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="PipelineInitialized",
                        statistic="Sum"
                    )
                ],
                period=Duration.minutes(5),
                width=8,
                height=6
            ),
            
            # Processing Mode Distribution
            cloudwatch.GraphWidget(
                title="Processing Mode Distribution",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="PipelineInitialized",
                        dimensions_map={"ProcessingMode": "single-tenant"},
                        statistic="Sum",
                        label="Single Tenant"
                    ),
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="PipelineInitialized",
                        dimensions_map={"ProcessingMode": "multi-tenant"},
                        statistic="Sum",
                        label="Multi Tenant"
                    )
                ],
                period=Duration.minutes(5),
                width=8,
                height=6
            ),
            
            # Active Jobs
            cloudwatch.SingleValueWidget(
                title="Active Jobs (Last 5 min)",
                metrics=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="PipelineInitialized",
                        statistic="Sum"
                    )
                ],
                period=Duration.minutes(5),
                width=8,
                height=6
            )
        )
        
        # Row 2: Processing Throughput
        dashboard.add_widgets(
            # Records Processed Over Time
            cloudwatch.GraphWidget(
                title="Records Processed Per Minute",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ChunkRecordsProcessed",
                        statistic="Sum"
                    )
                ],
                period=Duration.minutes(1),
                width=12,
                height=6
            ),
            
            # Processing Throughput
            cloudwatch.GraphWidget(
                title="Processing Throughput (Records/Second)",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ChunkThroughput",
                        statistic="Average"
                    )
                ],
                period=Duration.minutes(5),
                width=12,
                height=6
            )
        )
        
        # Row 3: Error Monitoring
        dashboard.add_widgets(
            # Error Rate
            cloudwatch.GraphWidget(
                title="Error Rate",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ProcessingErrors",
                        statistic="Sum"
                    )
                ],
                period=Duration.minutes(5),
                width=8,
                height=6
            ),
            
            # Error Types
            cloudwatch.GraphWidget(
                title="Error Types",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ProcessingErrors",
                        dimensions_map={"ErrorType": "initialization_failure"},
                        statistic="Sum",
                        label="Initialization"
                    ),
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ProcessingErrors",
                        dimensions_map={"ErrorType": "api_failure"},
                        statistic="Sum",
                        label="API Failure"
                    ),
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ProcessingErrors",
                        dimensions_map={"ErrorType": "timeout"},
                        statistic="Sum",
                        label="Timeout"
                    )
                ],
                period=Duration.minutes(5),
                width=8,
                height=6
            ),
            
            # Success Rate
            cloudwatch.SingleValueWidget(
                title="Success Rate (Last Hour)",
                metrics=[
                    cloudwatch.MathExpression(
                        expression="(successful / (successful + failed)) * 100",
                        using_metrics={
                            "successful": cloudwatch.Metric(
                                namespace=self.namespace,
                                metric_name="ChunkProcessed",
                                statistic="Sum"
                            ),
                            "failed": cloudwatch.Metric(
                                namespace=self.namespace,
                                metric_name="ProcessingErrors",
                                statistic="Sum"
                            )
                        },
                        label="Success Rate %"
                    )
                ],
                period=Duration.hours(1),
                width=8,
                height=6
            )
        )
        
        # Row 4: Performance Metrics
        dashboard.add_widgets(
            # Processing Time Distribution
            cloudwatch.GraphWidget(
                title="Processing Time Distribution",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ChunkProcessingTime",
                        statistic="Average",
                        label="Average"
                    ),
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ChunkProcessingTime",
                        statistic="p95",
                        label="95th Percentile"
                    ),
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ChunkProcessingTime",
                        statistic="p99",
                        label="99th Percentile"
                    )
                ],
                period=Duration.minutes(5),
                width=12,
                height=6
            ),
            
            # API Efficiency
            cloudwatch.GraphWidget(
                title="API Efficiency",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ApiEfficiency",
                        statistic="Average"
                    )
                ],
                period=Duration.minutes(5),
                width=12,
                height=6
            )
        )
        
        return dashboard
    
    def create_tenant_dashboard(self) -> cloudwatch.Dashboard:
        """Create tenant-specific monitoring dashboard."""
        dashboard = cloudwatch.Dashboard(
            None,
            f"AVESA-Tenants-{self.environment}",
            dashboard_name=f"AVESA-Tenants-{self.environment}"
        )
        
        # Row 1: Tenant Processing Overview
        dashboard.add_widgets(
            # Tenants Processed
            cloudwatch.GraphWidget(
                title="Tenants Processed",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="TenantProcessingStarted",
                        statistic="Sum"
                    ),
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="TenantProcessingCompleted",
                        statistic="Sum"
                    )
                ],
                period=Duration.minutes(5),
                width=12,
                height=6
            ),
            
            # Tenant Processing Time
            cloudwatch.GraphWidget(
                title="Tenant Processing Time",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="TenantProcessingTime",
                        statistic="Average"
                    )
                ],
                period=Duration.minutes(5),
                width=12,
                height=6
            )
        )
        
        # Row 2: Table Processing
        dashboard.add_widgets(
            # Tables Processed by Type (Dynamic - shows all table types)
            cloudwatch.GraphWidget(
                title="Tables Processed by Type",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="TableProcessingCompleted",
                        statistic="Sum",
                        label="All Tables"
                    )
                ],
                period=Duration.minutes(5),
                width=12,
                height=6
            ),
            
            # Chunk Distribution (Dynamic - shows all table types)
            cloudwatch.GraphWidget(
                title="Chunks Processed by Table",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ChunkProcessed",
                        statistic="Sum",
                        label="All Tables"
                    )
                ],
                period=Duration.minutes(5),
                width=12,
                height=6
            )
        )
        
        return dashboard
    
    def create_performance_dashboard(self) -> cloudwatch.Dashboard:
        """Create performance optimization dashboard."""
        dashboard = cloudwatch.Dashboard(
            None,
            f"AVESA-Performance-{self.environment}",
            dashboard_name=f"AVESA-Performance-{self.environment}"
        )
        
        # Row 1: Throughput Metrics
        dashboard.add_widgets(
            # Overall Throughput Trend
            cloudwatch.GraphWidget(
                title="Overall Throughput Trend",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="OverallThroughput",
                        statistic="Average"
                    )
                ],
                period=Duration.minutes(5),
                width=12,
                height=6
            ),
            
            # Throughput by Table Type (Dynamic - shows all table types)
            cloudwatch.GraphWidget(
                title="Throughput by Table Type",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ChunkThroughput",
                        statistic="Average",
                        label="All Tables"
                    )
                ],
                period=Duration.minutes(5),
                width=12,
                height=6
            )
        )
        
        # Row 2: Efficiency Metrics
        dashboard.add_widgets(
            # API Call Efficiency
            cloudwatch.GraphWidget(
                title="API Call Efficiency",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ChunkApiCalls",
                        statistic="Sum",
                        label="Total API Calls"
                    ),
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ChunkErrors",
                        statistic="Sum",
                        label="Failed API Calls"
                    )
                ],
                period=Duration.minutes(5),
                width=12,
                height=6
            ),
            
            # Cost Efficiency
            cloudwatch.GraphWidget(
                title="Cost Per Record",
                left=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="CostPerRecord",
                        statistic="Average"
                    )
                ],
                period=Duration.hours(1),
                width=12,
                height=6
            )
        )
        
        # Row 3: Optimization Metrics
        dashboard.add_widgets(
            # Parallelization Efficiency
            cloudwatch.SingleValueWidget(
                title="Avg Concurrent Chunks",
                metrics=[
                    cloudwatch.Metric(
                        namespace=self.namespace,
                        metric_name="ChunkProcessed",
                        statistic="Sum"
                    )
                ],
                period=Duration.minutes(5),
                width=8,
                height=6
            ),
            
            # Processing Time Improvement
            cloudwatch.SingleValueWidget(
                title="Avg Processing Time (min)",
                metrics=[
                    cloudwatch.MathExpression(
                        expression="avg_time / 60",
                        using_metrics={
                            "avg_time": cloudwatch.Metric(
                                namespace=self.namespace,
                                metric_name="TotalProcessingTime",
                                statistic="Average"
                            )
                        },
                        label="Minutes"
                    )
                ],
                period=Duration.hours(1),
                width=8,
                height=6
            ),
            
            # Data Freshness
            cloudwatch.SingleValueWidget(
                title="Data Freshness (hours)",
                metrics=[
                    cloudwatch.MathExpression(
                        expression="freshness / 3600",
                        using_metrics={
                            "freshness": cloudwatch.Metric(
                                namespace=self.namespace,
                                metric_name="DataFreshness",
                                statistic="Average"
                            )
                        },
                        label="Hours"
                    )
                ],
                period=Duration.hours(1),
                width=8,
                height=6
            )
        )
        
        return dashboard
    
    def get_dashboard_definitions(self) -> List[Dict[str, Any]]:
        """Get all dashboard definitions for CDK deployment."""
        return [
            {
                'name': 'main',
                'dashboard': self.create_main_dashboard(),
                'description': 'Main pipeline monitoring dashboard'
            },
            {
                'name': 'tenants',
                'dashboard': self.create_tenant_dashboard(),
                'description': 'Tenant-specific monitoring dashboard'
            },
            {
                'name': 'performance',
                'dashboard': self.create_performance_dashboard(),
                'description': 'Performance optimization dashboard'
            }
        ]