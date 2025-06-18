#!/usr/bin/env python3
"""
ConnectWise Data Pipeline CDK Application

This is the main entry point for the CDK application that deploys
the ConnectWise data ingestion and transformation pipeline.
"""

import os
from aws_cdk import App, Environment
from stacks.data_pipeline_stack import DataPipelineStack
from stacks.monitoring_stack import MonitoringStack

app = App()

# Get environment configuration
account = os.environ.get("CDK_DEFAULT_ACCOUNT")
region = os.environ.get("CDK_DEFAULT_REGION", "us-east-1")
env = Environment(account=account, region=region)

# Environment-specific configuration
environment = app.node.try_get_context("environment") or "dev"
config = {
    "dev": {
        "bucket_name": "data-storage-msp-dev",
        "enable_monitoring": False,
        "lambda_memory": 512,
        "lambda_timeout": 300
    },
    "staging": {
        "bucket_name": "data-storage-msp-staging", 
        "enable_monitoring": True,
        "lambda_memory": 1024,
        "lambda_timeout": 600
    },
    "prod": {
        "bucket_name": "data-storage-msp",
        "enable_monitoring": True,
        "lambda_memory": 1024,
        "lambda_timeout": 900
    }
}

env_config = config.get(environment, config["dev"])

# Deploy main data pipeline stack
data_pipeline_stack = DataPipelineStack(
    app,
    f"ConnectWiseDataPipeline-{environment}",
    env=env,
    environment=environment,
    **env_config
)

# Deploy monitoring stack if enabled
if env_config.get("enable_monitoring"):
    monitoring_stack = MonitoringStack(
        app,
        f"ConnectWiseMonitoring-{environment}",
        env=env,
        data_pipeline_stack=data_pipeline_stack,
        environment=environment
    )

app.synth()