#!/usr/bin/env python3
"""
ConnectWise Data Pipeline CDK Application
Updated for hybrid account strategy

This is the main entry point for the CDK application that deploys
the ConnectWise data ingestion and transformation pipeline.
Supports hybrid account isolation with production in separate account.
"""

import os
from aws_cdk import App, Environment
from stacks.data_pipeline_stack import DataPipelineStack
from stacks.monitoring_stack import MonitoringStack
from stacks.backfill_stack import BackfillStack
from stacks.cross_account_monitoring import CrossAccountMonitoringStack

app = App()

# Account configuration for hybrid approach
ACCOUNTS = {
    "dev": os.environ.get("CDK_DEFAULT_ACCOUNT"),      # Current account (non-prod)
    "staging": os.environ.get("CDK_DEFAULT_ACCOUNT"),  # Current account (non-prod)
    "prod": os.environ.get("CDK_PROD_ACCOUNT")         # Production account (to be set)
}

# Get environment configuration
environment = app.node.try_get_context("environment") or "dev"
account = ACCOUNTS.get(environment, ACCOUNTS["dev"])
region = os.environ.get("CDK_DEFAULT_REGION", "us-east-1")

# Validate production account is set when deploying to prod
if environment == "prod" and not account:
    raise ValueError(
        "CDK_PROD_ACCOUNT environment variable must be set when deploying to production. "
        "Example: export CDK_PROD_ACCOUNT=987654321098"
    )

env = Environment(account=account, region=region)

# Environment-specific configuration
config = {
    "dev": {
        "bucket_name": "data-storage-msp-dev",
        "enable_monitoring": True,
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
        "bucket_name": "data-storage-msp-prod",  # Updated for production account
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
    bucket_name=env_config["bucket_name"],
    lambda_memory=env_config.get("lambda_memory", 512),
    lambda_timeout=env_config.get("lambda_timeout", 300)
)

# Deploy backfill stack
backfill_stack = BackfillStack(
    app,
    f"ConnectWiseBackfill-{environment}",
    env=env,
    environment=environment,
    data_bucket_name=env_config["bucket_name"],
    tenant_services_table_name="TenantServices" if environment == "prod" else f"TenantServices-{environment}",
    lambda_memory=env_config.get("lambda_memory", 1024),
    lambda_timeout=env_config.get("lambda_timeout", 900)
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

# Deploy cross-account monitoring stack
cross_account_monitoring_stack = CrossAccountMonitoringStack(
    app,
    f"ConnectWiseCrossAccountMonitoring-{environment}",
    env=env,
    environment=environment,
    production_account_id=ACCOUNTS.get("prod") if environment != "prod" else None
)

app.synth()