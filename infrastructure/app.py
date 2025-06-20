#!/usr/bin/env python3
"""
AVESA Multi-Tenant Data Pipeline CDK Application
Main entry point for the CDK application that deploys the AVESA pipeline
and performance optimization components.
"""

import os
from aws_cdk import App, Environment
from stacks.data_pipeline_stack import DataPipelineStack
from stacks.monitoring_stack import MonitoringStack
from stacks.backfill_stack import BackfillStack
from stacks.cross_account_monitoring import CrossAccountMonitoringStack
from stacks.performance_optimization_stack import PerformanceOptimizationStack

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
region = os.environ.get("CDK_DEFAULT_REGION", "us-east-2")

# Validate production account is set when deploying to prod
if environment == "prod" and not account:
    raise ValueError(
        "CDK_PROD_ACCOUNT environment variable must be set when deploying to production. "
        "Example: export CDK_PROD_ACCOUNT=987654321098"
    )

env = Environment(account=account, region=region)

# Print deployment information for verification
print(f"🚀 AVESA Deployment Configuration:")
print(f"   Environment: {environment}")
print(f"   Account: {account}")
print(f"   Region: {region}")
print(f"   Stack Name: AVESAPerformanceOptimization-{environment}")
print(f"   CDK Environment: {env}")
print("=" * 60)

# Environment-specific configuration
config = {
    "dev": {
        "bucket_name": "data-storage-msp-dev",
        "enable_monitoring": True,
        "enable_optimization": True,  # Enable performance features for dev
        "lambda_memory": 512,
        "lambda_timeout": 300
    },
    "staging": {
        "bucket_name": "data-storage-msp-staging",
        "enable_monitoring": True,
        "enable_optimization": True,  # Enable performance features for staging
        "lambda_memory": 1024,
        "lambda_timeout": 600
    },
    "prod": {
        "bucket_name": "data-storage-msp-prod",
        "enable_monitoring": True,
        "enable_optimization": False,  # Disable performance features for prod initially
        "lambda_memory": 1024,
        "lambda_timeout": 900
    }
}

env_config = config.get(environment, config["dev"])

# Skip main data pipeline stack deployment since it already exists
# Deploy performance stack (new)
if env_config.get("enable_optimization", False):
    performance_stack = PerformanceOptimizationStack(
        app,
        f"AVESAPerformanceOptimization-{environment}",
        env=env,
        environment=environment,
        data_bucket_name=env_config["bucket_name"],
        tenant_services_table_name="TenantServices" if environment == "prod" else f"TenantServices-{environment}",
        last_updated_table_name="LastUpdated" if environment == "prod" else f"LastUpdated-{environment}"
    )

# Deploy backfill stack for testing
backfill_stack = BackfillStack(
    app,
    f"AVESABackfill-{environment}",
    env=env,
    environment=environment,
    data_bucket_name=env_config["bucket_name"],
    tenant_services_table_name="TenantServices" if environment == "prod" else f"TenantServices-{environment}",
    lambda_memory=env_config["lambda_memory"],
    lambda_timeout=env_config["lambda_timeout"]
)

# Skip other stacks for now - focus on performance and backfill deployment

app.synth()