#!/usr/bin/env python3
"""
AVESA Multi-Tenant Data Pipeline CDK Application
Main entry point for the CDK application that deploys the AVESA pipeline
and performance optimization components.
"""

import os
import json
import boto3
from aws_cdk import App, Environment
from stacks.backfill_stack import BackfillStack
from stacks.performance_optimization_stack import PerformanceOptimizationStack
from stacks.clickhouse_stack import ClickHouseStack

app = App()

def load_environment_config():
    """Load environment configuration from JSON file."""
    config_path = os.path.join(os.path.dirname(__file__), "environment_config.json")
    with open(config_path, 'r') as f:
        return json.load(f)

def detect_environment_from_profile():
    """Detect environment based on current AWS profile."""
    try:
        # Get current AWS profile
        session = boto3.Session()
        profile_name = session.profile_name or "default"
        
        # Load config to map profiles to environments
        config = load_environment_config()
        profile_mapping = config.get("deployment_profiles", {})
        
        # Map profile to environment
        environment = profile_mapping.get(profile_name)
        if environment:
            return environment
            
        # Fallback: try to detect from account ID
        sts = session.client('sts')
        account_id = sts.get_caller_identity()['Account']
        
        # Find environment by account ID
        for env_name, env_config in config["environments"].items():
            if env_config["account"] == account_id:
                return env_name
                
        # If no match found, require explicit environment
        raise ValueError(
            f"Could not auto-detect environment for profile '{profile_name}' and account '{account_id}'. "
            f"Please specify environment using: cdk deploy --context environment=<env>"
        )
        
    except Exception as e:
        print(f"⚠️  Warning: Could not auto-detect environment: {e}")
        return None

# Get environment configuration
environment = app.node.try_get_context("environment")
if not environment:
    environment = detect_environment_from_profile()
    if not environment:
        raise ValueError(
            "Environment must be specified. Use: cdk deploy --context environment=<env> "
            "where <env> is one of: dev, staging, prod"
        )

# Load environment configuration
config = load_environment_config()
env_config = config["environments"].get(environment)
if not env_config:
    raise ValueError(f"Unknown environment: {environment}. Valid environments: {list(config['environments'].keys())}")

account = env_config["account"]
region = env_config["region"]
env = Environment(account=account, region=region)

# Print deployment information for verification
print(f"🚀 AVESA Deployment Configuration:")
print(f"   Environment: {environment}")
print(f"   Account: {account}")
print(f"   Region: {region}")
print(f"   Bucket: {env_config['bucket_name']}")
print(f"   Table Suffix: {env_config['table_suffix']}")
print(f"   CDK Environment: {env}")
print("=" * 60)

# Validate we're deploying to the correct account
try:
    session = boto3.Session()
    sts = session.client('sts')
    current_account = sts.get_caller_identity()['Account']
    if current_account != account:
        raise ValueError(
            f"❌ ACCOUNT MISMATCH DETECTED!\n"
            f"   Expected account: {account} ({environment})\n"
            f"   Current account:  {current_account}\n"
            f"   Please check your AWS profile configuration."
        )
    print(f"✅ Account validation passed: {current_account}")
except Exception as e:
    print(f"⚠️  Could not validate account: {e}")

# Deploy performance stack (new)
performance_stack = PerformanceOptimizationStack(
    app,
    f"AVESAPerformanceOptimization{env_config['table_suffix']}",
    env=env,
    environment=environment,
    data_bucket_name=env_config["bucket_name"],
    tenant_services_table_name=f"TenantServices{env_config['table_suffix']}",
    last_updated_table_name=f"LastUpdated{env_config['table_suffix']}"
)

# Deploy backfill stack for testing
backfill_stack = BackfillStack(
    app,
    f"AVESABackfill{env_config['table_suffix']}",
    env=env,
    environment=environment,
    data_bucket_name=env_config["bucket_name"],
    tenant_services_table_name=f"TenantServices{env_config['table_suffix']}",
    lambda_memory=env_config["lambda_memory"],
    lambda_timeout=env_config["lambda_timeout"]
)

# Deploy ClickHouse stack for multi-tenant analytics
clickhouse_stack = ClickHouseStack(
    app,
    f"AVESAClickHouse{env_config['table_suffix']}",
    env=env,
    environment=environment,
    data_bucket_name=env_config["bucket_name"],
    tenant_services_table_name=f"TenantServices{env_config['table_suffix']}",
    last_updated_table_name=f"LastUpdated{env_config['table_suffix']}"
)

# CONSOLIDATED INFRASTRUCTURE ARCHITECTURE:
#
# ACTIVE STACKS (3):
# 1. PerformanceOptimizationStack - Consolidated core data pipeline including:
#    - DynamoDB tables (TenantServices, LastUpdated, ProcessingJobs, ChunkProgress)
#    - S3 data bucket with lifecycle management
#    - Step Functions orchestration for optimized processing
#    - Lambda functions for data ingestion and transformation
#    - IAM roles and policies for secure access
#
# 2. BackfillStack - Historical data processing and tenant onboarding:
#    - Backfill orchestration for new tenant onboarding
#    - Historical data migration capabilities
#    - Batch processing for large datasets
#
# 3. ClickHouseStack - Multi-tenant analytics database and API layer:
#    - ClickHouse Cloud integration for real-time analytics
#    - Multi-tenant data isolation and security
#    - REST API for data access and querying
#    - Schema management and migration tools
#
# REMOVED STACKS (Consolidated into PerformanceOptimizationStack):
# - DataPipelineStack - DynamoDB tables and S3 bucket moved to PerformanceOptimizationStack
# - MonitoringStack - Monitoring functionality integrated into PerformanceOptimizationStack
#
# ARCHIVED STACKS (moved to infrastructure/stacks/archive/):
# - CrossAccountMonitoringStack - Future multi-account deployment monitoring
#
# This consolidated three-stack architecture provides:
# - Unified resource management with reduced complexity
# - Scalable data ingestion and transformation
# - Historical data processing capabilities
# - Real-time analytics and multi-tenant data access
# - Simplified deployment and maintenance

app.synth()