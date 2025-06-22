"""
Shared utilities and libraries for the AVESA Multi-Tenant Data Pipeline.

This module provides centralized access to all shared components including:
- AWS client management and configuration
- ClickHouse connection and query management
- Data validation and quality checks
- Environment configuration and utilities
- Logging and utility functions
"""

# Legacy AWS clients (maintained for backward compatibility)
from .aws_clients import get_dynamodb_client, get_s3_client, get_secrets_client

# New centralized AWS client factory
from .aws_client_factory import (
    AWSClientFactory,
    get_dynamodb_client as get_dynamodb_client_v2,
    get_s3_client as get_s3_client_v2,
    get_secrets_client as get_secrets_client_v2,
    get_cloudwatch_client,
    get_lambda_client,
    get_stepfunctions_client
)

# ClickHouse client management
from .clickhouse_client import (
    ClickHouseClient,
    ClickHouseConnectionError,
    ClickHouseQueryError,
    get_clickhouse_connection
)

# Data validation and quality checks
from .validators import (
    CredentialValidator,
    DataQualityValidator,
    TenantConfigValidator,
    ValidationError,
    validate_connectwise_credentials,
    validate_tenant_config
)

# Canonical mapping and transformation
from .canonical_mapper import CanonicalMapper

# Configuration and environment management
from .config_simple import Config, TenantConfig, ServiceConfig, ConnectWiseCredentials
from .environment import Environment, EnvironmentConfig, get_current_environment, get_table_name

# Logging and utilities
from .logger import get_logger
from .utils import flatten_json, get_timestamp

# Path and environment utilities
from .path_utils import PathManager
from .env_validator import EnvironmentValidator

__all__ = [
    # Legacy AWS clients (backward compatibility)
    "get_dynamodb_client",
    "get_s3_client",
    "get_secrets_client",
    
    # New AWS client factory
    "AWSClientFactory",
    "get_dynamodb_client_v2",
    "get_s3_client_v2",
    "get_secrets_client_v2",
    "get_cloudwatch_client",
    "get_lambda_client",
    "get_stepfunctions_client",
    
    # ClickHouse client
    "ClickHouseClient",
    "ClickHouseConnectionError",
    "ClickHouseQueryError",
    "get_clickhouse_connection",
    
    # Validators
    "CredentialValidator",
    "DataQualityValidator",
    "TenantConfigValidator",
    "ValidationError",
    "validate_connectwise_credentials",
    "validate_tenant_config",
    
    # Canonical mapping
    "CanonicalMapper",
    
    # Configuration
    "Config",
    "TenantConfig",
    "ServiceConfig",
    "ConnectWiseCredentials",
    
    # Environment
    "Environment",
    "EnvironmentConfig",
    "get_current_environment",
    "get_table_name",
    
    # Utilities
    "get_logger",
    "flatten_json",
    "get_timestamp",
    
    # Path and environment utilities
    "PathManager",
    "EnvironmentValidator"
]