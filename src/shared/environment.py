"""
Environment Configuration Management
Centralized environment configuration for the AVESA Multi-Tenant Data Pipeline

This module provides:
- Centralized environment configuration loading
- Type-safe environment configuration with dataclasses
- Environment-specific table name generation
- Lambda environment variable generation for CDK
- Configuration validation and error handling
"""

import json
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class EnvironmentConfig:
    """
    Type-safe environment configuration dataclass.
    
    Attributes:
        account: AWS account ID for the environment
        region: AWS region for deployment
        bucket_name: S3 bucket name for data storage
        table_suffix: Suffix for DynamoDB table names
        lambda_memory: Default Lambda memory allocation (MB)
        lambda_timeout: Default Lambda timeout (seconds)
    """
    account: str
    region: str
    bucket_name: str
    table_suffix: str
    lambda_memory: int
    lambda_timeout: int


class Environment:
    """
    Environment configuration manager for AVESA pipeline.
    
    Provides centralized access to environment-specific configuration,
    table names, and Lambda environment variables.
    """
    
    _config_cache: Optional[Dict[str, Any]] = None
    
    @classmethod
    def _load_config(cls) -> Dict[str, Any]:
        """
        Load environment configuration from JSON file with caching.
        
        Returns:
            Dict containing the complete environment configuration
            
        Raises:
            FileNotFoundError: If environment_config.json is not found
            json.JSONDecodeError: If configuration file is invalid JSON
        """
        if cls._config_cache is None:
            # Determine config file path relative to this module
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, "..", "..", "infrastructure", "environment_config.json")
            config_path = os.path.normpath(config_path)
            
            try:
                with open(config_path, 'r') as f:
                    cls._config_cache = json.load(f)
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Environment configuration file not found at: {config_path}. "
                    f"Please ensure infrastructure/environment_config.json exists."
                )
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(
                    f"Invalid JSON in environment configuration file: {e}",
                    e.doc, e.pos
                )
        
        return cls._config_cache
    
    @classmethod
    def get_config(cls, env_name: str) -> EnvironmentConfig:
        """
        Get typed environment configuration for the specified environment.
        
        Args:
            env_name: Environment name (dev, staging, prod)
            
        Returns:
            EnvironmentConfig object with typed configuration
            
        Raises:
            ValueError: If environment name is not found in configuration
            KeyError: If required configuration keys are missing
        """
        config = cls._load_config()
        
        if env_name not in config.get("environments", {}):
            available_envs = list(config.get("environments", {}).keys())
            raise ValueError(
                f"Environment '{env_name}' not found in configuration. "
                f"Available environments: {available_envs}"
            )
        
        env_config = config["environments"][env_name]
        
        # Validate required keys
        required_keys = ["account", "region", "bucket_name", "table_suffix", "lambda_memory", "lambda_timeout"]
        missing_keys = [key for key in required_keys if key not in env_config]
        if missing_keys:
            raise KeyError(
                f"Missing required configuration keys for environment '{env_name}': {missing_keys}"
            )
        
        return EnvironmentConfig(
            account=env_config["account"],
            region=env_config["region"],
            bucket_name=env_config["bucket_name"],
            table_suffix=env_config["table_suffix"],
            lambda_memory=env_config["lambda_memory"],
            lambda_timeout=env_config["lambda_timeout"]
        )
    
    @classmethod
    def get_table_names(cls, env_name: str) -> Dict[str, str]:
        """
        Get environment-specific DynamoDB table names.
        
        Args:
            env_name: Environment name (dev, staging, prod)
            
        Returns:
            Dict mapping table types to environment-specific table names
        """
        config = cls.get_config(env_name)
        suffix = config.table_suffix
        
        return {
            "tenant_services": f"TenantServices{suffix}",
            "last_updated": f"LastUpdated{suffix}",
            "processing_jobs": f"ProcessingJobs{suffix}",
            "chunk_progress": f"ChunkProgress{suffix}",
            "data_quality_metrics": f"DataQualityMetrics{suffix}",
            "pipeline_metrics": f"PipelineMetrics{suffix}"
        }
    
    @classmethod
    def get_lambda_env_vars(cls, env_name: str) -> Dict[str, str]:
        """
        Get Lambda environment variables for CDK deployment.
        
        Args:
            env_name: Environment name (dev, staging, prod)
            
        Returns:
            Dict of environment variables for Lambda functions
        """
        config = cls.get_config(env_name)
        table_names = cls.get_table_names(env_name)
        
        return {
            "ENVIRONMENT": env_name,
            "AWS_REGION": config.region,
            "DATA_BUCKET": config.bucket_name,
            "TENANT_SERVICES_TABLE": table_names["tenant_services"],
            "LAST_UPDATED_TABLE": table_names["last_updated"],
            "PROCESSING_JOBS_TABLE": table_names["processing_jobs"],
            "CHUNK_PROGRESS_TABLE": table_names["chunk_progress"],
            "DATA_QUALITY_METRICS_TABLE": table_names["data_quality_metrics"],
            "PIPELINE_METRICS_TABLE": table_names["pipeline_metrics"],
            "TABLE_SUFFIX": config.table_suffix
        }
    
    @classmethod
    def get_deployment_profiles(cls) -> Dict[str, str]:
        """
        Get AWS profile to environment mapping.
        
        Returns:
            Dict mapping AWS profile names to environment names
        """
        config = cls._load_config()
        return config.get("deployment_profiles", {})
    
    @classmethod
    def get_environment_by_profile(cls, profile_name: str) -> Optional[str]:
        """
        Get environment name by AWS profile name.
        
        Args:
            profile_name: AWS profile name
            
        Returns:
            Environment name if found, None otherwise
        """
        profiles = cls.get_deployment_profiles()
        return profiles.get(profile_name)
    
    @classmethod
    def get_environment_by_account(cls, account_id: str) -> Optional[str]:
        """
        Get environment name by AWS account ID.
        
        Args:
            account_id: AWS account ID
            
        Returns:
            Environment name if found, None otherwise
        """
        config = cls._load_config()
        
        for env_name, env_config in config.get("environments", {}).items():
            if env_config.get("account") == account_id:
                return env_name
        
        return None
    
    @classmethod
    def validate_environment(cls, env_name: str) -> bool:
        """
        Validate that an environment name is valid and properly configured.
        
        Args:
            env_name: Environment name to validate
            
        Returns:
            True if environment is valid and properly configured
            
        Raises:
            ValueError: If environment is invalid
            KeyError: If required configuration is missing
        """
        try:
            cls.get_config(env_name)
            return True
        except (ValueError, KeyError):
            raise
    
    @classmethod
    def list_environments(cls) -> list[str]:
        """
        Get list of all available environment names.
        
        Returns:
            List of environment names
        """
        config = cls._load_config()
        return list(config.get("environments", {}).keys())
    
    @classmethod
    def clear_cache(cls) -> None:
        """
        Clear the configuration cache.
        Useful for testing or when configuration file changes.
        """
        cls._config_cache = None


# Convenience functions for common use cases
def get_current_environment() -> Optional[str]:
    """
    Attempt to detect current environment from environment variables.
    
    Returns:
        Environment name if detected, None otherwise
    """
    # Check for explicit environment variable
    env_name = os.environ.get("ENVIRONMENT")
    if env_name:
        return env_name
    
    # Check for CDK context environment
    env_name = os.environ.get("CDK_ENVIRONMENT")
    if env_name:
        return env_name
    
    return None


def get_table_name(table_type: str, env_name: Optional[str] = None) -> str:
    """
    Get a specific table name for the current or specified environment.
    
    Args:
        table_type: Type of table (tenant_services, last_updated, etc.)
        env_name: Environment name (auto-detected if not provided)
        
    Returns:
        Environment-specific table name
        
    Raises:
        ValueError: If environment cannot be determined or table type is invalid
    """
    if env_name is None:
        env_name = get_current_environment()
        if env_name is None:
            raise ValueError(
                "Environment name must be provided or set in ENVIRONMENT variable"
            )
    
    table_names = Environment.get_table_names(env_name)
    
    if table_type not in table_names:
        available_types = list(table_names.keys())
        raise ValueError(
            f"Invalid table type '{table_type}'. Available types: {available_types}"
        )
    
    return table_names[table_type]