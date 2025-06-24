"""
Environment variable validation utilities.
Consolidates duplicate validation logic across Lambda functions and scripts.
"""
import os
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class EnvironmentValidator:
    """Centralized environment variable validation."""
    
    @staticmethod
    def validate_required_vars(required_vars: List[str], context: str = "Application") -> Dict[str, str]:
        """
        Validate that required environment variables are set.
        
        Args:
            required_vars: List of required environment variable names
            context: Context name for error messages
            
        Returns:
            Dictionary of validated environment variables
            
        Raises:
            EnvironmentError: If any required variables are missing
        """
        missing_vars = []
        env_values = {}
        
        for var in required_vars:
            value = os.environ.get(var)
            if not value:
                missing_vars.append(var)
            else:
                env_values[var] = value
        
        if missing_vars:
            error_msg = f"{context} missing required environment variables: {missing_vars}"
            logger.error(error_msg)
            raise EnvironmentError(error_msg)
        
        logger.info(f"{context} environment validation successful")
        return env_values
    
    @staticmethod
    def validate_aws_credentials() -> bool:
        """
        Validate AWS credential configuration.
        
        Returns:
            True if AWS credentials are properly configured
        """
        aws_vars = ['AWS_REGION']
        optional_vars = ['AWS_PROFILE', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
        
        # Check for required AWS region
        if not os.environ.get('AWS_REGION'):
            logger.warning("AWS_REGION not set, using default region")
            os.environ['AWS_REGION'] = 'us-east-2'
        
        # Check for credential configuration
        has_profile = bool(os.environ.get('AWS_PROFILE'))
        has_keys = bool(os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY'))
        
        if not (has_profile or has_keys):
            logger.warning("No AWS credentials found in environment")
            return False
        
        logger.info("AWS credentials validation successful")
        return True
    
    @staticmethod
    def validate_clickhouse_env() -> bool:
        """
        Validate ClickHouse environment setup.
        
        Returns:
            True if ClickHouse environment is properly configured
        """
        required_vars = ['CLICKHOUSE_SECRET_NAME']
        
        try:
            EnvironmentValidator.validate_required_vars(required_vars, "ClickHouse")
            return True
        except EnvironmentError:
            logger.warning("ClickHouse environment not fully configured")
            return False
    
    @staticmethod
    def get_standard_lambda_env() -> Dict[str, str]:
        """
        Get standard Lambda environment variables.
        
        Returns:
            Dictionary of standard Lambda environment variables
        """
        required_vars = [
            'ENVIRONMENT',
            'AWS_REGION',
            'TENANT_SERVICES_TABLE',
            'LAST_UPDATED_TABLE',
            'BUCKET_NAME'
        ]
        
        return EnvironmentValidator.validate_required_vars(required_vars, "Lambda")
    
    @staticmethod
    def setup_development_env() -> None:
        """Setup development environment defaults."""
        defaults = {
            'AWS_REGION': 'us-east-2',
            'ENVIRONMENT': 'dev',
            'AWS_SDK_LOAD_CONFIG': '1'
        }
        
        for key, value in defaults.items():
            if not os.environ.get(key):
                os.environ[key] = value
                logger.info(f"Set default environment variable: {key}={value}")