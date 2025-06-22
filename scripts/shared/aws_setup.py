"""
Standardized AWS environment setup utilities.
Eliminates duplicate AWS profile and environment configuration across scripts.
"""
import os
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class AWSEnvironmentSetup:
    """Centralized AWS environment configuration."""
    
    @staticmethod
    def setup_aws_environment(profile_name: Optional[str] = None, region: Optional[str] = None) -> None:
        """
        Standardized AWS environment setup.
        
        Args:
            profile_name: AWS profile to use (optional)
            region: AWS region to use (optional)
        """
        # Set SDK configuration
        os.environ['AWS_SDK_LOAD_CONFIG'] = '1'
        
        # Set AWS profile if not already set
        if not os.environ.get('AWS_PROFILE'):
            default_profile = profile_name or 'AdministratorAccess-123938354448'
            os.environ['AWS_PROFILE'] = default_profile
            logger.info(f"Set AWS_PROFILE to: {default_profile}")
        
        # Set AWS region if not already set
        if not os.environ.get('AWS_REGION'):
            default_region = region or 'us-east-2'
            os.environ['AWS_REGION'] = default_region
            logger.info(f"Set AWS_REGION to: {default_region}")
    
    @staticmethod
    def setup_script_environment(script_name: str, profile_name: Optional[str] = None) -> Dict[str, str]:
        """
        Setup AWS environment for scripts with logging.
        
        Args:
            script_name: Name of the script for logging context
            profile_name: AWS profile to use (optional)
            
        Returns:
            Dictionary of AWS environment variables
        """
        logger.info(f"Setting up AWS environment for {script_name}")
        
        AWSEnvironmentSetup.setup_aws_environment(profile_name)
        
        # Return current AWS environment
        aws_env = {
            'AWS_PROFILE': os.environ.get('AWS_PROFILE', ''),
            'AWS_REGION': os.environ.get('AWS_REGION', ''),
            'AWS_SDK_LOAD_CONFIG': os.environ.get('AWS_SDK_LOAD_CONFIG', '')
        }
        
        logger.info(f"AWS environment configured: {aws_env}")
        return aws_env
    
    @staticmethod
    def validate_aws_setup() -> bool:
        """
        Validate that AWS environment is properly configured.
        
        Returns:
            True if AWS environment is valid
        """
        required_vars = ['AWS_SDK_LOAD_CONFIG']
        credential_vars = ['AWS_PROFILE', 'AWS_ACCESS_KEY_ID']
        
        # Check required configuration
        for var in required_vars:
            if not os.environ.get(var):
                logger.error(f"Missing required AWS environment variable: {var}")
                return False
        
        # Check for at least one credential method
        has_profile = bool(os.environ.get('AWS_PROFILE'))
        has_keys = bool(os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY'))
        
        if not (has_profile or has_keys):
            logger.error("No AWS credentials found (neither profile nor access keys)")
            return False
        
        logger.info("AWS environment validation successful")
        return True
    
    @staticmethod
    def get_aws_config() -> Dict[str, str]:
        """
        Get current AWS configuration.
        
        Returns:
            Dictionary of current AWS configuration
        """
        return {
            'profile': os.environ.get('AWS_PROFILE', 'default'),
            'region': os.environ.get('AWS_REGION', 'us-east-2'),
            'sdk_load_config': os.environ.get('AWS_SDK_LOAD_CONFIG', '1')
        }