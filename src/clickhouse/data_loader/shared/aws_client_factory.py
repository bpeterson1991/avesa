"""
AWS Client Factory - Centralized AWS client creation and configuration

This module provides:
- Centralized AWS client creation with consistent configuration
- Connection pooling and reuse for improved performance
- Environment-specific client configuration
- Comprehensive error handling and retry logic
"""

import boto3
import logging
from typing import Optional, Dict, Any, List
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class AWSClientFactory:
    """
    Centralized factory for creating and managing AWS clients with consistent configuration.
    
    Features:
    - Consistent retry and timeout configuration across all clients
    - Environment-specific optimizations
    - Connection pooling for improved performance
    - Comprehensive error handling
    """
    
    # Default configuration for all AWS clients
    DEFAULT_CONFIG = {
        'retries': {
            'max_attempts': 3,
            'mode': 'adaptive'
        },
        'max_pool_connections': 50,
        'connect_timeout': 30,
        'read_timeout': 30
    }
    
    # Service-specific configurations
    SERVICE_CONFIGS = {
        's3': {
            's3': {
                'max_concurrent_requests': 10,
                'max_bandwidth': None,
                'use_accelerate_endpoint': False,
                'addressing_style': 'auto'
            }
        },
        'dynamodb': {
            'max_pool_connections': 100  # DynamoDB benefits from more connections
        },
        'secretsmanager': {
            'max_pool_connections': 20  # Secrets Manager needs fewer connections
        }
    }
    
    def __init__(self, region_name: Optional[str] = None):
        """
        Initialize the AWS Client Factory.
        
        Args:
            region_name: AWS region name. If None, uses default region.
        """
        self.region_name = region_name
        self._clients: Dict[str, Any] = {}
        
    def _get_client_config(self, service_name: str) -> BotoConfig:
        """
        Get optimized configuration for a specific AWS service.
        
        Args:
            service_name: Name of the AWS service
            
        Returns:
            BotoConfig object with service-specific optimizations
        """
        # Start with default configuration
        config_dict = self.DEFAULT_CONFIG.copy()
        
        # Add region if specified
        if self.region_name:
            config_dict['region_name'] = self.region_name
            
        # Apply service-specific configurations
        if service_name in self.SERVICE_CONFIGS:
            config_dict.update(self.SERVICE_CONFIGS[service_name])
            
        return BotoConfig(**config_dict)
    
    def get_client(self, service_name: str, region_name: Optional[str] = None) -> Any:
        """
        Get an AWS client for the specified service with optimized configuration.
        
        Args:
            service_name: Name of the AWS service (e.g., 's3', 'dynamodb', 'secretsmanager')
            region_name: Override region for this specific client
            
        Returns:
            Configured AWS client
            
        Raises:
            ClientError: If client creation fails
            NoCredentialsError: If AWS credentials are not available
        """
        # Use provided region or fall back to instance region
        client_region = region_name or self.region_name
        
        # Create cache key
        cache_key = f"{service_name}_{client_region or 'default'}"
        
        # Return cached client if available
        if cache_key in self._clients:
            return self._clients[cache_key]
        
        try:
            # Get service-specific configuration
            config = self._get_client_config(service_name)
            
            # Create the client
            client = boto3.client(service_name, config=config)
            
            # Cache the client for reuse
            self._clients[cache_key] = client
            
            logger.debug(f"Created AWS {service_name} client for region {client_region or 'default'}")
            return client
            
        except NoCredentialsError:
            logger.error(f"AWS credentials not found for {service_name} client")
            raise
        except ClientError as e:
            logger.error(f"Failed to create {service_name} client: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating {service_name} client: {e}")
            raise ClientError(
                error_response={'Error': {'Code': 'ClientCreationError', 'Message': str(e)}},
                operation_name='CreateClient'
            )
    
    def get_all_clients(self, region_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get all commonly used AWS clients in a single call.
        
        Args:
            region_name: Override region for all clients
            
        Returns:
            Dictionary mapping service names to configured clients
        """
        services = ['dynamodb', 's3', 'secretsmanager', 'cloudwatch', 'lambda', 'stepfunctions']
        clients = {}
        
        for service in services:
            try:
                clients[service] = self.get_client(service, region_name)
            except Exception as e:
                logger.warning(f"Failed to create {service} client: {e}")
                # Continue with other clients even if one fails
                
        return clients
    
    def get_client_bundle(self, services: List[str], region_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get a specific bundle of AWS clients.
        
        Args:
            services: List of AWS service names to create clients for
            region_name: Override region for all clients
            
        Returns:
            Dictionary mapping service names to configured clients
        """
        clients = {}
        
        for service in services:
            try:
                clients[service] = self.get_client(service, region_name)
            except Exception as e:
                logger.error(f"Failed to create {service} client: {e}")
                # Re-raise for bundle requests since all services are typically required
                raise
                
        return clients
    
    def clear_cache(self):
        """Clear all cached clients. Useful for testing or credential rotation."""
        self._clients.clear()
        logger.debug("Cleared AWS client cache")
    
    @classmethod
    def create_default_factory(cls, region_name: Optional[str] = None) -> 'AWSClientFactory':
        """
        Create a default AWS client factory instance.
        
        Args:
            region_name: AWS region name
            
        Returns:
            Configured AWSClientFactory instance
        """
        return cls(region_name=region_name)


# Convenience functions for backward compatibility with existing code
def get_dynamodb_client(region_name: Optional[str] = None):
    """Get DynamoDB client with optimized configuration."""
    factory = AWSClientFactory(region_name)
    return factory.get_client('dynamodb')


def get_s3_client(region_name: Optional[str] = None):
    """Get S3 client with optimized configuration."""
    factory = AWSClientFactory(region_name)
    return factory.get_client('s3')


def get_secrets_client(region_name: Optional[str] = None):
    """Get Secrets Manager client with optimized configuration."""
    factory = AWSClientFactory(region_name)
    return factory.get_client('secretsmanager')


def get_cloudwatch_client(region_name: Optional[str] = None):
    """Get CloudWatch client with optimized configuration."""
    factory = AWSClientFactory(region_name)
    return factory.get_client('cloudwatch')


def get_lambda_client(region_name: Optional[str] = None):
    """Get Lambda client with optimized configuration."""
    factory = AWSClientFactory(region_name)
    return factory.get_client('lambda')


def get_stepfunctions_client(region_name: Optional[str] = None):
    """Get Step Functions client with optimized configuration."""
    factory = AWSClientFactory(region_name)
    return factory.get_client('stepfunctions')