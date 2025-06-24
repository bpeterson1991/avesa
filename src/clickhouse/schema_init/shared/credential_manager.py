"""
Secure Credential Management for AVESA Pipeline
Handles AWS credentials, ClickHouse credentials, and other sensitive data using AWS Secrets Manager
"""

import boto3
import json
import os
from typing import Dict, Optional, Any
from botocore.exceptions import ClientError
from .logger import get_logger

logger = get_logger(__name__)

class CredentialManager:
    """Secure credential management using AWS Secrets Manager and IAM roles"""
    
    def __init__(self, region_name: str = 'us-east-2'):
        self.region_name = region_name
        self.secrets_client = boto3.client('secretsmanager', region_name=region_name)
        self.sts_client = boto3.client('sts', region_name=region_name)
        
    def get_clickhouse_credentials(self, environment: str = 'dev') -> Dict[str, str]:
        """
        Retrieve ClickHouse credentials from AWS Secrets Manager
        
        Args:
            environment: Environment (dev, staging, prod)
            
        Returns:
            Dictionary containing ClickHouse connection parameters
        """
        secret_name = f"avesa/clickhouse/{environment}"
        
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            credentials = json.loads(response['SecretString'])
            
            logger.info(f"Successfully retrieved ClickHouse credentials for {environment}")
            return credentials
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.error(f"ClickHouse credentials not found: {secret_name}")
                raise ValueError(f"ClickHouse credentials not configured for environment: {environment}")
            else:
                logger.error(f"Error retrieving ClickHouse credentials: {e}")
                raise
                
    def store_clickhouse_credentials(self, environment: str, credentials: Dict[str, str]) -> bool:
        """
        Store ClickHouse credentials in AWS Secrets Manager
        
        Args:
            environment: Environment (dev, staging, prod)
            credentials: Dictionary containing ClickHouse connection parameters
            
        Returns:
            True if successful
        """
        secret_name = f"avesa/clickhouse/{environment}"
        
        try:
            # Try to update existing secret first
            try:
                self.secrets_client.update_secret(
                    SecretId=secret_name,
                    SecretString=json.dumps(credentials)
                )
                logger.info(f"Updated ClickHouse credentials for {environment}")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    # Create new secret if it doesn't exist
                    self.secrets_client.create_secret(
                        Name=secret_name,
                        Description=f"ClickHouse credentials for AVESA {environment} environment",
                        SecretString=json.dumps(credentials),
                        Tags=[
                            {'Key': 'Project', 'Value': 'AVESA'},
                            {'Key': 'Environment', 'Value': environment},
                            {'Key': 'Service', 'Value': 'ClickHouse'},
                            {'Key': 'ManagedBy', 'Value': 'CredentialManager'}
                        ]
                    )
                    logger.info(f"Created ClickHouse credentials for {environment}")
                else:
                    raise
                    
            return True
            
        except Exception as e:
            logger.error(f"Error storing ClickHouse credentials: {e}")
            raise
            
    def get_aws_credentials_for_service(self, service_name: str, environment: str = 'dev') -> Dict[str, str]:
        """
        Get AWS credentials for a specific service using IAM roles
        
        Args:
            service_name: Name of the service (e.g., 's3', 'lambda', 'clickhouse')
            environment: Environment (dev, staging, prod)
            
        Returns:
            Dictionary containing AWS credentials
        """
        try:
            # Use current session credentials (IAM role-based)
            session = boto3.Session()
            credentials = session.get_credentials()
            
            if not credentials:
                raise ValueError("No AWS credentials available")
                
            # Get current identity for logging
            identity = self.sts_client.get_caller_identity()
            logger.info(f"Using AWS credentials for {service_name} in {environment}: {identity.get('Arn', 'Unknown')}")
            
            return {
                'access_key_id': credentials.access_key,
                'secret_access_key': credentials.secret_key,
                'session_token': credentials.token if credentials.token else None,
                'region': self.region_name
            }
            
        except Exception as e:
            logger.error(f"Error getting AWS credentials for {service_name}: {e}")
            raise
            
    def validate_credentials(self, service: str, environment: str = 'dev') -> bool:
        """
        Validate that credentials are working for a specific service
        
        Args:
            service: Service to validate (clickhouse, s3, lambda)
            environment: Environment to validate
            
        Returns:
            True if credentials are valid
        """
        try:
            if service == 'clickhouse':
                return self._validate_clickhouse_credentials(environment)
            elif service == 's3':
                return self._validate_s3_credentials()
            elif service == 'lambda':
                return self._validate_lambda_credentials()
            else:
                logger.warning(f"Unknown service for validation: {service}")
                return False
                
        except Exception as e:
            logger.error(f"Credential validation failed for {service}: {e}")
            return False
            
    def _validate_clickhouse_credentials(self, environment: str) -> bool:
        """Validate ClickHouse credentials by attempting connection"""
        try:
            import clickhouse_connect
            
            credentials = self.get_clickhouse_credentials(environment)
            
            client = clickhouse_connect.get_client(
                host=credentials['host'],
                port=credentials.get('port', 8443),
                username=credentials['username'],
                password=credentials['password'],
                secure=credentials.get('secure', True),
                verify=credentials.get('verify', False)
            )
            
            # Test connection with simple query
            result = client.query("SELECT 1")
            logger.info("ClickHouse credential validation successful")
            return True
            
        except Exception as e:
            logger.error(f"ClickHouse credential validation failed: {e}")
            return False
            
    def _validate_s3_credentials(self) -> bool:
        """Validate S3 credentials by listing buckets"""
        try:
            s3_client = boto3.client('s3', region_name=self.region_name)
            s3_client.list_buckets()
            logger.info("S3 credential validation successful")
            return True
            
        except Exception as e:
            logger.error(f"S3 credential validation failed: {e}")
            return False
            
    def _validate_lambda_credentials(self) -> bool:
        """Validate Lambda credentials by listing functions"""
        try:
            lambda_client = boto3.client('lambda', region_name=self.region_name)
            lambda_client.list_functions(MaxItems=1)
            logger.info("Lambda credential validation successful")
            return True
            
        except Exception as e:
            logger.error(f"Lambda credential validation failed: {e}")
            return False
            
    def rotate_clickhouse_credentials(self, environment: str, new_password: str) -> bool:
        """
        Rotate ClickHouse credentials
        
        Args:
            environment: Environment to rotate credentials for
            new_password: New password to set
            
        Returns:
            True if successful
        """
        try:
            # Get current credentials
            current_creds = self.get_clickhouse_credentials(environment)
            
            # Update with new password
            new_creds = current_creds.copy()
            new_creds['password'] = new_password
            
            # Test new credentials before storing
            import clickhouse_connect
            client = clickhouse_connect.get_client(
                host=new_creds['host'],
                port=new_creds.get('port', 8443),
                username=new_creds['username'],
                password=new_creds['password'],
                secure=new_creds.get('secure', True),
                verify=new_creds.get('verify', False)
            )
            
            # Test connection
            client.query("SELECT 1")
            
            # Store new credentials
            self.store_clickhouse_credentials(environment, new_creds)
            
            logger.info(f"Successfully rotated ClickHouse credentials for {environment}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to rotate ClickHouse credentials: {e}")
            return False
            
    def get_environment_config(self) -> Dict[str, Any]:
        """
        Get environment-specific configuration
        
        Returns:
            Dictionary containing environment configuration
        """
        # Detect environment from AWS context
        try:
            identity = self.sts_client.get_caller_identity()
            account_id = identity['Account']
            
            # Map account IDs to environments (this should be configured)
            account_mapping = {
                # Add your actual account IDs here
                '123938354448': 'dev',
                # Add staging and prod account IDs
            }
            
            environment = account_mapping.get(account_id, 'dev')
            
            return {
                'environment': environment,
                'account_id': account_id,
                'region': self.region_name,
                'bucket_name': f'data-storage-msp-{environment}',
                'lambda_suffix': f'-{environment}' if environment != 'prod' else ''
            }
            
        except Exception as e:
            logger.warning(f"Could not detect environment, using defaults: {e}")
            return {
                'environment': 'dev',
                'account_id': 'unknown',
                'region': self.region_name,
                'bucket_name': 'data-storage-msp-dev',
                'lambda_suffix': '-dev'
            }


def get_credential_manager(region_name: str = 'us-east-2') -> CredentialManager:
    """
    Factory function to get a credential manager instance
    
    Args:
        region_name: AWS region name
        
    Returns:
        CredentialManager instance
    """
    return CredentialManager(region_name=region_name)


# Convenience functions for common operations
def get_clickhouse_connection_params(environment: str = 'dev') -> Dict[str, str]:
    """Get ClickHouse connection parameters for the specified environment"""
    manager = get_credential_manager()
    return manager.get_clickhouse_credentials(environment)


def get_aws_session_for_service(service_name: str, environment: str = 'dev') -> boto3.Session:
    """Get AWS session with appropriate credentials for a service"""
    manager = get_credential_manager()
    creds = manager.get_aws_credentials_for_service(service_name, environment)
    
    return boto3.Session(
        aws_access_key_id=creds['access_key_id'],
        aws_secret_access_key=creds['secret_access_key'],
        aws_session_token=creds.get('session_token'),
        region_name=creds['region']
    )


def validate_all_credentials(environment: str = 'dev') -> Dict[str, bool]:
    """Validate all service credentials for an environment"""
    manager = get_credential_manager()
    
    return {
        'clickhouse': manager.validate_credentials('clickhouse', environment),
        's3': manager.validate_credentials('s3', environment),
        'lambda': manager.validate_credentials('lambda', environment)
    }