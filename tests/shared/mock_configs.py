"""
Shared mock configurations for AVESA tests.
Eliminates duplicate mock environment configurations across test files.
"""
from typing import Dict, Any
from unittest.mock import Mock

class MockEnvironmentConfigs:
    """Centralized mock environment configurations."""
    
    @staticmethod
    def get_standard_environment_config() -> Dict[str, Any]:
        """
        Standard mock environment configuration for tests.
        
        Returns:
            Dictionary with mock environment configuration
        """
        return {
            "environments": {
                "dev": {
                    "account": "123456789012",
                    "region": "us-east-2",
                    "bucket_name": "avesa-data-dev",
                    "table_suffix": "-dev",
                    "lambda_memory": 512,
                    "lambda_timeout": 300,
                    "clickhouse_host": "test-clickhouse.com",
                    "clickhouse_port": 8443,
                    "clickhouse_database": "avesa_dev",
                    "clickhouse_username": "test_user",
                    "clickhouse_password": "test_password"
                },
                "staging": {
                    "account": "123456789012",
                    "region": "us-east-2",
                    "bucket_name": "avesa-data-staging",
                    "table_suffix": "-staging",
                    "lambda_memory": 1024,
                    "lambda_timeout": 600,
                    "clickhouse_host": "staging-clickhouse.com",
                    "clickhouse_port": 8443,
                    "clickhouse_database": "avesa_staging",
                    "clickhouse_username": "staging_user",
                    "clickhouse_password": "staging_password"
                },
                "prod": {
                    "account": "123456789012",
                    "region": "us-east-2",
                    "bucket_name": "avesa-data-prod",
                    "table_suffix": "",
                    "lambda_memory": 2048,
                    "lambda_timeout": 900,
                    "clickhouse_host": "prod-clickhouse.com",
                    "clickhouse_port": 8443,
                    "clickhouse_database": "avesa_prod",
                    "clickhouse_username": "prod_user",
                    "clickhouse_password": "prod_password"
                }
            }
        }
    
    @staticmethod
    def get_lambda_environment_variables(env_name: str = "dev") -> Dict[str, str]:
        """
        Get mock Lambda environment variables for testing.
        
        Args:
            env_name: Environment name (dev, staging, prod)
            
        Returns:
            Dictionary of Lambda environment variables
        """
        base_vars = {
            'ENVIRONMENT': env_name,
            'AWS_REGION': 'us-east-2',
            'TENANT_SERVICES_TABLE': f'tenant-services-{env_name}',
            'LAST_UPDATED_TABLE': f'last-updated-{env_name}',
            'BUCKET_NAME': f'avesa-data-{env_name}',
            'CLICKHOUSE_SECRET_NAME': f'clickhouse-credentials-{env_name}'
        }
        
        if env_name == "prod":
            base_vars['TENANT_SERVICES_TABLE'] = 'tenant-services'
            base_vars['LAST_UPDATED_TABLE'] = 'last-updated'
            base_vars['BUCKET_NAME'] = 'avesa-data-prod'
        
        return base_vars
    
    @staticmethod
    def get_mock_aws_credentials() -> Dict[str, str]:
        """
        Get mock AWS credentials for testing.
        
        Returns:
            Dictionary of mock AWS credentials
        """
        return {
            'AWS_ACCESS_KEY_ID': 'test-access-key',
            'AWS_SECRET_ACCESS_KEY': 'test-secret-key',
            'AWS_SESSION_TOKEN': 'test-session-token',
            'AWS_REGION': 'us-east-2',
            'AWS_PROFILE': 'test-profile'
        }
    
    @staticmethod
    def get_mock_clickhouse_credentials() -> Dict[str, str]:
        """
        Get mock ClickHouse credentials for testing.
        
        Returns:
            Dictionary of mock ClickHouse credentials
        """
        return {
            'host': 'test-clickhouse.com',
            'port': '8443',
            'username': 'test_user',
            'password': 'test_password',
            'database': 'avesa_test',
            'secure': 'true'
        }

class MockAWSClients:
    """Factory for creating mock AWS clients."""
    
    @staticmethod
    def create_mock_s3_client() -> Mock:
        """Create mock S3 client with standard methods."""
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        mock_s3.get_object.return_value = {'Body': Mock()}
        mock_s3.put_object.return_value = {'ETag': 'test-etag'}
        return mock_s3
    
    @staticmethod
    def create_mock_dynamodb_client() -> Mock:
        """Create mock DynamoDB client with standard methods."""
        mock_dynamodb = Mock()
        mock_dynamodb.get_item.return_value = {'Item': {}}
        mock_dynamodb.put_item.return_value = {}
        mock_dynamodb.scan.return_value = {'Items': []}
        return mock_dynamodb
    
    @staticmethod
    def create_mock_secrets_client() -> Mock:
        """Create mock Secrets Manager client with standard methods."""
        mock_secrets = Mock()
        mock_secrets.get_secret_value.return_value = {
            'SecretString': '{"username": "test", "password": "test"}'
        }
        return mock_secrets
    
    @staticmethod
    def create_all_mock_clients() -> Dict[str, Mock]:
        """Create all mock AWS clients."""
        return {
            's3': MockAWSClients.create_mock_s3_client(),
            'dynamodb': MockAWSClients.create_mock_dynamodb_client(),
            'secrets': MockAWSClients.create_mock_secrets_client()
        }

class MockClickHouseClient:
    """Factory for creating mock ClickHouse clients."""
    
    @staticmethod
    def create_mock_client() -> Mock:
        """Create mock ClickHouse client with standard methods."""
        mock_client = Mock()
        mock_client.query.return_value = []
        mock_client.insert.return_value = None
        mock_client.command.return_value = "OK"
        mock_client.ping.return_value = True
        return mock_client