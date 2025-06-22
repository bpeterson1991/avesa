"""
Tests for shared mock configurations.
"""
import pytest
import json
from unittest.mock import Mock

from tests.shared.mock_configs import MockEnvironmentConfigs, MockAWSClients, MockClickHouseClient


class TestMockEnvironmentConfigs:
    """Test mock environment configurations."""

    def test_get_standard_environment_config(self):
        """Test standard environment configuration structure."""
        config = MockEnvironmentConfigs.get_standard_environment_config()
        
        # Check top-level structure
        assert 'environments' in config
        assert isinstance(config['environments'], dict)
        
        # Check all required environments exist
        required_envs = ['dev', 'staging', 'prod']
        for env in required_envs:
            assert env in config['environments']
            
        # Check dev environment structure
        dev_config = config['environments']['dev']
        required_fields = [
            'account', 'region', 'bucket_name', 'table_suffix',
            'lambda_memory', 'lambda_timeout', 'clickhouse_host',
            'clickhouse_port', 'clickhouse_database', 'clickhouse_username',
            'clickhouse_password'
        ]
        
        for field in required_fields:
            assert field in dev_config
            assert dev_config[field] is not None

    def test_environment_config_consistency(self):
        """Test that environment configurations are consistent."""
        config = MockEnvironmentConfigs.get_standard_environment_config()
        environments = config['environments']
        
        # Check that all environments have the same structure
        dev_keys = set(environments['dev'].keys())
        staging_keys = set(environments['staging'].keys())
        prod_keys = set(environments['prod'].keys())
        
        assert dev_keys == staging_keys == prod_keys
        
        # Check that account and region are consistent
        for env_name, env_config in environments.items():
            assert env_config['account'] == '123456789012'
            assert env_config['region'] == 'us-east-2'

    def test_environment_specific_values(self):
        """Test environment-specific configuration values."""
        config = MockEnvironmentConfigs.get_standard_environment_config()
        environments = config['environments']
        
        # Check dev environment
        dev = environments['dev']
        assert dev['bucket_name'] == 'avesa-data-dev'
        assert dev['table_suffix'] == '-dev'
        assert dev['lambda_memory'] == 512
        assert dev['clickhouse_database'] == 'avesa_dev'
        
        # Check staging environment
        staging = environments['staging']
        assert staging['bucket_name'] == 'avesa-data-staging'
        assert staging['table_suffix'] == '-staging'
        assert staging['lambda_memory'] == 1024
        assert staging['clickhouse_database'] == 'avesa_staging'
        
        # Check prod environment
        prod = environments['prod']
        assert prod['bucket_name'] == 'avesa-data-prod'
        assert prod['table_suffix'] == ''
        assert prod['lambda_memory'] == 2048
        assert prod['clickhouse_database'] == 'avesa_prod'

    def test_get_lambda_environment_variables_dev(self):
        """Test Lambda environment variables for dev environment."""
        env_vars = MockEnvironmentConfigs.get_lambda_environment_variables('dev')
        
        expected_vars = {
            'ENVIRONMENT': 'dev',
            'AWS_REGION': 'us-east-2',
            'TENANT_SERVICES_TABLE': 'tenant-services-dev',
            'LAST_UPDATED_TABLE': 'last-updated-dev',
            'BUCKET_NAME': 'avesa-data-dev',
            'CLICKHOUSE_SECRET_NAME': 'clickhouse-credentials-dev'
        }
        
        assert env_vars == expected_vars

    def test_get_lambda_environment_variables_prod(self):
        """Test Lambda environment variables for prod environment."""
        env_vars = MockEnvironmentConfigs.get_lambda_environment_variables('prod')
        
        expected_vars = {
            'ENVIRONMENT': 'prod',
            'AWS_REGION': 'us-east-2',
            'TENANT_SERVICES_TABLE': 'tenant-services',
            'LAST_UPDATED_TABLE': 'last-updated',
            'BUCKET_NAME': 'avesa-data-prod',
            'CLICKHOUSE_SECRET_NAME': 'clickhouse-credentials-prod'
        }
        
        assert env_vars == expected_vars

    def test_get_lambda_environment_variables_default(self):
        """Test Lambda environment variables with default environment."""
        env_vars = MockEnvironmentConfigs.get_lambda_environment_variables()
        
        # Should default to dev
        assert env_vars['ENVIRONMENT'] == 'dev'
        assert env_vars['TENANT_SERVICES_TABLE'] == 'tenant-services-dev'

    def test_get_mock_aws_credentials(self):
        """Test mock AWS credentials structure."""
        credentials = MockEnvironmentConfigs.get_mock_aws_credentials()
        
        required_fields = [
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN',
            'AWS_REGION', 'AWS_PROFILE'
        ]
        
        for field in required_fields:
            assert field in credentials
            assert credentials[field] is not None
            assert isinstance(credentials[field], str)

    def test_get_mock_clickhouse_credentials(self):
        """Test mock ClickHouse credentials structure."""
        credentials = MockEnvironmentConfigs.get_mock_clickhouse_credentials()
        
        required_fields = ['host', 'port', 'username', 'password', 'database', 'secure']
        
        for field in required_fields:
            assert field in credentials
            assert credentials[field] is not None
            
        # Check specific values
        assert credentials['host'] == 'test-clickhouse.com'
        assert credentials['port'] == '8443'
        assert credentials['database'] == 'avesa_test'
        assert credentials['secure'] == 'true'


class TestMockAWSClients:
    """Test mock AWS clients factory."""

    def test_create_mock_s3_client(self):
        """Test S3 client mock creation."""
        mock_s3 = MockAWSClients.create_mock_s3_client()
        
        assert isinstance(mock_s3, Mock)
        
        # Test that standard methods are configured
        assert hasattr(mock_s3, 'list_objects_v2')
        assert hasattr(mock_s3, 'get_object')
        assert hasattr(mock_s3, 'put_object')
        
        # Test return values
        assert mock_s3.list_objects_v2.return_value == {'Contents': []}
        assert mock_s3.put_object.return_value == {'ETag': 'test-etag'}

    def test_create_mock_dynamodb_client(self):
        """Test DynamoDB client mock creation."""
        mock_dynamodb = MockAWSClients.create_mock_dynamodb_client()
        
        assert isinstance(mock_dynamodb, Mock)
        
        # Test that standard methods are configured
        assert hasattr(mock_dynamodb, 'get_item')
        assert hasattr(mock_dynamodb, 'put_item')
        assert hasattr(mock_dynamodb, 'scan')
        
        # Test return values
        assert mock_dynamodb.get_item.return_value == {'Item': {}}
        assert mock_dynamodb.put_item.return_value == {}
        assert mock_dynamodb.scan.return_value == {'Items': []}

    def test_create_mock_secrets_client(self):
        """Test Secrets Manager client mock creation."""
        mock_secrets = MockAWSClients.create_mock_secrets_client()
        
        assert isinstance(mock_secrets, Mock)
        assert hasattr(mock_secrets, 'get_secret_value')
        
        # Test return value structure
        result = mock_secrets.get_secret_value.return_value
        assert 'SecretString' in result
        
        # Test that SecretString contains valid JSON
        secret_data = json.loads(result['SecretString'])
        assert 'username' in secret_data
        assert 'password' in secret_data

    def test_create_all_mock_clients(self):
        """Test creation of all mock clients."""
        all_clients = MockAWSClients.create_all_mock_clients()
        
        assert isinstance(all_clients, dict)
        assert 's3' in all_clients
        assert 'dynamodb' in all_clients
        assert 'secrets' in all_clients
        
        # Test that each client is a Mock
        for client_name, client in all_clients.items():
            assert isinstance(client, Mock)

    def test_mock_clients_independence(self):
        """Test that mock clients are independent instances."""
        client1 = MockAWSClients.create_mock_s3_client()
        client2 = MockAWSClients.create_mock_s3_client()
        
        assert client1 is not client2
        
        # Test that they have independent call histories
        client1.get_object('bucket1', 'key1')
        client2.get_object('bucket2', 'key2')
        
        # Each should have only their own call
        assert client1.get_object.call_count == 1
        assert client2.get_object.call_count == 1


class TestMockClickHouseClient:
    """Test mock ClickHouse client factory."""

    def test_create_mock_client(self):
        """Test ClickHouse client mock creation."""
        mock_client = MockClickHouseClient.create_mock_client()
        
        assert isinstance(mock_client, Mock)
        
        # Test that standard methods are configured
        assert hasattr(mock_client, 'query')
        assert hasattr(mock_client, 'insert')
        assert hasattr(mock_client, 'command')
        assert hasattr(mock_client, 'ping')
        
        # Test return values
        assert mock_client.query.return_value == []
        assert mock_client.insert.return_value is None
        assert mock_client.command.return_value == "OK"
        assert mock_client.ping.return_value is True

    def test_mock_client_methods_callable(self):
        """Test that mock client methods are callable."""
        mock_client = MockClickHouseClient.create_mock_client()
        
        # Test that methods can be called
        query_result = mock_client.query("SELECT 1")
        assert query_result == []
        
        insert_result = mock_client.insert("table", [])
        assert insert_result is None
        
        command_result = mock_client.command("SHOW TABLES")
        assert command_result == "OK"
        
        ping_result = mock_client.ping()
        assert ping_result is True

    def test_mock_client_independence(self):
        """Test that mock clients are independent instances."""
        client1 = MockClickHouseClient.create_mock_client()
        client2 = MockClickHouseClient.create_mock_client()
        
        assert client1 is not client2
        
        # Modify one client and ensure the other is unaffected
        client1.query.return_value = [{'test': 'data'}]
        assert client2.query.return_value == []


class TestMockConfigsIntegration:
    """Test integration between different mock configurations."""

    def test_environment_and_lambda_vars_consistency(self):
        """Test consistency between environment config and Lambda vars."""
        env_config = MockEnvironmentConfigs.get_standard_environment_config()
        lambda_vars = MockEnvironmentConfigs.get_lambda_environment_variables('dev')
        
        dev_config = env_config['environments']['dev']
        
        # Check consistency
        assert lambda_vars['BUCKET_NAME'] == dev_config['bucket_name']
        assert lambda_vars['AWS_REGION'] == dev_config['region']

    def test_clickhouse_credentials_consistency(self):
        """Test consistency between environment config and ClickHouse credentials."""
        env_config = MockEnvironmentConfigs.get_standard_environment_config()
        ch_credentials = MockEnvironmentConfigs.get_mock_clickhouse_credentials()
        
        dev_config = env_config['environments']['dev']
        
        # Check that both have valid structure (they may have same values for testing)
        assert 'host' in ch_credentials
        assert 'database' in ch_credentials
        assert 'clickhouse_host' in dev_config
        assert 'clickhouse_database' in dev_config
        
        # Both should be valid test values
        assert ch_credentials['host'] is not None
        assert ch_credentials['database'] is not None

    def test_all_mocks_work_together(self):
        """Test that all mock configurations can be used together."""
        # Get all mock configurations
        env_config = MockEnvironmentConfigs.get_standard_environment_config()
        lambda_vars = MockEnvironmentConfigs.get_lambda_environment_variables('dev')
        aws_credentials = MockEnvironmentConfigs.get_mock_aws_credentials()
        ch_credentials = MockEnvironmentConfigs.get_mock_clickhouse_credentials()
        
        # Get all mock clients
        aws_clients = MockAWSClients.create_all_mock_clients()
        ch_client = MockClickHouseClient.create_mock_client()
        
        # Test that everything is properly structured
        assert isinstance(env_config, dict)
        assert isinstance(lambda_vars, dict)
        assert isinstance(aws_credentials, dict)
        assert isinstance(ch_credentials, dict)
        assert isinstance(aws_clients, dict)
        assert isinstance(ch_client, Mock)
        
        # Test that we can use them together without conflicts
        combined_config = {
            'environment': env_config,
            'lambda_vars': lambda_vars,
            'aws_credentials': aws_credentials,
            'clickhouse_credentials': ch_credentials,
            'aws_clients': aws_clients,
            'clickhouse_client': ch_client
        }
        
        assert len(combined_config) == 6


if __name__ == '__main__':
    pytest.main([__file__, '-v'])