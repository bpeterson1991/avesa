"""
Tests for ClickHouse Client

This module tests the centralized ClickHouse connection and query management functionality.
"""

import pytest
import json
import os
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timezone

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from shared.clickhouse_client import (
    ClickHouseClient,
    ClickHouseConnectionError,
    ClickHouseQueryError,
    get_clickhouse_connection
)


class TestClickHouseClient:
    """Test cases for ClickHouseClient class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.secret_name = 'test-clickhouse-secret'
        self.region_name = 'us-east-1'
    
    @patch('shared.clickhouse_client.clickhouse_connect')
    def test_init_success(self, mock_clickhouse_connect):
        """Test successful ClickHouse client initialization."""
        client = ClickHouseClient(self.secret_name, self.region_name)
        
        assert client.secret_name == self.secret_name
        assert client.region_name == self.region_name
        assert client._client is None
        assert client._credentials is None
    
    def test_init_without_clickhouse_connect(self):
        """Test initialization when clickhouse-connect is not available."""
        with patch('shared.clickhouse_client.clickhouse_connect', None):
            with pytest.raises(ImportError) as exc_info:
                ClickHouseClient(self.secret_name)
            
            assert "clickhouse-connect package is required" in str(exc_info.value)
    
    @patch('shared.clickhouse_client.AWSClientFactory')
    def test_get_credentials_success(self, mock_aws_factory_class):
        """Test successful credential retrieval."""
        # Mock AWS factory and secrets client
        mock_factory = Mock()
        mock_secrets_client = Mock()
        mock_aws_factory_class.return_value = mock_factory
        mock_factory.get_client.return_value = mock_secrets_client
        
        # Mock secrets response
        secret_data = {
            'host': 'test-host.clickhouse.cloud',
            'username': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
        mock_secrets_client.get_secret_value.return_value = {
            'SecretString': json.dumps(secret_data)
        }
        
        client = ClickHouseClient(self.secret_name, self.region_name)
        credentials = client._get_credentials()
        
        assert credentials == secret_data
        assert client._credentials == secret_data
        mock_secrets_client.get_secret_value.assert_called_once_with(SecretId=self.secret_name)
    
    @patch('shared.clickhouse_client.AWSClientFactory')
    def test_get_credentials_missing_required_fields(self, mock_aws_factory_class):
        """Test credential retrieval with missing required fields."""
        # Mock AWS factory and secrets client
        mock_factory = Mock()
        mock_secrets_client = Mock()
        mock_aws_factory_class.return_value = mock_factory
        mock_factory.get_client.return_value = mock_secrets_client
        
        # Mock secrets response with missing fields
        secret_data = {
            'host': 'test-host.clickhouse.cloud'
            # Missing username and password
        }
        mock_secrets_client.get_secret_value.return_value = {
            'SecretString': json.dumps(secret_data)
        }
        
        client = ClickHouseClient(self.secret_name)
        
        with pytest.raises(ClickHouseConnectionError) as exc_info:
            client._get_credentials()
        
        assert "Missing required ClickHouse credential fields" in str(exc_info.value)
    
    @patch('shared.clickhouse_client.AWSClientFactory')
    def test_get_credentials_secrets_error(self, mock_aws_factory_class):
        """Test credential retrieval with AWS Secrets Manager error."""
        # Mock AWS factory and secrets client
        mock_factory = Mock()
        mock_secrets_client = Mock()
        mock_aws_factory_class.return_value = mock_factory
        mock_factory.get_client.return_value = mock_secrets_client
        
        # Mock secrets client to raise exception
        mock_secrets_client.get_secret_value.side_effect = Exception("Secrets error")
        
        client = ClickHouseClient(self.secret_name)
        
        with pytest.raises(ClickHouseConnectionError) as exc_info:
            client._get_credentials()
        
        assert "Failed to retrieve ClickHouse credentials" in str(exc_info.value)
    
    @patch('shared.clickhouse_client.clickhouse_connect')
    @patch.object(ClickHouseClient, '_get_credentials')
    def test_create_connection_success(self, mock_get_credentials, mock_clickhouse_connect):
        """Test successful ClickHouse connection creation."""
        # Mock credentials
        credentials = {
            'host': 'test-host.clickhouse.cloud',
            'port': 8443,
            'username': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
        mock_get_credentials.return_value = credentials
        
        # Mock ClickHouse client
        mock_ch_client = Mock()
        mock_clickhouse_connect.get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        result = client._create_connection()
        
        assert result == mock_ch_client
        mock_ch_client.ping.assert_called_once()
        mock_clickhouse_connect.get_client.assert_called_once()
    
    @patch('shared.clickhouse_client.clickhouse_connect')
    @patch.object(ClickHouseClient, '_get_credentials')
    def test_create_connection_failure(self, mock_get_credentials, mock_clickhouse_connect):
        """Test ClickHouse connection creation failure."""
        # Mock credentials
        credentials = {
            'host': 'test-host.clickhouse.cloud',
            'username': 'test_user',
            'password': 'test_password'
        }
        mock_get_credentials.return_value = credentials
        
        # Mock ClickHouse connection to fail
        mock_clickhouse_connect.get_client.side_effect = Exception("Connection failed")
        
        client = ClickHouseClient(self.secret_name)
        
        with pytest.raises(ClickHouseConnectionError) as exc_info:
            client._create_connection()
        
        assert "Failed to connect to ClickHouse" in str(exc_info.value)
    
    @patch.object(ClickHouseClient, '_create_connection')
    def test_get_client_new_connection(self, mock_create_connection):
        """Test getting client when no connection exists."""
        mock_ch_client = Mock()
        mock_create_connection.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        result = client.get_client()
        
        assert result == mock_ch_client
        assert client._client == mock_ch_client
        mock_create_connection.assert_called_once()
    
    @patch.object(ClickHouseClient, '_create_connection')
    def test_get_client_existing_connection(self, mock_create_connection):
        """Test getting client when connection already exists."""
        mock_ch_client = Mock()
        mock_create_connection.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        client._client = mock_ch_client
        
        result = client.get_client()
        
        assert result == mock_ch_client
        mock_ch_client.ping.assert_called_once()
        # _create_connection should not be called since connection exists
        mock_create_connection.assert_not_called()
    
    @patch.object(ClickHouseClient, '_create_connection')
    def test_get_client_reconnect_on_ping_failure(self, mock_create_connection):
        """Test client reconnection when ping fails."""
        mock_ch_client_old = Mock()
        mock_ch_client_new = Mock()
        mock_ch_client_old.ping.side_effect = Exception("Connection lost")
        mock_create_connection.return_value = mock_ch_client_new
        
        client = ClickHouseClient(self.secret_name)
        client._client = mock_ch_client_old
        
        result = client.get_client()
        
        assert result == mock_ch_client_new
        assert client._client == mock_ch_client_new
        mock_create_connection.assert_called_once()
    
    @patch.dict(os.environ, {'CLICKHOUSE_SECRET_NAME': 'env-secret'})
    def test_from_environment_success(self):
        """Test creating client from environment variable."""
        client = ClickHouseClient.from_environment()
        
        assert client.secret_name == 'env-secret'
        assert client.region_name is None
    
    @patch.dict(os.environ, {}, clear=True)
    def test_from_environment_missing_env_var(self):
        """Test creating client from environment when variable is missing."""
        with pytest.raises(ClickHouseConnectionError) as exc_info:
            ClickHouseClient.from_environment()
        
        assert "Environment variable CLICKHOUSE_SECRET_NAME not set" in str(exc_info.value)
    
    @patch.dict(os.environ, {'CUSTOM_SECRET_NAME': 'custom-secret'})
    def test_from_environment_custom_env_name(self):
        """Test creating client from custom environment variable."""
        client = ClickHouseClient.from_environment('CUSTOM_SECRET_NAME', 'us-west-2')
        
        assert client.secret_name == 'custom-secret'
        assert client.region_name == 'us-west-2'
    
    @patch.object(ClickHouseClient, 'get_client')
    def test_execute_query_success(self, mock_get_client):
        """Test successful query execution."""
        mock_ch_client = Mock()
        mock_result = Mock()
        mock_ch_client.query.return_value = mock_result
        mock_get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        result = client.execute_query("SELECT 1")
        
        assert result == mock_result
        mock_ch_client.query.assert_called_once()
    
    @patch.object(ClickHouseClient, 'get_client')
    def test_execute_query_with_parameters(self, mock_get_client):
        """Test query execution with parameters."""
        mock_ch_client = Mock()
        mock_result = Mock()
        mock_ch_client.query.return_value = mock_result
        mock_get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        parameters = {'param1': 'value1'}
        settings = {'max_execution_time': 600}
        
        result = client.execute_query("SELECT * FROM table WHERE id = {param1:String}", 
                                    parameters=parameters, settings=settings)
        
        assert result == mock_result
        mock_ch_client.query.assert_called_once()
        call_args = mock_ch_client.query.call_args
        assert call_args[1]['parameters'] == parameters
        assert call_args[1]['settings']['max_execution_time'] == 600
    
    @patch.object(ClickHouseClient, 'get_client')
    def test_execute_query_failure(self, mock_get_client):
        """Test query execution failure."""
        mock_ch_client = Mock()
        mock_ch_client.query.side_effect = Exception("Query failed")
        mock_get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        
        with pytest.raises(ClickHouseQueryError) as exc_info:
            client.execute_query("SELECT 1")
        
        assert "Query execution failed" in str(exc_info.value)
    
    @patch.object(ClickHouseClient, 'get_client')
    def test_execute_command_success(self, mock_get_client):
        """Test successful command execution."""
        mock_ch_client = Mock()
        mock_get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        client.execute_command("CREATE TABLE test (id UInt32) ENGINE = Memory")
        
        mock_ch_client.command.assert_called_once()
    
    @patch.object(ClickHouseClient, 'get_client')
    def test_execute_command_failure(self, mock_get_client):
        """Test command execution failure."""
        mock_ch_client = Mock()
        mock_ch_client.command.side_effect = Exception("Command failed")
        mock_get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        
        with pytest.raises(ClickHouseQueryError) as exc_info:
            client.execute_command("CREATE TABLE test (id UInt32) ENGINE = Memory")
        
        assert "Command execution failed" in str(exc_info.value)
    
    @patch.object(ClickHouseClient, 'get_client')
    def test_bulk_insert_success(self, mock_get_client):
        """Test successful bulk insert."""
        mock_ch_client = Mock()
        mock_get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        data = [
            {'id': 1, 'name': 'Test 1'},
            {'id': 2, 'name': 'Test 2'}
        ]
        
        result = client.bulk_insert('test_table', data, batch_size=1)
        
        assert result == 2
        assert mock_ch_client.insert.call_count == 2  # Two batches
    
    @patch.object(ClickHouseClient, 'get_client')
    def test_bulk_insert_with_tenant_id(self, mock_get_client):
        """Test bulk insert with tenant ID."""
        mock_ch_client = Mock()
        mock_get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        data = [{'id': 1, 'name': 'Test 1'}]
        
        result = client.bulk_insert('test_table', data, tenant_id='tenant123')
        
        assert result == 1
        # Verify tenant_id was added to the data
        call_args = mock_ch_client.insert.call_args[0]
        inserted_data = call_args[1]
        assert inserted_data[0]['tenant_id'] == 'tenant123'
    
    @patch.object(ClickHouseClient, 'get_client')
    def test_bulk_insert_empty_data(self, mock_get_client):
        """Test bulk insert with empty data."""
        mock_ch_client = Mock()
        mock_get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        result = client.bulk_insert('test_table', [])
        
        assert result == 0
        mock_ch_client.insert.assert_not_called()
    
    @patch.object(ClickHouseClient, 'get_client')
    def test_bulk_insert_failure(self, mock_get_client):
        """Test bulk insert failure."""
        mock_ch_client = Mock()
        mock_ch_client.insert.side_effect = Exception("Insert failed")
        mock_get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        data = [{'id': 1, 'name': 'Test 1'}]
        
        with pytest.raises(ClickHouseQueryError) as exc_info:
            client.bulk_insert('test_table', data)
        
        assert "Bulk insert failed" in str(exc_info.value)
    
    @patch.object(ClickHouseClient, 'execute_query')
    def test_get_table_info_success(self, mock_execute_query):
        """Test successful table info retrieval."""
        # Mock schema query result
        schema_result = Mock()
        schema_result.result_rows = [
            ('id', 'UInt32', '', '', '', '', ''),
            ('name', 'String', '', '', '', '', '')
        ]
        
        # Mock stats query result
        stats_result = Mock()
        stats_result.result_rows = [(1000,)]
        
        mock_execute_query.side_effect = [schema_result, stats_result]
        
        client = ClickHouseClient(self.secret_name)
        result = client.get_table_info('test_table')
        
        assert result['table_name'] == 'test_table'
        assert result['total_rows'] == 1000
        assert result['columns'] == ['id', 'name']
        assert len(result['schema']) == 2
    
    @patch.object(ClickHouseClient, 'execute_query')
    def test_check_table_exists_true(self, mock_execute_query):
        """Test checking if table exists (returns True)."""
        mock_result = Mock()
        mock_result.result_rows = [(1,)]  # Table exists
        mock_execute_query.return_value = mock_result
        
        client = ClickHouseClient(self.secret_name)
        result = client.check_table_exists('test_table')
        
        assert result is True
    
    @patch.object(ClickHouseClient, 'execute_query')
    def test_check_table_exists_false(self, mock_execute_query):
        """Test checking if table exists (returns False)."""
        mock_result = Mock()
        mock_result.result_rows = [(0,)]  # Table doesn't exist
        mock_execute_query.return_value = mock_result
        
        client = ClickHouseClient(self.secret_name)
        result = client.check_table_exists('test_table')
        
        assert result is False
    
    @patch.object(ClickHouseClient, 'get_client')
    def test_transaction_context_manager(self, mock_get_client):
        """Test transaction context manager."""
        mock_ch_client = Mock()
        mock_get_client.return_value = mock_ch_client
        
        client = ClickHouseClient(self.secret_name)
        
        with client.transaction() as tx_client:
            assert tx_client == mock_ch_client
    
    def test_close_connection(self):
        """Test closing ClickHouse connection."""
        mock_ch_client = Mock()
        
        client = ClickHouseClient(self.secret_name)
        client._client = mock_ch_client
        client._connection_time = datetime.now(timezone.utc)
        
        client.close()
        
        mock_ch_client.close.assert_called_once()
        assert client._client is None
        assert client._connection_time is None
    
    def test_close_connection_error(self):
        """Test closing connection with error."""
        mock_ch_client = Mock()
        mock_ch_client.close.side_effect = Exception("Close failed")
        
        client = ClickHouseClient(self.secret_name)
        client._client = mock_ch_client
        
        # Should not raise exception
        client.close()
        
        assert client._client is None
    
    def test_context_manager(self):
        """Test ClickHouseClient as context manager."""
        mock_ch_client = Mock()
        
        client = ClickHouseClient(self.secret_name)
        client._client = mock_ch_client  # Set the internal client directly
        
        with client as ctx_client:
            assert ctx_client == client
        
        # close() should be called automatically
        mock_ch_client.close.assert_called_once()


class TestConvenienceFunction:
    """Test cases for backward compatibility convenience function."""
    
    @patch.dict(os.environ, {'CLICKHOUSE_SECRET_NAME': 'env-secret'})
    @patch('shared.clickhouse_client.ClickHouseClient')
    def test_get_clickhouse_connection_from_env(self, mock_client_class):
        """Test get_clickhouse_connection using environment variable."""
        mock_client = Mock()
        mock_ch_client = Mock()
        mock_client.get_client.return_value = mock_ch_client
        mock_client_class.return_value = mock_client
        
        result = get_clickhouse_connection()
        
        assert result == mock_ch_client
        mock_client_class.assert_called_once_with('env-secret')
        mock_client.get_client.assert_called_once()
    
    @patch('shared.clickhouse_client.ClickHouseClient')
    def test_get_clickhouse_connection_with_secret_name(self, mock_client_class):
        """Test get_clickhouse_connection with explicit secret name."""
        mock_client = Mock()
        mock_ch_client = Mock()
        mock_client.get_client.return_value = mock_ch_client
        mock_client_class.return_value = mock_client
        
        result = get_clickhouse_connection('custom-secret')
        
        assert result == mock_ch_client
        mock_client_class.assert_called_once_with('custom-secret')
    
    @patch.dict(os.environ, {}, clear=True)
    def test_get_clickhouse_connection_no_env_var(self):
        """Test get_clickhouse_connection when environment variable is missing."""
        with pytest.raises(ClickHouseConnectionError) as exc_info:
            get_clickhouse_connection()
        
        assert "CLICKHOUSE_SECRET_NAME environment variable not set" in str(exc_info.value)


if __name__ == '__main__':
    pytest.main([__file__])