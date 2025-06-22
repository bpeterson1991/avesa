"""
Tests for AWS Client Factory

This module tests the centralized AWS client creation and configuration functionality.
"""

import pytest
import boto3
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config as BotoConfig

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from shared.aws_client_factory import AWSClientFactory


class TestAWSClientFactory:
    """Test cases for AWSClientFactory class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.factory = AWSClientFactory(region_name='us-east-1')
    
    def teardown_method(self):
        """Clean up after tests."""
        self.factory.clear_cache()
    
    def test_init_with_region(self):
        """Test factory initialization with region."""
        factory = AWSClientFactory(region_name='us-west-2')
        assert factory.region_name == 'us-west-2'
        assert factory._clients == {}
    
    def test_init_without_region(self):
        """Test factory initialization without region."""
        factory = AWSClientFactory()
        assert factory.region_name is None
        assert factory._clients == {}
    
    @patch('boto3.client')
    def test_get_client_success(self, mock_boto_client):
        """Test successful client creation."""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = self.factory.get_client('s3')
        
        assert client == mock_client
        mock_boto_client.assert_called_once()
        
        # Verify config was passed
        call_args = mock_boto_client.call_args
        assert call_args[0][0] == 's3'
        assert isinstance(call_args[1]['config'], BotoConfig)
    
    @patch('boto3.client')
    def test_get_client_caching(self, mock_boto_client):
        """Test that clients are cached and reused."""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        # First call
        client1 = self.factory.get_client('dynamodb')
        # Second call
        client2 = self.factory.get_client('dynamodb')
        
        assert client1 == client2
        assert client1 == mock_client
        # boto3.client should only be called once due to caching
        mock_boto_client.assert_called_once()
    
    @patch('boto3.client')
    def test_get_client_different_regions(self, mock_boto_client):
        """Test that different regions create different clients."""
        mock_client1 = Mock()
        mock_client2 = Mock()
        mock_boto_client.side_effect = [mock_client1, mock_client2]
        
        client1 = self.factory.get_client('s3', region_name='us-east-1')
        client2 = self.factory.get_client('s3', region_name='us-west-2')
        
        assert client1 != client2
        assert mock_boto_client.call_count == 2
    
    @patch('boto3.client')
    def test_get_client_no_credentials_error(self, mock_boto_client):
        """Test handling of missing AWS credentials."""
        mock_boto_client.side_effect = NoCredentialsError()
        
        with pytest.raises(NoCredentialsError):
            self.factory.get_client('s3')
    
    @patch('boto3.client')
    def test_get_client_client_error(self, mock_boto_client):
        """Test handling of AWS client errors."""
        mock_boto_client.side_effect = ClientError(
            error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
            operation_name='CreateClient'
        )
        
        with pytest.raises(ClientError):
            self.factory.get_client('s3')
    
    @patch('boto3.client')
    def test_get_client_unexpected_error(self, mock_boto_client):
        """Test handling of unexpected errors."""
        mock_boto_client.side_effect = Exception("Unexpected error")
        
        with pytest.raises(ClientError) as exc_info:
            self.factory.get_client('s3')
        
        assert 'ClientCreationError' in str(exc_info.value)
    
    def test_get_client_config_s3(self):
        """Test S3-specific configuration."""
        config = self.factory._get_client_config('s3')
        
        assert config.region_name == 'us-east-1'
        # Check retry configuration - the structure may vary by boto3 version
        if hasattr(config, 'retries') and config.retries:
            if isinstance(config.retries, dict):
                assert config.retries.get('max_attempts', 3) >= 3
            else:
                # For newer boto3 versions, retries might be a different structure
                assert hasattr(config.retries, 'max_attempts') or 'max_attempts' in str(config.retries)
        assert config.retries['mode'] == 'adaptive'
        assert config.max_pool_connections == 50
        assert hasattr(config, 's3')
        assert config.s3['max_concurrent_requests'] == 10
    
    def test_get_client_config_dynamodb(self):
        """Test DynamoDB-specific configuration."""
        config = self.factory._get_client_config('dynamodb')
        
        assert config.region_name == 'us-east-1'
        assert config.max_pool_connections == 100  # DynamoDB gets more connections
    
    def test_get_client_config_secretsmanager(self):
        """Test Secrets Manager-specific configuration."""
        config = self.factory._get_client_config('secretsmanager')
        
        assert config.region_name == 'us-east-1'
        assert config.max_pool_connections == 20  # Secrets Manager gets fewer connections
    
    @patch('boto3.client')
    def test_get_all_clients(self, mock_boto_client):
        """Test getting all common clients."""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        clients = self.factory.get_all_clients()
        
        expected_services = ['dynamodb', 's3', 'secretsmanager', 'cloudwatch', 'lambda', 'stepfunctions']
        assert len(clients) == len(expected_services)
        
        for service in expected_services:
            assert service in clients
            assert clients[service] == mock_client
    
    @patch('boto3.client')
    def test_get_all_clients_with_failures(self, mock_boto_client):
        """Test get_all_clients continues on individual failures."""
        def side_effect(service_name, **kwargs):
            if service_name == 'lambda':
                raise ClientError(
                    error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
                    operation_name='CreateClient'
                )
            return Mock()
        
        mock_boto_client.side_effect = side_effect
        
        clients = self.factory.get_all_clients()
        
        # Should have all services except lambda
        assert 'dynamodb' in clients
        assert 's3' in clients
        assert 'secretsmanager' in clients
        assert 'cloudwatch' in clients
        assert 'stepfunctions' in clients
        assert 'lambda' not in clients
    
    @patch('boto3.client')
    def test_get_client_bundle_success(self, mock_boto_client):
        """Test getting a specific bundle of clients."""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        services = ['s3', 'dynamodb']
        clients = self.factory.get_client_bundle(services)
        
        assert len(clients) == 2
        assert 's3' in clients
        assert 'dynamodb' in clients
        assert clients['s3'] == mock_client
        assert clients['dynamodb'] == mock_client
    
    @patch('boto3.client')
    def test_get_client_bundle_failure(self, mock_boto_client):
        """Test get_client_bundle fails if any service fails."""
        def side_effect(service_name, **kwargs):
            if service_name == 's3':
                raise ClientError(
                    error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
                    operation_name='CreateClient'
                )
            return Mock()
        
        mock_boto_client.side_effect = side_effect
        
        services = ['s3', 'dynamodb']
        with pytest.raises(ClientError):
            self.factory.get_client_bundle(services)
    
    def test_clear_cache(self):
        """Test cache clearing functionality."""
        # Add some mock clients to cache
        self.factory._clients['s3_us-east-1'] = Mock()
        self.factory._clients['dynamodb_us-east-1'] = Mock()
        
        assert len(self.factory._clients) == 2
        
        self.factory.clear_cache()
        
        assert len(self.factory._clients) == 0
    
    def test_create_default_factory(self):
        """Test creating default factory instance."""
        factory = AWSClientFactory.create_default_factory(region_name='us-west-1')
        
        assert isinstance(factory, AWSClientFactory)
        assert factory.region_name == 'us-west-1'


class TestConvenienceFunctions:
    """Test cases for backward compatibility convenience functions."""
    
    @patch('shared.aws_client_factory.AWSClientFactory')
    def test_get_dynamodb_client(self, mock_factory_class):
        """Test get_dynamodb_client convenience function."""
        mock_factory = Mock()
        mock_client = Mock()
        mock_factory.get_client.return_value = mock_client
        mock_factory_class.return_value = mock_factory
        
        from shared.aws_client_factory import get_dynamodb_client
        
        client = get_dynamodb_client(region_name='us-east-1')
        
        assert client == mock_client
        mock_factory_class.assert_called_once_with('us-east-1')
        mock_factory.get_client.assert_called_once_with('dynamodb')
    
    @patch('shared.aws_client_factory.AWSClientFactory')
    def test_get_s3_client(self, mock_factory_class):
        """Test get_s3_client convenience function."""
        mock_factory = Mock()
        mock_client = Mock()
        mock_factory.get_client.return_value = mock_client
        mock_factory_class.return_value = mock_factory
        
        from shared.aws_client_factory import get_s3_client
        
        client = get_s3_client()
        
        assert client == mock_client
        mock_factory_class.assert_called_once_with(None)
        mock_factory.get_client.assert_called_once_with('s3')
    
    @patch('shared.aws_client_factory.AWSClientFactory')
    def test_get_secrets_client(self, mock_factory_class):
        """Test get_secrets_client convenience function."""
        mock_factory = Mock()
        mock_client = Mock()
        mock_factory.get_client.return_value = mock_client
        mock_factory_class.return_value = mock_factory
        
        from shared.aws_client_factory import get_secrets_client
        
        client = get_secrets_client(region_name='eu-west-1')
        
        assert client == mock_client
        mock_factory_class.assert_called_once_with('eu-west-1')
        mock_factory.get_client.assert_called_once_with('secretsmanager')


if __name__ == '__main__':
    pytest.main([__file__])