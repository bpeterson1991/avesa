"""
Integration tests for shared components after Phase 4 optimizations.

Tests the integration between optimized shared components and ensures
all components work together correctly after architectural optimizations.
"""

import pytest
import os
import sys
import json
from unittest.mock import Mock, patch, MagicMock

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from shared.aws_client_factory import AWSClientFactory
from shared.environment import Environment, get_current_environment
from shared.validators import DataValidator
from shared.clickhouse_client import ClickHouseClient
from shared.canonical_mapper import CanonicalMapper

# Import shared mock configurations
from tests.shared.mock_configs import MockEnvironmentConfigs, MockAWSClients, MockClickHouseClient


class TestSharedComponentsIntegration:
    """Test integration between optimized shared components."""

    def setup_method(self):
        """Set up test environment."""
        self.test_env = {
            'ENVIRONMENT': 'dev',  # Changed from 'test' to 'dev'
            'AWS_REGION': 'us-east-1',
            'TENANT_SERVICES_TABLE': 'TenantServices-dev',
            'LAST_UPDATED_TABLE': 'LastUpdated-dev'
        }

    @patch.dict(os.environ, {'ENVIRONMENT': 'dev', 'AWS_REGION': 'us-east-1'})  # Changed to 'dev'
    def test_environment_config_integration(self):
        """Test that environment config works with all components."""
        # Mock the environment config file since it may not exist in test
        mock_config = {
            "environments": {
                "dev": {  # Changed from 'test' to 'dev'
                    "account": "123456789012",
                    "region": "us-east-1",
                    "bucket_name": "dev-bucket",
                    "table_suffix": "-dev",
                    "lambda_memory": 512,
                    "lambda_timeout": 300
                }
            }
        }
        
        with patch.object(Environment, '_load_config', return_value=mock_config):
            env_name = get_current_environment()
            assert env_name == 'dev'  # Changed from 'test' to 'dev'
            
            config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
            assert config.region == 'us-east-1'
            assert config.account == '123456789012'
            
            table_names = Environment.get_table_names('dev')  # Changed from 'test' to 'dev'
            assert table_names['tenant_services'] == 'TenantServices-dev'  # Changed suffix

    @patch('boto3.client')
    def test_aws_client_factory_integration(self, mock_boto_client):
        """Test AWS client factory integration with environment config."""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        mock_config = {
            "environments": {
                "dev": {  # Changed from 'test' to 'dev'
                    "account": "123456789012",
                    "region": "us-east-1",
                    "bucket_name": "dev-bucket",
                    "table_suffix": "-dev",
                    "lambda_memory": 512,
                    "lambda_timeout": 300
                }
            }
        }
        
        with patch.dict(os.environ, self.test_env):
            with patch.object(Environment, '_load_config', return_value=mock_config):
                config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                factory = AWSClientFactory()
            
            # Test client creation
            dynamodb_client = factory.get_client('dynamodb')
            s3_client = factory.get_client('s3')
            
            assert dynamodb_client is not None
            assert s3_client is not None
            
            # Verify clients are cached
            assert factory.get_client('dynamodb') is dynamodb_client
            assert factory.get_client('s3') is s3_client

    def test_data_validator_integration(self):
        """Test data validator integration with canonical mapping."""
        validator = DataValidator()
        
        # Test company data validation
        company_data = {
            'id': 'comp_123',
            'name': 'Test Company',
            'created_date': '2023-01-01T00:00:00Z'
        }
        
        result = validator.validate_company_data(company_data)
        assert result['is_valid'] is True
        assert result['errors'] == []
        
        # Test invalid data
        invalid_data = {'name': ''}  # Missing required fields
        result = validator.validate_company_data(invalid_data)
        assert result['is_valid'] is False
        assert len(result['errors']) > 0

    @patch('boto3.client')
    @patch('clickhouse_connect.get_client')
    def test_clickhouse_client_integration(self, mock_get_client, mock_boto_client):
        """Test ClickHouse client integration with environment config."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        # Mock the secrets manager client
        mock_secrets = Mock()
        mock_secrets.get_secret_value.return_value = {
            'SecretString': json.dumps({
                'host': 'test-host',
                'username': 'test-user',
                'password': 'test-pass'
            })
        }
        mock_boto_client.return_value = mock_secrets
        
        mock_config = {
            "environments": {
                "dev": {  # Changed from 'test' to 'dev'
                    "account": "123456789012",
                    "region": "us-east-1",
                    "bucket_name": "dev-bucket",
                    "table_suffix": "-dev",
                    "lambda_memory": 512,
                    "lambda_timeout": 300
                }
            }
        }
        
        with patch.dict(os.environ, self.test_env):
            with patch.object(Environment, '_load_config', return_value=mock_config):
                config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                
                ch_client = ClickHouseClient('test-secret')
            
            # Test connection
            assert ch_client.client is not None
            
            # Test query execution
            mock_client.query.return_value.result_rows = [('test_result',)]
            result = ch_client.execute_query("SELECT 1")
            assert result.result_rows == [('test_result',)]

    def test_canonical_mapper_integration(self):
        """Test canonical mapper integration with validation."""
        mapper = CanonicalMapper()
        validator = DataValidator()
        
        # Test mapping and validation flow
        source_data = {
            'CompanyID': 123,
            'CompanyName': 'Test Company',
            'DateCreated': '2023-01-01T00:00:00Z'
        }
        
        # Map to canonical format
        canonical_data = mapper.map_company_data(source_data, 'connectwise')
        
        # Validate mapped data
        validation_result = validator.validate_company_data(canonical_data)
        
        assert validation_result['is_valid'] is True
        assert canonical_data['id'] == '123'
        assert canonical_data['name'] == 'Test Company'

    @patch('boto3.client')
    @patch('clickhouse_connect.get_client')
    def test_full_pipeline_integration(self, mock_ch_client, mock_boto_client):
        """Test full pipeline integration between all components."""
        # Setup mocks
        mock_dynamo = Mock()
        mock_s3 = Mock()
        mock_secrets = Mock()
        mock_ch = Mock()
        
        def boto_client_side_effect(service, **kwargs):
            if service == 'dynamodb':
                return mock_dynamo
            elif service == 's3':
                return mock_s3
            elif service == 'secretsmanager':
                return mock_secrets
            return Mock()
        
        mock_boto_client.side_effect = boto_client_side_effect
        mock_ch_client.return_value = mock_ch
        
        # Mock the secrets manager response
        mock_secrets.get_secret_value.return_value = {
            'SecretString': json.dumps({
                'host': 'test-host',
                'username': 'test-user',
                'password': 'test-pass'
            })
        }
        
        mock_config = {
            "environments": {
                "dev": {  # Changed from 'test' to 'dev'
                    "account": "123456789012",
                    "region": "us-east-1",
                    "bucket_name": "dev-bucket",
                    "table_suffix": "-dev",
                    "lambda_memory": 512,
                    "lambda_timeout": 300
                }
            }
        }
        
        with patch.dict(os.environ, self.test_env):
            with patch.object(Environment, '_load_config', return_value=mock_config):
                # Initialize all components
                config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                aws_factory = AWSClientFactory()
                validator = DataValidator()
                mapper = CanonicalMapper()
                
                ch_client = ClickHouseClient('test-secret')
            
            # Simulate data processing pipeline
            source_data = {
                'CompanyID': 456,
                'CompanyName': 'Integration Test Co',
                'DateCreated': '2023-06-01T12:00:00Z'
            }
            
            # Step 1: Map data
            canonical_data = mapper.map_company_data(source_data, 'connectwise')
            
            # Step 2: Validate data
            validation_result = validator.validate_company_data(canonical_data)
            assert validation_result['is_valid'] is True
            
            # Step 3: Store in ClickHouse (mocked)
            mock_ch.command.return_value = None
            ch_client.bulk_insert('companies', [canonical_data])
            
            # Verify all components worked together
            assert canonical_data['id'] == '456'
            assert canonical_data['name'] == 'Integration Test Co'
            mock_ch.insert.assert_called_once()

    def test_error_handling_integration(self):
        """Test error handling across integrated components."""
        validator = DataValidator()
        mapper = CanonicalMapper()
        
        # Test with malformed data
        malformed_data = {
            'CompanyID': None,  # Invalid ID
            'CompanyName': '',  # Empty name
            'DateCreated': 'invalid-date'  # Invalid date
        }
        
        # Map data (should handle gracefully)
        try:
            canonical_data = mapper.map_company_data(malformed_data, 'connectwise')
            
            # Validate (should catch errors)
            validation_result = validator.validate_company_data(canonical_data)
            assert validation_result['is_valid'] is False
            assert len(validation_result['errors']) > 0
            
        except Exception as e:
            # Should not raise unhandled exceptions
            pytest.fail(f"Integration should handle errors gracefully: {e}")

    @patch.dict(os.environ, {'ENVIRONMENT': 'prod'})
    def test_production_configuration_integration(self):
        """Test that components work correctly in production configuration."""
        mock_config = {
            "environments": {
                "prod": {
                    "account": "987654321098",
                    "region": "us-east-1",
                    "bucket_name": "prod-bucket",
                    "table_suffix": "-prod",
                    "lambda_memory": 1024,
                    "lambda_timeout": 900
                }
            }
        }
        
        with patch.object(Environment, '_load_config', return_value=mock_config):
            config = Environment.get_config('prod')
            
            assert config.account == '987654321098'
            table_names = Environment.get_table_names('prod')
            assert table_names['tenant_services'] == 'TenantServices-prod'
            
            # Production should have stricter validation
            validator = DataValidator(strict_mode=True)
            assert validator.strict_mode is True

    def test_memory_efficiency_integration(self):
        """Test that integrated components are memory efficient."""
        import gc
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        mock_config = {
            "environments": {
                "dev": {  # Changed from 'test' to 'dev'
                    "account": "123456789012",
                    "region": "us-east-1",
                    "bucket_name": "dev-bucket",
                    "table_suffix": "-dev",
                    "lambda_memory": 512,
                    "lambda_timeout": 300
                }
            }
        }
        
        # Create multiple instances to test memory usage
        components = []
        for i in range(10):
            with patch.dict(os.environ, self.test_env):
                with patch.object(Environment, '_load_config', return_value=mock_config):
                    config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                    validator = DataValidator()
                    mapper = CanonicalMapper()
                    components.append((config, validator, mapper))
        
        # Force garbage collection
        gc.collect()
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (less than 50MB for 10 instances)
        assert memory_increase < 50 * 1024 * 1024, f"Memory increase too high: {memory_increase} bytes"

    def test_concurrent_access_integration(self):
        """Test that components handle concurrent access correctly."""
        import threading
        import time
        
        results = []
        errors = []
        
        mock_config = {
            "environments": {
                "dev": {  # Changed from 'test' to 'dev'
                    "account": "123456789012",
                    "region": "us-east-1",
                    "bucket_name": "dev-bucket",
                    "table_suffix": "-dev",
                    "lambda_memory": 512,
                    "lambda_timeout": 300
                }
            }
        }
        
        def worker():
            try:
                with patch.dict(os.environ, self.test_env):
                    with patch.object(Environment, '_load_config', return_value=mock_config):
                        config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                        validator = DataValidator()
                        mapper = CanonicalMapper()
                    
                    # Simulate work
                    data = {
                        'CompanyID': threading.current_thread().ident,
                        'CompanyName': f'Thread Company {threading.current_thread().ident}',
                        'DateCreated': '2023-01-01T00:00:00Z'
                    }
                    
                    canonical = mapper.map_company_data(data, 'connectwise')
                    validation = validator.validate_company_data(canonical)
                    
                    results.append((canonical, validation))
                    
            except Exception as e:
                errors.append(e)
        
        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Verify results
        assert len(errors) == 0, f"Concurrent access errors: {errors}"
        assert len(results) == 5
        
        for canonical, validation in results:
            assert validation['is_valid'] is True
            assert canonical['name'].startswith('Thread Company')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])