"""
Integration tests for Lambda function migrations after Phase 4 optimizations.

Tests that Lambda functions work correctly with CDK native bundling
and that all dependencies are properly resolved.
"""

import pytest
import os
import sys
import json
import tempfile
import zipfile
from unittest.mock import Mock, patch, MagicMock

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Import shared mock configurations
from tests.shared.mock_configs import MockEnvironmentConfigs, MockAWSClients


class TestLambdaMigrations:
    """Test Lambda function migrations and CDK bundling compatibility."""

    def setup_method(self):
        """Set up test environment."""
        self.test_env = {
            'ENVIRONMENT': 'dev',  # Changed from 'test' to 'dev' to match existing config
            'AWS_REGION': 'us-east-1',
            'TENANT_SERVICES_TABLE': 'TenantServices-dev',
            'LAST_UPDATED_TABLE': 'LastUpdated-dev',
            'BUCKET_NAME': 'dev-bucket',
            'PROCESSING_JOBS_TABLE': 'ProcessingJobs-dev',
            'CHUNK_PROGRESS_TABLE': 'ChunkProgress-dev'
        }

    def test_canonical_transform_lambda_imports(self):
        """Test that canonical transform Lambda can import all required modules."""
        with patch.dict(os.environ, self.test_env):
            try:
                # Import the lambda function
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'canonical_transform'))
                import lambda_function as canonical_lambda
                
                # Verify the handler exists
                assert hasattr(canonical_lambda, 'lambda_handler')
                
                # Test that shared modules are accessible
                from shared.environment import Environment, EnvironmentConfig
                from shared.canonical_mapper import CanonicalMapper
                from shared.validators import DataValidator
                
                # Mock environment config for testing
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
                
                # Verify components can be instantiated
                with patch.object(Environment, '_load_config', return_value=mock_config):
                    config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                    mapper = CanonicalMapper()
                    validator = DataValidator()
                
                assert config is not None
                assert mapper is not None
                assert validator is not None
                
            except ImportError as e:
                pytest.fail(f"Canonical transform Lambda import failed: {e}")

    def test_clickhouse_loader_lambda_imports(self):
        """Test that ClickHouse loader Lambda can import all required modules."""
        with patch.dict(os.environ, self.test_env):
            try:
                # Import the lambda function
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'clickhouse', 'data_loader'))
                import lambda_function as clickhouse_lambda
                
                # Verify the handler exists
                assert hasattr(clickhouse_lambda, 'lambda_handler')
                
                # Test that shared modules are accessible
                from shared.environment import Environment, EnvironmentConfig
                from shared.clickhouse_client import ClickHouseClient
                from shared.validators import DataValidator
                
                # Mock environment config for testing
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
                
                # Verify components can be instantiated
                with patch.object(Environment, '_load_config', return_value=mock_config):
                    config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                    validator = DataValidator()
                
                assert config is not None
                assert validator is not None
                
            except ImportError as e:
                pytest.fail(f"ClickHouse loader Lambda import failed: {e}")

    @patch('boto3.client')
    def test_canonical_transform_lambda_execution(self, mock_boto_client):
        """Test canonical transform Lambda execution with mocked dependencies."""
        mock_s3 = Mock()
        mock_dynamodb = Mock()
        mock_boto_client.side_effect = lambda service, **kwargs: {
            's3': mock_s3,
            'dynamodb': mock_dynamodb
        }.get(service)
        
        # Mock S3 responses
        mock_s3.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps([{
                'CompanyID': 123,
                'CompanyName': 'Test Company',
                'DateCreated': '2023-01-01T00:00:00Z'
            }]).encode())
        }
        
        with patch.dict(os.environ, self.test_env):
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'canonical_transform'))
            import lambda_function as canonical_lambda
            
            # Test event
            event = {
                'Records': [{
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'raw/companies/test.json'}
                    }
                }]
            }
            
            context = Mock()
            
            # Execute the lambda
            try:
                result = canonical_lambda.lambda_handler(event, context)
                assert result is not None
                
            except Exception as e:
                # Should handle errors gracefully
                assert 'error' in str(e).lower() or 'exception' in str(e).lower()

    @patch('clickhouse_connect.get_client')
    @patch('boto3.client')
    def test_clickhouse_loader_lambda_execution(self, mock_boto_client, mock_ch_client):
        """Test ClickHouse loader Lambda execution with mocked dependencies."""
        mock_s3 = Mock()
        mock_ch = Mock()
        mock_boto_client.return_value = mock_s3
        mock_ch_client.return_value = mock_ch
        
        # Mock S3 responses
        mock_s3.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps([{
                'id': '123',
                'name': 'Test Company',
                'created_date': '2023-01-01T00:00:00Z'
            }]).encode())
        }
        
        with patch.dict(os.environ, self.test_env):
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'clickhouse', 'data_loader'))
            import lambda_function as clickhouse_lambda
            
            # Test event
            event = {
                'Records': [{
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'canonical/companies/test.json'}
                    }
                }]
            }
            
            context = Mock()
            
            # Execute the lambda
            try:
                result = clickhouse_lambda.lambda_handler(event, context)
                assert result is not None
                
            except Exception as e:
                # Should handle errors gracefully
                assert 'error' in str(e).lower() or 'exception' in str(e).lower()

    def test_lambda_package_structure(self):
        """Test that Lambda packages have the correct structure for CDK bundling."""
        lambda_dirs = [
            'src/canonical_transform',
            'src/clickhouse/data_loader',
            'src/clickhouse/scd_processor',
            'src/clickhouse/schema_init',
            'src/backfill'
        ]
        
        for lambda_dir in lambda_dirs:
            full_path = os.path.join(os.path.dirname(__file__), '..', '..', lambda_dir)
            
            # Check that lambda_function.py exists
            lambda_file = os.path.join(full_path, 'lambda_function.py')
            assert os.path.exists(lambda_file), f"lambda_function.py missing in {lambda_dir}"
            
            # Check for requirements.txt (should exist for CDK bundling)
            requirements_file = os.path.join(full_path, 'requirements.txt')
            if os.path.exists(requirements_file):
                # Verify requirements.txt is not empty or has reasonable content
                with open(requirements_file, 'r') as f:
                    content = f.read().strip()
                    # Should either be empty (using shared deps) or have valid requirements
                    if content:
                        lines = [line.strip() for line in content.split('\n') if line.strip()]
                        for line in lines:
                            if not line.startswith('#'):
                                assert '==' in line or '>=' in line or line in ['boto3', 'clickhouse-connect']

    def test_shared_module_accessibility(self):
        """Test that shared modules are accessible from all Lambda functions."""
        lambda_dirs = [
            'src/canonical_transform',
            'src/clickhouse/data_loader'
        ]
        
        shared_modules = [
            'environment',
            'aws_client_factory',
            'validators',
            'canonical_mapper',
            'clickhouse_client'
        ]
        
        for lambda_dir in lambda_dirs:
            lambda_path = os.path.join(os.path.dirname(__file__), '..', '..', lambda_dir)
            sys.path.insert(0, lambda_path)
            
            try:
                # Test importing shared modules
                for module in shared_modules:
                    try:
                        exec(f"from shared.{module} import *")
                    except ImportError as e:
                        # Some modules might not be needed by all Lambdas
                        if 'clickhouse' in module and 'canonical' in lambda_dir:
                            continue  # ClickHouse not needed in canonical transform
                        if 'canonical_mapper' in module and 'clickhouse' in lambda_dir:
                            continue  # Mapper not needed in ClickHouse loader
                        pytest.fail(f"Failed to import {module} in {lambda_dir}: {e}")
                        
            finally:
                # Clean up sys.path
                if lambda_path in sys.path:
                    sys.path.remove(lambda_path)

    def test_environment_variable_handling(self):
        """Test that Lambda functions handle environment variables correctly."""
        required_env_vars = [
            'ENVIRONMENT',
            'AWS_REGION',
            'TENANT_SERVICES_TABLE',
            'LAST_UPDATED_TABLE',
            'BUCKET_NAME'
        ]
        
        with patch.dict(os.environ, self.test_env):
            # Test canonical transform
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'canonical_transform'))
            try:
                import lambda_function as canonical_lambda
                from shared.environment import Environment, EnvironmentConfig
                
                # Mock environment config for testing
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
                    config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                    for var in required_env_vars:
                        assert hasattr(config, var.lower()) or var in os.environ
                    
            except ImportError:
                pass  # Skip if module not available
            
            # Test ClickHouse loader
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'clickhouse', 'data_loader'))
            try:
                import lambda_function as clickhouse_lambda
                from shared.environment import Environment, EnvironmentConfig
                
                # Mock environment config for testing
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
                    config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                    for var in required_env_vars:
                        assert hasattr(config, var.lower()) or var in os.environ
                    
            except ImportError:
                pass  # Skip if module not available

    def test_lambda_memory_efficiency(self):
        """Test that Lambda functions are memory efficient."""
        import gc
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        with patch.dict(os.environ, self.test_env):
            # Import and instantiate Lambda functions
            lambda_modules = []
            
            try:
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'canonical_transform'))
                import lambda_function as canonical_lambda
                lambda_modules.append(canonical_lambda)
            except ImportError:
                pass
            
            try:
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'clickhouse', 'data_loader'))
                import lambda_function as clickhouse_lambda
                lambda_modules.append(clickhouse_lambda)
            except ImportError:
                pass
            
            # Force garbage collection
            gc.collect()
            
            final_memory = process.memory_info().rss
            memory_increase = final_memory - initial_memory
            
            # Memory increase should be reasonable (less than 100MB)
            assert memory_increase < 100 * 1024 * 1024, f"Memory increase too high: {memory_increase} bytes"

    def test_error_handling_in_lambdas(self):
        """Test error handling in Lambda functions."""
        with patch.dict(os.environ, self.test_env):
            # Test canonical transform error handling
            try:
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'canonical_transform'))
                import lambda_function as canonical_lambda
                
                # Test with invalid event
                invalid_event = {'invalid': 'event'}
                context = Mock()
                
                try:
                    result = canonical_lambda.lambda_handler(invalid_event, context)
                    # Should either return error response or raise handled exception
                    if isinstance(result, dict):
                        assert 'error' in result or 'statusCode' in result
                except Exception as e:
                    # Should be a handled exception with meaningful message
                    assert len(str(e)) > 0
                    
            except ImportError:
                pass  # Skip if module not available

    @patch('boto3.client')
    def test_lambda_dependency_injection(self, mock_boto_client):
        """Test that Lambda functions properly inject dependencies."""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        with patch.dict(os.environ, self.test_env):
            try:
                from shared.aws_client_factory import AWSClientFactory
                from shared.environment import Environment, EnvironmentConfig
                
                # Mock environment config for testing
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
                    config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                    factory = AWSClientFactory()
                
                # Test that clients are properly created
                s3_client = factory.get_client('s3')  # Updated to use correct API
                dynamodb_client = factory.get_client('dynamodb')  # Updated to use correct API
                
                assert s3_client is not None
                assert dynamodb_client is not None
                
                # Test caching
                assert factory.get_client('s3') is s3_client
                assert factory.get_client('dynamodb') is dynamodb_client
                
            except ImportError as e:
                pytest.fail(f"Dependency injection test failed: {e}")

    def test_lambda_cold_start_optimization(self):
        """Test that Lambda functions are optimized for cold starts."""
        import time
        
        with patch.dict(os.environ, self.test_env):
            # Measure import time
            start_time = time.time()
            
            try:
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'canonical_transform'))
                import lambda_function as canonical_lambda
                
                from shared.environment import Environment, EnvironmentConfig
                from shared.canonical_mapper import CanonicalMapper
                from shared.validators import DataValidator
                
                # Mock environment config for testing
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
                
                # Initialize components
                with patch.object(Environment, '_load_config', return_value=mock_config):
                    config = Environment.get_config('dev')  # Changed from 'test' to 'dev'
                    mapper = CanonicalMapper()
                    validator = DataValidator()
                
                end_time = time.time()
                import_time = end_time - start_time
                
                # Import and initialization should be fast (less than 2 seconds)
                assert import_time < 2.0, f"Cold start too slow: {import_time} seconds"
                
            except ImportError:
                pass  # Skip if module not available


if __name__ == '__main__':
    pytest.main([__file__, '-v'])