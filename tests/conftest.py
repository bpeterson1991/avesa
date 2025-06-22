"""
Enhanced pytest configuration with shared fixtures and path setup.
Eliminates duplicate test setup across test files.
"""
import pytest
import sys
import os
from pathlib import Path
from unittest.mock import Mock, patch

# Ensure src is in path for all tests
test_root = Path(__file__).parent
project_root = test_root.parent
src_path = project_root / 'src'

if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

@pytest.fixture(autouse=True)
def setup_test_environment():
    """Automatically setup test environment for all tests."""
    # Set test environment variables
    test_env = {
        'ENVIRONMENT': 'dev',
        'AWS_REGION': 'us-east-2',
        'TENANT_SERVICES_TABLE': 'tenant-services-dev',
        'LAST_UPDATED_TABLE': 'last-updated-dev',
        'BUCKET_NAME': 'avesa-data-dev',
        'CLICKHOUSE_SECRET_NAME': 'clickhouse-credentials-dev'
    }
    
    # Store original values
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value
    
    yield
    
    # Restore original values
    for key, original_value in original_env.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value

@pytest.fixture
def mock_environment_config():
    """Standard mock environment configuration for tests."""
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
                "clickhouse_port": 8443
            },
            "staging": {
                "account": "123456789012",
                "region": "us-east-2",
                "bucket_name": "avesa-data-staging",
                "table_suffix": "-staging",
                "lambda_memory": 1024,
                "lambda_timeout": 600,
                "clickhouse_host": "staging-clickhouse.com",
                "clickhouse_port": 8443
            },
            "prod": {
                "account": "123456789012",
                "region": "us-east-2",
                "bucket_name": "avesa-data-prod",
                "table_suffix": "",
                "lambda_memory": 2048,
                "lambda_timeout": 900,
                "clickhouse_host": "prod-clickhouse.com",
                "clickhouse_port": 8443
            }
        }
    }

@pytest.fixture
def mock_aws_clients():
    """Mock AWS clients for testing."""
    mock_s3 = Mock()
    mock_dynamodb = Mock()
    mock_secrets = Mock()
    
    with patch('boto3.client') as mock_boto_client:
        def client_side_effect(service_name, **kwargs):
            if service_name == 's3':
                return mock_s3
            elif service_name == 'dynamodb':
                return mock_dynamodb
            elif service_name == 'secretsmanager':
                return mock_secrets
            else:
                return Mock()
        
        mock_boto_client.side_effect = client_side_effect
        
        yield {
            's3': mock_s3,
            'dynamodb': mock_dynamodb,
            'secrets': mock_secrets
        }

@pytest.fixture
def mock_clickhouse_client():
    """Mock ClickHouse client for testing."""
    with patch('clickhouse_connect.get_client') as mock_get_client:
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        yield mock_client

@pytest.fixture
def sample_test_data():
    """Sample test data for various test scenarios."""
    return {
        'company': {
            'id': 'test-company-123',
            'name': 'Test Company',
            'created_date': '2023-01-01T00:00:00Z',
            'last_updated': '2023-01-01T00:00:00Z'
        },
        'contact': {
            'id': 'test-contact-456',
            'company_id': 'test-company-123',
            'name': 'Test Contact',
            'email': 'test@example.com',
            'created_date': '2023-01-01T00:00:00Z',
            'last_updated': '2023-01-01T00:00:00Z'
        },
        'time_entry': {
            'id': 'test-entry-789',
            'company_id': 'test-company-123',
            'contact_id': 'test-contact-456',
            'hours': 8.0,
            'date_entered': '2023-01-01T00:00:00Z',
            'last_updated': '2023-01-01T00:00:00Z'
        }
    }