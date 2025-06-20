"""
Test Dynamic Orchestrator Functionality

Tests that the orchestrator works without hardcoded ConnectWise references
and can dynamically discover and process any configured service.
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the source directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'optimized', 'orchestrator'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'shared'))

class TestDynamicOrchestrator:
    """Test the dynamic orchestrator functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        # Mock environment variables
        self.env_vars = {
            'BUCKET_NAME': 'test-bucket',
            'TENANT_SERVICES_TABLE': 'TenantServices-test',
            'LAST_UPDATED_TABLE': 'LastUpdated-test',
            'ENVIRONMENT': 'test',
            'STATE_MACHINE_ARN': 'arn:aws:states:us-east-2:123456789012:stateMachine:TestStateMachine'
        }
        
        # Mock AWS clients
        self.mock_dynamodb = Mock()
        self.mock_cloudwatch = Mock()
        self.mock_stepfunctions = Mock()
        
    @patch.dict(os.environ, {
        'BUCKET_NAME': 'test-bucket',
        'TENANT_SERVICES_TABLE': 'TenantServices-test',
        'LAST_UPDATED_TABLE': 'LastUpdated-test',
        'ENVIRONMENT': 'test',
        'STATE_MACHINE_ARN': 'arn:aws:states:us-east-2:123456789012:stateMachine:TestStateMachine'
    })
    @patch('boto3.client')
    def test_orchestrator_discovers_multiple_services(self, mock_boto3_client):
        """Test that orchestrator can discover and process multiple services."""
        # Import after patching environment
        from lambda_function import PipelineOrchestrator
        
        # Set up mock clients
        mock_clients = {
            'dynamodb': self.mock_dynamodb,
            'cloudwatch': self.mock_cloudwatch,
            'stepfunctions': self.mock_stepfunctions
        }
        mock_boto3_client.side_effect = lambda service: mock_clients[service]
        
        # Mock tenant discovery with multiple services
        self.mock_dynamodb.scan.return_value = {
            'Items': [
                {
                    'tenant_id': {'S': 'tenant1'},
                    'service': {'S': 'connectwise'},
                    'enabled': {'BOOL': True}
                },
                {
                    'tenant_id': {'S': 'tenant1'},
                    'service': {'S': 'salesforce'},
                    'enabled': {'BOOL': True}
                },
                {
                    'tenant_id': {'S': 'tenant2'},
                    'service': {'S': 'servicenow'},
                    'enabled': {'BOOL': True}
                }
            ]
        }
        
        # Mock Step Functions execution
        self.mock_stepfunctions.start_execution.return_value = {
            'executionArn': 'arn:aws:states:us-east-2:123456789012:execution:TestStateMachine:test-execution'
        }
        
        # Create orchestrator and test
        orchestrator = PipelineOrchestrator()
        
        # Mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-id'
        
        # Test event
        event = {
            'tenant_id': None,  # Process all tenants
            'table_name': None,  # Process all tables
            'force_full_sync': False
        }
        
        # Execute
        result = orchestrator.lambda_handler(event, mock_context)
        
        # Verify results
        assert result['mode'] == 'multi-tenant'
        assert len(result['tenants']) == 2  # Two unique tenants
        assert result['tenants'][0]['tenant_id'] == 'tenant1'
        assert result['tenants'][1]['tenant_id'] == 'tenant2'
        
        # Verify tenant1 has multiple services
        tenant1_services = result['tenants'][0]['services']
        assert len(tenant1_services) == 2
        service_names = [s['service_name'] for s in tenant1_services]
        assert 'connectwise' in service_names
        assert 'salesforce' in service_names
        
        # Verify tenant2 has servicenow
        tenant2_services = result['tenants'][1]['services']
        assert len(tenant2_services) == 1
        assert tenant2_services[0]['service_name'] == 'servicenow'
        
        # Verify Step Functions was triggered
        self.mock_stepfunctions.start_execution.assert_called_once()
    
    @patch.dict(os.environ, {
        'BUCKET_NAME': 'test-bucket',
        'TENANT_SERVICES_TABLE': 'TenantServices-test',
        'LAST_UPDATED_TABLE': 'LastUpdated-test',
        'ENVIRONMENT': 'test'
    })
    @patch('boto3.client')
    def test_orchestrator_calculates_dynamic_estimates(self, mock_boto3_client):
        """Test that orchestrator calculates estimates dynamically."""
        # Import after patching environment
        from lambda_function import PipelineOrchestrator
        
        # Set up mock clients
        mock_clients = {
            'dynamodb': self.mock_dynamodb,
            'cloudwatch': self.mock_cloudwatch,
            'stepfunctions': self.mock_stepfunctions
        }
        mock_boto3_client.side_effect = lambda service: mock_clients[service]
        
        # Mock tenant with services
        tenants = [
            {
                'tenant_id': 'tenant1',
                'services': [
                    {'service_name': 'connectwise', 'enabled': True},
                    {'service_name': 'salesforce', 'enabled': True}
                ]
            }
        ]
        
        orchestrator = PipelineOrchestrator()
        
        # Test dynamic estimation
        estimate = orchestrator._calculate_processing_estimate(tenants, None)
        
        # Should return a reasonable estimate (not zero, not too large)
        assert estimate > 0
        assert estimate < 10000  # Less than ~3 hours
        
        # Test single table estimation
        single_table_estimate = orchestrator._calculate_processing_estimate(tenants, 'tickets')
        assert single_table_estimate > 0
        assert single_table_estimate < estimate  # Single table should be less than all tables
    
    @patch.dict(os.environ, {
        'BUCKET_NAME': 'test-bucket',
        'TENANT_SERVICES_TABLE': 'TenantServices-test',
        'LAST_UPDATED_TABLE': 'LastUpdated-test',
        'ENVIRONMENT': 'test'
    })
    @patch('boto3.client')
    def test_orchestrator_handles_service_agnostic_processing(self, mock_boto3_client):
        """Test that orchestrator works with any service type."""
        # Import after patching environment
        from lambda_function import PipelineOrchestrator
        
        # Set up mock clients
        mock_clients = {
            'dynamodb': self.mock_dynamodb,
            'cloudwatch': self.mock_cloudwatch,
            'stepfunctions': self.mock_stepfunctions
        }
        mock_boto3_client.side_effect = lambda service: mock_clients[service]
        
        # Mock tenant discovery with custom service
        self.mock_dynamodb.scan.return_value = {
            'Items': [
                {
                    'tenant_id': {'S': 'tenant1'},
                    'service': {'S': 'custom_service'},
                    'enabled': {'BOOL': True}
                }
            ]
        }
        
        orchestrator = PipelineOrchestrator()
        
        # Mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-id'
        
        # Test event with custom service
        event = {
            'tenant_id': 'tenant1',
            'table_name': None,
            'force_full_sync': False
        }
        
        # Execute - should not fail even with unknown service
        result = orchestrator.lambda_handler(event, mock_context)
        
        # Verify it processes the custom service
        assert result['mode'] == 'single-tenant'
        assert len(result['tenants']) == 1
        assert result['tenants'][0]['tenant_id'] == 'tenant1'
        
        # Should have the custom service
        services = result['tenants'][0]['services']
        assert len(services) == 1
        assert services[0]['service_name'] == 'custom_service'
    
    def test_tenant_config_supports_multiple_services(self):
        """Test that TenantConfig supports multiple services."""
        # Import the TenantConfig class
        from lambda_function import TenantConfig
        
        # Test with multiple services
        config_data = {
            'tenant_id': 'test-tenant',
            'service_url': 'https://api.example.com',
            'secret_name': 'test-secret',
            'enabled': True,
            'services': [
                {'service_name': 'connectwise', 'enabled': True},
                {'service_name': 'salesforce', 'enabled': True},
                {'service_name': 'servicenow', 'enabled': False}
            ]
        }
        
        tenant_config = TenantConfig(**config_data)
        
        # Verify all fields are set correctly
        assert tenant_config.tenant_id == 'test-tenant'
        assert tenant_config.service_url == 'https://api.example.com'
        assert tenant_config.secret_name == 'test-secret'
        assert tenant_config.enabled == True
        assert len(tenant_config.services) == 3
        
        # Verify services are stored correctly
        service_names = [s['service_name'] for s in tenant_config.services]
        assert 'connectwise' in service_names
        assert 'salesforce' in service_names
        assert 'servicenow' in service_names
    
    @patch.dict(os.environ, {
        'BUCKET_NAME': 'test-bucket',
        'TENANT_SERVICES_TABLE': 'TenantServices-test',
        'LAST_UPDATED_TABLE': 'LastUpdated-test',
        'ENVIRONMENT': 'test'
    })
    @patch('boto3.client')
    def test_orchestrator_handles_empty_tenant_list(self, mock_boto3_client):
        """Test that orchestrator handles empty tenant list gracefully."""
        # Import after patching environment
        from lambda_function import PipelineOrchestrator
        
        # Set up mock clients
        mock_clients = {
            'dynamodb': self.mock_dynamodb,
            'cloudwatch': self.mock_cloudwatch,
            'stepfunctions': self.mock_stepfunctions
        }
        mock_boto3_client.side_effect = lambda service: mock_clients[service]
        
        # Mock empty tenant discovery
        self.mock_dynamodb.scan.return_value = {'Items': []}
        
        orchestrator = PipelineOrchestrator()
        
        # Mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-id'
        
        # Test event
        event = {
            'tenant_id': None,
            'table_name': None,
            'force_full_sync': False
        }
        
        # Execute
        result = orchestrator.lambda_handler(event, mock_context)
        
        # Should handle empty list gracefully
        assert result['mode'] == 'multi-tenant'
        assert len(result['tenants']) == 0
        assert result['estimated_duration'] >= 0


if __name__ == '__main__':
    pytest.main([__file__])