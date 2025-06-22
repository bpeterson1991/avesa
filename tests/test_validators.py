"""
Tests for Validators Module

This module tests the centralized validation and quality check functionality.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timezone

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from shared.validators import (
    CredentialValidator,
    DataQualityValidator,
    TenantConfigValidator,
    ValidationError,
    validate_connectwise_credentials,
    validate_tenant_config
)


class TestCredentialValidator:
    """Test cases for CredentialValidator class."""
    
    def test_validate_connectwise_success(self):
        """Test successful ConnectWise credential validation."""
        credentials = {
            'company_id': 'testcompany',
            'public_key': 'test_public_key',
            'private_key': 'test_private_key',
            'client_id': '12345678-1234-1234-1234-123456789abc'
        }
        
        result = CredentialValidator.validate_connectwise(credentials)
        assert result is True
    
    def test_validate_connectwise_missing_fields(self):
        """Test ConnectWise validation with missing required fields."""
        credentials = {
            'company_id': 'testcompany',
            'public_key': 'test_public_key'
            # Missing private_key and client_id
        }
        
        with pytest.raises(ValidationError) as exc_info:
            CredentialValidator.validate_connectwise(credentials)
        
        assert "Missing required fields" in str(exc_info.value)
        assert "private_key" in str(exc_info.value)
        assert "client_id" in str(exc_info.value)
    
    def test_validate_connectwise_empty_fields(self):
        """Test ConnectWise validation with empty fields."""
        credentials = {
            'company_id': '',
            'public_key': 'test_public_key',
            'private_key': '   ',  # Whitespace only
            'client_id': 'test_client_id'
        }
        
        with pytest.raises(ValidationError) as exc_info:
            CredentialValidator.validate_connectwise(credentials)
        
        assert "Invalid fields" in str(exc_info.value)
    
    def test_validate_connectwise_invalid_company_id(self):
        """Test ConnectWise validation with invalid company ID format."""
        credentials = {
            'company_id': 'test company!',  # Invalid characters
            'public_key': 'test_public_key',
            'private_key': 'test_private_key',
            'client_id': 'test_client_id'
        }
        
        with pytest.raises(ValidationError) as exc_info:
            CredentialValidator.validate_connectwise(credentials)
        
        assert "company_id (invalid format)" in str(exc_info.value)
    
    def test_validate_connectwise_invalid_api_url(self):
        """Test ConnectWise validation with invalid API URL."""
        credentials = {
            'company_id': 'testcompany',
            'public_key': 'test_public_key',
            'private_key': 'test_private_key',
            'client_id': 'test_client_id',
            'api_base_url': 'invalid-url'  # Missing protocol
        }
        
        with pytest.raises(ValidationError) as exc_info:
            CredentialValidator.validate_connectwise(credentials)
        
        assert "api_base_url" in str(exc_info.value)
    
    def test_validate_connectwise_with_valid_api_url(self):
        """Test ConnectWise validation with valid API URL."""
        credentials = {
            'company_id': 'testcompany',
            'public_key': 'test_public_key',
            'private_key': 'test_private_key',
            'client_id': 'test_client_id',
            'api_base_url': 'https://api.connectwisedev.com'
        }
        
        result = CredentialValidator.validate_connectwise(credentials)
        assert result is True
    
    def test_validate_salesforce_success(self):
        """Test successful Salesforce credential validation."""
        credentials = {
            'username': 'test@example.com',
            'password': 'test_password',
            'security_token': 'test_token',
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret'
        }
        
        result = CredentialValidator.validate_salesforce(credentials)
        assert result is True
    
    def test_validate_salesforce_invalid_username(self):
        """Test Salesforce validation with invalid username format."""
        credentials = {
            'username': 'invalid_username',  # Missing @ symbol
            'password': 'test_password',
            'security_token': 'test_token',
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret'
        }
        
        with pytest.raises(ValidationError) as exc_info:
            CredentialValidator.validate_salesforce(credentials)
        
        assert "username (must be email format)" in str(exc_info.value)
    
    def test_validate_servicenow_success(self):
        """Test successful ServiceNow credential validation."""
        credentials = {
            'username': 'test_user',
            'password': 'test_password',
            'instance_url': 'https://dev12345.service-now.com'
        }
        
        result = CredentialValidator.validate_servicenow(credentials)
        assert result is True
    
    def test_validate_servicenow_invalid_instance_url(self):
        """Test ServiceNow validation with invalid instance URL."""
        credentials = {
            'username': 'test_user',
            'password': 'test_password',
            'instance_url': 'invalid-url'
        }
        
        with pytest.raises(ValidationError) as exc_info:
            CredentialValidator.validate_servicenow(credentials)
        
        assert "instance_url" in str(exc_info.value)
    
    def test_validate_service_credentials_connectwise(self):
        """Test generic service validation for ConnectWise."""
        credentials = {
            'company_id': 'testcompany',
            'public_key': 'test_public_key',
            'private_key': 'test_private_key',
            'client_id': 'test_client_id'
        }
        
        result = CredentialValidator.validate_service_credentials('connectwise', credentials)
        assert result is True
    
    def test_validate_service_credentials_unknown_service(self):
        """Test generic service validation for unknown service."""
        credentials = {
            'api_key': 'test_api_key',
            'username': 'test_user'
        }
        
        result = CredentialValidator.validate_service_credentials('unknown_service', credentials)
        assert result is True
    
    def test_validate_service_credentials_unknown_service_no_credentials(self):
        """Test generic service validation for unknown service with no valid credentials."""
        credentials = {
            'invalid_field': 'test_value'
        }
        
        with pytest.raises(ValidationError) as exc_info:
            CredentialValidator.validate_service_credentials('unknown_service', credentials)
        
        assert "No valid credential fields found" in str(exc_info.value)
    
    def test_get_required_fields(self):
        """Test getting required fields for services."""
        connectwise_fields = CredentialValidator.get_required_fields('connectwise')
        assert 'company_id' in connectwise_fields
        assert 'public_key' in connectwise_fields
        assert 'private_key' in connectwise_fields
        assert 'client_id' in connectwise_fields
        
        unknown_fields = CredentialValidator.get_required_fields('unknown_service')
        assert unknown_fields == []
    
    def test_get_optional_fields(self):
        """Test getting optional fields for services."""
        connectwise_fields = CredentialValidator.get_optional_fields('connectwise')
        assert 'api_base_url' in connectwise_fields
        assert 'timeout' in connectwise_fields
        
        unknown_fields = CredentialValidator.get_optional_fields('unknown_service')
        assert unknown_fields == []


class TestDataQualityValidator:
    """Test cases for DataQualityValidator class."""
    
    def test_validate_record_completeness_success(self):
        """Test successful record completeness validation."""
        record = {
            'id': '123',
            'name': 'Test Record',
            'email': 'test@example.com'
        }
        required_fields = ['id', 'name', 'email']
        
        result = DataQualityValidator.validate_record_completeness(record, required_fields)
        
        assert result['is_valid'] is True
        assert result['missing_fields'] == []
        assert result['empty_fields'] == []
        assert result['completeness_score'] == 1.0
    
    def test_validate_record_completeness_missing_fields(self):
        """Test record completeness validation with missing fields."""
        record = {
            'id': '123',
            'name': 'Test Record'
            # Missing email
        }
        required_fields = ['id', 'name', 'email']
        
        result = DataQualityValidator.validate_record_completeness(record, required_fields)
        
        assert result['is_valid'] is False
        assert 'email' in result['missing_fields']
        assert abs(result['completeness_score'] - 2/3) < 0.001  # 2 out of 3 fields present
    
    def test_validate_record_completeness_empty_fields(self):
        """Test record completeness validation with empty fields."""
        record = {
            'id': '123',
            'name': '',  # Empty string
            'email': None  # None value
        }
        required_fields = ['id', 'name', 'email']
        
        result = DataQualityValidator.validate_record_completeness(record, required_fields)
        
        assert result['is_valid'] is False
        assert 'name' in result['empty_fields']
        assert 'email' in result['empty_fields']
        assert abs(result['completeness_score'] - 1/3) < 0.001  # Only 1 out of 3 fields valid
    
    def test_validate_data_types_success(self):
        """Test successful data type validation."""
        record = {
            'id': 123,
            'name': 'Test Record',
            'active': True,
            'score': 95.5
        }
        field_types = {
            'id': int,
            'name': str,
            'active': bool,
            'score': float
        }
        
        result = DataQualityValidator.validate_data_types(record, field_types)
        
        assert result['is_valid'] is True
        assert result['type_errors'] == []
        assert result['type_accuracy'] == 1.0
    
    def test_validate_data_types_errors(self):
        """Test data type validation with type errors."""
        record = {
            'id': '123',  # Should be int
            'name': 'Test Record',
            'active': 'true',  # Should be bool
            'score': 95.5
        }
        field_types = {
            'id': int,
            'name': str,
            'active': bool,
            'score': float
        }
        
        result = DataQualityValidator.validate_data_types(record, field_types)
        
        assert result['is_valid'] is False
        assert len(result['type_errors']) == 2
        assert result['type_accuracy'] == 0.5  # 2 out of 4 fields correct
    
    def test_validate_date_fields_success(self):
        """Test successful date field validation."""
        record = {
            'created_at': '2023-01-01T00:00:00Z',
            'updated_at': '2023-01-02T12:30:45+00:00',
            'birth_date': datetime(1990, 1, 1, tzinfo=timezone.utc)
        }
        date_fields = ['created_at', 'updated_at', 'birth_date']
        
        result = DataQualityValidator.validate_date_fields(record, date_fields)
        
        assert result['is_valid'] is True
        assert result['date_errors'] == []
        assert result['date_accuracy'] == 1.0
    
    def test_validate_date_fields_errors(self):
        """Test date field validation with invalid dates."""
        record = {
            'created_at': 'invalid-date',
            'updated_at': '2023-01-02T12:30:45+00:00',
            'birth_date': 123456  # Invalid type
        }
        date_fields = ['created_at', 'updated_at', 'birth_date']
        
        result = DataQualityValidator.validate_date_fields(record, date_fields)
        
        assert result['is_valid'] is False
        assert len(result['date_errors']) == 2
        assert abs(result['date_accuracy'] - 1/3) < 0.001  # Only 1 out of 3 dates valid


class TestTenantConfigValidator:
    """Test cases for TenantConfigValidator class."""
    
    def test_validate_tenant_config_success(self):
        """Test successful tenant configuration validation."""
        config = {
            'tenant_id': 'test_tenant_123',
            'company_name': 'Test Company',
            'enabled': True,
            'created_at': '2023-01-01T00:00:00Z'
        }
        
        result = TenantConfigValidator.validate_tenant_config(config)
        
        assert result['is_valid'] is True
        assert result['errors'] == []
        assert result['warnings'] == []
    
    def test_validate_tenant_config_missing_required_fields(self):
        """Test tenant config validation with missing required fields."""
        config = {
            'company_name': 'Test Company'
            # Missing tenant_id and enabled
        }
        
        result = TenantConfigValidator.validate_tenant_config(config)
        
        assert result['is_valid'] is False
        assert len(result['errors']) == 2
        assert any('tenant_id' in error for error in result['errors'])
        assert any('enabled' in error for error in result['errors'])
    
    def test_validate_tenant_config_empty_required_fields(self):
        """Test tenant config validation with empty required fields."""
        config = {
            'tenant_id': '',
            'company_name': 'Test Company',
            'enabled': True
        }
        
        result = TenantConfigValidator.validate_tenant_config(config)
        
        assert result['is_valid'] is False
        assert any('Empty required field: tenant_id' in error for error in result['errors'])
    
    def test_validate_tenant_config_invalid_tenant_id(self):
        """Test tenant config validation with invalid tenant ID format."""
        config = {
            'tenant_id': 'test tenant!',  # Invalid characters
            'company_name': 'Test Company',
            'enabled': True
        }
        
        result = TenantConfigValidator.validate_tenant_config(config)
        
        assert result['is_valid'] is False
        assert any('tenant_id contains invalid characters' in error for error in result['errors'])
    
    def test_validate_tenant_config_invalid_enabled_type(self):
        """Test tenant config validation with invalid enabled field type."""
        config = {
            'tenant_id': 'test_tenant',
            'company_name': 'Test Company',
            'enabled': 'true'  # Should be boolean
        }
        
        result = TenantConfigValidator.validate_tenant_config(config)
        
        assert result['is_valid'] is False
        assert any('enabled field must be boolean' in error for error in result['errors'])


class TestBackwardCompatibilityFunctions:
    """Test cases for backward compatibility functions."""
    
    def test_validate_connectwise_credentials_function(self):
        """Test backward compatibility function for ConnectWise validation."""
        credentials = {
            'company_id': 'testcompany',
            'public_key': 'test_public_key',
            'private_key': 'test_private_key',
            'client_id': 'test_client_id'
        }
        
        result = validate_connectwise_credentials(credentials)
        assert result is True
    
    def test_validate_tenant_config_function(self):
        """Test backward compatibility function for tenant config validation."""
        config = {
            'tenant_id': 'test_tenant',
            'company_name': 'Test Company',
            'enabled': True
        }
        
        result = validate_tenant_config(config)
        assert result is True
    
    def test_validate_tenant_config_function_invalid(self):
        """Test backward compatibility function with invalid config."""
        config = {
            'company_name': 'Test Company'
            # Missing required fields
        }
        
        result = validate_tenant_config(config)
        assert result is False


if __name__ == '__main__':
    pytest.main([__file__])