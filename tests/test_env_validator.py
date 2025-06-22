"""
Tests for environment validator module.
"""
import pytest
import os
from unittest.mock import patch, MagicMock

from shared.env_validator import EnvironmentValidator


class TestEnvironmentValidator:
    """Test cases for EnvironmentValidator class."""
    
    def test_validate_required_vars_success(self):
        """Test successful validation of required environment variables."""
        test_vars = ['TEST_VAR1', 'TEST_VAR2']
        
        with patch.dict(os.environ, {'TEST_VAR1': 'value1', 'TEST_VAR2': 'value2'}):
            result = EnvironmentValidator.validate_required_vars(test_vars, "Test")
            
            assert result == {'TEST_VAR1': 'value1', 'TEST_VAR2': 'value2'}
    
    def test_validate_required_vars_missing(self):
        """Test validation failure when required variables are missing."""
        test_vars = ['MISSING_VAR1', 'MISSING_VAR2']
        
        # Ensure variables are not set
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError) as exc_info:
                EnvironmentValidator.validate_required_vars(test_vars, "Test")
            
            assert "Test missing required environment variables" in str(exc_info.value)
            assert "MISSING_VAR1" in str(exc_info.value)
            assert "MISSING_VAR2" in str(exc_info.value)
    
    def test_validate_required_vars_partial_missing(self):
        """Test validation when some variables are missing."""
        test_vars = ['PRESENT_VAR', 'MISSING_VAR']
        
        with patch.dict(os.environ, {'PRESENT_VAR': 'value'}, clear=True):
            with pytest.raises(EnvironmentError) as exc_info:
                EnvironmentValidator.validate_required_vars(test_vars, "Test")
            
            assert "MISSING_VAR" in str(exc_info.value)
            assert "PRESENT_VAR" not in str(exc_info.value)
    
    def test_validate_required_vars_empty_values(self):
        """Test validation treats empty strings as missing."""
        test_vars = ['EMPTY_VAR']
        
        with patch.dict(os.environ, {'EMPTY_VAR': ''}):
            with pytest.raises(EnvironmentError):
                EnvironmentValidator.validate_required_vars(test_vars, "Test")
    
    def test_validate_aws_credentials_with_profile(self):
        """Test AWS credentials validation with profile."""
        with patch.dict(os.environ, {'AWS_PROFILE': 'test-profile', 'AWS_REGION': 'us-west-2'}):
            result = EnvironmentValidator.validate_aws_credentials()
            assert result is True
    
    def test_validate_aws_credentials_with_keys(self):
        """Test AWS credentials validation with access keys."""
        env_vars = {
            'AWS_ACCESS_KEY_ID': 'test-key',
            'AWS_SECRET_ACCESS_KEY': 'test-secret',
            'AWS_REGION': 'us-west-2'
        }
        
        with patch.dict(os.environ, env_vars):
            result = EnvironmentValidator.validate_aws_credentials()
            assert result is True
    
    def test_validate_aws_credentials_no_credentials(self):
        """Test AWS credentials validation with no credentials."""
        with patch.dict(os.environ, {}, clear=True):
            result = EnvironmentValidator.validate_aws_credentials()
            assert result is False
            # Should set default region
            assert os.environ.get('AWS_REGION') == 'us-east-2'
    
    def test_validate_aws_credentials_sets_default_region(self):
        """Test that default region is set when not provided."""
        with patch.dict(os.environ, {'AWS_PROFILE': 'test'}, clear=True):
            EnvironmentValidator.validate_aws_credentials()
            assert os.environ.get('AWS_REGION') == 'us-east-2'
    
    def test_validate_aws_credentials_preserves_existing_region(self):
        """Test that existing region is preserved."""
        env_vars = {
            'AWS_PROFILE': 'test',
            'AWS_REGION': 'eu-west-1'
        }
        
        with patch.dict(os.environ, env_vars):
            EnvironmentValidator.validate_aws_credentials()
            assert os.environ.get('AWS_REGION') == 'eu-west-1'
    
    def test_validate_clickhouse_env_success(self):
        """Test successful ClickHouse environment validation."""
        with patch.dict(os.environ, {'CLICKHOUSE_SECRET_NAME': 'test-secret'}):
            result = EnvironmentValidator.validate_clickhouse_env()
            assert result is True
    
    def test_validate_clickhouse_env_missing(self):
        """Test ClickHouse environment validation with missing variables."""
        with patch.dict(os.environ, {}, clear=True):
            result = EnvironmentValidator.validate_clickhouse_env()
            assert result is False
    
    def test_get_standard_lambda_env_success(self):
        """Test successful Lambda environment variable retrieval."""
        lambda_env = {
            'ENVIRONMENT': 'dev',
            'AWS_REGION': 'us-east-2',
            'TENANT_SERVICES_TABLE': 'tenant-services-dev',
            'LAST_UPDATED_TABLE': 'last-updated-dev',
            'BUCKET_NAME': 'test-bucket'
        }
        
        with patch.dict(os.environ, lambda_env):
            result = EnvironmentValidator.get_standard_lambda_env()
            assert result == lambda_env
    
    def test_get_standard_lambda_env_missing(self):
        """Test Lambda environment validation with missing variables."""
        with patch.dict(os.environ, {'ENVIRONMENT': 'dev'}, clear=True):
            with pytest.raises(EnvironmentError) as exc_info:
                EnvironmentValidator.get_standard_lambda_env()
            
            assert "Lambda missing required environment variables" in str(exc_info.value)
    
    def test_setup_development_env(self):
        """Test development environment setup."""
        # Start with clean environment
        with patch.dict(os.environ, {}, clear=True):
            EnvironmentValidator.setup_development_env()
            
            # Check that defaults were set
            assert os.environ.get('AWS_REGION') == 'us-east-2'
            assert os.environ.get('ENVIRONMENT') == 'dev'
            assert os.environ.get('AWS_SDK_LOAD_CONFIG') == '1'
    
    def test_setup_development_env_preserves_existing(self):
        """Test that development setup preserves existing values."""
        existing_env = {
            'AWS_REGION': 'eu-west-1',
            'ENVIRONMENT': 'staging'
        }
        
        with patch.dict(os.environ, existing_env):
            EnvironmentValidator.setup_development_env()
            
            # Check that existing values were preserved
            assert os.environ.get('AWS_REGION') == 'eu-west-1'
            assert os.environ.get('ENVIRONMENT') == 'staging'
            # But new defaults should be set
            assert os.environ.get('AWS_SDK_LOAD_CONFIG') == '1'
    
    @patch('shared.env_validator.logger')
    def test_logging_on_success(self, mock_logger):
        """Test that success is logged appropriately."""
        with patch.dict(os.environ, {'TEST_VAR': 'value'}):
            EnvironmentValidator.validate_required_vars(['TEST_VAR'], "Test")
            mock_logger.info.assert_called_with("Test environment validation successful")
    
    @patch('shared.env_validator.logger')
    def test_logging_on_failure(self, mock_logger):
        """Test that failures are logged appropriately."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError):
                EnvironmentValidator.validate_required_vars(['MISSING_VAR'], "Test")
            
            mock_logger.error.assert_called()
            error_call = mock_logger.error.call_args[0][0]
            assert "Test missing required environment variables" in error_call
    
    @patch('shared.env_validator.logger')
    def test_aws_credentials_warning_logging(self, mock_logger):
        """Test that AWS credentials warnings are logged."""
        with patch.dict(os.environ, {}, clear=True):
            EnvironmentValidator.validate_aws_credentials()
            
            # Should log warnings for missing region and credentials
            warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
            assert any("AWS_REGION not set" in call for call in warning_calls)
            assert any("No AWS credentials found" in call for call in warning_calls)
    
    @patch('shared.env_validator.logger')
    def test_development_env_logging(self, mock_logger):
        """Test that development environment setup logs defaults."""
        with patch.dict(os.environ, {}, clear=True):
            EnvironmentValidator.setup_development_env()
            
            # Should log each default that was set
            info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
            assert any("AWS_REGION=us-east-2" in call for call in info_calls)
            assert any("ENVIRONMENT=dev" in call for call in info_calls)
            assert any("AWS_SDK_LOAD_CONFIG=1" in call for call in info_calls)