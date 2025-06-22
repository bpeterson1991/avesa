"""
Tests for AWS environment setup utilities.
"""
import pytest
import os
from unittest.mock import patch, Mock
import sys

# Add scripts/shared to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts', 'shared'))
from aws_setup import AWSEnvironmentSetup


class TestAWSEnvironmentSetup:
    """Test AWS environment setup functionality."""

    def setup_method(self):
        """Set up test environment."""
        # Store original environment
        self.original_env = os.environ.copy()
        
        # Clear AWS environment variables
        for var in ['AWS_PROFILE', 'AWS_REGION', 'AWS_SDK_LOAD_CONFIG']:
            if var in os.environ:
                del os.environ[var]

    def teardown_method(self):
        """Restore original environment."""
        os.environ.clear()
        os.environ.update(self.original_env)

    def test_setup_aws_environment_defaults(self):
        """Test AWS environment setup with default values."""
        AWSEnvironmentSetup.setup_aws_environment()
        
        assert os.environ['AWS_SDK_LOAD_CONFIG'] == '1'
        assert os.environ['AWS_PROFILE'] == 'AdministratorAccess-123938354448'
        assert os.environ['AWS_REGION'] == 'us-east-2'

    def test_setup_aws_environment_custom_values(self):
        """Test AWS environment setup with custom values."""
        AWSEnvironmentSetup.setup_aws_environment(
            profile_name='custom-profile',
            region='us-west-2'
        )
        
        assert os.environ['AWS_SDK_LOAD_CONFIG'] == '1'
        assert os.environ['AWS_PROFILE'] == 'custom-profile'
        assert os.environ['AWS_REGION'] == 'us-west-2'

    def test_setup_aws_environment_existing_values(self):
        """Test that existing environment variables are not overridden."""
        os.environ['AWS_PROFILE'] = 'existing-profile'
        os.environ['AWS_REGION'] = 'existing-region'
        
        AWSEnvironmentSetup.setup_aws_environment(
            profile_name='new-profile',
            region='new-region'
        )
        
        # Should not override existing values
        assert os.environ['AWS_PROFILE'] == 'existing-profile'
        assert os.environ['AWS_REGION'] == 'existing-region'
        assert os.environ['AWS_SDK_LOAD_CONFIG'] == '1'

    def test_setup_script_environment(self):
        """Test script environment setup with logging."""
        script_name = 'test_script.py'
        
        result = AWSEnvironmentSetup.setup_script_environment(script_name)
        
        # Check environment was set up
        assert os.environ['AWS_SDK_LOAD_CONFIG'] == '1'
        assert os.environ['AWS_PROFILE'] == 'AdministratorAccess-123938354448'
        assert os.environ['AWS_REGION'] == 'us-east-2'
        
        # Check return value
        expected_result = {
            'AWS_PROFILE': 'AdministratorAccess-123938354448',
            'AWS_REGION': 'us-east-2',
            'AWS_SDK_LOAD_CONFIG': '1'
        }
        assert result == expected_result

    def test_validate_aws_setup_valid(self):
        """Test AWS setup validation with valid configuration."""
        os.environ['AWS_SDK_LOAD_CONFIG'] = '1'
        os.environ['AWS_PROFILE'] = 'test-profile'
        
        result = AWSEnvironmentSetup.validate_aws_setup()
        assert result is True

    def test_validate_aws_setup_with_access_keys(self):
        """Test AWS setup validation with access keys."""
        os.environ['AWS_SDK_LOAD_CONFIG'] = '1'
        os.environ['AWS_ACCESS_KEY_ID'] = 'test-key'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'test-secret'
        
        result = AWSEnvironmentSetup.validate_aws_setup()
        assert result is True

    def test_validate_aws_setup_missing_sdk_config(self):
        """Test AWS setup validation with missing SDK config."""
        os.environ['AWS_PROFILE'] = 'test-profile'
        
        result = AWSEnvironmentSetup.validate_aws_setup()
        assert result is False

    def test_validate_aws_setup_no_credentials(self):
        """Test AWS setup validation with no credentials."""
        os.environ['AWS_SDK_LOAD_CONFIG'] = '1'
        
        result = AWSEnvironmentSetup.validate_aws_setup()
        assert result is False

    def test_get_aws_config(self):
        """Test getting current AWS configuration."""
        os.environ['AWS_PROFILE'] = 'test-profile'
        os.environ['AWS_REGION'] = 'us-west-1'
        os.environ['AWS_SDK_LOAD_CONFIG'] = '1'
        
        config = AWSEnvironmentSetup.get_aws_config()
        
        expected_config = {
            'profile': 'test-profile',
            'region': 'us-west-1',
            'sdk_load_config': '1'
        }
        assert config == expected_config

    def test_get_aws_config_defaults(self):
        """Test getting AWS configuration with default values."""
        config = AWSEnvironmentSetup.get_aws_config()
        
        expected_config = {
            'profile': 'default',
            'region': 'us-east-2',
            'sdk_load_config': '1'
        }
        assert config == expected_config


if __name__ == '__main__':
    pytest.main([__file__, '-v'])