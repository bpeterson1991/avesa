"""
Test suite for the centralized environment configuration module.
"""

import pytest
import os
import tempfile
import json
from unittest.mock import patch, mock_open

from src.shared.environment import Environment, EnvironmentConfig, get_current_environment, get_table_name


class TestEnvironmentConfig:
    """Test cases for Environment configuration management."""
    
    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing."""
        return {
            "environments": {
                "dev": {
                    "account": "123456789012",
                    "region": "us-east-2",
                    "bucket_name": "data-storage-msp-dev",
                    "table_suffix": "-dev",
                    "lambda_memory": 512,
                    "lambda_timeout": 300
                },
                "prod": {
                    "account": "987654321098",
                    "region": "us-east-2",
                    "bucket_name": "data-storage-msp-prod",
                    "table_suffix": "-prod",
                    "lambda_memory": 1024,
                    "lambda_timeout": 900
                }
            },
            "deployment_profiles": {
                "avesa-dev": "dev",
                "avesa-production": "prod"
            }
        }
    
    def test_get_config_success(self, sample_config):
        """Test successful configuration loading."""
        with patch('builtins.open', mock_open(read_data=json.dumps(sample_config))):
            Environment.clear_cache()  # Clear any cached config
            config = Environment.get_config("dev")
            
            assert isinstance(config, EnvironmentConfig)
            assert config.account == "123456789012"
            assert config.region == "us-east-2"
            assert config.bucket_name == "data-storage-msp-dev"
            assert config.table_suffix == "-dev"
            assert config.lambda_memory == 512
            assert config.lambda_timeout == 300
    
    def test_get_config_invalid_environment(self, sample_config):
        """Test error handling for invalid environment name."""
        with patch('builtins.open', mock_open(read_data=json.dumps(sample_config))):
            Environment.clear_cache()
            with pytest.raises(ValueError, match="Environment 'invalid' not found"):
                Environment.get_config("invalid")
    
    def test_get_table_names(self, sample_config):
        """Test table name generation."""
        with patch('builtins.open', mock_open(read_data=json.dumps(sample_config))):
            Environment.clear_cache()
            table_names = Environment.get_table_names("dev")
            
            expected_tables = {
                "tenant_services": "TenantServices-dev",
                "last_updated": "LastUpdated-dev",
                "processing_jobs": "ProcessingJobs-dev",
                "chunk_progress": "ChunkProgress-dev",
                "data_quality_metrics": "DataQualityMetrics-dev",
                "pipeline_metrics": "PipelineMetrics-dev"
            }
            
            assert table_names == expected_tables
    
    def test_get_lambda_env_vars(self, sample_config):
        """Test Lambda environment variables generation."""
        with patch('builtins.open', mock_open(read_data=json.dumps(sample_config))):
            Environment.clear_cache()
            env_vars = Environment.get_lambda_env_vars("dev")
            
            assert env_vars["ENVIRONMENT"] == "dev"
            assert env_vars["AWS_REGION"] == "us-east-2"
            assert env_vars["DATA_BUCKET"] == "data-storage-msp-dev"
            assert env_vars["TENANT_SERVICES_TABLE"] == "TenantServices-dev"
            assert env_vars["TABLE_SUFFIX"] == "-dev"
    
    def test_get_deployment_profiles(self, sample_config):
        """Test deployment profile mapping."""
        with patch('builtins.open', mock_open(read_data=json.dumps(sample_config))):
            Environment.clear_cache()
            profiles = Environment.get_deployment_profiles()
            
            assert profiles == {
                "avesa-dev": "dev",
                "avesa-production": "prod"
            }
    
    def test_get_environment_by_account(self, sample_config):
        """Test environment detection by account ID."""
        with patch('builtins.open', mock_open(read_data=json.dumps(sample_config))):
            Environment.clear_cache()
            
            assert Environment.get_environment_by_account("123456789012") == "dev"
            assert Environment.get_environment_by_account("987654321098") == "prod"
            assert Environment.get_environment_by_account("000000000000") is None
    
    def test_list_environments(self, sample_config):
        """Test listing all available environments."""
        with patch('builtins.open', mock_open(read_data=json.dumps(sample_config))):
            Environment.clear_cache()
            environments = Environment.list_environments()
            
            assert set(environments) == {"dev", "prod"}
    
    def test_validate_environment(self, sample_config):
        """Test environment validation."""
        with patch('builtins.open', mock_open(read_data=json.dumps(sample_config))):
            Environment.clear_cache()
            
            assert Environment.validate_environment("dev") is True
            assert Environment.validate_environment("prod") is True
            
            with pytest.raises(ValueError):
                Environment.validate_environment("invalid")


class TestConvenienceFunctions:
    """Test cases for convenience functions."""
    
    def test_get_current_environment_from_env_var(self):
        """Test environment detection from environment variable."""
        with patch.dict(os.environ, {"ENVIRONMENT": "dev"}):
            assert get_current_environment() == "dev"
    
    def test_get_current_environment_from_cdk_var(self):
        """Test environment detection from CDK environment variable."""
        with patch.dict(os.environ, {"CDK_ENVIRONMENT": "prod"}, clear=True):
            assert get_current_environment() == "prod"
    
    def test_get_current_environment_none(self):
        """Test environment detection when no variables are set."""
        with patch.dict(os.environ, {}, clear=True):
            assert get_current_environment() is None
    
    @patch('src.shared.environment.Environment.get_table_names')
    def test_get_table_name_with_env(self, mock_get_table_names):
        """Test table name retrieval with explicit environment."""
        mock_get_table_names.return_value = {
            "tenant_services": "TenantServices-dev",
            "last_updated": "LastUpdated-dev"
        }
        
        result = get_table_name("tenant_services", "dev")
        assert result == "TenantServices-dev"
        mock_get_table_names.assert_called_once_with("dev")
    
    def test_get_table_name_invalid_type(self):
        """Test error handling for invalid table type."""
        with patch('src.shared.environment.Environment.get_table_names') as mock_get_table_names:
            mock_get_table_names.return_value = {"tenant_services": "TenantServices-dev"}
            
            with pytest.raises(ValueError, match="Invalid table type 'invalid'"):
                get_table_name("invalid", "dev")
    
    def test_get_table_name_no_environment(self):
        """Test error handling when environment cannot be determined."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Environment name must be provided"):
                get_table_name("tenant_services")


if __name__ == "__main__":
    pytest.main([__file__])