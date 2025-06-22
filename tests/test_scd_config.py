"""
Tests for SCD Configuration Management

This module tests the SCD configuration utilities to ensure proper
handling of SCD type configuration from canonical mapping files.
"""

import pytest
import json
import os
from unittest.mock import Mock, patch, mock_open
from src.shared.scd_config import (
    SCDConfigManager,
    SCDTypeEnum,
    get_scd_type,
    is_scd_type_1,
    is_scd_type_2,
    filter_tables_by_scd_type,
    validate_scd_configuration
)


class TestSCDConfigManager:
    """Test cases for SCDConfigManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_s3_client = Mock()
        self.scd_manager = SCDConfigManager(s3_client=self.mock_s3_client)

    def test_init_with_s3_client(self):
        """Test initialization with provided S3 client."""
        manager = SCDConfigManager(s3_client=self.mock_s3_client)
        assert manager.s3_client == self.mock_s3_client
        assert manager._scd_config_cache == {}

    @patch('src.shared.scd_config.AWSClientFactory')
    def test_init_without_s3_client(self, mock_aws_factory):
        """Test initialization without S3 client creates one."""
        mock_factory_instance = Mock()
        mock_aws_factory.return_value = mock_factory_instance
        mock_factory_instance.get_client_bundle.return_value = {'s3': self.mock_s3_client}
        
        manager = SCDConfigManager()
        assert manager.s3_client == self.mock_s3_client

    def test_get_scd_type_companies(self):
        """Test getting SCD type for companies table."""
        # Mock the mapping file content
        companies_mapping = {
            "scd_type": "type_1",
            "connectwise": {
                "company/companies": {
                    "id": "id",
                    "company_name": "name"
                }
            }
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(companies_mapping))):
            with patch('os.path.exists', return_value=True):
                scd_type = self.scd_manager.get_scd_type('companies')
                assert scd_type == 'type_1'

    def test_get_scd_type_tickets(self):
        """Test getting SCD type for tickets table."""
        # Mock the mapping file content
        tickets_mapping = {
            "scd_type": "type_2",
            "connectwise": {
                "service/tickets": {
                    "id": "id",
                    "summary": "summary"
                }
            }
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(tickets_mapping))):
            with patch('os.path.exists', return_value=True):
                scd_type = self.scd_manager.get_scd_type('tickets')
                assert scd_type == 'type_2'

    def test_get_scd_type_invalid_type(self):
        """Test getting SCD type with invalid type defaults to type_1."""
        # Mock the mapping file content with invalid SCD type
        invalid_mapping = {
            "scd_type": "type_3",  # Invalid type
            "connectwise": {
                "company/companies": {
                    "id": "id"
                }
            }
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(invalid_mapping))):
            with patch('os.path.exists', return_value=True):
                scd_type = self.scd_manager.get_scd_type('companies')
                assert scd_type == 'type_1'  # Should default to type_1

    def test_get_scd_type_missing_scd_type(self):
        """Test getting SCD type when scd_type field is missing."""
        # Mock the mapping file content without scd_type
        mapping_without_scd = {
            "connectwise": {
                "company/companies": {
                    "id": "id"
                }
            }
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(mapping_without_scd))):
            with patch('os.path.exists', return_value=True):
                scd_type = self.scd_manager.get_scd_type('companies')
                assert scd_type == 'type_1'  # Should default to type_1

    def test_get_scd_type_file_not_found(self):
        """Test getting SCD type when mapping file doesn't exist."""
        with patch('os.path.exists', return_value=False):
            scd_type = self.scd_manager.get_scd_type('nonexistent_table')
            assert scd_type == 'type_1'  # Should use default

    def test_is_scd_type_1(self):
        """Test checking if table is SCD Type 1."""
        companies_mapping = {"scd_type": "type_1"}
        
        with patch('builtins.open', mock_open(read_data=json.dumps(companies_mapping))):
            with patch('os.path.exists', return_value=True):
                assert self.scd_manager.is_scd_type_1('companies') is True
                assert self.scd_manager.is_scd_type_2('companies') is False

    def test_is_scd_type_2(self):
        """Test checking if table is SCD Type 2."""
        tickets_mapping = {"scd_type": "type_2"}
        
        with patch('builtins.open', mock_open(read_data=json.dumps(tickets_mapping))):
            with patch('os.path.exists', return_value=True):
                assert self.scd_manager.is_scd_type_2('tickets') is True
                assert self.scd_manager.is_scd_type_1('tickets') is False

    def test_get_scd_config_for_tables(self):
        """Test getting SCD configuration for multiple tables."""
        def mock_get_scd_type(table_name, bucket=None):
            scd_types = {
                'companies': 'type_1',
                'contacts': 'type_1',
                'tickets': 'type_2',
                'time_entries': 'type_1'
            }
            return scd_types.get(table_name, 'type_1')
        
        with patch.object(self.scd_manager, 'get_scd_type', side_effect=mock_get_scd_type):
            tables = ['companies', 'contacts', 'tickets', 'time_entries']
            config = self.scd_manager.get_scd_config_for_tables(tables)
            
            expected = {
                'companies': 'type_1',
                'contacts': 'type_1',
                'tickets': 'type_2',
                'time_entries': 'type_1'
            }
            assert config == expected

    def test_filter_tables_by_scd_type(self):
        """Test filtering tables by SCD type."""
        def mock_get_scd_type(table_name, bucket=None):
            scd_types = {
                'companies': 'type_1',
                'contacts': 'type_1',
                'tickets': 'type_2',
                'time_entries': 'type_1'
            }
            return scd_types.get(table_name, 'type_1')
        
        with patch.object(self.scd_manager, 'get_scd_type', side_effect=mock_get_scd_type):
            all_tables = ['companies', 'contacts', 'tickets', 'time_entries']
            
            # Filter for Type 1 tables
            type_1_tables = self.scd_manager.filter_tables_by_scd_type(all_tables, 'type_1')
            assert type_1_tables == ['companies', 'contacts', 'time_entries']
            
            # Filter for Type 2 tables
            type_2_tables = self.scd_manager.filter_tables_by_scd_type(all_tables, 'type_2')
            assert type_2_tables == ['tickets']

    def test_validate_scd_configuration_valid(self):
        """Test validating a valid SCD configuration."""
        valid_mapping = {
            "scd_type": "type_2",
            "connectwise": {
                "service/tickets": {
                    "id": "id",
                    "summary": "summary"
                }
            },
            "servicenow": {
                "incident": {
                    "id": "sys_id",
                    "summary": "short_description"
                }
            }
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(valid_mapping))):
            with patch('os.path.exists', return_value=True):
                result = self.scd_manager.validate_scd_configuration('tickets')
                
                assert result['valid'] is True
                assert result['table_name'] == 'tickets'
                assert result['scd_type'] == 'type_2'
                assert result['service_count'] == 2
                assert result['issues'] == []

    def test_validate_scd_configuration_invalid(self):
        """Test validating an invalid SCD configuration."""
        invalid_mapping = {
            "scd_type": "type_3",  # Invalid SCD type
            # No service mappings
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(invalid_mapping))):
            with patch('os.path.exists', return_value=True):
                result = self.scd_manager.validate_scd_configuration('invalid_table')
                
                assert result['valid'] is False
                assert result['table_name'] == 'invalid_table'
                assert 'Invalid scd_type value: type_3' in result['issues']
                assert 'No service mappings found' in result['issues']

    def test_validate_scd_configuration_missing_scd_type(self):
        """Test validating configuration with missing scd_type."""
        mapping_without_scd = {
            "connectwise": {
                "company/companies": {
                    "id": "id"
                }
            }
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(mapping_without_scd))):
            with patch('os.path.exists', return_value=True):
                result = self.scd_manager.validate_scd_configuration('companies')
                
                assert result['valid'] is False
                assert 'Missing scd_type configuration' in result['issues']

    def test_caching_behavior(self):
        """Test that SCD configurations are cached properly."""
        companies_mapping = {"scd_type": "type_1"}
        
        with patch('builtins.open', mock_open(read_data=json.dumps(companies_mapping))) as mock_file:
            with patch('os.path.exists', return_value=True):
                # First call should read from file
                scd_type1 = self.scd_manager.get_scd_type('companies')
                assert scd_type1 == 'type_1'
                
                # Second call should use cache
                scd_type2 = self.scd_manager.get_scd_type('companies')
                assert scd_type2 == 'type_1'
                
                # File should only be opened once due to caching
                assert mock_file.call_count == 1


class TestSCDConfigConvenienceFunctions:
    """Test cases for convenience functions."""

    @patch('src.shared.scd_config.SCDConfigManager')
    def test_get_scd_type_function(self, mock_manager_class):
        """Test get_scd_type convenience function."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.get_scd_type.return_value = 'type_2'
        
        result = get_scd_type('tickets', 'test-bucket')
        
        mock_manager_class.assert_called_once()
        mock_manager.get_scd_type.assert_called_once_with('tickets', 'test-bucket')
        assert result == 'type_2'

    @patch('src.shared.scd_config.SCDConfigManager')
    def test_is_scd_type_1_function(self, mock_manager_class):
        """Test is_scd_type_1 convenience function."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.is_scd_type_1.return_value = True
        
        result = is_scd_type_1('companies')
        
        mock_manager.is_scd_type_1.assert_called_once_with('companies', None)
        assert result is True

    @patch('src.shared.scd_config.SCDConfigManager')
    def test_is_scd_type_2_function(self, mock_manager_class):
        """Test is_scd_type_2 convenience function."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.is_scd_type_2.return_value = True
        
        result = is_scd_type_2('tickets')
        
        mock_manager.is_scd_type_2.assert_called_once_with('tickets', None)
        assert result is True

    @patch('src.shared.scd_config.SCDConfigManager')
    def test_filter_tables_by_scd_type_function(self, mock_manager_class):
        """Test filter_tables_by_scd_type convenience function."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.filter_tables_by_scd_type.return_value = ['tickets']
        
        tables = ['companies', 'contacts', 'tickets', 'time_entries']
        result = filter_tables_by_scd_type(tables, 'type_2')
        
        mock_manager.filter_tables_by_scd_type.assert_called_once_with(tables, 'type_2', None)
        assert result == ['tickets']

    @patch('src.shared.scd_config.SCDConfigManager')
    def test_validate_scd_configuration_function(self, mock_manager_class):
        """Test validate_scd_configuration convenience function."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        expected_result = {
            'table_name': 'tickets',
            'valid': True,
            'scd_type': 'type_2'
        }
        mock_manager.validate_scd_configuration.return_value = expected_result
        
        result = validate_scd_configuration('tickets')
        
        mock_manager.validate_scd_configuration.assert_called_once_with('tickets', None)
        assert result == expected_result


class TestSCDTypeEnum:
    """Test cases for SCDTypeEnum."""

    def test_enum_values(self):
        """Test that enum has correct values."""
        assert SCDTypeEnum.TYPE_1.value == "type_1"
        assert SCDTypeEnum.TYPE_2.value == "type_2"

    def test_enum_membership(self):
        """Test enum membership checks."""
        assert "type_1" in [e.value for e in SCDTypeEnum]
        assert "type_2" in [e.value for e in SCDTypeEnum]
        assert "type_3" not in [e.value for e in SCDTypeEnum]


if __name__ == '__main__':
    pytest.main([__file__])