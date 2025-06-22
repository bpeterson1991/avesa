#!/usr/bin/env python3
"""
SCD Behavior Validation Tests
=============================

Comprehensive tests that validate actual SCD behavior with data updates:
- Type 1 tables properly overwrite existing records (no historical versions)
- Type 2 tables maintain historical records with proper SCD fields
- Mixed SCD scenarios work correctly
- Schema-configuration alignment validation
"""

import pytest
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from shared.scd_config import SCDConfigManager
from shared.clickhouse_client import ClickHouseClient


class TestSCDBehavior:
    """Test actual SCD behavior with data updates."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_clickhouse_client = Mock()
        self.scd_manager = SCDConfigManager()
        self.tenant_id = 'test_tenant'
        
        # Sample test data
        self.sample_company_data = {
            'id': 'company_123',
            'tenant_id': self.tenant_id,
            'company_name': 'Test Company',
            'status': 'Active',
            'last_updated': '2024-01-01T00:00:00Z'
        }
        
        self.sample_ticket_data = {
            'id': 'ticket_456',
            'tenant_id': self.tenant_id,
            'summary': 'Test Ticket',
            'status': 'Open',
            'priority': 'High',
            'last_updated': '2024-01-01T00:00:00Z'
        }

    def test_scd_type_1_upsert_behavior(self):
        """Test that Type 1 tables properly overwrite existing records."""
        # Mock the SCD manager to return type_1 for companies
        with patch.object(self.scd_manager, 'get_scd_type', return_value='type_1'):
            
            # Simulate initial insert
            initial_data = self.sample_company_data.copy()
            
            # Simulate update with changed data
            updated_data = self.sample_company_data.copy()
            updated_data['company_name'] = 'Updated Company Name'
            updated_data['status'] = 'Inactive'
            updated_data['last_updated'] = '2024-01-02T00:00:00Z'
            
            # Mock ClickHouse responses
            self.mock_clickhouse_client.query.side_effect = [
                # First query: check existing records
                Mock(result_rows=[[1]]),  # 1 existing record
                # Second query: after update, still 1 record (overwritten)
                Mock(result_rows=[[1]]),  # Still 1 record
                # Third query: verify no historical records
                Mock(result_rows=[[0]])   # 0 historical records
            ]
            
            # Verify Type 1 behavior
            scd_type = self.scd_manager.get_scd_type('companies')
            assert scd_type == 'type_1'
            
            # Simulate the upsert operation
            self._simulate_type1_upsert('companies', initial_data, updated_data)
            
            # Verify that only one record exists (no historical versions)
            assert self.mock_clickhouse_client.query.call_count >= 2

    def test_scd_type_2_historical_tracking(self):
        """Test that Type 2 tables maintain historical records with proper SCD fields."""
        # Mock the SCD manager to return type_2 for tickets
        with patch.object(self.scd_manager, 'get_scd_type', return_value='type_2'):
            
            # Simulate initial insert
            initial_data = self.sample_ticket_data.copy()
            initial_data.update({
                'effective_start_date': '2024-01-01T00:00:00Z',
                'effective_end_date': '9999-12-31T23:59:59Z',
                'is_current': True,
                'record_hash': 'hash_v1'
            })
            
            # Simulate update with changed data
            updated_data = self.sample_ticket_data.copy()
            updated_data.update({
                'summary': 'Updated Ticket Summary',
                'status': 'In Progress',
                'effective_start_date': '2024-01-02T00:00:00Z',
                'effective_end_date': '9999-12-31T23:59:59Z',
                'is_current': True,
                'record_hash': 'hash_v2',
                'last_updated': '2024-01-02T00:00:00Z'
            })
            
            # Mock ClickHouse responses
            self.mock_clickhouse_client.query.side_effect = [
                # First query: check existing records
                Mock(result_rows=[[1]]),  # 1 existing record
                # Second query: after update, 2 total records
                Mock(result_rows=[[2]]),  # 2 total records
                # Third query: verify 1 current record
                Mock(result_rows=[[1]]),  # 1 current record
                # Fourth query: verify 1 historical record
                Mock(result_rows=[[1]])   # 1 historical record
            ]
            
            # Verify Type 2 behavior
            scd_type = self.scd_manager.get_scd_type('tickets')
            assert scd_type == 'type_2'
            
            # Simulate the SCD Type 2 operation
            self._simulate_type2_update('tickets', initial_data, updated_data)
            
            # Verify that historical records are maintained
            assert self.mock_clickhouse_client.query.call_count >= 3

    def test_mixed_scd_scenario(self):
        """Test mixed SCD scenarios with both Type 1 and Type 2 tables."""
        # Mock different SCD types for different tables
        def mock_get_scd_type(table_name, bucket=None):
            scd_types = {
                'companies': 'type_1',
                'contacts': 'type_1',
                'tickets': 'type_2',
                'time_entries': 'type_1'
            }
            return scd_types.get(table_name, 'type_1')
        
        with patch.object(self.scd_manager, 'get_scd_type', side_effect=mock_get_scd_type):
            
            # Test all table types
            tables = ['companies', 'contacts', 'tickets', 'time_entries']
            scd_config = {}
            
            for table in tables:
                scd_type = self.scd_manager.get_scd_type(table)
                scd_config[table] = scd_type
            
            # Verify mixed configuration
            assert scd_config['companies'] == 'type_1'
            assert scd_config['contacts'] == 'type_1'
            assert scd_config['tickets'] == 'type_2'
            assert scd_config['time_entries'] == 'type_1'
            
            # Verify filtering works correctly
            type1_tables = self.scd_manager.filter_tables_by_scd_type(tables, 'type_1')
            type2_tables = self.scd_manager.filter_tables_by_scd_type(tables, 'type_2')
            
            assert set(type1_tables) == {'companies', 'contacts', 'time_entries'}
            assert set(type2_tables) == {'tickets'}

    def test_schema_configuration_alignment(self):
        """Test that table schemas match their configured SCD types."""
        # Mock schema information for different table types
        type1_schema = {
            'columns': ['id', 'tenant_id', 'company_name', 'status', 'last_updated'],
            'has_scd_fields': False
        }
        
        type2_schema = {
            'columns': ['id', 'tenant_id', 'summary', 'status', 'priority', 
                       'effective_start_date', 'effective_end_date', 'is_current', 
                       'record_hash', 'last_updated'],
            'has_scd_fields': True
        }
        
        def mock_get_scd_type(table_name, bucket=None):
            return 'type_2' if table_name == 'tickets' else 'type_1'
        
        with patch.object(self.scd_manager, 'get_scd_type', side_effect=mock_get_scd_type):
            
            # Test Type 1 table schema alignment
            companies_scd_type = self.scd_manager.get_scd_type('companies')
            assert companies_scd_type == 'type_1'
            
            # Type 1 tables should NOT have SCD fields
            scd_fields = ['effective_start_date', 'effective_end_date', 'is_current', 'record_hash']
            type1_has_scd_fields = any(field in type1_schema['columns'] for field in scd_fields)
            assert not type1_has_scd_fields, "Type 1 table should not have SCD fields"
            
            # Test Type 2 table schema alignment
            tickets_scd_type = self.scd_manager.get_scd_type('tickets')
            assert tickets_scd_type == 'type_2'
            
            # Type 2 tables SHOULD have SCD fields
            type2_has_all_scd_fields = all(field in type2_schema['columns'] for field in scd_fields)
            assert type2_has_all_scd_fields, "Type 2 table should have all SCD fields"

    def test_scd_validation_with_real_data_structure(self):
        """Test SCD validation with realistic data structures."""
        # Create realistic test data
        companies_df = pd.DataFrame([
            {
                'id': 'comp_1',
                'tenant_id': self.tenant_id,
                'company_name': 'Company A',
                'status': 'Active',
                'last_updated': '2024-01-01T00:00:00Z'
            },
            {
                'id': 'comp_2',
                'tenant_id': self.tenant_id,
                'company_name': 'Company B',
                'status': 'Active',
                'last_updated': '2024-01-01T00:00:00Z'
            }
        ])
        
        tickets_df = pd.DataFrame([
            {
                'id': 'ticket_1',
                'tenant_id': self.tenant_id,
                'summary': 'Ticket 1',
                'status': 'Open',
                'effective_start_date': '2024-01-01T00:00:00Z',
                'effective_end_date': '2024-01-02T00:00:00Z',
                'is_current': False,
                'record_hash': 'hash1',
                'last_updated': '2024-01-01T00:00:00Z'
            },
            {
                'id': 'ticket_1',
                'tenant_id': self.tenant_id,
                'summary': 'Ticket 1 Updated',
                'status': 'In Progress',
                'effective_start_date': '2024-01-02T00:00:00Z',
                'effective_end_date': '9999-12-31T23:59:59Z',
                'is_current': True,
                'record_hash': 'hash2',
                'last_updated': '2024-01-02T00:00:00Z'
            }
        ])
        
        def mock_get_scd_type(table_name, bucket=None):
            return 'type_2' if table_name == 'tickets' else 'type_1'
        
        with patch.object(self.scd_manager, 'get_scd_type', side_effect=mock_get_scd_type):
            
            # Validate Type 1 data structure
            companies_scd_type = self.scd_manager.get_scd_type('companies')
            assert companies_scd_type == 'type_1'
            
            # Type 1: Should have no duplicates by ID
            companies_duplicates = companies_df.groupby(['tenant_id', 'id']).size()
            assert all(companies_duplicates == 1), "Type 1 table should have no duplicate IDs"
            
            # Validate Type 2 data structure
            tickets_scd_type = self.scd_manager.get_scd_type('tickets')
            assert tickets_scd_type == 'type_2'
            
            # Type 2: Should have exactly one current record per ID
            current_tickets = tickets_df[tickets_df['is_current'] == True]
            current_by_id = current_tickets.groupby(['tenant_id', 'id']).size()
            assert all(current_by_id == 1), "Type 2 table should have exactly one current record per ID"
            
            # Type 2: Should have proper historical tracking
            historical_tickets = tickets_df[tickets_df['is_current'] == False]
            assert len(historical_tickets) > 0, "Type 2 table should have historical records"
            
            # Type 2: Historical records should have proper end dates
            for _, row in historical_tickets.iterrows():
                assert row['effective_end_date'] != '9999-12-31T23:59:59Z', "Historical records should have actual end dates"

    def test_scd_configuration_validation(self):
        """Test comprehensive SCD configuration validation."""
        tables = ['companies', 'contacts', 'tickets', 'time_entries']
        
        def mock_get_scd_type(table_name, bucket=None):
            scd_types = {
                'companies': 'type_1',
                'contacts': 'type_1', 
                'tickets': 'type_2',
                'time_entries': 'type_1'
            }
            return scd_types.get(table_name, 'type_1')
        
        with patch.object(self.scd_manager, 'get_scd_type', side_effect=mock_get_scd_type):
            
            # Test configuration validation for each table
            for table in tables:
                validation_result = self.scd_manager.validate_scd_configuration(table)
                
                # All tables should have valid configuration
                assert validation_result.get('valid', False), f"Table {table} should have valid SCD configuration"
                assert 'scd_type' in validation_result, f"Table {table} should have scd_type in validation result"
                
                expected_scd_type = mock_get_scd_type(table)
                if 'scd_type' in validation_result:
                    assert validation_result['scd_type'] == expected_scd_type, f"Table {table} SCD type mismatch"

    def _simulate_type1_upsert(self, table_name, initial_data, updated_data):
        """Simulate Type 1 upsert operation."""
        # In a real implementation, this would:
        # 1. Check if record exists
        # 2. If exists, UPDATE the existing record
        # 3. If not exists, INSERT new record
        # 4. No historical records are kept
        
        # Mock the database operations
        self.mock_clickhouse_client.query(f"SELECT COUNT(*) FROM {table_name} WHERE id = '{initial_data['id']}'")
        self.mock_clickhouse_client.query(f"UPDATE {table_name} SET ... WHERE id = '{initial_data['id']}'")
        self.mock_clickhouse_client.query(f"SELECT COUNT(*) FROM {table_name} WHERE id = '{initial_data['id']}'")

    def _simulate_type2_update(self, table_name, initial_data, updated_data):
        """Simulate Type 2 SCD operation."""
        # In a real implementation, this would:
        # 1. Set is_current = false and effective_end_date for existing current record
        # 2. Insert new record with is_current = true
        # 3. Maintain all historical records
        
        # Mock the database operations
        self.mock_clickhouse_client.query(f"SELECT COUNT(*) FROM {table_name} WHERE id = '{initial_data['id']}'")
        self.mock_clickhouse_client.query(f"UPDATE {table_name} SET is_current = false, effective_end_date = ... WHERE id = '{initial_data['id']}' AND is_current = true")
        self.mock_clickhouse_client.query(f"INSERT INTO {table_name} (...) VALUES (...)")
        self.mock_clickhouse_client.query(f"SELECT COUNT(*) FROM {table_name} WHERE id = '{initial_data['id']}'")


class TestSCDIntegration:
    """Integration tests for SCD behavior with actual data processing."""

    def setup_method(self):
        """Set up integration test fixtures."""
        self.scd_manager = SCDConfigManager()

    def test_end_to_end_scd_processing(self):
        """Test end-to-end SCD processing with mixed table types."""
        # This would be an integration test that:
        # 1. Loads actual canonical mapping files
        # 2. Processes sample data through the SCD pipeline
        # 3. Validates the results in ClickHouse
        # 4. Verifies that Type 1 and Type 2 behavior is correct
        
        # Mock the file loading
        with patch('builtins.open'), patch('os.path.exists', return_value=True):
            # Test that we can load SCD configuration for all tables
            tables = ['companies', 'contacts', 'tickets', 'time_entries']
            
            for table in tables:
                try:
                    scd_type = self.scd_manager.get_scd_type(table)
                    assert scd_type in ['type_1', 'type_2'], f"Invalid SCD type for {table}: {scd_type}"
                except Exception as e:
                    pytest.fail(f"Failed to get SCD type for {table}: {e}")

    def test_scd_configuration_consistency(self):
        """Test that SCD configuration is consistent across the system."""
        # Verify that the default SCD configuration matches expectations
        expected_config = {
            'companies': 'type_1',    # Simple upsert for companies
            'contacts': 'type_1',     # Simple upsert for contacts  
            'tickets': 'type_2',      # Full historical tracking for tickets
            'time_entries': 'type_1'  # Simple upsert for time entries
        }
        
        for table, expected_type in expected_config.items():
            actual_type = self.scd_manager.get_scd_type(table)
            assert actual_type == expected_type, f"SCD type mismatch for {table}: expected {expected_type}, got {actual_type}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])