#!/usr/bin/env python3
"""
Unit tests for schema alignment between canonical transform and ClickHouse
"""

import unittest
import sys
import os

# Add src paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'shared'))

from shared.canonical_schema import CanonicalSchemaManager


class TestSchemaAlignment(unittest.TestCase):
    """Test schema alignment functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.tables = ['companies', 'contacts', 'tickets', 'time_entries']
    
    def test_canonical_schema_manager_exists(self):
        """Test that the canonical schema manager is properly imported"""
        self.assertTrue(hasattr(CanonicalSchemaManager, 'get_standard_metadata_fields'))
        self.assertTrue(hasattr(CanonicalSchemaManager, 'get_complete_schema'))
        self.assertTrue(hasattr(CanonicalSchemaManager, 'validate_schema_alignment'))
    
    def test_metadata_fields_scd_type_1(self):
        """Test metadata fields for SCD Type 1"""
        fields = CanonicalSchemaManager.get_standard_metadata_fields('type_1')
        
        expected_fields = ['tenant_id', 'ingestion_timestamp', 'record_hash']
        self.assertEqual(set(fields), set(expected_fields))
        
        # SCD Type 1 should NOT have SCD Type 2 fields
        scd_type2_fields = ['effective_start_date', 'effective_end_date', 'is_current']
        for field in scd_type2_fields:
            self.assertNotIn(field, fields)
    
    def test_metadata_fields_scd_type_2(self):
        """Test metadata fields for SCD Type 2"""
        fields = CanonicalSchemaManager.get_standard_metadata_fields('type_2')
        
        expected_fields = [
            'tenant_id', 'ingestion_timestamp', 'record_hash',
            'effective_start_date', 'effective_end_date', 'is_current'
        ]
        self.assertEqual(set(fields), set(expected_fields))
    
    def test_load_canonical_mapping(self):
        """Test loading canonical mapping files"""
        for table_name in self.tables:
            with self.subTest(table=table_name):
                mapping = CanonicalSchemaManager.load_canonical_mapping(table_name)
                
                # Should have basic structure
                self.assertIsInstance(mapping, dict)
                self.assertIn('scd_type', mapping)
                
                # Should have at least one service mapping
                service_keys = [k for k in mapping.keys() if k != 'scd_type']
                self.assertGreater(len(service_keys), 0)
    
    def test_extract_canonical_fields(self):
        """Test extracting canonical fields from mappings"""
        for table_name in self.tables:
            with self.subTest(table=table_name):
                mapping = CanonicalSchemaManager.load_canonical_mapping(table_name)
                fields = CanonicalSchemaManager.extract_canonical_fields(mapping)
                
                # Should extract non-empty field set
                self.assertIsInstance(fields, set)
                self.assertGreater(len(fields), 0)
                
                # Should contain expected core fields
                self.assertIn('id', fields)
                
                # For companies, should contain company_name
                if table_name == 'companies':
                    self.assertIn('company_name', fields)
    
    def test_complete_schema_generation(self):
        """Test complete schema generation"""
        for table_name in self.tables:
            with self.subTest(table=table_name):
                mapping = CanonicalSchemaManager.load_canonical_mapping(table_name)
                canonical_fields = CanonicalSchemaManager.extract_canonical_fields(mapping)
                scd_type = CanonicalSchemaManager.get_scd_type(mapping)
                
                complete_schema = CanonicalSchemaManager.get_complete_schema(
                    table_name, canonical_fields, scd_type
                )
                
                # Should include all canonical fields
                for field in canonical_fields:
                    self.assertIn(field, complete_schema)
                
                # Should include metadata fields
                metadata_fields = CanonicalSchemaManager.get_standard_metadata_fields(scd_type)
                for field in metadata_fields:
                    self.assertIn(field, complete_schema)
                
                # Schema should be sorted for consistency
                self.assertEqual(complete_schema, sorted(complete_schema))
    
    def test_schema_validation(self):
        """Test schema validation functionality"""
        # Test perfect alignment
        schema1 = ['field_a', 'field_b', 'field_c']
        schema2 = ['field_a', 'field_b', 'field_c']
        
        validation = CanonicalSchemaManager.validate_schema_alignment(schema1, schema2)
        
        self.assertTrue(validation['is_aligned'])
        self.assertEqual(validation['canonical_field_count'], 3)
        self.assertEqual(validation['clickhouse_field_count'], 3)
        self.assertEqual(len(validation['missing_in_clickhouse']), 0)
        self.assertEqual(len(validation['extra_in_clickhouse']), 0)
        
        # Test misalignment
        schema3 = ['field_a', 'field_b', 'field_d']  # Different field
        validation = CanonicalSchemaManager.validate_schema_alignment(schema1, schema3)
        
        self.assertFalse(validation['is_aligned'])
        self.assertIn('field_c', validation['missing_in_clickhouse'])
        self.assertIn('field_d', validation['extra_in_clickhouse'])
    
    def test_companies_table_specific_schema(self):
        """Test companies table specific schema requirements"""
        mapping = CanonicalSchemaManager.load_canonical_mapping('companies')
        canonical_fields = CanonicalSchemaManager.extract_canonical_fields(mapping)
        scd_type = CanonicalSchemaManager.get_scd_type(mapping)
        
        complete_schema = CanonicalSchemaManager.get_complete_schema(
            'companies', canonical_fields, scd_type
        )
        
        # Companies should be SCD Type 1
        self.assertEqual(scd_type, 'type_1')
        
        # Should contain required business fields
        required_fields = ['id', 'company_name', 'tenant_id', 'ingestion_timestamp', 'record_hash']
        for field in required_fields:
            self.assertIn(field, complete_schema)
        
        # Should NOT contain SCD Type 2 fields for Type 1 table
        scd_type2_fields = ['effective_start_date', 'effective_end_date', 'is_current']
        for field in scd_type2_fields:
            self.assertNotIn(field, complete_schema)
    
    def test_field_count_consistency(self):
        """Test that field counts are consistent and reasonable"""
        for table_name in self.tables:
            with self.subTest(table=table_name):
                mapping = CanonicalSchemaManager.load_canonical_mapping(table_name)
                canonical_fields = CanonicalSchemaManager.extract_canonical_fields(mapping)
                scd_type = CanonicalSchemaManager.get_scd_type(mapping)
                
                complete_schema = CanonicalSchemaManager.get_complete_schema(
                    table_name, canonical_fields, scd_type
                )
                
                metadata_fields = CanonicalSchemaManager.get_standard_metadata_fields(scd_type)
                
                # Total should equal business + metadata fields
                expected_total = len(canonical_fields) + len(metadata_fields)
                self.assertEqual(len(complete_schema), expected_total)
                
                # Should have reasonable field counts
                self.assertGreaterEqual(len(canonical_fields), 10)  # At least 10 business fields
                self.assertLessEqual(len(canonical_fields), 100)    # Less than 100 business fields
                
                # Metadata should be 3-6 fields depending on SCD type
                if scd_type == 'type_1':
                    self.assertEqual(len(metadata_fields), 3)
                else:  # type_2
                    self.assertEqual(len(metadata_fields), 6)


if __name__ == '__main__':
    unittest.main()