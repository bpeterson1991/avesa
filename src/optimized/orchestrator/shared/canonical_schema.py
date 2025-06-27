#!/usr/bin/env python3
"""
Canonical Schema Manager

Single source of truth for canonical data schema definitions.
Ensures perfect alignment between canonical parquet files and ClickHouse tables.
"""

from typing import List, Set, Dict, Any
import os
import json


class CanonicalSchemaManager:
    """Manages canonical schema definitions and metadata fields"""
    
    @staticmethod
    def get_standard_metadata_fields(scd_type: str = 'type_1') -> List[str]:
        """
        Get standard metadata fields that should be added to all canonical data.
        
        Args:
            scd_type: 'type_1' or 'type_2'
            
        Returns:
            List of metadata field names
        """
        # Base metadata fields for all canonical data
        base_fields = [
            'tenant_id',           # Tenant isolation
            'ingestion_timestamp', # When record was processed
            'record_hash'          # Data integrity hash
        ]
        
        # SCD Type 2 adds historical tracking fields
        if scd_type == 'type_2':
            base_fields.extend([
                'effective_start_date',  # When this version became active
                'effective_end_date',    # When this version was superseded (NULL for current)
                'is_current'            # Boolean flag for current record
            ])
        
        return base_fields
    
    @staticmethod
    def get_clickhouse_field_types() -> Dict[str, str]:
        """
        Get ClickHouse field type mappings for standard metadata fields.
        
        Returns:
            Dictionary mapping field names to ClickHouse types
        """
        return {
            # Standard metadata fields
            'tenant_id': 'String',
            'ingestion_timestamp': 'DateTime DEFAULT now()',
            'record_hash': 'String',
            
            # SCD Type 2 fields  
            'effective_start_date': 'DateTime DEFAULT now()',
            'effective_end_date': 'Nullable(DateTime)',
            'is_current': 'Bool DEFAULT true',
            
            # Business field type patterns
            'id': 'String',
            'company_name': 'String',
            'updated_date': 'DateTime DEFAULT now()'
        }
    
    @staticmethod
    def get_complete_schema(table_name: str, canonical_fields: Set[str], scd_type: str = 'type_1') -> List[str]:
        """
        Get complete schema for a canonical table including business and metadata fields.
        
        Args:
            table_name: Name of the canonical table
            canonical_fields: Set of business fields from canonical mapping
            scd_type: 'type_1' or 'type_2'
            
        Returns:
            Sorted list of all field names
        """
        # Get standard metadata fields
        metadata_fields = CanonicalSchemaManager.get_standard_metadata_fields(scd_type)
        
        # Combine business and metadata fields
        all_fields = canonical_fields.union(set(metadata_fields))
        
        # Return sorted for consistency
        return sorted(all_fields)
    
    @staticmethod
    def validate_schema_alignment(canonical_schema: List[str], clickhouse_schema: List[str]) -> Dict[str, Any]:
        """
        Validate that canonical and ClickHouse schemas are perfectly aligned.
        
        Args:
            canonical_schema: List of field names from canonical data
            clickhouse_schema: List of field names from ClickHouse table
            
        Returns:
            Validation result with mismatches
        """
        canonical_set = set(canonical_schema)
        clickhouse_set = set(clickhouse_schema)
        
        missing_in_clickhouse = canonical_set - clickhouse_set
        extra_in_clickhouse = clickhouse_set - canonical_set
        
        is_aligned = len(missing_in_clickhouse) == 0 and len(extra_in_clickhouse) == 0
        
        return {
            'is_aligned': is_aligned,
            'canonical_field_count': len(canonical_set),
            'clickhouse_field_count': len(clickhouse_set),
            'missing_in_clickhouse': sorted(list(missing_in_clickhouse)),
            'extra_in_clickhouse': sorted(list(extra_in_clickhouse)),
            'common_fields': sorted(list(canonical_set.intersection(clickhouse_set)))
        }
    
    @staticmethod
    def load_canonical_mapping(table_name: str, mappings_dir: str = None) -> Dict[str, Any]:
        """
        Load canonical mapping file for a table.
        
        Args:
            table_name: Name of the canonical table
            mappings_dir: Directory containing mapping files (optional)
            
        Returns:
            Canonical mapping dictionary
        """
        if not mappings_dir:
            # Default to project mappings directory
            current_dir = os.path.dirname(__file__)
            project_root = os.path.join(current_dir, '..', '..')
            mappings_dir = os.path.join(project_root, 'mappings', 'canonical')
        
        mapping_file = os.path.join(mappings_dir, f"{table_name}.json")
        
        if not os.path.exists(mapping_file):
            raise FileNotFoundError(f"Canonical mapping not found: {mapping_file}")
            
        with open(mapping_file, 'r') as f:
            return json.load(f)
    
    @staticmethod
    def extract_canonical_fields(mapping: Dict[str, Any]) -> Set[str]:
        """
        Extract all canonical field names from mapping.
        
        Args:
            mapping: Canonical mapping dictionary
            
        Returns:
            Set of canonical field names
        """
        fields = set()
        
        # Skip scd_type key
        for service_name, service_mapping in mapping.items():
            if service_name == 'scd_type':
                continue
                
            if isinstance(service_mapping, dict):
                for endpoint_path, field_mappings in service_mapping.items():
                    if isinstance(field_mappings, dict):
                        # The keys are the canonical field names
                        fields.update(field_mappings.keys())
        
        return fields
    
    @staticmethod
    def get_scd_type(mapping: Dict[str, Any]) -> str:
        """
        Get SCD type from mapping.
        
        Args:
            mapping: Canonical mapping dictionary
            
        Returns:
            SCD type ('type_1' or 'type_2')
        """
        return mapping.get('scd_type', 'type_1')


class CanonicalFieldTypeMapper:
    """Maps canonical fields to appropriate ClickHouse types"""
    
    @staticmethod
    def determine_clickhouse_type(field_name: str, mapping: Dict[str, Any] = None) -> str:
        """
        Determine appropriate ClickHouse type for a canonical field with priority order:
        1. Explicit mapping file types (primary)
        2. Standard metadata types
        3. Pattern-based inference (fallback)
        
        Args:
            field_name: Name of the canonical field
            mapping: Optional canonical mapping dictionary containing field_types
            
        Returns:
            ClickHouse type definition
        """
        # Priority 1: Explicit field types from mapping
        if mapping and 'field_types' in mapping:
            explicit_types = mapping['field_types']
            if field_name in explicit_types:
                return explicit_types[field_name]
        
        # Priority 2: Standard metadata types
        standard_types = CanonicalSchemaManager.get_clickhouse_field_types()
        if field_name in standard_types:
            return standard_types[field_name]
        
        # Priority 3: Pattern-based fallback (legacy logic for unmapped fields)
        return CanonicalFieldTypeMapper._pattern_based_type_inference(field_name)
    
    @staticmethod
    def _pattern_based_type_inference(field_name: str) -> str:
        """
        Legacy pattern-based type inference for unmapped fields.
        
        Args:
            field_name: Name of the canonical field
            
        Returns:
            ClickHouse type definition based on pattern matching
        """
        # Additional business field type mappings (legacy support)
        type_mappings = {
            # Date fields
            'date_acquired': 'Nullable(Date)',
            'birth_day': 'Nullable(Date)',
            'anniversary': 'Nullable(Date)',
            'created_date': 'DateTime',
            'closed_date': 'Nullable(DateTime)',
            'required_date': 'Nullable(DateTime)',
            'time_start': 'Nullable(DateTime)',
            'time_end': 'Nullable(DateTime)',
            'date_entered': 'DateTime',
            'last_updated': 'DateTime',
            
            # Boolean fields
            'lead_flag': 'Nullable(Bool)',
            'unsubscribe_flag': 'Nullable(Bool)',
            'married_flag': 'Nullable(Bool)',
            'children_flag': 'Nullable(Bool)',
            'disable_portal_login_flag': 'Nullable(Bool)',
            'inactive_flag': 'Nullable(Bool)',
            'approved': 'Nullable(Bool)',
            
            # Numeric fields
            'annual_revenue': 'Nullable(Float64)',
            'number_of_employees': 'Nullable(UInt32)',
            'budget_hours': 'Nullable(Float64)',
            'actual_hours': 'Nullable(Float64)',
            'hours_deduct': 'Nullable(Float64)',
        }
        
        # Check business field mappings
        if field_name in type_mappings:
            return type_mappings[field_name]
        
        # Pattern-based matching
        if field_name.endswith('_id'):
            return 'Nullable(String)'
        elif field_name.endswith('_flag'):
            return 'Nullable(Bool)'
        elif field_name.endswith('_date'):
            return 'Nullable(DateTime)'
        elif field_name.endswith('_hours'):
            return 'Nullable(Float64)'
        elif field_name.endswith('_count'):
            return 'Nullable(UInt32)'
        elif 'revenue' in field_name or 'amount' in field_name:
            return 'Nullable(Float64)'
        elif 'phone' in field_name or 'fax' in field_name:
            return 'Nullable(String)'  # Phone/fax numbers are always strings
        elif 'number' in field_name and 'phone' not in field_name and 'fax' not in field_name:
            # Be more cautious with number fields - default to string unless explicitly numeric
            return 'Nullable(String)'
        else:
            # Default to nullable string
            return 'Nullable(String)'
    
    @staticmethod
    def load_field_types(table_name: str, mappings_dir: str = None) -> Dict[str, str]:
        """
        Load explicit field types from canonical mapping file.
        
        Args:
            table_name: Name of the canonical table
            mappings_dir: Directory containing mapping files (optional)
            
        Returns:
            Dictionary mapping field names to ClickHouse types
        """
        try:
            mapping = CanonicalSchemaManager.load_canonical_mapping(table_name, mappings_dir)
            return mapping.get('field_types', {})
        except FileNotFoundError:
            return {}
    
    @staticmethod
    def validate_field_types(field_types: Dict[str, str]) -> List[str]:
        """
        Validate that all field types are valid ClickHouse types.
        
        Args:
            field_types: Dictionary of field name to type mappings
            
        Returns:
            List of validation errors (empty if all valid)
        """
        errors = []
        valid_base_types = {
            'String', 'Bool', 'UInt8', 'UInt16', 'UInt32', 'UInt64',
            'Int8', 'Int16', 'Int32', 'Int64', 'Float32', 'Float64',
            'Date', 'DateTime', 'UUID'
        }
        
        for field_name, field_type in field_types.items():
            # Extract base type from nullable wrapper
            base_type = field_type
            if field_type.startswith('Nullable(') and field_type.endswith(')'):
                base_type = field_type[9:-1]  # Remove 'Nullable(' and ')'
            
            # Check for DEFAULT clause
            if ' DEFAULT ' in base_type:
                base_type = base_type.split(' DEFAULT ')[0].strip()
            
            if base_type not in valid_base_types:
                errors.append(f"Invalid ClickHouse type for field '{field_name}': {field_type}")
        
        return errors
    
    @staticmethod
    def get_missing_field_types(canonical_fields: Set[str], explicit_types: Dict[str, str]) -> Set[str]:
        """
        Identify fields without explicit type definitions.
        
        Args:
            canonical_fields: Set of canonical field names
            explicit_types: Dictionary of explicit field type mappings
            
        Returns:
            Set of field names missing explicit type definitions
        """
        return canonical_fields - set(explicit_types.keys())


# Convenience functions for backward compatibility
def get_standard_metadata_fields(scd_type: str = 'type_1') -> List[str]:
    """Get standard metadata fields for canonical data"""
    return CanonicalSchemaManager.get_standard_metadata_fields(scd_type)

def get_complete_canonical_schema(table_name: str, mappings_dir: str = None) -> List[str]:
    """Get complete schema for a canonical table"""
    mapping = CanonicalSchemaManager.load_canonical_mapping(table_name, mappings_dir)
    canonical_fields = CanonicalSchemaManager.extract_canonical_fields(mapping)
    scd_type = CanonicalSchemaManager.get_scd_type(mapping)
    
    return CanonicalSchemaManager.get_complete_schema(table_name, canonical_fields, scd_type)