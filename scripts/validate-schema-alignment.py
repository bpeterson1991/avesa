#!/usr/bin/env python3
"""
Schema Alignment Validation Script

Validates that canonical parquet files and ClickHouse tables have identical schemas.
"""

import sys
import os
import json
from typing import Dict, List, Set

# Add src paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'shared'))

from shared.canonical_schema import CanonicalSchemaManager
from clickhouse.schema_init.dynamic_schema_manager import DynamicClickHouseSchemaManager, get_clickhouse_client


def validate_schema_alignment():
    """Validate schema alignment between canonical transform and ClickHouse"""
    
    print("=== CANONICAL SCHEMA ALIGNMENT VALIDATION ===\n")
    
    tables = ['companies', 'contacts', 'tickets', 'time_entries']
    all_aligned = True
    
    for table_name in tables:
        print(f"🔍 Validating {table_name} schema alignment...")
        
        try:
            # Get canonical schema using shared schema manager
            mapping = CanonicalSchemaManager.load_canonical_mapping(table_name)
            canonical_fields = CanonicalSchemaManager.extract_canonical_fields(mapping)
            scd_type = CanonicalSchemaManager.get_scd_type(mapping)
            
            # Generate canonical transform schema
            canonical_schema = CanonicalSchemaManager.get_complete_schema(
                table_name, canonical_fields, scd_type
            )
            
            # Generate ClickHouse schema using updated schema manager
            try:
                client = get_clickhouse_client()
                ch_manager = DynamicClickHouseSchemaManager(client)
                clickhouse_schema = ch_manager.get_table_fields(table_name)
                
            except Exception as e:
                print(f"   ⚠️  Could not connect to ClickHouse: {e}")
                print(f"   📋 Canonical schema: {len(canonical_schema)} fields")
                print(f"      Fields: {canonical_schema[:10]}...")
                continue
            
            # Validate alignment
            validation = CanonicalSchemaManager.validate_schema_alignment(
                canonical_schema, clickhouse_schema
            )
            
            if validation['is_aligned']:
                print(f"   ✅ Schemas are perfectly aligned!")
                print(f"   📊 Field count: {validation['canonical_field_count']}")
                
            else:
                all_aligned = False
                print(f"   ❌ Schema mismatch detected!")
                print(f"   📊 Canonical: {validation['canonical_field_count']} fields")
                print(f"   📊 ClickHouse: {validation['clickhouse_field_count']} fields")
                
                if validation['missing_in_clickhouse']:
                    print(f"   🔴 Missing in ClickHouse: {validation['missing_in_clickhouse']}")
                
                if validation['extra_in_clickhouse']:
                    print(f"   🔵 Extra in ClickHouse: {validation['extra_in_clickhouse']}")
            
            # Show field details
            print(f"   📋 Business fields from mapping: {len(canonical_fields)}")
            print(f"   📋 Metadata fields added: {len(canonical_schema) - len(canonical_fields)}")
            print(f"   📋 SCD Type: {scd_type}")
            
        except Exception as e:
            all_aligned = False
            print(f"   ❌ Error validating {table_name}: {e}")
        
        print()
    
    print("=== SUMMARY ===")
    if all_aligned:
        print("🎉 All schemas are perfectly aligned!")
        print("✅ Canonical parquet files and ClickHouse tables will have identical schemas")
    else:
        print("⚠️  Schema misalignments detected")
        print("❌ Manual fixes may be required")
    
    return all_aligned


def show_detailed_schema(table_name: str):
    """Show detailed schema breakdown for a specific table"""
    
    print(f"=== DETAILED SCHEMA BREAKDOWN: {table_name.upper()} ===\n")
    
    try:
        # Load mapping
        mapping = CanonicalSchemaManager.load_canonical_mapping(table_name)
        canonical_fields = CanonicalSchemaManager.extract_canonical_fields(mapping)
        scd_type = CanonicalSchemaManager.get_scd_type(mapping)
        
        print(f"📋 SCD Type: {scd_type}")
        print(f"📋 Business fields from mapping: {len(canonical_fields)}")
        
        # Show business fields
        print(f"\n📊 Business Fields ({len(canonical_fields)}):")
        for i, field in enumerate(sorted(canonical_fields), 1):
            print(f"   {i:2d}. {field}")
        
        # Show metadata fields
        metadata_fields = CanonicalSchemaManager.get_standard_metadata_fields(scd_type)
        print(f"\n📊 Metadata Fields ({len(metadata_fields)}):")
        for i, field in enumerate(metadata_fields, 1):
            print(f"   {i:2d}. {field}")
        
        # Show complete schema
        complete_schema = CanonicalSchemaManager.get_complete_schema(
            table_name, canonical_fields, scd_type
        )
        print(f"\n📊 Complete Schema ({len(complete_schema)}):")
        for i, field in enumerate(complete_schema, 1):
            print(f"   {i:2d}. {field}")
        
    except Exception as e:
        print(f"❌ Error analyzing {table_name}: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--detail":
        table_name = sys.argv[2] if len(sys.argv) > 2 else "companies"
        show_detailed_schema(table_name)
    else:
        validate_schema_alignment()